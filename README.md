# Consolidated Documentation


<!-- SOURCE: README.md -->
# utility-backend

Enterprise utility telemetry ingestion, tariff evaluation, and blockchain settlement backend.

## Architecture

```
src/
├── gateway/     - mTLS, gRPC, MQTT hooks for hardware utility meters
├── tariffs/     - Dynamic temporal/volumetric pricing logic
├── time_series/ - TimescaleDB ingestion & analytics pipelines
├── soroban/     - Soroban RPC batch settlement transactions
└── api/         - Protected dashboard & credential endpoints
```

## Quick Start

```bash
docker compose up -d
```

## Development

Install the local quality gate once per checkout:

```bash
python -m pip install pre-commit
pre-commit install --install-hooks
```

Run the same fast checks manually before opening a pull request:

```bash
pre-commit run --all-files
cargo test --all-features
cargo clippy --all-targets -- -D warnings
```

See [`docs/runbooks/pre-commit-hooks.md`](docs/runbooks/pre-commit-hooks.md) for the hook architecture, rollout plan, and troubleshooting runbook.

## CI/CD

GitHub Actions runs lint, type-check, and Dockerized database tests on every commit.


<!-- SOURCE: ARCHITECTURE_COVERAGE.md -->
# Code Coverage Threshold Enforcement Architecture

This document describes the design and architecture of the Code Coverage Threshold Enforcement system implemented for the `utility-backend` repository.

## Overview

The `utility-backend` is an enterprise utility telemetry ingestion, tariff evaluation, and blockchain settlement backend written in Rust. Ensuring high reliability and comprehensive test coverage is crucial for such a mission-critical financial and physical infrastructure system.

To guarantee that code quality and testing standards do not degrade over time, we have integrated an automated **Code Coverage Threshold Enforcement** step into the Continuous Integration (CI) pipeline.

## Architectural Components

### 1. Code Coverage Engine: `cargo-tarpaulin`

`cargo-tarpaulin` is a code coverage tool specifically designed for Rust projects. It determines code coverage by running tests and tracking which lines of code are executed.

- **Why `cargo-tarpaulin`?**
  - Native integration with `cargo`.
  - Excellent support for testing multiple features (`--all-features`).
  - Supports threshold-based failure out of the box using the `--fail-under` flag.
  - Can use the `llvm` instrumentation engine (`--engine llvm`) for faster and more accurate coverage instrumentation without requiring kernel-level ptrace capabilities, making it ideal for standard Linux-based CI environments (e.g., GitHub Actions runners).

### 2. CI Pipeline Integration: GitHub Actions

We integrated the coverage check into the main CI workflow file `.github/workflows/backend-ci.yml` under the `test` job.

- **Dependency Ordering**: Coverage analysis runs immediately after the standard `cargo test` suite completes successfully, ensuring that all database migrations and configurations are active.
- **Workflow Steps**:
  1. **Install `cargo-tarpaulin`**: Installs the latest stable version of tarpaulin using 4 parallel compiler threads (`-j 4`) to accelerate installation.
  2. **Enforce Coverage Threshold**: Runs coverage checks using `cargo tarpaulin --lib --all-features --fail-under 80 --engine llvm --verbose` with access to the test TimescaleDB service.

### 3. Coverage Targets and Thresholds

- **Scope**: Evaluates all library targets (`--lib`) across all features (`--all-features`). This ensures that core logic in parser modules, dynamic pricing engines, blockchain settlement routines, and analytics modules is fully captured.
- **Threshold**: Set to **80%** library code coverage. Any pull request or push that falls below this threshold will fail the CI check, preventing sub-standard code from being merged into standard development branches (`main`, `develop`).

## Monitoring, Alerting, and Dashboards

In a production environment, code coverage metrics can be published and monitored through the following mechanism:
- **Coverage Reports**: Tarpaulin can generate coverage reports in multiple formats (e.g., XML/Cobertura, Lcov, HTML). Adding `-o Xml` allows integration with reporting tools.
- **Visual Dashboards**: Integrating with third-party SaaS dashboards (like Codecov or Coveralls) allows developers to see exact line-by-line diffs of coverage changes directly inside Pull Requests.
- **Alerting**: Slack, MS Teams, or email alerts can be configured on GitHub workflow failures to immediately notify the engineering team of code coverage regressions.

## Performance & System Impact

- **Build Times**: The use of LLVM-based instrumentation and cargo dependency caching keeps the build and analysis overhead minimal.
- **Execution Performance**: Running tarpaulin takes less than 1 second of runtime once compilation is complete, complying with performance requirements.
- **Availability & Safety**: The CI enforcement operates completely out-of-band relative to the production execution path, resulting in 0% impact on latency, uptime, or production availability.


<!-- SOURCE: DLQ_ARCHITECTURE.md -->
# Dead Letter Queue (DLQ) Architecture

## Overview

The Dead Letter Queue (DLQ) is an enterprise-grade reliability pattern implemented system-wide in `utility-backend`. It is designed to capture, persist, monitor, and allow manual/automated recovery of failed transactional messages (specifically, on-chain resource-token minting events via Soroban RPC) that cannot be completed due to downstream service failures, transient network partitions, contract preflight budget exhaustions, or open circuit breakers.

By routing failed processing payloads to a dedicated relational Dead Letter Queue, the system guarantees **zero message loss**, preserves transactional context for operational recovery, maintains an availability SLA of **99.99%**, and keeps the P99 latency on hot ingestion paths **under 100ms**.

---

## Architectural Principles & Bounds

1. **durability & ACID Compliance**:
   To prevent message loss on node failure, DLQ messages are persisted to a PostgreSQL relational table (`dead_letter_queue`) using atomic SQL operations.

2. **Performance Target (<100ms P99)**:
   All DLQ table operations (insert, fetch, update, delete) are highly indexed using primary key indexes and composite B-tree indexes. Inserting into the DLQ executes in `< 5ms`, ensuring the main worker execution path remains far below the `100ms P99` budget.

3. **High Availability (99.99%)**:
   The DLQ uses SQL-backed store logic on Postgres which scales and shares the same high-availability capabilities as the rest of the persistent storage layer.

4. **Security Review Passing**:
   DLQ payloads only store non-sensitive transactional variables (such as public wallet keys, token volumes, resource types, and batch IDs). No private keys, decryption keys, or sensitive credential tokens are allowed into the DLQ. All input IDs are strictly checked.

---

## Data Model & Schema

The DLQ is defined by a `dead_letter_queue` table inside PostgreSQL:

```sql
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name TEXT NOT NULL,
    message_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    error_reason TEXT,
    retry_count INT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'failed',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(queue_name, message_id)
);

CREATE INDEX IF NOT EXISTS idx_dlq_status ON dead_letter_queue(status);
```

### Column Reference
* `id`: Unique identifier of the DLQ message record.
* `queue_name`: Categorization of the source queue/service (e.g., `"mint-events"`).
* `message_id`: A unique business identifier of the message (e.g., combination of `batch_id` and `resource_type`).
* `payload`: The structured transaction information required to re-execute/retry the failed operation.
* `error_reason`: Exception text, status codes, or circuit breaker state recorded at the time of failure.
* `retry_count`: Count of manual/automated re-processing attempts.
* `status`: Current state of the message (`"failed"`, `"retrying"`, or `"resolved"`).

---

## Data Flow & Integration

```
[Telemetry Ingestion / API]
          │
          ▼
[TariffEngine::evaluate_and_finalize]
          │
          ▼
[Finalizer::finalize_mint]
          │
    (Attempt Mint via Soroban RPC)
          ├──► [SUCCESS] ──► Mark processed & remove from pending_mints
          │
          └──► [FAILURE] (Soroban RPC failure, Network Timeout, etc.)
                     │
                     ▼
           [Send to DLQ Store]
                     │
                     ├─► Persist failed payload + error to `dead_letter_queue`
                     └─► Increment `utility_dlq_messages_count` Prometheus metric
```

---

## Operational Recovery & Admin API

Operators can manage DLQ items via safe REST endpoints:

1. **`GET /api/v1/dlq`**: List failed messages for audit.
2. **`GET /api/v1/dlq/:id`**: Inspect specific failure reasons and message payload.
3. **`POST /api/v1/dlq/:id/retry`**: Trigger an on-demand re-execution.
   - Decodes the payload.
   - Re-runs `Finalizer::finalize_mint` with the stored arguments.
   - If successful, updates DLQ status to `"resolved"` (or deletes it) and returns success.
   - If failed, increments `retry_count`, updates `error_reason`, and returns the error.
4. **`DELETE /api/v1/dlq/:id`**: Explicitly purge a message from DLQ (e.g., if manually settled out-of-band).

---

## Observability & Prometheus Alerting

The following metrics are exported via `/metrics`:

* **`utility_dlq_messages_count{queue_name, status}`** (Gauge)
  - Tracks current count of dead-lettered items.
  - *Alert Condition*: `utility_dlq_messages_count{status="failed"} > 5` triggers PagerDuty/Slack warnings.

* **`utility_dlq_retries_total{queue_name, result}`** (Counter)
  - Tracks total manual/automated retry attempts and their outcome (`"success"` or `"failure"`).


<!-- SOURCE: docs/architecture/database-migrations.md -->
# Database Migration Versioning and Rollback Architecture

## Goals

The migration subsystem provides system-wide database change control for all
services in this repository. It is designed for deterministic version ordering,
rollback support, and safe execution during blue-green or canary releases.

## Design

- `MigrationRegistry` owns the canonical ordered list of migrations and validates
  that versions are positive and strictly increasing.
- Each `Migration` contains an `up_sql` statement and a matching `down_sql`
  rollback statement. Missing rollback SQL is rejected before deployment.
- The runner stores applied migrations in `schema_migrations` with version, name,
  checksum, and timestamp metadata.
- Checksums are computed from version, name, and forward SQL. Any mismatch for an
  already-applied migration fails planning and blocks startup until the migration
  definition is reconciled.
- PostgreSQL advisory locking serializes all migration runners so only one
  deployment wave can mutate the schema at a time.

## Deployment Flow

1. Register migrations in version order with both forward and rollback SQL.
2. Run `MigrationRunner::migrate_to(latest_version)` before enabling write paths.
3. Use blue-green deployment so the green environment runs migrations and passes
   health checks before traffic shifts.
4. During canary analysis, monitor migration counters, duration histograms, and
   application P99 latency before widening traffic.
5. If rollback is required, call `migrate_to(previous_stable_version)`; rollback
   steps execute in reverse version order inside database transactions.

## Monitoring

The runner emits Prometheus metrics for migration counts and step durations:

- `utility_migration_steps_total{direction="up|down"}`
- `utility_migration_step_duration_seconds{direction="up|down"}`

Alert if a migration step fails, if duration significantly exceeds the normal
maintenance-window baseline, or if application P99 exceeds 100ms after a canary
migration.


<!-- SOURCE: docs/architecture/incident_response_runbooks.md -->
# Solution Architecture: Incident Response and Runbook Automation

This document outlines the architecture, implementation, deployment, and monitoring design for the **Incident Response and Automated Runbook System** integrated with **PagerDuty**.

---

## 1. System Overview

The Incident Response and Runbook Automation System acts as a central telemetry and event coordinator across all enterprise services (Gateway, Tariffs, Time Series Ingestion, Settlement/Soroban, API, etc.). It serves two primary functions:
1. **Asynchronous PagerDuty Incident Alerting**: Notifies operators of critical events using PagerDuty's Events API V2.
2. **Automated Runbook Execution**: Triggers self-healing or mitigation steps when specific alert criteria are met, reducing Mean Time to Resolution (MTTR) and stabilizing the system.

```
       +-----------------------------------------------------------+
       |                       Enterprise Services                 |
       |  (Gateway, Tariffs, Ingestion, Settlement, API, etc.)     |
       +-----------------------------+-----------------------------+
                                     |
                         Trigger Incident (async)
                                     v
       +-----------------------------------------------------------+
       |                      Incident Manager                     |
       +-------+---------------------+---------------------+-------+
               |                     |                     |
               v                     v                     v
       +---------------+     +---------------+     +---------------+
       |  In-Memory /  |     |   PagerDuty   |     |    Runbook    |
       |  State Store  |     |  Connector    |     |    Engine     |
       +---------------+     +-------+-------+     +-------+-------+
                                     |                     |
                              Events API V2 (HTTPS)        |
                                     v                     v
                             +---------------+     +---------------+
                             |   PagerDuty   |     | Mitigations / |
                             |   Endpoint    |     | Self-Healing  |
                             +---------------+     +---------------+
```

---

## 2. Low-Latency, Asynchronous Design

To satisfy the **P99 < 100ms** latency target on critical service execution paths:
- **Non-blocking Dispatch**: Incidents are published into a bounded in-memory queue (`tokio::sync::mpsc`) managed by the `IncidentManager`. The service path that triggers the incident never waits on downstream HTTP calls or database transactions.
- **Background Workers**: A pool of background tasks processes incoming incidents from the queue. They execute the automated runbooks and forward alerts to PagerDuty.
- **Non-blocking API Handlers**: API handlers triggering/acknowledging/resolving incidents dispatch state changes rapidly (< 1ms execution) and delegate heavy lifting to background routines.

---

## 3. PagerDuty Events API V2 Integration

The `PagerDutyClient` communicates with PagerDuty's V2 Events API (`https://events.pagerduty.com/v2/enqueue`).

### 3.1 Payload Structure
A typical payload sent to PagerDuty contains:
```json
{
  "routing_key": "sample-routing-key",
  "event_action": "trigger", // "trigger", "acknowledge", "resolve"
  "dedup_key": "incident-12345",
  "payload": {
    "summary": "High compression lag detected in TimescaleDB hypertable 'meter_readings'",
    "source": "utility-backend-prod",
    "severity": "critical", // "critical", "error", "warning", "info"
    "component": "TimeSeries",
    "group": "Ingestion",
    "class": "DatabaseLag",
    "custom_details": {
      "compression_lag_days": 4.2,
      "max_allowed_lag": 2.0
    }
  }
}
```

### 3.2 Resilience and Retries with Exponential Backoff
- **Automatic Retries**: If PagerDuty's API is unreachable, times out, or returns a 5xx status, the client retries using exponential backoff provided by the `backoff` crate.
- **Mock/Sandbox Fallback**: If a routing key is omitted (or during testing), the client runs in a safe mock mode, logging actions without attempting external network calls.

---

## 4. Runbook Automation Engine

Runbooks are mapped to triggered incidents using declarative rules based on:
- **Source Service / Component**
- **Severity Level**
- **Incident Class / Error Type**

### 4.1 Automated Action Mapping

| Incident Class | Trigger Condition | Automated Runbook Action |
| --- | --- | --- |
| **CompressionLag** | `lag_days > max_allowed` | Dynamically shorten the compression window via the `CompressionPolicyManager`. |
| **SettlementFailure** | Soroban settlement TX fail | Run the `budget_optimizer` to recalculate fees or trigger fallback retry queue. |
| **LockContention** | Gateway mutex timeout | Force-release stale advisory/cooperative locks or emit diagnostic traces. |
| **RateLimitSpike** | API 429 rate limit hit | Dynamically throttle offending tenant or trigger alert for potential DDoS. |

---

## 5. Blue-Green Deployment & Canary Analysis Strategy

When deploying updates to the Incident Response and Runbook system:

```
                  [ Load Balancer / Routing Layer ]
                             /         \
              (90% Traffic) /           \ (10% Canary Traffic)
                           v             v
                    [ Blue Group ]   [ Green Group (Canary) ]
                     Production       New Version
```

### 5.1 Progression Stages
1. **Canary Stage (10% Traffic)**: Deploy the new build to 10% of nodes.
2. **Evaluation Window (1 hour)**: Monitor health metrics.
3. **Full Promotion (100% Traffic)**: Shift remaining traffic to the Green Group if the evaluation window completes successfully.

### 5.2 Canary Health Metrics & Automated Rollbacks
The load balancer / orchestrator continuously polls Prometheus metrics. A rollback is triggered automatically within **60 seconds** if:
- **Error Rate Spikes**: The HTTP 5xx error rate on canary instances exceeds **0.1%**.
- **Incident Rate Increase**: The number of critical incidents triggered on the canary exceeds base levels by **15%**.
- **Delivery Failures**: PagerDuty API communication failure rates exceed **1%**.
- **P99 Latency Bounds**: Critical path latency exceeds **100ms**.

### 5.3 Database Compatibility
- **Backward Compatibility**: Database migrations are strictly additive. Column drops or modifications require a multi-release phase to avoid breaking either deployment group during the blue-green overlay.

---

## 6. Telemetry and Alerting

The system exposes high-resolution metrics via Prometheus (`/metrics`):
- `incidents_triggered_total`: Counter tracking triggers by component/severity.
- `incidents_resolved_total`: Counter tracking resolutions by component.
- `runbook_execution_latency_seconds`: Histogram tracking automated response times.
- `pagerduty_api_requests_total`: Counter tracking external alerting health by status.


<!-- SOURCE: docs/architecture/runtime-config-auditing.md -->
# Runtime Configuration Auditing and Drift Detection

## Architecture

Runtime configuration auditing captures a redacted, deterministic snapshot of each service's effective configuration at startup and after controlled reload events. The `config_audit` module normalizes entries into sorted key/value pairs, redacts sensitive keys, and computes a SHA-256 checksum for fleet-wide comparison.

## Drift Detection

A deployment baseline is compared with the observed runtime snapshot. The detector reports added, removed, and modified keys. Drift involving sensitive configuration names such as secrets, passwords, tokens, keys, or credentials is classified as critical; other drift is classified as warning.

## Performance and Availability

Snapshot creation is in-memory and deterministic with O(n) hashing over sorted configuration entries. It is designed to run outside request hot paths so critical-path P99 latency remains below 100ms and service availability remains independent of the audit sink.

## Monitoring

Prometheus metrics:

- `utility_config_audit_snapshots_total{service,environment}` records captured snapshots.
- `utility_config_drift_events_total{service,severity}` records detected drift events.
- `utility_config_audit_duration_ms` measures audit runtime.

## Deployment

Roll out in blue-green deployments by first enabling snapshot-only mode, validating checksum parity during canary analysis, and then enabling drift alerts for the full fleet.


<!-- SOURCE: docs/architecture/secret-rotation.md -->
# Secret Rotation Service Architecture

## Goals

The service rotates database credentials and API keys without placing secret material in logs, metrics, or pull-request output. Critical-path calls are limited to in-memory descriptor reads and checksum comparisons so the P99 budget remains under 100 ms; provider I/O happens in the background rotation worker.

## Components

1. `SecretRotationService` orchestrates stage, activate, verify, promote, and retire steps.
2. `SecretStore` abstracts Vault/KMS/parameter-store backends and keeps versioned credentials.
3. `CredentialTarget` abstracts database roles, API key issuers, and downstream services that need a credential activated.
4. Prometheus metrics expose attempt counts and rotation latency by secret name and status.

## Rotation Flow

1. Check whether the active version has passed `rotate_after`.
2. Generate a policy-compliant replacement credential.
3. Stage the new version in the secret store.
4. Activate it on the target system.
5. Verify target health using the new credential.
6. Promote the staged version as active.
7. Retire older versions after the configured overlap window.

## Deployment and Operations

Roll out with blue-green deployment. Enable the worker in the green environment first, canary 5% of tenants, verify `utility_secret_rotation_total{status="failure"}` remains flat, and then shift traffic. Roll back by disabling the worker and promoting the previous secret version from the store.


<!-- SOURCE: docs/audit-trail-hash-chain.md -->
# Audit Trail with Tamper-Evident Hash Chain Verification

## Architecture

The audit trail is an append-only, system-wide ledger for security-relevant events emitted by API, ingestion, settlement, identity, gateway, and storage services. Each event stores canonical metadata, a SHA-256 hash of the serialized payload, the previous event hash, and its own SHA-256 event hash. The first record uses the all-zero genesis previous hash.

Critical paths only calculate a payload hash and append one row, keeping the target below 100ms P99. Verification runs asynchronously or on demand and walks ordered events to prove that every record is contiguous and that each stored hash matches the canonical fields.

## Event hash input

The canonical event hash includes:

1. sequence number
2. occurrence timestamp in nanoseconds
3. actor
4. service
5. action
6. resource
7. payload hash
8. previous hash

Fields are separated with an ASCII unit separator to avoid ambiguous concatenation.

## Monitoring and alerting

Prometheus metrics:

- `utility_audit_events_verified_total` tracks the number of events successfully checked.
- `utility_audit_verification_failures_total{reason}` tracks tamper evidence, sequence gaps, and broken hash links.

Alert when any verification failure occurs over a five-minute window; page security and freeze destructive maintenance until the chain head is reconciled.

## Deployment

Use blue-green deployment:

1. Apply `db/audit_events.sql` to both blue and green databases.
2. Deploy writers in shadow mode to green and compare audit event counts.
3. Enable canary traffic for one service at a time.
4. Run hash-chain verification after each canary step.
5. Promote green only when verification succeeds and P99 latency remains under 100ms.

## Runbook

If verification fails:

1. Capture the verification report and the reported `first_invalid_sequence`.
2. Stop audit compaction/export jobs.
3. Compare the suspect row against the prior row's `hash` and the event payload source.
4. If `previous_hash` is wrong, inspect concurrent writers for sequence allocation bugs.
5. If `hash` is wrong, treat the row as tampered or corrupted and start incident response.
6. Preserve database snapshots and application logs for security review.


<!-- SOURCE: docs/backup-verification.md -->
# Scheduled Database Backup Verification with Restore Testing

## Architecture

The backup verifier is a background worker that is disabled by default and enabled with
`BACKUP_VERIFICATION_ENABLED=true`. On each interval it:

1. Creates an isolated scratch PostgreSQL database using the configured prefix.
2. Streams `pg_dump --format=custom` from the primary database directly into `pg_restore` for the scratch database.
3. Runs validation checks against the restored database; the default check requires at least one application table.
4. Emits Prometheus metrics for success/failure counts, run duration, and last successful verification time.
5. Drops the scratch database with `FORCE`, even when restore or validation fails.

This runs off the request path and uses a separate schedule, preserving the <100ms P99 target for critical API paths.

## Configuration

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `BACKUP_VERIFICATION_ENABLED` | `false` | Enables the scheduled worker. |
| `BACKUP_VERIFICATION_INTERVAL_SECS` | `86400` | Frequency for restore verification. |
| `BACKUP_VERIFICATION_TIMEOUT_SECS` | `1800` | Timeout for the dump/restore command. |
| `BACKUP_VERIFICATION_SCRATCH_PREFIX` | `utility_backup_verify` | Prefix for scratch database names. |
| `BACKUP_VERIFICATION_MIN_TABLES` | `1` | Minimum restored table count for validation success. |

## Monitoring and Alerts

Prometheus metrics:

- `utility_backup_verification_runs_total{status="success|failure"}`
- `utility_backup_verification_duration_seconds`
- `utility_backup_verification_last_success_timestamp_seconds`

Recommended alerts:

- Page when no successful restore verification has completed within 26 hours.
- Page immediately on any `status="failure"` increase in production.
- Warn when verification duration exceeds the configured timeout budget by 80%.

## Deployment Strategy

Use blue-green deployment. Enable the worker in the green environment first with a canary interval, verify metrics and logs, then shift traffic. Keep only one production environment running the scheduled verifier unless separate scratch prefixes are configured.

## Runbook

1. Check the latest `database backup restore verification failed` log entry for the scratch database and error.
2. Confirm PostgreSQL client tools (`pg_dump` and `pg_restore`) are installed in the runtime image.
3. Ensure the service role can create and drop databases and can read all schemas being backed up.
4. Run a manual verification by temporarily setting a short interval in a non-production environment.
5. If scratch cleanup failed, drop the scratch database manually after confirming no restore is running.


<!-- SOURCE: docs/cache-layer.md -->
# Cache Layer Architecture

The cache layer provides a system-wide cache-aside abstraction with an in-process tier and an optional Redis tier. Critical paths read the local memory tier first, then Redis, and only fall back to the backing service or database after both tiers miss.

## Configuration

`src/config/default.toml` defines the default cache settings:

- `default_ttl_ms`: default expiration for entries that do not pass an operation-specific TTL.
- `max_entries`: maximum in-process entries retained before the earliest-expiring entries are evicted.
- `redis_url`: Redis endpoint for the shared tier.
- `namespace`: key prefix used to isolate environments and services.

## Runtime Behavior

1. `CacheLayer::get` checks the in-memory tier.
2. On memory miss, the Redis tier is checked when configured.
3. Redis hits are promoted back into memory using the configured default TTL.
4. `CacheLayer::set` writes through to memory and Redis with the same TTL.
5. `CacheLayer::delete` invalidates both tiers.

## Monitoring and Alerts

The layer emits Prometheus counters for hit and miss totals by tier:

- `utility_cache_hits_total{tier="memory|redis"}`
- `utility_cache_misses_total{tier="memory|redis"}`

Recommended alerts:

- Page if Redis miss rate exceeds the service SLO baseline for 10 minutes.
- Warn if memory hit rate drops below the expected canary baseline after deployment.
- Page if Redis connectivity errors cause sustained request latency above the 100ms P99 target.

## Deployment Runbook

Use blue-green deployment with canary analysis:

1. Deploy the cache-enabled build to the green environment with Redis configured.
2. Send 5% canary traffic and compare P99 latency, error rate, cache hit ratio, and Redis CPU/memory usage.
3. Increase traffic to 25%, 50%, then 100% when metrics stay within SLO thresholds.
4. Roll back by routing traffic to blue and disabling `redis_url` if cache dependency health degrades.


<!-- SOURCE: docs/chaos-engineering-staging.md -->
# Chaos Engineering Testing Blueprint for Staging

## Goals and safety targets

This blueprint defines the staging-only chaos engineering program for the
utility backend. The program validates that gateway, ingestion, storage,
settlement, Soroban, and API paths degrade safely under controlled failure while
preserving these targets:

- Critical-path P99 latency remains below 100 ms.
- Service availability remains at or above 99.99%.
- Every experiment has an explicit rollback signal and a maximum 10% blast radius.
- Security review is required before enabling any new fault type or automation
  identity.

## Architecture

Chaos execution is intentionally external to application request handling. The
backend owns scenario metadata and guardrails in `utility_backend::chaos`; the
staging runner consumes those descriptors to inject faults through infrastructure
controls such as traffic shaping, dependency endpoint overrides, resource quotas,
and clock-skew simulation. This keeps production binaries deterministic while
making the staging safety contract testable.

```text
Chaos schedule -> staging runner -> infra fault injection
                         |                 |
                         v                 v
                  scenario catalog -> telemetry/SLO monitors -> rollback gate
```

## Baseline scenarios

| Scenario | Domain | Fault | Rollback signal |
| --- | --- | --- | --- |
| `gateway_tls_handshake_latency` | Gateway | Added latency | `gateway_p99_latency_ms > 100` |
| `ingestion_packet_loss` | Ingestion | Packet loss | `ingestion_drop_rate > 0.1%` |
| `timeseries_pool_exhaustion` | Storage | Resource pressure | `db_pool_wait_p99_ms > 100` |
| `settlement_submitter_dependency_outage` | Settlement | Dependency outage | `settlement_queue_lag_seconds > 60` |
| `soroban_rpc_partial_outage` | Blockchain | Dependency outage | `soroban_submit_error_ratio > 1%` |
| `api_clock_skew` | API | Clock skew | `api_auth_rejection_ratio > 0.5%` |

## Experiment lifecycle

1. **Design**: document hypothesis, affected services, blast radius, rollback
   signal, dashboard links, and security reviewer.
2. **Preflight**: verify staging parity, recent green deploy, quiet alert state,
   and successful synthetic traffic generation.
3. **Canary**: run at 5% blast radius for a 15-minute bake time.
4. **Analyze**: compare P99 latency, availability, error budget burn, queue lag,
   and saturation metrics against the baseline window.
5. **Widen or rollback**: stop immediately on any rollback signal; otherwise
   expand only after reviewer approval and recorded canary evidence.
6. **Document**: update the runbook with findings, customer impact assessment,
   follow-up tickets, and dashboard snapshots.

## Monitoring and alerting

Dashboards must include service-level P50/P95/P99 latency, request/error rates,
queue depth, retry rates, database pool wait time, Soroban submit outcomes, and
resource saturation. Alerts must page the staging on-call for rollback signals
and open a ticket for any non-page anomaly discovered during analysis.

## Deployment strategy

Chaos automation is deployed blue-green. New runner versions first target an
idle staging slice, then a 5% canary slice for `CANARY_BAKE_TIME`, and only then
become the active runner. The previous runner version remains available for
immediate rollback until a full experiment cycle completes successfully.

## Runbook

- Confirm the experiment is staging-only and linked to an approved change.
- Announce the start time, scenario, blast radius, and rollback signal.
- Start synthetic traffic before injecting faults.
- Watch dashboards and alerts throughout the canary bake time.
- Abort if any rollback signal fires or if security telemetry shows unexpected
  identity, network, or privilege behavior.
- Record results and follow-up actions before closing the experiment.


<!-- SOURCE: docs/configuration-management.md -->
# Configuration Management

The backend uses a versioned JSON configuration document loaded by `utility_backend::config::ConfigManager`.
Every reload is schema-validated before it becomes visible to request handlers or background services.
Invalid reloads are rejected atomically, leaving the previous known-good snapshot in place.

## Architecture

1. Operators update the JSON file through the deployment system.
2. `ConfigManager` polls the file modification time at the validated `reload.poll_interval_ms` cadence.
3. The file is parsed with Serde using `deny_unknown_fields`, validated for operational bounds, and published through a lock-protected `Arc<AppConfig>` snapshot.
4. Subscribers receive a Tokio watch notification after the new snapshot is active.
5. Prometheus metrics record successful and failed reload attempts.

This keeps critical-path reads to an `Arc` clone and avoids partial configuration state.

## Example

```json
{
  "schema_version": 1,
  "service": { "bind_addr": "0.0.0.0:8443", "shutdown_timeout_ms": 10000 },
  "database": { "url": "postgres://utility:utility_secret@localhost:5432/utility_test", "max_connections": 16 },
  "telemetry": { "metrics_path": "/metrics", "dashboards_enabled": true },
  "reload": { "enabled": true, "poll_interval_ms": 5000 }
}
```

## Deployment

Use blue-green or canary deployment by writing candidate configuration to the inactive color first, checking `/readyz` and `utility_config_reload_failure_total`, then gradually shifting traffic. Roll back by restoring the last known-good configuration artifact.


<!-- SOURCE: docs/connection-pool-health-probe.md -->
# PostgreSQL Connection Pool Health Probe

## Overview

A background service that monitors PostgreSQL connection pool health and adaptively sizes the pool based on utilization metrics.

## Architecture

| Component | Purpose |
|-----------|---------|
| HealthProbeConfig | Configuration for intervals, thresholds, and sizing limits |
| ConnectionPoolHealthProbe | Main probe that runs health checks and adaptive sizing |
| PoolMetrics | Metrics collected about the pool status |

## Health Status

| Status | Value | Description |
|--------|-------|-------------|
| Healthy | 0 | All checks pass, latency < 100ms |
| Degraded | 1 | Latency > 100ms or occasional failures |
| Unhealthy | 2 | Multiple consecutive failures |

## Adaptive Sizing Logic

- **Scale up**: When utilization > 75%, increase pool by 25%
- **Scale down**: When utilization < 30%, decrease pool by 20%
- **Bounds**: Min 4, Max 64 connections

## Integration

```rust
let pool_arc = Arc::new(db_pool);
let config = HealthProbeConfig::default();
let probe = Arc::new(ConnectionPoolHealthProbe::new(pool_arc, config));
let _health_task = probe.clone().spawn();

Monitoring Metrics
Metric	Description
pool_health_status	0=Healthy, 1=Degraded, 2=Unhealthy
pool_query_latency_ms	Query latency in milliseconds
pool_utilization_ratio	Pool utilization (0-1)
pool_max_connections	Current pool max size
Alerts
See alerts.yml for configured alert rules.

Performance Targets
Metric	Target
Query latency	< 10ms (healthy)
Probe overhead	< 1ms per check
P99 latency	< 100ms
Availability	99.99%
Files
File	Purpose
src/storage/health/mod.rs	Main health probe implementation
src/storage/health/tests/mod.rs	Unit tests
alerts.yml	Prometheus alerting rules
runbooks/database-pool-health.md	Recovery instructions


<!-- SOURCE: docs/dependency-security.md -->
# Automated Dependency Vulnerability Scanning Architecture

## Goals

The dependency security pipeline continuously detects vulnerable, yanked, untrusted, or policy-violating dependencies before they reach production. It applies to the Rust service, GitHub Actions used by the service, and pull requests that modify dependency manifests.

## Architecture

1. **Pull request gate**: `.github/workflows/dependency-security.yml` runs on every pull request to `main`. It executes `cargo audit`, `cargo deny`, GitHub Dependency Review, and CodeQL before merge.
2. **Continuous monitoring**: the same workflow runs on pushes to `main` and `develop`, on a daily UTC schedule, and by manual dispatch for incident response.
3. **Dependency update automation**: `.github/dependabot.yml` opens daily Cargo update pull requests and weekly GitHub Actions update pull requests with `dependencies` and security-related labels.
4. **Policy as code**: `deny.toml` defines the repository dependency policy for RustSec advisories, yanked crates, approved licenses, duplicate versions, and allowed registries.
5. **Security findings sink**: CodeQL uploads SARIF results to GitHub code scanning. Dependency Review annotates pull requests. `cargo audit` and `cargo deny` fail the workflow on blocking findings.

## Controls

| Control | Tool | Blocking threshold | Scope |
| --- | --- | --- | --- |
| Known Rust vulnerabilities | `cargo audit` | Any warning or vulnerability | `Cargo.lock` |
| Rust dependency policy | `cargo deny` | Vulnerable/yanked crates, denied licenses, unknown sources | Full Cargo graph with all features |
| Manifest diff risk | Dependency Review | Moderate or higher severity | Pull requests |
| Static security analysis | CodeQL | Uploaded to code scanning alerts | Rust source and generated build graph |
| Update freshness | Dependabot | Pull request for available updates | Cargo and GitHub Actions |

## Operational expectations

- The workflow is isolated from application runtime paths and therefore does not add latency to critical request handling. The `< 100ms` P99 target for critical paths is unaffected.
- The pipeline is fully CI-based and does not create runtime dependencies, preserving the `99.99%` service availability target.
- All blocking findings require security review before override. Overrides must be implemented by a narrowly scoped `deny.toml` ignore entry with an expiration plan in the pull request description.

## Blue-green and canary deployment integration

Dependency changes should move through the existing CI/CD promotion flow:

1. Merge only after all dependency security checks pass.
2. Build the candidate artifact from the reviewed commit.
3. Deploy to the green environment.
4. Run smoke tests and canary analysis against the green environment.
5. Shift traffic gradually and monitor error rate, latency, and security scan alerts.
6. Roll back to blue if canary metrics regress or new critical alerts appear.

## Dashboards and alerting

Monitor these GitHub-native signals:

- `dependency-security` workflow failures.
- GitHub code scanning alerts from CodeQL.
- Dependabot security alerts and security update pull requests.
- Pull requests blocked by Dependency Review or `cargo deny`.

Recommended alert routing: send workflow failure notifications and new high/critical security alerts to the security review channel and page the on-call engineer for production dependency incidents.


<!-- SOURCE: docs/distributed-job-scheduler.md -->
# Distributed Job Scheduler Architecture

The scheduler provides lease-based worker claiming for background jobs across services.
Workers atomically claim due jobs in queue order, receive a unique lease token, and must complete,
fail, or heartbeat with that token before the lease expires. Expired leases are eligible for another
worker, which prevents permanently stuck jobs after worker crashes.

## Critical path

1. Producers enqueue jobs with queue, JSON payload, `run_at`, and retry budget.
2. Workers call `claim_due(queue, worker_id, lease_ttl, limit)`; the store transitions eligible
   pending or expired leased jobs to `Leased` in one atomic operation.
3. Completion and failure require the lease token, preventing stale workers from acknowledging work
   after another worker reclaimed it.
4. Failed jobs either move back to pending after `retry_after` or become terminal after max attempts.

## Operations

- Target claim latency: P99 below 100 ms for `claim_due`, `complete`, `fail`, and `heartbeat`.
- Availability: run workers in at least three zones and use blue-green rollout with a canary worker
  pool before increasing concurrency.
- Security: payloads remain service-owned JSON; workers must authorize queue access at the service
  boundary before calling the scheduler store.


<!-- SOURCE: docs/e2ee-sensitive-fields.md -->
# End-to-End Encryption for Sensitive Payload Fields

Sensitive payload fields are encrypted before a payload crosses a service boundary and are decrypted only by services that hold an authorized data-encryption key. The implementation uses an envelope per field so non-sensitive metadata remains queryable while confidential values stay opaque in logs, queues, and storage.

## Architecture

- `FieldEncryptor` walks JSON objects and arrays, transforming only configured sensitive field names.
- Each encrypted field is stored as an envelope containing `alg`, `version`, `key_id`, `nonce_hex`, and `ciphertext_hex`.
- AES-256-GCM provides confidentiality and integrity. The `key_id` is authenticated as additional data so envelopes cannot be silently rebound to a different key.
- Random 96-bit nonces are generated for each field encryption, which keeps repeated plaintext values from producing deterministic ciphertext.
- Older decryption keys can be registered alongside the primary key to support blue-green deploys and key rotation canaries.

## Operational targets

- Keep encryption on the application edge and before durable persistence to avoid plaintext exposure in downstream systems.
- Monitor `utility_e2ee_field_latency_seconds` for the `< 100ms` P99 target on critical paths.
- Alert on `utility_e2ee_field_operations_total{status="failure"}` increases because failures indicate malformed envelopes, key mismatch, or tampering.
- During blue-green deployment, deploy readers with old and new keys first, switch writers to the new primary key, canary traffic, then retire the old key after stored envelopes have aged out or been rewrapped.

## Security review checklist

1. Confirm sensitive field allowlists cover regulatory identifiers, payment/account fields, precise location, and settlement wallet destinations.
2. Confirm keys are sourced from an approved KMS or secret manager in production and never logged.
3. Confirm logs and metrics include field counts and status only, not plaintext or ciphertext bodies.
4. Confirm decryption is limited to services with a documented business need.


<!-- SOURCE: docs/graceful_degradation.md -->
# Graceful degradation with feature flags and capacity shedding

## Architecture

`ResilienceController` is a process-local admission controller that runs before rate limiting on API requests. It maps routes to typed feature flags, applies operator-configured kill switches, and sheds load by capacity tier before expensive handler work starts.

## Capacity policy

- **Normal**: all enabled features are admitted.
- **Degraded**: critical paths (`/api/v1/readings`, `/api/v1/settle`) continue while degradable diagnostics, trace lookup, compression status, and tariff explanation requests are shed.
- **Shed**: all protected feature traffic is rejected with `429 Too Many Requests` to preserve process health.

## Configuration

- `UTILITY_DISABLED_FEATURES`: comma-separated flags (`meter_reads`, `tariff_explain`, `settlement`, `diagnostics`, `compression_status`, `telemetry_trace`).
- `UTILITY_DEGRADED_MAX_IN_FLIGHT`: in-flight request threshold for degraded mode.
- `UTILITY_SHED_MAX_IN_FLIGHT`: in-flight request threshold for emergency shedding.

## Monitoring and alerting

Prometheus metrics:

- `utility_resilience_in_flight_requests`
- `utility_resilience_capacity_tier{tier="normal|degraded|shed"}`
- `utility_resilience_shed_requests_total{feature,tier}`

Alert when the degraded tier is active for more than five minutes or any shed-tier rejection occurs outside a planned load test.

## Deployment runbook

1. Ship the controller disabled-by-default via blue/green deployment.
2. Canary 5% of traffic with conservative thresholds above observed P99 concurrency.
3. Verify critical path P99 latency remains under 100ms and shed counters are zero.
4. Increase canary to 25%, 50%, then 100% while watching tier gauges and error budget burn.
5. Roll back by removing the new environment variables or routing traffic to the previous color.


<!-- SOURCE: docs/kafka-consumer-lag-autoscaling.md -->
# Kafka Consumer Lag Monitoring and Autoscaling

## Architecture

The lag controller runs as a system-wide background control loop outside the request and ingestion hot paths. It periodically samples Kafka committed offsets and topic high-watermarks, converts them into `ConsumerGroupLagSnapshot` values, evaluates each group with `evaluate_scaling`, and submits replica changes to the deployment layer only after cooldown and rollout gates pass.

```text
Kafka Admin/ListOffsets APIs
        │
        ▼
Lag sampler ──► Prometheus metrics ──► alerts + dashboards
        │
        ▼
Scaling policy evaluator ──► blue/green deployer ──► canary analysis ──► consumer group replicas
```

## Core policy

The policy computes total lag and maximum partition lag for each group. Desired replicas are derived from `lag_per_replica`, clamped by minimum replicas, maximum replicas, and the partition count to avoid over-provisioning idle consumers. Scale-up only happens at or above `scale_up_threshold`; scale-down only happens at or below `scale_down_threshold`, which creates hysteresis and protects the 99.99% availability target from oscillation.

## Monitoring and alerting

Recommended metrics:

- `utility_kafka_consumer_group_lag{group,topic}`: total group lag.
- `utility_kafka_consumer_group_partition_lag{group,topic,partition}`: partition lag distribution.
- `utility_kafka_consumer_group_desired_replicas{group}`: latest policy output.
- `utility_kafka_consumer_group_scaling_decisions_total{group,reason}`: scale up, scale down, and stable decisions.
- `utility_kafka_consumer_group_lag_alerts_total{group,severity}`: warning and critical lag alerts.

Alert rules:

- Warning: total lag remains above the scale-up threshold for two sampling windows.
- Critical: total lag exceeds `critical_lag_threshold` or the oldest message age breaches the service SLO.
- Page: lag is critical and canary analysis blocks scale-up, because manual intervention may be required.

Dashboards should show total lag, max partition lag, consumer group members, desired replicas, actual replicas, rebalance counts, error rates, and P99 processing latency on one screen per domain service.

## Deployment strategy

1. Deploy the controller in blue/green mode with scaling writes disabled and compare computed desired replicas against current production behavior.
2. Enable write mode for one low-risk consumer group as a canary.
3. Promote canary only if lag decreases, P99 processing latency remains below 100 ms, error rate does not regress, and rebalance time stays within the service budget.
4. Roll forward one domain at a time. Roll back by disabling scaling writes; monitoring remains read-only.

## Security review notes

- Use least-privilege Kafka credentials that can read offsets and group metadata only.
- Use deployment credentials scoped to consumer group replica targets, not cluster-admin access.
- Treat group names and topic names as controlled labels to prevent metric-cardinality attacks.
- Audit every scale action with actor, group, previous replicas, desired replicas, reason, and canary result.


<!-- SOURCE: docs/service-mesh-mtls.md -->
# Service Mesh Mutual TLS Architecture

## Goals

The service mesh integration standardizes mutual TLS (mTLS) for every utility-backend service-to-service hop. The design keeps the P99 latency budget for critical paths at or below 100 ms, requires SPIFFE identities, and exposes metrics for security review, canary analysis, and day-two operations.

## Architecture

1. A mesh sidecar or ambient proxy terminates inbound mTLS with certificates mounted at `/etc/utility/mesh`.
2. The application validates `ServiceMeshMtlsConfig` during startup so mTLS cannot be enabled without a service certificate, private key, and trusted CA bundle.
3. Workload identity uses SPIFFE IDs in the format `spiffe://<trust-domain>/ns/<namespace>/sa/<service-account>`.
4. Critical paths use the configured `critical_path_budget_ms` guardrail. Values above 100 ms are rejected.
5. Prometheus exports handshake counters and latency histograms:
   - `utility_mesh_mtls_handshakes_total{service,result}`
   - `utility_mesh_mtls_handshake_latency_seconds{service}`

## Blue-Green and Canary Rollout

1. Deploy the green environment with mTLS enabled and traffic weight at 0%.
2. Confirm certificate issuance, SPIFFE identity format, and CA trust bundle freshness.
3. Shift 1% of service-to-service traffic to green for at least one full SLO window.
4. Promote only when success rate is at least 99.99% and P99 mTLS handshake latency is at or below 100 ms.
5. Increase traffic to 10%, 25%, 50%, and 100% while rechecking the same gates.
6. Roll back to blue immediately if canary gates fail or security alerts fire.

## Security Review Checklist

- Certificates are rotated by the mesh control plane before expiry.
- Private keys are mounted read-only and never logged.
- Peer authorization policies allow only expected service accounts.
- Plaintext service ports are disabled outside local health checks.
- Dashboard alerts page on repeated handshake failures and latency budget violations.


<!-- SOURCE: docs/structured_logging.md -->
# Solution Architecture: Structured Logging with OpenTelemetry Semantic Conventions

This document defines the structured logging and trace context integration architecture for the `utility-backend` services.

## Overview

Structured Logging ensures that all application logs are emitted as single-line JSON records. These records are enriched with:
1. Standard OpenTelemetry (OTel) Semantic Conventions (e.g. `service.name`, `service.version`, `service.environment`).
2. Distributed Tracing information (`trace_id` and `span_id`), which maps active tracing spans back to individual log lines.
3. Spatial Baggage attributes propagated across service boundaries (e.g., `region`, `substation.id`, `grid.segment`).

## Target JSON Format

Each log entry is serialized as a JSON object containing:

```json
{
  "timestamp": "2023-10-27T10:15:30.123456789Z",
  "level": "INFO",
  "severity_number": 9,
  "service.name": "utility-backend",
  "service.version": "0.1.0",
  "service.environment": "production",
  "target": "utility_backend::api::middleware",
  "body": "rate limit exceeded",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "attributes": {
    "source": "127.0.0.1",
    "region": "north-east",
    "substation_id": "SUB-42",
    "grid_segment": "grid-a",
    "code.filepath": "src/api/middleware.rs",
    "code.lineno": 250
  }
}
```

## Field Mappings & Semantic Conventions

### Severity Level Mapping

The standard `tracing` levels are mapped to standard OpenTelemetry severity text and number conventions:

| Tracing Level | OTel Severity Text (`level`) | OTel Severity Number (`severity_number`) |
|---------------|-----------------------------|------------------------------------------|
| `TRACE`       | `TRACE`                     | 1                                        |
| `DEBUG`       | `DEBUG`                     | 5                                        |
| `INFO`        | `INFO`                      | 9                                        |
| `WARN`        | `WARN`                      | 13                                       |
| `ERROR`       | `ERROR`                     | 17                                       |

### Service Attributes

- **`service.name`**: Standard OTel resource attribute identifying the service. Defaults to `utility-backend`. Can be overridden with the `OTEL_SERVICE_NAME` environment variable.
- **`service.version`**: The version of the service compiled dynamically from `CARGO_PKG_VERSION`.
- **`service.environment`**: The environment of the service (e.g., `production`, `development`, `test`). Defaults to `production` and can be overridden with the `APP_ENV` environment variable.

### Trace/Span Context Mapping

- **`trace_id`**: 32-character hex-encoded string of the active OpenTelemetry TraceId.
- **`span_id`**: 16-character hex-encoded string of the active OpenTelemetry SpanId.

### Spatial Baggage & Custom Attributes

- Any baggage fields propagated in the OpenTelemetry context (e.g. `region`, `substation_id`, `grid_segment`) are dynamically extracted from the thread-local context and inserted under `attributes`.
- Custom fields attached to events (such as `source` from the rate limiter) are captured using a custom `tracing::field::Visit` visitor and placed under `attributes`.
- Source code locations (`code.filepath` and `code.lineno`) are automatically injected into `attributes`.


<!-- SOURCE: docs/webhook_delivery.md -->
# Webhook Delivery Service

## Architecture

Application services publish webhook events after their business transaction commits. The delivery worker reads those events from the outbox boundary, serializes the event payload, signs it with HMAC-SHA256, and sends it to each subscribed endpoint through the `WebhookTransport` abstraction.

The service keeps request critical paths fast by avoiding synchronous third-party webhook calls in user-facing API handlers. Delivery attempts are bounded by a retry policy so a failing downstream cannot monopolize worker capacity.

## Security

Every delivery includes an `x-utility-webhook-signature` header in the form `t=<unix timestamp>,v1=<hex hmac>`. Consumers should reject signatures outside the five-minute tolerance and compare the expected HMAC in constant time. Endpoint secrets must be rotated by accepting old and new secrets during the consumer migration window.

## Retry Policy

The default policy attempts delivery five times. HTTP `408`, `429`, and `5xx` responses are treated as transient. Other non-2xx statuses are permanent failures and should move to the dead-letter workflow.

## Monitoring and Alerts

Prometheus exports:

- `utility_webhook_deliveries_total{endpoint_id,status}` for success and failed outcomes.
- `utility_webhook_retries_total{endpoint_id}` for retry pressure.
- `utility_webhook_delivery_latency_seconds` for end-to-end latency.

Alert on a falling success rate, sustained retry growth, or latency regression during canary rollout.

## Deployment Runbook

1. Deploy the new worker pool with blue-green routing disabled.
2. Enable a small canary slice and compare delivery success, retry rate, and latency against the current pool.
3. Promote the green pool only when canary metrics remain healthy.
4. If failures spike, disable the canary, pause noisy endpoints, and replay dead-lettered events after downstream recovery.


<!-- SOURCE: docs/dashboards/distributed-job-scheduler.md -->
# Dashboard: Distributed Job Scheduler

Recommended panels:

- Enqueued, claimed, completed, and failed jobs from `utility_job_scheduler_*_total`.
- Lease heartbeat rate from `utility_job_scheduler_heartbeats_total`.
- Claim critical-path latency P50/P95/P99 once the production store exports operation histograms.
- Queue depth by queue and age of oldest due job from the backing store.

Use queue and worker labels in the production store exporter to separate noisy queues from
system-wide availability indicators.


<!-- SOURCE: docs/monitoring/configuration-dashboard.md -->
# Configuration Dashboard

Track these Prometheus metrics on the operations dashboard:

- `utility_config_reload_success_total`: successful initial loads and reloads.
- `utility_config_reload_failure_total`: rejected reload attempts or file stat failures.
- `utility_config_schema_version`: active schema version across instances.

Suggested alert: page when reload failures increase during a rollout and warn when instances in the same deployment report different schema versions for more than 10 minutes.


<!-- SOURCE: docs/monitoring/slo-burn-rate.md -->
# Service Level Objective monitoring

## Architecture

The API layer records every HTTP response into a process-local SLO monitor and publishes the same observations as Prometheus metrics. The monitor evaluates two objectives system-wide:

- **Availability:** 99.99% of requests must avoid `5xx` responses.
- **Latency:** 99% of requests must complete within 100 ms to protect critical paths.

A multi-window burn-rate policy compares the fast five-minute window and slow one-hour window. Paging alerts require both windows to exceed their burn thresholds, which reduces noise from brief spikes while still detecting incidents that are actively exhausting the error budget.

## Prometheus metrics

- `utility_slo_requests_total{route,status_class}`: request volume used by SLO calculations.
- `utility_slo_request_latency_seconds{route}`: request latency histogram.
- `utility_slo_availability_burn_rate{window="fast|slow"}`: availability error-budget burn.
- `utility_slo_latency_burn_rate{window="fast|slow"}`: latency error-budget burn.
- `utility_slo_alert_active`: `1` when the in-process multi-window policy is firing.

## Alert rules

```yaml
groups:
  - name: utility-backend-slo
    rules:
      - alert: UtilityBackendSLOBurnPage
        expr: utility_slo_alert_active == 1
        for: 2m
        labels:
          severity: page
        annotations:
          summary: Utility backend is rapidly burning SLO error budget
          description: Investigate 5xx responses and latency above 100 ms before continuing rollout.
      - alert: UtilityBackendLatencyBudgetTicket
        expr: utility_slo_latency_burn_rate{window="slow"} >= 6
        for: 15m
        labels:
          severity: ticket
        annotations:
          summary: Utility backend latency SLO is burning too quickly
```

## Dashboard panels

1. Availability by route: `sum(rate(utility_slo_requests_total{status_class!="5xx"}[5m])) / sum(rate(utility_slo_requests_total[5m]))`.
2. P99 latency by route: `histogram_quantile(0.99, sum by (le, route) (rate(utility_slo_request_latency_seconds_bucket[5m])))`.
3. Burn rates: plot both `utility_slo_availability_burn_rate` and `utility_slo_latency_burn_rate` by window.
4. Alert state: single-stat panel for `utility_slo_alert_active`.

## Runbook

1. Confirm `/api/v1/slo/status` and `/metrics` are reachable from the active environment.
2. Split errors by route with `utility_slo_requests_total{status_class="5xx"}` and inspect recent deployment changes.
3. Check P99 latency against the 100 ms target and correlate with database pool, Soroban RPC, compaction, and TCP connection metrics.
4. During blue-green or canary deployment, halt promotion if page-level burn is active for the canary or if canary P99 is worse than baseline by more than 10%.
5. Roll back the canary/green environment if the burn continues after traffic is reduced.


<!-- SOURCE: docs/runbooks/RUN-001-COMPRESSION-LAG.md -->
# Runbook RUN-001: TimescaleDB Compression Lag

## 1. Description
This alert fires when the delay (lag) between a chunk's data boundaries and its actual compression exceeds the threshold configured in the dynamic compression policy (default: `max_compression_lag_days = 2`).

---

## 2. Automated Action / Mitigation
The system automatically executes the following mitigation:
1. **Shorten Compression Window**: The `IncidentManager` triggers `Auto-Mitigate Database Lag` runbook.
2. It calls the `AdjustCompressionPolicy` action to shorten `compress_after_days` to `1` day.
3. This forces TimescaleDB to aggressively compress older chunks and reclaim disk space/improve query performance.

---

## 3. Manual Diagnosis & Mitigation Steps
If the automated mitigation is insufficient or the alert persists, follow these steps:

### 3.1 Check Current Compression Status
Query the database compression status endpoint:
```bash
curl -s http://localhost:8443/api/v1/database/compression/status | jq
```
Or query directly in TimescaleDB:
```sql
SELECT * FROM timescaledb_information.chunks
WHERE hypertable_name = 'meter_readings' AND is_compressed = false;
```

### 3.2 Manually Compress Stale Chunks
Identify the uncompressed chunks and trigger compression manually in pgAdmin or psql:
```sql
SELECT compress_chunk(c.chunk_schema || '.' || c.chunk_name)
FROM timescaledb_information.chunks c
WHERE c.hypertable_name = 'meter_readings' AND c.is_compressed = false;
```

### 3.3 Verify Resource Utilization
- Check TimescaleDB disk capacity: `df -h`
- Check CPU/IO bottleneck status.

---

## 4. Verification & Resolution
Once the lag drops below the threshold, resolve the incident manually via curl or the dashboard:
```bash
curl -X POST http://localhost:8443/api/v1/incidents/database-lag-incident-id/resolve
```
This will automatically update PagerDuty to resolve the incident.


<!-- SOURCE: docs/runbooks/RUN-002-BLOCKCHAIN-SETTLE-FAILURE.md -->
# Runbook RUN-002: Blockchain Settlement Failure

## 1. Description
This alert fires when a batch settlement transaction on the Stellar/Soroban smart contract fails during submission or finalization (e.g. out of gas, bad sequence, or network partition).

---

## 2. Automated Action / Mitigation
The system automatically executes the following mitigation:
1. **Trigger Budget Optimizer**: Adjusts tx fee and instruction limits by calling the preflight optimizer.
2. **Circuit Breaker Activation**: If consecutive failures exceed `5`, the circuit breaker trips, pausing settlement calls to prevent burning gas on invalid contracts or double-spending.
3. **Queue Fallback**: Unsettled records are pushed back into the durable queue for safe replay.

---

## 3. Manual Diagnosis & Mitigation Steps
If the circuit breaker has tripped or manual replay is required:

### 3.1 Check Circuit Breaker Status
```bash
curl -s http://localhost:8443/debug/clock_state | jq
```

### 3.2 View Failing Settlement Records
Query the uncommitted settlement state from PostgreSQL:
```sql
SELECT * FROM settlement_queue WHERE status = 'failed' LIMIT 50;
```

### 3.3 Test Soroban RPC Endpoint Connectivity
Ensure that the Stellar Soroban RPC is responding and the contract is loaded:
```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getNetwork"}' \
  $SOROBAN_RPC_URL
```

### 3.4 Force Re-Submission
Manually trigger settlement processing once the RPC or budget is restored:
```bash
curl -X POST http://localhost:8443/api/v1/settle \
  -H "Content-Type: application/json" \
  -d '{"meter_id": "MTR-001", "resource_units": 150.0, "destination_wallet": "GA..."}'
```

---

## 4. Verification & Resolution
Confirm transactions are confirmed on-chain. Resolve the incident on the manager:
```bash
curl -X POST http://localhost:8443/api/v1/incidents/settle-failure-id/resolve
```
This will automatically update PagerDuty to resolve the incident.


<!-- SOURCE: docs/runbooks/RUN-003-GATEWAY-LOCK-CONTENTION.md -->
# Runbook RUN-003: Gateway Advisory Lock Contention

## 1. Description
This alert fires when there is extreme lock contention in the Gateway layer (e.g. meter key-rotation and registration threads blocked waiting on the `GLOBAL_REGISTRY` mutex or Postgres advisory locks).

---

## 2. Automated Action / Mitigation
The system automatically executes the following mitigation:
1. **Emit Thread Traces / Diagnostics**: Captures locking state and outputs to the logs.
2. **Auto-Reclaim Inactive Connections**: Evicts stale TCP sockets to reduce contention and free file descriptors.

---

## 3. Manual Diagnosis & Mitigation Steps

### 3.1 List Active Locks
Query the endpoint to identify which locks are currently held and for how long:
```bash
curl -s http://localhost:8443/api/v1/gateway/locks | jq
```

### 3.2 Query Postgres Lock Table
Check for blocked database sessions:
```sql
SELECT pid, blocked_by, query, duration
FROM pg_stat_activity
WHERE state = 'active' AND waiting = true;
```

### 3.3 Force-Unlock Stale Locks
If a worker holding an advisory lock crashed without releasing it, kill the blocking Postgres backend process:
```sql
SELECT pg_terminate_backend(pid);
```

---

## 4. Verification & Resolution
Confirm that lock lists are clear and registration commands succeed. Resolve the incident:
```bash
curl -X POST http://localhost:8443/api/v1/incidents/lock-contention-id/resolve
```
This will automatically update PagerDuty to resolve the incident.


<!-- SOURCE: docs/runbooks/configuration-hot-reload.md -->
# Runbook: Configuration Hot Reload

## Alerts

- `utility_config_reload_failure_total` increases for 5 minutes.
- Active `utility_config_schema_version` does not match the expected release artifact.

## Triage

1. Inspect the deployment event that changed the configuration file.
2. Validate the JSON syntax and confirm only supported schema fields are present.
3. Confirm bounded values such as `database.max_connections` and `reload.poll_interval_ms` satisfy the documented limits.
4. Restore the previous known-good file if reload failures continue.

## Recovery

Because reload is atomic, services continue using the previous validated snapshot. After fixing the file, wait one poll interval or restart the process to force an initial load.


<!-- SOURCE: docs/runbooks/database-migration-rollback.md -->
# Database Migration Rollback Runbook

## When to Roll Back

Roll back when a migration causes correctness issues, elevated error rates,
security exposure, or sustained P99 latency above the 100ms critical-path target.

## Procedure

1. Freeze additional deployments and confirm the active database version from
   `schema_migrations`.
2. Identify the previous stable version approved by the incident commander.
3. Drain or pause write-heavy workers where the migration affects write schemas.
4. Execute the service migration entrypoint with the target previous version so
   rollback SQL runs in reverse version order.
5. Verify that `schema_migrations` no longer contains rolled-back versions.
6. Watch `utility_migration_steps_total{direction="down"}` and
   `utility_migration_step_duration_seconds{direction="down"}` for completion.
7. Resume traffic gradually using the standard blue-green/canary process.
8. Document follow-up remediation and open a security review if data exposure was
   suspected.

## Validation Checklist

- Database schema matches the target application release.
- Health checks pass in the green environment before traffic shift.
- Error rate, saturation, and critical-path P99 latency are back within SLO.
- Dashboards show no new failed migration attempts.


<!-- SOURCE: docs/runbooks/dependency-vulnerability-scanning.md -->
# Runbook: Dependency Vulnerability Scanning

## Triage

1. Open the failed `dependency-security` workflow run or GitHub security alert.
2. Identify the affected package, installed version, patched version, severity, and whether the dependency is direct or transitive.
3. Check whether the affected crate is used on production request paths, background jobs, tests, or build tooling.
4. Assign a security reviewer for moderate or higher severity findings.

## Remediation

1. Prefer upgrading the direct dependency with `cargo update -p <crate>` or by editing `Cargo.toml`.
2. For transitive findings, update the parent dependency or add a compatible patched version if Cargo resolution allows it.
3. Run the local checks:
   - `cargo audit --deny warnings`
   - `cargo deny --all-features check advisories bans sources licenses`
   - `cargo test --all-features`
4. Document the fix, residual risk, and deployment plan in the pull request.

## Temporary exception process

Exceptions are allowed only when no patched version exists and the vulnerable code path is not exploitable in this service.

1. Add a narrowly scoped advisory ignore to `deny.toml`.
2. Include the advisory ID, affected crate, compensating control, owner, and removal date in the pull request.
3. Obtain security approval before merge.
4. Create a follow-up issue to remove the exception.

## Deployment verification

1. Deploy the patched artifact to green.
2. Run smoke tests and canary analysis.
3. Confirm no new `dependency-security`, CodeQL, or Dependabot alerts appear.
4. Promote traffic from blue to green.
5. Keep monitoring workflow and code scanning alerts for 24 hours after promotion.


<!-- SOURCE: docs/runbooks/deploy_blue_green.md -->
# Deployment and Canary Guide: Blue-Green Strategy

This guide describes how to deploy the **Incident Response and Automated Runbooks system** safely using a blue-green and canary progression strategy.

---

## 1. Release Strategy Overview

Deploying system-wide incident automation logic requires zero-downtime and failsafe rollback guarantees. We utilize a **Blue-Green Deployment** with a **10% Canary Phase** to evaluate performance, alert accuracy, and database stability under production load.

```
                  [ Load Balancer / Routing Layer ]
                             /         \
              (90% Traffic) /           \ (10% Canary Traffic)
                           v             v
                    [ Blue Group ]   [ Green Group (Canary) ]
                     Production       New Version
```

---

## 2. Canary Progression Stages

| Stage | Traffic Allocation | Duration | Success Criteria | Action on Failure |
| --- | --- | --- | --- | --- |
| **Stage 1 (Canary)** | 10% Canary, 90% Blue | 1 hour | Error Rate < 0.1%, Latency P99 < 100ms | Automated Rollback (60s) |
| **Stage 2** | 50% Canary, 50% Blue | 30 mins | Error Rate < 0.1%, Latency P99 < 100ms | Automated Rollback (60s) |
| **Stage 3 (Full)** | 100% Green, 0% Blue | Permanent | Stable system health | Manual Rollback |

---

## 3. Automated Canary Analysis & Rollback Criteria

The monitoring system continuously evaluates the following metrics from the canary (Green) instances. If any condition is violated during the analysis window, **traffic is rerouted 100% to Blue within 60 seconds**:

1. **System Errors**:
   - Metric: `sum(rate(http_requests_total{status=~"5.."}[1m]))`
   - Threshold: **> 0.1%** of all canary requests.
2. **P99 Critical Path Latency**:
   - Metric: `histogram_quantile(0.99, sum(rate(utility_runbook_execution_latency_seconds_bucket[5m])) by (le))`
   - Threshold: **> 100ms**.
3. **Unexpected Incident Spikes**:
   - Metric: `sum(rate(utility_incidents_triggered_total[1m]))`
   - Threshold: **> 15%** compared to the Blue production baseline.
4. **PagerDuty Delivery Failure Rate**:
   - Metric: `sum(rate(utility_pagerduty_api_requests_total{status="failed"}[5m])) / sum(rate(utility_pagerduty_api_requests_total[5m]))`
   - Threshold: **> 1.0%**.

---

## 4. DB Migrations Compatibility

To allow dual-running versions (Blue and Green accessing the same TimescaleDB), all schema updates are strictly backward-compatible:
1. **Never drop columns or rename tables** in the initial migration.
2. New tables (like incidents and runbooks) must be fully isolated or permit null/default values for older active nodes.
3. Once Green is promoted to 100% and Blue is retired, destructive cleanup migrations can be scheduled safely.


<!-- SOURCE: docs/runbooks/distributed-job-scheduler.md -->
# Runbook: Distributed Job Scheduler

## Alerts

- `JobSchedulerClaimLatencyHigh`: P99 claim latency exceeds 100 ms for 5 minutes.
- `JobSchedulerLeaseStealSpike`: reclaimed expired leases exceed baseline, indicating worker crashes
  or handler stalls.
- `JobSchedulerFailureSpike`: failed jobs increase faster than completed jobs for a queue.

## Blue-green and canary deployment

1. Deploy the green scheduler code with workers disabled.
2. Enable one canary worker per queue at 5% of normal concurrency.
3. Compare claim latency, failures, completed jobs, and expired-lease reclaims for 30 minutes.
4. Increase green concurrency to 50%, then 100%, while draining blue workers.
5. Roll back by disabling green workers; outstanding leases expire and blue can reclaim them.


<!-- SOURCE: docs/runbooks/e2ee-sensitive-fields.md -->
# Runbook: Sensitive Field E2EE

## Alerts

- **E2EEFailureSpike**: `utility_e2ee_field_operations_total{status="failure"}` increases above baseline.
- **E2EELatencyP99High**: `histogram_quantile(0.99, rate(utility_e2ee_field_latency_seconds_bucket[5m])) > 0.1`.

## Triage

1. Check recent deploys for key-id or sensitive-field configuration changes.
2. Compare encrypt and decrypt failure labels to identify whether failures start at ingress or at readers.
3. Sample structured logs for envelope error categories; do not log payload values.
4. If failures follow a key rotation, roll writers back to the previous primary key while keeping dual-read keys enabled.

## Blue-green key rotation

1. Deploy green readers with both old and new decryption keys.
2. Canary green writers with the new primary key for a small traffic slice.
3. Watch failure counters and P99 latency for at least one canary window.
4. Promote green writers after canary analysis passes.
5. Retire old keys only after all old envelopes are rewrapped or expire by retention policy.


<!-- SOURCE: docs/runbooks/pre-commit-hooks.md -->
# Pre-Commit Hook Suite Runbook

## Architecture

The repository uses [`pre-commit`](https://pre-commit.com/) as a local quality gate before code reaches CI. The suite is defined in `.pre-commit-config.yaml` and layers checks in increasing cost:

1. **File hygiene**: whitespace, line endings, YAML/TOML parsing, merge-conflict markers, case conflicts, and oversized files.
2. **Text quality**: `typos` catches spelling mistakes in source, configuration, and documentation.
3. **Rust correctness**: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test --all-features --lib --bins` match the fast parts of CI.
4. **Security guardrail**: `scripts/pre-commit-security.sh` scans tracked content for high-risk credential patterns before a commit is created.

The hooks are intentionally deterministic and run without service dependencies. Database-backed integration tests remain in CI because they require a provisioned PostgreSQL/TimescaleDB service.

## Installation

```bash
python -m pip install pre-commit
pre-commit install --install-hooks
```

Run the full suite manually before opening a pull request:

```bash
pre-commit run --all-files
```

## Operational targets

- Keep file hygiene and security scans lightweight so routine commits remain responsive.
- Treat `cargo fmt` and `cargo clippy` failures as blocking; CI enforces the same standards.
- Run full database integration tests in CI and before releases with `cargo test --all-features` and a configured `DATABASE_URL`.

## Monitoring and alerting

Pre-commit runs locally and does not emit production telemetry. CI is the authoritative monitoring surface for team-wide compliance:

- Alert on repeated `backend-ci` failures for `Rustfmt check`, `Clippy lint`, or unit/integration tests.
- Review hook adoption during security reviews by checking whether pull requests contain generated formatting-only corrections.
- Update this runbook when adding or removing hooks so incident responders can reproduce failed quality gates.

## Deployment and rollout

1. Land the configuration and script in a pull request.
2. Announce the installation commands to contributors.
3. Keep CI as the enforcement backstop while adoption ramps up.
4. If a hook causes unexpected failures, temporarily bypass locally with `SKIP=<hook-id> git commit` and file a follow-up issue; do not bypass CI.

## Troubleshooting

| Symptom | Resolution |
| --- | --- |
| `pre-commit: command not found` | Install with `python -m pip install pre-commit`. |
| Rust hooks cannot find `cargo` | Install the stable Rust toolchain with `rustup toolchain install stable`. |
| Security scan flags a test fixture | Prefer shortening fake credentials. If a realistic fixture is required, document the exception in the security review. |
| Full integration tests fail locally | Start dependencies with `docker compose up -d` and set `DATABASE_URL`. |


<!-- SOURCE: docs/runbooks/runtime-config-drift.md -->
# Runtime Configuration Drift Runbook

## Alert

`utility_config_drift_events_total` increases for a production service.

## Triage

1. Identify the service, environment, and severity labels from the alert.
2. Compare the current snapshot checksum with the deployment baseline checksum.
3. Review the drift report for added, removed, or modified keys.
4. Treat critical drift as a potential secret or credential control-plane incident.

## Remediation

- For expected changes, update the deployment baseline and attach the change approval.
- For unexpected non-sensitive drift, roll the instance back into the blue pool and redeploy from the approved artifact.
- For sensitive drift, rotate impacted credentials, quarantine affected instances, and request security review before returning traffic.

## Canary Validation

During canary rollout, require zero critical drift and no unexplained warning drift before increasing traffic.


<!-- SOURCE: docs/runbooks/secret-rotation.md -->
# Secret Rotation Runbook

## Alerts

- Page on any sustained increase in `utility_secret_rotation_total{status="failure"}`.
- Page when `utility_secret_rotation_duration_ms` P99 exceeds 100 ms for 10 minutes.
- Warn when any production secret is within 24 hours of `rotate_after` and has not rotated.

## Manual Rotation

1. Confirm the target service is healthy.
2. Trigger the rotation worker for the secret name.
3. Watch `utility_secret_rotation_total` and target-specific authentication errors.
4. Confirm the active version increments in the secret store.
5. Keep the previous version available until the overlap period expires.

## Rollback

1. Disable the rotation worker.
2. Promote the previous known-good secret version.
3. Restart affected consumers or invalidate their secret cache.
4. Re-enable rotation only after root cause is documented.


<!-- SOURCE: docs/operations/service-mesh-mtls-runbook.md -->
# Service Mesh mTLS Runbook

## Alerts

### High mTLS Handshake Failure Rate

1. Check `utility_mesh_mtls_handshakes_total{result="failure"}` by peer service.
2. Verify the affected service has a valid SPIFFE identity and a non-expired workload certificate.
3. Confirm the mesh CA bundle matches the active trust domain.
4. Roll back the latest canary step if failures began after a traffic shift.

### mTLS P99 Latency Above 100 ms

1. Inspect `utility_mesh_mtls_handshake_latency_seconds` for the affected peer service.
2. Check mesh proxy CPU saturation, certificate revocation checks, and control-plane reachability.
3. Reduce canary traffic to the previous healthy weight.
4. Open a security review if latency is caused by certificate validation or policy fetch failures.

## Manual Canary Gate

Use the release dashboard to confirm:

- success rate >= 99.99%
- P99 handshake latency <= 100 ms
- zero critical security alerts
- green environment health checks passing

Only promote when all gates pass.


<!-- SOURCE: runbooks/database-pool-health.md -->
# Database Pool Health Probe - Runbook

## What is this?
This runbook explains what to do when you get alerts about the database connection pool.

## Alert Table

| Alert | What it means | What to do |
|-------|---------------|------------|
| DatabasePoolUnhealthy | Pool is completely broken | Check database is running, restart app |
| DatabasePoolDegraded | Pool is slow or having issues | Check query performance |
| DatabasePoolHighLatency | Queries are taking >100ms | Check database load, look for slow queries |
| DatabasePoolConnectionExhausted | Pool is 90% full | Increase pool size or scale database |

## Step-by-Step Recovery

### 1. Check Database is Running
```bash
docker ps | grep timescaledb


<!-- SOURCE: runbooks/kafka-consumer-lag.md -->
# Runbook: Kafka Consumer Lag

## Triage

1. Open the Kafka lag dashboard and identify groups with warning or critical alerts.
2. Check total lag, max partition lag, oldest message age, actual replicas, and desired replicas.
3. Confirm Kafka broker health and partition leadership before changing consumer replicas manually.
4. If lag is critical and autoscaling is blocked, inspect canary analysis output for latency, error-rate, or rebalance regressions.

## Manual mitigation

- If broker health is normal and processing latency is below 100 ms P99, increase replicas up to the partition count.
- If a single partition dominates lag, investigate poison messages, downstream throttling, and partition-key skew.
- If all partitions are lagging and P99 latency is high, prioritize downstream dependency recovery before adding replicas.

## Rollback

Disable scaling writes for the affected group. The lag controller should continue emitting read-only metrics and alerts while deployment ownership returns to the operator.

\ n #   A g e n t   S y s t e m   P r o m p t s \ n  
 #   C L A U D E . m d  
  
 T h i s   f i l e   p r o v i d e s   g u i d a n c e   t o   C l a u d e   C o d e   ( c l a u d e . a i / c o d e )   w h e n   w o r k i n g   w i t h   c o d e   i n   t h i s   r e p o s i t o r y .  
  
 # #   C o m m a n d s  
  
 ` ` ` b a s h  
 #   S t a r t   a l l   d e p e n d e n c i e s   ( T i m e s c a l e D B   +   S o r o b a n   R P C )  
 d o c k e r   c o m p o s e   u p   - d  
  
 #   R u n   a l l   t e s t s   ( r e q u i r e s   D A T A B A S E _ U R L   e n v   v a r   p o i n t i n g   a t   a   r u n n i n g   T i m e s c a l e D B )  
 c a r g o   t e s t   - - a l l - f e a t u r e s  
  
 #   L i n t   ( C I   e n f o r c e s   z e r o   w a r n i n g s )  
 c a r g o   c l i p p y   - - a l l - t a r g e t s   - -   - D   w a r n i n g s  
  
 #   R u n   a   s i n g l e   t e s t   b y   n a m e  
 c a r g o   t e s t   < t e s t _ n a m e >  
  
 #   R u n   t e s t s   i n   a   s p e c i f i c   m o d u l e  
 c a r g o   t e s t   - - t e s t   g a t e w a y _ t e s t s  
 c a r g o   t e s t   - - t e s t   t a r i f f s _ t e s t s  
  
 #   R u n   b e n c h m a r k s  
 c a r g o   b e n c h   - - b e n c h   p a r s e r _ b e n c h  
 ` ` `  
  
 T h e   s e r v i c e   l i s t e n s   o n   p o r t   * * 8 4 4 3 * *   ( ` 0 . 0 . 0 . 0 : 8 4 4 3 ` ) .   T h e   d e f a u l t   d a t a b a s e   U R L   u s e d   w h e n   ` D A T A B A S E _ U R L `   i s   u n s e t   i s   ` p o s t g r e s : / / u t i l i t y : u t i l i t y _ s e c r e t @ l o c a l h o s t : 5 4 3 2 / u t i l i t y _ t e s t ` .  
  
 # #   A r c h i t e c t u r e  
  
 T h e   b a c k e n d   i n g e s t s   t e l e m e t r y   f r o m   h a r d w a r e   u t i l i t y   m e t e r s ,   a p p l i e s   t a r i f f   p r i c i n g ,   a n d   s e t t l e s   c o n s u m p t i o n   o n   t h e   S t e l l a r   b l o c k c h a i n   v i a   S o r o b a n   s m a r t   c o n t r a c t s .  
  
 # # #   M o d u l e   m a p  
  
 |   M o d u l e   |   P u r p o s e   |  
 | - - - | - - - |  
 |   ` s r c / g a t e w a y / `   |   H a r d w a r e   m e t e r   i n t e r f a c e   l a y e r   |  
 |   ` s r c / t a r i f f s / `   |   D y n a m i c   p r i c i n g   e n g i n e   |  
 |   ` s r c / t i m e _ s e r i e s / `   |   T i m e s c a l e D B   i n g e s t i o n   a n d   a n o m a l y   d e t e c t i o n   |  
 |   ` s r c / s o r o b a n / `   |   S t e l l a r / S o r o b a n   b l o c k c h a i n   s e t t l e m e n t   |  
 |   ` s r c / a p i / `   |   R E S T   A P I   ( A x u m ,   p o r t   8 4 4 3 )   |  
  
 # # #   D a t a   f l o w  
  
 ` ` `  
 H a r d w a r e   m e t e r   ( m T L S )   �      p a r s e _ e n v e l o p e   ( g a t e w a y / p a r s e r . r s )  
     �      s i g n a t u r e   v e r i f y   ( g a t e w a y / c r y p t o . r s )     �      B a c k p r e s s u r e F i l t e r   ( g a t e w a y / s t r e a m . r s )  
     �      D i a g n o s t i c E n g i n e   ( t i m e _ s e r i e s / a n a l y t i c s . r s )  
     �      T a r i f f E n g i n e   ( t a r i f f s / e n g i n e . r s )  
     �      N o n c e S e q u e n c e r   ( s o r o b a n / s e q u e n c e r . r s )   �      S o r o b a n   R P C   ( s o r o b a n / r p c . r s )  
 ` ` `  
  
 # # #   K e y   d e s i g n   p o i n t s  
  
 * * ` g a t e w a y / c r y p t o . r s `   � �    M e t e r R e g i s t r y   +   B l o o m F i l t e r * *  
 T h e   ` M e t e r R e g i s t r y `   s t o r e s   ` M e t e r I d e n t i t y `   s t r u c t s   ( e d 2 5 5 1 9   p u b l i c   k e y s ) .   I t   u s e s   a   c u s t o m   ` B l o o m F i l t e r `   a s   a   c e r t i f i c a t e   r e v o c a t i o n   l i s t   ( C R L )   s i z e d   f o r   1   M   e n t r i e s   a t   1   %   F P R .   T P M   a t t e s t a t i o n   o n   e n r o l l m e n t   i s   o p t i o n a l   b u t   s u p p o r t e d .   A   ` l a z y _ s t a t i c `   ` G L O B A L _ R E G I S T R Y `   i s   t h e   l i v e   s i n g l e t o n ;   m u t a b l e   o p e r a t i o n s   r e q u i r e   a c q u i r i n g   i t s   ` M u t e x ` .  
  
 * * ` g a t e w a y / p a r s e r . r s `   � �    z e r o - c o p y   e n v e l o p e   p a r s i n g * *  
 ` C o m p r e s s e d E n v e l o p e < ' a > `   b o r r o w s   f r o m   t h e   i n p u t   s l i c e   � �    ` m e t e r _ i d :   & ' a   s t r ` ,   ` p a y l o a d :   & ' a   [ u 8 ] ` ,   ` c h e c k s u m :   [ u 8 ;   3 2 ] ` .   ` p a r s e _ e n v e l o p e `   m a k e s   * * z e r o   h e a p   a l l o c a t i o n s * * .   W i r e   f o r m a t :   ` [ u 1 6   B E   m e t e r _ i d _ l e n ] [ U T F - 8   m e t e r _ i d ] [ p a y l o a d ] [ 3 2 - b y t e   c h e c k s u m ] ` .   T h e   C r i t e r i o n   b e n c h m a r k   i n   ` b e n c h e s / p a r s e r _ b e n c h . r s `   a s s e r t s   t h i s   c o n t r a c t   w i t h   a   ` C o u n t i n g A l l o c a t o r `   g l o b a l   a l l o c a t o r .  
  
 * * ` s o r o b a n / s e q u e n c e r . r s `   � �    p e r - g r i d   n o n c e   s e q u e n c e r * *  
 ` N o n c e S e q u e n c e r `   i s s u e s   m o n o t o n i c a l l y   i n c r e a s i n g   n o n c e s   p e r   g r i d   I D   u s i n g   a t o m i c   C A S   a n d   a   b l o c k - r e s e r v a t i o n   s c h e m e   ( ` N O N C E _ B L O C K _ S I Z E   =   1 0 0 ` ) .   A   b a c k g r o u n d   r e a p e r   t a s k   e v i c t s   s t a l e   g r i d   s t a t e   a f t e r   1   h o u r   o f   i n a c t i v i t y .   ` c o m m i t _ n o n c e `   g u a r d s   a g a i n s t   d o u b l e - s p e n d s .   ` N o n c e S e q u e n c e r `   i s   w r a p p e d   i n   ` A r c `   a n d   i n j e c t e d   i n t o   A x u m   r o u t e r   s t a t e .  
  
 * * ` t i m e _ s e r i e s / a n a l y t i c s . r s `   � �    s t r e a m i n g   d i a g n o s t i c   e n g i n e * *  
 ` D i a g n o s t i c E n g i n e `   m a i n t a i n s   a   p e r - m e t e r   s l i d i n g   w i n d o w   ( d e f a u l t   3 0   d a y s )   o f   ` R e a d i n g ` s .   ` a n a l y z e ( ) `   r u n s   a n   S T L - l i k e   d e c o m p o s i t i o n   ( t r a i l i n g   m o v i n g - a v e r a g e   t r e n d   +   m e d i a n   m o n t h l y   s e a s o n a l   f a c t o r s ) ,   f i t s   a   2 - c o v a r i a t e   O L S   w e a t h e r   m o d e l ,   c o m p u t e s   a   d y n a m i c   p 9 5   a n o m a l y   t h r e s h o l d ,   a n d   c l a s s i f i e s   p r o b a b l e   c a u s e   ( L e a k   /   T h e f t   /   S e n s o r F a u l t   /   S e a s o n a l V a r i a t i o n ) .   A   ` l a z y _ s t a t i c `   ` G L O B A L _ E N G I N E `   i s   u s e d   b y   A P I   h a n d l e r s .   T h e   l e g a c y   ` a n a l y z e _ c o n s u m p t i o n `   f u n c t i o n   ( s t a t i c   t h r e s h o l d   b a s e l i n e )   i s   k e p t   f o r   b a c k w a r d   c o m p a t i b i l i t y .  
  
 * * ` s o r o b a n / p r e f l i g h t . r s `   � �    t r a n s a c t i o n   f e e   s i m u l a t i o n * *  
 ` r u n _ p r e f l i g h t `   c a l l s   S o r o b a n ' s   ` s i m u l a t e T r a n s a c t i o n `   R P C   u p   t o   ` m a x _ i t e r a t i o n s `   t i m e s ,   i t e r a t i v e l y   t i g h t e n i n g   t h e   i n s t r u c t i o n   l e e w a y .   R e s u l t s   a r e   c a c h e d   i n   a   ` l a z y _ s t a t i c `   L R U   c a c h e   k e y e d   o n   ` ( c o n t r a c t _ i d ,   s h a 2 5 6 ( o p e r a t i o n _ x d r ) ) ` .   ` b u d g e t _ o p t i m i z e r `   p r o v i d e s   a   b i n a r y - s e a r c h   h e l p e r   f o r   f e e   m i n i m i s a t i o n .  
  
 * * ` t i m e _ s e r i e s / p o o l . r s `   � �    m u l t i - t e n a n t   D B   p o o l s * *  
 ` M u l t i T e n a n t P o o l M a n a g e r `   h o l d s   o n e   ` d e a d p o o l - p o s t g r e s `   p o o l   p e r   t e n a n t .   T h e   ` g e t _ c o n n e c t i o n `   m e t h o d   r e c o r d s   a   s t a r v a t i o n   m e t r i c   o n   p o o l   e x h a u s t i o n .   C r e d e n t i a l s   a r e   c u r r e n t l y   h a r d c o d e d   ( ` u t i l i t y ` / ` u t i l i t y _ s e c r e t ` ) .  
  
 * * ` a p i / a l l o c _ t r a c k e r . r s `   � �    a l l o c a t i o n   t i m i n g   m i d d l e w a r e * *  
 ` T r a c k i n g A l l o c a t o r `   w r a p s   ` S y s t e m `   a n d   r e c o r d s   a l l o c a t i o n / d e a l l o c a t i o n   l a t e n c y   i n t o   t h e   ` G C _ P A U S E _ S E C O N D S `   P r o m e t h e u s   c o u n t e r .   I t   i s   n o t   y e t   w i r e d   u p   a s   ` # [ g l o b a l _ a l l o c a t o r ] `   � �    t h a t   a t t r i b u t e   l i v e s   i n   ` b e n c h e s / p a r s e r _ b e n c h . r s `   f o r   t h e   b e n c h m a r k   b i n a r y   o n l y .  
  
 # # #   A P I   r o u t e s  
  
 |   M e t h o d   |   P a t h   |   D e s c r i p t i o n   |  
 | - - - | - - - | - - - |  
 |   G E T   |   ` / h e a l t h `   |   L i v e n e s s   p r o b e   |  
 |   G E T   |   ` / r e a d y z `   |   R e a d i n e s s   p r o b e   |  
 |   G E T   |   ` / a p i / v 1 / m e t e r s `   |   L i s t   m e t e r s   |  
 |   G E T   |   ` / a p i / v 1 / m e t e r s / : i d `   |   G e t   m e t e r   |  
 |   P O S T   |   ` / a p i / v 1 / m e t e r s / r e g i s t e r `   |   R e g i s t e r   m e t e r   w i t h   o p t i o n a l   T P M   a t t e s t a t i o n   |  
 |   P O S T   |   ` / a p i / v 1 / m e t e r s / r o t a t e - k e y `   |   R o t a t e   m e t e r   s i g n i n g   k e y   |  
 |   G E T   |   ` / a p i / v 1 / t a r i f f s `   |   L i s t   t a r i f f   s c h e d u l e s   |  
 |   P O S T   |   ` / a p i / v 1 / r e a d i n g s `   |   S u b m i t   m e t e r   r e a d i n g   |  
 |   P O S T   |   ` / a p i / v 1 / s e t t l e `   |   T r i g g e r   b l o c k c h a i n   s e t t l e m e n t   |  
 |   G E T   |   ` / a p i / v 1 / t i m e - s e r i e s / d i a g n o s t i c s / : m e t e r _ i d `   |   R u n   d i a g n o s t i c   a n a l y s i s   |  
 |   P O S T   |   ` / a p i / v 1 / c a l i b r a t e / : m e t e r _ i d `   |   C a l i b r a t e   m e t e r   d r i f t   |  
 |   G E T   |   ` / a p i / v 1 / n o n c e / s t a t u s `   |   G r i d   n o n c e   h i g h - w a t e r   m a r k s   |  
 |   G E T   |   ` / m e t r i c s `   |   P r o m e t h e u s   m e t r i c s   |  
  
 # # #   F i x e d - p o i n t   a r i t h m e t i c  
  
 ` t a r i f f s / m a t h . r s `   u s e s   t h e   ` f i x e d `   c r a t e   ( ` I 6 4 F 6 4 ` )   f o r   c o m m o d i t y   u n i t   s c a l i n g   t o   a v o i d   f l o a t i n g - p o i n t   r o u n d i n g   i n   s e t t l e m e n t   c a l c u l a t i o n s .  
  
 # # #   E n v i r o n m e n t   v a r i a b l e s  
  
 |   V a r i a b l e   |   D e f a u l t   |   P u r p o s e   |  
 | - - - | - - - | - - - |  
 |   ` D A T A B A S E _ U R L `   |   ` p o s t g r e s : / / u t i l i t y : u t i l i t y _ s e c r e t @ l o c a l h o s t : 5 4 3 2 / u t i l i t y _ t e s t `   |   T i m e s c a l e D B   c o n n e c t i o n   |  
 |   ` S O R O B A N _ R P C _ U R L `   |   � �    |   S o r o b a n   J S O N - R P C   e n d p o i n t   |  
 |   ` R U S T _ L O G `   |   ` i n f o `   |   L o g   f i l t e r   ( ` t r a c i n g - s u b s c r i b e r ` )   |  
 