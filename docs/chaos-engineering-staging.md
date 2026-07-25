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
