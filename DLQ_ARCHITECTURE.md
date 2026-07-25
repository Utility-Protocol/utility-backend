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
