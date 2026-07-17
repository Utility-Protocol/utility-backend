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
