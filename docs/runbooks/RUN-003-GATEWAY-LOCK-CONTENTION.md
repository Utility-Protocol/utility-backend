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
