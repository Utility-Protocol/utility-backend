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
