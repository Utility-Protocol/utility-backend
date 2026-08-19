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
