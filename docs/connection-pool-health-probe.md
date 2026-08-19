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
