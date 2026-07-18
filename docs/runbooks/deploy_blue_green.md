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
