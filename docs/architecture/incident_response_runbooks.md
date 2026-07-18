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
