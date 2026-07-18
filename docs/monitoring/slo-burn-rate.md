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
