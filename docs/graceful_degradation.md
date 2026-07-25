# Graceful degradation with feature flags and capacity shedding

## Architecture

`ResilienceController` is a process-local admission controller that runs before rate limiting on API requests. It maps routes to typed feature flags, applies operator-configured kill switches, and sheds load by capacity tier before expensive handler work starts.

## Capacity policy

- **Normal**: all enabled features are admitted.
- **Degraded**: critical paths (`/api/v1/readings`, `/api/v1/settle`) continue while degradable diagnostics, trace lookup, compression status, and tariff explanation requests are shed.
- **Shed**: all protected feature traffic is rejected with `429 Too Many Requests` to preserve process health.

## Configuration

- `UTILITY_DISABLED_FEATURES`: comma-separated flags (`meter_reads`, `tariff_explain`, `settlement`, `diagnostics`, `compression_status`, `telemetry_trace`).
- `UTILITY_DEGRADED_MAX_IN_FLIGHT`: in-flight request threshold for degraded mode.
- `UTILITY_SHED_MAX_IN_FLIGHT`: in-flight request threshold for emergency shedding.

## Monitoring and alerting

Prometheus metrics:

- `utility_resilience_in_flight_requests`
- `utility_resilience_capacity_tier{tier="normal|degraded|shed"}`
- `utility_resilience_shed_requests_total{feature,tier}`

Alert when the degraded tier is active for more than five minutes or any shed-tier rejection occurs outside a planned load test.

## Deployment runbook

1. Ship the controller disabled-by-default via blue/green deployment.
2. Canary 5% of traffic with conservative thresholds above observed P99 concurrency.
3. Verify critical path P99 latency remains under 100ms and shed counters are zero.
4. Increase canary to 25%, 50%, then 100% while watching tier gauges and error budget burn.
5. Roll back by removing the new environment variables or routing traffic to the previous color.
