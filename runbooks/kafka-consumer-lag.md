# Runbook: Kafka Consumer Lag

## Triage

1. Open the Kafka lag dashboard and identify groups with warning or critical alerts.
2. Check total lag, max partition lag, oldest message age, actual replicas, and desired replicas.
3. Confirm Kafka broker health and partition leadership before changing consumer replicas manually.
4. If lag is critical and autoscaling is blocked, inspect canary analysis output for latency, error-rate, or rebalance regressions.

## Manual mitigation

- If broker health is normal and processing latency is below 100 ms P99, increase replicas up to the partition count.
- If a single partition dominates lag, investigate poison messages, downstream throttling, and partition-key skew.
- If all partitions are lagging and P99 latency is high, prioritize downstream dependency recovery before adding replicas.

## Rollback

Disable scaling writes for the affected group. The lag controller should continue emitting read-only metrics and alerts while deployment ownership returns to the operator.
