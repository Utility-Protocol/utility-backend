# Kafka Consumer Lag Monitoring and Autoscaling

## Architecture

The lag controller runs as a system-wide background control loop outside the request and ingestion hot paths. It periodically samples Kafka committed offsets and topic high-watermarks, converts them into `ConsumerGroupLagSnapshot` values, evaluates each group with `evaluate_scaling`, and submits replica changes to the deployment layer only after cooldown and rollout gates pass.

```text
Kafka Admin/ListOffsets APIs
        │
        ▼
Lag sampler ──► Prometheus metrics ──► alerts + dashboards
        │
        ▼
Scaling policy evaluator ──► blue/green deployer ──► canary analysis ──► consumer group replicas
```

## Core policy

The policy computes total lag and maximum partition lag for each group. Desired replicas are derived from `lag_per_replica`, clamped by minimum replicas, maximum replicas, and the partition count to avoid over-provisioning idle consumers. Scale-up only happens at or above `scale_up_threshold`; scale-down only happens at or below `scale_down_threshold`, which creates hysteresis and protects the 99.99% availability target from oscillation.

## Monitoring and alerting

Recommended metrics:

- `utility_kafka_consumer_group_lag{group,topic}`: total group lag.
- `utility_kafka_consumer_group_partition_lag{group,topic,partition}`: partition lag distribution.
- `utility_kafka_consumer_group_desired_replicas{group}`: latest policy output.
- `utility_kafka_consumer_group_scaling_decisions_total{group,reason}`: scale up, scale down, and stable decisions.
- `utility_kafka_consumer_group_lag_alerts_total{group,severity}`: warning and critical lag alerts.

Alert rules:

- Warning: total lag remains above the scale-up threshold for two sampling windows.
- Critical: total lag exceeds `critical_lag_threshold` or the oldest message age breaches the service SLO.
- Page: lag is critical and canary analysis blocks scale-up, because manual intervention may be required.

Dashboards should show total lag, max partition lag, consumer group members, desired replicas, actual replicas, rebalance counts, error rates, and P99 processing latency on one screen per domain service.

## Deployment strategy

1. Deploy the controller in blue/green mode with scaling writes disabled and compare computed desired replicas against current production behavior.
2. Enable write mode for one low-risk consumer group as a canary.
3. Promote canary only if lag decreases, P99 processing latency remains below 100 ms, error rate does not regress, and rebalance time stays within the service budget.
4. Roll forward one domain at a time. Roll back by disabling scaling writes; monitoring remains read-only.

## Security review notes

- Use least-privilege Kafka credentials that can read offsets and group metadata only.
- Use deployment credentials scoped to consumer group replica targets, not cluster-admin access.
- Treat group names and topic names as controlled labels to prevent metric-cardinality attacks.
- Audit every scale action with actor, group, previous replicas, desired replicas, reason, and canary result.
