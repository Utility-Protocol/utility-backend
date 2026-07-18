use std::time::Duration;

use utility_backend::kafka::{
    evaluate_scaling, AlertSeverity, ConsumerGroupLagSnapshot, ConsumerGroupScalingPolicy,
    PartitionLag, ScalingReason,
};

fn policy() -> ConsumerGroupScalingPolicy {
    ConsumerGroupScalingPolicy {
        min_replicas: 2,
        max_replicas: 10,
        lag_per_replica: 1_000,
        scale_up_threshold: 2_000,
        scale_down_threshold: 250,
        critical_lag_threshold: 8_000,
        cooldown: Duration::from_secs(60),
    }
}

fn snapshot(lags: &[u64]) -> ConsumerGroupLagSnapshot {
    ConsumerGroupLagSnapshot {
        group_id: "settlement-writer".to_string(),
        members: 2,
        partitions: lags
            .iter()
            .enumerate()
            .map(|(partition, lag)| PartitionLag {
                topic: "utility.events".to_string(),
                partition: partition as i32,
                current_offset: 10,
                high_watermark: 10 + *lag as i64,
            })
            .collect(),
    }
}

#[test]
fn computes_total_and_max_partition_lag() {
    let sample = snapshot(&[100, 5_000, 1_250]);

    assert_eq!(sample.total_lag(), 6_350);
    assert_eq!(sample.max_partition_lag(), 5_000);
}

#[test]
fn scales_up_and_caps_at_partition_count() {
    let decision = evaluate_scaling(&snapshot(&[2_500, 2_000, 2_000]), 2, &policy());

    assert_eq!(decision.reason, ScalingReason::ScaleUp);
    assert_eq!(decision.desired_replicas, 3);
    assert_eq!(decision.alert.unwrap().severity, AlertSeverity::Warning);
}

#[test]
fn scales_down_when_lag_is_below_threshold() {
    let decision = evaluate_scaling(&snapshot(&[10, 20, 30, 40]), 6, &policy());

    assert_eq!(decision.reason, ScalingReason::ScaleDown);
    assert_eq!(decision.desired_replicas, 2);
    assert!(decision.alert.is_none());
}

#[test]
fn emits_critical_alert_for_excessive_lag() {
    let decision = evaluate_scaling(&snapshot(&[4_500, 4_000]), 2, &policy());

    assert_eq!(decision.desired_replicas, 2);
    assert_eq!(decision.alert.unwrap().severity, AlertSeverity::Critical);
}

#[test]
fn saturates_negative_offsets_to_zero_lag() {
    let lag = PartitionLag {
        topic: "utility.events".to_string(),
        partition: 0,
        current_offset: 42,
        high_watermark: 10,
    };

    assert_eq!(lag.lag(), 0);
}
