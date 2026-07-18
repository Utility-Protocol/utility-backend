//! Kafka consumer lag monitoring and autoscaling policy evaluation.
//!
//! This module intentionally keeps broker/Kubernetes integrations behind simple
//! data structures so lag sampling stays off the ingestion critical path. A
//! background controller can feed [`ConsumerGroupLagSnapshot`] values from Kafka
//! Admin/ListOffsets APIs and apply the returned [`ScalingDecision`] through the
//! deployment platform.

use std::time::Duration;

/// Lag for a single topic partition assigned to a consumer group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionLag {
    pub topic: String,
    pub partition: i32,
    pub current_offset: i64,
    pub high_watermark: i64,
}

impl PartitionLag {
    pub fn lag(&self) -> u64 {
        self.high_watermark.saturating_sub(self.current_offset) as u64
    }
}

/// Aggregated lag sample for one consumer group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupLagSnapshot {
    pub group_id: String,
    pub members: u32,
    pub partitions: Vec<PartitionLag>,
}

impl ConsumerGroupLagSnapshot {
    pub fn total_lag(&self) -> u64 {
        self.partitions.iter().map(PartitionLag::lag).sum()
    }

    pub fn max_partition_lag(&self) -> u64 {
        self.partitions
            .iter()
            .map(PartitionLag::lag)
            .max()
            .unwrap_or(0)
    }

    pub fn partition_count(&self) -> u32 {
        self.partitions.len() as u32
    }
}

/// Autoscaling bounds and alert thresholds for a consumer group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupScalingPolicy {
    pub min_replicas: u32,
    pub max_replicas: u32,
    pub lag_per_replica: u64,
    pub scale_up_threshold: u64,
    pub scale_down_threshold: u64,
    pub critical_lag_threshold: u64,
    pub cooldown: Duration,
}

impl Default for ConsumerGroupScalingPolicy {
    fn default() -> Self {
        Self {
            min_replicas: 1,
            max_replicas: 32,
            lag_per_replica: 10_000,
            scale_up_threshold: 20_000,
            scale_down_threshold: 1_000,
            critical_lag_threshold: 100_000,
            cooldown: Duration::from_secs(120),
        }
    }
}

/// Result from evaluating a lag snapshot against an autoscaling policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalingDecision {
    pub desired_replicas: u32,
    pub reason: ScalingReason,
    pub alert: Option<LagAlert>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalingReason {
    ScaleUp,
    ScaleDown,
    Stable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LagAlert {
    pub severity: AlertSeverity,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertSeverity {
    Warning,
    Critical,
}

pub fn evaluate_scaling(
    snapshot: &ConsumerGroupLagSnapshot,
    current_replicas: u32,
    policy: &ConsumerGroupScalingPolicy,
) -> ScalingDecision {
    let total_lag = snapshot.total_lag();
    let max_replicas = policy.max_replicas.max(policy.min_replicas);
    let current = current_replicas.clamp(policy.min_replicas, max_replicas);
    let partitions = snapshot.partition_count().max(1);
    let replica_ceiling = max_replicas.min(partitions);

    let target_by_lag = if total_lag == 0 {
        policy.min_replicas
    } else {
        total_lag.div_ceil(policy.lag_per_replica.max(1)) as u32
    };
    let target = target_by_lag.clamp(
        policy.min_replicas,
        replica_ceiling.max(policy.min_replicas),
    );

    let (desired_replicas, reason) = if total_lag >= policy.scale_up_threshold && target > current {
        (target, ScalingReason::ScaleUp)
    } else if total_lag <= policy.scale_down_threshold && target < current {
        (target, ScalingReason::ScaleDown)
    } else {
        (current, ScalingReason::Stable)
    };

    let alert = if total_lag >= policy.critical_lag_threshold {
        Some(LagAlert {
            severity: AlertSeverity::Critical,
            message: format!(
                "consumer group {} lag {} exceeds critical threshold {}",
                snapshot.group_id, total_lag, policy.critical_lag_threshold
            ),
        })
    } else if total_lag >= policy.scale_up_threshold {
        Some(LagAlert {
            severity: AlertSeverity::Warning,
            message: format!(
                "consumer group {} lag {} requires additional capacity",
                snapshot.group_id, total_lag
            ),
        })
    } else {
        None
    };

    ScalingDecision {
        desired_replicas,
        reason,
        alert,
    }
}
