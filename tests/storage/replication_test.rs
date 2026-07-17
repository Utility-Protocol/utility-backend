use std::time::Duration;

use utility_backend::storage::replication::{
    meets_targets, plan_failover, DisasterRecoveryError, RegionConfig, RegionHealth,
    RegionSnapshot, DEFAULT_CRITICAL_RPO, DEFAULT_CRITICAL_RTO,
};

fn snapshot(config: RegionConfig, health: RegionHealth) -> RegionSnapshot {
    RegionSnapshot { config, health }
}

#[test]
fn failover_promotes_lowest_priority_healthy_standby() {
    let regions = vec![
        snapshot(
            RegionConfig::primary("us-east-1", "postgres://primary"),
            RegionHealth::unavailable(),
        ),
        snapshot(
            RegionConfig::standby("us-west-2", "postgres://west", 20),
            RegionHealth::healthy(50),
        ),
        snapshot(
            RegionConfig::standby("us-central-1", "postgres://central", 10),
            RegionHealth::healthy(40),
        ),
    ];

    let plan = plan_failover(&regions, Duration::from_millis(100)).unwrap();

    assert_eq!(plan.current_primary, "us-east-1");
    assert_eq!(plan.promote_region, "us-central-1");
    assert_eq!(plan.canary_percent, 5);
    assert!(plan.blue_green_cutover);
}

#[test]
fn failover_rejects_stale_standbys() {
    let regions = vec![
        snapshot(
            RegionConfig::primary("us-east-1", "postgres://primary"),
            RegionHealth::unavailable(),
        ),
        snapshot(
            RegionConfig::standby("us-west-2", "postgres://west", 1),
            RegionHealth::healthy(10_000),
        ),
    ];

    assert_eq!(
        plan_failover(&regions, DEFAULT_CRITICAL_RPO),
        Err(DisasterRecoveryError::NoPromotableStandby)
    );
}

#[test]
fn disaster_recovery_targets_capture_critical_path_bounds() {
    assert!(meets_targets(DEFAULT_CRITICAL_RPO, DEFAULT_CRITICAL_RTO));
    assert!(!meets_targets(
        DEFAULT_CRITICAL_RPO + Duration::from_millis(1),
        DEFAULT_CRITICAL_RTO
    ));
    assert!(!meets_targets(
        DEFAULT_CRITICAL_RPO,
        DEFAULT_CRITICAL_RTO + Duration::from_millis(1)
    ));
}
