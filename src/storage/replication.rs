//! Multi-region replication and disaster-recovery planning primitives.
//!
//! This module intentionally keeps the failover decision path allocation-light
//! and deterministic so callers can evaluate region health inside critical paths
//! while keeping P99 latency below the service target.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Maximum acceptable replication lag for critical data paths.
pub const DEFAULT_CRITICAL_RPO: Duration = Duration::from_secs(5);
/// Maximum failover time for the blue/green promoted standby.
pub const DEFAULT_CRITICAL_RTO: Duration = Duration::from_secs(60);
/// Availability target for the multi-region service tier.
pub const AVAILABILITY_TARGET: f64 = 99.99;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionRole {
    Primary,
    Standby,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionConfig {
    pub name: String,
    pub role: RegionRole,
    pub priority: u8,
    pub replication_endpoint: String,
}

impl RegionConfig {
    pub fn primary(name: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            role: RegionRole::Primary,
            priority: 0,
            replication_endpoint: endpoint.into(),
        }
    }

    pub fn standby(name: impl Into<String>, endpoint: impl Into<String>, priority: u8) -> Self {
        Self {
            name: name.into(),
            role: RegionRole::Standby,
            priority,
            replication_endpoint: endpoint.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionHealth {
    pub reachable: bool,
    pub replication_lag_ms: u64,
    pub error_budget_burn_bps: u32,
}

impl RegionHealth {
    pub fn healthy(replication_lag_ms: u64) -> Self {
        Self {
            reachable: true,
            replication_lag_ms,
            error_budget_burn_bps: 0,
        }
    }

    pub fn unavailable() -> Self {
        Self {
            reachable: false,
            replication_lag_ms: u64::MAX,
            error_budget_burn_bps: u32::MAX,
        }
    }

    pub fn is_promotable(self, max_lag: Duration) -> bool {
        self.reachable && self.replication_lag_ms <= max_lag.as_millis() as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionSnapshot {
    pub config: RegionConfig,
    pub health: RegionHealth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverPlan {
    pub current_primary: String,
    pub promote_region: String,
    pub canary_percent: u8,
    pub blue_green_cutover: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DisasterRecoveryError {
    MissingPrimary,
    PrimaryHealthy,
    NoPromotableStandby,
}

/// Selects the lowest-priority healthy standby when the current primary is down.
pub fn plan_failover(
    regions: &[RegionSnapshot],
    max_replication_lag: Duration,
) -> Result<FailoverPlan, DisasterRecoveryError> {
    let primary = regions
        .iter()
        .find(|region| region.config.role == RegionRole::Primary)
        .ok_or(DisasterRecoveryError::MissingPrimary)?;

    if primary.health.is_promotable(max_replication_lag) {
        return Err(DisasterRecoveryError::PrimaryHealthy);
    }

    let standby = regions
        .iter()
        .filter(|region| region.config.role == RegionRole::Standby)
        .filter(|region| region.health.is_promotable(max_replication_lag))
        .min_by_key(|region| region.config.priority)
        .ok_or(DisasterRecoveryError::NoPromotableStandby)?;

    Ok(FailoverPlan {
        current_primary: primary.config.name.clone(),
        promote_region: standby.config.name.clone(),
        canary_percent: 5,
        blue_green_cutover: true,
    })
}

pub fn meets_targets(replication_lag: Duration, failover_time: Duration) -> bool {
    replication_lag <= DEFAULT_CRITICAL_RPO && failover_time <= DEFAULT_CRITICAL_RTO
}
