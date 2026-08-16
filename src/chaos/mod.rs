//! Staging chaos engineering guardrails and experiment metadata.
//!
//! This module intentionally models the safety contract for chaos experiments
//! without executing faults in-process. Fault injection is orchestrated outside
//! the service by staging automation, while the backend exposes stable scenario
//! definitions that tests and runbooks can validate.

use std::time::Duration;

/// Availability budget required for staging chaos experiments.
pub const AVAILABILITY_TARGET_BASIS_POINTS: u32 = 9_999;

/// Maximum acceptable P99 latency for critical paths during experiments.
pub const CRITICAL_PATH_P99_TARGET: Duration = Duration::from_millis(100);

/// Canary bake time used before widening a chaos experiment blast radius.
pub const CANARY_BAKE_TIME: Duration = Duration::from_secs(15 * 60);

/// System area affected by a chaos scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChaosDomain {
    Gateway,
    Ingestion,
    Storage,
    Settlement,
    Blockchain,
    Api,
}

/// Fault class injected by the staging chaos runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    Latency,
    PacketLoss,
    DependencyUnavailable,
    ResourcePressure,
    ClockSkew,
}

/// Staging-only chaos experiment descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChaosScenario {
    pub name: &'static str,
    pub domain: ChaosDomain,
    pub fault: FaultKind,
    pub max_blast_radius_percent: u8,
    pub duration: Duration,
    pub rollback_signal: &'static str,
}

impl ChaosScenario {
    /// Returns true when the scenario respects staging safety guardrails.
    pub fn is_within_guardrails(&self) -> bool {
        self.max_blast_radius_percent > 0
            && self.max_blast_radius_percent <= 10
            && self.duration <= Duration::from_secs(30 * 60)
            && !self.rollback_signal.trim().is_empty()
    }
}

/// Baseline chaos scenarios covering all major backend service areas.
pub fn staging_scenarios() -> Vec<ChaosScenario> {
    vec![
        ChaosScenario {
            name: "gateway_tls_handshake_latency",
            domain: ChaosDomain::Gateway,
            fault: FaultKind::Latency,
            max_blast_radius_percent: 5,
            duration: Duration::from_secs(10 * 60),
            rollback_signal: "gateway_p99_latency_ms > 100",
        },
        ChaosScenario {
            name: "ingestion_packet_loss",
            domain: ChaosDomain::Ingestion,
            fault: FaultKind::PacketLoss,
            max_blast_radius_percent: 5,
            duration: Duration::from_secs(10 * 60),
            rollback_signal: "ingestion_drop_rate > 0.1%",
        },
        ChaosScenario {
            name: "timeseries_pool_exhaustion",
            domain: ChaosDomain::Storage,
            fault: FaultKind::ResourcePressure,
            max_blast_radius_percent: 5,
            duration: Duration::from_secs(15 * 60),
            rollback_signal: "db_pool_wait_p99_ms > 100",
        },
        ChaosScenario {
            name: "settlement_submitter_dependency_outage",
            domain: ChaosDomain::Settlement,
            fault: FaultKind::DependencyUnavailable,
            max_blast_radius_percent: 5,
            duration: Duration::from_secs(15 * 60),
            rollback_signal: "settlement_queue_lag_seconds > 60",
        },
        ChaosScenario {
            name: "soroban_rpc_partial_outage",
            domain: ChaosDomain::Blockchain,
            fault: FaultKind::DependencyUnavailable,
            max_blast_radius_percent: 5,
            duration: Duration::from_secs(15 * 60),
            rollback_signal: "soroban_submit_error_ratio > 1%",
        },
        ChaosScenario {
            name: "api_clock_skew",
            domain: ChaosDomain::Api,
            fault: FaultKind::ClockSkew,
            max_blast_radius_percent: 5,
            duration: Duration::from_secs(10 * 60),
            rollback_signal: "api_auth_rejection_ratio > 0.5%",
        },
    ]
}
