use crate::api::metrics;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureFlag {
    MeterReads,
    TariffExplain,
    Settlement,
    Diagnostics,
    CompressionStatus,
    TelemetryTrace,
}

impl FeatureFlag {
    pub fn from_path(path: &str) -> Option<Self> {
        match path {
            "/api/v1/readings" => Some(Self::MeterReads),
            "/api/v1/tariffs/explain" => Some(Self::TariffExplain),
            "/api/v1/settle" => Some(Self::Settlement),
            p if p.starts_with("/api/v1/time-series/diagnostics/") => Some(Self::Diagnostics),
            "/api/v1/database/compression/status" => Some(Self::CompressionStatus),
            p if p.starts_with("/api/v1/telemetry/trace/") => Some(Self::TelemetryTrace),
            _ => None,
        }
    }

    pub fn criticality(self) -> Criticality {
        match self {
            Self::MeterReads | Self::Settlement => Criticality::Critical,
            Self::TariffExplain
            | Self::Diagnostics
            | Self::CompressionStatus
            | Self::TelemetryTrace => Criticality::Degradable,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Criticality {
    Critical,
    Degradable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacityTier {
    Normal,
    Degraded,
    Shed,
}

#[derive(Debug, Clone)]
pub struct ResilienceConfig {
    pub disabled_flags: HashSet<FeatureFlag>,
    pub degraded_max_in_flight: u64,
    pub shed_max_in_flight: u64,
    pub recovery_window: Duration,
}

impl Default for ResilienceConfig {
    fn default() -> Self {
        Self {
            disabled_flags: HashSet::new(),
            degraded_max_in_flight: 2_000,
            shed_max_in_flight: 5_000,
            recovery_window: Duration::from_secs(30),
        }
    }
}

impl ResilienceConfig {
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Ok(raw) = std::env::var("UTILITY_DISABLED_FEATURES") {
            cfg.disabled_flags = raw
                .split(',')
                .filter_map(|s| parse_flag(s.trim()))
                .collect();
        }
        if let Ok(v) = std::env::var("UTILITY_DEGRADED_MAX_IN_FLIGHT") {
            if let Ok(parsed) = v.parse() {
                cfg.degraded_max_in_flight = parsed;
            }
        }
        if let Ok(v) = std::env::var("UTILITY_SHED_MAX_IN_FLIGHT") {
            if let Ok(parsed) = v.parse() {
                cfg.shed_max_in_flight = parsed;
            }
        }
        cfg
    }
}

fn parse_flag(s: &str) -> Option<FeatureFlag> {
    match s {
        "meter_reads" => Some(FeatureFlag::MeterReads),
        "tariff_explain" => Some(FeatureFlag::TariffExplain),
        "settlement" => Some(FeatureFlag::Settlement),
        "diagnostics" => Some(FeatureFlag::Diagnostics),
        "compression_status" => Some(FeatureFlag::CompressionStatus),
        "telemetry_trace" => Some(FeatureFlag::TelemetryTrace),
        _ => None,
    }
}

#[derive(Debug)]
pub struct ResilienceController {
    config: RwLock<ResilienceConfig>,
    in_flight: AtomicU64,
    last_shed: RwLock<Option<Instant>>,
}

impl ResilienceController {
    pub fn new(config: ResilienceConfig) -> Self {
        Self {
            config: RwLock::new(config),
            in_flight: AtomicU64::new(0),
            last_shed: RwLock::new(None),
        }
    }
    pub fn current_in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }
    pub fn update_config(&self, config: ResilienceConfig) {
        *self.config.write() = config;
    }
    pub fn snapshot(&self) -> ResilienceSnapshot {
        let cfg = self.config.read();
        let in_flight = self.current_in_flight();
        ResilienceSnapshot {
            tier: self.tier_for(in_flight, &cfg),
            in_flight,
            disabled_features: cfg.disabled_flags.iter().copied().collect(),
        }
    }
    fn tier_for(&self, in_flight: u64, cfg: &ResilienceConfig) -> CapacityTier {
        if in_flight >= cfg.shed_max_in_flight {
            CapacityTier::Shed
        } else if in_flight >= cfg.degraded_max_in_flight {
            CapacityTier::Degraded
        } else {
            CapacityTier::Normal
        }
    }
    pub fn admit(&self, flag: Option<FeatureFlag>) -> AdmissionGuard<'_> {
        let cfg = self.config.read().clone();
        if let Some(f) = flag {
            if cfg.disabled_flags.contains(&f) {
                return AdmissionGuard::rejected(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "feature disabled by flag",
                );
            }
        }
        let next = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        let tier = self.tier_for(next, &cfg);
        let reject = matches!(tier, CapacityTier::Shed)
            || (matches!(tier, CapacityTier::Degraded)
                && flag.map(|f| f.criticality()) == Some(Criticality::Degradable));
        metrics::set_resilience_in_flight(next as f64);
        metrics::set_resilience_capacity_tier(tier);
        if reject {
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
            *self.last_shed.write() = Some(Instant::now());
            metrics::inc_resilience_shed(flag, tier);
            AdmissionGuard::rejected(
                StatusCode::TOO_MANY_REQUESTS,
                "request shed to protect critical capacity",
            )
        } else {
            AdmissionGuard {
                controller: Some(self),
                status: StatusCode::OK,
                reason: None,
            }
        }
    }
}

pub struct AdmissionGuard<'a> {
    controller: Option<&'a ResilienceController>,
    status: StatusCode,
    reason: Option<&'static str>,
}
impl<'a> AdmissionGuard<'a> {
    fn rejected(status: StatusCode, reason: &'static str) -> Self {
        Self {
            controller: None,
            status,
            reason: Some(reason),
        }
    }
    pub fn is_admitted(&self) -> bool {
        self.reason.is_none()
    }
    pub fn rejection_response(&self) -> Option<Response> {
        self.reason.map(|r| (self.status, r).into_response())
    }
}
impl Drop for AdmissionGuard<'_> {
    fn drop(&mut self) {
        if let Some(c) = self.controller {
            let left = c.in_flight.fetch_sub(1, Ordering::AcqRel).saturating_sub(1);
            metrics::set_resilience_in_flight(left as f64);
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ResilienceSnapshot {
    pub tier: CapacityTier,
    pub in_flight: u64,
    pub disabled_features: Vec<FeatureFlag>,
}

pub async fn resilience_layer(req: Request<Body>, next: Next) -> Response {
    lazy_static::lazy_static! {
        static ref CONTROLLER: ResilienceController = ResilienceController::new(ResilienceConfig::from_env());
    }
    let flag = FeatureFlag::from_path(req.uri().path());
    let guard = CONTROLLER.admit(flag);
    if !guard.is_admitted() {
        return guard.rejection_response().unwrap();
    }
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn disabled_feature_is_rejected() {
        let mut cfg = ResilienceConfig::default();
        cfg.disabled_flags.insert(FeatureFlag::Settlement);
        let c = ResilienceController::new(cfg);
        let g = c.admit(Some(FeatureFlag::Settlement));
        assert!(!g.is_admitted());
        assert_eq!(c.current_in_flight(), 0);
    }
    #[test]
    fn degraded_sheds_non_critical_but_admits_critical() {
        let cfg = ResilienceConfig {
            degraded_max_in_flight: 1,
            shed_max_in_flight: 3,
            ..Default::default()
        };
        let c = ResilienceController::new(cfg);
        let _first = c.admit(Some(FeatureFlag::MeterReads));
        assert!(!c.admit(Some(FeatureFlag::Diagnostics)).is_admitted());
        assert!(c.admit(Some(FeatureFlag::Settlement)).is_admitted());
    }
}
