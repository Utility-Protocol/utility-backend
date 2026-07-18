use crate::api::metrics;
use std::time::Duration;
use thiserror::Error;

const DEFAULT_CRITICAL_PATH_BUDGET_MS: u64 = 100;
const DEFAULT_CANARY_MIN_SUCCESS_RATE: f64 = 0.9999;
const DEFAULT_CANARY_MAX_P99_MS: u64 = 100;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceMeshMtlsConfig {
    pub enabled: bool,
    pub service_name: String,
    pub trust_domain: String,
    pub cert_path: String,
    pub key_path: String,
    pub ca_cert_path: String,
    pub require_spiffe_id: bool,
    pub critical_path_budget: Duration,
    pub canary: CanaryPolicy,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CanaryPolicy {
    pub min_success_rate: f64,
    pub max_p99_latency: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeshIdentity {
    pub trust_domain: String,
    pub namespace: String,
    pub service_account: String,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ServiceMeshError {
    #[error("service mesh service_name must not be empty")]
    EmptyServiceName,
    #[error("service mesh trust_domain must not be empty")]
    EmptyTrustDomain,
    #[error("service mesh certificate, key, and CA paths are required when mTLS is enabled")]
    MissingCertificateMaterial,
    #[error("service mesh latency budget must be between 1ms and 100ms")]
    InvalidLatencyBudget,
    #[error("canary success rate must be in the inclusive range [0.0, 1.0]")]
    InvalidCanarySuccessRate,
    #[error("canary P99 latency threshold must be between 1ms and 100ms")]
    InvalidCanaryLatency,
}

impl Default for ServiceMeshMtlsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            service_name: "utility-backend".to_string(),
            trust_domain: "utility.local".to_string(),
            cert_path: "/etc/utility/mesh/tls.crt".to_string(),
            key_path: "/etc/utility/mesh/tls.key".to_string(),
            ca_cert_path: "/etc/utility/mesh/ca.crt".to_string(),
            require_spiffe_id: true,
            critical_path_budget: Duration::from_millis(DEFAULT_CRITICAL_PATH_BUDGET_MS),
            canary: CanaryPolicy::default(),
        }
    }
}

impl Default for CanaryPolicy {
    fn default() -> Self {
        Self {
            min_success_rate: DEFAULT_CANARY_MIN_SUCCESS_RATE,
            max_p99_latency: Duration::from_millis(DEFAULT_CANARY_MAX_P99_MS),
        }
    }
}

impl ServiceMeshMtlsConfig {
    pub fn validate(&self) -> Result<(), ServiceMeshError> {
        if self.service_name.trim().is_empty() {
            return Err(ServiceMeshError::EmptyServiceName);
        }
        if self.trust_domain.trim().is_empty() {
            return Err(ServiceMeshError::EmptyTrustDomain);
        }
        if self.enabled
            && (self.cert_path.trim().is_empty()
                || self.key_path.trim().is_empty()
                || self.ca_cert_path.trim().is_empty())
        {
            return Err(ServiceMeshError::MissingCertificateMaterial);
        }
        if self.critical_path_budget.is_zero()
            || self.critical_path_budget > Duration::from_millis(DEFAULT_CRITICAL_PATH_BUDGET_MS)
        {
            return Err(ServiceMeshError::InvalidLatencyBudget);
        }
        if !(0.0..=1.0).contains(&self.canary.min_success_rate) {
            return Err(ServiceMeshError::InvalidCanarySuccessRate);
        }
        if self.canary.max_p99_latency.is_zero()
            || self.canary.max_p99_latency > Duration::from_millis(DEFAULT_CANARY_MAX_P99_MS)
        {
            return Err(ServiceMeshError::InvalidCanaryLatency);
        }
        Ok(())
    }

    pub fn identity_for_namespace(&self, namespace: impl Into<String>) -> MeshIdentity {
        MeshIdentity {
            trust_domain: self.trust_domain.clone(),
            namespace: namespace.into(),
            service_account: self.service_name.clone(),
        }
    }
}

impl MeshIdentity {
    pub fn spiffe_id(&self) -> String {
        format!(
            "spiffe://{}/ns/{}/sa/{}",
            self.trust_domain, self.namespace, self.service_account
        )
    }
}

pub fn record_handshake_result(service: &str, result: &str, latency: Duration) {
    metrics::record_mesh_mtls_handshake(service, result);
    metrics::record_mesh_mtls_handshake_latency(service, latency.as_secs_f64());
}

pub fn canary_allows_promotion(
    policy: &CanaryPolicy,
    success_rate: f64,
    p99_latency: Duration,
) -> bool {
    success_rate >= policy.min_success_rate && p99_latency <= policy.max_p99_latency
}
