use std::time::Duration;
use utility_backend::service_mesh::{
    canary_allows_promotion, record_handshake_result, MeshIdentity, ServiceMeshError,
    ServiceMeshMtlsConfig,
};

#[test]
fn default_mesh_config_meets_security_and_latency_bounds() {
    let config = ServiceMeshMtlsConfig::default();

    assert!(config.enabled);
    assert!(config.require_spiffe_id);
    assert_eq!(config.critical_path_budget, Duration::from_millis(100));
    assert_eq!(config.validate(), Ok(()));
}

#[test]
fn mesh_identity_renders_spiffe_id() {
    let identity = MeshIdentity {
        trust_domain: "utility.local".to_string(),
        namespace: "prod".to_string(),
        service_account: "ingestion".to_string(),
    };

    assert_eq!(
        identity.spiffe_id(),
        "spiffe://utility.local/ns/prod/sa/ingestion"
    );
}

#[test]
fn validation_rejects_missing_certificate_material_when_enabled() {
    let config = ServiceMeshMtlsConfig {
        cert_path: "".to_string(),
        ..ServiceMeshMtlsConfig::default()
    };

    assert_eq!(
        config.validate(),
        Err(ServiceMeshError::MissingCertificateMaterial)
    );
}

#[test]
fn validation_enforces_p99_budget() {
    let config = ServiceMeshMtlsConfig {
        critical_path_budget: Duration::from_millis(101),
        ..ServiceMeshMtlsConfig::default()
    };

    assert_eq!(
        config.validate(),
        Err(ServiceMeshError::InvalidLatencyBudget)
    );
}

#[test]
fn canary_policy_requires_success_and_latency_targets() {
    let policy = ServiceMeshMtlsConfig::default().canary;

    assert!(canary_allows_promotion(
        &policy,
        0.99995,
        Duration::from_millis(90)
    ));
    assert!(!canary_allows_promotion(
        &policy,
        0.9990,
        Duration::from_millis(90)
    ));
    assert!(!canary_allows_promotion(
        &policy,
        0.99995,
        Duration::from_millis(101)
    ));
}

#[test]
fn handshake_metrics_can_be_recorded() {
    record_handshake_result("ingestion", "success", Duration::from_millis(7));
}
