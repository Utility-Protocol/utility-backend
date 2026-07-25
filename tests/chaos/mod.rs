use std::collections::HashSet;
use std::time::Duration;

use utility_backend::chaos::{
    staging_scenarios, ChaosDomain, AVAILABILITY_TARGET_BASIS_POINTS, CANARY_BAKE_TIME,
    CRITICAL_PATH_P99_TARGET,
};

#[test]
fn staging_scenarios_cover_all_service_domains() {
    let scenarios = staging_scenarios();
    let domains = scenarios
        .iter()
        .map(|scenario| scenario.domain)
        .collect::<HashSet<_>>();

    for expected in [
        ChaosDomain::Gateway,
        ChaosDomain::Ingestion,
        ChaosDomain::Storage,
        ChaosDomain::Settlement,
        ChaosDomain::Blockchain,
        ChaosDomain::Api,
    ] {
        assert!(
            domains.contains(&expected),
            "missing scenario for {expected:?}"
        );
    }
}

#[test]
fn staging_scenarios_stay_inside_safety_guardrails() {
    for scenario in staging_scenarios() {
        assert!(
            scenario.is_within_guardrails(),
            "unsafe scenario: {scenario:?}"
        );
        assert!(scenario.duration <= Duration::from_secs(30 * 60));
        assert!(scenario.max_blast_radius_percent <= 10);
    }
}

#[test]
fn chaos_targets_match_staging_slo_contract() {
    assert_eq!(CRITICAL_PATH_P99_TARGET, Duration::from_millis(100));
    assert_eq!(AVAILABILITY_TARGET_BASIS_POINTS, 9_999);
    assert_eq!(CANARY_BAKE_TIME, Duration::from_secs(15 * 60));
}
