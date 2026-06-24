use utility_backend::api::middleware::{DynamicRateLimiter, TokenBucket};

#[tokio::test]
async fn test_token_bucket_rate_limit() {
    let bucket = TokenBucket::new(5, 1);
    for _ in 0..5 {
        assert!(bucket.try_consume(1));
    }
    assert!(!bucket.try_consume(1));
}

#[tokio::test]
async fn test_dynamic_rate_limiter_enforces_independent_source_limits() {
    let limiter = DynamicRateLimiter::new();

    for _ in 0..100 {
        assert!(limiter.allow("meter:MTR-A").await);
    }
    assert!(!limiter.allow("meter:MTR-A").await);
    assert!(limiter.allow("meter:MTR-B").await);
}

#[tokio::test]
async fn test_fraud_signal_reduces_source_limit_immediately() {
    let limiter = DynamicRateLimiter::new();
    limiter.flag_source("meter:MTR-FRAUD").await;

    for _ in 0..10 {
        assert!(limiter.allow("meter:MTR-FRAUD").await);
    }
    assert!(!limiter.allow("meter:MTR-FRAUD").await);

    let status = limiter.status().await;
    let source = status
        .top_limited_sources
        .iter()
        .find(|source| source.source == "meter:MTR-FRAUD")
        .unwrap();
    assert!(source.flagged);
    assert_eq!(source.current_limit_per_second, 10);
}

#[tokio::test]
async fn test_flagged_source_exponential_backoff_records_cooldown() {
    let limiter = DynamicRateLimiter::new();
    limiter.flag_source("meter:MTR-BACKOFF").await;

    for _ in 0..11 {
        let _ = limiter.allow("meter:MTR-BACKOFF").await;
    }
    let first_status = limiter.status().await;
    let first_cooldown = first_status.top_limited_sources[0].cooldown_remaining_ms;

    assert!(!limiter.allow("meter:MTR-BACKOFF").await);
    let second_status = limiter.status().await;
    let second_cooldown = second_status.top_limited_sources[0].cooldown_remaining_ms;

    assert!(first_cooldown > 0);
    assert!(second_cooldown >= first_cooldown);
}

#[test]
fn test_meter_api_serialization() {
    let info = utility_backend::api::handlers::MeterInfo {
        id: "MTR-X".into(),
        tenant_id: "grid-north".into(),
        location: "dam-beta".into(),
        last_reading: 987.65,
    };
    let json = serde_json::to_string(&info).unwrap();
    assert!(json.contains("MTR-X"));
    assert!(json.contains("grid-north"));
}
