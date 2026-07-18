use std::time::Duration;

use utility_backend::cache::{CacheConfig, CacheLayer};

#[tokio::test]
async fn cache_returns_memory_hit_before_ttl_expires() {
    let cache = CacheLayer::new(CacheConfig {
        default_ttl: Duration::from_millis(200),
        max_entries: 8,
        redis_url: None,
        namespace: "test".to_string(),
    })
    .unwrap();

    cache.set("meter:1", &42_u64, None).await.unwrap();

    assert_eq!(cache.get::<u64>("meter:1").await.unwrap(), Some(42));
    assert_eq!(cache.local_len(), 1);
}

#[tokio::test]
async fn cache_expires_entries_after_configurable_ttl() {
    let cache = CacheLayer::new(CacheConfig {
        default_ttl: Duration::from_millis(10),
        max_entries: 8,
        redis_url: None,
        namespace: "test".to_string(),
    })
    .unwrap();

    cache
        .set("meter:1", &"reading", Some(Duration::from_millis(5)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;

    assert_eq!(cache.get::<String>("meter:1").await.unwrap(), None);
}

#[tokio::test]
async fn cache_evicts_to_configured_capacity() {
    let cache = CacheLayer::new(CacheConfig {
        default_ttl: Duration::from_secs(60),
        max_entries: 2,
        redis_url: None,
        namespace: "test".to_string(),
    })
    .unwrap();

    cache.set("a", &1_u8, None).await.unwrap();
    cache.set("b", &2_u8, None).await.unwrap();
    cache.set("c", &3_u8, None).await.unwrap();

    assert!(cache.local_len() <= 2);
}

#[test]
fn cache_config_rejects_zero_ttl() {
    let cfg = CacheConfig {
        default_ttl: Duration::ZERO,
        ..CacheConfig::default()
    };

    assert!(cfg.validate().is_err());
}
