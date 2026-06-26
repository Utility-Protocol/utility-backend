use std::time::Duration;
use utility_backend::api::middleware::RateLimiter;

#[tokio::test]
async fn test_rate_limiter_global_limit() {
    let limiter = RateLimiter::new(5, 100, 100);
    for _ in 0..5 {
        assert!(limiter.check_limit("source1").is_ok());
    }
    assert!(limiter.check_limit("source1").is_err());
}

#[tokio::test]
async fn test_rate_limiter_per_source_limit() {
    let limiter = RateLimiter::new(1000, 5, 5);
    for _ in 0..5 {
        assert!(limiter.check_limit("source1").is_ok());
    }
    assert!(limiter.check_limit("source1").is_err());

    // Different source should still be okay
    assert!(limiter.check_limit("source2").is_ok());
}

#[tokio::test]
async fn test_rate_limiter_refill() {
    let limiter = RateLimiter::new(1000, 5, 5);
    for _ in 0..5 {
        assert!(limiter.check_limit("source1").is_ok());
    }
    assert!(limiter.check_limit("source1").is_err());

    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(limiter.check_limit("source1").is_ok());
}

#[tokio::test]
async fn test_rate_limiter_spike_detection() {
    // normal rate 10 -> spike threshold 100
    // flagged rate 2
    let limiter = RateLimiter::new(1000, 10, 2);

    // Send 101 requests rapidly to trigger spike detection
    for _ in 0..101 {
        let _ = limiter.check_limit("source1");
    }

    // Now it's flagged. Limit is 2 req/s.
    // Wait for a small amount of time to allow some refill, but less than what would give 3 tokens.
    // At 2 req/s, we get 1 token every 500ms.
    tokio::time::sleep(Duration::from_millis(600)).await;

    // We should have at most 1 or 2 tokens now.
    let _ = limiter.check_limit("source1"); // Consume 1
    let _ = limiter.check_limit("source1"); // Consume 2 (if it refilled 2)

    let result = limiter.check_limit("source1");
    assert!(result.is_err(), "Expected 3rd request to fail due to flagged limit (2 req/s), but got Ok. Tokens might have over-refilled.");
}

#[tokio::test]
async fn test_rate_limiter_fraud_flagging() {
    let limiter = RateLimiter::new(1000, 10, 2);
    limiter.flag_source("source1");

    for _ in 0..2 {
        assert!(limiter.check_limit("source1").is_ok());
    }
    assert!(limiter.check_limit("source1").is_err());
}

#[tokio::test]
async fn test_rate_limiter_exponential_backoff() {
    let limiter = RateLimiter::new(1000, 10, 1);
    limiter.flag_source("source1");

    // Consume the only token
    assert!(limiter.check_limit("source1").is_ok());
    // Fail -> backoff starts (2^0 = 1s)
    assert!(limiter.check_limit("source1").is_err());

    // Should still be in backoff
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(limiter.check_limit("source1").is_err());

    // After 1s, backoff expires
    tokio::time::sleep(Duration::from_millis(600)).await;
    // Should be okay now
    assert!(limiter.check_limit("source1").is_ok());
}

#[tokio::test]
async fn test_rate_limiter_status() {
    let limiter = RateLimiter::new(1000, 1, 1);
    let _ = limiter.check_limit("source1");
    let _ = limiter.check_limit("source1"); // reject
    let _ = limiter.check_limit("source2");
    let _ = limiter.check_limit("source2"); // reject
    let _ = limiter.check_limit("source2"); // reject

    let status = limiter.get_status();
    assert_eq!(status[0].0, "source2");
    assert_eq!(status[0].1, 2);
    assert_eq!(status[1].0, "source1");
    assert_eq!(status[1].1, 1);
}
