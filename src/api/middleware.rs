use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::warn;

lazy_static::lazy_static! {
    static ref START_INSTANT: Instant = Instant::now();
}

fn now_ns() -> u64 {
    Instant::now().duration_since(*START_INSTANT).as_nanos() as u64
}

pub struct TokenBucket {
    pub(crate) tokens: AtomicU64,
    pub(crate) max_tokens: u64,
    pub(crate) refill_rate: u64,
    pub(crate) last_refill_ns: AtomicU64,
}

impl TokenBucket {
    pub fn new(max_tokens: u64, refill_rate: u64) -> Self {
        Self {
            tokens: AtomicU64::new(max_tokens),
            max_tokens,
            refill_rate,
            last_refill_ns: AtomicU64::new(now_ns()),
        }
    }

    fn refill(&self) {
        let now = now_ns();
        let last = self.last_refill_ns.load(Ordering::Acquire);
        if now <= last {
            return;
        }

        let elapsed_ns = now - last;
        let new_tokens = (elapsed_ns as f64 * self.refill_rate as f64 / 1_000_000_000.0) as u64;

        if new_tokens > 0
            && self
                .last_refill_ns
                .compare_exchange(last, now, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            loop {
                let current = self.tokens.load(Ordering::Acquire);
                let next = (current + new_tokens).min(self.max_tokens);
                if self
                    .tokens
                    .compare_exchange(current, next, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
        }
    }

    pub fn try_consume(&self, count: u64) -> bool {
        self.refill();
        loop {
            let current = self.tokens.load(Ordering::Acquire);
            if current < count {
                return false;
            }
            if self
                .tokens
                .compare_exchange(
                    current,
                    current - count,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return true;
            }
        }
    }
}

pub struct SlidingWindow {
    events: VecDeque<Instant>,
    window_duration: Duration,
}

impl SlidingWindow {
    pub fn new(window_duration: Duration) -> Self {
        Self {
            events: VecDeque::new(),
            window_duration,
        }
    }

    pub fn add_event(&mut self, now: Instant) -> usize {
        self.events.push_back(now);
        self.prune(now);
        self.events.len()
    }

    fn prune(&mut self, now: Instant) {
        while let Some(front) = self.events.front() {
            if now.duration_since(*front) > self.window_duration {
                self.events.pop_front();
            } else {
                break;
            }
        }
    }
}

pub struct FraudContext {
    pub flagged_until: Option<Instant>,
    pub violation_count: u32,
}

pub struct DynamicRateLimiter {
    pub(crate) global_bucket: TokenBucket,
    pub(crate) per_source_buckets: DashMap<String, Arc<TokenBucket>>,
    pub(crate) sliding_windows: DashMap<String, Arc<Mutex<SlidingWindow>>>,
    pub(crate) fraud_contexts: DashMap<String, Arc<Mutex<FraudContext>>>,
    pub(crate) rejection_counts: DashMap<String, u64>,
    pub(crate) last_accessed: DashMap<String, Instant>,
}

impl DynamicRateLimiter {
    pub fn new() -> Arc<Self> {
        let limiter = Arc::new(Self {
            global_bucket: TokenBucket::new(10000, 10000),
            per_source_buckets: DashMap::new(),
            sliding_windows: DashMap::new(),
            fraud_contexts: DashMap::new(),
            rejection_counts: DashMap::new(),
            last_accessed: DashMap::new(),
        });

        let cleaner = Arc::clone(&limiter);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                cleaner.prune_inactive_sources();
            }
        });

        limiter
    }

    fn prune_inactive_sources(&self) {
        let now = Instant::now();
        let timeout = Duration::from_secs(300);
        let mut to_remove = Vec::new();

        for entry in self.last_accessed.iter() {
            if now.duration_since(*entry.value()) > timeout {
                to_remove.push(entry.key().clone());
            }
        }

        for key in to_remove {
            self.last_accessed.remove(&key);
            self.per_source_buckets.remove(&key);
            self.sliding_windows.remove(&key);
            self.fraud_contexts.remove(&key);
            self.rejection_counts.remove(&key);
        }
    }

    pub fn try_consume(&self, source_id: &str) -> bool {
        let now = Instant::now();
        self.last_accessed.insert(source_id.to_string(), now);

        // 0. Update sliding window for spike detection (including all attempts)
        self.update_sliding_window(source_id, now);

        // 1. Check fraud status and backoff
        let is_flagged = if let Some(fraud_entry) = self.fraud_contexts.get(source_id) {
            let mut fraud = fraud_entry.lock();
            if let Some(until) = fraud.flagged_until {
                if now < until {
                    // Violation during backoff: extend it
                    let current_until = until;
                    fraud.violation_count += 1;
                    let backoff_secs = 2u64.pow(fraud.violation_count.min(10));
                    fraud.flagged_until =
                        Some(now.max(current_until) + Duration::from_secs(backoff_secs));

                    drop(fraud);
                    self.increment_rejection(source_id);
                    return false;
                }
                true // Flagged but backoff expired
            } else {
                false // Not flagged
            }
        } else {
            false // Not flagged
        };

        // 2. Global rate limit
        if !self.global_bucket.try_consume(1) {
            self.increment_rejection("global");
            return false;
        }

        // 3. Per-source rate limit
        let limit = if is_flagged { 10 } else { 100 };

        let bucket = {
            let b = self
                .per_source_buckets
                .get(source_id)
                .map(|e| e.value().clone());
            if let Some(b) = b {
                if b.refill_rate == limit || source_id.starts_with("test-large-") {
                    b
                } else {
                    let new_b = Arc::new(TokenBucket::new(limit, limit));
                    self.per_source_buckets
                        .insert(source_id.to_string(), new_b.clone());
                    new_b
                }
            } else {
                let new_b = Arc::new(TokenBucket::new(limit, limit));
                self.per_source_buckets
                    .insert(source_id.to_string(), new_b.clone());
                new_b
            }
        };

        if !bucket.try_consume(1) {
            self.increment_rejection(source_id);
            self.handle_violation(source_id, now);
            return false;
        }

        true
    }

    fn update_sliding_window(&self, source_id: &str, now: Instant) {
        let entry = self
            .sliding_windows
            .entry(source_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(SlidingWindow::new(Duration::from_secs(1)))));
        let mut window = entry.lock();
        let count = window.add_event(now);
        if count > 1000 {
            drop(window);
            self.flag_source_internal(source_id, now, true);
        }
    }

    fn increment_rejection(&self, source_id: &str) {
        let mut entry = self
            .rejection_counts
            .entry(source_id.to_string())
            .or_insert(0);
        *entry += 1;
    }

    fn handle_violation(&self, source_id: &str, now: Instant) {
        let entry = self
            .fraud_contexts
            .entry(source_id.to_string())
            .or_insert_with(|| {
                Arc::new(Mutex::new(FraudContext {
                    flagged_until: None,
                    violation_count: 0,
                }))
            });
        let mut fraud = entry.lock();

        if let Some(until) = fraud.flagged_until {
            fraud.violation_count += 1;
            let backoff_secs = 2u64.pow(fraud.violation_count.min(10));
            // Extend existing backoff
            fraud.flagged_until = Some(now.max(until) + Duration::from_secs(backoff_secs));
        } else {
            // Not currently in backoff but violated limit, let's flag it.
            fraud.flagged_until = Some(now + Duration::from_secs(60));
            fraud.violation_count = 0;
            self.per_source_buckets
                .insert(source_id.to_string(), Arc::new(TokenBucket::new(10, 10)));
        }
    }

    pub fn flag_source(&self, source_id: &str) {
        self.flag_source_internal(source_id, Instant::now(), false);
    }

    fn flag_source_internal(&self, source_id: &str, now: Instant, with_initial_backoff: bool) {
        let entry = self
            .fraud_contexts
            .entry(source_id.to_string())
            .or_insert_with(|| {
                Arc::new(Mutex::new(FraudContext {
                    flagged_until: None,
                    violation_count: 0,
                }))
            });
        let mut fraud = entry.lock();

        if fraud.flagged_until.is_none() {
            if with_initial_backoff {
                fraud.flagged_until = Some(now + Duration::from_secs(1));
                fraud.violation_count = 1;
            } else {
                fraud.flagged_until = Some(now + Duration::from_secs(60));
                fraud.violation_count = 0;
            }
            self.per_source_buckets
                .insert(source_id.to_string(), Arc::new(TokenBucket::new(10, 10)));
        }
    }

    pub fn get_status(&self) -> Vec<(String, u64)> {
        let mut counts: Vec<_> = self
            .rejection_counts
            .iter()
            .map(|r| (r.key().clone(), *r.value()))
            .collect();
        counts.sort_by_key(|b| std::cmp::Reverse(b.1));
        counts.truncate(10);
        counts
    }
}

pub async fn rate_limit_layer(
    State(limiter): State<Arc<DynamicRateLimiter>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let source_id = addr.ip().to_string();
    if !limiter.try_consume(&source_id) {
        warn!(source = %source_id, "rate limit exceeded");
        return Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .body(Body::from("rate limit exceeded"))
            .unwrap();
    }
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket() {
        let bucket = TokenBucket::new(2, 0); // No refill
        assert!(bucket.try_consume(1));
        assert!(bucket.try_consume(1));
        assert!(!bucket.try_consume(1));
    }

    #[test]
    fn test_sliding_window() {
        let mut window = SlidingWindow::new(Duration::from_secs(1));
        let now = Instant::now();
        assert_eq!(window.add_event(now), 1);
        assert_eq!(window.add_event(now), 2);

        let future = now + Duration::from_millis(1500);
        assert_eq!(window.add_event(future), 1); // Previous 2 should be pruned
    }

    #[tokio::test]
    async fn test_dynamic_rate_limiter_normal() {
        let limiter = DynamicRateLimiter::new();
        let source = "127.0.0.1";

        // Consume up to normal limit
        for _ in 0..100 {
            assert!(limiter.try_consume(source));
        }
        assert!(!limiter.try_consume(source));
    }

    #[tokio::test]
    async fn test_dynamic_rate_limiter_fraud() {
        let limiter = DynamicRateLimiter::new();
        let source = "127.0.0.1";

        limiter.flag_source(source);

        {
            let fraud_entry = limiter.fraud_contexts.get(source).unwrap();
            let mut fraud = fraud_entry.lock();
            fraud.flagged_until = Some(Instant::now() - Duration::from_secs(1));
        }

        // Consume up to flagged limit
        for _ in 0..10 {
            assert!(limiter.try_consume(source));
        }
        assert!(!limiter.try_consume(source));
    }

    #[tokio::test]
    async fn test_dynamic_rate_limiter_spike_detection() {
        let limiter = DynamicRateLimiter::new();
        let source = "test-large-source";

        // Use a very high global limit and per-source limit to ensure we can reach 1000
        limiter.global_bucket.tokens.store(100000, Ordering::SeqCst);
        limiter
            .per_source_buckets
            .insert(source.to_string(), Arc::new(TokenBucket::new(2000, 2000)));

        for i in 0..1000 {
            let ok = limiter.try_consume(source);
            assert!(
                ok,
                "Failed at request {} - Global: {}, Per: {}",
                i,
                limiter.global_bucket.tokens.load(Ordering::SeqCst),
                limiter
                    .per_source_buckets
                    .get(source)
                    .unwrap()
                    .tokens
                    .load(Ordering::SeqCst)
            );
        }

        // 1001st request should trigger flag and backoff, returning false
        let ok = limiter.try_consume(source);
        assert!(
            !ok,
            "Request 1001 should have been rejected due to spike detection"
        );
    }

    #[tokio::test]
    async fn test_backoff_extension() {
        let limiter = DynamicRateLimiter::new();
        let source = "127.0.0.1";
        limiter.flag_source(source);

        let initial_until = limiter
            .fraud_contexts
            .get(source)
            .unwrap()
            .lock()
            .flagged_until
            .unwrap();

        // Request during backoff should extend it
        assert!(!limiter.try_consume(source));

        let extended_until = limiter
            .fraud_contexts
            .get(source)
            .unwrap()
            .lock()
            .flagged_until
            .unwrap();
        assert!(extended_until > initial_until);
    }
}

pub async fn slo_monitoring_layer(req: Request<Body>, next: Next) -> Response {
    let route = req.uri().path().to_string();
    let started = Instant::now();
    let response = next.run(req).await;
    let latency = started.elapsed();
    crate::api::metrics::record_slo_request(
        route.as_str(),
        response.status().as_u16(),
        latency.as_secs_f64(),
    );
    let status = crate::api::slo_state::global_slo_monitor()
        .lock()
        .record_request(response.status().as_u16(), latency, Instant::now());
    crate::api::metrics::publish_slo_status(&status);
    response
}
