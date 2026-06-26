use axum::{
    extract::{ConnectInfo, State},
    http::{Request, StatusCode},
    middleware::Next,
    response::IntoResponse,
    response::Response,
};
use dashmap::DashMap;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::warn;

pub struct SlidingWindowCounter {
    buckets: Mutex<Vec<(Instant, u64)>>,
    window_size: Duration,
    bucket_count: usize,
}

impl SlidingWindowCounter {
    pub fn new(window_size: Duration, bucket_count: usize) -> Self {
        Self {
            buckets: Mutex::new(Vec::with_capacity(bucket_count)),
            window_size,
            bucket_count,
        }
    }

    pub fn observe(&self, count: u64) {
        let mut buckets = self.buckets.lock();
        let now = Instant::now();
        let bucket_duration = self.window_size / self.bucket_count as u32;

        if let Some((last_time, last_count)) = buckets.last_mut() {
            if now.duration_since(*last_time) < bucket_duration {
                *last_count += count;
            } else {
                buckets.push((now, count));
            }
        } else {
            buckets.push((now, count));
        }

        // Clean up old buckets
        let cutoff = now.checked_sub(self.window_size).unwrap_or(now);
        buckets.retain(|(time, _)| *time > cutoff);

        if buckets.len() > self.bucket_count * 2 {
            let drain_count = buckets.len() - self.bucket_count;
            buckets.drain(0..drain_count);
        }
    }

    pub fn count(&self) -> u64 {
        let buckets = self.buckets.lock();
        let now = Instant::now();
        let cutoff = now.checked_sub(self.window_size).unwrap_or(now);
        let total: u64 = buckets
            .iter()
            .filter(|(time, _)| *time > cutoff)
            .map(|(_, count)| *count)
            .sum();
        total
    }
}

pub struct SourceLimiter {
    pub bucket: TokenBucket,
    pub spike_counter: SlidingWindowCounter,
    pub flagged: Mutex<bool>,
    pub backoff_count: AtomicU64,
    pub backoff_until: Mutex<Option<Instant>>,
}

impl SourceLimiter {
    pub fn new(max_tokens: u64, refill_rate: u64) -> Self {
        Self {
            bucket: TokenBucket::new(max_tokens, refill_rate),
            spike_counter: SlidingWindowCounter::new(Duration::from_secs(1), 10),
            flagged: Mutex::new(false),
            backoff_count: AtomicU64::new(0),
            backoff_until: Mutex::new(None),
        }
    }
}

pub struct RejectionStats {
    pub count: AtomicU64,
    pub last_seen: Mutex<Instant>,
}

pub struct RateLimiter {
    global_bucket: TokenBucket,
    sources: DashMap<String, (Arc<SourceLimiter>, Instant)>,
    rejections: DashMap<String, RejectionStats>,
    normal_per_source_rate: u64,
    flagged_per_source_rate: u64,
}

impl RateLimiter {
    pub fn new(global_rate: u64, normal_rate: u64, flagged_rate: u64) -> Self {
        Self {
            global_bucket: TokenBucket::new(global_rate, global_rate),
            sources: DashMap::new(),
            rejections: DashMap::new(),
            normal_per_source_rate: normal_rate,
            flagged_per_source_rate: flagged_rate,
        }
    }

    pub fn check_limit(&self, source_id: &str) -> Result<(), StatusCode> {
        if !self.global_bucket.try_consume(1) {
            warn!("Global rate limit exceeded");
            self.record_rejection("global");
            return Err(StatusCode::TOO_MANY_REQUESTS);
        }

        let source_entry = self
            .sources
            .entry(source_id.to_string())
            .and_modify(|(_, last_seen)| *last_seen = Instant::now())
            .or_insert_with(|| {
                (
                    Arc::new(SourceLimiter::new(
                        self.normal_per_source_rate,
                        self.normal_per_source_rate,
                    )),
                    Instant::now(),
                )
            });

        let (source, _) = source_entry.value();

        // Spike detection
        source.spike_counter.observe(1);
        let current_count = source.spike_counter.count();
        if current_count > self.normal_per_source_rate * 10 {
            warn!(source_id, current_count, "Spike detected, flagging source");
            let mut flagged = source.flagged.lock();
            if !*flagged {
                *flagged = true;
                source.bucket.update_rate(self.flagged_per_source_rate);
            }
        }

        // Exponential backoff check
        {
            let backoff_until = source.backoff_until.lock();
            if let Some(until) = *backoff_until {
                if Instant::now() < until {
                    warn!(source_id, "Source is in exponential backoff");
                    self.record_rejection(source_id);
                    return Err(StatusCode::TOO_MANY_REQUESTS);
                }
            }
        }

        let is_flagged = *source.flagged.lock();

        if !source.bucket.try_consume(1) {
            warn!(source_id, is_flagged, "Per-source rate limit exceeded");
            self.record_rejection(source_id);

            if is_flagged {
                // Apply/increase exponential backoff
                let mut backoff_until = source.backoff_until.lock();
                let backoff_count = source.backoff_count.fetch_add(1, Ordering::SeqCst).min(30);
                let wait_secs = 2u64.saturating_pow(backoff_count as u32);
                *backoff_until = Some(Instant::now() + Duration::from_secs(wait_secs));
                warn!(source_id, wait_secs, "Increased backoff for flagged source");
            }

            return Err(StatusCode::TOO_MANY_REQUESTS);
        }

        // Reset backoff count on successful request
        source.backoff_count.store(0, Ordering::Relaxed);
        {
            let mut backoff_until = source.backoff_until.lock();
            *backoff_until = None;
        }

        Ok(())
    }

    fn record_rejection(&self, source_id: &str) {
        let stats = self
            .rejections
            .entry(source_id.to_string())
            .or_insert_with(|| RejectionStats {
                count: AtomicU64::new(0),
                last_seen: Mutex::new(Instant::now()),
            });
        stats.count.fetch_add(1, Ordering::Relaxed);
        *stats.last_seen.lock() = Instant::now();
    }

    pub fn flag_source(&self, source_id: &str) {
        let source_entry = self
            .sources
            .entry(source_id.to_string())
            .or_insert_with(|| {
                (
                    Arc::new(SourceLimiter::new(
                        self.normal_per_source_rate,
                        self.normal_per_source_rate,
                    )),
                    Instant::now(),
                )
            });
        let (source, _) = source_entry.value();
        let mut flagged = source.flagged.lock();
        *flagged = true;
        source.bucket.update_rate(self.flagged_per_source_rate);
    }

    pub fn get_status(&self) -> Vec<(String, u64)> {
        let mut stats: Vec<(String, u64)> = self
            .rejections
            .iter()
            .map(|entry| {
                (
                    entry.key().clone(),
                    entry.value().count.load(Ordering::Relaxed),
                )
            })
            .collect();

        stats.sort_by_key(|a| std::cmp::Reverse(a.1));
        stats.into_iter().take(10).collect()
    }

    pub fn cleanup(&self, max_age: Duration) {
        let now = Instant::now();
        self.sources
            .retain(|_, (_, last_seen)| now.duration_since(*last_seen) < max_age);
        self.rejections
            .retain(|_, stats| now.duration_since(*stats.last_seen.lock()) < max_age);
    }
}

pub struct TokenBucket {
    tokens: AtomicU64,
    max_tokens: AtomicU64,
    refill_rate: AtomicU64,
    last_refill: Mutex<Instant>,
}

impl TokenBucket {
    pub fn new(max_tokens: u64, refill_rate: u64) -> Self {
        Self {
            tokens: AtomicU64::new(max_tokens),
            max_tokens: AtomicU64::new(max_tokens),
            refill_rate: AtomicU64::new(refill_rate),
            last_refill: Mutex::new(Instant::now()),
        }
    }

    pub fn update_rate(&self, new_rate: u64) {
        self.max_tokens.store(new_rate, Ordering::Release);
        self.refill_rate.store(new_rate, Ordering::Release);

        loop {
            let tokens = self.tokens.load(Ordering::Acquire);
            if tokens > new_rate {
                if self
                    .tokens
                    .compare_exchange(tokens, new_rate, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    warn!(new_rate, tokens, "Capped tokens after rate reduction");
                    break;
                }
            } else {
                break;
            }
        }
    }

    fn refill(&self) {
        let now = Instant::now();
        let mut last_refill_guard = self.last_refill.lock();
        let elapsed = now.duration_since(*last_refill_guard).as_secs_f64();

        if elapsed > 0.0 {
            let refill_rate = self.refill_rate.load(Ordering::Acquire);
            let tokens_to_add = (elapsed * refill_rate as f64) as u64;
            if tokens_to_add > 0 {
                let max_tokens = self.max_tokens.load(Ordering::Acquire);
                loop {
                    let current = self.tokens.load(Ordering::Acquire);
                    let new_tokens =
                        std::cmp::min(max_tokens, current.saturating_add(tokens_to_add));
                    if self
                        .tokens
                        .compare_exchange(current, new_tokens, Ordering::SeqCst, Ordering::Relaxed)
                        .is_ok()
                    {
                        break;
                    }
                }
                *last_refill_guard = now;
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

pub async fn rate_limit_layer(
    State(rate_limiter): State<Arc<RateLimiter>>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let path = req.uri().path();
    let source_id = if path.starts_with("/api/v1/meters/") {
        path.split('/')
            .nth(4)
            .map(|s| s.to_string())
            .unwrap_or_else(|| addr.ip().to_string())
    } else {
        addr.ip().to_string()
    };

    match rate_limiter.check_limit(&source_id) {
        Ok(_) => next.run(req).await,
        Err(status) => {
            warn!(source_id, status = ?status, "Rate limit violation");
            (status, "Rate limit exceeded").into_response()
        }
    }
}
