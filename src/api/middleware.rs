use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use dashmap::DashMap;
use serde::Serialize;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::warn;

const GLOBAL_RATE_PER_SECOND: u64 = 10_000;
const NORMAL_SOURCE_RATE_PER_SECOND: u64 = 100;
const FLAGGED_SOURCE_RATE_PER_SECOND: u64 = 10;
const SPIKE_MULTIPLIER: u64 = 10;
const BASE_COOLDOWN: Duration = Duration::from_millis(250);
const MAX_COOLDOWN: Duration = Duration::from_secs(60);

#[allow(dead_code)]
pub struct TokenBucket {
    tokens: AtomicU64,
    max_tokens: u64,
    refill_rate: u64,
}

impl TokenBucket {
    pub fn new(max_tokens: u64, refill_rate: u64) -> Self {
        Self {
            tokens: AtomicU64::new(max_tokens),
            max_tokens,
            refill_rate,
        }
    }

    pub fn try_consume(&self, count: u64) -> bool {
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

#[derive(Debug)]
struct SlidingWindowCounter {
    events: VecDeque<Instant>,
    window: Duration,
}

impl SlidingWindowCounter {
    fn new(window: Duration) -> Self {
        Self {
            events: VecDeque::new(),
            window,
        }
    }

    fn record(&mut self, now: Instant) -> u64 {
        self.prune(now);
        self.events.push_back(now);
        self.events.len() as u64
    }

    fn count(&mut self, now: Instant) -> u64 {
        self.prune(now);
        self.events.len() as u64
    }

    fn prune(&mut self, now: Instant) {
        while self
            .events
            .front()
            .is_some_and(|event| now.duration_since(*event) >= self.window)
        {
            self.events.pop_front();
        }
    }
}

#[derive(Debug)]
struct SourceLimitState {
    window: SlidingWindowCounter,
    flagged: bool,
    violation_count: u32,
    cooldown_until: Option<Instant>,
    limited_requests: u64,
}

impl SourceLimitState {
    fn new() -> Self {
        Self {
            window: SlidingWindowCounter::new(Duration::from_secs(1)),
            flagged: false,
            violation_count: 0,
            cooldown_until: None,
            limited_requests: 0,
        }
    }

    fn rate_limit(&self) -> u64 {
        if self.flagged {
            FLAGGED_SOURCE_RATE_PER_SECOND
        } else {
            NORMAL_SOURCE_RATE_PER_SECOND
        }
    }

    fn register_violation(&mut self, now: Instant) {
        self.violation_count = self.violation_count.saturating_add(1);
        self.limited_requests = self.limited_requests.saturating_add(1);
        let exponent = self.violation_count.saturating_sub(1).min(16);
        let multiplier = 1_u32 << exponent;
        let cooldown = BASE_COOLDOWN.saturating_mul(multiplier).min(MAX_COOLDOWN);
        self.cooldown_until = Some(now + cooldown);
    }
}

#[derive(Debug, Serialize)]
pub struct RateLimitedSourceStatus {
    pub source: String,
    pub limited_requests: u64,
    pub flagged: bool,
    pub current_limit_per_second: u64,
    pub requests_in_current_window: u64,
    pub cooldown_remaining_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct RateLimiterStatus {
    pub global_limit_per_second: u64,
    pub normal_source_limit_per_second: u64,
    pub flagged_source_limit_per_second: u64,
    pub top_limited_sources: Vec<RateLimitedSourceStatus>,
}

#[derive(Debug)]
pub struct DynamicRateLimiter {
    global: Mutex<SlidingWindowCounter>,
    sources: DashMap<String, Mutex<SourceLimitState>>,
}

impl Default for DynamicRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl DynamicRateLimiter {
    pub fn new() -> Self {
        Self {
            global: Mutex::new(SlidingWindowCounter::new(Duration::from_secs(1))),
            sources: DashMap::new(),
        }
    }

    pub async fn allow(&self, source: &str) -> bool {
        let now = Instant::now();
        if self.global.lock().await.record(now) > GLOBAL_RATE_PER_SECOND {
            warn!(source, "global rate limit exceeded");
            return false;
        }

        let source_state = self
            .sources
            .entry(source.to_owned())
            .or_insert_with(|| Mutex::new(SourceLimitState::new()));
        let mut state = source_state.lock().await;

        if state
            .cooldown_until
            .is_some_and(|cooldown_until| now < cooldown_until)
        {
            state.register_violation(now);
            warn!(source, "source in rate-limit cooldown");
            return false;
        }

        let count = state.window.record(now);
        if !state.flagged && count >= NORMAL_SOURCE_RATE_PER_SECOND * SPIKE_MULTIPLIER {
            state.flagged = true;
            warn!(
                source,
                count, "source flagged by sliding-window spike detector"
            );
        }

        if count > state.rate_limit() {
            state.register_violation(now);
            warn!(source, count, "per-source rate limit exceeded");
            return false;
        }

        true
    }

    pub async fn flag_source(&self, source: impl Into<String>) {
        let source_state = self
            .sources
            .entry(source.into())
            .or_insert_with(|| Mutex::new(SourceLimitState::new()));
        source_state.lock().await.flagged = true;
    }

    pub async fn clear_flag(&self, source: &str) {
        if let Some(source_state) = self.sources.get(source) {
            let mut state = source_state.lock().await;
            state.flagged = false;
            state.violation_count = 0;
            state.cooldown_until = None;
        }
    }

    pub async fn status(&self) -> RateLimiterStatus {
        let now = Instant::now();
        let mut statuses = Vec::new();
        for entry in self.sources.iter() {
            let source = entry.key().clone();
            let mut state = entry.value().lock().await;
            let cooldown_remaining_ms = state
                .cooldown_until
                .and_then(|until| until.checked_duration_since(now))
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or_default();
            let requests_in_current_window = state.window.count(now);
            statuses.push(RateLimitedSourceStatus {
                source,
                limited_requests: state.limited_requests,
                flagged: state.flagged,
                current_limit_per_second: state.rate_limit(),
                requests_in_current_window,
                cooldown_remaining_ms,
            });
        }
        statuses.sort_by(|left, right| right.limited_requests.cmp(&left.limited_requests));
        statuses.truncate(10);

        RateLimiterStatus {
            global_limit_per_second: GLOBAL_RATE_PER_SECOND,
            normal_source_limit_per_second: NORMAL_SOURCE_RATE_PER_SECOND,
            flagged_source_limit_per_second: FLAGGED_SOURCE_RATE_PER_SECOND,
            top_limited_sources: statuses,
        }
    }
}

pub async fn rate_limiter_status(
    State(rate_limiter): State<Arc<DynamicRateLimiter>>,
) -> Json<RateLimiterStatus> {
    Json(rate_limiter.status().await)
}

pub async fn rate_limit_layer(
    State(rate_limiter): State<Arc<DynamicRateLimiter>>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let source = source_key(&req);
    if !rate_limiter.allow(&source).await {
        return Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .body(Body::from("rate limit exceeded"))
            .unwrap();
    }

    next.run(req).await
}

fn source_key(req: &Request<axum::body::Body>) -> String {
    if let Some(meter_id) = req
        .headers()
        .get("x-meter-id")
        .and_then(|v| v.to_str().ok())
    {
        return format!("meter:{meter_id}");
    }

    if let Some(forwarded_for) = req
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
    {
        return format!("ip:{}", forwarded_for.trim());
    }

    if let Some(addr) = req.extensions().get::<SocketAddr>() {
        return format!("ip:{}", addr.ip());
    }

    "ip:unknown".to_owned()
}

#[allow(dead_code)]
pub async fn legacy_rate_limit_layer(req: Request<axum::body::Body>, next: Next) -> Response {
    let bucket = req.extensions().get::<Arc<TokenBucket>>();
    match bucket {
        Some(b) if !b.try_consume(1) => {
            (StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded").into_response()
        }
        _ => next.run(req).await,
    }
}
