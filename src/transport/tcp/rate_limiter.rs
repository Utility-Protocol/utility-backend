//! Connection-rate limiting and surge detection.
//!
//! [`ConnectionRateLimiter`] keeps a sliding window of recent accept timestamps
//! and throttles new connections once the configured rate is exceeded. This
//! protects against the post-power-outage reconnection storms described in
//! issue #53, where thousands of meters reconnect within seconds.

use std::collections::VecDeque;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::Instant;

/// Sliding-window connection rate limiter with surge detection.
pub struct ConnectionRateLimiter {
    /// Steady-state accept rate (connections/second) before throttling kicks in.
    max_per_sec: u64,
    /// Rate above which a surge is declared (reduced backlog recommended).
    surge_threshold_per_sec: u64,
    /// Moving-average window.
    window: Duration,
    events: Mutex<VecDeque<Instant>>,
}

impl ConnectionRateLimiter {
    /// Create a limiter with the given steady-state rate, surge threshold and
    /// moving-average window.
    pub fn new(max_per_sec: u64, surge_threshold_per_sec: u64, window: Duration) -> Self {
        Self {
            max_per_sec,
            surge_threshold_per_sec,
            window,
            events: Mutex::new(VecDeque::new()),
        }
    }

    /// Drop timestamps that have fallen out of the moving-average window.
    fn prune(&self, deque: &mut VecDeque<Instant>, now: Instant) {
        while let Some(front) = deque.front() {
            if now.duration_since(*front) > self.window {
                deque.pop_front();
            } else {
                break;
            }
        }
    }

    /// Current connection rate (connections/second) over the window.
    pub fn current_rate(&self) -> f64 {
        let now = Instant::now();
        let mut events = self.events.lock();
        self.prune(&mut events, now);
        events.len() as f64 / self.window.as_secs_f64()
    }

    /// Whether the current rate exceeds the surge threshold. The acceptor uses
    /// this to drop the listen backlog while a storm is in progress.
    pub fn is_surging(&self) -> bool {
        self.current_rate() >= self.surge_threshold_per_sec as f64
    }

    /// Record an accept attempt and return whether it is within the rate budget
    /// without blocking.
    pub fn try_acquire(&self) -> bool {
        let now = Instant::now();
        let mut events = self.events.lock();
        self.prune(&mut events, now);
        let rate = events.len() as f64 / self.window.as_secs_f64();
        events.push_back(now);
        rate < self.max_per_sec as f64
    }

    /// Record an accept attempt, sleeping for `1/max_per_sec` when the rate is
    /// exceeded so callers are smoothly throttled to the token-bucket rate.
    pub async fn acquire(&self) {
        let over_budget = !self.try_acquire();
        if over_budget && self.max_per_sec > 0 {
            let delay = Duration::from_secs_f64(1.0 / self.max_per_sec as f64);
            tokio::time::sleep(delay).await;
        }
    }
}
