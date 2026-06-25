//! Configuration for the adaptive TCP connection lifecycle manager.
//!
//! These knobs drive the [`ConnectionManager`](super::connection_manager::ConnectionManager),
//! [`FdMonitor`](super::fd_monitor::FdMonitor) and
//! [`ConnectionRateLimiter`](super::rate_limiter::ConnectionRateLimiter). All
//! values have sensible defaults sized for the ~16 K concurrent long-lived
//! meter sockets described in the design (issue #53).

use std::time::Duration;

/// Tunable parameters for the TCP transport layer.
#[derive(Clone, Debug)]
pub struct TcpTransportConfig {
    /// Maximum number of simultaneously tracked connections. Each meter ID may
    /// hold at most one connection, so this also bounds the number of meters
    /// that can be online at once.
    pub max_concurrent_connections: usize,
    /// Connections with no read/write activity for this long are considered
    /// idle and become eligible for graceful eviction.
    pub idle_timeout_secs: u64,
    /// Steady-state token-bucket accept rate enforced by the rate limiter.
    pub max_conn_rate_per_sec: u64,
    /// Connection rate above which surge protection engages (reduced backlog +
    /// rate limiting). Measured over [`Self::rate_window_secs`].
    pub surge_threshold_per_sec: u64,
    /// Fraction of `rlimit_cur` at which proactive idle reclamation begins.
    pub fd_soft_limit_ratio: f64,
    /// Fraction of `rlimit_cur` at which aggressive FD reclamation triggers.
    pub fd_hard_limit_ratio: f64,
    /// How often the FD monitor samples `/proc/self/fd`.
    pub fd_poll_interval_secs: u64,
    /// Moving-average window used by the rate limiter / surge detector.
    pub rate_window_secs: u64,
    /// Listener backlog used under normal conditions.
    pub normal_backlog: i32,
    /// Reduced listener backlog applied while a surge is in progress.
    pub surge_backlog: i32,
}

impl Default for TcpTransportConfig {
    fn default() -> Self {
        Self {
            max_concurrent_connections: 16_384,
            idle_timeout_secs: 300,
            max_conn_rate_per_sec: 500,
            surge_threshold_per_sec: 1_000,
            fd_soft_limit_ratio: 0.80,
            fd_hard_limit_ratio: 0.95,
            fd_poll_interval_secs: 5,
            rate_window_secs: 10,
            normal_backlog: 1_024,
            surge_backlog: 128,
        }
    }
}

impl TcpTransportConfig {
    /// Idle timeout as a [`Duration`].
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_secs(self.idle_timeout_secs)
    }

    /// FD monitor poll interval as a [`Duration`].
    pub fn fd_poll_interval(&self) -> Duration {
        Duration::from_secs(self.fd_poll_interval_secs)
    }

    /// Rate-limiter moving-average window as a [`Duration`].
    pub fn rate_window(&self) -> Duration {
        Duration::from_secs(self.rate_window_secs)
    }

    /// Compute the soft FD limit for a given `rlimit_cur`.
    pub fn soft_limit(&self, rlimit_cur: u64) -> u64 {
        (rlimit_cur as f64 * self.fd_soft_limit_ratio) as u64
    }

    /// Compute the hard FD limit for a given `rlimit_cur`.
    pub fn hard_limit(&self, rlimit_cur: u64) -> u64 {
        (rlimit_cur as f64 * self.fd_hard_limit_ratio) as u64
    }
}
