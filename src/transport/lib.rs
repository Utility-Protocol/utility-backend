//! Transport-layer startup wiring.
//!
//! [`spawn_transport_monitors`] constructs the shared [`ConnectionManager`] and
//! [`ConnectionRateLimiter`] and starts the background [`FdMonitor`] loop. It is
//! called once from `main` during service bootstrap.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use super::tcp::config::TcpTransportConfig;
use super::tcp::connection_manager::ConnectionManager;
use super::tcp::fd_monitor::{rlimit_nofile, FdMonitor};
use super::tcp::rate_limiter::ConnectionRateLimiter;

/// Shared transport runtime handles returned to the caller so the accept loop
/// can register connections against the same manager the FD monitor reclaims
/// from.
#[derive(Clone)]
pub struct TransportRuntime {
    pub connection_manager: Arc<ConnectionManager>,
    pub rate_limiter: Arc<ConnectionRateLimiter>,
}

/// Build the connection manager + rate limiter and spawn the FD monitor loop.
///
/// On platforms where `RLIMIT_NOFILE` cannot be read (non-Unix dev machines)
/// the monitor is constructed against a conservative default so the rest of the
/// transport stack still functions; the monitor's per-tick FD sampling itself
/// no-ops where `/proc/self/fd` is unavailable.
pub fn spawn_transport_monitors(cfg: TcpTransportConfig) -> TransportRuntime {
    let connection_manager = Arc::new(ConnectionManager::new(
        cfg.idle_timeout(),
        cfg.max_concurrent_connections,
    ));
    let rate_limiter = Arc::new(ConnectionRateLimiter::new(
        cfg.max_conn_rate_per_sec,
        cfg.surge_threshold_per_sec,
        cfg.rate_window(),
    ));

    let rlimit_cur = rlimit_nofile().unwrap_or_else(|e| {
        warn!(error = %e, "could not read RLIMIT_NOFILE; assuming 65536");
        65_536
    });

    let monitor = Arc::new(FdMonitor::new(
        rlimit_cur,
        cfg.fd_soft_limit_ratio,
        cfg.fd_hard_limit_ratio,
        cfg.idle_timeout(),
        cfg.fd_poll_interval(),
    ));
    info!(
        rlimit_cur,
        soft_limit = monitor.soft_limit(),
        hard_limit = monitor.hard_limit(),
        "starting FD monitor"
    );

    let cm = connection_manager.clone();
    tokio::spawn(async move {
        monitor.monitor_loop(cm).await;
    });

    TransportRuntime {
        connection_manager,
        rate_limiter,
    }
}

/// Convenience wrapper using default tuning. Returns the runtime and the poll
/// interval used (handy for tests/diagnostics).
pub fn spawn_default_transport_monitors() -> (TransportRuntime, Duration) {
    let cfg = TcpTransportConfig::default();
    let interval = cfg.fd_poll_interval();
    (spawn_transport_monitors(cfg), interval)
}
