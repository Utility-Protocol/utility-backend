//! File-descriptor pressure monitor.
//!
//! [`FdMonitor`] periodically samples the process's open descriptor count and
//! drives staged reclamation through the [`ConnectionManager`] before the
//! kernel starts failing `accept()` with `EMFILE`.
//!
//! Descriptor counting and `rlimit` inspection are inherently OS specific. The
//! real targets run Linux (the design references `/proc/self/fd`), so those
//! paths are implemented for Linux/Unix and degrade gracefully elsewhere so the
//! crate still builds and tests on developer machines.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tracing::{error, warn};

use super::connection_manager::ConnectionManager;
use crate::api::metrics;

/// Periodically samples FD usage and triggers reclamation.
pub struct FdMonitor {
    soft_limit: u64,
    hard_limit: u64,
    /// Idle threshold used during soft-pressure reclamation.
    idle_timeout: Duration,
    poll_interval: Duration,
}

impl FdMonitor {
    /// Construct a monitor from an absolute `rlimit_cur` and the configured
    /// soft/hard ratios.
    pub fn new(
        rlimit_cur: u64,
        soft_ratio: f64,
        hard_ratio: f64,
        idle_timeout: Duration,
        poll_interval: Duration,
    ) -> Self {
        let soft_limit = (rlimit_cur as f64 * soft_ratio) as u64;
        let hard_limit = (rlimit_cur as f64 * hard_ratio) as u64;
        metrics::set_fd_soft_limit(soft_limit as f64);
        metrics::set_fd_hard_limit(hard_limit as f64);
        Self {
            soft_limit,
            hard_limit,
            idle_timeout,
            poll_interval,
        }
    }

    /// Build a monitor by reading the current process `RLIMIT_NOFILE`.
    pub fn from_rlimit(
        soft_ratio: f64,
        hard_ratio: f64,
        idle_timeout: Duration,
        poll_interval: Duration,
    ) -> io::Result<Self> {
        let rlimit_cur = rlimit_nofile()?;
        Ok(Self::new(
            rlimit_cur,
            soft_ratio,
            hard_ratio,
            idle_timeout,
            poll_interval,
        ))
    }

    pub fn soft_limit(&self) -> u64 {
        self.soft_limit
    }

    pub fn hard_limit(&self) -> u64 {
        self.hard_limit
    }

    /// Run a single monitoring tick: sample FD usage, publish metrics and run
    /// the staged reclamation ladder. Returns the number of connections closed.
    pub async fn tick(&self, cm: &Arc<ConnectionManager>) -> usize {
        let count = match current_fd_count() {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "FD count unavailable on this platform; skipping reclamation");
                return 0;
            }
        };
        metrics::set_fd_current_open(count as f64);

        if count > self.hard_limit {
            error!(
                fd_open = count,
                hard_limit = self.hard_limit,
                "CRIT: FD usage exceeded hard limit; performing emergency reclamation"
            );
            self.reclaim(cm, count).await
        } else if count > self.soft_limit {
            warn!(
                fd_open = count,
                soft_limit = self.soft_limit,
                "FD usage exceeded soft limit; shedding idle connections"
            );
            cm.evict_idle_exceeding(self.idle_timeout).await
        } else {
            0
        }
    }

    /// Staged FD reclamation, escalating until usage drops below the hard
    /// limit or no further connections can be shed:
    ///   1. close everything past the idle timeout,
    ///   2. close anything idle for >60s,
    ///   3. drop the lowest-priority connections,
    ///   4. as a last resort, close the oldest 10% of all connections.
    async fn reclaim(&self, cm: &Arc<ConnectionManager>, fd_count: u64) -> usize {
        let mut closed = cm.evict_idle_exceeding(self.idle_timeout).await;

        if current_fd_count().unwrap_or(0) > self.hard_limit {
            closed += cm.evict_idle_exceeding(Duration::from_secs(60)).await;
        }
        if current_fd_count().unwrap_or(0) > self.hard_limit {
            closed += cm.evict_low_priority().await;
        }
        if current_fd_count().unwrap_or(0) > self.hard_limit {
            // Oldest 10% of the descriptor budget, bounded by live connections.
            let target = ((fd_count as f64) * 0.1).ceil() as usize;
            closed += cm.evict_oldest(target).await;
        }
        closed
    }

    /// Long-running loop driving [`Self::tick`] every poll interval.
    pub async fn monitor_loop(self: Arc<Self>, cm: Arc<ConnectionManager>) {
        let mut interval = tokio::time::interval(self.poll_interval);
        loop {
            interval.tick().await;
            self.tick(&cm).await;
        }
    }
}

/// Current number of open file descriptors for this process.
///
/// On Linux this counts entries in `/proc/self/fd`. Other platforms return
/// [`io::ErrorKind::Unsupported`].
pub fn current_fd_count() -> io::Result<u64> {
    #[cfg(target_os = "linux")]
    {
        let mut count: u64 = 0;
        for entry in std::fs::read_dir("/proc/self/fd")? {
            entry?;
            count += 1;
        }
        // Subtract the descriptor opened by read_dir itself.
        Ok(count.saturating_sub(1))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "FD counting only implemented for Linux (/proc/self/fd)",
        ))
    }
}

/// Read the soft limit of `RLIMIT_NOFILE` for the current process.
pub fn rlimit_nofile() -> io::Result<u64> {
    #[cfg(unix)]
    {
        // SAFETY: `getrlimit` only writes into the provided rlimit struct.
        let mut rl = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(rl.rlim_cur as u64)
    }
    #[cfg(not(unix))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "RLIMIT_NOFILE only available on Unix",
        ))
    }
}
