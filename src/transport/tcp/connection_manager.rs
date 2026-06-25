//! Per-meter TCP connection registry and lifecycle enforcement.
//!
//! The [`ConnectionManager`] maps each meter ID to at most one live
//! [`TcpStream`]. Registering a meter that already has an open connection
//! immediately resets the stale one (TCP `RST`), satisfying the "1 connection
//! per meter" invariant. It also provides the eviction primitives the
//! [`FdMonitor`](super::fd_monitor::FdMonitor) uses to reclaim descriptors
//! under pressure.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use lazy_static::lazy_static;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::api::metrics;

/// Meter identifier used as the connection registry key.
pub type MeterId = String;

lazy_static! {
    /// Process-wide monotonic origin used to express last-activity timestamps
    /// as `u64` milliseconds in an [`AtomicU64`] (Instant is not atomically
    /// storable).
    static ref CLOCK_ORIGIN: Instant = Instant::now();
}

/// Milliseconds elapsed since the process clock origin.
fn now_millis() -> u64 {
    CLOCK_ORIGIN.elapsed().as_millis() as u64
}

/// Scheduling/eviction priority for a connection. Under hard FD pressure the
/// lowest-priority connections are sacrificed first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Low,
    #[default]
    Normal,
    High,
}

/// A registered connection plus its activity bookkeeping.
struct ConnEntry {
    stream: Arc<AsyncMutex<TcpStream>>,
    last_activity: Arc<AtomicU64>,
    priority: Priority,
}

impl ConnEntry {
    fn new(stream: TcpStream, priority: Priority) -> Self {
        Self {
            stream: Arc::new(AsyncMutex::new(stream)),
            last_activity: Arc::new(AtomicU64::new(now_millis())),
            priority,
        }
    }

    /// Milliseconds since this connection last saw I/O.
    fn idle_millis(&self) -> u64 {
        now_millis().saturating_sub(self.last_activity.load(Ordering::Relaxed))
    }

    /// Forcibly tear the connection down with a TCP `RST`.
    ///
    /// `SO_LINGER` is set to zero so the subsequent close (when the last `Arc`
    /// to the stream is dropped) emits a reset rather than a graceful FIN,
    /// immediately releasing the descriptor.
    async fn reset(&self) {
        let mut stream = self.stream.lock().await;
        let _ = stream.set_linger(Some(Duration::ZERO));
        let _ = stream.shutdown().await;
    }
}

/// A read/write handle over a managed connection that records activity on every
/// successful I/O so the idle-eviction logic stays accurate. Equivalent to the
/// "TimedTcpStream" in the design blueprint.
#[derive(Clone)]
pub struct TimedTcpStream {
    stream: Arc<AsyncMutex<TcpStream>>,
    last_activity: Arc<AtomicU64>,
}

impl TimedTcpStream {
    /// Read into `buf`, stamping the activity clock on success.
    pub async fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = {
            let mut s = self.stream.lock().await;
            s.read(buf).await?
        };
        self.last_activity.store(now_millis(), Ordering::Relaxed);
        Ok(n)
    }

    /// Write `buf`, stamping the activity clock on success.
    pub async fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
        let n = {
            let mut s = self.stream.lock().await;
            s.write(buf).await?
        };
        self.last_activity.store(now_millis(), Ordering::Relaxed);
        Ok(n)
    }
}

/// Tracks every live meter connection and enforces lifecycle policy.
pub struct ConnectionManager {
    active: DashMap<MeterId, ConnEntry>,
    idle_timeout: Duration,
    max_connections: usize,
}

impl ConnectionManager {
    /// Create a manager with the given idle timeout and capacity bound.
    pub fn new(idle_timeout: Duration, max_connections: usize) -> Self {
        Self {
            active: DashMap::new(),
            idle_timeout,
            max_connections,
        }
    }

    /// Number of currently tracked connections.
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    /// Whether the registry is at or above its configured capacity.
    pub fn at_capacity(&self) -> bool {
        self.active.len() >= self.max_connections
    }

    fn publish_gauge(&self) {
        metrics::set_tcp_active_connections(self.active.len() as f64);
    }

    /// Register a meter connection, returning a [`TimedTcpStream`] handle for
    /// subsequent I/O.
    ///
    /// If the meter already had a live connection it is immediately reset with
    /// a TCP `RST`, enforcing the one-connection-per-meter invariant.
    pub async fn register(
        &self,
        meter_id: MeterId,
        stream: TcpStream,
        priority: Priority,
    ) -> TimedTcpStream {
        let entry = ConnEntry::new(stream, priority);
        let handle = TimedTcpStream {
            stream: entry.stream.clone(),
            last_activity: entry.last_activity.clone(),
        };

        if let Some(old) = self.active.insert(meter_id.clone(), entry) {
            old.reset().await;
            metrics::inc_fd_connection_resets();
            info!(meter_id = %meter_id, "Replaced stale connection for meter");
        }

        self.publish_gauge();
        handle
    }

    /// Record activity for a meter (e.g. after a read loop tick that did not go
    /// through [`TimedTcpStream`]).
    pub fn touch(&self, meter_id: &str) {
        if let Some(entry) = self.active.get(meter_id) {
            entry.last_activity.store(now_millis(), Ordering::Relaxed);
        }
    }

    /// Gracefully close and deregister a single meter, if present.
    pub async fn close(&self, meter_id: &str) -> bool {
        if let Some((_, entry)) = self.active.remove(meter_id) {
            entry.reset().await;
            self.publish_gauge();
            true
        } else {
            false
        }
    }

    /// Close every connection idle for longer than `idle_timeout + min_idle`.
    ///
    /// Passing `Duration::ZERO` evicts everything past the configured idle
    /// timeout; a larger `min_idle` is more conservative.
    pub async fn evict_idle(&self, min_idle: Duration) -> usize {
        self.evict_idle_exceeding(self.idle_timeout + min_idle).await
    }

    /// Close every connection that has been idle for longer than `threshold`,
    /// independent of the configured idle timeout. Used by FD reclamation to
    /// shed connections idle for >60s even when the normal timeout is 300s.
    pub async fn evict_idle_exceeding(&self, threshold: Duration) -> usize {
        let threshold_ms = threshold.as_millis() as u64;
        let victims: Vec<MeterId> = self
            .active
            .iter()
            .filter(|e| e.idle_millis() >= threshold_ms)
            .map(|e| e.key().clone())
            .collect();
        self.evict_keys(victims, "idle").await
    }

    /// Close the `count` connections with the oldest last-activity timestamps.
    pub async fn evict_oldest(&self, count: usize) -> usize {
        if count == 0 {
            return 0;
        }
        let mut by_age: Vec<(MeterId, u64)> = self
            .active
            .iter()
            .map(|e| (e.key().clone(), e.last_activity.load(Ordering::Relaxed)))
            .collect();
        // Ascending last-activity == oldest first.
        by_age.sort_by_key(|(_, ts)| *ts);
        let victims: Vec<MeterId> = by_age
            .into_iter()
            .take(count.min(self.active.len()))
            .map(|(id, _)| id)
            .collect();
        self.evict_keys(victims, "oldest").await
    }

    /// Reset all [`Priority::Low`] connections. Last-resort FD reclamation.
    pub async fn evict_low_priority(&self) -> usize {
        let victims: Vec<MeterId> = self
            .active
            .iter()
            .filter(|e| e.priority == Priority::Low)
            .map(|e| e.key().clone())
            .collect();
        self.evict_keys(victims, "low_priority").await
    }

    async fn evict_keys(&self, keys: Vec<MeterId>, reason: &str) -> usize {
        let mut closed: usize = 0;
        for key in keys {
            if let Some((_, entry)) = self.active.remove(&key) {
                entry.reset().await;
                closed += 1;
            }
        }
        if closed > 0 {
            metrics::inc_fd_eviction_count(closed as u64);
            self.publish_gauge();
            warn!(reason, closed, "evicted connections to reclaim descriptors");
        }
        closed
    }
}
