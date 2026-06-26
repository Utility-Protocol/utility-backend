//! Graceful shutdown protocol with strict phase ordering (issue #49).
//!
//! On SIGTERM/SIGINT/SIGHUP the pipeline must drain in a precise order so no
//! telemetry is lost and the watermark is persisted before exit. Each phase owns
//! a [`StructuredTaskGroup`]; [`ShutdownProtocol::shutdown`] cancels and drains
//! them in order, persisting a [`ShutdownCheckpoint`] after each so a crash mid
//! shutdown can resume from the last completed phase.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use tracing::{error, info, warn};

use super::task_group::{CancelToken, StructuredTaskGroup};
use crate::storage::checkpoint::{CheckpointStore, ShutdownCheckpoint};

/// Ordered shutdown phases. Drainable phases run in declaration order; `Complete`
/// is the terminal marker.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShutdownPhase {
    Acceptors = 0,
    Reassembly = 1,
    Parsing = 2,
    Evaluation = 3,
    Storage = 4,
    Watermark = 5,
    Blockchain = 6,
    Complete = 7,
}

impl ShutdownPhase {
    /// The drainable phases, in the order they must be shut down.
    pub const DRAIN_ORDER: [ShutdownPhase; 7] = [
        ShutdownPhase::Acceptors,
        ShutdownPhase::Reassembly,
        ShutdownPhase::Parsing,
        ShutdownPhase::Evaluation,
        ShutdownPhase::Storage,
        ShutdownPhase::Watermark,
        ShutdownPhase::Blockchain,
    ];

    pub fn index(self) -> usize {
        self as usize
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ShutdownPhase::Acceptors => "acceptors",
            ShutdownPhase::Reassembly => "reassembly",
            ShutdownPhase::Parsing => "parsing",
            ShutdownPhase::Evaluation => "evaluation",
            ShutdownPhase::Storage => "storage",
            ShutdownPhase::Watermark => "watermark",
            ShutdownPhase::Blockchain => "blockchain",
            ShutdownPhase::Complete => "complete",
        }
    }
}

/// Hard cap on the per-phase deadline (issue #49 bound).
const MAX_PHASE_TIMEOUT: Duration = Duration::from_secs(300);

/// Tunables for the shutdown protocol.
#[derive(Clone, Debug)]
pub struct ShutdownConfig {
    /// Deadline for draining each phase (clamped to 300s).
    pub per_phase_timeout: Duration,
    /// In-flight event ceiling; exceeding it aborts shutdown immediately.
    pub max_in_flight: usize,
    /// Marker file written once shutdown completes.
    pub marker_path: PathBuf,
    /// Optional path for the durable checkpoint.
    pub checkpoint_path: Option<PathBuf>,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            per_phase_timeout: Duration::from_secs(30),
            max_in_flight: 1_000_000,
            marker_path: PathBuf::from("/tmp/utility_shutdown_complete"),
            checkpoint_path: None,
        }
    }
}

/// Why a shutdown did not complete cleanly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShutdownError {
    /// One or more phases exceeded their deadline (shutdown still completed; the
    /// caller should exit non-zero to trigger a supervisor restart).
    PhaseTimeout(Vec<ShutdownPhase>),
    /// In-flight events exceeded the configured ceiling; shutdown aborted.
    InFlightExceeded(usize),
    /// An I/O error persisting the checkpoint or marker.
    Io(String),
}

impl std::fmt::Display for ShutdownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShutdownError::PhaseTimeout(phases) => {
                write!(f, "shutdown phases timed out: {phases:?}")
            }
            ShutdownError::InFlightExceeded(n) => {
                write!(f, "in-flight events {n} exceeded shutdown ceiling")
            }
            ShutdownError::Io(e) => write!(f, "shutdown io error: {e}"),
        }
    }
}

impl std::error::Error for ShutdownError {}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Orchestrates ordered, deadline-bounded shutdown of the pipeline stages.
pub struct ShutdownProtocol {
    phase: AtomicU8,
    config: ShutdownConfig,
    root: CancelToken,
    groups: Vec<Arc<StructuredTaskGroup>>,
    checkpoint: Arc<RwLock<ShutdownCheckpoint>>,
}

impl ShutdownProtocol {
    /// Build a protocol with one task group per drainable phase.
    pub fn new(mut config: ShutdownConfig) -> Self {
        if config.per_phase_timeout > MAX_PHASE_TIMEOUT {
            config.per_phase_timeout = MAX_PHASE_TIMEOUT;
        }
        let root = CancelToken::new();
        let groups = ShutdownPhase::DRAIN_ORDER
            .iter()
            .map(|_| Arc::new(StructuredTaskGroup::new(root.child())))
            .collect();
        Self {
            phase: AtomicU8::new(ShutdownPhase::Acceptors as u8),
            config,
            root,
            groups,
            checkpoint: Arc::new(RwLock::new(ShutdownCheckpoint::new())),
        }
    }

    /// The task group for `phase` (panics for the terminal `Complete` phase).
    pub fn group(&self, phase: ShutdownPhase) -> &Arc<StructuredTaskGroup> {
        &self.groups[phase.index()]
    }

    /// The root cancellation token (cancels every stage when fired).
    pub fn root_token(&self) -> &CancelToken {
        &self.root
    }

    /// The shared checkpoint (e.g. to record per-source watermarks before exit).
    pub fn checkpoint(&self) -> &Arc<RwLock<ShutdownCheckpoint>> {
        &self.checkpoint
    }

    /// The phase currently being processed.
    pub fn current_phase(&self) -> ShutdownPhase {
        match self.phase.load(Ordering::Acquire) {
            0 => ShutdownPhase::Acceptors,
            1 => ShutdownPhase::Reassembly,
            2 => ShutdownPhase::Parsing,
            3 => ShutdownPhase::Evaluation,
            4 => ShutdownPhase::Storage,
            5 => ShutdownPhase::Watermark,
            6 => ShutdownPhase::Blockchain,
            _ => ShutdownPhase::Complete,
        }
    }

    /// Total in-flight events across all stages.
    pub fn total_in_flight(&self) -> usize {
        self.groups.iter().map(|g| g.in_flight()).sum()
    }

    /// Drain all phases in order. Each phase is cancelled then awaited within the
    /// per-phase deadline; the checkpoint is persisted after each. Returns:
    /// * `Ok(())` on a clean drain,
    /// * `Err(PhaseTimeout)` if some phases timed out (shutdown still completed),
    /// * `Err(InFlightExceeded)` if the in-flight ceiling was hit (aborted).
    pub async fn shutdown(&self) -> Result<(), ShutdownError> {
        let mut timed_out: Vec<ShutdownPhase> = Vec::new();

        for phase in ShutdownPhase::DRAIN_ORDER {
            self.phase.store(phase as u8, Ordering::Release);

            let total = self.total_in_flight();
            if total > self.config.max_in_flight {
                error!(
                    in_flight = total,
                    ceiling = self.config.max_in_flight,
                    "CRIT: in-flight events exceed shutdown ceiling; aborting"
                );
                return Err(ShutdownError::InFlightExceeded(total));
            }

            info!(phase = phase.as_str(), "draining shutdown phase");
            let remaining = self.groups[phase.index()]
                .shutdown(self.config.per_phase_timeout)
                .await;

            {
                let mut cp = self.checkpoint.write();
                cp.mark_stage(phase.index() as u8);
                cp.timestamp_ns = now_ns();
            }
            self.persist_checkpoint()?;

            if remaining > 0 {
                warn!(
                    phase = phase.as_str(),
                    remaining, "shutdown phase timed out; proceeding"
                );
                timed_out.push(phase);
            }
        }

        self.phase
            .store(ShutdownPhase::Complete as u8, Ordering::Release);
        self.write_marker()?;

        if timed_out.is_empty() {
            info!("graceful shutdown complete");
            Ok(())
        } else {
            Err(ShutdownError::PhaseTimeout(timed_out))
        }
    }

    fn persist_checkpoint(&self) -> Result<(), ShutdownError> {
        if let Some(path) = &self.config.checkpoint_path {
            let snapshot = self.checkpoint.read().clone();
            CheckpointStore::open(path)
                .save(&snapshot)
                .map_err(|e| ShutdownError::Io(e.to_string()))?;
        }
        Ok(())
    }

    fn write_marker(&self) -> Result<(), ShutdownError> {
        std::fs::write(&self.config.marker_path, b"").map_err(|e| ShutdownError::Io(e.to_string()))
    }
}

/// Block until a shutdown signal arrives, returning its name.
///
/// On Unix this listens for SIGTERM, SIGINT, and SIGHUP; elsewhere it falls back
/// to Ctrl-C.
#[cfg(unix)]
pub async fn wait_for_shutdown_signal() -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut interrupt = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    let mut hangup = signal(SignalKind::hangup()).expect("install SIGHUP handler");
    tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = interrupt.recv() => "SIGINT",
        _ = hangup.recv() => "SIGHUP",
    }
}

#[cfg(not(unix))]
pub async fn wait_for_shutdown_signal() -> &'static str {
    let _ = tokio::signal::ctrl_c().await;
    "CTRL_C"
}

/// Spawn a task that fires `token` when a shutdown signal arrives. Wire this in
/// `main` to initiate [`ShutdownProtocol::shutdown`].
pub fn spawn_signal_listener(token: CancelToken) {
    tokio::spawn(async move {
        let signal = wait_for_shutdown_signal().await;
        info!(signal, "received shutdown signal");
        token.cancel();
    });
}
