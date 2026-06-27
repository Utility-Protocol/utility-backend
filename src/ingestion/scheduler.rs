//! Adaptive window-sizing scheduler for the ingestion pipeline (issue #46).
//!
//! Each of the five serial stages (Accept → Reassemble → Parse → Evaluate →
//! Store) has a credit window. Every scheduling tick the scheduler reads each
//! stage's buffer occupancy and p99 latency and resizes its window with an AIMD
//! controller: additive increase (+`increase_step`) while healthy, multiplicative
//! decrease (×0.7) when congested (occupancy > 0.85 or a p99 spike past the
//! running baseline). A global slot budget caps total memory; if exceeded all
//! windows are scaled down proportionally.
//!
//! State is plain atomics (occupancy is stored as parts-per-million so no
//! `AtomicF64` dependency is needed); the windowed channel that applies a
//! computed window lives in [`super::windowed_channel`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tracing::warn;

/// Default per-stage minimum window (slots).
pub const MIN_WINDOW: u64 = 1_024;
/// Default per-stage maximum window (slots).
pub const MAX_WINDOW: u64 = 131_072;

/// Pipeline stages, in dataflow order.
#[repr(usize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Accept = 0,
    Reassemble = 1,
    Parse = 2,
    Evaluate = 3,
    Store = 4,
}

impl Stage {
    pub const ALL: [Stage; 5] = [
        Stage::Accept,
        Stage::Reassemble,
        Stage::Parse,
        Stage::Evaluate,
        Stage::Store,
    ];

    pub fn index(self) -> usize {
        self as usize
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Stage::Accept => "accept",
            Stage::Reassemble => "reassemble",
            Stage::Parse => "parse",
            Stage::Evaluate => "evaluate",
            Stage::Store => "store",
        }
    }
}

/// Per-stage live state shared between the stage and the scheduler.
#[derive(Debug)]
pub struct StageState {
    window: AtomicU64,
    occupancy_ppm: AtomicU64,
    p99_latency_ns: AtomicU64,
    baseline_p99_ns: AtomicU64,
    throughput: AtomicU64,
}

impl StageState {
    fn new(window: u64) -> Self {
        Self {
            window: AtomicU64::new(window),
            occupancy_ppm: AtomicU64::new(0),
            p99_latency_ns: AtomicU64::new(0),
            baseline_p99_ns: AtomicU64::new(0),
            throughput: AtomicU64::new(0),
        }
    }

    /// Current window size in slots.
    pub fn window(&self) -> u64 {
        self.window.load(Ordering::Acquire)
    }

    fn set_window(&self, window: u64) {
        self.window.store(window, Ordering::Release);
    }

    /// Report buffer occupancy in `0.0..=1.0`.
    pub fn set_occupancy(&self, occupancy: f64) {
        let ppm = (occupancy.clamp(0.0, 1.0) * 1_000_000.0) as u64;
        self.occupancy_ppm.store(ppm, Ordering::Release);
    }

    /// Current occupancy in `0.0..=1.0`.
    pub fn occupancy(&self) -> f64 {
        self.occupancy_ppm.load(Ordering::Acquire) as f64 / 1_000_000.0
    }

    /// Report the measured p99 processing latency (nanoseconds).
    pub fn set_p99_latency_ns(&self, ns: u64) {
        self.p99_latency_ns.store(ns, Ordering::Release);
    }

    pub fn p99_latency_ns(&self) -> u64 {
        self.p99_latency_ns.load(Ordering::Acquire)
    }

    /// The running p99 baseline the spike check compares against.
    pub fn baseline_p99_ns(&self) -> u64 {
        self.baseline_p99_ns.load(Ordering::Acquire)
    }

    /// Add `n` to the throughput accumulator for this measurement period.
    pub fn record_throughput(&self, n: u64) {
        self.throughput.fetch_add(n, Ordering::Relaxed);
    }

    /// Read and reset the throughput accumulator.
    pub fn take_throughput(&self) -> u64 {
        self.throughput.swap(0, Ordering::AcqRel)
    }
}

/// AIMD / budget parameters.
#[derive(Clone, Copy, Debug)]
pub struct SchedulerConfig {
    /// Additive increase per tick (slots).
    pub increase_step: u64,
    /// Multiplicative decrease numerator/denominator (e.g. 7/10 = ×0.7).
    pub md_numerator: u64,
    pub md_denominator: u64,
    /// Occupancy (ppm) above which a stage is congested.
    pub occupancy_high_ppm: u64,
    /// p99 multiple of baseline above which a stage is congested.
    pub p99_spike_multiplier: u64,
    pub min_window: u64,
    pub max_window: u64,
    /// Total slot budget across all stages (memory cap / slot size).
    pub budget_slots: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            increase_step: 64,
            md_numerator: 7,
            md_denominator: 10,
            occupancy_high_ppm: 850_000,
            p99_spike_multiplier: 2,
            min_window: MIN_WINDOW,
            max_window: MAX_WINDOW,
            // 512 MiB / 4 KiB slots.
            budget_slots: 512 * 1024 * 1024 / 4096,
        }
    }
}

/// Adaptive window-sizing scheduler over the five pipeline stages.
pub struct AdaptiveScheduler {
    stages: [Arc<StageState>; 5],
    config: SchedulerConfig,
}

impl AdaptiveScheduler {
    /// Create a scheduler with every stage starting at `min_window`.
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            stages: [
                Arc::new(StageState::new(config.min_window)),
                Arc::new(StageState::new(config.min_window)),
                Arc::new(StageState::new(config.min_window)),
                Arc::new(StageState::new(config.min_window)),
                Arc::new(StageState::new(config.min_window)),
            ],
            config,
        }
    }

    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    /// Shared state for `stage` (clone the `Arc` into the stage task).
    pub fn stage(&self, stage: Stage) -> &Arc<StageState> {
        &self.stages[stage.index()]
    }

    /// Current window size for `stage`.
    pub fn window(&self, stage: Stage) -> u64 {
        self.stages[stage.index()].window()
    }

    /// Current window sizes, indexed by [`Stage::index`].
    pub fn windows(&self) -> [u64; 5] {
        [
            self.stages[0].window(),
            self.stages[1].window(),
            self.stages[2].window(),
            self.stages[3].window(),
            self.stages[4].window(),
        ]
    }

    /// Sum of all window sizes.
    pub fn total_window(&self) -> u64 {
        self.windows().iter().sum()
    }

    /// Run one AIMD adjustment over all stages (downstream → upstream), enforce
    /// the global budget, and return the resulting windows.
    pub fn tick(&self) -> [u64; 5] {
        // Downstream-first (Store → Accept) to avoid head-of-line amplification.
        for stage in Stage::ALL.into_iter().rev() {
            self.adjust_stage(&self.stages[stage.index()]);
        }
        self.enforce_budget();
        self.windows()
    }

    fn adjust_stage(&self, state: &StageState) {
        let window = state.window();
        let occupancy = state.occupancy_ppm.load(Ordering::Acquire);
        let p99 = state.p99_latency_ns.load(Ordering::Acquire);
        let baseline = state.baseline_p99_ns.load(Ordering::Acquire);

        let congested = occupancy > self.config.occupancy_high_ppm
            || (baseline > 0 && p99 > self.config.p99_spike_multiplier.saturating_mul(baseline));

        let new_window = if congested {
            (window * self.config.md_numerator / self.config.md_denominator)
                .max(self.config.min_window)
        } else {
            window
                .saturating_add(self.config.increase_step)
                .min(self.config.max_window)
        };
        state.set_window(new_window);

        // Update the running baseline (EWMA toward the latest p99).
        let next_baseline = if baseline == 0 {
            p99
        } else {
            (baseline.saturating_mul(15).saturating_add(p99)) / 16
        };
        state
            .baseline_p99_ns
            .store(next_baseline, Ordering::Release);
    }

    fn enforce_budget(&self) {
        let total: u64 = self.total_window();
        if total <= self.config.budget_slots {
            return;
        }
        for state in &self.stages {
            let window = state.window();
            let scaled = (window as u128 * self.config.budget_slots as u128 / total as u128) as u64;
            state.set_window(scaled.max(self.config.min_window));
        }
        warn!(
            total,
            budget = self.config.budget_slots,
            "pipeline window budget exceeded; scaling windows down"
        );
    }
}

/// A coarse log-bucketed latency histogram (a dependency-free stand-in for
/// `hdrhistogram`). Buckets are powers of two of nanoseconds.
#[derive(Debug)]
pub struct LatencyHistogram {
    buckets: [u64; 64],
    total: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: [0; 64],
            total: 0,
        }
    }
}

impl LatencyHistogram {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a latency sample in nanoseconds.
    pub fn record(&mut self, ns: u64) {
        let idx = if ns == 0 {
            0
        } else {
            (63 - ns.leading_zeros()) as usize
        };
        self.buckets[idx] += 1;
        self.total += 1;
    }

    /// Number of samples recorded.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Approximate value at quantile `q` (0.0..=1.0), as the lower bound of the
    /// bucket containing it. Returns 0 when empty.
    pub fn value_at_quantile(&self, q: f64) -> u64 {
        if self.total == 0 {
            return 0;
        }
        let target = (q.clamp(0.0, 1.0) * self.total as f64).ceil() as u64;
        let mut cumulative = 0u64;
        for (idx, count) in self.buckets.iter().enumerate() {
            cumulative += count;
            if cumulative >= target {
                return 1u64 << idx;
            }
        }
        1u64 << 63
    }

    /// Reset for the next measurement period.
    pub fn clear(&mut self) {
        self.buckets = [0; 64];
        self.total = 0;
    }
}
