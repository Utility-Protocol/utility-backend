//! Sliding-window, credit-based backpressure for cross-shard settlement.
//!
//! Each destination shard has a [`CreditFlowController`] holding a signed credit
//! balance. A sender consumes one credit per in-flight message; the receiver
//! replenishes credits via `CreditGrant` after processing a batch. This gives
//! unbounded *logical* capacity (messages never silently drop) while bounding
//! *physical* in-flight work, with spillover to a durable queue handled by the
//! [`ShardRouter`](super::shard_router::ShardRouter).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::Notify;

/// Tunable bounds for the credit protocol (issue #54 invariants).
#[derive(Clone, Copy, Debug)]
pub struct CreditConfig {
    /// Starting credit balance granted to each shard.
    pub initial_credit_balance: i64,
    /// Number of messages the receiver processes before emitting a grant; also
    /// the size of each `CreditGrant`.
    pub grant_batch: u32,
    /// In-memory buffered messages per shard before spilling to disk.
    pub in_memory_buffer: usize,
    /// Maximum durable-queue depth per shard before the sender blocks upstream.
    pub durable_queue_max: usize,
    /// How long an unacknowledged message waits before retransmission.
    pub ack_timeout: Duration,
}

impl Default for CreditConfig {
    fn default() -> Self {
        Self {
            initial_credit_balance: 100_000,
            grant_batch: 1_000,
            in_memory_buffer: 200_000,
            durable_queue_max: 10_000_000,
            ack_timeout: Duration::from_secs(1),
        }
    }
}

/// Returned by [`CreditFlowController::acquire`] when no credit is available.
/// The caller should spill to the durable queue or wait for a grant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CreditExhausted;

impl std::fmt::Display for CreditExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "credit balance exhausted")
    }
}

impl std::error::Error for CreditExhausted {}

/// Per-shard credit accounting plus unacknowledged-message tracking.
pub struct CreditFlowController {
    shard_id: u64,
    credits: AtomicI64,
    pending_acks: Mutex<HashMap<u64, Instant>>,
    acked_watermark: AtomicU64,
    notify: Notify,
    config: CreditConfig,
}

impl CreditFlowController {
    /// Create a controller for `shard_id` starting at the configured balance.
    pub fn new(shard_id: u64, config: CreditConfig) -> Self {
        Self {
            shard_id,
            credits: AtomicI64::new(config.initial_credit_balance),
            pending_acks: Mutex::new(HashMap::new()),
            acked_watermark: AtomicU64::new(0),
            notify: Notify::new(),
            config,
        }
    }

    /// The shard this controller governs.
    pub fn shard_id(&self) -> u64 {
        self.shard_id
    }

    /// Current credit balance (may be observed transiently).
    pub fn credit_balance(&self) -> i64 {
        self.credits.load(Ordering::Acquire)
    }

    /// Try to reserve `count` credits without blocking. Returns `true` on
    /// success; never drives the balance negative.
    pub fn try_acquire(&self, count: u32) -> bool {
        let count = count as i64;
        let mut cur = self.credits.load(Ordering::Acquire);
        loop {
            if cur < count {
                return false;
            }
            match self.credits.compare_exchange_weak(
                cur,
                cur - count,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Reserve `count` credits, or report exhaustion so the caller can spill or
    /// wait. Non-blocking.
    pub fn acquire(&self, count: u32) -> Result<(), CreditExhausted> {
        if self.try_acquire(count) {
            Ok(())
        } else {
            Err(CreditExhausted)
        }
    }

    /// Wait until `count` credits can be reserved, parking on grant
    /// notifications. Resolves once the credits have been reserved.
    ///
    /// A bounded re-poll backstops the wait: `Notify::notify_waiters` stores no
    /// permit, so a grant racing ahead of registration could otherwise be
    /// missed. The timeout guarantees liveness at the cost of a small worst-case
    /// latency in that rare race.
    pub async fn wait_for_credits(&self, count: u32) {
        loop {
            if self.try_acquire(count) {
                return;
            }
            let notified = self.notify.notified();
            if self.try_acquire(count) {
                return;
            }
            let _ = tokio::time::timeout(Duration::from_millis(50), notified).await;
        }
    }

    /// Add `delta` credits (on receiving a `CreditGrant`) and wake any waiters.
    pub fn grant_credits(&self, delta: u32) {
        self.credits.fetch_add(delta as i64, Ordering::AcqRel);
        self.notify.notify_waiters();
    }

    /// Record that `msg_id` has been forwarded and is awaiting acknowledgement.
    pub fn record_pending(&self, msg_id: u64) {
        self.pending_acks.lock().insert(msg_id, Instant::now());
    }

    /// Number of messages awaiting acknowledgement.
    pub fn pending_count(&self) -> usize {
        self.pending_acks.lock().len()
    }

    /// Highest contiguous `msg_id` acknowledged by the receiver.
    pub fn acked_watermark(&self) -> u64 {
        self.acked_watermark.load(Ordering::Acquire)
    }

    /// Apply an incoming ack: advance the watermark and drop every pending entry
    /// at or below `acked_msg_id`.
    pub fn process_ack(&self, acked_msg_id: u64) {
        self.acked_watermark
            .fetch_max(acked_msg_id, Ordering::AcqRel);
        let mut pending = self.pending_acks.lock();
        pending.retain(|id, _| *id > acked_msg_id);
    }

    /// Ids of messages that have been pending longer than the ack timeout and
    /// should be retransmitted. Their timers are reset so they are not returned
    /// again until the timeout elapses afresh.
    pub fn timed_out(&self) -> Vec<u64> {
        let now = Instant::now();
        let timeout = self.config.ack_timeout;
        let mut pending = self.pending_acks.lock();
        let stale: Vec<u64> = pending
            .iter()
            .filter(|(_, sent)| now.duration_since(**sent) >= timeout)
            .map(|(id, _)| *id)
            .collect();
        for id in &stale {
            pending.insert(*id, now);
        }
        stale
    }
}
