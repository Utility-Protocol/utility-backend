//! Soroban reorg detection, rollback, and replay orchestration (#41).
//!
//! Soroban can produce short reorgs (1–5 ledgers) under network asynchrony,
//! which can orphan ledgers that local settlement state already depends on. This
//! module tracks `(ledger_seq, ledger_hash)` per settled batch, detects
//! divergence from a trusted archival node, rolls back affected batches on the
//! contract, and replays them under a fresh per-reorg nonce so duplicate mints
//! are rejected.
//!
//! Chain access is abstracted behind [`LedgerOracle`] (canonical chain queries)
//! and [`ReorgContract`] (rollback/replay calls) so the orchestration is unit
//! testable without a live Soroban devnet.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::storage::reorg_log::ReorgLog;

/// Reason code passed to the contract's `rollback_batch`.
pub const REORG_ROLLBACK: u32 = 1;

/// A ledger's canonical identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LedgerInfo {
    pub seq: u64,
    pub hash: [u8; 32],
}

/// Snapshot of a settled batch, retained for rollback/replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchState {
    pub batch_id: u64,
    pub source_meter_ids: Vec<String>,
    pub total_scaled_amount: u128,
    pub ledger_seq: u64,
    pub ledger_hash: [u8; 32],
    pub ledger_close_time: u64,
    pub tx_envelope_hash: [u8; 32],
    pub state_commitment: [u8; 32],
}

impl BatchState {
    /// Serialize to a compact little-endian byte form (the durable-store value,
    /// standing in for the protobuf `BatchState`).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.batch_id.to_le_bytes());
        buf.extend_from_slice(&(self.source_meter_ids.len() as u32).to_le_bytes());
        for id in &self.source_meter_ids {
            buf.extend_from_slice(&(id.len() as u32).to_le_bytes());
            buf.extend_from_slice(id.as_bytes());
        }
        buf.extend_from_slice(&self.total_scaled_amount.to_le_bytes());
        buf.extend_from_slice(&self.ledger_seq.to_le_bytes());
        buf.extend_from_slice(&self.ledger_hash);
        buf.extend_from_slice(&self.ledger_close_time.to_le_bytes());
        buf.extend_from_slice(&self.tx_envelope_hash);
        buf.extend_from_slice(&self.state_commitment);
        buf
    }

    /// Parse a value produced by [`Self::to_bytes`]; `None` on truncation.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let mut pos = 0usize;
        let take = |bytes: &[u8], pos: &mut usize, n: usize| -> Option<Vec<u8>> {
            let slice = bytes.get(*pos..*pos + n)?.to_vec();
            *pos += n;
            Some(slice)
        };
        let batch_id = u64::from_le_bytes(take(bytes, &mut pos, 8)?.try_into().ok()?);
        let count = u32::from_le_bytes(take(bytes, &mut pos, 4)?.try_into().ok()?) as usize;
        let mut source_meter_ids = Vec::with_capacity(count);
        for _ in 0..count {
            let len = u32::from_le_bytes(take(bytes, &mut pos, 4)?.try_into().ok()?) as usize;
            let raw = take(bytes, &mut pos, len)?;
            source_meter_ids.push(String::from_utf8(raw).ok()?);
        }
        let total_scaled_amount = u128::from_le_bytes(take(bytes, &mut pos, 16)?.try_into().ok()?);
        let ledger_seq = u64::from_le_bytes(take(bytes, &mut pos, 8)?.try_into().ok()?);
        let ledger_hash = take(bytes, &mut pos, 32)?.try_into().ok()?;
        let ledger_close_time = u64::from_le_bytes(take(bytes, &mut pos, 8)?.try_into().ok()?);
        let tx_envelope_hash = take(bytes, &mut pos, 32)?.try_into().ok()?;
        let state_commitment = take(bytes, &mut pos, 32)?.try_into().ok()?;
        Some(Self {
            batch_id,
            source_meter_ids,
            total_scaled_amount,
            ledger_seq,
            ledger_hash,
            ledger_close_time,
            tx_envelope_hash,
            state_commitment,
        })
    }
}

/// A detected reorg and the batches it affects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LedgerReorg {
    /// First ledger sequence where the local chain diverges from canonical.
    pub divergence_seq: u64,
    /// Number of ledgers from the divergence point to the local tip.
    pub depth: usize,
    /// Batch ids settled at or after `divergence_seq`.
    pub affected_batches: Vec<u64>,
    /// Whether the depth exceeds the auto-recovery limit (operator action).
    pub requires_manual: bool,
}

/// Result of a completed recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryOutcome {
    pub epoch: u64,
    pub rolled_back: usize,
    pub replayed: usize,
}

/// Tunables for reorg handling (issue #41 bounds).
#[derive(Clone, Copy, Debug)]
pub struct ReorgConfig {
    pub poll_interval: Duration,
    /// How many recent batches/ledgers to compare against canonical.
    pub compare_window: usize,
    /// Consecutive diverging ledgers required to declare a reorg.
    pub depth_threshold: usize,
    /// Maximum depth recoverable automatically; deeper escalates to an operator.
    pub max_auto_depth: usize,
    /// Count-based retention for the reorg log.
    pub retention_batches: usize,
    /// Age-based retention (seconds).
    pub retention_secs: u64,
    /// Max settlements buffered while a recovery is in progress.
    pub queue_capacity: usize,
}

impl Default for ReorgConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(30),
            compare_window: 100,
            depth_threshold: 2,
            max_auto_depth: 10,
            retention_batches: 1000,
            retention_secs: 24 * 60 * 60,
            queue_capacity: 10_000,
        }
    }
}

/// Canonical-chain queries against a trusted archival node.
#[async_trait]
pub trait LedgerOracle: Send + Sync {
    /// Canonical `(seq, hash)` for the inclusive range `[start, end]`.
    async fn get_ledger_range(&self, start: u64, end: u64) -> Result<Vec<LedgerInfo>, String>;

    /// Whether `tx_hash` is included in the canonical ledger `ledger_seq`.
    async fn is_tx_in_ledger(&self, tx_hash: [u8; 32], ledger_seq: u64) -> Result<bool, String>;
}

/// Contract operations used during recovery.
#[async_trait]
pub trait ReorgContract: Send + Sync {
    /// Roll back `batch_id` on the contract with the given reason code.
    async fn rollback_batch(&self, batch_id: u64, reason: u32) -> Result<(), String>;

    /// Re-submit `batch_id` under a fresh `nonce`. The contract must reject
    /// duplicate nonces.
    async fn submit_replay(
        &self,
        batch_id: u64,
        nonce: u64,
        meter_ids: &[String],
    ) -> Result<(), String>;
}

/// Orchestrates reorg detection and recovery over a [`ReorgLog`].
pub struct ReorgHandler {
    oracle: Arc<dyn LedgerOracle>,
    contract: Arc<dyn ReorgContract>,
    log: Arc<ReorgLog>,
    config: ReorgConfig,
    reorg_epoch: AtomicU64,
    reorg_in_progress: AtomicBool,
    queue: Mutex<VecDeque<BatchState>>,
    used_nonces: Mutex<HashSet<u64>>,
}

impl ReorgHandler {
    /// Build a handler over the given oracle, contract, and log.
    pub fn new(
        oracle: Arc<dyn LedgerOracle>,
        contract: Arc<dyn ReorgContract>,
        log: Arc<ReorgLog>,
        config: ReorgConfig,
    ) -> Self {
        Self {
            oracle,
            contract,
            log,
            config,
            reorg_epoch: AtomicU64::new(0),
            reorg_in_progress: AtomicBool::new(false),
            queue: Mutex::new(VecDeque::new()),
            used_nonces: Mutex::new(HashSet::new()),
        }
    }

    /// The shared reorg log.
    pub fn log(&self) -> &Arc<ReorgLog> {
        &self.log
    }

    /// Whether a recovery is currently in progress.
    pub fn is_recovering(&self) -> bool {
        self.reorg_in_progress.load(Ordering::Acquire)
    }

    /// Number of settlements buffered awaiting recovery completion.
    pub fn queued_len(&self) -> usize {
        self.queue.lock().len()
    }

    /// Current reorg epoch (incremented once per recovery).
    pub fn epoch(&self) -> u64 {
        self.reorg_epoch.load(Ordering::Acquire)
    }

    /// Record a freshly settled batch. While a recovery is in progress the batch
    /// is buffered (up to the queue capacity) instead of being logged. Returns
    /// `Ok(true)` if it was queued, `Ok(false)` if it was logged.
    pub fn record_settled_batch(&self, batch: BatchState) -> Result<bool, String> {
        if self.reorg_in_progress.load(Ordering::Acquire) {
            let mut q = self.queue.lock();
            if q.len() >= self.config.queue_capacity {
                return Err("recovery queue is full".to_string());
            }
            q.push_back(batch);
            Ok(true)
        } else {
            self.log.put(batch);
            Ok(false)
        }
    }

    /// Nonce for replaying `batch_id` in `epoch`: unique per (epoch, batch_id).
    fn replay_nonce(epoch: u64, batch_id: u64) -> u64 {
        (epoch << 32) | (batch_id & 0xFFFF_FFFF)
    }

    /// Compare the most-recent cached ledgers against canonical and report a
    /// reorg if at least `depth_threshold` consecutive ledgers diverge.
    pub async fn detect_reorg(&self) -> Result<Option<LedgerReorg>, String> {
        let cached = self.log.recent(self.config.compare_window);
        if cached.is_empty() {
            return Ok(None);
        }
        let min_seq = cached.iter().map(|b| b.ledger_seq).min().unwrap_or(0);
        let max_seq = cached.iter().map(|b| b.ledger_seq).max().unwrap_or(0);
        let canonical = self.oracle.get_ledger_range(min_seq, max_seq).await?;

        let mut canon = std::collections::HashMap::new();
        for l in canonical {
            canon.insert(l.seq, l.hash);
        }

        // Cached hash per ledger sequence, ascending.
        let mut cached_hash = std::collections::BTreeMap::new();
        for b in &cached {
            cached_hash.insert(b.ledger_seq, b.ledger_hash);
        }

        let mut run_start: Option<u64> = None;
        let mut run_len = 0usize;
        let mut found = None;
        for (&seq, &chash) in &cached_hash {
            let diverges = canon.get(&seq).is_some_and(|c| *c != chash);
            if diverges {
                if run_start.is_none() {
                    run_start = Some(seq);
                    run_len = 0;
                }
                run_len += 1;
                if run_len >= self.config.depth_threshold {
                    found = run_start;
                    break;
                }
            } else {
                run_start = None;
                run_len = 0;
            }
        }

        let Some(divergence_seq) = found else {
            return Ok(None);
        };
        let depth = (max_seq - divergence_seq + 1) as usize;
        let mut affected_batches: Vec<u64> = cached
            .iter()
            .filter(|b| b.ledger_seq >= divergence_seq)
            .map(|b| b.batch_id)
            .collect();
        affected_batches.sort_unstable();
        Ok(Some(LedgerReorg {
            divergence_seq,
            depth,
            affected_batches,
            requires_manual: depth > self.config.max_auto_depth,
        }))
    }

    /// Roll back and replay all batches affected by `reorg`. Errors (without
    /// mutating state) when the reorg requires manual intervention.
    pub async fn recover(&self, reorg: &LedgerReorg) -> Result<RecoveryOutcome, String> {
        if reorg.requires_manual {
            return Err(format!(
                "reorg depth {} exceeds auto-recovery limit {}; operator action required",
                reorg.depth, self.config.max_auto_depth
            ));
        }

        self.reorg_in_progress.store(true, Ordering::Release);
        let epoch = self.reorg_epoch.fetch_add(1, Ordering::AcqRel) + 1;

        let affected = self.log.batches_from_seq(reorg.divergence_seq);

        let mut rolled_back = 0;
        for batch in &affected {
            self.contract
                .rollback_batch(batch.batch_id, REORG_ROLLBACK)
                .await?;
            self.log.remove(batch.ledger_seq, batch.tx_envelope_hash);
            rolled_back += 1;
        }

        let mut replayed = 0;
        for batch in &affected {
            let nonce = Self::replay_nonce(epoch, batch.batch_id);
            if !self.used_nonces.lock().insert(nonce) {
                self.reorg_in_progress.store(false, Ordering::Release);
                return Err(format!("replay nonce {nonce} already used"));
            }
            self.contract
                .submit_replay(batch.batch_id, nonce, &batch.source_meter_ids)
                .await?;
            replayed += 1;
        }

        // Flush settlements buffered during recovery.
        let queued: Vec<BatchState> = self.queue.lock().drain(..).collect();
        for batch in queued {
            self.log.put(batch);
        }
        self.reorg_in_progress.store(false, Ordering::Release);

        Ok(RecoveryOutcome {
            epoch,
            rolled_back,
            replayed,
        })
    }

    /// Prune reorg-log entries older than the configured age relative to `now`
    /// (Unix seconds). Count-based retention is enforced continuously on insert.
    pub fn prune(&self, now: u64) -> usize {
        self.log.prune_older_than(now, self.config.retention_secs)
    }
}
