//! Reorg log: durable record of settled batch state for rollback/replay (#41).
//!
//! The blueprint specifies a RocksDB column family keyed by `ledger_seq` with
//! prefix seeks. To keep the release build free of a `librocksdb-sys` C++
//! toolchain (which `rust:slim` does not carry), this is a portable, ordered
//! in-memory store with the same access shape: a `BTreeMap` keyed by
//! `(ledger_seq, tx_envelope_hash)` gives ordered prefix seeks by ledger
//! sequence, and an insertion-order ring enforces the count-based retention
//! window. A RocksDB backend can replace this behind the same API later.

use std::collections::{BTreeMap, VecDeque};

use parking_lot::Mutex;

use crate::blockchain::soroban::reorg_handler::BatchState;

type Key = (u64, [u8; 32]);

struct LogInner {
    entries: BTreeMap<Key, BatchState>,
    order: VecDeque<Key>,
}

/// Ordered, bounded log of settled batch state.
pub struct ReorgLog {
    inner: Mutex<LogInner>,
    retention_batches: usize,
}

impl ReorgLog {
    /// Create a log retaining at most `retention_batches` most-recent entries.
    pub fn new(retention_batches: usize) -> Self {
        Self {
            inner: Mutex::new(LogInner {
                entries: BTreeMap::new(),
                order: VecDeque::new(),
            }),
            retention_batches,
        }
    }

    /// Record (or overwrite) a settled batch, evicting the oldest entries beyond
    /// the retention window.
    pub fn put(&self, batch: BatchState) {
        let key = (batch.ledger_seq, batch.tx_envelope_hash);
        let mut g = self.inner.lock();
        if g.entries.insert(key, batch).is_none() {
            g.order.push_back(key);
        }
        while g.order.len() > self.retention_batches {
            if let Some(old) = g.order.pop_front() {
                g.entries.remove(&old);
            }
        }
    }

    /// All batches with `ledger_seq >= seq`, ascending (prefix seek).
    pub fn batches_from_seq(&self, seq: u64) -> Vec<BatchState> {
        let g = self.inner.lock();
        g.entries
            .range((seq, [0u8; 32])..)
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// The `n` most recently recorded batches, newest first.
    pub fn recent(&self, n: usize) -> Vec<BatchState> {
        let g = self.inner.lock();
        g.order
            .iter()
            .rev()
            .take(n)
            .filter_map(|k| g.entries.get(k).cloned())
            .collect()
    }

    /// Remove a single batch by its key, returning it if present.
    pub fn remove(&self, ledger_seq: u64, tx_envelope_hash: [u8; 32]) -> Option<BatchState> {
        let key = (ledger_seq, tx_envelope_hash);
        let mut g = self.inner.lock();
        let removed = g.entries.remove(&key);
        if removed.is_some() {
            g.order.retain(|k| *k != key);
        }
        removed
    }

    /// Drop entries whose `ledger_close_time` is older than `max_age` relative to
    /// `now` (both Unix seconds). Returns the number pruned.
    pub fn prune_older_than(&self, now: u64, max_age: u64) -> usize {
        let mut g = self.inner.lock();
        let stale: Vec<Key> = g
            .entries
            .iter()
            .filter(|(_, v)| now.saturating_sub(v.ledger_close_time) > max_age)
            .map(|(k, _)| *k)
            .collect();
        for k in &stale {
            g.entries.remove(k);
        }
        g.order.retain(|k| !stale.contains(k));
        stale.len()
    }

    /// Number of retained batches.
    pub fn len(&self) -> usize {
        self.inner.lock().entries.len()
    }

    /// Whether the log is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().entries.is_empty()
    }
}
