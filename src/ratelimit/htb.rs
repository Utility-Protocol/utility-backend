//! Hierarchical Token Bucket (HTB) scheduler for grid capacity allocation (#48).
//!
//! Models the grid as a tree of rate-limited buckets (root → region →
//! substation → feeder → meter). A request at a leaf debits tokens from the leaf
//! **and every ancestor**, so the sum of children can never exceed a parent's
//! capacity. A node may go negative down to `-burst` (bursting / borrowing);
//! beyond that the request is rejected and the whole path is rolled back.
//!
//! Time is injected explicitly (`*_at(now_ns)`) so behaviour is deterministic
//! and testable; a production wrapper supplies the wall clock. Token unit: one
//! token = 1 µWh.

use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::{DashMap, DashSet};
use parking_lot::RwLock;

/// Sentinel stored in [`HtbNode::parent`] for a root node.
pub const NO_PARENT: u32 = u32::MAX;

/// Defensive cap on parent-chain traversal (the tree is ≤ 5 deep; this guards
/// against accidental cycles).
const MAX_PATH: usize = 64;

const NANOS_PER_SEC: i128 = 1_000_000_000;
const SECONDS_PER_YEAR: f64 = 365.0 * 24.0 * 60.0 * 60.0;

/// Per-node configuration.
#[derive(Clone, Copy, Debug)]
pub struct NodeConfig {
    /// Sustained refill rate in tokens/second.
    pub rate: u64,
    /// Bucket capacity / burst allowance in tokens.
    pub burst: u64,
    /// Absolute maximum rate (≥ `rate`); headroom for borrowing.
    pub ceil: u64,
    /// Parent node id, or `None` for the root.
    pub parent: Option<u32>,
}

/// A single bucket in the hierarchy.
pub struct HtbNode {
    id: u32,
    parent: AtomicU32,
    rate: AtomicU64,
    ceil: AtomicU64,
    burst: AtomicU64,
    tokens: AtomicI64,
    last_refill_ns: AtomicI64,
    debt: AtomicI64,
    children: DashSet<u32>,
}

impl HtbNode {
    pub fn id(&self) -> u32 {
        self.id
    }
    pub fn parent(&self) -> Option<u32> {
        match self.parent.load(Ordering::Acquire) {
            NO_PARENT => None,
            p => Some(p),
        }
    }
    pub fn rate(&self) -> u64 {
        self.rate.load(Ordering::Acquire)
    }
    pub fn ceil(&self) -> u64 {
        self.ceil.load(Ordering::Acquire)
    }
    pub fn burst(&self) -> u64 {
        self.burst.load(Ordering::Acquire)
    }
    pub fn tokens(&self) -> i64 {
        self.tokens.load(Ordering::Acquire)
    }
    /// Currently borrowed amount (negative balance), or 0 if in credit.
    pub fn debt(&self) -> i64 {
        self.debt.load(Ordering::Acquire)
    }
    pub fn child_ids(&self) -> Vec<u32> {
        self.children.iter().map(|c| *c).collect()
    }

    /// Live reconfiguration (used by the admin endpoints).
    pub fn set_rate(&self, rate: u64) {
        self.rate.store(rate, Ordering::Release);
    }
    pub fn set_ceil(&self, ceil: u64) {
        self.ceil.store(ceil, Ordering::Release);
    }
    pub fn set_burst(&self, burst: u64) {
        self.burst.store(burst, Ordering::Release);
    }
}

/// The hierarchical token-bucket tree.
pub struct HtbTree {
    nodes: DashMap<u32, Arc<HtbNode>>,
    root: RwLock<Option<u32>>,
    interest_per_sec: f64,
}

impl HtbTree {
    /// Create an empty tree with the given debt interest rate (annual, e.g. 0.10
    /// for 10% APR).
    pub fn new(interest_apr: f64) -> Self {
        Self {
            nodes: DashMap::new(),
            root: RwLock::new(None),
            interest_per_sec: interest_apr / SECONDS_PER_YEAR,
        }
    }

    /// Insert a node, linking it under its parent (or marking it the root).
    /// `start_ns` seeds the refill clock so the first refill measures from here.
    pub fn add_node(&self, id: u32, config: NodeConfig, start_ns: i64) {
        let node = Arc::new(HtbNode {
            id,
            parent: AtomicU32::new(config.parent.unwrap_or(NO_PARENT)),
            rate: AtomicU64::new(config.rate),
            ceil: AtomicU64::new(config.ceil),
            burst: AtomicU64::new(config.burst),
            tokens: AtomicI64::new(config.burst as i64),
            last_refill_ns: AtomicI64::new(start_ns),
            debt: AtomicI64::new(0),
            children: DashSet::new(),
        });
        self.nodes.insert(id, node);
        match config.parent {
            Some(parent_id) => {
                if let Some(parent) = self.nodes.get(&parent_id) {
                    parent.children.insert(id);
                }
            }
            None => *self.root.write() = Some(id),
        }
    }

    /// Look up a node by id.
    pub fn node(&self, id: u32) -> Option<Arc<HtbNode>> {
        self.nodes.get(&id).map(|n| n.value().clone())
    }

    /// The root node id, if set.
    pub fn root(&self) -> Option<u32> {
        *self.root.read()
    }

    /// Number of nodes in the tree.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the tree has no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The leaf→root path of nodes, or `None` if `leaf_id` is unknown or the
    /// chain exceeds [`MAX_PATH`] (cycle guard).
    fn path_to_root(&self, leaf_id: u32) -> Option<Vec<Arc<HtbNode>>> {
        let mut path = Vec::new();
        let mut cur = leaf_id;
        loop {
            let node = self.nodes.get(&cur)?.value().clone();
            let parent = node.parent.load(Ordering::Acquire);
            path.push(node);
            if path.len() > MAX_PATH {
                return None;
            }
            if parent == NO_PARENT {
                return Some(path);
            }
            cur = parent;
        }
    }

    /// Refill a single node up to `now_ns`. Claims the refill window atomically
    /// (only one caller advances the clock) and tops up tokens via a CAS loop so
    /// concurrent conformance deductions are not lost.
    fn refill_node(&self, node: &HtbNode, now_ns: i64) {
        let last = node.last_refill_ns.load(Ordering::Acquire);
        if now_ns <= last {
            return;
        }
        if node
            .last_refill_ns
            .compare_exchange(last, now_ns, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return; // another caller refilled this window
        }

        let elapsed_ns = (now_ns - last) as i128;
        let rate = node.rate.load(Ordering::Acquire) as i128;
        let mut add = (rate * elapsed_ns / NANOS_PER_SEC) as i64;

        // Debt accrues interest while a node is negative, slightly slowing
        // recovery (10% APR is negligible per tick, so refill still converges).
        let current = node.tokens.load(Ordering::Acquire);
        if current < 0 {
            let elapsed_sec = elapsed_ns as f64 / NANOS_PER_SEC as f64;
            let interest = ((-current) as f64 * self.interest_per_sec * elapsed_sec) as i64;
            add -= interest;
        }

        let burst = node.burst.load(Ordering::Acquire) as i64;
        let mut cur = node.tokens.load(Ordering::Acquire);
        loop {
            let new = (cur + add).min(burst);
            match node
                .tokens
                .compare_exchange_weak(cur, new, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    node.debt.store((-new).max(0), Ordering::Release);
                    return;
                }
                Err(actual) => cur = actual,
            }
        }
    }

    /// Refill every node up to `now_ns` (the periodic O(n) tick).
    pub fn refill_at(&self, now_ns: i64) {
        for entry in self.nodes.iter() {
            self.refill_node(entry.value(), now_ns);
        }
    }

    /// Check and reserve `requested` tokens for `leaf_id` at `now_ns`.
    ///
    /// Lazily refills the leaf→root path, then debits `requested` from every node
    /// on it. If any node would drop below `-burst` the whole path is rolled back
    /// and `false` is returned (the request is non-conforming).
    pub fn conform_at(&self, leaf_id: u32, requested: u64, now_ns: i64) -> bool {
        let path = match self.path_to_root(leaf_id) {
            Some(p) => p,
            None => return false,
        };
        for node in &path {
            self.refill_node(node, now_ns);
        }

        let req = requested as i64;
        let mut deducted = 0usize;
        for node in &path {
            let after = node.tokens.fetch_sub(req, Ordering::AcqRel) - req;
            deducted += 1;
            let floor = -(node.burst.load(Ordering::Acquire) as i64);
            if after < floor {
                // Roll back every node debited so far (this one included).
                for n in &path[..deducted] {
                    n.tokens.fetch_add(req, Ordering::AcqRel);
                }
                return false;
            }
        }

        // Record borrowing (negative balance) as debt.
        for node in &path {
            let t = node.tokens.load(Ordering::Acquire);
            node.debt.store((-t).max(0), Ordering::Release);
        }
        true
    }
}
