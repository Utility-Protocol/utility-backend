//! Workload priority classes and priority-inheritance bookkeeping for the
//! partitioned connection pool (issue #52).

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use dashmap::DashMap;

/// Workload priority class. Lower discriminant == higher scheduling priority, so
/// the derived `Ord` makes `Critical < High < Normal < Low`.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    /// Settlement finalization, Soroban calls.
    Critical = 0,
    /// Tariff evaluation, watermark persistence.
    High = 1,
    /// Telemetry ingestion writes.
    Normal = 2,
    /// Admin queries, reporting, debugging.
    Low = 3,
}

impl Priority {
    /// All classes from highest to lowest priority.
    pub const ALL: [Priority; 4] = [
        Priority::Critical,
        Priority::High,
        Priority::Normal,
        Priority::Low,
    ];

    /// Index into per-class arrays (`Critical` = 0 ... `Low` = 3).
    pub fn index(self) -> usize {
        self as usize
    }

    /// Reconstruct a priority from its discriminant.
    pub fn from_u8(value: u8) -> Option<Priority> {
        match value {
            0 => Some(Priority::Critical),
            1 => Some(Priority::High),
            2 => Some(Priority::Normal),
            3 => Some(Priority::Low),
            _ => None,
        }
    }

    /// The next lower-priority class, if any (`Critical` -> `High` -> `Normal`
    /// -> `Low` -> `None`). Used when stealing/borrowing capacity downward.
    pub fn lower(self) -> Option<Priority> {
        Priority::from_u8(self as u8 + 1)
    }

    /// Whether `self` outranks `other`.
    pub fn is_higher_than(self, other: Priority) -> bool {
        self < other
    }

    /// Stable label for Prometheus `class` dimensions.
    pub fn as_str(self) -> &'static str {
        match self {
            Priority::Critical => "critical",
            Priority::High => "high",
            Priority::Normal => "normal",
            Priority::Low => "low",
        }
    }
}

/// Tracks the *effective* priority of each in-use connection so a higher-priority
/// waiter can elevate the holder of a connection it needs (priority inheritance).
#[derive(Default)]
pub struct ConnectionRegistry {
    holders: DashMap<u64, Arc<AtomicU8>>,
}

impl ConnectionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `conn_id` is held by a task of base priority `base`, returning
    /// the shared cell so the holder's effective priority can be elevated later.
    pub fn register(&self, conn_id: u64, base: Priority) -> Arc<AtomicU8> {
        let cell = Arc::new(AtomicU8::new(base as u8));
        self.holders.insert(conn_id, cell.clone());
        cell
    }

    /// Stop tracking `conn_id` (the connection was released).
    pub fn unregister(&self, conn_id: u64) {
        self.holders.remove(&conn_id);
    }

    /// Current effective priority of `conn_id`, if still held.
    pub fn effective_priority(&self, conn_id: u64) -> Option<Priority> {
        self.holders
            .get(&conn_id)
            .and_then(|cell| Priority::from_u8(cell.load(Ordering::Acquire)))
    }

    /// Shared effective-priority cell for `conn_id`, if still held.
    pub fn cell(&self, conn_id: u64) -> Option<Arc<AtomicU8>> {
        self.holders.get(&conn_id).map(|c| c.value().clone())
    }

    /// Number of connections currently tracked as in-use.
    pub fn active_len(&self) -> usize {
        self.holders.len()
    }
}

/// RAII guard that elevates a connection holder's effective priority for the
/// duration of a higher-priority task's wait, restoring it on drop. The cell
/// holds the *highest* priority (smallest discriminant) among the base holder
/// and any current inheritors.
pub struct PriorityInheritanceGuard {
    cell: Arc<AtomicU8>,
    previous: u8,
}

impl PriorityInheritanceGuard {
    /// Elevate `cell` to at least `inheriting` (a no-op if it is already at least
    /// that high) and remember the prior value for restoration.
    pub fn new(cell: Arc<AtomicU8>, inheriting: Priority) -> Self {
        let previous = cell.fetch_min(inheriting as u8, Ordering::AcqRel);
        Self { cell, previous }
    }

    /// The effective priority currently recorded in the guarded cell.
    pub fn effective(&self) -> Option<Priority> {
        Priority::from_u8(self.cell.load(Ordering::Acquire))
    }
}

impl Drop for PriorityInheritanceGuard {
    fn drop(&mut self) {
        // Best-effort restoration of the holder's pre-inheritance priority.
        self.cell.store(self.previous, Ordering::Release);
    }
}
