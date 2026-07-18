//! Workload-priority-partitioned connection pool with starvation prevention
//! (issue #52).
//!
//! Capacity is modelled with semaphores so the allocation logic is exercisable
//! without a live database (the production wrapper in [`super::pool`] pairs each
//! granted slot with a real `deadpool` connection checkout):
//!
//! * a per-class **reserved** semaphore of `min` permits guarantees each class
//!   its floor even under global contention;
//! * a shared **floating** semaphore of `total - sum(min)` permits is competed
//!   for by usage beyond each class's reservation;
//! * a per-class **cap** semaphore of `max` permits bounds per-class concurrency.
//!
//! Reserved + floating == `total`, so the global connection budget is never
//! exceeded, while each class is guaranteed `min` and limited to `max`.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::priority::{ConnectionRegistry, Priority, PriorityInheritanceGuard};
use crate::api::metrics;

/// Per-class minimum/maximum connection bounds.
#[derive(Clone, Copy, Debug)]
pub struct ClassBounds {
    pub min: usize,
    pub max: usize,
}

/// Configuration for a [`PartitionedPool`].
#[derive(Clone, Debug)]
pub struct PartitionConfig {
    /// Total connection budget shared across all classes.
    pub total: usize,
    /// Per-class bounds, indexed by [`Priority::index`].
    pub bounds: [ClassBounds; 4],
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            total: 32,
            bounds: [
                ClassBounds { min: 2, max: 8 },  // Critical
                ClassBounds { min: 4, max: 16 }, // High
                ClassBounds { min: 8, max: 24 }, // Normal
                ClassBounds { min: 2, max: 8 },  // Low
            ],
        }
    }
}

impl PartitionConfig {
    /// Bounds for `priority`.
    pub fn bounds(&self, priority: Priority) -> ClassBounds {
        self.bounds[priority.index()]
    }

    /// Sum of per-class minimums.
    pub fn sum_min(&self) -> usize {
        self.bounds.iter().map(|b| b.min).sum()
    }

    /// Validate the invariants: `sum(min) <= total <= 128` and `min <= max`.
    pub fn validate(&self) -> Result<(), String> {
        if self.total > 128 {
            return Err(format!("total {} exceeds hard cap 128", self.total));
        }
        for (i, b) in self.bounds.iter().enumerate() {
            if b.min > b.max {
                return Err(format!("class {i}: min {} > max {}", b.min, b.max));
            }
        }
        let sum_min = self.sum_min();
        if sum_min > self.total {
            return Err(format!(
                "sum of mins {sum_min} exceeds total {}",
                self.total
            ));
        }
        Ok(())
    }
}

/// Error returned when a connection slot cannot be obtained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolError {
    /// The class (and its lower-priority donors) had no capacity within the
    /// timeout.
    Exhausted {
        class: Priority,
        waited_ms: u64,
        active: usize,
    },
    /// The pool was closed.
    Closed,
}

impl std::fmt::Display for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolError::Exhausted {
                class,
                waited_ms,
                active,
            } => write!(
                f,
                "pool exhausted for class {} after {waited_ms}ms ({active} active)",
                class.as_str()
            ),
            PoolError::Closed => write!(f, "pool closed"),
        }
    }
}

impl std::error::Error for PoolError {}

const STARVATION_WAIT_MS: u64 = 500;
const STARVATION_CONSECUTIVE: u32 = 3;
const BORROW_MAX: u32 = 2;
const BORROW_TTL: Duration = Duration::from_secs(30);

/// Live state for a single priority class.
struct ClassState {
    /// Bounds per-class concurrency at `max`.
    cap: Arc<Semaphore>,
    /// Guarantees the class its `min` reserved slots.
    reserved: Arc<Semaphore>,
    active: AtomicU32,
    /// Current effective `max` (mutated by rebalancing borrows).
    current_max: AtomicU32,
    /// Whether a starving wait was observed since the last rebalance tick.
    slow: AtomicBool,
    consecutive_slow: AtomicU32,
}

/// A record of capacity temporarily borrowed from a donor class.
#[derive(Clone, Copy)]
struct Borrow {
    donor: Priority,
    recipient: Priority,
    amount: u32,
    started: Instant,
}

/// The kind of resource permit backing a granted slot. The inner permits are
/// held purely for RAII release on drop.
#[allow(dead_code)]
enum ResourcePermit {
    /// One of the class's own reserved slots.
    Reserved(OwnedSemaphorePermit),
    /// A shared floating slot.
    Floating(OwnedSemaphorePermit),
    /// A lower-priority class's reserved slot, taken via priority inheritance.
    Stolen(OwnedSemaphorePermit),
}

/// A granted connection slot. Releasing it (drop) returns all underlying permits
/// and updates metrics/registry.
pub struct PoolPermit {
    /// Held for RAII: releases the per-class cap permit on drop.
    #[allow(dead_code)]
    _cap: OwnedSemaphorePermit,
    _resource: ResourcePermit,
    class: Arc<ClassState>,
    slot_class: Priority,
    effective: Priority,
    conn_id: u64,
    registry: Arc<ConnectionRegistry>,
}

impl PoolPermit {
    /// Unique id of the underlying connection slot.
    pub fn connection_id(&self) -> u64 {
        self.conn_id
    }

    /// The class whose capacity backs this slot.
    pub fn slot_class(&self) -> Priority {
        self.slot_class
    }

    /// The effective priority of the task holding this slot.
    pub fn effective_priority(&self) -> Priority {
        self.effective
    }

    /// Whether this slot was obtained by stealing a lower-priority reservation.
    pub fn is_inherited(&self) -> bool {
        matches!(self._resource, ResourcePermit::Stolen(_))
    }
}

impl Drop for PoolPermit {
    fn drop(&mut self) {
        let active = self
            .class
            .active
            .fetch_sub(1, Ordering::AcqRel)
            .saturating_sub(1);
        self.registry.unregister(self.conn_id);
        metrics::set_pool_active(self.slot_class.as_str(), active as f64);
        let max = self.class.current_max.load(Ordering::Acquire);
        metrics::set_pool_idle(self.slot_class.as_str(), max.saturating_sub(active) as f64);
    }
}

/// Connection pool partitioned by workload priority.
pub struct PartitionedPool {
    classes: [Arc<ClassState>; 4],
    floating: Arc<Semaphore>,
    config: PartitionConfig,
    registry: Arc<ConnectionRegistry>,
    next_conn_id: AtomicU64,
    borrows: Mutex<Vec<Borrow>>,
}

impl PartitionedPool {
    /// Build a pool from `config`, validating its invariants.
    pub fn new(config: PartitionConfig) -> Result<Self, String> {
        config.validate()?;
        let make_class = |b: ClassBounds| {
            Arc::new(ClassState {
                cap: Arc::new(Semaphore::new(b.max)),
                reserved: Arc::new(Semaphore::new(b.min)),
                active: AtomicU32::new(0),
                current_max: AtomicU32::new(b.max as u32),
                slow: AtomicBool::new(false),
                consecutive_slow: AtomicU32::new(0),
            })
        };
        let classes = [
            make_class(config.bounds[0]),
            make_class(config.bounds[1]),
            make_class(config.bounds[2]),
            make_class(config.bounds[3]),
        ];
        let floating = Arc::new(Semaphore::new(config.total - config.sum_min()));
        Ok(Self {
            classes,
            floating,
            config,
            registry: Arc::new(ConnectionRegistry::new()),
            next_conn_id: AtomicU64::new(1),
            borrows: Mutex::new(Vec::new()),
        })
    }

    /// The connection registry tracking effective priorities.
    pub fn registry(&self) -> &Arc<ConnectionRegistry> {
        &self.registry
    }

    /// Number of in-use slots for `priority`.
    pub fn active(&self, priority: Priority) -> usize {
        self.classes[priority.index()]
            .active
            .load(Ordering::Acquire) as usize
    }

    /// Floating slots currently available.
    pub fn floating_available(&self) -> usize {
        self.floating.available_permits()
    }

    /// Current effective `max` for `priority` (reflects rebalancing borrows).
    pub fn current_max(&self, priority: Priority) -> usize {
        self.classes[priority.index()]
            .current_max
            .load(Ordering::Acquire) as usize
    }

    /// Acquire a connection slot for `priority`, waiting up to `timeout`.
    ///
    /// Tries the class's reservation first, then the shared floating pool, then
    /// (priority inheritance) a lower-priority class's reservation, before
    /// reporting [`PoolError::Exhausted`].
    pub async fn get(
        &self,
        priority: Priority,
        timeout: Duration,
    ) -> Result<PoolPermit, PoolError> {
        let start = Instant::now();
        let class = &self.classes[priority.index()];

        // 1. Per-class concurrency cap.
        let cap_permit =
            match tokio::time::timeout(timeout, class.cap.clone().acquire_owned()).await {
                Ok(Ok(p)) => p,
                Ok(Err(_)) => return Err(PoolError::Closed),
                Err(_) => {
                    self.mark_starved(priority);
                    return Err(self.exhausted(priority, start));
                }
            };

        // 2. Resource: reserved -> floating -> stolen.
        let resource = if let Ok(p) = class.reserved.clone().try_acquire_owned() {
            ResourcePermit::Reserved(p)
        } else {
            let remaining = timeout.saturating_sub(start.elapsed());
            match tokio::time::timeout(remaining, self.floating.clone().acquire_owned()).await {
                Ok(Ok(p)) => ResourcePermit::Floating(p),
                Ok(Err(_)) => return Err(PoolError::Closed),
                Err(_) => match self.try_steal_resource(priority) {
                    Some(r) => r,
                    None => {
                        self.mark_starved(priority);
                        return Err(self.exhausted(priority, start));
                    }
                },
            }
        };

        let effective = priority;
        self.record_wait(priority, start.elapsed());
        Ok(self.make_permit(priority, effective, cap_permit, resource))
    }

    /// Non-blocking acquire for `priority`; `None` if no slot is immediately
    /// available (reserved or floating, no stealing).
    pub fn try_get(&self, priority: Priority) -> Option<PoolPermit> {
        let class = &self.classes[priority.index()];
        let cap_permit = class.cap.clone().try_acquire_owned().ok()?;
        let resource = if let Ok(p) = class.reserved.clone().try_acquire_owned() {
            ResourcePermit::Reserved(p)
        } else {
            ResourcePermit::Floating(self.floating.clone().try_acquire_owned().ok()?)
        };
        Some(self.make_permit(priority, priority, cap_permit, resource))
    }

    /// Try to steal a reserved slot from a lower-priority class (lowest first):
    /// this is the priority-inheritance fast path when the floating pool is dry.
    fn try_steal_resource(&self, requester: Priority) -> Option<ResourcePermit> {
        for donor in [Priority::Low, Priority::Normal, Priority::High] {
            if donor.index() <= requester.index() {
                continue;
            }
            if let Ok(p) = self.classes[donor.index()]
                .reserved
                .clone()
                .try_acquire_owned()
            {
                metrics::inc_priority_inheritance();
                return Some(ResourcePermit::Stolen(p));
            }
        }
        None
    }

    fn make_permit(
        &self,
        slot_class: Priority,
        effective: Priority,
        cap_permit: OwnedSemaphorePermit,
        resource: ResourcePermit,
    ) -> PoolPermit {
        let conn_id = self.next_conn_id.fetch_add(1, Ordering::SeqCst);
        let class = self.classes[slot_class.index()].clone();
        let active = class.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.registry.register(conn_id, effective);
        metrics::set_pool_active(slot_class.as_str(), active as f64);
        let max = class.current_max.load(Ordering::Acquire);
        metrics::set_pool_idle(slot_class.as_str(), max.saturating_sub(active) as f64);
        PoolPermit {
            _cap: cap_permit,
            _resource: resource,
            class,
            slot_class,
            effective,
            conn_id,
            registry: self.registry.clone(),
        }
    }

    /// Explicitly elevate the holder of `conn_id` to `inheriting` priority,
    /// returning a guard that restores it on drop. Models priority inheritance
    /// when a higher-priority task must wait on a connection a lower-priority
    /// task is using.
    pub fn inherit(&self, conn_id: u64, inheriting: Priority) -> Option<PriorityInheritanceGuard> {
        let cell = self.registry.cell(conn_id)?;
        metrics::inc_priority_inheritance();
        Some(PriorityInheritanceGuard::new(cell, inheriting))
    }

    fn record_wait(&self, priority: Priority, waited: Duration) {
        let ms = waited.as_millis() as u64;
        metrics::observe_pool_wait_ms(priority.as_str(), ms as f64);
        if ms > STARVATION_WAIT_MS {
            self.classes[priority.index()]
                .slow
                .store(true, Ordering::Release);
        }
    }

    fn mark_starved(&self, priority: Priority) {
        self.classes[priority.index()]
            .slow
            .store(true, Ordering::Release);
        metrics::inc_pool_starvation(priority.as_str());
    }

    fn exhausted(&self, priority: Priority, start: Instant) -> PoolError {
        PoolError::Exhausted {
            class: priority,
            waited_ms: start.elapsed().as_millis() as u64,
            active: self.active(priority),
        }
    }

    /// One rebalancing step: expire stale borrows, then borrow capacity for any
    /// class starved for [`STARVATION_CONSECUTIVE`] consecutive ticks.
    pub fn rebalance_tick(&self) {
        self.expire_borrows();
        for p in Priority::ALL {
            let class = &self.classes[p.index()];
            let was_slow = class.slow.swap(false, Ordering::AcqRel);
            let consecutive = if was_slow {
                class.consecutive_slow.fetch_add(1, Ordering::AcqRel) + 1
            } else {
                class.consecutive_slow.store(0, Ordering::Release);
                0
            };
            if consecutive >= STARVATION_CONSECUTIVE {
                class.consecutive_slow.store(0, Ordering::Release);
                self.borrow_for(p, BORROW_MAX);
            }
            let active = class.active.load(Ordering::Acquire);
            let max = class.current_max.load(Ordering::Acquire);
            metrics::set_pool_idle(p.as_str(), max.saturating_sub(active) as f64);
        }
    }

    /// Borrow up to `amount` cap slots for `starved` from lower-priority classes
    /// (Low, then Normal, then High), never reducing a donor below its `min`.
    pub fn borrow_for(&self, starved: Priority, amount: u32) -> u32 {
        let mut remaining = amount;
        for donor in [Priority::Low, Priority::Normal, Priority::High] {
            if remaining == 0 {
                break;
            }
            if donor.index() <= starved.index() {
                continue;
            }
            remaining -= self.move_cap(donor, starved, remaining);
        }
        amount - remaining
    }

    /// Move up to `n` cap slots from `donor` to `recipient`, returning the count
    /// actually moved.
    fn move_cap(&self, donor: Priority, recipient: Priority, n: u32) -> u32 {
        let donor_class = &self.classes[donor.index()];
        let donor_min = self.config.bounds(donor).min as u32;
        let donor_max = donor_class.current_max.load(Ordering::Acquire);
        let removable = donor_max.saturating_sub(donor_min);
        let available = donor_class.cap.available_permits() as u32;
        let want = n.min(removable).min(available);
        if want == 0 {
            return 0;
        }
        let taken = match donor_class.cap.try_acquire_many(want) {
            Ok(permit) => {
                permit.forget();
                want
            }
            Err(_) => return 0,
        };
        donor_class.current_max.fetch_sub(taken, Ordering::AcqRel);
        let recipient_class = &self.classes[recipient.index()];
        recipient_class.cap.add_permits(taken as usize);
        recipient_class
            .current_max
            .fetch_add(taken, Ordering::AcqRel);
        self.borrows.lock().push(Borrow {
            donor,
            recipient,
            amount: taken,
            started: Instant::now(),
        });
        taken
    }

    fn expire_borrows(&self) {
        let now = Instant::now();
        let expired: Vec<Borrow> = {
            let mut borrows = self.borrows.lock();
            let mut expired = Vec::new();
            borrows.retain(|b| {
                if now.duration_since(b.started) >= BORROW_TTL {
                    expired.push(*b);
                    false
                } else {
                    true
                }
            });
            expired
        };
        for b in expired {
            self.return_cap(b);
        }
    }

    /// Return borrowed cap slots from recipient back to donor. If the recipient
    /// is too busy to free them now, the unreturned remainder is re-queued for a
    /// later tick.
    fn return_cap(&self, borrow: Borrow) {
        let recipient_class = &self.classes[borrow.recipient.index()];
        let available = recipient_class.cap.available_permits() as u32;
        let take = borrow.amount.min(available);
        // `try_acquire_many(0)` is a harmless no-op, so no separate `take > 0`
        // guard is needed.
        if let Ok(permit) = recipient_class.cap.try_acquire_many(take) {
            permit.forget();
            recipient_class
                .current_max
                .fetch_sub(take, Ordering::AcqRel);
            let donor_class = &self.classes[borrow.donor.index()];
            donor_class.cap.add_permits(take as usize);
            donor_class.current_max.fetch_add(take, Ordering::AcqRel);
        }
        if take < borrow.amount {
            self.borrows.lock().push(Borrow {
                amount: borrow.amount - take,
                ..borrow
            });
        }
    }

    /// Spawn the background rebalancing task (1 Hz). Returns its join handle.
    pub fn spawn_rebalancer(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                self.rebalance_tick();
            }
        })
    }
}
