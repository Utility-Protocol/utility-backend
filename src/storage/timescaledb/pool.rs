//! Public facade over the priority-partitioned connection pool.
//!
//! [`PriorityPool`] is the entry point call sites use instead of a plain
//! `pool.get()`: it exposes `get(priority)` so each workload acquires from its
//! own class (see the priority map in the module docs of [`super`]). In
//! production each granted [`PoolPermit`] is paired with a real `deadpool`
//! connection checkout; the slot accounting and starvation prevention live in
//! [`PartitionedPool`].

use std::sync::Arc;
use std::time::Duration;

use super::pool_partitioned::{PartitionConfig, PartitionedPool, PoolError, PoolPermit};
use super::priority::Priority;

/// Default acquisition timeout when a caller does not specify one.
pub const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// Workload-aware connection pool. Cheaply cloneable (shares one
/// [`PartitionedPool`]).
#[derive(Clone)]
pub struct PriorityPool {
    inner: Arc<PartitionedPool>,
}

impl PriorityPool {
    /// Create a pool from `config`, validating its invariants.
    pub fn new(config: PartitionConfig) -> Result<Self, String> {
        Ok(Self {
            inner: Arc::new(PartitionedPool::new(config)?),
        })
    }

    /// Create a pool with the default 32-connection partitioning.
    pub fn with_defaults() -> Self {
        Self::new(PartitionConfig::default()).expect("default partition config is valid")
    }

    /// The underlying partitioned pool.
    pub fn inner(&self) -> &Arc<PartitionedPool> {
        &self.inner
    }

    /// Acquire a slot for `priority` using the default timeout.
    pub async fn get(&self, priority: Priority) -> Result<PoolPermit, PoolError> {
        self.inner.get(priority, DEFAULT_ACQUIRE_TIMEOUT).await
    }

    /// Acquire a slot for `priority` with an explicit timeout.
    pub async fn get_with_timeout(
        &self,
        priority: Priority,
        timeout: Duration,
    ) -> Result<PoolPermit, PoolError> {
        self.inner.get(priority, timeout).await
    }

    /// Start the background rebalancing task (1 Hz starvation-driven borrowing).
    pub fn start_rebalancer(&self) -> tokio::task::JoinHandle<()> {
        self.inner.clone().spawn_rebalancer()
    }
}
