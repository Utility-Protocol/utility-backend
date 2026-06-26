//! Tests for the workload-priority-partitioned connection pool (issue #52).
//!
//! Capacity is semaphore-modelled, so these run without a live database. They
//! are deterministic except for the final stress test, which is wrapped in an
//! overall timeout so a hypothetical deadlock fails fast instead of hanging CI.

use std::sync::Arc;
use std::time::Duration;

use utility_backend::storage::timescaledb::pool::PriorityPool;
use utility_backend::storage::timescaledb::pool_partitioned::{
    ClassBounds, PartitionConfig, PartitionedPool, PoolError,
};
use utility_backend::storage::timescaledb::priority::Priority;

/// total=5, every class {min:1, max:2} -> sum(min)=4, floating=1.
fn cfg5() -> PartitionConfig {
    PartitionConfig {
        total: 5,
        bounds: [
            ClassBounds { min: 1, max: 2 },
            ClassBounds { min: 1, max: 2 },
            ClassBounds { min: 1, max: 2 },
            ClassBounds { min: 1, max: 2 },
        ],
    }
}

#[test]
fn test_config_validation() {
    assert!(PartitionConfig::default().validate().is_ok());
    // sum(min) = 16 > total 4
    let bad = PartitionConfig {
        total: 4,
        ..PartitionConfig::default()
    };
    assert!(bad.validate().is_err());
    // total over hard cap
    let too_big = PartitionConfig {
        total: 200,
        ..PartitionConfig::default()
    };
    assert!(too_big.validate().is_err());
}

#[tokio::test]
async fn test_reserved_min_is_guaranteed_under_floating_pressure() {
    let pool = PriorityPool::new(cfg5()).unwrap();
    let short = Duration::from_millis(50);

    // Drain the single floating slot by pushing Normal beyond its reservation.
    let _n1 = pool
        .get_with_timeout(Priority::Normal, short)
        .await
        .unwrap();
    let _n2 = pool
        .get_with_timeout(Priority::Normal, short)
        .await
        .unwrap(); // uses floating
    assert_eq!(pool.inner().floating_available(), 0);

    // Critical still gets its reserved slot immediately despite floating = 0.
    let c = pool.get_with_timeout(Priority::Critical, short).await;
    assert!(c.is_ok(), "reserved min must be guaranteed");
    assert_eq!(pool.inner().active(Priority::Critical), 1);
}

#[tokio::test]
async fn test_max_cap_backpressure() {
    let pool = PriorityPool::new(cfg5()).unwrap();
    let short = Duration::from_millis(50);

    let _c1 = pool
        .get_with_timeout(Priority::Critical, short)
        .await
        .unwrap(); // reserved
    let _c2 = pool
        .get_with_timeout(Priority::Critical, short)
        .await
        .unwrap(); // floating
    assert_eq!(pool.inner().active(Priority::Critical), 2); // at max

    // Third exceeds Critical's max of 2 -> backpressure. (Matched on the
    // `Result` directly: `unwrap_err` would require `PoolPermit: Debug`.)
    let result = pool.get_with_timeout(Priority::Critical, short).await;
    assert!(matches!(result, Err(PoolError::Exhausted { .. })));
}

#[tokio::test]
async fn test_priority_inheritance_steal_from_lower_class() {
    let pool = PriorityPool::new(cfg5()).unwrap();
    let short = Duration::from_millis(50);

    let _h1 = pool.get_with_timeout(Priority::High, short).await.unwrap(); // High reserved
    let _n1 = pool
        .get_with_timeout(Priority::Normal, short)
        .await
        .unwrap(); // Normal reserved
    let _n2 = pool
        .get_with_timeout(Priority::Normal, short)
        .await
        .unwrap(); // floating -> 0
    assert_eq!(pool.inner().floating_available(), 0);

    // High needs another slot: its reservation is taken and floating is dry, so
    // it steals Low's reserved slot (priority inheritance).
    let h2 = pool.get_with_timeout(Priority::High, short).await.unwrap();
    assert!(
        h2.is_inherited(),
        "should have stolen a lower-class reservation"
    );
}

#[tokio::test]
async fn test_inheritance_guard_elevates_and_restores() {
    let pool = PriorityPool::new(cfg5()).unwrap();
    let permit = pool
        .get_with_timeout(Priority::Normal, Duration::from_millis(50))
        .await
        .unwrap();
    let conn_id = permit.connection_id();
    let registry = pool.inner().registry();
    assert_eq!(registry.effective_priority(conn_id), Some(Priority::Normal));

    {
        let _guard = pool.inner().inherit(conn_id, Priority::Critical).unwrap();
        assert_eq!(
            registry.effective_priority(conn_id),
            Some(Priority::Critical)
        );
    }
    // Restored after the guard drops.
    assert_eq!(registry.effective_priority(conn_id), Some(Priority::Normal));
}

#[test]
fn test_rebalance_borrow_moves_capacity() {
    let pool = PartitionedPool::new(PartitionConfig::default()).unwrap();
    assert_eq!(pool.current_max(Priority::Critical), 8);
    assert_eq!(pool.current_max(Priority::Low), 8);

    let moved = pool.borrow_for(Priority::Critical, 2);
    assert_eq!(moved, 2);
    // Borrowed from Low first (lowest priority donor).
    assert_eq!(pool.current_max(Priority::Critical), 10);
    assert_eq!(pool.current_max(Priority::Low), 6);
}

#[tokio::test]
async fn test_mixed_workload_stress_no_deadlock() {
    let pool = Arc::new(PriorityPool::new(cfg5()).unwrap());

    let run = async {
        let mut handles = Vec::new();
        for i in 0..50usize {
            let priority = match i {
                0..=4 => Priority::Critical,
                5..=14 => Priority::High,
                15..=44 => Priority::Normal,
                _ => Priority::Low,
            };
            let hold = match priority {
                Priority::Critical => 5,
                Priority::High => 3,
                Priority::Normal => 4,
                Priority::Low => 8,
            };
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                let mut acquired = 0u32;
                for _ in 0..5 {
                    if let Ok(permit) = pool
                        .get_with_timeout(priority, Duration::from_secs(2))
                        .await
                    {
                        tokio::time::sleep(Duration::from_millis(hold)).await;
                        drop(permit);
                        acquired += 1;
                    }
                }
                acquired
            }));
        }
        let mut total = 0u32;
        for h in handles {
            total += h.await.unwrap();
        }
        total
    };

    let total = tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .expect("stress workload deadlocked");

    assert!(total > 0, "no work completed");
    // Everything released: pool fully drained.
    for p in Priority::ALL {
        assert_eq!(pool.inner().active(p), 0, "class {p:?} not drained");
    }
}
