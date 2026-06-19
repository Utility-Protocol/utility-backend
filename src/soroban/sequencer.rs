use dashmap::DashMap;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info};

const NONCE_BLOCK_SIZE: u64 = 100;
const STALE_TIMEOUT: Duration = Duration::from_secs(3600);
const REAP_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Debug)]
struct GridState {
    next: AtomicU64,
    end: AtomicU64,
    last_activity: Instant,
}

#[derive(Debug, Serialize, Clone)]
pub struct GridNonceInfo {
    pub grid_id: String,
    pub high_water_mark: u64,
}

#[derive(Debug, Serialize, Clone)]
pub struct NonceStatus {
    pub grids: Vec<GridNonceInfo>,
}

pub struct NonceSequencer {
    global_allocator: AtomicU64,
    grids: Arc<DashMap<String, GridState>>,
    last_committed: Arc<DashMap<String, AtomicU64>>,
    reaper_started: AtomicBool,
}

impl Default for NonceSequencer {
    fn default() -> Self {
        Self::new()
    }
}

impl NonceSequencer {
    pub fn new() -> Self {
        Self {
            global_allocator: AtomicU64::new(0),
            grids: Arc::new(DashMap::new()),
            last_committed: Arc::new(DashMap::new()),
            reaper_started: AtomicBool::new(false),
        }
    }

    pub fn next_nonce(&self, grid_id: &str) -> u64 {
        self.ensure_reaper_started();

        let mut entry = self.grids.entry(grid_id.to_string()).or_insert_with(|| {
            let base = self
                .global_allocator
                .fetch_add(NONCE_BLOCK_SIZE, Ordering::SeqCst);
            info!(
                grid = %grid_id,
                base,
                block_size = NONCE_BLOCK_SIZE,
                "new grid nonce block allocated"
            );
            GridState {
                next: AtomicU64::new(base),
                end: AtomicU64::new(base + NONCE_BLOCK_SIZE),
                last_activity: Instant::now(),
            }
        });

        entry.last_activity = Instant::now();

        let next = entry.next.load(Ordering::Relaxed);
        let end = entry.end.load(Ordering::Relaxed);

        if next < end {
            entry.next.store(next + 1, Ordering::Release);
            debug!(grid = %grid_id, nonce = next, "nonce issued");
            next
        } else {
            let new_base = self
                .global_allocator
                .fetch_add(NONCE_BLOCK_SIZE, Ordering::SeqCst);
            entry
                .end
                .store(new_base + NONCE_BLOCK_SIZE, Ordering::Release);
            entry.next.store(new_base + 1, Ordering::Release);
            info!(
                grid = %grid_id,
                nonce = new_base,
                block_size = NONCE_BLOCK_SIZE,
                "nonce block exhausted, new block allocated"
            );
            new_base
        }
    }

    pub fn commit_nonce(&self, grid_id: &str, nonce: u64) -> Result<(), &'static str> {
        let last = self
            .last_committed
            .entry(grid_id.to_string())
            .or_insert(AtomicU64::new(u64::MAX));

        loop {
            let current = last.load(Ordering::Acquire);
            if current != u64::MAX && nonce <= current {
                return Err("nonce already committed: possible double-spend");
            }
            if last
                .compare_exchange_weak(current, nonce, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                info!(grid = %grid_id, nonce, "nonce committed");
                return Ok(());
            }
        }
    }

    pub fn status(&self) -> NonceStatus {
        let mut grids: Vec<GridNonceInfo> = self
            .grids
            .iter()
            .map(|entry| {
                let grid_id = entry.key().clone();
                let next = entry.value().next.load(Ordering::Acquire);
                let hwm = if next > 0 { next - 1 } else { 0 };
                GridNonceInfo {
                    grid_id,
                    high_water_mark: hwm,
                }
            })
            .collect();
        grids.sort_by(|a, b| a.grid_id.cmp(&b.grid_id));
        NonceStatus { grids }
    }

    fn ensure_reaper_started(&self) {
        if self
            .reaper_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            let grids = Arc::clone(&self.grids);
            let last_committed = Arc::clone(&self.last_committed);
            std::thread::spawn(move || {
                info!("nonce reaper started");
                loop {
                    std::thread::sleep(REAP_INTERVAL);
                    let count = reap_stale(&grids, &last_committed);
                    if count > 0 {
                        info!(
                            stale_count = count,
                            "nonce reaper cleaned stale grid entries"
                        );
                    }
                }
            });
        }
    }
}

fn reap_stale(
    grids: &Arc<DashMap<String, GridState>>,
    last_committed: &Arc<DashMap<String, AtomicU64>>,
) -> usize {
    let now = Instant::now();
    let stale_keys: Vec<String> = grids
        .iter()
        .filter(|e| now.duration_since(e.value().last_activity) >= STALE_TIMEOUT)
        .map(|e| e.key().clone())
        .collect();

    for key in &stale_keys {
        grids.remove(key);
        let _ = last_committed.remove(key);
        info!(grid = %key, "reaped stale nonce state");
    }

    stale_keys.len()
}

lazy_static::lazy_static! {
    static ref GLOBAL_SEQUENCER: NonceSequencer = NonceSequencer::new();
}

pub fn global_sequencer() -> &'static NonceSequencer {
    &GLOBAL_SEQUENCER
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::ProptestConfig;
    use proptest::test_runner::TestRunner;
    use std::collections::HashSet;

    #[test]
    fn test_next_nonce_monotonic_per_grid() {
        let seq = NonceSequencer::new();
        let n1 = seq.next_nonce("grid-east");
        let n2 = seq.next_nonce("grid-east");
        let n3 = seq.next_nonce("grid-west");
        assert!(
            n1 < n2,
            "nonces for same grid must be monotonically increasing"
        );
        assert_ne!(n1, n3, "nonces for different grids must not be equal");
    }

    #[test]
    fn test_block_allocation_reduces_atomic_ops() {
        let seq = NonceSequencer::new();
        let first = seq.next_nonce("grid-test");
        assert_eq!(first, 0, "first nonce should be 0");

        let second = seq.next_nonce("grid-test");
        assert_eq!(second, 1, "second nonce should be 1");

        for i in 2..NONCE_BLOCK_SIZE {
            let n = seq.next_nonce("grid-test");
            assert_eq!(n, i, "nonce {} should be {}", i, i);
        }

        let after_block = seq.next_nonce("grid-test");
        assert_eq!(
            after_block, NONCE_BLOCK_SIZE,
            "after block exhaustion, next nonce should be BLOCK_SIZE"
        );
    }

    #[test]
    fn test_commit_nonce_rejects_out_of_order() {
        let seq = NonceSequencer::new();
        assert!(seq.commit_nonce("grid-east", 5).is_ok());
        assert!(seq.commit_nonce("grid-east", 5).is_err());
        assert!(seq.commit_nonce("grid-east", 3).is_err());
        assert!(seq.commit_nonce("grid-east", 10).is_ok());
    }

    #[tokio::test]
    async fn test_concurrent_nonce_issuance_no_collisions() {
        let seq = Arc::new(NonceSequencer::new());
        let grid_count = 20;
        let ops_per_grid = 100;
        let mut handles = Vec::new();

        for g in 0..grid_count {
            let seq = Arc::clone(&seq);
            let grid_id = format!("grid-{}", g);
            handles.push(tokio::spawn(async move {
                let mut nonces = Vec::with_capacity(ops_per_grid);
                for _ in 0..ops_per_grid {
                    nonces.push(seq.next_nonce(&grid_id));
                }
                nonces
            }));
        }

        let mut all_nonces: Vec<Vec<u64>> = Vec::new();
        for h in handles {
            all_nonces.push(h.await.unwrap());
        }

        // Verify per-grid uniqueness and monotonicity
        for (g, nonces) in all_nonces.iter().enumerate() {
            let mut sorted = nonces.clone();
            sorted.sort();
            assert!(
                nonces.windows(2).all(|w| w[0] <= w[1]),
                "grid-{} nonces must be monotonically increasing: {:?}",
                g,
                nonces
            );
            let unique: HashSet<u64> = nonces.iter().copied().collect();
            assert_eq!(
                unique.len(),
                nonces.len(),
                "grid-{} has duplicate nonces",
                g
            );
        }

        // Verify no cross-grid collisions
        let mut global_set: HashSet<u64> = HashSet::new();
        for nonces in &all_nonces {
            for n in nonces {
                assert!(
                    global_set.insert(*n),
                    "nonce {} collision across different grids",
                    n
                );
            }
        }
    }

    #[tokio::test]
    async fn test_concurrent_block_exhaustion() {
        let seq = Arc::new(NonceSequencer::new());
        let grid_id = "grid-stressed";
        let concurrent_tasks = 50;
        let ops_per_task = 10;

        let mut handles = Vec::new();
        for _ in 0..concurrent_tasks {
            let seq = Arc::clone(&seq);
            let gid = grid_id.to_string();
            handles.push(tokio::spawn(async move {
                let mut nonces = Vec::with_capacity(ops_per_task);
                for _ in 0..ops_per_task {
                    nonces.push(seq.next_nonce(&gid));
                }
                nonces
            }));
        }

        let mut all_nonces = Vec::new();
        for h in handles {
            all_nonces.extend(h.await.unwrap());
        }

        assert_eq!(all_nonces.len(), concurrent_tasks * ops_per_task);
        let unique: HashSet<u64> = all_nonces.iter().copied().collect();
        assert_eq!(
            unique.len(),
            all_nonces.len(),
            "duplicate nonces detected under concurrent block exhaustion stress"
        );
    }

    #[test]
    fn test_commit_nonce_in_order() {
        let seq = NonceSequencer::new();
        let grid_id = "grid-commit-test";

        let nonces: Vec<u64> = (0..50).map(|_| seq.next_nonce(grid_id)).collect();

        for (i, n) in nonces.iter().enumerate() {
            assert!(
                seq.commit_nonce(grid_id, *n).is_ok(),
                "commit of nonce {} should succeed",
                i
            );
        }

        // Verify double-commit is rejected
        assert!(seq.commit_nonce(grid_id, 0).is_err());
    }

    #[test]
    fn test_status_reflects_issued_nonces() {
        let seq = NonceSequencer::new();
        seq.next_nonce("grid-alpha");
        seq.next_nonce("grid-alpha");
        seq.next_nonce("grid-beta");

        let status = seq.status();
        assert_eq!(status.grids.len(), 2);

        let alpha = status
            .grids
            .iter()
            .find(|g| g.grid_id == "grid-alpha")
            .unwrap();
        assert_eq!(alpha.high_water_mark, 1);

        let beta = status
            .grids
            .iter()
            .find(|g| g.grid_id == "grid-beta")
            .unwrap();
        assert_eq!(beta.high_water_mark, 100);
    }

    #[test]
    fn prop_nonce_sequence_increases() {
        let mut runner = TestRunner::new(ProptestConfig::with_cases(100));
        runner
            .run(&(0..1000u64, 1..50u64), |(_start_offset, steps)| {
                let seq = NonceSequencer::new();
                let grid_id = format!("prop-seq-{}", steps);
                let first = seq.next_nonce(&grid_id);
                let mut prev = first;
                for _ in 1..steps {
                    let n = seq.next_nonce(&grid_id);
                    assert!(n > prev, "nonces must be monotonically increasing");
                    prev = n;
                }
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn prop_multi_grid_no_collisions() {
        let mut runner = TestRunner::new(ProptestConfig::with_cases(20));

        use proptest::collection;

        let strategy = (
            collection::vec("[a-z]{1,8}", 20..=20),
            collection::vec(1..10u64, 20),
        );

        runner
            .run(&strategy, |(grid_ids, operations)| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    let seq = Arc::new(NonceSequencer::new());
                    let mut handles = Vec::new();

                    for (g, ops) in grid_ids.iter().zip(operations.iter()) {
                        let seq = Arc::clone(&seq);
                        let gid = g.clone();
                        let count = *ops;
                        handles.push(tokio::spawn(async move {
                            let mut nonces = Vec::with_capacity(count as usize);
                            for _ in 0..count {
                                nonces.push(seq.next_nonce(&gid));
                            }
                            nonces
                        }));
                    }

                    let mut all_nonces = Vec::new();
                    for h in handles {
                        all_nonces.extend(h.await.unwrap());
                    }

                    let unique: HashSet<u64> = all_nonces.iter().copied().collect();
                    assert_eq!(
                        unique.len(),
                        all_nonces.len(),
                        "collision detected across {} grids with {} total operations",
                        grid_ids.len(),
                        all_nonces.len()
                    );
                });
                Ok(())
            })
            .unwrap();
    }
}
