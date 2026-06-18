use dashmap::DashMap;
use parking_lot::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

const NONCE_BLOCK_SIZE: u64 = 100;
const STALE_GRID_TIMEOUT: Duration = Duration::from_secs(3600);

pub struct GridNonceState {
    current: AtomicU64,
    reserved_until: AtomicU64,
    last_activity: AtomicU64,
    reservation_lock: StdMutex<()>,
}

pub struct NonceSequencer {
    grids: DashMap<String, Arc<GridNonceState>>,
    last_committed: DashMap<String, u64>,
}

impl Default for NonceSequencer {
    fn default() -> Self {
        Self::new()
    }
}

fn current_time_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl NonceSequencer {
    pub fn new() -> Self {
        Self {
            grids: DashMap::new(),
            last_committed: DashMap::new(),
        }
    }

    fn get_or_create_grid(&self, grid_id: &str) -> Arc<GridNonceState> {
        let entry = self.grids.entry(grid_id.to_string()).or_insert_with(|| {
            Arc::new(GridNonceState {
                current: AtomicU64::new(0),
                reserved_until: AtomicU64::new(0),
                last_activity: AtomicU64::new(current_time_secs()),
                reservation_lock: StdMutex::new(()),
            })
        });
        entry.value().clone()
    }

    pub fn next_nonce(&self, grid_id: &str) -> u64 {
        let state = self.get_or_create_grid(grid_id);
        state
            .last_activity
            .store(current_time_secs(), Ordering::Relaxed);

        loop {
            let cur = state.current.load(Ordering::Acquire);
            let limit = state.reserved_until.load(Ordering::Acquire);
            if cur < limit {
                if state
                    .current
                    .compare_exchange(cur, cur + 1, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    info!(grid = %grid_id, nonce = cur, "nonce issued");
                    return cur;
                }
            } else {
                let _lock = state.reservation_lock.lock();
                if state.current.load(Ordering::Acquire)
                    < state.reserved_until.load(Ordering::Acquire)
                {
                    continue;
                }
                let hwm = self.get_grid_high_water_mark(grid_id);
                let start = hwm + 1;
                let end = start + NONCE_BLOCK_SIZE;
                state.current.store(start, Ordering::Release);
                state.reserved_until.store(end, Ordering::Release);
            }
        }
    }

    pub fn commit_nonce(&self, grid_id: &str, nonce: u64) -> Result<(), &'static str> {
        let mut entry = self.last_committed.entry(grid_id.to_string()).or_insert(0);
        if nonce <= *entry {
            return Err("nonce already committed: possible double-spend");
        }
        *entry = nonce;
        Ok(())
    }

    pub fn get_grid_high_water_mark(&self, grid_id: &str) -> u64 {
        self.last_committed.get(grid_id).map(|r| *r).unwrap_or(0)
    }

    pub fn get_all_grid_high_water_marks(&self) -> Vec<(String, u64)> {
        self.last_committed
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect()
    }

    pub fn reserve_nonce_block(&self, grid_id: &str) -> (u64, u64) {
        let state = self.get_or_create_grid(grid_id);
        let _lock = state.reservation_lock.lock();
        let hwm = self.get_grid_high_water_mark(grid_id);
        let start = hwm + 1;
        let end = start + NONCE_BLOCK_SIZE;
        state.current.store(start, Ordering::Release);
        state.reserved_until.store(end, Ordering::Release);
        (start, end)
    }

    pub async fn start_reaper(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(Duration::from_secs(300));
        loop {
            ticker.tick().await;
            let cutoff = current_time_secs().saturating_sub(STALE_GRID_TIMEOUT.as_secs());
            let stale_ids: Vec<String> = self
                .grids
                .iter()
                .filter_map(|entry| {
                    let state = entry.value();
                    if state.last_activity.load(Ordering::Acquire) < cutoff {
                        Some(entry.key().clone())
                    } else {
                        None
                    }
                })
                .collect();
            for id in stale_ids {
                self.grids.remove(&id);
                self.last_committed.remove(&id);
                info!(grid = %id, "reaped stale grid state");
            }
        }
    }
}
