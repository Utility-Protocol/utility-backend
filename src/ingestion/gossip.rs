use crate::ingestion::watermark::WatermarkVector;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
pub const MISSED_HEARTBEAT_SUSPECT_ROUNDS: u32 = 3;
pub const PHI_ACCRUAL_SUSPECT_AFTER: Duration = Duration::from_secs(3);
pub const GOSSIP_WATERMARK_DELTA_LIMIT_BYTES: usize = 8 * 1024;
pub const GOSSIP_WATERMARK_DELTA_FALLBACK_ENTRIES: usize = 1_000;

#[derive(Clone, Debug)]
pub struct GossipPayload {
    pub watermark_delta: WatermarkVector,
}

#[derive(Clone, Debug)]
pub struct PeerFailureState {
    pub missed_rounds: u32,
    pub last_heartbeat: Instant,
}
impl PeerFailureState {
    pub fn is_suspect(&self, now: Instant) -> bool {
        self.missed_rounds >= MISSED_HEARTBEAT_SUSPECT_ROUNDS
            || now.duration_since(self.last_heartbeat) >= PHI_ACCRUAL_SUSPECT_AFTER
    }
}

pub async fn build_gossip_payload(
    shared: &Arc<RwLock<WatermarkVector>>,
    last_epoch: u64,
) -> GossipPayload {
    let guard = shared.read().await;
    GossipPayload {
        watermark_delta: guard
            .delta_since_epoch(last_epoch, GOSSIP_WATERMARK_DELTA_FALLBACK_ENTRIES),
    }
}

pub async fn apply_gossip_payload(
    shared: &Arc<RwLock<WatermarkVector>>,
    payload: &GossipPayload,
) -> usize {
    let mut guard = shared.write().await;
    guard.merge(&payload.watermark_delta).len()
}
