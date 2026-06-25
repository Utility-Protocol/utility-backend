use crate::ingestion::watermark::{HlcTimestamp, MeterSourceId, WatermarkVector};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

pub const WATERMARK_PERSIST_INTERVAL: Duration = Duration::from_secs(10);

pub fn current_physical_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub async fn acknowledge_batch(
    shared: &Arc<RwLock<WatermarkVector>>,
    sources: &[(MeterSourceId, u64)],
) {
    let now = current_physical_ms();
    let mut vector = shared.write().await;
    for (source_id, offset) in sources {
        let previous = vector
            .entries
            .get(source_id)
            .map(|entry| entry.hlc)
            .unwrap_or_else(HlcTimestamp::zero);
        vector.upsert(*source_id, previous.tick(now), *offset);
    }
}

pub async fn persist_watermark_snapshot(vector: &WatermarkVector) -> Vec<u8> {
    let mut rows: Vec<_> = vector
        .entries
        .iter()
        .map(|(source, entry)| (*source, entry.hlc.as_u64(), entry.offset))
        .collect();
    rows.sort_unstable_by_key(|row| row.0);
    format!("{:?}", rows).into_bytes()
}
