use crate::ingestion::watermark::{
    MeterSourceId, OffsetDivergence, WatermarkVector, OFFSET_DIVERGENCE_THRESHOLD,
};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

pub const RECONCILIATION_SCAN_THROUGHPUT_PER_SEC: usize = 100_000;
pub const MAX_TOLERATED_PARTITION_DURATION: Duration = Duration::from_secs(300);
pub const PERIODIC_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OffsetReconciliationRequest {
    pub source_id: MeterSourceId,
    pub start_offset: u64,
    pub end_offset: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OffsetReconciliationResponse {
    pub source_id: MeterSourceId,
    pub event_ids: Vec<u64>,
}
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReconciliationOutcome {
    pub missing_event_ids: Vec<u64>,
    pub duplicate_event_ids: Vec<u64>,
}

pub fn request_for_divergence(divergence: &OffsetDivergence) -> OffsetReconciliationRequest {
    OffsetReconciliationRequest {
        source_id: divergence.source_id,
        start_offset: divergence.winner.offset.min(divergence.loser.offset),
        end_offset: divergence.winner.offset.max(divergence.loser.offset),
    }
}

pub fn reconcile_event_ids(
    local_event_ids: &[u64],
    remote_event_ids: &[u64],
) -> ReconciliationOutcome {
    let local: HashSet<_> = local_event_ids.iter().copied().collect();
    let remote: HashSet<_> = remote_event_ids.iter().copied().collect();
    let mut counts = HashMap::new();
    for id in local_event_ids {
        *counts.entry(*id).or_insert(0usize) += 1;
    }
    let mut missing_event_ids: Vec<_> = remote.difference(&local).copied().collect();
    let mut duplicate_event_ids: Vec<_> = counts
        .into_iter()
        .filter_map(|(id, count)| (count > 1).then_some(id))
        .collect();
    missing_event_ids.sort_unstable();
    duplicate_event_ids.sort_unstable();
    ReconciliationOutcome {
        missing_event_ids,
        duplicate_event_ids,
    }
}

pub fn proactive_reconciliation_sources(
    local: &WatermarkVector,
    peer: &WatermarkVector,
) -> Vec<MeterSourceId> {
    local
        .entries
        .iter()
        .filter_map(|(source, local_entry)| {
            peer.entries.get(source).and_then(|peer_entry| {
                (local_entry.offset > peer_entry.offset + OFFSET_DIVERGENCE_THRESHOLD)
                    .then_some(*source)
            })
        })
        .collect()
}

pub fn partition_exceeded(started_at: Instant, now: Instant) -> bool {
    now.duration_since(started_at) > MAX_TOLERATED_PARTITION_DURATION
}
