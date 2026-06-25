use utility_backend::ingestion::reconciliation::{
    proactive_reconciliation_sources, reconcile_event_ids,
};
use utility_backend::ingestion::watermark::{HlcTimestamp, WatermarkVector};

#[tokio::test]
async fn partitioned_vectors_converge_after_healing() {
    let mut a = WatermarkVector::new();
    let mut b = WatermarkVector::new();
    a.upsert(7, HlcTimestamp::new(1_000, 1), 500);
    b.upsert(7, HlcTimestamp::new(1_001, 0), 501);
    a.upsert(9, HlcTimestamp::new(2_000, 0), 1_000);
    b.upsert(9, HlcTimestamp::new(1_999, 42), 999);

    let da = a.merge(&b);
    let db = b.merge(&a);
    assert!(da.is_empty());
    assert!(db.is_empty());
    assert_eq!(a.entries, b.entries);
    assert_eq!(a.entries.get(&7).unwrap().offset, 501);
    assert_eq!(a.entries.get(&9).unwrap().offset, 1_000);
}

#[test]
fn divergent_offsets_trigger_reconciliation_and_exactly_once_diffing() {
    let mut local = WatermarkVector::new();
    let mut peer = WatermarkVector::new();
    local.upsert(11, HlcTimestamp::new(5_000, 2), 2_500);
    peer.upsert(11, HlcTimestamp::new(5_001, 0), 4_000);
    assert_eq!(proactive_reconciliation_sources(&peer, &local), vec![11]);

    peer.upsert(11, HlcTimestamp::new(5_002, 0), 5);

    let divergences = local.merge(&peer);
    assert_eq!(divergences.len(), 1);
    assert_eq!(divergences[0].source_id, 11);

    let outcome = reconcile_event_ids(&[1, 2, 2, 3], &[2, 3, 4, 5]);
    assert_eq!(outcome.missing_event_ids, vec![4, 5]);
    assert_eq!(outcome.duplicate_event_ids, vec![2]);
}
