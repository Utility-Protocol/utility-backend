use std::sync::Arc;
use tokio::sync::mpsc;

use utility_backend::gateway::hlc::{HlcTimestamp, HybridLogicalClock};
use utility_backend::gateway::ordering::CausalOrderer;
use utility_backend::gateway::stream::MeterEvent;
use utility_backend::gateway::watermark::HlcWatermarkStore;

/// Simulates the classic "causal delivery" scenario:
///
/// Event A causally precedes event B: both carry the same wall-clock time,
/// but B's HLC has a higher logical component.  B arrives at the orderer
/// first, yet the output stream must deliver A before B.
#[tokio::test]
async fn test_causal_delivery_preserved() {
    let hlc = Arc::new(HybridLogicalClock::new());
    let (tx, _rx) = mpsc::channel(100);
    let mut sync_orderer = CausalOrderer::new(tx, hlc.clone());

    // A is causally first: ticked before B at the same wall-clock time.
    let ts_a = hlc.tick(1000); // (1000, logical 0)
    let ts_b = hlc.tick(1000); // (1000, logical 1)

    let event_a = MeterEvent {
        meter_id: "MTR-CAUSAL".into(),
        timestamp: 1000,
        reading: 100.0,
        token_volume: 50,
        hlc_timestamp: ts_a.0,
    };
    let event_b = MeterEvent {
        meter_id: "MTR-CAUSAL".into(),
        timestamp: 1000,
        reading: 200.0,
        token_volume: 100,
        hlc_timestamp: ts_b.0,
    };

    // Push events in reverse causal order (B first, then A).
    sync_orderer.push(event_b.clone());
    sync_orderer.push(event_a.clone());

    // Both events sit at the current HLC time, so nothing is ready yet.
    assert!(
        sync_orderer.flush_ready().is_empty(),
        "events at the current HLC time should be held within the skew window"
    );

    // Once the clock advances past the skew window, A (the causally-earlier
    // event, smaller HLC) must be emitted before B.
    hlc.tick(2000);
    let ready = sync_orderer.flush_ready();
    assert_eq!(ready.len(), 2, "both events should be ready");
    assert_eq!(
        ready[0].reading, 100.0,
        "A (causally first) should be emitted before B"
    );
    assert_eq!(ready[1].reading, 200.0, "B should be emitted after A");
}

/// Test that HLC on two different collectors produces causally ordered
/// timestamps even when wall clocks are identical.
#[test]
fn test_hlc_causal_order_across_collectors() {
    let hlc_a = HybridLogicalClock::new();
    let hlc_b = HybridLogicalClock::new();

    // Both collectors observe the same wall clock time
    let wall = 5000u64;

    // Collector A processes event first
    let ts_a = hlc_a.tick(wall);

    // Collector B processes event second (causally after A)
    let ts_b = hlc_b.tick(wall);

    // Both timestamps have the same physical time but different logical clocks
    assert_eq!(ts_a.physical(), wall);
    assert_eq!(ts_b.physical(), wall);
    // A has logical=0, B has logical=1
    assert_eq!(ts_a.logical(), 0);
    assert_eq!(
        ts_b.logical(),
        0,
        "separate HLCs both start at logical 0 for the same physical time"
    );

    // Now simulate collector B receiving event A's HLC timestamp.
    // This is the "HLC propagation" rule.
    hlc_b.update(ts_a);
    // After update, B's logical should advance to be > A's logical
    let ts_c = hlc_b.tick(wall);
    assert_eq!(ts_c.physical(), wall);
    assert!(
        ts_c.logical() > ts_a.logical(),
        "after receiving A's HLC, B's logical should advance: {} > {}",
        ts_c.logical(),
        ts_a.logical()
    );
}

/// Test that HlcWatermarkStore CRDT merge resolves correctly using HLC.
#[test]
fn test_watermark_crdt_merge_with_hlc() {
    use std::collections::HashMap;

    let store = HlcWatermarkStore::new();

    // Collector 1's watermark for meter M1
    store.record("M1", HlcTimestamp::new(1000, 5), 10);

    // Collector 2's watermark for same meter (higher HLC)
    let mut other = HashMap::new();
    other.insert(
        "M1".into(),
        utility_backend::gateway::watermark::HlcWatermark {
            last_hlc: HlcTimestamp::new(1000, 10),
            last_offset: 8,
        },
    );

    store.merge(&other);
    let merged = store.get("M1").unwrap();
    assert_eq!(
        merged.last_hlc.logical(),
        10,
        "higher HLC logical should win"
    );
    assert_eq!(
        merged.last_offset, 8,
        "offset should come from winning entry"
    );
}

/// Test the HLC format packing/unpacking.
#[test]
fn test_hlc_timestamp_packing() {
    let ts = HlcTimestamp::new(0xABCD_EF01_2345, 0xDEAD);
    assert_eq!(ts.physical(), 0xABCD_EF01_2345);
    assert_eq!(ts.logical(), 0xDEAD);
}

/// Test that HLC preserves ordering under concurrent access.
#[test]
fn test_concurrent_hlc_tick() {
    use std::thread;
    let hlc = Arc::new(HybridLogicalClock::new());
    let mut handles = Vec::new();
    let wall = 9999u64;

    for _ in 0..100 {
        let c = hlc.clone();
        handles.push(thread::spawn(move || c.tick(wall)));
    }

    let mut timestamps: Vec<HlcTimestamp> =
        handles.into_iter().map(|h| h.join().unwrap()).collect();

    timestamps.sort();

    // All timestamps should have distinct logical values in increasing order
    for i in 1..timestamps.len() {
        assert!(
            timestamps[i] > timestamps[i - 1],
            "concurrent HLC ticks must produce strictly increasing timestamps"
        );
    }
}

/// Test that CausalOrderer holds events within the clock skew window.
#[tokio::test]
async fn test_causal_orderer_holds_within_skew() {
    let hlc = Arc::new(HybridLogicalClock::new());
    let (tx, _rx) = mpsc::channel(100);

    let mut orderer = CausalOrderer::with_skew(tx, hlc.clone(), 200);
    hlc.tick(2000);

    // Push event with wall clock just within the skew window (1900, where HLC is at 2000)
    orderer.push(MeterEvent {
        meter_id: "M1".into(),
        timestamp: 1900,
        reading: 1.0,
        token_volume: 0,
        hlc_timestamp: 0,
    });

    let ready = orderer.flush_ready();
    assert!(
        ready.is_empty(),
        "event at 1900 should be held when HLC is at 2000 with skew 200"
    );
}

/// Test that older stragglers are eventually flushed.
#[tokio::test]
async fn test_causal_orderer_flushes_stragglers() {
    let hlc = Arc::new(HybridLogicalClock::new());
    let (tx, _rx) = mpsc::channel(100);

    let mut orderer = CausalOrderer::with_skew(tx, hlc.clone(), 200);
    hlc.tick(5000);

    // Push event with old wall clock
    orderer.push(MeterEvent {
        meter_id: "M1".into(),
        timestamp: 1000,
        reading: 1.0,
        token_volume: 0,
        hlc_timestamp: 0,
    });

    let ready = orderer.flush_ready();
    assert_eq!(ready.len(), 1, "old event should be flushed immediately");
}
