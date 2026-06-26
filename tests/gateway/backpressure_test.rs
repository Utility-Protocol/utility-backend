//! Tests for the priority backpressure filter with spill-to-disk (issue #4).

use utility_backend::gateway::stream::{
    EventPriority, FileSpillStore, MeterEvent, PriorityBackpressureFilter, PushOutcome, SpillCodec,
    SpillStore,
};
use utility_backend::ingestion::tai64n::Tai64N;

fn event(meter_id: &str) -> MeterEvent {
    MeterEvent {
        meter_id: meter_id.to_string(),
        timestamp_tai: Tai64N::now_with_correction(0),
        correction_ns: 0,
        reading: 0.0,
        token_volume: 0,
    }
}

#[test]
fn test_strict_priority_ordering() {
    let filter = PriorityBackpressureFilter::<MeterEvent>::with_in_memory_spill(1_000_000, 1_000);
    filter.push(EventPriority::Low, event("low"), 10);
    filter.push(EventPriority::Normal, event("normal"), 10);
    filter.push(EventPriority::Critical, event("crit"), 10);
    filter.push(EventPriority::High, event("high"), 10);

    assert_eq!(filter.pop().unwrap().meter_id, "crit");
    assert_eq!(filter.pop().unwrap().meter_id, "high");
    assert_eq!(filter.pop().unwrap().meter_id, "normal");
    assert_eq!(filter.pop().unwrap().meter_id, "low");
    assert!(filter.pop().is_none());
}

#[test]
fn test_critical_bypasses_budget_normal_spills() {
    // Budget for two 256-byte events.
    let filter = PriorityBackpressureFilter::<MeterEvent>::with_in_memory_spill(512, 1_000);
    assert_eq!(
        filter.push(EventPriority::Normal, event("a"), 256),
        PushOutcome::Enqueued
    );
    assert_eq!(
        filter.push(EventPriority::Normal, event("b"), 256),
        PushOutcome::Enqueued
    );
    // Over budget -> spilled.
    assert_eq!(
        filter.push(EventPriority::Normal, event("c"), 256),
        PushOutcome::Spilled
    );
    // Critical bypasses the budget entirely.
    assert_eq!(
        filter.push(EventPriority::Critical, event("alert"), 256),
        PushOutcome::Enqueued
    );

    let stats = filter.stats();
    assert_eq!(stats.dropped_total, 0);
    assert_eq!(stats.spilled_total, 1);
    assert_eq!(stats.critical, 1);
}

#[test]
fn test_zero_critical_dropped_under_saturation() {
    // Budget for a single event; flood with criticals.
    let filter = PriorityBackpressureFilter::<MeterEvent>::with_in_memory_spill(256, 1_000);
    for i in 0..1_000 {
        let outcome = filter.push(EventPriority::Critical, event(&format!("crit-{i}")), 256);
        assert_eq!(outcome, PushOutcome::Enqueued);
    }
    let stats = filter.stats();
    assert_eq!(stats.dropped_total, 0);
    assert_eq!(stats.critical, 1_000);
}

#[test]
fn test_spill_and_drain_recovers_all() {
    let filter = PriorityBackpressureFilter::<MeterEvent>::with_in_memory_spill(512, 1_000);
    filter.push(EventPriority::Normal, event("a"), 256);
    filter.push(EventPriority::Normal, event("b"), 256);
    for i in 0..5 {
        filter.push(EventPriority::Normal, event(&format!("spill-{i}")), 256);
    }
    assert_eq!(filter.stats().spill_backlog, 5);

    let mut recovered = 0;
    // Drain by alternating pops (free memory) and spill replays.
    while filter.len() > 0 || filter.stats().spill_backlog > 0 {
        filter.drain_spill().unwrap();
        if filter.pop().is_some() {
            recovered += 1;
        } else if filter.stats().spill_backlog == 0 {
            break;
        }
    }
    assert_eq!(recovered, 7, "all 7 events recovered");
    assert_eq!(filter.stats().spill_backlog, 0);
}

#[test]
fn test_priority_delivery_during_saturation() {
    // 4-event budget; 1000 events, 5% critical.
    let filter = PriorityBackpressureFilter::<MeterEvent>::with_in_memory_spill(256 * 4, 100_000);
    for i in 0..1_000 {
        if i % 20 == 0 {
            assert_ne!(
                filter.push(EventPriority::Critical, event(&format!("crit-{i}")), 256),
                PushOutcome::Dropped
            );
        } else {
            filter.push(EventPriority::Normal, event(&format!("norm-{i}")), 256);
        }
    }

    let mut critical_delivered = 0;
    let mut total = 0;
    loop {
        filter.drain_spill().unwrap();
        match filter.pop() {
            Some(ev) => {
                if ev.meter_id.starts_with("crit-") {
                    critical_delivered += 1;
                }
                total += 1;
            }
            None if filter.stats().spill_backlog == 0 => break,
            None => {}
        }
    }
    assert_eq!(critical_delivered, 50, "all critical events delivered");
    assert_eq!(total, 1_000, "no events lost");
    assert_eq!(filter.stats().dropped_total, 0);
}

#[derive(Debug, PartialEq, Eq)]
struct Reading {
    id: u32,
    value: u64,
}

impl SpillCodec for Reading {
    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.id.to_le_bytes());
        out.extend_from_slice(&self.value.to_le_bytes());
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let id = u32::from_le_bytes(bytes.get(0..4)?.try_into().ok()?);
        let value = u64::from_le_bytes(bytes.get(4..12)?.try_into().ok()?);
        Some(Reading { id, value })
    }
}

#[test]
fn test_file_spill_store_fifo_roundtrip() {
    let path = std::env::temp_dir().join(format!("util_spill_{}.log", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let store = FileSpillStore::<Reading>::open(&path).expect("open spill file");
    store.push(Reading { id: 1, value: 10 }).unwrap();
    store.push(Reading { id: 2, value: 20 }).unwrap();
    assert_eq!(store.len(), 2);

    assert_eq!(store.pop().unwrap(), Some(Reading { id: 1, value: 10 }));
    assert_eq!(store.pop().unwrap(), Some(Reading { id: 2, value: 20 }));
    assert!(store.pop().unwrap().is_none());
    assert!(store.is_empty());

    let _ = std::fs::remove_file(&path);
}
