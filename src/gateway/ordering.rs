use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::gateway::hlc::{HlcTimestamp, HybridLogicalClock};
use crate::gateway::stream::MeterEvent;

const MAX_CLOCK_SKEW_MS: u64 = 200;

#[derive(Debug, Clone)]
pub struct OrderedEvent {
    pub event: MeterEvent,
    pub hlc: HlcTimestamp,
}

impl Eq for OrderedEvent {}

impl PartialEq for OrderedEvent {
    fn eq(&self, other: &Self) -> bool {
        self.hlc == other.hlc
    }
}

impl PartialOrd for OrderedEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse: BinaryHeap is max-heap, we want min-heap (earliest HLC first)
        other.hlc.cmp(&self.hlc)
    }
}

pub struct CausalOrderer {
    buffer: HashMap<String, BinaryHeap<OrderedEvent>>,
    tx: mpsc::Sender<MeterEvent>,
    hlc: Arc<HybridLogicalClock>,
    max_clock_skew_ms: u64,
}

impl CausalOrderer {
    pub fn new(tx: mpsc::Sender<MeterEvent>, hlc: Arc<HybridLogicalClock>) -> Self {
        Self {
            buffer: HashMap::new(),
            tx,
            hlc,
            max_clock_skew_ms: MAX_CLOCK_SKEW_MS,
        }
    }

    pub fn with_skew(
        tx: mpsc::Sender<MeterEvent>,
        hlc: Arc<HybridLogicalClock>,
        max_clock_skew_ms: u64,
    ) -> Self {
        Self {
            buffer: HashMap::new(),
            tx,
            hlc,
            max_clock_skew_ms,
        }
    }

    pub fn push(&mut self, mut event: MeterEvent) {
        let hlc_ts = if event.hlc_timestamp != 0 {
            let ts = event.hlc();
            self.hlc.update(ts);
            ts
        } else {
            let ts = self.hlc.tick(event.timestamp as u64);
            event.hlc_timestamp = ts.0;
            ts
        };
        let ordered = OrderedEvent { hlc: hlc_ts, event };
        let source = self.source_id(&ordered);
        self.buffer.entry(source).or_default().push(ordered);
    }

    pub fn flush_ready(&mut self) -> Vec<MeterEvent> {
        let max_physical = self.hlc.get_current().physical();
        let cutoff = max_physical.saturating_sub(self.max_clock_skew_ms);
        let mut ready = Vec::new();
        let sources: Vec<String> = self.buffer.keys().cloned().collect();
        for source in sources {
            let heap = match self.buffer.get_mut(&source) {
                Some(h) => h,
                None => continue,
            };
            while let Some(top) = heap.peek() {
                let event_wall = top.event.timestamp as u64;
                // An event is ready once it has aged past the skew window:
                // either its assigned HLC is old enough, or its original
                // wall-clock timestamp marks it as a straggler that was
                // re-stamped at the current time on arrival.
                if top.hlc.physical() <= cutoff || event_wall <= cutoff {
                    ready.push(heap.pop().unwrap().event);
                } else {
                    break;
                }
            }
            if heap.is_empty() {
                self.buffer.remove(&source);
            }
        }
        ready
    }

    pub async fn flush_to_channel(&mut self) {
        let ready = self.flush_ready();
        for event in ready {
            if self.tx.send(event).await.is_err() {
                tracing::warn!("ordering downstream channel closed");
                return;
            }
        }
    }

    async fn flush_loop(mut self) {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(50));
        loop {
            interval.tick().await;
            self.flush_to_channel().await;
        }
    }

    pub fn spawn(self) {
        tokio::spawn(async move {
            self.flush_loop().await;
        });
    }

    fn source_id(&self, event: &OrderedEvent) -> String {
        event.event.meter_id.clone()
    }
}

pub fn ordered_buffer(
    capacity: usize,
    hlc: Arc<HybridLogicalClock>,
) -> (CausalOrderer, mpsc::Receiver<MeterEvent>) {
    let (tx, rx) = mpsc::channel(capacity);
    let orderer = CausalOrderer::new(tx, hlc);
    (orderer, rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::hlc::HybridLogicalClock;
    use std::sync::Arc;

    #[test]
    fn test_ordered_event_ordering() {
        let a = OrderedEvent {
            hlc: HlcTimestamp::new(100, 0),
            event: MeterEvent {
                meter_id: "M1".into(),
                timestamp: 100,
                reading: 1.0,
                token_volume: 0,
                hlc_timestamp: 0,
            },
        };
        let b = OrderedEvent {
            hlc: HlcTimestamp::new(100, 1),
            event: MeterEvent {
                meter_id: "M1".into(),
                timestamp: 100,
                reading: 2.0,
                token_volume: 0,
                hlc_timestamp: 0,
            },
        };
        let c = OrderedEvent {
            hlc: HlcTimestamp::new(200, 0),
            event: MeterEvent {
                meter_id: "M1".into(),
                timestamp: 200,
                reading: 3.0,
                token_volume: 0,
                hlc_timestamp: 0,
            },
        };
        let mut heap = BinaryHeap::new();
        heap.push(c.clone());
        heap.push(b.clone());
        heap.push(a.clone());
        assert_eq!(heap.pop().unwrap().hlc, a.hlc);
        assert_eq!(heap.pop().unwrap().hlc, b.hlc);
        assert_eq!(heap.pop().unwrap().hlc, c.hlc);
    }

    #[test]
    fn test_flush_ready_respects_skew() {
        let hlc = Arc::new(HybridLogicalClock::new());
        let (tx, _rx) = mpsc::channel(100);
        let mut orderer = CausalOrderer::with_skew(tx, hlc.clone(), 200);
        // Push event while the HLC is at 500.
        orderer.push(MeterEvent {
            meter_id: "M1".into(),
            timestamp: 500,
            reading: 1.0,
            token_volume: 0,
            hlc_timestamp: 0,
        });
        // Advance the HLC past the skew window; the old event should flush.
        hlc.tick(1000);
        let ready = orderer.flush_ready();
        assert!(
            !ready.is_empty(),
            "event at 500 should be ready when HLC is at 1000 with skew 200"
        );
    }

    #[test]
    fn test_flush_ready_holds_future_events() {
        let hlc = Arc::new(HybridLogicalClock::new());
        let (tx, _rx) = mpsc::channel(100);
        let mut orderer = CausalOrderer::with_skew(tx, hlc.clone(), 200);
        // Tick HLC to 1000
        hlc.tick(1000);
        // Push event at close to current HLC time
        orderer.push(MeterEvent {
            meter_id: "M1".into(),
            timestamp: 1100,
            reading: 1.0,
            token_volume: 0,
            hlc_timestamp: 0,
        });
        let ready = orderer.flush_ready();
        assert!(
            ready.is_empty(),
            "event at 1100 should be held when HLC is at 1000 with skew 200"
        );
    }
}
