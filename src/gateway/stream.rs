use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::gateway::hlc::{HlcTimestamp, HybridLogicalClock};

pub struct MeterEvent {
    pub meter_id: String,
    pub timestamp: i64,
    pub reading: f64,
    pub token_volume: u64,
    pub hlc_timestamp: u64,
}

impl MeterEvent {
    pub fn hlc(&self) -> HlcTimestamp {
        HlcTimestamp(self.hlc_timestamp)
    }
}

#[allow(dead_code)]
pub struct BackpressureFilter {
    buffer_capacity: usize,
    tx: mpsc::Sender<MeterEvent>,
    hlc: Arc<HybridLogicalClock>,
}

impl BackpressureFilter {
    pub fn new(capacity: usize, hlc: Arc<HybridLogicalClock>) -> (Self, mpsc::Receiver<MeterEvent>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self {
                buffer_capacity: capacity,
                tx,
                hlc,
            },
            rx,
        )
    }

    pub async fn push(&self, mut event: MeterEvent) -> Result<(), &'static str> {
        if event.hlc_timestamp != 0 {
            self.hlc.update(event.hlc());
        }
        let hlc_ts = self.hlc.tick(event.timestamp as u64);
        event.hlc_timestamp = hlc_ts.0;
        self.tx
            .send(event)
            .await
            .map_err(|_| "backpressure buffer full: dropping event")
    }
}

pub async fn ingest_stream(
    filter: Arc<BackpressureFilter>,
    mut stream: impl tokio_stream::Stream<Item = Result<Bytes, std::io::Error>> + Unpin,
) {
    use tokio_stream::StreamExt;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(data) => {
                info!(len = data.len(), "received meter datagram");
                let wall_clock = chrono::Utc::now().timestamp_millis();
                let event = MeterEvent {
                    meter_id: String::from("unknown"),
                    timestamp: wall_clock,
                    reading: 0.0,
                    token_volume: 0,
                    hlc_timestamp: 0,
                };
                if let Err(e) = filter.push(event).await {
                    warn!("{}", e);
                }
            }
            Err(e) => warn!(error = %e, "stream read error"),
        }
    }
}
