use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

pub struct PhiAccrualFailureDetector {
    threshold: f64,
    min_std_dev: f64,
    history_size: usize,
    samples: HashMap<String, Vec<Duration>>,
    last_heartbeat: HashMap<String, Instant>,
}

impl PhiAccrualFailureDetector {
    pub fn new(threshold: f64, history_size: usize) -> Self {
        Self {
            threshold,
            min_std_dev: 0.5,
            history_size,
            samples: HashMap::new(),
            last_heartbeat: HashMap::new(),
        }
    }

    pub fn heartbeat(&mut self, node_id: &str) {
        let now = Instant::now();
        if let Some(last) = self.last_heartbeat.insert(node_id.to_string(), now) {
            let delta = now.duration_since(last);
            let samples = self.samples.entry(node_id.to_string()).or_default();
            samples.push(delta);
            if samples.len() > self.history_size {
                samples.remove(0);
            }
        }
    }

    pub fn phi(&self, node_id: &str, now: Instant) -> f64 {
        let last = match self.last_heartbeat.get(node_id) {
            Some(last) => last,
            None => return 0.0,
        };

        let samples = match self.samples.get(node_id) {
            Some(samples) if !samples.is_empty() => samples,
            _ => return 0.0,
        };

        let diff = now.duration_since(*last).as_secs_f64();
        let mean = samples.iter().map(|d| d.as_secs_f64()).sum::<f64>() / samples.len() as f64;
        let variance = samples.iter().map(|d| (d.as_secs_f64() - mean).powi(2)).sum::<f64>() / samples.len() as f64;
        let std_dev = variance.sqrt().max(self.min_std_dev);

        let exponent = -(diff - mean) / std_dev;
        if exponent > 0.0 {
            return 0.0;
        }

        // Simplified phi calculation
        -(exponent / 1.0f64.exp()).log10()
    }

    pub fn is_available(&self, node_id: &str, now: Instant) -> bool {
        self.phi(node_id, now) < self.threshold
    }
}

pub mod proto {
    pub mod watermark {
        tonic::include_proto!("utility.watermark");
    }
    pub mod gossip {
        tonic::include_proto!("utility.gossip");
    }
}

use crate::ingestion::watermark::{WatermarkVector, HlcTimestamp, WatermarkEntry};

pub struct GossipService {
    node_id: String,
    failure_detector: Arc<RwLock<PhiAccrualFailureDetector>>,
    watermark_vector: Arc<RwLock<WatermarkVector>>,
    last_sent_epoch: u64,
}

impl GossipService {
    pub fn new(node_id: String, watermark_vector: Arc<RwLock<WatermarkVector>>) -> Self {
        Self {
            node_id,
            failure_detector: Arc::new(RwLock::new(PhiAccrualFailureDetector::new(3.0, 100))),
            watermark_vector,
            last_sent_epoch: 0,
        }
    }

    pub fn create_gossip_message(&mut self) -> proto::gossip::GossipMessage {
        let wv = self.watermark_vector.read().unwrap();
        let current_epoch = wv.epoch.load(std::sync::atomic::Ordering::SeqCst);

        let mut delta = proto::watermark::WatermarkVector {
            entries: HashMap::new(),
        };

        for (count, (id, entry)) in wv.entries.iter().enumerate() {
            if count >= 1000 { break; }
            delta.entries.insert(*id, proto::watermark::WatermarkEntry {
                hlc: Some(proto::watermark::HlcTimestamp { value: entry.hlc.as_u64() }),
                offset: entry.offset,
            });
        }

        self.last_sent_epoch = current_epoch;

        proto::gossip::GossipMessage {
            node_id: self.node_id.clone(),
            heartbeat: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
            watermark_delta: Some(delta),
        }
    }

    pub fn handle_gossip_message(&self, msg: proto::gossip::GossipMessage) -> Vec<u32> {
        self.failure_detector.write().unwrap().heartbeat(&msg.node_id);

        if let Some(delta) = msg.watermark_delta {
            let mut wv = self.watermark_vector.write().unwrap();
            let mut other_wv = WatermarkVector::new();
            for (id, entry) in delta.entries {
                other_wv.entries.insert(id, WatermarkEntry {
                    hlc: HlcTimestamp::from(entry.hlc.map(|h| h.value).unwrap_or(0)),
                    offset: entry.offset,
                });
            }
            return wv.merge(&other_wv);
        }
        Vec::new()
    }
}
