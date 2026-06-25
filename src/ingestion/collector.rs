use super::{
    drift_estimator::KalmanClockState,
    tai64n::{ClockCorrection, Tai64N},
    watermark::{HlcTimestamp, WatermarkVector},
};
use rocksdb::{Options, DB};
use std::path::Path;
use std::sync::RwLock;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct ClockCorrector {
    state: Arc<Mutex<KalmanClockState>>,
}

impl Default for ClockCorrector {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(KalmanClockState::default())),
        }
    }
}

impl ClockCorrector {
    pub fn new(state: KalmanClockState) -> Self {
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    pub fn apply_ntp_sample(&self, measured_offset_seconds: f64, dt_seconds: f64) {
        let mut state = self.state.lock().expect("clock state poisoned");
        state.predict(dt_seconds);
        state.update(measured_offset_seconds);
    }

    pub fn normalize_unix_ms(&self, unix_ms: i64) -> ClockCorrection {
        let correction_ns = self
            .state
            .lock()
            .expect("clock state poisoned")
            .correction_ns();
        ClockCorrection {
            correction_ns,
            timestamp_tai: Tai64N::from_unix_ms(unix_ms, correction_ns),
        }
    }

    pub fn state(&self) -> KalmanClockState {
        self.state.lock().expect("clock state poisoned").clone()
    }
}

pub struct Collector {
    pub clock_corrector: ClockCorrector,
    pub watermark_vector: Arc<RwLock<WatermarkVector>>,
    pub db: Arc<DB>,
}

impl Collector {
    pub fn new(db_path: impl AsRef<Path>) -> Self {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, db_path).expect("Failed to open RocksDB");

        let mut collector = Self {
            clock_corrector: ClockCorrector::default(),
            watermark_vector: Arc::new(RwLock::new(WatermarkVector::new())),
            db: Arc::new(db),
        };

        collector.load_watermarks();
        collector
    }

    pub fn acknowledge_batch(&self, source_id: u32, last_offset: u64) {
        {
            let mut wv = self.watermark_vector.write().unwrap();
            wv.update(source_id, last_offset);
        }

        // In a real implementation, we might trigger persistence here or periodically.
        // The requirement says: persist to local RocksDB every 10 seconds.
    }

    pub fn persist_watermarks(&self) {
        let wv = self.watermark_vector.read().unwrap();
        for (id, entry) in &wv.entries {
            let key = format!("wm:{}", id);
            let value = format!("{}:{}", entry.hlc.as_u64(), entry.offset);
            self.db
                .put(key.as_bytes(), value.as_bytes())
                .expect("Failed to persist watermark");
        }
    }

    fn load_watermarks(&mut self) {
        let mut wv = self.watermark_vector.write().unwrap();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for (key, value) in iter.flatten() {
            let key_str = String::from_utf8_lossy(&key);
            if let Some(id_str) = key_str.strip_prefix("wm:") {
                if let Ok(id) = id_str.parse::<u32>() {
                    let val_str = String::from_utf8_lossy(&value);
                    let parts: Vec<&str> = val_str.split(':').collect();
                    if parts.len() == 2 {
                        if let (Ok(hlc_val), Ok(offset)) =
                            (parts[0].parse::<u64>(), parts[1].parse::<u64>())
                        {
                            wv.entries.insert(
                                id,
                                crate::ingestion::watermark::WatermarkEntry {
                                    hlc: HlcTimestamp::from(hlc_val),
                                    offset,
                                },
                            );
                        }
                    }
                }
            }
        }
    }
}
