use super::{
    drift_estimator::KalmanClockState,
    tai64n::{ClockCorrection, Tai64N},
};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct ClockCorrector {
    state: Arc<Mutex<KalmanClockState>>,
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
