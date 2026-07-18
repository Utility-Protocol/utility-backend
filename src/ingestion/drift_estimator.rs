use serde::{Deserialize, Serialize};
use std::{fs, io, path::Path};

pub const DEFAULT_Q_OFFSET: f64 = 1e-9;
pub const DEFAULT_Q_DRIFT: f64 = 1e-12;
pub const DEFAULT_R: f64 = 1e-6;
pub const DEFAULT_MAX_CORRECTION_PPM: f64 = 500.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalmanClockState {
    pub offset_seconds: f64,
    pub drift_seconds_per_second: f64,
    pub covariance: [[f64; 2]; 2],
    pub q_offset: f64,
    pub q_drift: f64,
    pub r: f64,
    pub last_rtt_ms: Option<f64>,
}

impl Default for KalmanClockState {
    fn default() -> Self {
        Self {
            offset_seconds: 0.0,
            drift_seconds_per_second: 0.0,
            covariance: [[1.0, 0.0], [0.0, 1.0]],
            q_offset: DEFAULT_Q_OFFSET,
            q_drift: DEFAULT_Q_DRIFT,
            r: DEFAULT_R,
            last_rtt_ms: None,
        }
    }
}

impl KalmanClockState {
    pub fn predict(&mut self, dt_seconds: f64) {
        let capped_drift = self.drift_seconds_per_second.clamp(
            -DEFAULT_MAX_CORRECTION_PPM / 1_000_000.0,
            DEFAULT_MAX_CORRECTION_PPM / 1_000_000.0,
        );
        self.offset_seconds += capped_drift * dt_seconds;
        let p = self.covariance;
        self.covariance = [
            [
                p[0][0]
                    + dt_seconds * (p[1][0] + p[0][1])
                    + dt_seconds * dt_seconds * p[1][1]
                    + self.q_offset,
                p[0][1] + dt_seconds * p[1][1],
            ],
            [p[1][0] + dt_seconds * p[1][1], p[1][1] + self.q_drift],
        ];
    }

    pub fn update(&mut self, measured_offset_seconds: f64) {
        let innovation = measured_offset_seconds - self.offset_seconds;
        let s = self.covariance[0][0] + self.r;
        let k0 = self.covariance[0][0] / s;
        let k1 = self.covariance[1][0] / s;
        self.offset_seconds += k0 * innovation;
        self.drift_seconds_per_second += k1 * innovation;
        let p = self.covariance;
        self.covariance = [
            [(1.0 - k0) * p[0][0], (1.0 - k0) * p[0][1]],
            [p[1][0] - k1 * p[0][0], p[1][1] - k1 * p[0][1]],
        ];
    }

    pub fn update_cross_collector(&mut self, offset_seconds: f64, rtt_ms: f64) -> bool {
        self.last_rtt_ms = Some(rtt_ms);
        let sigma = self.covariance[0][0].sqrt();
        if rtt_ms <= 10.0 && (offset_seconds - self.offset_seconds).abs() <= 3.0 * sigma.max(1e-6) {
            self.update(offset_seconds);
            true
        } else {
            false
        }
    }

    pub fn correction_ns(&self) -> i64 {
        crate::ingestion::tai64n::correction_for_offset_seconds(self.offset_seconds)
    }

    pub fn drift_ppm(&self) -> f64 {
        self.drift_seconds_per_second * 1_000_000.0
    }

    pub fn save<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let bytes = serde_json::to_vec(self).map_err(io::Error::other)?;
        fs::write(path, bytes)
    }

    pub fn load<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let bytes = fs::read(path)?;
        serde_json::from_slice(&bytes).map_err(io::Error::other)
    }
}
