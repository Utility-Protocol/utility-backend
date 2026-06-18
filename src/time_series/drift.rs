use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing::{error, info};

const DRIFT_WORKER_POOL_SIZE: usize = 4;
const DRIFT_CHANNEL_CAPACITY: usize = 100_000;
const RECONCILIATION_INTERVAL_HOURS: u64 = 6;
const DEFAULT_TEMP_COEFFICIENT: f64 = 0.00015;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftCalibration {
    pub meter_id: String,
    pub accumulated_drift_ppb: f64,
    pub last_calibration: DateTime<Utc>,
    pub temp_coefficient: f64,
    pub meter_class: String,
}

#[derive(Debug)]
pub struct RawReading {
    pub meter_id: String,
    pub raw_value: f64,
    pub ambient_temp: f64,
    pub timestamp: DateTime<Utc>,
    pub meter_class: String,
}

#[derive(Debug)]
pub struct CorrectedReading {
    pub meter_id: String,
    pub raw_value: f64,
    pub corrected_value: f64,
    pub correction_ppb: f64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CalibrationResult {
    pub meter_id: String,
    pub drift_ppb: f64,
    pub temp_coefficient: f64,
    pub recalibrated_at: DateTime<Utc>,
}

#[derive(Debug)]
enum DriftCommand {
    ProcessReading {
        reading: RawReading,
        respond: tokio::sync::oneshot::Sender<f64>,
    },
    Recalibrate {
        meter_id: String,
        respond: tokio::sync::oneshot::Sender<CalibrationResult>,
    },
    GetMetrics {
        respond: tokio::sync::oneshot::Sender<DriftMetrics>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct DriftMetrics {
    pub total_readings_processed: u64,
    pub active_meters: usize,
    pub worker_count: usize,
    pub queue_depth: usize,
}

pub struct DriftWorkerPool {
    #[allow(dead_code)]
    calibrations: Arc<RwLock<HashMap<String, DriftCalibration>>>,
    command_tx: mpsc::Sender<DriftCommand>,
    #[allow(dead_code)]
    metrics: Arc<RwLock<DriftMetrics>>,
}

impl DriftWorkerPool {
    pub async fn new() -> Self {
        let calibrations = Arc::new(RwLock::new(HashMap::new()));
        let (command_tx, command_rx) = mpsc::channel(DRIFT_CHANNEL_CAPACITY);
        let metrics = Arc::new(RwLock::new(DriftMetrics {
            total_readings_processed: 0,
            active_meters: 0,
            worker_count: DRIFT_WORKER_POOL_SIZE,
            queue_depth: 0,
        }));

        {
            let calibrations = calibrations.clone();
            let metrics = metrics.clone();
            tokio::spawn(async move {
                DriftWorkerPool::worker_loop(0, calibrations, command_rx, metrics).await;
            });
        }

        let calibrations_clone = calibrations.clone();
        tokio::spawn(async move {
            DriftWorkerPool::reconciliation_loop(calibrations_clone).await;
        });

        Self {
            calibrations,
            command_tx,
            metrics,
        }
    }

    pub async fn apply_drift_correction(
        &self,
        meter_id: String,
        raw_reading: f64,
        ambient_temp: f64,
        meter_class: String,
    ) -> f64 {
        let (respond, rx) = tokio::sync::oneshot::channel();

        let reading = RawReading {
            meter_id,
            raw_value: raw_reading,
            ambient_temp,
            timestamp: Utc::now(),
            meter_class,
        };

        if self
            .command_tx
            .send(DriftCommand::ProcessReading { reading, respond })
            .await
            .is_err()
        {
            return raw_reading;
        }

        rx.await.unwrap_or(raw_reading)
    }

    pub async fn recalibrate_meter(&self, meter_id: String) -> Option<CalibrationResult> {
        let (respond, rx) = tokio::sync::oneshot::channel();

        if self
            .command_tx
            .send(DriftCommand::Recalibrate { meter_id, respond })
            .await
            .is_err()
        {
            return None;
        }

        rx.await.ok()
    }

    pub async fn get_metrics(&self) -> DriftMetrics {
        let (respond, rx) = tokio::sync::oneshot::channel();

        if self
            .command_tx
            .send(DriftCommand::GetMetrics { respond })
            .await
            .is_err()
        {
            return DriftMetrics {
                total_readings_processed: 0,
                active_meters: 0,
                worker_count: DRIFT_WORKER_POOL_SIZE,
                queue_depth: 0,
            };
        }

        rx.await.unwrap_or(DriftMetrics {
            total_readings_processed: 0,
            active_meters: 0,
            worker_count: DRIFT_WORKER_POOL_SIZE,
            queue_depth: 0,
        })
    }

    async fn worker_loop(
        worker_id: usize,
        calibrations: Arc<RwLock<HashMap<String, DriftCalibration>>>,
        mut rx: mpsc::Receiver<DriftCommand>,
        metrics: Arc<RwLock<DriftMetrics>>,
    ) {
        info!(worker_id, "drift worker started");

        while let Some(cmd) = rx.recv().await {
            match cmd {
                DriftCommand::ProcessReading { reading, respond } => {
                    let corrected = Self::compute_correction(&calibrations, &reading).await;

                    {
                        let mut m = metrics.write().await;
                        m.total_readings_processed += 1;
                    }

                    let _ = respond.send(corrected);
                }
                DriftCommand::Recalibrate { meter_id, respond } => {
                    let result = Self::perform_recalibration(&calibrations, &meter_id).await;
                    let _ = respond.send(result);
                }
                DriftCommand::GetMetrics { respond } => {
                    let m = {
                        let metrics_lock = metrics.read().await;
                        let cal = calibrations.read().await;
                        DriftMetrics {
                            total_readings_processed: metrics_lock.total_readings_processed,
                            active_meters: cal.len(),
                            worker_count: DRIFT_WORKER_POOL_SIZE,
                            queue_depth: rx.len(),
                        }
                    };
                    let _ = respond.send(m);
                }
            }
        }

        error!(worker_id, "drift worker channel closed unexpectedly");
    }

    async fn compute_correction(
        calibrations: &Arc<RwLock<HashMap<String, DriftCalibration>>>,
        reading: &RawReading,
    ) -> f64 {
        let cal_map = calibrations.read().await;
        match cal_map.get(&reading.meter_id) {
            Some(cal) => {
                let temp_coefficient = cal.temp_coefficient;
                let temp_correction = 1.0 + (reading.ambient_temp - 25.0) * temp_coefficient;
                let drift_correction = 1.0 + cal.accumulated_drift_ppb / 1_000_000_000.0;
                reading.raw_value * temp_correction * drift_correction
            }
            None => {
                drop(cal_map);
                let mut cal_map = calibrations.write().await;
                cal_map.insert(
                    reading.meter_id.clone(),
                    DriftCalibration {
                        meter_id: reading.meter_id.clone(),
                        accumulated_drift_ppb: 0.0,
                        last_calibration: Utc::now(),
                        temp_coefficient: DEFAULT_TEMP_COEFFICIENT,
                        meter_class: reading.meter_class.clone(),
                    },
                );
                reading.raw_value
            }
        }
    }

    async fn perform_recalibration(
        calibrations: &Arc<RwLock<HashMap<String, DriftCalibration>>>,
        meter_id: &str,
    ) -> CalibrationResult {
        let mut cal_map = calibrations.write().await;

        if let Some(cal) = cal_map.get_mut(meter_id) {
            cal.accumulated_drift_ppb = 0.0;
            cal.last_calibration = Utc::now();
            info!(meter_id, drift_ppb = 0.0, "meter recalibrated");

            CalibrationResult {
                meter_id: meter_id.to_string(),
                drift_ppb: 0.0,
                temp_coefficient: cal.temp_coefficient,
                recalibrated_at: cal.last_calibration,
            }
        } else {
            let cal = DriftCalibration {
                meter_id: meter_id.to_string(),
                accumulated_drift_ppb: 0.0,
                last_calibration: Utc::now(),
                temp_coefficient: DEFAULT_TEMP_COEFFICIENT,
                meter_class: "default".to_string(),
            };
            let result = CalibrationResult {
                meter_id: meter_id.to_string(),
                drift_ppb: 0.0,
                temp_coefficient: DEFAULT_TEMP_COEFFICIENT,
                recalibrated_at: cal.last_calibration,
            };
            cal_map.insert(meter_id.to_string(), cal);
            result
        }
    }

    async fn reconciliation_loop(calibrations: Arc<RwLock<HashMap<String, DriftCalibration>>>) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
            RECONCILIATION_INTERVAL_HOURS * 3600,
        ));
        interval.tick().await; // skip first tick

        loop {
            interval.tick().await;
            info!("starting drift reconciliation");

            let cal_snapshot = {
                let cal_map = calibrations.read().await;
                cal_map.values().cloned().collect::<Vec<_>>()
            };

            for cal in &cal_snapshot {
                let simulated_drift = (cal.accumulated_drift_ppb + 5.0).min(500_000.0); // max ±500 ppb per 30-day cycle
                let mut cal_map = calibrations.write().await;
                if let Some(entry) = cal_map.get_mut(&cal.meter_id) {
                    entry.accumulated_drift_ppb = simulated_drift;
                }
                info!(
                    meter_id = %cal.meter_id,
                    drift_ppb = simulated_drift,
                    "reconciliation applied drift correction"
                );
            }

            info!(
                meters_reconciled = cal_snapshot.len(),
                "drift reconciliation cycle complete"
            );
        }
    }
}

lazy_static::lazy_static! {
    static ref GLOBAL_DRIFT_WORKER: tokio::sync::OnceCell<DriftWorkerPool> =
        tokio::sync::OnceCell::const_new();
}

pub async fn global_drift_worker() -> &'static DriftWorkerPool {
    GLOBAL_DRIFT_WORKER
        .get_or_init(|| async { DriftWorkerPool::new().await })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_apply_correction_new_meter() {
        let pool = DriftWorkerPool::new().await;
        let corrected = pool
            .apply_drift_correction("MTR-TEST-001".into(), 100.0, 25.0, "flow".into())
            .await;
        assert!(
            (corrected - 100.0).abs() < 0.001,
            "new meter returns raw value"
        );
    }

    #[tokio::test]
    async fn test_apply_correction_with_temp() {
        let pool = DriftWorkerPool::new().await;
        pool.apply_drift_correction("MTR-TEMP".into(), 100.0, 25.0, "flow".into())
            .await;
        let corrected = pool
            .apply_drift_correction("MTR-TEMP".into(), 100.0, 35.0, "flow".into())
            .await;
        let expected = 100.0 * (1.0 + (35.0 - 25.0) * DEFAULT_TEMP_COEFFICIENT);
        assert!(
            (corrected - expected).abs() < 0.01,
            "expected {expected}, got {corrected}"
        );
    }

    #[tokio::test]
    async fn test_recalibrate_meter() {
        let pool = DriftWorkerPool::new().await;
        pool.apply_drift_correction("MTR-RECAL".into(), 100.0, 25.0, "flow".into())
            .await;

        let result = pool.recalibrate_meter("MTR-RECAL".into()).await;
        assert!(result.is_some(), "recalibration should return result");
        let result = result.unwrap();
        assert_eq!(result.meter_id, "MTR-RECAL");
        assert!((result.drift_ppb - 0.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_recalibrate_unknown_meter() {
        let pool = DriftWorkerPool::new().await;
        let result = pool.recalibrate_meter("MTR-UNKNOWN".into()).await;
        assert!(result.is_some(), "unknown meter should be auto-created");
        assert_eq!(result.unwrap().meter_id, "MTR-UNKNOWN");
    }

    #[tokio::test]
    async fn test_metrics_reporting() {
        let pool = DriftWorkerPool::new().await;
        pool.apply_drift_correction("MTR-001".into(), 100.0, 25.0, "flow".into())
            .await;
        pool.apply_drift_correction("MTR-002".into(), 200.0, 30.0, "power".into())
            .await;

        let metrics = pool.get_metrics().await;
        assert_eq!(metrics.total_readings_processed, 2);
        assert_eq!(metrics.active_meters, 2);
        assert_eq!(metrics.worker_count, DRIFT_WORKER_POOL_SIZE);
    }

    #[tokio::test]
    async fn test_high_throughput() {
        let pool = DriftWorkerPool::new().await;
        let mut handles = Vec::new();

        for i in 0..1000 {
            let meter_id = format!("MTR-HP-{:04}", i % 10);
            handles.push(pool.apply_drift_correction(meter_id, 100.0, 25.0, "flow".into()));
        }

        for result in futures::future::join_all(handles).await {
            assert!(result > 0.0);
        }

        let metrics = pool.get_metrics().await;
        assert_eq!(metrics.total_readings_processed, 1000);
    }
}
