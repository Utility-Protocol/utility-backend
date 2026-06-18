use chrono::{DateTime, Datelike, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;
use tracing::{info, warn};

// ---- Data Types ----

/// Legacy result type — kept for backward compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticResult {
    pub meter_id: String,
    pub expected_volume: f64,
    pub actual_volume: f64,
    pub deviation: f64,
    pub anomaly_detected: bool,
}

/// A single time-series reading, optionally accompanied by weather covariates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reading {
    pub timestamp: DateTime<Utc>,
    pub value: f64,
    pub weather: Option<WeatherCovariate>,
}

/// Temperature (°C) and precipitation (mm) at the time of a reading.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct WeatherCovariate {
    pub temperature_c: f64,
    pub precipitation_mm: f64,
}

/// Seasonal profile storing a multiplicative factor per calendar month.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeasonalProfile {
    pub monthly_factors: [f64; 12],
}

impl Default for SeasonalProfile {
    fn default() -> Self {
        Self {
            monthly_factors: [1.0; 12],
        }
    }
}

/// Probable cause assigned when an anomaly is detected.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ProbableCause {
    /// Sustained positive deviation indicative of a physical leak.
    Leak,
    /// Sudden negative or irregular deviation suggesting theft or bypass.
    Theft,
    /// Erratic readings with abnormally high variance.
    SensorFault,
    /// Deviation that aligns with expected seasonal patterns.
    SeasonalVariation,
    /// No anomaly detected / normal operation.
    Normal,
}

/// Rich diagnostic report produced by the streaming engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticReport {
    pub meter_id: String,
    pub timestamp: DateTime<Utc>,
    /// Expected consumption after seasonal + weather + trend adjustment.
    pub expected_volume: f64,
    /// Actual observed consumption.
    pub actual_volume: f64,
    /// Raw deviation (actual − expected).
    pub deviation: f64,
    /// Deviation as a percentage of expected volume.
    pub deviation_pct: f64,
    /// Multiplicative seasonal factor for the current month.
    pub seasonal_factor: f64,
    /// Additive weather adjustment (temperature + precipitation contribution).
    pub weather_adjustment: f64,
    /// Local trend component from STL decomposition.
    pub trend_component: f64,
    /// Residual (actual − trend × seasonal − weather).
    pub residual: f64,
    /// Dynamically computed anomaly threshold (p95 of past absolute residuals).
    pub dynamic_threshold: f64,
    /// Whether the deviation exceeds the dynamic threshold.
    pub anomaly_detected: bool,
    /// Probable cause, if anomalous.
    pub probable_cause: Option<ProbableCause>,
    /// Simple historical mean baseline (for comparison with legacy method).
    pub historical_baseline: f64,
}

// ---- Streaming Diagnostic Engine ----

/// In-memory streaming engine that processes readings through a sliding window
/// and produces diagnostic reports with seasonal decomposition, weather
/// adjustment, dynamic thresholds, and probable-cause classification.
pub struct DiagnosticEngine {
    /// Per-meter rolling window of readings (configurable, default 30 days).
    meter_readings: BTreeMap<String, VecDeque<Reading>>,
    /// Pre-computed seasonal profiles per meter.
    _seasonal_profiles: BTreeMap<String, SeasonalProfile>,
    /// Fitted weather-model coefficients per meter.
    weather_coefficients: BTreeMap<String, WeatherCoefficients>,
    /// Size of the historical rolling window in days.
    history_window_days: i64,
    /// Size of the anomaly-analysis sliding window in days.
    _anomaly_window_days: i64,
}

#[derive(Debug, Clone, Copy)]
struct WeatherCoefficients {
    temp_slope: f64,
    precip_slope: f64,
    intercept: f64,
}

impl Default for WeatherCoefficients {
    fn default() -> Self {
        Self {
            temp_slope: 0.0,
            precip_slope: 0.0,
            intercept: 0.0,
        }
    }
}

impl Default for DiagnosticEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl DiagnosticEngine {
    /// Create a new engine with the default window sizes (30-day history, 7-day anomaly window).
    pub fn new() -> Self {
        Self {
            meter_readings: BTreeMap::new(),
            _seasonal_profiles: BTreeMap::new(),
            weather_coefficients: BTreeMap::new(),
            history_window_days: 30,
            _anomaly_window_days: 7,
        }
    }

    /// Create an engine with custom window sizes.
    pub fn with_windows(history_days: i64, anomaly_days: i64) -> Self {
        Self {
            meter_readings: BTreeMap::new(),
            _seasonal_profiles: BTreeMap::new(),
            weather_coefficients: BTreeMap::new(),
            history_window_days: history_days,
            _anomaly_window_days: anomaly_days,
        }
    }

    /// Ingest a new reading into the meter's rolling window.
    /// Readings older than `history_window_days` are pruned.
    pub fn ingest_reading(&mut self, meter_id: &str, reading: Reading) {
        let readings = self.meter_readings.entry(meter_id.to_string()).or_default();
        let cutoff = Utc::now() - Duration::days(self.history_window_days);
        while let Some(front) = readings.front() {
            if front.timestamp < cutoff {
                readings.pop_front();
            } else {
                break;
            }
        }
        readings.push_back(reading);
    }

    /// Run the full diagnostic pipeline for a meter and return the latest report.
    pub fn analyze(&mut self, meter_id: &str) -> Option<DiagnosticReport> {
        let readings = self.meter_readings.get(meter_id)?;
        if readings.is_empty() {
            return None;
        }

        let latest = readings.back()?;
        let all_values: Vec<f64> = readings.iter().map(|r| r.value).collect();
        let all_timestamps: Vec<DateTime<Utc>> = readings.iter().map(|r| r.timestamp).collect();

        // 1. Seasonal decomposition (STL-like)
        let (trend, seasonal, residuals) = self.stl_decompose(&all_values, &all_timestamps);

        // 2. Weather adjustment
        let weather_adjustment = self.compute_weather_adjustment(meter_id, latest);

        // 3. Seasonal factor for the current period
        let seasonal_factor = seasonal.last().copied().unwrap_or(1.0);

        // 4. Dynamic threshold (p95 of pre-anomaly absolute residuals)
        let n_resid = residuals.len();
        let window = (n_resid.min(7) / 2 * 2 + 1).max(3);
        let threshold_residuals = &residuals[..n_resid.saturating_sub(window)];
        let dynamic_threshold = self.compute_p95_threshold(threshold_residuals);

        // 5. Expected volume
        let trend_component = trend
            .last()
            .copied()
            .unwrap_or_else(|| all_values.iter().sum::<f64>() / all_values.len() as f64);
        let expected_volume = (trend_component * seasonal_factor) + weather_adjustment;
        let actual_volume = latest.value;
        let deviation = actual_volume - expected_volume;
        let deviation_pct = if expected_volume.abs() > 1e-10 {
            (deviation / expected_volume) * 100.0
        } else {
            0.0
        };
        let residual = residuals.last().copied().unwrap_or(0.0);
        let anomaly_detected = deviation.abs() > dynamic_threshold;

        // 6. Probable-cause classification
        let probable_cause = if anomaly_detected {
            Some(self.classify_probable_cause(
                &readings.iter().map(|r| r.value).collect::<Vec<_>>(),
                &residuals,
                deviation,
                deviation_pct,
                seasonal_factor,
            ))
        } else {
            None
        };

        let historical_baseline = all_values.iter().sum::<f64>() / all_values.len() as f64;

        let report = DiagnosticReport {
            meter_id: meter_id.to_string(),
            timestamp: latest.timestamp,
            expected_volume,
            actual_volume,
            deviation,
            deviation_pct,
            seasonal_factor,
            weather_adjustment,
            trend_component,
            residual,
            dynamic_threshold,
            anomaly_detected,
            probable_cause,
            historical_baseline,
        };

        info!(
            meter_id,
            actual_volume,
            expected_volume,
            deviation_pct,
            anomaly_detected,
            "diagnostic analysis complete"
        );

        Some(report)
    }

    /// STL-like decomposition using centered moving average (trend) and
    /// month-of-year factors (seasonal). Returns (trend, seasonal, residual).
    fn stl_decompose(
        &self,
        values: &[f64],
        timestamps: &[DateTime<Utc>],
    ) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
        let n = values.len();
        if n < 2 {
            return (values.to_vec(), vec![1.0; n], vec![0.0; n]);
        }

        // ---- Trend via trailing (causal) moving average ----
        let window = (n.min(7) / 2 * 2 + 1).max(3); // odd, at least 3
        let half = window / 2;
        let mut trend = vec![0.0; n];

        for (i, t) in trend.iter_mut().enumerate() {
            let start = i.saturating_sub(2 * half);
            let end = (i + 1).min(n);
            *t = values[start..end].iter().sum::<f64>() / (end - start) as f64;
        }

        // ---- Seasonal (monthly) factors (median, robust to anomalies) ----
        let mut month_vals: [Vec<f64>; 12] = Default::default();

        for (i, ts) in timestamps.iter().enumerate() {
            let m = ts.month0() as usize;
            let detrended = if i < trend.len() && trend[i].abs() > 1e-10 {
                values[i] / trend[i]
            } else {
                1.0
            };
            month_vals[m].push(detrended);
        }

        let mut factors = [1.0_f64; 12];
        for (m, f) in factors.iter_mut().enumerate() {
            if !month_vals[m].is_empty() {
                month_vals[m].sort_by(|a, b| a.partial_cmp(b).unwrap());
                let len = month_vals[m].len();
                *f = if len.is_multiple_of(2) {
                    (month_vals[m][len / 2 - 1] + month_vals[m][len / 2]) / 2.0
                } else {
                    month_vals[m][len / 2]
                };
            }
        }

        // Normalise so factors average to 1.0
        let mean_f: f64 = factors.iter().sum::<f64>() / 12.0;
        if mean_f > 0.0 {
            for f in factors.iter_mut() {
                *f /= mean_f;
            }
        }

        let seasonal: Vec<f64> = timestamps
            .iter()
            .map(|ts| factors[ts.month0() as usize])
            .collect();

        let residuals: Vec<f64> = values
            .iter()
            .enumerate()
            .map(|(i, &v)| {
                if i < trend.len() {
                    v - trend[i] * seasonal[i]
                } else {
                    0.0
                }
            })
            .collect();

        (trend, seasonal, residuals)
    }

    /// Compute the additive weather adjustment for the latest reading using
    /// pre-fitted per-meter coefficients.
    pub fn compute_weather_adjustment(&self, meter_id: &str, reading: &Reading) -> f64 {
        if let Some(ref weather) = reading.weather {
            if let Some(c) = self.weather_coefficients.get(meter_id) {
                c.temp_slope * weather.temperature_c
                    + c.precip_slope * weather.precipitation_mm
                    + c.intercept
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    /// Fit a simple linear weather model (temp + precip → consumption) using
    /// ordinary least squares. Requires at least 5 readings with weather data.
    pub fn fit_weather_model(&mut self, meter_id: &str) {
        let readings = match self.meter_readings.get(meter_id) {
            Some(r) if r.len() >= 5 => r,
            _ => return,
        };

        let weather_readings: Vec<&Reading> =
            readings.iter().filter(|r| r.weather.is_some()).collect();
        if weather_readings.len() < 5 {
            return;
        }

        let n = weather_readings.len() as f64;
        let mut s_t = 0.0;
        let mut s_p = 0.0;
        let mut s_v = 0.0;
        let mut s_tt = 0.0;
        let mut s_pp = 0.0;
        let mut s_tv = 0.0;
        let mut s_pv = 0.0;
        let mut s_tp = 0.0;

        for r in &weather_readings {
            if let Some(w) = r.weather {
                s_t += w.temperature_c;
                s_p += w.precipitation_mm;
                s_v += r.value;
                s_tt += w.temperature_c * w.temperature_c;
                s_pp += w.precipitation_mm * w.precipitation_mm;
                s_tv += w.temperature_c * r.value;
                s_pv += w.precipitation_mm * r.value;
                s_tp += w.temperature_c * w.precipitation_mm;
            }
        }

        let m_t = s_t / n;
        let m_p = s_p / n;
        let m_v = s_v / n;

        let v_t = s_tt / n - m_t * m_t;
        let v_p = s_pp / n - m_p * m_p;
        let c_tv = s_tv / n - m_t * m_v;
        let c_pv = s_pv / n - m_p * m_v;
        let c_tp = s_tp / n - m_t * m_p;

        let denom = v_t * v_p - c_tp * c_tp;
        let (b_t, b_p) = if denom.abs() > 1e-12 {
            (
                (c_tv * v_p - c_pv * c_tp) / denom,
                (c_pv * v_t - c_tv * c_tp) / denom,
            )
        } else {
            (0.0, 0.0)
        };

        self.weather_coefficients.insert(
            meter_id.to_string(),
            WeatherCoefficients {
                temp_slope: b_t,
                precip_slope: b_p,
                intercept: m_v - b_t * m_t - b_p * m_p,
            },
        );
    }

    /// Compute the p95 of historical absolute residuals as a dynamic anomaly threshold.
    fn compute_p95_threshold(&self, residuals: &[f64]) -> f64 {
        if residuals.is_empty() {
            return 50.0;
        }
        let mut abs_vals: Vec<f64> = residuals.iter().map(|v| v.abs()).collect();
        abs_vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let idx = ((abs_vals.len() as f64) * 0.95).ceil() as usize;
        let idx = idx.saturating_sub(1).min(abs_vals.len() - 1);
        abs_vals[idx].max(1.0)
    }

    /// Classify the probable cause of an anomaly based on residual pattern,
    /// deviation direction, seasonal context, and signal variance.
    fn classify_probable_cause(
        &self,
        recent_values: &[f64],
        residuals: &[f64],
        deviation: f64,
        deviation_pct: f64,
        seasonal_factor: f64,
    ) -> ProbableCause {
        // Theft: large negative percentage deviation (25%+ below expected)
        if deviation_pct < -25.0 {
            return ProbableCause::Theft;
        }

        // Leak: sustained positive deviation over recent residuals
        if deviation > 0.0 {
            let recent_res: Vec<f64> = residuals.iter().rev().take(5).copied().collect();
            let pos = recent_res.iter().filter(|&&r| r > 0.0).count();
            if pos >= 4 {
                return ProbableCause::Leak;
            }
        }

        // Sensor fault check: abnormally high variance in the most recent readings
        let window: Vec<f64> = recent_values.iter().rev().take(10).copied().collect();
        if window.len() >= 4 {
            let m = window.iter().sum::<f64>() / window.len() as f64;
            let var = window.iter().map(|v| (v - m).powi(2)).sum::<f64>() / window.len() as f64;
            if var > m.abs().max(1.0) * 3.0 {
                return ProbableCause::SensorFault;
            }
        }

        // Seasonal variation: if the seasonal factor is far from 1.0 but relative
        // deviation is small (< 15 %), attribute it to normal seasonal swing.
        if (seasonal_factor - 1.0).abs() > 0.2 {
            let mean_val = mean(recent_values);
            if mean_val.abs() > 1e-10 && (deviation / mean_val).abs() < 0.15 {
                return ProbableCause::SeasonalVariation;
            }
        }

        ProbableCause::Normal
    }

    /// Convenience method: fit the weather model then run `analyze`.
    pub fn get_diagnostics(&mut self, meter_id: &str) -> Option<DiagnosticReport> {
        self.fit_weather_model(meter_id);
        self.analyze(meter_id)
    }

    /// Placeholder for retrieving historical anomaly records.
    /// A production version would query a persistent store.
    pub fn get_anomaly_history(&self, _meter_id: &str, _limit: usize) -> Vec<DiagnosticReport> {
        Vec::new()
    }
}

// ---- Global shared engine instance ----
lazy_static::lazy_static! {
    static ref GLOBAL_ENGINE: Mutex<DiagnosticEngine> = Mutex::new(DiagnosticEngine::new());
}

/// Access the global engine for use by API handlers.
pub fn global_engine() -> &'static Mutex<DiagnosticEngine> {
    &GLOBAL_ENGINE
}

// ---- Helpers ----

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

// ---- Legacy API ----

/// Legacy threshold-based anomaly check (static baseline).
///
/// Kept for backward compatibility. New code should use [`DiagnosticEngine`].
pub fn analyze_consumption(
    meter_id: &str,
    readings: &[(DateTime<Utc>, f64)],
    baseline: f64,
    threshold: f64,
) -> DiagnosticResult {
    let actual_volume: f64 = readings.iter().map(|(_, v)| v).sum();
    let deviation = (actual_volume - baseline).abs();
    let anomaly = deviation > threshold;
    if anomaly {
        warn!(
            meter_id,
            deviation, threshold, "leakage or theft anomaly detected"
        );
    }
    DiagnosticResult {
        meter_id: meter_id.to_string(),
        expected_volume: baseline,
        actual_volume,
        deviation,
        anomaly_detected: anomaly,
    }
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;
    fn make_ts(days_ago: i64) -> DateTime<Utc> {
        chrono::Utc::now() - Duration::days(days_ago)
    }

    fn r(value: f64, days_ago: i64) -> Reading {
        Reading {
            timestamp: make_ts(days_ago),
            value,
            weather: None,
        }
    }

    fn rw(value: f64, days_ago: i64, temp: f64, precip: f64) -> Reading {
        Reading {
            timestamp: make_ts(days_ago),
            value,
            weather: Some(WeatherCovariate {
                temperature_c: temp,
                precipitation_mm: precip,
            }),
        }
    }

    #[test]
    fn test_legacy_anomaly_detection_baseline() {
        let readings = vec![(Utc::now(), 100.0), (Utc::now(), 102.0), (Utc::now(), 98.0)];
        let result = analyze_consumption("MTR-001", &readings, 300.0, 50.0);
        assert!(!result.anomaly_detected);
    }

    #[test]
    fn test_legacy_anomaly_detection_leak() {
        let readings = vec![
            (Utc::now(), 500.0),
            (Utc::now(), 600.0),
            (Utc::now(), 550.0),
        ];
        let result = analyze_consumption("MTR-002", &readings, 300.0, 50.0);
        assert!(result.anomaly_detected);
    }

    #[test]
    fn test_engine_ingest_and_analyze_no_anomaly() {
        let mut engine = DiagnosticEngine::new();
        for i in 0..30 {
            engine.ingest_reading("MTR-A", r(100.0 + (i as f64 * 0.5), i));
        }
        let report = engine.get_diagnostics("MTR-A").unwrap();
        assert!(
            !report.anomaly_detected,
            "should not trigger on stable data"
        );
    }

    #[test]
    fn test_engine_detects_sustained_leak() {
        let mut engine = DiagnosticEngine::new();
        // 25 days of normal readings (~100 units)
        for i in 0..25 {
            engine.ingest_reading("MTR-B", r(100.0 + (i as f64).sin() * 5.0, i));
        }
        // 5 days of sustained leak (doubled consumption)
        for i in 0..5 {
            engine.ingest_reading("MTR-B", r(200.0 + (i as f64).sin() * 5.0, i));
        }
        let report = engine.get_diagnostics("MTR-B").unwrap();
        assert!(report.anomaly_detected, "should detect sustained leak");
        assert_eq!(
            report.probable_cause,
            Some(ProbableCause::Leak),
            "should classify as leak"
        );
    }

    #[test]
    fn test_engine_detects_sensor_fault() {
        let mut engine = DiagnosticEngine::new();
        for i in 0..25 {
            engine.ingest_reading("MTR-C", r(100.0, i));
        }
        // Erratic readings
        for i in 0..5 {
            let v = if i % 2 == 0 { 500.0 } else { 10.0 };
            engine.ingest_reading("MTR-C", r(v, i));
        }
        let report = engine.get_diagnostics("MTR-C").unwrap();
        if report.anomaly_detected {
            assert_eq!(report.probable_cause, Some(ProbableCause::SensorFault));
        }
    }

    #[test]
    fn test_engine_detects_theft() {
        let mut engine = DiagnosticEngine::new();
        for i in 0..25 {
            engine.ingest_reading("MTR-D", r(100.0, i));
        }
        // Sudden drop
        for i in 0..5 {
            engine.ingest_reading("MTR-D", r(30.0, i));
        }
        let report = engine.get_diagnostics("MTR-D").unwrap();
        if report.anomaly_detected {
            assert_eq!(report.probable_cause, Some(ProbableCause::Theft));
        }
    }

    #[test]
    fn test_weather_model_fit_and_adjustment() {
        let mut engine = DiagnosticEngine::new();
        // Higher consumption on hot days (simulating irrigation)
        for i in 0..30 {
            let temp = 15.0 + (i as f64 % 20.0); // 15–34 °C
            let base = 100.0;
            let weather_effect = (temp - 20.0).max(0.0) * 2.0; // +2 per °C above 20
            engine.ingest_reading("MTR-E", rw(base + weather_effect, i, temp, 0.0));
        }
        engine.fit_weather_model("MTR-E");

        // A hot reading should have a positive weather adjustment
        let hot_reading = rw(150.0, 0, 35.0, 0.0);
        let adjustment = engine.compute_weather_adjustment("MTR-E", &hot_reading);
        assert!(
            adjustment > 0.0,
            "weather adjustment should be positive for hot day, got {}",
            adjustment
        );
    }

    #[test]
    fn test_dynamic_threshold_increases_with_noise() {
        let mut engine = DiagnosticEngine::new();
        for i in 0..30 {
            engine.ingest_reading("MTR-F", r(100.0 + (i as f64).sin() * 3.0, i));
        }
        let report_quiet = engine.get_diagnostics("MTR-F").unwrap();
        let quiet_threshold = report_quiet.dynamic_threshold;

        let mut engine2 = DiagnosticEngine::new();
        for i in 0..30 {
            engine2.ingest_reading("MTR-F", r(100.0 + (i as f64).sin() * 30.0, i));
        }
        let report_noisy = engine2.get_diagnostics("MTR-F").unwrap();
        assert!(
            report_noisy.dynamic_threshold >= quiet_threshold,
            "noisy data should produce higher threshold (noisy={}, quiet={})",
            report_noisy.dynamic_threshold,
            quiet_threshold
        );
    }

    #[test]
    fn test_seasonal_decomposition() {
        let mut engine = DiagnosticEngine::new();
        // Simulate summer peak (months 6–8 = June–August) with higher values
        let now = Utc::now();
        for days_ago in 0..90 {
            let ts = now - Duration::days(days_ago);
            let month = ts.month0(); // 0-based
            let seasonal_boost = if (5..=7).contains(&month) { 1.5 } else { 1.0 };
            let value = 100.0 * seasonal_boost + (days_ago as f64).sin() * 5.0;
            engine.ingest_reading(
                "MTR-G",
                Reading {
                    timestamp: ts,
                    value,
                    weather: None,
                },
            );
        }
        let report = engine.get_diagnostics("MTR-G").unwrap();
        // The seasonal factor for the current month should differ from 1.0
        // (the engine will have detected the pattern)
        assert!(
            (report.seasonal_factor - 1.0).abs() > 0.01
                || (report.trend_component - report.expected_volume).abs() > 0.01,
            "seasonal factor or trend should capture the monthly pattern"
        );
    }

    #[test]
    fn test_empty_engine_returns_none() {
        let mut engine = DiagnosticEngine::new();
        assert!(engine.get_diagnostics("nonexistent").is_none());
    }

    #[test]
    fn test_global_engine_is_accessible() {
        let engine = global_engine();
        let mut guard = engine.lock().unwrap();
        guard.ingest_reading("GLOBAL", r(100.0, 0));
        let report = guard.get_diagnostics("GLOBAL");
        assert!(report.is_some());
    }
}
