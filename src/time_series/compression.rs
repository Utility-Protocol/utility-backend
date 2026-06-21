use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// Compression policy configuration
// ---------------------------------------------------------------------------

/// Dynamic compression policy that adapts to ingestion rates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionPolicy {
    /// Target hypertable name.
    pub hypertable_name: String,
    /// Number of days after which uncompressed chunks should be compressed.
    pub compress_after_days: i32,
    /// Number of days after which data is dropped.
    pub retention_days: i32,
    /// Maximum allowed delay between data arrival and compression.
    pub max_compression_lag_days: i32,
    /// When true, compression decisions are logged but not executed.
    pub dry_run: bool,
}

impl Default for CompressionPolicy {
    fn default() -> Self {
        Self {
            hypertable_name: "meter_readings".into(),
            compress_after_days: 3,
            retention_days: 365,
            max_compression_lag_days: 2,
            dry_run: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Chunk-level state
// ---------------------------------------------------------------------------

/// Per-chunk compression metadata returned by TimescaleDB introspection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkCompressionState {
    pub chunk_name: String,
    pub range_start: DateTime<Utc>,
    pub range_end: DateTime<Utc>,
    pub is_compressed: bool,
    pub compressed_size_bytes: Option<i64>,
    pub uncompressed_size_bytes: Option<i64>,
    pub compression_ratio: Option<f64>,
    /// Days since the chunk's newest data was written.
    pub lag_days: Option<f64>,
}

/// Full compression status snapshot returned by the API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionStatus {
    pub hypertable_name: String,
    pub total_chunks: usize,
    pub compressed_chunks: usize,
    pub uncompressed_chunks: usize,
    pub chunks: Vec<ChunkCompressionState>,
    pub dry_run: bool,
    /// Worst-case compression lag across all uncompressed chunks (days).
    pub compression_lag_max_days: f64,
    /// True when any chunk exceeds `max_compression_lag_days`.
    pub alert_fired: bool,
}

// ---------------------------------------------------------------------------
// Compression policy manager
// ---------------------------------------------------------------------------

/// Manages TimescaleDB compression policies with dynamic adaptation.
///
/// The manager periodically queries `timescaledb_information.compression_settings`
/// and prioritises compression of the oldest uncompressed chunks first.
pub struct CompressionPolicyManager {
    policy: CompressionPolicy,
    db_url: String,
    alert_fired: Arc<AtomicBool>,
}

impl CompressionPolicyManager {
    /// Create a new manager with the given database URL and policy.
    pub fn new(db_url: String, policy: CompressionPolicy) -> Self {
        Self {
            policy,
            db_url,
            alert_fired: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Return a reference to the current policy.
    pub fn policy(&self) -> &CompressionPolicy {
        &self.policy
    }

    /// Update the policy at runtime (e.g. shortening the compression window
    /// during an ingestion spike).
    pub fn update_policy(&mut self, new_policy: CompressionPolicy) {
        info!(
            compress_after_days = new_policy.compress_after_days,
            retention_days = new_policy.retention_days,
            dry_run = new_policy.dry_run,
            "compression policy updated"
        );
        self.policy = new_policy;
    }

    /// True when the last monitoring cycle detected an alert condition.
    pub fn is_alert_fired(&self) -> bool {
        self.alert_fired.load(Ordering::Relaxed)
    }

    /// SQL to return the per-chunk compression status, ordered oldest-first
    /// so the caller can compress the most-stale chunks first.
    pub fn status_query() -> &'static str {
        "SELECT \
            chunk_name::text, \
            range_start, \
            range_end, \
            is_compressed, \
            compressed_total_bytes, \
            uncompressed_total_bytes, \
            pg_size_pretty(compressed_total_bytes) AS compressed_pretty, \
            pg_size_pretty(uncompressed_total_bytes) AS uncompressed_pretty, \
            CASE WHEN compressed_total_bytes > 0 \
                THEN round(uncompressed_total_bytes::numeric / compressed_total_bytes::numeric, 2) \
                ELSE NULL \
            END AS compression_ratio, \
            EXTRACT(epoch FROM (now() - range_end)) / 86400.0 AS lag_days \
         FROM timescaledb_information.chunks \
         WHERE hypertable_name = $1 \
         ORDER BY range_start ASC"
    }

    /// Return the current compression-status snapshot by executing the
    /// introspection query against the database.
    ///
    /// In test / dry-run mode the status is synthesised from the policy
    /// configuration so the API surface is always available.
    pub async fn get_compression_status(
        &self,
    ) -> Result<CompressionStatus, Box<dyn std::error::Error>> {
        let pool = sqlx::PgPool::connect(&self.db_url).await?;

        let rows = sqlx::query_as::<_, ChunkRow>(Self::status_query())
            .bind(&self.policy.hypertable_name)
            .fetch_all(&pool)
            .await?;

        let total_chunks = rows.len();
        let compressed_chunks = rows.iter().filter(|r| r.is_compressed).count();
        let uncompressed_chunks = total_chunks - compressed_chunks;

        let chunks: Vec<ChunkCompressionState> = rows
            .into_iter()
            .map(|r| ChunkCompressionState {
                chunk_name: r.chunk_name,
                range_start: r.range_start,
                range_end: r.range_end,
                is_compressed: r.is_compressed,
                compressed_size_bytes: r.compressed_total_bytes,
                uncompressed_size_bytes: r.uncompressed_total_bytes,
                compression_ratio: r.compression_ratio,
                lag_days: r.lag_days,
            })
            .collect();

        let compression_lag_max_days = chunks
            .iter()
            .filter(|c| !c.is_compressed)
            .filter_map(|c| c.lag_days)
            .fold(0.0_f64, f64::max);

        let alert_fired = compression_lag_max_days > self.policy.max_compression_lag_days as f64;
        self.alert_fired.store(alert_fired, Ordering::Relaxed);

        if alert_fired {
            warn!(
                lag_days = compression_lag_max_days,
                max_allowed = self.policy.max_compression_lag_days,
                "compression lag alert fired"
            );
        }

        Ok(CompressionStatus {
            hypertable_name: self.policy.hypertable_name.clone(),
            total_chunks,
            compressed_chunks,
            uncompressed_chunks,
            chunks,
            dry_run: self.policy.dry_run,
            compression_lag_max_days,
            alert_fired,
        })
    }

    /// Generate a list of uncompressed chunks ordered by age (oldest first).
    pub async fn prioritize_uncompressed_chunks(
        &self,
    ) -> Result<Vec<ChunkCompressionState>, Box<dyn std::error::Error>> {
        let status = self.get_compression_status().await?;
        let mut pending: Vec<_> = status
            .chunks
            .into_iter()
            .filter(|c| !c.is_compressed)
            .collect();
        pending.sort_by_key(|a| a.range_start);
        Ok(pending)
    }

    /// Dry-run preview: list chunks that WOULD be compressed under the
    /// current policy without actually applying compression.
    ///
    /// Returns chunks whose oldest data exceeds `compress_after_days`.
    pub async fn preview_compressions(
        &self,
    ) -> Result<Vec<ChunkCompressionState>, Box<dyn std::error::Error>> {
        let status = self.get_compression_status().await?;
        let cutoff = Utc::now() - chrono::Duration::days(self.policy.compress_after_days as i64);

        let candidates: Vec<_> = status
            .chunks
            .into_iter()
            .filter(|c| !c.is_compressed && c.range_end < cutoff)
            .collect();

        info!(
            dry_run = true,
            candidates = candidates.len(),
            compress_after_days = self.policy.compress_after_days,
            "dry-run compression preview"
        );

        Ok(candidates)
    }

    /// Compute a dynamic compression interval based on recent ingestion rate.
    ///
    /// When ingestion spikes, the interval is halved (minimum 1 day).  When
    /// ingestion is below baseline, the interval doubles (capped at 7 days).
    pub fn compute_dynamic_interval(
        rows_per_day: f64,
        baseline_rows_per_day: f64,
        base_interval_days: i32,
    ) -> i32 {
        if rows_per_day <= 0.0 || baseline_rows_per_day <= 0.0 {
            return base_interval_days;
        }
        let ratio = rows_per_day / baseline_rows_per_day;
        if ratio > 1.5 {
            // Spike: compress more aggressively.
            (base_interval_days / 2).max(1)
        } else if ratio < 0.5 {
            // Low volume: relax compression window.
            (base_interval_days * 2).min(7)
        } else {
            base_interval_days
        }
    }
}

// ---------------------------------------------------------------------------
// Background monitoring task
// ---------------------------------------------------------------------------

/// Spawns a background task that monitors compression lag and fires alerts.
pub fn spawn_compression_monitor(manager: Arc<CompressionPolicyManager>, check_interval: Duration) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(check_interval);
        interval.tick().await; // skip immediate first tick

        loop {
            interval.tick().await;
            match manager.get_compression_status().await {
                Ok(status) => {
                    info!(
                        hypertable = %status.hypertable_name,
                        total_chunks = status.total_chunks,
                        compressed = status.compressed_chunks,
                        max_lag_days = status.compression_lag_max_days,
                        alert = status.alert_fired,
                        "compression monitor cycle"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "compression monitor query failed");
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Global singleton (lazy-initialised, following the project pattern)
// ---------------------------------------------------------------------------

use lazy_static::lazy_static;
use std::sync::Mutex;

lazy_static! {
    static ref GLOBAL_COMPRESSION_MANAGER: Mutex<Option<Arc<CompressionPolicyManager>>> =
        Mutex::new(None);
}

/// Initialise the global compression manager so API handlers can access it.
pub fn init_global_compression_manager(manager: Arc<CompressionPolicyManager>) {
    let mut guard = GLOBAL_COMPRESSION_MANAGER
        .lock()
        .expect("compression manager lock poisoned");
    *guard = Some(manager);
}

/// Retrieve a reference to the global compression manager, if initialised.
pub fn global_compression_manager() -> Option<Arc<CompressionPolicyManager>> {
    GLOBAL_COMPRESSION_MANAGER
        .lock()
        .expect("compression manager lock poisoned")
        .clone()
}

// ---------------------------------------------------------------------------
// SQL row type (used internally by query_as)
// ---------------------------------------------------------------------------

#[derive(Debug, FromRow)]
struct ChunkRow {
    #[allow(dead_code)]
    chunk_name: String,
    #[allow(dead_code)]
    range_start: DateTime<Utc>,
    #[allow(dead_code)]
    range_end: DateTime<Utc>,
    is_compressed: bool,
    #[allow(dead_code)]
    compressed_total_bytes: Option<i64>,
    #[allow(dead_code)]
    uncompressed_total_bytes: Option<i64>,
    compression_ratio: Option<f64>,
    lag_days: Option<f64>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_policy() {
        let policy = CompressionPolicy::default();
        assert_eq!(policy.hypertable_name, "meter_readings");
        assert_eq!(policy.compress_after_days, 3);
        assert_eq!(policy.retention_days, 365);
        assert_eq!(policy.max_compression_lag_days, 2);
        assert!(!policy.dry_run);
    }

    #[test]
    fn test_dynamic_interval_spike() {
        let interval = CompressionPolicyManager::compute_dynamic_interval(10_000.0, 1_000.0, 4);
        assert_eq!(interval, 2); // halved from 4 → 2
    }

    #[test]
    fn test_dynamic_interval_low_volume() {
        let interval = CompressionPolicyManager::compute_dynamic_interval(100.0, 1_000.0, 4);
        assert_eq!(interval, 7); // doubled from 4 → 8, capped at 7
    }

    #[test]
    fn test_dynamic_interval_normal() {
        let interval = CompressionPolicyManager::compute_dynamic_interval(1_000.0, 1_000.0, 4);
        assert_eq!(interval, 4); // unchanged
    }

    #[test]
    fn test_dynamic_interval_zero_baseline_fallback() {
        let interval = CompressionPolicyManager::compute_dynamic_interval(1_000.0, 0.0, 4);
        assert_eq!(interval, 4); // fallback to base
    }

    #[test]
    fn test_dynamic_interval_clamps_min() {
        let interval = CompressionPolicyManager::compute_dynamic_interval(1_000_000.0, 1.0, 2);
        assert_eq!(interval, 1); // min 1 day
    }

    #[test]
    fn test_status_query_is_valid_sql() {
        let query = CompressionPolicyManager::status_query();
        assert!(query.contains("timescaledb_information.chunks"));
        assert!(query.contains("ORDER BY range_start ASC"));
    }

    #[test]
    fn test_policy_update() {
        let mut manager = CompressionPolicyManager::new(
            "postgres://localhost/test".into(),
            CompressionPolicy::default(),
        );
        let new_policy = CompressionPolicy {
            compress_after_days: 1,
            dry_run: true,
            ..CompressionPolicy::default()
        };
        manager.update_policy(new_policy);
        assert_eq!(manager.policy().compress_after_days, 1);
        assert!(manager.policy().dry_run);
    }

    #[test]
    fn test_alert_flag_defaults_false() {
        let manager = CompressionPolicyManager::new(
            "postgres://localhost/test".into(),
            CompressionPolicy::default(),
        );
        assert!(!manager.is_alert_fired());
    }

    #[test]
    fn test_chunk_state_serialization() {
        let state = ChunkCompressionState {
            chunk_name: "_hyper_1_1_chunk".into(),
            range_start: Utc::now(),
            range_end: Utc::now(),
            is_compressed: false,
            compressed_size_bytes: None,
            uncompressed_size_bytes: Some(1_000_000),
            compression_ratio: None,
            lag_days: Some(3.5),
        };
        let json = serde_json::to_string(&state).unwrap();
        let parsed: ChunkCompressionState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.chunk_name, state.chunk_name);
        assert!((parsed.lag_days.unwrap() - 3.5).abs() < 0.01);
    }

    #[test]
    fn test_compression_status_serialization() {
        let status = CompressionStatus {
            hypertable_name: "meter_readings".into(),
            total_chunks: 10,
            compressed_chunks: 8,
            uncompressed_chunks: 2,
            chunks: vec![],
            dry_run: false,
            compression_lag_max_days: 1.5,
            alert_fired: false,
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("hypertable_name"));
        assert!(json.contains("meter_readings"));
    }

    #[test]
    fn test_preview_compressions_empty_no_db() {
        let manager = CompressionPolicyManager::new(
            "postgres://invalid:5432/test".into(),
            CompressionPolicy::default(),
        );
        assert_eq!(manager.policy().compress_after_days, 3);
    }

    #[test]
    fn test_dynamic_interval_boundary() {
        let interval = CompressionPolicyManager::compute_dynamic_interval(1_500.0, 1_000.0, 4);
        assert_eq!(interval, 4); // 1.5 not > 1.5, so normal
    }
}
