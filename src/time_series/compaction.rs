use crate::api::metrics;
use crate::time_series::pool::AdvisoryLockMode;
use crate::time_series::schema::{list_compressable_chunks, CompressableChunk};
use chrono::{Duration as ChronoDuration, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    pub hypertable_name: String,
    pub worker_count: usize,
    pub chunk_lease_ttl_ms: u64,
    pub max_compaction_duration_s: u64,
    pub min_age_hours: i64,
    pub poll_interval_s: u64,
    pub chunk_limit: u32,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            hypertable_name: "telemetry_events".into(),
            worker_count: 2,
            chunk_lease_ttl_ms: 5_000,
            max_compaction_duration_s: 30,
            min_age_hours: 6,
            poll_interval_s: 30,
            chunk_limit: 64,
        }
    }
}

#[derive(Clone)]
pub struct CompactionOrchestrator {
    pool: PgPool,
    config: Arc<CompactionConfig>,
    semaphore: Arc<Semaphore>,
    consecutive_skips: Arc<DashMap<String, u32>>,
}

impl CompactionOrchestrator {
    pub fn new(pool: PgPool, config: CompactionConfig) -> Self {
        let worker_count = config.worker_count.max(1);
        Self {
            pool,
            config: Arc::new(config),
            semaphore: Arc::new(Semaphore::new(worker_count)),
            consecutive_skips: Arc::new(DashMap::new()),
        }
    }

    pub async fn run_loop(self) {
        let mut interval = tokio::time::interval(Duration::from_secs(self.config.poll_interval_s));
        loop {
            interval.tick().await;
            if let Err(error) = self.run_once().await {
                warn!(%error, "compaction scheduler cycle failed");
            }
        }
    }

    pub async fn run_once(&self) -> anyhow::Result<()> {
        let before = Utc::now() - ChronoDuration::hours(self.config.min_age_hours);
        let chunks = list_compressable_chunks(
            &self.pool,
            &self.config.hypertable_name,
            before,
            self.config.chunk_limit,
        )
        .await?;

        for chunk in chunks {
            let permit = self.semaphore.clone().acquire_owned().await?;
            let worker = self.clone();
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(error) = worker.compact_chunk(chunk).await {
                    warn!(%error, "chunk compaction failed");
                }
            });
        }

        Ok(())
    }

    async fn compact_chunk(&self, chunk: CompressableChunk) -> anyhow::Result<()> {
        metrics::record_compaction_attempt();
        let lock_timeout = Duration::from_millis(500);
        let max_duration = Duration::from_secs(self.config.max_compaction_duration_s);
        let chunk_label = format!("{}.{}", chunk.chunk_schema, chunk.chunk_name);
        let started = Instant::now();

        let result = crate::time_series::pool::with_advisory_lock(
            &self.pool,
            chunk.chunk_id,
            AdvisoryLockMode::Exclusive,
            lock_timeout,
            |conn| {
                let chunk_label = chunk_label.clone();
                Box::pin(async move {
                    sqlx::query("SET LOCAL lock_timeout = '500ms'").execute(&mut *conn).await?;
                    sqlx::query("ALTER TABLE telemetry_events SET (timescaledb.compress, timescaledb.compress_segmentby = 'meter_id')")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("SELECT compress_chunk($1::regclass, if_not_compressed => TRUE)")
                        .bind(&chunk_label)
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            },
        )
        .await;

        match result {
            Ok(()) if started.elapsed() <= max_duration => {
                self.consecutive_skips.remove(&chunk_label);
                metrics::record_compaction_duration(started.elapsed().as_millis() as f64);
                info!(chunk = %chunk_label, duration_ms = started.elapsed().as_millis(), "chunk compacted");
                Ok(())
            }
            Ok(()) => {
                error!(chunk = %chunk_label, "chunk compaction exceeded maximum duration");
                Err(anyhow::anyhow!(
                    "compaction exceeded max duration for {chunk_label}"
                ))
            }
            Err(error) => {
                metrics::record_compaction_skipped();
                let mut skips = self
                    .consecutive_skips
                    .entry(chunk_label.clone())
                    .or_insert(0);
                *skips += 1;
                if *skips >= 3 {
                    metrics::record_compaction_lock_contention();
                    error!(chunk = %chunk_label, skips = *skips, "CRIT: compaction lease contention threshold exceeded");
                } else {
                    warn!(chunk = %chunk_label, skips = *skips, %error, "compaction lease unavailable; skipping hot chunk");
                }
                Ok(())
            }
        }
    }
}
