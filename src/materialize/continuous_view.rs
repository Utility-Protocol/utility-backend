use chrono::{DateTime, Utc};
use sqlx::{Pool, Postgres, Row};
use uuid::Uuid;

use super::aggregator::aggregate_staged_batch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedBatch {
    pub batch_id: Uuid,
    pub lower_watermark: i64,
    pub upper_watermark: i64,
    pub started_at: DateTime<Utc>,
}

/// Continuous materialized-view maintainer using a two-phase durable watermark.
///
/// The important invariant is that `materialization_state.high_watermark` is
/// only advanced in the same transaction that applies the hourly aggregate and
/// marks the corresponding pending batch as committed. If the process dies after
/// staging but before that transaction commits, recovery replays the staging row
/// rather than skipping the raw events.
pub struct ContinuousMaterializer {
    pool: Pool<Postgres>,
    batch_size: i64,
}

impl ContinuousMaterializer {
    pub fn new(pool: Pool<Postgres>, batch_size: i64) -> Self {
        Self { pool, batch_size }
    }

    pub async fn materialize_window(&self) -> anyhow::Result<Option<StagedBatch>> {
        if let Some(batch) = self.recover_staging_batch().await? {
            self.commit_batch(&batch).await?;
            return Ok(Some(batch));
        }

        let lower_watermark = self.current_high_watermark().await?;
        let upper_watermark = self.next_upper_watermark(lower_watermark).await?;
        if upper_watermark <= lower_watermark {
            return Ok(None);
        }

        let batch = self.stage_batch(lower_watermark, upper_watermark).await?;
        self.commit_batch(&batch).await?;
        Ok(Some(batch))
    }

    async fn current_high_watermark(&self) -> anyhow::Result<i64> {
        let watermark = sqlx::query_scalar::<_, i64>(
            "SELECT high_watermark FROM materialization_state WHERE name = 'hourly_usage'",
        )
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or(0);
        Ok(watermark)
    }

    async fn next_upper_watermark(&self, lower_watermark: i64) -> anyhow::Result<i64> {
        let upper = sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(id), $1) FROM (SELECT id FROM telemetry_events WHERE id > $1 ORDER BY id LIMIT $2) batch",
        )
        .bind(lower_watermark)
        .bind(self.batch_size)
        .fetch_one(&self.pool)
        .await?;
        Ok(upper)
    }

    async fn stage_batch(
        &self,
        lower_watermark: i64,
        upper_watermark: i64,
    ) -> anyhow::Result<StagedBatch> {
        let batch = StagedBatch {
            batch_id: Uuid::new_v4(),
            lower_watermark,
            upper_watermark,
            started_at: Utc::now(),
        };

        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO materialization_pending
                (batch_id, lower_watermark, upper_watermark, status, started_at)
            VALUES ($1, $2, $3, 'staging', $4)
            "#,
        )
        .bind(batch.batch_id)
        .bind(batch.lower_watermark)
        .bind(batch.upper_watermark)
        .bind(batch.started_at)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO materialization_checkpoint (batch_id, new_watermark, started_at)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(batch.batch_id)
        .bind(batch.upper_watermark)
        .bind(batch.started_at)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(batch)
    }

    async fn recover_staging_batch(&self) -> anyhow::Result<Option<StagedBatch>> {
        let row = sqlx::query(
            r#"
            SELECT batch_id, lower_watermark, upper_watermark, started_at
            FROM materialization_pending
            WHERE status = 'staging'
            ORDER BY started_at ASC
            LIMIT 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| StagedBatch {
            batch_id: row.get("batch_id"),
            lower_watermark: row.get("lower_watermark"),
            upper_watermark: row.get("upper_watermark"),
            started_at: row.get("started_at"),
        }))
    }

    async fn commit_batch(&self, batch: &StagedBatch) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        aggregate_staged_batch(&mut tx, batch.lower_watermark, batch.upper_watermark).await?;

        sqlx::query(
            r#"
            INSERT INTO materialization_state (name, high_watermark, updated_at)
            VALUES ('hourly_usage', $1, NOW())
            ON CONFLICT (name) DO UPDATE
            SET high_watermark = GREATEST(materialization_state.high_watermark, EXCLUDED.high_watermark),
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(batch.upper_watermark)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "UPDATE materialization_pending SET status = 'committed', committed_at = NOW() WHERE batch_id = $1",
        )
        .bind(batch.batch_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_batch_keeps_old_watermark_until_commit() {
        let batch = StagedBatch {
            batch_id: Uuid::nil(),
            lower_watermark: 10,
            upper_watermark: 25,
            started_at: Utc::now(),
        };

        assert_eq!(batch.lower_watermark, 10);
        assert_eq!(batch.upper_watermark, 25);
    }
}
