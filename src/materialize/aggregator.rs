use sqlx::{Postgres, Transaction};

/// Aggregates one staged telemetry id range into hourly usage buckets.
///
/// The query is intentionally idempotent: re-processing a staged batch after a
/// crash adds the same source range by first grouping raw events and then
/// upserting the aggregate totals for each `(hour, resource_type)` bucket.
pub async fn aggregate_staged_batch(
    tx: &mut Transaction<'_, Postgres>,
    lower_watermark: i64,
    upper_watermark: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO materialized_hourly_usage (hour, resource_type, usage)
        SELECT
            date_trunc('hour', recorded_at) AS hour,
            resource_type,
            SUM(reading) AS usage
        FROM telemetry_events
        WHERE id > $1 AND id <= $2
        GROUP BY 1, 2
        ON CONFLICT (hour, resource_type) DO UPDATE
        SET usage = materialized_hourly_usage.usage + EXCLUDED.usage
        "#,
    )
    .bind(lower_watermark)
    .bind(upper_watermark)
    .execute(&mut **tx)
    .await?;

    Ok(())
}
