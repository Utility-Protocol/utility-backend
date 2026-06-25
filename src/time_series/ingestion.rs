use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::{Pool, Postgres};
use tracing::info;

/// Ingests a telemetry reading into the database, ensuring strictly monotonic
/// sequences per meter even under high concurrency.
///
/// This implementation uses a PostgreSQL advisory transaction-level lock to
/// serialize sequence generation for a specific meter_id.
pub async fn ingest_telemetry(
    pool: &Pool<Postgres>,
    meter_id: &str,
    reading: f64,
    recorded_at: DateTime<Utc>,
    hlc_timestamp: u64,
) -> anyhow::Result<i32> {
    let mut tx = pool.begin().await?;

    // 1. Acquire advisory lock for the meter_id to serialize sequence generation.
    // We use a stable hash (Sha256) of the meter_id to get a 64-bit integer
    // for pg_advisory_xact_lock to ensure stability across builds and instances.
    let mut hasher = Sha256::new();
    hasher.update(meter_id.as_bytes());
    let result = hasher.finalize();
    let lock_id = i64::from_be_bytes(result[0..8].try_into().unwrap());

    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(lock_id)
        .execute(&mut *tx)
        .await?;

    // 2. Compute the next sequence number by finding the current max.
    // Because we hold the advisory lock, no other transaction can be
    // computing the next sequence for this meter_id simultaneously.
    let next_seq: i32 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(sequence), 0) + 1 FROM telemetry_events WHERE meter_id = $1",
    )
    .bind(meter_id)
    .fetch_one(&mut *tx)
    .await?;

    // 3. Insert the new telemetry event with the computed sequence and HLC timestamp.
    sqlx::query(
        "INSERT INTO telemetry_events (meter_id, recorded_at, reading, sequence, hlc_timestamp) VALUES ($1, $2, $3, $4, $5)"
    )
    .bind(meter_id)
    .bind(recorded_at)
    .bind(reading)
    .bind(next_seq)
    .bind(hlc_timestamp as i64)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    info!(
        meter_id = %meter_id,
        sequence = next_seq,
        reading = reading,
        hlc_timestamp = hlc_timestamp,
        "telemetry ingested successfully"
    );

    Ok(next_seq)
}
