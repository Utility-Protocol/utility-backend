use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};

/// TimescaleDB chunk metadata used by cooperative compaction.
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct CompressableChunk {
    pub chunk_name: String,
    pub chunk_schema: String,
    pub range_start: DateTime<Utc>,
    pub range_end: DateTime<Utc>,
    pub chunk_id: i64,
}

/// Lists uncompressed chunks ending before `before`, oldest first.
pub async fn list_compressable_chunks(
    pool: &PgPool,
    hypertable_name: &str,
    before: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<CompressableChunk>, sqlx::Error> {
    sqlx::query_as::<_, CompressableChunk>(
        "SELECT \
            chunk_name::text, \
            chunk_schema::text, \
            range_start, \
            range_end, \
            hashtextextended(chunk_schema::text || '.' || chunk_name::text, 0)::bigint AS chunk_id \
         FROM timescaledb_information.chunks \
         WHERE hypertable_name = $1 \
           AND range_end < $2 \
           AND is_compressed = false \
         ORDER BY range_start ASC \
         LIMIT $3",
    )
    .bind(hypertable_name)
    .bind(before)
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
}
