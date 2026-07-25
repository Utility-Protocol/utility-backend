use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, PartialEq)]
pub struct DlqMessage {
    pub id: Uuid,
    pub queue_name: String,
    pub message_id: String,
    pub payload: serde_json::Value,
    pub error_reason: Option<String>,
    pub retry_count: i32,
    pub status: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

/// Send or update a failed message in the Dead Letter Queue (DLQ).
/// If a message with the same queue_name and message_id already exists, it will
/// overwrite/update the record with the new error_reason, reset its status to 'failed',
/// and increment the retry_count (or handle it based on business needs).
pub async fn send_to_dlq(
    pool: &PgPool,
    queue_name: &str,
    message_id: &str,
    payload: &serde_json::Value,
    error_reason: Option<&str>,
) -> Result<Uuid, sqlx::Error> {
    let row = sqlx::query(
        r#"
        INSERT INTO dead_letter_queue (queue_name, message_id, payload, error_reason, status)
        VALUES ($1, $2, $3, $4, 'failed')
        ON CONFLICT (queue_name, message_id)
        DO UPDATE SET
            payload = EXCLUDED.payload,
            error_reason = EXCLUDED.error_reason,
            status = 'failed',
            updated_at = CURRENT_TIMESTAMP
        RETURNING id
        "#,
    )
    .bind(queue_name)
    .bind(message_id)
    .bind(payload)
    .bind(error_reason)
    .fetch_one(pool)
    .await?;

    let id: Uuid = sqlx::Row::get(&row, "id");
    Ok(id)
}

/// Retrieve a list of DLQ messages with optional status filtering.
pub async fn list_dlq(
    pool: &PgPool,
    status_filter: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<Vec<DlqMessage>, sqlx::Error> {
    let messages = if let Some(status) = status_filter {
        sqlx::query_as::<_, DlqMessage>(
            r#"
            SELECT id, queue_name, message_id, payload, error_reason, retry_count, status, created_at, updated_at
            FROM dead_letter_queue
            WHERE status = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(status)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as::<_, DlqMessage>(
            r#"
            SELECT id, queue_name, message_id, payload, error_reason, retry_count, status, created_at, updated_at
            FROM dead_letter_queue
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?
    };

    Ok(messages)
}

/// Get a specific DLQ message by its ID.
pub async fn get_dlq_by_id(pool: &PgPool, id: Uuid) -> Result<Option<DlqMessage>, sqlx::Error> {
    let message = sqlx::query_as::<_, DlqMessage>(
        r#"
        SELECT id, queue_name, message_id, payload, error_reason, retry_count, status, created_at, updated_at
        FROM dead_letter_queue
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(message)
}

/// Delete or resolve/acknowledge a DLQ message.
pub async fn delete_dlq_message(pool: &PgPool, id: Uuid) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "DELETE FROM dead_letter_queue WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Update status of a DLQ message.
pub async fn update_dlq_status(
    pool: &PgPool,
    id: Uuid,
    status: &str,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        r#"
        UPDATE dead_letter_queue
        SET status = $1, updated_at = CURRENT_TIMESTAMP
        WHERE id = $2
        "#,
    )
    .bind(status)
    .bind(id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Increment the retry_count of a DLQ message and optionally update error_reason.
pub async fn increment_retry_count(
    pool: &PgPool,
    id: Uuid,
    new_error_reason: Option<&str>,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        r#"
        UPDATE dead_letter_queue
        SET
            retry_count = retry_count + 1,
            error_reason = COALESCE($1, error_reason),
            status = 'failed',
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $2
        "#,
    )
    .bind(new_error_reason)
    .bind(id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Ensure that the dead_letter_queue table schema is present.
/// Useful for automatic setup in tests if the postgres DB is run on-demand.
pub async fn ensure_dlq_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS dead_letter_queue (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            queue_name TEXT NOT NULL,
            message_id TEXT NOT NULL,
            payload JSONB NOT NULL,
            error_reason TEXT,
            retry_count INT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'failed',
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(queue_name, message_id)
        );
        CREATE INDEX IF NOT EXISTS idx_dlq_status ON dead_letter_queue(status);
        "#
    )
    .execute(pool)
    .await?;
    Ok(())
}
