use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Dead letter entry
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct DeadLetterEntry {
    pub id: Uuid,
    pub endpoint_id: String,
    pub event_id: Uuid,
    pub payload: serde_json::Value,
    pub event_type: String,
    pub failed_at: DateTime<Utc>,
    pub retry_count: i32,
    pub last_error: Option<String>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

#[async_trait]
pub trait DeadLetterQueue: Send + Sync + 'static {
    /// Persist a failed delivery so it can be inspected and retried later.
    async fn enqueue(
        &self,
        endpoint_id: &str,
        event_id: Uuid,
        event_type: &str,
        payload: &serde_json::Value,
        error: &str,
    ) -> Result<DeadLetterEntry, String>;

    /// List dead-letter entries for an optional endpoint filter.
    async fn list(
        &self,
        endpoint_id: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<DeadLetterEntry>, String>;

    /// Load a single dead-letter entry by id.
    async fn get(&self, id: Uuid) -> Result<Option<DeadLetterEntry>, String>;

    /// Remove a dead-letter entry after successful retry.
    async fn remove(&self, id: Uuid) -> Result<(), String>;
}

// ---------------------------------------------------------------------------
// Postgres-backed implementation
// ---------------------------------------------------------------------------

pub struct PostgresDlq {
    pool: PgPool,
}

impl PostgresDlq {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DeadLetterQueue for PostgresDlq {
    async fn enqueue(
        &self,
        endpoint_id: &str,
        event_id: Uuid,
        event_type: &str,
        payload: &serde_json::Value,
        error: &str,
    ) -> Result<DeadLetterEntry, String> {
        let id = Uuid::new_v4();
        let entry = DeadLetterEntry {
            id,
            endpoint_id: endpoint_id.to_string(),
            event_id,
            event_type: event_type.to_string(),
            payload: payload.clone(),
            failed_at: Utc::now(),
            retry_count: 0,
            last_error: Some(error.to_string()),
        };

        sqlx::query(
            "INSERT INTO dead_letter_webhooks (id, endpoint_id, event_id, payload, event_type, failed_at, retry_count, last_error) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(entry.id)
        .bind(&entry.endpoint_id)
        .bind(entry.event_id)
        .bind(&entry.payload)
        .bind(&entry.event_type)
        .bind(entry.failed_at)
        .bind(entry.retry_count)
        .bind(&entry.last_error)
        .execute(&self.pool)
        .await
        .map_err(|e| format!("failed to enqueue dead letter: {}", e))?;

        Ok(entry)
    }

    async fn list(
        &self,
        endpoint_id: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<DeadLetterEntry>, String> {
        if let Some(eid) = endpoint_id {
            sqlx::query_as::<_, DeadLetterEntry>(
                "SELECT id, endpoint_id, event_id, payload, event_type, failed_at, retry_count, last_error \
                 FROM dead_letter_webhooks WHERE endpoint_id = $1 ORDER BY failed_at DESC LIMIT $2 OFFSET $3",
            )
            .bind(eid)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| format!("failed to list dead letters: {}", e))
        } else {
            sqlx::query_as::<_, DeadLetterEntry>(
                "SELECT id, endpoint_id, event_id, payload, event_type, failed_at, retry_count, last_error \
                 FROM dead_letter_webhooks ORDER BY failed_at DESC LIMIT $1 OFFSET $2",
            )
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| format!("failed to list dead letters: {}", e))
        }
    }

    async fn get(&self, id: Uuid) -> Result<Option<DeadLetterEntry>, String> {
        sqlx::query_as::<_, DeadLetterEntry>(
            "SELECT id, endpoint_id, event_id, payload, event_type, failed_at, retry_count, last_error \
             FROM dead_letter_webhooks WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("failed to get dead letter: {}", e))
    }

    async fn remove(&self, id: Uuid) -> Result<(), String> {
        sqlx::query("DELETE FROM dead_letter_webhooks WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("failed to remove dead letter: {}", e))?;
        Ok(())
    }
}
