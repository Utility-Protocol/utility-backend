use serde::{Deserialize, Serialize};
use sqlx::{PgPool, FromRow};
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct MintEvent {
    pub id: Uuid,
    pub batch_id: String,
    pub resource_type: String,
    pub amount: f64,
    pub destination_wallet: String,
    pub created_at: Option<DateTime<Utc>>,
}

pub struct MintQueue {
    pool: PgPool,
}

impl MintQueue {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn enqueue(&self, batch_id: &str, resource_type: &str, amount: f64, destination_wallet: &str) -> Result<Uuid, sqlx::Error> {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO pending_mints (id, batch_id, resource_type, amount, destination_wallet)
            VALUES ($1, $2, $3, $4, $5)
            "#
        )
        .bind(id)
        .bind(batch_id)
        .bind(resource_type)
        .bind(amount)
        .bind(destination_wallet)
        .execute(&self.pool)
        .await?;

        Ok(id)
    }

    pub async fn get_pending(&self, batch_id: &str) -> Result<Vec<MintEvent>, sqlx::Error> {
        let events = sqlx::query_as::<_, MintEvent>(
            r#"
            SELECT id, batch_id, resource_type, amount, destination_wallet, created_at
            FROM pending_mints
            WHERE batch_id = $1
            "#
        )
        .bind(batch_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(events)
    }

    pub async fn remove_event(&self, id: Uuid) -> Result<(), sqlx::Error> {
        sqlx::query(
            "DELETE FROM pending_mints WHERE id = $1"
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
