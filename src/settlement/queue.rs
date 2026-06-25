use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DedupClaim {
    Claimed { idempotency_key: Uuid },
    AlreadyClaimed,
}

#[async_trait]
pub trait DepositDedupStore: Send + Sync {
    async fn claim_deposit(
        &self,
        deposit_id: &str,
        idempotency_key: Uuid,
    ) -> Result<DedupClaim, String>;
}

#[derive(Clone)]
pub struct PgDepositDedupStore {
    pool: PgPool,
}

impl PgDepositDedupStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DepositDedupStore for PgDepositDedupStore {
    async fn claim_deposit(
        &self,
        deposit_id: &str,
        idempotency_key: Uuid,
    ) -> Result<DedupClaim, String> {
        let mut tx = self.pool.begin().await.map_err(|err| err.to_string())?;
        let lock_key = advisory_lock_key(deposit_id);
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock_key)
            .execute(&mut *tx)
            .await
            .map_err(|err| err.to_string())?;

        let inserted = sqlx::query(
            "INSERT INTO processed_deposits (deposit_id, idempotency_key) VALUES ($1, $2) \
             ON CONFLICT (deposit_id) DO NOTHING RETURNING id",
        )
        .bind(deposit_id)
        .bind(idempotency_key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;

        tx.commit().await.map_err(|err| err.to_string())?;
        if inserted.is_some() {
            Ok(DedupClaim::Claimed { idempotency_key })
        } else {
            Ok(DedupClaim::AlreadyClaimed)
        }
    }
}

fn advisory_lock_key(deposit_id: &str) -> i64 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(format!("deposit:{deposit_id}").as_bytes());
    i64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("sha digest has at least 8 bytes"),
    )
}
