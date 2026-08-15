use crate::settlement::mint_queue::MintQueue;
use crate::soroban::rpc::CircuitBreaker;
use hex;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

pub struct Finalizer {
    pool: PgPool,
    rpc_url: String,
    mint_queue: MintQueue,
    breaker: Arc<Mutex<CircuitBreaker>>,
}

impl Finalizer {
    pub fn new(pool: PgPool, rpc_url: String, breaker: Arc<Mutex<CircuitBreaker>>) -> Self {
        let mint_queue = MintQueue::new(pool.clone());
        Self {
            pool,
            rpc_url,
            mint_queue,
            breaker,
        }
    }

    pub async fn finalize_mint(
        &self,
        batch_id: &str,
        resource_type: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 1. Generate idempotency key: SHA256(batch_id || resource_type || 'mint')
        let mut hasher = Sha256::new();
        hasher.update(batch_id);
        hasher.update(resource_type);
        hasher.update("mint");
        let idempotency_key = hex::encode(hasher.finalize());

        // 2. Attempt to mark as processed using a transaction for atomicity and deduplication
        let mut tx: Transaction<'_, Postgres> = self.pool.begin().await?;

        let result = sqlx::query(
            r#"
            INSERT INTO processed_mints (batch_id, resource_type, idempotency_key)
            VALUES ($1, $2, $3)
            ON CONFLICT (batch_id, resource_type) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(batch_id)
        .bind(resource_type)
        .bind(idempotency_key.clone())
        .fetch_optional(&mut *tx)
        .await?;

        if result.is_none() {
            info!(batch_id = %batch_id, resource_type = %resource_type, "mint already processed, skipping");
            tx.rollback().await?;
            return Ok(());
        }

        // 3. Get pending mints for this batch and resource type
        let pending = self.mint_queue.get_pending(batch_id).await?;
        let filtered_pending: Vec<_> = pending
            .into_iter()
            .filter(|e| e.resource_type == resource_type)
            .collect();

        if filtered_pending.is_empty() {
            warn!(batch_id = %batch_id, resource_type = %resource_type, "no pending mints found for finalization");
            tx.rollback().await?;
            return Ok(());
        }

        // AGGREGATION: We must mint the TOTAL amount in ONE transaction to honor the single idempotency key.
        let total_amount: f64 = filtered_pending.iter().map(|e| e.amount).sum();
        let destination = filtered_pending[0].destination_wallet.clone(); // Assume same destination for batch/resource

        // 4. Submit to Soroban RPC
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": format!("mint-{}-{}", batch_id, resource_type),
            "method": "sendTransaction",
            "params": {
                "batch_id": batch_id,
                "resource_type": resource_type,
                "amount": total_amount,
                "destination": destination,
                "idempotency_key": idempotency_key
            }
        });

        let mut breaker = self.breaker.lock().await;
        match breaker.call_rpc(&self.rpc_url, payload).await {
            Ok(_) => {
                info!(batch_id = %batch_id, "soroban aggregated mint transaction submitted");
                for event in filtered_pending {
                    self.mint_queue.remove_event(event.id).await?;
                }
            }
            Err(e) => {
                warn!(batch_id = %batch_id, error = %e, "soroban aggregated mint transaction failed");
                tx.rollback().await?;

                let dlq_payload = serde_json::json!({
                    "batch_id": batch_id,
                    "resource_type": resource_type,
                    "amount": total_amount,
                    "destination": destination,
                    "idempotency_key": idempotency_key,
                });
                let msg_id_str = format!("{}:{}", batch_id, resource_type);
                if let Err(dlq_err) = crate::settlement::dlq::send_to_dlq(
                    &self.pool,
                    "mint-events",
                    &msg_id_str,
                    &dlq_payload,
                    Some(&e.to_string()),
                )
                .await
                {
                    tracing::error!(error = %dlq_err, "failed to send message to DLQ");
                }

                return Err(e.into());
            }
        }

        tx.commit().await?;
        info!(batch_id = %batch_id, resource_type = %resource_type, "mint finalization complete");
        Ok(())
    }
}
