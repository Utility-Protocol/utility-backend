use async_trait::async_trait;
use uuid::Uuid;

use super::attestation_verifier::DepositAttestation;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MintTransaction {
    pub deposit_id: String,
    pub resource_id: String,
    pub amount: u64,
    pub idempotency_key: Uuid,
    pub memo: String,
}

impl MintTransaction {
    pub fn from_attestation(attestation: &DepositAttestation, idempotency_key: Uuid) -> Self {
        Self {
            deposit_id: attestation.deposit_id.clone(),
            resource_id: attestation.resource_id.clone(),
            amount: attestation.amount,
            idempotency_key,
            memo: format!(
                "deposit:{};idempotency:{}",
                attestation.deposit_id, idempotency_key
            ),
        }
    }
}

#[async_trait]
pub trait MintSubmitter: Send + Sync {
    async fn submit_mint_transaction(&self, transaction: MintTransaction)
        -> Result<String, String>;
}
