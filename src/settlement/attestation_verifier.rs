use async_trait::async_trait;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use thiserror::Error;
use uuid::Uuid;

use super::queue::{DedupClaim, DepositDedupStore};
use super::submitter::{MintSubmitter, MintTransaction};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DepositAttestation {
    pub deposit_id: String,
    pub resource_id: String,
    pub amount: u64,
    pub meter_id: String,
    pub signature: [u8; 64],
}

impl DepositAttestation {
    pub fn signing_payload(&self) -> Vec<u8> {
        format!(
            "{}:{}:{}:{}",
            self.deposit_id, self.resource_id, self.amount, self.meter_id
        )
        .into_bytes()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationOutcome {
    Submitted {
        deposit_id: String,
        transaction_id: String,
    },
    DuplicateSkipped {
        deposit_id: String,
    },
}

#[derive(Debug, Error)]
pub enum AttestationError {
    #[error("invalid attestation signature")]
    InvalidSignature,
    #[error("dedup store error: {0}")]
    DedupStore(String),
    #[error("mint submission error: {0}")]
    Submit(String),
}

#[async_trait]
pub trait AttestationQueue: Send + Sync {
    async fn next_attestation(&self) -> Result<Option<DepositAttestation>, AttestationError>;
}

pub struct AttestationVerifier<Q, D, S> {
    queue: Q,
    dedup_store: D,
    submitter: S,
    verifying_key: VerifyingKey,
}

impl<Q, D, S> AttestationVerifier<Q, D, S>
where
    Q: AttestationQueue,
    D: DepositDedupStore,
    S: MintSubmitter,
{
    pub fn new(queue: Q, dedup_store: D, submitter: S, verifying_key: VerifyingKey) -> Self {
        Self {
            queue,
            dedup_store,
            submitter,
            verifying_key,
        }
    }

    pub async fn verify_and_submit(&self) -> Result<Option<VerificationOutcome>, AttestationError> {
        let Some(attestation) = self.queue.next_attestation().await? else {
            return Ok(None);
        };

        self.verify_signature(&attestation)?;

        let idempotency_key = idempotency_key_for_deposit(&attestation.deposit_id);
        let claim = self
            .dedup_store
            .claim_deposit(&attestation.deposit_id, idempotency_key)
            .await
            .map_err(AttestationError::DedupStore)?;

        match claim {
            DedupClaim::AlreadyClaimed => Ok(Some(VerificationOutcome::DuplicateSkipped {
                deposit_id: attestation.deposit_id,
            })),
            DedupClaim::Claimed { idempotency_key } => {
                let tx = MintTransaction::from_attestation(&attestation, idempotency_key);
                let transaction_id = self
                    .submitter
                    .submit_mint_transaction(tx)
                    .await
                    .map_err(AttestationError::Submit)?;
                Ok(Some(VerificationOutcome::Submitted {
                    deposit_id: attestation.deposit_id,
                    transaction_id,
                }))
            }
        }
    }

    fn verify_signature(&self, attestation: &DepositAttestation) -> Result<(), AttestationError> {
        let signature = Signature::from_bytes(&attestation.signature);
        self.verifying_key
            .verify(&attestation.signing_payload(), &signature)
            .map_err(|_| AttestationError::InvalidSignature)
    }
}

pub fn idempotency_key_for_deposit(deposit_id: &str) -> Uuid {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(format!("utility-backend:deposit:{deposit_id}").as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}
