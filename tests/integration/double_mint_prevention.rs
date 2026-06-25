use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use ed25519_dalek::{Signer, SigningKey};
use parking_lot::Mutex;
use rand::rngs::OsRng;
use tokio::task::JoinSet;
use utility_backend::settlement::attestation_verifier::{
    AttestationError, AttestationQueue, AttestationVerifier, DepositAttestation,
    VerificationOutcome,
};
use utility_backend::settlement::queue::{DedupClaim, DepositDedupStore};
use utility_backend::settlement::submitter::{MintSubmitter, MintTransaction};
use uuid::Uuid;

#[derive(Clone)]
struct InMemoryQueue {
    attestations: Arc<Mutex<VecDeque<DepositAttestation>>>,
}

#[async_trait]
impl AttestationQueue for InMemoryQueue {
    async fn next_attestation(&self) -> Result<Option<DepositAttestation>, AttestationError> {
        Ok(self.attestations.lock().pop_front())
    }
}

#[derive(Clone, Default)]
struct InMemoryDedupStore {
    processed: Arc<Mutex<HashSet<String>>>,
}

#[async_trait]
impl DepositDedupStore for InMemoryDedupStore {
    async fn claim_deposit(
        &self,
        deposit_id: &str,
        idempotency_key: Uuid,
    ) -> Result<DedupClaim, String> {
        let mut processed = self.processed.lock();
        if processed.insert(deposit_id.to_string()) {
            Ok(DedupClaim::Claimed { idempotency_key })
        } else {
            Ok(DedupClaim::AlreadyClaimed)
        }
    }
}

#[derive(Clone, Default)]
struct RecordingSubmitter {
    submitted: Arc<Mutex<Vec<MintTransaction>>>,
}

#[async_trait]
impl MintSubmitter for RecordingSubmitter {
    async fn submit_mint_transaction(
        &self,
        transaction: MintTransaction,
    ) -> Result<String, String> {
        tokio::task::yield_now().await;
        let mut submitted = self.submitted.lock();
        submitted.push(transaction);
        Ok(format!("tx-{}", submitted.len()))
    }
}

fn signed_attestation(
    signing_key: &SigningKey,
    deposit_id: &str,
    meter_id: &str,
) -> DepositAttestation {
    let mut attestation = DepositAttestation {
        deposit_id: deposit_id.to_string(),
        resource_id: "resource-node-7".to_string(),
        amount: 42,
        meter_id: meter_id.to_string(),
        signature: [0; 64],
    };
    attestation.signature = signing_key.sign(&attestation.signing_payload()).to_bytes();
    attestation
}

#[tokio::test]
async fn duplicate_deposit_attestations_submit_exactly_one_mint() {
    let signing_key = SigningKey::generate(&mut OsRng);
    let verifying_key = signing_key.verifying_key();
    let attestations = (0..10)
        .map(|idx| {
            signed_attestation(
                &signing_key,
                "deposit-duplicate-001",
                &format!("meter-{idx}"),
            )
        })
        .collect::<VecDeque<_>>();

    let queue = InMemoryQueue {
        attestations: Arc::new(Mutex::new(attestations)),
    };
    let dedup = InMemoryDedupStore::default();
    let submitter = RecordingSubmitter::default();

    let mut tasks = JoinSet::new();
    for _ in 0..10 {
        let verifier = AttestationVerifier::new(
            queue.clone(),
            dedup.clone(),
            submitter.clone(),
            verifying_key,
        );
        tasks.spawn(async move { verifier.verify_and_submit().await });
    }

    let mut submitted = 0;
    let mut skipped = 0;
    while let Some(result) = tasks.join_next().await {
        match result.expect("task panicked").expect("verification failed") {
            Some(VerificationOutcome::Submitted { .. }) => submitted += 1,
            Some(VerificationOutcome::DuplicateSkipped { .. }) => skipped += 1,
            None => {}
        }
    }

    assert_eq!(submitted, 1);
    assert_eq!(skipped, 9);
    let txs = submitter.submitted.lock();
    assert_eq!(txs.len(), 1);
    assert_eq!(txs[0].deposit_id, "deposit-duplicate-001");
    assert!(txs[0].memo.contains("idempotency:"));
}
