//! Tests for Soroban reorg detection and recovery (issue #41).
//!
//! Chain access is mocked, so these run without a Soroban devnet while still
//! exercising divergence detection, rollback, replay-nonce protection, recovery
//! gating, depth-limit escalation, and reorg-log retention.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;

use utility_backend::blockchain::soroban::reorg_handler::{
    BatchState, LedgerInfo, LedgerOracle, ReorgConfig, ReorgContract, ReorgHandler,
};
use utility_backend::storage::reorg_log::ReorgLog;

struct MockOracle {
    canonical: Vec<LedgerInfo>,
}

#[async_trait]
impl LedgerOracle for MockOracle {
    async fn get_ledger_range(&self, start: u64, end: u64) -> Result<Vec<LedgerInfo>, String> {
        Ok(self
            .canonical
            .iter()
            .copied()
            .filter(|l| l.seq >= start && l.seq <= end)
            .collect())
    }

    async fn is_tx_in_ledger(&self, _tx: [u8; 32], _seq: u64) -> Result<bool, String> {
        Ok(true)
    }
}

#[derive(Default)]
struct MockContract {
    rollbacks: Mutex<Vec<u64>>,
    replays: Mutex<Vec<(u64, u64)>>,
    /// When set, rollback blocks on this lock (to simulate an in-flight recovery).
    gate: Option<Arc<tokio::sync::Mutex<()>>>,
}

#[async_trait]
impl ReorgContract for MockContract {
    async fn rollback_batch(&self, batch_id: u64, _reason: u32) -> Result<(), String> {
        if let Some(gate) = &self.gate {
            // Block until the test releases the gate, simulating in-flight work.
            // Named binding (not `_`) so it isn't flagged as an unused guard.
            let _held = gate.lock().await;
        }
        self.rollbacks.lock().push(batch_id);
        Ok(())
    }

    async fn submit_replay(
        &self,
        batch_id: u64,
        nonce: u64,
        _meter_ids: &[String],
    ) -> Result<(), String> {
        let mut replays = self.replays.lock();
        if replays.iter().any(|(_, n)| *n == nonce) {
            return Err("contract rejected duplicate nonce".to_string());
        }
        replays.push((batch_id, nonce));
        Ok(())
    }
}

fn hsh(b: u8) -> [u8; 32] {
    [b; 32]
}

fn batch(id: u64, seq: u64, hash: [u8; 32], close_time: u64) -> BatchState {
    let mut tx = [0u8; 32];
    tx[0] = id as u8;
    tx[1..9].copy_from_slice(&seq.to_le_bytes());
    BatchState {
        batch_id: id,
        source_meter_ids: vec![format!("MTR-{id}")],
        total_scaled_amount: u128::from(id) * 1000,
        ledger_seq: seq,
        ledger_hash: hash,
        ledger_close_time: close_time,
        tx_envelope_hash: tx,
        state_commitment: [id as u8; 32],
    }
}

fn canonical(seqs: std::ops::RangeInclusive<u64>) -> Vec<LedgerInfo> {
    seqs.map(|s| LedgerInfo {
        seq: s,
        hash: hsh(s as u8),
    })
    .collect()
}

fn handler_with(
    canon: Vec<LedgerInfo>,
    retention: usize,
    contract: Arc<MockContract>,
) -> (Arc<ReorgHandler>, Arc<ReorgLog>) {
    let oracle: Arc<dyn LedgerOracle> = Arc::new(MockOracle { canonical: canon });
    let log = Arc::new(ReorgLog::new(retention));
    let handler = Arc::new(ReorgHandler::new(
        oracle,
        contract as Arc<dyn ReorgContract>,
        log.clone(),
        ReorgConfig::default(),
    ));
    (handler, log)
}

#[test]
fn test_batch_state_codec_roundtrip() {
    let b = batch(7, 42, hsh(9), 1700);
    let bytes = b.to_bytes();
    assert_eq!(BatchState::from_bytes(&bytes).as_ref(), Some(&b));
    assert!(BatchState::from_bytes(&bytes[..bytes.len() - 1]).is_none());
}

#[tokio::test]
async fn test_detect_and_recover_reorg() {
    let contract = Arc::new(MockContract::default());
    let (handler, log) = handler_with(canonical(1..=10), 1000, contract.clone());

    // Local chain matches canonical below seq 6 and diverges from 6 onward.
    for s in 1..=10u64 {
        let local_hash = if s < 6 {
            hsh(s as u8)
        } else {
            hsh(100 + s as u8)
        };
        handler
            .record_settled_batch(batch(s, s, local_hash, 1000))
            .unwrap();
    }
    assert_eq!(log.len(), 10);

    let reorg = handler
        .detect_reorg()
        .await
        .unwrap()
        .expect("reorg should be detected");
    assert_eq!(reorg.divergence_seq, 6);
    assert_eq!(reorg.depth, 5);
    assert_eq!(reorg.affected_batches, vec![6, 7, 8, 9, 10]);
    assert!(!reorg.requires_manual);

    let outcome = handler.recover(&reorg).await.unwrap();
    assert_eq!(outcome.rolled_back, 5);
    assert_eq!(outcome.replayed, 5);
    assert_eq!(outcome.epoch, 1);

    assert_eq!(*contract.rollbacks.lock(), vec![6, 7, 8, 9, 10]);

    let replays = contract.replays.lock().clone();
    assert_eq!(replays.len(), 5);
    let nonces: HashSet<u64> = replays.iter().map(|(_, n)| *n).collect();
    assert_eq!(nonces.len(), 5, "replay nonces must be unique");

    assert!(
        log.batches_from_seq(6).is_empty(),
        "rolled-back batches gone"
    );
    assert!(!handler.is_recovering());
}

#[tokio::test]
async fn test_deep_reorg_requires_manual_intervention() {
    let contract = Arc::new(MockContract::default());
    let (handler, _log) = handler_with(canonical(1..=20), 1000, contract.clone());

    // Diverge from seq 5 -> depth 16 > auto limit 10.
    for s in 1..=20u64 {
        let local_hash = if s < 5 {
            hsh(s as u8)
        } else {
            hsh(100 + s as u8)
        };
        handler
            .record_settled_batch(batch(s, s, local_hash, 1000))
            .unwrap();
    }

    let reorg = handler.detect_reorg().await.unwrap().expect("reorg");
    assert_eq!(reorg.divergence_seq, 5);
    assert!(reorg.requires_manual);

    let err = handler.recover(&reorg).await.unwrap_err();
    assert!(err.contains("operator action"), "got: {err}");
    assert!(contract.rollbacks.lock().is_empty(), "no auto rollback");
}

#[tokio::test]
async fn test_settlements_queued_during_recovery() {
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let contract = Arc::new(MockContract {
        gate: Some(gate.clone()),
        ..Default::default()
    });
    let (handler, log) = handler_with(canonical(1..=4), 1000, contract);

    for s in 1..=4u64 {
        let local_hash = if s < 3 {
            hsh(s as u8)
        } else {
            hsh(50 + s as u8)
        };
        handler
            .record_settled_batch(batch(s, s, local_hash, 1000))
            .unwrap();
    }
    let reorg = handler.detect_reorg().await.unwrap().expect("reorg");
    assert_eq!(reorg.affected_batches, vec![3, 4]);

    // Hold the gate so the first rollback blocks inside recover().
    let guard = gate.lock().await;
    let task = {
        let handler = handler.clone();
        tokio::spawn(async move { handler.recover(&reorg).await })
    };

    for _ in 0..10_000 {
        if handler.is_recovering() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(handler.is_recovering());

    // A settlement arriving mid-recovery must be buffered, not logged.
    let queued = handler
        .record_settled_batch(batch(99, 99, hsh(7), 1000))
        .unwrap();
    assert!(queued, "settlement should be queued during recovery");
    assert_eq!(handler.queued_len(), 1);
    assert!(log.batches_from_seq(99).is_empty(), "not logged yet");

    drop(guard);
    let outcome = task.await.unwrap().unwrap();
    assert_eq!(outcome.rolled_back, 2);

    // Queue flushed into the log once recovery completed.
    assert_eq!(handler.queued_len(), 0);
    assert_eq!(log.batches_from_seq(99).len(), 1);
}

#[test]
fn test_count_retention_window() {
    let contract = Arc::new(MockContract::default());
    let (handler, log) = handler_with(Vec::new(), 1000, contract);

    for i in 0..1200u64 {
        handler
            .record_settled_batch(batch(i, i, hsh((i % 250) as u8), 1000))
            .unwrap();
    }
    assert_eq!(log.len(), 1000, "only the last 1000 batches are retained");
    assert!(log.batches_from_seq(0).iter().all(|b| b.ledger_seq >= 200));
}

#[test]
fn test_age_based_pruning() {
    let contract = Arc::new(MockContract::default());
    let (handler, log) = handler_with(Vec::new(), 1000, contract);

    handler
        .record_settled_batch(batch(1, 1, hsh(1), 0))
        .unwrap(); // very old
    handler
        .record_settled_batch(batch(2, 2, hsh(2), 190_000))
        .unwrap(); // recent

    // now = 200_000, retention = 24h (86_400s): batch 1 is stale, batch 2 is not.
    let pruned = handler.prune(200_000);
    assert_eq!(pruned, 1);
    assert_eq!(log.len(), 1);
    assert_eq!(log.batches_from_seq(0)[0].batch_id, 2);
}
