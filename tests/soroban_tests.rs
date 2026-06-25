use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use utility_backend::soroban::sequencer::NonceSequencer;
use utility_backend::soroban::tx_state::TxStateController;

#[test]
fn test_nonce_sequencer_order() {
    let seq = NonceSequencer::new();
    let n1 = seq.next_nonce("grid-east");
    let n2 = seq.next_nonce("grid-east");
    let _n3 = seq.next_nonce("grid-west");
    assert!(n1 < n2, "nonces should be strictly increasing per grid");
}

#[test]
fn test_commit_nonce_per_grid() {
    let seq = NonceSequencer::new();
    assert!(seq.commit_nonce("grid-east", 1).is_ok());
    assert!(seq.commit_nonce("grid-west", 1).is_ok());
    assert!(seq.commit_nonce("grid-east", 2).is_ok());
    assert!(seq.commit_nonce("grid-east", 1).is_err());
}

#[tokio::test]
async fn test_two_phase_commit_rollback() {
    let ctrl = TxStateController::new();
    ctrl.begin("tx-001".into()).await;
    ctrl.begin("tx-002".into()).await;
    assert!(ctrl.commit("tx-001").await.is_ok());
    assert!(ctrl.rollback("tx-002").await.is_ok());
    assert!(ctrl.commit("tx-003").await.is_err());
}

proptest::proptest! {
    #[test]
    fn test_concurrent_nonce_issuance(
        grid_count in 1usize..=20,
        tasks_per_grid in 1usize..=5,
        nonces_per_task in 1usize..=20,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let seq = Arc::new(NonceSequencer::new());
            let mut handles: Vec<(String, tokio::task::JoinHandle<Vec<u64>>)> = Vec::new();

            for g in 0..grid_count {
                let gid = format!("grid-{:02}", g);
                for _ in 0..tasks_per_grid {
                    let seq = seq.clone();
                    let gid = gid.clone();
                    handles.push((gid.clone(), tokio::spawn(async move {
                        let mut v = Vec::with_capacity(nonces_per_task);
                        for _ in 0..nonces_per_task {
                            v.push(seq.next_nonce(&gid));
                        }
                        v
                    })));
                }
            }

            let mut grid_nonces: HashMap<String, Vec<u64>> = HashMap::new();
            for (gid, h) in handles {
                let nonces = h.await.expect("task panicked");
                grid_nonces.entry(gid).or_default().extend(nonces);
            }

            for (grid, nonces) in &grid_nonces {
                let unique: HashSet<u64> = nonces.iter().copied().collect();
                assert_eq!(
                    nonces.len(),
                    unique.len(),
                    "duplicate nonces detected for grid {}: {} nonces, {} unique",
                    grid,
                    nonces.len(),
                    unique.len()
                );
                let mut sorted = nonces.clone();
                sorted.sort();
                for i in 1..sorted.len() {
                    assert!(
                        sorted[i - 1] < sorted[i],
                        "nonce ordering violation for grid {}: {} >= {}",
                        grid,
                        sorted[i - 1],
                        sorted[i]
                    );
                }
            }
        });
    }
}

#[test]
fn test_soroban_sync_gap_detection_allows_small_and_large_skips() {
    use chrono::Utc;
    use utility_backend::soroban::sync::{detect_gaps, LedgerEvent};

    let events = vec![
        LedgerEvent {
            event_id: "evt-11".into(),
            contract_id: "contract-a".into(),
            sequence: 11,
            timestamp: Utc::now(),
        },
        LedgerEvent {
            event_id: "evt-150".into(),
            contract_id: "contract-a".into(),
            sequence: 150,
            timestamp: Utc::now(),
        },
    ];

    let gaps = detect_gaps("contract-a", 10, &events);
    assert_eq!(gaps.len(), 1);
    assert_eq!(gaps[0].start_sequence, 12);
    assert_eq!(gaps[0].end_sequence, 149);
}

#[test]
fn test_soroban_sync_gap_detection_ignores_contiguous_events() {
    use chrono::Utc;
    use utility_backend::soroban::sync::{detect_gaps, LedgerEvent};

    let events = (6..=8)
        .map(|sequence| LedgerEvent {
            event_id: format!("evt-{sequence}"),
            contract_id: "contract-b".into(),
            sequence,
            timestamp: Utc::now(),
        })
        .collect::<Vec<_>>();

    assert!(detect_gaps("contract-b", 5, &events).is_empty());
}
