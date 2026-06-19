use utility_backend::soroban::sequencer::NonceSequencer;
use utility_backend::soroban::tx_state::TxStateController;

#[test]
fn test_nonce_sequencer_order() {
    let seq = NonceSequencer::new();
    let n1 = seq.next_nonce("grid-east");
    let n2 = seq.next_nonce("grid-east");
    let n3 = seq.next_nonce("grid-west");
    assert!(n1 < n2);
    assert_ne!(n1, n3);
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
