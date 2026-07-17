use std::{sync::Arc, time::Duration};

use utility_backend::storage::job_scheduler::{
    InMemoryJobStore, JobScheduler, JobState, JobStore, SchedulerConfig,
};

#[tokio::test]
async fn only_one_worker_claims_a_pending_job() {
    let store = Arc::new(InMemoryJobStore::default());
    let worker_a = JobScheduler::new(store.clone(), SchedulerConfig::new("billing", "worker-a"));
    let worker_b = JobScheduler::new(store.clone(), SchedulerConfig::new("billing", "worker-b"));

    let id = worker_a
        .enqueue(serde_json::json!({"meter":"m-1"}))
        .await
        .unwrap();

    let claimed_a = worker_a.claim_batch().await.unwrap();
    let claimed_b = worker_b.claim_batch().await.unwrap();

    assert_eq!(claimed_a.len(), 1);
    assert!(claimed_b.is_empty());
    assert_eq!(claimed_a[0].job.id, id);
    assert_eq!(
        store.get(id).unwrap().lease_owner.as_deref(),
        Some("worker-a")
    );
}

#[tokio::test]
async fn expired_lease_can_be_reclaimed_by_another_worker() {
    let store = Arc::new(InMemoryJobStore::default());
    let mut config_a = SchedulerConfig::new("billing", "worker-a");
    config_a.lease_ttl = Duration::from_millis(1);
    let worker_a = JobScheduler::new(store.clone(), config_a);
    let worker_b = JobScheduler::new(store.clone(), SchedulerConfig::new("billing", "worker-b"));

    let id = worker_a
        .enqueue(serde_json::json!({"meter":"m-2"}))
        .await
        .unwrap();
    assert_eq!(worker_a.claim_batch().await.unwrap().len(), 1);
    tokio::time::sleep(Duration::from_millis(5)).await;

    let reclaimed = worker_b.claim_batch().await.unwrap();

    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].job.id, id);
    let job = store.get(id).unwrap();
    assert_eq!(job.lease_owner.as_deref(), Some("worker-b"));
    assert_eq!(job.attempts, 2);
}

#[tokio::test]
async fn stale_lease_token_cannot_complete_reclaimed_job() {
    let store = Arc::new(InMemoryJobStore::default());
    let mut config_a = SchedulerConfig::new("billing", "worker-a");
    config_a.lease_ttl = Duration::from_millis(1);
    let worker_a = JobScheduler::new(store.clone(), config_a);
    let worker_b = JobScheduler::new(store.clone(), SchedulerConfig::new("billing", "worker-b"));

    let id = worker_a
        .enqueue(serde_json::json!({"meter":"m-3"}))
        .await
        .unwrap();
    let first = worker_a.claim_batch().await.unwrap().pop().unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let second = worker_b.claim_batch().await.unwrap().pop().unwrap();

    assert!(store.complete(id, first.lease_token).await.is_err());
    store.complete(id, second.lease_token).await.unwrap();
    assert_eq!(store.get(id).unwrap().state, JobState::Completed);
}
