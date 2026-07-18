//! Distributed job scheduler with lease-based worker claiming.
//!
//! The scheduler is intentionally storage-agnostic: [`JobStore`] can be backed
//! by Postgres, RocksDB, or the in-memory implementation used by tests. Claiming
//! is atomic at the store boundary so multiple workers can race safely. Leases
//! make crashed workers self-healing: once `lease_expires_at` passes, another
//! worker may reclaim the job.

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time;
use uuid::Uuid;

use crate::api::metrics;

pub type JobResult<T> = Result<T, JobSchedulerError>;
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    Pending,
    Leased,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub queue: String,
    pub payload: serde_json::Value,
    pub state: JobState,
    pub attempts: u32,
    pub max_attempts: u32,
    pub run_at: DateTime<Utc>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

impl Job {
    pub fn new(queue: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            queue: queue.into(),
            payload,
            state: JobState::Pending,
            attempts: 0,
            max_attempts: 3,
            run_at: Utc::now(),
            lease_owner: None,
            lease_expires_at: None,
            last_error: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClaimedJob {
    pub job: Job,
    pub lease_token: Uuid,
}

#[derive(Clone, Debug)]
pub struct SchedulerConfig {
    pub queue: String,
    pub worker_id: String,
    pub lease_ttl: Duration,
    pub poll_interval: Duration,
    pub batch_size: usize,
}

impl SchedulerConfig {
    pub fn new(queue: impl Into<String>, worker_id: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            worker_id: worker_id.into(),
            lease_ttl: Duration::from_secs(30),
            poll_interval: Duration::from_millis(50),
            batch_size: 32,
        }
    }
}

#[derive(Debug, Error)]
pub enum JobSchedulerError {
    #[error("job lease is no longer owned by this worker")]
    LeaseLost,
    #[error("job not found: {0}")]
    NotFound(Uuid),
    #[error("store error: {0}")]
    Store(String),
}

pub trait JobStore: Send + Sync + 'static {
    fn enqueue<'a>(&'a self, job: Job) -> BoxFuture<'a, JobResult<Uuid>>;
    fn claim_due<'a>(
        &'a self,
        queue: &'a str,
        worker_id: &'a str,
        lease_ttl: Duration,
        limit: usize,
    ) -> BoxFuture<'a, JobResult<Vec<ClaimedJob>>>;
    fn heartbeat<'a>(
        &'a self,
        job_id: Uuid,
        lease_token: Uuid,
        lease_ttl: Duration,
    ) -> BoxFuture<'a, JobResult<()>>;
    fn complete<'a>(&'a self, job_id: Uuid, lease_token: Uuid) -> BoxFuture<'a, JobResult<()>>;
    fn fail<'a>(
        &'a self,
        job_id: Uuid,
        lease_token: Uuid,
        error: String,
        retry_after: Duration,
    ) -> BoxFuture<'a, JobResult<()>>;
}

#[derive(Default)]
pub struct InMemoryJobStore {
    jobs: Mutex<HashMap<Uuid, (Job, Option<Uuid>)>>,
}

impl InMemoryJobStore {
    pub fn get(&self, id: Uuid) -> Option<Job> {
        self.jobs.lock().get(&id).map(|(job, _)| job.clone())
    }

    fn validate_lease(
        jobs: &mut HashMap<Uuid, (Job, Option<Uuid>)>,
        job_id: Uuid,
        lease_token: Uuid,
    ) -> JobResult<&mut Job> {
        let (job, token) = jobs
            .get_mut(&job_id)
            .ok_or(JobSchedulerError::NotFound(job_id))?;
        if job.state != JobState::Leased || *token != Some(lease_token) {
            return Err(JobSchedulerError::LeaseLost);
        }
        Ok(job)
    }
}

impl JobStore for InMemoryJobStore {
    fn enqueue<'a>(&'a self, job: Job) -> BoxFuture<'a, JobResult<Uuid>> {
        Box::pin(async move {
            let id = job.id;
            self.jobs.lock().insert(id, (job, None));
            metrics::inc_job_scheduler_enqueued();
            Ok(id)
        })
    }

    fn claim_due<'a>(
        &'a self,
        queue: &'a str,
        worker_id: &'a str,
        lease_ttl: Duration,
        limit: usize,
    ) -> BoxFuture<'a, JobResult<Vec<ClaimedJob>>> {
        Box::pin(async move {
            let now = Utc::now();
            let expires = now + chrono::Duration::from_std(lease_ttl).unwrap_or_default();
            let mut jobs = self.jobs.lock();
            let mut claimed = Vec::new();
            for (job, token) in jobs.values_mut() {
                if claimed.len() == limit {
                    break;
                }
                let lease_expired = job.lease_expires_at.map(|ts| ts <= now).unwrap_or(true);
                if job.queue == queue
                    && job.run_at <= now
                    && job.attempts < job.max_attempts
                    && (job.state == JobState::Pending
                        || (job.state == JobState::Leased && lease_expired))
                {
                    let lease_token = Uuid::new_v4();
                    job.state = JobState::Leased;
                    job.attempts += 1;
                    job.lease_owner = Some(worker_id.to_owned());
                    job.lease_expires_at = Some(expires);
                    *token = Some(lease_token);
                    claimed.push(ClaimedJob {
                        job: job.clone(),
                        lease_token,
                    });
                }
            }
            metrics::inc_job_scheduler_claimed(claimed.len() as u64);
            Ok(claimed)
        })
    }

    fn heartbeat<'a>(
        &'a self,
        job_id: Uuid,
        lease_token: Uuid,
        lease_ttl: Duration,
    ) -> BoxFuture<'a, JobResult<()>> {
        Box::pin(async move {
            let mut jobs = self.jobs.lock();
            let job = Self::validate_lease(&mut jobs, job_id, lease_token)?;
            job.lease_expires_at =
                Some(Utc::now() + chrono::Duration::from_std(lease_ttl).unwrap_or_default());
            metrics::inc_job_scheduler_heartbeat();
            Ok(())
        })
    }

    fn complete<'a>(&'a self, job_id: Uuid, lease_token: Uuid) -> BoxFuture<'a, JobResult<()>> {
        Box::pin(async move {
            let mut jobs = self.jobs.lock();
            let job = Self::validate_lease(&mut jobs, job_id, lease_token)?;
            job.state = JobState::Completed;
            job.lease_expires_at = None;
            metrics::inc_job_scheduler_completed();
            Ok(())
        })
    }

    fn fail<'a>(
        &'a self,
        job_id: Uuid,
        lease_token: Uuid,
        error: String,
        retry_after: Duration,
    ) -> BoxFuture<'a, JobResult<()>> {
        Box::pin(async move {
            let mut jobs = self.jobs.lock();
            let job = Self::validate_lease(&mut jobs, job_id, lease_token)?;
            job.last_error = Some(error);
            job.lease_owner = None;
            job.lease_expires_at = None;
            job.state = if job.attempts >= job.max_attempts {
                JobState::Failed
            } else {
                JobState::Pending
            };
            job.run_at = Utc::now() + chrono::Duration::from_std(retry_after).unwrap_or_default();
            metrics::inc_job_scheduler_failed();
            Ok(())
        })
    }
}

pub struct JobScheduler<S> {
    store: Arc<S>,
    config: SchedulerConfig,
}

impl<S: JobStore> JobScheduler<S> {
    pub fn new(store: Arc<S>, config: SchedulerConfig) -> Self {
        Self { store, config }
    }

    pub async fn enqueue(&self, payload: serde_json::Value) -> JobResult<Uuid> {
        self.store
            .enqueue(Job::new(self.config.queue.clone(), payload))
            .await
    }

    pub async fn claim_batch(&self) -> JobResult<Vec<ClaimedJob>> {
        self.store
            .claim_due(
                &self.config.queue,
                &self.config.worker_id,
                self.config.lease_ttl,
                self.config.batch_size,
            )
            .await
    }

    pub async fn run<F, Fut>(&self, mut handler: F) -> JobResult<()>
    where
        F: FnMut(ClaimedJob) -> Fut,
        Fut: Future<Output = Result<(), String>>,
    {
        let mut interval = time::interval(self.config.poll_interval);
        loop {
            interval.tick().await;
            for claimed in self.claim_batch().await? {
                let id = claimed.job.id;
                let token = claimed.lease_token;
                match handler(claimed).await {
                    Ok(()) => self.store.complete(id, token).await?,
                    Err(error) => {
                        self.store
                            .fail(id, token, error, self.config.poll_interval)
                            .await?
                    }
                }
            }
        }
    }
}
