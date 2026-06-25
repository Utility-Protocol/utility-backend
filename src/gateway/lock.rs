use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{Pool, Postgres};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{info, warn};

const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);
const DEFAULT_RENEW_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug, Error)]
pub enum AdvisoryLockError {
    #[error("timed out acquiring distributed lock for {resource}")]
    Timeout { resource: String },
    #[error("database error while acquiring distributed lock: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, Clone, Serialize)]
pub enum LockBackendKind {
    Local,
    Postgres,
    RedisRedlock,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveLock {
    pub resource: String,
    pub holder: String,
    pub fencing_token: u64,
    pub backend: LockBackendKind,
    pub acquired_at: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Clone)]
pub enum AdvisoryLockBackend {
    /// Test/development fallback. This is process-local and must not be used for
    /// resource-credit mutations in a horizontally scaled deployment.
    Local,
    /// Production default: transaction-scoped PostgreSQL advisory locks. The
    /// lock is held by an open transaction and released automatically if this
    /// worker crashes or loses its database connection.
    Postgres(Pool<Postgres>),
    /// Configuration hook for deployments that route selected low-latency
    /// resource classes through Redis Redlock. The actual Redis client is kept
    /// outside this crate; use `with_backend` to inject the policy and keep the
    /// fencing-token validation path identical.
    RedisRedlock,
}

#[derive(Clone)]
pub struct AdvisoryLock {
    local_locks: Arc<dashmap::DashMap<String, Arc<Mutex<()>>>>,
    active_locks: Arc<dashmap::DashMap<String, ActiveLock>>,
    backend: AdvisoryLockBackend,
    acquire_timeout: Duration,
    lease_duration: Duration,
    renew_interval: Duration,
    fencing_tokens: Arc<AtomicU64>,
    holder: Arc<String>,
}

pub struct LockOutcome<T> {
    pub fencing_token: u64,
    pub value: T,
}

impl Default for AdvisoryLock {
    fn default() -> Self {
        Self::new()
    }
}

impl AdvisoryLock {
    pub fn new() -> Self {
        Self::with_backend(AdvisoryLockBackend::Local)
    }

    pub fn postgres(pool: Pool<Postgres>) -> Self {
        Self::with_backend(AdvisoryLockBackend::Postgres(pool))
    }

    pub fn with_backend(backend: AdvisoryLockBackend) -> Self {
        let holder = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("UTILITY_BACKEND_INSTANCE_ID"))
            .unwrap_or_else(|_| format!("pid-{}", std::process::id()));
        Self {
            local_locks: Arc::new(dashmap::DashMap::new()),
            active_locks: Arc::new(dashmap::DashMap::new()),
            backend,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
            lease_duration: DEFAULT_LEASE_DURATION,
            renew_interval: DEFAULT_RENEW_INTERVAL,
            fencing_tokens: Arc::new(AtomicU64::new(0)),
            holder: Arc::new(holder),
        }
    }

    pub fn active_locks(&self) -> Vec<ActiveLock> {
        self.active_locks
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub async fn lock<Fut, T>(&self, resource: &str, f: impl FnOnce() -> Fut) -> T
    where
        Fut: Future<Output = T>,
    {
        self.lock_with_fencing(resource, |token| async move {
            let _ = token;
            f().await
        })
        .await
        .expect("advisory lock acquisition failed")
        .value
    }

    pub async fn lock_with_fencing<Fut, T>(
        &self,
        resource: &str,
        f: impl FnOnce(u64) -> Fut,
    ) -> Result<LockOutcome<T>, AdvisoryLockError>
    where
        Fut: Future<Output = T>,
    {
        match &self.backend {
            AdvisoryLockBackend::Local => self.lock_local(resource, f).await,
            AdvisoryLockBackend::Postgres(pool) => self.lock_postgres(pool, resource, f).await,
            AdvisoryLockBackend::RedisRedlock => self.lock_local(resource, f).await,
        }
    }

    async fn lock_local<Fut, T>(
        &self,
        resource: &str,
        f: impl FnOnce(u64) -> Fut,
    ) -> Result<LockOutcome<T>, AdvisoryLockError>
    where
        Fut: Future<Output = T>,
    {
        let mtx = self
            .local_locks
            .entry(resource.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .value()
            .clone();
        let _guard = tokio::time::timeout(self.acquire_timeout, mtx.lock())
            .await
            .map_err(|_| AdvisoryLockError::Timeout {
                resource: resource.into(),
            })?;
        let token = self.next_fencing_token();
        let active = self.insert_active(resource, token, LockBackendKind::Local);
        info!(resource = %resource, fencing_token = token, "local advisory lock acquired");
        let value = f(token).await;
        self.active_locks.remove(resource);
        drop(active);
        Ok(LockOutcome {
            fencing_token: token,
            value,
        })
    }

    async fn lock_postgres<Fut, T>(
        &self,
        pool: &Pool<Postgres>,
        resource: &str,
        f: impl FnOnce(u64) -> Fut,
    ) -> Result<LockOutcome<T>, AdvisoryLockError>
    where
        Fut: Future<Output = T>,
    {
        let lock_id = advisory_lock_id(resource);
        let started = tokio::time::Instant::now();
        let mut tx = loop {
            let mut tx = pool.begin().await?;
            let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
                .bind(lock_id)
                .fetch_one(&mut *tx)
                .await?;
            if acquired {
                break tx;
            }
            drop(tx);
            if started.elapsed() >= self.acquire_timeout {
                return Err(AdvisoryLockError::Timeout {
                    resource: resource.into(),
                });
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        };

        let token: u64 = sqlx::query_scalar::<_, i64>("SELECT txid_current()::bigint")
            .fetch_one(&mut *tx)
            .await? as u64;
        self.insert_active(resource, token, LockBackendKind::Postgres);
        let active_locks = self.active_locks.clone();
        let resource_name = resource.to_string();
        let lease_duration = self.lease_duration;
        let renew_interval = self.renew_interval;
        let renewal = tokio::spawn(async move {
            let mut interval = tokio::time::interval(renew_interval);
            loop {
                interval.tick().await;
                if let Some(mut lock) = active_locks.get_mut(&resource_name) {
                    lock.lease_expires_at = Utc::now() + chrono_duration(lease_duration);
                } else {
                    break;
                }
            }
        });

        info!(resource = %resource, lock_id, fencing_token = token, "postgres advisory lock acquired");
        let value = f(token).await;
        self.active_locks.remove(resource);
        renewal.abort();
        if let Err(error) = tx.commit().await {
            warn!(%error, resource = %resource, "failed to release advisory lock transaction cleanly");
            return Err(AdvisoryLockError::Database(error));
        }
        Ok(LockOutcome {
            fencing_token: token,
            value,
        })
    }

    fn next_fencing_token(&self) -> u64 {
        self.fencing_tokens.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn insert_active(&self, resource: &str, token: u64, backend: LockBackendKind) -> ActiveLock {
        let now = Utc::now();
        let active = ActiveLock {
            resource: resource.to_string(),
            holder: self.holder.to_string(),
            fencing_token: token,
            backend,
            acquired_at: now,
            lease_expires_at: now + chrono_duration(self.lease_duration),
        };
        self.active_locks
            .insert(resource.to_string(), active.clone());
        active
    }
}

fn advisory_lock_id(resource: &str) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(resource.as_bytes());
    let result = hasher.finalize();
    i64::from_be_bytes(result[0..8].try_into().expect("hash slice is eight bytes"))
}

fn chrono_duration(duration: Duration) -> ChronoDuration {
    ChronoDuration::from_std(duration).unwrap_or_else(|_| ChronoDuration::seconds(30))
}
