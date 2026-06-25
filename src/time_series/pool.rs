use crate::api::metrics;
use deadpool_postgres::tokio_postgres::NoTls;
use deadpool_postgres::{
    Config, ManagerConfig, Object, Pool, PoolConfig, RecyclingMethod, Runtime,
};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use tracing::{info, warn};

const DEFAULT_STATEMENT_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECTION_MEMORY_BYTES: usize = 10 * 1024 * 1024;
const DEFAULT_MEMORY_BUDGET_BYTES: usize = 50 * 1024 * 1024;
const MIN_GUARANTEE_PERCENT: usize = 20;
const BURST_PERCENT: usize = 80;
const CIRCUIT_BREAKER_ERROR_RATE: f64 = 0.10;
const CIRCUIT_BREAKER_WINDOW: Duration = Duration::from_secs(5 * 60);
const CIRCUIT_BREAKER_COOLDOWN: Duration = Duration::from_secs(60);

static GLOBAL_POOL_MANAGER: OnceLock<Arc<MultiTenantPoolManager>> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct TenantPool {
    pub tenant_id: String,
    pub host: String,
    pub dbname: String,
    pub statement_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct TenantIsolationPolicy {
    pub tenant_id: String,
    pub statement_timeout: Duration,
    pub max_cpu_time: Duration,
    pub max_memory_bytes: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct TenantUsage {
    pub tenant_id: String,
    pub active_connections: usize,
    pub queued_queries: usize,
    pub average_latency_ms: f64,
    pub total_queries: u64,
    pub error_rate: f64,
    pub cpu_time_ms: f64,
    pub memory_bytes: i64,
    pub circuit_breaker_open: bool,
    pub low_priority: bool,
}

#[derive(Debug)]
struct TenantRuntimeState {
    active_connections: usize,
    queued_queries: usize,
    latency_samples: VecDeque<Duration>,
    window_queries: VecDeque<(Instant, bool)>,
    total_queries: u64,
    cpu_time: Duration,
    memory_bytes: i64,
    circuit_breaker_opened_at: Option<Instant>,
}

impl TenantRuntimeState {
    fn new() -> Self {
        Self {
            active_connections: 0,
            queued_queries: 0,
            latency_samples: VecDeque::with_capacity(128),
            window_queries: VecDeque::with_capacity(128),
            total_queries: 0,
            cpu_time: Duration::ZERO,
            memory_bytes: 0,
            circuit_breaker_opened_at: None,
        }
    }

    fn record_completion(&mut self, latency: Duration, failed: bool) {
        self.total_queries += 1;
        self.latency_samples.push_back(latency);
        if self.latency_samples.len() > 100 {
            self.latency_samples.pop_front();
        }
        let now = Instant::now();
        self.window_queries.push_back((now, failed));
        self.prune_error_window(now);
        if self.error_rate(now) > CIRCUIT_BREAKER_ERROR_RATE {
            self.circuit_breaker_opened_at = Some(now);
        }
    }

    fn prune_error_window(&mut self, now: Instant) {
        while self
            .window_queries
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > CIRCUIT_BREAKER_WINDOW)
        {
            self.window_queries.pop_front();
        }
        if self
            .circuit_breaker_opened_at
            .is_some_and(|opened| now.duration_since(opened) >= CIRCUIT_BREAKER_COOLDOWN)
            && self.error_rate(now) <= CIRCUIT_BREAKER_ERROR_RATE
        {
            self.circuit_breaker_opened_at = None;
        }
    }

    fn error_rate(&self, now: Instant) -> f64 {
        let mut total = 0_u64;
        let mut errors = 0_u64;
        for (at, failed) in &self.window_queries {
            if now.duration_since(*at) <= CIRCUIT_BREAKER_WINDOW {
                total += 1;
                errors += u64::from(*failed);
            }
        }
        if total == 0 {
            0.0
        } else {
            errors as f64 / total as f64
        }
    }

    fn usage(&self, tenant_id: &str) -> TenantUsage {
        let now = Instant::now();
        let avg = if self.latency_samples.is_empty() {
            0.0
        } else {
            self.latency_samples
                .iter()
                .map(|d| d.as_secs_f64() * 1000.0)
                .sum::<f64>()
                / self.latency_samples.len() as f64
        };
        TenantUsage {
            tenant_id: tenant_id.to_string(),
            active_connections: self.active_connections,
            queued_queries: self.queued_queries,
            average_latency_ms: avg,
            total_queries: self.total_queries,
            error_rate: self.error_rate(now),
            cpu_time_ms: self.cpu_time.as_secs_f64() * 1000.0,
            memory_bytes: self.memory_bytes,
            circuit_breaker_open: self.circuit_breaker_opened_at.is_some(),
            low_priority: self.circuit_breaker_opened_at.is_some(),
        }
    }
}

pub struct MultiTenantPoolManager {
    tenants: HashMap<String, TenantIsolationPolicy>,
    tenant_catalog: Vec<TenantPool>,
    shared_pool: Pool,
    states: Arc<RwLock<HashMap<String, TenantRuntimeState>>>,
    global_slots: Arc<Semaphore>,
    tenant_slots: Arc<RwLock<HashMap<String, Arc<Semaphore>>>>,
    low_priority_slots: Arc<Semaphore>,
    max_connections: usize,
    minimum_connections: usize,
    burst_connections: usize,
}

pub struct TenantConnection {
    tenant_id: String,
    conn: Option<Object>,
    state: Arc<RwLock<HashMap<String, TenantRuntimeState>>>,
    started_at: Instant,
    failed: bool,
    _global_permit: OwnedSemaphorePermit,
    _tenant_permit: OwnedSemaphorePermit,
    _low_priority_permit: Option<OwnedSemaphorePermit>,
}

impl Deref for TenantConnection {
    type Target = Object;

    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().expect("tenant connection missing")
    }
}

impl DerefMut for TenantConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().expect("tenant connection missing")
    }
}

impl TenantConnection {
    pub fn mark_failed(&mut self) {
        self.failed = true;
    }
}

impl Drop for TenantConnection {
    fn drop(&mut self) {
        let tenant_id = self.tenant_id.clone();
        let state = self.state.clone();
        let latency = self.started_at.elapsed();
        let failed = self.failed;
        tokio::spawn(async move {
            let mut states = state.write().await;
            if let Some(tenant) = states.get_mut(&tenant_id) {
                tenant.active_connections = tenant.active_connections.saturating_sub(1);
                tenant.record_completion(latency, failed);
            }
        });
    }
}

impl MultiTenantPoolManager {
    pub async fn new(tenants: &[(&str, &str, &str)]) -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_limits(
            tenants,
            DEFAULT_MEMORY_BUDGET_BYTES,
            CONNECTION_MEMORY_BYTES,
        )
        .await
    }

    pub async fn with_limits(
        tenants: &[(&str, &str, &str)],
        memory_budget_bytes: usize,
        connection_memory_bytes: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let max_connections = (memory_budget_bytes / connection_memory_bytes).max(1);
        let minimum_connections = ((max_connections * MIN_GUARANTEE_PERCENT) / 100).max(1);
        let burst_connections = ((max_connections * BURST_PERCENT) / 100).max(1);

        let (_, host, dbname) = tenants.first().ok_or("at least one tenant is required")?;
        let mut cfg = Config::new();
        cfg.host = Some(host.to_string());
        cfg.dbname = Some(dbname.to_string());
        cfg.user = Some("utility".into());
        cfg.password = Some("utility_secret".into());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        cfg.pool = Some(PoolConfig {
            max_size: max_connections,
            ..Default::default()
        });
        let shared_pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;

        let mut policies = HashMap::new();
        let mut catalog = Vec::with_capacity(tenants.len());
        let mut states = HashMap::new();
        let mut tenant_slots = HashMap::new();
        for (tenant_id, host, dbname) in tenants {
            policies.insert(
                (*tenant_id).to_string(),
                TenantIsolationPolicy {
                    tenant_id: (*tenant_id).to_string(),
                    statement_timeout: DEFAULT_STATEMENT_TIMEOUT,
                    max_cpu_time: DEFAULT_STATEMENT_TIMEOUT,
                    max_memory_bytes: 128 * 1024 * 1024,
                },
            );
            catalog.push(TenantPool {
                tenant_id: (*tenant_id).to_string(),
                host: (*host).to_string(),
                dbname: (*dbname).to_string(),
                statement_timeout: DEFAULT_STATEMENT_TIMEOUT,
            });
            states.insert((*tenant_id).to_string(), TenantRuntimeState::new());
            tenant_slots.insert(
                (*tenant_id).to_string(),
                Arc::new(Semaphore::new(burst_connections)),
            );
            info!(tenant = %tenant_id, db = %dbname, "tenant registered on shared connection pool");
        }

        Ok(Self {
            tenants: policies,
            tenant_catalog: catalog,
            shared_pool,
            states: Arc::new(RwLock::new(states)),
            global_slots: Arc::new(Semaphore::new(max_connections)),
            tenant_slots: Arc::new(RwLock::new(tenant_slots)),
            low_priority_slots: Arc::new(Semaphore::new(minimum_connections)),
            max_connections,
            minimum_connections,
            burst_connections,
        })
    }

    pub fn register_global(self: Arc<Self>) -> Result<(), Arc<Self>> {
        GLOBAL_POOL_MANAGER.set(self)
    }

    pub fn shared_pool(&self) -> &Pool {
        &self.shared_pool
    }

    pub fn get_pool(&self, tenant_id: &str) -> Option<&Pool> {
        self.tenants
            .contains_key(tenant_id)
            .then_some(&self.shared_pool)
    }

    pub async fn get_connection(
        &self,
        tenant_id: &str,
    ) -> Result<TenantConnection, deadpool_postgres::PoolError> {
        let policy = self
            .tenants
            .get(tenant_id)
            .ok_or(deadpool_postgres::PoolError::Closed)?
            .clone();
        let tenant_slots = self.tenant_slots.read().await;
        let tenant_sem = tenant_slots
            .get(tenant_id)
            .ok_or(deadpool_postgres::PoolError::Closed)?
            .clone();
        drop(tenant_slots);

        let low_priority = self
            .tenant_usage(tenant_id)
            .await
            .map(|u| u.low_priority)
            .unwrap_or(false);
        let low_priority_permit = if low_priority {
            Some(
                self.low_priority_slots
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| deadpool_postgres::PoolError::Closed)?,
            )
        } else {
            None
        };

        {
            let mut states = self.states.write().await;
            if let Some(state) = states.get_mut(tenant_id) {
                state.queued_queries += 1;
            }
        }

        let global_permit = self
            .global_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| deadpool_postgres::PoolError::Closed)?;
        let tenant_permit = tenant_sem
            .acquire_owned()
            .await
            .map_err(|_| deadpool_postgres::PoolError::Closed)?;

        match self.shared_pool.get().await {
            Ok(conn) => {
                let timeout = format!("{}ms", policy.statement_timeout.as_millis());
                if let Err(e) = conn
                    .execute(
                        "SELECT set_config('statement_timeout', $1, false), set_config('application_name', $2, false)",
                        &[&timeout, &tenant_id],
                    )
                    .await
                {
                    warn!(tenant = %tenant_id, error = %e, "failed to set tenant session isolation settings");
                }
                let mut states = self.states.write().await;
                if let Some(state) = states.get_mut(tenant_id) {
                    state.queued_queries = state.queued_queries.saturating_sub(1);
                    state.active_connections += 1;
                }
                Ok(TenantConnection {
                    tenant_id: tenant_id.to_string(),
                    conn: Some(conn),
                    state: self.states.clone(),
                    started_at: Instant::now(),
                    failed: false,
                    _global_permit: global_permit,
                    _tenant_permit: tenant_permit,
                    _low_priority_permit: low_priority_permit,
                })
            }
            Err(e) => {
                let mut states = self.states.write().await;
                if let Some(state) = states.get_mut(tenant_id) {
                    state.queued_queries = state.queued_queries.saturating_sub(1);
                }
                metrics::record_db_starvation();
                Err(e)
            }
        }
    }

    pub fn pools(&self) -> &[TenantPool] {
        &self.tenant_catalog
    }

    pub fn max_connections(&self) -> usize {
        self.max_connections
    }
    pub fn minimum_connections(&self) -> usize {
        self.minimum_connections
    }
    pub fn burst_connections(&self) -> usize {
        self.burst_connections
    }

    pub async fn tenant_usage(&self, tenant_id: &str) -> Option<TenantUsage> {
        let states = self.states.read().await;
        states.get(tenant_id).map(|s| s.usage(tenant_id))
    }

    pub async fn all_tenant_usage(&self) -> Vec<TenantUsage> {
        let states = self.states.read().await;
        states
            .iter()
            .map(|(tenant_id, state)| state.usage(tenant_id))
            .collect()
    }

    pub async fn record_tenant_error(&self, tenant_id: &str) {
        self.record_tenant_completion(tenant_id, Duration::ZERO, true)
            .await;
    }

    pub async fn record_tenant_success(&self, tenant_id: &str, latency: Duration) {
        self.record_tenant_completion(tenant_id, latency, false)
            .await;
    }

    async fn record_tenant_completion(&self, tenant_id: &str, latency: Duration, failed: bool) {
        let mut states = self.states.write().await;
        if let Some(state) = states.get_mut(tenant_id) {
            state.record_completion(latency, failed);
        }
    }

    pub async fn refresh_resource_governor(&self) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.shared_pool.get().await?;
        let rows = conn
            .query(
                "SELECT application_name, COALESCE(total_exec_time, 0)::float8, COALESCE(shared_blks_hit + shared_blks_read, 0)::bigint FROM pg_stat_statements",
                &[],
            )
            .await?;
        let mut states = self.states.write().await;
        for row in rows {
            let tenant_id: String = row.get(0);
            if let Some(policy) = self.tenants.get(&tenant_id) {
                let cpu_ms: f64 = row.get(1);
                let memory_blocks: i64 = row.get(2);
                let memory_bytes = memory_blocks * 8192;
                if let Some(state) = states.get_mut(&tenant_id) {
                    state.cpu_time = Duration::from_secs_f64(cpu_ms / 1000.0);
                    state.memory_bytes = memory_bytes;
                }
                if cpu_ms > policy.max_cpu_time.as_secs_f64() * 1000.0
                    || memory_bytes > policy.max_memory_bytes
                {
                    warn!(tenant = %tenant_id, "tenant exceeded resource governor thresholds; cancelling active queries");
                    let _ = conn.execute("SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name = $1", &[&tenant_id]).await;
                }
            }
        }
        Ok(())
    }
}

pub fn global_pool_manager() -> Option<Arc<MultiTenantPoolManager>> {
    GLOBAL_POOL_MANAGER.get().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shared_pool_respects_memory_budget_for_many_tenants() {
        let tenants: Vec<(String, String, String)> = (0..250)
            .map(|i| {
                (
                    format!("tenant-{i}"),
                    "localhost".to_string(),
                    "utility_test".to_string(),
                )
            })
            .collect();
        let refs: Vec<(&str, &str, &str)> = tenants
            .iter()
            .map(|(tenant, host, db)| (tenant.as_str(), host.as_str(), db.as_str()))
            .collect();

        let manager = MultiTenantPoolManager::new(&refs).await.unwrap();

        assert_eq!(manager.pools().len(), 250);
        assert_eq!(manager.max_connections(), 5);
        assert_eq!(manager.minimum_connections(), 1);
        assert_eq!(manager.burst_connections(), 4);
        assert!(manager.get_pool("tenant-42").is_some());
    }

    #[tokio::test]
    async fn tenant_usage_tracks_circuit_breaker_state() {
        let manager = MultiTenantPoolManager::new(&[("grid-east", "localhost", "utility_test")])
            .await
            .unwrap();

        manager
            .record_tenant_success("grid-east", Duration::from_millis(25))
            .await;
        let healthy = manager.tenant_usage("grid-east").await.unwrap();
        assert!(!healthy.circuit_breaker_open);
        assert_eq!(healthy.total_queries, 1);
        assert_eq!(healthy.average_latency_ms, 25.0);

        manager.record_tenant_error("grid-east").await;
        let isolated = manager.tenant_usage("grid-east").await.unwrap();
        assert!(isolated.circuit_breaker_open);
        assert!(isolated.low_priority);
        assert!(isolated.error_rate > CIRCUIT_BREAKER_ERROR_RATE);
    }
}

use futures::future::BoxFuture;
use sqlx::{PgConnection, PgPool};
use tokio::time::sleep;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdvisoryLockMode {
    Shared,
    Exclusive,
}

fn advisory_lock_key(namespace: &str, chunk_id: i64) -> i64 {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(namespace.as_bytes());
    hasher.update(b":");
    hasher.update(chunk_id.to_be_bytes());
    let digest = hasher.finalize();
    i64::from_be_bytes(digest[0..8].try_into().expect("sha256 digest has 8 bytes"))
}

pub fn telemetry_chunk_id(recorded_at: chrono::DateTime<chrono::Utc>) -> i64 {
    recorded_at.timestamp().div_euclid(3_600)
}

pub async fn with_advisory_lock<T, F>(
    pool: &PgPool,
    chunk_id: i64,
    mode: AdvisoryLockMode,
    timeout: Duration,
    f: F,
) -> anyhow::Result<T>
where
    T: Send,
    F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, anyhow::Result<T>> + Send,
{
    let mut conn = pool.acquire().await?;
    let lock_key = advisory_lock_key("chunk_write", chunk_id);
    let started = Instant::now();

    loop {
        let acquired: bool = match mode {
            AdvisoryLockMode::Shared => {
                sqlx::query_scalar("SELECT pg_try_advisory_lock_shared($1)")
                    .bind(lock_key)
                    .fetch_one(&mut *conn)
                    .await?
            }
            AdvisoryLockMode::Exclusive => {
                sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                    .bind(lock_key)
                    .fetch_one(&mut *conn)
                    .await?
            }
        };

        if acquired {
            break;
        }
        if started.elapsed() >= timeout {
            return Err(anyhow::anyhow!(
                "advisory lock timeout for chunk {chunk_id}"
            ));
        }
        sleep(Duration::from_millis(10)).await;
    }

    let result = f(&mut conn).await;
    let unlock_result = match mode {
        AdvisoryLockMode::Shared => {
            sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock_shared($1)")
                .bind(lock_key)
                .fetch_one(&mut *conn)
                .await
        }
        AdvisoryLockMode::Exclusive => {
            sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
                .bind(lock_key)
                .fetch_one(&mut *conn)
                .await
        }
    };

    if let Err(error) = unlock_result {
        tracing::warn!(%error, chunk_id, "failed to release advisory chunk lease");
    }

    result
}
