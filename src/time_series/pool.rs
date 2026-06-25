use crate::api::metrics;
use deadpool_postgres::tokio_postgres::NoTls;
use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tracing::info;

pub struct TenantPool {
    tenant_id: String,
    pool: Pool,
}

pub struct MultiTenantPoolManager {
    pools: Vec<TenantPool>,
}

impl MultiTenantPoolManager {
    pub async fn new(tenants: &[(&str, &str, &str)]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut pools = Vec::new();
        for (tenant_id, host, dbname) in tenants {
            let mut cfg = Config::new();
            cfg.host = Some(host.to_string());
            cfg.dbname = Some(dbname.to_string());
            cfg.user = Some("utility".into());
            cfg.password = Some("utility_secret".into());
            cfg.manager = Some(ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            });
            let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
            pools.push(TenantPool {
                tenant_id: tenant_id.to_string(),
                pool,
            });
            info!(tenant = %tenant_id, db = %dbname, "tenant connection pool created");
        }
        Ok(Self { pools })
    }

    pub fn get_pool(&self, tenant_id: &str) -> Option<&Pool> {
        self.pools
            .iter()
            .find(|t| t.tenant_id == tenant_id)
            .map(|t| &t.pool)
    }

    pub async fn get_connection(
        &self,
        tenant_id: &str,
    ) -> Result<deadpool_postgres::Object, deadpool_postgres::PoolError> {
        let pool = self
            .get_pool(tenant_id)
            .ok_or(deadpool_postgres::PoolError::Closed)?;

        match pool.get().await {
            Ok(conn) => Ok(conn),
            Err(e) => {
                metrics::record_db_starvation();
                Err(e)
            }
        }
    }

    pub fn pools(&self) -> &[TenantPool] {
        &self.pools
    }
}

use futures::future::BoxFuture;
use sqlx::{PgConnection, PgPool};
use std::time::{Duration, Instant};
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
