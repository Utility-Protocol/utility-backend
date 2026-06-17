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
    ) -> Result<
        deadpool_postgres::Object,
        deadpool_postgres::PoolError,
    > {
        let pool = self.get_pool(tenant_id).ok_or_else(|| {
            deadpool_postgres::PoolError::NoConnectionSlot(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no pool for tenant {}", tenant_id),
            )))
        })?;

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
