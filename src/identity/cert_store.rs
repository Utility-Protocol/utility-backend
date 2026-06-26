use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use parking_lot::RwLock;
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use tracing::{info, warn};

use crate::transport::tcp::connection_manager::MeterId;
use super::Result;

pub struct CertStore {
    db: Arc<DB>,
    crl_cache: Arc<RwLock<HashSet<MeterId>>>,
}

impl CertStore {
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let certs_cf = ColumnFamilyDescriptor::new("certificates", Options::default());
        let crl_cf = ColumnFamilyDescriptor::new("crl", Options::default());

        let db = DB::open_cf_descriptors(&opts, path, vec![certs_cf, crl_cf])?;

        Ok(Self {
            db: Arc::new(db),
            crl_cache: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    pub fn store_certificate(&self, meter_id: &MeterId, cert_bytes: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle("certificates")
            .ok_or_else(|| super::IdentityError::Internal("Missing certificates CF".into()))?;
        self.db.put_cf(cf, meter_id.as_bytes(), cert_bytes)
            .map_err(|e| super::IdentityError::Internal(e.to_string()))
    }

    pub fn get_certificate(&self, meter_id: &MeterId) -> Option<Vec<u8>> {
        let cf = self.db.cf_handle("certificates")?;
        self.db.get_cf(cf, meter_id.as_bytes()).ok().flatten()
    }

    pub fn is_revoked(&self, meter_id: &MeterId) -> bool {
        self.crl_cache.read().contains(meter_id)
    }

    pub fn update_crl(&self, revoked_ids: HashSet<MeterId>) -> Result<()> {
        let cf = self.db.cf_handle("crl")
            .ok_or_else(|| super::IdentityError::Internal("Missing crl CF".into()))?;

        for id in &revoked_ids {
            self.db.put_cf(cf, id.as_bytes(), b"")
                .map_err(|e| super::IdentityError::Internal(e.to_string()))?;
        }

        let mut cache = self.crl_cache.write();
        *cache = revoked_ids;

        Ok(())
    }

    pub async fn spawn_crl_refresh_task(
        self: Arc<Self>,
        interval: Duration,
    ) {
        let mut timer = tokio::time::interval(interval);
        loop {
            timer.tick().await;
            info!("Refreshing CRL from Soroban contract...");
            let revoked_ids = HashSet::new();
            if let Err(e) = self.update_crl(revoked_ids) {
                warn!("Failed to refresh CRL: {}", e);
            }
        }
    }
}
