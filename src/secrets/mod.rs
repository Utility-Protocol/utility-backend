//! Secret rotation primitives for database credentials and API keys.
//!
//! The rotation service is intentionally provider-agnostic: callers plug in a
//! [`SecretStore`] implementation for their vault/KMS and a [`CredentialTarget`]
//! implementation for PostgreSQL users, upstream API keys, or any other system
//! whose credential can be updated and health-checked.

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;

pub mod memory;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecretKind {
    DatabaseCredential,
    ApiKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretDescriptor {
    pub name: String,
    pub kind: SecretKind,
    pub version: u64,
    pub rotate_after: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretVersion {
    pub descriptor: SecretDescriptor,
    pub value: String,
    pub created_at: DateTime<Utc>,
    pub checksum: String,
}

impl SecretVersion {
    pub fn new(mut descriptor: SecretDescriptor, value: String, created_at: DateTime<Utc>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(value.as_bytes());
        descriptor.version = descriptor.version.max(1);
        Self {
            descriptor,
            value,
            created_at,
            checksum: hex::encode(hasher.finalize()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RotationPolicy {
    pub max_age: ChronoDuration,
    pub overlap: ChronoDuration,
    pub min_secret_len: usize,
}

impl Default for RotationPolicy {
    fn default() -> Self {
        Self {
            max_age: ChronoDuration::days(30),
            overlap: ChronoDuration::minutes(15),
            min_secret_len: 48,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationReport {
    pub name: String,
    pub previous_version: u64,
    pub active_version: u64,
    pub checksum: String,
    pub duration_ms: u128,
}

#[derive(Debug, Error)]
pub enum SecretRotationError {
    #[error("secret {0} was not found")]
    NotFound(String),
    #[error("target activation failed: {0}")]
    Activation(String),
    #[error("health check failed: {0}")]
    HealthCheck(String),
    #[error("secret store failed: {0}")]
    Store(String),
    #[error("generated secret failed policy")]
    PolicyViolation,
}

#[async_trait]
pub trait SecretStore: Send + Sync {
    async fn current(&self, name: &str) -> Result<SecretVersion, SecretRotationError>;
    async fn stage(&self, secret: SecretVersion) -> Result<(), SecretRotationError>;
    async fn promote(&self, name: &str, version: u64) -> Result<(), SecretRotationError>;
    async fn retire_before(&self, name: &str, version: u64) -> Result<(), SecretRotationError>;
}

#[async_trait]
pub trait CredentialTarget: Send + Sync {
    async fn activate(&self, secret: &SecretVersion) -> Result<(), SecretRotationError>;
    async fn verify(&self, secret: &SecretVersion) -> Result<(), SecretRotationError>;
}

#[derive(Clone)]
pub struct SecretRotationService<S, T> {
    store: Arc<S>,
    target: Arc<T>,
    policy: RotationPolicy,
}

impl<S, T> SecretRotationService<S, T>
where
    S: SecretStore,
    T: CredentialTarget,
{
    pub fn new(store: Arc<S>, target: Arc<T>, policy: RotationPolicy) -> Self {
        Self {
            store,
            target,
            policy,
        }
    }

    pub async fn rotate_due(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<RotationReport>, SecretRotationError> {
        let current = self.store.current("primary").await?;
        if current.descriptor.rotate_after > now {
            return Ok(None);
        }
        self.rotate("primary", now).await.map(Some)
    }

    pub async fn rotate(
        &self,
        name: &str,
        now: DateTime<Utc>,
    ) -> Result<RotationReport, SecretRotationError> {
        let started = Instant::now();
        let current = self.store.current(name).await?;
        let value = generate_secret(self.policy.min_secret_len);
        if value.len() < self.policy.min_secret_len {
            return Err(SecretRotationError::PolicyViolation);
        }

        let next_descriptor = SecretDescriptor {
            name: current.descriptor.name.clone(),
            kind: current.descriptor.kind,
            version: current.descriptor.version + 1,
            rotate_after: now + self.policy.max_age,
        };
        let next = SecretVersion::new(next_descriptor, value, now);

        self.store.stage(next.clone()).await?;
        self.target.activate(&next).await?;
        self.target.verify(&next).await?;
        self.store.promote(name, next.descriptor.version).await?;
        self.store
            .retire_before(name, next.descriptor.version)
            .await?;

        let report = RotationReport {
            name: name.to_string(),
            previous_version: current.descriptor.version,
            active_version: next.descriptor.version,
            checksum: next.checksum,
            duration_ms: started.elapsed().as_millis(),
        };
        crate::api::metrics::record_secret_rotation(
            &report.name,
            "success",
            report.duration_ms as f64,
        );
        Ok(report)
    }
}

pub async fn run_rotation_loop<S, T>(service: SecretRotationService<S, T>, interval: Duration)
where
    S: SecretStore + 'static,
    T: CredentialTarget + 'static,
{
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        if let Err(error) = service.rotate_due(Utc::now()).await {
            crate::api::metrics::record_secret_rotation("primary", "failure", 0.0);
            tracing::error!(%error, "secret rotation failed");
        }
    }
}

fn generate_secret(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
