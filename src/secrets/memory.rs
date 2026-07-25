use super::{CredentialTarget, SecretRotationError, SecretStore, SecretVersion};
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::BTreeMap;

#[derive(Default)]
pub struct InMemorySecretStore {
    secrets: RwLock<BTreeMap<String, BTreeMap<u64, SecretVersion>>>,
    active: RwLock<BTreeMap<String, u64>>,
}

impl InMemorySecretStore {
    pub fn insert_active(&self, secret: SecretVersion) {
        let name = secret.descriptor.name.clone();
        let version = secret.descriptor.version;
        self.secrets
            .write()
            .entry(name.clone())
            .or_default()
            .insert(version, secret);
        self.active.write().insert(name, version);
    }

    pub fn versions(&self, name: &str) -> usize {
        self.secrets.read().get(name).map_or(0, BTreeMap::len)
    }
}

#[async_trait]
impl SecretStore for InMemorySecretStore {
    async fn current(&self, name: &str) -> Result<SecretVersion, SecretRotationError> {
        let version = *self
            .active
            .read()
            .get(name)
            .ok_or_else(|| SecretRotationError::NotFound(name.to_string()))?;
        self.secrets
            .read()
            .get(name)
            .and_then(|versions| versions.get(&version))
            .cloned()
            .ok_or_else(|| SecretRotationError::NotFound(name.to_string()))
    }

    async fn stage(&self, secret: SecretVersion) -> Result<(), SecretRotationError> {
        self.secrets
            .write()
            .entry(secret.descriptor.name.clone())
            .or_default()
            .insert(secret.descriptor.version, secret);
        Ok(())
    }

    async fn promote(&self, name: &str, version: u64) -> Result<(), SecretRotationError> {
        if !self
            .secrets
            .read()
            .get(name)
            .is_some_and(|versions| versions.contains_key(&version))
        {
            return Err(SecretRotationError::NotFound(name.to_string()));
        }
        self.active.write().insert(name.to_string(), version);
        Ok(())
    }

    async fn retire_before(&self, name: &str, version: u64) -> Result<(), SecretRotationError> {
        if let Some(versions) = self.secrets.write().get_mut(name) {
            versions.retain(|candidate, _| *candidate >= version);
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct InMemoryCredentialTarget {
    active_checksum: RwLock<Option<String>>,
}

impl InMemoryCredentialTarget {
    pub fn active_checksum(&self) -> Option<String> {
        self.active_checksum.read().clone()
    }
}

#[async_trait]
impl CredentialTarget for InMemoryCredentialTarget {
    async fn activate(&self, secret: &SecretVersion) -> Result<(), SecretRotationError> {
        self.active_checksum
            .write()
            .replace(secret.checksum.clone());
        Ok(())
    }

    async fn verify(&self, secret: &SecretVersion) -> Result<(), SecretRotationError> {
        match self.active_checksum() {
            Some(checksum) if checksum == secret.checksum => Ok(()),
            _ => Err(SecretRotationError::HealthCheck(
                "target checksum mismatch".into(),
            )),
        }
    }
}
