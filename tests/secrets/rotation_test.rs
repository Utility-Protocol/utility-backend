use chrono::{Duration, Utc};
use std::sync::Arc;
use utility_backend::secrets::memory::{InMemoryCredentialTarget, InMemorySecretStore};
use utility_backend::secrets::{
    RotationPolicy, SecretDescriptor, SecretKind, SecretRotationService, SecretStore, SecretVersion,
};

fn seed_store() -> Arc<InMemorySecretStore> {
    let store = Arc::new(InMemorySecretStore::default());
    let now = Utc::now();
    store.insert_active(SecretVersion::new(
        SecretDescriptor {
            name: "primary".to_string(),
            kind: SecretKind::DatabaseCredential,
            version: 1,
            rotate_after: now - Duration::seconds(1),
        },
        "old-secret-value".to_string(),
        now - Duration::days(30),
    ));
    store
}

#[tokio::test]
async fn rotates_due_secret_and_retires_old_version() {
    let store = seed_store();
    let target = Arc::new(InMemoryCredentialTarget::default());
    let service =
        SecretRotationService::new(store.clone(), target.clone(), RotationPolicy::default());

    let report = service.rotate_due(Utc::now()).await.unwrap().unwrap();

    assert_eq!(report.previous_version, 1);
    assert_eq!(report.active_version, 2);
    assert_eq!(store.versions("primary"), 1);
    let current = store.current("primary").await.unwrap();
    assert_eq!(current.descriptor.version, 2);
    assert_eq!(target.active_checksum(), Some(current.checksum));
}

#[tokio::test]
async fn skips_secret_before_rotation_deadline() {
    let store = Arc::new(InMemorySecretStore::default());
    let now = Utc::now();
    store.insert_active(SecretVersion::new(
        SecretDescriptor {
            name: "primary".to_string(),
            kind: SecretKind::ApiKey,
            version: 7,
            rotate_after: now + Duration::hours(1),
        },
        "still-fresh-api-key".to_string(),
        now,
    ));
    let service = SecretRotationService::new(
        store.clone(),
        Arc::new(InMemoryCredentialTarget::default()),
        RotationPolicy::default(),
    );

    assert!(service.rotate_due(now).await.unwrap().is_none());
    assert_eq!(
        store.current("primary").await.unwrap().descriptor.version,
        7
    );
}
