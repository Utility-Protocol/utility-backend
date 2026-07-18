use chrono::Utc;
use utility_backend::storage::migrations::{
    AppliedMigration, Migration, MigrationError, MigrationRegistry, MigrationStep, MigrationVersion,
};

fn migration(version: i64, name: &'static str) -> Migration {
    Migration {
        version: MigrationVersion(version),
        name,
        up_sql: "CREATE TABLE example(id BIGINT PRIMARY KEY)",
        down_sql: "DROP TABLE example",
    }
}

fn applied(migration: &Migration) -> AppliedMigration {
    AppliedMigration {
        version: migration.version,
        name: migration.name.to_string(),
        checksum: migration.checksum(),
        applied_at: Utc::now(),
    }
}

#[test]
fn plans_forward_migrations_in_version_order() {
    let first = migration(1, "create_accounts");
    let second = migration(2, "add_balance");
    let registry = MigrationRegistry::new(vec![second.clone(), first.clone()]).unwrap();

    let plan = registry.plan(&[], MigrationVersion(2)).unwrap();

    assert_eq!(plan.len(), 2);
    assert!(matches!(&plan[0], MigrationStep::Apply(m) if m.version == first.version));
    assert!(matches!(&plan[1], MigrationStep::Apply(m) if m.version == second.version));
}

#[test]
fn plans_rollbacks_in_reverse_version_order() {
    let first = migration(1, "create_accounts");
    let second = migration(2, "add_balance");
    let registry = MigrationRegistry::new(vec![first.clone(), second.clone()]).unwrap();
    let applied = vec![applied(&first), applied(&second)];

    let plan = registry.plan(&applied, MigrationVersion(0)).unwrap();

    assert_eq!(plan.len(), 2);
    assert!(matches!(&plan[0], MigrationStep::Rollback(m) if m.version == second.version));
    assert!(matches!(&plan[1], MigrationStep::Rollback(m) if m.version == first.version));
}

#[test]
fn rejects_checksum_drift_for_applied_migrations() {
    let migration = migration(1, "create_accounts");
    let registry = MigrationRegistry::new(vec![migration.clone()]).unwrap();
    let mut applied = applied(&migration);
    applied.checksum = "tampered".to_string();

    let error = registry.plan(&[applied], MigrationVersion(1)).unwrap_err();

    assert!(matches!(error, MigrationError::ChecksumMismatch { .. }));
}

#[test]
fn rejects_unknown_targets() {
    let registry = MigrationRegistry::new(vec![migration(1, "create_accounts")]).unwrap();

    let error = registry.plan(&[], MigrationVersion(2)).unwrap_err();

    assert!(matches!(error, MigrationError::UnknownTarget(2)));
}
