//! Database migration versioning with rollback support.
//!
//! This module centralizes migration metadata and deployment planning so every
//! service can apply the same ordered, reversible migration stream. The runtime
//! executor uses PostgreSQL advisory locking plus a small schema-history table to
//! make upgrades idempotent and to prevent concurrent migrators during
//! blue-green/canary deploys.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Executor, PgPool, Row};
use std::time::Instant;
use thiserror::Error;
use tracing::{info, warn};

use crate::api::metrics;

const MIGRATION_LOCK_KEY: i64 = 0x7574_696c_6d69_6772; // "util_migr" prefix

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MigrationVersion(pub i64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Migration {
    pub version: MigrationVersion,
    pub name: &'static str,
    pub up_sql: &'static str,
    pub down_sql: &'static str,
}

impl Migration {
    pub fn checksum(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.version.0.to_be_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.up_sql.as_bytes());
        hex::encode(hasher.finalize())
    }

    pub fn validate(&self) -> Result<(), MigrationError> {
        if self.version.0 <= 0 {
            return Err(MigrationError::InvalidDefinition(
                "version must be positive",
            ));
        }
        if self.name.trim().is_empty() {
            return Err(MigrationError::InvalidDefinition("name cannot be empty"));
        }
        if self.up_sql.trim().is_empty() || self.down_sql.trim().is_empty() {
            return Err(MigrationError::InvalidDefinition(
                "up_sql and down_sql must both be supplied",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedMigration {
    pub version: MigrationVersion,
    pub name: String,
    pub checksum: String,
    pub applied_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationStep {
    Apply(Migration),
    Rollback(Migration),
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("invalid migration definition: {0}")]
    InvalidDefinition(&'static str),
    #[error("migration versions must be strictly increasing")]
    DuplicateOrUnorderedVersion,
    #[error("applied migration {version} has checksum {applied}, expected {expected}")]
    ChecksumMismatch {
        version: i64,
        applied: String,
        expected: String,
    },
    #[error("target version {0} is not known")]
    UnknownTarget(i64),
    #[error("cannot rollback applied migration {0}; definition is missing")]
    MissingRollback(i64),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

#[derive(Debug, Clone)]
pub struct MigrationRegistry {
    migrations: Vec<Migration>,
}

impl MigrationRegistry {
    pub fn new(mut migrations: Vec<Migration>) -> Result<Self, MigrationError> {
        migrations.sort_by_key(|migration| migration.version);
        let mut previous = None;
        for migration in &migrations {
            migration.validate()?;
            if previous.is_some_and(|v| migration.version <= v) {
                return Err(MigrationError::DuplicateOrUnorderedVersion);
            }
            previous = Some(migration.version);
        }
        Ok(Self { migrations })
    }

    pub fn latest_version(&self) -> MigrationVersion {
        self.migrations
            .last()
            .map(|migration| migration.version)
            .unwrap_or(MigrationVersion(0))
    }

    pub fn plan(
        &self,
        applied: &[AppliedMigration],
        target: MigrationVersion,
    ) -> Result<Vec<MigrationStep>, MigrationError> {
        if target.0 != 0
            && !self
                .migrations
                .iter()
                .any(|migration| migration.version == target)
        {
            return Err(MigrationError::UnknownTarget(target.0));
        }

        for applied_migration in applied {
            if let Some(expected) = self
                .migrations
                .iter()
                .find(|migration| migration.version == applied_migration.version)
            {
                if expected.checksum() != applied_migration.checksum {
                    return Err(MigrationError::ChecksumMismatch {
                        version: applied_migration.version.0,
                        applied: applied_migration.checksum.clone(),
                        expected: expected.checksum(),
                    });
                }
            } else if applied_migration.version > target {
                return Err(MigrationError::MissingRollback(applied_migration.version.0));
            }
        }

        let current = applied
            .last()
            .map(|migration| migration.version)
            .unwrap_or(MigrationVersion(0));
        if current < target {
            Ok(self
                .migrations
                .iter()
                .filter(|migration| migration.version > current && migration.version <= target)
                .cloned()
                .map(MigrationStep::Apply)
                .collect())
        } else {
            Ok(self
                .migrations
                .iter()
                .rev()
                .filter(|migration| migration.version <= current && migration.version > target)
                .cloned()
                .map(MigrationStep::Rollback)
                .collect())
        }
    }
}

pub struct MigrationRunner {
    pool: PgPool,
    registry: MigrationRegistry,
}

impl MigrationRunner {
    pub fn new(pool: PgPool, registry: MigrationRegistry) -> Self {
        Self { pool, registry }
    }

    pub async fn migrate_to(
        &self,
        target: MigrationVersion,
    ) -> Result<Vec<MigrationStep>, MigrationError> {
        ensure_schema(&self.pool).await?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(MIGRATION_LOCK_KEY)
            .execute(&self.pool)
            .await?;

        let result = self.migrate_locked(target).await;
        let unlock = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(MIGRATION_LOCK_KEY)
            .execute(&self.pool)
            .await;
        if let Err(error) = unlock {
            warn!(%error, "failed to release migration advisory lock");
        }
        result
    }

    async fn migrate_locked(
        &self,
        target: MigrationVersion,
    ) -> Result<Vec<MigrationStep>, MigrationError> {
        let applied = load_applied(&self.pool).await?;
        let plan = self.registry.plan(&applied, target)?;
        for step in &plan {
            let start = Instant::now();
            match step {
                MigrationStep::Apply(migration) => apply_migration(&self.pool, migration).await?,
                MigrationStep::Rollback(migration) => {
                    rollback_migration(&self.pool, migration).await?
                }
            }
            metrics::record_migration_step(step.direction(), start.elapsed().as_secs_f64());
        }
        Ok(plan)
    }
}

impl MigrationStep {
    pub fn version(&self) -> MigrationVersion {
        match self {
            MigrationStep::Apply(migration) | MigrationStep::Rollback(migration) => {
                migration.version
            }
        }
    }

    pub fn direction(&self) -> &'static str {
        match self {
            MigrationStep::Apply(_) => "up",
            MigrationStep::Rollback(_) => "down",
        }
    }
}

async fn ensure_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    pool.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            checksum TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .await?;
    Ok(())
}

async fn load_applied(pool: &PgPool) -> Result<Vec<AppliedMigration>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT version, name, checksum, applied_at FROM schema_migrations ORDER BY version",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| AppliedMigration {
            version: MigrationVersion(row.get::<i64, _>("version")),
            name: row.get("name"),
            checksum: row.get("checksum"),
            applied_at: row.get("applied_at"),
        })
        .collect())
}

async fn apply_migration(pool: &PgPool, migration: &Migration) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    tx.execute(migration.up_sql).await?;
    sqlx::query("INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)")
        .bind(migration.version.0)
        .bind(migration.name)
        .bind(migration.checksum())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    info!(
        version = migration.version.0,
        name = migration.name,
        "applied database migration"
    );
    Ok(())
}

async fn rollback_migration(pool: &PgPool, migration: &Migration) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    tx.execute(migration.down_sql).await?;
    sqlx::query("DELETE FROM schema_migrations WHERE version = $1")
        .bind(migration.version.0)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    info!(
        version = migration.version.0,
        name = migration.name,
        "rolled back database migration"
    );
    Ok(())
}
