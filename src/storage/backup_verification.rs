//! Scheduled database backup verification with restore testing.
//!
//! The verifier intentionally runs outside request critical paths. It creates a
//! logical backup from the primary database, restores it into an isolated
//! scratch database, runs smoke checks, records metrics, and drops the scratch
//! database before the next interval.

use std::process::Stdio;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::process::Command;

use crate::api::metrics;

#[derive(Debug, Clone)]
pub struct BackupVerificationConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub statement_timeout: Duration,
    pub scratch_database_prefix: String,
    pub minimum_table_count: i64,
}

impl Default for BackupVerificationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval: Duration::from_secs(24 * 60 * 60),
            statement_timeout: Duration::from_secs(30 * 60),
            scratch_database_prefix: "utility_backup_verify".to_string(),
            minimum_table_count: 1,
        }
    }
}

impl BackupVerificationConfig {
    pub fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            enabled: env_bool("BACKUP_VERIFICATION_ENABLED", defaults.enabled),
            interval: env_duration_secs("BACKUP_VERIFICATION_INTERVAL_SECS", defaults.interval),
            statement_timeout: env_duration_secs(
                "BACKUP_VERIFICATION_TIMEOUT_SECS",
                defaults.statement_timeout,
            ),
            scratch_database_prefix: std::env::var("BACKUP_VERIFICATION_SCRATCH_PREFIX")
                .unwrap_or(defaults.scratch_database_prefix),
            minimum_table_count: std::env::var("BACKUP_VERIFICATION_MIN_TABLES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(defaults.minimum_table_count),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackupVerificationStatus {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone)]
pub struct BackupVerificationReport {
    pub status: BackupVerificationStatus,
    pub started_at: DateTime<Utc>,
    pub duration: Duration,
    pub scratch_database: String,
    pub restored_table_count: Option<i64>,
    pub error: Option<String>,
}

#[derive(Debug, Error)]
pub enum BackupVerificationError {
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("backup command failed during {phase}: {message}")]
    Command {
        phase: &'static str,
        message: String,
    },
    #[error("restore validation found {actual} tables, expected at least {minimum}")]
    Validation { actual: i64, minimum: i64 },
}

pub struct BackupVerifier {
    pool: sqlx::PgPool,
    database_url: String,
    config: BackupVerificationConfig,
}

impl BackupVerifier {
    pub fn new(
        pool: sqlx::PgPool,
        database_url: impl Into<String>,
        config: BackupVerificationConfig,
    ) -> Self {
        Self {
            pool,
            database_url: database_url.into(),
            config,
        }
    }

    pub async fn run_once(&self) -> BackupVerificationReport {
        let started_at = Utc::now();
        let timer = Instant::now();
        let scratch_database =
            scratch_database_name(&self.config.scratch_database_prefix, started_at);

        let result = self.verify_restore(&scratch_database).await;
        let duration = timer.elapsed();

        match result {
            Ok(table_count) => {
                metrics::record_backup_verification_success(duration.as_secs_f64());
                BackupVerificationReport {
                    status: BackupVerificationStatus::Succeeded,
                    started_at,
                    duration,
                    scratch_database,
                    restored_table_count: Some(table_count),
                    error: None,
                }
            }
            Err(error) => {
                metrics::record_backup_verification_failure(duration.as_secs_f64());
                BackupVerificationReport {
                    status: BackupVerificationStatus::Failed,
                    started_at,
                    duration,
                    scratch_database,
                    restored_table_count: None,
                    error: Some(error.to_string()),
                }
            }
        }
    }

    async fn verify_restore(&self, scratch_database: &str) -> Result<i64, BackupVerificationError> {
        let quoted = quote_identifier(scratch_database);
        sqlx::query(&format!("CREATE DATABASE {quoted}"))
            .execute(&self.pool)
            .await?;

        let restore_result = async {
            run_shell_command(
                "dump_restore",
                &format!(
                    "pg_dump --format=custom --no-owner --no-acl {source_url} | pg_restore --no-owner --no-acl --dbname={scratch_url}",
                    source_url = shell_escape(&self.database_url),
                    scratch_url = shell_escape(&scratch_database_url(&self.database_url, scratch_database))
                ),
                self.config.statement_timeout,
            )
            .await?;

            let scratch_pool = sqlx::PgPool::connect(&scratch_database_url(
                &self.database_url,
                scratch_database,
            ))
            .await?;
            let table_count = sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')",
            )
            .fetch_one(&scratch_pool)
            .await?;
            scratch_pool.close().await;

            if table_count < self.config.minimum_table_count {
                return Err(BackupVerificationError::Validation {
                    actual: table_count,
                    minimum: self.config.minimum_table_count,
                });
            }
            Ok(table_count)
        }
        .await;

        let drop_result = sqlx::query(&format!("DROP DATABASE IF EXISTS {quoted} WITH (FORCE)"))
            .execute(&self.pool)
            .await;

        if let Err(error) = drop_result {
            tracing::error!(%scratch_database, %error, "failed to drop backup verification scratch database");
        }

        restore_result
    }
}

pub fn spawn_backup_verification(verifier: BackupVerifier) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(verifier.config.interval);
        loop {
            interval.tick().await;
            let report = verifier.run_once().await;
            match report.status {
                BackupVerificationStatus::Succeeded => tracing::info!(
                    scratch_database = %report.scratch_database,
                    table_count = report.restored_table_count,
                    duration_ms = report.duration.as_millis(),
                    "database backup restore verification succeeded"
                ),
                BackupVerificationStatus::Failed => tracing::error!(
                    scratch_database = %report.scratch_database,
                    error = %report.error.unwrap_or_else(|| "unknown error".to_string()),
                    duration_ms = report.duration.as_millis(),
                    "database backup restore verification failed"
                ),
            }
        }
    })
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn env_duration_secs(name: &str, default: Duration) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(default)
}

fn scratch_database_name(prefix: &str, timestamp: DateTime<Utc>) -> String {
    format!("{}_{}", prefix, timestamp.format("%Y%m%d%H%M%S%3f"))
}

fn scratch_database_url(database_url: &str, scratch_database: &str) -> String {
    let without_query = database_url.split('?').next().unwrap_or(database_url);
    match without_query.rsplit_once('/') {
        Some((base, _)) => format!("{base}/{scratch_database}"),
        None => scratch_database.to_string(),
    }
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn shell_escape(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

async fn run_shell_command(
    phase: &'static str,
    script: &str,
    timeout: Duration,
) -> Result<(), BackupVerificationError> {
    let output = tokio::time::timeout(
        timeout,
        Command::new("bash")
            .arg("-o")
            .arg("pipefail")
            .arg("-c")
            .arg(script)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output(),
    )
    .await
    .map_err(|_| BackupVerificationError::Command {
        phase,
        message: "command timed out".to_string(),
    })?
    .map_err(|error| BackupVerificationError::Command {
        phase,
        message: error.to_string(),
    })?;

    if !output.status.success() {
        return Err(BackupVerificationError::Command {
            phase,
            message: String::from_utf8_lossy(&output.stderr).to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn scratch_database_names_are_timestamped() {
        let timestamp = Utc.with_ymd_and_hms(2026, 7, 17, 8, 9, 10).unwrap();
        assert!(scratch_database_name("verify", timestamp).starts_with("verify_20260717080910"));
    }

    #[test]
    fn scratch_database_url_replaces_database_name_and_drops_query() {
        assert_eq!(
            scratch_database_url(
                "postgres://user:pass@localhost:5432/utility?sslmode=require",
                "scratch"
            ),
            "postgres://user:pass@localhost:5432/scratch"
        );
    }

    #[test]
    fn quote_identifier_escapes_embedded_quotes() {
        assert_eq!(
            quote_identifier("utility\"scratch"),
            "\"utility\"\"scratch\""
        );
    }

    #[test]
    fn shell_escape_wraps_and_escapes_single_quotes() {
        assert_eq!(
            shell_escape("postgres://u:p'@host/db"),
            "'postgres://u:p'\\''@host/db'"
        );
    }
}
