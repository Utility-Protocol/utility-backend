//! System-wide configuration loading, schema validation, and hot reload.
//!
//! The manager publishes validated snapshots through an `ArcSwap`-style RwLock so
//! readers only clone an `Arc` on critical paths. Reloads are atomic: invalid
//! files are rejected and the previously validated configuration remains active.

use crate::api::metrics;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle};
use tracing::{error, info, warn};

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);
const MIN_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct AppConfig {
    pub schema_version: u32,
    pub service: ServiceConfig,
    pub database: DatabaseConfig,
    pub telemetry: TelemetryConfig,
    pub reload: ReloadConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            schema_version: 1,
            service: ServiceConfig::default(),
            database: DatabaseConfig::default(),
            telemetry: TelemetryConfig::default(),
            reload: ReloadConfig::default(),
        }
    }
}

impl AppConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.schema_version != 1 {
            return Err(ConfigError::Validation(format!(
                "unsupported schema_version {}; expected 1",
                self.schema_version
            )));
        }
        if self.service.bind_addr.trim().is_empty() {
            return Err(ConfigError::Validation(
                "service.bind_addr is required".into(),
            ));
        }
        if self.service.shutdown_timeout_ms > 30_000 {
            return Err(ConfigError::Validation(
                "service.shutdown_timeout_ms must be <= 30000".into(),
            ));
        }
        if self.database.url.trim().is_empty() {
            return Err(ConfigError::Validation("database.url is required".into()));
        }
        if self.database.max_connections == 0 || self.database.max_connections > 512 {
            return Err(ConfigError::Validation(
                "database.max_connections must be between 1 and 512".into(),
            ));
        }
        if self.telemetry.metrics_path.is_empty() || !self.telemetry.metrics_path.starts_with('/') {
            return Err(ConfigError::Validation(
                "telemetry.metrics_path must be an absolute HTTP path".into(),
            ));
        }
        if self.reload.poll_interval_ms < MIN_POLL_INTERVAL.as_millis() as u64 {
            return Err(ConfigError::Validation(format!(
                "reload.poll_interval_ms must be at least {}",
                MIN_POLL_INTERVAL.as_millis()
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ServiceConfig {
    pub bind_addr: String,
    pub shutdown_timeout_ms: u64,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:8443".into(),
            shutdown_timeout_ms: 10_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "postgres://utility:utility_secret@localhost:5432/utility_test".into(),
            max_connections: 16,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct TelemetryConfig {
    pub metrics_path: String,
    pub dashboards_enabled: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            metrics_path: "/metrics".into(),
            dashboards_enabled: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ReloadConfig {
    pub enabled: bool,
    pub poll_interval_ms: u64,
}

impl Default for ReloadConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_ms: DEFAULT_POLL_INTERVAL.as_millis() as u64,
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse JSON config {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid config: {0}")]
    Validation(String),
    #[error("config watcher task failed: {0}")]
    Watcher(String),
}

pub fn load_config(path: impl AsRef<Path>) -> Result<AppConfig, ConfigError> {
    let path = path.as_ref();
    let bytes = fs::read(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let config: AppConfig =
        serde_json::from_slice(&bytes).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
    config.validate()?;
    Ok(config)
}

#[derive(Clone)]
pub struct ConfigManager {
    path: PathBuf,
    current: Arc<RwLock<Arc<AppConfig>>>,
    tx: watch::Sender<Arc<AppConfig>>,
}

impl ConfigManager {
    pub fn from_path(path: impl Into<PathBuf>) -> Result<Self, ConfigError> {
        let path = path.into();
        let config = Arc::new(load_config(&path)?);
        metrics::record_config_reload_success();
        metrics::set_config_schema_version(config.schema_version as f64);
        let (tx, _) = watch::channel(config.clone());
        Ok(Self {
            path,
            current: Arc::new(RwLock::new(config)),
            tx,
        })
    }

    pub fn current(&self) -> Arc<AppConfig> {
        self.current.read().expect("config lock poisoned").clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<AppConfig>> {
        self.tx.subscribe()
    }

    pub fn reload(&self) -> Result<Arc<AppConfig>, ConfigError> {
        let config = Arc::new(load_config(&self.path)?);
        {
            let mut guard = self.current.write().expect("config lock poisoned");
            *guard = config.clone();
        }
        let _ = self.tx.send(config.clone());
        metrics::record_config_reload_success();
        metrics::set_config_schema_version(config.schema_version as f64);
        info!(path = %self.path.display(), "configuration reloaded");
        Ok(config)
    }

    pub fn spawn_hot_reload(&self) -> JoinHandle<Result<(), ConfigError>> {
        let manager = self.clone();
        tokio::spawn(async move {
            let mut last_modified =
                modified_at(&manager.path).map_err(|source| ConfigError::Read {
                    path: manager.path.clone(),
                    source,
                })?;
            loop {
                let poll = manager.current().reload.poll_interval_ms;
                tokio::time::sleep(Duration::from_millis(poll)).await;
                match modified_at(&manager.path) {
                    Ok(modified) if modified > last_modified => {
                        last_modified = modified;
                        if let Err(err) = manager.reload() {
                            metrics::record_config_reload_failure();
                            error!(error = %err, "configuration reload rejected");
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        metrics::record_config_reload_failure();
                        warn!(path = %manager.path.display(), error = %err, "unable to stat config file");
                    }
                }
            }
        })
    }
}

fn modified_at(path: &Path) -> std::io::Result<SystemTime> {
    fs::metadata(path)?.modified()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn temp_config(contents: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file
    }

    #[test]
    fn validates_and_loads_defaults() {
        let file = temp_config(r#"{"schema_version":1,"database":{"max_connections":32}}"#);
        let config = load_config(file.path()).unwrap();
        assert_eq!(config.schema_version, 1);
        assert_eq!(config.database.max_connections, 32);
        assert_eq!(config.service.bind_addr, "0.0.0.0:8443");
    }

    #[test]
    fn rejects_unknown_fields_and_bad_ranges() {
        let unknown = temp_config(r#"{"schema_version":1,"not_in_schema":true}"#);
        assert!(matches!(
            load_config(unknown.path()),
            Err(ConfigError::Parse { .. })
        ));

        let invalid = temp_config(r#"{"schema_version":1,"database":{"max_connections":0}}"#);
        assert!(matches!(
            load_config(invalid.path()),
            Err(ConfigError::Validation(_))
        ));
    }

    #[test]
    fn reload_keeps_last_good_snapshot_on_validation_failure() {
        let mut file = temp_config(r#"{"schema_version":1,"database":{"max_connections":8}}"#);
        let manager = ConfigManager::from_path(file.path()).unwrap();
        assert_eq!(manager.current().database.max_connections, 8);

        file.as_file_mut()
            .set_len(0)
            .expect("truncate temporary config");
        file.write_all(r#"{"schema_version":1,"database":{"max_connections":0}}"#.as_bytes())
            .unwrap();

        assert!(manager.reload().is_err());
        assert_eq!(manager.current().database.max_connections, 8);
    }

    #[tokio::test]
    async fn publishes_reload_notifications() {
        let mut file = temp_config(
            r#"{"schema_version":1,"database":{"max_connections":8},"reload":{"poll_interval_ms":100}}"#,
        );
        let manager = ConfigManager::from_path(file.path()).unwrap();
        let mut rx = manager.subscribe();

        file.as_file_mut()
            .set_len(0)
            .expect("truncate temporary config");
        file.write_all(
            r#"{"schema_version":1,"database":{"max_connections":12},"reload":{"poll_interval_ms":100}}"#
                .as_bytes(),
        )
        .unwrap();

        manager.reload().unwrap();
        rx.changed().await.unwrap();
        assert_eq!(rx.borrow().database.max_connections, 12);
    }
}
