use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

const SECRET_MARKERS: &[&str] = &["secret", "password", "token", "key", "credential"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigSnapshot {
    pub service: String,
    pub environment: String,
    pub version: String,
    pub captured_at_unix_ms: u128,
    pub checksum: String,
    pub entries: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriftReport {
    pub service: String,
    pub baseline_checksum: String,
    pub observed_checksum: String,
    pub changes: Vec<ConfigDrift>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigDrift {
    Added {
        key: String,
        value: String,
    },
    Removed {
        key: String,
        previous: String,
    },
    Modified {
        key: String,
        previous: String,
        current: String,
    },
}

impl DriftReport {
    pub fn has_drift(&self) -> bool {
        !self.changes.is_empty()
    }

    pub fn severity(&self) -> DriftSeverity {
        if self.changes.iter().any(ConfigDrift::is_sensitive) {
            DriftSeverity::Critical
        } else if self.has_drift() {
            DriftSeverity::Warning
        } else {
            DriftSeverity::None
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DriftSeverity {
    None,
    Warning,
    Critical,
}

impl ConfigDrift {
    fn is_sensitive(&self) -> bool {
        let key = match self {
            ConfigDrift::Added { key, .. }
            | ConfigDrift::Removed { key, .. }
            | ConfigDrift::Modified { key, .. } => key,
        };
        is_sensitive_key(key)
    }
}

pub fn capture_snapshot<I, K, V>(
    service: &str,
    environment: &str,
    version: &str,
    entries: I,
) -> ConfigSnapshot
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    let entries = entries
        .into_iter()
        .map(|(k, v)| {
            let key = k.into();
            let value = v.into();
            let redacted = if is_sensitive_key(&key) {
                "<redacted>".to_string()
            } else {
                value
            };
            (key, redacted)
        })
        .collect::<BTreeMap<_, _>>();

    let checksum = checksum_entries(&entries);
    ConfigSnapshot {
        service: service.to_string(),
        environment: environment.to_string(),
        version: version.to_string(),
        captured_at_unix_ms: now_ms(),
        checksum,
        entries,
    }
}

pub fn detect_drift(baseline: &ConfigSnapshot, observed: &ConfigSnapshot) -> DriftReport {
    let mut changes = Vec::new();

    for (key, previous) in &baseline.entries {
        match observed.entries.get(key) {
            Some(current) if current != previous => changes.push(ConfigDrift::Modified {
                key: key.clone(),
                previous: previous.clone(),
                current: current.clone(),
            }),
            None => changes.push(ConfigDrift::Removed {
                key: key.clone(),
                previous: previous.clone(),
            }),
            _ => {}
        }
    }

    for (key, value) in &observed.entries {
        if !baseline.entries.contains_key(key) {
            changes.push(ConfigDrift::Added {
                key: key.clone(),
                value: value.clone(),
            });
        }
    }

    DriftReport {
        service: observed.service.clone(),
        baseline_checksum: baseline.checksum.clone(),
        observed_checksum: observed.checksum.clone(),
        changes,
    }
}

fn checksum_entries(entries: &BTreeMap<String, String>) -> String {
    let mut hasher = Sha256::new();
    for (key, value) in entries {
        hasher.update(key.as_bytes());
        hasher.update(b"=");
        hasher.update(value.as_bytes());
        hasher.update(b"\n");
    }
    hex::encode(hasher.finalize())
}

fn is_sensitive_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    SECRET_MARKERS
        .iter()
        .any(|marker| normalized.contains(marker))
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_checksum_is_stable_and_redacts_secrets() {
        let first = capture_snapshot(
            "api",
            "prod",
            "v1",
            [
                ("DATABASE_URL", "postgres://user:secret@db"),
                ("RATE_LIMIT", "100"),
            ],
        );
        let second = capture_snapshot(
            "api",
            "prod",
            "v1",
            [
                ("RATE_LIMIT", "100"),
                ("DATABASE_URL", "postgres://different"),
            ],
        );

        assert_eq!(first.entries["DATABASE_URL"], "<redacted>");
        assert_eq!(first.checksum, second.checksum);
    }

    #[test]
    fn drift_report_detects_added_removed_and_modified_entries() {
        let baseline = capture_snapshot("api", "prod", "v1", [("A", "1"), ("B", "2")]);
        let observed = capture_snapshot("api", "prod", "v1", [("B", "3"), ("C", "4")]);

        let report = detect_drift(&baseline, &observed);

        assert!(report.has_drift());
        assert_eq!(report.severity(), DriftSeverity::Warning);
        assert_eq!(report.changes.len(), 3);
    }

    #[test]
    fn sensitive_drift_is_critical() {
        let baseline = capture_snapshot("api", "prod", "v1", [("PUBLIC", "1")]);
        let observed =
            capture_snapshot("api", "prod", "v1", [("PUBLIC", "1"), ("API_TOKEN", "new")]);

        let report = detect_drift(&baseline, &observed);

        assert_eq!(report.severity(), DriftSeverity::Critical);
    }
}
