use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::api::metrics;

pub const GENESIS_PREVIOUS_HASH: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditEvent {
    pub sequence: u64,
    pub occurred_at: DateTime<Utc>,
    pub actor: String,
    pub service: String,
    pub action: String,
    pub resource: String,
    pub payload_hash: String,
    pub previous_hash: String,
    pub hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NewAuditEvent {
    pub occurred_at: DateTime<Utc>,
    pub actor: String,
    pub service: String,
    pub action: String,
    pub resource: String,
    pub payload_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditVerificationReport {
    pub verified: bool,
    pub checked_events: usize,
    pub first_invalid_sequence: Option<u64>,
    pub reason: Option<String>,
    pub head_hash: Option<String>,
}

impl AuditEvent {
    pub fn append(sequence: u64, previous_hash: impl Into<String>, event: NewAuditEvent) -> Self {
        let previous_hash = previous_hash.into();
        let hash = compute_event_hash(
            sequence,
            event.occurred_at,
            &event.actor,
            &event.service,
            &event.action,
            &event.resource,
            &event.payload_hash,
            &previous_hash,
        );
        Self {
            sequence,
            occurred_at: event.occurred_at,
            actor: event.actor,
            service: event.service,
            action: event.action,
            resource: event.resource,
            payload_hash: event.payload_hash,
            previous_hash,
            hash,
        }
    }
}

pub fn payload_hash<T: Serialize>(payload: &T) -> Result<String, serde_json::Error> {
    let bytes = serde_json::to_vec(payload)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(hex::encode(hasher.finalize()))
}

#[allow(clippy::too_many_arguments)]
pub fn compute_event_hash(
    sequence: u64,
    occurred_at: DateTime<Utc>,
    actor: &str,
    service: &str,
    action: &str,
    resource: &str,
    payload_hash: &str,
    previous_hash: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(sequence.to_be_bytes());
    hasher.update(b"\x1f");
    hasher.update(
        occurred_at
            .timestamp_nanos_opt()
            .unwrap_or_default()
            .to_be_bytes(),
    );
    for field in [
        actor,
        service,
        action,
        resource,
        payload_hash,
        previous_hash,
    ] {
        hasher.update(b"\x1f");
        hasher.update(field.as_bytes());
    }
    hex::encode(hasher.finalize())
}

pub fn verify_hash_chain(events: &[AuditEvent]) -> AuditVerificationReport {
    let mut previous_hash = GENESIS_PREVIOUS_HASH.to_string();
    let first_sequence = events.first().map(|e| e.sequence).unwrap_or(1);

    for (index, event) in events.iter().enumerate() {
        if event.sequence != first_sequence + index as u64 {
            metrics::record_audit_verification_failure("sequence_gap");
            return invalid(events, event.sequence, "non-contiguous audit sequence");
        }
        if event.previous_hash != previous_hash {
            metrics::record_audit_verification_failure("previous_hash_mismatch");
            return invalid(
                events,
                event.sequence,
                "previous hash does not match prior event",
            );
        }
        let expected_hash = compute_event_hash(
            event.sequence,
            event.occurred_at,
            &event.actor,
            &event.service,
            &event.action,
            &event.resource,
            &event.payload_hash,
            &event.previous_hash,
        );
        if event.hash != expected_hash {
            metrics::record_audit_verification_failure("event_hash_mismatch");
            return invalid(
                events,
                event.sequence,
                "event hash does not match canonical fields",
            );
        }
        previous_hash = event.hash.clone();
    }

    metrics::record_audit_verification(events.len() as u64);
    AuditVerificationReport {
        verified: true,
        checked_events: events.len(),
        first_invalid_sequence: None,
        reason: None,
        head_hash: events.last().map(|event| event.hash.clone()),
    }
}

fn invalid(events: &[AuditEvent], sequence: u64, reason: &str) -> AuditVerificationReport {
    AuditVerificationReport {
        verified: false,
        checked_events: events.len(),
        first_invalid_sequence: Some(sequence),
        reason: Some(reason.to_string()),
        head_hash: events.last().map(|event| event.hash.clone()),
    }
}
