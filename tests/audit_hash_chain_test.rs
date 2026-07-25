use chrono::{TimeZone, Utc};
use utility_backend::audit::{
    payload_hash, verify_hash_chain, AuditEvent, NewAuditEvent, GENESIS_PREVIOUS_HASH,
};

fn event(action: &str, resource: &str, payload: serde_json::Value) -> NewAuditEvent {
    NewAuditEvent {
        occurred_at: Utc.with_ymd_and_hms(2026, 7, 17, 12, 0, 0).unwrap(),
        actor: "system".into(),
        service: "utility-backend".into(),
        action: action.into(),
        resource: resource.into(),
        payload_hash: payload_hash(&payload).unwrap(),
    }
}

#[test]
fn verifies_valid_hash_chain() {
    let first = AuditEvent::append(
        1,
        GENESIS_PREVIOUS_HASH,
        event(
            "meter.registered",
            "meter/MTR-001",
            serde_json::json!({"id":"MTR-001"}),
        ),
    );
    let second = AuditEvent::append(
        2,
        first.hash.clone(),
        event(
            "reading.accepted",
            "meter/MTR-001",
            serde_json::json!({"kwh":42}),
        ),
    );

    let report = verify_hash_chain(&[first, second]);

    assert!(report.verified);
    assert_eq!(report.checked_events, 2);
    assert!(report.first_invalid_sequence.is_none());
}

#[test]
fn detects_tampered_payload_hash() {
    let mut first = AuditEvent::append(
        1,
        GENESIS_PREVIOUS_HASH,
        event(
            "meter.registered",
            "meter/MTR-001",
            serde_json::json!({"id":"MTR-001"}),
        ),
    );
    first.payload_hash = "f".repeat(64);

    let report = verify_hash_chain(&[first]);

    assert!(!report.verified);
    assert_eq!(report.first_invalid_sequence, Some(1));
    assert_eq!(
        report.reason.as_deref(),
        Some("event hash does not match canonical fields")
    );
}

#[test]
fn detects_broken_previous_hash_link() {
    let first = AuditEvent::append(
        1,
        GENESIS_PREVIOUS_HASH,
        event(
            "meter.registered",
            "meter/MTR-001",
            serde_json::json!({"id":"MTR-001"}),
        ),
    );
    let second = AuditEvent::append(
        2,
        "0".repeat(64),
        event(
            "reading.accepted",
            "meter/MTR-001",
            serde_json::json!({"kwh":42}),
        ),
    );

    let report = verify_hash_chain(&[first, second]);

    assert!(!report.verified);
    assert_eq!(report.first_invalid_sequence, Some(2));
    assert_eq!(
        report.reason.as_deref(),
        Some("previous hash does not match prior event")
    );
}
