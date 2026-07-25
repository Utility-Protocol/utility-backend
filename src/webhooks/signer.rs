use chrono::{DateTime, Utc};
use ring::hmac;
use std::time::Duration;

use super::{subtle_eq, WebhookError};

/// Generate an HMAC-SHA256 signature header value for a webhook payload.
///
/// Format: `t={unix_timestamp},v1={hex_hmac}`
pub fn sign(secret: &[u8], timestamp: DateTime<Utc>, body: &[u8]) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
    let payload = format!(
        "{}.{}",
        timestamp.timestamp(),
        String::from_utf8_lossy(body)
    );
    let tag = hmac::sign(&key, payload.as_bytes());
    format!(
        "t={},v1={}",
        timestamp.timestamp(),
        hex::encode(tag.as_ref())
    )
}

/// Verify a webhook signature header against the given secret, timestamp and body.
///
/// `clock_tolerance` is the maximum allowed clock skew (age of the timestamp).
pub fn verify_signature(
    secret: &[u8],
    timestamp: DateTime<Utc>,
    body: &[u8],
    header: &str,
    clock_tolerance: Duration,
) -> Result<(), WebhookError> {
    let age = Utc::now()
        .signed_duration_since(timestamp)
        .num_seconds()
        .unsigned_abs();
    if age > clock_tolerance.as_secs() {
        return Err(WebhookError::StaleSignature);
    }
    let expected = sign(secret, timestamp, body);
    if subtle_eq(expected.as_bytes(), header.as_bytes()) {
        Ok(())
    } else {
        Err(WebhookError::InvalidSignature)
    }
}
