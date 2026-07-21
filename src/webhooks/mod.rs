pub mod dead_letter;
pub mod dispatcher;
pub mod signer;

pub use dead_letter::DeadLetterQueue;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookEvent {
    pub id: Uuid,
    pub event_type: String,
    pub created_at: DateTime<Utc>,
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookEndpoint {
    pub id: String,
    pub url: String,
    pub secret: String,
    pub tenant_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryReceipt {
    pub attempts: u32,
    pub status: u16,
}

// ---------------------------------------------------------------------------
// Retry policy — fixed exponential delays per the specification.
//
//   Attempt   Delay
//   1         10 s
//   2         30 s
//   3          2 min
//   4         10 min
//   5         30 min
// ---------------------------------------------------------------------------

/// Production retry delays matching the specification.
pub const SPEC_RETRY_DELAYS: [Duration; 5] = [
    Duration::from_secs(10),
    Duration::from_secs(30),
    Duration::from_secs(120),
    Duration::from_secs(600),
    Duration::from_secs(1800),
];

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub delays: [Duration; 5],
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: SPEC_RETRY_DELAYS.len() as u32,
            delays: SPEC_RETRY_DELAYS,
        }
    }
}

impl RetryPolicy {
    /// A policy with short delays suitable for tests.
    pub fn fast() -> Self {
        Self {
            max_attempts: 5,
            delays: [Duration::from_millis(1); 5],
        }
    }

    /// Duration to wait before the n-th retry (1-indexed).
    pub fn delay_for(&self, attempt: u32) -> Duration {
        let idx = attempt.saturating_sub(1) as usize;
        if idx < self.delays.len() {
            self.delays[idx]
        } else {
            *self.delays.last().unwrap_or(&Duration::from_secs(1800))
        }
    }
}

// ---------------------------------------------------------------------------
// Webhook transport trait
// ---------------------------------------------------------------------------

#[async_trait]
pub trait WebhookTransport: Send + Sync + 'static {
    async fn send(
        &self,
        url: &str,
        body: &[u8],
        signature: &str,
        attempt: u32,
    ) -> Result<u16, WebhookError>;
}

pub struct ReqwestWebhookTransport {
    client: reqwest::Client,
    timeout: Duration,
}

impl ReqwestWebhookTransport {
    pub fn new(timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::new(),
            timeout,
        }
    }
}

impl Default for ReqwestWebhookTransport {
    fn default() -> Self {
        Self::new(Duration::from_secs(10))
    }
}

#[async_trait]
impl WebhookTransport for ReqwestWebhookTransport {
    async fn send(
        &self,
        url: &str,
        body: &[u8],
        signature: &str,
        attempt: u32,
    ) -> Result<u16, WebhookError> {
        let response = self
            .client
            .post(url)
            .timeout(self.timeout)
            .header("content-type", "application/json")
            .header("x-utility-webhook-signature", signature)
            .header("x-utility-webhook-attempt", attempt)
            .body(body.to_vec())
            .send()
            .await
            .map_err(|err| {
                if err.is_timeout() {
                    WebhookError::Transient("delivery timed out".into())
                } else {
                    WebhookError::Transient(err.to_string())
                }
            })?;
        Ok(response.status().as_u16())
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum WebhookError {
    #[error("invalid webhook signature")]
    InvalidSignature,
    #[error("stale webhook signature")]
    StaleSignature,
    #[error("permanent downstream status {0}")]
    PermanentStatus(u16),
    #[error("transient delivery failure: {0}")]
    Transient(String),
    #[error("endpoint rate limit exceeded")]
    RateLimited,
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub(crate) fn is_retryable(status: u16) -> bool {
    status == 408 || status == 429 || (500..600).contains(&status)
}

pub(crate) fn subtle_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len()
        && a
            .iter()
            .zip(b.iter())
            .fold(0u8, |acc, (left, right)| acc | (left ^ right))
            == 0
}
