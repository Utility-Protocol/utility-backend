use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ring::hmac;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;
use uuid::Uuid;

use crate::api::metrics;

/// Architecture notes for the webhook delivery subsystem:
///
/// * Producers enqueue a [`WebhookEvent`] on the durable outbox boundary after
///   the business transaction commits, keeping request critical paths below the
///   100ms P99 target by avoiding synchronous third-party calls.
/// * Workers deliver events through [`WebhookTransport`], sign every request
///   with HMAC-SHA256, and retry only transient failures with bounded
///   exponential backoff.
/// * Operators monitor the Prometheus counters/histograms registered in
///   `api::metrics` and roll the worker fleet with blue-green/canary gates on
///   delivery success rate, retry volume, and latency.
/// * Runbook: rotate endpoint secrets by overlapping old/new secrets during the
///   consumer migration window, pause a noisy endpoint when retry pressure rises,
///   and replay dead-lettered events by event id after the downstream recovers.
#[derive(Clone)]
pub struct WebhookDeliveryService<T> {
    transport: Arc<T>,
    retry: RetryPolicy,
    clock_tolerance: Duration,
}

impl<T> WebhookDeliveryService<T>
where
    T: WebhookTransport,
{
    pub fn new(transport: Arc<T>, retry: RetryPolicy) -> Self {
        Self {
            transport,
            retry,
            clock_tolerance: Duration::from_secs(300),
        }
    }

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

    pub fn verify_signature(
        &self,
        secret: &[u8],
        timestamp: DateTime<Utc>,
        body: &[u8],
        header: &str,
    ) -> Result<(), WebhookError> {
        let age = Utc::now()
            .signed_duration_since(timestamp)
            .num_seconds()
            .unsigned_abs();
        if age > self.clock_tolerance.as_secs() {
            return Err(WebhookError::StaleSignature);
        }
        let expected = Self::sign(secret, timestamp, body);
        if subtle_eq(expected.as_bytes(), header.as_bytes()) {
            Ok(())
        } else {
            Err(WebhookError::InvalidSignature)
        }
    }

    pub async fn deliver(
        &self,
        endpoint: &WebhookEndpoint,
        event: &WebhookEvent,
    ) -> Result<DeliveryReceipt, WebhookError> {
        let body = serde_json::to_vec(event)?;
        let signature = Self::sign(endpoint.secret.as_bytes(), event.created_at, &body);
        let mut attempt = 0;
        let started = std::time::Instant::now();

        loop {
            attempt += 1;
            let result = self
                .transport
                .send(&endpoint.url, &body, &signature, attempt)
                .await;

            match result {
                Ok(status) if (200..300).contains(&status) => {
                    metrics::record_webhook_delivery(&endpoint.id, "success");
                    metrics::observe_webhook_latency(started.elapsed().as_secs_f64());
                    return Ok(DeliveryReceipt {
                        attempts: attempt,
                        status,
                    });
                }
                Ok(status) if !is_retryable(status) || attempt >= self.retry.max_attempts => {
                    metrics::record_webhook_delivery(&endpoint.id, "failed");
                    return Err(WebhookError::PermanentStatus(status));
                }
                Ok(_) | Err(WebhookError::Transient(_)) if attempt < self.retry.max_attempts => {
                    metrics::record_webhook_retry(&endpoint.id);
                    sleep(self.retry.delay_for(attempt)).await;
                }
                Err(err) => {
                    metrics::record_webhook_delivery(&endpoint.id, "failed");
                    return Err(err);
                }
            }
        }
    }
}

fn is_retryable(status: u16) -> bool {
    status == 408 || status == 429 || (500..600).contains(&status)
}

fn subtle_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .fold(0u8, |acc, (left, right)| acc | (left ^ right))
            == 0
}

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    fn delay_for(&self, attempt: u32) -> Duration {
        let multiplier = 2_u32.saturating_pow(attempt.saturating_sub(1));
        self.base_delay
            .saturating_mul(multiplier)
            .min(self.max_delay)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookEvent {
    pub id: Uuid,
    pub event_type: String,
    pub created_at: DateTime<Utc>,
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug)]
pub struct WebhookEndpoint {
    pub id: String,
    pub url: String,
    pub secret: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryReceipt {
    pub attempts: u32,
    pub status: u16,
}

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
}

impl Default for ReqwestWebhookTransport {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
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
            .header("content-type", "application/json")
            .header("x-utility-webhook-signature", signature)
            .header("x-utility-webhook-attempt", attempt)
            .body(body.to_vec())
            .send()
            .await
            .map_err(|err| WebhookError::Transient(err.to_string()))?;
        Ok(response.status().as_u16())
    }
}

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
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}
