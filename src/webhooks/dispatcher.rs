use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::time::{sleep, timeout};

use super::signer;
use super::{
    is_retryable, DeadLetterQueue, DeliveryReceipt, RetryPolicy, WebhookEndpoint, WebhookError,
    WebhookEvent, WebhookTransport,
};

use crate::api::metrics;

// ---------------------------------------------------------------------------
// Per-endpoint rate limiter (fixed-window: 100 requests per 60 s)
// ---------------------------------------------------------------------------

const WEBHOOK_RATE_MAX: u64 = 100;
const WEBHOOK_RATE_REFILL_MS: u64 = 60_000;

struct RateBucket {
    tokens: AtomicU64,
    last_refill: AtomicU64,
}

impl RateBucket {
    fn new() -> Self {
        Self {
            tokens: AtomicU64::new(WEBHOOK_RATE_MAX),
            last_refill: AtomicU64::new(now_millis()),
        }
    }

    fn try_consume(&self) -> bool {
        self.refill();
        loop {
            let current = self.tokens.load(Ordering::Acquire);
            if current == 0 {
                return false;
            }
            if self
                .tokens
                .compare_exchange(current, current - 1, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    fn refill(&self) {
        let now = now_millis();
        let last = self.last_refill.load(Ordering::Acquire);
        let elapsed_ms = now.saturating_sub(last);
        if elapsed_ms < WEBHOOK_RATE_REFILL_MS {
            return;
        }
        if self
            .last_refill
            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            self.tokens.store(WEBHOOK_RATE_MAX, Ordering::Release);
        }
    }
}

fn now_millis() -> u64 {
    static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let epoch = EPOCH.get_or_init(Instant::now);
    epoch.elapsed().as_millis() as u64
}

// ---------------------------------------------------------------------------
// Delivery service
// ---------------------------------------------------------------------------

/// Webhook delivery service with HMAC signing, exponential-backoff retry,
/// per-attempt timeout, and per-endpoint rate limiting.
#[derive(Clone)]
pub struct WebhookDeliveryService<T, D = ()> {
    transport: Arc<T>,
    retry: RetryPolicy,
    clock_tolerance: Duration,
    deadline: Duration,
    rate_buckets: Arc<DashMap<String, RateBucket>>,
    dlq: Option<Arc<D>>,
}

// --- Methods available regardless of DLQ ---

impl<T, D> WebhookDeliveryService<T, D>
where
    T: WebhookTransport,
{
    /// Sign a payload with the given secret and timestamp.
    pub fn sign(secret: &[u8], timestamp: chrono::DateTime<chrono::Utc>, body: &[u8]) -> String {
        signer::sign(secret, timestamp, body)
    }

    /// Verify a signature header.
    pub fn verify_signature(
        &self,
        secret: &[u8],
        timestamp: chrono::DateTime<chrono::Utc>,
        body: &[u8],
        header: &str,
    ) -> Result<(), WebhookError> {
        signer::verify_signature(secret, timestamp, body, header, self.clock_tolerance)
    }
}

// --- Construction ---

impl<T> WebhookDeliveryService<T, ()>
where
    T: WebhookTransport,
{
    /// Create a new delivery service without a dead-letter queue.
    pub fn new(transport: Arc<T>, retry: RetryPolicy) -> Self {
        Self {
            transport,
            retry,
            clock_tolerance: Duration::from_secs(300),
            deadline: Duration::from_secs(10),
            rate_buckets: Arc::new(DashMap::new()),
            dlq: None,
        }
    }
}

impl<T, D> WebhookDeliveryService<T, D>
where
    T: WebhookTransport,
    D: DeadLetterQueue,
{
    /// Create a delivery service backed by a dead-letter queue.
    pub fn with_dlq(transport: Arc<T>, retry: RetryPolicy, dlq: Arc<D>) -> Self {
        Self {
            transport,
            retry,
            clock_tolerance: Duration::from_secs(300),
            deadline: Duration::from_secs(10),
            rate_buckets: Arc::new(DashMap::new()),
            dlq: Some(dlq),
        }
    }
}

// --- Delivery (requires DLQ) ---

impl<T, D> WebhookDeliveryService<T, D>
where
    T: WebhookTransport,
    D: DeadLetterQueue,
{
    /// Deliver a webhook event to an endpoint with full retry semantics.
    ///
    /// - **Permanent failures** (non-retryable 4xx) are returned immediately
    ///   as [`WebhookError::PermanentStatus`] and are NOT enqueued to the
    ///   dead-letter queue.
    /// - **Transient failures** (5xx, 429, 408, timeouts, connection errors)
    ///   are retried with exponential backoff up to `max_attempts`.  After
    ///   the last attempt the event is moved to the dead-letter queue (when
    ///   configured) and a [`WebhookError::Transient`] is returned.
    pub async fn deliver(
        &self,
        endpoint: &WebhookEndpoint,
        event: &WebhookEvent,
    ) -> Result<DeliveryReceipt, WebhookError> {
        // Rate-limit per endpoint
        let bucket = self
            .rate_buckets
            .entry(endpoint.id.clone())
            .or_insert_with(RateBucket::new);
        if !bucket.try_consume() {
            metrics::record_webhook_delivery(&endpoint.id, "rate_limited");
            return Err(WebhookError::RateLimited);
        }

        let body = serde_json::to_vec(event)?;
        let signature = Self::sign(endpoint.secret.as_bytes(), event.created_at, &body);
        let mut attempt = 0;
        let started = std::time::Instant::now();

        let error_msg = loop {
            attempt += 1;
            let result = timeout(
                self.deadline,
                self.transport
                    .send(&endpoint.url, &body, &signature, attempt),
            )
            .await;

            match result {
                // Success
                Ok(Ok(status)) if (200..300).contains(&status) => {
                    metrics::record_webhook_delivery(&endpoint.id, "success");
                    metrics::observe_webhook_latency(started.elapsed().as_secs_f64());
                    return Ok(DeliveryReceipt {
                        attempts: attempt,
                        status,
                    });
                }
                // Permanent failure — return immediately, no retry, no DLQ
                Ok(Ok(status)) if !is_retryable(status) => {
                    metrics::record_webhook_delivery(&endpoint.id, "failed");
                    return Err(WebhookError::PermanentStatus(status));
                }
                // Exhausted retries on a retryable status
                Ok(Ok(status)) if attempt >= self.retry.max_attempts => {
                    metrics::record_webhook_delivery(&endpoint.id, "failed");
                    break format!(
                        "retryable status {} persisted after {} attempts",
                        status, attempt
                    );
                }
                // Still have retries left for a retryable status
                Ok(Ok(_)) => {
                    metrics::record_webhook_retry(&endpoint.id);
                    sleep(self.retry.delay_for(attempt)).await;
                }
                // Transient transport error — retry if possible
                Ok(Err(WebhookError::Transient(msg))) => {
                    if attempt < self.retry.max_attempts {
                        metrics::record_webhook_retry(&endpoint.id);
                        sleep(self.retry.delay_for(attempt)).await;
                    } else {
                        metrics::record_webhook_delivery(&endpoint.id, "failed");
                        break msg;
                    }
                }
                // Timeout
                Err(_) => {
                    if attempt < self.retry.max_attempts {
                        metrics::record_webhook_retry(&endpoint.id);
                        sleep(self.retry.delay_for(attempt)).await;
                    } else {
                        metrics::record_webhook_delivery(&endpoint.id, "failed");
                        break "delivery timed out after max attempts".to_string();
                    }
                }
                // Non-recoverable error (e.g. serialization) — bail immediately
                Ok(Err(err)) => {
                    metrics::record_webhook_delivery(&endpoint.id, "failed");
                    return Err(err);
                }
            }
        };

        // All retries exhausted — move to dead-letter queue.
        if let Some(ref dlq) = self.dlq {
            let _ = dlq
                .enqueue(
                    &endpoint.id,
                    event.id,
                    &event.event_type,
                    &event.payload,
                    &error_msg,
                )
                .await;
        }

        Err(WebhookError::Transient(error_msg))
    }
}
