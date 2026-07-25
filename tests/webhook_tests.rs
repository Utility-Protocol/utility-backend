use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::json;
use std::sync::Arc;
use utility_backend::webhooks::dead_letter::{DeadLetterEntry, DeadLetterQueue};
use utility_backend::webhooks::dispatcher::WebhookDeliveryService;
use utility_backend::webhooks::signer;
use utility_backend::webhooks::{
    RetryPolicy, WebhookEndpoint, WebhookError, WebhookEvent, WebhookTransport,
};
use uuid::Uuid;

// ----- Mock transport -------------------------------------------------------

struct SequenceTransport {
    statuses: Mutex<Vec<u16>>,
    attempts: Mutex<Vec<u32>>,
}

#[async_trait]
impl WebhookTransport for SequenceTransport {
    async fn send(
        &self,
        _url: &str,
        _body: &[u8],
        _signature: &str,
        attempt: u32,
    ) -> Result<u16, WebhookError> {
        self.attempts.lock().push(attempt);
        Ok(self.statuses.lock().remove(0))
    }
}

// ----- Mock dead-letter queue ------------------------------------------------

struct MockDlq {
    entries: Mutex<Vec<DeadLetterEntry>>,
}

impl MockDlq {
    fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl DeadLetterQueue for MockDlq {
    async fn enqueue(
        &self,
        endpoint_id: &str,
        event_id: Uuid,
        event_type: &str,
        payload: &serde_json::Value,
        error: &str,
    ) -> Result<DeadLetterEntry, String> {
        let entry = DeadLetterEntry {
            id: Uuid::new_v4(),
            endpoint_id: endpoint_id.to_string(),
            event_id,
            event_type: event_type.to_string(),
            payload: payload.clone(),
            failed_at: chrono::Utc::now(),
            retry_count: 0,
            last_error: Some(error.to_string()),
        };
        self.entries.lock().push(entry.clone());
        Ok(entry)
    }

    async fn list(
        &self,
        _endpoint_id: Option<&str>,
        _limit: i64,
        _offset: i64,
    ) -> Result<Vec<DeadLetterEntry>, String> {
        Ok(self.entries.lock().clone())
    }

    async fn get(&self, _id: Uuid) -> Result<Option<DeadLetterEntry>, String> {
        Ok(None)
    }

    async fn remove(&self, _id: Uuid) -> Result<(), String> {
        Ok(())
    }
}

// ----- Helpers --------------------------------------------------------------

fn event() -> WebhookEvent {
    WebhookEvent {
        id: Uuid::new_v4(),
        event_type: "meter.reading.created".into(),
        created_at: chrono::Utc::now(),
        payload: json!({"meter_id":"MTR-001","value":42.0}),
    }
}

fn endpoint() -> WebhookEndpoint {
    WebhookEndpoint {
        id: "tenant-a".into(),
        url: "https://example.test/webhook".into(),
        secret: "super-secret".into(),
        tenant_id: "grid-east".into(),
    }
}

fn fast_service(
    transport: Arc<SequenceTransport>,
) -> WebhookDeliveryService<SequenceTransport, MockDlq> {
    WebhookDeliveryService::with_dlq(transport, RetryPolicy::fast(), Arc::new(MockDlq::new()))
}

// ----- Tests ----------------------------------------------------------------

#[tokio::test]
async fn signs_and_verifies_webhook_payloads() {
    let transport = Arc::new(SequenceTransport {
        statuses: Mutex::new(vec![200]),
        attempts: Mutex::new(vec![]),
    });
    let service = fast_service(transport);
    let event = event();
    let body = serde_json::to_vec(&event).unwrap();
    let signature = signer::sign(endpoint().secret.as_bytes(), event.created_at, &body);

    service
        .verify_signature(
            endpoint().secret.as_bytes(),
            event.created_at,
            &body,
            &signature,
        )
        .unwrap();
    assert!(service
        .verify_signature(
            endpoint().secret.as_bytes(),
            event.created_at,
            b"tampered",
            &signature
        )
        .is_err());
}

#[tokio::test]
async fn retries_transient_statuses_until_success() {
    let transport = Arc::new(SequenceTransport {
        statuses: Mutex::new(vec![500, 429, 204]),
        attempts: Mutex::new(vec![]),
    });
    let service = fast_service(transport.clone());

    let receipt = service.deliver(&endpoint(), &event()).await.unwrap();

    assert_eq!(receipt.attempts, 3);
    assert_eq!(*transport.attempts.lock(), vec![1, 2, 3]);
}

#[tokio::test]
async fn does_not_retry_permanent_statuses() {
    let transport = Arc::new(SequenceTransport {
        statuses: Mutex::new(vec![400]),
        attempts: Mutex::new(vec![]),
    });
    let service = fast_service(transport.clone());

    let err = service.deliver(&endpoint(), &event()).await.unwrap_err();

    assert!(matches!(err, WebhookError::PermanentStatus(400)));
    assert_eq!(*transport.attempts.lock(), vec![1]);
}
