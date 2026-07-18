use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use utility_backend::webhooks::{
    RetryPolicy, WebhookDeliveryService, WebhookEndpoint, WebhookError, WebhookEvent,
    WebhookTransport,
};
use uuid::Uuid;

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
    }
}

#[tokio::test]
async fn signs_and_verifies_webhook_payloads() {
    let transport = Arc::new(SequenceTransport {
        statuses: Mutex::new(vec![200]),
        attempts: Mutex::new(vec![]),
    });
    let service = WebhookDeliveryService::new(transport, RetryPolicy::default());
    let event = event();
    let body = serde_json::to_vec(&event).unwrap();
    let signature = WebhookDeliveryService::<SequenceTransport>::sign(
        endpoint().secret.as_bytes(),
        event.created_at,
        &body,
    );

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
    let service = WebhookDeliveryService::new(
        transport.clone(),
        RetryPolicy {
            max_attempts: 5,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
        },
    );

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
    let service = WebhookDeliveryService::new(transport.clone(), RetryPolicy::default());

    let err = service.deliver(&endpoint(), &event()).await.unwrap_err();

    assert!(matches!(err, WebhookError::PermanentStatus(400)));
    assert_eq!(*transport.attempts.lock(), vec![1]);
}
