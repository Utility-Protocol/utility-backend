use std::sync::Arc;

use utility_backend::gateway::{
    crypto::{verify_packet, MeterIdentity},
    hlc::HybridLogicalClock,
    lock::AdvisoryLock,
    stream::{BackpressureFilter, MeterEvent},
};

#[tokio::test]
async fn test_backpressure_filter_roundtrip() {
    let hlc = Arc::new(HybridLogicalClock::new());
    let (filter, mut rx) = BackpressureFilter::new(1024, hlc);
    let event = MeterEvent {
        meter_id: "MTR-TEST".into(),
        timestamp: 1_700_000_000,
        reading: 240.5,
        token_volume: 1000,
        hlc_timestamp: 0,
    };
    filter.push(event).await.unwrap();
    let received = rx.recv().await.unwrap();
    assert_eq!(received.meter_id, "MTR-TEST");
    assert_eq!(received.token_volume, 1000);
    assert!(
        received.hlc_timestamp > 0,
        "HLC timestamp should be assigned"
    );
}

#[tokio::test]
async fn test_advisory_lock_prevents_concurrent_deductions() {
    let lock = AdvisoryLock::new();
    let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut handles = vec![];
    for _ in 0..100 {
        let c = counter.clone();
        let l = lock.clone();
        handles.push(tokio::spawn(async move {
            l.lock("resource:water:001", || async {
                let val = c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_micros(10)).await;
                val
            })
            .await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 100);
}

#[test]
fn test_crypto_verify_hardware_meter() {
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    use utility_backend::gateway::crypto::MeterStatus;

    let mut csprng = OsRng;
    let signing_key = SigningKey::generate(&mut csprng);
    let verifying_key = signing_key.verifying_key();
    let identity = MeterIdentity {
        meter_id: "MTR-HW-99".into(),
        public_key: verifying_key,
        status: MeterStatus::Active,
        enrolled_at: 1000,
        key_rotated_at: 1000,
    };
    let payload = b"flow_rate:15.7;pressure:42.3";
    let signature = signing_key.sign(payload);
    assert!(verify_packet(&identity, payload, &signature.to_bytes()).is_ok());
}

#[tokio::test]
async fn test_advisory_lock_reports_fencing_token_and_active_lease() {
    let lock = AdvisoryLock::new();
    let outcome = lock
        .lock_with_fencing("resource:electric:active", |token| {
            let lock = lock.clone();
            async move {
                let active = lock.active_locks();
                assert_eq!(token, 1);
                assert_eq!(active.len(), 1);
                assert_eq!(active[0].resource, "resource:electric:active");
                assert_eq!(active[0].fencing_token, token);
                42_u64
            }
        })
        .await
        .unwrap();

    assert_eq!(outcome.fencing_token, 1);
    assert_eq!(outcome.value, 42);
    assert!(lock.active_locks().is_empty());
}
