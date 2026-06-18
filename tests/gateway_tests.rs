use utility_backend::gateway::{
    crypto::{verify_packet, MeterIdentity},
    lock::AdvisoryLock,
    stream::{BackpressureFilter, MeterEvent},
};

#[tokio::test]
async fn test_backpressure_filter_roundtrip() {
    let (filter, mut rx) = BackpressureFilter::new(1024);
    let event = MeterEvent {
        meter_id: "MTR-TEST".into(),
        timestamp: 1700000000,
        reading: 240.5,
        token_volume: 1000,
    };
    filter.push(event).await.unwrap();
    let received = rx.recv().await.unwrap();
    assert_eq!(received.meter_id, "MTR-TEST");
    assert_eq!(received.token_volume, 1000);
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
