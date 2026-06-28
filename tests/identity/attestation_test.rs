use p256::ecdsa::{signature::Signer, Signature, SigningKey};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use utility_backend::identity::attestation::{AttestationVerifier, IdentityConfig};
use utility_backend::identity::cert_store::CertStore;
use utility_backend::identity::signer::MeterSigner;
use utility_backend::transport::tcp::acceptor::{accept_and_register, bind_listener};
use utility_backend::transport::tcp::connection_manager::ConnectionManager;
use utility_backend::transport::tcp::rate_limiter::ConnectionRateLimiter;

#[tokio::test]
async fn test_attestation_and_signature_flow() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("rocksdb");
    let cert_store = Arc::new(CertStore::new(db_path.to_str().unwrap())?);

    let config = IdentityConfig {
        attestation_enabled: true,
        crl_refresh_interval_s: 300,
        max_attestation_time_ms: 5000,
    };
    let verifier = Arc::new(AttestationVerifier::new(config));
    let meter_signer = MeterSigner::new(cert_store.clone());

    let cm = Arc::new(ConnectionManager::new(Duration::from_secs(300), 1000));
    let limiter = Arc::new(ConnectionRateLimiter::new(100, 200, Duration::from_secs(1)));
    let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 128)?;
    let addr = listener.local_addr()?;

    let meter_id = "meter-123".to_string();

    let client_task = tokio::spawn({
        let meter_id = meter_id.clone();
        async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();

            stream.write_u16(meter_id.len() as u16).await.unwrap();
            stream.write_all(meter_id.as_bytes()).await.unwrap();

            let mut nonce = [0u8; 32];
            stream.read_exact(&mut nonce).await.unwrap();

            let signing_key = SigningKey::random(&mut rand::thread_rng());
            let verifying_key = signing_key.verifying_key();
            let pk_bytes = verifying_key.to_sec1_bytes();

            let mut hasher = Sha256::new();
            hasher.update(meter_id.as_bytes());
            hasher.update(nonce);
            let quote = hasher.finalize();

            stream.write_u16(quote.len() as u16).await.unwrap();
            stream.write_all(&quote).await.unwrap();

            stream.write_u16(pk_bytes.len() as u16).await.unwrap();
            stream.write_all(&pk_bytes).await.unwrap();

            let enclave_sig = [0u8; 64];
            stream.write_u16(enclave_sig.len() as u16).await.unwrap();
            stream.write_all(&enclave_sig).await.unwrap();

            let cert_len = stream.read_u16().await.unwrap() as usize;
            let mut cert = vec![0u8; cert_len];
            stream.read_exact(&mut cert).await.unwrap();

            (signing_key, cert)
        }
    });

    let (accepted_meter_id, _handle) =
        accept_and_register(&listener, &cm, &limiter, Some(&verifier), Some(&cert_store)).await?;

    assert_eq!(accepted_meter_id, meter_id);

    let (signing_key, _cert) = client_task.await?;

    let data = b"telemetry-data";
    let signature: Signature = signing_key.sign(data);
    let signature_bytes: [u8; 64] = signature.to_bytes().as_slice().try_into().unwrap();

    meter_signer
        .verify_signature(&meter_id, data, &signature_bytes)
        .expect("Signature verification failed");

    let mut revoked = HashSet::new();
    revoked.insert(meter_id.clone());
    cert_store.update_crl(revoked)?;

    let result = meter_signer.verify_signature(&meter_id, data, &signature_bytes);
    assert!(
        result.is_err(),
        "Signature verification should fail for revoked meter"
    );

    Ok(())
}
