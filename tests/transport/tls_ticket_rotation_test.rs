use std::{fs, sync::Arc, time::Duration};
use utility_backend::transport::tls::{SessionTicketConfig, SessionTicketStore};

#[tokio::test]
async fn session_ticket_keys_rotate_with_overlap_and_fallback() {
    let dir = std::env::temp_dir().join(format!(
        "utility-stk-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let config = SessionTicketConfig {
        rotation_interval_hours: 24,
        key_directory: dir.clone(),
        ticket_lifetime_hours: 12,
    };
    let store = Arc::new(SessionTicketStore::load_or_generate(&config).unwrap());

    let old_ticket = store.encrypt_ticket(b"meter session").unwrap();
    assert_eq!(
        store.decrypt_ticket(&old_ticket).as_deref(),
        Some(&b"meter session"[..])
    );

    store.rotate_now().unwrap();
    let new_ticket = store.encrypt_ticket(b"new meter session").unwrap();

    assert_eq!(
        store.decrypt_ticket(&old_ticket).as_deref(),
        Some(&b"meter session"[..])
    );
    assert_eq!(
        store.decrypt_ticket(&new_ticket).as_deref(),
        Some(&b"new meter session"[..])
    );

    let mut tampered = old_ticket.clone();
    let last = tampered.len() - 1;
    tampered[last] ^= 0x80;
    assert!(
        store.decrypt_ticket(&tampered).is_none(),
        "bad tickets fall back to a full handshake by returning None"
    );

    tokio::time::sleep(Duration::from_millis(10)).await;
    fs::remove_dir_all(dir).ok();
}
