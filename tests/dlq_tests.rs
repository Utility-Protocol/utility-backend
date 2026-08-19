use axum_test::TestServer;
use std::sync::Arc;
use tokio::sync::Mutex;
use utility_backend::api::middleware::DynamicRateLimiter;
use utility_backend::api::router::build_router;
use utility_backend::api::AppState;
use utility_backend::gateway::lock::AdvisoryLock;
use utility_backend::settlement::dlq::{
    delete_dlq_message, ensure_dlq_schema, get_dlq_by_id, increment_retry_count, list_dlq,
    send_to_dlq, update_dlq_status,
};
use utility_backend::settlement::finalizer::Finalizer;
use utility_backend::settlement::mint_queue::MintQueue;
use utility_backend::soroban::rpc::CircuitBreaker;

async fn setup_test_db() -> Option<(sqlx::PgPool, sqlx::pool::PoolConnection<sqlx::Postgres>)> {
    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://utility:utility_secret@localhost:5432/utility_test".into());

    match sqlx::PgPool::connect(&db_url).await {
        Ok(pool) => {
            // Serialize DB access across all test binaries that share this
            // database.  The advisory lock is session-scoped and released when
            // the guard connection closes (pool drop at end of test).
            let mut lock_conn = match pool.acquire().await {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("Could not acquire DB lock connection: {e}");
                    return None;
                }
            };
            if let Err(e) = sqlx::query("SELECT pg_advisory_lock(4000000001)")
                .execute(&mut *lock_conn)
                .await
            {
                eprintln!("Could not acquire DB advisory lock: {e}");
                return None;
            }

            // Clean up tables
            let _ = ensure_dlq_schema(&pool).await;
            let _ = sqlx::query("DELETE FROM dead_letter_queue")
                .execute(&pool)
                .await;
            let _ = sqlx::query("DELETE FROM processed_mints")
                .execute(&pool)
                .await;
            let _ = sqlx::query("DELETE FROM pending_mints")
                .execute(&pool)
                .await;
            Some((pool, lock_conn))
        }
        Err(_) => {
            eprintln!("Skipping integration test: DATABASE_URL not available");
            None
        }
    }
}

#[tokio::test]
async fn test_dlq_core_repository_operations() {
    let Some((pool, _db_guard)) = setup_test_db().await else {
        return;
    };

    let queue_name = "test-queue";
    let message_id = "msg-123";
    let payload = serde_json::json!({
        "batch_id": "test-batch",
        "amount": 42.5
    });
    let error_reason = "Connection timed out";

    // 1. Send to DLQ
    let id = send_to_dlq(&pool, queue_name, message_id, &payload, Some(error_reason))
        .await
        .unwrap();

    // 2. Get by ID
    let msg = get_dlq_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(msg.queue_name, queue_name);
    assert_eq!(msg.message_id, message_id);
    assert_eq!(msg.payload, payload);
    assert_eq!(msg.error_reason.as_deref(), Some(error_reason));
    assert_eq!(msg.status, "failed");
    assert_eq!(msg.retry_count, 0);

    // 3. Upsert / Duplicate message updates status/error but preserves uniqueness
    let new_error = "Authentication failed";
    let new_id = send_to_dlq(&pool, queue_name, message_id, &payload, Some(new_error))
        .await
        .unwrap();
    assert_eq!(id, new_id);

    let msg_updated = get_dlq_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(msg_updated.error_reason.as_deref(), Some(new_error));

    // 4. Update status
    let ok = update_dlq_status(&pool, id, "resolved").await.unwrap();
    assert!(ok);
    let msg_status = get_dlq_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(msg_status.status, "resolved");

    // 5. Increment retry count
    let ok = increment_retry_count(&pool, id, Some("Retried and failed again"))
        .await
        .unwrap();
    assert!(ok);
    let msg_retry = get_dlq_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(msg_retry.retry_count, 1);
    assert_eq!(msg_retry.status, "failed");
    assert_eq!(
        msg_retry.error_reason.as_deref(),
        Some("Retried and failed again")
    );

    // 6. List DLQ
    let list = list_dlq(&pool, Some("failed"), 10, 0).await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, id);

    // 7. Delete DLQ message
    let ok = delete_dlq_message(&pool, id).await.unwrap();
    assert!(ok);
    let msg_deleted = get_dlq_by_id(&pool, id).await.unwrap();
    assert!(msg_deleted.is_none());
}

#[tokio::test]
async fn test_finalizer_automatic_dead_lettering() {
    let Some((pool, _db_guard)) = setup_test_db().await else {
        return;
    };

    let batch_id = "test-batch-dlq-auto";
    let resource_type = "electricity";
    let destination = "GABC...456";

    let mint_queue = MintQueue::new(pool.clone());
    mint_queue
        .enqueue(batch_id, resource_type, 150.0, destination)
        .await
        .unwrap();

    let breaker = Arc::new(Mutex::new(CircuitBreaker::new(5)));
    // Use an invalid URL that will definitely fail to connect
    let finalizer = Finalizer::new(
        pool.clone(),
        "http://127.0.0.1:1/invalid-rpc-url".into(),
        breaker,
    );

    // 1. Try to finalize. This should fail because of the invalid URL.
    let result = finalizer.finalize_mint(batch_id, resource_type).await;
    assert!(result.is_err());

    // 2. Verify that the failed attempt was automatically dead-lettered!
    let list = list_dlq(&pool, Some("failed"), 10, 0).await.unwrap();
    assert_eq!(list.len(), 1);

    let dlq_msg = &list[0];
    assert_eq!(dlq_msg.queue_name, "mint-events");
    assert_eq!(
        dlq_msg.message_id,
        format!("{}:{}", batch_id, resource_type)
    );
    assert_eq!(dlq_msg.payload.get("batch_id").unwrap(), batch_id);
    assert_eq!(dlq_msg.payload.get("resource_type").unwrap(), resource_type);
    assert_eq!(dlq_msg.payload.get("amount").unwrap(), 150.0);
    assert_eq!(dlq_msg.payload.get("destination").unwrap(), destination);
}

#[tokio::test]
async fn test_dlq_admin_api_endpoints() {
    let Some((pool, _db_guard)) = setup_test_db().await else {
        return;
    };

    // The admin DLQ endpoints are backed by the webhooks dead-letter table
    // (db/webhooks.sql).  Apply that schema so this test works against a
    // freshly created CI database.
    for statement in include_str!("../db/webhooks.sql").split(';') {
        let statement = statement.trim();
        if !statement.is_empty() {
            sqlx::query(statement).execute(&pool).await.unwrap();
        }
    }

    // Pre-populate with one webhooks dead-letter entry and its endpoint.
    use utility_backend::webhooks::dead_letter::{DeadLetterEntry, PostgresDlq};
    use utility_backend::webhooks::DeadLetterQueue;

    let dlq = PostgresDlq::new(pool.clone());
    let entry: DeadLetterEntry = dlq
        .enqueue(
            "ep-test",
            uuid::Uuid::new_v4(),
            "meter.reading",
            &serde_json::json!({ "meter_id": "M-42" }),
            "Initial failure",
        )
        .await
        .unwrap();

    sqlx::query(
        "INSERT INTO webhook_endpoints (id, url, secret, tenant_id) VALUES ($1, $2, $3, $4)",
    )
    .bind("ep-test")
    .bind("http://127.0.0.1:1/unreachable")
    .bind("test-secret")
    .bind("tenant-1")
    .execute(&pool)
    .await
    .unwrap();

    // Setup Axum app state and router
    let sequencer = Arc::new(utility_backend::soroban::sequencer::NonceSequencer::new());
    let advisory_lock = Arc::new(AdvisoryLock::postgres(pool.clone()));
    let breaker = Arc::new(Mutex::new(CircuitBreaker::new(5)));
    let rate_limiter = DynamicRateLimiter::new();

    let state = AppState {
        sequencer,
        pool: pool.clone(),
        advisory_lock,
        breaker,
        rate_limiter,
        tenant_rate_limiter: utility_backend::api::middleware::TenantRateLimiter::new(100, 10),
        service_rate_limiter: utility_backend::api::middleware::ServiceRateLimiter::new(100, 10),
        hlc: Arc::new(utility_backend::gateway::hlc::HybridLogicalClock::new()),
    };

    let app = build_router(state).await.unwrap();
    let server = TestServer::new(app).unwrap();

    // 1. Test GET /api/v1/dlq (List)
    let response = server.get("/api/v1/dlq").await;
    response.assert_status_ok();
    let list: Vec<DeadLetterEntry> = response.json();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, entry.id);
    assert_eq!(list[0].endpoint_id, "ep-test");

    // 2. Test GET /api/v1/dlq/:id (Get Specific)
    let response = server.get(&format!("/api/v1/dlq/{}", entry.id)).await;
    response.assert_status_ok();
    let msg: DeadLetterEntry = response.json();
    assert_eq!(msg.event_type, "meter.reading");
    assert_eq!(msg.endpoint_id, "ep-test");

    // 3. Test POST /api/v1/dlq/:id/retry (delivery fails: endpoint is
    //    unreachable).  The handler removes the entry first, so the original
    //    is gone afterwards.
    let response = server
        .post(&format!("/api/v1/dlq/{}/retry", entry.id))
        .await;
    response.assert_status_ok();
    let receipt: utility_backend::api::handlers::RetryDeadLetterResponse = response.json();
    assert_eq!(receipt.status, 0);

    let response = server.get(&format!("/api/v1/dlq/{}", entry.id)).await;
    response.assert_status(axum::http::StatusCode::NOT_FOUND);

    // 4. Test DELETE /api/v1/dlq/:id (Manual deletion)
    let response = server.delete(&format!("/api/v1/dlq/{}", entry.id)).await;
    response.assert_status(axum::http::StatusCode::NO_CONTENT);
}
