use axum::http::StatusCode;
use axum_test::TestServer;
use std::sync::Arc;
use tokio::sync::Mutex;
use utility_backend::api::middleware::{DynamicRateLimiter, ServiceRateLimiter, TenantRateLimiter};
use utility_backend::api::rate_limit_config::{
    ensure_schema, RateLimitConfig, RateLimitScopeType, GLOBAL_SCOPE_KEY,
};
use utility_backend::api::router::build_router;
use utility_backend::api::AppState;
use utility_backend::audit::verify_hash_chain;
use utility_backend::gateway::lock::AdvisoryLock;
use utility_backend::soroban::rpc::CircuitBreaker;

async fn setup_test_db() -> Option<(sqlx::PgPool, sqlx::pool::PoolConnection<sqlx::Postgres>)> {
    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://utility:utility_secret@localhost:5432/utility_test".into());

    match sqlx::PgPool::connect(&db_url).await {
        Ok(pool) => {
            let mut lock_conn = match pool.acquire().await {
                Ok(conn) => conn,
                Err(e) => {
                    eprintln!("Could not acquire DB lock connection: {e}");
                    return None;
                }
            };
            if let Err(e) = sqlx::query("SELECT pg_advisory_lock(4000000003)")
                .execute(&mut *lock_conn)
                .await
            {
                eprintln!("Could not acquire DB advisory lock: {e}");
                return None;
            }

            for statement in include_str!("../db/audit_events.sql").split(';') {
                let statement = statement.trim();
                if !statement.is_empty() {
                    let _ = sqlx::query(statement).execute(&pool).await;
                }
            }
            let _ = ensure_schema(&pool).await;
            let _ = sqlx::query("DELETE FROM rate_limit_configs")
                .execute(&pool)
                .await;
            let _ = sqlx::query("DELETE FROM audit_events")
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

async fn build_test_server(
    pool: sqlx::PgPool,
    tenant_limiter: Arc<TenantRateLimiter>,
    service_limiter: Arc<ServiceRateLimiter>,
) -> TestServer {
    let state = AppState {
        sequencer: Arc::new(utility_backend::soroban::sequencer::NonceSequencer::new()),
        pool: pool.clone(),
        advisory_lock: Arc::new(AdvisoryLock::postgres(pool)),
        breaker: Arc::new(Mutex::new(CircuitBreaker::new(5))),
        rate_limiter: DynamicRateLimiter::new(),
        tenant_rate_limiter: tenant_limiter,
        service_rate_limiter: service_limiter,
        hlc: Arc::new(utility_backend::gateway::hlc::HybridLogicalClock::new()),
    };

    TestServer::new(build_router(state).await.unwrap()).unwrap()
}

#[tokio::test]
async fn test_rate_limit_config_crud_and_audit() {
    let Some((pool, _guard)) = setup_test_db().await else {
        return;
    };

    let server = build_test_server(
        pool.clone(),
        TenantRateLimiter::new(1000, 1000),
        ServiceRateLimiter::new(1000, 1000),
    )
    .await;

    let create_body = serde_json::json!({
        "scope_type": "user",
        "scope_key": "tenant-alpha",
        "max_tokens": 3,
        "refill_rate": 0,
        "actor": "ops@example.com"
    });

    let response = server
        .post("/api/v1/rate-limits/configs")
        .json(&create_body)
        .await;
    response.assert_status(StatusCode::CREATED);
    let created: RateLimitConfig = response.json();
    assert_eq!(created.scope_type, RateLimitScopeType::User);
    assert_eq!(created.scope_key, "tenant-alpha");
    assert_eq!(created.max_tokens, 3);

    let response = server.get("/api/v1/rate-limits/configs").await;
    response.assert_status_ok();
    let list: Vec<RateLimitConfig> = response.json();
    assert_eq!(list.len(), 1);

    let response = server
        .get(&format!("/api/v1/rate-limits/configs/{}", created.id))
        .await;
    response.assert_status_ok();

    let update_body = serde_json::json!({
        "max_tokens": 5,
        "refill_rate": 0,
        "actor": "ops@example.com"
    });
    let response = server
        .put(&format!("/api/v1/rate-limits/configs/{}", created.id))
        .json(&update_body)
        .await;
    response.assert_status_ok();
    let updated: RateLimitConfig = response.json();
    assert_eq!(updated.max_tokens, 5);

    let response = server
        .delete(&format!(
            "/api/v1/rate-limits/configs/{}?actor=ops@example.com",
            created.id
        ))
        .await;
    response.assert_status(StatusCode::NO_CONTENT);

    let response = server
        .get(&format!("/api/v1/rate-limits/configs/{}", created.id))
        .await;
    response.assert_status(StatusCode::NOT_FOUND);

    let response = server.get("/api/v1/rate-limits/configs/audit?limit=10").await;
    response.assert_status_ok();
    let audit_entries: Vec<utility_backend::api::rate_limit_config::RateLimitAuditEntry> =
        response.json();
    assert_eq!(audit_entries.len(), 3);
    assert!(audit_entries.iter().any(|entry| entry.action == "rate_limit.create"));
    assert!(audit_entries.iter().any(|entry| entry.action == "rate_limit.update"));
    assert!(audit_entries.iter().any(|entry| entry.action == "rate_limit.delete"));

    let rows = sqlx::query_as::<
        _,
        (
            i64,
            chrono::DateTime<chrono::Utc>,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
        ),
    >(
        "SELECT sequence, occurred_at, actor, service, action, resource, payload_hash, previous_hash, hash
         FROM audit_events
         ORDER BY sequence",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    let events: Vec<_> = rows
        .into_iter()
        .map(
            |(
                sequence,
                occurred_at,
                actor,
                service,
                action,
                resource,
                payload_hash,
                previous_hash,
                hash,
            )| {
                utility_backend::audit::AuditEvent {
                    sequence: sequence as u64,
                    occurred_at,
                    actor,
                    service,
                    action,
                    resource,
                    payload_hash,
                    previous_hash,
                    hash,
                }
            },
        )
        .collect();
    let report = verify_hash_chain(&events);
    assert!(report.verified, "{:?}", report.reason);
}

#[tokio::test]
async fn test_user_rate_limit_hot_reload_via_api() {
    let Some((pool, _guard)) = setup_test_db().await else {
        return;
    };

    let server = build_test_server(
        pool,
        TenantRateLimiter::new(100, 0),
        ServiceRateLimiter::new(100, 0),
    )
    .await;

    let create_body = serde_json::json!({
        "scope_type": "user",
        "scope_key": "grid-east",
        "max_tokens": 2,
        "refill_rate": 0,
        "actor": "ops"
    });
    server
        .post("/api/v1/rate-limits/configs")
        .json(&create_body)
        .await
        .assert_status(StatusCode::CREATED);

    for _ in 0..2 {
        let response = server
            .get("/health")
            .add_header("x-tenant-id", "grid-east")
            .await;
        response.assert_status_ok();
    }

    let response = server
        .get("/health")
        .add_header("x-tenant-id", "grid-east")
        .await;
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    let response = server
        .get("/health")
        .add_header("x-tenant-id", "grid-west")
        .await;
    response.assert_status_ok();
}

#[tokio::test]
async fn test_global_rate_limit_hot_reload_via_api() {
    let Some((pool, _guard)) = setup_test_db().await else {
        return;
    };

    let state = AppState {
        sequencer: Arc::new(utility_backend::soroban::sequencer::NonceSequencer::new()),
        pool: pool.clone(),
        advisory_lock: Arc::new(AdvisoryLock::postgres(pool.clone())),
        breaker: Arc::new(Mutex::new(CircuitBreaker::new(5))),
        rate_limiter: DynamicRateLimiter::new(),
        tenant_rate_limiter: TenantRateLimiter::new(10_000, 10_000),
        service_rate_limiter: ServiceRateLimiter::new(10_000, 10_000),
        hlc: Arc::new(utility_backend::gateway::hlc::HybridLogicalClock::new()),
    };
    let server = TestServer::new(build_router(state).await.unwrap()).unwrap();

    let create_body = serde_json::json!({
        "scope_type": "global",
        "max_tokens": 1,
        "refill_rate": 0,
        "actor": "ops"
    });
    server
        .post("/api/v1/rate-limits/configs")
        .json(&create_body)
        .await
        .assert_status(StatusCode::CREATED);

    let response = server.get("/health").await;
    response.assert_status_ok();

    let response = server.get("/health").await;
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn test_service_rate_limit_hot_reload_via_api() {
    let Some((pool, _guard)) = setup_test_db().await else {
        return;
    };

    let server = build_test_server(
        pool,
        TenantRateLimiter::new(100, 100),
        ServiceRateLimiter::new(100, 0),
    )
    .await;

    let create_body = serde_json::json!({
        "scope_type": "service",
        "scope_key": "readings",
        "max_tokens": 2,
        "refill_rate": 0,
        "actor": "ops"
    });
    server
        .post("/api/v1/rate-limits/configs")
        .json(&create_body)
        .await
        .assert_status(StatusCode::CREATED);

    for _ in 0..2 {
        let response = server
            .get("/health")
            .add_header("x-service-id", "readings")
            .await;
        response.assert_status_ok();
    }

    let response = server
        .get("/health")
        .add_header("x-service-id", "readings")
        .await;
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn test_global_scope_key_validation() {
    let Some((pool, _guard)) = setup_test_db().await else {
        return;
    };

    let server = build_test_server(
        pool,
        TenantRateLimiter::new(100, 100),
        ServiceRateLimiter::new(100, 100),
    )
    .await;

    let create_body = serde_json::json!({
        "scope_type": "global",
        "scope_key": "wrong-key",
        "max_tokens": 10,
        "refill_rate": 1,
        "actor": "ops"
    });
    server
        .post("/api/v1/rate-limits/configs")
        .json(&create_body)
        .await
        .assert_status(StatusCode::BAD_REQUEST);

    let create_body = serde_json::json!({
        "scope_type": "global",
        "scope_key": GLOBAL_SCOPE_KEY,
        "max_tokens": 10,
        "refill_rate": 1,
        "actor": "ops"
    });
    server
        .post("/api/v1/rate-limits/configs")
        .json(&create_body)
        .await
        .assert_status(StatusCode::CREATED);
}
