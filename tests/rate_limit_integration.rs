use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{Request, StatusCode},
    middleware as axum_mw,
    routing::get,
    Router,
};
use std::net::SocketAddr;
use utility_backend::api::middleware::{
    rate_limit_layer, tenant_rate_limit_layer, DynamicRateLimiter, TenantRateLimiter,
};

#[tokio::test]
async fn test_rate_limit_integration() {
    let limiter = DynamicRateLimiter::new();

    let app = Router::new()
        .route("/", get(|| async { "ok" }))
        .layer(axum_mw::from_fn_with_state(
            limiter.clone(),
            rate_limit_layer,
        ))
        .with_state(limiter.clone());

    let addr = SocketAddr::from(([127, 0, 0, 1], 12345));

    // Helper to send request
    let send_request = |app: Router| async move {
        let req = Request::builder()
            .uri("/")
            .extension(ConnectInfo(addr))
            .body(Body::empty())
            .unwrap();
        tower::ServiceExt::oneshot(app, req).await.unwrap()
    };

    // 1. Normal rate limiting (100 req/s)
    for _ in 0..100 {
        let res = send_request(app.clone()).await;
        assert_eq!(res.status(), StatusCode::OK);
    }
    let res = send_request(app.clone()).await;
    assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);

    // 2. Fraud flagging reduces limit to 10 req/s
    limiter.flag_source("127.0.0.1");

    // flag_source sets a 60s backoff by default.
    let res = send_request(app.clone()).await;
    assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn test_tenant_rate_limit_integration() {
    let tenant_limiter = TenantRateLimiter::new(5, 0);

    let app = Router::new()
        .route("/", get(|| async { "ok" }))
        .layer(axum_mw::from_fn_with_state(
            tenant_limiter.clone(),
            tenant_rate_limit_layer,
        ))
        .with_state(tenant_limiter.clone());

    let send_request = |app: Router, tenant: &str| async move {
        let req = Request::builder()
            .uri("/")
            .header("x-tenant-id", tenant)
            .body(Body::empty())
            .unwrap();
        tower::ServiceExt::oneshot(app, req).await.unwrap()
    };

    // grid-east gets 5 tokens (no refill)
    for _ in 0..5 {
        let res = send_request(app.clone(), "grid-east").await;
        assert_eq!(res.status(), StatusCode::OK);
    }
    let res = send_request(app.clone(), "grid-east").await;
    assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);

    // grid-west is a separate bucket — should still have tokens
    let res = send_request(app.clone(), "grid-west").await;
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_tenant_rate_limit_anonymous() {
    let tenant_limiter = TenantRateLimiter::new(3, 0);

    let app = Router::new()
        .route("/", get(|| async { "ok" }))
        .layer(axum_mw::from_fn_with_state(
            tenant_limiter.clone(),
            tenant_rate_limit_layer,
        ))
        .with_state(tenant_limiter.clone());

    let send_no_header = |app: Router| async move {
        let req = Request::builder().uri("/").body(Body::empty()).unwrap();
        tower::ServiceExt::oneshot(app, req).await.unwrap()
    };

    // No header → anonymous tenant gets default limit
    for _ in 0..3 {
        let res = send_no_header(app.clone()).await;
        assert_eq!(res.status(), StatusCode::OK);
    }
    let res = send_no_header(app.clone()).await;
    assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
}
