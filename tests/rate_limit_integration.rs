use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::get,
    Router,
    extract::ConnectInfo,
    middleware as axum_mw,
};
use std::net::SocketAddr;
use utility_backend::api::middleware::{DynamicRateLimiter, rate_limit_layer};

#[tokio::test]
async fn test_rate_limit_integration() {
    let limiter = DynamicRateLimiter::new();

    let app = Router::new()
        .route("/", get(|| async { "ok" }))
        .layer(axum_mw::from_fn_with_state(limiter.clone(), rate_limit_layer))
        .with_state(limiter.clone());

    let addr = SocketAddr::from(([127, 0, 0, 1], 12345));

    // Helper to send request
    let send_request = |app: Router| {
        async move {
            let req = Request::builder()
                .uri("/")
                .extension(ConnectInfo(addr))
                .body(Body::empty())
                .unwrap();
            tower::ServiceExt::oneshot(app, req).await.unwrap()
        }
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
