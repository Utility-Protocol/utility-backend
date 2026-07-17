use axum::{
    middleware as axum_mw,
    routing::{get, post},
    Router,
};
use tower_http::cors::CorsLayer;

use super::handlers;
use super::AppState;

pub async fn build_router(state: AppState) -> anyhow::Result<Router> {
    let cors = CorsLayer::permissive();

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/readyz", get(handlers::readyz_handler))
        .route("/api/v1/meters", get(handlers::list_meters))
        .route("/api/v1/meters/:id", get(handlers::get_meter))
        .route("/api/v1/tariffs", get(handlers::list_tariffs))
        .route("/api/v1/tariffs/explain", get(handlers::explain_tariff))
        .route("/api/v1/readings", post(handlers::submit_reading))
        .route("/api/v1/settle", post(handlers::settle_account))
        .route(
            "/api/v1/time-series/diagnostics/:meter_id",
            get(handlers::get_diagnostics),
        )
        .route(
            "/api/v1/calibrate/:meter_id",
            post(handlers::calibrate_meter),
        )
        .route("/api/v1/meters/register", post(handlers::register_meter))
        .route("/api/v1/meters/rotate-key", post(handlers::rotate_key))
        .route("/api/v1/nonce/status", get(handlers::nonce_status))
        .route("/api/v1/gateway/locks", get(handlers::list_gateway_locks))
        .route("/api/v1/dlq", get(handlers::list_dlq))
        .route("/api/v1/dlq/:id", get(handlers::get_dlq).delete(handlers::delete_dlq))
        .route("/api/v1/dlq/:id/retry", post(handlers::retry_dlq))
        .route("/metrics", get(handlers::metrics_handler))
        .route("/debug/clock_state", get(handlers::clock_state))
        .route(
            "/api/v1/telemetry/trace/:trace_id",
            get(crate::gateway::telemetry::get_trace),
        )
        .route(
            "/api/v1/database/compression/status",
            get(handlers::compression_status),
        )
        .route(
            "/api/v1/rate-limiter/status",
            get(handlers::rate_limiter_status),
        )
        .layer(axum_mw::from_fn_with_state(
            state.clone(),
            crate::api::middleware::rate_limit_layer,
        ))
        .layer(axum_mw::from_fn(
            crate::gateway::telemetry::tracing_middleware,
        ))
        .layer(cors)
        .with_state(state);

    Ok(app)
}
