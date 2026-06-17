use axum::{
    middleware as axum_mw,
    routing::{get, post},
    Router,
};
use tower_http::cors::CorsLayer;

use super::handlers;

pub async fn build_router() -> anyhow::Result<Router> {
    let cors = CorsLayer::permissive();

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/api/v1/meters", get(handlers::list_meters))
        .route("/api/v1/meters/:id", get(handlers::get_meter))
        .route("/api/v1/tariffs", get(handlers::list_tariffs))
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
        .route("/metrics", get(handlers::metrics_handler))
        .layer(axum_mw::from_fn(crate::api::middleware::rate_limit_layer))
        .layer(cors);

    Ok(app)
}
