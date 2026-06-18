use std::sync::Arc;

use axum::{
    middleware as axum_mw,
    routing::{get, post},
    Router,
};
use tower_http::cors::CorsLayer;

use super::handlers;
use crate::soroban::sequencer::NonceSequencer;

pub async fn build_router(sequencer: Arc<NonceSequencer>) -> anyhow::Result<Router> {
    let cors = CorsLayer::permissive();

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/api/v1/meters", get(handlers::list_meters))
        .route("/api/v1/meters/:id", get(handlers::get_meter))
        .route("/api/v1/tariffs", get(handlers::list_tariffs))
        .route("/api/v1/readings", post(handlers::submit_reading))
        .route("/api/v1/settle", post(handlers::settle_account))
        .route("/metrics", get(handlers::metrics_handler))
        .route("/api/v1/nonce/status", get(handlers::nonce_status))
        .layer(axum_mw::from_fn(crate::api::middleware::rate_limit_layer))
        .layer(cors)
        .with_state(sequencer);

    Ok(app)
}
