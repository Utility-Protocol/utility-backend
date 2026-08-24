use axum::{
    middleware as axum_mw,
    routing::{delete, get, post},
    Router,
};
use std::sync::Arc;
use tower_http::cors::CorsLayer;

use super::handlers;
use super::AppState;

pub async fn build_router(state: AppState) -> anyhow::Result<Router> {
    let cors = CorsLayer::permissive();

    // ── GraphQL schema with subscription support (#242) ──────────────
    let pubsub = Arc::new(crate::graphql::pubsub::SimplePubSub::new(256));
    let graphql_schema = crate::graphql::build_schema(pubsub.clone());

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/health/summary", get(crate::api::health::summary_health))
        .route("/health/detailed", get(crate::api::health::detailed_health))
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
        .route(
            "/api/v1/dlq/:id",
            get(handlers::get_dlq).delete(handlers::delete_dlq),
        )
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
        .route("/api/v1/slo/status", get(handlers::slo_status))
        .route(
            "/api/v1/tenant-rate-limiter/status",
            get(handlers::tenant_rate_limiter_status),
        )
        .route(
            "/api/v1/rate-limits/configs",
            get(handlers::list_rate_limit_configs).post(handlers::create_rate_limit_config),
        )
        .route(
            "/api/v1/rate-limits/configs/audit",
            get(handlers::list_rate_limit_config_audit),
        )
        .route(
            "/api/v1/rate-limits/configs/:id",
            get(handlers::get_rate_limit_config)
                .put(handlers::update_rate_limit_config)
                .delete(handlers::delete_rate_limit_config),
        )
        .route(
            "/api/v1/webhooks/endpoints",
            get(handlers::list_webhook_endpoints).post(handlers::create_webhook_endpoint),
        )
        .route(
            "/api/v1/webhooks/endpoints/:id",
            delete(handlers::delete_webhook_endpoint),
        )
        .route(
            "/api/v1/webhooks/endpoints/:id/test",
            post(handlers::test_webhook_endpoint),
        )
        .route(
            "/api/v1/webhooks/dead-letter",
            get(handlers::list_dead_letters),
        )
        .route(
            "/api/v1/webhooks/dead-letter/:id/retry",
            post(handlers::retry_dead_letter),
        )
        // ── GraphQL endpoint with subscription support (#242) ─────────
        .route(
            "/api/graphql",
            get(async_graphql_axum::GraphQL::new(graphql_schema.clone()))
                .post(async_graphql_axum::GraphQL::new(graphql_schema.clone())),
        )
        .route(
            "/api/graphql/ws",
            get(async_graphql_axum::GraphQLSubscription::new(
                graphql_schema.clone(),
            )),
        )
        .layer(axum_mw::from_fn_with_state(
            state.clone(),
            crate::api::middleware::tenant_rate_limit_layer,
        ))
        .layer(axum_mw::from_fn_with_state(
            state.clone(),
            crate::api::middleware::service_rate_limit_layer,
        ))
        .layer(axum_mw::from_fn_with_state(
            state.clone(),
            crate::api::middleware::rate_limit_layer,
        ))
        .layer(axum_mw::from_fn(
            crate::gateway::telemetry::tracing_middleware,
        ))
        .layer(axum_mw::from_fn(
            crate::api::middleware::slo_monitoring_layer,
        ))
        .layer(axum_mw::from_fn(
            crate::api::middleware::correlation_id_layer,
        ))
        .layer(cors)
        .with_state(state);

    Ok(app)
}