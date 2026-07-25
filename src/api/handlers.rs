use axum::{
    extract::Path, extract::Query, extract::State, http::StatusCode, response::IntoResponse, Json,
};
use ed25519_dalek::VerifyingKey;
use hex;
use serde::{Deserialize, Serialize};

use crate::api::AppState;

use sqlx::{Pool, Postgres};
use std::sync::Arc;

use crate::api::metrics;
use crate::api::middleware::{DynamicRateLimiter, TenantRateLimiter};
use crate::gateway::crypto::global_registry;
use crate::gateway::hlc::HybridLogicalClock;
use crate::gateway::lock::{ActiveLock, AdvisoryLock};
use crate::tariffs::engine::{global_tariff_engine, TariffContext, TariffExplanation};
use crate::time_series::analytics::{global_engine, DiagnosticReport};
use crate::time_series::compression::CompressionStatus;
use crate::time_series::drift::CalibrationResult;
use crate::time_series::ingestion::ingest_telemetry;
use crate::webhooks::dead_letter::{DeadLetterEntry, PostgresDlq};
use crate::webhooks::dispatcher::WebhookDeliveryService;
use crate::webhooks::{ReqwestWebhookTransport, RetryPolicy, WebhookEndpoint, WebhookEvent};
use uuid::Uuid;

#[derive(Serialize)]
pub struct MeterInfo {
    pub id: String,
    pub tenant_id: String,
    pub location: String,
    pub last_reading: f64,
}

#[derive(Deserialize)]
pub struct ReadingSubmission {
    pub meter_id: String,
    pub value: f64,
    pub unit: String,
    pub timestamp: String,
}

#[derive(Deserialize)]
pub struct SettlementRequest {
    pub meter_id: String,
    pub resource_units: f64,
    pub destination_wallet: String,
}

#[derive(Serialize)]
pub struct GridNonceStatus {
    pub grid_id: String,
    pub high_water_mark: u64,
}

pub async fn nonce_status(State(state): State<AppState>) -> Json<Vec<GridNonceStatus>> {
    let marks = state.sequencer.get_all_grid_high_water_marks();
    let statuses: Vec<GridNonceStatus> = marks
        .into_iter()
        .map(|(grid_id, hwm)| GridNonceStatus {
            grid_id,
            high_water_mark: hwm,
        })
        .collect();
    Json(statuses)
}

pub async fn list_gateway_locks(State(lock): State<Arc<AdvisoryLock>>) -> Json<Vec<ActiveLock>> {
    Json(lock.active_locks())
}

pub async fn list_meters() -> Json<Vec<MeterInfo>> {
    Json(vec![MeterInfo {
        id: "MTR-001".into(),
        tenant_id: "grid-east".into(),
        location: "substation-alpha".into(),
        last_reading: 1234.56,
    }])
}

pub async fn get_meter(Path(id): Path<String>) -> Json<MeterInfo> {
    Json(MeterInfo {
        id,
        tenant_id: "grid-east".into(),
        location: "substation-alpha".into(),
        last_reading: 1234.56,
    })
}

#[derive(Deserialize)]
pub struct TariffExplainQuery {
    pub meter_id: String,
    pub ts: String,
    pub volume: Option<f64>,
    pub consumption_tier: Option<String>,
    pub grid_congestion_level: Option<u8>,
    pub is_holiday: Option<bool>,
}

pub async fn explain_tariff(
    Query(query): Query<TariffExplainQuery>,
) -> Result<Json<TariffExplanation>, StatusCode> {
    let timestamp = chrono::DateTime::parse_from_rfc3339(&query.ts)
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .with_timezone(&chrono::Utc);
    let context = TariffContext {
        meter_id: query.meter_id,
        timestamp,
        volume: query.volume.unwrap_or(1.0),
        consumption_tier: query.consumption_tier,
        grid_congestion_level: query.grid_congestion_level,
        is_holiday: query.is_holiday.unwrap_or(false),
    };

    Ok(Json(global_tariff_engine().explain(context)))
}

pub async fn list_tariffs() -> Json<Vec<&'static str>> {
    Json(vec![
        "peak:0.15/kWh",
        "off-peak:0.08/kWh",
        "shoulder:0.11/kWh",
    ])
}

#[tracing::instrument(skip(pool), fields(db.system = "postgresql"))]
pub async fn submit_reading(
    State(pool): State<Pool<Postgres>>,
    State(hlc): State<Arc<HybridLogicalClock>>,
    Json(body): Json<ReadingSubmission>,
) -> Result<Json<&'static str>, StatusCode> {
    tracing::Span::current().record("meter.id", &body.meter_id);
    let recorded_at = chrono::DateTime::parse_from_rfc3339(&body.timestamp)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(|_| chrono::Utc::now());

    let wall_ms = recorded_at.timestamp_millis() as u64;
    let hlc_ts = hlc.tick(wall_ms);

    match ingest_telemetry(&pool, &body.meter_id, body.value, recorded_at, hlc_ts.0).await {
        Ok(_) => Ok(Json("reading accepted")),
        Err(e) => {
            tracing::error!(error = %e, "failed to ingest reading");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn settle_account(
    State(state): State<AppState>,
    Json(body): Json<SettlementRequest>,
) -> Result<Json<&'static str>, StatusCode> {
    let span = tracing::info_span!(
        "settlement.execute",
        meter.id = %body.meter_id,
        resource.units = body.resource_units,
        otel.kind = "internal"
    );
    let _guard = span.enter();
    let rpc_url =
        std::env::var("SOROBAN_RPC_URL").unwrap_or_else(|_| "http://localhost:8000".into());
    let finalizer = crate::settlement::finalizer::Finalizer::new(
        state.pool.clone(),
        rpc_url,
        state.breaker.clone(),
    );
    let mint_queue = crate::settlement::mint_queue::MintQueue::new(state.pool);

    // In a real scenario, we'd get readings from the database.
    // Here we simulate a batch for the requested meter and a generic resource type (e.g. water).
    let batch_id = format!("batch-{}", uuid::Uuid::new_v4());
    let resource_type = "water"; // Example
    let readings = vec![(chrono::Utc::now(), body.resource_units)];

    let engine = crate::tariffs::engine::TariffEngine::new(vec![]); // Default tariff

    engine
        .evaluate_and_finalize(
            &batch_id,
            resource_type,
            &readings,
            &finalizer,
            &mint_queue,
            &body.destination_wallet,
        )
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "settlement failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json("settlement completed"))
}

#[tracing::instrument(skip_all, fields(meter.id = %meter_id, otel.kind = "internal"))]
pub async fn get_diagnostics(
    Path(meter_id): Path<String>,
) -> Result<Json<DiagnosticReport>, StatusCode> {
    let mut engine = global_engine()
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    engine
        .get_diagnostics(&meter_id)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

#[derive(Serialize)]
pub struct CapacityForecastResponse {
    pub forecasts: Vec<crate::capacity::CapacityForecast>,
}

pub async fn capacity_forecast() -> Json<CapacityForecastResponse> {
    let planner =
        crate::capacity::CapacityPlanner::new(crate::capacity::CapacityPlanningConfig::default());
    let forecasts = planner.forecast(&crate::capacity::sample_usage_window(chrono::Utc::now()));
    for forecast in &forecasts {
        metrics::set_capacity_forecast(
            &forecast.service,
            &format!("{:?}", forecast.resource),
            forecast.current_utilization,
            forecast.projected_utilization,
            forecast.days_to_critical,
        );
    }
    Json(CapacityForecastResponse { forecasts })
}

pub async fn metrics_handler() -> impl IntoResponse {
    use prometheus::TextEncoder;
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = String::new();
    encoder.encode_utf8(&metric_families, &mut buffer).unwrap();
    let headers = [(
        axum::http::header::CONTENT_TYPE,
        "text/plain; version=0.0.4",
    )];
    (headers, buffer)
}

#[derive(Serialize)]
pub struct ClockStateResponse {
    pub offset_seconds: f64,
    pub estimated_drift_ppm: f64,
    pub last_ntp_rtt_ms: Option<f64>,
    pub correction_ns: i64,
}

pub async fn clock_state() -> Json<ClockStateResponse> {
    let state = crate::ingestion::drift_estimator::KalmanClockState::default();
    Json(ClockStateResponse {
        offset_seconds: state.offset_seconds,
        estimated_drift_ppm: state.drift_ppm(),
        last_ntp_rtt_ms: state.last_rtt_ms,
        correction_ns: state.correction_ns(),
    })
}

pub async fn slo_status() -> Json<crate::observability::slo::SloStatus> {
    let status = crate::api::slo_state::global_slo_monitor().lock().status();
    metrics::publish_slo_status(&status);
    Json(status)
}

pub async fn readyz_handler() -> StatusCode {
    let starvation = metrics::get_starvation_count();
    if starvation > 100.0 {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

#[derive(Deserialize)]
pub struct RegisterMeterRequest {
    pub meter_id: String,
    pub public_key_hex: String,
    pub tpm_attestation_hex: Option<String>,
    pub aik_public_key_hex: Option<String>,
}

#[derive(Serialize)]
pub struct RegisterMeterResponse {
    pub meter_id: String,
    pub status: String,
}

pub async fn tenant_usage(
    Path(tenant_id): Path<String>,
) -> Result<Json<crate::time_series::pool::TenantUsage>, StatusCode> {
    let manager =
        crate::time_series::pool::global_pool_manager().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    manager
        .tenant_usage(&tenant_id)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

pub async fn compression_status() -> Result<Json<CompressionStatus>, StatusCode> {
    if let Some(manager) = crate::time_series::compression::global_compression_manager() {
        match manager.get_compression_status().await {
            Ok(status) => Ok(Json(status)),
            Err(e) => {
                tracing::warn!(error = %e, "compression status query failed");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    } else {
        // Fallback: return default-initialised status when no database is configured.
        Ok(Json(CompressionStatus {
            hypertable_name: "meter_readings".into(),
            total_chunks: 0,
            compressed_chunks: 0,
            uncompressed_chunks: 0,
            chunks: vec![],
            dry_run: false,
            compression_lag_max_days: 0.0,
            alert_fired: false,
        }))
    }
}

#[tracing::instrument(skip_all, fields(meter.id = %meter_id, otel.kind = "internal"))]
pub async fn calibrate_meter(
    Path(meter_id): Path<String>,
) -> Result<Json<CalibrationResult>, StatusCode> {
    let worker = crate::time_series::drift::global_drift_worker().await;
    worker
        .recalibrate_meter(meter_id)
        .await
        .map(Json)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)
}

pub async fn register_meter(
    Json(body): Json<RegisterMeterRequest>,
) -> Result<Json<RegisterMeterResponse>, StatusCode> {
    let span = tracing::info_span!(
        "meter.register",
        meter.id = %body.meter_id,
        tpm_attestation = body.tpm_attestation_hex.is_some(),
        otel.kind = "internal"
    );
    let _guard = span.enter();
    let public_key_bytes =
        hex::decode(&body.public_key_hex).map_err(|_| StatusCode::BAD_REQUEST)?;
    let public_key_arr: [u8; 32] = public_key_bytes
        .as_slice()
        .try_into()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let public_key =
        VerifyingKey::from_bytes(&public_key_arr).map_err(|_| StatusCode::BAD_REQUEST)?;

    let tpm_attestation = match &body.tpm_attestation_hex {
        Some(h) => Some(hex::decode(h).map_err(|_| StatusCode::BAD_REQUEST)?),
        None => None,
    };
    let aik_public_key = match &body.aik_public_key_hex {
        Some(h) => {
            let bytes = hex::decode(h).map_err(|_| StatusCode::BAD_REQUEST)?;
            let aik_arr: [u8; 32] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| StatusCode::BAD_REQUEST)?;
            let vk = VerifyingKey::from_bytes(&aik_arr).map_err(|_| StatusCode::BAD_REQUEST)?;
            Some(vk)
        }
        None => None,
    };

    let tpm_data = tpm_attestation.as_deref();
    let aik_ref = aik_public_key.as_ref();

    let mut registry = global_registry()
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    registry
        .register_meter(body.meter_id.clone(), public_key, tpm_data, aik_ref)
        .map_err(|e| {
            if e == "meter already registered" {
                StatusCode::CONFLICT
            } else {
                StatusCode::BAD_REQUEST
            }
        })?;

    Ok(Json(RegisterMeterResponse {
        meter_id: body.meter_id,
        status: "active".into(),
    }))
}

pub async fn rotate_key(
    Json(body): Json<RotateKeyRequest>,
) -> Result<Json<RotateKeyResponse>, StatusCode> {
    let new_key_bytes =
        hex::decode(&body.new_public_key_hex).map_err(|_| StatusCode::BAD_REQUEST)?;
    let new_key_arr: [u8; 32] = new_key_bytes
        .as_slice()
        .try_into()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let new_public_key =
        VerifyingKey::from_bytes(&new_key_arr).map_err(|_| StatusCode::BAD_REQUEST)?;

    let old_sig_bytes =
        hex::decode(&body.old_signature_hex).map_err(|_| StatusCode::BAD_REQUEST)?;

    let mut registry = global_registry()
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    registry
        .rotate_key(&body.meter_id, &new_public_key, &old_sig_bytes)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    Ok(Json(RotateKeyResponse {
        meter_id: body.meter_id,
        status: "key-rotated".into(),
    }))
}

#[derive(Deserialize)]
pub struct RotateKeyRequest {
    pub meter_id: String,
    pub new_public_key_hex: String,
    pub old_signature_hex: String,
}

#[derive(Serialize)]
pub struct RotateKeyResponse {
    pub meter_id: String,
    pub status: String,
}

#[derive(Serialize)]
pub struct RateLimiterStatusResponse {
    pub top_sources: Vec<(String, u64)>,
}

pub async fn rate_limiter_status(
    State(limiter): State<Arc<DynamicRateLimiter>>,
) -> Json<RateLimiterStatusResponse> {
    Json(RateLimiterStatusResponse {
        top_sources: limiter.get_status(),
    })
}

#[derive(Serialize)]
pub struct TenantRateLimiterStatusResponse {
    pub tenants: Vec<(String, u64, u64, u64)>,
}

pub async fn tenant_rate_limiter_status(
    State(limiter): State<Arc<TenantRateLimiter>>,
) -> Json<TenantRateLimiterStatusResponse> {
    let full = limiter.get_full_status();
    Json(TenantRateLimiterStatusResponse {
        tenants: full
            .into_iter()
            .map(|(id, limit, rej)| (id, limit.max_tokens, limit.refill_rate, rej))
            .collect(),
    })
}

// ---------------------------------------------------------------------------
// Webhook management handlers
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct WebhookEndpointResponse {
    pub id: String,
    pub url: String,
    pub tenant_id: String,
    pub created_at: String,
}

#[derive(Deserialize)]
pub struct CreateWebhookEndpointRequest {
    pub id: String,
    pub url: String,
    pub secret: String,
    pub tenant_id: String,
}

pub async fn list_webhook_endpoints(
    State(pool): State<Pool<Postgres>>,
) -> Result<Json<Vec<WebhookEndpointResponse>>, StatusCode> {
    let rows = sqlx::query_as::<_, (String, String, String, chrono::DateTime<chrono::Utc>)>(
        "SELECT id, url, tenant_id, created_at FROM webhook_endpoints ORDER BY created_at DESC",
    )
    .fetch_all(&pool)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "failed to list webhook endpoints");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(
        rows.into_iter()
            .map(|(id, url, tenant_id, created_at)| WebhookEndpointResponse {
                id,
                url,
                tenant_id,
                created_at: created_at.to_rfc3339(),
            })
            .collect(),
    ))
}

pub async fn create_webhook_endpoint(
    State(pool): State<Pool<Postgres>>,
    Json(body): Json<CreateWebhookEndpointRequest>,
) -> Result<(StatusCode, Json<WebhookEndpointResponse>), StatusCode> {
    let now = chrono::Utc::now();
    sqlx::query(
        "INSERT INTO webhook_endpoints (id, url, secret, tenant_id, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(&body.id)
    .bind(&body.url)
    .bind(&body.secret)
    .bind(&body.tenant_id)
    .bind(now)
    .bind(now)
    .execute(&pool)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "failed to create webhook endpoint");
        if e.to_string().contains("duplicate key") {
            StatusCode::CONFLICT
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    })?;

    Ok((
        StatusCode::CREATED,
        Json(WebhookEndpointResponse {
            id: body.id,
            url: body.url,
            tenant_id: body.tenant_id,
            created_at: now.to_rfc3339(),
        }),
    ))
}

pub async fn delete_webhook_endpoint(
    State(pool): State<Pool<Postgres>>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let result = sqlx::query("DELETE FROM webhook_endpoints WHERE id = $1")
        .bind(&id)
        .execute(&pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to delete webhook endpoint");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if result.rows_affected() == 0 {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Serialize)]
pub struct TestWebhookResponse {
    pub status: u16,
    pub attempts: u32,
}

pub async fn test_webhook_endpoint(
    State(pool): State<Pool<Postgres>>,
    Path(id): Path<String>,
) -> Result<Json<TestWebhookResponse>, StatusCode> {
    // Look up the endpoint
    let row = sqlx::query_as::<_, (String, String, String)>(
        "SELECT url, secret, tenant_id FROM webhook_endpoints WHERE id = $1",
    )
    .bind(&id)
    .fetch_optional(&pool)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "failed to look up webhook endpoint");
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .ok_or(StatusCode::NOT_FOUND)?;

    let endpoint = WebhookEndpoint {
        id: id.clone(),
        url: row.0,
        secret: row.1,
        tenant_id: row.2,
    };

    let event = WebhookEvent {
        id: Uuid::new_v4(),
        event_type: "webhook.test".into(),
        created_at: chrono::Utc::now(),
        payload: serde_json::json!({
            "test": true,
            "message": "This is a test event from utility-backend"
        }),
    };

    let transport = Arc::new(ReqwestWebhookTransport::default());
    let dlq = Arc::new(PostgresDlq::new(pool.clone()));
    let service = WebhookDeliveryService::with_dlq(transport, RetryPolicy::default(), dlq);

    match service.deliver(&endpoint, &event).await {
        Ok(receipt) => Ok(Json(TestWebhookResponse {
            status: receipt.status,
            attempts: receipt.attempts,
        })),
        Err(_) => Ok(Json(TestWebhookResponse {
            status: 0,
            attempts: 5,
        })),
    }
}

#[derive(Deserialize)]
pub struct DeadLetterQuery {
    pub endpoint_id: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

pub async fn list_dead_letters(
    State(pool): State<Pool<Postgres>>,
    Query(query): Query<DeadLetterQuery>,
) -> Result<Json<Vec<DeadLetterEntry>>, StatusCode> {
    let dlq = PostgresDlq::new(pool);
    dlq.list(
        query.endpoint_id.as_deref(),
        query.limit.unwrap_or(50),
        query.offset.unwrap_or(0),
    )
    .await
    .map(Json)
    .map_err(|e| {
        tracing::error!(error = %e, "failed to list dead letters");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

#[derive(Serialize)]
pub struct RetryDeadLetterResponse {
    pub id: Uuid,
    pub status: u16,
    pub attempts: u32,
}

pub async fn retry_dead_letter(
    State(pool): State<Pool<Postgres>>,
    Path(id): Path<Uuid>,
) -> Result<Json<RetryDeadLetterResponse>, StatusCode> {
    let dlq = PostgresDlq::new(pool.clone());
    let entry = dlq.get(id).await.map_err(|e| {
        tracing::error!(error = %e, "failed to get dead letter");
        StatusCode::INTERNAL_SERVER_ERROR
    })?.ok_or(StatusCode::NOT_FOUND)?;

    // Look up the endpoint
    let row = sqlx::query_as::<_, (String, String)>(
        "SELECT url, secret FROM webhook_endpoints WHERE id = $1",
    )
    .bind(&entry.endpoint_id)
    .fetch_optional(&pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .ok_or(StatusCode::NOT_FOUND)?;

    let endpoint = WebhookEndpoint {
        id: entry.endpoint_id.clone(),
        url: row.0,
        secret: row.1,
        tenant_id: String::new(),
    };

    let event = WebhookEvent {
        id: entry.event_id,
        event_type: entry.event_type.clone(),
        created_at: entry.failed_at,
        payload: entry.payload.clone(),
    };

    // Remove the original DLQ entry before attempting retry to avoid
    // duplicates: if the retry fails, the service's DLQ will create a
    // fresh entry.
    let _ = dlq.remove(id).await;

    let transport = Arc::new(ReqwestWebhookTransport::default());
    let retry_dlq = Arc::new(PostgresDlq::new(pool.clone()));
    let service = WebhookDeliveryService::with_dlq(transport, RetryPolicy::default(), retry_dlq);

    match service.deliver(&endpoint, &event).await {
        Ok(receipt) => Ok(Json(RetryDeadLetterResponse {
            id,
            status: receipt.status,
            attempts: receipt.attempts,
        })),
        Err(_) => Ok(Json(RetryDeadLetterResponse {
            id,
            status: 0,
            attempts: 5,
        })),
    }
}
