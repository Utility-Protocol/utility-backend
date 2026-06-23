use axum::{extract::Path, extract::State, http::StatusCode, response::IntoResponse, Json};
use ed25519_dalek::VerifyingKey;
use hex;
use serde::{Deserialize, Serialize};

use crate::api::AppState;

use crate::api::metrics;
use crate::gateway::crypto::global_registry;
use crate::time_series::analytics::{global_engine, DiagnosticReport};
use crate::time_series::compression::CompressionStatus;
use crate::time_series::drift::CalibrationResult;

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

pub async fn nonce_status(
    State(state): State<AppState>,
) -> Json<Vec<GridNonceStatus>> {
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

pub async fn list_tariffs() -> Json<Vec<&'static str>> {
    Json(vec![
        "peak:0.15/kWh",
        "off-peak:0.08/kWh",
        "shoulder:0.11/kWh",
    ])
}

pub async fn submit_reading(Json(_body): Json<ReadingSubmission>) -> Json<&'static str> {
    Json("reading accepted")
}

pub async fn settle_account(
    State(state): State<AppState>,
    Json(body): Json<SettlementRequest>,
) -> Result<Json<&'static str>, StatusCode> {
    let rpc_url = std::env::var("SOROBAN_RPC_URL").unwrap_or_else(|_| "http://localhost:8000".into());
    let finalizer = crate::settlement::finalizer::Finalizer::new(state.pool.clone(), rpc_url, state.breaker.clone());
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
