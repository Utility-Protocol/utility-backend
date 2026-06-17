use axum::{extract::Path, http::StatusCode, Json};
use ed25519_dalek::VerifyingKey;
use hex;
use serde::{Deserialize, Serialize};

use crate::gateway::crypto::global_registry;
use crate::time_series::analytics::{global_engine, DiagnosticReport};
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

pub async fn settle_account(Json(_body): Json<SettlementRequest>) -> Json<&'static str> {
    Json("settlement initiated")
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

pub async fn metrics_handler() -> &'static str {
    use prometheus::TextEncoder;
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = String::new();
    encoder.encode_utf8(&metric_families, &mut buffer).unwrap();
    Box::leak(buffer.into_boxed_str())
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
