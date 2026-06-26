use std::time::{Duration, SystemTime};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use x509_cert::time::{Time, Validity};
use p256::ecdsa::VerifyingKey;

use crate::transport::tcp::connection_manager::MeterId;
use super::{IdentityError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttestationReport {
    pub meter_id: MeterId,
    pub tcb_level: String,
}

pub struct IdentityConfig {
    pub attestation_enabled: bool,
    pub crl_refresh_interval_s: u64,
    pub max_attestation_time_ms: u64,
}

pub struct AttestationVerifier {
    pub config: IdentityConfig,
}

impl AttestationVerifier {
    pub fn new(config: IdentityConfig) -> Self {
        Self { config }
    }

    pub async fn verify_quote(
        &self,
        quote: &[u8],
        meter_id: MeterId,
        nonce: &[u8; 32],
    ) -> Result<AttestationReport> {
        if !self.config.attestation_enabled {
            return Ok(AttestationReport {
                meter_id,
                tcb_level: "UpToDate".to_string(),
            });
        }

        // In a real implementation, we would use dcap-qvl to verify the quote.

        let tcb_level = "SWHardeningNeeded".to_string();

        let mut hasher = Sha256::new();
        hasher.update(meter_id.as_bytes());
        hasher.update(nonce);
        let expected_report_data = hasher.finalize();

        if quote.len() < 32 {
             return Err(IdentityError::AttestationFailed("Quote too short".into()));
        }
        let report_data = &quote[quote.len() - 32..];
        if report_data != expected_report_data.as_slice() {
            return Err(IdentityError::AttestationFailed("Report data mismatch".into()));
        }

        Ok(AttestationReport {
            meter_id,
            tcb_level,
        })
    }

    pub fn certify_key(
        &self,
        _meter_id: MeterId,
        public_key_bytes: &[u8],
        _enclave_signature: &[u8],
    ) -> Result<Vec<u8>> {
        let _public_key = VerifyingKey::from_sec1_bytes(public_key_bytes)
            .map_err(|e| IdentityError::CertificateError(format!("Invalid public key: {}", e)))?;

        let now = SystemTime::now();
        let seven_days = Duration::from_secs(7 * 24 * 60 * 60);
        let expiry = now + seven_days;

        let not_before = Time::try_from(now)
            .map_err(|e| IdentityError::CertificateError(format!("Invalid start time: {}", e)))?;
        let not_after = Time::try_from(expiry)
            .map_err(|e| IdentityError::CertificateError(format!("Invalid expiry time: {}", e)))?;

        let _validity = Validity {
            not_before,
            not_after,
        };

        Ok(public_key_bytes.to_vec())
    }
}
