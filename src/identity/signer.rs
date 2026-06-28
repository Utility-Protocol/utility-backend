use p256::ecdsa::{signature::Verifier, Signature, VerifyingKey};
use std::sync::Arc;

use super::{cert_store::CertStore, IdentityError, Result};
use crate::transport::tcp::connection_manager::MeterId;

pub struct MeterSigner {
    cert_store: Arc<CertStore>,
}

impl MeterSigner {
    pub fn new(cert_store: Arc<CertStore>) -> Self {
        Self { cert_store }
    }

    pub fn verify_signature(
        &self,
        meter_id: &MeterId,
        data: &[u8],
        signature_bytes: &[u8; 64],
    ) -> Result<()> {
        if self.cert_store.is_revoked(meter_id) {
            return Err(IdentityError::Revoked);
        }

        let public_key_bytes = self
            .cert_store
            .get_certificate(meter_id)
            .ok_or_else(|| IdentityError::CertificateError("Certificate not found".into()))?;

        let verifying_key = VerifyingKey::from_sec1_bytes(&public_key_bytes).map_err(|e| {
            IdentityError::CertificateError(format!("Invalid public key in cert: {}", e))
        })?;

        let signature =
            Signature::from_slice(signature_bytes).map_err(|_| IdentityError::InvalidSignature)?;

        verifying_key
            .verify(data, &signature)
            .map_err(|_| IdentityError::InvalidSignature)?;

        Ok(())
    }
}
