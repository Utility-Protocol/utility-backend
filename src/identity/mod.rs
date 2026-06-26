pub mod attestation;
pub mod cert_store;
pub mod signer;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("Attestation failed: {0}")]
    AttestationFailed(String),
    #[error("Certificate error: {0}")]
    CertificateError(String),
    #[error("Signature verification failed")]
    InvalidSignature,
    #[error("Identity revoked")]
    Revoked,
    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, IdentityError>;
