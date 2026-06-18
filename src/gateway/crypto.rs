use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeterStatus {
    Active,
    Revoked,
    Pending,
}

#[derive(Debug, Clone)]
pub struct MeterIdentity {
    pub meter_id: String,
    pub public_key: VerifyingKey,
    pub status: MeterStatus,
    pub enrolled_at: u64,
    pub key_rotated_at: u64,
}

#[derive(Debug, Clone)]
pub struct AuthAuditEntry {
    pub meter_id: String,
    pub timestamp: u64,
    pub reason: String,
    pub source_ip: Option<String>,
}

pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: u32,
}

impl BloomFilter {
    pub fn new(num_items: usize, false_positive_rate: f64) -> Self {
        let num_bits = Self::optimal_bits(num_items, false_positive_rate);
        let num_hashes = Self::optimal_hashes(num_bits, num_items);
        Self {
            bits: vec![0u64; num_bits.div_ceil(64)],
            num_bits,
            num_hashes,
        }
    }

    fn optimal_bits(n: usize, p: f64) -> usize {
        let n = n.max(1) as f64;
        let bits = -(n * p.ln()) / (2.0_f64.ln().powi(2));
        bits.ceil() as usize
    }

    fn optimal_hashes(bits: usize, n: usize) -> u32 {
        let bits = bits as f64;
        let n = n.max(1) as f64;
        ((bits / n) * 2.0_f64.ln()).ceil() as u32
    }

    fn hash_indices(&self, data: &[u8]) -> Vec<usize> {
        let hash = Sha256::digest(data);
        let h1 = u64::from_le_bytes(hash[0..8].try_into().unwrap());
        let h2 = u64::from_le_bytes(hash[8..16].try_into().unwrap());

        (0..self.num_hashes)
            .map(|i| h1.wrapping_add((i as u64).wrapping_mul(h2)) as usize % self.num_bits)
            .collect()
    }

    pub fn insert(&mut self, data: &[u8]) {
        for idx in self.hash_indices(data) {
            self.bits[idx / 64] |= 1u64 << (idx % 64);
        }
    }

    pub fn contains(&self, data: &[u8]) -> bool {
        self.hash_indices(data)
            .iter()
            .all(|&idx| (self.bits[idx / 64] & (1u64 << (idx % 64))) != 0)
    }
}

pub struct MeterRegistry {
    meters: HashMap<String, MeterIdentity>,
    crl: BloomFilter,
}

impl Default for MeterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MeterRegistry {
    pub fn new() -> Self {
        Self {
            meters: HashMap::new(),
            crl: BloomFilter::new(1_000_000, 0.01),
        }
    }

    pub fn register_meter(
        &mut self,
        meter_id: String,
        public_key: VerifyingKey,
        tpm_attestation: Option<&[u8]>,
        aik_public_key: Option<&VerifyingKey>,
    ) -> Result<(), &'static str> {
        if self.meters.contains_key(&meter_id) {
            return Err("meter already registered");
        }

        if let (Some(attestation), Some(aik)) = (tpm_attestation, aik_public_key) {
            if !verify_tpm_attestation(aik, attestation) {
                return Err("TPM attestation verification failed");
            }
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let identity = MeterIdentity {
            meter_id: meter_id.clone(),
            public_key,
            status: MeterStatus::Active,
            enrolled_at: now,
            key_rotated_at: now,
        };

        self.meters.insert(meter_id.clone(), identity);
        info!(meter_id = %meter_id, "meter registered successfully");
        Ok(())
    }

    pub fn verify_packet(
        &self,
        meter_id: &str,
        payload: &[u8],
        signature: &[u8],
        source_ip: Option<String>,
    ) -> Result<(), &'static str> {
        let identity = self.meters.get(meter_id).ok_or("unknown meter")?;

        if identity.status == MeterStatus::Revoked {
            Self::log_auth_failure(meter_id, "revoked meter", source_ip);
            return Err("meter is revoked");
        }

        if self.crl.contains(meter_id.as_bytes()) {
            Self::log_auth_failure(meter_id, "meter in CRL", source_ip);
            return Err("meter certificate revoked");
        }

        let sig = Signature::from_slice(signature).map_err(|_| "invalid signature format")?;
        identity.public_key.verify(payload, &sig).map_err(|_| {
            Self::log_auth_failure(meter_id, "signature mismatch", source_ip);
            "cryptographic signature mismatch"
        })
    }

    pub fn rotate_key(
        &mut self,
        meter_id: &str,
        new_public_key: &VerifyingKey,
        old_signature: &[u8],
    ) -> Result<(), &'static str> {
        let identity = self.meters.get(meter_id).ok_or("unknown meter")?;

        if identity.status != MeterStatus::Active {
            return Err("meter is not active");
        }

        let new_key_bytes = new_public_key.to_bytes();
        let sig = Signature::from_slice(old_signature).map_err(|_| "invalid signature format")?;
        identity
            .public_key
            .verify(&new_key_bytes, &sig)
            .map_err(|_| "key rotation signature verification failed")?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if let Some(identity) = self.meters.get_mut(meter_id) {
            identity.public_key = *new_public_key;
            identity.key_rotated_at = now;
        }

        info!(meter_id = %meter_id, "key rotation successful");
        Ok(())
    }

    pub fn revoke_meter(&mut self, meter_id: &str) -> Result<(), &'static str> {
        let identity = self.meters.get_mut(meter_id).ok_or("unknown meter")?;
        identity.status = MeterStatus::Revoked;
        self.crl.insert(meter_id.as_bytes());
        info!(meter_id = %meter_id, "meter revoked");
        Ok(())
    }

    pub fn get_meter(&self, meter_id: &str) -> Option<&MeterIdentity> {
        self.meters.get(meter_id)
    }

    pub fn meter_count(&self) -> usize {
        self.meters.len()
    }

    fn log_auth_failure(meter_id: &str, reason: &str, _source_ip: Option<String>) {
        warn!(
            meter_id = %meter_id,
            reason = %reason,
            "authentication failure"
        );
    }
}

pub fn verify_tpm_attestation(aik_public_key: &VerifyingKey, attestation_data: &[u8]) -> bool {
    if attestation_data.len() < 64 {
        return false;
    }

    let (sig_bytes, signed_data) = attestation_data.split_at(64);
    let sig = match Signature::from_slice(sig_bytes) {
        Ok(s) => s,
        Err(_) => return false,
    };

    aik_public_key.verify(signed_data, &sig).is_ok()
}

pub fn verify_packet(
    identity: &MeterIdentity,
    payload: &[u8],
    signature: &[u8],
) -> Result<(), &'static str> {
    let sig = Signature::from_slice(signature).map_err(|_| "invalid signature format")?;
    identity
        .public_key
        .verify(payload, &sig)
        .map_err(|_| "cryptographic signature mismatch")
}

lazy_static::lazy_static! {
    static ref GLOBAL_REGISTRY: std::sync::Mutex<MeterRegistry> =
        std::sync::Mutex::new(MeterRegistry::new());
}

pub fn global_registry() -> &'static std::sync::Mutex<MeterRegistry> {
    &GLOBAL_REGISTRY
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;
    use rand::Rng;
    use rand::SeedableRng;

    fn make_keypair() -> (SigningKey, VerifyingKey) {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let verifying_key = signing_key.verifying_key();
        (signing_key, verifying_key)
    }

    #[test]
    fn test_sign_verify_legacy() {
        let (signing_key, verifying_key) = make_keypair();
        let identity = MeterIdentity {
            meter_id: "MTR-007".into(),
            public_key: verifying_key,
            status: MeterStatus::Active,
            enrolled_at: 1000,
            key_rotated_at: 1000,
        };
        let payload = b"voltage:240.1;current:15.3";
        let signature = signing_key.sign(payload);
        assert!(verify_packet(&identity, payload, &signature.to_bytes()).is_ok());
    }

    #[test]
    fn test_register_and_verify() {
        let (signing_key, verifying_key) = make_keypair();
        let mut registry = MeterRegistry::new();

        registry
            .register_meter("MTR-001".into(), verifying_key, None, None)
            .unwrap();

        let payload = b"flow:12.5";
        let sig = signing_key.sign(payload);
        assert!(registry
            .verify_packet("MTR-001", payload, &sig.to_bytes(), None)
            .is_ok());
    }

    #[test]
    fn test_verify_unknown_meter_fails() {
        let (_signing_key, verifying_key) = make_keypair();
        let (signing_key2, _verifying_key2) = make_keypair();
        let mut registry = MeterRegistry::new();
        registry
            .register_meter("MTR-001".into(), verifying_key, None, None)
            .unwrap();

        let payload = b"test";
        let sig = signing_key2.sign(payload);
        let result = registry.verify_packet("MTR-999", payload, &sig.to_bytes(), None);
        assert_eq!(result, Err("unknown meter"));
    }

    #[test]
    fn test_verify_wrong_key_fails() {
        let (_signing_key1, verifying_key1) = make_keypair();
        let (signing_key2, _verifying_key2) = make_keypair();
        let mut registry = MeterRegistry::new();
        registry
            .register_meter("MTR-001".into(), verifying_key1, None, None)
            .unwrap();

        let payload = b"test";
        let sig = signing_key2.sign(payload);
        let result = registry.verify_packet("MTR-001", payload, &sig.to_bytes(), None);
        assert_eq!(result, Err("cryptographic signature mismatch"));
    }

    #[test]
    fn test_revoke_meter() {
        let (signing_key, verifying_key) = make_keypair();
        let mut registry = MeterRegistry::new();
        registry
            .register_meter("MTR-001".into(), verifying_key, None, None)
            .unwrap();

        registry.revoke_meter("MTR-001").unwrap();

        let payload = b"test";
        let sig = signing_key.sign(payload);
        let result = registry.verify_packet("MTR-001", payload, &sig.to_bytes(), None);
        assert_eq!(result, Err("meter is revoked"));
    }

    #[test]
    fn test_key_rotation() {
        let (old_key, old_vk) = make_keypair();
        let (new_key, new_vk) = make_keypair();

        let mut registry = MeterRegistry::new();
        registry
            .register_meter("MTR-001".into(), old_vk, None, None)
            .unwrap();

        let new_key_bytes = new_vk.to_bytes();
        let rotation_sig = old_key.sign(&new_key_bytes);

        registry
            .rotate_key("MTR-001", &new_vk, &rotation_sig.to_bytes())
            .unwrap();

        let payload = b"after-rotation";
        let sig = new_key.sign(payload);
        assert!(registry
            .verify_packet("MTR-001", payload, &sig.to_bytes(), None)
            .is_ok());
    }

    #[test]
    fn test_key_rotation_rejects_invalid_signature() {
        let (_old_key, old_vk) = make_keypair();
        let (new_key, new_vk) = make_keypair();

        let mut registry = MeterRegistry::new();
        registry
            .register_meter("MTR-001".into(), old_vk, None, None)
            .unwrap();

        let wrong_key_bytes = new_vk.to_bytes();
        let bad_sig = new_key.sign(&wrong_key_bytes);

        let result = registry.rotate_key("MTR-001", &new_vk, &bad_sig.to_bytes());
        assert_eq!(result, Err("key rotation signature verification failed"));
    }

    #[test]
    fn test_duplicate_register_fails() {
        let (_, vk1) = make_keypair();
        let (_, vk2) = make_keypair();
        let mut registry = MeterRegistry::new();
        registry
            .register_meter("MTR-001".into(), vk1, None, None)
            .unwrap();
        let result = registry.register_meter("MTR-001".into(), vk2, None, None);
        assert_eq!(result, Err("meter already registered"));
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let mut bf = BloomFilter::new(1000, 0.01);
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let inserted: Vec<[u8; 16]> = (0..800).map(|_| rng.gen()).collect();
        let not_inserted: Vec<[u8; 16]> = (800..10800).map(|_| rng.gen()).collect();
        for item in &inserted {
            bf.insert(item);
        }
        let mut fp = 0usize;
        for item in &not_inserted {
            if bf.contains(item) {
                fp += 1;
            }
        }
        let rate = fp as f64 / not_inserted.len() as f64;
        assert!(rate < 0.05);
    }

    #[test]
    fn test_bloom_filter_contains_inserted() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert(b"meter-123");
        assert!(bf.contains(b"meter-123"));
    }

    #[test]
    fn test_bloom_filter_no_false_negative() {
        let mut bf = BloomFilter::new(100, 0.01);
        for i in 0..50 {
            bf.insert(format!("meter-{}", i).as_bytes());
        }
        for i in 0..50 {
            assert!(bf.contains(format!("meter-{}", i).as_bytes()));
        }
    }

    #[test]
    fn test_tpm_attestation_invalid_length() {
        let (_, vk) = make_keypair();
        assert!(!verify_tpm_attestation(&vk, &[0u8; 10]));
    }

    #[test]
    fn test_tpm_attestation_valid() {
        let (aik_sk, aik_vk) = make_keypair();
        let signed_data = b"pcr-values-and-nonce";
        let sig = aik_sk.sign(signed_data);
        let mut attestation = Vec::new();
        attestation.extend_from_slice(&sig.to_bytes());
        attestation.extend_from_slice(signed_data);

        assert!(verify_tpm_attestation(&aik_vk, &attestation));
    }

    #[test]
    fn test_tpm_attestation_invalid_signature() {
        let (aik_sk, _aik_vk) = make_keypair();
        let (_wrong_sk, wrong_vk) = make_keypair();
        let signed_data = b"pcr-values-and-nonce";
        let sig = aik_sk.sign(signed_data);
        let mut attestation = Vec::new();
        attestation.extend_from_slice(&sig.to_bytes());
        attestation.extend_from_slice(signed_data);

        assert!(!verify_tpm_attestation(&wrong_vk, &attestation));
    }

    #[test]
    fn test_register_with_tpm_attestation() {
        let (meter_sk, meter_vk) = make_keypair();
        let (aik_sk, aik_vk) = make_keypair();

        let attestation_data = b"enrollment-request-nonce";
        let sig = aik_sk.sign(attestation_data);
        let mut attestation = Vec::new();
        attestation.extend_from_slice(&sig.to_bytes());
        attestation.extend_from_slice(attestation_data);

        let mut registry = MeterRegistry::new();
        registry
            .register_meter(
                "MTR-TPM".into(),
                meter_vk,
                Some(&attestation),
                Some(&aik_vk),
            )
            .unwrap();

        let payload = b"test";
        let sig = meter_sk.sign(payload);
        assert!(registry
            .verify_packet("MTR-TPM", payload, &sig.to_bytes(), None)
            .is_ok());
    }

    #[test]
    fn test_register_with_bad_tpm_attestation() {
        let (_, meter_vk) = make_keypair();
        let (aik_sk, _aik_vk) = make_keypair();
        let (_wrong_sk, wrong_vk) = make_keypair();

        let attestation_data = b"enrollment-request-nonce";
        let sig = aik_sk.sign(attestation_data);
        let mut attestation = Vec::new();
        attestation.extend_from_slice(&sig.to_bytes());
        attestation.extend_from_slice(attestation_data);

        // Use wrong AIK
        let mut registry = MeterRegistry::new();
        let result = registry.register_meter(
            "MTR-TPM".into(),
            meter_vk,
            Some(&attestation),
            Some(&wrong_vk),
        );
        assert_eq!(result, Err("TPM attestation verification failed"));
    }

    #[test]
    fn test_meter_count() {
        let mut registry = MeterRegistry::new();
        assert_eq!(registry.meter_count(), 0);
        let (_, vk) = make_keypair();
        registry
            .register_meter("MTR-001".into(), vk, None, None)
            .unwrap();
        assert_eq!(registry.meter_count(), 1);
    }

    #[test]
    fn test_revoke_nonexistent_fails() {
        let mut registry = MeterRegistry::new();
        assert_eq!(registry.revoke_meter("ghost"), Err("unknown meter"));
    }

    #[test]
    fn test_global_registry_is_accessible() {
        let reg = global_registry();
        let guard = reg.lock().unwrap();
        assert_eq!(guard.meter_count(), 0);
    }

    #[test]
    fn test_bloom_filter_optimal_params() {
        let bf = BloomFilter::new(1_000_000, 0.01);
        assert!(bf.num_hashes >= 1);
        assert!(!bf.bits.is_empty());
    }
}
