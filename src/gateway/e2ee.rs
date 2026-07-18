use crate::api::metrics;
use ring::{
    aead,
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use thiserror::Error;

const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;
const ENVELOPE_VERSION: u8 = 1;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FieldEncryptionError {
    #[error("encryption key must be 32 bytes")]
    InvalidKeyLength,
    #[error("failed to generate nonce")]
    NonceGeneration,
    #[error("failed to seal sensitive field")]
    Seal,
    #[error("failed to open sensitive field")]
    Open,
    #[error("encrypted field envelope is malformed")]
    MalformedEnvelope,
    #[error("encrypted field references an unknown key")]
    UnknownKey,
    #[error("encrypted field value is not valid json")]
    InvalidJson,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptedField {
    pub alg: String,
    pub version: u8,
    pub key_id: String,
    pub nonce_hex: String,
    pub ciphertext_hex: String,
}

#[derive(Debug, Clone)]
pub struct EncryptionKey {
    pub key_id: String,
    bytes: [u8; KEY_LEN],
}

impl EncryptionKey {
    pub fn new(key_id: impl Into<String>, bytes: [u8; KEY_LEN]) -> Self {
        Self {
            key_id: key_id.into(),
            bytes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FieldEncryptor {
    primary_key_id: String,
    keys: HashMap<String, EncryptionKey>,
    sensitive_fields: HashSet<String>,
}

impl FieldEncryptor {
    pub fn new(
        primary_key: EncryptionKey,
        sensitive_fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let primary_key_id = primary_key.key_id.clone();
        let mut keys = HashMap::new();
        keys.insert(primary_key_id.clone(), primary_key);
        Self {
            primary_key_id,
            keys,
            sensitive_fields: sensitive_fields.into_iter().map(Into::into).collect(),
        }
    }

    pub fn add_decryption_key(&mut self, key: EncryptionKey) {
        self.keys.insert(key.key_id.clone(), key);
    }

    pub fn encrypt_payload(&self, payload: &mut Value) -> Result<usize, FieldEncryptionError> {
        let started = Instant::now();
        let result = self.transform_payload(payload, true);
        metrics::record_e2ee_operation("encrypt", result.is_ok(), started.elapsed().as_secs_f64());
        result
    }

    pub fn decrypt_payload(&self, payload: &mut Value) -> Result<usize, FieldEncryptionError> {
        let started = Instant::now();
        let result = self.transform_payload(payload, false);
        metrics::record_e2ee_operation("decrypt", result.is_ok(), started.elapsed().as_secs_f64());
        result
    }

    fn transform_payload(
        &self,
        payload: &mut Value,
        encrypt: bool,
    ) -> Result<usize, FieldEncryptionError> {
        match payload {
            Value::Object(map) => self.transform_object(map, encrypt),
            _ => Ok(0),
        }
    }

    fn transform_object(
        &self,
        object: &mut Map<String, Value>,
        encrypt: bool,
    ) -> Result<usize, FieldEncryptionError> {
        let mut changed = 0;
        let keys: Vec<String> = object.keys().cloned().collect();
        for name in keys {
            if let Some(value) = object.get_mut(&name) {
                if self.sensitive_fields.contains(&name) {
                    if encrypt && !is_envelope(value) {
                        *value = serde_json::to_value(self.encrypt_value(value)?)
                            .map_err(|_| FieldEncryptionError::Seal)?;
                        changed += 1;
                    } else if !encrypt && is_envelope(value) {
                        *value = self.decrypt_value(value)?;
                        changed += 1;
                    }
                } else if let Value::Object(child) = value {
                    changed += self.transform_object(child, encrypt)?;
                } else if let Value::Array(items) = value {
                    for item in items {
                        changed += self.transform_payload(item, encrypt)?;
                    }
                }
            }
        }
        Ok(changed)
    }

    fn encrypt_value(&self, value: &Value) -> Result<EncryptedField, FieldEncryptionError> {
        let key = self
            .keys
            .get(&self.primary_key_id)
            .ok_or(FieldEncryptionError::UnknownKey)?;
        let unbound = aead::UnboundKey::new(&aead::AES_256_GCM, &key.bytes)
            .map_err(|_| FieldEncryptionError::InvalidKeyLength)?;
        let sealing_key = aead::LessSafeKey::new(unbound);
        let mut nonce = [0u8; NONCE_LEN];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| FieldEncryptionError::NonceGeneration)?;
        let mut in_out =
            serde_json::to_vec(value).map_err(|_| FieldEncryptionError::InvalidJson)?;
        sealing_key
            .seal_in_place_append_tag(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(key.key_id.as_bytes()),
                &mut in_out,
            )
            .map_err(|_| FieldEncryptionError::Seal)?;
        Ok(EncryptedField {
            alg: "AES-256-GCM".into(),
            version: ENVELOPE_VERSION,
            key_id: key.key_id.clone(),
            nonce_hex: hex::encode(nonce),
            ciphertext_hex: hex::encode(in_out),
        })
    }

    fn decrypt_value(&self, value: &Value) -> Result<Value, FieldEncryptionError> {
        let envelope: EncryptedField = serde_json::from_value(value.clone())
            .map_err(|_| FieldEncryptionError::MalformedEnvelope)?;
        if envelope.alg != "AES-256-GCM" || envelope.version != ENVELOPE_VERSION {
            return Err(FieldEncryptionError::MalformedEnvelope);
        }
        let key = self
            .keys
            .get(&envelope.key_id)
            .ok_or(FieldEncryptionError::UnknownKey)?;
        let nonce_vec = hex::decode(&envelope.nonce_hex)
            .map_err(|_| FieldEncryptionError::MalformedEnvelope)?;
        let nonce: [u8; NONCE_LEN] = nonce_vec
            .as_slice()
            .try_into()
            .map_err(|_| FieldEncryptionError::MalformedEnvelope)?;
        let mut ciphertext = hex::decode(&envelope.ciphertext_hex)
            .map_err(|_| FieldEncryptionError::MalformedEnvelope)?;
        let unbound = aead::UnboundKey::new(&aead::AES_256_GCM, &key.bytes)
            .map_err(|_| FieldEncryptionError::InvalidKeyLength)?;
        let opening_key = aead::LessSafeKey::new(unbound);
        let plaintext = opening_key
            .open_in_place(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(key.key_id.as_bytes()),
                &mut ciphertext,
            )
            .map_err(|_| FieldEncryptionError::Open)?;
        serde_json::from_slice(plaintext).map_err(|_| FieldEncryptionError::InvalidJson)
    }
}

fn is_envelope(value: &Value) -> bool {
    value.get("alg").and_then(Value::as_str) == Some("AES-256-GCM")
        && value.get("ciphertext_hex").is_some()
        && value.get("nonce_hex").is_some()
        && value.get("key_id").is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn encryptor() -> FieldEncryptor {
        FieldEncryptor::new(
            EncryptionKey::new("k1", [7u8; 32]),
            ["ssn", "account_number", "destination_wallet"],
        )
    }

    #[test]
    fn encrypts_and_decrypts_only_sensitive_fields() {
        let enc = encryptor();
        let mut payload = json!({"name":"Ada","ssn":"123-45-6789","nested":{"destination_wallet":"GABC","public":"ok"}});
        assert_eq!(enc.encrypt_payload(&mut payload).unwrap(), 2);
        assert_eq!(payload["name"], "Ada");
        assert!(payload["ssn"]["ciphertext_hex"].as_str().unwrap().len() > 32);
        assert_ne!(payload["nested"]["destination_wallet"], "GABC");
        assert_eq!(enc.decrypt_payload(&mut payload).unwrap(), 2);
        assert_eq!(payload["ssn"], "123-45-6789");
        assert_eq!(payload["nested"]["destination_wallet"], "GABC");
    }

    #[test]
    fn encryption_is_not_deterministic() {
        let enc = encryptor();
        let mut first = json!({"account_number":"A-1"});
        let mut second = first.clone();
        enc.encrypt_payload(&mut first).unwrap();
        enc.encrypt_payload(&mut second).unwrap();
        assert_ne!(
            first["account_number"]["nonce_hex"],
            second["account_number"]["nonce_hex"]
        );
        assert_ne!(
            first["account_number"]["ciphertext_hex"],
            second["account_number"]["ciphertext_hex"]
        );
    }

    #[test]
    fn rejects_unknown_key() {
        let enc = encryptor();
        let mut payload = json!({"ssn":"123"});
        enc.encrypt_payload(&mut payload).unwrap();
        payload["ssn"]["key_id"] = json!("missing");
        assert_eq!(
            enc.decrypt_payload(&mut payload),
            Err(FieldEncryptionError::UnknownKey)
        );
    }
}
