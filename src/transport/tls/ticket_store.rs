use crate::transport::tls::config::SessionTicketConfig;
use ring::{
    aead, hmac,
    rand::{SecureRandom, SystemRandom},
};
use rustls::server::ProducesTickets;
use std::{
    fmt, fs, io,
    path::PathBuf,
    sync::RwLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing::{debug, warn};

const KEY_LEN: usize = 64;
const AES_KEY_LEN: usize = 32;
const HMAC_KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;
const TS_LEN: usize = 8;
const OVERLAP: Duration = Duration::from_secs(60 * 60);
const TIMESTAMP_SKEW: u64 = 1;

#[derive(Clone)]
pub struct StoredKey {
    pub created_at: SystemTime,
    pub aes_key: [u8; AES_KEY_LEN],
    pub hmac_key: [u8; HMAC_KEY_LEN],
    path: Option<PathBuf>,
}

impl StoredKey {
    fn unix_ts(&self) -> u64 {
        self.created_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn zeroize(&mut self) {
        self.aes_key.fill(0);
        self.hmac_key.fill(0);
    }
}

impl fmt::Debug for StoredKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredKey")
            .field("created_at", &self.created_at)
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct SessionTicketStore {
    keys: RwLock<Vec<StoredKey>>,
    config: SessionTicketConfig,
}

impl SessionTicketStore {
    pub fn load_or_generate(config: &SessionTicketConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.key_directory)?;
        let mut keys = Self::load_keys(config)?;
        keys.sort_by_key(|k| std::cmp::Reverse(k.unix_ts()));
        keys.truncate(2);
        if keys.is_empty() {
            keys.push(Self::generate_and_persist_key(config)?);
        }
        Ok(Self {
            keys: RwLock::new(keys),
            config: config.clone(),
        })
    }

    fn load_keys(config: &SessionTicketConfig) -> io::Result<Vec<StoredKey>> {
        let mut keys = Vec::new();
        for entry in fs::read_dir(&config.key_directory)? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(ts) = name
                .strip_prefix("stk-")
                .and_then(|s| s.strip_suffix(".key"))
                .and_then(|s| s.parse::<u64>().ok())
            else {
                continue;
            };
            let data = fs::read(&path)?;
            if data.len() != KEY_LEN {
                warn!(path = %path.display(), len = data.len(), "ignoring invalid STK file");
                continue;
            }
            let mut aes_key = [0u8; AES_KEY_LEN];
            let mut hmac_key = [0u8; HMAC_KEY_LEN];
            aes_key.copy_from_slice(&data[..AES_KEY_LEN]);
            hmac_key.copy_from_slice(&data[AES_KEY_LEN..]);
            keys.push(StoredKey {
                created_at: UNIX_EPOCH + Duration::from_secs(ts),
                aes_key,
                hmac_key,
                path: Some(path),
            });
        }
        Ok(keys)
    }

    fn generate_key(created_at: SystemTime) -> io::Result<StoredKey> {
        let mut material = [0u8; KEY_LEN];
        SystemRandom::new()
            .fill(&mut material)
            .map_err(|_| io::Error::other("secure random generation failed"))?;
        let mut aes_key = [0u8; AES_KEY_LEN];
        let mut hmac_key = [0u8; HMAC_KEY_LEN];
        aes_key.copy_from_slice(&material[..AES_KEY_LEN]);
        hmac_key.copy_from_slice(&material[AES_KEY_LEN..]);
        material.fill(0);
        Ok(StoredKey {
            created_at,
            aes_key,
            hmac_key,
            path: None,
        })
    }

    fn generate_and_persist_key(config: &SessionTicketConfig) -> io::Result<StoredKey> {
        let mut key = Self::generate_key(SystemTime::now())?;
        let ts = key.unix_ts();
        let path = config.key_directory.join(format!("stk-{ts}.key"));
        let mut data = Vec::with_capacity(KEY_LEN);
        data.extend_from_slice(&key.aes_key);
        data.extend_from_slice(&key.hmac_key);
        fs::write(&path, data)?;
        key.path = Some(path);
        Ok(key)
    }

    pub async fn rotation_task(self: std::sync::Arc<Self>) {
        let interval = Duration::from_secs(self.config.rotation_interval_seconds());
        loop {
            tokio::time::sleep(interval).await;
            if let Err(err) = self.rotate_now() {
                warn!(%err, "failed to rotate TLS session ticket key");
            }
            let store = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(OVERLAP).await;
                store.prune_expired();
            });
        }
    }

    pub fn rotate_now(&self) -> io::Result<()> {
        let key = Self::generate_and_persist_key(&self.config)?;
        let mut keys = self.keys.write().expect("ticket store lock poisoned");
        keys.insert(0, key);
        keys.truncate(2);
        debug!("rotated TLS session ticket key");
        Ok(())
    }

    pub fn prune_expired(&self) {
        let cutoff = SystemTime::now().checked_sub(OVERLAP).unwrap_or(UNIX_EPOCH);
        let mut keys = self.keys.write().expect("ticket store lock poisoned");
        while keys.len() > 1 && keys.last().is_some_and(|k| k.created_at <= cutoff) {
            let mut old = keys.pop().expect("last key exists");
            if let Some(path) = old.path.take() {
                let _ = fs::remove_file(path);
            }
            old.zeroize();
        }
    }

    pub fn encrypt_ticket(&self, ticket_data: &[u8]) -> Option<Vec<u8>> {
        let key = self.keys.read().ok()?.first()?.clone();
        Self::encrypt_with_key(&key, ticket_data)
    }

    fn encrypt_with_key(key: &StoredKey, ticket_data: &[u8]) -> Option<Vec<u8>> {
        let unbound = aead::UnboundKey::new(&aead::AES_256_GCM, &key.aes_key).ok()?;
        let sealing_key = aead::LessSafeKey::new(unbound);
        let mut nonce = [0u8; NONCE_LEN];
        SystemRandom::new().fill(&mut nonce).ok()?;
        let mut out = Vec::with_capacity(
            TS_LEN + NONCE_LEN + ticket_data.len() + aead::AES_256_GCM.tag_len() + 32,
        );
        out.extend_from_slice(&key.unix_ts().to_be_bytes());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(ticket_data);
        let aad_ts = key.unix_ts().to_be_bytes();
        let tag = sealing_key
            .seal_in_place_separate_tag(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(&aad_ts),
                &mut out[TS_LEN + NONCE_LEN..],
            )
            .ok()?;
        out.extend_from_slice(tag.as_ref());
        let sig = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, &key.hmac_key), &out);
        out.extend_from_slice(sig.as_ref());
        Some(out)
    }

    pub fn decrypt_ticket(&self, encrypted: &[u8]) -> Option<Vec<u8>> {
        if encrypted.len() < TS_LEN + NONCE_LEN + aead::AES_256_GCM.tag_len() + 32 {
            return None;
        }
        let ts = u64::from_be_bytes(encrypted[..TS_LEN].try_into().ok()?);
        let keys = self.keys.read().ok()?.clone();
        let mut ordered = keys
            .iter()
            .filter(|k| k.unix_ts().abs_diff(ts) <= TIMESTAMP_SKEW)
            .cloned()
            .collect::<Vec<_>>();
        ordered.extend(
            keys.into_iter()
                .filter(|k| k.unix_ts().abs_diff(ts) > TIMESTAMP_SKEW),
        );
        for key in ordered {
            if let Some(plain) = Self::decrypt_with_key(&key, encrypted) {
                return Some(plain);
            }
        }
        None
    }

    fn decrypt_with_key(key: &StoredKey, encrypted: &[u8]) -> Option<Vec<u8>> {
        let (body, mac) = encrypted.split_at(encrypted.len().checked_sub(32)?);
        hmac::verify(&hmac::Key::new(hmac::HMAC_SHA256, &key.hmac_key), body, mac).ok()?;
        let nonce =
            aead::Nonce::try_assume_unique_for_key(&body[TS_LEN..TS_LEN + NONCE_LEN]).ok()?;
        let unbound = aead::UnboundKey::new(&aead::AES_256_GCM, &key.aes_key).ok()?;
        let opening_key = aead::LessSafeKey::new(unbound);
        let mut out = body[TS_LEN + NONCE_LEN..].to_vec();
        let len = opening_key
            .open_in_place(nonce, aead::Aad::from(&body[..TS_LEN]), &mut out)
            .ok()?
            .len();
        out.truncate(len);
        Some(out)
    }
}

impl ProducesTickets for SessionTicketStore {
    fn enabled(&self) -> bool {
        true
    }
    fn lifetime(&self) -> u32 {
        self.config.ticket_lifetime_seconds()
    }
    fn encrypt(&self, plain: &[u8]) -> Option<Vec<u8>> {
        self.encrypt_ticket(plain)
    }
    fn decrypt(&self, cipher: &[u8]) -> Option<Vec<u8>> {
        self.decrypt_ticket(cipher)
    }
}
