use crate::api::metrics;
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{de::DeserializeOwned, Serialize};
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheConfig {
    pub default_ttl: Duration,
    pub max_entries: usize,
    pub redis_url: Option<String>,
    pub namespace: String,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            default_ttl: Duration::from_secs(60),
            max_entries: 10_000,
            redis_url: None,
            namespace: "utility-backend".to_string(),
        }
    }
}

impl CacheConfig {
    pub fn validate(&self) -> Result<(), CacheError> {
        if self.default_ttl.is_zero() {
            return Err(CacheError::InvalidConfig(
                "default_ttl must be greater than zero",
            ));
        }
        if self.max_entries == 0 {
            return Err(CacheError::InvalidConfig(
                "max_entries must be greater than zero",
            ));
        }
        if self.namespace.trim().is_empty() {
            return Err(CacheError::InvalidConfig("namespace must not be empty"));
        }
        Ok(())
    }

    fn scoped_key(&self, key: &str) -> String {
        format!("{}:{}", self.namespace, key)
    }
}

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("invalid cache config: {0}")]
    InvalidConfig(&'static str),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("redis connection error: {0}")]
    RedisConnection(String),
    #[error("redis protocol error: {0}")]
    RedisProtocol(String),
}

#[async_trait]
pub trait CacheStore: Send + Sync {
    async fn get_raw(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError>;
    async fn set_raw(&self, key: &str, value: Vec<u8>, ttl: Duration) -> Result<(), CacheError>;
    async fn delete(&self, key: &str) -> Result<(), CacheError>;
}

struct Entry {
    value: Vec<u8>,
    expires_at: Instant,
}

pub struct InMemoryCacheStore {
    max_entries: usize,
    entries: DashMap<String, Entry>,
}

impl InMemoryCacheStore {
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            entries: DashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.evict_expired();
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn evict_expired(&self) {
        let now = Instant::now();
        self.entries.retain(|_, entry| entry.expires_at > now);
    }

    fn evict_to_capacity(&self) {
        self.evict_expired();
        while self.entries.len() > self.max_entries {
            if let Some(key) = self
                .entries
                .iter()
                .min_by_key(|entry| entry.expires_at)
                .map(|entry| entry.key().clone())
            {
                self.entries.remove(&key);
            } else {
                break;
            }
        }
    }
}

#[async_trait]
impl CacheStore for InMemoryCacheStore {
    async fn get_raw(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        if let Some(entry) = self.entries.get(key) {
            if entry.expires_at > Instant::now() {
                return Ok(Some(entry.value.clone()));
            }
        }
        self.entries.remove(key);
        Ok(None)
    }

    async fn set_raw(&self, key: &str, value: Vec<u8>, ttl: Duration) -> Result<(), CacheError> {
        self.entries.insert(
            key.to_string(),
            Entry {
                value,
                expires_at: Instant::now() + ttl,
            },
        );
        self.evict_to_capacity();
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), CacheError> {
        self.entries.remove(key);
        Ok(())
    }
}

pub struct RedisCacheStore {
    address: String,
}

impl RedisCacheStore {
    pub fn new(redis_url: &str) -> Result<Self, CacheError> {
        let address = parse_redis_address(redis_url)?;
        Ok(Self { address })
    }

    async fn command(&self, parts: &[&[u8]]) -> Result<RedisReply, CacheError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let mut stream = TcpStream::connect(&self.address)
            .await
            .map_err(|err| CacheError::RedisConnection(err.to_string()))?;
        let mut request = Vec::new();
        request.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for part in parts {
            request.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            request.extend_from_slice(part);
            request.extend_from_slice(b"\r\n");
        }
        stream
            .write_all(&request)
            .await
            .map_err(|err| CacheError::RedisConnection(err.to_string()))?;

        let mut buf = vec![0; 1024 * 1024];
        let read = stream
            .read(&mut buf)
            .await
            .map_err(|err| CacheError::RedisConnection(err.to_string()))?;
        parse_redis_reply(&buf[..read])
    }
}

#[async_trait]
impl CacheStore for RedisCacheStore {
    async fn get_raw(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        match self.command(&[b"GET", key.as_bytes()]).await? {
            RedisReply::Bulk(value) => Ok(value),
            other => Err(CacheError::RedisProtocol(format!(
                "unexpected GET reply: {other:?}"
            ))),
        }
    }

    async fn set_raw(&self, key: &str, value: Vec<u8>, ttl: Duration) -> Result<(), CacheError> {
        let seconds = ttl.as_secs().max(1).to_string();
        match self
            .command(&[b"SETEX", key.as_bytes(), seconds.as_bytes(), &value])
            .await?
        {
            RedisReply::Simple(status) if status == "OK" => Ok(()),
            other => Err(CacheError::RedisProtocol(format!(
                "unexpected SETEX reply: {other:?}"
            ))),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), CacheError> {
        match self.command(&[b"DEL", key.as_bytes()]).await? {
            RedisReply::Integer => Ok(()),
            other => Err(CacheError::RedisProtocol(format!(
                "unexpected DEL reply: {other:?}"
            ))),
        }
    }
}

#[derive(Debug)]
enum RedisReply {
    Simple(String),
    Bulk(Option<Vec<u8>>),
    Integer,
}

fn parse_redis_address(redis_url: &str) -> Result<String, CacheError> {
    let trimmed = redis_url.strip_prefix("redis://").ok_or_else(|| {
        CacheError::RedisProtocol("redis_url must start with redis://".to_string())
    })?;
    let authority = trimmed.split('/').next().unwrap_or(trimmed);
    if authority.is_empty() {
        return Err(CacheError::RedisProtocol(
            "redis_url host is empty".to_string(),
        ));
    }
    Ok(if authority.contains(':') {
        authority.to_string()
    } else {
        format!("{authority}:6379")
    })
}

fn parse_redis_reply(buf: &[u8]) -> Result<RedisReply, CacheError> {
    if buf.is_empty() {
        return Err(CacheError::RedisProtocol("empty redis reply".to_string()));
    }
    match buf[0] {
        b'+' => Ok(RedisReply::Simple(read_line(&buf[1..])?)),
        b':' => {
            let _ = read_line(&buf[1..])?
                .parse::<i64>()
                .map_err(|e| CacheError::RedisProtocol(format!("invalid integer reply: {e}")))?;
            Ok(RedisReply::Integer)
        }
        b'$' => {
            let line = read_line(&buf[1..])?;
            let len: isize = line
                .parse()
                .map_err(|_| CacheError::RedisProtocol("invalid bulk length".to_string()))?;
            if len < 0 {
                return Ok(RedisReply::Bulk(None));
            }
            let header_len = 1 + line.len() + 2;
            let len = len as usize;
            if buf.len() < header_len + len + 2 {
                return Err(CacheError::RedisProtocol(
                    "truncated bulk reply".to_string(),
                ));
            }
            Ok(RedisReply::Bulk(Some(
                buf[header_len..header_len + len].to_vec(),
            )))
        }
        b'-' => Err(CacheError::RedisProtocol(read_line(&buf[1..])?)),
        _ => Err(CacheError::RedisProtocol("unknown reply type".to_string())),
    }
}

fn read_line(buf: &[u8]) -> Result<String, CacheError> {
    let pos = buf
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or_else(|| CacheError::RedisProtocol("missing CRLF".to_string()))?;
    String::from_utf8(buf[..pos].to_vec())
        .map_err(|_| CacheError::RedisProtocol("reply was not UTF-8".to_string()))
}

pub struct CacheLayer {
    config: CacheConfig,
    local: Arc<InMemoryCacheStore>,
    remote: Option<Arc<dyn CacheStore>>,
}

impl CacheLayer {
    pub fn new(config: CacheConfig) -> Result<Self, CacheError> {
        config.validate()?;
        let remote = match &config.redis_url {
            Some(url) => Some(Arc::new(RedisCacheStore::new(url)?) as Arc<dyn CacheStore>),
            None => None,
        };
        Ok(Self::with_remote(config, remote))
    }

    pub fn with_remote(config: CacheConfig, remote: Option<Arc<dyn CacheStore>>) -> Self {
        Self {
            local: Arc::new(InMemoryCacheStore::new(config.max_entries)),
            config,
            remote,
        }
    }

    pub async fn get<T>(&self, key: &str) -> Result<Option<T>, CacheError>
    where
        T: DeserializeOwned,
    {
        let scoped = self.config.scoped_key(key);
        if let Some(bytes) = self.local.get_raw(&scoped).await? {
            metrics::record_cache_hit("memory");
            return Ok(Some(serde_json::from_slice(&bytes)?));
        }
        metrics::record_cache_miss("memory");

        if let Some(remote) = &self.remote {
            if let Some(bytes) = remote.get_raw(&scoped).await? {
                metrics::record_cache_hit("redis");
                self.local
                    .set_raw(&scoped, bytes.clone(), self.config.default_ttl)
                    .await?;
                return Ok(Some(serde_json::from_slice(&bytes)?));
            }
            metrics::record_cache_miss("redis");
        }
        Ok(None)
    }

    pub async fn set<T>(
        &self,
        key: &str,
        value: &T,
        ttl: Option<Duration>,
    ) -> Result<(), CacheError>
    where
        T: Serialize + Sync,
    {
        let scoped = self.config.scoped_key(key);
        let ttl = ttl.unwrap_or(self.config.default_ttl);
        let bytes = serde_json::to_vec(value)?;
        self.local.set_raw(&scoped, bytes.clone(), ttl).await?;
        if let Some(remote) = &self.remote {
            remote.set_raw(&scoped, bytes, ttl).await?;
        }
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<(), CacheError> {
        let scoped = self.config.scoped_key(key);
        self.local.delete(&scoped).await?;
        if let Some(remote) = &self.remote {
            remote.delete(&scoped).await?;
        }
        Ok(())
    }

    pub fn local_len(&self) -> usize {
        self.local.len()
    }
}
