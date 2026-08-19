//! Rate limit configuration persistence and hot-reload.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;

use crate::api::middleware::{DynamicRateLimiter, ServiceRateLimiter, TenantLimit, TenantRateLimiter};
use crate::audit::store::audit_rate_limit_change_in_tx;

pub const GLOBAL_SCOPE_KEY: &str = "_global";
pub const AUDIT_ACTION_CREATE: &str = "rate_limit.create";
pub const AUDIT_ACTION_UPDATE: &str = "rate_limit.update";
pub const AUDIT_ACTION_DELETE: &str = "rate_limit.delete";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RateLimitScopeType {
    Global,
    Service,
    User,
}

impl RateLimitScopeType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Service => "service",
            Self::User => "user",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "global" => Some(Self::Global),
            "service" => Some(Self::Service),
            "user" => Some(Self::User),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RateLimitConfig {
    pub id: i64,
    pub scope_type: RateLimitScopeType,
    pub scope_key: String,
    pub max_tokens: i64,
    pub refill_rate: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateRateLimitConfigRequest {
    pub scope_type: RateLimitScopeType,
    pub scope_key: Option<String>,
    pub max_tokens: i64,
    pub refill_rate: i64,
    #[serde(default = "default_actor")]
    pub actor: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdateRateLimitConfigRequest {
    pub max_tokens: i64,
    pub refill_rate: i64,
    #[serde(default = "default_actor")]
    pub actor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitAuditEntry {
    pub sequence: i64,
    pub occurred_at: DateTime<Utc>,
    pub actor: String,
    pub action: String,
    pub resource: String,
    pub payload_hash: String,
}

pub fn default_actor() -> String {
    "operator".to_string()
}

#[derive(Debug, thiserror::Error)]
pub enum RateLimitConfigError {
    #[error("invalid scope_key")]
    InvalidScopeKey,
    #[error("max_tokens must be between 1 and 1000000")]
    InvalidMaxTokens,
    #[error("refill_rate must be between 0 and 1000000")]
    InvalidRefillRate,
    #[error("global scope_key must be '{GLOBAL_SCOPE_KEY}'")]
    InvalidGlobalScopeKey,
    #[error("configuration not found")]
    NotFound,
    #[error("configuration already exists for this scope")]
    Conflict,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

pub fn validate_create_request(req: &CreateRateLimitConfigRequest) -> Result<String, RateLimitConfigError> {
    validate_limits(req.max_tokens, req.refill_rate)?;
    let scope_key = normalize_scope_key(req.scope_type, req.scope_key.as_deref())?;
    Ok(scope_key)
}

pub fn validate_update_request(req: &UpdateRateLimitConfigRequest) -> Result<(), RateLimitConfigError> {
    validate_limits(req.max_tokens, req.refill_rate)
}

fn validate_limits(max_tokens: i64, refill_rate: i64) -> Result<(), RateLimitConfigError> {
    if !(1..=1_000_000).contains(&max_tokens) {
        return Err(RateLimitConfigError::InvalidMaxTokens);
    }
    if !(0..=1_000_000).contains(&refill_rate) {
        return Err(RateLimitConfigError::InvalidRefillRate);
    }
    Ok(())
}

fn normalize_scope_key(
    scope_type: RateLimitScopeType,
    scope_key: Option<&str>,
) -> Result<String, RateLimitConfigError> {
    match scope_type {
        RateLimitScopeType::Global => {
            match scope_key {
                None | Some(GLOBAL_SCOPE_KEY) => Ok(GLOBAL_SCOPE_KEY.to_string()),
                Some(_) => Err(RateLimitConfigError::InvalidGlobalScopeKey),
            }
        }
        RateLimitScopeType::Service | RateLimitScopeType::User => {
            let key = scope_key.unwrap_or("").trim();
            if key.is_empty() || key.len() > 255 || !key.chars().all(is_scope_char) {
                return Err(RateLimitConfigError::InvalidScopeKey);
            }
            Ok(key.to_string())
        }
    }
}

fn is_scope_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.')
}

pub async fn ensure_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    for statement in include_str!("../../db/rate_limits.sql").split(';') {
        let statement = statement.trim();
        if !statement.is_empty() {
            sqlx::query(statement).execute(pool).await?;
        }
    }
    Ok(())
}

pub async fn list_configs(
    pool: &PgPool,
    scope_type: Option<RateLimitScopeType>,
) -> Result<Vec<RateLimitConfig>, sqlx::Error> {
    let rows = if let Some(scope) = scope_type {
        sqlx::query_as::<_, RateLimitConfigRow>(
            "SELECT id, scope_type, scope_key, max_tokens, refill_rate, created_at, updated_at
             FROM rate_limit_configs
             WHERE scope_type = $1
             ORDER BY scope_type, scope_key",
        )
        .bind(scope.as_str())
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as::<_, RateLimitConfigRow>(
            "SELECT id, scope_type, scope_key, max_tokens, refill_rate, created_at, updated_at
             FROM rate_limit_configs
             ORDER BY scope_type, scope_key",
        )
        .fetch_all(pool)
        .await?
    };

    Ok(rows.into_iter().map(RateLimitConfigRow::into_config).collect())
}

pub async fn get_config(pool: &PgPool, id: i64) -> Result<Option<RateLimitConfig>, sqlx::Error> {
    let row = sqlx::query_as::<_, RateLimitConfigRow>(
        "SELECT id, scope_type, scope_key, max_tokens, refill_rate, created_at, updated_at
         FROM rate_limit_configs
         WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(RateLimitConfigRow::into_config))
}

pub async fn create_config(
    pool: &PgPool,
    dynamic: &Arc<DynamicRateLimiter>,
    tenant: &Arc<TenantRateLimiter>,
    service: &Arc<ServiceRateLimiter>,
    req: CreateRateLimitConfigRequest,
) -> Result<RateLimitConfig, RateLimitConfigError> {
    let scope_key = validate_create_request(&req)?;
    let now = Utc::now();

    let mut tx = pool.begin().await.map_err(RateLimitConfigError::Database)?;

    let row = sqlx::query_as::<_, RateLimitConfigRow>(
        "INSERT INTO rate_limit_configs (scope_type, scope_key, max_tokens, refill_rate, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING id, scope_type, scope_key, max_tokens, refill_rate, created_at, updated_at",
    )
    .bind(req.scope_type.as_str())
    .bind(&scope_key)
    .bind(req.max_tokens)
    .bind(req.refill_rate)
    .bind(now)
    .bind(now)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if e.to_string().contains("duplicate key") {
            RateLimitConfigError::Conflict
        } else {
            RateLimitConfigError::Database(e)
        }
    })?;

    let config = row.into_config();
    audit_rate_limit_change_in_tx(
        &mut tx,
        &req.actor,
        AUDIT_ACTION_CREATE,
        &resource_id(&config),
        &config,
    )
    .await
    .map_err(RateLimitConfigError::Database)?;

    tx.commit().await.map_err(RateLimitConfigError::Database)?;
    apply_config(&config, dynamic, tenant, service);

    Ok(config)
}

pub async fn update_config(
    pool: &PgPool,
    dynamic: &Arc<DynamicRateLimiter>,
    tenant: &Arc<TenantRateLimiter>,
    service: &Arc<ServiceRateLimiter>,
    id: i64,
    req: UpdateRateLimitConfigRequest,
) -> Result<RateLimitConfig, RateLimitConfigError> {
    validate_update_request(&req)?;
    let now = Utc::now();

    let mut tx = pool.begin().await.map_err(RateLimitConfigError::Database)?;

    let row = sqlx::query_as::<_, RateLimitConfigRow>(
        "UPDATE rate_limit_configs
         SET max_tokens = $1, refill_rate = $2, updated_at = $3
         WHERE id = $4
         RETURNING id, scope_type, scope_key, max_tokens, refill_rate, created_at, updated_at",
    )
    .bind(req.max_tokens)
    .bind(req.refill_rate)
    .bind(now)
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(RateLimitConfigError::Database)?;

    let Some(row) = row else {
        return Err(RateLimitConfigError::NotFound);
    };

    let config = row.into_config();
    audit_rate_limit_change_in_tx(
        &mut tx,
        &req.actor,
        AUDIT_ACTION_UPDATE,
        &resource_id(&config),
        &config,
    )
    .await
    .map_err(RateLimitConfigError::Database)?;

    tx.commit().await.map_err(RateLimitConfigError::Database)?;
    apply_config(&config, dynamic, tenant, service);

    Ok(config)
}

pub async fn delete_config(
    pool: &PgPool,
    dynamic: &Arc<DynamicRateLimiter>,
    tenant: &Arc<TenantRateLimiter>,
    service: &Arc<ServiceRateLimiter>,
    id: i64,
    actor: &str,
) -> Result<RateLimitConfig, RateLimitConfigError> {
    let mut tx = pool.begin().await.map_err(RateLimitConfigError::Database)?;

    let row = sqlx::query_as::<_, RateLimitConfigRow>(
        "DELETE FROM rate_limit_configs
         WHERE id = $1
         RETURNING id, scope_type, scope_key, max_tokens, refill_rate, created_at, updated_at",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(RateLimitConfigError::Database)?;

    let Some(row) = row else {
        return Err(RateLimitConfigError::NotFound);
    };

    let config = row.into_config();
    audit_rate_limit_change_in_tx(
        &mut tx,
        actor,
        AUDIT_ACTION_DELETE,
        &resource_id(&config),
        &config,
    )
    .await
    .map_err(RateLimitConfigError::Database)?;

    tx.commit().await.map_err(RateLimitConfigError::Database)?;
    remove_config(&config, dynamic, tenant, service);

    Ok(config)
}

pub async fn list_audit_entries(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<RateLimitAuditEntry>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (i64, DateTime<Utc>, String, String, String, String)>(
        "SELECT sequence, occurred_at, actor, action, resource, payload_hash
         FROM audit_events
         WHERE action LIKE 'rate_limit.%'
         ORDER BY sequence DESC
         LIMIT $1",
    )
    .bind(limit.clamp(1, 500))
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(sequence, occurred_at, actor, action, resource, payload_hash)| RateLimitAuditEntry {
                sequence,
                occurred_at,
                actor,
                action,
                resource,
                payload_hash,
            },
        )
        .collect())
}

pub async fn hydrate_from_db(
    pool: &PgPool,
    dynamic: &Arc<DynamicRateLimiter>,
    tenant: &Arc<TenantRateLimiter>,
    service: &Arc<ServiceRateLimiter>,
) -> Result<(), sqlx::Error> {
    let configs = list_configs(pool, None).await?;
    for config in configs {
        apply_config(&config, dynamic, tenant, service);
    }
    Ok(())
}

pub fn apply_config(
    config: &RateLimitConfig,
    dynamic: &Arc<DynamicRateLimiter>,
    tenant: &Arc<TenantRateLimiter>,
    service: &Arc<ServiceRateLimiter>,
) {
    let limit = TenantLimit::new(config.max_tokens as u64, config.refill_rate as u64);
    match config.scope_type {
        RateLimitScopeType::Global => {
            dynamic.set_global_limit(config.max_tokens as u64, config.refill_rate as u64);
        }
        RateLimitScopeType::User => {
            tenant.set_tenant_limit(&config.scope_key, limit);
        }
        RateLimitScopeType::Service => {
            service.set_service_limit(&config.scope_key, limit);
        }
    }
}

pub fn remove_config(
    config: &RateLimitConfig,
    dynamic: &Arc<DynamicRateLimiter>,
    tenant: &Arc<TenantRateLimiter>,
    service: &Arc<ServiceRateLimiter>,
) {
    match config.scope_type {
        RateLimitScopeType::Global => dynamic.reset_global_limit(),
        RateLimitScopeType::User => tenant.remove_tenant_override(&config.scope_key),
        RateLimitScopeType::Service => service.remove_service_override(&config.scope_key),
    }
}

fn resource_id(config: &RateLimitConfig) -> String {
    format!("rate_limit/{}/{}", config.scope_type.as_str(), config.scope_key)
}

#[derive(sqlx::FromRow)]
struct RateLimitConfigRow {
    id: i64,
    scope_type: String,
    scope_key: String,
    max_tokens: i64,
    refill_rate: i64,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl RateLimitConfigRow {
    fn into_config(self) -> RateLimitConfig {
        RateLimitConfig {
            id: self.id,
            scope_type: RateLimitScopeType::parse(&self.scope_type).unwrap_or(RateLimitScopeType::User),
            scope_key: self.scope_key,
            max_tokens: self.max_tokens,
            refill_rate: self.refill_rate,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_global_scope_key() {
        let req = CreateRateLimitConfigRequest {
            scope_type: RateLimitScopeType::Global,
            scope_key: None,
            max_tokens: 100,
            refill_rate: 10,
            actor: "ops".into(),
        };
        assert_eq!(validate_create_request(&req).unwrap(), GLOBAL_SCOPE_KEY);
    }

    #[test]
    fn rejects_invalid_user_scope_key() {
        let req = CreateRateLimitConfigRequest {
            scope_type: RateLimitScopeType::User,
            scope_key: Some("".into()),
            max_tokens: 100,
            refill_rate: 10,
            actor: "ops".into(),
        };
        assert!(matches!(
            validate_create_request(&req),
            Err(RateLimitConfigError::InvalidScopeKey)
        ));
    }
}
