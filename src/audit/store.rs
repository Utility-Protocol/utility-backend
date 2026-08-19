//! Persist tamper-evident audit events to PostgreSQL.

use sqlx::PgPool;

use super::{payload_hash, AuditEvent, NewAuditEvent, GENESIS_PREVIOUS_HASH};

const AUDIT_APPEND_LOCK_KEY: i64 = 4_000_000_002;

/// Append a new audit event to the hash chain inside a short transaction.
pub async fn append_audit_event(
    pool: &PgPool,
    event: NewAuditEvent,
) -> Result<AuditEvent, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let audit = append_audit_event_in_tx(&mut tx, event).await?;
    tx.commit().await?;
    Ok(audit)
}

/// Append within an existing transaction so config mutations can commit atomically.
pub async fn append_audit_event_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    event: NewAuditEvent,
) -> Result<AuditEvent, sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(AUDIT_APPEND_LOCK_KEY)
        .execute(&mut **tx)
        .await?;

    let (previous_hash, next_sequence) = match sqlx::query_as::<_, (i64, String)>(
        "SELECT sequence, hash FROM audit_events ORDER BY sequence DESC LIMIT 1",
    )
    .fetch_optional(&mut **tx)
    .await?
    {
        Some((seq, hash)) => (hash, seq + 1),
        None => (GENESIS_PREVIOUS_HASH.to_string(), 1),
    };

    let audit = AuditEvent::append(next_sequence as u64, previous_hash, event);

    sqlx::query(
        "INSERT INTO audit_events
            (sequence, occurred_at, actor, service, action, resource, payload_hash, previous_hash, hash)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(audit.sequence as i64)
    .bind(audit.occurred_at)
    .bind(&audit.actor)
    .bind(&audit.service)
    .bind(&audit.action)
    .bind(&audit.resource)
    .bind(&audit.payload_hash)
    .bind(&audit.previous_hash)
    .bind(&audit.hash)
    .execute(&mut **tx)
    .await?;

    Ok(audit)
}

/// Convenience helper for rate-limit configuration mutations.
pub async fn audit_rate_limit_change<T: serde::Serialize>(
    pool: &PgPool,
    actor: &str,
    action: &str,
    resource: &str,
    payload: &T,
) -> Result<AuditEvent, sqlx::Error> {
    append_audit_event(
        pool,
        NewAuditEvent {
            occurred_at: chrono::Utc::now(),
            actor: actor.to_string(),
            service: "api".to_string(),
            action: action.to_string(),
            resource: resource.to_string(),
            payload_hash: payload_hash(payload).map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
        },
    )
    .await
}

pub async fn audit_rate_limit_change_in_tx<T: serde::Serialize>(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    actor: &str,
    action: &str,
    resource: &str,
    payload: &T,
) -> Result<AuditEvent, sqlx::Error> {
    append_audit_event_in_tx(
        tx,
        NewAuditEvent {
            occurred_at: chrono::Utc::now(),
            actor: actor.to_string(),
            service: "api".to_string(),
            action: action.to_string(),
            resource: resource.to_string(),
            payload_hash: payload_hash(payload).map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
        },
    )
    .await
}
