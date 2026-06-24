use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres};
use tracing::{info, warn};

const GAP_WARNING_THRESHOLD: u64 = 100;
const DEFAULT_PAGE_LIMIT: u32 = 200;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct LedgerEvent {
    pub event_id: String,
    pub contract_id: String,
    pub sequence: u64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncCursorStatus {
    pub contract_id: String,
    pub last_ledger_sequence: u64,
    pub last_timestamp: Option<DateTime<Utc>>,
    pub gap_count: i64,
}

#[derive(Debug, Clone)]
pub struct DetectedGap {
    pub contract_id: String,
    pub start_sequence: u64,
    pub end_sequence: u64,
}

#[derive(Debug, Deserialize)]
struct SorobanEventsEnvelope {
    result: Option<SorobanEventsResult>,
}

#[derive(Debug, Deserialize)]
struct SorobanEventsResult {
    #[serde(default)]
    events: Vec<LedgerEvent>,
    #[serde(default)]
    cursor: Option<String>,
}

#[allow(dead_code)]
pub struct SlidingWindowSyncer {
    window_start: DateTime<Utc>,
    last_synced_sequence: u64,
    pool: Option<Pool<Postgres>>,
    backfill_page_limit: u32,
}

impl SlidingWindowSyncer {
    pub fn new(window_days: i64) -> Self {
        Self {
            window_start: Utc::now() - chrono::Duration::days(window_days),
            last_synced_sequence: 0,
            pool: None,
            backfill_page_limit: DEFAULT_PAGE_LIMIT,
        }
    }

    pub async fn with_postgres(
        pool: Pool<Postgres>,
        window_days: i64,
        contract_id: &str,
    ) -> Result<Self, sqlx::Error> {
        ensure_schema(&pool).await?;
        let cursor = load_cursor(&pool, contract_id).await?;
        Ok(Self {
            window_start: Utc::now() - chrono::Duration::days(window_days),
            last_synced_sequence: cursor
                .as_ref()
                .map(|c| c.last_ledger_sequence)
                .unwrap_or_default(),
            pool: Some(pool),
            backfill_page_limit: DEFAULT_PAGE_LIMIT,
        })
    }

    pub async fn sync_events(
        &mut self,
        rpc_url: &str,
        contract_id: &str,
    ) -> Result<Vec<LedgerEvent>, &'static str> {
        if let Some(pool) = self.pool.clone() {
            ensure_schema(&pool)
                .await
                .map_err(|_| "failed to ensure sync schema")?;
            if let Some(cursor) = load_cursor(&pool, contract_id)
                .await
                .map_err(|_| "failed to load sync cursor")?
            {
                self.last_synced_sequence = cursor.last_ledger_sequence;
            }
        }

        let mut events = self
            .fetch_events(rpc_url, contract_id, self.last_synced_sequence, None)
            .await?;
        events.retain(|event| event.timestamp >= self.window_start);
        events.sort_by_key(|event| event.sequence);

        let gaps = detect_gaps(contract_id, self.last_synced_sequence, &events);
        for gap in &gaps {
            if gap.end_sequence.saturating_sub(gap.start_sequence) + 1 > GAP_WARNING_THRESHOLD {
                warn!(
                    contract = %contract_id,
                    start_sequence = gap.start_sequence,
                    end_sequence = gap.end_sequence,
                    "detected Soroban event sequence gap"
                );
            }
        }

        if let Some(pool) = self.pool.clone() {
            for gap in &gaps {
                record_gap(&pool, gap)
                    .await
                    .map_err(|_| "failed to record gap")?;
                let backfilled = self
                    .backfill_gap(
                        &pool,
                        rpc_url,
                        contract_id,
                        gap.start_sequence,
                        gap.end_sequence,
                    )
                    .await?;
                events.extend(backfilled);
                resolve_gap(&pool, contract_id, gap.start_sequence, gap.end_sequence)
                    .await
                    .map_err(|_| "failed to resolve gap")?;
            }

            events.sort_by_key(|event| event.sequence);
            events.dedup_by(|a, b| a.contract_id == b.contract_id && a.event_id == b.event_id);
            persist_events_and_cursor(&pool, contract_id, &events)
                .await
                .map_err(|_| "failed to persist events")?;
        } else if let Some(last) = events.last() {
            self.last_synced_sequence = last.sequence;
        }

        info!(count = events.len(), contract = %contract_id, "synced ledger events");
        if let Some(last) = events.last() {
            self.last_synced_sequence = last.sequence;
        }
        Ok(events)
    }

    async fn backfill_gap(
        &self,
        pool: &Pool<Postgres>,
        rpc_url: &str,
        contract_id: &str,
        start_sequence: u64,
        end_sequence: u64,
    ) -> Result<Vec<LedgerEvent>, &'static str> {
        let mut cursor = None;
        let mut backfilled = Vec::new();
        loop {
            let page = self
                .fetch_events_envelope(rpc_url, contract_id, start_sequence, cursor.as_deref())
                .await?;
            let Some(result) = page.result else { break };
            let mut page_events: Vec<_> = result
                .events
                .into_iter()
                .filter(|event| event.sequence >= start_sequence && event.sequence <= end_sequence)
                .collect();
            persist_events(pool, &page_events)
                .await
                .map_err(|_| "failed to persist backfilled events")?;
            backfilled.append(&mut page_events);
            cursor = result.cursor;
            if cursor.is_none() || backfilled.len() as u32 >= self.backfill_page_limit {
                break;
            }
        }
        Ok(backfilled)
    }

    async fn fetch_events(
        &self,
        rpc_url: &str,
        contract_id: &str,
        start_ledger: u64,
        cursor: Option<&str>,
    ) -> Result<Vec<LedgerEvent>, &'static str> {
        let envelope = self
            .fetch_events_envelope(rpc_url, contract_id, start_ledger, cursor)
            .await?;
        Ok(envelope.result.map(|r| r.events).unwrap_or_default())
    }

    async fn fetch_events_envelope(
        &self,
        rpc_url: &str,
        contract_id: &str,
        start_ledger: u64,
        cursor: Option<&str>,
    ) -> Result<SorobanEventsEnvelope, &'static str> {
        let mut params = serde_json::json!({
            "contractId": contract_id,
            "startLedger": start_ledger,
            "limit": self.backfill_page_limit,
        });
        if let Some(cursor) = cursor {
            params["cursor"] = serde_json::Value::String(cursor.to_owned());
        }
        let payload =
            serde_json::json!({"jsonrpc":"2.0","id":1,"method":"getEvents","params":params});
        let client = reqwest::Client::new();
        client
            .post(rpc_url)
            .json(&payload)
            .send()
            .await
            .map_err(|_| "failed to fetch ledger events")?
            .json()
            .await
            .map_err(|_| "failed to parse events")
    }
}

pub fn detect_gaps(
    contract_id: &str,
    last_synced_sequence: u64,
    events: &[LedgerEvent],
) -> Vec<DetectedGap> {
    let mut gaps = Vec::new();
    let mut expected = last_synced_sequence.saturating_add(1);
    for event in events {
        if event.sequence > expected {
            gaps.push(DetectedGap {
                contract_id: contract_id.to_owned(),
                start_sequence: expected,
                end_sequence: event.sequence - 1,
            });
        }
        expected = event.sequence.saturating_add(1);
    }
    gaps
}

pub async fn sync_status(pool: &Pool<Postgres>) -> Result<Vec<SyncCursorStatus>, sqlx::Error> {
    ensure_schema(pool).await?;
    sqlx::query_as::<_, (String, i64, Option<DateTime<Utc>>, i64)>(
        r#"
        SELECT c.contract_id,
               c.last_ledger_sequence,
               c.last_timestamp,
               COALESCE(g.gap_count, 0) AS gap_count
        FROM sync_cursors c
        LEFT JOIN (
            SELECT contract_id, count(*) AS gap_count
            FROM soroban_event_gaps
            WHERE resolved_at IS NULL
            GROUP BY contract_id
        ) g ON g.contract_id = c.contract_id
        ORDER BY c.contract_id
        "#,
    )
    .fetch_all(pool)
    .await
    .map(|rows| {
        rows.into_iter()
            .map(
                |(contract_id, seq, last_timestamp, gap_count)| SyncCursorStatus {
                    contract_id,
                    last_ledger_sequence: seq.max(0) as u64,
                    last_timestamp,
                    gap_count,
                },
            )
            .collect()
    })
}

async fn load_cursor(
    pool: &Pool<Postgres>,
    contract_id: &str,
) -> Result<Option<SyncCursorStatus>, sqlx::Error> {
    sqlx::query_as::<_, (String, i64, Option<DateTime<Utc>>, i64)>(
        "SELECT contract_id, last_ledger_sequence, last_timestamp, 0 FROM sync_cursors WHERE contract_id = $1",
    )
    .bind(contract_id)
    .fetch_optional(pool)
    .await
    .map(|row| row.map(|(contract_id, seq, last_timestamp, gap_count)| SyncCursorStatus {
        contract_id,
        last_ledger_sequence: seq.max(0) as u64,
        last_timestamp,
        gap_count,
    }))
}

async fn persist_events_and_cursor(
    pool: &Pool<Postgres>,
    contract_id: &str,
    events: &[LedgerEvent],
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    for event in events {
        sqlx::query(
            "INSERT INTO soroban_events (contract_id, event_id, ledger_sequence, event_timestamp, payload) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (contract_id, event_id) DO NOTHING",
        )
        .bind(&event.contract_id)
        .bind(&event.event_id)
        .bind(event.sequence as i64)
        .bind(event.timestamp)
        .bind(serde_json::to_value(event).unwrap_or(serde_json::Value::Null))
        .execute(&mut *tx)
        .await?;
    }
    if let Some(last) = events.last() {
        sqlx::query(
            "INSERT INTO sync_cursors (contract_id, last_ledger_sequence, last_timestamp) VALUES ($1, $2, $3) ON CONFLICT (contract_id) DO UPDATE SET last_ledger_sequence = GREATEST(sync_cursors.last_ledger_sequence, EXCLUDED.last_ledger_sequence), last_timestamp = EXCLUDED.last_timestamp, updated_at = now()",
        )
        .bind(contract_id)
        .bind(last.sequence as i64)
        .bind(last.timestamp)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await
}

async fn persist_events(pool: &Pool<Postgres>, events: &[LedgerEvent]) -> Result<(), sqlx::Error> {
    for event in events {
        sqlx::query("INSERT INTO soroban_events (contract_id, event_id, ledger_sequence, event_timestamp, payload) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (contract_id, event_id) DO NOTHING")
            .bind(&event.contract_id).bind(&event.event_id).bind(event.sequence as i64).bind(event.timestamp)
            .bind(serde_json::to_value(event).unwrap_or(serde_json::Value::Null)).execute(pool).await?;
    }
    Ok(())
}

async fn record_gap(pool: &Pool<Postgres>, gap: &DetectedGap) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO soroban_event_gaps (contract_id, start_sequence, end_sequence) VALUES ($1, $2, $3) ON CONFLICT (contract_id, start_sequence, end_sequence) DO NOTHING")
        .bind(&gap.contract_id).bind(gap.start_sequence as i64).bind(gap.end_sequence as i64).execute(pool).await?;
    Ok(())
}

async fn resolve_gap(
    pool: &Pool<Postgres>,
    contract_id: &str,
    start_sequence: u64,
    end_sequence: u64,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE soroban_event_gaps SET resolved_at = now() WHERE contract_id = $1 AND start_sequence = $2 AND end_sequence = $3")
        .bind(contract_id).bind(start_sequence as i64).bind(end_sequence as i64).execute(pool).await?;
    Ok(())
}

pub async fn ensure_schema(pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
    for statement in include_str!("sync.sql")
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
    {
        sqlx::query(statement).execute(pool).await?;
    }
    Ok(())
}
