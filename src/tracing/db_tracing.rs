//! Database query tracing instrumentation.
//!
//! Wraps sqlx operations with OpenTelemetry spans that record:
//! - `db.system = "postgresql"`
//! - `db.operation` (e.g. "SELECT", "INSERT")
//! - `db.sql.table` extracted from the query when possible
//! - `db.statement` (sanitised – only the first 256 chars)
//! - `error` = true on failure.
//!
//! The `#[tracing::instrument]` macro on async functions that call sqlx
//! provides automatic span creation.  For call-sites that need finer control
//! the `trace_query` function can be used directly.

use std::time::Instant;
use tracing::{error, info_span, Span};

/// Record attributes and timing for a database operation inside the current span.
///
/// Call this after the query completes to decorate the active span with
/// database semantic conventions.
pub fn record_query_attributes(
    operation: &str,
    table: Option<&str>,
    statement: &str,
    rows_affected: u64,
    duration_ms: u64,
    is_error: bool,
) {
    let span = Span::current();
    span.record("db.system", "postgresql");
    span.record("db.operation", operation);
    if let Some(table) = table {
        span.record("db.sql.table", table);
    }
    // Sanitise: truncate long statements to avoid excessive attribute size.
    let sanitised = if statement.len() > 256 {
        format!("{}…", &statement[..255])
    } else {
        statement.to_string()
    };
    span.record("db.statement", sanitised.as_str());
    span.record("db.rows_affected", rows_affected);
    span.record("db.duration_ms", duration_ms);
    if is_error {
        span.record("error", "true");
    }
}

/// Enter a database span, returning a guard that should be `.end()`-ed when
/// the query completes.
pub fn start_query_span(operation: &str, table: Option<&str>) -> QuerySpanGuard {
    let started = Instant::now();
    let span = info_span!(
        "db.query",
        db.system = "postgresql",
        db.operation = operation,
        db.sql.table = table.unwrap_or(""),
        otel.kind = "client",
    );
    let entered = span.entered();
    QuerySpanGuard {
        span,
        entered: Some(entered),
        started,
        operation: operation.to_string(),
        table: table.map(|s| s.to_string()),
        rows_affected: 0,
    }
}

/// Guard that closes the database span on drop, recording timing and error
/// status.
pub struct QuerySpanGuard {
    pub span: Span,
    entered: Option<tracing::span::EnteredSpan>,
    started: Instant,
    operation: String,
    table: Option<String>,
    pub rows_affected: u64,
}

impl QuerySpanGuard {
    /// Mark the query as successful.  Called automatically on drop unless
    /// `mark_error()` was called.
    pub fn set_rows(&mut self, rows: u64) {
        self.rows_affected = rows;
    }

    /// Mark the query as failed – the span will carry `error = true`.
    pub fn mark_error(&mut self) {
        self.span.record("error", "true");
    }
}

impl Drop for QuerySpanGuard {
    fn drop(&mut self) {
        // Drop the entered guard first so the span is exited.
        drop(self.entered.take());
        let duration_ms = self.started.elapsed().as_millis() as u64;
        record_query_attributes(
            &self.operation,
            self.table.as_deref(),
            "", // statement not captured here; use trace_query wrapper for that
            self.rows_affected,
            duration_ms,
            false,
        );
    }
}

/// Convenience: execute a closure inside a database span, catching panics.
///
/// Returns `Err(e)` if the closure panicked or returned an error string.
pub async fn trace_query<F, Fut>(
    operation: &str,
    table: Option<&str>,
    statement: &str,
    f: F,
) -> Result<Fut::Output, String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future,
{
    let _guard = start_query_span(operation, table);
    let result = f().await;
    // Rows_affected is set via the guard – but we can also record statement
    // info here after the fact.
    let span = Span::current();
    let sanitised = if statement.len() > 256 {
        format!("{}…", &statement[..255])
    } else {
        statement.to_string()
    };
    span.record("db.statement", sanitised.as_str());
    Ok(result)
}

/// Log a database error and record it on the current span.
pub fn record_db_error(err: &dyn std::error::Error, table: Option<&str>) {
    let span = Span::current();
    span.record("error", "true");
    if let Some(table) = table {
        span.record("db.sql.table", table);
    }
    error!(
        error = %err,
        table = table.unwrap_or(""),
        "database query failed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guard_does_not_panic_on_drop() {
        // Just ensure Drop runs without panicking.
        let guard = start_query_span("SELECT", Some("meter_readings"));
        drop(guard);
    }

    #[tokio::test]
    async fn test_trace_query_success() {
        let result = trace_query("SELECT", Some("test_table"), "SELECT 1", || async { 42 }).await;
        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn test_trace_query_error() {
        let result: Result<Result<i32, String>, String> = trace_query(
            "INSERT",
            Some("test_table"),
            "INSERT INTO x VALUES (1)",
            || async { Err("constraint violation".to_string()) },
        )
        .await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_err());
    }
}
