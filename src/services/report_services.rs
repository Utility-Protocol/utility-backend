use sqlx::PgPool;
use redis::AsyncCommands;
use std::time::Instant;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct Report {
    pub account_id: String,
    pub report_month: chrono::NaiveDateTime,
    pub total_transactions: i64,
    pub total_amount: f64,
    pub avg_amount: f64,
}

pub async fn get_monthly_report(
    pool: &PgPool, 
    redis: &mut redis::aio::Connection,
    account_id: &str, 
    year: i32, 
    month: u32
) -> Result<Report, Box<dyn std::error::Error>> {
    let cache_key = format!("report:{}:{}:{}", account_id, year, month);
    
    // 1. Check Redis - < 5ms
    if let Ok(cached) = redis.get::<_, String>(&cache_key).await {
        return Ok(serde_json::from_str(&cached)?);
    }

    let start = Instant::now();
    
    // 2. Refresh MV CONCURRENTLY don kada ya toshe DB
    sqlx::query("REFRESH MATERIALIZED VIEW CONCURRENTLY monthly_billing_report")
        .execute(pool).await?;

    // 3. Query daga MV maimakon billing_records
    let report = sqlx::query_as!(
        Report,
        "SELECT * FROM monthly_billing_report WHERE account_id = $1 AND report_month = $2",
        account_id,
        chrono::NaiveDate::from_ymd_opt(year, month, 1).unwrap().and_hms_opt(0,0,0).unwrap()
    ).fetch_one(pool).await?;

    let duration = start.elapsed().as_millis();
    tracing::info!(target: "report_metric", account_id, duration_ms = duration);

    // 4. Cache for 24 hours = 86400 seconds
    let _ = redis.set_ex(&cache_key, serde_json::to_string(&report)?, 86400).await;
    
    Ok(report)
}
