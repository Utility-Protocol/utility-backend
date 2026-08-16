use chrono::{Duration, Utc};
use futures::future::try_join_all;
use utility_backend::time_series::compaction::{CompactionConfig, CompactionOrchestrator};
use utility_backend::time_series::ingestion::ingest_telemetry;

#[tokio::test]
#[ignore = "requires a TimescaleDB 2.x database in TEST_TIMESCALE_DATABASE_URL"]
async fn compaction_leases_do_not_fail_concurrent_writes() -> anyhow::Result<()> {
    let database_url = std::env::var("TEST_TIMESCALE_DATABASE_URL")?;
    let pool = sqlx::PgPool::connect(&database_url).await?;

    sqlx::query(include_str!("../src/time_series/compress.sql"))
        .execute(&pool)
        .await?;

    let orchestrator = CompactionOrchestrator::new(
        pool.clone(),
        CompactionConfig {
            min_age_hours: 0,
            poll_interval_s: 1,
            chunk_limit: 4,
            ..CompactionConfig::default()
        },
    );

    let now = Utc::now() - Duration::hours(8);
    let mut writes = Vec::new();
    for writer in 0..4 {
        let pool = pool.clone();
        writes.push(tokio::spawn(async move {
            for i in 0..25_000 {
                let recorded_at = now + Duration::hours((i % 4) as i64);
                ingest_telemetry(&pool, &format!("stress-{writer}"), i as f64, recorded_at, 0)
                    .await?;
            }
            anyhow::Ok(())
        }));
    }

    let compactor = tokio::spawn(async move { orchestrator.run_once().await });
    for result in try_join_all(writes).await? {
        result?;
    }
    compactor.await??;

    Ok(())
}
