use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

use utility_backend::soroban::sequencer::NonceSequencer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    tracing::info!("starting utility-backend service");

    let sequencer = Arc::new(NonceSequencer::new());
    let reaper = sequencer.clone();
    tokio::spawn(async move {
        reaper.start_reaper().await;
    });
    tokio::spawn(async {
        db_active_connection_poller().await;
    });

    let app = utility_backend::api::router::build_router(sequencer).await?;

    let addr = SocketAddr::from(([0, 0, 0, 0], 8443));
    tracing::info!("listening on {}", addr);

    axum::serve(
        tokio::net::TcpListener::bind(addr).await?,
        app.into_make_service(),
    )
    .await?;

    Ok(())
}

async fn db_active_connection_poller() {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://utility:utility_secret@localhost:5432/utility_test".into());
    let pool = match sqlx::PgPool::connect(&database_url).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("cannot connect to database for metrics polling: {}", e);
            return;
        }
    };
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        match sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_stat_activity WHERE state = 'active'",
        )
        .fetch_one(&pool)
        .await
        {
            Ok(active) => {
                utility_backend::api::metrics::set_db_active_connections(active as f64);
            }
            Err(e) => {
                tracing::warn!("failed to poll active connections: {}", e);
            }
        }
        match sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_stat_activity WHERE state = 'idle'",
        )
        .fetch_one(&pool)
        .await
        {
            Ok(idle) => {
                utility_backend::api::metrics::set_db_idle_connections(idle as f64);
            }
            Err(e) => {
                tracing::warn!("failed to poll idle connections: {}", e);
            }
        }
    }
}
