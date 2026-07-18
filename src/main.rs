use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing_subscriber::EnvFilter;

use utility_backend::api::middleware::DynamicRateLimiter;
use utility_backend::api::AppState;
use utility_backend::gateway::lock::AdvisoryLock;
use utility_backend::soroban::rpc::CircuitBreaker;
use utility_backend::soroban::sequencer::NonceSequencer;
use utility_backend::time_series::compression::{
    init_global_compression_manager, spawn_compression_monitor, CompressionPolicy,
    CompressionPolicyManager,
};
use utility_backend::transport::lib::spawn_transport_monitors;
use utility_backend::transport::tcp::config::TcpTransportConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    tracing::info!("starting utility-backend service");

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://utility:utility_secret@localhost:5432/utility_test".into());

    let db_pool = sqlx::PgPool::connect(&db_url).await?;

    let sequencer = Arc::new(NonceSequencer::new());
    let reaper = sequencer.clone();
    tokio::spawn(async move {
        reaper.start_reaper().await;
    });

    let metrics_pool = db_pool.clone();
    tokio::spawn(async move {
        db_active_connection_poller(metrics_pool).await;
    });

    // Initialise the global compression policy manager and spawn its
    // background monitoring task.
    let compression_manager = Arc::new(CompressionPolicyManager::new(
        db_url,
        CompressionPolicy::default(),
    ));
    init_global_compression_manager(compression_manager.clone());
    spawn_compression_monitor(compression_manager, Duration::from_secs(60));

    // Start the adaptive TCP connection lifecycle manager and FD monitor so
    // long-held meter sockets cannot exhaust the process descriptor budget.
    let _transport = spawn_transport_monitors(TcpTransportConfig::default());

    let advisory_lock = Arc::new(AdvisoryLock::postgres(db_pool.clone()));
    let breaker = Arc::new(Mutex::new(CircuitBreaker::new(5)));
    let rate_limiter = DynamicRateLimiter::new();

    let pd_client = utility_backend::incident::PagerDutyClient::from_env();
    let incident_manager = utility_backend::incident::IncidentManager::new(pd_client);

    // Register a default automated runbook and rule for Database Lag mitigation
    incident_manager.register_runbook(utility_backend::incident::Runbook {
        name: "Auto-Mitigate Database Lag".to_string(),
        description: "Shortens the compression window to alleviate disk/lag issues".to_string(),
        actions: vec![utility_backend::incident::RunbookAction::AdjustCompressionPolicy {
            compress_after_days: 1,
        }],
    });

    incident_manager.register_rule(utility_backend::incident::AutomationRule {
        id: "RULE-001".to_string(),
        component: "TimeSeries".to_string(),
        severity: "critical".to_string(),
        incident_class: "DatabaseLag".to_string(),
        runbook_name: "Auto-Mitigate Database Lag".to_string(),
    });

    utility_backend::incident::init_global_incident_manager(incident_manager.clone());

    let state = AppState {
        sequencer,
        pool: db_pool,
        advisory_lock,
        breaker,
        rate_limiter,
        incident_manager,
    };

    let app = utility_backend::api::router::build_router(state).await?;

    let addr = SocketAddr::from(([0, 0, 0, 0], 8443));
    tracing::info!("listening on {}", addr);

    axum::serve(
        tokio::net::TcpListener::bind(addr).await?,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}

async fn db_active_connection_poller(pool: sqlx::PgPool) {
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
