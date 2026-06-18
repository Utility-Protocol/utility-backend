use std::net::SocketAddr;
use std::sync::Arc;
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
