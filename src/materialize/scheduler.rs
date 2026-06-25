use std::time::Duration;

use tokio::time;
use tracing::{error, info};

use super::continuous_view::ContinuousMaterializer;

pub const DEFAULT_MICRO_BATCH_INTERVAL: Duration = Duration::from_secs(5);

pub async fn run_micro_batch_scheduler(materializer: ContinuousMaterializer) -> anyhow::Result<()> {
    let mut interval = time::interval(DEFAULT_MICRO_BATCH_INTERVAL);
    loop {
        interval.tick().await;
        match materializer.materialize_window().await {
            Ok(Some(batch)) => {
                info!(batch_id = %batch.batch_id, upper_watermark = batch.upper_watermark, "materialized telemetry batch")
            }
            Ok(None) => {}
            Err(error) => error!(?error, "continuous materialization batch failed"),
        }
    }
}
