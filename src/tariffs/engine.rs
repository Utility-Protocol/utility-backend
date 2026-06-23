use chrono::DateTime;
use chrono::Utc;

pub struct TariffEngine {
    schedules: Vec<TariffSchedule>,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub enum TariffTier {
    Peak,
    OffPeak,
    Shoulder,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct TariffSchedule {
    pub tier: TariffTier,
    pub rate_per_unit: f64,
    pub start_hour: u8,
    pub end_hour: u8,
}

impl TariffEngine {
    pub fn new(schedules: Vec<TariffSchedule>) -> Self {
        Self { schedules }
    }

    pub fn evaluate(&self, timestamp: DateTime<Utc>, volume: f64) -> f64 {
        use chrono::Timelike;
        let hour = timestamp.hour() as u8;
        for schedule in &self.schedules {
            if hour >= schedule.start_hour && hour < schedule.end_hour {
                return volume * schedule.rate_per_unit;
            }
        }
        volume * 0.12
    }

    pub fn evaluate_batch(&self, readings: &[(DateTime<Utc>, f64)]) -> f64 {
        readings
            .iter()
            .map(|(ts, vol)| self.evaluate(*ts, *vol))
            .sum()
    }

    pub async fn evaluate_and_finalize(
        &self,
        batch_id: &str,
        resource_type: &str,
        readings: &[(DateTime<Utc>, f64)],
        finalizer: &crate::settlement::finalizer::Finalizer,
        mint_queue: &crate::settlement::mint_queue::MintQueue,
        destination_wallet: &str,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        let total_cost = self.evaluate_batch(readings);

        // Enqueue the mint event
        mint_queue
            .enqueue(batch_id, resource_type, total_cost, destination_wallet)
            .await?;

        // Trigger finalization
        finalizer.finalize_mint(batch_id, resource_type).await?;

        Ok(total_cost)
    }
}
