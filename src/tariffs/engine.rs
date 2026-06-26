use chrono::DateTime;
use chrono::Utc;

pub struct TariffEngine {
    schedules: Vec<TariffSchedule>,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock, RwLock};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TariffTier {
    Peak,
    OffPeak,
    Shoulder,
    Holiday,
    Congestion,
    Dynamic,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct TariffSchedule {
    pub tier: TariffTier,
    pub rate_per_unit: f64,
    pub start_hour: u8,
    pub end_hour: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TariffContext {
    pub meter_id: String,
    pub timestamp: DateTime<Utc>,
    pub volume: f64,
    pub consumption_tier: Option<String>,
    pub grid_congestion_level: Option<u8>,
    pub is_holiday: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum TariffCondition {
    Always,
    TimeWindow { start_hour: u8, end_hour: u8 },
    VolumeAtLeast { minimum: f64 },
    ConsumptionTier { tier: String },
    CongestionAtLeast { level: u8 },
    Holiday,
    Weekday { iso_weekday: u32 },
    And { conditions: Vec<TariffCondition> },
    Or { conditions: Vec<TariffCondition> },
    Not { condition: Box<TariffCondition> },
}

impl TariffCondition {
    pub fn matches(&self, ctx: &TariffContext) -> bool {
        match self {
            Self::Always => true,
            Self::TimeWindow {
                start_hour,
                end_hour,
            } => {
                let hour = ctx.timestamp.hour() as u8;
                if start_hour <= end_hour {
                    hour >= *start_hour && hour < *end_hour
                } else {
                    hour >= *start_hour || hour < *end_hour
                }
            }
            Self::VolumeAtLeast { minimum } => ctx.volume >= *minimum,
            Self::ConsumptionTier { tier } => ctx.consumption_tier.as_ref() == Some(tier),
            Self::CongestionAtLeast { level } => {
                ctx.grid_congestion_level.is_some_and(|v| v >= *level)
            }
            Self::Holiday => ctx.is_holiday,
            Self::Weekday { iso_weekday } => {
                ctx.timestamp.weekday().number_from_monday() == *iso_weekday
            }
            Self::And { conditions } => conditions.iter().all(|c| c.matches(ctx)),
            Self::Or { conditions } => conditions.iter().any(|c| c.matches(ctx)),
            Self::Not { condition } => !condition.matches(ctx),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RateExpression {
    PerUnit { rate: f64 },
    Surcharge { amount: f64 },
    Multiplier { factor: f64 },
}

impl RateExpression {
    fn amount(&self, running_cost: f64, ctx: &TariffContext) -> f64 {
        match self {
            Self::PerUnit { rate } => ctx.volume * rate,
            Self::Surcharge { amount } => ctx.volume * amount,
            Self::Multiplier { factor } => running_cost * (factor - 1.0),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TariffRule {
    pub id: String,
    pub description: String,
    pub tier: TariffTier,
    pub condition: TariffCondition,
    pub rate: RateExpression,
    #[serde(default)]
    pub priority: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TariffDsl {
    pub version: String,
    pub rules: Vec<TariffRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppliedTariffRule {
    pub id: String,
    pub description: String,
    pub tier: TariffTier,
    pub amount: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TariffExplanation {
    pub meter_id: String,
    pub timestamp: DateTime<Utc>,
    pub volume: f64,
    pub total_cost: f64,
    pub applied_rules: Vec<AppliedTariffRule>,
}

#[derive(Debug, Clone)]
struct TariffDag {
    rules: Vec<TariffRule>,
}

impl TariffDag {
    fn new(mut rules: Vec<TariffRule>) -> Self {
        rules.sort_by_key(|rule| rule.priority);
        Self { rules }
    }

    fn evaluate(&self, ctx: &TariffContext) -> TariffExplanation {
        let mut total_cost = 0.0;
        let mut applied_rules = Vec::new();
        for rule in &self.rules {
            if rule.condition.matches(ctx) {
                let amount = rule.rate.amount(total_cost, ctx);
                total_cost += amount;
                applied_rules.push(AppliedTariffRule {
                    id: rule.id.clone(),
                    description: rule.description.clone(),
                    tier: rule.tier.clone(),
                    amount,
                });
            }
        }
        TariffExplanation {
            meter_id: ctx.meter_id.clone(),
            timestamp: ctx.timestamp,
            volume: ctx.volume,
            total_cost,
            applied_rules,
        }
    }
}

#[derive(Clone)]
pub struct TariffEngine {
    dag: Arc<RwLock<Arc<TariffDag>>>,
}

impl TariffEngine {
    pub fn new(schedules: Vec<TariffSchedule>) -> Self {
        let rules = schedules
            .into_iter()
            .enumerate()
            .map(|(priority, schedule)| TariffRule {
                id: format!(
                    "legacy-{:?}-{}-{}",
                    schedule.tier, schedule.start_hour, schedule.end_hour
                ),
                description: format!("legacy {:?} time-of-use rate", schedule.tier),
                tier: schedule.tier,
                condition: TariffCondition::TimeWindow {
                    start_hour: schedule.start_hour,
                    end_hour: schedule.end_hour,
                },
                rate: RateExpression::PerUnit {
                    rate: schedule.rate_per_unit,
                },
                priority: priority as u16,
            })
            .collect();
        Self::from_rules(rules)
    }

    pub fn from_rules(rules: Vec<TariffRule>) -> Self {
        Self {
            dag: Arc::new(RwLock::new(Arc::new(TariffDag::new(rules)))),
        }
    }

    pub fn from_json(json: &str) -> serde_json::Result<Self> {
        let dsl: TariffDsl = serde_json::from_str(json)?;
        Ok(Self::from_rules(dsl.rules))
    }

    pub fn reload_from_json(&self, json: &str) -> serde_json::Result<()> {
        let dsl: TariffDsl = serde_json::from_str(json)?;
        self.reload_rules(dsl.rules);
        Ok(())
    }

    pub fn reload_rules(&self, rules: Vec<TariffRule>) {
        let new_dag = Arc::new(TariffDag::new(rules));
        *self.dag.write().expect("tariff DAG lock poisoned") = new_dag;
    }

    pub fn evaluate_context(&self, ctx: TariffContext) -> TariffExplanation {
        let dag = self.dag.read().expect("tariff DAG lock poisoned").clone();
        let explanation = dag.evaluate(&ctx);
        info!(meter_id = %ctx.meter_id, cost = explanation.total_cost, rules = explanation.applied_rules.len(), "tariff evaluated");
        explanation
    }

    pub fn explain(&self, ctx: TariffContext) -> TariffExplanation {
        self.evaluate_context(ctx)
    }

    pub fn evaluate(&self, timestamp: DateTime<Utc>, volume: f64) -> f64 {
        use chrono::Timelike;
        let hour = timestamp.hour() as u8;
        for schedule in &self.schedules {
            if hour >= schedule.start_hour && hour < schedule.end_hour {
                return volume * schedule.rate_per_unit;
            }
        let ctx = TariffContext {
            meter_id: "legacy".into(),
            timestamp,
            volume,
            ..Default::default()
        };
        let cost = self.evaluate_context(ctx).total_cost;
        if cost == 0.0 {
            volume * 0.12
        } else {
            cost
        }
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

pub fn global_tariff_engine() -> &'static TariffEngine {
    static ENGINE: OnceLock<TariffEngine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        TariffEngine::new(vec![
            TariffSchedule {
                tier: TariffTier::Peak,
                rate_per_unit: 0.15,
                start_hour: 16,
                end_hour: 21,
            },
            TariffSchedule {
                tier: TariffTier::Shoulder,
                rate_per_unit: 0.11,
                start_hour: 6,
                end_hour: 16,
            },
            TariffSchedule {
                tier: TariffTier::OffPeak,
                rate_per_unit: 0.08,
                start_hour: 21,
                end_hour: 6,
            },
        ])
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlaps_holiday_and_peak_surcharges() {
        let engine = TariffEngine::from_rules(vec![
            TariffRule {
                id: "peak".into(),
                description: "peak base".into(),
                tier: TariffTier::Peak,
                condition: TariffCondition::TimeWindow {
                    start_hour: 17,
                    end_hour: 21,
                },
                rate: RateExpression::PerUnit { rate: 0.25 },
                priority: 1,
            },
            TariffRule {
                id: "holiday".into(),
                description: "holiday surcharge".into(),
                tier: TariffTier::Holiday,
                condition: TariffCondition::Holiday,
                rate: RateExpression::Surcharge { amount: 0.05 },
                priority: 2,
            },
        ]);
        let ctx = TariffContext {
            meter_id: "MTR-1".into(),
            timestamp: "2026-12-25T18:00:00Z".parse().unwrap(),
            volume: 100.0,
            is_holiday: true,
            ..Default::default()
        };
        let explanation = engine.explain(ctx);
        assert!((explanation.total_cost - 30.0).abs() < 0.001);
        assert_eq!(explanation.applied_rules.len(), 2);
    }
}
