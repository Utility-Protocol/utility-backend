use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// A single historical utilization observation for a system resource.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UsageSample {
    pub service: String,
    pub resource: ResourceKind,
    pub used: f64,
    pub capacity: f64,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Cpu,
    Memory,
    Storage,
    Connections,
    QueueDepth,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapacityPlanningConfig {
    pub warning_utilization: f64,
    pub critical_utilization: f64,
    pub forecast_horizon_days: i64,
    pub min_samples: usize,
}

impl Default for CapacityPlanningConfig {
    fn default() -> Self {
        Self {
            warning_utilization: 0.70,
            critical_utilization: 0.85,
            forecast_horizon_days: 30,
            min_samples: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapacityForecast {
    pub service: String,
    pub resource: ResourceKind,
    pub current_utilization: f64,
    pub projected_utilization: f64,
    pub daily_growth_rate: f64,
    pub days_to_warning: Option<f64>,
    pub days_to_critical: Option<f64>,
    pub recommendation: CapacityRecommendation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CapacityRecommendation {
    Stable,
    Monitor,
    ScaleWithinHorizon,
    ScaleImmediately,
    InsufficientData,
}

pub struct CapacityPlanner {
    config: CapacityPlanningConfig,
}

impl CapacityPlanner {
    pub fn new(config: CapacityPlanningConfig) -> Self {
        Self { config }
    }

    pub fn forecast(&self, samples: &[UsageSample]) -> Vec<CapacityForecast> {
        let mut groups: std::collections::BTreeMap<String, Vec<UsageSample>> = Default::default();
        for sample in samples.iter().filter(|s| s.capacity > 0.0 && s.used >= 0.0) {
            groups
                .entry(format!("{}::{:?}", sample.service, sample.resource))
                .or_default()
                .push(sample.clone());
        }

        groups
            .into_values()
            .map(|mut group| {
                group.sort_by_key(|s| s.observed_at);
                self.forecast_group(&group)
            })
            .collect()
    }

    fn forecast_group(&self, samples: &[UsageSample]) -> CapacityForecast {
        let latest = samples.last().expect("forecast groups are non-empty");
        let current = utilization(latest);
        if samples.len() < self.config.min_samples {
            return CapacityForecast {
                service: latest.service.clone(),
                resource: latest.resource.clone(),
                current_utilization: current,
                projected_utilization: current,
                daily_growth_rate: 0.0,
                days_to_warning: None,
                days_to_critical: None,
                recommendation: CapacityRecommendation::InsufficientData,
            };
        }

        let first_ts = samples.first().unwrap().observed_at;
        let xs: Vec<f64> = samples
            .iter()
            .map(|s| (s.observed_at - first_ts).num_seconds() as f64 / 86_400.0)
            .collect();
        let ys: Vec<f64> = samples.iter().map(utilization).collect();
        let slope = linear_regression_slope(&xs, &ys).max(0.0);
        let projected = (current + slope * self.config.forecast_horizon_days as f64).max(0.0);
        let days_to_warning = days_to_threshold(current, slope, self.config.warning_utilization);
        let days_to_critical = days_to_threshold(current, slope, self.config.critical_utilization);
        let recommendation = if current >= self.config.critical_utilization {
            CapacityRecommendation::ScaleImmediately
        } else if projected >= self.config.critical_utilization {
            CapacityRecommendation::ScaleWithinHorizon
        } else if projected >= self.config.warning_utilization {
            CapacityRecommendation::Monitor
        } else {
            CapacityRecommendation::Stable
        };

        CapacityForecast {
            service: latest.service.clone(),
            resource: latest.resource.clone(),
            current_utilization: current,
            projected_utilization: projected,
            daily_growth_rate: slope,
            days_to_warning,
            days_to_critical,
            recommendation,
        }
    }
}

fn utilization(sample: &UsageSample) -> f64 {
    (sample.used / sample.capacity).clamp(0.0, 10.0)
}

fn days_to_threshold(current: f64, daily_growth_rate: f64, threshold: f64) -> Option<f64> {
    if current >= threshold {
        Some(0.0)
    } else if daily_growth_rate > f64::EPSILON {
        Some((threshold - current) / daily_growth_rate)
    } else {
        None
    }
}

fn linear_regression_slope(xs: &[f64], ys: &[f64]) -> f64 {
    let n = xs.len() as f64;
    let mean_x = xs.iter().sum::<f64>() / n;
    let mean_y = ys.iter().sum::<f64>() / n;
    let numerator: f64 = xs
        .iter()
        .zip(ys)
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum();
    let denominator: f64 = xs.iter().map(|x| (x - mean_x).powi(2)).sum();
    if denominator.abs() <= f64::EPSILON {
        0.0
    } else {
        numerator / denominator
    }
}

pub fn sample_usage_window(now: DateTime<Utc>) -> Vec<UsageSample> {
    (0..7)
        .map(|day| UsageSample {
            service: "ingestion".to_string(),
            resource: ResourceKind::Cpu,
            used: 55.0 + day as f64 * 3.0,
            capacity: 100.0,
            observed_at: now - Duration::days(6 - day),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forecasts_growth_and_scale_recommendation() {
        let now = Utc::now();
        let samples = sample_usage_window(now);
        let forecast = CapacityPlanner::new(CapacityPlanningConfig::default())
            .forecast(&samples)
            .pop()
            .unwrap();

        assert_eq!(
            forecast.recommendation,
            CapacityRecommendation::ScaleWithinHorizon
        );
        assert!(forecast.daily_growth_rate > 0.02);
        assert!(forecast.days_to_critical.unwrap() < 10.0);
    }

    #[test]
    fn requires_minimum_history() {
        let now = Utc::now();
        let samples = vec![UsageSample {
            service: "api".into(),
            resource: ResourceKind::Memory,
            used: 1.0,
            capacity: 10.0,
            observed_at: now,
        }];

        let forecast = CapacityPlanner::new(CapacityPlanningConfig::default())
            .forecast(&samples)
            .pop()
            .unwrap();

        assert_eq!(
            forecast.recommendation,
            CapacityRecommendation::InsufficientData
        );
    }
}
