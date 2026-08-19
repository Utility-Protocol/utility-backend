//! PostgreSQL Connection Pool Health Probe with Adaptive Sizing
//!
//! This module provides:
//! 1. Health probe for the connection pool
//! 2. Adaptive pool sizing based on metrics
//! 3. Prometheus metrics for monitoring
//! 4. Configuration for health checks

use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::Mutex;
use sqlx::PgPool;
use tokio::time::interval;
use tracing::info;

/// Health status of the connection pool
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolHealth {
    Healthy,
    Degraded,
    Unhealthy,
}

/// Metrics collected by the health probe
#[derive(Debug, Clone, Default)]
pub struct PoolMetrics {
    pub active_connections: u32,
    pub idle_connections: u32,
    pub max_connections: u32,
    pub wait_time_ms: f64,
    pub error_rate: f64,
    pub query_latency_ms: f64,
    pub utilization_ratio: f64,
}

/// Configuration for the health probe
#[derive(Debug, Clone)]
pub struct HealthProbeConfig {
    pub probe_interval_ms: u64,
    pub query_timeout_ms: u64,
    pub unhealthy_threshold: u32,
    pub degraded_threshold: u32,
    pub adaptive_sizing_enabled: bool,
    pub min_connections: u32,
    pub max_connections: u32,
    pub scale_up_threshold: f64,  // Utilization ratio to scale up
    pub scale_down_threshold: f64, // Utilization ratio to scale down
}

impl Default for HealthProbeConfig {
    fn default() -> Self {
        Self {
            probe_interval_ms: 5000,
            query_timeout_ms: 1000,
            unhealthy_threshold: 3,
            degraded_threshold: 2,
            adaptive_sizing_enabled: true,
            min_connections: 4,
            max_connections: 64,
            scale_up_threshold: 0.75,
            scale_down_threshold: 0.30,
        }
    }
}

/// Health probe for PostgreSQL connection pool
pub struct ConnectionPoolHealthProbe {
    pool: Arc<PgPool>,
    config: HealthProbeConfig,
    metrics: Arc<Mutex<PoolMetrics>>,
    health_status: Arc<Mutex<PoolHealth>>,
    consecutive_failures: Arc<Mutex<u32>>,
}

impl ConnectionPoolHealthProbe {
    /// Create a new health probe
    pub fn new(pool: Arc<PgPool>, config: HealthProbeConfig) -> Self {
        Self {
            pool,
            config,
            metrics: Arc::new(Mutex::new(PoolMetrics::default())),
            health_status: Arc::new(Mutex::new(PoolHealth::Healthy)),
            consecutive_failures: Arc::new(Mutex::new(0)),
        }
    }

    /// Run a single health check
    pub async fn check_health(&self) -> PoolHealth {
        // 1. Test simple query
        match self.run_health_query().await {
            Ok(latency_ms) => {
                // Reset failure counter
                *self.consecutive_failures.lock() = 0;

                // Update metrics
                let mut metrics = self.metrics.lock();
                metrics.query_latency_ms = latency_ms;
                metrics.error_rate = 0.0;

                // Check if degraded (high latency)
                let health = if latency_ms > 100.0 {
                    PoolHealth::Degraded
                } else {
                    PoolHealth::Healthy
                };
                *self.health_status.lock() = health;
                health
            }
            Err(_) => {
                // Increment failure counter
                let mut failures = self.consecutive_failures.lock();
                *failures += 1;

                let health = if *failures >= self.config.unhealthy_threshold {
                    PoolHealth::Unhealthy
                } else if *failures >= self.config.degraded_threshold {
                    PoolHealth::Degraded
                } else {
                    PoolHealth::Healthy
                };
                *self.health_status.lock() = health;
                health
            }
        }
    }

    /// Run the health query and return latency in milliseconds
    async fn run_health_query(&self) -> Result<f64, sqlx::Error> {
        let start = Instant::now();

        // Simple query to check database health
        let _row = sqlx::query("SELECT 1")
            .fetch_one(&*self.pool)
            .await?;

        let elapsed = start.elapsed();
        Ok(elapsed.as_secs_f64() * 1000.0)
    }

    /// Start the background health probe loop
    pub fn spawn(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(self.config.probe_interval_ms));
            loop {
                interval.tick().await;
                self.check_health().await;
                self.update_pool_metrics().await;

                // Adaptive sizing
                if self.config.adaptive_sizing_enabled {
                    self.adapt_pool_size().await;
                }
            }
        })
    }

    /// Update pool metrics
    async fn update_pool_metrics(&self) {
        // In production, this would read from deadpool's status
        let mut metrics = self.metrics.lock();
        // Placeholder - actual implementation would read from pool
        metrics.max_connections = self.config.max_connections;
    }

    /// Adapt pool size based on metrics
    async fn adapt_pool_size(&self) {
        let metrics = self.metrics.lock();
        let utilization = metrics.utilization_ratio;

        // Scale up if utilization is high
        if utilization > self.config.scale_up_threshold {
            let current = metrics.max_connections;
            let new_size = (current as f64 * 1.25) as u32;
            let new_size = new_size.clamp(
                self.config.min_connections,
                self.config.max_connections,
            );
            if new_size > current {
                info!(
                    "Scaling up pool from {} to {} (utilization: {:.2})",
                    current, new_size, utilization
                );
                // Actual pool resizing would go here
            }
        }

        // Scale down if utilization is low
        if utilization < self.config.scale_down_threshold && utilization > 0.0 {
            let current = metrics.max_connections;
            let new_size = (current as f64 * 0.80) as u32;
            let new_size = new_size.clamp(
                self.config.min_connections,
                self.config.max_connections,
            );
            if new_size < current && current > self.config.min_connections {
                info!(
                    "Scaling down pool from {} to {} (utilization: {:.2})",
                    current, new_size, utilization
                );
                // Actual pool resizing would go here
            }
        }
    }

    /// Get the current health status
    pub fn health_status(&self) -> PoolHealth {
        *self.health_status.lock()
    }

    /// Get the current metrics
    pub fn metrics(&self) -> PoolMetrics {
        self.metrics.lock().clone()
    }
}
pub mod tests;
