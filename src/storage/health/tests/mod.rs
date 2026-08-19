//! Unit tests for the PostgreSQL connection pool health probe.

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use sqlx::PgPool;
    use tokio::time::sleep;

    use crate::storage::health::{
        ConnectionPoolHealthProbe, HealthProbeConfig, PoolHealth,
    };

    /// Check if database is available
    async fn db_available() -> bool {
        let db_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://utility:utility_secret@localhost:5432/utility_test".into());
        PgPool::connect(&db_url).await.is_ok()
    }

    /// Create a test database pool if available
    async fn test_pool() -> Option<Arc<PgPool>> {
        let db_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://utility:utility_secret@localhost:5432/utility_test".into());
        match PgPool::connect(&db_url).await {
            Ok(pool) => Some(Arc::new(pool)),
            Err(e) => {
                eprintln!("Skipping test: database not available - {}", e);
                None
            }
        }
    }

    #[tokio::test]
    async fn test_config_defaults() {
        let config = HealthProbeConfig::default();
        assert_eq!(config.probe_interval_ms, 5000);
        assert_eq!(config.query_timeout_ms, 1000);
        assert_eq!(config.unhealthy_threshold, 3);
        assert_eq!(config.degraded_threshold, 2);
        assert_eq!(config.adaptive_sizing_enabled, true);
        assert_eq!(config.min_connections, 4);
        assert_eq!(config.max_connections, 64);
        assert_eq!(config.scale_up_threshold, 0.75);
        assert_eq!(config.scale_down_threshold, 0.30);
    }

    #[tokio::test]
    async fn test_config_custom() {
        let config = HealthProbeConfig {
            probe_interval_ms: 1000,
            query_timeout_ms: 500,
            unhealthy_threshold: 5,
            degraded_threshold: 3,
            adaptive_sizing_enabled: false,
            min_connections: 2,
            max_connections: 32,
            scale_up_threshold: 0.80,
            scale_down_threshold: 0.20,
        };

        assert_eq!(config.probe_interval_ms, 1000);
        assert_eq!(config.query_timeout_ms, 500);
        assert_eq!(config.unhealthy_threshold, 5);
        assert_eq!(config.degraded_threshold, 3);
        assert_eq!(config.adaptive_sizing_enabled, false);
        assert_eq!(config.min_connections, 2);
        assert_eq!(config.max_connections, 32);
    }

    #[tokio::test]
    async fn test_adaptive_sizing_scale_up() {
        let pool = match test_pool().await {
            Some(p) => p,
            None => {
                eprintln!("Skipping test: database not available");
                return;
            }
        };
        let config = HealthProbeConfig::default();
        let probe = ConnectionPoolHealthProbe::new(pool, config);

        // Simulate high utilization
        {
            let mut metrics = probe.metrics.lock();
            metrics.utilization_ratio = 0.85;
            metrics.max_connections = 16;
        }

        probe.adapt_pool_size().await;

        let metrics = probe.metrics();
        // Should scale up from 16 to 20 (16 * 1.25)
        assert_eq!(metrics.max_connections, 20);
    }

    #[tokio::test]
    async fn test_adaptive_sizing_scale_down() {
        let pool = match test_pool().await {
            Some(p) => p,
            None => {
                eprintln!("Skipping test: database not available");
                return;
            }
        };
        let config = HealthProbeConfig::default();
        let probe = ConnectionPoolHealthProbe::new(pool, config);

        // Simulate low utilization
        {
            let mut metrics = probe.metrics.lock();
            metrics.utilization_ratio = 0.20;
            metrics.max_connections = 32;
        }

        probe.adapt_pool_size().await;

        let metrics = probe.metrics();
        // Should scale down from 32 to 25 (32 * 0.80)
        assert_eq!(metrics.max_connections, 25);
    }

    #[tokio::test]
    async fn test_adaptive_sizing_no_scale_below_min() {
        let pool = match test_pool().await {
            Some(p) => p,
            None => {
                eprintln!("Skipping test: database not available");
                return;
            }
        };
        let config = HealthProbeConfig::default();
        let probe = ConnectionPoolHealthProbe::new(pool, config);

        // Simulate low utilization at min connections
        {
            let mut metrics = probe.metrics.lock();
            metrics.utilization_ratio = 0.10;
            metrics.max_connections = 4; // min_connections
        }

        probe.adapt_pool_size().await;

        let metrics = probe.metrics();
        // Should not scale below min
        assert_eq!(metrics.max_connections, 4);
    }

    #[tokio::test]
    async fn test_adaptive_sizing_no_scale_above_max() {
        let pool = match test_pool().await {
            Some(p) => p,
            None => {
                eprintln!("Skipping test: database not available");
                return;
            }
        };
        let config = HealthProbeConfig::default();
        let probe = ConnectionPoolHealthProbe::new(pool, config);

        // Simulate high utilization at max connections
        {
            let mut metrics = probe.metrics.lock();
            metrics.utilization_ratio = 0.90;
            metrics.max_connections = 64; // max_connections
        }

        probe.adapt_pool_size().await;

        let metrics = probe.metrics();
        // Should not scale above max
        assert_eq!(metrics.max_connections, 64);
    }

    // Unit tests that don't need database
    #[test]
    fn test_pool_health_status_equality() {
        assert_eq!(PoolHealth::Healthy, PoolHealth::Healthy);
        assert_ne!(PoolHealth::Healthy, PoolHealth::Degraded);
        assert_ne!(PoolHealth::Healthy, PoolHealth::Unhealthy);
    }

    #[test]
    fn test_health_probe_config_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HealthProbeConfig>();
        assert_send_sync::<PoolHealth>();
    }
}
