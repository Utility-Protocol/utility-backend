use serde::Serialize;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct SloConfig {
    pub availability_target: f64,
    pub latency_target: f64,
    pub latency_threshold: Duration,
    pub fast_window: Duration,
    pub slow_window: Duration,
    pub fast_burn_threshold: f64,
    pub slow_burn_threshold: f64,
}

impl Default for SloConfig {
    fn default() -> Self {
        Self {
            availability_target: 0.9999,
            latency_target: 0.99,
            latency_threshold: Duration::from_millis(100),
            fast_window: Duration::from_secs(5 * 60),
            slow_window: Duration::from_secs(60 * 60),
            fast_burn_threshold: 14.4,
            slow_burn_threshold: 6.0,
        }
    }
}

#[derive(Clone, Debug)]
struct RequestSample {
    at: Instant,
    success: bool,
    latency: Duration,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub enum AlertSeverity {
    Page,
    Ticket,
    Healthy,
}

#[derive(Clone, Debug, Serialize)]
pub struct SloWindowStatus {
    pub window_seconds: u64,
    pub total_requests: u64,
    pub availability: f64,
    pub latency_compliance: f64,
    pub availability_burn_rate: f64,
    pub latency_burn_rate: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct SloStatus {
    pub availability_target: f64,
    pub latency_target: f64,
    pub latency_threshold_ms: u64,
    pub fast_window: SloWindowStatus,
    pub slow_window: SloWindowStatus,
    pub severity: AlertSeverity,
    pub alert_active: bool,
}

#[derive(Debug)]
pub struct SloMonitor {
    config: SloConfig,
    samples: VecDeque<RequestSample>,
}

impl SloMonitor {
    pub fn new(config: SloConfig) -> Self {
        Self {
            config,
            samples: VecDeque::new(),
        }
    }

    pub fn record_request(
        &mut self,
        status_code: u16,
        latency: Duration,
        now: Instant,
    ) -> SloStatus {
        let success = status_code < 500;
        self.samples.push_back(RequestSample {
            at: now,
            success,
            latency,
        });
        self.prune(now);
        self.status_at(now)
    }

    pub fn status(&mut self) -> SloStatus {
        let now = Instant::now();
        self.prune(now);
        self.status_at(now)
    }

    fn prune(&mut self, now: Instant) {
        while self
            .samples
            .front()
            .is_some_and(|s| now.duration_since(s.at) > self.config.slow_window)
        {
            self.samples.pop_front();
        }
    }

    fn window_status(&self, now: Instant, window: Duration) -> SloWindowStatus {
        let mut total = 0_u64;
        let mut failures = 0_u64;
        let mut slow = 0_u64;
        for sample in self
            .samples
            .iter()
            .filter(|s| now.duration_since(s.at) <= window)
        {
            total += 1;
            if !sample.success {
                failures += 1;
            }
            if sample.latency > self.config.latency_threshold {
                slow += 1;
            }
        }
        let availability = ratio(total - failures, total);
        let latency_compliance = ratio(total - slow, total);
        SloWindowStatus {
            window_seconds: window.as_secs(),
            total_requests: total,
            availability,
            latency_compliance,
            availability_burn_rate: burn_rate(
                1.0 - availability,
                1.0 - self.config.availability_target,
            ),
            latency_burn_rate: burn_rate(
                1.0 - latency_compliance,
                1.0 - self.config.latency_target,
            ),
        }
    }

    fn status_at(&self, now: Instant) -> SloStatus {
        let fast_window = self.window_status(now, self.config.fast_window);
        let slow_window = self.window_status(now, self.config.slow_window);
        let fast_burn = fast_window
            .availability_burn_rate
            .max(fast_window.latency_burn_rate);
        let slow_burn = slow_window
            .availability_burn_rate
            .max(slow_window.latency_burn_rate);
        let severity = if fast_burn >= self.config.fast_burn_threshold
            && slow_burn >= self.config.slow_burn_threshold
        {
            AlertSeverity::Page
        } else if slow_burn >= self.config.slow_burn_threshold {
            AlertSeverity::Ticket
        } else {
            AlertSeverity::Healthy
        };
        let alert_active = severity != AlertSeverity::Healthy;
        SloStatus {
            availability_target: self.config.availability_target,
            latency_target: self.config.latency_target,
            latency_threshold_ms: self.config.latency_threshold.as_millis() as u64,
            fast_window,
            slow_window,
            severity,
            alert_active,
        }
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        1.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn burn_rate(error_ratio: f64, error_budget: f64) -> f64 {
    if error_budget <= f64::EPSILON {
        0.0
    } else {
        error_ratio / error_budget
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn monitor() -> SloMonitor {
        SloMonitor::new(SloConfig {
            fast_window: Duration::from_secs(60),
            slow_window: Duration::from_secs(600),
            ..SloConfig::default()
        })
    }

    #[test]
    fn healthy_traffic_does_not_alert() {
        let mut slo = monitor();
        let now = Instant::now();
        for i in 0..100 {
            slo.record_request(200, Duration::from_millis(20), now + Duration::from_secs(i));
        }
        let status = slo.status_at(now + Duration::from_secs(100));
        assert_eq!(status.severity, AlertSeverity::Healthy);
        assert_eq!(status.slow_window.availability, 1.0);
    }

    #[test]
    fn sustained_failures_page_on_multi_window_burn() {
        let mut slo = monitor();
        let now = Instant::now();
        for i in 0..100 {
            slo.record_request(
                if i >= 90 { 500 } else { 200 },
                Duration::from_millis(20),
                now + Duration::from_secs(i),
            );
        }
        let status = slo.status_at(now + Duration::from_secs(100));
        assert_eq!(status.severity, AlertSeverity::Page);
        assert!(status.fast_window.availability_burn_rate > 14.4);
    }

    #[test]
    fn slow_requests_consume_latency_error_budget() {
        let mut slo = monitor();
        let now = Instant::now();
        slo.record_request(200, Duration::from_millis(150), now);
        let status = slo.status_at(now);
        assert!(status.slow_window.latency_burn_rate >= 99.99);
    }
}
