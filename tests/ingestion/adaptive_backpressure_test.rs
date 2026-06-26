//! Tests for the adaptive window-sizing backpressure scheduler (issue #46).

use proptest::prelude::*;

use utility_backend::ingestion::scheduler::{
    AdaptiveScheduler, LatencyHistogram, SchedulerConfig, Stage, MAX_WINDOW, MIN_WINDOW,
};
use utility_backend::ingestion::windowed_channel::{windowed_channel, TrySendError};

#[test]
fn test_aimd_additive_increase_when_healthy() {
    let sched = AdaptiveScheduler::new(SchedulerConfig::default());
    let windows = sched.tick(); // all stages idle -> additive increase
    for w in windows {
        assert_eq!(w, MIN_WINDOW + 64);
    }
}

#[test]
fn test_aimd_multiplicative_decrease_on_occupancy() {
    let sched = AdaptiveScheduler::new(SchedulerConfig::default());
    for _ in 0..20 {
        sched.tick(); // grow windows while idle
    }
    let before = sched.window(Stage::Accept);
    assert!(before > MIN_WINDOW);

    for stage in Stage::ALL {
        sched.stage(stage).set_occupancy(0.9); // above 0.85
    }
    sched.tick();
    let after = sched.window(Stage::Accept);
    assert!(after < before, "occupancy should shrink window");
    assert!(after >= MIN_WINDOW);
}

#[test]
fn test_aimd_decrease_on_p99_spike() {
    let sched = AdaptiveScheduler::new(SchedulerConfig::default());
    // Establish a steady p99 baseline while growing.
    for _ in 0..10 {
        for stage in Stage::ALL {
            sched.stage(stage).set_p99_latency_ns(1_000);
        }
        sched.tick();
    }
    let before = sched.window(Stage::Accept);

    // A p99 spike past 2x baseline triggers a decrease.
    for stage in Stage::ALL {
        sched.stage(stage).set_p99_latency_ns(5_000);
    }
    sched.tick();
    assert!(
        sched.window(Stage::Accept) < before,
        "p99 spike should shrink window"
    );
}

#[test]
fn test_global_budget_is_enforced() {
    let cfg = SchedulerConfig {
        budget_slots: 6_000,
        ..SchedulerConfig::default()
    };
    let sched = AdaptiveScheduler::new(cfg);
    for _ in 0..50 {
        sched.tick(); // grow past the budget; enforcement scales down
        assert!(
            sched.total_window() <= 6_000,
            "total {} exceeds budget",
            sched.total_window()
        );
    }
    for stage in Stage::ALL {
        assert!(sched.window(stage) >= MIN_WINDOW);
    }
}

#[tokio::test]
async fn test_windowed_channel_gating_and_resize() {
    let (tx, mut rx) = windowed_channel::<u32>(2);
    assert_eq!(tx.window(), 2);
    assert_eq!(tx.available_credits(), 2);

    assert!(tx.try_send(1).is_ok());
    assert!(tx.try_send(2).is_ok());
    assert_eq!(tx.available_credits(), 0);
    assert_eq!(tx.try_send(3), Err(TrySendError::Full));

    // Receiving returns a credit.
    assert_eq!(rx.try_recv(), Some(1));
    assert_eq!(tx.available_credits(), 1);
    assert!(tx.try_send(3).is_ok());

    // Grow the window: 2 items still in flight, so 3 new credits become free.
    tx.resize(5);
    assert_eq!(tx.window(), 5);
    assert_eq!(tx.available_credits(), 3);
}

#[tokio::test]
async fn test_windowed_channel_blocking_send_unblocks_on_recv() {
    let (tx, mut rx) = windowed_channel::<u32>(1);
    tx.send(1).await.unwrap(); // consumes the only credit
    assert_eq!(tx.available_credits(), 0);

    // A second send blocks until a receive frees the credit.
    let sender = tx.clone();
    let task = tokio::spawn(async move { sender.send(2).await });
    // Drain, which replenishes the credit and unblocks the sender.
    assert_eq!(rx.recv().await, Some(1));
    task.await.unwrap().unwrap();
    assert_eq!(rx.recv().await, Some(2));
}

#[test]
fn test_latency_histogram_quantiles_ordered() {
    let mut hist = LatencyHistogram::new();
    for _ in 0..90 {
        hist.record(100); // ~64ns bucket
    }
    for _ in 0..10 {
        hist.record(1_000_000); // ~512us bucket
    }
    assert_eq!(hist.total(), 100);

    let p50 = hist.value_at_quantile(0.5);
    let p99 = hist.value_at_quantile(0.99);
    assert!(p50 > 0);
    assert!(p99 > p50, "p99 {p99} should exceed p50 {p50}");

    hist.clear();
    assert_eq!(hist.total(), 0);
    assert_eq!(hist.value_at_quantile(0.99), 0);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Across any sequence of (occupancy, p99) inputs, windows stay within
    /// [MIN, MAX] and the total never exceeds the budget.
    #[test]
    fn prop_windows_within_bounds_and_budget(
        inputs in prop::collection::vec((0.0f64..1.0, 0u64..5_000_000_000u64), 1..30)
    ) {
        let sched = AdaptiveScheduler::new(SchedulerConfig::default());
        let budget = SchedulerConfig::default().budget_slots;
        for (occ, p99) in inputs {
            for stage in Stage::ALL {
                sched.stage(stage).set_occupancy(occ);
                sched.stage(stage).set_p99_latency_ns(p99);
            }
            let windows = sched.tick();
            for w in windows {
                prop_assert!(w >= MIN_WINDOW, "window {w} below min");
                prop_assert!(w <= MAX_WINDOW, "window {w} above max");
            }
            let total: u64 = windows.iter().sum();
            prop_assert!(total <= budget.max(5 * MIN_WINDOW), "total {total} over budget");
        }
    }
}
