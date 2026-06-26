//! Tests for the graceful shutdown protocol (issue #49).
//!
//! These drive the protocol directly (no real signals/devnet) but cover every
//! invariant: cancellation hierarchy, per-stage draining, ordered phase
//! shutdown, per-phase timeout handling, the in-flight ceiling, and durable
//! checkpoint persistence.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use utility_backend::lifecycle::shutdown::{
    ShutdownConfig, ShutdownError, ShutdownPhase, ShutdownProtocol,
};
use utility_backend::lifecycle::task_group::{CancelToken, StructuredTaskGroup};
use utility_backend::storage::checkpoint::{CheckpointStore, ShutdownCheckpoint};

fn temp_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("util_shutdown_{}_{tag}", std::process::id()))
}

#[tokio::test]
async fn test_cancel_token_hierarchy() {
    let root = CancelToken::new();
    let child = root.child();
    assert!(!child.is_cancelled());

    root.cancel();
    assert!(child.is_cancelled());
    child.cancelled().await; // resolves immediately

    // A child created after cancellation is already cancelled.
    assert!(root.child().is_cancelled());
}

#[tokio::test]
async fn test_task_group_drains_on_cancel() {
    let group = StructuredTaskGroup::new(CancelToken::new());
    let done = Arc::new(AtomicUsize::new(0));
    for _ in 0..5 {
        let token = group.token().clone();
        let done = done.clone();
        group.spawn(async move {
            token.cancelled().await;
            done.fetch_add(1, Ordering::SeqCst);
        });
    }
    assert_eq!(group.in_flight(), 5);

    let remaining = group.shutdown(Duration::from_secs(5)).await;
    assert_eq!(remaining, 0);
    assert_eq!(done.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn test_task_group_reports_timeout() {
    let group = StructuredTaskGroup::new(CancelToken::new());
    // Ignores cancellation and outlives the deadline.
    group.spawn(async {
        tokio::time::sleep(Duration::from_secs(30)).await;
    });
    let remaining = group.shutdown(Duration::from_millis(50)).await;
    assert_eq!(remaining, 1);
}

#[tokio::test]
async fn test_shutdown_drains_phases_in_order() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let cfg = ShutdownConfig {
        per_phase_timeout: Duration::from_secs(5),
        max_in_flight: 1_000_000,
        marker_path: temp_path("marker"),
        checkpoint_path: Some(temp_path("cp")),
    };
    let proto = ShutdownProtocol::new(cfg.clone());

    for phase in ShutdownPhase::DRAIN_ORDER {
        let group = proto.group(phase);
        let token = group.token().clone();
        let order = order.clone();
        group.spawn(async move {
            token.cancelled().await;
            order.lock().push(phase);
        });
    }

    let result = proto.shutdown().await;
    assert!(result.is_ok(), "expected clean shutdown, got {result:?}");
    assert_eq!(*order.lock(), ShutdownPhase::DRAIN_ORDER.to_vec());
    assert!(
        cfg.marker_path.exists(),
        "completion marker must be written"
    );

    let checkpoint = CheckpointStore::open(cfg.checkpoint_path.as_ref().unwrap())
        .load()
        .unwrap()
        .expect("checkpoint persisted");
    for phase in ShutdownPhase::DRAIN_ORDER {
        assert!(
            checkpoint.is_stage_drained(phase.index() as u8),
            "stage {phase:?} should be marked drained"
        );
    }

    let _ = std::fs::remove_file(&cfg.marker_path);
    let _ = std::fs::remove_file(cfg.checkpoint_path.as_ref().unwrap());
}

#[tokio::test]
async fn test_shutdown_reports_phase_timeout_but_completes() {
    let cfg = ShutdownConfig {
        per_phase_timeout: Duration::from_millis(80),
        max_in_flight: 1_000_000,
        marker_path: temp_path("marker_to"),
        checkpoint_path: None,
    };
    let proto = ShutdownProtocol::new(cfg.clone());
    // The parsing phase has a task that ignores cancellation.
    proto.group(ShutdownPhase::Parsing).spawn(async {
        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    match proto.shutdown().await {
        Err(ShutdownError::PhaseTimeout(phases)) => {
            assert_eq!(phases, vec![ShutdownPhase::Parsing]);
        }
        other => panic!("expected PhaseTimeout, got {other:?}"),
    }
    // Shutdown still completes and writes the marker after a phase timeout.
    assert!(cfg.marker_path.exists());
    let _ = std::fs::remove_file(&cfg.marker_path);
}

#[tokio::test]
async fn test_shutdown_aborts_when_in_flight_exceeds_ceiling() {
    let cfg = ShutdownConfig {
        per_phase_timeout: Duration::from_millis(100),
        max_in_flight: 2,
        marker_path: temp_path("marker_oom"),
        checkpoint_path: None,
    };
    let proto = ShutdownProtocol::new(cfg);
    // Five never-finishing tasks parked in a later stage exceed the ceiling.
    let group = proto.group(ShutdownPhase::Blockchain);
    for _ in 0..5 {
        group.spawn(async {
            std::future::pending::<()>().await;
        });
    }
    assert_eq!(proto.total_in_flight(), 5);

    let result = proto.shutdown().await;
    assert!(matches!(result, Err(ShutdownError::InFlightExceeded(5))));
}

#[test]
fn test_checkpoint_codec_and_durable_store() {
    let mut cp = ShutdownCheckpoint::new();
    cp.timestamp_ns = 1_700_000_000_000;
    cp.mark_stage(0);
    cp.mark_stage(3);
    cp.set_watermark("meter-a", 100);
    cp.set_watermark("meter-b", 250);

    assert!(cp.is_stage_drained(0));
    assert!(cp.is_stage_drained(3));
    assert!(!cp.is_stage_drained(1));

    let bytes = cp.to_bytes();
    assert_eq!(ShutdownCheckpoint::from_bytes(&bytes), Some(cp.clone()));
    assert!(ShutdownCheckpoint::from_bytes(&bytes[..bytes.len() - 1]).is_none());

    let path = temp_path("cpfile");
    let store = CheckpointStore::open(&path);
    assert!(store.load().unwrap().is_none(), "absent before first save");
    store.save(&cp).unwrap();
    assert_eq!(store.load().unwrap(), Some(cp));

    let _ = std::fs::remove_file(&path);
}
