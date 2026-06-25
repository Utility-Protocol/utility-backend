//! Integration tests for the adaptive TCP connection lifecycle manager
//! (issue #53).
//!
//! The descriptor-exhaustion scenario is Linux-specific (it lowers
//! `RLIMIT_NOFILE` via `libc::setrlimit` and counts `/proc/self/fd`), so that
//! test is gated to Linux. The connection-manager and rate-limiter invariants
//! are portable and run everywhere.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

use utility_backend::transport::tcp::acceptor::bind_listener;
use utility_backend::transport::tcp::connection_manager::{ConnectionManager, Priority};
use utility_backend::transport::tcp::rate_limiter::ConnectionRateLimiter;

/// Establish a loopback connection and return the *server* side stream. The
/// client side is dropped (freeing its descriptor); the meter side is what the
/// connection manager owns.
async fn server_stream(listener: &TcpListener, addr: SocketAddr) -> TcpStream {
    let (client, accepted) =
        tokio::join!(async { TcpStream::connect(addr).await.unwrap() }, async {
            listener.accept().await.unwrap()
        });
    drop(client);
    accepted.0
}

#[tokio::test]
async fn test_per_meter_single_connection_is_replaced() {
    let cm = Arc::new(ConnectionManager::new(Duration::from_secs(300), 1000));
    let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 128).unwrap();
    let addr = listener.local_addr().unwrap();

    let s1 = server_stream(&listener, addr).await;
    cm.register("meter-A".into(), s1, Priority::Normal).await;
    assert_eq!(cm.active_count(), 1);

    // Re-registering the same meter must replace (RST) the old connection, not
    // grow the registry — the one-connection-per-meter invariant.
    let s2 = server_stream(&listener, addr).await;
    cm.register("meter-A".into(), s2, Priority::Normal).await;
    assert_eq!(cm.active_count(), 1);

    assert!(cm.close("meter-A").await);
    assert_eq!(cm.active_count(), 0);
}

#[tokio::test]
async fn test_evict_idle_and_oldest() {
    // idle_timeout of zero makes every just-registered connection immediately
    // eligible for idle eviction.
    let cm = Arc::new(ConnectionManager::new(Duration::ZERO, 1000));
    let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 128).unwrap();
    let addr = listener.local_addr().unwrap();

    for i in 0..5 {
        let s = server_stream(&listener, addr).await;
        cm.register(format!("meter-{i}"), s, Priority::Normal).await;
    }
    assert_eq!(cm.active_count(), 5);

    let closed = cm.evict_oldest(2).await;
    assert_eq!(closed, 2);
    assert_eq!(cm.active_count(), 3);

    let idle_closed = cm.evict_idle(Duration::ZERO).await;
    assert_eq!(idle_closed, 3);
    assert_eq!(cm.active_count(), 0);
}

#[tokio::test]
async fn test_low_priority_evicted_first() {
    let cm = Arc::new(ConnectionManager::new(Duration::from_secs(300), 1000));
    let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 128).unwrap();
    let addr = listener.local_addr().unwrap();

    let s_low = server_stream(&listener, addr).await;
    cm.register("low".into(), s_low, Priority::Low).await;
    let s_hi = server_stream(&listener, addr).await;
    cm.register("hi".into(), s_hi, Priority::High).await;

    let closed = cm.evict_low_priority().await;
    assert_eq!(closed, 1);
    assert_eq!(cm.active_count(), 1);
    // The high-priority connection survives.
    assert!(cm.close("hi").await);
}

#[tokio::test]
async fn test_rate_limiter_throttles_over_budget() {
    // 5 conns/sec budget over a 1-second window => 6th attempt is over budget.
    let limiter = ConnectionRateLimiter::new(5, 10, Duration::from_secs(1));
    for _ in 0..5 {
        assert!(limiter.try_acquire());
    }
    assert!(!limiter.try_acquire());
    assert!(limiter.current_rate() > 0.0);
}

// ---------------------------------------------------------------------------
// Linux-only FD exhaustion scenario.
// ---------------------------------------------------------------------------

/// Reproduces FD exhaustion by lowering this process's `RLIMIT_NOFILE` to 128.
///
/// This mutates a *process-global* resource and is therefore `#[ignore]`d so it
/// never runs alongside the other (parallel, DB-touching) tests in the
/// aggregated test binary, which would otherwise lose their descriptor budget.
/// Run it in isolation:
///
/// ```text
/// cargo test --test mod fd_exhaustion -- --ignored --test-threads=1
/// ```
///
/// The original limit is saved and restored, and the test bails out early if
/// the process already holds too many descriptors for a 128-FD ceiling to be
/// meaningful.
#[cfg(target_os = "linux")]
#[ignore = "mutates process-global RLIMIT_NOFILE; run in isolation"]
#[tokio::test]
async fn test_fd_exhaustion_is_mitigated() {
    use utility_backend::transport::tcp::config::TcpTransportConfig;
    use utility_backend::transport::tcp::fd_monitor::{current_fd_count, FdMonitor};

    let base = current_fd_count().unwrap();
    if base >= 96 {
        eprintln!("skipping: baseline FD count {base} too high for a 128-FD test");
        return;
    }

    // Save the original limit so we can restore it afterwards.
    let mut original = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    unsafe {
        assert_eq!(libc::getrlimit(libc::RLIMIT_NOFILE, &mut original), 0);
    }

    // Artificially lower the soft limit to 128, keeping the hard limit intact so
    // we can restore it.
    let rlimit_cur: u64 = 128;
    unsafe {
        let rl = libc::rlimit {
            rlim_cur: 128,
            rlim_max: original.rlim_max,
        };
        assert_eq!(libc::setrlimit(libc::RLIMIT_NOFILE, &rl), 0);
    }

    let cfg = TcpTransportConfig::default();
    let cm = Arc::new(ConnectionManager::new(Duration::from_secs(300), 1000));
    let monitor = FdMonitor::new(
        rlimit_cur,
        cfg.fd_soft_limit_ratio,
        cfg.fd_hard_limit_ratio,
        Duration::from_secs(300),
        Duration::from_secs(5),
    );
    let soft = monitor.soft_limit();
    let hard = monitor.hard_limit();

    let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 128).unwrap();
    let addr = listener.local_addr().unwrap();

    // Spawn 200 "fake meter" connections against a 128-FD ceiling. The manager
    // must keep us below the limit and accept() must never fail with EMFILE.
    for i in 0..200u32 {
        // Proactively reclaim before risking the ceiling (models the acceptor
        // consulting the FD monitor before taking on more work).
        if current_fd_count().unwrap() >= soft {
            monitor.tick(&cm).await;
            if current_fd_count().unwrap() >= soft {
                cm.evict_low_priority().await;
            }
        }

        let client = TcpStream::connect(addr).await.expect("connect");
        let accepted = listener.accept().await;
        assert!(
            accepted.is_ok(),
            "accept() must not fail with EMFILE under FD pressure"
        );
        let (server, _) = accepted.unwrap();
        cm.register(format!("meter-{i}"), server, Priority::Low)
            .await;
        drop(client);

        assert!(
            current_fd_count().unwrap() <= hard,
            "FD count {} exceeded hard limit {hard}",
            current_fd_count().unwrap()
        );
    }

    // Reconnecting an existing meter replaces the FD without increasing the count.
    let before = current_fd_count().unwrap();
    let count_before = cm.active_count();
    let s = server_stream(&listener, addr).await;
    cm.register("meter-0".into(), s, Priority::Low).await;
    assert_eq!(
        cm.active_count(),
        count_before,
        "re-register must not grow registry"
    );
    assert!(
        current_fd_count().unwrap() <= before + 1,
        "re-register should not leak descriptors"
    );

    // A full reclamation pass brings descriptor usage back under the soft limit.
    cm.evict_low_priority().await;
    let final_count = current_fd_count().unwrap();

    // Restore the original descriptor limit before asserting so a failure here
    // doesn't leave the process crippled.
    unsafe {
        assert_eq!(libc::setrlimit(libc::RLIMIT_NOFILE, &original), 0);
    }

    assert!(
        final_count < soft,
        "FD count {final_count} not below soft limit {soft} after reclamation"
    );
}
