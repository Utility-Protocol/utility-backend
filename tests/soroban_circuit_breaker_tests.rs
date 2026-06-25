use std::time::{Duration, Instant};

use utility_backend::soroban::rpc::{CircuitBreaker, CircuitState};

#[test]
fn circuit_breaker_cycles_closed_to_half_open_to_closed() {
    let mut breaker = CircuitBreaker::new(3);
    assert_eq!(breaker.state(), CircuitState::Closed);

    for _ in 0..10 {
        breaker.record_success_for_test(Duration::from_millis(3_500));
    }
    assert_eq!(breaker.state(), CircuitState::Open);

    breaker.mark_opened_at_for_test(Instant::now() - Duration::from_secs(30));
    assert!(breaker.admit_probe_for_test());
    assert_eq!(breaker.state(), CircuitState::HalfOpen);

    breaker.record_success_for_test(Duration::from_millis(75));
    assert_eq!(breaker.state(), CircuitState::Closed);
}
