use proptest::prelude::*;
use utility_backend::ingestion::{drift_estimator::KalmanClockState, tai64n::Tai64N};

proptest! {
    #[test]
    fn tai64n_round_trips_unix_ms(ms in 0i64..4_102_444_800_000i64, correction in -5_000_000i64..5_000_000i64) {
        let ts = Tai64N::from_unix_ms(ms, correction);
        let expected = ((ms as i128 * 1_000_000 + correction as i128) / 1_000_000) as i64;
        prop_assert_eq!(ts.to_unix_ms(), expected);
        prop_assert_eq!(Tai64N::from_bytes(&ts.to_bytes()), Some(ts));
    }
}

#[test]
fn kalman_corrects_200ppm_drift_over_24h() {
    let mut state = KalmanClockState::default();
    let drift = 200.0 / 1_000_000.0;
    let dt = 30.0;
    let mut max_error_ms: f64 = 0.0;
    for i in 1..=(24 * 60 * 2) {
        let true_elapsed = i as f64 * dt;
        let measured_offset = drift * true_elapsed + (((i % 7) as f64) - 3.0) * 0.00025;
        state.predict(dt);
        state.update(measured_offset);
        let error_ms = ((state.offset_seconds - drift * true_elapsed) * 1000.0).abs();
        max_error_ms = max_error_ms.max(error_ms);
    }
    assert!(
        max_error_ms < 5.0,
        "max correction error {max_error_ms}ms exceeded 5ms"
    );
}
