use fixed::types::I64F64;
use utility_backend::tariffs::engine::{TariffEngine, TariffSchedule, TariffTier};
use utility_backend::tariffs::math::{convert_units, RoundingMode};

#[test]
fn test_tariff_peak_vs_offpeak() {
    use chrono::Utc;
    let schedules = vec![
        TariffSchedule {
            tier: TariffTier::Peak,
            rate_per_unit: 0.25,
            start_hour: 17,
            end_hour: 21,
        },
        TariffSchedule {
            tier: TariffTier::OffPeak,
            rate_per_unit: 0.07,
            start_hour: 0,
            end_hour: 6,
        },
    ];
    let engine = TariffEngine::new(schedules);
    let peak_time = Utc::now()
        .date_naive()
        .and_hms_opt(18, 0, 0)
        .unwrap()
        .and_utc();
    let offpeak_time = Utc::now()
        .date_naive()
        .and_hms_opt(3, 0, 0)
        .unwrap()
        .and_utc();

    assert!((engine.evaluate(peak_time, 100.0) - 25.0).abs() < 0.001);
    assert!((engine.evaluate(offpeak_time, 100.0) - 7.0).abs() < 0.001);
}

#[test]
fn test_unit_conversion() {
    let val = I64F64::from_num(1000);
    let result = convert_units(val, "kWh", "MWh", RoundingMode::RoundHalfUp).unwrap();
    assert!((result.to_num::<f64>() - 1.0).abs() < 0.0001);
}
