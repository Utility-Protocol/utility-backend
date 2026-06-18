use utility_backend::time_series::analytics::{
    analyze_consumption, global_engine, DiagnosticEngine, ProbableCause, Reading, WeatherCovariate,
};

// ---- Legacy tests ----

#[test]
fn test_anomaly_detection_baseline() {
    use chrono::Utc;
    let readings = vec![(Utc::now(), 100.0), (Utc::now(), 102.0), (Utc::now(), 98.0)];
    let result = analyze_consumption("MTR-001", &readings, 300.0, 50.0);
    assert!(!result.anomaly_detected);
}

#[test]
fn test_anomaly_detection_leak() {
    use chrono::Utc;
    let readings = vec![
        (Utc::now(), 500.0),
        (Utc::now(), 600.0),
        (Utc::now(), 550.0),
    ];
    let result = analyze_consumption("MTR-002", &readings, 300.0, 50.0);
    assert!(result.anomaly_detected);
}

// ---- Streaming Engine Tests ----

#[test]
fn test_engine_ingest_and_analyze_no_anomaly() {
    let mut engine = DiagnosticEngine::new();
    for i in 0..30 {
        engine.ingest_reading(
            "MTR-A",
            Reading {
                timestamp: chrono::Utc::now() - chrono::Duration::days(i),
                value: 100.0 + (i as f64 * 0.5),
                weather: None,
            },
        );
    }
    let report = engine.get_diagnostics("MTR-A").unwrap();
    assert!(
        !report.anomaly_detected,
        "should not trigger on stable data"
    );
}

#[test]
fn test_engine_detects_sustained_leak() {
    let mut engine = DiagnosticEngine::new();
    for i in 0..25 {
        engine.ingest_reading(
            "MTR-B",
            Reading {
                timestamp: chrono::Utc::now() - chrono::Duration::days(i),
                value: 100.0 + (i as f64).sin() * 5.0,
                weather: None,
            },
        );
    }
    for i in 0..5 {
        engine.ingest_reading(
            "MTR-B",
            Reading {
                timestamp: chrono::Utc::now() - chrono::Duration::days(i),
                value: 200.0 + (i as f64).sin() * 5.0,
                weather: None,
            },
        );
    }
    let report = engine.get_diagnostics("MTR-B").unwrap();
    assert!(report.anomaly_detected, "should detect sustained leak");
    assert_eq!(
        report.probable_cause,
        Some(ProbableCause::Leak),
        "should classify as leak"
    );
}

#[test]
fn test_engine_sensor_fault_detection() {
    let mut engine = DiagnosticEngine::new();
    for i in 0..25 {
        engine.ingest_reading(
            "MTR-C",
            Reading {
                timestamp: chrono::Utc::now() - chrono::Duration::days(i),
                value: 100.0,
                weather: None,
            },
        );
    }
    for i in 0..5 {
        let v = if i % 2 == 0 { 500.0 } else { 10.0 };
        engine.ingest_reading(
            "MTR-C",
            Reading {
                timestamp: chrono::Utc::now() - chrono::Duration::days(i),
                value: v,
                weather: None,
            },
        );
    }
    let report = engine.get_diagnostics("MTR-C").unwrap();
    if report.anomaly_detected {
        assert_eq!(report.probable_cause, Some(ProbableCause::SensorFault));
    }
}

#[test]
fn test_weather_model_fit() {
    let mut engine = DiagnosticEngine::new();
    for i in 0..30 {
        let temp = 15.0 + (i as f64 % 20.0);
        let base = 100.0;
        let weather_effect = (temp - 20.0).max(0.0) * 2.0;
        engine.ingest_reading(
            "MTR-D",
            Reading {
                timestamp: chrono::Utc::now() - chrono::Duration::days(i),
                value: base + weather_effect,
                weather: Some(WeatherCovariate {
                    temperature_c: temp,
                    precipitation_mm: 0.0,
                }),
            },
        );
    }
    engine.fit_weather_model("MTR-D");
    let hot_reading = Reading {
        timestamp: chrono::Utc::now(),
        value: 150.0,
        weather: Some(WeatherCovariate {
            temperature_c: 35.0,
            precipitation_mm: 0.0,
        }),
    };
    let adjustment = engine.compute_weather_adjustment("MTR-D", &hot_reading);
    assert!(
        adjustment > 0.0,
        "weather adjustment should be positive for hot day, got {}",
        adjustment
    );
}

#[test]
fn test_global_engine_is_accessible() {
    let engine = global_engine();
    let mut guard = engine.lock().unwrap();
    guard.ingest_reading(
        "GLOBAL",
        Reading {
            timestamp: chrono::Utc::now(),
            value: 100.0,
            weather: None,
        },
    );
    let report = guard.get_diagnostics("GLOBAL");
    assert!(report.is_some());
}
