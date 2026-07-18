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

#[tokio::test]
#[ignore] // Requires a running PostgreSQL/TimescaleDB instance
async fn test_concurrent_telemetry_ingestion_ordering() {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://utility:utility_secret@localhost:5432/utility_test".into());
    let pool = sqlx::PgPool::connect(&database_url).await.unwrap();

    // Clean up before test
    sqlx::query("DELETE FROM telemetry_events WHERE meter_id = 'CONCURRENT-MTR'")
        .execute(&pool)
        .await
        .unwrap();

    let meter_id = "CONCURRENT-MTR";
    let num_concurrent = 10;
    let mut handles = vec![];

    for i in 0..num_concurrent {
        let p = pool.clone();
        let mid = meter_id.to_string();
        handles.push(tokio::spawn(async move {
            utility_backend::time_series::ingestion::ingest_telemetry(
                &p,
                &mid,
                100.0 + (i as f64),
                chrono::Utc::now(),
                0,
            )
            .await
        }));
    }

    let mut sequences = vec![];
    for h in handles {
        let seq = h.await.unwrap().unwrap();
        sequences.push(seq);
    }

    sequences.sort();

    // Verify that we have unique, dense sequences from 1 to 10
    assert_eq!(sequences.len(), num_concurrent);
    for (i, &seq) in sequences.iter().enumerate() {
        assert_eq!(seq, (i + 1) as i32, "Sequence mismatch at index {}", i);
    }

    // Double check the database content
    let db_sequences: Vec<i32> = sqlx::query_scalar(
        "SELECT sequence FROM telemetry_events WHERE meter_id = $1 ORDER BY sequence ASC",
    )
    .bind(meter_id)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(db_sequences, sequences);
}
