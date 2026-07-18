use std::time::{Duration, Instant};
use serde_json::json;

use utility_backend::incident::{
    AutomationRule, IncidentManager, IncidentStatus, PagerDutyClient, Runbook, RunbookAction,
};

#[tokio::test]
async fn test_pagerduty_client_mock() {
    let client = PagerDutyClient::new(None, None);
    let payload = utility_backend::incident::PagerDutyPayload {
        summary: "Database connection spike".to_string(),
        source: "test-env".to_string(),
        severity: "warning".to_string(),
        component: Some("TimeSeries".to_string()),
        group: None,
        class: None,
        custom_details: None,
    };

    let res = client
        .enqueue_event("trigger", "inc-101", Some(payload))
        .await;

    assert!(res.is_ok());
    let response = res.unwrap().unwrap();
    assert_eq!(response.status, "success");
    assert_eq!(response.dedup_key, "inc-101");
}

#[tokio::test]
async fn test_incident_manager_lifecycle_and_runbook() {
    // 1. Initialize PagerDuty client and manager
    let pd_client = PagerDutyClient::new(None, None);
    let manager = IncidentManager::new(pd_client);

    // Register a custom runbook
    let runbook = Runbook {
        name: "Scale Ingestion Workers".to_string(),
        description: "Scale ingestion cluster during high volume".to_string(),
        actions: vec![
            RunbookAction::ScaleResources {
                service: "ingestion-worker".to_string(),
                replicas: 10,
            },
            RunbookAction::NotifySlack {
                channel: "alerts-dev".to_string(),
                message: "Scaled ingestion-worker to 10 replicas due to high load".to_string(),
            },
        ],
    };
    manager.register_runbook(runbook);

    // Register an automation rule matching the class & component
    let rule = AutomationRule {
        id: "RULE-WORKER-SCALE".to_string(),
        component: "Gateway".to_string(),
        severity: "critical".to_string(),
        incident_class: "HighConnectionVolume".to_string(),
        runbook_name: "Scale Ingestion Workers".to_string(),
    };
    manager.register_rule(rule);

    // Verify rules and runbooks lists
    assert_eq!(manager.list_runbooks().len(), 1);
    assert_eq!(manager.list_rules().len(), 1);

    // 2. Trigger incident and measure latency (assert < 100ms P99)
    let start = Instant::now();
    let res = manager.trigger_incident(
        "inc-001".to_string(),
        "Too many concurrent TCP connections".to_string(),
        "critical".to_string(),
        "Gateway".to_string(),
        "HighConnectionVolume".to_string(),
        Some(json!({ "active_connections": 1500 })),
    );
    let duration = start.elapsed();

    assert!(res.is_ok());
    // Triggering should be non-blocking and execute almost instantly (typically <1ms, definitely <10ms)
    assert!(
        duration < Duration::from_millis(50),
        "Triggering took too long: {:?}",
        duration
    );

    // Give background worker a brief moment to process the queue
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Check incident exists and has Triggered status
    let incident = manager.get_incident("inc-001");
    assert!(incident.is_some());
    let incident = incident.unwrap();
    assert_eq!(incident.status, IncidentStatus::Triggered);
    assert_eq!(incident.severity, "critical");
    assert_eq!(incident.component, "Gateway");

    // Check that automated runbook was triggered and completed in the background
    let logs = manager.get_execution_logs().await;
    assert_eq!(logs.len(), 1);
    let runbook_log = &logs[0];
    assert_eq!(runbook_log.runbook_name, "Scale Ingestion Workers");
    assert_eq!(runbook_log.status, "success");
    assert!(runbook_log.logs.iter().any(|l| l.contains("Scaled ingestion-worker to 10 replicas")));

    // 3. Acknowledge incident
    let res = manager.acknowledge_incident("inc-001".to_string());
    assert!(res.is_ok());
    tokio::time::sleep(Duration::from_millis(100)).await;

    let incident = manager.get_incident("inc-001").unwrap();
    assert_eq!(incident.status, IncidentStatus::Acknowledged);

    // 4. Resolve incident
    let res = manager.resolve_incident("inc-001".to_string());
    assert!(res.is_ok());
    tokio::time::sleep(Duration::from_millis(100)).await;

    let incident = manager.get_incident("inc-001").unwrap();
    assert_eq!(incident.status, IncidentStatus::Resolved);
}
