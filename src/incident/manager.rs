use chrono::Utc;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::RwLock;
use tracing::{info, warn};

use super::metrics::{record_incident_resolved, record_incident_triggered};
use super::pagerduty::{PagerDutyClient, PagerDutyPayload};
use super::runbook::{AutomationRule, Runbook, RunbookExecutionLog};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IncidentStatus {
    Triggered,
    Acknowledged,
    Resolved,
}

impl std::fmt::Display for IncidentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IncidentStatus::Triggered => write!(f, "triggered"),
            IncidentStatus::Acknowledged => write!(f, "acknowledged"),
            IncidentStatus::Resolved => write!(f, "resolved"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Incident {
    pub id: String,
    pub title: String,
    pub severity: String,       // critical, error, warning, info
    pub component: String,      // TimeSeries, Settlement, Gateway, API, etc.
    pub incident_class: String, // DatabaseLag, TransactionFailure, etc.
    pub status: IncidentStatus,
    pub custom_details: Option<serde_json::Value>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug)]
pub enum ManagerEvent {
    Trigger {
        id: String,
        title: String,
        severity: String,
        component: String,
        incident_class: String,
        custom_details: Option<serde_json::Value>,
    },
    Acknowledge {
        id: String,
    },
    Resolve {
        id: String,
    },
}

pub struct IncidentManager {
    incidents: DashMap<String, Incident>,
    runbooks: DashMap<String, Runbook>,
    rules: DashMap<String, AutomationRule>,
    execution_logs: RwLock<Vec<RunbookExecutionLog>>,
    pd_client: PagerDutyClient,
    tx: Sender<ManagerEvent>,
}

impl IncidentManager {
    pub fn new(pd_client: PagerDutyClient) -> Arc<Self> {
        let (tx, rx) = mpsc::channel::<ManagerEvent>(1000);

        let manager = Arc::new(Self {
            incidents: DashMap::new(),
            runbooks: DashMap::new(),
            rules: DashMap::new(),
            execution_logs: RwLock::new(Vec::new()),
            pd_client,
            tx,
        });

        // Spawn background worker to handle asynchronous queue processing
        let manager_clone = manager.clone();
        tokio::spawn(async move {
            manager_clone.background_worker(rx).await;
        });

        manager
    }

    /// Triggers an incident asynchronously and non-blockingly (< 100ms P99)
    pub fn trigger_incident(
        &self,
        id: String,
        title: String,
        severity: String,
        component: String,
        incident_class: String,
        custom_details: Option<serde_json::Value>,
    ) -> Result<(), String> {
        let event = ManagerEvent::Trigger {
            id,
            title,
            severity,
            component,
            incident_class,
            custom_details,
        };

        self.tx.try_send(event).map_err(|e| {
            warn!("Failed to queue incident event: {:?}", e);
            e.to_string()
        })
    }

    /// Acknowledges an incident asynchronously
    pub fn acknowledge_incident(&self, id: String) -> Result<(), String> {
        let event = ManagerEvent::Acknowledge { id };
        self.tx.try_send(event).map_err(|e| e.to_string())
    }

    /// Resolves an incident asynchronously
    pub fn resolve_incident(&self, id: String) -> Result<(), String> {
        let event = ManagerEvent::Resolve { id };
        self.tx.try_send(event).map_err(|e| e.to_string())
    }

    pub fn get_incident(&self, id: &str) -> Option<Incident> {
        self.incidents.get(id).map(|r| r.value().clone())
    }

    pub fn list_incidents(&self) -> Vec<Incident> {
        self.incidents.iter().map(|r| r.value().clone()).collect()
    }

    pub fn register_runbook(&self, runbook: Runbook) {
        self.runbooks.insert(runbook.name.clone(), runbook);
    }

    pub fn register_rule(&self, rule: AutomationRule) {
        self.rules.insert(rule.id.clone(), rule);
    }

    pub fn list_runbooks(&self) -> Vec<Runbook> {
        self.runbooks.iter().map(|r| r.value().clone()).collect()
    }

    pub fn list_rules(&self) -> Vec<AutomationRule> {
        self.rules.iter().map(|r| r.value().clone()).collect()
    }

    pub async fn get_execution_logs(&self) -> Vec<RunbookExecutionLog> {
        self.execution_logs.read().await.clone()
    }

    /// Process events in the background without blocking critical paths
    async fn background_worker(self: Arc<Self>, mut rx: Receiver<ManagerEvent>) {
        info!("IncidentManager background worker started.");

        while let Some(event) = rx.recv().await {
            match event {
                ManagerEvent::Trigger {
                    id,
                    title,
                    severity,
                    component,
                    incident_class,
                    custom_details,
                } => {
                    let now_str = Utc::now().to_rfc3339();

                    // Update metrics
                    record_incident_triggered(&component, &severity);

                    // Insert/Update active incidents
                    let incident = Incident {
                        id: id.clone(),
                        title: title.clone(),
                        severity: severity.clone(),
                        component: component.clone(),
                        incident_class: incident_class.clone(),
                        status: IncidentStatus::Triggered,
                        custom_details: custom_details.clone(),
                        created_at: now_str.clone(),
                        updated_at: now_str.clone(),
                    };

                    self.incidents.insert(id.clone(), incident);
                    info!(incident_id = %id, "Incident state updated to Triggered");

                    // Send alert to PagerDuty asynchronously
                    let pd_payload = PagerDutyPayload {
                        summary: title.clone(),
                        source: "utility-backend".to_string(),
                        severity: severity.clone(),
                        component: Some(component.clone()),
                        group: Some("Operations".to_string()),
                        class: Some(incident_class.clone()),
                        custom_details: custom_details.clone(),
                    };

                    let client_clone = self.pd_client.clone();
                    let id_clone = id.clone();
                    tokio::spawn(async move {
                        let _ = client_clone
                            .enqueue_event("trigger", &id_clone, Some(pd_payload))
                            .await;
                    });

                    // Evaluate matching automation rules
                    let rules: Vec<AutomationRule> = self
                        .rules
                        .iter()
                        .filter(|r| {
                            r.component == component
                                && r.severity == severity
                                && r.incident_class == incident_class
                        })
                        .map(|r| r.value().clone())
                        .collect();

                    for rule in rules {
                        if let Some(runbook) = self.runbooks.get(&rule.runbook_name) {
                            let runbook_val = runbook.value().clone();
                            let self_clone = self.clone();

                            // Execute runbook asynchronously
                            tokio::spawn(async move {
                                info!(runbook = %runbook_val.name, "Triggering automated runbook execution");
                                let log = runbook_val.execute().await;
                                let mut guard = self_clone.execution_logs.write().await;
                                guard.push(log);
                            });
                        }
                    }
                }
                ManagerEvent::Acknowledge { id } => {
                    if let Some(mut incident_ref) = self.incidents.get_mut(&id) {
                        incident_ref.status = IncidentStatus::Acknowledged;
                        incident_ref.updated_at = Utc::now().to_rfc3339();
                        info!(incident_id = %id, "Incident state updated to Acknowledged");

                        let client_clone = self.pd_client.clone();
                        tokio::spawn(async move {
                            let _ = client_clone.enqueue_event("acknowledge", &id, None).await;
                        });
                    }
                }
                ManagerEvent::Resolve { id } => {
                    if let Some(mut incident_ref) = self.incidents.get_mut(&id) {
                        incident_ref.status = IncidentStatus::Resolved;
                        incident_ref.updated_at = Utc::now().to_rfc3339();
                        info!(incident_id = %id, "Incident state updated to Resolved");

                        record_incident_resolved(&incident_ref.component);

                        let client_clone = self.pd_client.clone();
                        tokio::spawn(async move {
                            let _ = client_clone.enqueue_event("resolve", &id, None).await;
                        });
                    }
                }
            }
        }
    }
}
