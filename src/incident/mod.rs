pub mod manager;
pub mod metrics;
pub mod pagerduty;
pub mod runbook;

pub use manager::{Incident, IncidentManager, IncidentStatus, ManagerEvent};
pub use pagerduty::{PagerDutyClient, PagerDutyPayload};
pub use runbook::{AutomationRule, Runbook, RunbookAction, RunbookExecutionLog};

use lazy_static::lazy_static;
use std::sync::{Arc, Mutex};

lazy_static! {
    static ref GLOBAL_INCIDENT_MANAGER: Mutex<Option<Arc<IncidentManager>>> = Mutex::new(None);
}

/// Initialises the global incident manager so non-API background components can access it.
pub fn init_global_incident_manager(manager: Arc<IncidentManager>) {
    let mut guard = GLOBAL_INCIDENT_MANAGER
        .lock()
        .expect("incident manager lock poisoned");
    *guard = Some(manager);
}

/// Retrieves a reference to the global incident manager, if initialised.
pub fn global_incident_manager() -> Option<Arc<IncidentManager>> {
    GLOBAL_INCIDENT_MANAGER
        .lock()
        .expect("incident manager lock poisoned")
        .clone()
}
