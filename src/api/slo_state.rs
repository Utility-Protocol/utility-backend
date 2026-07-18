use crate::observability::slo::{SloConfig, SloMonitor};
use lazy_static::lazy_static;
use parking_lot::Mutex;

lazy_static! {
    static ref GLOBAL_SLO_MONITOR: Mutex<SloMonitor> =
        Mutex::new(SloMonitor::new(SloConfig::default()));
}

pub fn global_slo_monitor() -> &'static Mutex<SloMonitor> {
    &GLOBAL_SLO_MONITOR
}
