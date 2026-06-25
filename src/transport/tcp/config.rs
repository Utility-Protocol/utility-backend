use std::time::Duration;

pub const DEFAULT_MAX_BUFFER_PER_CONN: usize = 65_536;
pub const DEFAULT_MAX_FRAME_PAYLOAD: usize = u16::MAX as usize;
pub const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 60;

#[derive(Debug, Clone)]
pub struct ReassemblyConfig {
    pub max_buffer_per_conn: usize,
    pub max_frame_payload: usize,
    pub idle_timeout: Duration,
}

impl ReassemblyConfig {
    pub fn new(
        max_buffer_per_conn: usize,
        max_frame_payload: usize,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            max_buffer_per_conn,
            max_frame_payload,
            idle_timeout,
        }
    }
}

impl Default for ReassemblyConfig {
    fn default() -> Self {
        Self {
            max_buffer_per_conn: DEFAULT_MAX_BUFFER_PER_CONN,
            max_frame_payload: DEFAULT_MAX_FRAME_PAYLOAD,
            idle_timeout: Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS),
        }
    }
}
