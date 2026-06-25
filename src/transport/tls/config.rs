use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionTicketConfig {
    pub rotation_interval_hours: u32,
    pub key_directory: PathBuf,
    pub ticket_lifetime_hours: u32,
}

impl Default for SessionTicketConfig {
    fn default() -> Self {
        Self {
            rotation_interval_hours: 24,
            key_directory: PathBuf::from("/etc/utility/tls/session-ticket-keys"),
            ticket_lifetime_hours: 12,
        }
    }
}

impl SessionTicketConfig {
    pub fn rotation_interval_seconds(&self) -> u64 {
        u64::from(self.rotation_interval_hours).saturating_mul(60 * 60)
    }

    pub fn ticket_lifetime_seconds(&self) -> u32 {
        self.ticket_lifetime_hours.saturating_mul(60 * 60)
    }
}
