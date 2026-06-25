use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub const TAI_UTC_OFFSET_SECONDS: i64 = 37;
pub const TAI64N_LEN: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Tai64N {
    pub seconds: u64,
    pub nanos: u32,
}

impl Tai64N {
    pub fn from_unix_ms(ms: i64, correction_ns: i64) -> Self {
        let unix_ns = (ms as i128) * 1_000_000 + correction_ns as i128;
        Self::from_unix_ns(unix_ns)
    }

    pub fn from_unix_ns(unix_ns: i128) -> Self {
        let tai_ns = unix_ns + (TAI_UTC_OFFSET_SECONDS as i128) * 1_000_000_000;
        let seconds = tai_ns.div_euclid(1_000_000_000) as u64;
        let nanos = tai_ns.rem_euclid(1_000_000_000) as u32;
        Self { seconds, nanos }
    }

    pub fn now_with_correction(correction_ns: i64) -> Self {
        let unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i128)
            .unwrap_or(0)
            + correction_ns as i128;
        Self::from_unix_ns(unix_ns)
    }

    pub fn to_unix_ms(&self) -> i64 {
        (self.to_unix_ns() / 1_000_000) as i64
    }

    pub fn to_unix_ns(&self) -> i128 {
        (self.seconds as i128) * 1_000_000_000 + self.nanos as i128
            - (TAI_UTC_OFFSET_SECONDS as i128) * 1_000_000_000
    }

    pub fn to_bytes(&self) -> [u8; TAI64N_LEN] {
        let mut out = [0u8; TAI64N_LEN];
        out[..8].copy_from_slice(&self.seconds.to_be_bytes());
        out[8..].copy_from_slice(&self.nanos.to_be_bytes());
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != TAI64N_LEN {
            return None;
        }
        let seconds = u64::from_be_bytes(bytes[..8].try_into().ok()?);
        let nanos = u32::from_be_bytes(bytes[8..].try_into().ok()?);
        if nanos >= 1_000_000_000 {
            return None;
        }
        Some(Self { seconds, nanos })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ClockCorrection {
    pub correction_ns: i64,
    pub timestamp_tai: Tai64N,
}

pub fn correction_for_offset_seconds(offset_seconds: f64) -> i64 {
    (-offset_seconds * 1_000_000_000.0).round() as i64
}
