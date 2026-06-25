use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

pub type MeterSourceId = u32;
pub const HLC_PHYSICAL_BITS: u64 = 48;
pub const HLC_LOGICAL_MASK: u64 = 0xffff;
pub const OFFSET_DIVERGENCE_THRESHOLD: u64 = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct HlcTimestamp(u64);

impl HlcTimestamp {
    pub fn new(physical_ms: u64, logical: u16) -> Self {
        assert!(
            physical_ms < (1u64 << HLC_PHYSICAL_BITS),
            "physical HLC component exceeds 48 bits"
        );
        Self((physical_ms << 16) | u64::from(logical))
    }

    pub fn zero() -> Self {
        Self(0)
    }
    pub fn physical(self) -> u64 {
        self.0 >> 16
    }
    pub fn logical(self) -> u16 {
        (self.0 & HLC_LOGICAL_MASK) as u16
    }
    pub fn as_u64(self) -> u64 {
        self.0
    }

    pub fn tick(self, observed_physical_ms: u64) -> Self {
        let previous_physical = self.physical();
        if observed_physical_ms > previous_physical {
            Self::new(observed_physical_ms, 0)
        } else {
            Self::new(previous_physical, self.logical().saturating_add(1))
        }
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.physical()
            .cmp(&other.physical())
            .then_with(|| self.logical().cmp(&other.logical()))
    }
}
impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatermarkEntry {
    pub hlc: HlcTimestamp,
    pub offset: u64,
}

#[derive(Debug)]
pub struct WatermarkVector {
    pub entries: HashMap<MeterSourceId, WatermarkEntry>,
    pub epoch: AtomicU64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OffsetDivergence {
    pub source_id: MeterSourceId,
    pub winner: WatermarkEntry,
    pub loser: WatermarkEntry,
    pub offset_gap: u64,
}

impl Default for WatermarkVector {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            epoch: AtomicU64::new(0),
        }
    }
}

impl Clone for WatermarkVector {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            epoch: AtomicU64::new(self.epoch.load(AtomicOrdering::Relaxed)),
        }
    }
}

impl WatermarkVector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&mut self, source_id: MeterSourceId, hlc: HlcTimestamp, offset: u64) {
        self.entries
            .insert(source_id, WatermarkEntry { hlc, offset });
        self.epoch.fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub fn merge(&mut self, other: &WatermarkVector) -> Vec<OffsetDivergence> {
        let mut divergences = Vec::new();
        let mut changed = false;
        for (&source_id, incoming) in &other.entries {
            match self.entries.get(&source_id).cloned() {
                None => {
                    self.entries.insert(source_id, incoming.clone());
                    changed = true;
                }
                Some(local) => {
                    let (winner, loser, incoming_wins) = if incoming.hlc > local.hlc {
                        (incoming.clone(), local, true)
                    } else if incoming.hlc < local.hlc {
                        (local.clone(), incoming.clone(), false)
                    } else if incoming.offset >= local.offset {
                        (
                            incoming.clone(),
                            local.clone(),
                            incoming.offset != local.offset,
                        )
                    } else {
                        (local.clone(), incoming.clone(), false)
                    };
                    let gap = winner.offset.abs_diff(loser.offset);
                    if winner.offset < loser.offset && gap > OFFSET_DIVERGENCE_THRESHOLD {
                        divergences.push(OffsetDivergence {
                            source_id,
                            winner: winner.clone(),
                            loser,
                            offset_gap: gap,
                        });
                    }
                    if incoming_wins {
                        self.entries.insert(source_id, winner);
                        changed = true;
                    }
                }
            }
        }
        if changed {
            self.epoch.fetch_add(1, AtomicOrdering::Relaxed);
        }
        divergences
    }

    pub fn delta_since_epoch(&self, _epoch: u64, max_entries: usize) -> WatermarkVector {
        let mut entries: Vec<_> = self.entries.iter().collect();
        entries.sort_by(|a, b| b.1.hlc.cmp(&a.1.hlc));
        let entries = entries
            .into_iter()
            .take(max_entries)
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        WatermarkVector {
            entries,
            epoch: AtomicU64::new(self.epoch.load(AtomicOrdering::Relaxed)),
        }
    }
}
