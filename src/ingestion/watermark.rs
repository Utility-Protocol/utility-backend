use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HlcTimestamp(u64);

impl HlcTimestamp {
    pub fn new(physical: u64, logical: u16) -> Self {
        // 48-bit physical | 16-bit logical
        Self((physical << 16) | (logical as u64))
    }

    pub fn physical(&self) -> u64 {
        self.0 >> 16
    }

    pub fn logical(&self) -> u16 {
        (self.0 & 0xFFFF) as u16
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn tick(&self, now_physical: u64) -> Self {
        if now_physical > self.physical() {
            Self::new(now_physical, 0)
        } else {
            Self::new(self.physical(), self.logical() + 1)
        }
    }
}

impl From<u64> for HlcTimestamp {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatermarkEntry {
    pub hlc: HlcTimestamp,
    pub offset: u64,
}

#[derive(Debug, Default)]
pub struct WatermarkVector {
    pub entries: HashMap<u32, WatermarkEntry>,
    pub epoch: AtomicU64,
}

impl WatermarkVector {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            epoch: AtomicU64::new(0),
        }
    }

    pub fn update(&mut self, source_id: u32, offset: u64) {
        let now_physical = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let entry = self.entries.entry(source_id).or_insert_with(|| {
            WatermarkEntry {
                hlc: HlcTimestamp::new(now_physical, 0),
                offset: 0,
            }
        });

        entry.hlc = entry.hlc.tick(now_physical);
        entry.offset = offset;
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }

    pub fn merge(&mut self, other: &WatermarkVector) -> Vec<u32> {
        let mut diverged = Vec::new();
        let mut changed = false;

        for (source_id, other_entry) in &other.entries {
            let entry = self.entries.entry(*source_id).or_insert_with(|| {
                changed = true;
                other_entry.clone()
            });

            if other_entry.hlc > entry.hlc {
                // If offsets are divergent (i.e., the HLC winner has an offset lower than the loser's offset by more than 1000)
                if entry.offset > other_entry.offset + 1000 {
                    diverged.push(*source_id);
                }
                *entry = other_entry.clone();
                changed = true;
            } else if entry.hlc > other_entry.hlc {
                if other_entry.offset > entry.offset + 1000 {
                    diverged.push(*source_id);
                }
            } else if entry.hlc == other_entry.hlc && entry.offset != other_entry.offset {
                // Same HLC but different offset? This shouldn't happen with proper HLC but lets be safe
                if (entry.offset as i64 - other_entry.offset as i64).abs() > 1000 {
                    diverged.push(*source_id);
                }
                if other_entry.offset > entry.offset {
                    entry.offset = other_entry.offset;
                    changed = true;
                }
            }
        }

        if changed {
            self.epoch.fetch_add(1, Ordering::SeqCst);
        }

        diverged
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hlc_tick() {
        let hlc = HlcTimestamp::new(100, 5);
        let ticked = hlc.tick(100);
        assert_eq!(ticked.physical(), 100);
        assert_eq!(ticked.logical(), 6);

        let ticked_newer = hlc.tick(101);
        assert_eq!(ticked_newer.physical(), 101);
        assert_eq!(ticked_newer.logical(), 0);
    }

    #[test]
    fn test_hlc_cmp() {
        let h1 = HlcTimestamp::new(100, 5);
        let h2 = HlcTimestamp::new(100, 6);
        let h3 = HlcTimestamp::new(101, 0);

        assert!(h2 > h1);
        assert!(h3 > h2);
    }

    #[test]
    fn test_watermark_merge() {
        let mut v1 = WatermarkVector::new();
        let mut v2 = WatermarkVector::new();

        v1.entries.insert(1, WatermarkEntry { hlc: HlcTimestamp::new(100, 0), offset: 10 });
        v2.entries.insert(1, WatermarkEntry { hlc: HlcTimestamp::new(100, 1), offset: 11 });
        v2.entries.insert(2, WatermarkEntry { hlc: HlcTimestamp::new(100, 0), offset: 5 });

        let diverged = v1.merge(&v2);
        assert!(diverged.is_empty());
        assert_eq!(v1.entries.get(&1).unwrap().offset, 11);
        assert_eq!(v1.entries.get(&2).unwrap().offset, 5);
    }

    #[test]
    fn test_divergence_detection() {
        let mut v1 = WatermarkVector::new();
        let mut v2 = WatermarkVector::new();

        // v1 is ahead in offset but behind in HLC
        v1.entries.insert(1, WatermarkEntry { hlc: HlcTimestamp::new(100, 0), offset: 2000 });
        v2.entries.insert(1, WatermarkEntry { hlc: HlcTimestamp::new(101, 0), offset: 500 });

        let diverged = v1.merge(&v2);
        assert_eq!(diverged, vec![1]);
        // v1 should still adopt v2's HLC/offset because v2 has higher HLC
        assert_eq!(v1.entries.get(&1).unwrap().hlc.physical(), 101);
        assert_eq!(v1.entries.get(&1).unwrap().offset, 500);
    }
}
