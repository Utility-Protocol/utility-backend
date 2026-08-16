use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::gateway::hlc::HlcTimestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HlcWatermark {
    pub last_hlc: HlcTimestamp,
    pub last_offset: u64,
}

#[derive(Debug, Clone)]
pub struct HlcWatermarkStore {
    store: Arc<RwLock<HashMap<String, HlcWatermark>>>,
}

impl HlcWatermarkStore {
    pub fn new() -> Self {
        Self {
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn record(&self, source: &str, hlc: HlcTimestamp, offset: u64) {
        let mut store = self.store.write().expect("watermark store lock poisoned");
        store.insert(
            source.to_string(),
            HlcWatermark {
                last_hlc: hlc,
                last_offset: offset,
            },
        );
    }

    pub fn get(&self, source: &str) -> Option<HlcWatermark> {
        let store = self.store.read().expect("watermark store lock poisoned");
        store.get(source).copied()
    }

    /// CRDT merge: for each source in `other`, resolve by max HLC,
    /// tiebreak by max offset.
    pub fn merge(&self, other: &HashMap<String, HlcWatermark>) {
        let mut store = self.store.write().expect("watermark store lock poisoned");
        for (source, other_wm) in other {
            let entry = store.entry(source.clone()).or_insert(*other_wm);
            if other_wm.last_hlc > entry.last_hlc
                || (other_wm.last_hlc == entry.last_hlc && other_wm.last_offset > entry.last_offset)
            {
                *entry = *other_wm;
            }
        }
    }

    pub fn snapshot(&self) -> HashMap<String, HlcWatermark> {
        let store = self.store.read().expect("watermark store lock poisoned");
        store.clone()
    }
}

impl Default for HlcWatermarkStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::hlc::HlcTimestamp;

    #[test]
    fn test_record_and_get() {
        let store = HlcWatermarkStore::new();
        let hlc = HlcTimestamp::new(1000, 5);
        store.record("M1", hlc, 42);
        let wm = store.get("M1").unwrap();
        assert_eq!(wm.last_hlc, hlc);
        assert_eq!(wm.last_offset, 42);
    }

    #[test]
    fn test_merge_resolves_by_hlc() {
        let store = HlcWatermarkStore::new();
        store.record("M1", HlcTimestamp::new(1000, 0), 10);

        let mut other = HashMap::new();
        other.insert(
            "M1".into(),
            HlcWatermark {
                last_hlc: HlcTimestamp::new(2000, 0),
                last_offset: 5,
            },
        );
        store.merge(&other);
        let wm = store.get("M1").unwrap();
        assert_eq!(wm.last_hlc.physical(), 2000);
        assert_eq!(wm.last_offset, 5);
    }

    #[test]
    fn test_merge_tiebreak_by_offset() {
        let store = HlcWatermarkStore::new();
        store.record("M1", HlcTimestamp::new(1000, 0), 10);

        let mut other = HashMap::new();
        other.insert(
            "M1".into(),
            HlcWatermark {
                last_hlc: HlcTimestamp::new(1000, 0),
                last_offset: 20,
            },
        );
        store.merge(&other);
        let wm = store.get("M1").unwrap();
        assert_eq!(wm.last_offset, 20);
    }

    #[test]
    fn test_snapshot_isolation() {
        let store = HlcWatermarkStore::new();
        store.record("M1", HlcTimestamp::new(100, 0), 1);
        let snap = store.snapshot();
        assert_eq!(snap.len(), 1);
        store.record("M2", HlcTimestamp::new(200, 0), 2);
        assert_eq!(snap.len(), 1);
    }
}
