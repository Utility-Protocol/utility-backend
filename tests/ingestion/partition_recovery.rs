use utility_backend::ingestion::watermark::{WatermarkVector, HlcTimestamp};
use std::sync::{Arc, RwLock};

#[tokio::test]
async fn test_partition_recovery_convergence() {
    let wv1 = Arc::new(RwLock::new(WatermarkVector::new()));
    let wv2 = Arc::new(RwLock::new(WatermarkVector::new()));

    // Simulate partition: nodes update independently
    {
        let mut v1 = wv1.write().unwrap();
        // Node 1 physical time 1000
        v1.entries.insert(1, utility_backend::ingestion::watermark::WatermarkEntry {
            hlc: HlcTimestamp::new(1000, 0),
            offset: 100,
        });
    }

    {
        let mut v2 = wv2.write().unwrap();
        // Node 2 physical time 1100
        v2.entries.insert(1, utility_backend::ingestion::watermark::WatermarkEntry {
            hlc: HlcTimestamp::new(1100, 0),
            offset: 105,
        });
    }

    // Reconciliation: merge vectors
    {
        let v2_snapshot = {
            let v2 = wv2.read().unwrap();
            let mut snapshot = WatermarkVector::new();
            for (id, entry) in &v2.entries {
                snapshot.entries.insert(*id, entry.clone());
            }
            snapshot
        };

        let mut v1 = wv1.write().unwrap();
        let diverged = v1.merge(&v2_snapshot);
        assert!(diverged.is_empty(), "Should not diverge with small offset difference");
    }

    // Verify convergence
    {
        let v1 = wv1.read().unwrap();
        let entry = v1.entries.get(&1).unwrap();
        assert_eq!(entry.hlc, HlcTimestamp::new(1100, 0));
        assert_eq!(entry.offset, 105);
    }
}

#[tokio::test]
async fn test_divergence_triggers_reconciliation() {
    let wv1 = Arc::new(RwLock::new(WatermarkVector::new()));
    let wv2 = Arc::new(RwLock::new(WatermarkVector::new()));

    // Node 1 is way ahead in offset but behind in HLC (e.g. clock drift or late arrival)
    {
        let mut v1 = wv1.write().unwrap();
        v1.entries.insert(1, utility_backend::ingestion::watermark::WatermarkEntry {
            hlc: HlcTimestamp::new(1000, 0),
            offset: 5000,
        });
    }

    {
        let mut v2 = wv2.write().unwrap();
        v2.entries.insert(1, utility_backend::ingestion::watermark::WatermarkEntry {
            hlc: HlcTimestamp::new(1100, 0),
            offset: 1000,
        });
    }

    // Merge v2 into v1
    let diverged = {
        let v2_snapshot = {
            let v2 = wv2.read().unwrap();
            let mut snapshot = WatermarkVector::new();
            for (id, entry) in &v2.entries {
                snapshot.entries.insert(*id, entry.clone());
            }
            snapshot
        };
        let mut v1 = wv1.write().unwrap();
        v1.merge(&v2_snapshot)
    };

    assert_eq!(diverged, vec![1], "Should detect divergence for source 1");
}
