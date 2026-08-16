use std::sync::atomic::{AtomicU64, Ordering};

const PHYSICAL_BITS: u64 = 48;
const LOGICAL_MASK: u64 = 0xFFFF;
const MAX_LOGICAL: u16 = 0xFFFF;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HlcTimestamp(pub u64);

impl HlcTimestamp {
    pub fn physical(self) -> u64 {
        self.0 >> PHYSICAL_BITS
    }

    pub fn logical(self) -> u16 {
        (self.0 & LOGICAL_MASK) as u16
    }

    pub fn new(physical: u64, logical: u16) -> Self {
        HlcTimestamp((physical << PHYSICAL_BITS) | (logical as u64))
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.physical()
            .cmp(&other.physical())
            .then(self.logical().cmp(&other.logical()))
    }
}

#[derive(Debug)]
pub struct HybridLogicalClock {
    current: AtomicU64,
}

impl HybridLogicalClock {
    pub fn new() -> Self {
        Self {
            current: AtomicU64::new(0),
        }
    }

    pub fn tick(&self, wall_clock_ms: u64) -> HlcTimestamp {
        loop {
            let old = self.current.load(Ordering::Acquire);
            let old_physical = old >> PHYSICAL_BITS;
            let old_logical = (old & LOGICAL_MASK) as u16;

            let candidate_physical = old_physical.max(wall_clock_ms);
            let new_logical = if candidate_physical == old_physical {
                old_logical.wrapping_add(1)
            } else {
                0
            };

            let (candidate_physical, new_logical) = if new_logical == 0
                && candidate_physical == old_physical
                && old_logical == MAX_LOGICAL
            {
                (candidate_physical.wrapping_add(1), 0)
            } else {
                (candidate_physical, new_logical)
            };

            let new_val = (candidate_physical << PHYSICAL_BITS) | (new_logical as u64);
            if self
                .current
                .compare_exchange(old, new_val, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return HlcTimestamp(new_val);
            }
        }
    }

    pub fn update(&self, incoming: HlcTimestamp) {
        loop {
            let old = self.current.load(Ordering::Acquire);
            let new_val = old.max(incoming.0);
            if self
                .current
                .compare_exchange(old, new_val, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    pub fn get_current(&self) -> HlcTimestamp {
        HlcTimestamp(self.current.load(Ordering::Acquire))
    }
}

impl Default for HybridLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_hlc_timestamp_ord() {
        let a = HlcTimestamp::new(100, 5);
        let b = HlcTimestamp::new(100, 10);
        let c = HlcTimestamp::new(200, 0);
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
        assert_eq!(a, a);
    }

    #[test]
    fn test_hlc_tick_increases_physical() {
        let clock = HybridLogicalClock::new();
        let t1 = clock.tick(1000);
        let t2 = clock.tick(2000);
        assert_eq!(t1.physical(), 1000);
        assert_eq!(t1.logical(), 0);
        assert_eq!(t2.physical(), 2000);
        assert_eq!(t2.logical(), 0);
    }

    #[test]
    fn test_hlc_tick_increases_logical_same_physical() {
        let clock = HybridLogicalClock::new();
        let t1 = clock.tick(1000);
        let t2 = clock.tick(1000);
        assert_eq!(t1.physical(), 1000);
        assert_eq!(t1.logical(), 0);
        assert_eq!(t2.physical(), 1000);
        assert_eq!(t2.logical(), 1);
    }

    #[test]
    fn test_hlc_update_advances_clock() {
        let clock = HybridLogicalClock::new();
        clock.tick(500);
        // incoming HLC with higher physical
        let incoming = HlcTimestamp::new(800, 10);
        clock.update(incoming);
        assert_eq!(clock.get_current().physical(), 800);
        assert_eq!(clock.get_current().logical(), 10);
    }

    #[test]
    fn test_hlc_update_ignores_older() {
        let clock = HybridLogicalClock::new();
        clock.tick(1000);
        let older = HlcTimestamp::new(500, 0);
        clock.update(older);
        assert_eq!(clock.get_current().physical(), 1000);
    }

    #[test]
    fn test_concurrent_tick() {
        let clock = Arc::new(HybridLogicalClock::new());
        let mut handles = Vec::new();
        for i in 0..10 {
            let c = clock.clone();
            handles.push(thread::spawn(move || {
                c.tick(100 + i);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // All ticks at different physical times should work fine
        assert!(clock.get_current().physical() >= 100);
    }

    #[test]
    fn test_logical_wrap_increments_physical() {
        let clock = HybridLogicalClock::new();
        // tick many times at same wall clock to force wrap
        let wall = 1000u64;
        let mut prev_logical = 0u16;
        let mut wrapped = false;
        for _ in 0..=0x10000 {
            let t = clock.tick(wall);
            if t.physical() > wall {
                wrapped = true;
                assert_eq!(t.logical(), 0);
                break;
            }
            if t.logical() < prev_logical && t.logical() == 0 {
                // wrapped around
                wrapped = true;
                assert_eq!(t.physical(), wall + 1);
                break;
            }
            prev_logical = t.logical();
        }
        assert!(wrapped, "logical counter should have wrapped");
    }
}
