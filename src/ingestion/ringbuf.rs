use std::fs::OpenOptions;
use std::io;
use std::mem::{align_of, size_of};
use std::path::Path;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use memmap2::{MmapMut, MmapOptions};

pub const DEFAULT_RING_CAPACITY: usize = 262_144;
pub const METER_EVENT_SIZE: usize = 128;
const HEADER_SIZE: usize = 64;
const FULL_SPIN_BUDGET: Duration = Duration::from_nanos(100);

#[repr(C, align(16))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MeterEvent {
    pub meter_id: u64,
    pub timestamp_ns: u64,
    pub commodity: u8,
    reserved_align: [u8; 15],
    pub scaled_value: i128,
    pub reserved: [u8; 80],
}

impl Default for MeterEvent {
    fn default() -> Self {
        Self {
            meter_id: 0,
            timestamp_ns: 0,
            commodity: 0,
            reserved_align: [0; 15],
            scaled_value: 0,
            reserved: [0; 80],
        }
    }
}

impl MeterEvent {
    pub fn new(meter_id: u64, timestamp_ns: u64, commodity: u8, scaled_value: i128) -> Self {
        Self {
            meter_id,
            timestamp_ns,
            commodity,
            reserved_align: [0; 15],
            scaled_value,
            reserved: [0; 80],
        }
    }
}

#[repr(C)]
struct RingBufHeader {
    head: AtomicU64,
    tail: AtomicU64,
    capacity: u32,
    slot_mask: u32,
    _reserved: [u8; 40],
}

#[repr(C, align(16))]
struct MeterEventSlot {
    seq: AtomicU64,
    _pad: [u8; 8],
    event: MeterEvent,
}

#[derive(Debug, thiserror::Error)]
pub enum RingBufferError {
    #[error("ring buffer capacity must be a non-zero power of two")]
    InvalidCapacity,
    #[error("ring buffer capacity exceeds u32::MAX")]
    CapacityTooLarge,
    #[error("ring buffer mapping is too small")]
    MappingTooSmall,
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TryPushError {
    #[error("ring buffer is full")]
    WouldBlock,
}

pub struct SharedRingBuffer {
    _mmap: MmapMut,
    header: NonNull<RingBufHeader>,
    slots: NonNull<MeterEventSlot>,
    capacity: usize,
    slot_mask: u64,
}

unsafe impl Send for SharedRingBuffer {}
unsafe impl Sync for SharedRingBuffer {}

impl SharedRingBuffer {
    pub fn create(path: impl AsRef<Path>, capacity: usize) -> Result<Self, RingBufferError> {
        validate_capacity(capacity)?;
        let len = mapping_len(capacity);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(len as u64)?;
        let mut mmap = unsafe { MmapOptions::new().len(len).map_mut(&file)? };
        mmap.fill(0);
        let mut ring = Self::from_mmap(mmap)?;
        ring.initialize(capacity)?;
        Ok(ring)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, RingBufferError> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        Self::from_mmap(mmap)
    }

    pub fn try_push(&self, event: MeterEvent) -> Result<(), TryPushError> {
        let tail = self.header().tail.load(Ordering::Acquire);
        let head = self.header().head.load(Ordering::Acquire);
        if tail.wrapping_sub(head) >= self.capacity as u64 {
            let start = Instant::now();
            while start.elapsed() < FULL_SPIN_BUDGET {
                std::hint::spin_loop();
            }
            return Err(TryPushError::WouldBlock);
        }

        let slot = self.slot(tail & self.slot_mask);
        unsafe { std::ptr::write(std::ptr::addr_of_mut!((*slot).event), event) };
        unsafe { (*slot).seq.store(tail.wrapping_add(1), Ordering::Release) };
        self.header().tail.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub fn try_pop(&self) -> Option<MeterEvent> {
        let head = self.header().head.load(Ordering::Acquire);
        let tail = self.header().tail.load(Ordering::Acquire);
        if head == tail {
            return None;
        }

        let slot = self.slot(head & self.slot_mask);
        if unsafe { (*slot).seq.load(Ordering::Acquire) } != head.wrapping_add(1) {
            std::hint::spin_loop();
            if unsafe { (*slot).seq.load(Ordering::Acquire) } != head.wrapping_add(1) {
                return None;
            }
        }

        let event = unsafe { std::ptr::read(std::ptr::addr_of!((*slot).event)) };
        unsafe { (*slot).seq.store(0, Ordering::Release) };
        self.header().head.fetch_add(1, Ordering::AcqRel);
        Some(event)
    }

    pub fn len(&self) -> usize {
        let head = self.header().head.load(Ordering::Acquire);
        let tail = self.header().tail.load(Ordering::Acquire);
        tail.wrapping_sub(head) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    fn from_mmap(mmap: MmapMut) -> Result<Self, RingBufferError> {
        if mmap.len() < HEADER_SIZE {
            return Err(RingBufferError::MappingTooSmall);
        }
        let header = NonNull::new(mmap.as_ptr() as *mut RingBufHeader).unwrap();
        let capacity = unsafe { header.as_ref().capacity as usize };
        let slot_mask = unsafe { header.as_ref().slot_mask as u64 };
        let slots = unsafe {
            NonNull::new_unchecked(mmap.as_ptr().add(HEADER_SIZE) as *mut MeterEventSlot)
        };
        if capacity > 0 && mmap.len() < mapping_len(capacity) {
            return Err(RingBufferError::MappingTooSmall);
        }
        Ok(Self {
            _mmap: mmap,
            header,
            slots,
            capacity,
            slot_mask,
        })
    }

    fn initialize(&mut self, capacity: usize) -> Result<(), RingBufferError> {
        let capacity_u32 =
            u32::try_from(capacity).map_err(|_| RingBufferError::CapacityTooLarge)?;
        unsafe {
            let header = self.header.as_ptr();
            std::ptr::write(&mut (*header).head, AtomicU64::new(0));
            std::ptr::write(&mut (*header).tail, AtomicU64::new(0));
            (*header).capacity = capacity_u32;
            (*header).slot_mask = capacity_u32 - 1;
        }
        self.capacity = capacity;
        self.slot_mask = (capacity - 1) as u64;
        Ok(())
    }

    fn header(&self) -> &RingBufHeader {
        unsafe { self.header.as_ref() }
    }

    fn slot(&self, index: u64) -> *mut MeterEventSlot {
        unsafe { self.slots.as_ptr().add(index as usize) }
    }
}

fn validate_capacity(capacity: usize) -> Result<(), RingBufferError> {
    if capacity == 0 || !capacity.is_power_of_two() {
        return Err(RingBufferError::InvalidCapacity);
    }
    if capacity > u32::MAX as usize {
        return Err(RingBufferError::CapacityTooLarge);
    }
    debug_assert_eq!(size_of::<MeterEvent>(), METER_EVENT_SIZE);
    debug_assert_eq!(align_of::<MeterEvent>(), 16);
    Ok(())
}

fn mapping_len(capacity: usize) -> usize {
    HEADER_SIZE + capacity * size_of::<MeterEventSlot>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meter_event_is_fixed_size() {
        assert_eq!(size_of::<MeterEvent>(), METER_EVENT_SIZE);
    }

    #[test]
    fn push_pop_round_trip() {
        let path = format!("/tmp/utility-ringbuf-{}", std::process::id());
        let ring = SharedRingBuffer::create(&path, 8).unwrap();
        let event = MeterEvent::new(7, 11, 2, -42);
        ring.try_push(event).unwrap();
        assert_eq!(ring.len(), 1);
        assert_eq!(ring.try_pop(), Some(event));
        assert_eq!(ring.try_pop(), None);
        let _ = std::fs::remove_file(path);
    }
}
