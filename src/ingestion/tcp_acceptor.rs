use std::collections::VecDeque;
use std::sync::Arc;

use tracing::warn;

use super::eventfd::EventFd;
use super::ringbuf::{MeterEvent, SharedRingBuffer, TryPushError};

pub const OVERFLOW_LIMIT: usize = 1_024;

pub struct TcpAcceptRingSink {
    ring: Arc<SharedRingBuffer>,
    notifier: Arc<EventFd>,
    overflow: VecDeque<MeterEvent>,
}

impl TcpAcceptRingSink {
    pub fn new(ring: Arc<SharedRingBuffer>, notifier: Arc<EventFd>) -> Self {
        Self {
            ring,
            notifier,
            overflow: VecDeque::with_capacity(OVERFLOW_LIMIT),
        }
    }

    pub fn accept_meter_event(&mut self, event: MeterEvent) -> Result<(), std::io::Error> {
        match self.ring.try_push(event) {
            Ok(()) => self.notifier.notify(),
            Err(TryPushError::WouldBlock) => {
                if self.overflow.len() == OVERFLOW_LIMIT {
                    self.overflow.pop_front();
                }
                self.overflow.push_back(event);
                warn!(
                    overflow_len = self.overflow.len(),
                    "ring buffer full; queued meter event in overflow buffer"
                );
                Ok(())
            }
        }
    }

    pub fn overflow_len(&self) -> usize {
        self.overflow.len()
    }
}
