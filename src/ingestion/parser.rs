use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use super::eventfd::AsyncEventFd;
use super::ringbuf::{MeterEvent, SharedRingBuffer};

pub const PARSER_BATCH_SIZE: usize = 64;

pub async fn run_ring_parser(
    ring: Arc<SharedRingBuffer>,
    notifier: AsyncEventFd,
    parsed_tx: mpsc::Sender<MeterEvent>,
) {
    let mut poll = tokio::time::interval(Duration::from_millis(1));
    loop {
        tokio::select! {
            wait_result = notifier.wait() => {
                if wait_result.is_err() {
                    break;
                }
            }
            _ = poll.tick() => {}
        }

        for _ in 0..PARSER_BATCH_SIZE {
            let Some(event) = ring.try_pop() else {
                break;
            };
            if parsed_tx.send(event).await.is_err() {
                return;
            }
        }
    }
}
