//! Dynamically resizable bounded channel for pipeline stages (issue #46).
//!
//! The blueprint resizes by recreating the channel and bridging messages, which
//! risks loss/reordering. Instead this wraps an unbounded `mpsc` with a
//! credit [`Semaphore`] whose permit count is the window: a send consumes a
//! credit (awaiting when none remain — backpressure) and a receive returns one
//! (capped at the current window). [`WindowedSender::resize`] adjusts the window
//! atomically with no message loss: growing adds credits immediately; shrinking
//! lowers the target so receives stop replenishing until the window is reached.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, Semaphore};

/// The channel was closed (the other half was dropped).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

/// Reason a non-blocking send failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrySendError {
    /// The window is full (no credit available).
    Full,
    /// The receiver was dropped.
    Closed,
}

struct Shared {
    credits: Semaphore,
    window: AtomicUsize,
}

/// Sending half of a windowed channel. Cheaply cloneable.
#[derive(Clone)]
pub struct WindowedSender<T> {
    tx: mpsc::UnboundedSender<T>,
    shared: Arc<Shared>,
}

/// Receiving half of a windowed channel.
pub struct WindowedReceiver<T> {
    rx: mpsc::UnboundedReceiver<T>,
    shared: Arc<Shared>,
}

/// Create a windowed channel with an initial window of `initial` slots.
pub fn windowed_channel<T>(initial: usize) -> (WindowedSender<T>, WindowedReceiver<T>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let shared = Arc::new(Shared {
        credits: Semaphore::new(initial),
        window: AtomicUsize::new(initial),
    });
    (
        WindowedSender {
            tx,
            shared: shared.clone(),
        },
        WindowedReceiver { rx, shared },
    )
}

impl<T> WindowedSender<T> {
    /// Send `item`, awaiting a free credit when the window is full.
    pub async fn send(&self, item: T) -> Result<(), Closed> {
        let permit = self.shared.credits.acquire().await.map_err(|_| Closed)?;
        self.tx.send(item).map_err(|_| Closed)?;
        permit.forget(); // credit is held until the item is received
        Ok(())
    }

    /// Send without waiting; fails if the window is full or the channel closed.
    pub fn try_send(&self, item: T) -> Result<(), TrySendError> {
        let permit = self
            .shared
            .credits
            .try_acquire()
            .map_err(|_| TrySendError::Full)?;
        self.tx.send(item).map_err(|_| TrySendError::Closed)?;
        permit.forget();
        Ok(())
    }

    /// Resize the window. Growing adds credits immediately; shrinking lowers the
    /// target so receives stop replenishing until in-flight items drain it down.
    pub fn resize(&self, new_window: usize) {
        let previous = self.shared.window.swap(new_window, Ordering::AcqRel);
        if new_window > previous {
            self.shared.credits.add_permits(new_window - previous);
        }
    }

    /// Current window target.
    pub fn window(&self) -> usize {
        self.shared.window.load(Ordering::Acquire)
    }

    /// Credits currently available to send without waiting.
    pub fn available_credits(&self) -> usize {
        self.shared.credits.available_permits()
    }
}

impl<T> WindowedReceiver<T> {
    /// Receive the next item, returning a credit (unless the window shrank).
    pub async fn recv(&mut self) -> Option<T> {
        let item = self.rx.recv().await?;
        self.replenish();
        Some(item)
    }

    /// Non-blocking receive.
    pub fn try_recv(&mut self) -> Option<T> {
        match self.rx.try_recv() {
            Ok(item) => {
                self.replenish();
                Some(item)
            }
            Err(_) => None,
        }
    }

    /// Return one credit, but only while under the current window target (so a
    /// shrink takes effect as items drain).
    fn replenish(&self) {
        if self.shared.credits.available_permits() < self.shared.window.load(Ordering::Acquire) {
            self.shared.credits.add_permits(1);
        }
    }

    /// Current window target.
    pub fn window(&self) -> usize {
        self.shared.window.load(Ordering::Acquire)
    }
}
