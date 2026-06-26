//! Structured concurrency primitives for graceful shutdown (issue #49).
//!
//! The blueprint references `tokio_util`'s `CancellationToken` / `TaskGroup`.
//! To avoid a new dependency (and an API that does not exist under that exact
//! name), this provides self-contained equivalents over plain `tokio`:
//!
//! * [`CancelToken`] — a hierarchical cancellation signal. Cancelling a parent
//!   cascades to all of its child tokens, modelling the per-stage token tree.
//! * [`StructuredTaskGroup`] — owns the tasks of one pipeline stage and its
//!   cancellation token; [`StructuredTaskGroup::shutdown`] cancels then drains
//!   them within a deadline, reporting how many failed to finish in time.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

#[derive(Default)]
struct CancelInner {
    cancelled: AtomicBool,
    notify: Notify,
    children: Mutex<Vec<CancelToken>>,
}

/// A hierarchical cancellation signal. Cheaply cloneable; clones share state.
#[derive(Clone, Default)]
pub struct CancelToken {
    inner: Arc<CancelInner>,
}

impl CancelToken {
    /// Create a new, un-cancelled root token.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    /// Request cancellation, waking waiters and cascading to child tokens.
    pub fn cancel(&self) {
        if !self.inner.cancelled.swap(true, Ordering::AcqRel) {
            self.inner.notify.notify_waiters();
            for child in self.inner.children.lock().iter() {
                child.cancel();
            }
        }
    }

    /// Resolve once cancellation is requested. A bounded re-poll backstops the
    /// wait so a `notify_waiters` racing ahead of registration cannot hang it.
    pub async fn cancelled(&self) {
        while !self.is_cancelled() {
            let notified = self.inner.notify.notified();
            if self.is_cancelled() {
                return;
            }
            let _ = tokio::time::timeout(Duration::from_millis(50), notified).await;
        }
    }

    /// Create a child token that is cancelled when this token is (or already is).
    pub fn child(&self) -> CancelToken {
        let child = CancelToken::new();
        if self.is_cancelled() {
            child.cancel();
        } else {
            self.inner.children.lock().push(child.clone());
        }
        child
    }
}

/// Owns the tasks of one pipeline stage plus its cancellation token.
pub struct StructuredTaskGroup {
    token: CancelToken,
    handles: Mutex<Vec<JoinHandle<()>>>,
    in_flight: Arc<AtomicUsize>,
}

impl StructuredTaskGroup {
    /// Create a group governed by `token`.
    pub fn new(token: CancelToken) -> Self {
        Self {
            token,
            handles: Mutex::new(Vec::new()),
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The group's cancellation token (clone it into spawned tasks to observe
    /// cancellation).
    pub fn token(&self) -> &CancelToken {
        &self.token
    }

    /// Spawn a task owned by this group; it is counted as in-flight until it
    /// completes.
    pub fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        let counter = self.in_flight.clone();
        let handle = tokio::spawn(async move {
            fut.await;
            counter.fetch_sub(1, Ordering::AcqRel);
        });
        self.handles.lock().push(handle);
    }

    /// Number of spawned tasks that have not yet completed.
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    /// Number of registered (not-yet-awaited) task handles.
    pub fn len(&self) -> usize {
        self.handles.lock().len()
    }

    /// Whether the group has no registered handles.
    pub fn is_empty(&self) -> bool {
        self.handles.lock().is_empty()
    }

    /// Cancel the group's token and await its tasks, up to `deadline`. Returns
    /// the number of tasks still running when the deadline elapsed (0 on a clean
    /// drain). Timed-out tasks are detached, not aborted, per the graceful
    /// protocol.
    pub async fn shutdown(&self, deadline: Duration) -> usize {
        self.token.cancel();
        let handles: Vec<JoinHandle<()>> = std::mem::take(&mut *self.handles.lock());
        let waiter = async move {
            for handle in handles {
                let _ = handle.await;
            }
        };
        if tokio::time::timeout(deadline, waiter).await.is_err() {
            self.in_flight.load(Ordering::Acquire)
        } else {
            0
        }
    }
}
