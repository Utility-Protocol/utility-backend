//! Settlement execution entry point for cross-shard messaging.
//!
//! The executor is the integration seam described in issue #54: before emitting
//! cross-shard settlement messages it goes through the credit-flow-controlled
//! [`ShardRouter`], so flow control and durable spillover apply uniformly to all
//! settlement traffic.

use std::io;
use std::sync::Arc;

use super::messages::{AckMessage, CreditGrant};
use super::shard_router::{MessageSink, ShardRouter};

/// Drives credit-controlled cross-shard settlement delivery.
pub struct SettlementExecutor {
    router: Arc<ShardRouter>,
}

impl SettlementExecutor {
    /// Build an executor over the given router.
    pub fn new(router: Arc<ShardRouter>) -> Self {
        Self { router }
    }

    /// The underlying router (for wiring inbound acks/grants and drain loops).
    pub fn router(&self) -> &Arc<ShardRouter> {
        &self.router
    }

    /// Submit one settlement payload to `dest_shard`. Returns the assigned
    /// `msg_id`. Flow control / spillover is enforced inside the router.
    #[tracing::instrument(skip(self, payload), fields(otel.kind = "producer", messaging.system = "settlement"))]
    pub async fn submit_cross_shard(&self, dest_shard: u64, payload: Vec<u8>) -> io::Result<u64> {
        self.router.send(dest_shard, payload).await
    }

    /// Submit a batch of payloads to `dest_shard`, returning the assigned ids in
    /// order. Credits are acquired per message by the router as each is enqueued.
    #[tracing::instrument(skip(self, payloads), fields(otel.kind = "producer", messaging.system = "settlement"))]
    pub async fn submit_batch(
        &self,
        dest_shard: u64,
        payloads: Vec<Vec<u8>>,
    ) -> io::Result<Vec<u64>> {
        let mut ids = Vec::with_capacity(payloads.len());
        for payload in payloads {
            ids.push(self.router.send(dest_shard, payload).await?);
        }
        Ok(ids)
    }

    /// Push buffered/spilled traffic for `dest_shard` onto the wire.
    pub async fn flush(&self, dest_shard: u64, sink: &dyn MessageSink) -> io::Result<usize> {
        self.router.drain(dest_shard, sink).await
    }

    /// Apply an acknowledgement received from a peer.
    pub fn on_ack(&self, ack: AckMessage) {
        self.router.on_ack(ack);
    }

    /// Apply a credit grant received from a peer.
    pub fn on_credit_grant(&self, grant: CreditGrant) {
        self.router.on_credit_grant(grant);
    }
}
