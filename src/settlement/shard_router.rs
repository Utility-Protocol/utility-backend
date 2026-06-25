//! Credit-controlled routing of cross-shard settlement messages.
//!
//! [`ShardSender`] is the egress side for one destination shard: it consumes
//! credits, buffers in memory, spills to the durable queue under pressure, and
//! forwards via a pluggable [`MessageSink`] (the "network"). [`ShardReceiver`]
//! is the ingress side: it deduplicates by `msg_id`, acknowledges the highest
//! contiguous id, and emits a [`CreditGrant`] every `grant_batch` messages.
//! [`ShardRouter`] owns the per-destination senders and routes acks/grants back
//! to them.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::Mutex;

use super::credit_flow::{CreditConfig, CreditFlowController};
use super::messages::{AckMessage, CreditGrant, SettlementMessage};
use crate::storage::durable_queue::DurableQueue;

/// The transport over which a [`ShardSender`] forwards messages. Implementations
/// model the inter-shard network (an mpsc channel, RPC, etc.).
#[async_trait]
pub trait MessageSink: Send + Sync {
    /// Deliver `msg` toward its destination shard. Delivery may be lossy; the
    /// retransmission timer recovers dropped messages.
    async fn deliver(&self, msg: SettlementMessage);
}

/// Egress side for a single destination shard.
pub struct ShardSender {
    dest_shard: u64,
    credit: Arc<CreditFlowController>,
    durable: Arc<dyn DurableQueue>,
    outbound: Mutex<VecDeque<SettlementMessage>>,
    /// Forwarded-but-unacknowledged messages, retained for retransmission.
    sent_log: Mutex<BTreeMap<u64, SettlementMessage>>,
    next_msg_id: AtomicU64,
    config: CreditConfig,
}

impl ShardSender {
    /// Create a sender targeting `dest_shard`.
    pub fn new(
        dest_shard: u64,
        credit: Arc<CreditFlowController>,
        durable: Arc<dyn DurableQueue>,
        config: CreditConfig,
    ) -> Self {
        Self {
            dest_shard,
            credit,
            durable,
            outbound: Mutex::new(VecDeque::new()),
            sent_log: Mutex::new(BTreeMap::new()),
            next_msg_id: AtomicU64::new(0),
            config,
        }
    }

    /// The credit controller backing this sender.
    pub fn credit(&self) -> &Arc<CreditFlowController> {
        &self.credit
    }

    fn next_id(&self) -> u64 {
        self.next_msg_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Enqueue `payload` for delivery, assigning the next `msg_id`.
    ///
    /// Buffers in memory when a credit is available and there is room; otherwise
    /// spills to the durable queue; and only blocks (backpressure to ingestion)
    /// once the durable queue is also full.
    pub async fn send(&self, payload: Vec<u8>) -> io::Result<u64> {
        let msg_id = self.next_id();
        let msg = SettlementMessage {
            shard_id: self.dest_shard,
            msg_id,
            payload,
        };

        let has_room = self.outbound.lock().len() < self.config.in_memory_buffer;
        if has_room && self.credit.try_acquire(1) {
            self.outbound.lock().push_back(msg);
        } else if self.durable.len(self.dest_shard) < self.config.durable_queue_max {
            self.durable.push(self.dest_shard, &msg)?;
        } else {
            self.credit.wait_for_credits(1).await;
            self.outbound.lock().push_back(msg);
        }
        Ok(msg_id)
    }

    /// Promote any spilled messages back into the in-memory buffer (subject to
    /// room and credit) and forward everything currently buffered through
    /// `sink`. Returns the number of messages forwarded.
    pub async fn drain_once(&self, sink: &dyn MessageSink) -> io::Result<usize> {
        loop {
            let room = {
                let buf = self.outbound.lock();
                self.config.in_memory_buffer.saturating_sub(buf.len())
            };
            if room == 0 || self.durable.is_empty(self.dest_shard) {
                break;
            }
            if !self.credit.try_acquire(1) {
                break;
            }
            match self
                .durable
                .pop_batch(self.dest_shard, 1)?
                .into_iter()
                .next()
            {
                Some(m) => self.outbound.lock().push_back(m),
                None => {
                    // Nothing to promote after all; hand the credit back.
                    self.credit.grant_credits(1);
                    break;
                }
            }
        }

        let mut forwarded = 0;
        loop {
            let next = { self.outbound.lock().pop_front() };
            let Some(msg) = next else { break };
            let id = msg.msg_id;
            self.sent_log.lock().insert(id, msg.clone());
            self.credit.record_pending(id);
            sink.deliver(msg).await;
            forwarded += 1;
        }
        Ok(forwarded)
    }

    /// Retransmit every message whose ack has timed out. Receivers deduplicate,
    /// so re-delivery is safe.
    pub async fn retransmit_timed_out(&self, sink: &dyn MessageSink) -> usize {
        let ids = self.credit.timed_out();
        let mut resent = 0;
        for id in ids {
            let msg = self.sent_log.lock().get(&id).cloned();
            if let Some(msg) = msg {
                sink.deliver(msg).await;
                resent += 1;
            }
        }
        resent
    }

    /// Apply an incoming ack: advance the credit watermark and drop acknowledged
    /// messages from the retransmission log.
    pub fn handle_ack(&self, acked_msg_id: u64) {
        self.credit.process_ack(acked_msg_id);
        self.sent_log.lock().retain(|id, _| *id > acked_msg_id);
    }

    /// Total messages still buffered in memory awaiting forwarding.
    pub fn buffered_len(&self) -> usize {
        self.outbound.lock().len()
    }

    /// Messages spilled to the durable queue for this destination.
    pub fn durable_len(&self) -> usize {
        self.durable.len(self.dest_shard)
    }

    /// Messages forwarded but not yet acknowledged.
    pub fn unacked_len(&self) -> usize {
        self.sent_log.lock().len()
    }
}

/// Per-source receiver state guarded by a single lock to keep dedup, watermark,
/// and grant accounting consistent.
struct ReceiverState {
    /// Accepted ids strictly above `contiguous` (the out-of-order window).
    gap: HashSet<u64>,
    /// Highest contiguous accepted `msg_id` (0 = none).
    contiguous: u64,
    processed_since_grant: u32,
    total_accepted: u64,
    delivered: Vec<SettlementMessage>,
}

/// The result of feeding one message to a [`ShardReceiver`].
#[derive(Clone, Debug)]
pub struct ReceiverOutcome {
    /// Acknowledgement to send back to the sender.
    pub ack: AckMessage,
    /// Credit grant to send back, if a batch boundary was crossed.
    pub grant: Option<CreditGrant>,
    /// Whether the message was a duplicate (already accepted).
    pub duplicate: bool,
}

/// Ingress side for messages arriving at this shard.
pub struct ShardReceiver {
    /// This receiver's own shard id; used to address acks/grants back so the
    /// sender can route them to the correct egress stream.
    shard_id: u64,
    state: Mutex<ReceiverState>,
    config: CreditConfig,
}

impl ShardReceiver {
    /// Create a receiver for the local `shard_id`.
    pub fn new(shard_id: u64, config: CreditConfig) -> Self {
        Self {
            shard_id,
            state: Mutex::new(ReceiverState {
                gap: HashSet::new(),
                contiguous: 0,
                processed_since_grant: 0,
                total_accepted: 0,
                delivered: Vec::new(),
            }),
            config,
        }
    }

    /// Accept a message: deduplicate, advance the contiguous watermark, and emit
    /// an ack (always) plus a credit grant (every `grant_batch` new messages).
    pub fn on_message(&self, msg: SettlementMessage) -> ReceiverOutcome {
        let id = msg.msg_id;
        let mut st = self.state.lock();

        if id <= st.contiguous || st.gap.contains(&id) {
            let ack = AckMessage {
                shard_id: self.shard_id,
                acked_msg_id: st.contiguous,
            };
            return ReceiverOutcome {
                ack,
                grant: None,
                duplicate: true,
            };
        }

        st.gap.insert(id);
        let mut c = st.contiguous;
        while st.gap.remove(&(c + 1)) {
            c += 1;
        }
        st.contiguous = c;
        st.total_accepted += 1;
        st.delivered.push(msg);

        st.processed_since_grant += 1;
        let grant = if st.processed_since_grant >= self.config.grant_batch {
            st.processed_since_grant -= self.config.grant_batch;
            Some(CreditGrant {
                shard_id: self.shard_id,
                delta: self.config.grant_batch,
            })
        } else {
            None
        };

        let ack = AckMessage {
            shard_id: self.shard_id,
            acked_msg_id: c,
        };
        ReceiverOutcome {
            ack,
            grant,
            duplicate: false,
        }
    }

    /// Total distinct messages accepted (exactly-once delivered to the app).
    pub fn accepted_count(&self) -> u64 {
        self.state.lock().total_accepted
    }

    /// Highest contiguous accepted `msg_id`.
    pub fn contiguous_watermark(&self) -> u64 {
        self.state.lock().contiguous
    }

    /// Drain and return all messages delivered to the application so far.
    pub fn take_delivered(&self) -> Vec<SettlementMessage> {
        std::mem::take(&mut self.state.lock().delivered)
    }
}

/// Owns the per-destination senders for a node and routes acks/grants back.
pub struct ShardRouter {
    local_shard: u64,
    config: CreditConfig,
    durable: Arc<dyn DurableQueue>,
    senders: DashMap<u64, Arc<ShardSender>>,
}

impl ShardRouter {
    /// Create a router for `local_shard` using `durable` for spillover.
    pub fn new(local_shard: u64, durable: Arc<dyn DurableQueue>, config: CreditConfig) -> Self {
        Self {
            local_shard,
            config,
            durable,
            senders: DashMap::new(),
        }
    }

    /// This node's shard id.
    pub fn local_shard(&self) -> u64 {
        self.local_shard
    }

    /// Get (creating on first use) the egress sender for `dest_shard`.
    pub fn sender(&self, dest_shard: u64) -> Arc<ShardSender> {
        let entry = self.senders.entry(dest_shard).or_insert_with(|| {
            let credit = Arc::new(CreditFlowController::new(dest_shard, self.config));
            Arc::new(ShardSender::new(
                dest_shard,
                credit,
                self.durable.clone(),
                self.config,
            ))
        });
        entry.value().clone()
    }

    /// Enqueue `payload` for `dest_shard`.
    pub async fn send(&self, dest_shard: u64, payload: Vec<u8>) -> io::Result<u64> {
        self.sender(dest_shard).send(payload).await
    }

    /// Drain and forward buffered/spilled messages for `dest_shard`.
    pub async fn drain(&self, dest_shard: u64, sink: &dyn MessageSink) -> io::Result<usize> {
        self.sender(dest_shard).drain_once(sink).await
    }

    /// Route an incoming credit grant to the sender it addresses.
    pub fn on_credit_grant(&self, grant: CreditGrant) {
        if let Some(sender) = self.senders.get(&grant.shard_id) {
            sender.credit().grant_credits(grant.delta);
        }
    }

    /// Route an incoming ack to the sender it addresses.
    pub fn on_ack(&self, ack: AckMessage) {
        if let Some(sender) = self.senders.get(&ack.shard_id) {
            sender.handle_ack(ack.acked_msg_id);
        }
    }
}
