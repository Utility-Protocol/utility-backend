//! Stress/integration tests for the cross-shard credit-flow protocol (issue #54).
//!
//! These tests are deterministic on purpose: a fixed (not random) packet-loss
//! pattern, a zero ack-timeout so retransmission is driven by the pump loop
//! rather than wall-clock time, and a bounded convergence loop. That keeps them
//! fast and flake-free in CI while still exercising every invariant: credit
//! flow, durable spillover, exactly-once delivery, lossy retransmission, and
//! full backlog drain.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;

use utility_backend::settlement::credit_flow::{CreditConfig, CreditFlowController};
use utility_backend::settlement::messages::SettlementMessage;
use utility_backend::settlement::shard_router::{MessageSink, ShardReceiver, ShardRouter};
use utility_backend::storage::durable_queue::{
    DurableQueue, FileDurableQueue, InMemoryDurableQueue,
};

/// A lossy in-process network: forwards to the destination receiver and feeds
/// the resulting ack/grant straight back to the sending router. Each id whose
/// `id % drop_modulo == 0` is dropped exactly once (its first delivery attempt)
/// to exercise timeout-based retransmission.
struct LossySink {
    receiver: Arc<ShardReceiver>,
    sender_router: Arc<ShardRouter>,
    dropped_once: Mutex<HashSet<u64>>,
    drop_modulo: u64,
}

#[async_trait]
impl MessageSink for LossySink {
    async fn deliver(&self, msg: SettlementMessage) {
        let id = msg.msg_id;
        // Drop each `drop_modulo`-th id exactly once: `insert` returns true the
        // first time we see it, so that first delivery attempt is dropped.
        if self.drop_modulo != 0
            && id % self.drop_modulo == 0
            && self.dropped_once.lock().insert(id)
        {
            return;
        }
        let outcome = self.receiver.on_message(msg);
        self.sender_router.on_ack(outcome.ack);
        if let Some(grant) = outcome.grant {
            self.sender_router.on_credit_grant(grant);
        }
    }
}

#[tokio::test]
async fn test_burst_migration_exactly_once_with_spillover_and_drain() {
    const NUM_SHARDS: u64 = 4;
    const MESSAGES_PER_SHARD: u64 = 3_000;
    const MAX_ROUNDS: u32 = 1_000_000;

    // Tight credit/buffer bounds (relative to the load) so spillover to the
    // durable queue is forced during the burst.
    let config = CreditConfig {
        initial_credit_balance: 500,
        grant_batch: 100,
        in_memory_buffer: 200,
        durable_queue_max: 10_000_000,
        ack_timeout: std::time::Duration::ZERO,
    };

    struct Pair {
        dest: u64,
        router: Arc<ShardRouter>,
        receiver: Arc<ShardReceiver>,
        sink: Arc<LossySink>,
    }

    let mut pairs = Vec::new();
    for dest in 0..NUM_SHARDS {
        let durable: Arc<dyn DurableQueue> = Arc::new(InMemoryDurableQueue::new());
        let router = Arc::new(ShardRouter::new(dest, durable, config));
        let receiver = Arc::new(ShardReceiver::new(dest, config));
        let sink = Arc::new(LossySink {
            receiver: receiver.clone(),
            sender_router: router.clone(),
            dropped_once: Mutex::new(HashSet::new()),
            drop_modulo: 50, // ~2% loss on first attempt
        });
        pairs.push(Pair {
            dest,
            router,
            receiver,
            sink,
        });
    }

    // Burst: each shard enqueues its whole migration batch at once.
    for pair in &pairs {
        for i in 0..MESSAGES_PER_SHARD {
            pair.router
                .send(pair.dest, i.to_le_bytes().to_vec())
                .await
                .expect("send");
        }
    }

    // (c) Durable spillover must have been used during the burst.
    let mut durable_used = false;
    for pair in &pairs {
        if pair.router.sender(pair.dest).durable_len() > 0 {
            durable_used = true;
        }
    }
    assert!(
        durable_used,
        "durable queue should absorb the burst overflow"
    );

    // Pump drain + retransmission until the system fully quiesces.
    let mut rounds = 0u32;
    loop {
        rounds += 1;
        assert!(rounds < MAX_ROUNDS, "did not converge");

        let mut active = false;
        for pair in &pairs {
            let sender = pair.router.sender(pair.dest);
            let forwarded = sender.drain_once(pair.sink.as_ref()).await.expect("drain");
            let resent = sender.retransmit_timed_out(pair.sink.as_ref()).await;
            if forwarded > 0
                || resent > 0
                || sender.buffered_len() > 0
                || sender.durable_len() > 0
                || sender.unacked_len() > 0
            {
                active = true;
            }
        }
        if !active {
            break;
        }
    }

    // (d) Zero in-memory and on-disk backlog after the burst drains.
    for pair in &pairs {
        let sender = pair.router.sender(pair.dest);
        assert_eq!(sender.buffered_len(), 0, "in-memory backlog not drained");
        assert_eq!(sender.durable_len(), 0, "durable backlog not drained");
        assert_eq!(sender.unacked_len(), 0, "unacked messages remain");
    }

    // (a) + (b) Exactly-once delivery: no loss, no duplicates.
    for pair in &pairs {
        assert_eq!(pair.receiver.accepted_count(), MESSAGES_PER_SHARD);
        assert_eq!(pair.receiver.contiguous_watermark(), MESSAGES_PER_SHARD);

        let delivered = pair.receiver.take_delivered();
        assert_eq!(delivered.len() as u64, MESSAGES_PER_SHARD);
        let ids: HashSet<u64> = delivered.iter().map(|m| m.msg_id).collect();
        assert_eq!(ids.len() as u64, MESSAGES_PER_SHARD, "duplicate delivery");
        for id in 1..=MESSAGES_PER_SHARD {
            assert!(ids.contains(&id), "missing message {id}");
        }
    }
}

#[tokio::test]
async fn test_credit_acquire_grant_and_wait() {
    let config = CreditConfig {
        initial_credit_balance: 2,
        grant_batch: 1,
        in_memory_buffer: 10,
        durable_queue_max: 10,
        ack_timeout: std::time::Duration::ZERO,
    };
    let credit = Arc::new(CreditFlowController::new(7, config));

    assert!(credit.try_acquire(2));
    assert!(!credit.try_acquire(1));
    assert_eq!(credit.credit_balance(), 0);

    // A waiter parks until a grant arrives.
    let waiter = {
        let c = credit.clone();
        tokio::spawn(async move {
            c.wait_for_credits(1).await;
        })
    };
    // Give the waiter a moment to park, then replenish.
    tokio::task::yield_now().await;
    credit.grant_credits(5);
    waiter.await.unwrap();
    // 5 granted, 1 taken by the waiter.
    assert_eq!(credit.credit_balance(), 4);
}

#[tokio::test]
async fn test_ack_advances_watermark_and_clears_pending() {
    let credit = CreditFlowController::new(1, CreditConfig::default());
    for id in 1..=5 {
        credit.record_pending(id);
    }
    assert_eq!(credit.pending_count(), 5);

    credit.process_ack(3);
    assert_eq!(credit.acked_watermark(), 3);
    assert_eq!(credit.pending_count(), 2); // ids 4 and 5 remain
}

#[test]
fn test_file_durable_queue_fifo_and_reclaim() {
    let dir = std::env::temp_dir().join(format!("credit_flow_dq_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let queue = FileDurableQueue::open(&dir).expect("open durable queue");

    for i in 0..5u64 {
        let msg = SettlementMessage {
            shard_id: 9,
            msg_id: i + 1,
            payload: vec![i as u8; 16],
        };
        queue.push(9, &msg).expect("push");
    }
    assert_eq!(queue.len(9), 5);

    let first = queue.pop_batch(9, 3).expect("pop");
    assert_eq!(first.len(), 3);
    assert_eq!(first[0].msg_id, 1);
    assert_eq!(first[2].msg_id, 3);
    assert_eq!(queue.len(9), 2);

    let rest = queue.pop_batch(9, 10).expect("pop");
    assert_eq!(rest.len(), 2);
    assert_eq!(rest[1].msg_id, 5);
    assert!(queue.is_empty(9));

    // Queue is reusable after draining (file reclaimed).
    let msg = SettlementMessage {
        shard_id: 9,
        msg_id: 99,
        payload: vec![7; 4],
    };
    queue.push(9, &msg).expect("push after drain");
    assert_eq!(queue.len(9), 1);
    assert_eq!(queue.pop_batch(9, 1).expect("pop")[0].msg_id, 99);

    let _ = std::fs::remove_dir_all(&dir);
}
