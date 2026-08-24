/// GraphQL PubSub for subscription event delivery (#242).
///
/// Wraps `tokio::sync::broadcast` to provide per-topic event streams
/// that GraphQL subscription resolvers can yield to connected clients.
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use tokio::sync::broadcast;

/// Simple broadcast-based PubSub for GraphQL subscriptions.
///
/// Each topic is a broadcast channel. When `publish` is called,
/// all active subscribers on that topic receive the event.
#[derive(Clone)]
pub struct SimplePubSub {
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<serde_json::Value>>>>,
    capacity: usize,
}

impl SimplePubSub {
    /// Create a new PubSub with the given per-channel buffer capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            capacity,
        }
    }

    /// Publish a JSON payload to all subscribers of `topic`.
    /// Returns the number of subscribers that received the message.
    pub fn publish(&self, topic: &str, payload: serde_json::Value) -> usize {
        let channels = self.channels.read();
        if let Some(tx) = channels.get(topic) {
            tx.send(payload).unwrap_or(0)
        } else {
            0
        }
    }

    /// Subscribe to `topic` and return a broadcast receiver.
    ///
    /// The receiver yields `serde_json::Value` events. When all receivers
    /// for a topic are dropped, the channel is cleaned up lazily.
    pub fn subscribe(&self, topic: &str) -> broadcast::Receiver<serde_json::Value> {
        let mut channels = self.channels.write();
        if let Some(tx) = channels.get(topic) {
            // Check if the sender still has active receivers; if not, replace it
            if tx.receiver_count() > 0 {
                return tx.subscribe();
            }
        }
        // Create a new channel
        let (tx, rx) = broadcast::channel(self.capacity);
        channels.insert(topic.to_string(), tx);
        rx
    }

    /// Return the number of active subscribers for a topic.
    pub fn subscriber_count(&self, topic: &str) -> usize {
        let channels = self.channels.read();
        channels
            .get(topic)
            .map(|tx| tx.receiver_count())
            .unwrap_or(0)
    }

    /// Total active subscriptions across all topics.
    pub fn total_subscriber_count(&self) -> usize {
        let channels = self.channels.read();
        channels.values().map(|tx| tx.receiver_count()).sum()
    }

    /// Clean up channels with zero receivers (lazy GC).
    pub fn gc(&self) {
        let mut channels = self.channels.write();
        channels.retain(|_, tx| tx.receiver_count() > 0);
    }
}

impl Default for SimplePubSub {
    fn default() -> Self {
        Self::new(256)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn publish_subscribe_delivers_payload() {
        let ps = SimplePubSub::new(16);
        let mut rx = ps.subscribe("topic.a");

        let payload = json!({"deviceId": "d1", "value": "42"});
        let count = ps.publish("topic.a", payload.clone());
        assert_eq!(count, 1);

        let received = rx.recv().await.unwrap();
        assert_eq!(received, payload);
    }

    #[tokio::test]
    async fn topic_isolation() {
        let ps = SimplePubSub::new(16);
        let mut rx_a = ps.subscribe("topic.a");
        let mut rx_b = ps.subscribe("topic.b");

        ps.publish("topic.a", json!({"msg": "a"}));

        let val_a = rx_a.recv().await.unwrap();
        assert_eq!(val_a, json!({"msg": "a"}));

        // topic.b should be empty
        assert!(rx_b.try_recv().is_err());
    }

    #[tokio::test]
    async fn subscriber_count_tracking() {
        let ps = SimplePubSub::new(16);
        assert_eq!(ps.subscriber_count("topic.x"), 0);

        let rx1 = ps.subscribe("topic.x");
        assert_eq!(ps.subscriber_count("topic.x"), 1);

        let rx2 = ps.subscribe("topic.x");
        assert_eq!(ps.subscriber_count("topic.x"), 2);

        drop(rx1);
        // broadcast counts lag until the sender's send hits; we can publish to flush
        let _ = ps.publish("topic.x", json!({"ping": 1}));
        assert_eq!(ps.subscriber_count("topic.x"), 1);

        drop(rx2);
        let _ = ps.publish("topic.x", json!({"ping": 2}));
        assert_eq!(ps.subscriber_count("topic.x"), 0);
    }

    #[tokio::test]
    async fn gc_removes_dead_channels() {
        let ps = SimplePubSub::new(16);
        {
            let _rx = ps.subscribe("ephemeral");
        }
        ps.gc();
        assert_eq!(ps.total_subscriber_count(), 0);
    }
}