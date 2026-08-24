/// GraphQL module providing query, mutation, and subscription support (#242).
///
/// Exposes a `build_schema()` function that constructs the async-graphql
/// schema wired with a shared `SimplePubSub` for real-time subscriptions.
pub mod pubsub;
pub mod types;

use async_graphql::*;
use std::sync::Arc;
use tokio::sync::broadcast;

use self::pubsub::SimplePubSub;
use self::types::{BillingEvent, BillingEventFilter, MeterReading, MeterReadingFilter};

// ─── Topic constants ──────────────────────────────────────────────────

pub const METER_READINGS_TOPIC: &str = "METER_READINGS";
pub const BILLING_EVENTS_TOPIC: &str = "BILLING_EVENTS";

// ─── Query ────────────────────────────────────────────────────────────

pub struct Query;

#[Object]
impl Query {
    /// Returns the API version.
    async fn api_version(&self) -> &str {
        "1.0.0"
    }

    /// Health check via GraphQL.
    async fn health(&self) -> &str {
        "ok"
    }

    /// Total active GraphQL subscription connections.
    async fn active_subscriptions(&self, ctx: &Context<'_>) -> usize {
        ctx.data_unchecked::<Arc<SimplePubSub>>()
            .total_subscriber_count()
    }
}

// ─── Mutation ─────────────────────────────────────────────────────────

pub struct Mutation;

#[Object]
impl Mutation {
    /// Publish a meter reading (used by internal ingestion pipeline).
    async fn publish_meter_reading(
        &self,
        ctx: &Context<'_>,
        reading_id: String,
        device_id: String,
        service_type: String,
        value: String,
        unit: String,
        timestamp: String,
    ) -> Result<bool> {
        let ps = ctx.data_unchecked::<Arc<SimplePubSub>>();
        let reading = MeterReading {
            reading_id,
            device_id,
            service_type,
            value,
            unit,
            timestamp,
        };
        let payload =
            serde_json::to_value(&reading).map_err(|e| Error::new(e.to_string()))?;
        ps.publish(METER_READINGS_TOPIC, payload);
        Ok(true)
    }

    /// Publish a billing event (used by billing pipeline).
    async fn publish_billing_event(
        &self,
        ctx: &Context<'_>,
        event_id: String,
        device_id: String,
        service_type: String,
        amount: String,
        currency: String,
        status: String,
        timestamp: String,
        #[graphql(default)] description: Option<String>,
    ) -> Result<bool> {
        let ps = ctx.data_unchecked::<Arc<SimplePubSub>>();
        let event = BillingEvent {
            event_id,
            device_id,
            service_type,
            amount,
            currency,
            status,
            timestamp,
            description,
        };
        let payload =
            serde_json::to_value(&event).map_err(|e| Error::new(e.to_string()))?;
        ps.publish(BILLING_EVENTS_TOPIC, payload);
        Ok(true)
    }
}

// ─── Subscription ─────────────────────────────────────────────────────

pub struct Subscription;

#[Subscription]
impl Subscription {
    /// Subscribe to real-time meter readings with optional filtering.
    async fn meter_readings(
        &self,
        ctx: &Context<'_>,
        #[graphql(default)] filter: Option<MeterReadingFilter>,
    ) -> impl Stream<Item = MeterReading> {
        let ps = ctx.data_unchecked::<Arc<SimplePubSub>>().clone();
        let rx = ps.subscribe(METER_READINGS_TOPIC);

        MeterReadingStream {
            rx,
            device_id_filter: filter.as_ref().and_then(|f| f.device_id.clone()),
            service_type_filter: filter.as_ref().and_then(|f| f.service_type.clone()),
        }
    }

    /// Subscribe to real-time billing events with optional filtering.
    async fn billing_events(
        &self,
        ctx: &Context<'_>,
        #[graphql(default)] filter: Option<BillingEventFilter>,
    ) -> impl Stream<Item = BillingEvent> {
        let ps = ctx.data_unchecked::<Arc<SimplePubSub>>().clone();
        let rx = ps.subscribe(BILLING_EVENTS_TOPIC);

        BillingEventStream {
            rx,
            device_id_filter: filter.as_ref().and_then(|f| f.device_id.clone()),
            service_type_filter: filter.as_ref().and_then(|f| f.service_type.clone()),
        }
    }
}

// ─── Stream wrappers with filtering ───────────────────────────────────

use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use futures::Stream;
use serde::{Deserialize, Serialize};

struct MeterReadingStream {
    rx: broadcast::Receiver<serde_json::Value>,
    device_id_filter: Option<String>,
    service_type_filter: Option<String>,
}

impl Stream for MeterReadingStream {
    type Item = MeterReading;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match this.rx.poll_recv(cx) {
                Poll::Ready(Ok(value)) => {
                    if let Ok(reading) = serde_json::from_value::<MeterReading>(value) {
                        // Apply filters
                        if let Some(ref device_id) = this.device_id_filter {
                            if &reading.device_id != device_id {
                                continue;
                            }
                        }
                        if let Some(ref service_type) = this.service_type_filter {
                            if &reading.service_type != service_type {
                                continue;
                            }
                        }
                        return Poll::Ready(Some(reading));
                    }
                }
                Poll::Ready(Err(broadcast::error::RecvError::Lagged(n))) => {
                    tracing::warn!(
                        skipped = n,
                        "MeterReading subscription lagged, {} messages dropped",
                        n
                    );
                    continue;
                }
                Poll::Ready(Err(broadcast::error::RecvError::Closed)) => {
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

struct BillingEventStream {
    rx: broadcast::Receiver<serde_json::Value>,
    device_id_filter: Option<String>,
    service_type_filter: Option<String>,
}

impl Stream for BillingEventStream {
    type Item = BillingEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match this.rx.poll_recv(cx) {
                Poll::Ready(Ok(value)) => {
                    if let Ok(event) = serde_json::from_value::<BillingEvent>(value) {
                        if let Some(ref device_id) = this.device_id_filter {
                            if &event.device_id != device_id {
                                continue;
                            }
                        }
                        if let Some(ref service_type) = this.service_type_filter {
                            if &event.service_type != service_type {
                                continue;
                            }
                        }
                        return Poll::Ready(Some(event));
                    }
                }
                Poll::Ready(Err(broadcast::error::RecvError::Lagged(n))) => {
                    tracing::warn!(
                        skipped = n,
                        "BillingEvent subscription lagged, {} messages dropped",
                        n
                    );
                    continue;
                }
                Poll::Ready(Err(broadcast::error::RecvError::Closed)) => {
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ─── Schema builder ───────────────────────────────────────────────────

/// Build the GraphQL schema with the given PubSub instance.
pub fn build_schema(pubsub: Arc<SimplePubSub>) -> Schema<Query, Mutation, Subscription> {
    Schema::build(Query, Mutation, Subscription)
        .data(pubsub)
        .finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_schema() -> Schema<Query, Mutation, Subscription> {
        let ps = Arc::new(SimplePubSub::new(16));
        build_schema(ps)
    }

    #[tokio::test]
    async fn query_api_version() {
        let schema = test_schema();
        let resp = schema.execute("{ apiVersion }").await;
        assert_eq!(resp.data.into_json().unwrap(), json!({"apiVersion": "1.0.0"}));
    }

    #[tokio::test]
    async fn query_health() {
        let schema = test_schema();
        let resp = schema.execute("{ health }").await;
        assert_eq!(resp.data.into_json().unwrap(), json!({"health": "ok"}));
    }

    #[tokio::test]
    async fn publish_and_subscribe_meter_reading() {
        let schema = test_schema();

        // Start subscription in a task
        let schema_clone = schema.clone();
        let handle = tokio::spawn(async move {
            let mut stream = schema_clone
                .execute_stream("subscription { meterReadings { readingId deviceId serviceType value unit timestamp } }")
                .await
                .unwrap();

            // First event on the stream
            use futures::StreamExt;
            stream.next().await
        });

        // Give the subscription a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Publish via mutation
        schema
            .execute(
                r#"mutation {
                    publishMeterReading(
                        readingId: "r-1"
                        deviceId: "meter-1"
                        serviceType: "electricity"
                        value: "100"
                        unit: "kWh"
                        timestamp: "2026-08-24T10:00:00Z"
                    )
                }"#,
            )
            .await;

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .unwrap()
            .unwrap();

        assert!(result.is_some());
        let data = result.unwrap().data.into_json().unwrap();
        let reading = &data["meterReadings"];
        assert_eq!(reading["readingId"], "r-1");
        assert_eq!(reading["value"], "100");
    }

    #[tokio::test]
    async fn meter_reading_filter_by_device_id() {
        let schema = test_schema();

        let schema_clone = schema.clone();
        let handle = tokio::spawn(async move {
            let mut stream = schema_clone
                .execute_stream(
                    r#"subscription {
                        meterReadings(filter: { deviceId: "meter-2" }) {
                            readingId deviceId value
                        }
                    }"#,
                )
                .await
                .unwrap();

            use futures::StreamExt;
            stream.next().await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Publish one that should be filtered out (deviceId: meter-1)
        schema
            .execute(
                r#"mutation {
                    publishMeterReading(
                        readingId: "r-1", deviceId: "meter-1", serviceType: "electricity",
                        value: "50", unit: "kWh", timestamp: "2026-08-24T10:00:00Z"
                    )
                }"#,
            )
            .await;

        // Publish one that should match (deviceId: meter-2)
        schema
            .execute(
                r#"mutation {
                    publishMeterReading(
                        readingId: "r-2", deviceId: "meter-2", serviceType: "electricity",
                        value: "75", unit: "kWh", timestamp: "2026-08-24T10:01:00Z"
                    )
                }"#,
            )
            .await;

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let data = result.data.into_json().unwrap();
        assert_eq!(data["meterReadings"]["readingId"], "r-2");
        assert_eq!(data["meterReadings"]["deviceId"], "meter-2");
    }

    #[tokio::test]
    async fn active_subscriptions_query() {
        let schema = test_schema();
        let resp = schema.execute("{ activeSubscriptions }").await;
        assert_eq!(
            resp.data.into_json().unwrap(),
            json!({"activeSubscriptions": 0})
        );
    }
}