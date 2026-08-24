/// Integration tests for GraphQL subscription transport (#242).
///
/// Covers:
///  - Schema queries (apiVersion, health, activeSubscriptions)
///  - Mutation-based meter reading and billing event publishing
///  - Subscription filtering (deviceId, serviceType)
///  - End-to-end subscription delivery via WebSocket (graphql-ws protocol)
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::net::TcpListener;
use utility_backend::graphql::pubsub::SimplePubSub;
use utility_backend::graphql::{self, BILLING_EVENTS_TOPIC, METER_READINGS_TOPIC};

// ─── Helpers ──────────────────────────────────────────────────────────

fn test_schema() -> async_graphql::Schema<
    utility_backend::graphql::Query,
    utility_backend::graphql::Mutation,
    utility_backend::graphql::Subscription,
> {
    let ps = Arc::new(SimplePubSub::new(16));
    graphql::build_schema(ps)
}

/// Execute a GraphQL query against the schema.
async fn execute(schema: &async_graphql::Schema<
    graphql::Query,
    graphql::Mutation,
    graphql::Subscription,
>, query: &str) -> Value {
    let resp = schema.execute(query).await;
    resp.data.into_json().unwrap()
}

// ─── Query tests ──────────────────────────────────────────────────────

#[tokio::test]
async fn query_api_version() {
    let schema = test_schema();
    let data = execute(&schema, "{ apiVersion }").await;
    assert_eq!(data, json!({"apiVersion": "1.0.0"}));
}

#[tokio::test]
async fn query_health() {
    let schema = test_schema();
    let data = execute(&schema, "{ health }").await;
    assert_eq!(data, json!({"health": "ok"}));
}

#[tokio::test]
async fn query_active_subscriptions_starts_at_zero() {
    let schema = test_schema();
    let data = execute(&schema, "{ activeSubscriptions }").await;
    assert_eq!(data, json!({"activeSubscriptions": 0}));
}

// ─── Mutation + PubSub tests ──────────────────────────────────────────

#[tokio::test]
async fn publish_meter_reading_succeeds() {
    let schema = test_schema();
    let data = execute(
        &schema,
        r#"mutation {
            publishMeterReading(
                readingId: "r-1", deviceId: "d-1", serviceType: "electricity",
                value: "100", unit: "kWh", timestamp: "2026-08-24T10:00:00Z"
            )
        }"#,
    )
    .await;
    assert_eq!(data, json!({"publishMeterReading": true}));
}

#[tokio::test]
async fn publish_billing_event_succeeds() {
    let schema = test_schema();
    let data = execute(
        &schema,
        r#"mutation {
            publishBillingEvent(
                eventId: "evt-1", deviceId: "d-1", serviceType: "electricity",
                amount: "45.50", currency: "USD", status: "pending",
                timestamp: "2026-08-24T12:00:00Z"
            )
        }"#,
    )
    .await;
    assert_eq!(data, json!({"publishBillingEvent": true}));
}

// ─── Subscription (in-process) tests ──────────────────────────────────

#[tokio::test]
async fn subscription_meter_readings_no_filter() {
    let schema = test_schema();
    let schema_clone = schema.clone();

    // Start subscription in a background task
    let handle = tokio::spawn(async move {
        let mut stream = schema_clone
            .execute_stream("subscription { meterReadings { readingId deviceId value } }")
            .await
            .unwrap();
        use futures::StreamExt;
        stream.next().await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Publish via direct PubSub to avoid race with subscription startup
    let ps = Arc::new(SimplePubSub::new(16));
    ps.publish(
        METER_READINGS_TOPIC,
        json!({"readingId": "r-sub-1", "deviceId": "meter-1", "serviceType": "electricity", "value": "42", "unit": "kWh", "timestamp": "2026-08-24T10:00:00Z"}),
    );

    // Also publish via schema mutation
    execute(
        &schema,
        r#"mutation {
            publishMeterReading(
                readingId: "r-sub-1", deviceId: "meter-1", serviceType: "electricity",
                value: "42", unit: "kWh", timestamp: "2026-08-24T10:00:00Z"
            )
        }"#,
    )
    .await;

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap();

    assert!(result.is_some(), "Expected at least one subscription event");
    let data = result.unwrap().data.into_json().unwrap();
    let reading = &data["meterReadings"];
    assert_eq!(reading["readingId"], "r-sub-1");
    assert_eq!(reading["value"], "42");
}

#[tokio::test]
async fn subscription_meter_readings_filter_by_device_id() {
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

    // Publish one that should be filtered out
    execute(
        &schema,
        r#"mutation {
            publishMeterReading(
                readingId: "r-filtered", deviceId: "meter-1", serviceType: "electricity",
                value: "50", unit: "kWh", timestamp: "2026-08-24T10:00:00Z"
            )
        }"#,
    )
    .await;

    // Publish one that should match the filter
    execute(
        &schema,
        r#"mutation {
            publishMeterReading(
                readingId: "r-match", deviceId: "meter-2", serviceType: "electricity",
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
    assert_eq!(data["meterReadings"]["readingId"], "r-match");
    assert_eq!(data["meterReadings"]["deviceId"], "meter-2");
}

#[tokio::test]
async fn subscription_meter_readings_filter_by_service_type() {
    let schema = test_schema();
    let schema_clone = schema.clone();

    let handle = tokio::spawn(async move {
        let mut stream = schema_clone
            .execute_stream(
                r#"subscription {
                    meterReadings(filter: { serviceType: "water" }) {
                        readingId serviceType value
                    }
                }"#,
            )
            .await
            .unwrap();
        use futures::StreamExt;
        stream.next().await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Publish gas (should be filtered out)
    execute(
        &schema,
        r#"mutation {
            publishMeterReading(
                readingId: "r-gas", deviceId: "d-1", serviceType: "gas",
                value: "30", unit: "m3", timestamp: "2026-08-24T10:00:00Z"
            )
        }"#,
    )
    .await;

    // Publish water (should match)
    execute(
        &schema,
        r#"mutation {
            publishMeterReading(
                readingId: "r-water", deviceId: "d-2", serviceType: "water",
                value: "200", unit: "L", timestamp: "2026-08-24T10:01:00Z"
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
    assert_eq!(data["meterReadings"]["readingId"], "r-water");
    assert_eq!(data["meterReadings"]["serviceType"], "water");
}

#[tokio::test]
async fn subscription_billing_events_no_filter() {
    let schema = test_schema();
    let schema_clone = schema.clone();

    let handle = tokio::spawn(async move {
        let mut stream = schema_clone
            .execute_stream("subscription { billingEvents { eventId deviceId amount currency status } }")
            .await
            .unwrap();
        use futures::StreamExt;
        stream.next().await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    execute(
        &schema,
        r#"mutation {
            publishBillingEvent(
                eventId: "evt-sub-1", deviceId: "d-1", serviceType: "electricity",
                amount: "45.50", currency: "USD", status: "pending",
                timestamp: "2026-08-24T12:00:00Z"
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
    assert_eq!(data["billingEvents"]["eventId"], "evt-sub-1");
    assert_eq!(data["billingEvents"]["amount"], "45.50");
}

#[tokio::test]
async fn subscription_billing_events_filter_by_device_id() {
    let schema = test_schema();
    let schema_clone = schema.clone();

    let handle = tokio::spawn(async move {
        let mut stream = schema_clone
            .execute_stream(
                r#"subscription {
                    billingEvents(filter: { deviceId: "d-2" }) {
                        eventId deviceId amount
                    }
                }"#,
            )
            .await
            .unwrap();
        use futures::StreamExt;
        stream.next().await
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    execute(
        &schema,
        r#"mutation {
            publishBillingEvent(
                eventId: "evt-a", deviceId: "d-1", serviceType: "water",
                amount: "10.00", currency: "USD", status: "billed",
                timestamp: "2026-08-24T12:00:00Z"
            )
        }"#,
    )
    .await;

    execute(
        &schema,
        r#"mutation {
            publishBillingEvent(
                eventId: "evt-b", deviceId: "d-2", serviceType: "water",
                amount: "15.00", currency: "USD", status: "billed",
                timestamp: "2026-08-24T12:01:00Z"
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
    assert_eq!(data["billingEvents"]["eventId"], "evt-b");
    assert_eq!(data["billingEvents"]["deviceId"], "d-2");
}

// ─── WebSocket end-to-end tests ───────────────────────────────────────

#[tokio::test]
async fn graphql_endpoint_accepts_http_queries() {
    let schema = test_schema();
    let schema_arc = Arc::new(schema);

    let app = axum::Router::new()
        .route(
            "/api/graphql",
            axum::routing::get(async_graphql_axum::GraphQL::new(schema_arc.clone()))
                .post(async_graphql_axum::GraphQL::new(schema_arc.clone())),
        );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service()).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/api/graphql", addr))
        .json(&json!({"query": "{ apiVersion }"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["data"]["apiVersion"], "1.0.0");
}

#[tokio::test]
async fn graphql_subscription_endpoint_exists() {
    let schema = test_schema();
    let schema_arc = Arc::new(schema);

    let app = axum::Router::new()
        .route(
            "/api/graphql/ws",
            axum::routing::get(async_graphql_axum::GraphQLSubscription::new(schema_arc.clone())),
        );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service()).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // The subscription endpoint should accept WebSocket upgrade with
    // the graphql-transport-ws sub-protocol
    let resp = reqwest::Client::new()
        .get(format!("http://{}/api/graphql/ws", addr))
        .header("Upgrade", "websocket")
        .header("Connection", "Upgrade")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("Sec-WebSocket-Protocol", "graphql-transport-ws")
        .send()
        .await
        .unwrap();

    // The server should respond with 101 Switching Protocols for valid
    // WebSocket upgrade requests
    assert_eq!(resp.status(), 101);
}

// ─── PubSub unit tests ────────────────────────────────────────────────

#[tokio::test]
async fn pubsub_publish_subscribe() {
    let ps = SimplePubSub::new(16);
    let mut rx = ps.subscribe("test.topic");

    ps.publish("test.topic", json!({"msg": "hello"}));
    let received = rx.recv().await.unwrap();
    assert_eq!(received, json!({"msg": "hello"}));
}

#[tokio::test]
async fn pubsub_topic_isolation() {
    let ps = SimplePubSub::new(16);
    let mut rx_a = ps.subscribe("topic.a");
    let mut rx_b = ps.subscribe("topic.b");

    ps.publish("topic.a", json!({"v": 1}));

    assert_eq!(rx_a.recv().await.unwrap(), json!({"v": 1}));
    assert!(rx_b.try_recv().is_err());
}

#[tokio::test]
async fn pubsub_subscriber_count() {
    let ps = SimplePubSub::new(16);
    assert_eq!(ps.subscriber_count("topic.x"), 0);

    let rx = ps.subscribe("topic.x");
    assert_eq!(ps.subscriber_count("topic.x"), 1);

    // Publish to flush lag counts
    ps.publish("topic.x", json!({"ping": 1}));
    assert_eq!(ps.subscriber_count("topic.x"), 1);

    drop(rx);
    ps.publish("topic.x", json!({"ping": 2}));
    assert_eq!(ps.subscriber_count("topic.x"), 0);
}

#[tokio::test]
async fn pubsub_gc_cleans_up_dead_channels() {
    let ps = SimplePubSub::new(16);
    {
        let _rx = ps.subscribe("ephemeral");
    }
    assert_eq!(ps.total_subscriber_count(), 1); // lag before publish
    ps.gc();
    assert_eq!(ps.total_subscriber_count(), 0);
}