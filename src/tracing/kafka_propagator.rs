//! Kafka message header trace context propagation.
//!
//! Injects / extracts W3C trace context (`traceparent`) and baggage into
//! Kafka record headers so distributed traces can cross message-broker
//! boundaries.
//!
//! # Wire format
//!
//! | Header key      | Value                                          |
//! |-----------------|------------------------------------------------|
//! | `traceparent`   | `00-{trace_id}-{span_id}-{trace_flags}`        |
//! | `tracestate`    | vendor-specific trace state (if present)        |
//! | `baggage`       | comma-separated key=value pairs                 |
//!
//! Lengths: trace_id = 32 hex, span_id = 16 hex, flags = 2 hex.

use opentelemetry::{
    global,
    propagation::Injector,
    trace::{TraceContextExt, Tracer},
    Context,
};
use std::collections::HashMap;

/// Inject the trace context from `cx` into a set of Kafka record headers.
///
/// The caller should merge the returned `HashMap` into the Kafka producer
/// record's `headers` field.
pub fn inject_into_kafka_headers(cx: &Context) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(cx, &mut KafkaHeaderInjector(&mut headers));
    });
    headers
}

/// Extract a trace context from Kafka record headers.
///
/// The caller should pass the raw `Headers` from the Kafka consumer record.
/// Returns a new `Context` with the remote span context set.
pub fn extract_from_kafka_headers(headers: &HashMap<String, String>) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&KafkaHeaderExtractor(headers)))
}

// ── Injector ─────────────────────────────────────────────────────────

struct KafkaHeaderInjector<'a>(&'a mut HashMap<String, String>);

impl<'a> Injector for KafkaHeaderInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_lowercase(), value);
    }
}

// ── Extractor ────────────────────────────────────────────────────────

struct KafkaHeaderExtractor<'a>(&'a HashMap<String, String>);

impl<'a> opentelemetry::propagation::Extractor for KafkaHeaderExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(&key.to_lowercase()).map(|s| s.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|s| s.as_str()).collect()
    }
}

/// Convenience: start a consumer span linked to the remote producer span.
///
/// ```ignore
/// let parent_cx = kafka_propagator::extract_from_kafka_headers(&record_headers);
/// let span = kafka_propagator::start_consumer_span(&parent_cx, "meter-readings", 3);
/// // ... process the record ...
/// drop(span);
/// ```
pub fn start_consumer_span(parent_context: &Context, topic: &str, partition: i32) -> tracing::Span {
    let tracer = global::tracer("kafka-consumer");
    let span = tracer
        .span_builder(format!("kafka.consume {}", topic))
        .with_kind(opentelemetry::trace::SpanKind::Consumer)
        .with_attributes(vec![
            opentelemetry::KeyValue::new("messaging.system", "kafka"),
            opentelemetry::KeyValue::new("messaging.destination", topic.to_string()),
            opentelemetry::KeyValue::new("messaging.destination_kind", "topic"),
            opentelemetry::KeyValue::new("messaging.kafka.partition", partition as i64),
        ])
        .start_with_context(&tracer, parent_context);

    // Bridge into the tracing world.
    let cx = Context::current_with_span(span);
    let _guard = cx.attach();
    tracing::Span::current()
}

/// Convenience: start a producer span and return the carrier headers + span.
///
/// ```ignore
/// let (headers, span) = kafka_propagator::start_producer_span("meter-readings", 3);
/// // ... produce the record with headers ...
/// drop(span);
/// ```
pub fn start_producer_span(
    topic: &str,
    partition: i32,
) -> (HashMap<String, String>, tracing::Span) {
    let tracer = global::tracer("kafka-producer");
    let mut span = tracer
        .span_builder(format!("kafka.produce {}", topic))
        .with_kind(opentelemetry::trace::SpanKind::Producer)
        .with_attributes(vec![
            opentelemetry::KeyValue::new("messaging.system", "kafka"),
            opentelemetry::KeyValue::new("messaging.destination", topic.to_string()),
            opentelemetry::KeyValue::new("messaging.destination_kind", "topic"),
            opentelemetry::KeyValue::new("messaging.kafka.partition", partition as i64),
        ])
        .start(&tracer);

    let cx = Context::current_with_span(span);
    let headers = inject_into_kafka_headers(&cx);

    let _guard = cx.attach();
    (headers, tracing::Span::current())
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    };

    fn init_test_propagators() {
        use opentelemetry_sdk::propagation::{
            BaggagePropagator, TextMapCompositePropagator, TraceContextPropagator,
        };
        let _ = global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
            Box::new(TraceContextPropagator::new()),
            Box::new(BaggagePropagator::new()),
        ]));
    }

    #[test]
    fn test_inject_extract_roundtrip() {
        init_test_propagators();

        let trace_id = TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap();
        let span_id = SpanId::from_hex("00f067aa0ba902b7").unwrap();
        let span_context = SpanContext::new(
            trace_id,
            span_id,
            TraceFlags::SAMPLED,
            false,
            TraceState::default(),
        );
        let cx = Context::new().with_remote_span_context(span_context);

        let headers = inject_into_kafka_headers(&cx);

        assert!(
            headers.contains_key("traceparent"),
            "traceparent must be injected"
        );

        let extracted = extract_from_kafka_headers(&headers);

        assert_eq!(
            extracted.span().span_context().trace_id(),
            trace_id,
            "trace_id must survive round-trip"
        );
    }

    #[test]
    fn test_extract_empty_headers_returns_empty_context() {
        init_test_propagators();
        let headers = HashMap::new();
        let cx = extract_from_kafka_headers(&headers);
        assert!(!cx.span().span_context().is_valid());
    }
}
