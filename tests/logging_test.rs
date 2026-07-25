use std::io;
use std::sync::{Arc, Mutex};
use tracing::{info, span, Level};
use tracing_subscriber::{layer::SubscriberExt, Registry, filter::LevelFilter};
use serde_json::Value;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{self as sdktrace, Sampler};
use utility_backend::tracing::logging::OtelJsonFormatter;

#[derive(Clone)]
struct BufferWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for BufferWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct BufferMakeWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufferMakeWriter {
    type Writer = BufferWriter;

    fn make_writer(&self) -> Self::Writer {
        BufferWriter {
            buffer: self.buffer.clone(),
        }
    }
}

#[tokio::test]
async fn test_otel_json_logging_format() {
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let make_writer = BufferMakeWriter {
        buffer: buffer.clone(),
    };

    let fmt_layer = tracing_subscriber::fmt::layer()
        .event_format(OtelJsonFormatter::new())
        .with_writer(make_writer);

    // Initialize a local TracerProvider so we can generate valid spans
    let provider = sdktrace::TracerProvider::builder()
        .with_config(sdktrace::Config::default().with_sampler(Sampler::AlwaysOn))
        .build();
    let tracer = provider.tracer("test-logger");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let filter = LevelFilter::INFO;

    let subscriber = Registry::default()
        .with(filter)
        .with(otel_layer)
        .with(fmt_layer);

    // Run within our custom subscriber
    tracing::subscriber::with_default(subscriber, || {
        let test_span = span!(Level::INFO, "test_operation");
        let _enter = test_span.enter();

        info!("structured log inside span");
    });

    let output_bytes = buffer.lock().unwrap().clone();
    let output_str = String::from_utf8(output_bytes).unwrap();
    let lines: Vec<&str> = output_str.trim().split('\n').collect();

    assert!(lines.len() >= 1, "Should have logged at least one message, got: {}", output_str);

    // Verify first log line (span with baggage)
    let log2: Value = serde_json::from_str(lines[0]).expect("Log output is not valid JSON");
    assert_eq!(log2["level"], "INFO");
    assert_eq!(log2["body"], "structured log inside span");

    // Check tracing IDs
    assert!(log2["trace_id"].is_string(), "Missing trace_id in span log");
    assert!(log2["span_id"].is_string(), "Missing span_id in span log");
    assert!(!log2["trace_id"].as_str().unwrap().is_empty());
    assert!(!log2["span_id"].as_str().unwrap().is_empty());
}
