use std::collections::HashMap;
use std::fmt;
use tracing::{Event, Subscriber};
use tracing_subscriber::{
    fmt::{format::Writer, FmtContext, FormatEvent, FormatFields},
    registry::LookupSpan,
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};
use opentelemetry::{global, Context};
use opentelemetry_sdk::trace::{self as sdktrace, Sampler};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::baggage::BaggageExt as _;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::propagation::TextMapCompositePropagator;
use opentelemetry::trace::TraceContextExt as _;

pub struct OtelJsonFormatter {
    service_name: String,
    service_version: String,
    environment: String,
}

impl OtelJsonFormatter {
    pub fn new() -> Self {
        let service_name = std::env::var("OTEL_SERVICE_NAME")
            .unwrap_or_else(|_| "utility-backend".to_string());
        let service_version = env!("CARGO_PKG_VERSION").to_string();
        let environment = std::env::var("APP_ENV")
            .unwrap_or_else(|_| "production".to_string());

        Self {
            service_name,
            service_version,
            environment,
        }
    }
}

impl Default for OtelJsonFormatter {
    fn default() -> Self {
        Self::new()
    }
}

struct OtelEventVisitor {
    message: String,
    attributes: HashMap<String, serde_json::Value>,
}

impl tracing::field::Visit for OtelEventVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        let field_name = field.name();
        let val_str = format!("{:?}", value);
        if field_name == "message" {
            self.message = val_str;
        } else {
            self.attributes.insert(field_name.to_string(), serde_json::Value::String(val_str));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        let field_name = field.name();
        if field_name == "message" {
            self.message = value.to_string();
        } else {
            self.attributes.insert(field_name.to_string(), serde_json::Value::String(value.to_string()));
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.attributes.insert(field.name().to_string(), serde_json::Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.attributes.insert(field.name().to_string(), serde_json::Value::Number(value.into()));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.attributes.insert(field.name().to_string(), serde_json::Value::Bool(value));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if let Some(num) = serde_json::Number::from_f64(value) {
            self.attributes.insert(field.name().to_string(), serde_json::Value::Number(num));
        } else {
            self.attributes.insert(field.name().to_string(), serde_json::Value::String(value.to_string()));
        }
    }
}

impl<S, N> FormatEvent<S, N> for OtelJsonFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut visitor = OtelEventVisitor {
            message: String::new(),
            attributes: HashMap::new(),
        };
        event.record(&mut visitor);

        // Get active trace context and span ID
        let mut trace_id = None;
        let mut span_id = None;

        if let Some(span_ref) = ctx.lookup_current() {
            let extensions = span_ref.extensions();
            if let Some(otel_data) = extensions.get::<tracing_opentelemetry::OtelData>() {
                let otel_span = otel_data.parent_cx.span();
                let span_context = otel_span.span_context();
                if span_context.is_valid() {
                    trace_id = Some(span_context.trace_id().to_string());
                } else if let Some(t_id) = otel_data.builder.trace_id {
                    trace_id = Some(t_id.to_string());
                }

                if let Some(s_id) = otel_data.builder.span_id {
                    span_id = Some(s_id.to_string());
                }
            }
        }

        // Extract Baggage keys if present in current context
        let otel_ctx = Context::current();
        let baggage = otel_ctx.baggage();
        for key in &["region", "substation_id", "grid_segment"] {
            if let Some(val) = baggage.get(*key) {
                visitor.attributes.insert(key.to_string(), serde_json::Value::String(val.to_string()));
            }
        }

        let metadata = event.metadata();
        let (severity_text, severity_number) = match *metadata.level() {
            tracing::Level::TRACE => ("TRACE", 1),
            tracing::Level::DEBUG => ("DEBUG", 5),
            tracing::Level::INFO => ("INFO", 9),
            tracing::Level::WARN => ("WARN", 13),
            tracing::Level::ERROR => ("ERROR", 17),
        };

        if let Some(file) = metadata.file() {
            visitor.attributes.insert("code.filepath".to_string(), serde_json::Value::String(file.to_string()));
        }
        if let Some(line) = metadata.line() {
            visitor.attributes.insert("code.lineno".to_string(), serde_json::Value::Number(line.into()));
        }

        let mut log_record = serde_json::json!({
            "timestamp": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            "level": severity_text,
            "severity_number": severity_number,
            "service.name": self.service_name,
            "service.version": self.service_version,
            "service.environment": self.environment,
            "target": metadata.target(),
            "body": visitor.message,
        });

        if let Some(t_id) = trace_id {
            log_record.as_object_mut().unwrap().insert("trace_id".to_string(), serde_json::Value::String(t_id));
        }
        if let Some(s_id) = span_id {
            log_record.as_object_mut().unwrap().insert("span_id".to_string(), serde_json::Value::String(s_id));
        }

        if !visitor.attributes.is_empty() {
            log_record.as_object_mut().unwrap().insert("attributes".to_string(), serde_json::to_value(visitor.attributes).unwrap());
        }

        let serialized = serde_json::to_string(&log_record).map_err(|_| fmt::Error)?;
        writeln!(writer, "{}", serialized)
    }
}

pub fn init_structured_logging() -> anyhow::Result<()> {
    // 1. Initialize composite text map propagator
    global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
        Box::new(opentelemetry_sdk::propagation::TraceContextPropagator::new()),
        Box::new(opentelemetry_sdk::propagation::BaggagePropagator::new()),
    ]));

    // 2. Setup the global tracer provider
    // If an OTLP endpoint is specified, configure the OTLP pipeline, otherwise fallback to local
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .event_format(OtelJsonFormatter::new());

    if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        if let Ok(tracer) = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(opentelemetry_otlp::new_exporter().tonic().with_endpoint(endpoint))
            .with_trace_config(sdktrace::Config::default().with_sampler(Sampler::AlwaysOn))
            .install_batch(opentelemetry_sdk::runtime::Tokio)
        {
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
            tracing_subscriber::registry()
                .with(env_filter)
                .with(otel_layer)
                .with(fmt_layer)
                .init();
            return Ok(());
        }
    }

    // Fallback: local TracerProvider that generates unique IDs without exporting
    let provider = sdktrace::TracerProvider::builder()
        .with_config(sdktrace::Config::default().with_sampler(Sampler::AlwaysOn))
        .build();
    let tracer = provider.tracer("utility-backend");
    global::set_tracer_provider(provider);

    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing_subscriber::registry()
        .with(env_filter)
        .with(otel_layer)
        .with(fmt_layer)
                .init();

    Ok(())
}
