use std::collections::HashMap;
use std::time::Duration;

use axum::{
    body::Body,
    extract::Path,
    http::{HeaderMap, Request},
    middleware::Next,
    response::Response,
};
use opentelemetry::{
    baggage::BaggageExt,
    global,
    propagation::Extractor,
    trace::{SamplingResult, SpanKind, TraceContextExt},
    Context, KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    propagation::{BaggagePropagator, TextMapCompositePropagator, TraceContextPropagator},
    runtime::Tokio,
    trace::{BatchSpanProcessor, Config, ShouldSample, TracerProvider},
    Resource,
};
use tracing::{info, Instrument, Span};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

pub const BAGGAGE_REGION: &str = "region";
pub const BAGGAGE_SUBSTATION_ID: &str = "substation_id";
pub const BAGGAGE_GRID_SEGMENT: &str = "grid_segment";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpatialBaggage {
    pub region: String,
    pub substation_id: String,
    pub grid_segment: String,
}

impl SpatialBaggage {
    pub fn new(
        region: impl Into<String>,
        substation_id: impl Into<String>,
        grid_segment: impl Into<String>,
    ) -> Self {
        Self {
            region: region.into(),
            substation_id: substation_id.into(),
            grid_segment: grid_segment.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SpatialTraceSampler {
    success_ratio: f64,
}

impl Default for SpatialTraceSampler {
    fn default() -> Self {
        Self {
            success_ratio: 0.01,
        }
    }
}

impl SpatialTraceSampler {
    pub fn should_sample(&self, is_error: bool, trace_id: u128) -> bool {
        if is_error {
            return true;
        }
        let threshold = (self.success_ratio.clamp(0.0, 1.0) * u128::MAX as f64) as u128;
        trace_id <= threshold
    }
}

/// Initializes the OpenTelemetry propagators only (safe to call in tests).
///
/// Sets up W3C traceparent + Baggage propagators without trying to connect
/// to an OTLP collector.  Use this in unit tests and environments where no
/// collector is available.
pub fn init_propagators_only() {
    global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ]));
}

/// Initializes the full OpenTelemetry pipeline:
/// - W3C traceparent + Baggage propagators
/// - Head-based sampling: 1% for success, 100% for errors
/// - OTLP gRPC batch exporter (5s batch interval, 30s export timeout)
///
/// The OTLP exporter is only created when `OTEL_EXPORTER_OTLP_ENDPOINT`
/// is set (or defaults to `http://localhost:4317`).  If the connection
/// fails, propagators are still configured so W3C context propagation
/// continues to work for incoming requests.
pub fn init_open_telemetry(service_name: &str) -> anyhow::Result<()> {
    // ── propagators (always available) ───────────────────────────────
    init_propagators_only();

    // ── OTLP exporter (best-effort) ─────────────────────────────────
    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    match opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(&otlp_endpoint)
        .with_timeout(Duration::from_secs(30))
        .build_span_exporter()
    {
        Ok(exporter) => {
            // ── batch processor (5 s batch interval) ─────────────────
            let batch = BatchSpanProcessor::builder(exporter, Tokio)
                .with_scheduled_delay(Duration::from_secs(5))
                .with_max_export_batch_size(512)
                .with_max_queue_size(2048)
                .build();

            // ── TracerProvider with head-based error-aware sampler ──
            // The HeadBasedErrorSampler is provided as the ShouldSample
            // implementation; it keeps 100% of error spans and 1% of
            // success spans (head-based, using trace id bits).
            let provider = TracerProvider::builder()
                .with_config(
                    Config::default()
                        .with_sampler(HeadBasedErrorSampler::new(0.01))
                        .with_resource(Resource::new(vec![
                            KeyValue::new("service.name", service_name.to_string()),
                            KeyValue::new("service.version", env!("CARGO_PKG_VERSION").to_string()),
                        ])),
                )
                .with_span_processor(batch)
                .build();

            global::set_tracer_provider(provider);

            info!(
                service_name,
                otlp_endpoint = %otlp_endpoint,
                propagation = "w3c_trace_context,baggage",
                success_sample_rate = 0.01,
                error_sample_rate = 1.0,
                otlp_export = "batch_5s",
                "OpenTelemetry tracing initialized with OTLP exporter"
            );
        }
        Err(e) => {
            // Propagators are already set; keep W3C context propagation
            // working even without an OTLP backend.
            tracing::warn!(
                otlp_endpoint = %otlp_endpoint,
                error = %e,
                "OTLP exporter unavailable; W3C trace context propagation is still active"
            );
        }
    }

    Ok(())
}

/// Installs the tracing-opentelemetry bridge layer onto the global
/// `tracing_subscriber` registry so that `tracing` spans are exported
/// as OpenTelemetry spans through the configured TracerProvider.
pub fn init_tracing_otel_bridge() -> anyhow::Result<()> {
    let tracer = global::tracer("utility-backend");
    let otel_layer = OpenTelemetryLayer::new(tracer);

    let subscriber = Registry::default()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(otel_layer);

    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| anyhow::anyhow!("failed to set tracing subscriber: {}", e))?;

    info!("tracing-opentelemetry bridge installed");
    Ok(())
}

// ── Head‑based Error‑Aware Sampler ──────────────────────────────────

/// Implements head-based sampling:
/// - 100% of traces that include an error span are kept.
/// - Otherwise the configured `ratio` (0.01 = 1%) is used.
#[derive(Debug, Clone)]
pub struct HeadBasedErrorSampler {
    ratio: f64,
}

impl HeadBasedErrorSampler {
    pub fn new(ratio: f64) -> Self {
        Self {
            ratio: ratio.clamp(0.0, 1.0),
        }
    }
}

impl ShouldSample for HeadBasedErrorSampler {
    fn should_sample(
        &self,
        parent_context: Option<&Context>,
        trace_id: opentelemetry::trace::TraceId,
        _name: &str,
        _span_kind: &SpanKind,
        attributes: &[KeyValue],
        _links: &[opentelemetry::trace::Link],
    ) -> SamplingResult {
        // Always sample if a parent already decided to sample.
        if let Some(parent) = parent_context {
            if parent.span().span_context().is_sampled() {
                return SamplingResult::RecordAndSample;
            }
        }

        // 100% sampling for spans marked as errors.
        let is_error = attributes
            .iter()
            .any(|kv| kv.key.as_str() == "error" && kv.value.as_str() == "true");
        if is_error {
            return SamplingResult::RecordAndSample;
        }

        // Head-based probability sampling using the high-64 bits of the
        // trace id as the decision value.
        let threshold = (self.ratio * u64::MAX as f64) as u64;
        let decision = (trace_id.to_bytes()[0..8])
            .try_into()
            .map(u64::from_be_bytes)
            .unwrap_or(0);

        if decision <= threshold {
            SamplingResult::RecordAndSample
        } else {
            SamplingResult::Drop
        }
    }
}

pub fn context_with_spatial_baggage(base: &Context, spatial: &SpatialBaggage) -> Context {
    base.with_baggage(vec![
        KeyValue::new(BAGGAGE_REGION, spatial.region.clone()),
        KeyValue::new(BAGGAGE_SUBSTATION_ID, spatial.substation_id.clone()),
        KeyValue::new(BAGGAGE_GRID_SEGMENT, spatial.grid_segment.clone()),
    ])
}

pub fn inject_context(context: &Context, carrier: &mut HashMap<String, String>) {
    global::get_text_map_propagator(|propagator| propagator.inject_context(context, carrier));
}

pub fn extract_context(carrier: &HashMap<String, String>) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(carrier))
}

pub fn spatial_baggage_from_context(context: &Context) -> Option<SpatialBaggage> {
    let baggage = context.baggage();
    Some(SpatialBaggage {
        region: baggage.get(BAGGAGE_REGION)?.to_string(),
        substation_id: baggage.get(BAGGAGE_SUBSTATION_ID)?.to_string(),
        grid_segment: baggage.get(BAGGAGE_GRID_SEGMENT)?.to_string(),
    })
}

pub fn trace_substation_route(substation_id: &str) {
    Span::current().record("substation.id", substation_id);
    tracing::info!(
        substation_id,
        baggage_key = BAGGAGE_SUBSTATION_ID,
        "tracing substation route"
    );
}

pub async fn tracing_middleware(req: Request<Body>, next: Next) -> Response {
    let parent_context = extract_context_from_headers(req.headers());
    let spatial = spatial_baggage_from_context(&parent_context);
    let method = req.method().clone();
    let path = req.uri().path().to_owned();

    let span = tracing::info_span!(
        "http.request",
        http.method = %method,
        http.route = %path,
        otel.kind = "server",
        region = spatial.as_ref().map(|b| b.region.as_str()).unwrap_or(""),
        substation.id = spatial.as_ref().map(|b| b.substation_id.as_str()).unwrap_or(""),
        grid.segment = spatial.as_ref().map(|b| b.grid_segment.as_str()).unwrap_or("")
    );

    async move { next.run(req).await }.instrument(span).await
}

fn extract_context_from_headers(headers: &HeaderMap) -> Context {
    struct HeaderExtractor<'a>(&'a HeaderMap);

    impl<'a> Extractor for HeaderExtractor<'a> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).and_then(|value| value.to_str().ok())
        }

        fn keys(&self) -> Vec<&str> {
            self.0.keys().map(|name| name.as_str()).collect()
        }
    }

    global::get_text_map_propagator(|propagator| propagator.extract(&HeaderExtractor(headers)))
}

pub async fn get_trace(Path(trace_id): Path<String>) -> axum::Json<TraceLookupResponse> {
    axum::Json(TraceLookupResponse {
        trace_id,
        backend: std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .unwrap_or_else(|_| "otlp://collector".into()),
        message: "trace lookup should be resolved by the configured OTLP backend".into(),
    })
}

#[derive(serde::Serialize)]
pub struct TraceLookupResponse {
    pub trace_id: String,
    pub backend: String,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{SpanContext, TraceContextExt, TraceFlags, TraceId, TraceState};

    #[test]
    fn propagates_w3c_trace_context_and_spatial_baggage() {
        init_propagators_only();
        let span_context = SpanContext::new(
            TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
            opentelemetry::trace::SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            TraceFlags::SAMPLED,
            false,
            TraceState::default(),
        );
        let context = Context::new().with_remote_span_context(span_context);
        let context = context_with_spatial_baggage(
            &context,
            &SpatialBaggage::new("north-east", "SUB-42", "grid-a"),
        );

        let mut carrier = HashMap::new();
        inject_context(&context, &mut carrier);

        assert!(carrier.contains_key("traceparent"));
        assert!(carrier
            .get("baggage")
            .unwrap()
            .contains("region=north-east"));

        let extracted = extract_context(&carrier);
        assert_eq!(
            spatial_baggage_from_context(&extracted).unwrap(),
            SpatialBaggage::new("north-east", "SUB-42", "grid-a")
        );
        assert_eq!(
            extracted.span().span_context().trace_id(),
            TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap()
        );
    }

    #[test]
    fn sampler_keeps_all_errors_and_one_percent_successes() {
        let sampler = SpatialTraceSampler::default();
        assert!(sampler.should_sample(true, u128::MAX));
        assert!(sampler.should_sample(false, 1));
        assert!(!sampler.should_sample(false, u128::MAX));
    }
}
