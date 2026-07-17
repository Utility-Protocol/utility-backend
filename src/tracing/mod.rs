pub mod exporters;
pub mod soroban_propagator;
pub mod logging;

use opentelemetry::trace::TraceContextExt;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub fn get_current_span_context() -> Option<opentelemetry::trace::SpanContext> {
    let span = Span::current();
    let context = span.context();
    let span_context = context.span().span_context().clone();
    if span_context.is_valid() {
        Some(span_context)
    } else {
        None
    }
}
