use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use opentelemetry::trace::{Tracer, Span, SpanKind, TraceContextExt, SpanContext};
use opentelemetry::{global, Context};
use crate::blockchain::soroban::client::SorobanClient;
use crate::tracing::soroban_propagator::{extract_context};
use std::collections::HashMap;

pub struct SorobanEventPoller {
    client: Arc<Mutex<SorobanClient>>,
    poll_interval: Duration,
    last_ledger: u64,
    contract_ids: Vec<String>,
    active_spans: HashMap<String, (global::BoxedSpan, Instant)>,
}

impl SorobanEventPoller {
    pub fn new(client: Arc<Mutex<SorobanClient>>, poll_interval: Duration, contract_ids: Vec<String>) -> Self {
        Self {
            client,
            poll_interval,
            last_ledger: 0,
            contract_ids,
            active_spans: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(self.poll_interval);
        let tracer = global::tracer("soroban-exporter");

        loop {
            interval.tick().await;

            let mut client = self.client.lock().await;
            if let Ok(events) = client.get_events(self.last_ledger, self.contract_ids.clone()).await {
                for event in events {
                    if let Some((trace_id, span_id, flags)) = extract_context(&event) {
                        let context = SpanContext::new(trace_id, span_id, flags, true, opentelemetry::trace::TraceState::default());

                        let event_type = event.topics.get(0).map(|s| s.as_str()).unwrap_or("");

                        let key = format!("{}-{}-{}", trace_id, span_id, event.contract_id);

                        if event_type == "1" || event_type == "01" {
                            let span_name = format!("soroban.contract.{}", event.contract_id);
                            let parent_cx = Context::new().with_remote_span_context(context);

                            let span = tracer.span_builder(span_name)
                                .with_kind(SpanKind::Server)
                                .start_with_context(&tracer, &parent_cx);

                            self.active_spans.insert(key, (span, Instant::now()));
                        } else if event_type == "2" || event_type == "02" {
                            if let Some((mut span, _start)) = self.active_spans.remove(&key) {
                                span.end();
                            }
                        }
                    }
                }
            }
        }
    }
}
