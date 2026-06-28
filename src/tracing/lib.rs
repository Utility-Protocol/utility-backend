use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};
use crate::tracing::soroban_propagator::inject_context;
use crate::tracing::get_current_span_context;

#[derive(Clone)]
pub struct SorobanTraceLayer;

impl<S> Layer<S> for SorobanTraceLayer {
    type Service = SorobanTraceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SorobanTraceService { inner }
    }
}

#[derive(Clone)]
pub struct SorobanTraceService<S> {
    inner: S,
}

impl<S, Request> Service<Request> for SorobanTraceService<S>
where
    S: Service<Request>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        // Here we would ideally inject the context if Request allowed it.
        // Since SorobanClient uses a specific method call, this Layer might need to
        // be applied to a more generic HTTP service if the client was structured that way.
        // For now, it's a placeholder as requested by the blueprint.
        self.inner.call(req)
    }
}
