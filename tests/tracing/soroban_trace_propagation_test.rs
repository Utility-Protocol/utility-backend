use utility_backend::blockchain::soroban::client::SorobanClient;
use utility_backend::tracing::soroban_propagator::inject_context;
use opentelemetry::trace::{SpanContext, TraceId, SpanId, TraceFlags, TraceState};

#[tokio::test]
async fn test_trace_context_injection_in_client() {
    let mut client = SorobanClient::new("http://localhost:8000");
    let trace_id = TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap();
    let span_id = SpanId::from_hex("00f067aa0ba902b7").unwrap();
    let span_ctx = SpanContext::new(trace_id, span_id, TraceFlags::SAMPLED, false, TraceState::default());

    // We can't easily test the actual RPC call without a mock server,
    // but we can at least ensure it compiles and the logic runs.
    let root = [0u8; 32];
    let proof_hashes = vec![[0u8; 32]];

    // This will fail because no RPC server is running, but we check if it fails for the right reason (connection error)
    // and not a compilation or logic error.
    let result = client.submit_batch_proof(root, 1, proof_hashes, Some(&span_ctx)).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("rpc request failed") || result.unwrap_err().contains("failed to build rpc client"));
}
