use crate::soroban::rpc::{CircuitBreaker, SorobanRpcResponse};
use serde_json::json;

pub struct SorobanClient {
    rpc_url: String,
    circuit_breaker: CircuitBreaker,
}

impl SorobanClient {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self {
            rpc_url: rpc_url.into(),
            circuit_breaker: CircuitBreaker::new(5),
        }
    }

    pub async fn submit_batch_proof(
        &mut self,
        root: [u8; 32],
        leaf_count: u32,
        proof_hashes: Vec<[u8; 32]>,
    ) -> Result<SorobanRpcResponse, &'static str> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": "submit_batch_proof",
            "method": "sendTransaction",
            "params": {
                "operation": "submit_batch_proof",
                "root": hex::encode(root),
                "leaf_count": leaf_count,
                "proof_hashes": proof_hashes.into_iter().map(hex::encode).collect::<Vec<_>>()
            }
        });

        self.circuit_breaker.call_rpc(&self.rpc_url, payload).await
    }
}
