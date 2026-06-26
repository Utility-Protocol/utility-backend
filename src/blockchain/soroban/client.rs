use crate::blockchain::soroban::reorg_handler::LedgerInfo;
use crate::soroban::rpc::{CircuitBreaker, SorobanRpcResponse};
use opentelemetry::trace::SpanContext;
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
        trace_ctx: Option<&SpanContext>,
    ) -> Result<SorobanRpcResponse, &'static str> {
        let trace_hex = trace_ctx
            .map(crate::tracing::soroban_propagator::inject_context)
            .unwrap_or_else(|| "0".repeat(82));

        let payload = json!({
            "jsonrpc": "2.0",
            "id": "submit_batch_proof",
            "method": "sendTransaction",
            "params": {
                "operation": "submit_batch_proof",
                "trace_ctx": trace_hex,
                "root": hex::encode(root),
                "leaf_count": leaf_count,
                "proof_hashes": proof_hashes.into_iter().map(hex::encode).collect::<Vec<_>>()
            }
        });

        self.circuit_breaker.call_rpc(&self.rpc_url, payload).await
    }

    /// Fetch canonical `(seq, hash)` for the inclusive ledger range, used by the
    /// reorg handler to compare against the locally indexed chain.
    pub async fn get_ledger_range(
        &mut self,
        start_seq: u64,
        end_seq: u64,
    ) -> Result<Vec<LedgerInfo>, &'static str> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": "get_ledger_range",
            "method": "getLedgers",
            "params": { "startLedger": start_seq, "endLedger": end_seq }
        });
        let resp = self
            .circuit_breaker
            .call_rpc(&self.rpc_url, payload)
            .await?;
        let result = resp.result.ok_or("missing result")?;
        let ledgers = result
            .get("ledgers")
            .and_then(|v| v.as_array())
            .ok_or("missing ledgers array")?;

        let mut out = Vec::with_capacity(ledgers.len());
        for entry in ledgers {
            let seq = entry
                .get("sequence")
                .and_then(|v| v.as_u64())
                .ok_or("missing ledger sequence")?;
            let hash_hex = entry
                .get("hash")
                .and_then(|v| v.as_str())
                .ok_or("missing ledger hash")?;
            let bytes = hex::decode(hash_hex).map_err(|_| "invalid ledger hash hex")?;
            let hash: [u8; 32] = bytes.try_into().map_err(|_| "ledger hash not 32 bytes")?;
            out.push(LedgerInfo { seq, hash });
        }
        Ok(out)
    }

    pub async fn get_events(
        &mut self,
        start_ledger: u64,
        contract_ids: Vec<String>,
    ) -> Result<Vec<crate::tracing::soroban_propagator::SorobanEvent>, &'static str> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": "get_events",
            "method": "getEvents",
            "params": {
                "startLedger": start_ledger,
                "filters": [
                    {
                        "type": "diagnostic",
                        "contractIds": contract_ids
                    }
                ]
            }
        });

        let resp = self
            .circuit_breaker
            .call_rpc(&self.rpc_url, payload)
            .await?;
        let result = resp.result.ok_or("missing result")?;
        let events = result
            .as_array()
            .ok_or("missing events array")?;

        let mut out = Vec::with_capacity(events.len());
        for entry in events {
            let event_type = entry.get("type").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let contract_id = entry.get("contractId").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let value = entry.get("value").and_then(|v| v.get("xdr")).and_then(|v| v.as_str()).unwrap_or("").to_string();

            // Simplified parsing for topics
            let topics = entry.get("topic").and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|t| t.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();

            out.push(crate::tracing::soroban_propagator::SorobanEvent {
                event_type,
                contract_id,
                topics,
                value,
            });
        }
        Ok(out)
    }

    /// Whether `tx_hash` is included in the canonical ledger `ledger_seq`.
    pub async fn is_tx_in_ledger(
        &mut self,
        tx_hash: [u8; 32],
        ledger_seq: u64,
    ) -> Result<bool, &'static str> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": "is_tx_in_ledger",
            "method": "getTransaction",
            "params": { "hash": hex::encode(tx_hash) }
        });
        let resp = self
            .circuit_breaker
            .call_rpc(&self.rpc_url, payload)
            .await?;
        let result = resp.result.ok_or("missing result")?;
        let included = result.get("ledger").and_then(|v| v.as_u64()) == Some(ledger_seq);
        Ok(included)
    }
}
