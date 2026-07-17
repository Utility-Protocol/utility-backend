# Runbook RUN-002: Blockchain Settlement Failure

## 1. Description
This alert fires when a batch settlement transaction on the Stellar/Soroban smart contract fails during submission or finalization (e.g. out of gas, bad sequence, or network partition).

---

## 2. Automated Action / Mitigation
The system automatically executes the following mitigation:
1. **Trigger Budget Optimizer**: Adjusts tx fee and instruction limits by calling the preflight optimizer.
2. **Circuit Breaker Activation**: If consecutive failures exceed `5`, the circuit breaker trips, pausing settlement calls to prevent burning gas on invalid contracts or double-spending.
3. **Queue Fallback**: Unsettled records are pushed back into the durable queue for safe replay.

---

## 3. Manual Diagnosis & Mitigation Steps
If the circuit breaker has tripped or manual replay is required:

### 3.1 Check Circuit Breaker Status
```bash
curl -s http://localhost:8443/debug/clock_state | jq
```

### 3.2 View Failing Settlement Records
Query the uncommitted settlement state from PostgreSQL:
```sql
SELECT * FROM settlement_queue WHERE status = 'failed' LIMIT 50;
```

### 3.3 Test Soroban RPC Endpoint Connectivity
Ensure that the Stellar Soroban RPC is responding and the contract is loaded:
```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getNetwork"}' \
  $SOROBAN_RPC_URL
```

### 3.4 Force Re-Submission
Manually trigger settlement processing once the RPC or budget is restored:
```bash
curl -X POST http://localhost:8443/api/v1/settle \
  -H "Content-Type: application/json" \
  -d '{"meter_id": "MTR-001", "resource_units": 150.0, "destination_wallet": "GA..."}'
```

---

## 4. Verification & Resolution
Confirm transactions are confirmed on-chain. Resolve the incident on the manager:
```bash
curl -X POST http://localhost:8443/api/v1/incidents/settle-failure-id/resolve
```
This will automatically update PagerDuty to resolve the incident.
