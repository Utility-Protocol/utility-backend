# Runbook: Sensitive Field E2EE

## Alerts

- **E2EEFailureSpike**: `utility_e2ee_field_operations_total{status="failure"}` increases above baseline.
- **E2EELatencyP99High**: `histogram_quantile(0.99, rate(utility_e2ee_field_latency_seconds_bucket[5m])) > 0.1`.

## Triage

1. Check recent deploys for key-id or sensitive-field configuration changes.
2. Compare encrypt and decrypt failure labels to identify whether failures start at ingress or at readers.
3. Sample structured logs for envelope error categories; do not log payload values.
4. If failures follow a key rotation, roll writers back to the previous primary key while keeping dual-read keys enabled.

## Blue-green key rotation

1. Deploy green readers with both old and new decryption keys.
2. Canary green writers with the new primary key for a small traffic slice.
3. Watch failure counters and P99 latency for at least one canary window.
4. Promote green writers after canary analysis passes.
5. Retire old keys only after all old envelopes are rewrapped or expire by retention policy.
