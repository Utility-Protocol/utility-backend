# Service Mesh Mutual TLS Architecture

## Goals

The service mesh integration standardizes mutual TLS (mTLS) for every utility-backend service-to-service hop. The design keeps the P99 latency budget for critical paths at or below 100 ms, requires SPIFFE identities, and exposes metrics for security review, canary analysis, and day-two operations.

## Architecture

1. A mesh sidecar or ambient proxy terminates inbound mTLS with certificates mounted at `/etc/utility/mesh`.
2. The application validates `ServiceMeshMtlsConfig` during startup so mTLS cannot be enabled without a service certificate, private key, and trusted CA bundle.
3. Workload identity uses SPIFFE IDs in the format `spiffe://<trust-domain>/ns/<namespace>/sa/<service-account>`.
4. Critical paths use the configured `critical_path_budget_ms` guardrail. Values above 100 ms are rejected.
5. Prometheus exports handshake counters and latency histograms:
   - `utility_mesh_mtls_handshakes_total{service,result}`
   - `utility_mesh_mtls_handshake_latency_seconds{service}`

## Blue-Green and Canary Rollout

1. Deploy the green environment with mTLS enabled and traffic weight at 0%.
2. Confirm certificate issuance, SPIFFE identity format, and CA trust bundle freshness.
3. Shift 1% of service-to-service traffic to green for at least one full SLO window.
4. Promote only when success rate is at least 99.99% and P99 mTLS handshake latency is at or below 100 ms.
5. Increase traffic to 10%, 25%, 50%, and 100% while rechecking the same gates.
6. Roll back to blue immediately if canary gates fail or security alerts fire.

## Security Review Checklist

- Certificates are rotated by the mesh control plane before expiry.
- Private keys are mounted read-only and never logged.
- Peer authorization policies allow only expected service accounts.
- Plaintext service ports are disabled outside local health checks.
- Dashboard alerts page on repeated handshake failures and latency budget violations.
