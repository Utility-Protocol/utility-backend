# Service Mesh mTLS Runbook

## Alerts

### High mTLS Handshake Failure Rate

1. Check `utility_mesh_mtls_handshakes_total{result="failure"}` by peer service.
2. Verify the affected service has a valid SPIFFE identity and a non-expired workload certificate.
3. Confirm the mesh CA bundle matches the active trust domain.
4. Roll back the latest canary step if failures began after a traffic shift.

### mTLS P99 Latency Above 100 ms

1. Inspect `utility_mesh_mtls_handshake_latency_seconds` for the affected peer service.
2. Check mesh proxy CPU saturation, certificate revocation checks, and control-plane reachability.
3. Reduce canary traffic to the previous healthy weight.
4. Open a security review if latency is caused by certificate validation or policy fetch failures.

## Manual Canary Gate

Use the release dashboard to confirm:

- success rate >= 99.99%
- P99 handshake latency <= 100 ms
- zero critical security alerts
- green environment health checks passing

Only promote when all gates pass.
