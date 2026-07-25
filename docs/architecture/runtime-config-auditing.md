# Runtime Configuration Auditing and Drift Detection

## Architecture

Runtime configuration auditing captures a redacted, deterministic snapshot of each service's effective configuration at startup and after controlled reload events. The `config_audit` module normalizes entries into sorted key/value pairs, redacts sensitive keys, and computes a SHA-256 checksum for fleet-wide comparison.

## Drift Detection

A deployment baseline is compared with the observed runtime snapshot. The detector reports added, removed, and modified keys. Drift involving sensitive configuration names such as secrets, passwords, tokens, keys, or credentials is classified as critical; other drift is classified as warning.

## Performance and Availability

Snapshot creation is in-memory and deterministic with O(n) hashing over sorted configuration entries. It is designed to run outside request hot paths so critical-path P99 latency remains below 100ms and service availability remains independent of the audit sink.

## Monitoring

Prometheus metrics:

- `utility_config_audit_snapshots_total{service,environment}` records captured snapshots.
- `utility_config_drift_events_total{service,severity}` records detected drift events.
- `utility_config_audit_duration_ms` measures audit runtime.

## Deployment

Roll out in blue-green deployments by first enabling snapshot-only mode, validating checksum parity during canary analysis, and then enabling drift alerts for the full fleet.
