# Configuration Management

The backend uses a versioned JSON configuration document loaded by `utility_backend::config::ConfigManager`.
Every reload is schema-validated before it becomes visible to request handlers or background services.
Invalid reloads are rejected atomically, leaving the previous known-good snapshot in place.

## Architecture

1. Operators update the JSON file through the deployment system.
2. `ConfigManager` polls the file modification time at the validated `reload.poll_interval_ms` cadence.
3. The file is parsed with Serde using `deny_unknown_fields`, validated for operational bounds, and published through a lock-protected `Arc<AppConfig>` snapshot.
4. Subscribers receive a Tokio watch notification after the new snapshot is active.
5. Prometheus metrics record successful and failed reload attempts.

This keeps critical-path reads to an `Arc` clone and avoids partial configuration state.

## Example

```json
{
  "schema_version": 1,
  "service": { "bind_addr": "0.0.0.0:8443", "shutdown_timeout_ms": 10000 },
  "database": { "url": "postgres://utility:utility_secret@localhost:5432/utility_test", "max_connections": 16 },
  "telemetry": { "metrics_path": "/metrics", "dashboards_enabled": true },
  "reload": { "enabled": true, "poll_interval_ms": 5000 }
}
```

## Deployment

Use blue-green or canary deployment by writing candidate configuration to the inactive color first, checking `/readyz` and `utility_config_reload_failure_total`, then gradually shifting traffic. Roll back by restoring the last known-good configuration artifact.
