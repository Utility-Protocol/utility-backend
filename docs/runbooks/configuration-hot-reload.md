# Runbook: Configuration Hot Reload

## Alerts

- `utility_config_reload_failure_total` increases for 5 minutes.
- Active `utility_config_schema_version` does not match the expected release artifact.

## Triage

1. Inspect the deployment event that changed the configuration file.
2. Validate the JSON syntax and confirm only supported schema fields are present.
3. Confirm bounded values such as `database.max_connections` and `reload.poll_interval_ms` satisfy the documented limits.
4. Restore the previous known-good file if reload failures continue.

## Recovery

Because reload is atomic, services continue using the previous validated snapshot. After fixing the file, wait one poll interval or restart the process to force an initial load.
