# Scheduled Database Backup Verification with Restore Testing

## Architecture

The backup verifier is a background worker that is disabled by default and enabled with
`BACKUP_VERIFICATION_ENABLED=true`. On each interval it:

1. Creates an isolated scratch PostgreSQL database using the configured prefix.
2. Streams `pg_dump --format=custom` from the primary database directly into `pg_restore` for the scratch database.
3. Runs validation checks against the restored database; the default check requires at least one application table.
4. Emits Prometheus metrics for success/failure counts, run duration, and last successful verification time.
5. Drops the scratch database with `FORCE`, even when restore or validation fails.

This runs off the request path and uses a separate schedule, preserving the <100ms P99 target for critical API paths.

## Configuration

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `BACKUP_VERIFICATION_ENABLED` | `false` | Enables the scheduled worker. |
| `BACKUP_VERIFICATION_INTERVAL_SECS` | `86400` | Frequency for restore verification. |
| `BACKUP_VERIFICATION_TIMEOUT_SECS` | `1800` | Timeout for the dump/restore command. |
| `BACKUP_VERIFICATION_SCRATCH_PREFIX` | `utility_backup_verify` | Prefix for scratch database names. |
| `BACKUP_VERIFICATION_MIN_TABLES` | `1` | Minimum restored table count for validation success. |

## Monitoring and Alerts

Prometheus metrics:

- `utility_backup_verification_runs_total{status="success|failure"}`
- `utility_backup_verification_duration_seconds`
- `utility_backup_verification_last_success_timestamp_seconds`

Recommended alerts:

- Page when no successful restore verification has completed within 26 hours.
- Page immediately on any `status="failure"` increase in production.
- Warn when verification duration exceeds the configured timeout budget by 80%.

## Deployment Strategy

Use blue-green deployment. Enable the worker in the green environment first with a canary interval, verify metrics and logs, then shift traffic. Keep only one production environment running the scheduled verifier unless separate scratch prefixes are configured.

## Runbook

1. Check the latest `database backup restore verification failed` log entry for the scratch database and error.
2. Confirm PostgreSQL client tools (`pg_dump` and `pg_restore`) are installed in the runtime image.
3. Ensure the service role can create and drop databases and can read all schemas being backed up.
4. Run a manual verification by temporarily setting a short interval in a non-production environment.
5. If scratch cleanup failed, drop the scratch database manually after confirming no restore is running.
