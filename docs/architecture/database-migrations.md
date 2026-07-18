# Database Migration Versioning and Rollback Architecture

## Goals

The migration subsystem provides system-wide database change control for all
services in this repository. It is designed for deterministic version ordering,
rollback support, and safe execution during blue-green or canary releases.

## Design

- `MigrationRegistry` owns the canonical ordered list of migrations and validates
  that versions are positive and strictly increasing.
- Each `Migration` contains an `up_sql` statement and a matching `down_sql`
  rollback statement. Missing rollback SQL is rejected before deployment.
- The runner stores applied migrations in `schema_migrations` with version, name,
  checksum, and timestamp metadata.
- Checksums are computed from version, name, and forward SQL. Any mismatch for an
  already-applied migration fails planning and blocks startup until the migration
  definition is reconciled.
- PostgreSQL advisory locking serializes all migration runners so only one
  deployment wave can mutate the schema at a time.

## Deployment Flow

1. Register migrations in version order with both forward and rollback SQL.
2. Run `MigrationRunner::migrate_to(latest_version)` before enabling write paths.
3. Use blue-green deployment so the green environment runs migrations and passes
   health checks before traffic shifts.
4. During canary analysis, monitor migration counters, duration histograms, and
   application P99 latency before widening traffic.
5. If rollback is required, call `migrate_to(previous_stable_version)`; rollback
   steps execute in reverse version order inside database transactions.

## Monitoring

The runner emits Prometheus metrics for migration counts and step durations:

- `utility_migration_steps_total{direction="up|down"}`
- `utility_migration_step_duration_seconds{direction="up|down"}`

Alert if a migration step fails, if duration significantly exceeds the normal
maintenance-window baseline, or if application P99 exceeds 100ms after a canary
migration.
