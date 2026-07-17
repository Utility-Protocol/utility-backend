# Database Migration Rollback Runbook

## When to Roll Back

Roll back when a migration causes correctness issues, elevated error rates,
security exposure, or sustained P99 latency above the 100ms critical-path target.

## Procedure

1. Freeze additional deployments and confirm the active database version from
   `schema_migrations`.
2. Identify the previous stable version approved by the incident commander.
3. Drain or pause write-heavy workers where the migration affects write schemas.
4. Execute the service migration entrypoint with the target previous version so
   rollback SQL runs in reverse version order.
5. Verify that `schema_migrations` no longer contains rolled-back versions.
6. Watch `utility_migration_steps_total{direction="down"}` and
   `utility_migration_step_duration_seconds{direction="down"}` for completion.
7. Resume traffic gradually using the standard blue-green/canary process.
8. Document follow-up remediation and open a security review if data exposure was
   suspected.

## Validation Checklist

- Database schema matches the target application release.
- Health checks pass in the green environment before traffic shift.
- Error rate, saturation, and critical-path P99 latency are back within SLO.
- Dashboards show no new failed migration attempts.
