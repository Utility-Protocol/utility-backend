# Secret Rotation Runbook

## Alerts

- Page on any sustained increase in `utility_secret_rotation_total{status="failure"}`.
- Page when `utility_secret_rotation_duration_ms` P99 exceeds 100 ms for 10 minutes.
- Warn when any production secret is within 24 hours of `rotate_after` and has not rotated.

## Manual Rotation

1. Confirm the target service is healthy.
2. Trigger the rotation worker for the secret name.
3. Watch `utility_secret_rotation_total` and target-specific authentication errors.
4. Confirm the active version increments in the secret store.
5. Keep the previous version available until the overlap period expires.

## Rollback

1. Disable the rotation worker.
2. Promote the previous known-good secret version.
3. Restart affected consumers or invalidate their secret cache.
4. Re-enable rotation only after root cause is documented.
