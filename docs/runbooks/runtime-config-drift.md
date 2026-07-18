# Runtime Configuration Drift Runbook

## Alert

`utility_config_drift_events_total` increases for a production service.

## Triage

1. Identify the service, environment, and severity labels from the alert.
2. Compare the current snapshot checksum with the deployment baseline checksum.
3. Review the drift report for added, removed, or modified keys.
4. Treat critical drift as a potential secret or credential control-plane incident.

## Remediation

- For expected changes, update the deployment baseline and attach the change approval.
- For unexpected non-sensitive drift, roll the instance back into the blue pool and redeploy from the approved artifact.
- For sensitive drift, rotate impacted credentials, quarantine affected instances, and request security review before returning traffic.

## Canary Validation

During canary rollout, require zero critical drift and no unexplained warning drift before increasing traffic.
