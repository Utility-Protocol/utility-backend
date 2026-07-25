# Secret Rotation Service Architecture

## Goals

The service rotates database credentials and API keys without placing secret material in logs, metrics, or pull-request output. Critical-path calls are limited to in-memory descriptor reads and checksum comparisons so the P99 budget remains under 100 ms; provider I/O happens in the background rotation worker.

## Components

1. `SecretRotationService` orchestrates stage, activate, verify, promote, and retire steps.
2. `SecretStore` abstracts Vault/KMS/parameter-store backends and keeps versioned credentials.
3. `CredentialTarget` abstracts database roles, API key issuers, and downstream services that need a credential activated.
4. Prometheus metrics expose attempt counts and rotation latency by secret name and status.

## Rotation Flow

1. Check whether the active version has passed `rotate_after`.
2. Generate a policy-compliant replacement credential.
3. Stage the new version in the secret store.
4. Activate it on the target system.
5. Verify target health using the new credential.
6. Promote the staged version as active.
7. Retire older versions after the configured overlap window.

## Deployment and Operations

Roll out with blue-green deployment. Enable the worker in the green environment first, canary 5% of tenants, verify `utility_secret_rotation_total{status="failure"}` remains flat, and then shift traffic. Roll back by disabling the worker and promoting the previous secret version from the store.
