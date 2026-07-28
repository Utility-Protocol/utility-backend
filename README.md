# utility-backend

Enterprise utility telemetry ingestion, tariff evaluation, and blockchain settlement backend.

## Architecture

```
src/
├── gateway/     - mTLS, gRPC, MQTT hooks for hardware utility meters
├── tariffs/     - Dynamic temporal/volumetric pricing logic
├── time_series/ - TimescaleDB ingestion & analytics pipelines
├── soroban/     - Soroban RPC batch settlement transactions
└── api/         - Protected dashboard & credential endpoints
```

## Quick Start

```bash
docker compose up -d
```

## Development

Install the local quality gate once per checkout:

```bash
python -m pip install pre-commit
pre-commit install --install-hooks
```

Run the same fast checks manually before opening a pull request:

```bash
pre-commit run --all-files
cargo test --all-features
cargo clippy --all-targets -- -D warnings
```

See [`docs/runbooks/pre-commit-hooks.md`](docs/runbooks/pre-commit-hooks.md) for the hook architecture, rollout plan, and troubleshooting runbook.

## CI/CD

GitHub Actions runs lint, type-check, and Dockerized database tests on every commit.
