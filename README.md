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

```bash
cargo test --all-features
cargo clippy --all-targets -- -D warnings
```

## CI/CD

GitHub Actions runs lint, type-check, and Dockerized database tests on every commit.

## Capacity Planning

The backend exposes a system-wide capacity planning forecast at
`GET /api/v1/capacity/forecast`. The planner groups historical usage samples by
service and resource, computes a linear utilization trend, and returns the
current utilization, projected utilization over the default 30-day horizon,
estimated days to warning/critical thresholds, and an operational recommendation.

Prometheus metrics are emitted for dashboarding and alerting:

- `utility_capacity_current_utilization_ratio{service,resource}`
- `utility_capacity_projected_utilization_ratio{service,resource}`
- `utility_capacity_days_to_critical{service,resource}`

Suggested alerts:

- Page when projected utilization reaches the critical threshold inside the
  planning horizon.
- Warn when current or projected utilization exceeds the warning threshold.
- Review runbooks before blue-green promotion when any resource returns
  `scale_immediately` or `scale_within_horizon`.
