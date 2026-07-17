# utility-backend

Enterprise utility telemetry ingestion, tariff evaluation, and blockchain settlement backend.

## Architecture

```
src/
├── gateway/     - mTLS, gRPC, MQTT hooks for hardware utility meters
├── tariffs/     - Dynamic temporal/volumetric pricing logic
├── time_series/ - TimescaleDB ingestion & analytics pipelines
├── soroban/     - Soroban RPC batch settlement transactions
├── incident/    - Incident management & PagerDuty automated runbooks
└── api/         - Protected dashboard & credential endpoints
```

## Incident Response & Runbook Automation

The backend features an asynchronous, event-driven Incident Response system integrated with **PagerDuty**. It allows real-time alerting alongside automated, self-healing runbook execution for system stability under 100ms P99 latency bounds.

### REST API Endpoints

| Method | Path | Description |
|---|---|---|
| POST | `/api/v1/incidents` | Trigger an incident manually / via monitoring |
| GET | `/api/v1/incidents` | List active incidents |
| GET | `/api/v1/incidents/:id` | Get incident details |
| POST | `/api/v1/incidents/:id/acknowledge` | Acknowledge an active incident |
| POST | `/api/v1/incidents/:id/resolve` | Resolve an incident |
| POST | `/api/v1/runbooks` | Register a new automated runbook |
| GET | `/api/v1/runbooks` | List registered runbooks |
| POST | `/api/v1/runbooks/rules` | Register an automation rule mapping incidents to runbooks |
| GET | `/api/v1/runbooks/rules` | List registered automation rules |
| GET | `/api/v1/runbooks/logs` | Retrieve automated runbook execution logs |

### Runbook and Deployment Guides
- **Architecture & System Design**: [docs/architecture/incident_response_runbooks.md](docs/architecture/incident_response_runbooks.md)
- **Blue-Green Deployment Strategy**: [docs/runbooks/deploy_blue_green.md](docs/runbooks/deploy_blue_green.md)
- **Runbooks**:
  - [RUN-001: TimescaleDB Compression Lag](docs/runbooks/RUN-001-COMPRESSION-LAG.md)
  - [RUN-002: Blockchain Settlement Failure](docs/runbooks/RUN-002-BLOCKCHAIN-SETTLE-FAILURE.md)
  - [RUN-003: Gateway Advisory Lock Contention](docs/runbooks/RUN-003-GATEWAY-LOCK-CONTENTION.md)

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
