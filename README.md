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

## Multi-Region Replication and Disaster Recovery

The disaster-recovery design uses one write primary and one or more ordered standby
regions. Standbys continuously replicate critical storage state and are eligible for
promotion only when their observed replication lag is within the critical RPO target.
The failover planner selects the lowest-priority promotable standby and returns a
blue-green cutover plan with an initial 5% canary so operators can validate traffic
before promoting the green region globally.

Operational targets:

- Critical path P99 remains below 100 ms by keeping failover planning deterministic
  and local to the latest health snapshot.
- Critical recovery point objective (RPO): <= 5 seconds of replication lag.
- Critical recovery time objective (RTO): <= 60 seconds for standby promotion.
- Availability objective: 99.99% service uptime.

Monitoring and alerting should use `utility_replication_lag_ms` to page before RPO
is breached and `utility_dr_failover_attempts_total` to audit failover outcomes.
During an outage, run the failover planner against the latest region health snapshot,
promote the selected standby through the blue-green deployment pipeline, route 5% of
traffic for canary analysis, then complete the DNS/load-balancer cutover after error
rates and latency remain within SLO.
