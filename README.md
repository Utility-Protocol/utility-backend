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

## CI/CD & Caching Optimizations

GitHub Actions runs lint, type-check, and Dockerized database tests on every commit.

To optimize build and CI performance, the repository utilizes advanced Docker and Cargo caching mechanisms:

### 1. Docker Multi-Stage Builds with `cargo-chef`
The `Dockerfile` employs a multi-stage compilation flow using `lukemathwalker/cargo-chef` to isolate external dependency builds from internal source changes:
* **Planner Stage**: Analyzes the project structure to generate a minimal dependency recipe (`recipe.json`).
* **Builder Stage**: Installs development system packages and runs `cargo chef cook` to compile third-party dependencies. This layer remains cached unless `Cargo.lock` changes.
* **Compiler Stage**: Copies the remaining application code and performs a rapid compile of only the workspace crates.
* **Runner Stage**: Employs `debian:bookworm-slim` for a minimal, secure production-ready execution footprint.

### 2. CI Runner Caching
Our `.github/workflows/backend-ci.yml` CI workflow is optimized to save time and system resources:
* **GitHub Actions Layer Cache (`type=gha`)**: Configured with Docker Buildx and `docker/build-push-action@v6` to persist all intermediate image layers directly inside the GitHub Actions virtual machine runner cache using `mode=max`.
* **Rust/Cargo Caching (`swatinem/rust-cache@v2`)**: Replaced manual cargo home caching to securely, safely, and transparently cache cargo indexes, git databases, registry downloads, and built target directory objects. This yields significant build-time savings across commits.
