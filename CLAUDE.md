# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Start all dependencies (TimescaleDB + Soroban RPC)
docker compose up -d

# Run all tests (requires DATABASE_URL env var pointing at a running TimescaleDB)
cargo test --all-features

# Lint (CI enforces zero warnings)
cargo clippy --all-targets -- -D warnings

# Run a single test by name
cargo test <test_name>

# Run tests in a specific module
cargo test --test gateway_tests
cargo test --test tariffs_tests

# Run benchmarks
cargo bench --bench parser_bench
```

The service listens on port **8443** (`0.0.0.0:8443`). The default database URL used when `DATABASE_URL` is unset is `postgres://utility:utility_secret@localhost:5432/utility_test`.

## Architecture

The backend ingests telemetry from hardware utility meters, applies tariff pricing, and settles consumption on the Stellar blockchain via Soroban smart contracts.

### Module map

| Module | Purpose |
|---|---|
| `src/gateway/` | Hardware meter interface layer |
| `src/tariffs/` | Dynamic pricing engine |
| `src/time_series/` | TimescaleDB ingestion and anomaly detection |
| `src/soroban/` | Stellar/Soroban blockchain settlement |
| `src/api/` | REST API (Axum, port 8443) |

### Data flow

```
Hardware meter (mTLS) → parse_envelope (gateway/parser.rs)
  → signature verify (gateway/crypto.rs)  → BackpressureFilter (gateway/stream.rs)
  → DiagnosticEngine (time_series/analytics.rs)
  → TariffEngine (tariffs/engine.rs)
  → NonceSequencer (soroban/sequencer.rs) → Soroban RPC (soroban/rpc.rs)
```

### Key design points

**`gateway/crypto.rs` — MeterRegistry + BloomFilter**
The `MeterRegistry` stores `MeterIdentity` structs (ed25519 public keys). It uses a custom `BloomFilter` as a certificate revocation list (CRL) sized for 1 M entries at 1 % FPR. TPM attestation on enrollment is optional but supported. A `lazy_static` `GLOBAL_REGISTRY` is the live singleton; mutable operations require acquiring its `Mutex`.

**`gateway/parser.rs` — zero-copy envelope parsing**
`CompressedEnvelope<'a>` borrows from the input slice — `meter_id: &'a str`, `payload: &'a [u8]`, `checksum: [u8; 32]`. `parse_envelope` makes **zero heap allocations**. Wire format: `[u16 BE meter_id_len][UTF-8 meter_id][payload][32-byte checksum]`. The Criterion benchmark in `benches/parser_bench.rs` asserts this contract with a `CountingAllocator` global allocator.

**`soroban/sequencer.rs` — per-grid nonce sequencer**
`NonceSequencer` issues monotonically increasing nonces per grid ID using atomic CAS and a block-reservation scheme (`NONCE_BLOCK_SIZE = 100`). A background reaper task evicts stale grid state after 1 hour of inactivity. `commit_nonce` guards against double-spends. `NonceSequencer` is wrapped in `Arc` and injected into Axum router state.

**`time_series/analytics.rs` — streaming diagnostic engine**
`DiagnosticEngine` maintains a per-meter sliding window (default 30 days) of `Reading`s. `analyze()` runs an STL-like decomposition (trailing moving-average trend + median monthly seasonal factors), fits a 2-covariate OLS weather model, computes a dynamic p95 anomaly threshold, and classifies probable cause (Leak / Theft / SensorFault / SeasonalVariation). A `lazy_static` `GLOBAL_ENGINE` is used by API handlers. The legacy `analyze_consumption` function (static threshold baseline) is kept for backward compatibility.

**`soroban/preflight.rs` — transaction fee simulation**
`run_preflight` calls Soroban's `simulateTransaction` RPC up to `max_iterations` times, iteratively tightening the instruction leeway. Results are cached in a `lazy_static` LRU cache keyed on `(contract_id, sha256(operation_xdr))`. `budget_optimizer` provides a binary-search helper for fee minimisation.

**`time_series/pool.rs` — multi-tenant DB pools**
`MultiTenantPoolManager` holds one `deadpool-postgres` pool per tenant. The `get_connection` method records a starvation metric on pool exhaustion. Credentials are currently hardcoded (`utility`/`utility_secret`).

**`api/alloc_tracker.rs` — allocation timing middleware**
`TrackingAllocator` wraps `System` and records allocation/deallocation latency into the `GC_PAUSE_SECONDS` Prometheus counter. It is not yet wired up as `#[global_allocator]` — that attribute lives in `benches/parser_bench.rs` for the benchmark binary only.

### API routes

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Liveness probe |
| GET | `/readyz` | Readiness probe |
| GET | `/api/v1/meters` | List meters |
| GET | `/api/v1/meters/:id` | Get meter |
| POST | `/api/v1/meters/register` | Register meter with optional TPM attestation |
| POST | `/api/v1/meters/rotate-key` | Rotate meter signing key |
| GET | `/api/v1/tariffs` | List tariff schedules |
| POST | `/api/v1/readings` | Submit meter reading |
| POST | `/api/v1/settle` | Trigger blockchain settlement |
| GET | `/api/v1/time-series/diagnostics/:meter_id` | Run diagnostic analysis |
| POST | `/api/v1/calibrate/:meter_id` | Calibrate meter drift |
| GET | `/api/v1/nonce/status` | Grid nonce high-water marks |
| GET | `/metrics` | Prometheus metrics |

### Fixed-point arithmetic

`tariffs/math.rs` uses the `fixed` crate (`I64F64`) for commodity unit scaling to avoid floating-point rounding in settlement calculations.

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgres://utility:utility_secret@localhost:5432/utility_test` | TimescaleDB connection |
| `SOROBAN_RPC_URL` | — | Soroban JSON-RPC endpoint |
| `RUST_LOG` | `info` | Log filter (`tracing-subscriber`) |
