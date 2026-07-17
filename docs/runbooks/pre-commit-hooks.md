# Pre-Commit Hook Suite Runbook

## Architecture

The repository uses [`pre-commit`](https://pre-commit.com/) as a local quality gate before code reaches CI. The suite is defined in `.pre-commit-config.yaml` and layers checks in increasing cost:

1. **File hygiene**: whitespace, line endings, YAML/TOML parsing, merge-conflict markers, case conflicts, and oversized files.
2. **Text quality**: `typos` catches spelling mistakes in source, configuration, and documentation.
3. **Rust correctness**: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test --all-features --lib --bins` match the fast parts of CI.
4. **Security guardrail**: `scripts/pre-commit-security.sh` scans tracked content for high-risk credential patterns before a commit is created.

The hooks are intentionally deterministic and run without service dependencies. Database-backed integration tests remain in CI because they require a provisioned PostgreSQL/TimescaleDB service.

## Installation

```bash
python -m pip install pre-commit
pre-commit install --install-hooks
```

Run the full suite manually before opening a pull request:

```bash
pre-commit run --all-files
```

## Operational targets

- Keep file hygiene and security scans lightweight so routine commits remain responsive.
- Treat `cargo fmt` and `cargo clippy` failures as blocking; CI enforces the same standards.
- Run full database integration tests in CI and before releases with `cargo test --all-features` and a configured `DATABASE_URL`.

## Monitoring and alerting

Pre-commit runs locally and does not emit production telemetry. CI is the authoritative monitoring surface for team-wide compliance:

- Alert on repeated `backend-ci` failures for `Rustfmt check`, `Clippy lint`, or unit/integration tests.
- Review hook adoption during security reviews by checking whether pull requests contain generated formatting-only corrections.
- Update this runbook when adding or removing hooks so incident responders can reproduce failed quality gates.

## Deployment and rollout

1. Land the configuration and script in a pull request.
2. Announce the installation commands to contributors.
3. Keep CI as the enforcement backstop while adoption ramps up.
4. If a hook causes unexpected failures, temporarily bypass locally with `SKIP=<hook-id> git commit` and file a follow-up issue; do not bypass CI.

## Troubleshooting

| Symptom | Resolution |
| --- | --- |
| `pre-commit: command not found` | Install with `python -m pip install pre-commit`. |
| Rust hooks cannot find `cargo` | Install the stable Rust toolchain with `rustup toolchain install stable`. |
| Security scan flags a test fixture | Prefer shortening fake credentials. If a realistic fixture is required, document the exception in the security review. |
| Full integration tests fail locally | Start dependencies with `docker compose up -d` and set `DATABASE_URL`. |
