# Performance regression runbook

## Triage

1. Open the failed CI job and read the `Detect performance regressions` output.
2. Restore or inspect the `perf-baseline` cache and compare it with `target/perf-current/perf-baseline.json` from the failed run.
3. Identify whether the failure is a relative regression, a critical-path 100ms budget violation, or both.
4. Check production alerts in `ops/alerts/performance-regression.yml` before retrying a deployment.

## Remediation

- For code regressions, profile the named benchmark locally with `cargo bench --bench <name>` and revert or optimize the slow path.
- For noisy measurements, rerun CI once. Do not raise thresholds unless two maintainers approve the new baseline and document the reason in the pull request.
- For canary regressions, stop blue-green promotion, shift traffic back to blue, and keep the green environment available for profiling.

## Baseline refresh

A successful push to `main` saves a new `perf-baseline` cache. If the cache expires, the checker still enforces the 100ms critical-path budget and writes a fresh baseline for the next successful `main` build.
