# Automated performance regression detection

The backend CI pipeline now treats performance as a release gate. Criterion benchmarks run in the `performance-regression` job and produce a normalized `perf-baseline.json` cache. Pull requests compare their benchmark means against the latest restored baseline and fail when either condition is true:

- a benchmark is more than 5% and at least 1ms slower than baseline;
- a critical-path benchmark exceeds the 100ms P99 latency budget.

Critical paths are configured by passing repeated `--critical-path` values to `scripts/perf_regression.py`; the default set covers telemetry parsing and pooled ingestion reads. The script always writes the current baseline to `target/perf-current/perf-baseline.json` so successful `main` builds can publish the next comparison point.

## CI architecture

1. Restore the prior `perf-baseline` cache if it exists.
2. Run `cargo bench --all-features` to refresh Criterion estimates.
3. Execute `python3 scripts/perf_regression.py` with the 5% regression threshold and 100ms P99 budget.
4. Publish the new baseline only from successful `main` branch pushes.

## Monitoring and deployment controls

Runtime alerting mirrors the CI gate in `ops/alerts/performance-regression.yml`. The `CriticalPathP99LatencyBudgetBurn` page fires when production critical paths stay above 100ms P99 for 10 minutes. The `PerformanceRegressionCanaryMismatch` alert blocks blue-green promotion when canary latency is more than 5% slower than stable for 15 minutes.

During blue-green deployments, keep the green slice at canary size until both alerts remain healthy for at least one analysis window. If either alert fires, roll traffic back to blue, capture the Criterion cache from the candidate build, and attach the CI regression output to the incident.
