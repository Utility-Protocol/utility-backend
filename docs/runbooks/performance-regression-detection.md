# Runbook: Automated Performance Regression Detection

Related issue: #238.

## What this is

A CI gate (`perf-regression` job in `.github/workflows/backend-ci.yml`)
that runs the repo's existing `criterion` benchmark suite
(`benches/parser_bench.rs`, `merkle_bench.rs`, `stream_throughput.rs`,
`arena_bench.rs`) on every push and pull request, compares the results
against a stored baseline from the most recent `main` build, and **fails
the build if any benchmark got more than 10% slower**.

## What's covered today, and what isn't

This gates the four existing criterion benchmarks — envelope parsing, the
settlement Merkle tree, telemetry stream throughput, and the arena
allocator. It does **not** currently cover live HTTP endpoint latency
(the issue's "<100ms P99 for critical paths" language implies
request-level measurement); that would need a load-testing step (e.g. k6
or wrk hitting a running instance) added as a further CI job, and isn't
built here — there's no existing endpoint-benchmark harness in the repo to
extend, and standing one up is a separate, larger piece of work. The
`arena_bench.rs` file existed on disk already but was missing from
`Cargo.toml`'s `[[bench]]` list, so `cargo bench` was silently skipping it
before this change — it's now included.

Likewise, "blue-green deploy with canary analysis" and "99.99% uptime"
from the issue's technical bounds describe infrastructure this repo
doesn't have configured (there's no deployment/canary tooling in this
codebase to hook into) — out of scope here; see the note at the bottom.

## How it works

1. `cargo bench --all-features` runs and criterion writes results to
   `target/criterion/**/new/estimates.json`.
2. `scripts/perf_regression_check.py extract` walks that directory and
   writes a flat `{benchmark_id: mean_ns}` snapshot.
3. `scripts/perf_regression_check.py compare` diffs that snapshot against
   the baseline restored from the GitHub Actions cache (see below),
   printing a table to the job log **and** to the run's summary page
   (`$GITHUB_STEP_SUMMARY`), and exits non-zero if anything regressed by
   more than the threshold.
4. On `main` only, the just-computed snapshot is saved as the new baseline
   (`actions/cache/save`, keyed by commit SHA) for future PRs to compare
   against.

## Reading a failure

Open the failed job's **Summary** tab — the same markdown table posted to
stdout is rendered there. Rows marked 🔴 REGRESSION exceeded the 10%
threshold; 🟢 improved and 🆕 new/⚪ removed rows are informational only
and never fail the build.

```
| Benchmark | Baseline | Current | Change | Verdict |
|---|---|---|---|---|
| merkle_tree_build_4096 | 812043 ns | 1105210 ns | +36.1% | 🔴 REGRESSION |
```

**To investigate:** download the `criterion-report` artifact from the same
run (uploaded regardless of pass/fail) and open
`<benchmark>/report/index.html` for criterion's full distribution/violin
plots — useful for telling a genuine regression apart from CI-runner
noise.

**If it's noise, not a real regression:** re-run the job. Shared CI
runners have enough scheduling jitter that an occasional single-run false
positive near the threshold is expected; a regression that reproduces
across re-runs is real.

**If it's a real, intentional trade-off** (e.g. added a safety check that
costs a few percent): merge to `main` as normal — the next `main` run
records the new, slower number as the baseline going forward, so this
isn't a permanent gate against that specific number, only against
*further* regressions from wherever `main` currently stands.

## Adjusting the threshold

`--threshold 0.10` in the workflow step is a fraction (10%). Change it in
`.github/workflows/backend-ci.yml` if it's too noisy or too loose in
practice; there's nothing else to update in sync with it.

## First run on a fresh clone / fork

If no baseline cache exists yet (e.g. a brand-new fork, or the very first
run of this workflow), `compare` prints a warning and exits `0` rather
than failing — there's nothing to compare against yet. The first push to
`main` establishes the initial baseline.

## Adding a new benchmark

Add the `.rs` file under `benches/`, register it in `Cargo.toml`'s
`[[bench]]` list (the same step that was missing for `arena_bench.rs`
before this change), and it's picked up automatically — no changes needed
to the CI job or the Python script.

## Out of scope (see issue #238 for the full ask)

- HTTP endpoint-level P99 latency gating (needs a load-testing harness
  against a running instance — not present in this repo today).
- Blue-green / canary deploy integration (no deploy tooling in this repo
  to hook into).
- A Grafana dashboard for benchmark trends over time (the criterion HTML
  report + this job's summary table serve that purpose today; a proper
  time-series dashboard would need bench results pushed to Prometheus,
  which they aren't currently — see `docs/dashboards/incident_response.json`
  for the pattern this repo already uses for `Prometheus`-backed panels
  if that's built out later).
