#!/usr/bin/env python3
"""
scripts/perf_regression_check.py

Automated performance regression detection for CI (issue #238), built on
top of the criterion benchmark suite that already exists in this repo
(benches/parser_bench.rs, merkle_bench.rs, stream_throughput.rs,
arena_bench.rs — the last of these existed on disk but was never wired
into Cargo.toml's [[bench]] list until this change, so `cargo bench` was
silently skipping it).

Two subcommands, run from the repo root after `cargo bench --all-features`
has produced `target/criterion/`:

  extract   Walk target/criterion/**/new/estimates.json and write a flat
            {benchmark_id: mean_ns} JSON snapshot of the current run.

  compare   Compare a "current" snapshot against a "baseline" snapshot and
            exit non-zero if any benchmark regressed past --threshold
            (default 10%). Missing baseline (e.g. first run on a fresh
            repo, or before this workflow's first main-branch run) is
            treated as "nothing to compare against yet" and exits 0 with a
            warning, rather than failing CI.

Both subcommands are side-effect-free beyond reading/writing the given
paths, so the comparison logic is directly unit-testable (see
scripts/tests/test_perf_regression_check.py) without needing to actually
run cargo bench.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, Tuple

DEFAULT_THRESHOLD = 0.10  # 10% slower than baseline counts as a regression.


# ─── extract ─────────────────────────────────────────────────────────────────


def extract_results(criterion_dir: Path) -> Dict[str, float]:
    """Walks target/criterion/**/new/estimates.json and returns
    {benchmark_id: mean_point_estimate_ns}.

    Criterion nests each benchmark under
    target/criterion/<group>/<bench_name>/new/estimates.json (or
    target/criterion/<bench_name>/new/estimates.json for ungrouped
    benchmarks); the benchmark_id used here is that path relative to
    criterion_dir with the trailing "/new/estimates.json" stripped, so it's
    stable across runs and matches how criterion itself labels benchmarks
    in its HTML report.
    """
    results: Dict[str, float] = {}
    if not criterion_dir.is_dir():
        return results

    for estimates_path in sorted(criterion_dir.glob("**/new/estimates.json")):
        benchmark_id = str(estimates_path.parent.parent.relative_to(criterion_dir))
        try:
            with estimates_path.open() as f:
                data = json.load(f)
            mean_ns = data["mean"]["point_estimate"]
        except (KeyError, json.JSONDecodeError, OSError):
            # A malformed/partial estimates.json shouldn't take down the
            # whole extraction; skip just that one benchmark.
            continue
        results[benchmark_id] = float(mean_ns)

    return results


def cmd_extract(args: argparse.Namespace) -> int:
    results = extract_results(Path(args.criterion_dir))
    Path(args.out).write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    print(f"Extracted {len(results)} benchmark result(s) to {args.out}")
    return 0


# ─── compare ─────────────────────────────────────────────────────────────────


class Verdict:
    OK = "ok"
    REGRESSION = "regression"
    IMPROVEMENT = "improvement"
    NEW = "new"  # present in current, absent from baseline
    REMOVED = "removed"  # present in baseline, absent from current


def compare_results(
    baseline: Dict[str, float], current: Dict[str, float], threshold: float
) -> Tuple[bool, list[dict]]:
    """Returns (any_regression, rows) where each row describes one
    benchmark's verdict. A benchmark counts as regressed when its current
    mean exceeds its baseline mean by more than `threshold` (a fraction,
    e.g. 0.10 for 10%)."""
    rows: list[dict] = []
    any_regression = False

    all_ids = sorted(set(baseline) | set(current))
    for benchmark_id in all_ids:
        base_ns = baseline.get(benchmark_id)
        cur_ns = current.get(benchmark_id)

        if base_ns is None:
            rows.append({"id": benchmark_id, "verdict": Verdict.NEW, "current_ns": cur_ns})
            continue
        if cur_ns is None:
            rows.append({"id": benchmark_id, "verdict": Verdict.REMOVED, "baseline_ns": base_ns})
            continue

        pct_change = (cur_ns - base_ns) / base_ns if base_ns > 0 else 0.0
        if pct_change > threshold:
            verdict = Verdict.REGRESSION
            any_regression = True
        elif pct_change < -threshold:
            verdict = Verdict.IMPROVEMENT
        else:
            verdict = Verdict.OK

        rows.append(
            {
                "id": benchmark_id,
                "verdict": verdict,
                "baseline_ns": base_ns,
                "current_ns": cur_ns,
                "pct_change": pct_change,
            }
        )

    return any_regression, rows


def format_report(rows: list[dict], threshold: float) -> str:
    lines = [
        f"## Performance regression check (threshold: {threshold:.0%})",
        "",
        "| Benchmark | Baseline | Current | Change | Verdict |",
        "|---|---|---|---|---|",
    ]
    icons = {
        Verdict.OK: "✅",
        Verdict.REGRESSION: "🔴 REGRESSION",
        Verdict.IMPROVEMENT: "🟢 improved",
        Verdict.NEW: "🆕 new",
        Verdict.REMOVED: "⚪ removed",
    }
    for row in rows:
        baseline_str = f"{row['baseline_ns']:.0f} ns" if "baseline_ns" in row else "—"
        current_str = f"{row['current_ns']:.0f} ns" if "current_ns" in row else "—"
        change_str = f"{row['pct_change']:+.1%}" if "pct_change" in row else "—"
        lines.append(f"| {row['id']} | {baseline_str} | {current_str} | {change_str} | {icons[row['verdict']]} |")
    return "\n".join(lines)


def cmd_compare(args: argparse.Namespace) -> int:
    baseline_path = Path(args.baseline)
    current_path = Path(args.current)

    current = json.loads(current_path.read_text())

    if not baseline_path.is_file():
        print(
            "No performance baseline found yet (expected at "
            f"{baseline_path}) — skipping the regression gate. "
            "A baseline is recorded automatically on the next push to main.",
            file=sys.stderr,
        )
        return 0

    baseline = json.loads(baseline_path.read_text())
    any_regression, rows = compare_results(baseline, current, args.threshold)

    report = format_report(rows, args.threshold)
    print(report)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a") as f:
            f.write(report + "\n")

    if any_regression:
        print(
            f"\nOne or more benchmarks regressed by more than {args.threshold:.0%}. "
            "See the table above.",
            file=sys.stderr,
        )
        return 1

    print("\nNo regressions detected.")
    return 0


# ─── CLI ─────────────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract_parser = subparsers.add_parser("extract", help="Extract results from target/criterion into a JSON snapshot")
    extract_parser.add_argument("--criterion-dir", default="target/criterion", help="Path to criterion's output directory")
    extract_parser.add_argument("--out", required=True, help="Where to write the extracted JSON snapshot")
    extract_parser.set_defaults(func=cmd_extract)

    compare_parser = subparsers.add_parser("compare", help="Compare a current snapshot against a baseline snapshot")
    compare_parser.add_argument("--baseline", required=True, help="Path to the baseline JSON snapshot")
    compare_parser.add_argument("--current", required=True, help="Path to the current JSON snapshot")
    compare_parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD, help="Regression threshold as a fraction (default: 0.10 = 10%%)")
    compare_parser.set_defaults(func=cmd_compare)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
