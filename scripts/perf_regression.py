#!/usr/bin/env python3
"""Detect Criterion benchmark regressions and hard latency SLO violations.

The checker reads Criterion ``estimates.json`` files from ``target/criterion`` and
compares the current mean point estimate with a committed or downloaded baseline.
Benchmark names containing a configured critical-path token are also checked
against the P99 latency budget. Criterion reports estimates in nanoseconds.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

DEFAULT_CRITICAL_PATHS = ("parse_envelope", "pooled_read")


@dataclass(frozen=True)
class BenchmarkResult:
    name: str
    mean_ns: float
    p99_budget_ns: float | None = None

    @property
    def mean_ms(self) -> float:
        return self.mean_ns / 1_000_000


def _criterion_name(path: Path, root: Path) -> str:
    rel = path.relative_to(root)
    return "/".join(rel.parts[:-2])


def load_criterion_results(root: Path, critical_paths: Iterable[str], p99_budget_ms: float) -> dict[str, BenchmarkResult]:
    results: dict[str, BenchmarkResult] = {}
    if not root.exists():
        return results

    critical_tokens = tuple(token for token in critical_paths if token)
    budget_ns = p99_budget_ms * 1_000_000
    for estimates in root.rglob("estimates.json"):
        if estimates.parent.name != "new":
            continue
        data = json.loads(estimates.read_text())
        mean = data.get("mean", {}).get("point_estimate")
        if not isinstance(mean, (int, float)):
            continue
        name = _criterion_name(estimates, root)
        p99_budget_ns = budget_ns if any(token in name for token in critical_tokens) else None
        results[name] = BenchmarkResult(name=name, mean_ns=float(mean), p99_budget_ns=p99_budget_ns)
    return results


def load_baseline(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text())
    if "benchmarks" in raw:
        raw = raw["benchmarks"]
    return {str(name): float(value) for name, value in raw.items()}


def write_baseline(path: Path, results: dict[str, BenchmarkResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "unit": "ns",
        "benchmarks": {name: round(result.mean_ns, 3) for name, result in sorted(results.items())},
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def detect_regressions(
    current: dict[str, BenchmarkResult],
    baseline: dict[str, float],
    max_regression_pct: float,
    min_regression_ns: float,
) -> list[str]:
    failures: list[str] = []
    for name, result in sorted(current.items()):
        if result.p99_budget_ns is not None and result.mean_ns > result.p99_budget_ns:
            failures.append(
                f"{name} exceeds critical-path budget: {result.mean_ms:.3f}ms > "
                f"{result.p99_budget_ns / 1_000_000:.3f}ms"
            )

        previous = baseline.get(name)
        if previous is None or previous <= 0:
            continue
        delta = result.mean_ns - previous
        pct = (delta / previous) * 100
        if delta > min_regression_ns and pct > max_regression_pct:
            failures.append(
                f"{name} regressed by {pct:.2f}%: current {result.mean_ms:.3f}ms vs "
                f"baseline {previous / 1_000_000:.3f}ms"
            )
    return failures


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--criterion-dir", type=Path, default=Path("target/criterion"))
    parser.add_argument("--baseline", type=Path, default=Path("target/perf-baseline/perf-baseline.json"))
    parser.add_argument("--output-baseline", type=Path, default=Path("target/perf-current/perf-baseline.json"))
    parser.add_argument("--max-regression-pct", type=float, default=5.0)
    parser.add_argument("--min-regression-ms", type=float, default=1.0)
    parser.add_argument("--p99-budget-ms", type=float, default=100.0)
    parser.add_argument("--critical-path", action="append", default=list(DEFAULT_CRITICAL_PATHS))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    current = load_criterion_results(args.criterion_dir, args.critical_path, args.p99_budget_ms)
    if not current:
        print(f"No Criterion estimates found in {args.criterion_dir}", file=sys.stderr)
        return 2

    baseline = load_baseline(args.baseline)
    write_baseline(args.output_baseline, current)
    failures = detect_regressions(
        current,
        baseline,
        args.max_regression_pct,
        args.min_regression_ms * 1_000_000,
    )
    print(f"Checked {len(current)} benchmark(s); baseline entries: {len(baseline)}")
    if failures:
        print("Performance regression detected:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
