#!/usr/bin/env python3
"""
scripts/tests/test_perf_regression_check.py

Unit tests for the performance regression gate (issue #238). Uses only the
standard library (unittest) so it runs with a plain `python3 -m unittest`
in CI without adding a new Python dependency to the repo.
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from perf_regression_check import (  # noqa: E402
    Verdict,
    compare_results,
    extract_results,
)


class ExtractResultsTests(unittest.TestCase):
    def test_returns_empty_dict_for_missing_directory(self):
        self.assertEqual(extract_results(Path("/nonexistent/path")), {})

    def test_extracts_mean_point_estimate_from_nested_layout(self):
        with tempfile.TemporaryDirectory() as tmp:
            criterion_dir = Path(tmp)
            bench_dir = criterion_dir / "merkle_tree_build_1024" / "new"
            bench_dir.mkdir(parents=True)
            (bench_dir / "estimates.json").write_text(json.dumps({"mean": {"point_estimate": 12345.6}}))

            results = extract_results(criterion_dir)
            self.assertEqual(results, {"merkle_tree_build_1024": 12345.6})

    def test_extracts_multiple_benchmarks_including_grouped_ones(self):
        with tempfile.TemporaryDirectory() as tmp:
            criterion_dir = Path(tmp)
            for group, name, mean in [
                ("alloc_free_128_same_thread", "arena", 100.0),
                ("alloc_free_128_same_thread", "system", 250.0),
                (None, "parse_envelope", 500.0),
            ]:
                bench_dir = criterion_dir / (f"{group}/{name}" if group else name) / "new"
                bench_dir.mkdir(parents=True)
                (bench_dir / "estimates.json").write_text(json.dumps({"mean": {"point_estimate": mean}}))

            results = extract_results(criterion_dir)
            self.assertEqual(
                results,
                {
                    "alloc_free_128_same_thread/arena": 100.0,
                    "alloc_free_128_same_thread/system": 250.0,
                    "parse_envelope": 500.0,
                },
            )

    def test_skips_malformed_estimates_file_without_crashing(self):
        with tempfile.TemporaryDirectory() as tmp:
            criterion_dir = Path(tmp)
            bench_dir = criterion_dir / "broken_bench" / "new"
            bench_dir.mkdir(parents=True)
            (bench_dir / "estimates.json").write_text("not valid json {{{")

            results = extract_results(criterion_dir)
            self.assertEqual(results, {})


class CompareResultsTests(unittest.TestCase):
    def test_no_regression_within_threshold(self):
        baseline = {"parse_envelope": 1000.0}
        current = {"parse_envelope": 1050.0}  # +5%, threshold is 10%
        any_regression, rows = compare_results(baseline, current, threshold=0.10)
        self.assertFalse(any_regression)
        self.assertEqual(rows[0]["verdict"], Verdict.OK)

    def test_detects_regression_past_threshold(self):
        baseline = {"parse_envelope": 1000.0}
        current = {"parse_envelope": 1200.0}  # +20%, threshold is 10%
        any_regression, rows = compare_results(baseline, current, threshold=0.10)
        self.assertTrue(any_regression)
        self.assertEqual(rows[0]["verdict"], Verdict.REGRESSION)
        self.assertAlmostEqual(rows[0]["pct_change"], 0.20)

    def test_detects_improvement_past_threshold(self):
        baseline = {"parse_envelope": 1000.0}
        current = {"parse_envelope": 800.0}  # -20%
        any_regression, rows = compare_results(baseline, current, threshold=0.10)
        self.assertFalse(any_regression)
        self.assertEqual(rows[0]["verdict"], Verdict.IMPROVEMENT)

    def test_flags_new_benchmark_without_failing(self):
        baseline: dict = {}
        current = {"new_bench": 500.0}
        any_regression, rows = compare_results(baseline, current, threshold=0.10)
        self.assertFalse(any_regression)
        self.assertEqual(rows[0]["verdict"], Verdict.NEW)

    def test_flags_removed_benchmark_without_failing(self):
        baseline = {"old_bench": 500.0}
        current: dict = {}
        any_regression, rows = compare_results(baseline, current, threshold=0.10)
        self.assertFalse(any_regression)
        self.assertEqual(rows[0]["verdict"], Verdict.REMOVED)

    def test_one_regression_among_many_still_fails_overall(self):
        baseline = {"a": 100.0, "b": 100.0, "c": 100.0}
        current = {"a": 105.0, "b": 200.0, "c": 95.0}  # only b regresses
        any_regression, rows = compare_results(baseline, current, threshold=0.10)
        self.assertTrue(any_regression)
        verdicts = {row["id"]: row["verdict"] for row in rows}
        self.assertEqual(verdicts["a"], Verdict.OK)
        self.assertEqual(verdicts["b"], Verdict.REGRESSION)
        self.assertEqual(verdicts["c"], Verdict.OK)

    def test_zero_baseline_does_not_crash(self):
        baseline = {"degenerate": 0.0}
        current = {"degenerate": 100.0}
        # Should not raise a ZeroDivisionError.
        any_regression, rows = compare_results(baseline, current, threshold=0.10)
        self.assertFalse(any_regression)
        self.assertEqual(rows[0]["verdict"], Verdict.OK)


if __name__ == "__main__":
    unittest.main()
