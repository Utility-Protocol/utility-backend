import json
import tempfile
import unittest
from pathlib import Path

from scripts.perf_regression import BenchmarkResult, detect_regressions, load_criterion_results


def write_estimate(root: Path, bench: str, mean_ns: float) -> None:
    path = root.joinpath(*bench.split("/"), "new", "estimates.json")
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"mean": {"point_estimate": mean_ns}}))


class PerfRegressionTests(unittest.TestCase):
    def test_loads_criterion_results_and_marks_critical_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_estimate(root, "parse_envelope/throughput/envelope_bytes/64", 90_000_000)
            write_estimate(root, "merkle/proof", 5_000_000)

            results = load_criterion_results(root, ["parse_envelope"], 100.0)

        self.assertEqual(results["parse_envelope/throughput/envelope_bytes/64"].p99_budget_ns, 100_000_000)
        self.assertIsNone(results["merkle/proof"].p99_budget_ns)

    def test_detects_regression_and_slo_failure(self):
        current = {
            "critical": BenchmarkResult("critical", mean_ns=120_000_000, p99_budget_ns=100_000_000),
            "worker": BenchmarkResult("worker", mean_ns=11_100_000),
        }
        baseline = {"critical": 90_000_000, "worker": 10_000_000}

        failures = detect_regressions(current, baseline, max_regression_pct=5.0, min_regression_ns=1_000_000)

        self.assertTrue(any("exceeds critical-path budget" in failure for failure in failures))
        self.assertTrue(any("worker regressed" in failure for failure in failures))

    def test_ignores_noise_below_absolute_floor(self):
        current = {"worker": BenchmarkResult("worker", mean_ns=10_500_000)}
        baseline = {"worker": 10_000_000}

        failures = detect_regressions(current, baseline, max_regression_pct=1.0, min_regression_ns=1_000_000)

        self.assertEqual(failures, [])


if __name__ == "__main__":
    unittest.main()
