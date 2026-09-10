from __future__ import annotations

import importlib.util
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "check-performance-regression.py"
SPEC = importlib.util.spec_from_file_location("performance_regression", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class PerformanceRegressionEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def sample(self, name: str, values: list[tuple[str, str, float]]) -> Path:
        path = self.root / f"{name}.json"
        path.write_text(
            json.dumps(
                {
                    "run_id": name,
                    "metrics": [
                        {"participant": participant, "case": case, "elapsed_ms": elapsed}
                        for participant, case, elapsed in values
                    ],
                }
            ),
            encoding="utf-8",
        )
        return path

    def series(self, prefix: str, values: list[float]) -> list[Path]:
        return [
            self.sample(f"{prefix}-{index}", [("server", "scan", value)])
            for index, value in enumerate(values)
        ]

    def test_uses_five_sample_median(self) -> None:
        failures = MODULE.compare(
            self.series("base", [10.0, 10.1, 9.9, 1000.0, 10.0]),
            self.series("current", [11.0, 11.1, 10.9, 0.1, 11.0]),
        )
        self.assertEqual(failures, [])

    def test_rejects_non_finite_and_duplicate_metrics(self) -> None:
        non_finite = self.sample("nan", [("server", "scan", math.nan)])
        with self.assertRaises(MODULE.EvidenceError):
            MODULE.load_sample(non_finite)

        duplicate = self.sample(
            "duplicate",
            [("server", "scan", 1.0), ("server", "scan", 2.0)],
        )
        with self.assertRaises(MODULE.EvidenceError):
            MODULE.load_sample(duplicate)

    def test_rejects_too_few_or_reused_samples(self) -> None:
        paths = self.series("short", [1.0, 1.1, 1.2, 1.3])
        with self.assertRaises(MODULE.EvidenceError):
            MODULE.load_series(paths, 5, "base")

        reused = self.series("same", [1.0] * 5)
        payload = json.loads(reused[-1].read_text(encoding="utf-8"))
        payload["run_id"] = "same-0"
        reused[-1].write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaises(MODULE.EvidenceError):
            MODULE.load_series(reused, 5, "base")

    def test_rejects_inventory_drift(self) -> None:
        paths = self.series("inventory", [1.0] * 5)
        payload = json.loads(paths[-1].read_text(encoding="utf-8"))
        payload["metrics"][0]["case"] = "other"
        paths[-1].write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaises(MODULE.EvidenceError):
            MODULE.load_series(paths, 5, "base")


if __name__ == "__main__":
    unittest.main()
