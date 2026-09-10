#!/usr/bin/env python3
"""Fail closed when repeated benchmark medians exceed the regression policy."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path


MetricKey = tuple[str, str]


class EvidenceError(ValueError):
    """Benchmark input is not sufficient for a regression decision."""


@dataclass(frozen=True)
class Sample:
    run_id: str
    metrics: dict[MetricKey, float]


def _reject_non_finite_json(value: str) -> None:
    raise EvidenceError(f"non-standard/non-finite JSON number `{value}`")


def load_sample(path: Path) -> Sample:
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            parse_constant=_reject_non_finite_json,
        )
    except (OSError, json.JSONDecodeError) as error:
        raise EvidenceError(f"cannot read {path}: {error}") from error
    if not isinstance(payload, dict):
        raise EvidenceError(f"{path}: root must be an object")
    run_id = payload.get("run_id")
    raw_metrics = payload.get("metrics")
    if not isinstance(run_id, str) or not run_id:
        raise EvidenceError(f"{path}: non-empty run_id is required")
    if not isinstance(raw_metrics, list) or not raw_metrics:
        raise EvidenceError(f"{path}: non-empty metrics array is required")

    result: dict[MetricKey, float] = {}
    for index, metric in enumerate(raw_metrics):
        if not isinstance(metric, dict):
            raise EvidenceError(f"{path}: metric {index} must be an object")
        participant = metric.get("participant")
        case = metric.get("case")
        elapsed = metric.get("elapsed_ms")
        if not isinstance(participant, str) or not participant:
            raise EvidenceError(f"{path}: metric {index} has invalid participant")
        if not isinstance(case, str) or not case:
            raise EvidenceError(f"{path}: metric {index} has invalid case")
        if isinstance(elapsed, bool) or not isinstance(elapsed, (int, float)):
            raise EvidenceError(f"{path}: {participant}/{case} elapsed_ms is not numeric")
        elapsed = float(elapsed)
        if not math.isfinite(elapsed) or elapsed < 0.0:
            raise EvidenceError(
                f"{path}: {participant}/{case} elapsed_ms must be finite and non-negative"
            )
        key = (participant, case)
        if key in result:
            raise EvidenceError(f"{path}: duplicate metric {participant}/{case}")
        result[key] = elapsed
    return Sample(run_id=run_id, metrics=result)


def load_series(paths: list[Path], minimum_samples: int, label: str) -> dict[MetricKey, list[float]]:
    if minimum_samples < 3:
        raise EvidenceError("minimum sample policy cannot be below three")
    if len(paths) < minimum_samples:
        raise EvidenceError(
            f"{label}: {len(paths)} samples supplied, at least {minimum_samples} are required"
        )
    samples = [load_sample(path) for path in paths]
    run_ids = [sample.run_id for sample in samples]
    if len(set(run_ids)) != len(run_ids):
        raise EvidenceError(f"{label}: duplicate run_id values do not prove independent samples")

    inventory = set(samples[0].metrics)
    for path, sample in zip(paths[1:], samples[1:]):
        if set(sample.metrics) != inventory:
            difference = sorted(set(sample.metrics) ^ inventory)
            raise EvidenceError(f"{label}: metric inventory differs in {path}: {difference}")
    return {
        key: [sample.metrics[key] for sample in samples]
        for key in sorted(inventory)
    }


def compare(
    base_paths: list[Path],
    current_paths: list[Path],
    minimum_samples: int = 5,
) -> list[str]:
    base = load_series(base_paths, minimum_samples, "base")
    current = load_series(current_paths, minimum_samples, "current")
    if base.keys() != current.keys():
        missing = sorted(base.keys() ^ current.keys())
        raise EvidenceError(f"base/current metric inventory differs: {missing}")

    failures: list[str] = []
    print(
        "participant/case\tbase_median_ms\tcurrent_median_ms\tdelta"
        "\tbase_range_ms\tcurrent_range_ms\tsamples\tdecision"
    )
    for key in sorted(base):
        base_values = base[key]
        current_values = current[key]
        before = statistics.median(base_values)
        after = statistics.median(current_values)
        if before == 0.0:
            delta = None
            decision = "record-only: zero/sub-ms baseline"
        else:
            delta = (after / before - 1.0) * 100.0
            if before < 1.0:
                decision = "record-only: sub-ms"
            elif delta > 25.0:
                decision = "FAIL: >25%"
                failures.append(f"{key[0]}/{key[1]} {delta:+.2f}%")
            else:
                decision = "pass"
        delta_text = "n/a" if delta is None else f"{delta:+.2f}%"
        print(
            f"{key[0]}/{key[1]}\t{before:.6f}\t{after:.6f}\t{delta_text}"
            f"\t{min(base_values):.6f}..{max(base_values):.6f}"
            f"\t{min(current_values):.6f}..{max(current_values):.6f}"
            f"\t{len(base_values)}/{len(current_values)}\t{decision}"
        )
    return failures


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare repeated base/current benchmark medians fail-closed"
    )
    parser.add_argument("--base", nargs="+", type=Path, required=True)
    parser.add_argument("--current", nargs="+", type=Path, required=True)
    parser.add_argument("--minimum-samples", type=int, default=5)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    failures = compare(args.base, args.current, args.minimum_samples)
    if failures:
        print("performance regressions require owner review:", file=sys.stderr)
        print("\n".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except EvidenceError as error:
        raise SystemExit(f"invalid benchmark evidence: {error}") from error
