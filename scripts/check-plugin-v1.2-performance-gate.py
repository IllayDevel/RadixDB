#!/usr/bin/env python3
"""Validate the frozen RadixDB 1.2 plugin-disabled 100M NVMe gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
from pathlib import Path


EXPECTED_ROWS = 100_000_000
EXPECTED_CHECKSUM = "100000000:49734600639880"
EXPECTED_ACCESS_PATH_DIGEST = (
    "499488e7eeca18a91a2ebb763473308e15d4e929a7c0770d933c9fd9bba084d4"
)
MAX_RATIO = 1.20
BASELINES_MS = {
    "aggregate.group_having": 10.150,
    "correctness.seed_checksum": 12.007,
    "reference.fact_dictionary": 572.626,
    "scan.full": 105.224,
    "update.rollback": 42.994,
    "delete.rollback": 2.285,
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def access_path_digest(document: dict[str, object]) -> str:
    canonical = (json.dumps(document["access_paths"], indent=2) + "\n").encode()
    return hashlib.sha256(canonical).hexdigest()


def fail(message: str) -> None:
    raise ValueError(message)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("results", nargs=3, type=Path)
    arguments = parser.parse_args()

    documents: list[dict[str, object]] = []
    identities: set[str] = set()
    executable_hashes: set[str] = set()
    run_ids: set[str] = set()
    artifacts: list[dict[str, str]] = []
    try:
        for path in arguments.results:
            document = json.loads(path.read_text())
            environment_path = path.with_name("environment.json")
            environment_document = json.loads(environment_path.read_text())
            if document["total_rows"] != EXPECTED_ROWS:
                fail(f"{path}: total_rows differs from {EXPECTED_ROWS}")
            if document["participants"] != ["server"]:
                fail(f"{path}: participant set is not server-only")
            run_id = str(document["run_id"])
            if run_id in run_ids:
                fail(f"{path}: duplicate run_id: {run_id}")
            run_ids.add(run_id)
            resolved = document["resolved"]["benchmark"]
            server = document["resolved"]["radixdb_server"]
            if not resolved["verify_existing"]:
                fail(f"{path}: verify_existing is false")
            if resolved["warmup_runs"] != 1 or resolved["repeat_runs"] != 5:
                fail(f"{path}: warmup/repeat contract differs from 1/5")
            if server["page_cache_level"] != 0:
                fail(f"{path}: page cache is not disabled")
            if server["storage_cpu_workers"] != 0:
                fail(f"{path}: storage workers are not automatic")

            metrics = {item["case"]: item for item in document["metrics"]}
            checksum = metrics["correctness.seed_checksum"]
            if checksum["rows"] != EXPECTED_ROWS or checksum["checksum"] != EXPECTED_CHECKSUM:
                fail(f"{path}: correctness cardinality/checksum mismatch")
            digest = access_path_digest(document)
            if digest != EXPECTED_ACCESS_PATH_DIGEST:
                fail(f"{path}: access-path digest mismatch: {digest}")

            build = document["readiness"][-1]["status"]["build"]
            identity = build["git_revision"]
            if identity.endswith("-dirty"):
                fail(f"{path}: release binary identity is dirty")
            environment_build = environment_document["build"]
            source = environment_document["source_checkout_context"]
            if environment_build["git_revision"] != identity or source["git_head"] != identity:
                fail(f"{path}: result, environment, and checkout revisions differ")
            if source["git_status_porcelain"]:
                fail(f"{path}: source checkout is not clean")
            server_config = environment_document["server_config"]["content"]
            if "[plugins]" in server_config:
                fail(f"{path}: canonical plugin-disabled config contains a plugins section")
            identities.add(identity)
            executable_hashes.add(environment_build["executable_sha256"])
            report = path.with_name("REPORT.md")
            artifacts.append(
                {
                    "run_id": run_id,
                    "results_sha256": sha256(path),
                    "report_sha256": sha256(report),
                    "environment_sha256": sha256(environment_path),
                    "access_path_sha256": digest,
                }
            )
            documents.append(document)
        if len(identities) != 1:
            fail(f"runs do not share one release binary identity: {sorted(identities)}")
        if len(executable_hashes) != 1:
            fail("runs do not share one exact release binary SHA-256")

        cases: list[dict[str, object]] = []
        for case, baseline in BASELINES_MS.items():
            samples = []
            for document in documents:
                metrics = {item["case"]: item for item in document["metrics"]}
                samples.append(float(metrics[case]["elapsed_ms"]))
            median = statistics.median(samples)
            ratio = median / baseline
            cases.append(
                {
                    "case": case,
                    "samples_ms": samples,
                    "median_ms": median,
                    "baseline_ms": baseline,
                    "ratio": ratio,
                    "accepted": ratio <= MAX_RATIO,
                }
            )
        failed = [case for case in cases if not case["accepted"]]
        if failed:
            fail(
                "performance corridor exceeded: "
                + ", ".join(f"{case['case']}={case['ratio']:.3f}x" for case in failed)
            )
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        print(f"RadixDB v1.2 performance gate FAILED: {error}", file=sys.stderr)
        return 1

    print(
        json.dumps(
            {
                "status": "passed",
                "release_binary_git_revision": next(iter(identities)),
                "maximum_accepted_ratio": MAX_RATIO,
                "cases": cases,
                "artifacts": artifacts,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
