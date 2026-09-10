#!/usr/bin/env python3
from pathlib import Path
import json
import sys

thresholds = {
    "lines": 75.0,
    "functions": 75.0,
    "regions": 75.0,
    "branches": 55.0,
}

if len(sys.argv) != 2:
    raise SystemExit(f"usage: {Path(sys.argv[0]).name} <llvm-cov-summary.json>")

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
try:
    totals = payload["data"][0]["totals"]
except (KeyError, IndexError, TypeError) as error:
    raise SystemExit(f"invalid llvm-cov summary: missing data[0].totals: {error}")

failures = []
for metric, threshold in thresholds.items():
    try:
        count = int(totals[metric]["count"])
        covered = int(totals[metric]["covered"])
    except (KeyError, TypeError, ValueError) as error:
        failures.append(f"{metric}: missing coverage counters: {error}")
        continue
    if count == 0:
        failures.append(f"{metric}: zero instrumented items")
        continue
    percent = covered * 100.0 / count
    print(f"{metric}: {covered}/{count} = {percent:.2f}% (minimum {threshold:.2f}%)")
    if percent + 1e-9 < threshold:
        failures.append(f"{metric}: {percent:.2f}% is below {threshold:.2f}%")

if failures:
    raise SystemExit("coverage threshold failures:\n" + "\n".join(failures))
