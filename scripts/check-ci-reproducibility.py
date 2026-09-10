#!/usr/bin/env python3
from pathlib import Path
import re

root = Path(__file__).resolve().parent.parent
workflows = [root / ".github/workflows/ci.yml", root / ".github/workflows/nightly.yml"]
sha_ref = re.compile(r"^[^@\s]+@[0-9a-f]{40}(?:\s+#.*)?$")
toolchain = re.compile(r"^(?:[0-9]+\.[0-9]+\.[0-9]+|nightly-[0-9]{4}-[0-9]{2}-[0-9]{2})$")
tool_pin = re.compile(r"^[a-zA-Z0-9_-]+@[0-9]+(?:\.[0-9]+)+(?:[-+][a-zA-Z0-9_.-]+)?$")
errors = []

for workflow in workflows:
    lines = workflow.read_text(encoding="utf-8").splitlines()
    for line_number, raw in enumerate(lines, 1):
        stripped = raw.strip()
        if stripped.startswith("uses:"):
            value = stripped.removeprefix("uses:").strip()
            if not sha_ref.fullmatch(value):
                errors.append(f"{workflow}:{line_number}: mutable action ref: {value}")
        elif stripped.startswith("toolchain:"):
            value = stripped.removeprefix("toolchain:").strip()
            if not toolchain.fullmatch(value):
                errors.append(f"{workflow}:{line_number}: mutable Rust toolchain: {value}")
        elif stripped.startswith("tool:"):
            values = stripped.removeprefix("tool:").strip().split(",")
            for value in values:
                if not tool_pin.fullmatch(value):
                    errors.append(f"{workflow}:{line_number}: unpinned installed tool: {value}")

    index = 0
    while index < len(lines):
        command = lines[index].strip()
        if command.startswith("cargo ") or command.startswith("cargo +"):
            start = index + 1
            while command.endswith("\\") and index + 1 < len(lines):
                index += 1
                command += " " + lines[index].strip()
            if not re.match(r"cargo (?:\+\S+ )?fmt(?:\s|$)", command) and "--locked" not in command:
                errors.append(
                    f"{workflow}:{start}: dependency-resolving Cargo command lacks --locked: {command}"
                )
        index += 1

required_workflow_contracts = {
    root / ".github/workflows/ci.yml": (
        "cargo test --locked --workspace --all-targets --features test-filedb",
        "cargo check --locked --workspace --all-targets --no-default-features",
        "cargo test --locked --test r2_l06_batch_b_terminal_outcome_test",
        "cargo test --locked --bin radixdb-cli --features cli",
        "cargo test --locked --bin radixdb-bench --features bench-harness",
        "feature: [duckdb, mimalloc, semantic, ann-benchmark, dhat-heap]",
        "--target aarch64-unknown-linux-gnu",
        "llvm-cov --locked --workspace --all-targets",
        "python3 scripts/check-coverage-thresholds.py",
    ),
    root / ".github/workflows/nightly.yml": (
        "mutation_shards='[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]'",
        "miri_groups='[0,1,2,3,4]'",
        "cargo mutants --cargo-arg=--locked",
        "testcase.get('filter-match', {}).get('status') == 'matches'",
        "cargo +nightly-2026-07-07 miri nextest run --locked",
        "nightly-complete-cycle-${{ github.sha }}",
    ),
}
for workflow, required in required_workflow_contracts.items():
    text = workflow.read_text(encoding="utf-8")
    for snippet in required:
        if snippet not in text:
            errors.append(f"{workflow}: missing workflow contract: {snippet}")

if errors:
    raise SystemExit("\n".join(errors))

print("CI reproducibility contract: immutable actions, toolchains, tools and locked Cargo commands")
