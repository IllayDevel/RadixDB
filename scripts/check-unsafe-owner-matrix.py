#!/usr/bin/env python3
from pathlib import Path
import re

root = Path(__file__).resolve().parent.parent
matrix_path = root / "scripts/unsafe-owner-matrix.tsv"
nightly_path = root / ".github/workflows/nightly.yml"

owner_contracts = {
    "miri-group-0": ("compact_arc|compact_vec|smart_string",),
    "miri-group-1": ("test(/^i64_map::/)",),
    "miri-groups-2-and-4": ("test(/^cow_btree::/)", "deep_tree"),
    "miri-group-3": ("row_vec|value",),
    "asan-executor-lib": ("-p radixdb-executor --lib",),
    "asan-function-lib": ("-p radixdb-functions --lib scalar::semantic::",),
    "asan-parser-lib": ("-p radixdb-sql --lib",),
    "asan-index-lib": ("-p radixdb-storage --lib index::",),
    "asan-hnsw-and-index-lib": ("--test hnsw_index_test", "-p radixdb-storage --lib index::"),
    "asan-storage-v6": ("-p radixdb-storage --lib v6::",),
    "asan-plugin-platform": ("-p radixdb-plugin-abi", "-p radixdb-plugin-host"),
    "asan-plugin-tooling": ("-p cargo-radixdb-plugin --all-targets",),
    "asan-soak": ("-p radixdb-soak --lib",),
    "asan-workload-binaries": ("-p radixdb-join-workload --all-targets", "--bin radixdb-bench"),
    "asan-root-library": ("--lib sql_dump::",),
    "tsan-query-integrations": ("--test parallel_execution_tests",),
    "tsan-wire-integrations": ("--test wire_runtime_contract_test",),
    "tsan-mvcc-integrations": (
        "--test dirty_read_test",
        "--test mvcc_isolation_sql_test",
        "--test r8_write_pipeline_test",
    ),
}

declared = {}
for line_number, raw in enumerate(matrix_path.read_text(encoding="utf-8").splitlines(), 1):
    if not raw or raw.startswith("#"):
        continue
    fields = raw.split("\t")
    if len(fields) != 3 or not all(fields):
        raise SystemExit(f"{matrix_path}:{line_number}: expected three non-empty TSV fields")
    source, evidence, boundary = fields
    if source in declared:
        raise SystemExit(f"duplicate unsafe owner: {source}")
    declared[source] = (evidence, boundary)

unsafe_construct = re.compile(r"\bunsafe\s*(?:\{|fn\b|extern\b|impl\b|trait\b)")
actual = set()
sources = list((root / "src").rglob("*.rs"))
for crate in sorted((root / "crates").iterdir()):
    crate_sources = crate / "src"
    if crate_sources.is_dir():
        sources.extend(crate_sources.rglob("*.rs"))
for source in sorted(set(sources)):
    # Ignore line comments so prose such as "this would be unsafe" does not
    # create a false owner. Generated `unsafe` tokens inside proc-macro input
    # remain visible and are deliberately owned.
    code = "\n".join(
        line.split("//", 1)[0]
        for line in source.read_text(encoding="utf-8").splitlines()
    )
    if unsafe_construct.search(code):
        actual.add(source.relative_to(root).as_posix())

declared_paths = set(declared)
missing = sorted(actual - declared_paths)
stale = sorted(declared_paths - actual)
unknown_owners = sorted({evidence for evidence, _ in declared.values()} - set(owner_contracts))
nightly = nightly_path.read_text(encoding="utf-8")
missing_owner_evidence = {
    owner: [needle for needle in needles if needle not in nightly]
    for owner, needles in owner_contracts.items()
}
missing_owner_evidence = {
    owner: needles for owner, needles in missing_owner_evidence.items() if needles
}
if missing or stale or unknown_owners or missing_owner_evidence:
    if missing:
        print("unsafe sources missing from matrix:")
        print("\n".join(f"  {path}" for path in missing))
    if stale:
        print("stale matrix entries without unsafe source:")
        print("\n".join(f"  {path}" for path in stale))
    if unknown_owners:
        print("unknown evidence owners:")
        print("\n".join(f"  {owner}" for owner in unknown_owners))
    if missing_owner_evidence:
        print("nightly evidence owners missing required commands:")
        for owner, needles in missing_owner_evidence.items():
            print(f"  {owner}: {', '.join(needles)}")
    raise SystemExit(1)

print(f"unsafe owner matrix complete: {len(actual)} source files")
