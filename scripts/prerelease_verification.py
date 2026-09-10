#!/usr/bin/env python3
"""Fail-closed, resumable orchestration for RadixDB prerelease verification.

The runner owns orchestration and evidence only. The individual Rust tests and
existing shell scripts remain the owners of database fixtures and destructive
fault injection. Commands are executed serially and a successful stage becomes
resumable only after its complete artifact boundary has been hashed.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import fcntl
import hashlib
import json
import os
import platform
import re
import resource
import shlex
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


FORMAT_VERSION = 1
LARGE_FIXTURE_MIN_FREE_BYTES = 64 * 1024 * 1024 * 1024
IDENTIFIER = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
BUILD_IDENTITY = re.compile(
    r"git=(?P<git>[0-9a-f]{40}(?:-dirty)?)\s+"
    r"protocol=(?P<protocol>\d+)\s+profile=(?P<profile>\S+)\s+"
    r"target=(?P<target>\S+)\s+lock=(?P<lock>[0-9a-f]{64})"
)
HEAVY_CARGO_ACTIONS = {"build", "check", "clippy", "doc", "run", "test"}
PROCESS_GLOBAL_STAGES = {
    "benchmark-100m",
    "capacity-boundary-512",
    "capacity-kill-256",
    "faults",
    "large-fixture-128",
    "mutation-proof",
    "profile",
    "recovery",
    "soak",
}
POSTGRES_ORACLE_ENV = "RADIXDB_PRERELEASE_PG_DSN"


class PrereleaseError(RuntimeError):
    """Base class for a fail-closed runner error."""


class PreflightError(PrereleaseError):
    """Candidate or host state is not eligible for execution."""


class ArtifactIntegrityError(PrereleaseError):
    """A supposedly immutable completed artifact changed or disappeared."""


class StageFailed(PrereleaseError):
    """A stage command exited unsuccessfully."""


@dataclasses.dataclass(frozen=True)
class CommandSpec:
    label: str
    argv: tuple[str, ...]
    env: tuple[tuple[str, str], ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "argv": list(self.argv),
            "env": dict(self.env),
        }


@dataclasses.dataclass(frozen=True)
class StageSpec:
    name: str
    commands: tuple[CommandSpec, ...]
    heavy: bool = True
    process_global: bool = False
    evidence_paths: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "heavy": self.heavy,
            "process_global": self.process_global,
            "evidence_paths": list(self.evidence_paths),
            "commands": [command.as_dict() for command in self.commands],
        }


@dataclasses.dataclass(frozen=True)
class RunRequest:
    repo: Path
    artifact_root: Path
    run_id: str
    candidate: str | None
    profile: str
    seed: int
    binaries: tuple[Path, ...]
    configs: tuple[Path, ...]
    selection_args: tuple[str, ...] = ()
    resume: bool = False
    dry_run: bool = False


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with temporary.open("wb") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def write_json(path: Path, value: Any) -> None:
    atomic_write(path, json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8") + b"\n")


def command_output(argv: Sequence[str], cwd: Path) -> str:
    completed = subprocess.run(
        argv,
        cwd=cwd,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return completed.stdout.strip()


def git_head(repo: Path) -> str:
    return command_output(("git", "rev-parse", "--verify", "HEAD"), repo)


def git_status(repo: Path) -> str:
    return command_output(
        ("git", "status", "--porcelain=v1", "--untracked-files=all"), repo
    )


def parse_meminfo(path: Path = Path("/proc/meminfo")) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise PreflightError(f"cannot read {path}: {error}") from error
    for line in lines:
        name, separator, remainder = line.partition(":")
        if not separator:
            continue
        fields = remainder.strip().split()
        if not fields:
            continue
        multiplier = 1024 if len(fields) > 1 and fields[1] == "kB" else 1
        try:
            values[name] = int(fields[0]) * multiplier
        except ValueError:
            continue
    return values


def ancestor_pids(pid: int | None = None, proc_root: Path = Path("/proc")) -> set[int]:
    current = os.getpid() if pid is None else pid
    result: set[int] = set()
    while current > 1 and current not in result:
        result.add(current)
        status = proc_root / str(current) / "status"
        try:
            ppid_line = next(
                line for line in status.read_text(encoding="utf-8").splitlines()
                if line.startswith("PPid:")
            )
            current = int(ppid_line.split()[1])
        except (OSError, StopIteration, ValueError, IndexError):
            break
    return result


def is_heavy_process(argv: Sequence[str]) -> bool:
    if not argv:
        return False
    basenames = [Path(part).name for part in argv]
    if "rustc" in basenames:
        return True
    if "radixdb-bench" in basenames:
        return True
    if any(
        name.startswith("run-prerelease-verification")
        or name == "prerelease_verification.py"
        for name in basenames
    ):
        return True
    for index, name in enumerate(basenames):
        if name == "cargo" and index + 1 < len(argv):
            return argv[index + 1] in HEAVY_CARGO_ACTIONS
    return False


def foreign_heavy_processes(
    proc_root: Path = Path("/proc"), excluded_pids: set[int] | None = None
) -> list[dict[str, Any]]:
    excluded = ancestor_pids(proc_root=proc_root) if excluded_pids is None else excluded_pids
    result: list[dict[str, Any]] = []
    try:
        entries = list(proc_root.iterdir())
    except OSError as error:
        raise PreflightError(f"cannot inspect process table {proc_root}: {error}") from error
    for entry in entries:
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if pid in excluded:
            continue
        try:
            raw = (entry / "cmdline").read_bytes()
        except OSError:
            continue
        argv = [part.decode("utf-8", "replace") for part in raw.split(b"\0") if part]
        if is_heavy_process(argv):
            result.append({"pid": pid, "argv": argv})
    return sorted(result, key=lambda item: item["pid"])


def binary_snapshot(
    path: Path, repo: Path, commit: str, lock_sha: str, *, strict: bool = True
) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    if not resolved.is_file() or not os.access(resolved, os.X_OK):
        if strict:
            raise PreflightError(f"release binary is missing or not executable: {resolved}")
        return {
            "path": str(resolved),
            "sha256": None,
            "size_bytes": None,
            "identity": None,
            "identity_text": None,
            "validation_errors": ["release binary is missing or not executable"],
        }
    try:
        identity_text = command_output((str(resolved), "--version"), repo)
    except (OSError, subprocess.CalledProcessError):
        identity_text = ""
    match = BUILD_IDENTITY.search(identity_text)
    identity: dict[str, Any] | None = None
    validation_errors: list[str] = []
    if match:
        identity = match.groupdict()
        if identity["git"] != commit:
            validation_errors.append(
                f"built from {identity['git']}, expected clean {commit}"
            )
        if identity["profile"] != "release":
            validation_errors.append(
                f"profile is {identity['profile']}, expected release"
            )
        if identity["lock"] != lock_sha:
            validation_errors.append(
                f"Cargo.lock is {identity['lock']}, expected {lock_sha}"
            )
    else:
        validation_errors.append("binary does not expose a parseable --version build identity")
    if strict and validation_errors:
        raise PreflightError(f"binary {resolved}: " + "; ".join(validation_errors))
    return {
        "path": str(resolved),
        "sha256": sha256_file(resolved),
        "size_bytes": resolved.stat().st_size,
        "identity": identity,
        "identity_text": identity_text or None,
        "validation_errors": validation_errors,
    }


def filesystem_type(path: Path) -> str:
    try:
        return command_output(("findmnt", "-n", "-o", "FSTYPE", "-T", str(path)), path)
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def collect_preflight(
    request: RunRequest,
    stages: Sequence[StageSpec],
    *,
    proc_root: Path = Path("/proc"),
    enforce_clean: bool = True,
    enforce_foreign_processes: bool = True,
) -> dict[str, Any]:
    required_environment = validate_required_environment(stages)
    repo = request.repo.resolve()
    if not (repo / ".git").exists():
        raise PreflightError(f"not a Git checkout: {repo}")
    commit = git_head(repo)
    if request.candidate is not None and request.candidate != commit:
        raise PreflightError(
            f"candidate mismatch: requested {request.candidate}, current HEAD is {commit}"
        )
    status = git_status(repo)
    if enforce_clean and status:
        raise PreflightError("candidate worktree is not clean:\n" + status)
    lock_path = repo / "Cargo.lock"
    if not lock_path.is_file():
        raise PreflightError(f"Cargo.lock is missing: {lock_path}")
    lock_sha = sha256_file(lock_path)

    binaries = [
        binary_snapshot(path, repo, commit, lock_sha, strict=enforce_clean)
        for path in request.binaries
    ]
    configs: list[dict[str, Any]] = []
    for path in request.configs:
        resolved = path.expanduser().resolve()
        if not resolved.is_file():
            if enforce_clean:
                raise PreflightError(f"config is missing: {resolved}")
            configs.append(
                {
                    "path": str(resolved),
                    "sha256": None,
                    "size_bytes": None,
                    "validation_errors": ["config is missing"],
                }
            )
            continue
        configs.append(
            {
                "path": str(resolved),
                "sha256": sha256_file(resolved),
                "size_bytes": resolved.stat().st_size,
                "validation_errors": [],
            }
        )

    heavy = any(stage.heavy for stage in stages)
    foreign = foreign_heavy_processes(proc_root=proc_root)
    if heavy and enforce_foreign_processes and foreign:
        rendered = "; ".join(
            f"pid={item['pid']} {shlex.join(item['argv'])}" for item in foreign
        )
        raise PreflightError(f"foreign build/benchmark load is active: {rendered}")

    meminfo = parse_meminfo(proc_root / "meminfo")
    disk = shutil.disk_usage(repo)
    if (
        enforce_clean
        and any(stage.name == "large-fixture-128" for stage in stages)
        and disk.free < LARGE_FIXTURE_MIN_FREE_BYTES
    ):
        raise PreflightError(
            "large-fixture-128 requires at least "
            f"{LARGE_FIXTURE_MIN_FREE_BYTES} free bytes, found {disk.free}"
        )
    nofile_soft, nofile_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    host = {
        "hostname": socket.gethostname(),
        "os": platform.platform(),
        "kernel": platform.release(),
        "machine": platform.machine(),
        "filesystem": filesystem_type(repo),
        "cpu_count": os.cpu_count() or 1,
        "load_average": list(os.getloadavg()) if hasattr(os, "getloadavg") else None,
        "memory_total_bytes": meminfo.get("MemTotal", 0),
        "memory_available_bytes": meminfo.get("MemAvailable", 0),
        "swap_total_bytes": meminfo.get("SwapTotal", 0),
        "swap_free_bytes": meminfo.get("SwapFree", 0),
        "disk_total_bytes": disk.total,
        "disk_free_bytes": disk.free,
        "nofile_soft": nofile_soft,
        "nofile_hard": nofile_hard,
    }
    return {
        "format_version": FORMAT_VERSION,
        "captured_at": utc_now(),
        "candidate": {
            "commit": commit,
            "clean": not bool(status),
            "git_status": status,
            "cargo_lock_sha256": lock_sha,
            "binaries": binaries,
            "configs": configs,
        },
        "toolchain": {
            "rustc": command_output(("rustc", "--version", "--verbose"), repo),
            "cargo": command_output(("cargo", "--version", "--verbose"), repo),
            "python": sys.version,
        },
        "host": host,
        "required_environment": {
            name: "configured" for name in required_environment
        },
        "foreign_heavy_processes": foreign,
    }


def validate_required_environment(
    stages: Sequence[StageSpec],
    environ: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    source = os.environ if environ is None else environ
    required: set[str] = set()
    for stage in stages:
        for spec in stage.commands:
            for index, argument in enumerate(spec.argv[:-1]):
                if argument != "--features":
                    continue
                features = {
                    feature
                    for feature in re.split(r"[\s,]+", spec.argv[index + 1])
                    if feature
                }
                if "prerelease-postgres" in features:
                    required.add(POSTGRES_ORACLE_ENV)
    missing = sorted(name for name in required if not source.get(name, "").strip())
    if missing:
        raise PreflightError(
            "required prerelease environment is missing: " + ", ".join(missing)
        )
    return tuple(sorted(required))


def command(label: str, *argv: str, env: dict[str, str] | None = None) -> CommandSpec:
    return CommandSpec(label=label, argv=tuple(argv), env=tuple(sorted((env or {}).items())))


def cargo_test(
    label: str,
    *,
    features: str | None = None,
    release: bool = False,
    targets: Sequence[str] = (),
    test_filter: str | None = None,
    exact: bool = False,
    env: dict[str, str] | None = None,
    harness_args: bool = True,
) -> CommandSpec:
    argv = ["cargo", "test", "--locked"]
    if release:
        argv.append("--release")
    if features:
        argv.extend(("--features", features))
    argv.extend(targets)
    if test_filter:
        argv.append(test_filter)
    if harness_args:
        argv.append("--")
        if exact:
            argv.append("--exact")
        argv.append("--test-threads=1")
    elif exact or test_filter:
        raise ValueError("test filters and --exact require libtest harness arguments")
    return command(label, *argv, env=env)


def configured_concurrency(clients: int, seed: int) -> StageSpec:
    if clients < 16 or clients & (clients - 1):
        raise PreflightError("--clients must be a power of two and at least 16")
    env = {
        "RADIXDB_PRERELEASE_CLIENTS": str(clients),
        "RADIXDB_PRERELEASE_CONCURRENCY_EVIDENCE": (
            f"{{attempt_dir}}/concurrency-{clients}.json"
        ),
        "RADIXDB_PRERELEASE_SEED": str(seed),
    }
    return StageSpec(
        name=f"concurrency-{clients}",
        commands=(
            cargo_test(
                f"configured concurrency {clients}",
                features="stress-tests",
                targets=("--test", "prerelease_concurrency_test"),
                test_filter="b4_runner_configured_tcp_step_preserves_contracts",
                exact=True,
                env=env,
            ),
        ),
        process_global=False,
    )


def stage_catalog(
    repo: Path,
    profile: str,
    seed: int,
    clients: int,
    duration: str,
    replay: Path | None,
    run_id: str,
    benchmark_root: Path,
) -> dict[str, StageSpec]:
    short = profile == "short"
    duration_seconds = parse_duration(duration)
    stages: dict[str, StageSpec] = {}
    stages["harness"] = StageSpec(
        "harness",
        (
            cargo_test(
                "prerelease harness",
                features="stress-tests",
                targets=("--test", "prerelease_harness_test"),
            ),
        ),
    )
    stages["workspace"] = StageSpec(
        "workspace",
        (
            command("format", "cargo", "fmt", "--all", "--", "--check"),
            command("diff", "git", "diff", "--check"),
            cargo_test(
                "workspace all targets",
                targets=("--workspace", "--all-targets"),
                harness_args=False,
            ),
            cargo_test(
                "workspace no defaults",
                targets=("--workspace", "--all-targets", "--no-default-features"),
                harness_args=False,
            ),
            command(
                "clippy",
                "cargo",
                "clippy",
                "--locked",
                "--workspace",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ),
            cargo_test("doctests", targets=("--workspace", "--doc")),
        ),
    )
    stages["sql-reopen"] = StageSpec(
        "sql-reopen",
        (
            cargo_test("SQLLogicTest", targets=("--test", "sqllogictest_test")),
            cargo_test(
                "file and reopen integrations",
                targets=(
                    "--test",
                    "volume_integration_test",
                    "--test",
                    "join_reopen_cold_test",
                    "--test",
                    "bug_unique_lost_after_reopen_test",
                ),
            ),
        ),
    )
    stages["differential"] = StageSpec(
        "differential",
        (
            cargo_test(
                "SQLite differential",
                features="sqlite",
                targets=("--test", "differential_oracle_test"),
            ),
            cargo_test(
                "PostgreSQL differential",
                features="prerelease-postgres",
                targets=("--test", "prerelease_postgres_oracle_test"),
            ),
            cargo_test(
                "metamorphic",
                features="stress-tests",
                targets=("--test", "metamorphic_test"),
            ),
            cargo_test(
                "execution parity",
                features="stress-tests,test-failpoints",
                targets=("--test", "prerelease_execution_parity_test"),
            ),
        ),
    )
    stages["state-machine"] = StageSpec(
        "state-machine",
        (
            cargo_test(
                "state machine and shrink",
                features="stress-tests",
                targets=("--test", "prerelease_state_machine_test"),
                test_filter=("b3_pre_generated_corpus_covers_all_outcomes_and_validates_model" if short else None),
                exact=short,
                env={"RADIXDB_PRERELEASE_SEED": str(seed)},
            ),
        ),
    )
    stages["races"] = StageSpec(
        "races",
        (
            cargo_test(
                "deterministic race corpus",
                features="stress-tests,test-failpoints",
                targets=("--test", "prerelease_race_test"),
            ),
        ),
    )
    fault_commands = [
        cargo_test(
            "prerelease fault matrix",
            features="stress-tests,test-failpoints",
            targets=("--test", "prerelease_fault_test"),
            test_filter=("b6_manifest_covers_fault_corruption_and_resource_contracts" if short else None),
            exact=short,
        )
    ]
    if not short:
        fault_commands.append(
            cargo_test(
                "I/O failpoints",
                features="test-failpoints",
                targets=("--test", "failpoint_io_test"),
            )
        )
    stages["faults"] = StageSpec(
        "faults", tuple(fault_commands), process_global=True
    )
    stages["core"] = StageSpec(
        "core",
        (
            cargo_test(
                "historical messenger chaos",
                features="stress-tests",
                targets=("--test", "messenger_chaos_test"),
                test_filter="messenger_multi_client_commit_rollback_disconnect_checkpoint_chaos",
                exact=True,
                env={"RADIXDB_PRERELEASE_SEED": str(seed)},
            ),
        ),
    )
    stages["schema-view"] = StageSpec(
        "schema-view",
        (
            cargo_test(
                "schema and VIEW lifecycle",
                features="stress-tests",
                targets=("--test", "prerelease_schema_view_test"),
            ),
        ),
    )
    stages["concurrency"] = configured_concurrency(clients, seed)
    large_fixture_root = repo / "target/prerelease/large-fixtures"
    stages["large-fixture-128"] = StageSpec(
        "large-fixture-128",
        (
            cargo_test(
                "100M messenger fixture with 128 clients",
                features="stress-tests",
                release=True,
                targets=("--test", "prerelease_concurrency_test"),
                test_filter="b4_runner_large_fixture_128_clients_preserves_contracts",
                exact=True,
                env={
                    "RADIXDB_PRERELEASE_CLIENTS": "128",
                    "RADIXDB_PRERELEASE_CONCURRENCY_EVIDENCE": (
                        "{attempt_dir}/large-fixture-128.json"
                    ),
                    "RADIXDB_PRERELEASE_LARGE_FIXTURE": "1",
                    "RADIXDB_PRERELEASE_LARGE_ROOT": str(large_fixture_root),
                    "RADIXDB_PRERELEASE_SEED": str(seed + 128),
                },
            ),
        ),
        process_global=True,
    )
    stages["capacity-boundary-512"] = StageSpec(
        "capacity-boundary-512",
        (
            cargo_test(
                "controlled 512-client capacity boundary",
                features="stress-tests",
                targets=("--test", "prerelease_concurrency_test"),
                test_filter="b4_runner_capacity_boundary_is_controlled_and_reopenable",
                exact=True,
                env={
                    "RADIXDB_PRERELEASE_CLIENTS": "512",
                    "RADIXDB_PRERELEASE_CONCURRENCY_EVIDENCE": (
                        "{attempt_dir}/capacity-boundary-512.json"
                    ),
                },
            ),
        ),
        process_global=True,
    )
    stages["capacity-kill-256"] = StageSpec(
        "capacity-kill-256",
        (
            cargo_test(
                "256-client kill and reopen boundary",
                features="stress-tests",
                targets=("--test", "prerelease_concurrency_test"),
                test_filter="b4_runner_max_capacity_kill_reopen_is_fail_closed",
                exact=True,
                env={"RADIXDB_PRERELEASE_CLIENTS": "256"},
            ),
        ),
        process_global=True,
    )
    recovery_commands: list[CommandSpec] = []
    if not short:
        recovery_commands.append(
            cargo_test(
                "legacy crash soak",
                features="stress-tests",
                targets=("--test", "crash_soak_test"),
            )
        )
    recovery_commands.append(
        cargo_test(
            "recovery matrix",
            features="stress-tests,test-failpoints",
            targets=("--test", "prerelease_recovery_soak_test"),
            test_filter=("b8_resource_slope_detects_leak_and_quiescence_returns_to_corridor" if short else None),
            exact=short,
        )
    )
    stages["recovery"] = StageSpec(
        "recovery", tuple(recovery_commands), process_global=True
    )
    stages["ticket-belt"] = StageSpec(
        "ticket-belt",
        (command("Messenger ticket belt", str(repo / "scripts/run-prerelease-ticket-belt.sh")),),
    )
    mutation_evidence = repo / "target/prerelease" / run_id / "mutation-proof-{attempt}"
    stages["mutation-proof"] = StageSpec(
        "mutation-proof",
        (
            command(
                "invariant mutation proof",
                str(repo / "scripts/run-prerelease-mutation-proof.sh"),
                str(mutation_evidence),
            ),
        ),
        process_global=True,
        evidence_paths=(str(mutation_evidence),),
    )
    stages["soak"] = StageSpec(
        "soak",
        (
            cargo_test(
                "configurable recovery soak",
                features="stress-tests,test-failpoints",
                targets=("--test", "prerelease_recovery_soak_test"),
                test_filter="b8_configurable_soak_profile_runs_transactional_view_workload",
                exact=True,
                env={"RADIXDB_PRERELEASE_SOAK_DURATION_SECS": str(duration_seconds)},
            ),
        ),
        process_global=True,
    )
    stages["release"] = StageSpec(
        "release",
        (
            command("release lifecycle", str(repo / "scripts/check-release-lifecycle.sh")),
            cargo_test(
                "sync async client crate",
                features="tokio",
                targets=("-p", "radixdb-client", "--all-targets"),
                harness_args=False,
            ),
            cargo_test(
                "sync async ORM TCP integration",
                features="orm-async-tests",
                targets=("-p", "radixdb", "--test", "tcp_ddl_transaction_test"),
            ),
        ),
    )
    profile_root = repo / "target/prerelease" / run_id / "profiles/{attempt}"
    profile_evidence = profile_root / f"artifacts-{run_id}"
    stages["profile"] = StageSpec(
        "profile",
        (
            command(
                "release owner profiles",
                str(repo / "scripts/run-prerelease-profile.sh"),
                env={
                    "RADIXDB_B11_ROOT": str(profile_root),
                    "RADIXDB_B11_RUN_TAG": run_id,
                },
            ),
        ),
        process_global=True,
        evidence_paths=(str(profile_evidence),),
    )
    bench = repo / "target/release/radixdb-bench"
    benchmark_commands = tuple(
        command(
            f"100M reopen {ordinal}",
            str(bench),
            "--run",
            "--verify-existing",
            "--scale",
            "100m",
            "--participant",
            "server",
            "--root",
            str(benchmark_root),
            "--expected-checksum",
            "100000000:49734600639880",
            "--read-queue-depth",
            "4",
            "--run-id",
            f"{run_id}-{{attempt}}-100m-r{ordinal}",
            "--allow-large",
        )
        for ordinal in range(1, 6)
    )
    stages["benchmark-100m"] = StageSpec(
        "benchmark-100m",
        benchmark_commands,
        process_global=True,
        evidence_paths=tuple(
            str(benchmark_root / "results" / f"{run_id}-{{attempt}}-100m-r{ordinal}")
            for ordinal in range(1, 6)
        ),
    )
    if replay is not None:
        stages["replay"] = StageSpec(
            "replay",
            (
                cargo_test(
                    "standalone trace replay",
                    features="stress-tests",
                    targets=("--test", "prerelease_state_machine_test"),
                    test_filter="b3_runner_replays_supplied_trace",
                    exact=True,
                    env={"RADIXDB_PRERELEASE_REPLAY_TRACE": str(replay.resolve())},
                ),
            ),
        )
    return stages


def parse_duration(value: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)([smhd])", value)
    if not match:
        raise PreflightError("duration must be a positive integer followed by s, m, h or d")
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return int(match.group(1)) * units[match.group(2)]


def full_plan(catalog: dict[str, StageSpec], seed: int) -> list[StageSpec]:
    names = [
        "workspace",
        "sql-reopen",
        "differential",
        "ticket-belt",
        "state-machine",
        "races",
        "faults",
        "schema-view",
        "core",
    ]
    result = [catalog[name] for name in names]
    result.append(catalog["large-fixture-128"])
    result.extend(
        configured_concurrency(clients, seed + clients)
        for clients in (16, 32, 64, 128, 256)
    )
    result.append(catalog["capacity-boundary-512"])
    result.append(catalog["capacity-kill-256"])
    result.extend(
        catalog[name]
        for name in (
            "recovery",
            "mutation-proof",
            "soak",
            "release",
            "profile",
            "benchmark-100m",
        )
    )
    return result


def short_plan(catalog: dict[str, StageSpec], seed: int) -> list[StageSpec]:
    """Representative runner acceptance; never counts as a B13 full run."""
    result = [
        catalog["harness"],
        catalog["state-machine"],
        catalog["faults"],
        catalog["schema-view"],
        configured_concurrency(16, seed + 16),
        catalog["recovery"],
    ]
    return result


def request_payload(request: RunRequest, stages: Sequence[StageSpec]) -> dict[str, Any]:
    return {
        "run_id": request.run_id,
        "profile": request.profile,
        "seed": request.seed,
        "candidate": request.candidate,
        "repo": str(request.repo.resolve()),
        "artifact_root": str(request.artifact_root.resolve()),
        "binaries": [str(path.expanduser().resolve()) for path in request.binaries],
        "configs": [str(path.expanduser().resolve()) for path in request.configs],
        "selection_args": list(request.selection_args),
        "plan": [stage.as_dict() for stage in stages],
    }


def run_fingerprint(request: RunRequest, stages: Sequence[StageSpec], preflight: dict[str, Any]) -> str:
    stable_preflight = {
        "candidate": preflight["candidate"],
        "toolchain": preflight["toolchain"],
        "host_identity": {
            key: preflight["host"][key]
            for key in ("hostname", "os", "kernel", "machine", "filesystem", "cpu_count")
        },
    }
    return sha256_bytes(canonical_json({"request": request_payload(request, stages), "preflight": stable_preflight}))


def stage_fingerprint(stage: StageSpec, run_hash: str) -> str:
    return sha256_bytes(canonical_json({"run_fingerprint": run_hash, "stage": stage.as_dict()}))


def artifact_inventory(root: Path, relative_paths: Iterable[str]) -> list[dict[str, Any]]:
    inventory: list[dict[str, Any]] = []
    for relative in sorted(relative_paths):
        path = root / relative
        if not path.is_file():
            raise ArtifactIntegrityError(f"completed artifact is missing: {path}")
        inventory.append(
            {"path": relative, "sha256": sha256_file(path), "size_bytes": path.stat().st_size}
        )
    return inventory


def external_artifact_inventory(paths: Iterable[str]) -> list[dict[str, Any]]:
    inventory: list[dict[str, Any]] = []
    for raw_path in sorted(paths):
        root = Path(raw_path).expanduser().resolve()
        if not root.exists():
            raise ArtifactIntegrityError(f"declared external evidence is missing: {root}")
        files = [root] if root.is_file() else sorted(path for path in root.rglob("*") if path.is_file())
        if not files:
            raise ArtifactIntegrityError(f"declared external evidence is empty: {root}")
        inventory.append(
            {
                "root": str(root),
                "files": [
                    {
                        "path": str(path.relative_to(root)) if root.is_dir() else path.name,
                        "sha256": sha256_file(path),
                        "size_bytes": path.stat().st_size,
                    }
                    for path in files
                ],
            }
        )
    return inventory


def validate_completed_stage(run_dir: Path, record: dict[str, Any], expected_hash: str) -> None:
    if record.get("status") != "passed":
        raise ArtifactIntegrityError("resume record is not a completed stage")
    boundary_relative = record.get("boundary")
    boundary_sha = record.get("boundary_sha256")
    if not isinstance(boundary_relative, str) or not isinstance(boundary_sha, str):
        raise ArtifactIntegrityError("completed stage has no immutable boundary")
    boundary_path = run_dir / boundary_relative
    if not boundary_path.is_file() or sha256_file(boundary_path) != boundary_sha:
        raise ArtifactIntegrityError(f"completed stage boundary changed: {boundary_path}")
    boundary = json.loads(boundary_path.read_text(encoding="utf-8"))
    if boundary.get("stage_fingerprint") != expected_hash:
        raise ArtifactIntegrityError("completed stage belongs to another plan or candidate")
    for item in boundary.get("artifacts", []):
        path = run_dir / item["path"]
        if not path.is_file() or sha256_file(path) != item["sha256"]:
            raise ArtifactIntegrityError(f"completed stage artifact changed: {path}")
    for external in boundary.get("external_artifacts", []):
        root = Path(external["root"])
        for item in external["files"]:
            path = root / item["path"] if root.is_dir() else root
            if not path.is_file() or sha256_file(path) != item["sha256"]:
                raise ArtifactIntegrityError(f"completed external artifact changed: {path}")


def next_attempt(stage_root: Path) -> Path:
    existing = [
        int(path.name.removeprefix("attempt-"))
        for path in stage_root.glob("attempt-[0-9][0-9][0-9]")
        if path.name.removeprefix("attempt-").isdigit()
    ]
    ordinal = max(existing, default=0) + 1
    attempt = stage_root / f"attempt-{ordinal:03d}"
    attempt.mkdir(parents=True, exist_ok=False)
    return attempt


def render_attempt(value: str, attempt: Path) -> str:
    return value.replace("{attempt}", attempt.name).replace("{attempt_dir}", str(attempt))


def run_command(spec: CommandSpec, repo: Path, attempt: Path, ordinal: int) -> dict[str, Any]:
    prefix = f"command-{ordinal:02d}"
    log_path = attempt / f"{prefix}.log"
    time_path = attempt / f"{prefix}.time.txt"
    command_path = attempt / f"{prefix}.json"
    env = os.environ.copy()
    rendered_env = {key: render_attempt(value, attempt) for key, value in spec.env}
    rendered_argv = tuple(render_attempt(value, attempt) for value in spec.argv)
    env.update(rendered_env)
    started_at = utc_now()
    started = time.monotonic()
    argv = ["/usr/bin/time", "-v", "-o", str(time_path), *rendered_argv]
    with log_path.open("wb") as log:
        completed = subprocess.run(argv, cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT)
    duration = time.monotonic() - started
    time_metrics: dict[str, str | int] = {}
    if time_path.is_file():
        for line in time_path.read_text(encoding="utf-8", errors="replace").splitlines():
            key, separator, value = line.strip().partition(": ")
            if not separator:
                continue
            if key == "Maximum resident set size (kbytes)":
                try:
                    time_metrics["max_rss_kib"] = int(value)
                except ValueError:
                    time_metrics["max_rss_kib"] = value
            elif key in {
                "User time (seconds)",
                "System time (seconds)",
                "Percent of CPU this job got",
                "File system inputs",
                "File system outputs",
                "Major (requiring I/O) page faults",
                "Minor (reclaiming a frame) page faults",
                "Voluntary context switches",
                "Involuntary context switches",
            }:
                time_metrics[key] = value
    result = {
        "label": spec.label,
        "argv": list(rendered_argv),
        "env": rendered_env,
        "started_at": started_at,
        "finished_at": utc_now(),
        "duration_seconds": round(duration, 6),
        "exit_code": completed.returncode,
        "resource": time_metrics,
        "log": log_path.name,
        "time_report": time_path.name,
    }
    write_json(command_path, result)
    return result


def execute_stage(
    stage: StageSpec,
    repo: Path,
    run_dir: Path,
    stage_index: int,
    stage_hash: str,
) -> dict[str, Any]:
    stage_root = run_dir / "stages" / f"{stage_index:02d}-{stage.name}"
    attempt = next_attempt(stage_root)
    write_json(
        attempt / "STARTED.json",
        {
            "format_version": FORMAT_VERSION,
            "stage": stage.as_dict(),
            "stage_fingerprint": stage_hash,
            "started_at": utc_now(),
        },
    )
    results: list[dict[str, Any]] = []
    try:
        for ordinal, spec in enumerate(stage.commands, start=1):
            result = run_command(spec, repo, attempt, ordinal)
            results.append(result)
            if result["exit_code"] != 0:
                raise StageFailed(
                    f"stage {stage.name}, command {spec.label} exited {result['exit_code']}"
                )
    except BaseException as error:
        write_json(
            attempt / "FAILED.json",
            {
                "format_version": FORMAT_VERSION,
                "stage": stage.name,
                "stage_fingerprint": stage_hash,
                "status": "interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
                "finished_at": utc_now(),
                "error": str(error),
                "commands": results,
            },
        )
        raise

    external_artifacts = external_artifact_inventory(
        render_attempt(path, attempt) for path in stage.evidence_paths
    )
    if external_artifacts:
        write_json(attempt / "external-artifacts.json", external_artifacts)
    relative_files = [str(path.relative_to(run_dir)) for path in attempt.iterdir() if path.is_file()]
    artifacts = artifact_inventory(run_dir, relative_files)
    boundary = {
        "format_version": FORMAT_VERSION,
        "stage": stage.name,
        "stage_fingerprint": stage_hash,
        "status": "passed",
        "finished_at": utc_now(),
        "commands": results,
        "artifacts": artifacts,
        "external_artifacts": external_artifacts,
    }
    boundary_path = attempt / "COMPLETE.json"
    write_json(boundary_path, boundary)
    return {
        "name": stage.name,
        "status": "passed",
        "attempt": attempt.name,
        "boundary": str(boundary_path.relative_to(run_dir)),
        "boundary_sha256": sha256_file(boundary_path),
        "commands": results,
    }


def replay_commands(request: RunRequest, stages: Sequence[StageSpec]) -> list[str]:
    base = [
        "scripts/run-prerelease-verification.sh",
        "--candidate",
        request.candidate or "$(git rev-parse HEAD)",
        "--run-id",
        request.run_id,
        "--profile",
        request.profile,
        "--seed",
        str(request.seed),
        "--artifact-root",
        str(request.artifact_root.resolve()),
    ]
    for binary in request.binaries:
        base.extend(("--binary", str(binary.expanduser().resolve())))
    for config in request.configs:
        base.extend(("--config", str(config.expanduser().resolve())))
    base.extend(request.selection_args)
    base.append("--resume")
    commands = [shlex.join(base)]
    commands.extend(shlex.join(spec.argv) for stage in stages for spec in stage.commands)
    return commands


def markdown_report(manifest: dict[str, Any], report: dict[str, Any]) -> str:
    candidate = manifest["preflight"]["candidate"]
    host = manifest["preflight"]["host"]
    lines = [
        f"# Prerelease verification: {manifest['request']['run_id']}",
        "",
        f"- Status: **{report['status'].upper()}**",
        f"- Candidate: `{candidate['commit']}`",
        f"- Clean worktree: `{candidate['clean']}`",
        f"- Cargo.lock SHA-256: `{candidate['cargo_lock_sha256']}`",
        f"- Profile/seed: `{manifest['request']['profile']}` / `{manifest['request']['seed']}`",
        f"- Host: `{host['hostname']}` / `{host['kernel']}` / `{host['filesystem']}`",
        f"- Memory available: `{host['memory_available_bytes']}` bytes",
        f"- Disk free: `{host['disk_free_bytes']}` bytes",
        "",
        "## Frozen binaries",
        "",
        "| Path | SHA-256 | Build identity |",
        "|---|---|---|",
    ]
    for binary in candidate["binaries"]:
        identity = binary["identity_text"] or "hash-only"
        lines.append(f"| `{binary['path']}` | `{binary['sha256']}` | `{identity}` |")
    lines.extend(("", "## Stages", "", "| Stage | Status | Attempt | Boundary SHA-256 |", "|---|---|---|---|"))
    by_name = {stage["name"]: stage for stage in report["stages"]}
    for planned in manifest["request"]["plan"]:
        record = by_name.get(planned["name"])
        if record is None:
            lines.append(f"| `{planned['name']}` | pending | — | — |")
        else:
            lines.append(
                f"| `{planned['name']}` | {record['status']} | `{record['attempt']}` | "
                f"`{record['boundary_sha256']}` |"
            )
    lines.extend(("", "## Commands", "", "| Stage | Exit | Wall, s | Peak RSS, KiB | Command |", "|---|---:|---:|---:|---|"))
    for record in report["stages"]:
        for command_result in record.get("commands", []):
            peak = command_result.get("resource", {}).get("max_rss_kib", "—")
            rendered = shlex.join(command_result["argv"]).replace("|", "\\|")
            lines.append(
                f"| `{record['name']}` | {command_result['exit_code']} | "
                f"{command_result['duration_seconds']:.3f} | {peak} | `{rendered}` |"
            )
    lines.extend(("", "## Replay", "", "```bash"))
    lines.extend(report["replay_commands"])
    lines.extend(("```", "", "## Integrity", "", f"- Run fingerprint: `{manifest['run_fingerprint']}`"))
    return "\n".join(lines) + "\n"


def create_or_load_run(
    request: RunRequest,
    stages: Sequence[StageSpec],
    preflight: dict[str, Any],
) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    if not IDENTIFIER.fullmatch(request.run_id):
        raise PreflightError("run id must be one portable ASCII filename component")
    artifact_root = request.artifact_root.expanduser().resolve()
    run_dir = artifact_root / request.run_id
    if run_dir.parent != artifact_root:
        raise PreflightError("run directory escaped artifact root")
    payload = request_payload(request, stages)
    fingerprint = run_fingerprint(request, stages, preflight)
    manifest_path = run_dir / "manifest.json"
    report_path = run_dir / "report.json"
    if request.resume:
        if not manifest_path.is_file() or not report_path.is_file():
            raise ArtifactIntegrityError("resume requires existing manifest.json and report.json")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        report = json.loads(report_path.read_text(encoding="utf-8"))
        if manifest.get("run_fingerprint") != fingerprint:
            raise ArtifactIntegrityError("candidate, host identity, config or plan changed since run start")
        if manifest.get("request") != payload:
            raise ArtifactIntegrityError("resume request differs from the original request")
        return run_dir, manifest, report
    if run_dir.exists():
        raise PreflightError(f"run directory already exists; use --resume: {run_dir}")
    run_dir.mkdir(parents=True)
    manifest = {
        "format_version": FORMAT_VERSION,
        "created_at": utc_now(),
        "request": payload,
        "preflight": preflight,
        "run_fingerprint": fingerprint,
    }
    report = {
        "format_version": FORMAT_VERSION,
        "status": "running",
        "started_at": utc_now(),
        "finished_at": None,
        "stages": [],
        "replay_commands": replay_commands(request, stages),
        "failure": None,
    }
    write_json(manifest_path, manifest)
    write_json(report_path, report)
    return run_dir, manifest, report


def execute_run(
    request: RunRequest,
    stages: Sequence[StageSpec],
    *,
    proc_root: Path = Path("/proc"),
    enforce_foreign_processes: bool = True,
) -> Path:
    if not stages:
        raise PreflightError("execution plan is empty")
    if any(stage.process_global and stage.name not in PROCESS_GLOBAL_STAGES for stage in stages):
        unknown = [stage.name for stage in stages if stage.process_global and stage.name not in PROCESS_GLOBAL_STAGES]
        raise PreflightError(f"unregistered process-global stages: {unknown}")
    preflight = collect_preflight(
        request,
        stages,
        proc_root=proc_root,
        enforce_clean=not request.dry_run,
        enforce_foreign_processes=enforce_foreign_processes and not request.dry_run,
    )
    if request.dry_run:
        print(json.dumps({"request": request_payload(request, stages), "preflight": preflight}, indent=2))
        for stage in stages:
            for spec in stage.commands:
                print(f"[{stage.name}] {shlex.join(spec.argv)}")
        return request.artifact_root.expanduser().resolve() / request.run_id

    request.artifact_root.expanduser().resolve().mkdir(parents=True, exist_ok=True)
    git_lock = command_output(
        ("git", "rev-parse", "--path-format=absolute", "--git-path", "prerelease.lock"),
        request.repo.resolve(),
    )
    lock_path = Path(git_lock)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise PreflightError("another prerelease runner owns the global artifact lock") from error
        run_dir, manifest, report = create_or_load_run(request, stages, preflight)
        completed = {record["name"]: record for record in report["stages"] if record.get("status") == "passed"}
        try:
            for index, stage in enumerate(stages, start=1):
                stage_hash = stage_fingerprint(stage, manifest["run_fingerprint"])
                if stage.name in completed:
                    validate_completed_stage(run_dir, completed[stage.name], stage_hash)
                    continue
                record = execute_stage(stage, request.repo.resolve(), run_dir, index, stage_hash)
                report["stages"] = [item for item in report["stages"] if item["name"] != stage.name]
                report["stages"].append(record)
                report["failure"] = None
                write_json(run_dir / "report.json", report)
        except BaseException as error:
            report["status"] = "interrupted" if isinstance(error, KeyboardInterrupt) else "failed"
            report["finished_at"] = utc_now()
            report["failure"] = {"type": type(error).__name__, "message": str(error)}
            write_json(run_dir / "report.json", report)
            atomic_write(run_dir / "REPORT.md", markdown_report(manifest, report).encode("utf-8"))
            raise
        report["status"] = "passed"
        report["finished_at"] = utc_now()
        report["failure"] = None
        write_json(run_dir / "report.json", report)
        atomic_write(run_dir / "REPORT.md", markdown_report(manifest, report).encode("utf-8"))
        return run_dir


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--stage")
    selection.add_argument("--all", action="store_true")
    selection.add_argument("--replay", type=Path)
    parser.add_argument("--profile", choices=("short", "full"), default="full")
    parser.add_argument("--seed", type=int, default=0x5EED_1200)
    clients = parser.add_mutually_exclusive_group()
    clients.add_argument("--clients", type=int, default=16)
    clients.add_argument("--next-power-of-two", action="store_true")
    parser.add_argument("--duration", default="6h")
    parser.add_argument("--candidate")
    parser.add_argument("--run-id")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--binary", type=Path, action="append")
    parser.add_argument("--config", type=Path, action="append")
    parser.add_argument("--benchmark-root", type=Path)
    return parser.parse_args(argv)


def default_paths(args: argparse.Namespace) -> tuple[tuple[Path, ...], tuple[Path, ...], Path, Path]:
    repo = args.repo_root.expanduser().resolve()
    artifact_root = (
        args.artifact_root.expanduser().resolve()
        if args.artifact_root
        else repo / "target/prerelease/runs"
    )
    binaries = tuple(args.binary or (repo / "target/release/radixdb-server", repo / "target/release/radixdb-bench"))
    if args.config:
        configs = tuple(args.config)
    else:
        server_config = repo.parent / "RadixTest/RD_test/server.toml"
        configs = (server_config,) if server_config.is_file() else (repo / "Cargo.toml",)
    benchmark_root = (
        args.benchmark_root.expanduser().resolve()
        if args.benchmark_root
        else repo.parent / "RadixTest"
    )
    return binaries, configs, artifact_root, benchmark_root


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    binaries, configs, artifact_root, benchmark_root = default_paths(args)
    candidate = args.candidate or git_head(args.repo_root.resolve())
    run_id = args.run_id or f"prerelease-{args.profile}-{int(time.time())}"
    clients = 256 if args.next_power_of_two else args.clients
    catalog = stage_catalog(
        args.repo_root.resolve(),
        args.profile,
        args.seed,
        clients,
        args.duration,
        args.replay,
        run_id,
        benchmark_root,
    )
    if args.all:
        stages = short_plan(catalog, args.seed) if args.profile == "short" else full_plan(catalog, args.seed)
        selection_args = (
            "--all",
            "--duration",
            args.duration,
            "--benchmark-root",
            str(benchmark_root),
        )
    elif args.replay is not None:
        if not args.replay.is_file():
            raise PreflightError(f"replay trace is missing: {args.replay}")
        stages = [catalog["replay"]]
        selection_args = ("--replay", str(args.replay.resolve()))
    else:
        assert args.stage is not None
        if args.stage not in catalog:
            raise PreflightError(
                f"unknown stage {args.stage!r}; available: {', '.join(sorted(catalog))}"
            )
        stages = [catalog[args.stage]]
        selection = ["--stage", args.stage]
        if args.stage == "concurrency":
            if args.next_power_of_two:
                selection.append("--next-power-of-two")
            else:
                selection.extend(("--clients", str(args.clients)))
        if args.stage == "soak":
            selection.extend(("--duration", args.duration))
        selection_args = tuple(selection)
    request = RunRequest(
        repo=args.repo_root,
        artifact_root=artifact_root,
        run_id=run_id,
        candidate=candidate,
        profile=args.profile,
        seed=args.seed,
        binaries=binaries,
        configs=configs,
        selection_args=selection_args,
        resume=args.resume,
        dry_run=args.dry_run,
    )
    run_dir = execute_run(request, stages)
    if args.dry_run:
        print("dry-run: no artifact directory or database fixture was created")
    else:
        print(f"prerelease run passed: {run_dir}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PrereleaseError as error:
        raise SystemExit(f"prerelease verification refused: {error}") from error
