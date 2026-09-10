from __future__ import annotations

import contextlib
import dataclasses
import fcntl
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "prerelease_verification.py"
SPEC = importlib.util.spec_from_file_location("prerelease_verification", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class PrereleaseRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        subprocess.run(("git", "init", "-q"), cwd=self.repo, check=True)
        (self.repo / "Cargo.lock").write_text("runner-lock\n", encoding="utf-8")
        (self.repo / "README.md").write_text("fixture\n", encoding="utf-8")
        subprocess.run(("git", "add", "."), cwd=self.repo, check=True)
        subprocess.run(
            (
                "git",
                "-c",
                "user.name=Prerelease Test",
                "-c",
                "user.email=prerelease@example.invalid",
                "commit",
                "-qm",
                "fixture",
            ),
            cwd=self.repo,
            check=True,
        )
        self.commit = subprocess.run(
            ("git", "rev-parse", "HEAD"),
            cwd=self.repo,
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        ).stdout.strip()
        self.lock_sha = MODULE.sha256_file(self.repo / "Cargo.lock")
        self.binary = self.root / "radixdb-server"
        self.binary.write_text(
            "#!/usr/bin/env sh\n"
            f"echo 'radixdb-server 0.5.2 git={self.commit} protocol=14 "
            f"profile=release target=test-target lock={self.lock_sha}'\n",
            encoding="utf-8",
        )
        self.binary.chmod(0o755)
        self.config = self.root / "server.toml"
        self.config.write_text("port = 0\n", encoding="utf-8")
        self.proc = self.root / "proc"
        self.proc.mkdir()
        (self.proc / "meminfo").write_text(
            "MemTotal: 32768 kB\nMemAvailable: 24576 kB\n"
            "SwapTotal: 4096 kB\nSwapFree: 4096 kB\n",
            encoding="utf-8",
        )
        self.artifacts = self.root / "artifacts"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def request(self, run_id: str, **changes: object) -> MODULE.RunRequest:
        request = MODULE.RunRequest(
            repo=self.repo,
            artifact_root=self.artifacts,
            run_id=run_id,
            candidate=self.commit,
            profile="short",
            seed=12,
            binaries=(self.binary,),
            configs=(self.config,),
            selection_args=("--stage", "harness"),
        )
        return dataclasses.replace(request, **changes)

    @staticmethod
    def stage(name: str, code: str) -> MODULE.StageSpec:
        return MODULE.StageSpec(
            name=name,
            commands=(MODULE.command(name, sys.executable, "-c", code),),
            heavy=False,
        )

    def test_dry_run_prints_full_plan_without_creating_artifacts(self) -> None:
        request = self.request("dry-run", dry_run=True)
        stages = [self.stage("first", "print('one')"), self.stage("second", "print('two')")]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            MODULE.execute_run(
                request,
                stages,
                proc_root=self.proc,
                enforce_foreign_processes=False,
            )
        rendered = output.getvalue()
        self.assertIn("[first]", rendered)
        self.assertIn("[second]", rendered)
        self.assertIn(self.commit, rendered)
        self.assertFalse(self.artifacts.exists())

    def test_interrupted_run_resumes_only_after_completed_boundary(self) -> None:
        order = self.root / "order.txt"
        marker = self.root / "fail"
        marker.write_text("fail\n", encoding="utf-8")
        stage_one = self.stage(
            "one",
            f"from pathlib import Path; p=Path({str(order)!r}); "
            "p.write_text(p.read_text() + 'one\\n' if p.exists() else 'one\\n')",
        )
        stage_two = self.stage(
            "two",
            f"import sys; from pathlib import Path; m=Path({str(marker)!r}); "
            f"p=Path({str(order)!r}); sys.exit(23) if m.exists() else "
            "p.write_text(p.read_text() + 'two\\n')",
        )
        request = self.request("resume")
        with self.assertRaises(MODULE.StageFailed):
            MODULE.execute_run(
                request,
                [stage_one, stage_two],
                proc_root=self.proc,
                enforce_foreign_processes=False,
            )
        self.assertEqual(order.read_text(encoding="utf-8"), "one\n")
        marker.unlink()
        run_dir = MODULE.execute_run(
            dataclasses.replace(request, resume=True),
            [stage_one, stage_two],
            proc_root=self.proc,
            enforce_foreign_processes=False,
        )
        self.assertEqual(order.read_text(encoding="utf-8"), "one\ntwo\n")
        self.assertEqual(len(list((run_dir / "stages/01-one").glob("attempt-*"))), 1)
        self.assertEqual(len(list((run_dir / "stages/02-two").glob("attempt-*"))), 2)
        report = json.loads((run_dir / "report.json").read_text(encoding="utf-8"))
        self.assertEqual(report["status"], "passed")

    def test_failed_external_stage_restarts_with_a_fresh_attempt_namespace(self) -> None:
        marker = self.root / "external-fail"
        marker.write_text("fail\n", encoding="utf-8")
        output_template = str(self.root / "outputs/{attempt}")
        code = (
            "import sys; from pathlib import Path; "
            f"out=Path({output_template!r}); out.mkdir(parents=True); "
            "(out/'result.txt').write_text(out.name); "
            f"sys.exit(19) if Path({str(marker)!r}).exists() else None"
        )
        stage = dataclasses.replace(
            self.stage("external", code), evidence_paths=(output_template,)
        )
        request = self.request("external-retry")
        with self.assertRaises(MODULE.StageFailed):
            MODULE.execute_run(
                request,
                [stage],
                proc_root=self.proc,
                enforce_foreign_processes=False,
            )
        marker.unlink()
        MODULE.execute_run(
            dataclasses.replace(request, resume=True),
            [stage],
            proc_root=self.proc,
            enforce_foreign_processes=False,
        )
        self.assertEqual(
            (self.root / "outputs/attempt-001/result.txt").read_text(encoding="utf-8"),
            "attempt-001",
        )
        self.assertEqual(
            (self.root / "outputs/attempt-002/result.txt").read_text(encoding="utf-8"),
            "attempt-002",
        )

    def test_resume_rejects_changed_completed_artifact(self) -> None:
        request = self.request("tamper")
        stage = self.stage("complete", "print('immutable')")
        run_dir = MODULE.execute_run(
            request,
            [stage],
            proc_root=self.proc,
            enforce_foreign_processes=False,
        )
        log = next((run_dir / "stages/01-complete/attempt-001").glob("command-*.log"))
        log.write_text("tampered\n", encoding="utf-8")
        with self.assertRaises(MODULE.ArtifactIntegrityError):
            MODULE.execute_run(
                dataclasses.replace(request, resume=True),
                [stage],
                proc_root=self.proc,
                enforce_foreign_processes=False,
            )

    def test_resume_rejects_changed_external_stage_evidence(self) -> None:
        evidence = self.root / "external-evidence"
        evidence.mkdir()
        payload = evidence / "result.json"
        payload.write_text('{"status":"pass"}\n', encoding="utf-8")
        request = self.request("external-tamper")
        stage = dataclasses.replace(
            self.stage("complete", "print('immutable')"),
            evidence_paths=(str(evidence),),
        )
        MODULE.execute_run(
            request,
            [stage],
            proc_root=self.proc,
            enforce_foreign_processes=False,
        )
        payload.write_text('{"status":"changed"}\n', encoding="utf-8")
        with self.assertRaises(MODULE.ArtifactIntegrityError):
            MODULE.execute_run(
                dataclasses.replace(request, resume=True),
                [stage],
                proc_root=self.proc,
                enforce_foreign_processes=False,
            )

    def test_short_profile_report_contains_mandatory_candidate_host_and_replay(self) -> None:
        request = self.request("report")
        stages = [self.stage("alpha", "print('alpha')"), self.stage("beta", "print('beta')")]
        run_dir = MODULE.execute_run(
            request,
            stages,
            proc_root=self.proc,
            enforce_foreign_processes=False,
        )
        manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
        report = json.loads((run_dir / "report.json").read_text(encoding="utf-8"))
        markdown = (run_dir / "REPORT.md").read_text(encoding="utf-8")
        self.assertEqual(manifest["preflight"]["candidate"]["commit"], self.commit)
        self.assertTrue(manifest["preflight"]["candidate"]["clean"])
        self.assertEqual(
            manifest["preflight"]["candidate"]["cargo_lock_sha256"], self.lock_sha
        )
        self.assertIn("memory_available_bytes", manifest["preflight"]["host"])
        self.assertEqual(report["status"], "passed")
        self.assertEqual([stage["name"] for stage in report["stages"]], ["alpha", "beta"])
        self.assertEqual(report["stages"][0]["commands"][0]["exit_code"], 0)
        self.assertGreaterEqual(report["stages"][0]["commands"][0]["duration_seconds"], 0.0)
        self.assertIn("max_rss_kib", report["stages"][0]["commands"][0]["resource"])
        self.assertTrue(report["replay_commands"])
        self.assertIn("--stage harness", report["replay_commands"][0])
        self.assertIn("## Frozen binaries", markdown)
        self.assertIn("## Stages", markdown)
        self.assertIn("## Commands", markdown)
        self.assertIn("## Replay", markdown)
        self.assertIn("## Integrity", markdown)

    def test_foreign_heavy_process_detection_is_fail_closed(self) -> None:
        process = self.proc / "4242"
        process.mkdir()
        (process / "cmdline").write_bytes(b"/usr/bin/cargo\0test\0--workspace\0")
        detected = MODULE.foreign_heavy_processes(self.proc, excluded_pids=set())
        self.assertEqual(detected[0]["pid"], 4242)
        self.assertIn("cargo", detected[0]["argv"][0])
        with self.assertRaises(MODULE.PreflightError):
            MODULE.collect_preflight(
                self.request("foreign-load"),
                [dataclasses.replace(self.stage("heavy", "print('x')"), heavy=True)],
                proc_root=self.proc,
            )

    def test_repository_global_lock_forbids_a_second_runner(self) -> None:
        lock_path = self.repo / ".git/prerelease.lock"
        with lock_path.open("a+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(MODULE.PreflightError):
                MODULE.execute_run(
                    self.request("locked"),
                    [self.stage("stage", "print('x')")],
                    proc_root=self.proc,
                    enforce_foreign_processes=False,
                )
        self.assertFalse((self.artifacts / "locked").exists())

    def test_binary_identity_must_match_clean_candidate(self) -> None:
        self.binary.write_text(
            "#!/usr/bin/env sh\n"
            f"echo 'radixdb-server 0.5.2 git={'0' * 40} protocol=14 "
            f"profile=release target=test-target lock={self.lock_sha}'\n",
            encoding="utf-8",
        )
        self.binary.chmod(0o755)
        with self.assertRaises(MODULE.PreflightError):
            MODULE.collect_preflight(
                self.request("stale"),
                [self.stage("stage", "print('x')")],
                proc_root=self.proc,
                enforce_foreign_processes=False,
            )

    def test_replay_cli_routes_the_supplied_trace_to_the_standalone_owner(self) -> None:
        trace = self.root / "replay/trace.json"
        trace.parent.mkdir()
        trace.write_text("{}\n", encoding="utf-8")
        catalog = MODULE.stage_catalog(
            self.repo,
            "short",
            12,
            16,
            "30s",
            trace,
            "replay-run",
            self.root / "benchmark",
        )
        replay = catalog["replay"].commands[0]
        self.assertEqual(
            dict(replay.env)["RADIXDB_PRERELEASE_REPLAY_TRACE"], str(trace.resolve())
        )
        self.assertIn("b3_runner_replays_supplied_trace", replay.argv)

    def test_all_target_commands_do_not_forward_libtest_flags_to_benches(self) -> None:
        catalog = MODULE.stage_catalog(
            self.repo,
            "full",
            12,
            16,
            "6h",
            None,
            "full-run",
            self.root / "benchmark",
        )
        commands = catalog["workspace"].commands
        all_targets = [
            command
            for command in commands
            if command.argv[:2] == ("cargo", "test") and "--all-targets" in command.argv
        ]
        self.assertEqual(len(all_targets), 2)
        for command in all_targets:
            self.assertNotIn("--test-threads=1", command.argv)
            self.assertNotIn("--", command.argv)

    def test_postgres_oracle_is_required_before_a_selected_run_starts(self) -> None:
        stage = MODULE.StageSpec(
            name="differential",
            commands=(
                MODULE.cargo_test(
                    "PostgreSQL differential",
                    features="sqlite,prerelease-postgres",
                    targets=("--test", "prerelease_postgres_oracle_test"),
                ),
            ),
        )
        with self.assertRaisesRegex(
            MODULE.PreflightError, MODULE.POSTGRES_ORACLE_ENV
        ):
            MODULE.validate_required_environment([stage], {})
        self.assertEqual(
            MODULE.validate_required_environment(
                [stage], {MODULE.POSTGRES_ORACLE_ENV: "postgresql://oracle"}
            ),
            (MODULE.POSTGRES_ORACLE_ENV,),
        )
        with mock.patch.dict(
            os.environ, {MODULE.POSTGRES_ORACLE_ENV: "postgresql://oracle"}
        ):
            preflight = MODULE.collect_preflight(
                self.request("postgres-configured"),
                [stage],
                proc_root=self.proc,
                enforce_foreign_processes=False,
            )
        self.assertEqual(
            preflight["required_environment"],
            {MODULE.POSTGRES_ORACLE_ENV: "configured"},
        )
        self.assertNotIn("postgresql://oracle", json.dumps(preflight))

    def test_postgres_oracle_runs_before_the_long_metamorphic_gate(self) -> None:
        catalog = MODULE.stage_catalog(
            self.repo,
            "full",
            12,
            16,
            "6h",
            None,
            "full-run",
            self.root / "benchmark",
        )
        labels = [command.label for command in catalog["differential"].commands]
        self.assertLess(
            labels.index("PostgreSQL differential"), labels.index("metamorphic")
        )

    def test_release_routes_features_to_the_package_that_owns_them(self) -> None:
        catalog = MODULE.stage_catalog(
            self.repo,
            "full",
            12,
            16,
            "6h",
            None,
            "full-run",
            self.root / "benchmark",
        )
        client, integration = catalog["release"].commands[1:]

        self.assertIn("tokio", client.argv)
        self.assertIn("radixdb-client", client.argv)
        self.assertNotIn("orm-async-tests", client.argv)
        self.assertIn("orm-async-tests", integration.argv)
        self.assertIn("radixdb", integration.argv)
        self.assertIn("tcp_ddl_transaction_test", integration.argv)

    def test_full_plan_contains_large_fixture_extended_ladder_and_capacity_boundary(self) -> None:
        catalog = MODULE.stage_catalog(
            self.repo,
            "full",
            12,
            16,
            "6h",
            None,
            "full-run",
            self.root / "benchmark",
        )
        plan = MODULE.full_plan(catalog, 12)
        names = [stage.name for stage in plan]
        self.assertIn("large-fixture-128", names)
        for clients in (16, 32, 64, 128, 256):
            self.assertIn(f"concurrency-{clients}", names)
        self.assertNotIn("concurrency-512", names)
        self.assertIn("capacity-boundary-512", names)
        self.assertIn("capacity-kill-256", names)
        self.assertLess(names.index("large-fixture-128"), names.index("concurrency-16"))
        self.assertLess(names.index("concurrency-256"), names.index("capacity-boundary-512"))
        self.assertLess(names.index("capacity-boundary-512"), names.index("capacity-kill-256"))

        unregistered_process_global = [
            stage.name
            for stage in plan
            if stage.process_global
            and stage.name not in MODULE.PROCESS_GLOBAL_STAGES
        ]
        self.assertEqual(unregistered_process_global, [])

        large_env = dict(catalog["large-fixture-128"].commands[0].env)
        self.assertIn("--release", catalog["large-fixture-128"].commands[0].argv)
        self.assertEqual(large_env["RADIXDB_PRERELEASE_LARGE_FIXTURE"], "1")
        self.assertIn("{attempt_dir}", large_env["RADIXDB_PRERELEASE_CONCURRENCY_EVIDENCE"])
        for clients in (16, 32, 64, 128, 256):
            stage = next(stage for stage in plan if stage.name == f"concurrency-{clients}")
            evidence = dict(stage.commands[0].env)[
                "RADIXDB_PRERELEASE_CONCURRENCY_EVIDENCE"
            ]
            self.assertEqual(evidence, f"{{attempt_dir}}/concurrency-{clients}.json")


if __name__ == "__main__":
    unittest.main()
