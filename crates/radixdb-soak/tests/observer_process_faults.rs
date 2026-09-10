#![cfg(unix)]

use std::{
    fs,
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use radixdb_soak::diagnostics::{AgentTelemetryClient, SemanticProgressTracker};

#[derive(Clone, Copy, Debug)]
enum FaultTarget {
    Observer,
    Agent,
    Server,
}

struct FaultScenario {
    _directory: tempfile::TempDir,
    run_dir: PathBuf,
    socket: PathBuf,
    observer_executable: PathBuf,
    agent_executable: PathBuf,
    server_executable: PathBuf,
    config: PathBuf,
    observer: Child,
    agent: Child,
    server: Child,
    telemetry: AgentTelemetryClient,
    progress: SemanticProgressTracker,
}

impl FaultScenario {
    fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let bin = root.join("bin");
        let runs = root.join("runs");
        let run_dir = runs.join("fault-gate");
        let data = root.join("data");
        fs::create_dir(&bin).unwrap();
        fs::create_dir_all(&run_dir).unwrap();
        fs::create_dir(&data).unwrap();

        let observer_executable = bin.join("radixdb-soak-observer");
        let agent_executable = bin.join("radixdb-soak");
        let server_executable = bin.join("radixdb-server");
        copy_executable(
            Path::new(env!("CARGO_BIN_EXE_radixdb-soak-observer")),
            &observer_executable,
        );
        copy_executable(Path::new("/bin/sleep"), &agent_executable);
        copy_executable(Path::new("/bin/sleep"), &server_executable);

        let auth = root.join("auth.env");
        fs::write(
            &auth,
            "RADIXDB_SOAK_HTTP_USER=observer\nRADIXDB_SOAK_HTTP_PASSWORD=secret\n",
        )
        .unwrap();
        fs::set_permissions(&auth, fs::Permissions::from_mode(0o600)).unwrap();
        let socket = root.join("agent.sock");
        let config = root.join("soak.toml");
        fs::write(
            &config,
            format!(
                r#"format = 1
profile = "smoke"
duration = "10m"
seed = 1
[database]
address = "127.0.0.1:1"
name = "observer_fault_gate"
connect_timeout_secs = 1
read_timeout_secs = 1
write_timeout_secs = 1
[status]
bind = "127.0.0.1:0"
auth_file = "{}"
[artifacts]
root = "{}"
[monitor]
server_executable = "{}"
data_dir = "{}"
[diagnostics]
enabled = true
bind = "127.0.0.1:0"
auth_file = "{}"
channel_path = "{}"
normal_interval = "1s"
alert_interval = "250ms"
burst_interval = "100ms"
burst_duration = "1s"
history_window = "2s"
history_max_samples = 16
artifact_quota_bytes = 67108864
max_incidents = 16
engine_snapshot_interval = "1s"
engine_snapshot_timeout = "20ms"
smart_interval = "30m"
workload_stall_timeout = "5s"
agent_heartbeat_timeout = "5s"
[load]
client_steps = [2]
active_rows = 1
checkpoint_interval = "30s"
sample_interval = "1s"
invariant_interval = "5s"
"#,
                auth.display(),
                runs.display(),
                server_executable.display(),
                data.display(),
                auth.display(),
                socket.display(),
            ),
        )
        .unwrap();

        let agent = spawn_sleep(&agent_executable);
        let server = spawn_sleep(&server_executable);
        let observer = spawn_observer(&observer_executable, &config);
        wait_until(
            Duration::from_secs(5),
            || socket.exists(),
            "observer socket",
        );
        let telemetry = connect_telemetry(&socket);
        let progress = SemanticProgressTracker::new(unix_millis(), "clients-2").unwrap();
        let mut scenario = Self {
            _directory: directory,
            run_dir,
            socket,
            observer_executable,
            agent_executable,
            server_executable,
            config,
            observer,
            agent,
            server,
            telemetry,
            progress,
        };
        scenario.publish_telemetry();
        wait_until(
            Duration::from_secs(5),
            || jsonl_lines(&scenario.run_dir.join("diagnostic-frames.jsonl")) >= 2,
            "initial observer samples",
        );
        scenario
    }

    fn publish_telemetry(&mut self) {
        self.telemetry
            .publish(unix_millis(), self.progress.snapshot());
    }

    fn signal(&mut self, target: FaultTarget, signal: libc::c_int) {
        let pid = match target {
            FaultTarget::Observer => self.observer.id(),
            FaultTarget::Agent => self.agent.id(),
            FaultTarget::Server => self.server.id(),
        };
        // SAFETY: the PID belongs to a child owned by this test and the signal
        // is one of SIGSTOP/SIGCONT/SIGKILL/SIGTERM.
        assert_eq!(unsafe { libc::kill(pid as libc::pid_t, signal) }, 0);
    }

    fn replace_killed(&mut self, target: FaultTarget) {
        match target {
            FaultTarget::Observer => {
                let status = self.observer.wait().unwrap();
                assert_eq!(status.signal(), Some(libc::SIGKILL));
                self.observer = spawn_observer(&self.observer_executable, &self.config);
                wait_until(
                    Duration::from_secs(5),
                    || self.socket.exists(),
                    "rebound observer socket",
                );
                self.telemetry = connect_telemetry(&self.socket);
                self.publish_telemetry();
            }
            FaultTarget::Agent => {
                let status = self.agent.wait().unwrap();
                assert_eq!(status.signal(), Some(libc::SIGKILL));
                self.agent = spawn_sleep(&self.agent_executable);
                self.publish_telemetry();
            }
            FaultTarget::Server => {
                let status = self.server.wait().unwrap();
                assert_eq!(status.signal(), Some(libc::SIGKILL));
                self.server = spawn_sleep(&self.server_executable);
            }
        }
    }

    fn finish(mut self, kind: &str) {
        terminate(&mut self.observer, libc::SIGTERM);
        terminate(&mut self.agent, libc::SIGKILL);
        terminate(&mut self.server, libc::SIGKILL);

        let incident = incident_with_alert(&self.run_dir, kind)
            .unwrap_or_else(|| panic!("missing incident for {kind}"));
        for required in [
            "incident.json",
            "alerts.jsonl",
            "pre-trigger-samples.jsonl",
            "host.json",
            "process.json",
            "disk.json",
            "cgroup.json",
            "filesystem.json",
            "network.json",
            "SHA256SUMS",
        ] {
            assert!(incident.join(required).is_file(), "missing {required}");
        }
        let metadata: serde_json::Value =
            serde_json::from_slice(&fs::read(incident.join("incident.json")).unwrap()).unwrap();
        let missing = metadata["missing_evidence"].as_array().unwrap();
        for optional in [
            "engine-before.json",
            "engine-after.json",
            "threads.json",
            "threads-after.json",
            "journal.txt",
            "smart.json",
        ] {
            assert!(
                incident.join(optional).is_file()
                    || missing.iter().any(|entry| {
                        entry
                            .as_str()
                            .is_some_and(|entry| entry == optional || entry.starts_with(optional))
                    }),
                "{optional} is neither captured nor explicitly missing"
            );
        }
        assert!(Command::new("sha256sum")
            .args(["-c", "SHA256SUMS"])
            .current_dir(&incident)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success());
    }
}

impl Drop for FaultScenario {
    fn drop(&mut self) {
        let _ = unsafe { libc::kill(self.observer.id() as libc::pid_t, libc::SIGCONT) };
        let _ = unsafe { libc::kill(self.agent.id() as libc::pid_t, libc::SIGCONT) };
        let _ = unsafe { libc::kill(self.server.id() as libc::pid_t, libc::SIGCONT) };
        let _ = self.observer.kill();
        let _ = self.agent.kill();
        let _ = self.server.kill();
        let _ = self.observer.wait();
        let _ = self.agent.wait();
        let _ = self.server.wait();
    }
}

#[test]
fn sigstop_and_sigkill_each_soak_process_leave_checksummed_incidents() {
    for (target, kind) in [
        (FaultTarget::Observer, "observer_failure"),
        (FaultTarget::Agent, "agent_failure"),
        (FaultTarget::Server, "server_failure"),
    ] {
        eprintln!("fault gate: {target:?} SIGSTOP");
        let mut scenario = FaultScenario::start();
        scenario.signal(target, libc::SIGSTOP);
        if matches!(target, FaultTarget::Observer) {
            thread::sleep(Duration::from_millis(3_500));
            scenario.signal(target, libc::SIGCONT);
        } else {
            wait_for_alert(&scenario.run_dir, kind, "active");
            scenario.signal(target, libc::SIGCONT);
        }
        scenario.publish_telemetry();
        wait_for_alert(&scenario.run_dir, kind, "active");
        wait_for_alert(&scenario.run_dir, kind, "recovered");
        scenario.finish(kind);

        eprintln!("fault gate: {target:?} SIGKILL");
        let mut scenario = FaultScenario::start();
        scenario.signal(target, libc::SIGKILL);
        if matches!(target, FaultTarget::Observer) {
            thread::sleep(Duration::from_millis(3_500));
            scenario.replace_killed(target);
        } else {
            wait_for_alert(&scenario.run_dir, kind, "active");
            scenario.replace_killed(target);
        }
        wait_for_alert(&scenario.run_dir, kind, "active");
        wait_for_alert(&scenario.run_dir, kind, "recovered");
        scenario.finish(kind);
    }
}

fn copy_executable(source: &Path, target: &Path) {
    fs::copy(source, target).unwrap();
    fs::set_permissions(target, fs::Permissions::from_mode(0o755)).unwrap();
}

fn spawn_sleep(executable: &Path) -> Child {
    Command::new(executable)
        .arg("120")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

fn spawn_observer(executable: &Path, config: &Path) -> Child {
    Command::new(executable)
        .args([
            "--config",
            config.to_str().unwrap(),
            "--run-id",
            "fault-gate",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
}

fn connect_telemetry(socket: &Path) -> AgentTelemetryClient {
    let mut result = None;
    wait_until(
        Duration::from_secs(5),
        || match AgentTelemetryClient::connect(socket, "fault-gate") {
            Ok(client) => {
                result = Some(client);
                true
            }
            Err(_) => false,
        },
        "agent telemetry connection",
    );
    result.unwrap()
}

fn wait_for_alert(run_dir: &Path, kind: &str, state: &str) {
    let started = Instant::now();
    loop {
        let found = fs::read_dir(run_dir.join("incidents"))
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .any(|entry| {
                fs::read_to_string(entry.path().join("alerts.jsonl"))
                    .unwrap_or_default()
                    .lines()
                    .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                    .any(|alert| alert["kind"] == kind && alert["state"] == state)
            });
        if found {
            return;
        }
        if started.elapsed() >= Duration::from_secs(8) {
            let alerts = fs::read_dir(run_dir.join("incidents"))
                .ok()
                .into_iter()
                .flatten()
                .filter_map(Result::ok)
                .map(|entry| {
                    fs::read_to_string(entry.path().join("alerts.jsonl"))
                        .unwrap_or_else(|error| format!("read alerts: {error}"))
                })
                .collect::<Vec<_>>()
                .join("\n");
            panic!("timed out waiting for {kind}/{state}; alerts:\n{alerts}");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn incident_with_alert(run_dir: &Path, kind: &str) -> Option<PathBuf> {
    fs::read_dir(run_dir.join("incidents"))
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            fs::read_to_string(path.join("alerts.jsonl"))
                .unwrap_or_default()
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .any(|alert| alert["kind"] == kind)
        })
}

fn jsonl_lines(path: &Path) -> usize {
    fs::read_to_string(path)
        .map(|contents| contents.lines().count())
        .unwrap_or(0)
}

fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool, description: &str) {
    let started = Instant::now();
    while !condition() {
        assert!(
            started.elapsed() < timeout,
            "timed out waiting for {description}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn terminate(child: &mut Child, signal: libc::c_int) {
    if child.try_wait().unwrap().is_some() {
        return;
    }
    // SAFETY: the PID belongs to this test's child process.
    let _ = unsafe { libc::kill(child.id() as libc::pid_t, signal) };
    let started = Instant::now();
    while child.try_wait().unwrap().is_none() {
        if started.elapsed() >= Duration::from_secs(10) {
            child.kill().unwrap();
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let _ = child.wait();
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
