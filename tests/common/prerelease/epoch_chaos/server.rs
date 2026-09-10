use std::{
    fs::{self, File, OpenOptions},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use radixdb::server::{
    default_copy_max_transaction_bytes, default_max_database_name_bytes, default_max_databases,
    default_max_inflight_frame_bytes, default_page_cache_level, default_page_cache_max_bytes,
    default_page_cache_memory_reserve, default_storage_cpu_workers, ServerConfig, ServerConfigFile,
};
use radixdb_client::Connection;

use super::{config::DATABASE, telemetry::Telemetry};

const CRASH_POINT_ENV: &str = "RADIXDB_PRERELEASE_CRASH_POINT";
const CRASH_READY_ENV: &str = "RADIXDB_PRERELEASE_CRASH_READY";

pub struct ServerSupervisor<'a> {
    run_root: PathBuf,
    data_root: PathBuf,
    telemetry: &'a Telemetry,
    child: Option<Child>,
    address: Option<SocketAddr>,
    crash_ready: Option<PathBuf>,
    generation: u64,
}

impl<'a> ServerSupervisor<'a> {
    pub fn new(run_root: PathBuf, telemetry: &'a Telemetry) -> Result<Self, String> {
        let data_root = run_root.join("server-data");
        fs::create_dir_all(&data_root).map_err(|error| error.to_string())?;
        Ok(Self {
            run_root,
            data_root,
            telemetry,
            child: None,
            address: None,
            crash_ready: None,
            generation: 0,
        })
    }

    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

    pub fn address(&self) -> Result<SocketAddr, String> {
        self.address
            .ok_or_else(|| "epoch chaos server is not running".to_string())
    }

    pub fn pid(&self) -> Result<u32, String> {
        self.child
            .as_ref()
            .map(std::process::Child::id)
            .ok_or_else(|| "epoch chaos server is not running".to_string())
    }

    pub fn start(&mut self, crash_point: Option<&str>) -> Result<SocketAddr, String> {
        if self.child.is_some() {
            return Err("epoch chaos server is already running".to_string());
        }
        self.generation += 1;
        let port = reserve_port()?;
        let config = server_config(self.data_root.clone(), port);
        let config_path = self
            .run_root
            .join(format!("server-{:03}.toml", self.generation));
        let config_source = toml::to_string_pretty(&ServerConfigFile {
            server: config,
            plugins: Default::default(),
        })
        .map_err(|error| error.to_string())?;
        fs::write(&config_path, config_source).map_err(|error| error.to_string())?;

        let log_path = self
            .run_root
            .join(format!("server-{:03}.log", self.generation));
        let log = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&log_path)
            .map_err(|error| error.to_string())?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_radixdb-server"));
        command
            .arg("--config")
            .arg(&config_path)
            .current_dir(&self.run_root)
            .stdout(Stdio::from(
                log.try_clone().map_err(|error| error.to_string())?,
            ))
            .stderr(Stdio::from(log));

        self.crash_ready = if let Some(point) = crash_point {
            let ready = self
                .run_root
                .join(format!("crash-{:03}-{point}.ready", self.generation));
            if ready.exists() {
                return Err(format!("stale crash barrier exists: {}", ready.display()));
            }
            command
                .env(CRASH_POINT_ENV, point)
                .env(CRASH_READY_ENV, &ready);
            Some(ready)
        } else {
            None
        };

        let child = command.spawn().map_err(|error| error.to_string())?;
        self.telemetry.set_pid(child.id());
        self.child = Some(child);
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        self.address = Some(address);
        self.wait_listening(Duration::from_secs(30))?;
        Ok(address)
    }

    pub fn connect(&mut self, timeout: Duration) -> Result<Connection, String> {
        let address = self.address()?;
        let deadline = Instant::now() + timeout;
        loop {
            self.ensure_running()?;
            match super::super::tcp_connect_with_read_timeout(address, DATABASE, timeout) {
                Ok(connection) => return Ok(connection),
                Err(error)
                    if Instant::now() < deadline
                        && (error.contains("opening/recovering")
                            || error.contains("Connection refused")
                            || error.contains("connect")) =>
                {
                    thread::sleep(Duration::from_millis(100));
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn stop_graceful(&mut self, timeout: Duration) -> Result<ExitStatus, String> {
        let pid = self
            .child
            .as_ref()
            .ok_or_else(|| "epoch chaos server is not running".to_string())?
            .id();
        let signal_result = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
        if signal_result != 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        let status = self.wait_exit(timeout)?;
        if !status.success() {
            return Err(format!("server did not stop cleanly: {status}"));
        }
        Ok(status)
    }

    pub fn wait_barrier_and_kill(&mut self, timeout: Duration) -> Result<ExitStatus, String> {
        let ready = self
            .crash_ready
            .clone()
            .ok_or_else(|| "server has no configured crash barrier".to_string())?;
        let deadline = Instant::now() + timeout;
        while !ready.exists() {
            self.ensure_running()?;
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for crash barrier {}",
                    ready.display()
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
        File::open(&ready)
            .and_then(|file| file.sync_all())
            .map_err(|error| error.to_string())?;
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| "epoch chaos server is not running".to_string())?;
        child.kill().map_err(|error| error.to_string())?;
        let status = self.wait_exit(Duration::from_secs(30))?;
        if status.success() {
            return Err("crash-barrier child exited successfully".to_string());
        }
        Ok(status)
    }

    fn wait_listening(&mut self, timeout: Duration) -> Result<(), String> {
        let address = self.address()?;
        let deadline = Instant::now() + timeout;
        loop {
            self.ensure_running()?;
            if TcpStream::connect_timeout(&address, Duration::from_millis(250)).is_ok() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "server did not listen on {address} within {timeout:?}"
                ));
            }
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn ensure_running(&mut self) -> Result<(), String> {
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| "epoch chaos server is not running".to_string())?;
        match child.try_wait().map_err(|error| error.to_string())? {
            Some(status) => Err(format!("epoch chaos server exited early: {status}")),
            None => Ok(()),
        }
    }

    fn wait_exit(&mut self, timeout: Duration) -> Result<ExitStatus, String> {
        let deadline = Instant::now() + timeout;
        loop {
            let status = self
                .child
                .as_mut()
                .ok_or_else(|| "epoch chaos server is not running".to_string())?
                .try_wait()
                .map_err(|error| error.to_string())?;
            if let Some(status) = status {
                self.child = None;
                self.address = None;
                self.crash_ready = None;
                self.telemetry.clear_pid();
                return Ok(status);
            }
            if Instant::now() >= deadline {
                if let Some(child) = self.child.as_mut() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                self.child = None;
                self.address = None;
                self.crash_ready = None;
                self.telemetry.clear_pid();
                return Err(format!("server did not exit within {timeout:?}"));
            }
            thread::sleep(Duration::from_millis(25));
        }
    }
}

impl Drop for ServerSupervisor<'_> {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.telemetry.clear_pid();
    }
}

fn reserve_port() -> Result<u16, String> {
    let listener =
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).map_err(|error| error.to_string())?;
    let port = listener
        .local_addr()
        .map_err(|error| error.to_string())?
        .port();
    drop(listener);
    Ok(port)
}

fn server_config(data_dir: PathBuf, port: u16) -> ServerConfig {
    ServerConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port,
        data_dir,
        transport: Default::default(),
        authentication: Default::default(),
        max_connections: 640,
        max_inflight_frame_bytes: default_max_inflight_frame_bytes(),
        max_databases: default_max_databases(),
        max_database_name_bytes: default_max_database_name_bytes(),
        connect_timeout_secs: 10,
        connection_idle_timeout_secs: 6 * 60 * 60,
        net_read_timeout_secs: 120,
        net_write_timeout_secs: 120,
        cursor_batch_max_rows: 1_024,
        cursor_batch_max_bytes: 8 * 1024 * 1024,
        max_frame_bytes: 64 * 1024 * 1024,
        copy_max_transaction_bytes: default_copy_max_transaction_bytes(),
        max_compaction_jobs: 2,
        storage_cpu_workers: default_storage_cpu_workers(),
        page_cache_level: default_page_cache_level(),
        page_cache_max_bytes: default_page_cache_max_bytes(),
        page_cache_memory_reserve: default_page_cache_memory_reserve(),
        target_volume_rows: 65_536,
        seal_hot_bytes_threshold: 1024 * 1024,
        seal_incremental_hot_bytes_threshold: 256 * 1024,
        read_queue_depth: 2,
    }
}
