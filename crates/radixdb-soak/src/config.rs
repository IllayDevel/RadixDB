use std::{
    collections::BTreeSet,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};

const CONFIG_FORMAT: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read soak config `{path}`: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("cannot parse soak config `{path}`: {source}")]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("invalid soak config: {0}")]
    Invalid(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SoakConfig {
    pub format: u32,
    pub profile: String,
    pub duration: String,
    pub seed: u64,
    pub database: DatabaseConfig,
    pub status: StatusConfig,
    pub artifacts: ArtifactConfig,
    pub monitor: MonitorConfig,
    #[serde(default)]
    pub diagnostics: DiagnosticsConfig,
    #[serde(default)]
    pub faults: FaultConfig,
    pub load: LoadConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    #[serde(default)]
    pub engine: DatabaseEngine,
    pub address: String,
    pub name: String,
    #[serde(default = "default_login")]
    pub login: String,
    #[serde(default)]
    pub password_file: Option<PathBuf>,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,
    #[serde(default = "default_read_timeout")]
    pub read_timeout_secs: u64,
    #[serde(default = "default_write_timeout")]
    pub write_timeout_secs: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseEngine {
    #[default]
    Radixdb,
    Postgresql,
}

impl DatabaseEngine {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Radixdb => "radixdb",
            Self::Postgresql => "postgresql",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusConfig {
    pub bind: String,
    pub auth_file: PathBuf,
    #[serde(default = "default_event_capacity")]
    pub event_capacity: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactConfig {
    pub root: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitorConfig {
    pub server_executable: PathBuf,
    pub data_dir: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_diagnostics_bind")]
    pub bind: String,
    #[serde(default = "default_diagnostics_auth_file")]
    pub auth_file: PathBuf,
    #[serde(default = "default_diagnostics_channel_path")]
    pub channel_path: PathBuf,
    #[serde(default = "default_normal_interval")]
    pub normal_interval: String,
    #[serde(default = "default_alert_interval")]
    pub alert_interval: String,
    #[serde(default = "default_burst_interval")]
    pub burst_interval: String,
    #[serde(default = "default_burst_duration")]
    pub burst_duration: String,
    #[serde(default = "default_history_window")]
    pub history_window: String,
    #[serde(default = "default_history_max_samples")]
    pub history_max_samples: usize,
    #[serde(default = "default_artifact_quota_bytes")]
    pub artifact_quota_bytes: u64,
    #[serde(default = "default_max_incidents")]
    pub max_incidents: usize,
    #[serde(default = "default_engine_snapshot_interval")]
    pub engine_snapshot_interval: String,
    #[serde(default = "default_engine_snapshot_timeout")]
    pub engine_snapshot_timeout: String,
    #[serde(default = "default_smart_interval")]
    pub smart_interval: String,
    #[serde(default = "default_workload_stall_timeout")]
    pub workload_stall_timeout: String,
    #[serde(default = "default_agent_heartbeat_timeout")]
    pub agent_heartbeat_timeout: String,
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: default_diagnostics_bind(),
            auth_file: default_diagnostics_auth_file(),
            channel_path: default_diagnostics_channel_path(),
            normal_interval: default_normal_interval(),
            alert_interval: default_alert_interval(),
            burst_interval: default_burst_interval(),
            burst_duration: default_burst_duration(),
            history_window: default_history_window(),
            history_max_samples: default_history_max_samples(),
            artifact_quota_bytes: default_artifact_quota_bytes(),
            max_incidents: default_max_incidents(),
            engine_snapshot_interval: default_engine_snapshot_interval(),
            engine_snapshot_timeout: default_engine_snapshot_timeout(),
            smart_interval: default_smart_interval(),
            workload_stall_timeout: default_workload_stall_timeout(),
            agent_heartbeat_timeout: default_agent_heartbeat_timeout(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultConfig {
    #[serde(default)]
    pub graceful_reopen_at: Option<String>,
    #[serde(default)]
    pub kill_reopen_at: Option<String>,
    #[serde(default = "default_recovery_timeout")]
    pub recovery_timeout: String,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            graceful_reopen_at: None,
            kill_reopen_at: None,
            recovery_timeout: default_recovery_timeout(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadConfig {
    pub client_steps: Vec<usize>,
    pub active_rows: u64,
    #[serde(default)]
    pub import_dir: Option<PathBuf>,
    pub checkpoint_interval: String,
    pub sample_interval: String,
    pub invariant_interval: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SoakProfile {
    Smoke,
    Tuning,
    #[serde(rename = "6h")]
    SixHours,
    #[serde(rename = "24h")]
    TwentyFourHours,
    #[serde(rename = "48h")]
    FortyEightHours,
}

impl SoakProfile {
    pub fn parse(value: &str) -> Result<Self, ConfigError> {
        match value {
            "smoke" => Ok(Self::Smoke),
            "tuning" => Ok(Self::Tuning),
            "6h" => Ok(Self::SixHours),
            "24h" => Ok(Self::TwentyFourHours),
            "48h" => Ok(Self::FortyEightHours),
            other => Err(ConfigError::Invalid(format!(
                "unsupported profile `{other}`; expected smoke, tuning, 6h, 24h or 48h"
            ))),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Tuning => "tuning",
            Self::SixHours => "6h",
            Self::TwentyFourHours => "24h",
            Self::FortyEightHours => "48h",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ResolvedSoakConfig {
    pub format: u32,
    pub profile: SoakProfile,
    pub duration: Duration,
    pub seed: u64,
    pub database: ResolvedDatabaseConfig,
    pub status: ResolvedStatusConfig,
    pub artifacts: ArtifactConfig,
    pub monitor: MonitorConfig,
    pub diagnostics: ResolvedDiagnosticsConfig,
    pub faults: ResolvedFaultConfig,
    pub load: ResolvedLoadConfig,
}

#[derive(Clone, Debug)]
pub struct ResolvedDatabaseConfig {
    pub engine: DatabaseEngine,
    pub address: SocketAddr,
    pub name: String,
    pub login: String,
    pub password_file: Option<PathBuf>,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct ResolvedStatusConfig {
    pub bind: SocketAddr,
    pub auth_file: PathBuf,
    pub event_capacity: usize,
}

#[derive(Clone, Debug)]
pub struct ResolvedLoadConfig {
    pub client_steps: Vec<usize>,
    pub active_rows: u64,
    pub import_dir: PathBuf,
    pub checkpoint_interval: Duration,
    pub sample_interval: Duration,
    pub invariant_interval: Duration,
}

#[derive(Clone, Debug)]
pub struct ResolvedFaultConfig {
    pub graceful_reopen_at: Option<Duration>,
    pub kill_reopen_at: Option<Duration>,
    pub recovery_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct ResolvedDiagnosticsConfig {
    pub enabled: bool,
    pub bind: SocketAddr,
    pub auth_file: PathBuf,
    pub channel_path: PathBuf,
    pub normal_interval: Duration,
    pub alert_interval: Duration,
    pub burst_interval: Duration,
    pub burst_duration: Duration,
    pub history_window: Duration,
    pub history_max_samples: usize,
    pub artifact_quota_bytes: u64,
    pub max_incidents: usize,
    pub engine_snapshot_interval: Duration,
    pub engine_snapshot_timeout: Duration,
    pub smart_interval: Duration,
    pub workload_stall_timeout: Duration,
    pub agent_heartbeat_timeout: Duration,
}

impl SoakConfig {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        toml::from_str(&text).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })
    }

    pub fn resolve(self) -> Result<ResolvedSoakConfig, ConfigError> {
        if self.format != CONFIG_FORMAT {
            return Err(ConfigError::Invalid(format!(
                "unsupported format {}, expected {CONFIG_FORMAT}",
                self.format
            )));
        }
        let profile = SoakProfile::parse(&self.profile)?;
        let duration = parse_duration(&self.duration)?;
        let expected_duration = match profile {
            SoakProfile::Smoke => Duration::from_secs(10 * 60),
            SoakProfile::Tuning => Duration::from_secs(30 * 60),
            SoakProfile::SixHours => Duration::from_secs(6 * 60 * 60),
            SoakProfile::TwentyFourHours => Duration::from_secs(24 * 60 * 60),
            SoakProfile::FortyEightHours => Duration::from_secs(48 * 60 * 60),
        };
        if duration != expected_duration {
            return Err(ConfigError::Invalid(format!(
                "profile `{}` requires duration {} seconds",
                profile.as_str(),
                expected_duration.as_secs()
            )));
        }
        let address = parse_address("database.address", &self.database.address)?;
        let status_bind = parse_address("status.bind", &self.status.bind)?;
        validate_name("database.name", &self.database.name)?;
        validate_name("database.login", &self.database.login)?;
        if self.status.auth_file.as_os_str().is_empty() {
            return Err(ConfigError::Invalid(
                "status.auth_file must not be empty".into(),
            ));
        }
        if self.artifacts.root.as_os_str().is_empty() {
            return Err(ConfigError::Invalid(
                "artifacts.root must not be empty".into(),
            ));
        }
        validate_absolute_path("monitor.server_executable", &self.monitor.server_executable)?;
        validate_absolute_path("monitor.data_dir", &self.monitor.data_dir)?;
        let diagnostics_bind = parse_address("diagnostics.bind", &self.diagnostics.bind)?;
        validate_absolute_path("diagnostics.auth_file", &self.diagnostics.auth_file)?;
        validate_absolute_path("diagnostics.channel_path", &self.diagnostics.channel_path)?;
        let normal_interval = parse_duration(&self.diagnostics.normal_interval)?;
        let alert_interval = parse_duration(&self.diagnostics.alert_interval)?;
        let burst_interval = parse_duration(&self.diagnostics.burst_interval)?;
        let burst_duration = parse_duration(&self.diagnostics.burst_duration)?;
        let history_window = parse_duration(&self.diagnostics.history_window)?;
        let engine_snapshot_interval = parse_duration(&self.diagnostics.engine_snapshot_interval)?;
        let engine_snapshot_timeout = parse_duration(&self.diagnostics.engine_snapshot_timeout)?;
        let smart_interval = parse_duration(&self.diagnostics.smart_interval)?;
        let workload_stall_timeout = parse_duration(&self.diagnostics.workload_stall_timeout)?;
        let agent_heartbeat_timeout = parse_duration(&self.diagnostics.agent_heartbeat_timeout)?;
        if normal_interval < Duration::from_secs(1)
            || alert_interval < Duration::from_millis(250)
            || burst_interval < Duration::from_millis(100)
        {
            return Err(ConfigError::Invalid(
                "diagnostic intervals must be at least normal=1s, alert=250ms, burst=100ms".into(),
            ));
        }
        if !(burst_interval <= alert_interval && alert_interval <= normal_interval) {
            return Err(ConfigError::Invalid(
                "diagnostic intervals must satisfy burst <= alert <= normal".into(),
            ));
        }
        if burst_duration < alert_interval || history_window < burst_duration {
            return Err(ConfigError::Invalid(
                "diagnostic burst/history intervals are inconsistent".into(),
            ));
        }
        if burst_duration > Duration::from_secs(5 * 60)
            || history_window > Duration::from_secs(2 * 60 * 60)
        {
            return Err(ConfigError::Invalid(
                "diagnostic burst/history limits are burst<=5m and history<=2h".into(),
            ));
        }
        if !(16..=4096).contains(&self.diagnostics.history_max_samples) {
            return Err(ConfigError::Invalid(
                "diagnostics.history_max_samples must be in 16..=4096".into(),
            ));
        }
        if engine_snapshot_timeout > engine_snapshot_interval {
            return Err(ConfigError::Invalid(
                "diagnostics.engine_snapshot_timeout cannot exceed engine_snapshot_interval".into(),
            ));
        }
        if engine_snapshot_interval < Duration::from_secs(1)
            || smart_interval < Duration::from_secs(60)
        {
            return Err(ConfigError::Invalid(
                "diagnostic collectors require engine_snapshot_interval>=1s and smart_interval>=60s"
                    .into(),
            ));
        }
        if workload_stall_timeout < normal_interval || agent_heartbeat_timeout < normal_interval {
            return Err(ConfigError::Invalid(
                "diagnostic progress/heartbeat timeouts cannot be shorter than normal_interval"
                    .into(),
            ));
        }
        if !(64 * 1024 * 1024..=64 * 1024 * 1024 * 1024)
            .contains(&self.diagnostics.artifact_quota_bytes)
        {
            return Err(ConfigError::Invalid(
                "diagnostics.artifact_quota_bytes must be in 64 MiB..=64 GiB".into(),
            ));
        }
        if !(1..=1024).contains(&self.diagnostics.max_incidents) {
            return Err(ConfigError::Invalid(
                "diagnostics.max_incidents must be in 1..=1024".into(),
            ));
        }
        let graceful_reopen_at = optional_duration(self.faults.graceful_reopen_at.as_deref())?;
        let kill_reopen_at = optional_duration(self.faults.kill_reopen_at.as_deref())?;
        let recovery_timeout = parse_duration(&self.faults.recovery_timeout)?;
        for (name, value) in [
            ("faults.graceful_reopen_at", graceful_reopen_at),
            ("faults.kill_reopen_at", kill_reopen_at),
        ] {
            if value.is_some_and(|value| value >= duration) {
                return Err(ConfigError::Invalid(format!(
                    "{name} must be earlier than the workload duration"
                )));
            }
        }
        if graceful_reopen_at.is_some() && graceful_reopen_at == kill_reopen_at {
            return Err(ConfigError::Invalid(
                "graceful and kill reopen times must differ".into(),
            ));
        }
        if matches!(
            profile,
            SoakProfile::SixHours | SoakProfile::TwentyFourHours | SoakProfile::FortyEightHours
        ) && (graceful_reopen_at.is_none() || kill_reopen_at.is_none())
        {
            return Err(ConfigError::Invalid(
                "long profiles require both graceful and kill reopen phases".into(),
            ));
        }
        if !(16..=65_536).contains(&self.status.event_capacity) {
            return Err(ConfigError::Invalid(
                "status.event_capacity must be in 16..=65536".into(),
            ));
        }
        let client_steps = validate_client_steps(self.load.client_steps)?;
        if self.load.active_rows == 0 {
            return Err(ConfigError::Invalid(
                "load.active_rows must be non-zero".into(),
            ));
        }
        let required_long_profile_rows = match profile {
            SoakProfile::SixHours | SoakProfile::TwentyFourHours | SoakProfile::FortyEightHours => {
                Some(100_000_000)
            }
            SoakProfile::Smoke | SoakProfile::Tuning => None,
        };
        if required_long_profile_rows.is_some_and(|rows| self.load.active_rows != rows)
            || required_long_profile_rows.is_some() && client_steps != [16, 32, 64, 128, 256]
        {
            return Err(ConfigError::Invalid(
                "6h/24h/48h require active_rows=100000000 and client_steps=[16,32,64,128,256]"
                    .into(),
            ));
        }
        let import_dir = self
            .load
            .import_dir
            .unwrap_or_else(|| self.monitor.data_dir.clone());
        validate_absolute_path("load.import_dir", &import_dir)?;
        let checkpoint_interval = parse_duration(&self.load.checkpoint_interval)?;
        let sample_interval = parse_duration(&self.load.sample_interval)?;
        let invariant_interval = parse_duration(&self.load.invariant_interval)?;
        if sample_interval > invariant_interval {
            return Err(ConfigError::Invalid(
                "load.sample_interval cannot exceed load.invariant_interval".into(),
            ));
        }
        if invariant_interval > checkpoint_interval {
            return Err(ConfigError::Invalid(
                "load.invariant_interval cannot exceed load.checkpoint_interval".into(),
            ));
        }
        Ok(ResolvedSoakConfig {
            format: self.format,
            profile,
            duration,
            seed: self.seed,
            database: ResolvedDatabaseConfig {
                engine: self.database.engine,
                address,
                name: self.database.name,
                login: self.database.login,
                password_file: self.database.password_file,
                connect_timeout: nonzero_seconds(
                    "database.connect_timeout_secs",
                    self.database.connect_timeout_secs,
                )?,
                read_timeout: nonzero_seconds(
                    "database.read_timeout_secs",
                    self.database.read_timeout_secs,
                )?,
                write_timeout: nonzero_seconds(
                    "database.write_timeout_secs",
                    self.database.write_timeout_secs,
                )?,
            },
            status: ResolvedStatusConfig {
                bind: status_bind,
                auth_file: self.status.auth_file,
                event_capacity: self.status.event_capacity,
            },
            artifacts: self.artifacts,
            monitor: self.monitor,
            diagnostics: ResolvedDiagnosticsConfig {
                enabled: self.diagnostics.enabled,
                bind: diagnostics_bind,
                auth_file: self.diagnostics.auth_file,
                channel_path: self.diagnostics.channel_path,
                normal_interval,
                alert_interval,
                burst_interval,
                burst_duration,
                history_window,
                history_max_samples: self.diagnostics.history_max_samples,
                artifact_quota_bytes: self.diagnostics.artifact_quota_bytes,
                max_incidents: self.diagnostics.max_incidents,
                engine_snapshot_interval,
                engine_snapshot_timeout,
                smart_interval,
                workload_stall_timeout,
                agent_heartbeat_timeout,
            },
            faults: ResolvedFaultConfig {
                graceful_reopen_at,
                kill_reopen_at,
                recovery_timeout,
            },
            load: ResolvedLoadConfig {
                client_steps,
                active_rows: self.load.active_rows,
                import_dir,
                checkpoint_interval,
                sample_interval,
                invariant_interval,
            },
        })
    }
}

fn optional_duration(value: Option<&str>) -> Result<Option<Duration>, ConfigError> {
    value.map(parse_duration).transpose()
}

fn validate_absolute_path(name: &str, path: &Path) -> Result<(), ConfigError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err(ConfigError::Invalid(format!(
            "{name} must be an absolute path without `..`"
        )));
    }
    Ok(())
}

fn parse_address(name: &str, value: &str) -> Result<SocketAddr, ConfigError> {
    value
        .parse()
        .map_err(|_| ConfigError::Invalid(format!("{name} must be a numeric socket address")))
}

fn validate_name(name: &str, value: &str) -> Result<(), ConfigError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(ConfigError::Invalid(format!(
            "{name} must contain 1..=128 ASCII letters, digits, `_` or `-`"
        )));
    }
    Ok(())
}

fn validate_client_steps(values: Vec<usize>) -> Result<Vec<usize>, ConfigError> {
    if values.is_empty() {
        return Err(ConfigError::Invalid(
            "load.client_steps must not be empty".into(),
        ));
    }
    let unique = values.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != values.len()
        || values.windows(2).any(|pair| pair[0] >= pair[1])
        || values
            .iter()
            .any(|value| *value < 2 || !value.is_power_of_two())
    {
        return Err(ConfigError::Invalid(
            "load.client_steps must be unique ascending powers of two >= 2".into(),
        ));
    }
    Ok(values)
}

pub fn parse_duration(value: &str) -> Result<Duration, ConfigError> {
    let split = value
        .find(|byte: char| !byte.is_ascii_digit())
        .ok_or_else(|| ConfigError::Invalid(format!("duration `{value}` has no unit")))?;
    let (amount, unit) = value.split_at(split);
    let amount = amount
        .parse::<u64>()
        .map_err(|_| ConfigError::Invalid(format!("invalid duration `{value}`")))?;
    if amount == 0 {
        return Err(ConfigError::Invalid(format!(
            "duration `{value}` must be non-zero"
        )));
    }
    let millis = match unit {
        "ms" => Some(amount),
        "s" => amount.checked_mul(1_000),
        "m" => amount.checked_mul(60_000),
        "h" => amount.checked_mul(60 * 60 * 1_000),
        _ => {
            return Err(ConfigError::Invalid(format!(
                "duration `{value}` must use ms, s, m or h"
            )))
        }
    };
    millis
        .map(Duration::from_millis)
        .ok_or_else(|| ConfigError::Invalid(format!("duration `{value}` overflows")))
}

fn nonzero_seconds(name: &str, seconds: u64) -> Result<Duration, ConfigError> {
    if seconds == 0 {
        return Err(ConfigError::Invalid(format!("{name} must be non-zero")));
    }
    Ok(Duration::from_secs(seconds))
}

fn default_login() -> String {
    "root".into()
}

const fn default_connect_timeout() -> u64 {
    5
}

const fn default_read_timeout() -> u64 {
    30
}

const fn default_write_timeout() -> u64 {
    10
}

const fn default_event_capacity() -> usize {
    2048
}

fn default_recovery_timeout() -> String {
    "2m".into()
}

fn default_diagnostics_bind() -> String {
    "127.0.0.1:18089".into()
}

fn default_diagnostics_auth_file() -> PathBuf {
    "/run/credentials/radixdb-soak-observer.service/status-auth".into()
}

fn default_diagnostics_channel_path() -> PathBuf {
    "/run/radixdb-soak/agent.sock".into()
}

fn default_normal_interval() -> String {
    "5s".into()
}

fn default_alert_interval() -> String {
    "1s".into()
}

fn default_burst_interval() -> String {
    "250ms".into()
}

fn default_burst_duration() -> String {
    "60s".into()
}

fn default_history_window() -> String {
    "30m".into()
}

const fn default_history_max_samples() -> usize {
    512
}

const fn default_artifact_quota_bytes() -> u64 {
    10 * 1024 * 1024 * 1024
}

const fn default_max_incidents() -> usize {
    128
}

fn default_engine_snapshot_interval() -> String {
    "5s".into()
}

fn default_engine_snapshot_timeout() -> String {
    "2s".into()
}

fn default_smart_interval() -> String {
    "30m".into()
}

fn default_workload_stall_timeout() -> String {
    "60s".into()
}

fn default_agent_heartbeat_timeout() -> String {
    "30s".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> SoakConfig {
        SoakConfig {
            format: 1,
            profile: "6h".into(),
            duration: "6h".into(),
            seed: 7,
            database: DatabaseConfig {
                engine: DatabaseEngine::Radixdb,
                address: "127.0.0.1:25441".into(),
                name: "radixdb_soak".into(),
                login: "root".into(),
                password_file: None,
                connect_timeout_secs: 5,
                read_timeout_secs: 30,
                write_timeout_secs: 10,
            },
            status: StatusConfig {
                bind: "0.0.0.0:18088".into(),
                auth_file: "/run/soak-auth".into(),
                event_capacity: 2048,
            },
            artifacts: ArtifactConfig {
                root: "/tmp/soak".into(),
            },
            monitor: MonitorConfig {
                server_executable: "/tmp/radixdb-server".into(),
                data_dir: "/tmp/soak-data".into(),
            },
            diagnostics: DiagnosticsConfig {
                enabled: true,
                bind: "0.0.0.0:18089".into(),
                ..DiagnosticsConfig::default()
            },
            faults: FaultConfig {
                graceful_reopen_at: Some("2h".into()),
                kill_reopen_at: Some("4h".into()),
                recovery_timeout: "2m".into(),
            },
            load: LoadConfig {
                client_steps: vec![16, 32, 64, 128, 256],
                active_rows: 100_000_000,
                import_dir: None,
                checkpoint_interval: "5m".into(),
                sample_interval: "5s".into(),
                invariant_interval: "30s".into(),
            },
        }
    }

    #[test]
    fn resolves_strict_contract() {
        let config = valid().resolve().unwrap();
        assert_eq!(config.duration, Duration::from_secs(21_600));
        assert_eq!(config.status.bind, "0.0.0.0:18088".parse().unwrap());
        assert_eq!(config.load.client_steps, [16, 32, 64, 128, 256]);
        assert_eq!(config.database.engine, DatabaseEngine::Radixdb);
        assert_eq!(config.load.import_dir, PathBuf::from("/tmp/soak-data"));
        assert_eq!(
            config.diagnostics.burst_interval,
            Duration::from_millis(250)
        );
        assert_eq!(config.diagnostics.history_max_samples, 512);
    }

    #[test]
    fn rejects_bad_client_ladder_and_duration() {
        let mut config = valid();
        config.load.client_steps = vec![16, 24];
        assert!(config.resolve().is_err());
        let mut config = valid();
        config.duration = "0h".into();
        assert!(config.resolve().is_err());
    }

    #[test]
    fn keeps_profile_specific_long_seed_contracts() {
        let mut six_hours = valid();
        six_hours.load.active_rows = 20_000_000;
        assert!(six_hours.resolve().is_err());

        let mut twenty_four_hours = valid();
        twenty_four_hours.profile = "24h".into();
        twenty_four_hours.duration = "24h".into();
        twenty_four_hours.faults.graceful_reopen_at = Some("8h".into());
        twenty_four_hours.faults.kill_reopen_at = Some("16h".into());
        twenty_four_hours.load.active_rows = 20_000_000;
        assert!(twenty_four_hours.resolve().is_err());

        let mut twenty_four_hours = valid();
        twenty_four_hours.profile = "24h".into();
        twenty_four_hours.duration = "24h".into();
        twenty_four_hours.faults.graceful_reopen_at = Some("8h".into());
        twenty_four_hours.faults.kill_reopen_at = Some("16h".into());
        twenty_four_hours.load.active_rows = 100_000_000;
        assert!(twenty_four_hours.resolve().is_ok());
    }

    #[test]
    fn rejects_unknown_toml_fields() {
        let text = r#"
format = 1
profile = "smoke"
duration = "10s"
seed = 1
unexpected = true
"#;
        assert!(toml::from_str::<SoakConfig>(text).is_err());
    }

    #[test]
    fn rejects_diagnostic_resource_amplification() {
        let mut config = valid();
        config.diagnostics.normal_interval = "999ms".into();
        config.diagnostics.alert_interval = "250ms".into();
        config.diagnostics.burst_interval = "100ms".into();
        assert!(config
            .resolve()
            .unwrap_err()
            .to_string()
            .contains("normal=1s"));

        let mut config = valid();
        config.diagnostics.history_max_samples = 4_097;
        assert!(config.resolve().is_err());

        let mut config = valid();
        config.diagnostics.history_window = "3h".into();
        assert!(config.resolve().is_err());

        let mut config = valid();
        config.diagnostics.artifact_quota_bytes = 64 * 1024 * 1024 * 1024 + 1;
        assert!(config.resolve().is_err());

        let mut config = valid();
        config.diagnostics.engine_snapshot_interval = "999ms".into();
        config.diagnostics.engine_snapshot_timeout = "500ms".into();
        assert!(config.resolve().is_err());

        let mut config = valid();
        config.diagnostics.smart_interval = "59s".into();
        assert!(config.resolve().is_err());
    }

    #[test]
    fn resolves_postgresql_engine_and_separate_import_directory() {
        let mut config = valid();
        config.database.engine = DatabaseEngine::Postgresql;
        config.database.address = "127.0.0.1:25432".into();
        config.database.login = "soak".into();
        config.load.import_dir = Some("/storage/radixdb-soak-postgresql/import".into());
        let resolved = config.resolve().unwrap();
        assert_eq!(resolved.database.engine, DatabaseEngine::Postgresql);
        assert_eq!(
            resolved.load.import_dir,
            PathBuf::from("/storage/radixdb-soak-postgresql/import")
        );
    }
}
