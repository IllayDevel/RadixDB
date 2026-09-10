use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const MANIFEST_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunProfile {
    Core,
    Prerelease128,
    ConcurrencyLadder { clients: usize },
    Soak { duration_secs: u64 },
    Recovery,
    Replay { source_run_id: String },
}

impl RunProfile {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::ConcurrencyLadder { clients } => {
                if *clients < 16 || !clients.is_power_of_two() {
                    return Err(format!(
                        "concurrency clients must be a power of two and at least 16, got {clients}"
                    ));
                }
            }
            Self::Soak { duration_secs } if *duration_secs == 0 => {
                return Err("soak duration must be greater than zero".to_string());
            }
            Self::Replay { source_run_id } => validate_identifier("source run id", source_run_id)?,
            _ => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateIdentity {
    pub commit: String,
    pub dirty: bool,
    pub cargo_lock_sha256: String,
    pub release_binary_sha256: String,
    pub server_config_sha256: String,
}

impl CandidateIdentity {
    pub fn validate(&self) -> Result<(), String> {
        validate_hex("commit", &self.commit, 40)?;
        validate_hex("Cargo.lock SHA-256", &self.cargo_lock_sha256, 64)?;
        validate_hex("release binary SHA-256", &self.release_binary_sha256, 64)?;
        validate_hex("server config SHA-256", &self.server_config_sha256, 64)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostProfile {
    pub hostname: String,
    pub operating_system: String,
    pub kernel: String,
    pub filesystem: String,
    pub cpu_count: usize,
    pub memory_bytes: u64,
}

impl HostProfile {
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("hostname", self.hostname.as_str()),
            ("operating system", self.operating_system.as_str()),
            ("kernel", self.kernel.as_str()),
            ("filesystem", self.filesystem.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("{field} must not be empty"));
            }
        }
        if self.cpu_count == 0 {
            return Err("cpu_count must be greater than zero".to_string());
        }
        if self.memory_bytes == 0 {
            return Err("memory_bytes must be greater than zero".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunConfig {
    pub run_id: String,
    pub seed: u64,
    pub profile: RunProfile,
    pub max_connections: usize,
    pub operation_budget: u64,
    pub timeout_secs: u64,
    pub labels: BTreeMap<String, String>,
}

impl RunConfig {
    pub fn historical_core(run_id: impl Into<String>, seed: u64) -> Self {
        Self {
            run_id: run_id.into(),
            seed,
            profile: RunProfile::Core,
            max_connections: 128,
            operation_budget: 1_024,
            timeout_secs: 180,
            labels: BTreeMap::new(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_identifier("run id", &self.run_id)?;
        self.profile.validate()?;
        if self.max_connections == 0 {
            return Err("max_connections must be greater than zero".to_string());
        }
        if self.operation_budget == 0 {
            return Err("operation_budget must be greater than zero".to_string());
        }
        if self.timeout_secs == 0 {
            return Err("timeout_secs must be greater than zero".to_string());
        }
        for (key, value) in &self.labels {
            validate_identifier("label key", key)?;
            if value.is_empty() || value.contains(['\n', '\r']) {
                return Err(format!("label `{key}` has an invalid value"));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunManifest {
    pub format_version: u32,
    pub config: RunConfig,
    pub candidate: CandidateIdentity,
    pub host: HostProfile,
    pub rustc: String,
}

impl RunManifest {
    pub fn new(
        config: RunConfig,
        candidate: CandidateIdentity,
        host: HostProfile,
        rustc: impl Into<String>,
    ) -> Result<Self, String> {
        let manifest = Self {
            format_version: MANIFEST_FORMAT_VERSION,
            config,
            candidate,
            host,
            rustc: rustc.into(),
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.format_version != MANIFEST_FORMAT_VERSION {
            return Err(format!(
                "unsupported manifest format version {}, expected {MANIFEST_FORMAT_VERSION}",
                self.format_version
            ));
        }
        self.config.validate()?;
        self.candidate.validate()?;
        self.host.validate()?;
        if self.rustc.trim().is_empty() {
            return Err("rustc identity must not be empty".to_string());
        }
        Ok(())
    }

    pub fn to_pretty_json(&self) -> Result<String, String> {
        self.validate()?;
        serde_json::to_string_pretty(self).map_err(|error| error.to_string())
    }

    pub fn from_json(json: &str) -> Result<Self, String> {
        let manifest: Self = serde_json::from_str(json).map_err(|error| error.to_string())?;
        manifest.validate()?;
        Ok(manifest)
    }
}

fn validate_identifier(field: &str, value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(format!(
            "{field} must contain only ASCII letters, digits, '.', '-' or '_'"
        ));
    }
    Ok(())
}

fn validate_hex(field: &str, value: &str, expected_len: usize) -> Result<(), String> {
    if value.len() != expected_len || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!(
            "{field} must be exactly {expected_len} hexadecimal characters"
        ));
    }
    Ok(())
}
