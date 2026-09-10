use std::{net::IpAddr, path::PathBuf};

use crate::api::{ServerCredentialContract, ServerStorageContract};
use radixdb_plugin_host::PluginHostConfig;
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};

/// A configured password verifier whose contents must not appear in debug logs.
#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct RootPasswordVerifier(String);

impl RootPasswordVerifier {
    pub fn parse(encoded: impl Into<String>) -> Result<Self, String> {
        let encoded = encoded.into();
        ServerCredentialContract::validate_password_verifier(&encoded)
            .map_err(|error| error.to_string())?;
        Ok(Self(encoded))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for RootPasswordVerifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RootPasswordVerifier(<redacted>)")
    }
}

impl<'de> Deserialize<'de> for RootPasswordVerifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        Self::parse(encoded).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ServerAuthenticationConfig {
    #[serde(default)]
    pub root_password_verifier: Option<RootPasswordVerifier>,
}

/// Transport boundary for the stock server. Password-authenticated plaintext
/// remains a supported deployment mode; TLS is enabled only when selected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ServerTransportConfig {
    #[default]
    Plaintext,
    Tls {
        certificate_chain: PathBuf,
        private_key: PathBuf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub bind_ip: IpAddr,
    pub port: u16,
    pub data_dir: PathBuf,
    #[serde(default)]
    pub transport: ServerTransportConfig,
    #[serde(default)]
    pub authentication: ServerAuthenticationConfig,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    #[serde(default = "default_max_inflight_frame_bytes")]
    pub max_inflight_frame_bytes: usize,
    #[serde(default = "default_max_databases")]
    pub max_databases: usize,
    #[serde(default = "default_max_database_name_bytes")]
    pub max_database_name_bytes: usize,
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    #[serde(default = "default_connection_idle_timeout_secs")]
    pub connection_idle_timeout_secs: u64,
    #[serde(default = "default_net_read_timeout_secs")]
    pub net_read_timeout_secs: u64,
    #[serde(default = "default_net_write_timeout_secs")]
    pub net_write_timeout_secs: u64,
    #[serde(default = "default_cursor_batch_max_rows")]
    pub cursor_batch_max_rows: usize,
    #[serde(default = "default_cursor_batch_max_bytes")]
    pub cursor_batch_max_bytes: usize,
    #[serde(default = "default_max_frame_bytes")]
    pub max_frame_bytes: u32,
    #[serde(default = "default_copy_max_transaction_bytes")]
    pub copy_max_transaction_bytes: usize,
    #[serde(default = "default_max_compaction_jobs")]
    pub max_compaction_jobs: usize,
    #[serde(default = "default_storage_cpu_workers")]
    pub storage_cpu_workers: usize,
    #[serde(default = "default_page_cache_level")]
    pub page_cache_level: u8,
    #[serde(default = "default_page_cache_max_bytes")]
    pub page_cache_max_bytes: u64,
    #[serde(default = "default_page_cache_memory_reserve")]
    pub page_cache_memory_reserve: u64,
    #[serde(default = "default_target_volume_rows")]
    pub target_volume_rows: usize,
    #[serde(default = "default_seal_hot_bytes_threshold")]
    pub seal_hot_bytes_threshold: usize,
    #[serde(default = "default_seal_incremental_hot_bytes_threshold")]
    pub seal_incremental_hot_bytes_threshold: usize,
    #[serde(default = "default_read_queue_depth", rename = "read_queue_depth")]
    pub read_queue_depth: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfigFile {
    pub server: ServerConfig,
    #[serde(default)]
    pub plugins: PluginHostConfig,
}

pub fn parse_server_config_file(source: &str) -> Result<ServerConfigFile, String> {
    toml::from_str(source).map_err(|error| error.to_string())
}

pub const fn default_connect_timeout_secs() -> u64 {
    10
}
pub const fn default_connection_idle_timeout_secs() -> u64 {
    28_800
}
pub const fn default_max_connections() -> usize {
    151
}
pub const fn default_max_inflight_frame_bytes() -> usize {
    256 * 1024 * 1024
}
pub const fn default_max_databases() -> usize {
    64
}
pub const fn default_max_database_name_bytes() -> usize {
    64
}
pub const fn default_net_read_timeout_secs() -> u64 {
    30
}
pub const fn default_net_write_timeout_secs() -> u64 {
    60
}
pub const fn default_cursor_batch_max_rows() -> usize {
    1_024
}
pub const fn default_cursor_batch_max_bytes() -> usize {
    8 * 1024 * 1024
}
pub const fn default_max_frame_bytes() -> u32 {
    64 * 1024 * 1024
}
pub const fn default_copy_max_transaction_bytes() -> usize {
    ServerStorageContract::DEFAULT_COPY_MAX_TRANSACTION_BYTES
}
pub const fn default_max_compaction_jobs() -> usize {
    ServerStorageContract::DEFAULT_MAX_COMPACTION_JOBS
}
pub const fn default_storage_cpu_workers() -> usize {
    ServerStorageContract::DEFAULT_STORAGE_CPU_WORKERS
}
pub const fn default_page_cache_level() -> u8 {
    ServerStorageContract::DEFAULT_PAGE_CACHE_LEVEL
}
pub const fn default_page_cache_max_bytes() -> u64 {
    ServerStorageContract::DEFAULT_PAGE_CACHE_MAX_BYTES
}
pub const fn default_page_cache_memory_reserve() -> u64 {
    ServerStorageContract::DEFAULT_PAGE_CACHE_MEMORY_RESERVE
}
pub const fn default_target_volume_rows() -> usize {
    1_048_576
}
pub const fn default_seal_hot_bytes_threshold() -> usize {
    64 * 1024 * 1024
}
pub const fn default_seal_incremental_hot_bytes_threshold() -> usize {
    16 * 1024 * 1024
}
pub const fn default_read_queue_depth() -> usize {
    1
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerConfigError {
    ZeroPort,
    EmptyDataDirectory,
    ZeroMaxConnections,
    ZeroMaxInflightFrameBytes,
    FrameExceedsGlobalBudget,
    ZeroMaxDatabases,
    ZeroMaxDatabaseNameBytes,
    UnsafeDataDirectory,
    ZeroConnectTimeout,
    ZeroConnectionIdleTimeout,
    ZeroNetReadTimeout,
    ZeroNetWriteTimeout,
    ZeroCursorBatchRows,
    ZeroCursorBatchBytes,
    FrameBelowControlMinimum,
    CursorBatchExceedsFrame,
    ZeroCopyMaxTransactionBytes,
    CompactionJobsOutOfRange,
    PageCacheLevelOutOfRange,
    TargetVolumeRowsTooSmall,
    ZeroSealHotBytesThreshold,
    ZeroSealIncrementalHotBytesThreshold,
    ZeroReadQueueDepth,
}

impl std::fmt::Display for ServerConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroPort => formatter.write_str("server port must not be zero"),
            Self::EmptyDataDirectory => formatter.write_str("server data_dir must not be empty"),
            Self::ZeroMaxConnections => formatter.write_str("max_connections must not be zero"),
            Self::ZeroMaxInflightFrameBytes => formatter.write_str("max_inflight_frame_bytes must not be zero"),
            Self::FrameExceedsGlobalBudget => formatter.write_str("max_frame_bytes must not exceed max_inflight_frame_bytes"),
            Self::ZeroMaxDatabases => formatter.write_str("max_databases must not be zero"),
            Self::ZeroMaxDatabaseNameBytes => formatter.write_str("max_database_name_bytes must not be zero"),
            Self::UnsafeDataDirectory => formatter.write_str("data_dir must be valid UTF-8 and must not contain `?` until the storage config is PathBuf-native"),
            Self::ZeroConnectTimeout => {
                formatter.write_str("connect_timeout_secs must not be zero")
            }
            Self::ZeroConnectionIdleTimeout => {
                formatter.write_str("connection_idle_timeout_secs must not be zero")
            }
            Self::ZeroNetReadTimeout => {
                formatter.write_str("net_read_timeout_secs must not be zero")
            }
            Self::ZeroNetWriteTimeout => {
                formatter.write_str("net_write_timeout_secs must not be zero")
            }
            Self::ZeroCursorBatchRows => {
                formatter.write_str("cursor_batch_max_rows must not be zero")
            }
            Self::ZeroCursorBatchBytes => {
                formatter.write_str("cursor_batch_max_bytes must not be zero")
            }
            Self::FrameBelowControlMinimum => write!(
                formatter,
                "max_frame_bytes must be at least {} bytes",
                crate::protocol::MIN_CONTROL_FRAME_BYTES
            ),
            Self::CursorBatchExceedsFrame => {
                formatter.write_str("cursor_batch_max_bytes must not exceed max_frame_bytes")
            }
            Self::ZeroCopyMaxTransactionBytes => {
                formatter.write_str("copy_max_transaction_bytes must not be zero")
            }
            Self::CompactionJobsOutOfRange => write!(
                formatter,
                "max_compaction_jobs must be in 1..={}",
                ServerStorageContract::MAX_COMPACTION_JOBS
            ),
            Self::PageCacheLevelOutOfRange => write!(
                formatter,
                "page_cache_level must be in 0..={}",
                ServerStorageContract::MAX_PAGE_CACHE_LEVEL
            ),
            Self::TargetVolumeRowsTooSmall => {
                formatter.write_str("target_volume_rows must be at least 65536")
            }
            Self::ZeroSealHotBytesThreshold => {
                formatter.write_str("seal_hot_bytes_threshold must not be zero")
            }
            Self::ZeroSealIncrementalHotBytesThreshold => {
                formatter.write_str("seal_incremental_hot_bytes_threshold must not be zero")
            }
            Self::ZeroReadQueueDepth => {
                formatter.write_str("read_queue_depth must not be zero")
            }
        }
    }
}

impl std::error::Error for ServerConfigError {}

impl ServerConfig {
    pub fn validate(&self) -> Result<(), ServerConfigError> {
        if self.port == 0 {
            return Err(ServerConfigError::ZeroPort);
        }
        self.validate_without_port()
    }

    pub(crate) fn validate_for_ephemeral_bind(&self) -> Result<(), ServerConfigError> {
        self.validate_without_port()
    }

    fn validate_without_port(&self) -> Result<(), ServerConfigError> {
        if self.data_dir.as_os_str().is_empty() {
            return Err(ServerConfigError::EmptyDataDirectory);
        }
        if self.data_dir.to_str().is_none_or(|path| path.contains('?')) {
            return Err(ServerConfigError::UnsafeDataDirectory);
        }
        if self.max_connections == 0 {
            return Err(ServerConfigError::ZeroMaxConnections);
        }
        if self.max_inflight_frame_bytes == 0 {
            return Err(ServerConfigError::ZeroMaxInflightFrameBytes);
        }
        if self.max_databases == 0 {
            return Err(ServerConfigError::ZeroMaxDatabases);
        }
        if self.max_database_name_bytes == 0 {
            return Err(ServerConfigError::ZeroMaxDatabaseNameBytes);
        }
        if self.connect_timeout_secs == 0 {
            return Err(ServerConfigError::ZeroConnectTimeout);
        }
        if self.connection_idle_timeout_secs == 0 {
            return Err(ServerConfigError::ZeroConnectionIdleTimeout);
        }
        if self.net_read_timeout_secs == 0 {
            return Err(ServerConfigError::ZeroNetReadTimeout);
        }
        if self.net_write_timeout_secs == 0 {
            return Err(ServerConfigError::ZeroNetWriteTimeout);
        }
        if self.cursor_batch_max_rows == 0 {
            return Err(ServerConfigError::ZeroCursorBatchRows);
        }
        if self.cursor_batch_max_bytes == 0 {
            return Err(ServerConfigError::ZeroCursorBatchBytes);
        }
        if self.max_frame_bytes < crate::protocol::MIN_CONTROL_FRAME_BYTES {
            return Err(ServerConfigError::FrameBelowControlMinimum);
        }
        if self.max_frame_bytes as usize > self.max_inflight_frame_bytes {
            return Err(ServerConfigError::FrameExceedsGlobalBudget);
        }
        if self.cursor_batch_max_bytes > self.max_frame_bytes as usize {
            return Err(ServerConfigError::CursorBatchExceedsFrame);
        }
        if self.copy_max_transaction_bytes == 0 {
            return Err(ServerConfigError::ZeroCopyMaxTransactionBytes);
        }
        if !(1..=ServerStorageContract::MAX_COMPACTION_JOBS).contains(&self.max_compaction_jobs) {
            return Err(ServerConfigError::CompactionJobsOutOfRange);
        }
        if self.page_cache_level > ServerStorageContract::MAX_PAGE_CACHE_LEVEL {
            return Err(ServerConfigError::PageCacheLevelOutOfRange);
        }
        if self.target_volume_rows < 65_536 {
            return Err(ServerConfigError::TargetVolumeRowsTooSmall);
        }
        if self.seal_hot_bytes_threshold == 0 {
            return Err(ServerConfigError::ZeroSealHotBytesThreshold);
        }
        if self.seal_incremental_hot_bytes_threshold == 0 {
            return Err(ServerConfigError::ZeroSealIncrementalHotBytesThreshold);
        }
        if self.read_queue_depth == 0 {
            return Err(ServerConfigError::ZeroReadQueueDepth);
        }
        Ok(())
    }

    pub fn root_passwordless_is_permitted(&self) -> bool {
        self.authentication.root_password_verifier.is_none()
            && self.bind_ip.is_loopback()
            && matches!(self.transport, ServerTransportConfig::Plaintext)
    }

    pub fn root_password_is_configured(&self) -> bool {
        self.authentication.root_password_verifier.is_some()
    }

    pub(crate) fn root_password_matches(&self, password: &str) -> bool {
        self.authentication
            .root_password_verifier
            .as_ref()
            .is_some_and(|verifier| {
                ServerCredentialContract::verify_password_verifier(verifier.as_str(), password)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_connection_limit_uses_mysql_compatible_default() {
        let config: ServerConfig = toml::from_str(
            r#"
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"
"#,
        )
        .unwrap();

        assert_eq!(config.max_connections, 151);
        assert_eq!(config.connection_idle_timeout_secs, 28_800);
        assert_eq!(
            config.copy_max_transaction_bytes,
            default_copy_max_transaction_bytes()
        );
        assert_eq!(config.max_compaction_jobs, default_max_compaction_jobs());
        assert_eq!(config.storage_cpu_workers, default_storage_cpu_workers());
        assert_eq!(config.page_cache_level, 0);
        assert_eq!(config.page_cache_max_bytes, 0);
        assert_eq!(config.page_cache_memory_reserve, 0);
        assert_eq!(config.target_volume_rows, default_target_volume_rows());
        assert_eq!(
            config.seal_hot_bytes_threshold,
            default_seal_hot_bytes_threshold()
        );
        assert_eq!(
            config.seal_incremental_hot_bytes_threshold,
            default_seal_incremental_hot_bytes_threshold()
        );
        assert_eq!(config.read_queue_depth, 1);
        assert_eq!(config.transport, ServerTransportConfig::Plaintext);
        assert_eq!(config.authentication, ServerAuthenticationConfig::default());
        assert!(config.root_passwordless_is_permitted());
    }

    #[test]
    fn configured_root_verifier_is_validated_and_redacted() {
        let encoded = ServerCredentialContract::hash_password_verifier("root-secret")
            .expect("derive verifier");
        let config: ServerConfig = toml::from_str(&format!(
            r#"
bind_ip = "0.0.0.0"
port = 5440
data_dir = "/tmp/radixdb-server"

[authentication]
root_password_verifier = {encoded:?}
"#
        ))
        .expect("parse configured root verifier");

        assert!(config.root_password_is_configured());
        assert!(!config.root_passwordless_is_permitted());
        assert!(config.root_password_matches("root-secret"));
        assert!(!config.root_password_matches("wrong"));
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains(&encoded));
    }

    #[test]
    fn configured_root_verifier_uses_the_documented_server_file_section() {
        let encoded = ServerCredentialContract::hash_password_verifier("root-secret")
            .expect("derive verifier");
        let file = parse_server_config_file(&format!(
            r#"
[server]
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"

[server.authentication]
root_password_verifier = {encoded:?}
"#
        ))
        .expect("parse server authentication section");

        assert!(file.server.root_password_is_configured());
        assert!(file.server.root_password_matches("root-secret"));
    }

    #[test]
    fn malformed_or_non_argon2id_root_verifier_is_rejected() {
        for encoded in [
            "not-a-phc-string",
            "$argon2i$v=19$m=4096,t=3,p=1$c2FsdA$YWJj",
        ] {
            let source = format!(
                r#"
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"

[authentication]
root_password_verifier = {encoded:?}
"#
            );
            assert!(toml::from_str::<ServerConfig>(&source).is_err());
        }
    }

    #[test]
    fn plaintext_is_an_explicitly_supported_remote_password_transport() {
        let config: ServerConfig = toml::from_str(
            r#"
bind_ip = "0.0.0.0"
port = 5440
data_dir = "/tmp/radixdb-server"

[transport]
mode = "plaintext"
"#,
        )
        .expect("parse remote plaintext endpoint");

        assert_eq!(config.validate(), Ok(()));
        assert_eq!(config.transport, ServerTransportConfig::Plaintext);
        assert!(
            !config.root_passwordless_is_permitted(),
            "remote plaintext remains available to database/login/password authentication, not bootstrap root"
        );
    }

    #[test]
    fn tls_material_is_required_only_when_tls_mode_is_selected() {
        let config: ServerConfig = toml::from_str(
            r#"
bind_ip = "0.0.0.0"
port = 5440
data_dir = "/tmp/radixdb-server"

[transport]
mode = "tls"
certificate_chain = "/etc/radixdb/server-chain.pem"
private_key = "/etc/radixdb/server-key.pem"
"#,
        )
        .expect("parse explicit TLS endpoint");

        assert!(matches!(
            config.transport,
            ServerTransportConfig::Tls { .. }
        ));
        assert_eq!(config.validate(), Ok(()));
        assert!(!config.root_passwordless_is_permitted());
    }

    #[test]
    fn documented_server_section_is_the_file_contract() {
        let file: ServerConfigFile = toml::from_str(
            r#"
[server]
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"
"#,
        )
        .unwrap();

        assert_eq!(file.server.port, 5440);
        assert_eq!(file.server.max_connections, 151);
        assert!(file.plugins.package_directories.is_empty());
        assert!(toml::from_str::<ServerConfigFile>(
            r#"
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"
"#,
        )
        .is_err());
    }

    #[test]
    fn plugin_allowlist_is_an_explicit_top_level_section() {
        let file = parse_server_config_file(
            r#"
[server]
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"

[plugins]
package_directories = ["/opt/radixdb/plugins/spatial"]
"#,
        )
        .unwrap();

        assert_eq!(
            file.plugins.package_directories,
            vec![PathBuf::from("/opt/radixdb/plugins/spatial")]
        );
    }

    #[test]
    fn explicit_persistence_knobs_are_accepted() {
        let config: ServerConfig = toml::from_str(
            r#"
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"
target_volume_rows = 131072
copy_max_transaction_bytes = 16777216
max_compaction_jobs = 4
storage_cpu_workers = 3
page_cache_level = 5
page_cache_max_bytes = 1073741824
page_cache_memory_reserve = 536870912
seal_hot_bytes_threshold = 1048576
seal_incremental_hot_bytes_threshold = 262144
read_queue_depth = 8
"#,
        )
        .unwrap();

        assert_eq!(config.target_volume_rows, 131_072);
        assert_eq!(config.copy_max_transaction_bytes, 16_777_216);
        assert_eq!(config.max_compaction_jobs, 4);
        assert_eq!(config.storage_cpu_workers, 3);
        assert_eq!(config.page_cache_level, 5);
        assert_eq!(config.page_cache_max_bytes, 1_073_741_824);
        assert_eq!(config.page_cache_memory_reserve, 536_870_912);
        assert_eq!(config.seal_hot_bytes_threshold, 1_048_576);
        assert_eq!(config.seal_incremental_hot_bytes_threshold, 262_144);
        assert_eq!(config.read_queue_depth, 8);
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn zero_copy_transaction_budget_is_rejected() {
        let config: ServerConfig = toml::from_str(
            r#"
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"
copy_max_transaction_bytes = 0
"#,
        )
        .unwrap();

        assert_eq!(
            config.validate(),
            Err(ServerConfigError::ZeroCopyMaxTransactionBytes)
        );
    }

    #[test]
    fn compaction_worker_limit_is_fail_closed() {
        let mut config: ServerConfig = toml::from_str(
            r#"
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"
"#,
        )
        .unwrap();

        config.max_compaction_jobs = 0;
        assert_eq!(
            config.validate(),
            Err(ServerConfigError::CompactionJobsOutOfRange)
        );
        config.max_compaction_jobs = ServerStorageContract::MAX_COMPACTION_JOBS + 1;
        assert_eq!(
            config.validate(),
            Err(ServerConfigError::CompactionJobsOutOfRange)
        );
    }

    #[test]
    fn page_cache_level_is_fail_closed() {
        let config: ServerConfig = toml::from_str(
            r#"
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"
page_cache_level = 11
"#,
        )
        .unwrap();
        assert_eq!(
            config.validate(),
            Err(ServerConfigError::PageCacheLevelOutOfRange)
        );
    }

    #[test]
    fn configuration_rejects_unknown_and_retired_keys() {
        for key in [
            "max_conections = 12",
            "scan_prefetch_cache_bytes = 1048576",
            "block_cache_bytes = 2097152",
            "compression_threshold = 64",
        ] {
            let source = format!(
                r#"
[server]
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"
{key}
"#
            );
            assert!(
                toml::from_str::<ServerConfigFile>(&source).is_err(),
                "server config key must fail closed: {key}"
            );
        }
    }

    #[test]
    fn zero_connection_limit_is_rejected() {
        let mut config: ServerConfig = toml::from_str(
            r#"
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"
"#,
        )
        .unwrap();
        config.max_connections = 0;

        assert_eq!(
            config.validate(),
            Err(ServerConfigError::ZeroMaxConnections)
        );
    }

    #[test]
    fn zero_connect_timeout_is_rejected() {
        let mut config: ServerConfig = toml::from_str(
            r#"
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"
"#,
        )
        .unwrap();
        config.connect_timeout_secs = 0;

        assert_eq!(
            config.validate(),
            Err(ServerConfigError::ZeroConnectTimeout)
        );
    }

    #[test]
    fn zero_session_socket_timeouts_are_rejected() {
        let config: ServerConfig = toml::from_str(
            r#"
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"
"#,
        )
        .unwrap();
        let mut idle = config.clone();
        idle.connection_idle_timeout_secs = 0;
        let mut read = config.clone();
        read.net_read_timeout_secs = 0;
        let mut write = config;
        write.net_write_timeout_secs = 0;

        assert_eq!(
            idle.validate(),
            Err(ServerConfigError::ZeroConnectionIdleTimeout)
        );
        assert_eq!(read.validate(), Err(ServerConfigError::ZeroNetReadTimeout));
        assert_eq!(
            write.validate(),
            Err(ServerConfigError::ZeroNetWriteTimeout)
        );
    }

    #[test]
    fn zero_read_queue_depth_is_rejected() {
        let mut config: ServerConfig = toml::from_str(
            r#"
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"
"#,
        )
        .unwrap();
        config.read_queue_depth = 0;

        assert_eq!(
            config.validate(),
            Err(ServerConfigError::ZeroReadQueueDepth)
        );
    }

    #[test]
    fn invalid_persistence_knobs_are_rejected() {
        let mut config: ServerConfig = toml::from_str(
            r#"
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"
"#,
        )
        .unwrap();

        config.target_volume_rows = 65_535;
        assert_eq!(
            config.validate(),
            Err(ServerConfigError::TargetVolumeRowsTooSmall)
        );

        config.target_volume_rows = 65_536;
        config.seal_hot_bytes_threshold = 0;
        assert_eq!(
            config.validate(),
            Err(ServerConfigError::ZeroSealHotBytesThreshold)
        );

        config.seal_hot_bytes_threshold = 1;
        config.seal_incremental_hot_bytes_threshold = 0;
        assert_eq!(
            config.validate(),
            Err(ServerConfigError::ZeroSealIncrementalHotBytesThreshold)
        );
    }

    #[test]
    fn cursor_batch_budget_cannot_exceed_frame_budget() {
        let mut config: ServerConfig = toml::from_str(
            r#"
bind_ip = "127.0.0.1"
port = 5440
data_dir = "/tmp/radixdb-server"
"#,
        )
        .unwrap();
        config.max_frame_bytes = 1024;
        config.cursor_batch_max_bytes = 1025;
        assert_eq!(
            config.validate(),
            Err(ServerConfigError::CursorBatchExceedsFrame)
        );
    }
}
