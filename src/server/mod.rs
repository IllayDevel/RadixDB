//! TCP server layer for the binary protocol.

mod config;
mod identity;
mod job_scheduler;
mod launcher;
mod session;
mod smoke;
mod tcp_server;
mod value_codec;

pub use config::{
    default_copy_max_transaction_bytes, default_max_compaction_jobs,
    default_max_database_name_bytes, default_max_databases, default_max_inflight_frame_bytes,
    default_page_cache_level, default_page_cache_max_bytes, default_page_cache_memory_reserve,
    default_seal_hot_bytes_threshold, default_seal_incremental_hot_bytes_threshold,
    default_storage_cpu_workers, default_target_volume_rows, parse_server_config_file,
    RootPasswordVerifier, ServerAuthenticationConfig, ServerConfig, ServerConfigError,
    ServerConfigFile, ServerTransportConfig,
};
pub use identity::{build_identity, version_line};
pub use launcher::{help_text, run_configured_server, run_configured_server_from_env};
pub use radixdb_plugin_host::{PluginHostConfig, PluginRegistryStatus};
pub use session::ServerSessionError;
pub use smoke::probe_smoke_endpoint;
pub use tcp_server::{Server, ServerStartError, TlsRuntimeStatus};
