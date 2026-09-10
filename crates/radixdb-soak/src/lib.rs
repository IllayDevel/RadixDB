pub mod artifacts;
pub mod auth;
pub mod comparison;
pub mod config;
pub mod database;
pub mod diagnostics;
pub mod http;
pub mod metrics;
pub mod runner;
pub mod runtime;
pub mod status;
pub mod workload;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const GIT_COMMIT: &str = env!("RADIXDB_SOAK_GIT_COMMIT");
pub const BUILD_PROFILE: &str = env!("RADIXDB_SOAK_BUILD_PROFILE");
pub const BUILD_TARGET: &str = env!("RADIXDB_SOAK_BUILD_TARGET");
pub const CARGO_LOCK_SHA256: &str = env!("RADIXDB_SOAK_CARGO_LOCK_SHA256");

pub fn build_identity() -> String {
    format!(
        "radixdb-soak {} git={} protocol={} profile={} target={} lock={}",
        VERSION,
        GIT_COMMIT,
        radixdb_client::PROTOCOL_VERSION,
        BUILD_PROFILE,
        BUILD_TARGET,
        CARGO_LOCK_SHA256
    )
}
