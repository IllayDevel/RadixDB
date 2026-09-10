pub(crate) fn version_info() -> String {
    format!(
        "radixdb {} (commit: {}, built: {})",
        env!("CARGO_PKG_VERSION"),
        option_env!("RADIXDB_GIT_COMMIT").unwrap_or("unknown"),
        option_env!("RADIXDB_BUILD_TIME").unwrap_or("unknown")
    )
}
