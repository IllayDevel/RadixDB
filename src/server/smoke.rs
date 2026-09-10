use std::time::Duration;

use radixdb_client::Connection;

/// Execute the release smoke contract against one TCP endpoint.
///
/// Argument parsing, presentation and process exit mapping remain in the
/// binary entrypoint; this function owns the negotiated protocol checks.
pub fn probe_smoke_endpoint(endpoint: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut connection = Connection::connect_with_timeouts(
        endpoint,
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
    )?;
    connection.authenticate("root", None)?;
    let status = connection.server_status()?;
    connection.shutdown()?;
    let build = status
        .build
        .ok_or("server did not publish negotiated build identity")?;
    if !status.ready {
        return Err(format!("server is not ready: {}", status.message).into());
    }
    Ok(format!(
        "ready version={} protocol={} state={:?}",
        build.semantic_version, build.protocol_version, status.lifecycle
    ))
}
