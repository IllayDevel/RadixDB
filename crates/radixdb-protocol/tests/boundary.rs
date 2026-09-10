use std::fs;
use std::path::PathBuf;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("protocol crate lives below the workspace root")
        .to_path_buf()
}

#[test]
fn protocol_is_a_neutral_private_workspace_member() {
    assert_eq!(env!("CARGO_PKG_NAME"), "radixdb-protocol");

    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest = fs::read_to_string(crate_dir.join("Cargo.toml")).expect("read manifest");
    assert!(manifest
        .lines()
        .any(|line| line.trim() == "publish = false"));
    assert!(!manifest
        .lines()
        .any(|line| { line.trim_start().starts_with("radixdb-") && line.contains('=') }));

    let workspace =
        fs::read_to_string(workspace_root().join("Cargo.toml")).expect("read workspace manifest");
    assert!(workspace.contains("\"crates/radixdb-protocol\""));
}

#[test]
fn wire_contract_has_one_owner_and_identity_preserving_facades() {
    let root = workspace_root();
    let owner = fs::read_to_string(root.join("crates/radixdb-protocol/src/lib.rs"))
        .expect("read protocol owner");
    let client = fs::read_to_string(root.join("crates/radixdb-client/src/protocol.rs"))
        .expect("read client protocol facade");
    let facade = fs::read_to_string(root.join("src/lib.rs")).expect("read root facade");

    assert!(owner.contains("pub enum ClientMessage"));
    assert!(owner.contains("pub enum ServerMessage"));
    assert!(owner.contains("pub enum WireValue"));
    assert!(owner.contains("pub fn read_frame"));
    assert!(client.contains("pub use radixdb_protocol::*;"));
    assert!(facade.contains("pub use radixdb_protocol as protocol;"));
    assert!(!root.join("src/protocol.rs").exists());
    assert!(!client.contains("pub enum ClientMessage"));
    assert!(!facade.contains("pub enum ClientMessage"));
}

#[test]
fn server_contract_sources_do_not_depend_on_client_transport() {
    let root = workspace_root();
    for source in [
        "src/server/config.rs",
        "src/server/identity.rs",
        "src/server/session.rs",
        "src/server/tcp_server.rs",
    ] {
        let text = fs::read_to_string(root.join(source)).expect("read server source");
        assert!(
            !text.contains("radixdb_client::"),
            "{source} reaches the client transport instead of the neutral protocol"
        );
    }
}
