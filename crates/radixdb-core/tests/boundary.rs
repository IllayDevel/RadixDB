use std::fs;
use std::path::PathBuf;

#[test]
fn core_manifest_has_no_radixdb_dependencies() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let text = fs::read_to_string(&manifest).expect("read radixdb-core manifest");

    let mut in_dependency_section = false;
    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.starts_with('[') {
            in_dependency_section = matches!(
                line,
                "[dependencies]" | "[dev-dependencies]" | "[build-dependencies]"
            );
            continue;
        }
        if in_dependency_section && !line.is_empty() && !line.starts_with('#') {
            let dependency_name = line.split_once('=').map_or(line, |(name, _)| name).trim();
            assert!(
                !dependency_name.starts_with("radixdb"),
                "radixdb-core must not depend on another RadixDB crate, found `{line}`"
            );
        }
    }
}

#[test]
fn core_crate_is_an_internal_workspace_member() {
    assert_eq!(env!("CARGO_PKG_NAME"), "radixdb-core");

    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let text = fs::read_to_string(&manifest).expect("read radixdb-core manifest");
    assert!(text.lines().any(|line| line.trim() == "publish = false"));

    let workspace_manifest = manifest
        .parent()
        .and_then(|path| path.parent())
        .and_then(|path| path.parent())
        .expect("radixdb-core lives below the workspace root")
        .join("Cargo.toml");
    let workspace = fs::read_to_string(workspace_manifest).expect("read workspace manifest");
    assert!(workspace.contains("\"crates/radixdb-core\""));
}

#[test]
fn root_core_is_a_direct_alias_without_a_mirror_tree() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("radixdb-core lives below the workspace root");
    let root = fs::read_to_string(workspace.join("src/lib.rs")).expect("read root facade");
    assert!(root.contains("pub use radixdb_core as core;"));
    assert!(!workspace.join("src/core").exists());
}

#[test]
fn root_common_cow_btree_uses_the_canonical_module_directly() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("radixdb-core lives below the workspace root");
    let source =
        fs::read_to_string(workspace.join("src/common/mod.rs")).expect("read root common module");

    assert!(source.contains("pub use radixdb_core::{"));
    assert!(source.contains("cow_btree"));
    assert!(!workspace.join("src/common/cow_btree.rs").exists());
}
