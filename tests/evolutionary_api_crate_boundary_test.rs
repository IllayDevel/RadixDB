use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

fn rust_sources_below(directory: &Path, sources: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("read API source directory") {
        let path = entry.expect("read API source entry").path();
        if path.is_dir() {
            rust_sources_below(&path, sources);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            sources.push(path);
        }
    }
}

#[test]
fn api_manifest_has_only_the_measured_internal_dependencies() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest =
        fs::read_to_string(root.join("crates/radixdb-api/Cargo.toml")).expect("read API manifest");
    let dependencies = manifest
        .lines()
        .filter_map(|line| line.split_once('=').map(|(name, _)| name.trim()))
        .filter(|name| name.starts_with("radixdb-"))
        .collect::<BTreeSet<_>>();

    assert_eq!(
        dependencies,
        BTreeSet::from([
            "radixdb-catalog",
            "radixdb-core",
            "radixdb-executor",
            "radixdb-orm",
            "radixdb-storage",
        ])
    );
    assert!(manifest
        .lines()
        .any(|line| line.trim() == "publish = false"));
}

#[test]
fn api_sources_do_not_reach_upward_into_runtime_or_transport() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    rust_sources_below(&root.join("crates/radixdb-api/src"), &mut sources);
    sources.sort();

    let forbidden = [
        "crate::server",
        "crate::cli",
        "crate::sql_dump",
        "radixdb_client::",
        "radixdb_protocol::",
    ];
    let mut violations = Vec::new();
    for path in sources {
        let source = fs::read_to_string(&path).expect("read API source");
        for (line_index, line) in source.lines().enumerate() {
            if forbidden.iter().any(|needle| line.contains(needle)) {
                violations.push(format!("{}:{}: {line}", path.display(), line_index + 1));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "API owner imports an upward runtime/transport layer:\n{}",
        violations.join("\n")
    );
}

#[test]
fn root_api_is_one_identity_preserving_reexport() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let facade = fs::read_to_string(root.join("src/lib.rs")).expect("read root facade");

    assert!(facade.contains("pub use radixdb_api as api;"));
    assert!(!root.join("src/api").exists());
    for duplicate in [
        "pub struct Database",
        "pub struct Statement",
        "pub struct Transaction",
        "pub struct Rows",
    ] {
        assert!(!facade.contains(duplicate));
    }

    assert_eq!(
        std::any::TypeId::of::<radixdb::Database>(),
        std::any::TypeId::of::<radixdb_api::Database>()
    );
    assert_eq!(
        std::any::TypeId::of::<radixdb::api::Rows>(),
        std::any::TypeId::of::<radixdb_api::Rows>()
    );
}
