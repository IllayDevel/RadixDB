use std::{fs, path::Path};

fn section(manifest: &str, name: &str) -> String {
    manifest
        .split_once(name)
        .map(|(_, tail)| {
            tail.split('\n')
                .take_while(|line| !line.starts_with('['))
                .collect()
        })
        .unwrap_or_default()
}

fn assert_no_private_owner(manifest: &Path) {
    let source = fs::read_to_string(manifest).expect("read Cargo.toml");
    let dependencies = section(&source, "[dependencies]");
    for private in [
        "radixdb-storage",
        "radixdb-executor",
        "radixdb-catalog",
        "radixdb-core",
        "radixdb-wal",
    ] {
        assert!(
            !dependencies.contains(private),
            "{} imports private owner {private}",
            manifest.display()
        );
    }
}

#[test]
fn public_abi_has_no_storage_wal_mvcc_catalog_or_transaction_handle() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let raw = fs::read_to_string(crate_root.join("src/raw.rs")).unwrap();
    let descriptor = fs::read_to_string(crate_root.join("src/descriptor.rs")).unwrap();
    let public_surface = format!("{raw}\n{descriptor}").to_ascii_lowercase();

    for forbidden in [
        "storagehandle",
        "storage_handle",
        "walhandle",
        "wal_handle",
        "mvcchandle",
        "mvcc_handle",
        "cataloghandle",
        "catalog_handle",
        "transactionhandle",
        "transaction_handle",
        "pagehandle",
        "page_handle",
        "commit_transaction",
        "rollback_transaction",
        "write_page",
        "append_wal",
        "mutate_catalog",
    ] {
        assert!(
            !public_surface.contains(forbidden),
            "public ABI exposes forbidden capability {forbidden}"
        );
    }

    let host_start = raw.find("pub struct RadixHostApiV1").unwrap();
    let host_tail = &raw[host_start..];
    let host_end = host_tail.find("\n}").unwrap();
    let host = &host_tail[..=host_end + 1];
    assert_eq!(
        host,
        "pub struct RadixHostApiV1 {\n    pub header: RadixAbiHeaderV1,\n    pub handle: u64,\n    pub max_external_value_bytes: u32,\n    pub max_batch_rows: u32,\n    pub max_planner_spans: u32,\n    pub reserved: u32,\n    pub log: Option<RadixAbiHostLogFnV1>,\n}"
    );
}

#[test]
fn public_plugin_crates_have_no_private_engine_dependency() {
    let abi = Path::new(env!("CARGO_MANIFEST_DIR"));
    let crates = abi.parent().expect("ABI crate is inside crates/");
    for manifest in [
        abi.join("Cargo.toml"),
        crates.join("radixdb-plugin/Cargo.toml"),
        crates.join("radixdb-plugin-macros/Cargo.toml"),
        crates.join("radixdb-spatial/Cargo.toml"),
    ] {
        assert_no_private_owner(&manifest);
    }
}
