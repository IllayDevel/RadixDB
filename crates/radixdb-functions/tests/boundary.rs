use std::fs;
use std::path::PathBuf;

#[test]
fn functions_contract_crate_has_only_the_allowed_internal_dependency() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest = fs::read_to_string(crate_dir.join("Cargo.toml")).expect("read manifest");
    let manifest: toml::Value = manifest.parse().expect("parse manifest");
    let dependencies = manifest["dependencies"].as_table().expect("dependencies");
    for section in ["dev-dependencies", "build-dependencies"] {
        if let Some(table) = manifest.get(section).and_then(toml::Value::as_table) {
            assert!(
                table.keys().all(|name| !name.starts_with("radixdb")),
                "unexpected internal dependency in {section}"
            );
        }
    }
    let internal: Vec<_> = dependencies
        .keys()
        .filter(|name| name.starts_with("radixdb"))
        .map(String::as_str)
        .collect();
    assert_eq!(internal, ["radixdb-core"]);
    assert_eq!(
        dependencies["radixdb-core"]["path"].as_str(),
        Some("../radixdb-core")
    );
    assert_eq!(
        dependencies["radixdb-core"]["version"].as_str(),
        Some(concat!("=", env!("CARGO_PKG_VERSION")))
    );
}

#[test]
fn root_function_module_does_not_redefine_canonical_contracts() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("functions crate lives below workspace root");
    let source = fs::read_to_string(workspace.join("src/lib.rs")).expect("read root facade");

    assert!(source.contains("pub use radixdb_functions as functions;"));
    assert!(!workspace.join("src/functions").exists());
    for declaration in [
        "pub trait AggregateFunction",
        "pub trait ScalarFunction",
        "pub trait WindowFunction",
        "pub struct FunctionInfo",
        "pub struct FunctionSignature",
        "pub enum FunctionType",
    ] {
        assert!(
            !source.contains(declaration),
            "duplicate owner: {declaration}"
        );
    }
}

#[test]
fn scalar_implementations_have_one_bounded_owner() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("functions crate lives below workspace root");
    assert!(!workspace.join("src/functions").exists());

    for name in [
        "conversion.rs",
        "datetime.rs",
        "hash.rs",
        "math.rs",
        "semantic.rs",
        "string.rs",
        "utility.rs",
        "vector.rs",
    ] {
        let source = fs::read_to_string(crate_dir.join("src/scalar").join(name))
            .expect("read canonical scalar owner");
        assert!(
            source.lines().count() <= 2_000,
            "scalar module {name} exceeds the migration corridor"
        );
        assert!(!source.contains("crate::executor"));
    }
}

#[test]
fn aggregate_implementations_have_one_bounded_owner() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("functions crate lives below workspace root");
    assert!(!workspace.join("src/functions").exists());

    for entry in fs::read_dir(crate_dir.join("src/aggregate")).expect("read aggregate owners") {
        let path = entry.expect("read aggregate entry").path();
        if path.extension().is_none_or(|extension| extension != "rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("read canonical aggregate owner");
        assert!(
            source.lines().count() <= 2_000,
            "aggregate module {} exceeds the migration corridor",
            path.display()
        );
        assert!(
            !source.contains("crate::executor"),
            "aggregate owner {} depends on executor",
            path.display()
        );
    }
}

#[test]
fn window_and_tvf_implementations_have_one_bounded_owner() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("functions crate lives below workspace root");

    assert!(!workspace.join("src/functions").exists());

    let mut owners: Vec<_> = fs::read_dir(crate_dir.join("src/window"))
        .expect("read window owners")
        .map(|entry| entry.expect("read window entry").path())
        .collect();
    owners.push(crate_dir.join("src/tvf.rs"));
    for path in owners {
        let source = fs::read_to_string(&path).expect("read canonical function owner");
        assert!(
            source.lines().count() <= 2_000,
            "function module {} exceeds the migration corridor",
            path.display()
        );
        assert!(
            !source.contains("crate::executor"),
            "function owner {} depends on executor",
            path.display()
        );
    }
}

#[test]
fn registry_has_one_bounded_owner_and_no_higher_layer_dependency() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("functions crate lives below workspace root");

    let facade = fs::read_to_string(workspace.join("src/lib.rs")).expect("read root facade");
    assert!(facade.contains("pub use radixdb_functions as functions;"));
    assert!(!workspace.join("src/functions").exists());

    let registry =
        fs::read_to_string(crate_dir.join("src/registry.rs")).expect("read registry owner");
    assert!(registry.lines().count() <= 2_000);
    for forbidden in [
        "crate::executor",
        "radixdb_executor",
        "radixdb_sql",
        "radixdb_storage",
    ] {
        assert!(
            !registry.contains(forbidden),
            "registry depends on forbidden higher owner: {forbidden}"
        );
    }
}
