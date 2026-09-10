use std::fs;
use std::path::{Path, PathBuf};

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn workspace_dir() -> PathBuf {
    crate_dir()
        .parent()
        .and_then(Path::parent)
        .expect("procedural crate lives below the workspace root")
        .to_owned()
}

fn rust_sources_below(path: &Path, output: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(path).expect("read source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            rust_sources_below(&path, output);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push(path);
        }
    }
}

#[test]
fn procedural_crate_has_only_the_approved_lower_dependencies() {
    let manifest = fs::read_to_string(crate_dir().join("Cargo.toml")).expect("read manifest");
    let internal_dependencies: Vec<_> = manifest
        .lines()
        .filter(|line| line.trim_start().starts_with("radixdb-"))
        .collect();
    assert_eq!(
        internal_dependencies,
        [
            "radixdb-catalog = { path = \"../radixdb-catalog\" }",
            "radixdb-core = { path = \"../radixdb-core\" }",
            "radixdb-sql = { path = \"../radixdb-sql\" }",
        ]
    );
}

#[test]
fn sql_crate_has_no_reverse_dependency_on_procedural_runtime() {
    let manifest = fs::read_to_string(workspace_dir().join("crates/radixdb-sql/Cargo.toml"))
        .expect("read SQL manifest");
    assert!(!manifest.contains("radixdb-procedural"));
}

#[test]
fn source_ownership_starts_after_sql_parsing() {
    let source = fs::read_to_string(crate_dir().join("src/lib.rs")).expect("read facade");
    assert!(!source.contains("pub struct Lexer"));
    assert!(!source.contains("pub fn parse_sql"));
    assert!(source.contains("pub mod ir"));
    assert!(source.contains("pub mod runtime"));
}

#[test]
fn runtime_has_no_external_io_capability() {
    let mut sources = Vec::new();
    rust_sources_below(&crate_dir().join("src"), &mut sources);
    for path in sources {
        let source = fs::read_to_string(&path).expect("read production source");
        for forbidden in [
            "std::fs",
            "std::net",
            "std::process",
            "TcpStream",
            "UdpSocket",
            "tokio::net",
            "reqwest",
            "ureq",
        ] {
            assert!(
                !source.contains(forbidden),
                "procedural runtime gained external I/O capability {forbidden} in {}",
                path.display()
            );
        }
    }
}

#[test]
fn foundation_is_split_into_bounded_responsibility_modules() {
    for relative in [
        "src/budget.rs",
        "src/diagnostic.rs",
        "src/host.rs",
        "src/value.rs",
        "src/ir/model.rs",
        "src/ir/verify.rs",
        "src/runtime/interpreter.rs",
    ] {
        let source = fs::read_to_string(crate_dir().join(relative)).expect("read source module");
        assert!(
            source.lines().count() <= 750,
            "procedural module {relative} exceeded the foundation boundary"
        );
    }
}
