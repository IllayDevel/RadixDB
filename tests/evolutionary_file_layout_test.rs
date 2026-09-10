use std::fs;
use std::path::{Path, PathBuf};

fn rust_sources(directory: &Path, output: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("read source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            rust_sources(&path, output);
        } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
            output.push(path);
        }
    }
}

fn is_test_file(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("");
    name == "tests.rs"
        || name.starts_with("test_")
        || name.ends_with("_test.rs")
        || path
            .components()
            .any(|component| component.as_os_str() == "tests")
}

#[test]
fn every_production_file_obeys_its_documented_size_ceiling() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    rust_sources(&root.join("src"), &mut sources);
    rust_sources(&root.join("crates"), &mut sources);
    sources.sort();

    let mut oversized = Vec::new();
    for path in sources.into_iter().filter(|path| !is_test_file(path)) {
        let relative = path.strip_prefix(root).expect("workspace-relative path");
        let ceiling = match relative.to_str().expect("UTF-8 source path") {
            "crates/radixdb-storage/src/volume/io.rs" => 3_396,
            "crates/radixdb-executor/src/expression/vm.rs" => 3_324,
            "crates/radixdb-storage/src/volume/format.rs" => 3_033,
            _ => 3_000,
        };
        let lines = fs::read_to_string(&path)
            .expect("read production source")
            .lines()
            .count();
        if lines > ceiling {
            oversized.push(format!(
                "{}: {lines} lines (ceiling {ceiling})",
                relative.display()
            ));
        }
    }

    assert!(
        oversized.is_empty(),
        "production files exceeded their owner ceilings:\n{}",
        oversized.join("\n")
    );
}

#[test]
fn benchmark_harness_remains_functionally_split() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let entrypoint = fs::read_to_string(root.join("src/bin/radixdb_bench.rs"))
        .expect("read benchmark entrypoint");
    assert!(entrypoint.lines().count() <= 400);

    for module in [
        "command.rs",
        "metrics.rs",
        "fixture.rs",
        "relational.rs",
        "server.rs",
        "tests.rs",
    ] {
        assert!(
            entrypoint.contains(&format!("include!(\"radixdb_bench/{module}\");")),
            "benchmark entrypoint omits {module}"
        );
        assert!(root.join("src/bin/radixdb_bench").join(module).is_file());
    }
}
