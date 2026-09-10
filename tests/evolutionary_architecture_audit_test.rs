use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

fn internal_dependencies(manifest: &Path) -> BTreeSet<String> {
    fs::read_to_string(manifest)
        .expect("read crate manifest")
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("radixdb-") && line.contains("path"))
        .map(|line| {
            line.split_once('=')
                .expect("path dependency assignment")
                .0
                .trim()
                .to_owned()
        })
        .collect()
}

fn visit(
    crate_name: &str,
    graph: &BTreeMap<String, BTreeSet<String>>,
    visiting: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
) {
    if visited.contains(crate_name) {
        return;
    }
    assert!(
        visiting.insert(crate_name.to_owned()),
        "cycle detected at {crate_name}"
    );
    for dependency in &graph[crate_name] {
        visit(dependency, graph, visiting, visited);
    }
    visiting.remove(crate_name);
    visited.insert(crate_name.to_owned());
}

#[test]
fn product_crate_graph_matches_the_approved_acyclic_architecture() {
    let expected = BTreeMap::from([
        ("radixdb-core", BTreeSet::new()),
        ("radixdb-catalog", BTreeSet::from(["radixdb-core"])),
        ("radixdb-sql", BTreeSet::from(["radixdb-core"])),
        (
            "radixdb-procedural",
            BTreeSet::from(["radixdb-catalog", "radixdb-core", "radixdb-sql"]),
        ),
        ("radixdb-functions", BTreeSet::from(["radixdb-core"])),
        ("radixdb-plugin-abi", BTreeSet::new()),
        (
            "radixdb-plugin-host",
            BTreeSet::from(["radixdb-core", "radixdb-plugin-abi"]),
        ),
        (
            "radixdb-storage",
            BTreeSet::from(["radixdb-catalog", "radixdb-core"]),
        ),
        ("radixdb-orm", BTreeSet::from(["radixdb-core"])),
        (
            "radixdb-executor",
            BTreeSet::from([
                "radixdb-catalog",
                "radixdb-core",
                "radixdb-functions",
                "radixdb-plugin-abi",
                "radixdb-plugin-host",
                "radixdb-procedural",
                "radixdb-sql",
                "radixdb-storage",
            ]),
        ),
        (
            "radixdb-api",
            BTreeSet::from([
                "radixdb-catalog",
                "radixdb-core",
                "radixdb-executor",
                "radixdb-orm",
                "radixdb-storage",
            ]),
        ),
        ("radixdb-protocol", BTreeSet::new()),
        (
            "radixdb-client",
            BTreeSet::from(["radixdb-orm", "radixdb-protocol"]),
        ),
    ]);

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let actual = expected
        .keys()
        .map(|crate_name| {
            let manifest = root.join("crates").join(crate_name).join("Cargo.toml");
            ((*crate_name).to_owned(), internal_dependencies(&manifest))
        })
        .collect::<BTreeMap<_, _>>();

    let expected_owned = expected
        .iter()
        .map(|(name, dependencies)| {
            (
                (*name).to_owned(),
                dependencies
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect(),
            )
        })
        .collect::<BTreeMap<String, BTreeSet<String>>>();
    assert_eq!(actual, expected_owned);

    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for crate_name in actual.keys() {
        visit(crate_name, &actual, &mut visiting, &mut visited);
    }
}

#[test]
fn completed_executor_cutover_has_no_stale_root_migration_promises() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let executor = root.join("crates/radixdb-executor/src");
    for relative in [
        "dispatch/program.rs",
        "dispatch/statement.rs",
        "executor_host.rs",
        "index_optimizer.rs",
        "index_optimizer_host.rs",
        "mutation/host.rs",
        "mutation_host.rs",
        "navigation/mod.rs",
        "subquery/mod.rs",
        "cte/mod.rs",
        "aggregation/mod.rs",
    ] {
        let source = fs::read_to_string(executor.join(relative)).expect("read executor source");
        for stale in [
            "while SELECT owners still live in the root",
            "until SELECT cutover",
            "while the SELECT pipeline still lives in",
            "moving in EVO-62",
        ] {
            assert!(
                !source.contains(stale),
                "{relative} retains stale migration contract {stale:?}"
            );
        }
    }
}

#[test]
fn root_facade_has_no_mirror_module_trees_or_leaf_files() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "src/api",
        "src/core",
        "src/executor",
        "src/functions",
        "src/optimizer",
        "src/parser",
        "src/storage",
        "src/client.rs",
        "src/protocol.rs",
        "src/test_failpoints.rs",
        "src/common/buffer_pool.rs",
        "src/common/compact_arc.rs",
        "src/common/compact_vec.rs",
        "src/common/cow_btree.rs",
        "src/common/i64_map.rs",
        "src/common/smart_string.rs",
        "src/common/time_compat.rs",
    ] {
        assert!(
            !root.join(relative).exists(),
            "facade-only path returned: {relative}"
        );
    }

    let library = fs::read_to_string(root.join("src/lib.rs")).expect("read root facade");
    for declaration in [
        "pub use radixdb_api as api;",
        "pub use radixdb_client as client;",
        "pub use radixdb_core as core;",
        "pub use radixdb_executor as executor;",
        "pub use radixdb_functions as functions;",
        "pub use radixdb_executor::optimizer;",
        "pub use radixdb_sql as parser;",
        "pub use radixdb_protocol as protocol;",
        "pub use radixdb_storage as storage;",
    ] {
        assert!(
            library.contains(declaration),
            "missing direct alias: {declaration}"
        );
    }
}
