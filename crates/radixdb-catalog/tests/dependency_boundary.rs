// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs;
use std::path::Path;

#[test]
fn catalog_has_exactly_one_radixdb_dependency() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = fs::read_to_string(crate_root.join("Cargo.toml")).expect("read manifest");
    let parsed: toml::Value = manifest.parse().expect("parse manifest");
    let dependencies = parsed["dependencies"].as_table().expect("dependencies");
    assert_eq!(
        dependencies.keys().map(String::as_str).collect::<Vec<_>>(),
        vec!["radixdb-core"],
        "catalog may depend only on the canonical core layer"
    );
    assert_eq!(
        dependencies["radixdb-core"]["path"].as_str(),
        Some("../radixdb-core")
    );
    assert_eq!(
        dependencies["radixdb-core"]["version"].as_str(),
        Some(concat!("=", env!("CARGO_PKG_VERSION")))
    );

    for forbidden in [
        "radixdb-sql",
        "radixdb-executor",
        "radixdb-storage",
        "radixdb-api",
        "radixdb-protocol",
        "radixdb-client",
        "radixdb-functions",
        "radixdb-orm",
        "radixdb =",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "catalog manifest crosses its boundary through {forbidden}"
        );
    }
}

#[test]
fn catalog_is_a_workspace_member() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repository = crate_root
        .parent()
        .and_then(Path::parent)
        .expect("catalog crate is nested under repository/crates");
    let workspace = fs::read_to_string(repository.join("Cargo.toml")).expect("read workspace");
    assert!(
        workspace.contains("\"crates/radixdb-catalog\""),
        "catalog crate is not a workspace member"
    );
}
