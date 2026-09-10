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

//! Executable ownership gates for the storage dependency inversion.

use radixdb::{DataType, Database, Engine, Value};
use std::fs;
use std::path::{Path, PathBuf};

fn rust_sources_below(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("read storage source directory") {
        let path = entry.expect("read storage source entry").path();
        if path.is_dir() {
            rust_sources_below(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

#[test]
fn evo_40_storage_sources_do_not_depend_on_upper_layers() {
    let storage_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("crates/radixdb-storage/src");
    let mut sources = Vec::new();
    rust_sources_below(&storage_root, &mut sources);
    sources.sort();

    let forbidden = ["api", "executor", "functions", "parser", "server"];
    let mut violations = Vec::new();
    for path in sources {
        let source = fs::read_to_string(&path).expect("read storage Rust source");
        for owner in forbidden {
            let qualified = format!("crate::{owner}");
            if source.contains(&qualified) {
                violations.push(format!("{} imports {qualified}", path.display()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "storage must not depend on upper layers:\n{}",
        violations.join("\n")
    );
}

#[test]
fn executor_owned_ctas_binding_preserves_typed_storage_schema() {
    let db = Database::open_in_memory().expect("open composed database");
    db.execute(
        "CREATE TABLE typed_source (
            id INTEGER PRIMARY KEY,
            nullable_value INTEGER
        )",
        (),
    )
    .expect("create typed source");
    db.execute("INSERT INTO typed_source VALUES (1, NULL), (2, 7)", ())
        .expect("seed typed source");

    for statement in [
        "CREATE TABLE first_null_copy AS
         SELECT nullable_value FROM typed_source ORDER BY id",
        "CREATE TABLE empty_copy AS
         SELECT nullable_value FROM typed_source WHERE id < 0",
        "CREATE TABLE typed_null_copy AS
         SELECT CAST(NULL AS UUID) AS token FROM typed_source WHERE id < 0",
        "CREATE TABLE aggregate_copy AS
         SELECT COUNT(*) AS total, SUM(nullable_value) AS sum_value
         FROM typed_source WHERE id < 0",
        "CREATE TABLE polymorphic_copy AS
         SELECT NULLIF(nullable_value, 7) AS nullable_result,
                IIF(id > 0, nullable_value, 0) AS conditional_result,
                COALESCE(nullable_value, 0) AS filled_result
         FROM typed_source WHERE id < 0",
    ] {
        db.execute(statement, ()).expect("create typed CTAS table");
    }

    for table in ["first_null_copy", "empty_copy"] {
        assert_eq!(
            db.engine()
                .get_table_schema(table)
                .expect("typed CTAS schema")
                .columns[0]
                .data_type,
            DataType::Integer
        );
    }
    assert_eq!(
        db.engine()
            .get_table_schema("typed_null_copy")
            .expect("typed NULL CTAS schema")
            .columns[0]
            .data_type,
        DataType::Uuid
    );
    assert!(db
        .engine()
        .get_table_schema("aggregate_copy")
        .expect("aggregate CTAS schema")
        .columns
        .iter()
        .all(|column| column.data_type == DataType::Integer));
    assert!(db
        .engine()
        .get_table_schema("polymorphic_copy")
        .expect("polymorphic CTAS schema")
        .columns
        .iter()
        .all(|column| column.data_type == DataType::Integer));

    let copied = db
        .query("SELECT nullable_value FROM first_null_copy", ())
        .expect("read copied rows")
        .map(|row| {
            row.expect("read CTAS row")
                .get::<Value>(0)
                .expect("read value")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        copied,
        vec![Value::null(DataType::Integer), Value::Integer(7)]
    );

    assert!(db
        .execute("CREATE TABLE unknown_copy AS SELECT NULL AS value", ())
        .is_err());
    assert!(!db
        .engine()
        .table_exists("unknown_copy")
        .expect("inspect rejected CTAS"));

    db.execute("CREATE TABLE fk_parent (id INTEGER PRIMARY KEY)", ())
        .expect("create FK parent");
    assert!(db
        .execute(
            "CREATE TABLE bad_child (
                id INTEGER PRIMARY KEY,
                parent_id TEXT REFERENCES fk_parent(id)
            )",
            (),
        )
        .is_err());
    assert!(!db
        .engine()
        .table_exists("bad_child")
        .expect("inspect rejected FK table"));
    db.execute(
        "CREATE TABLE good_child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES fk_parent(id)
        )",
        (),
    )
    .expect("create compatible FK table");
}
