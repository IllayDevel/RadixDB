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

use radixdb::storage::instrumentation;
use radixdb::{Database, Result};
use tempfile::TempDir;

fn open(path: &std::path::Path) -> Result<Database> {
    Database::open(&format!("file://{}", path.display()))
}

#[test]
fn integer_primary_key_foreign_key_probe_is_metadata_only_and_transaction_aware() -> Result<()> {
    let directory = TempDir::new()?;
    {
        let db = open(directory.path())?;
        db.execute(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
            (),
        )?;
        db.execute(
            "CREATE TABLE children (
                id INTEGER,
                parent_id INTEGER NOT NULL REFERENCES parents(id)
            )",
            (),
        )?;
        db.execute(
            "INSERT INTO parents VALUES (100, 'cold-parent'), (200, 'delete-parent')",
            (),
        )?;
        db.close()?;
    }

    let db = open(directory.path())?;
    instrumentation::reset();
    db.execute("INSERT INTO children VALUES (1, 100)", ())?;
    let counters = instrumentation::snapshot();
    assert_eq!(
        counters.artifact_payload_decompress_calls, 0,
        "FK probe decompressed cold payload"
    );
    assert_eq!(
        counters.artifact_column_deserialize_calls, 0,
        "FK probe deserialized a cold column"
    );
    assert_eq!(
        counters.row_materialization_rows, 0,
        "FK probe materialized a parent row"
    );

    assert!(db
        .execute("INSERT INTO children VALUES (2, 999)", ())
        .is_err());

    db.execute("BEGIN", ())?;
    db.execute("INSERT INTO parents VALUES (300, 'local-parent')", ())?;
    db.execute("INSERT INTO children VALUES (3, 300)", ())?;
    db.execute("ROLLBACK", ())?;

    db.execute("BEGIN", ())?;
    db.execute("DELETE FROM parents WHERE id = 200", ())?;
    assert!(db
        .execute("INSERT INTO children VALUES (4, 200)", ())
        .is_err());
    db.execute("ROLLBACK", ())?;

    Ok(())
}
