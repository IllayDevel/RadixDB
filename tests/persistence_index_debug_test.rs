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

use radixdb::Database;
use tempfile::tempdir;

#[path = "support/wal_inventory.rs"]
mod wal_inventory;

#[test]
fn test_persistence_index_debug() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    eprintln!("=== PHASE 1: Create table, index, and insert ===");

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)",
            (),
        )
        .unwrap();
        eprintln!("Table created");

        db.execute("CREATE UNIQUE INDEX idx_email ON users(email)", ())
            .unwrap();
        eprintln!("Unique index created");

        db.execute(
            "INSERT INTO users (id, email) VALUES (1, 'test@example.com')",
            (),
        )
        .unwrap();
        eprintln!("Row inserted");

        db.close().unwrap();
    }

    wal_inventory::print(&db_path.join("wal"));

    eprintln!("\n=== PHASE 2: Reopen and test unique constraint ===");

    let db = Database::open(&dsn).unwrap();

    // Check existing data
    let count: i64 = db.query_one("SELECT COUNT(*) FROM users", ()).unwrap();
    eprintln!("Users count after reopen: {}", count);

    // Try to insert duplicate
    let result = db.execute(
        "INSERT INTO users (id, email) VALUES (2, 'test@example.com')",
        (),
    );
    let is_err = result.is_err();
    match result {
        Ok(_) => eprintln!("FAILURE: Duplicate insert succeeded (should have failed)"),
        Err(e) => eprintln!("Duplicate rejected as expected: {}", e),
    }

    assert!(is_err, "Duplicate should be rejected");
}
