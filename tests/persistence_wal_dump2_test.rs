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
fn test_persistence_wal_dump2() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dsn = format!("file://{}", db_path.display());

    eprintln!("=== PHASE 1: Create and Insert ===");

    // Phase 1: Create table and insert data
    {
        let db = Database::open(&dsn).unwrap();

        db.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO test (id, value) VALUES (1, 100)", ())
            .unwrap();
        db.execute("INSERT INTO test (id, value) VALUES (2, 200)", ())
            .unwrap();

        let count: i64 = db.query_one("SELECT COUNT(*) FROM test", ()).unwrap();
        eprintln!("Phase 1 count: {}", count);

        db.close().unwrap();
    }

    wal_inventory::print(&db_path.join("wal"));

    eprintln!("\n=== PHASE 2: Reopen and verify ===");

    let db = Database::open(&dsn).unwrap();
    let count: i64 = db.query_one("SELECT COUNT(*) FROM test", ()).unwrap();
    eprintln!("Phase 2 count: {}", count);

    assert_eq!(count, 2);
}
