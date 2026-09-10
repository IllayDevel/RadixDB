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

//! Arena Slot Reuse and Version Chain Tests
//!
//! This test verifies that snapshot isolation is preserved when arena slots
//! are reused during UPDATE operations.
//!
//! Transaction IDs are engine-owned identities. Tests capture the ID of the
//! version-producing transaction instead of assuming which earlier DDL or
//! maintenance operations consumed IDs.
//!
//! AS OF semantics (using <= comparison):
//! - AS OF TRANSACTION N returns versions where txn_id <= N
//! - AS OF the INSERT transaction returns the original version
//! - AS OF the UPDATE transaction returns the updated version
//!
//! See: crates/radixdb-storage/src/mvcc/version_store/

use radixdb::Database;

/// Test that AS OF queries return historical data after UPDATE
///
/// This test verifies that snapshot isolation is preserved:
/// - INSERT creates a version under its actual transaction ID
/// - UPDATE preserves the prior data in the version chain
/// - AS OF the INSERT transaction should return the historical value
#[test]
fn test_arena_slot_reuse_snapshot_isolation() {
    let db = Database::open("memory://arena_slot_test1").expect("Failed to create database");

    // DDL transaction IDs are an implementation detail and may precede DML.
    db.execute(
        "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER, name TEXT)",
        (),
    )
    .expect("Create failed");

    let mut insert_tx = db.begin().expect("Begin insert transaction failed");
    let insert_txn_id = insert_tx.id();
    insert_tx
        .execute("INSERT INTO accounts VALUES (1, 1000, 'Alice')", ())
        .expect("Insert failed");
    insert_tx.commit().expect("Insert commit failed");

    // Record the current state - balance should be 1000
    let balance_after_insert: i64 = db
        .query_one("SELECT balance FROM accounts WHERE id = 1", ())
        .expect("Query failed");
    assert_eq!(
        balance_after_insert, 1000,
        "Balance after insert should be 1000"
    );

    // UPDATE creates a new version while preserving the INSERT version.
    db.execute("UPDATE accounts SET balance = 2000 WHERE id = 1", ())
        .expect("Update failed");

    // Current query should see the new value
    let current_balance: i64 = db
        .query_one("SELECT balance FROM accounts WHERE id = 1", ())
        .expect("Current query failed");
    assert_eq!(current_balance, 2000, "Current balance should be 2000");

    let historical_sql =
        format!("SELECT balance FROM accounts AS OF TRANSACTION {insert_txn_id} WHERE id = 1");
    let historical_result = db.query(&historical_sql, ());
    assert!(historical_result.is_ok(), "AS OF query should execute");

    let rows = historical_result.unwrap();
    let mut found = false;
    for row_result in rows {
        let row = row_result.expect("Row iteration failed");
        let historical_balance: i64 = row.get(0).expect("Get balance failed");
        found = true;

        // Historical query should return the INSERT value
        assert_eq!(
            historical_balance, 1000,
            "AS OF the INSERT transaction should return historical balance 1000, not {}",
            historical_balance
        );
    }

    assert!(
        found,
        "AS OF query should return the row that existed at the INSERT transaction"
    );
}

/// Test multiple updates - each update creates a new version in the chain
#[test]
fn test_arena_slot_reuse_multiple_updates() {
    let db = Database::open("memory://arena_slot_test2").expect("Failed to create database");

    // CREATE TABLE (DDL - doesn't consume txn ID for data)
    db.execute(
        "CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER)",
        (),
    )
    .expect("Create failed");

    // INSERT: txn_id=1, value=100
    db.execute("INSERT INTO counters VALUES (1, 100)", ())
        .expect("Insert failed");

    // Perform multiple updates:
    // UPDATE 1: txn_id=2, value=200
    // UPDATE 2: txn_id=3, value=300
    // UPDATE 3: txn_id=4, value=400
    // UPDATE 4: txn_id=5, value=500
    for i in 2..=5 {
        db.execute(
            &format!("UPDATE counters SET value = {} WHERE id = 1", i * 100),
            (),
        )
        .expect("Update failed");
    }

    // Current value should be 500
    let current: i64 = db
        .query_one("SELECT value FROM counters WHERE id = 1", ())
        .expect("Current query failed");
    assert_eq!(current, 500, "Current value should be 500");

    // Test AS OF at transaction 1 (the INSERT transaction)
    // Should return 100, the original INSERT value
    let result = db.query(
        "SELECT value FROM counters AS OF TRANSACTION 1 WHERE id = 1",
        (),
    );
    assert!(result.is_ok(), "AS OF query should execute");

    let mut rows = result.unwrap();
    if let Some(row_result) = rows.next() {
        let row = row_result.expect("Row iteration failed");
        let value: i64 = row.get(0).expect("Get value failed");

        assert_eq!(
            value, 100,
            "AS OF TRANSACTION 1 should return original INSERT value 100, not {}\n\
             The INSERT version has txn_id=1.",
            value
        );
    }
}

/// Test that version chain data (version.data) preserves historical state
/// This test verifies that the data IS preserved in the version chain
#[test]
fn test_version_chain_data_preservation() {
    let db = Database::open("memory://arena_slot_test3").expect("Failed to create database");

    // CREATE TABLE (DDL - doesn't consume txn ID for data)
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, status TEXT, quantity INTEGER)",
        (),
    )
    .expect("Create failed");

    // INSERT: txn_id=1
    db.execute("INSERT INTO items VALUES (1, 'available', 10)", ())
        .expect("Insert failed");

    // UPDATE: txn_id=2
    db.execute(
        "UPDATE items SET status = 'sold', quantity = 0 WHERE id = 1",
        (),
    )
    .expect("Update failed");

    // Current state
    let current_status: String = db
        .query_one("SELECT status FROM items WHERE id = 1", ())
        .expect("Current query failed");
    let current_qty: i64 = db
        .query_one("SELECT quantity FROM items WHERE id = 1", ())
        .expect("Current query failed");
    assert_eq!(current_status, "sold");
    assert_eq!(current_qty, 0);

    // Historical query AS OF TRANSACTION 1 should return 'available' and 10
    let result = db.query(
        "SELECT status, quantity FROM items AS OF TRANSACTION 1 WHERE id = 1",
        (),
    );
    assert!(result.is_ok(), "AS OF query should execute");

    let mut rows = result.unwrap();
    if let Some(row_result) = rows.next() {
        let row = row_result.expect("Row iteration failed");
        let status: String = row.get(0).expect("Get status failed");
        let qty: i64 = row.get(1).expect("Get quantity failed");

        assert_eq!(
            status, "available",
            "AS OF TRANSACTION 1 should return historical status 'available', not '{}'",
            status
        );
        assert_eq!(
            qty, 10,
            "AS OF TRANSACTION 1 should return historical quantity 10, not {}",
            qty
        );
    }
}

/// Test batch visibility path (get_visible_versions_batch)
#[test]
fn test_batch_visibility_arena_bug() {
    let db = Database::open("memory://arena_slot_test4").expect("Failed to create database");

    // CREATE TABLE (DDL - doesn't consume txn ID for data)
    db.execute(
        "CREATE TABLE batch_test (id INTEGER PRIMARY KEY, version INTEGER)",
        (),
    )
    .expect("Create failed");

    // INSERT: txn_id=1, version=1
    db.execute("INSERT INTO batch_test VALUES (1, 1)", ())
        .expect("Insert failed");

    // UPDATE: txn_id=2, version=2
    db.execute("UPDATE batch_test SET version = 2 WHERE id = 1", ())
        .expect("Update failed");

    // Current query should see version 2
    let current: i64 = db
        .query_one("SELECT version FROM batch_test WHERE id = 1", ())
        .expect("Current query failed");
    assert_eq!(current, 2, "Current version should be 2");

    // AS OF TRANSACTION 1 should return version 1 (the INSERT version)
    let historical_result = db.query(
        "SELECT version FROM batch_test AS OF TRANSACTION 1 WHERE id = 1",
        (),
    );
    assert!(
        historical_result.is_ok(),
        "AS OF batch query should execute"
    );

    let mut rows = historical_result.unwrap();
    if let Some(row_result) = rows.next() {
        let row = row_result.unwrap();
        let version: i64 = row.get(0).unwrap();

        assert_eq!(
            version, 1,
            "AS OF TRANSACTION 1 should return historical version 1, not {}.\n\
             The INSERT version has txn_id=1.",
            version
        );
    }
}

/// Test AS OF visibility after UPDATE preserves historical data
#[test]
fn test_get_visible_versions_batch_arena_preference() {
    let db = Database::open("memory://arena_slot_test5").expect("Failed to create database");

    // CREATE TABLE (DDL - doesn't consume txn ID for data)
    db.execute(
        "CREATE TABLE arena_pref (id INTEGER PRIMARY KEY, val INTEGER)",
        (),
    )
    .expect("Create failed");

    // INSERT: txn_id=1, val=42
    db.execute("INSERT INTO arena_pref VALUES (1, 42)", ())
        .expect("Insert failed");

    // Query current value
    let current: i64 = db
        .query_one("SELECT val FROM arena_pref WHERE id = 1", ())
        .expect("Query failed");
    eprintln!("DEBUG: Current value = {}", current);

    // Check AS OF for various transaction IDs before UPDATE
    for txn_id in 1..=5 {
        let result = db.query(
            &format!(
                "SELECT val FROM arena_pref AS OF TRANSACTION {} WHERE id = 1",
                txn_id
            ),
            (),
        );
        match result {
            Ok(mut rows) => {
                if let Some(row_result) = rows.next() {
                    if let Ok(row) = row_result {
                        let val: i64 = row.get(0).unwrap_or(-1);
                        eprintln!("DEBUG: AS OF TRANSACTION {} returns val = {}", txn_id, val);
                    }
                } else {
                    eprintln!("DEBUG: AS OF TRANSACTION {} returns no rows", txn_id);
                }
            }
            Err(e) => {
                eprintln!("DEBUG: AS OF TRANSACTION {} error: {}", txn_id, e);
            }
        }
    }

    // UPDATE: creates new version, historical data preserved in prev chain
    // Note: UPDATE uses the next available txn_id after all the queries above
    db.execute("UPDATE arena_pref SET val = 999 WHERE id = 1", ())
        .expect("Update failed");

    eprintln!("DEBUG: After UPDATE...");
    // Check AS OF for various transaction IDs after UPDATE
    for txn_id in 1..=5 {
        let result = db.query(
            &format!(
                "SELECT val FROM arena_pref AS OF TRANSACTION {} WHERE id = 1",
                txn_id
            ),
            (),
        );
        match result {
            Ok(mut rows) => {
                if let Some(row_result) = rows.next() {
                    if let Ok(row) = row_result {
                        let val: i64 = row.get(0).unwrap_or(-1);
                        eprintln!("DEBUG: AS OF TRANSACTION {} returns val = {}", txn_id, val);
                    }
                } else {
                    eprintln!("DEBUG: AS OF TRANSACTION {} returns no rows", txn_id);
                }
            }
            Err(e) => {
                eprintln!("DEBUG: AS OF TRANSACTION {} error: {}", txn_id, e);
            }
        }
    }

    // AS OF TRANSACTION 1 should return the INSERT value (42)
    // The INSERT version has txn_id=1, so it's visible at AS OF 1
    let result = db.query(
        "SELECT val FROM arena_pref AS OF TRANSACTION 1 WHERE id = 1",
        (),
    );
    assert!(result.is_ok());

    let mut rows = result.unwrap();
    if let Some(row_result) = rows.next() {
        let row = row_result.unwrap();
        let val: i64 = row.get(0).unwrap();

        assert_eq!(
            val, 42,
            "AS OF TRANSACTION 1 should return INSERT value 42, not {}.\n\
             The INSERT version has txn_id=1.",
            val
        );
    }
}
