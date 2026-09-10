// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![cfg(feature = "test-mutations")]

mod common;

use radixdb::{Database, Result};

fn scalar_i64(db: &Database, sql: &str) -> i64 {
    db.query_one(sql, ()).expect("read B10 scalar")
}

fn first_column(db: &Database, sql: &str) -> Vec<i64> {
    db.query(sql, ())
        .expect("execute B10 query")
        .map(|row| row.expect("read B10 row").get(0).expect("read B10 value"))
        .collect()
}

#[test]
fn b10_constraint_validation_invariant() -> Result<()> {
    let db = Database::open("memory://prerelease-b10-constraint")?;
    db.execute(
        "CREATE TABLE guarded_rows (
            id INTEGER PRIMARY KEY,
            balance INTEGER NOT NULL CHECK (balance >= 0)
        )",
        (),
    )?;

    let rejected = db.execute("INSERT INTO guarded_rows VALUES (1, -1)", ());
    assert!(
        rejected.is_err(),
        "PRV-B10 invariant constraint_validation: invalid CHECK row was published"
    );
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM guarded_rows"), 0);
    Ok(())
}

#[test]
fn b10_index_publication_invariant() -> Result<()> {
    let db = Database::open("memory://prerelease-b10-index")?;
    db.execute(
        "CREATE TABLE indexed_rows (
            id INTEGER PRIMARY KEY,
            lookup_key INTEGER NOT NULL,
            payload TEXT NOT NULL
        )",
        (),
    )?;
    db.execute(
        "CREATE UNIQUE INDEX indexed_rows_lookup_idx ON indexed_rows(lookup_key)",
        (),
    )?;
    db.execute("INSERT INTO indexed_rows VALUES (1, 42, 'published')", ())?;

    assert_eq!(
        scalar_i64(&db, "SELECT COUNT(*) FROM indexed_rows"),
        1,
        "row publication control failed"
    );
    let plan: String = db
        .query(
            "EXPLAIN SELECT id FROM indexed_rows WHERE lookup_key = 42",
            (),
        )?
        .map(|row| row.and_then(|row| row.get::<String>(0)))
        .collect::<Result<Vec<_>>>()?
        .join("\n");
    assert!(
        plan.contains("Access Path: scan.index"),
        "B10 index oracle did not select its secondary index:\n{plan}"
    );
    let duplicate = db.execute("INSERT INTO indexed_rows VALUES (2, 42, 'duplicate')", ());
    assert!(
        duplicate.is_err(),
        "PRV-B10 invariant index_publication: duplicate key was admitted after index publication was skipped"
    );
    Ok(())
}

#[test]
fn b10_visibility_fence_invariant() {
    common::prerelease::run_historical_mixed_epoch_invariant();
}

#[test]
fn b10_view_invalidation_invariant() -> Result<()> {
    let db = Database::open("memory://prerelease-b10-view")?;
    db.execute(
        "CREATE TABLE view_rows (id INTEGER PRIMARY KEY, visible BOOLEAN NOT NULL)",
        (),
    )?;
    db.execute("INSERT INTO view_rows VALUES (1, TRUE)", ())?;
    db.execute(
        "CREATE VIEW active_view_rows AS SELECT id FROM view_rows WHERE visible = TRUE",
        (),
    )?;
    assert_eq!(
        first_column(&db, "SELECT id FROM active_view_rows"),
        vec![1]
    );

    db.execute("DROP VIEW active_view_rows", ())?;
    let after_drop = db.query("SELECT id FROM active_view_rows", ());
    assert!(
        after_drop.is_err(),
        "PRV-B10 invariant view_invalidation: DROP VIEW left the old catalog object executable"
    );
    Ok(())
}
