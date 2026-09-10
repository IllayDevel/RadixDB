// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Messenger RDB-0030: an index prepared from the transaction's final row view
//! must not receive the same transaction's old-to-new DML delta a second time.

use radixdb::{Database, Result};

const PROFILE_ID: &str = "018f2b34-7a10-7cc2-8f3a-9d4b5c6d7000";

fn endpoint_id(position: i64) -> String {
    format!("018f2b34-7a10-7cc2-8f3a-{position:012x}")
}

fn open(path: &std::path::Path) -> Result<Database> {
    Database::open(&format!("file://{}", path.display()))
}

fn assert_final_order(db: &Database) -> Result<()> {
    let rows: Vec<(i64, i64, i64)> = db
        .query(
            "SELECT position, sort_key, unique_sort_key
             FROM egress_proxy_endpoints
             ORDER BY profile_id, sort_key, id",
            (),
        )?
        .map(|row| {
            let row = row?;
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .collect::<Result<_>>()?;

    let expected: Vec<_> = (1..=24)
        .map(|position| (position, position, position))
        .collect();
    assert_eq!(rows, expected);
    Ok(())
}

#[test]
fn rdb_0030_statement_rejection_preserves_pending_private_index() -> Result<()> {
    let db = Database::open("memory://rdb_0030_retry")?;
    db.execute(
        "CREATE TABLE retry_target (
            id INTEGER PRIMARY KEY,
            code TEXT NOT NULL,
            value INTEGER NOT NULL
        )",
        (),
    )?;
    db.execute(
        "CREATE UNIQUE INDEX retry_code_uq ON retry_target(code)",
        (),
    )?;

    let mut winner = db.begin()?;
    winner.execute("INSERT INTO retry_target VALUES (1, 'same', 10)", ())?;
    let mut transaction = db.begin()?;
    transaction.execute("CREATE INDEX retry_value_idx ON retry_target(value)", ())?;
    winner.commit()?;

    let error = transaction
        .execute("INSERT INTO retry_target VALUES (2, 'same', 20)", ())
        .expect_err("a committed UNIQUE owner must reject a contender statement");
    assert!(error.to_string().contains("unique"), "{error}");

    // The SQL transaction remains active after statement failure. Its pending
    // DDL must still build one index generation from the corrected final view.
    assert!(transaction.is_active());
    transaction.execute("INSERT INTO retry_target VALUES (2, 'fixed', 20)", ())?;
    transaction.commit()?;

    let value: i64 = db.query_one("SELECT value FROM retry_target WHERE code = 'fixed'", ())?;
    assert_eq!(value, 20);
    let indexes: Vec<String> = db
        .query("SHOW INDEXES FROM retry_target", ())?
        .map(|row| row.and_then(|row| row.get(1)))
        .collect::<Result<_>>()?;
    assert_eq!(
        indexes
            .iter()
            .filter(|name| name.as_str() == "retry_value_idx")
            .count(),
        1
    );
    Ok(())
}

#[test]
fn rdb_0030_transactional_index_uses_final_updated_values_before_and_after_reopen() -> Result<()> {
    let directory = tempfile::tempdir()?;

    {
        let db = open(directory.path())?;
        db.execute(
            "CREATE TABLE egress_proxy_endpoints (
                id UUID PRIMARY KEY,
                profile_id UUID NOT NULL,
                position INTEGER NOT NULL CHECK (position >= 1)
            )",
            (),
        )?;
        for position in 1..=24 {
            db.execute(
                &format!(
                    "INSERT INTO egress_proxy_endpoints
                     VALUES ('{}', '{PROFILE_ID}', {position})",
                    endpoint_id(position)
                ),
                (),
            )?;
        }
        db.execute("PRAGMA CHECKPOINT", ())?;
        db.close()?;
    }

    // Reopen first: the production failure was observed after a normal restart
    // with the seeded rows already in immutable storage.
    {
        let db = open(directory.path())?;
        db.execute("BEGIN", ())?;
        db.execute(
            "ALTER TABLE egress_proxy_endpoints
             ADD COLUMN sort_key INTEGER NOT NULL DEFAULT 1 CHECK (sort_key >= 1)",
            (),
        )?;
        db.execute("UPDATE egress_proxy_endpoints SET sort_key = position", ())?;
        db.execute(
            "CREATE INDEX egress_proxy_endpoint_sort_idx
             ON egress_proxy_endpoints (profile_id, sort_key, id)",
            (),
        )?;
        db.execute("COMMIT", ())?;

        // Reverse the statement order and cover UNIQUE publication as well.
        db.execute("BEGIN", ())?;
        db.execute(
            "ALTER TABLE egress_proxy_endpoints
             ADD COLUMN unique_sort_key INTEGER NOT NULL DEFAULT 1
             CHECK (unique_sort_key >= 1)",
            (),
        )?;
        db.execute(
            "CREATE UNIQUE INDEX egress_proxy_endpoint_unique_sort_idx
             ON egress_proxy_endpoints (profile_id, unique_sort_key, id)",
            (),
        )?;
        db.execute(
            "UPDATE egress_proxy_endpoints SET unique_sort_key = position",
            (),
        )?;
        db.execute("COMMIT", ())?;

        assert_final_order(&db)?;
        db.execute("PRAGMA CHECKPOINT", ())?;
        db.close()?;
    }

    {
        let db = open(directory.path())?;
        assert_final_order(&db)?;
        let indexes: Vec<String> = db
            .query("SHOW INDEXES FROM egress_proxy_endpoints", ())?
            .map(|row| row.and_then(|row| row.get(1)))
            .collect::<Result<_>>()?;
        assert!(indexes
            .iter()
            .any(|name| name == "egress_proxy_endpoint_sort_idx"));
        assert!(indexes
            .iter()
            .any(|name| name == "egress_proxy_endpoint_unique_sort_idx"));
    }

    Ok(())
}
