// Copyright 2026 RadixDB Contributors
// SPDX-License-Identifier: Apache-2.0

use radixdb::{Database, Error};

#[test]
fn r2_l06_batch_c_create_index_rejects_missing_columns() {
    let directory = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}", directory.path().join("index-wal").display());
    let db = Database::open(&dsn).unwrap();
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)", ())
        .unwrap();

    let error = db
        .execute("CREATE INDEX idx_missing ON items(missing)", ())
        .expect_err("success must guarantee that every WAL column was resolved");
    assert!(matches!(error, Error::ColumnNotFound(column) if column == "missing"));
    db.close().unwrap();
}
