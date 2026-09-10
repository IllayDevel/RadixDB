// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use radixdb::Database;

#[test]
fn non_tail_drop_preserves_hot_row_column_identity_across_multiple_evolutions() {
    let db = Database::open_in_memory().unwrap();
    db.execute(
        "CREATE TABLE drop_identity (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO drop_identity VALUES (1, 'a1', 'b1', 'c1')", ())
        .unwrap();

    db.execute(
        "ALTER TABLE drop_identity ADD COLUMN d TEXT DEFAULT 'old_d'",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO drop_identity VALUES (2, 'a2', 'b2', 'c2', 'd2')",
        (),
    )
    .unwrap();
    db.execute("ALTER TABLE drop_identity DROP COLUMN a", ())
        .unwrap();

    let rows: Vec<(i64, String, String, String)> = db
        .query("SELECT id, b, c, d FROM drop_identity ORDER BY id", ())
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            (
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
                row.get(3).unwrap(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            (1, "b1".into(), "c1".into(), "old_d".into()),
            (2, "b2".into(), "c2".into(), "d2".into()),
        ]
    );

    db.execute("ALTER TABLE drop_identity DROP COLUMN c", ())
        .unwrap();
    db.execute("UPDATE drop_identity SET d = 'updated' WHERE id = 1", ())
        .unwrap();
    db.execute("INSERT INTO drop_identity VALUES (3, 'b3', 'd3')", ())
        .unwrap();

    let rows: Vec<(i64, String, String)> = db
        .query("SELECT id, b, d FROM drop_identity ORDER BY id", ())
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            (
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            (1, "b1".into(), "updated".into()),
            (2, "b2".into(), "d2".into()),
            (3, "b3".into(), "d3".into()),
        ]
    );
}

#[test]
fn non_tail_drop_preserves_identity_after_checkpoint_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}/drop_identity", directory.path().display());

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE persisted_drop (id INTEGER PRIMARY KEY, discarded TEXT, retained TEXT)",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO persisted_drop VALUES (1, 'wrong', 'right')",
            (),
        )
        .unwrap();
        db.execute("ALTER TABLE persisted_drop DROP COLUMN discarded", ())
            .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    }

    let lock_path = directory.path().join("drop_identity").join("db.lock");
    let _ = std::fs::remove_file(lock_path);
    let db = Database::open(&dsn).unwrap();
    let retained: String = db
        .query_one("SELECT retained FROM persisted_drop WHERE id = 1", ())
        .unwrap();
    assert_eq!(retained, "right");
}
