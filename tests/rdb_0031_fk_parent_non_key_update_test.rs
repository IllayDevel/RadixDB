// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! RDB-0031: replacing a cold parent row to update a non-key column must not
//! look like removal of its unchanged referenced identity at commit.

use radixdb::{Database, Result};

const PARENT_ID: &str = "01a00508-7a7f-72c2-a225-30b4d55cc1fc";

fn child_id(position: i64) -> String {
    format!("01a004d7-d9e7-784d-9d8f-{position:012x}")
}

fn open(path: &std::path::Path) -> Result<Database> {
    Database::open(&format!("file://{}", path.display()))
}

#[test]
fn cold_parent_non_key_update_preserves_primary_and_unique_referenced_keys() -> Result<()> {
    let directory = tempfile::tempdir()?;

    {
        let db = open(directory.path())?;
        db.execute(
            "CREATE TABLE primary_parents (
                id UUID PRIMARY KEY,
                revision INTEGER NOT NULL
            )",
            (),
        )?;
        db.execute(
            "CREATE TABLE primary_children (
                id UUID PRIMARY KEY,
                parent_id UUID NOT NULL REFERENCES primary_parents(id),
                position INTEGER NOT NULL
            )",
            (),
        )?;
        db.execute(
            "CREATE TABLE unique_parents (
                id INTEGER PRIMARY KEY,
                code TEXT NOT NULL UNIQUE,
                revision INTEGER NOT NULL
            )",
            (),
        )?;
        db.execute(
            "CREATE TABLE unique_children (
                id INTEGER PRIMARY KEY,
                parent_code TEXT NOT NULL REFERENCES unique_parents(code)
            )",
            (),
        )?;
        db.execute(
            &format!("INSERT INTO primary_parents VALUES ('{PARENT_ID}', 1)"),
            (),
        )?;
        for position in 1..=8 {
            db.execute(
                &format!(
                    "INSERT INTO primary_children VALUES ('{}', '{PARENT_ID}', {position})",
                    child_id(position)
                ),
                (),
            )?;
        }
        db.execute("INSERT INTO unique_parents VALUES (1, 'stable', 1)", ())?;
        db.execute("INSERT INTO unique_children VALUES (1, 'stable')", ())?;

        // The same contract must hold while both parent rows are still hot.
        db.execute("BEGIN", ())?;
        db.execute(
            &format!("UPDATE primary_parents SET revision = 2 WHERE id = '{PARENT_ID}'"),
            (),
        )?;
        db.execute(
            "UPDATE unique_parents SET revision = 2 WHERE code = 'stable'",
            (),
        )?;
        db.execute("COMMIT", ())?;
        db.execute("PRAGMA CHECKPOINT", ())?;
        db.close()?;
    }

    {
        let db = open(directory.path())?;

        // Ticket order: child first, then a non-key update of the cold parent.
        db.execute("BEGIN", ())?;
        db.execute(
            &format!(
                "INSERT INTO primary_children VALUES ('{}', '{PARENT_ID}', 9)",
                child_id(9)
            ),
            (),
        )?;
        db.execute(
            &format!(
                "UPDATE primary_parents SET revision = revision + 1
                 WHERE id = '{PARENT_ID}' AND revision = 2"
            ),
            (),
        )?;
        let visible: i64 = db.query_one(
            &format!("SELECT COUNT(*) FROM primary_children WHERE parent_id = '{PARENT_ID}'"),
            (),
        )?;
        assert_eq!(visible, 9);
        db.execute("COMMIT", ())?;

        // Reverse order and cover a referenced UNIQUE key.
        db.execute("BEGIN", ())?;
        db.execute(
            "UPDATE unique_parents SET revision = revision + 1
             WHERE code = 'stable' AND revision = 2",
            (),
        )?;
        db.execute("INSERT INTO unique_children VALUES (2, 'stable')", ())?;
        let visible: i64 = db.query_one(
            "SELECT COUNT(*) FROM unique_children WHERE parent_code = 'stable'",
            (),
        )?;
        assert_eq!(visible, 2);
        db.execute("COMMIT", ())?;

        assert_eq!(
            db.query_one::<i64, _>(
                &format!("SELECT revision FROM primary_parents WHERE id = '{PARENT_ID}'"),
                (),
            )?,
            3
        );
        assert_eq!(
            db.query_one::<i64, _>(
                "SELECT revision FROM unique_parents WHERE code = 'stable'",
                (),
            )?,
            3
        );

        // A real referenced-key change or delete remains restricted, and the
        // failed statement cannot publish its unrelated assignment either.
        db.execute("BEGIN", ())?;
        let update_error = db
            .execute(
                "UPDATE unique_parents
                 SET code = 'moved', revision = 99 WHERE id = 1",
                (),
            )
            .expect_err("referenced UNIQUE key update must remain restricted");
        assert!(update_error.to_string().contains("foreign key"));
        db.execute("ROLLBACK", ())?;
        assert_eq!(
            db.query_one::<i64, _>(
                "SELECT revision FROM unique_parents WHERE code = 'stable'",
                (),
            )?,
            3
        );

        db.execute("BEGIN", ())?;
        let delete_error = db
            .execute(
                &format!("DELETE FROM primary_parents WHERE id = '{PARENT_ID}'"),
                (),
            )
            .expect_err("referenced primary parent delete must remain restricted");
        assert!(delete_error.to_string().contains("foreign key"));
        db.execute("ROLLBACK", ())?;

        db.execute("PRAGMA CHECKPOINT", ())?;
        db.close()?;
    }

    {
        let db = open(directory.path())?;
        assert_eq!(
            db.query_one::<i64, _>(
                &format!("SELECT revision FROM primary_parents WHERE id = '{PARENT_ID}'"),
                (),
            )?,
            3
        );
        assert_eq!(
            db.query_one::<i64, _>("SELECT COUNT(*) FROM primary_children", (),)?,
            9
        );
        assert_eq!(
            db.query_one::<i64, _>(
                "SELECT COUNT(*) FROM unique_children WHERE parent_code = 'stable'",
                (),
            )?,
            2
        );
    }

    Ok(())
}
