// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");

//! R4-L01 contract oracles for transactional index/FK identity boundaries.

use radixdb::{Database, Engine, Value};

const GRANDPARENT_ID: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0x11, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa1,
];
const PARENT_ID: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0x12, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa2,
];
const CHILD_ID: [u8; 16] = [
    0x01, 0x9f, 0xe3, 0xbf, 0x68, 0x80, 0x73, 0x13, 0xb5, 0xd0, 0x58, 0xce, 0x35, 0x54, 0x57, 0xa3,
];

fn integer_column(db: &Database, sql: &str) -> Vec<i64> {
    db.query(sql, ())
        .unwrap()
        .map(|row| row.unwrap().get::<i64>(0).unwrap())
        .collect()
}

fn count(db: &Database, table: &str) -> i64 {
    integer_column(db, &format!("SELECT COUNT(*) FROM {table}"))[0]
}

fn column_names(db: &Database, table: &str) -> Vec<String> {
    db.engine()
        .get_table_schema(table)
        .unwrap()
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect()
}

#[test]
fn explicit_transaction_join_reads_private_inner_index_changes() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}?checkpoint_on_close=off",
        dir.path().join("r4-explicit-join").display()
    );
    {
        let db = Database::open(&dsn).unwrap();
        db.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        db.execute(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL)",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE INDEX children_parent_idx ON children(parent_id)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO parents VALUES (1), (2)", ())
            .unwrap();
        db.execute("INSERT INTO children VALUES (1, 1), (3, 2)", ())
            .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    }

    let db = Database::open(&dsn).unwrap();
    db.execute("BEGIN", ()).unwrap();
    db.execute("INSERT INTO children VALUES (2, 1)", ())
        .unwrap();
    assert_eq!(
        integer_column(
            &db,
            "SELECT c.id FROM parents p JOIN children c ON p.id = c.parent_id WHERE p.id = 1 ORDER BY c.id",
        ),
        vec![1, 2]
    );
    db.execute("UPDATE children SET parent_id = 1 WHERE id = 3", ())
        .unwrap();
    assert_eq!(
        integer_column(
            &db,
            "SELECT c.id FROM parents p JOIN children c ON p.id = c.parent_id WHERE p.id = 1 ORDER BY c.id",
        ),
        vec![1, 2, 3]
    );
    db.execute("DELETE FROM children WHERE id = 1", ()).unwrap();
    assert_eq!(
        integer_column(
            &db,
            "SELECT c.id FROM parents p JOIN children c ON p.id = c.parent_id WHERE p.id = 1 ORDER BY c.id",
        ),
        vec![2, 3]
    );
    db.execute("ROLLBACK", ()).unwrap();
    assert_eq!(
        integer_column(
            &db,
            "SELECT c.id FROM parents p JOIN children c ON p.id = c.parent_id ORDER BY c.id",
        ),
        vec![1, 3]
    );
}

#[test]
fn uuid_delete_returning_preserves_three_level_cascade_identity() {
    let db = Database::open_in_memory().unwrap();
    db.execute("CREATE TABLE grandparents (id UUID PRIMARY KEY)", ())
        .unwrap();
    db.execute(
        "CREATE TABLE parents (id UUID PRIMARY KEY, grandparent_id UUID NOT NULL REFERENCES grandparents(id) ON DELETE CASCADE)",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE children (id UUID PRIMARY KEY, parent_id UUID NOT NULL REFERENCES parents(id) ON DELETE CASCADE)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO grandparents VALUES (?)",
        (Value::uuid(GRANDPARENT_ID),),
    )
    .unwrap();
    db.execute(
        "INSERT INTO parents VALUES (?, ?)",
        (Value::uuid(PARENT_ID), Value::uuid(GRANDPARENT_ID)),
    )
    .unwrap();
    db.execute(
        "INSERT INTO children VALUES (?, ?)",
        (Value::uuid(CHILD_ID), Value::uuid(PARENT_ID)),
    )
    .unwrap();

    let returned = db
        .query(
            "DELETE FROM grandparents WHERE id = ? RETURNING id",
            (Value::uuid(GRANDPARENT_ID),),
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(returned.len(), 1);
    assert_eq!(count(&db, "grandparents"), 0);
    assert_eq!(count(&db, "parents"), 0);
    assert_eq!(count(&db, "children"), 0);
}

#[test]
fn uuid_foreign_key_restrict_and_set_null_keep_generic_identity() {
    let db = Database::open_in_memory().unwrap();
    db.execute("CREATE TABLE parents (id UUID PRIMARY KEY)", ())
        .unwrap();
    db.execute(
        "CREATE TABLE restricted_children (id UUID PRIMARY KEY, parent_id UUID NOT NULL REFERENCES parents(id) ON DELETE RESTRICT)",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE nullable_children (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id) ON DELETE SET NULL)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO parents VALUES (?)",
        (Value::uuid(GRANDPARENT_ID),),
    )
    .unwrap();
    db.execute(
        "INSERT INTO restricted_children VALUES (?, ?)",
        (Value::uuid(PARENT_ID), Value::uuid(GRANDPARENT_ID)),
    )
    .unwrap();
    db.execute(
        "INSERT INTO nullable_children VALUES (?, ?)",
        (Value::uuid(CHILD_ID), Value::uuid(GRANDPARENT_ID)),
    )
    .unwrap();

    assert!(db
        .execute(
            "DELETE FROM parents WHERE id = ?",
            (Value::uuid(GRANDPARENT_ID),),
        )
        .is_err());
    db.execute(
        "DELETE FROM restricted_children WHERE id = ?",
        (Value::uuid(PARENT_ID),),
    )
    .unwrap();
    db.execute(
        "DELETE FROM parents WHERE id = ?",
        (Value::uuid(GRANDPARENT_ID),),
    )
    .unwrap();
    assert_eq!(count(&db, "nullable_children"), 1);
    assert_eq!(
        integer_column(
            &db,
            "SELECT COUNT(*) FROM nullable_children WHERE parent_id IS NULL",
        ),
        vec![1]
    );
}

#[test]
fn foreign_key_requires_and_retains_full_unique_backing() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}?checkpoint_on_close=off",
        dir.path().join("r4-fk-backing").display()
    );
    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY, code TEXT, active BOOLEAN NOT NULL)",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE UNIQUE INDEX parents_code_partial ON parents(code) WHERE active = TRUE",
            (),
        )
        .unwrap();
        assert!(db
            .execute(
                "CREATE TABLE rejected_children (id INTEGER PRIMARY KEY, parent_code TEXT REFERENCES parents(code))",
                (),
            )
            .is_err());

        db.execute("CREATE UNIQUE INDEX parents_code_uq ON parents(code)", ())
            .unwrap();
        db.execute(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_code TEXT REFERENCES parents(code))",
            (),
        )
        .unwrap();
        db.execute("BEGIN", ()).unwrap();
        assert!(db
            .execute("DROP INDEX parents_code_uq ON parents", ())
            .is_err());
        assert!(db
            .execute(
                "ALTER INDEX parents_code_uq RENAME TO parents_code_renamed",
                (),
            )
            .is_err());
        db.execute("ROLLBACK", ()).unwrap();
        assert!(db
            .execute("DROP INDEX parents_code_uq ON parents", ())
            .is_err());

        db.execute("CREATE UNIQUE INDEX parents_code_uq_2 ON parents(code)", ())
            .unwrap();
        db.execute("DROP INDEX parents_code_uq ON parents", ())
            .unwrap();
        assert!(db
            .execute("DROP INDEX parents_code_uq_2 ON parents", ())
            .is_err());
        db.execute("INSERT INTO parents VALUES (1, 'one', TRUE)", ())
            .unwrap();
        db.execute("INSERT INTO children VALUES (1, 'one')", ())
            .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    }

    let db = Database::open(&dsn).unwrap();
    assert!(db
        .execute("DROP INDEX parents_code_uq_2 ON parents", ())
        .is_err());
    assert!(db
        .execute("INSERT INTO parents VALUES (2, 'one', TRUE)", ())
        .is_err());
}

#[test]
fn indexed_column_metadata_survives_drop_rename_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}?checkpoint_on_close=off",
        dir.path().join("r4-index-remap").display()
    );
    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, prefix TEXT, code TEXT NOT NULL, active BOOLEAN NOT NULL)",
            (),
        )
        .unwrap();
        db.execute("CREATE INDEX items_code_idx ON items(code)", ())
            .unwrap();
        db.execute("INSERT INTO items VALUES (1, 'unused', 'one', TRUE)", ())
            .unwrap();
        db.execute("ALTER TABLE items DROP COLUMN prefix", ())
            .unwrap();
        assert_eq!(column_names(&db, "items"), ["id", "code", "active"]);
        db.execute("ALTER TABLE items RENAME COLUMN code TO external_code", ())
            .unwrap();
        assert_eq!(
            column_names(&db, "items"),
            ["id", "external_code", "active"]
        );
        db.execute(
            "CREATE INDEX items_live_idx ON items(external_code) WHERE active = TRUE",
            (),
        )
        .unwrap();
        assert!(db
            .execute("ALTER TABLE items DROP COLUMN active", ())
            .is_err());
        assert!(db
            .execute("ALTER TABLE items RENAME COLUMN active TO enabled", ())
            .is_err());
        assert_eq!(
            integer_column(&db, "SELECT id FROM items WHERE external_code = 'one'",),
            vec![1]
        );
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    }

    let db = Database::open(&dsn).unwrap();
    assert_eq!(
        column_names(&db, "items"),
        ["id", "external_code", "active"]
    );
    assert_eq!(
        integer_column(&db, "SELECT id FROM items WHERE external_code = 'one'",),
        vec![1]
    );
    db.execute("INSERT INTO items VALUES (2, 'two', TRUE)", ())
        .unwrap();
    assert_eq!(
        integer_column(
            &db,
            "SELECT id FROM items WHERE external_code IN ('one', 'two') ORDER BY id",
        ),
        vec![1, 2]
    );
}

#[test]
fn bitmap_index_preserves_signed_pk_lifecycle_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}?checkpoint_on_close=off",
        dir.path().join("r4-bitmap-signed-pk").display()
    );
    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE flags (id INTEGER PRIMARY KEY, active BOOLEAN NOT NULL)",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO flags VALUES (-7, TRUE), (0, FALSE), (8, TRUE)",
            (),
        )
        .unwrap();
        db.execute("CREATE INDEX flags_active_idx ON flags(active)", ())
            .unwrap();
        db.execute("INSERT INTO flags VALUES (-9, TRUE), (10, FALSE)", ())
            .unwrap();
        assert_eq!(
            integer_column(&db, "SELECT id FROM flags WHERE active = TRUE ORDER BY id"),
            vec![-9, -7, 8]
        );

        db.execute("BEGIN", ()).unwrap();
        db.execute("UPDATE flags SET active = FALSE WHERE id = -7", ())
            .unwrap();
        db.execute("DELETE FROM flags WHERE id = -9", ()).unwrap();
        assert_eq!(
            integer_column(&db, "SELECT id FROM flags WHERE active = TRUE ORDER BY id"),
            vec![8]
        );
        db.execute("ROLLBACK", ()).unwrap();
        assert_eq!(
            integer_column(&db, "SELECT id FROM flags WHERE active = TRUE ORDER BY id"),
            vec![-9, -7, 8]
        );

        db.execute("UPDATE flags SET active = FALSE WHERE id = -7", ())
            .unwrap();
        db.execute("DELETE FROM flags WHERE id = -9", ()).unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
    }

    let db = Database::open(&dsn).unwrap();
    assert_eq!(
        integer_column(&db, "SELECT id FROM flags WHERE active = TRUE ORDER BY id"),
        vec![8]
    );
    assert_eq!(
        integer_column(&db, "SELECT id FROM flags WHERE active = FALSE ORDER BY id"),
        vec![-7, 0, 10]
    );
}

#[test]
fn unique_index_creation_rejects_cross_tier_duplicates_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}?checkpoint_on_close=off",
        dir.path().join("r4-unique-cross-tier").display()
    );
    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE keys (id INTEGER PRIMARY KEY, code TEXT NOT NULL, region TEXT NOT NULL)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO keys VALUES (1, 'same', 'west')", ())
            .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        db.execute("INSERT INTO keys VALUES (2, 'same', 'west')", ())
            .unwrap();
        assert!(db
            .execute("CREATE UNIQUE INDEX keys_code_uq ON keys(code)", ())
            .is_err());
        assert!(db
            .execute(
                "CREATE UNIQUE INDEX keys_composite_uq ON keys(code, region)",
                (),
            )
            .is_err());
    }

    let db = Database::open(&dsn).unwrap();
    assert!(db
        .execute("CREATE UNIQUE INDEX keys_code_uq ON keys(code)", ())
        .is_err());
    assert!(db
        .execute(
            "CREATE UNIQUE INDEX keys_composite_uq ON keys(code, region)",
            (),
        )
        .is_err());
}

#[test]
fn in_subquery_and_hashset_paths_keep_cold_and_hot_index_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=off&compact_threshold=2",
        dir.path().join("r4-in-candidates").display()
    );

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE targets (id INTEGER PRIMARY KEY, code TEXT NOT NULL)",
            (),
        )
        .unwrap();
        db.execute("CREATE INDEX targets_code_idx ON targets(code)", ())
            .unwrap();
        db.execute(
            "CREATE TABLE lookup_values (id INTEGER PRIMARY KEY, code TEXT NOT NULL)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO targets VALUES (1, 'cold')", ())
            .unwrap();
        db.execute(
            "INSERT INTO lookup_values VALUES (1, 'cold'), (2, 'hot')",
            (),
        )
        .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        db.execute("INSERT INTO targets VALUES (2, 'hot')", ())
            .unwrap();
    }

    let db = Database::open(&dsn).unwrap();
    assert_eq!(
        integer_column(
            &db,
            "SELECT id FROM targets WHERE code IN (SELECT code FROM lookup_values) ORDER BY id",
        ),
        vec![1, 2]
    );
    assert_eq!(
        integer_column(
            &db,
            "SELECT t.id FROM targets t WHERE EXISTS (SELECT 1 FROM lookup_values l WHERE l.code = t.code) ORDER BY t.id",
        ),
        vec![1, 2]
    );
}
