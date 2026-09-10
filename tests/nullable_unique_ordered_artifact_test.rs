// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! SQL-to-artifact regression for nullable UNIQUE BTREE indexes.

use radixdb::Database;

fn ids(database: &Database, sql: &str) -> Vec<i64> {
    database
        .query(sql, ())
        .unwrap_or_else(|error| panic!("query failed: {sql}: {error}"))
        .map(|row| row.unwrap().get::<i64>(0).unwrap())
        .collect()
}

fn assert_null_group_order(
    database: &Database,
    table: &str,
    order: &str,
    non_null_ids: &[i64],
    nulls_first: bool,
) {
    let ordered = ids(
        database,
        &format!("SELECT id FROM {table} ORDER BY code {order} LIMIT 10"),
    );
    let (null_ids, actual_non_null) = if nulls_first {
        (&ordered[..2], &ordered[2..])
    } else {
        (&ordered[3..], &ordered[..3])
    };
    let mut null_ids = null_ids.to_vec();
    null_ids.sort_unstable();
    assert_eq!(null_ids, vec![1, 2]);
    assert_eq!(actual_non_null, non_null_ids);
}

fn assert_nullable_order(database: &Database, table: &str) {
    assert_null_group_order(database, table, "ASC", &[4, 3, 5], false);
    assert_null_group_order(database, table, "ASC NULLS FIRST", &[4, 3, 5], true);
    assert_null_group_order(database, table, "DESC NULLS FIRST", &[5, 3, 4], true);
    assert_null_group_order(database, table, "DESC NULLS LAST", &[5, 3, 4], false);

    let window = ids(
        database,
        &format!("SELECT id FROM {table} ORDER BY code ASC NULLS LAST LIMIT 3 OFFSET 1"),
    );
    assert_eq!(&window[..2], &[3, 5]);
    assert!(matches!(window[2], 1 | 2));
    assert_eq!(
        database
            .query_one::<i64, _>(&format!("SELECT COUNT(*) FROM {table}"), ())
            .unwrap(),
        5
    );
}

fn create_nullable_fixture(database: &Database, table: &str, index: &str) {
    database
        .execute(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, code INTEGER)"),
            (),
        )
        .unwrap();
    database
        .execute(&format!("CREATE UNIQUE INDEX {index} ON {table}(code)"), ())
        .unwrap();
    database
        .execute(
            &format!(
                "INSERT INTO {table} VALUES
                 (1, NULL), (2, NULL), (3, 20), (4, 10), (5, 30)"
            ),
            (),
        )
        .unwrap();
    assert!(
        database
            .execute(&format!("INSERT INTO {table} VALUES (6, 20)"), ())
            .is_err(),
        "duplicate complete UNIQUE key was accepted"
    );
}

#[test]
fn nullable_unique_btree_preserves_rows_through_checkpoint_reopen_and_rebuild() {
    let directory = tempfile::tempdir().unwrap();
    let database_root = directory.path().join("nullable-unique-ordered");
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=off&sync_mode=none",
        database_root.display()
    );

    {
        let database = Database::open(&dsn).unwrap();
        create_nullable_fixture(&database, "direct_rows", "direct_rows_code_uq");
        assert_nullable_order(&database, "direct_rows");

        database
            .execute(
                "CREATE TABLE rebuilt_rows (id INTEGER PRIMARY KEY, code INTEGER)",
                (),
            )
            .unwrap();
        database
            .execute(
                "INSERT INTO rebuilt_rows VALUES
                 (1, NULL), (2, NULL), (3, 20), (4, 10), (5, 30)",
                (),
            )
            .unwrap();
        database.execute("PRAGMA CHECKPOINT", ()).unwrap();
        database
            .execute(
                "CREATE UNIQUE INDEX rebuilt_rows_code_uq ON rebuilt_rows(code)",
                (),
            )
            .unwrap();
        database.execute("PRAGMA CHECKPOINT", ()).unwrap();
        assert_nullable_order(&database, "direct_rows");
        assert_nullable_order(&database, "rebuilt_rows");
        database.close().unwrap();
    }

    {
        let database = Database::open(&dsn).unwrap();
        assert_nullable_order(&database, "direct_rows");
        assert_nullable_order(&database, "rebuilt_rows");
        for table in ["direct_rows", "rebuilt_rows"] {
            assert!(
                database
                    .execute(&format!("INSERT INTO {table} VALUES (6, 20)"), ())
                    .is_err(),
                "reopened index accepted a duplicate complete UNIQUE key"
            );
        }

        database
            .execute(
                "CREATE TABLE composite_rows (
                    id INTEGER PRIMARY KEY,
                    scope INTEGER,
                    code INTEGER
                 )",
                (),
            )
            .unwrap();
        database
            .execute(
                "CREATE UNIQUE INDEX composite_rows_scope_code_uq
                 ON composite_rows(scope, code)",
                (),
            )
            .unwrap();
        database
            .execute(
                "INSERT INTO composite_rows VALUES
                 (1, NULL, 7), (2, NULL, 7),
                 (3, 2, NULL), (4, 2, NULL), (5, 2, 7)",
                (),
            )
            .unwrap();
        assert!(database
            .execute("INSERT INTO composite_rows VALUES (6, 2, 7)", ())
            .is_err());
        database.execute("PRAGMA CHECKPOINT", ()).unwrap();
        database.close().unwrap();
    }

    {
        let database = Database::open(&dsn).unwrap();
        assert_eq!(
            database
                .query_one::<i64, _>("SELECT COUNT(*) FROM composite_rows", ())
                .unwrap(),
            5
        );
        assert!(database
            .execute("INSERT INTO composite_rows VALUES (6, 2, 7)", ())
            .is_err());
        database.close().unwrap();
    }
}
