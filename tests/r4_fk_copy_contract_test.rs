//! R4-L03 batch-D oracle: FK actions, bound SET parameters, and JSON COPY.

use radixdb::{named_params, Database};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_FILE: AtomicU64 = AtomicU64::new(1);

fn json_file(contents: &str) -> PathBuf {
    let id = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "radixdb-r4-fk-copy-{}-{id}.json",
        std::process::id()
    ));
    fs::write(&path, contents).expect("write JSON COPY fixture");
    path
}

fn copy_json(db: &Database, table: &str, path: &Path) -> radixdb::Result<i64> {
    db.execute(
        &format!("COPY {table} FROM '{}' WITH (FORMAT JSON)", path.display()),
        [],
    )
}

fn execute(db: &Database, sql: &str) -> radixdb::Result<i64> {
    db.execute(sql, [])
}

#[test]
fn fk_actions_bound_set_and_json_copy_are_fail_closed() -> radixdb::Result<()> {
    let db = Database::open_in_memory()?;

    // FK actions must route the actual referenced UNIQUE value at every level.
    execute(
        &db,
        "CREATE TABLE parents (id INTEGER PRIMARY KEY, code INTEGER UNIQUE NOT NULL)",
    )?;
    execute(
        &db,
        "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_code INTEGER UNIQUE NOT NULL \
         REFERENCES parents(code) ON DELETE CASCADE ON UPDATE CASCADE, \
         CHECK (parent_code > 0))",
    )?;
    execute(
        &db,
        "CREATE TABLE grandchildren (id INTEGER PRIMARY KEY, child_code INTEGER NOT NULL \
         REFERENCES children(parent_code) ON DELETE CASCADE ON UPDATE CASCADE)",
    )?;
    execute(&db, "INSERT INTO parents VALUES (1, 10), (2, 30)")?;
    execute(&db, "INSERT INTO children VALUES (101, 10), (102, 30)")?;
    execute(
        &db,
        "INSERT INTO grandchildren VALUES (1001, 10), (1002, 30)",
    )?;

    assert_eq!(
        execute(&db, "UPDATE parents SET code = 20 WHERE id = 1")?,
        1
    );
    assert_eq!(
        execute(
            &db,
            "UPDATE grandchildren SET child_code = child_code WHERE child_code = 20"
        )?,
        1,
        "grandchild ON UPDATE must receive the referenced child value"
    );
    assert_eq!(execute(&db, "DELETE FROM parents WHERE id = 2")?, 1);
    assert_eq!(
        execute(
            &db,
            "UPDATE grandchildren SET child_code = child_code WHERE child_code = 30"
        )?,
        0,
        "recursive ON DELETE must route the referenced value, not the child PK"
    );

    // Cascades must run ordinary row constraints atomically.
    assert!(execute(&db, "UPDATE parents SET code = -1 WHERE id = 1").is_err());
    assert_eq!(
        execute(
            &db,
            "UPDATE parents SET code = code WHERE id = 1 AND code = 20"
        )?,
        1,
        "failed child CHECK must roll the parent update back"
    );

    execute(
        &db,
        "CREATE TABLE nullable_parents (id INTEGER PRIMARY KEY, code INTEGER UNIQUE NOT NULL)",
    )?;
    execute(
        &db,
        "CREATE TABLE nonnull_children (id INTEGER PRIMARY KEY, parent_code INTEGER \
         REFERENCES nullable_parents(code) ON UPDATE SET NULL, \
         CHECK (parent_code IS NOT NULL))",
    )?;
    execute(&db, "INSERT INTO nullable_parents VALUES (1, 7)")?;
    execute(&db, "INSERT INTO nonnull_children VALUES (1, 7)")?;
    assert!(execute(&db, "UPDATE nullable_parents SET code = 8 WHERE id = 1").is_err());
    assert_eq!(
        execute(
            &db,
            "UPDATE nullable_parents SET code = code WHERE code = 7"
        )?,
        1,
        "SET NULL must honor NOT NULL and roll back the parent"
    );

    // RESTRICT applies to identity changes, not merely a column named in SET.
    execute(
        &db,
        "CREATE TABLE restricted_parents (id INTEGER PRIMARY KEY, code INTEGER UNIQUE NOT NULL)",
    )?;
    execute(
        &db,
        "CREATE TABLE restricted_children (id INTEGER PRIMARY KEY, parent_code INTEGER NOT NULL \
         REFERENCES restricted_parents(code) ON UPDATE RESTRICT)",
    )?;
    execute(&db, "INSERT INTO restricted_parents VALUES (1, 42)")?;
    execute(&db, "INSERT INTO restricted_children VALUES (1, 42)")?;
    assert_eq!(
        execute(
            &db,
            "UPDATE restricted_parents SET code = code WHERE id = 1"
        )?,
        1
    );

    // A missing named SET parameter is an error, never a successful no-op write.
    assert!(db
        .execute_named(
            "UPDATE restricted_parents SET code = :missing WHERE id = :id",
            named_params! { id: 1 },
        )
        .is_err());
    assert_eq!(
        execute(
            &db,
            "UPDATE restricted_parents SET code = code WHERE id = 1 AND code = 42"
        )?,
        1
    );

    execute(
        &db,
        "CREATE TABLE json_rows (id INTEGER PRIMARY KEY, n TEXT, marker INTEGER DEFAULT 0)",
    )?;

    let truncated = json_file(r#"[{"id":1,"n":"x"}"#);
    assert!(copy_json(&db, "json_rows", &truncated).is_err());
    assert_eq!(execute(&db, "UPDATE json_rows SET marker = 1")?, 0);

    let trailing = json_file(r#"[{"id":2,"n":"x"}] {"id":3,"n":"y"}"#);
    assert!(copy_json(&db, "json_rows", &trailing).is_err());
    assert_eq!(execute(&db, "UPDATE json_rows SET marker = 1")?, 0);

    let unterminated_string = json_file("[{\"id\":4,\"n\":\"unterminated}]");
    assert!(copy_json(&db, "json_rows", &unterminated_string).is_err());
    assert_eq!(execute(&db, "UPDATE json_rows SET marker = 1")?, 0);

    let exact_u64 = json_file(r#"[{"id":5,"n":18446744073709551615}]"#);
    assert_eq!(copy_json(&db, "json_rows", &exact_u64)?, 1);
    assert_eq!(
        execute(
            &db,
            "UPDATE json_rows SET marker = 1 WHERE id = 5 AND n = '18446744073709551615'",
        )?,
        1,
        "JSON integer text must retain every decimal digit"
    );

    execute(
        &db,
        "CREATE TABLE json_ints (id INTEGER PRIMARY KEY, n INTEGER)",
    )?;
    let overflow_i64 = json_file(r#"[{"id":1,"n":9223372036854775808}]"#);
    assert!(copy_json(&db, "json_ints", &overflow_i64).is_err());
    assert_eq!(execute(&db, "UPDATE json_ints SET n = n")?, 0);

    for path in [
        truncated,
        trailing,
        unterminated_string,
        exact_u64,
        overflow_i64,
    ] {
        let _ = fs::remove_file(path);
    }
    Ok(())
}
