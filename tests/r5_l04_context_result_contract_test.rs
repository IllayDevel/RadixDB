use radixdb::{named_params, Database};

#[test]
fn r5_l04_batch_h_context_lazy_result_and_identity_contracts() {
    let db = Database::open("memory://r5_l04_context_result").unwrap();
    db.execute(
        "CREATE TABLE temporal (id INTEGER PRIMARY KEY, value TEXT)",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO temporal VALUES (1, 'old')", ())
        .unwrap();
    db.execute("UPDATE temporal SET value = 'new' WHERE id = 1", ())
        .unwrap();
    let historical: String = db
        .query(
            "SELECT * FROM temporal AS OF TRANSACTION 2 WHERE id = 1",
            (),
        )
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .get(1)
        .unwrap();
    assert_eq!(historical, "old", "AS OF must bypass latest-PK lookup");

    db.execute("CREATE TABLE projected (id INTEGER PRIMARY KEY)", ())
        .unwrap();
    db.execute("INSERT INTO projected VALUES (7)", ()).unwrap();
    let projected: i64 = db
        .query_one_named(
            "SELECT id + :delta FROM projected",
            named_params! { delta: 5_i64 },
        )
        .unwrap();
    assert_eq!(
        projected, 12,
        "streaming projection must retain named params"
    );

    db.execute("BEGIN", ()).unwrap();
    db.execute("CREATE TABLE private_table (id INTEGER PRIMARY KEY)", ())
        .unwrap();
    let shown = db
        .query("SHOW TABLES", ())
        .unwrap()
        .map(|row| row.unwrap().get::<String>(0).unwrap())
        .collect::<Vec<_>>();
    assert!(shown.iter().any(|name| name == "private_table"));
    assert!(db.query("SHOW CREATE TABLE private_table", ()).is_ok());
    db.execute("ROLLBACK", ()).unwrap();

    db.execute("CREATE TABLE side_effect (id INTEGER PRIMARY KEY)", ())
        .unwrap();
    db.execute("CREATE TABLE bad_input (value TEXT)", ())
        .unwrap();
    db.execute("INSERT INTO bad_input VALUES ('x')", ())
        .unwrap();
    let lazy = db.execute(
        "SELECT CAST(value AS INTEGER) FROM bad_input; INSERT INTO side_effect VALUES (1)",
        (),
    );
    assert!(
        lazy.is_err(),
        "intermediate lazy error must abort the program"
    );
    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM side_effect", ())
        .unwrap();
    assert_eq!(count, 0, "later statement must not run after lazy failure");

    assert!(db.execute("COMMIT", ()).is_err());
    assert!(db.execute("ROLLBACK", ()).is_err());
    db.execute("BEGIN", ()).unwrap();
    assert!(db.execute("BEGIN", ()).is_err());
    db.execute("ROLLBACK", ()).unwrap();

    let temp = tempfile::tempdir().unwrap();
    let physical = temp.path().join("registry-db");
    let canonical_dsn = format!("file://{}?sync_mode=full&cleanup=off", physical.display());
    let alias_path = physical.join("..").join("registry-db");
    let alias_dsn = format!("file://{}?cleanup=off&sync_mode=2", alias_path.display());
    let first = Database::open(&canonical_dsn).unwrap();
    first
        .execute("CREATE TABLE shared_owner (id INTEGER PRIMARY KEY)", ())
        .unwrap();
    let second = Database::open(&alias_dsn).expect("equivalent DSN must reuse owner");
    assert!(second.table_exists("shared_owner").unwrap());
}
