//! R4-L03 batch B: catalog/DML admission precedes durable mutation.

use radixdb::{Database, Result};

fn count(db: &Database, table: &str) -> i64 {
    let rows: Vec<_> = db
        .query(&format!("SELECT COUNT(*) FROM {table}"), ())
        .unwrap()
        .collect();
    rows[0].as_ref().unwrap().get(0).unwrap()
}

#[test]
fn dml_admission_and_terminal_outcome_are_atomic() -> Result<()> {
    let db = Database::open_in_memory()?;

    db.execute("BEGIN", ())?;
    db.execute("CREATE TABLE private_t (id INTEGER PRIMARY KEY)", ())?;
    db.execute("DROP TABLE private_t", ())?;
    db.execute("COMMIT", ())?;
    assert!(db.query("SELECT * FROM private_t", ()).is_err());

    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
        (),
    )?;
    for sql in [
        "INSERT INTO t VALUES (1, 10) RETURNING missing",
        "UPDATE t SET v = 20 WHERE id = 999 RETURNING missing",
        "DELETE FROM t WHERE id = 999 RETURNING missing",
    ] {
        assert!(
            db.execute(sql, ()).is_err(),
            "must reject before mutation: {sql}"
        );
    }
    assert_eq!(count(&db, "t"), 0);

    db.execute(
        "CREATE TABLE defaults_t (id INTEGER PRIMARY KEY, v FLOAT DEFAULT RANDOM())",
        (),
    )?;
    db.execute("INSERT INTO defaults_t (id) VALUES (1)", ())?;
    db.execute("INSERT INTO defaults_t (id) VALUES (2)", ())?;
    let rows: Vec<_> = db
        .query("SELECT v FROM defaults_t ORDER BY id", ())?
        .collect();
    let first: f64 = rows[0].as_ref().unwrap().get(0).unwrap();
    let second: f64 = rows[1].as_ref().unwrap().get(0).unwrap();
    assert_ne!(
        first, second,
        "volatile DEFAULT must execute per row/statement"
    );

    assert!(db
        .execute("INSERT INTO t (id, v, v) VALUES (1, 10, 20)", ())
        .is_err());
    assert_eq!(count(&db, "t"), 0);

    db.execute(
        "CREATE TABLE checked_t (id INTEGER PRIMARY KEY, v INTEGER, CHECK (v > 0))",
        (),
    )?;
    assert!(db
        .execute(
            "INSERT INTO checked_t VALUES (1, -1) ON CONFLICT (id) DO NOTHING",
            (),
        )
        .is_err());
    assert_eq!(count(&db, "checked_t"), 0);

    db.execute("INSERT INTO t VALUES (1, 10)", ())?;
    for sql in [
        "UPDATE t SET missing = 1 WHERE id = 1",
        "UPDATE t SET v = missing + 1 WHERE id = 1",
        "INSERT INTO t VALUES (1, 11) ON CONFLICT (id) DO UPDATE SET missing = 2",
        "INSERT INTO t VALUES (1, 11) ON CONFLICT (id) DO UPDATE SET v = missing",
        "INSERT INTO t VALUES (1, 11) ON CONFLICT (id) DO UPDATE SET id = 2",
        "INSERT INTO t VALUES (1, 11) ON CONFLICT (id) DO UPDATE SET v = NULL",
    ] {
        assert!(db.execute(sql, ()).is_err(), "must fail closed: {sql}");
    }
    let rows: Vec<_> = db.query("SELECT id, v FROM t", ())?.collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].as_ref().unwrap().get::<i64>(0).unwrap(), 1);
    assert_eq!(rows[0].as_ref().unwrap().get::<i64>(1).unwrap(), 10);

    Ok(())
}
