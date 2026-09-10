//! R5-L04 batch C: ordering, window frames, DISTINCT, and ordered shortcuts.

use radixdb::{Database, IsolationLevel, Result};

fn db(name: &str) -> Database {
    Database::open(&format!("memory://r5_l04_ordering_{name}")).expect("open in-memory database")
}

#[test]
fn navigation_range_offsets_are_value_based() -> Result<()> {
    let db = db("range");
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER, v INTEGER)",
        (),
    )?;
    db.execute("INSERT INTO t VALUES (1,1,10),(2,2,20),(3,4,40)", ())?;
    let values: Vec<i64> = db
        .query(
            "SELECT FIRST_VALUE(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) \
             FROM t ORDER BY id",
            (),
        )?
        .map(|row| row.and_then(|row| row.get::<i64>(0)))
        .collect::<Result<_>>()?;
    assert_eq!(values, vec![10, 10, 40]);
    Ok(())
}

#[test]
fn aggregate_index_shortcuts_preserve_full_admission() -> Result<()> {
    let db = db("aggregate");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)", ())?;
    db.execute("CREATE INDEX idx_v ON t(v)", ())?;
    db.execute("INSERT INTO t VALUES (1,1),(2,2)", ())?;

    let filtered = db
        .query("SELECT MIN(v) FILTER (WHERE v > 10) FROM t", ())?
        .collect_vec()?;
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].get::<Option<i64>>(0)?, None);
    assert!(db
        .query("SELECT COUNT(*) FROM t HAVING false", ())?
        .collect_vec()?
        .is_empty());
    assert!(db.query("SELECT MIN(v, id) FROM t", ()).is_err());
    Ok(())
}

#[test]
fn ordered_index_topn_is_snapshot_qualified() -> Result<()> {
    let db = db("snapshot");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k INTEGER)", ())?;
    db.execute("CREATE INDEX idx_k ON t(k)", ())?;
    db.execute("INSERT INTO t VALUES (1,10),(2,20)", ())?;

    let mut tx = db.begin_with_isolation(IsolationLevel::SnapshotIsolation)?;
    db.execute("UPDATE t SET k = 100 WHERE id = 1", ())?;
    db.execute("UPDATE t SET k = 5 WHERE id = 2", ())?;

    let rows = tx
        .query("SELECT id, k FROM t ORDER BY k LIMIT 1", ())?
        .collect_vec()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>(0)?, 1);
    assert_eq!(rows[0].get::<i64>(1)?, 10);
    tx.rollback()?;
    Ok(())
}

#[test]
fn default_null_order_is_independent_of_sort_path() -> Result<()> {
    let db = db("null_order");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)", ())?;
    db.execute(
        "INSERT INTO t VALUES (1,NULL),(2,0),(3,-9223372036854775808)",
        (),
    )?;

    let asc: Vec<Option<i64>> = db
        .query("SELECT v FROM t ORDER BY v ASC", ())?
        .map(|row| row.and_then(|row| row.get::<Option<i64>>(0)))
        .collect::<Result<_>>()?;
    assert_eq!(asc, vec![Some(i64::MIN), Some(0), None]);

    let desc: Vec<Option<i64>> = db
        .query("SELECT v FROM t ORDER BY v DESC LIMIT 3", ())?
        .map(|row| row.and_then(|row| row.get::<Option<i64>>(0)))
        .collect::<Result<_>>()?;
    assert_eq!(desc, vec![None, Some(0), Some(i64::MIN)]);
    Ok(())
}

#[test]
fn invalid_order_by_ordinals_are_binding_errors() -> Result<()> {
    let db = db("ordinal");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)", ())?;
    db.execute("INSERT INTO t VALUES (1,10)", ())?;
    for sql in [
        "SELECT id, v FROM t ORDER BY 0",
        "SELECT id, v FROM t ORDER BY 3 LIMIT 1",
    ] {
        assert!(db.query(sql, ()).is_err(), "must reject: {sql}");
    }
    Ok(())
}

#[test]
fn unresolved_distinct_on_keys_are_binding_errors() -> Result<()> {
    let db = db("distinct_on");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)", ())?;
    db.execute("INSERT INTO t VALUES (1,10),(2,20)", ())?;
    assert!(db
        .query("SELECT DISTINCT ON (missing) id FROM t ORDER BY id", ())
        .is_err());
    assert!(db
        .query("SELECT DISTINCT ON (2) id FROM t ORDER BY id", ())
        .is_err());
    Ok(())
}

#[test]
fn distinct_representative_is_chosen_after_hidden_order_key() -> Result<()> {
    let db = db("distinct_order");
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)",
        (),
    )?;
    db.execute("INSERT INTO t VALUES (1,'X',3),(2,'Y',2),(3,'X',1)", ())?;
    let values: Vec<String> = db
        .query("SELECT DISTINCT a FROM t ORDER BY b", ())?
        .map(|row| row.and_then(|row| row.get::<String>(0)))
        .collect::<Result<_>>()?;
    assert_eq!(values, vec!["X", "Y"]);
    Ok(())
}
