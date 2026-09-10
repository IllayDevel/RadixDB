//! R5-L04 batch D: CTE recursion/inlining/pushdown and view pipeline parity.

use radixdb::{Database, Result};

fn db(name: &str) -> Database {
    Database::open(&format!("memory://r5_l04_cte_{name}")).expect("open in-memory database")
}

fn ints(db: &Database, sql: &str) -> Result<Vec<i64>> {
    db.query(sql, ())?
        .map(|row| row.and_then(|row| row.get::<i64>(0)))
        .collect()
}

#[test]
fn cte_computed_paging_uses_the_common_binder() -> Result<()> {
    let db = db("paging");
    let rows: Vec<i64> = db
        .query_named(
            "WITH q AS (SELECT value AS x FROM generate_series(1,5)) \
             SELECT x FROM q ORDER BY x LIMIT 1+1 OFFSET :off",
            radixdb::named_params! { off: 1_i64 },
        )?
        .map(|row| row.and_then(|row| row.get::<i64>(0)))
        .collect::<Result<_>>()?;
    assert_eq!(rows, vec![2, 3]);
    Ok(())
}

#[test]
fn recursive_cte_binds_alias_and_member_shape() {
    let db = db("shape");
    assert!(db
        .query(
            "WITH RECURSIVE q(a,b) AS (SELECT 1 UNION ALL SELECT a+1 FROM q WHERE a<2) \
             SELECT * FROM q",
            (),
        )
        .is_err());
    assert!(db
        .query(
            "WITH RECURSIVE q(a) AS (SELECT 1 UNION ALL SELECT a+1, a FROM q WHERE a<2) \
             SELECT * FROM q",
            (),
        )
        .is_err());
}

#[test]
fn recursive_cte_cap_is_an_error_not_partial_success() {
    let db = db("cap");
    assert!(db
        .query(
            "WITH RECURSIVE q(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM q) \
             SELECT COUNT(*) FROM q",
            (),
        )
        .is_err());
}

#[test]
fn recursive_compound_modifiers_apply_after_fixpoint() -> Result<()> {
    let db = db("modifiers");
    assert_eq!(
        ints(
            &db,
            "WITH RECURSIVE q(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM q WHERE x<5 \
             ORDER BY x DESC LIMIT 2 OFFSET 1) SELECT x FROM q",
        )?,
        vec![4, 3]
    );
    Ok(())
}

#[test]
fn cte_inlining_never_drops_set_branch_dependencies() -> Result<()> {
    let db = db("inline");
    assert_eq!(
        ints(
            &db,
            "WITH q AS (SELECT 1 AS x) SELECT x FROM q UNION ALL SELECT x FROM q",
        )?,
        vec![1, 1]
    );
    Ok(())
}

#[test]
fn grouped_cte_is_not_bounded_before_join() -> Result<()> {
    let db = db("pushdown");
    db.execute("CREATE TABLE facts (id INTEGER PRIMARY KEY, k INTEGER)", ())?;
    db.execute("INSERT INTO facts VALUES (1,1),(2,2),(3,3)", ())?;
    db.execute("CREATE TABLE wanted (k INTEGER PRIMARY KEY)", ())?;
    db.execute("INSERT INTO wanted VALUES (3)", ())?;
    assert_eq!(
        ints(
            &db,
            "WITH g AS (SELECT k, COUNT(*) AS c FROM facts GROUP BY k) \
             SELECT g.k FROM g JOIN wanted w ON g.k=w.k LIMIT 1",
        )?,
        vec![3]
    );
    Ok(())
}

#[test]
fn view_aggregate_then_window_matches_derived_source() -> Result<()> {
    let db = db("view");
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)", ())?;
    db.execute("INSERT INTO t VALUES (1,10),(2,20)", ())?;
    db.execute("CREATE VIEW tv AS SELECT v FROM t", ())?;
    let rows = db
        .query(
            "SELECT SUM(v) AS total, ROW_NUMBER() OVER (ORDER BY SUM(v)) AS rn FROM tv",
            (),
        )?
        .collect_vec()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>(0)?, 30);
    assert_eq!(rows[0].get::<i64>(1)?, 1);
    Ok(())
}
