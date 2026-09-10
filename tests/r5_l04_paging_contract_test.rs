//! R5-L04 batch B: LIMIT/OFFSET are bound once and applied once.

use radixdb::{Database, Result};

fn db(name: &str) -> Database {
    Database::open(&format!("memory://r5_l04_paging_{name}")).expect("open in-memory database")
}

fn collect_ids(db: &Database, sql: &str) -> Result<Vec<i64>> {
    db.query(sql, ())?
        .map(|row| row.and_then(|row| row.get::<i64>(0)))
        .collect()
}

fn seed_ids(db: &Database) -> Result<()> {
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)",
        (),
    )?;
    db.execute(
        "INSERT INTO items VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
        (),
    )?;
    Ok(())
}

#[test]
fn unbounded_tvf_over_safety_limit_errors_instead_of_truncating() {
    let db = db("tvf_cap");
    assert!(db
        .query("SELECT COUNT(*) FROM generate_series(1, 10000001)", ())
        .and_then(|rows| rows.collect_vec())
        .is_err());
}

#[test]
fn pk_fast_path_obeys_limit_and_offset() -> Result<()> {
    let db = db("pk");
    seed_ids(&db)?;
    assert!(collect_ids(&db, "SELECT id FROM items WHERE id = 1 LIMIT 0")?.is_empty());
    assert!(collect_ids(&db, "SELECT id FROM items WHERE id = 1 OFFSET 1")?.is_empty());
    Ok(())
}

#[test]
fn window_page_is_computed_after_the_full_partition() -> Result<()> {
    let db = db("window");
    seed_ids(&db)?;
    let rows = db
        .query(
            "SELECT id, LEAD(id) OVER (ORDER BY id) AS next_id \
             FROM items ORDER BY id LIMIT 1 OFFSET 1",
            (),
        )?
        .collect_vec()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64>(0)?, 2);
    assert_eq!(rows[0].get::<i64>(1)?, 3);
    Ok(())
}

#[test]
fn fast_in_and_streaming_join_do_not_apply_page_twice() -> Result<()> {
    let db = db("fast_paths");
    seed_ids(&db)?;
    assert_eq!(
        collect_ids(
            &db,
            "SELECT id FROM items WHERE id IN (1,2,3,4,5) ORDER BY id LIMIT 2 OFFSET 1",
        )?,
        vec![2, 3]
    );

    db.execute("CREATE TABLE rhs (id INTEGER PRIMARY KEY)", ())?;
    db.execute("INSERT INTO rhs VALUES (1),(2),(3),(4),(5)", ())?;
    assert_eq!(
        collect_ids(
            &db,
            "SELECT items.id FROM items JOIN rhs ON items.id = rhs.id \
             ORDER BY items.id LIMIT 2 OFFSET 1",
        )?,
        vec![2, 3]
    );
    Ok(())
}

#[test]
fn offset_evaluation_errors_are_not_offset_zero() -> Result<()> {
    let db = db("offset_error");
    seed_ids(&db)?;
    assert!(db
        .query("SELECT id FROM items ORDER BY id OFFSET :missing", ())
        .is_err());
    Ok(())
}

#[test]
fn fractional_paging_is_rejected_in_ordinary_and_cte_paths() -> Result<()> {
    let db = db("fractional");
    seed_ids(&db)?;
    for sql in [
        "SELECT id FROM items LIMIT 1.5",
        "SELECT id FROM items OFFSET 0.5",
        "WITH q AS (SELECT id FROM items) SELECT id FROM q LIMIT 1.5",
        "WITH q AS (SELECT id FROM items) SELECT id FROM q OFFSET 0.5",
    ] {
        assert!(
            db.query(sql, ()).is_err(),
            "must reject fractional page: {sql}"
        );
    }
    Ok(())
}

#[test]
fn union_all_validates_all_branches_before_limit() {
    let db = db("union_validation");
    assert!(db
        .query(
            "SELECT 1 UNION ALL SELECT missing FROM no_such_table LIMIT 1",
            ()
        )
        .is_err());
    assert!(db
        .query("SELECT 1 UNION ALL SELECT 2 LIMIT 0", ())
        .and_then(|rows| rows.collect_vec())
        .is_ok_and(|rows| rows.is_empty()));
}

#[test]
fn tvf_range_narrowing_never_reverses_the_original_series() -> Result<()> {
    let db = db("tvf_range");
    assert!(collect_ids(
        &db,
        "SELECT value FROM generate_series(5, 1) WHERE value > 10 ORDER BY value",
    )?
    .is_empty());
    Ok(())
}
