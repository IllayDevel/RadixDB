//! R5-L04 batch E: subquery three-valued logic, cache identity, and safe rewrites.

use radixdb::{Database, Result};

fn db(name: &str) -> Database {
    Database::open(&format!("memory://r5_l04_subquery_{name}")).expect("open in-memory database")
}

#[test]
fn all_any_preserves_unknown_values() -> Result<()> {
    let db = db("all_any");
    db.execute("CREATE TABLE vals (v INTEGER)", ())?;
    db.execute("INSERT INTO vals VALUES (NULL)", ())?;

    let all_null: Option<bool> = db.query_one("SELECT 1 > ALL(SELECT v FROM vals)", ())?;
    let any_null: Option<bool> = db.query_one("SELECT 1 = ANY(SELECT v FROM vals)", ())?;
    assert_eq!(all_null, None);
    assert_eq!(any_null, None);

    db.execute("INSERT INTO vals VALUES (2)", ())?;
    let undecided: Option<bool> = db.query_one("SELECT 1 > ANY(SELECT v FROM vals)", ())?;
    let decided: Option<bool> = db.query_one("SELECT 3 > ANY(SELECT v FROM vals)", ())?;
    assert_eq!(undecided, None);
    assert_eq!(decided, Some(true));
    Ok(())
}

#[test]
fn pk_not_in_uses_real_sparse_keys_and_null_semantics() -> Result<()> {
    let db = db("sparse_pk");
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)", ())?;
    db.execute("CREATE TABLE excluded (id INTEGER)", ())?;
    db.execute("INSERT INTO items VALUES (100),(200)", ())?;
    db.execute("INSERT INTO excluded VALUES (100)", ())?;

    let ids: Vec<i64> = db
        .query(
            "SELECT id FROM items WHERE id NOT IN (SELECT id FROM excluded) ORDER BY id",
            (),
        )?
        .map(|row| row.and_then(|row| row.get::<i64>(0)))
        .collect::<Result<_>>()?;
    assert_eq!(ids, vec![200]);

    db.execute("INSERT INTO excluded VALUES (NULL)", ())?;
    assert!(db
        .query(
            "SELECT id FROM items WHERE id NOT IN (SELECT id FROM excluded)",
            (),
        )?
        .collect_vec()?
        .is_empty());
    Ok(())
}

#[test]
fn select_without_from_uses_the_regular_logical_pipeline() -> Result<()> {
    let db = db("no_from");
    let count: i64 = db.query_one("SELECT COUNT(*) WHERE false", ())?;
    assert_eq!(count, 0);
    assert!(db
        .query("SELECT 1 HAVING false", ())?
        .collect_vec()?
        .is_empty());
    let row_number: i64 = db.query_one("SELECT ROW_NUMBER() OVER ()", ())?;
    assert_eq!(row_number, 1);
    Ok(())
}

#[test]
fn correlated_aggregate_cache_keys_include_the_full_aggregate() -> Result<()> {
    let db = db("aggregate_cache");
    db.execute("CREATE TABLE owners (id INTEGER PRIMARY KEY)", ())?;
    db.execute(
        "CREATE TABLE details (id INTEGER PRIMARY KEY, owner_id INTEGER, v INTEGER)",
        (),
    )?;
    db.execute("INSERT INTO owners VALUES (1),(2)", ())?;
    db.execute("INSERT INTO details VALUES (1,1,5),(2,1,7),(3,2,3)", ())?;

    let rows = db
        .query(
            "SELECT o.id, \
                    (SELECT SUM(d.v) FROM details d WHERE d.owner_id = o.id), \
                    (SELECT SUM(d.v * 10) FROM details d WHERE d.owner_id = o.id) \
             FROM owners o ORDER BY o.id",
            (),
        )?
        .collect_vec()?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i64>(1)?, 12);
    assert_eq!(rows[0].get::<i64>(2)?, 120);
    assert_eq!(rows[1].get::<i64>(1)?, 3);
    assert_eq!(rows[1].get::<i64>(2)?, 30);
    Ok(())
}

#[test]
fn anti_join_declines_unconvertible_inner_predicates() -> Result<()> {
    let db = db("anti_join");
    db.execute("CREATE TABLE owners (id INTEGER PRIMARY KEY)", ())?;
    db.execute(
        "CREATE TABLE details (id INTEGER PRIMARY KEY, owner_id INTEGER, tag TEXT)",
        (),
    )?;
    db.execute("INSERT INTO owners VALUES (1),(2)", ())?;
    db.execute("INSERT INTO details VALUES (1,1,'y')", ())?;

    let ids: Vec<i64> = db
        .query(
            "SELECT o.id FROM owners o \
             WHERE NOT EXISTS (SELECT 1 FROM details d \
                               WHERE d.owner_id = o.id AND LOWER(d.tag) = 'x') \
             ORDER BY o.id",
            (),
        )?
        .map(|row| row.and_then(|row| row.get::<i64>(0)))
        .collect::<Result<_>>()?;
    assert_eq!(ids, vec![1, 2]);
    Ok(())
}

#[test]
fn subquery_cache_is_scoped_to_one_execution_context() -> Result<()> {
    let first = db("cache_scope_first");
    let second = db("cache_scope_second");
    for (database, value) in [(&first, 10_i64), (&second, 20_i64)] {
        database.execute(
            "CREATE TABLE values_table (id INTEGER PRIMARY KEY, v INTEGER)",
            (),
        )?;
        database.execute(
            "INSERT INTO values_table VALUES (1, $1), (2, $2)",
            (value, value + 1),
        )?;
    }

    let sql = "SELECT (SELECT v FROM values_table WHERE id = $1)";
    assert_eq!(first.query_one::<i64, _>(sql, (1_i64,))?, 10);
    assert_eq!(second.query_one::<i64, _>(sql, (1_i64,))?, 20);
    assert_eq!(second.query_one::<i64, _>(sql, (2_i64,))?, 21);
    Ok(())
}
