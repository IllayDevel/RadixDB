use radixdb::{Database, Result};

#[test]
fn r4_batch_c_table_admission_contract() -> Result<()> {
    let db = Database::open_in_memory()?;

    db.execute(
        "CREATE TABLE keyed (id INTEGER, value TEXT, PRIMARY KEY(id))",
        (),
    )?;
    db.execute("INSERT INTO keyed VALUES (1, 'first')", ())?;
    assert!(db
        .execute("INSERT INTO keyed VALUES (1, 'duplicate')", ())
        .is_err());

    db.execute(
        "CREATE TABLE composite_key (a INTEGER, b INTEGER, PRIMARY KEY(a,b))",
        (),
    )?;
    db.execute(
        "INSERT INTO composite_key VALUES (1, 1), (1, 2), (2, 1)",
        (),
    )?;
    assert!(db
        .execute("INSERT INTO composite_key VALUES (1, 1)", ())
        .is_err());
    assert!(db
        .execute("INSERT INTO composite_key VALUES (NULL, 3)", ())
        .is_err());
    assert!(db
        .execute("INSERT INTO composite_key VALUES (3, NULL)", ())
        .is_err());
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM composite_key", ())?,
        3
    );
    db.execute(
        "CREATE TABLE modifier_bounded (id INTEGER PRIMARY KEY, v VARCHAR(7))",
        (),
    )?;
    db.execute("INSERT INTO modifier_bounded VALUES (1, '1234567')", ())?;
    assert!(db
        .execute("INSERT INTO modifier_bounded VALUES (2, '12345678')", ())
        .is_err());
    assert!(db
        .execute(
            "CREATE TABLE action_bad (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES keyed(id) ON DELETE VANISH)",
            (),
        )
        .is_err());

    db.execute(
        "CREATE TABLE node (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY(parent_id) REFERENCES node(id))",
        (),
    )?;
    db.execute("INSERT INTO node VALUES (1, NULL)", ())?;
    db.execute("INSERT INTO node VALUES (2, 1)", ())?;
    assert!(db.execute("INSERT INTO node VALUES (3, 999)", ()).is_err());

    Ok(())
}
