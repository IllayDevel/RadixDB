//! R4-L03 batch C: conflict admission and correlated UPDATE identity.

use radixdb::{Database, Result};

#[test]
fn conflict_and_correlated_update_contract_is_fail_closed() -> Result<()> {
    let db = Database::open_in_memory()?;
    db.execute(
        "CREATE TABLE conflicts (id INTEGER PRIMARY KEY, value INTEGER, UNIQUE (value))",
        (),
    )?;

    for sql in [
        "INSERT INTO conflicts VALUES (1, 10) ON CONFLICT (missing) DO NOTHING",
        "INSERT INTO conflicts VALUES (1, 10) ON CONFLICT (id, id) DO NOTHING",
        "INSERT INTO conflicts VALUES (1, 10) ON CONFLICT (id, value) DO NOTHING",
    ] {
        assert!(
            db.execute(sql, ()).is_err(),
            "must reject target eagerly: {sql}"
        );
    }

    db.execute(
        "CREATE TABLE corr (bucket INTEGER, seq INTEGER, result INTEGER)",
        (),
    )?;
    db.execute("INSERT INTO corr VALUES (1, 1, 0), (1, 2, 0)", ())?;
    db.execute(
        "UPDATE corr SET result = (SELECT c2.seq + 10 FROM corr c2 WHERE c2.seq = corr.seq LIMIT 1)",
        (),
    )?;
    let rows: Vec<_> = db
        .query("SELECT seq, result FROM corr ORDER BY seq", ())?
        .collect();
    assert_eq!(rows[0].as_ref().unwrap().get::<i64>(1).unwrap(), 11);
    assert_eq!(rows[1].as_ref().unwrap().get::<i64>(1).unwrap(), 12);

    // The unmatched seq=2 row would divide by zero if correlated assignment
    // precomputation ignored the pushed WHERE candidate set.
    db.execute(
        "UPDATE corr SET result = (SELECT 100 / (corr.seq - 2) FROM corr c2 WHERE c2.seq = corr.seq LIMIT 1) WHERE seq = 1",
        (),
    )?;
    let rows: Vec<_> = db
        .query("SELECT seq, result FROM corr ORDER BY seq", ())?
        .collect();
    assert_eq!(rows[0].as_ref().unwrap().get::<i64>(1).unwrap(), -100);
    assert_eq!(rows[1].as_ref().unwrap().get::<i64>(1).unwrap(), 12);

    Ok(())
}
