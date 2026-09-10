use radixdb::api::{Database, ResultRow};
use radixdb::{named_params, params, Result};

fn main() -> Result<()> {
    let database = std::env::args()
        .nth(1)
        .expect("usage: embedded <database-directory>");
    let dsn = format!("file://{database}?sync_mode=full&checkpoint_on_close=off");
    let db = Database::open(&dsn)?;

    db.execute("DROP TABLE IF EXISTS docs_embedded_notes", ())?;
    db.execute(
        "CREATE TABLE docs_embedded_notes (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL UNIQUE,
            done BOOLEAN NOT NULL DEFAULT false
        )",
        (),
    )?;
    db.execute(
        "INSERT INTO docs_embedded_notes (id, title) VALUES ($1, $2)",
        params![1_i64, "first"],
    )?;
    db.execute_named(
        "INSERT INTO docs_embedded_notes (id, title, done)
         VALUES (:id, :title, :done)",
        named_params! { id: 2_i64, title: "second", done: true },
    )?;

    let duplicate = db.execute(
        "INSERT INTO docs_embedded_notes (id, title) VALUES ($1, $2)",
        (3_i64, "first"),
    );
    assert!(duplicate.is_err(), "UNIQUE violation must be returned");

    let rows = db
        .query(
            "SELECT id, title, done FROM docs_embedded_notes ORDER BY id",
            (),
        )?
        .collect::<Result<Vec<ResultRow>>>()?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get_by_name::<String>("title")?, "first");
    assert!(rows[1].get::<bool>(2)?);

    let mut transaction = db.begin()?;
    transaction.execute(
        "INSERT INTO docs_embedded_notes (id, title) VALUES ($1, $2)",
        (3_i64, "rolled back"),
    )?;
    assert_eq!(
        transaction.query_one::<i64, _>("SELECT COUNT(*) FROM docs_embedded_notes", (),)?,
        3
    );
    transaction.rollback()?;
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM docs_embedded_notes", ())?,
        2
    );

    db.close()?;
    println!("embedded-ok rows=2 duplicate=rejected rollback=verified");
    Ok(())
}
