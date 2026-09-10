use radixdb::{Database, Result};

fn sql_path(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

#[test]
fn r4_batch_d_write_and_copy_admission_contract() -> Result<()> {
    let db = Database::open_in_memory()?;
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, required INTEGER NOT NULL, token FLOAT DEFAULT RANDOM())",
        (),
    )?;
    db.execute("INSERT INTO items (id, required) VALUES (1, 10)", ())?;

    assert!(db
        .execute("UPDATE items SET required=11, REQUIRED=12 WHERE id=1", ())
        .is_err());
    assert!(db
        .execute("UPDATE items SET required=NULL WHERE id=1", ())
        .is_err());

    let dir = tempfile::tempdir().expect("tempdir");
    let csv = dir.path().join("rows.csv");
    std::fs::write(&csv, "id,required\n2,20\n3,30\n").expect("write csv");
    db.execute(
        &format!(
            "COPY items(id,required) FROM '{}' WITH (FORMAT CSV, HEADER true)",
            sql_path(&csv)
        ),
        (),
    )?;
    let distinct: i64 = db.query_one(
        "SELECT COUNT(DISTINCT token) FROM items WHERE id IN (2,3)",
        (),
    )?;
    assert_eq!(distinct, 2);

    assert!(db
        .execute(
            &format!(
                "COPY items FROM '{}' WITH (FORMAT JSON, HEADER true)",
                sql_path(&csv)
            ),
            (),
        )
        .is_err());
    assert!(db
        .execute(
            &format!(
                "COPY items(id,ID) FROM '{}' WITH (FORMAT CSV, HEADER true)",
                sql_path(&csv)
            ),
            (),
        )
        .is_err());

    let duplicate_header = dir.path().join("duplicate-header.csv");
    std::fs::write(&duplicate_header, "id,ID\n4,4\n").expect("write duplicate header");
    assert!(db
        .execute(
            &format!(
                "COPY items FROM '{}' WITH (FORMAT CSV, HEADER true)",
                sql_path(&duplicate_header)
            ),
            (),
        )
        .is_err());

    let bad_reordered = dir.path().join("bad-reordered.csv");
    std::fs::write(&bad_reordered, "required,id\nnot-an-int,4\n").expect("write reordered csv");
    let error = db
        .execute(
            &format!(
                "COPY items FROM '{}' WITH (FORMAT CSV, HEADER true)",
                sql_path(&bad_reordered)
            ),
            (),
        )
        .expect_err("bad target value");
    assert!(error.to_string().contains("required"), "{error}");

    let duplicate_json = dir.path().join("duplicate.json");
    std::fs::write(&duplicate_json, "{\"id\":4,\"ID\":5,\"required\":40}\n")
        .expect("write duplicate json");
    assert!(db
        .execute(
            &format!(
                "COPY items FROM '{}' WITH (FORMAT JSON)",
                sql_path(&duplicate_json)
            ),
            (),
        )
        .is_err());

    Ok(())
}
