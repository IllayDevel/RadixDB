// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use radixdb::{Database, Result};

fn single_text(db: &Database, sql: &str) -> Result<String> {
    db.query(sql, ())?
        .next()
        .expect("query must return one row")?
        .get(0)
}

#[test]
fn text_limit_is_durable_unicode_aware_and_storage_neutral() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("bounded-text");
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=off",
        path.display()
    );

    {
        let db = Database::open(&dsn)?;
        db.execute(
            "CREATE TABLE bounded_text (\
                id INTEGER PRIMARY KEY, \
                value TEXT(2) NOT NULL, \
                legacy_varchar VARCHAR(3), \
                legacy_char CHAR(1), \
                unbounded TEXT\
            )",
            (),
        )?;

        // The limit counts Unicode scalar values, not UTF-8 bytes.
        db.execute(
            "INSERT INTO bounded_text VALUES (1, '🙂é', 'abc', 'x', '')",
            (),
        )?;
        assert!(db
            .execute(
                "INSERT INTO bounded_text VALUES (2, 'abc', 'abc', 'x', '')",
                (),
            )
            .is_err());
        assert!(db
            .execute("UPDATE bounded_text SET value = 'xyz' WHERE id = 1", ())
            .is_err());

        assert_eq!(
            single_text(&db, "SELECT value FROM bounded_text WHERE id = 1")?,
            "🙂é"
        );
        assert_eq!(
            single_text(&db, "SELECT unbounded FROM bounded_text WHERE id = 1")?,
            ""
        );

        let create_sql: String = db
            .query("SHOW CREATE TABLE bounded_text", ())?
            .next()
            .expect("SHOW CREATE TABLE row")?
            .get(1)?;
        assert!(create_sql.contains("\"value\" TEXT(2)"), "{create_sql}");
        assert!(
            create_sql.contains("\"legacy_varchar\" TEXT(3)"),
            "{create_sql}"
        );
        assert!(
            create_sql.contains("\"legacy_char\" TEXT(1)"),
            "{create_sql}"
        );
        assert!(create_sql.contains("\"unbounded\" TEXT"), "{create_sql}");

        let descriptor: String = db.query_one("DESCRIBE TABLE bounded_text FORMAT JSON", ())?;
        let descriptor: serde_json::Value =
            serde_json::from_str(&descriptor).expect("valid schema descriptor JSON");
        let columns = descriptor["payload"]["columns"]
            .as_array()
            .expect("descriptor columns");
        let value_type = &columns
            .iter()
            .find(|column| column["name"] == "value")
            .expect("bounded value descriptor")["data_type"];
        assert_eq!(value_type["type"], "text");
        assert_eq!(value_type["max_chars"], 2);
        let unbounded_type = &columns
            .iter()
            .find(|column| column["name"] == "unbounded")
            .expect("unbounded descriptor")["data_type"];
        assert_eq!(unbounded_type, &serde_json::json!({ "type": "text" }));

        db.execute("PRAGMA CHECKPOINT", ())?;
    }

    {
        let db = Database::open(&dsn)?;
        assert_eq!(
            single_text(&db, "SELECT value FROM bounded_text WHERE id = 1")?,
            "🙂é"
        );
        assert!(db
            .execute(
                "INSERT INTO bounded_text VALUES (2, 'три', 'abc', 'x', '')",
                (),
            )
            .is_err());
    }

    Ok(())
}

#[test]
fn invalid_text_limits_fail_before_catalog_publication() -> Result<()> {
    let db = Database::open("memory://invalid-text-limits")?;

    for declaration in ["TEXT(0)", "TEXT(-1)", "TEXT(1,2)", "VARCHAR(foo)"] {
        let sql = format!("CREATE TABLE bad (id INTEGER PRIMARY KEY, value {declaration})");
        assert!(db.execute(&sql, ()).is_err(), "{declaration} must fail");
        assert!(db.query("SELECT * FROM bad", ()).is_err());
    }

    assert!(db
        .execute(
            "CREATE TABLE bad_default (\
                id INTEGER PRIMARY KEY, \
                value TEXT(2) DEFAULT 'abc'\
            )",
            (),
        )
        .is_err());
    assert!(db.query("SELECT * FROM bad_default", ()).is_err());

    Ok(())
}
