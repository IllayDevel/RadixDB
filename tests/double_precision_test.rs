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

#[test]
fn double_precision_is_durable_and_storage_compatible_with_float() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("double-precision");
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=off",
        path.display()
    );

    {
        let db = Database::open(&dsn)?;
        db.execute(
            "CREATE TABLE measurements (\
                id INTEGER PRIMARY KEY, \
                legacy FLOAT NOT NULL, \
                precise DOUBLE PRECISION NOT NULL, \
                short_alias DOUBLE, \
                real_alias REAL\
            )",
            (),
        )?;
        db.execute(
            "CREATE INDEX idx_measurements_precise ON measurements(precise)",
            (),
        )?;
        db.execute(
            "CREATE FUNCTION echo_double(input_value DOUBLE PRECISION NOT NULL) \
             RETURNS DOUBLE PRECISION NOT NULL LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS \
             BEGIN RETURN input_value; END;",
            (),
        )?;
        db.execute(
            "CREATE PROCEDURE echo_double_proc(\
                 IN input_value DOUBLE PRECISION NOT NULL, \
                 OUT output_value DOUBLE PRECISION NOT NULL\
             ) LANGUAGE RADIX SECURITY INVOKER AS \
             BEGIN output_value := input_value; END;",
            (),
        )?;
        db.execute(
            "CREATE VIEW measurement_view AS SELECT precise FROM measurements",
            (),
        )?;
        db.execute(
            "INSERT INTO measurements VALUES (1, 1.25, 1.25, 2.5, 3.75)",
            (),
        )?;

        assert_eq!(
            db.query_one::<f64, _>("SELECT precise FROM measurements WHERE id = 1", ())?,
            1.25
        );
        assert_eq!(
            db.query_one::<i64, _>("SELECT id FROM measurements WHERE precise = 1.25", (),)?,
            1
        );
        assert_eq!(
            db.query_one::<f64, _>("SELECT CAST(6.5 AS DOUBLE PRECISION)", ())?,
            6.5
        );
        assert_eq!(db.query_one::<f64, _>("SELECT echo_double(4.5)", ())?, 4.5);

        let create_sql: String = db
            .query("SHOW CREATE TABLE measurements", ())?
            .next()
            .expect("SHOW CREATE TABLE row")?
            .get(1)?;
        assert!(create_sql.contains("\"legacy\" FLOAT"), "{create_sql}");
        assert!(
            create_sql.contains("\"precise\" DOUBLE PRECISION"),
            "{create_sql}"
        );
        assert!(
            create_sql.contains("\"short_alias\" DOUBLE PRECISION"),
            "{create_sql}"
        );
        assert!(create_sql.contains("\"real_alias\" FLOAT"), "{create_sql}");

        let descriptor: String = db.query_one("DESCRIBE TABLE measurements FORMAT JSON", ())?;
        let descriptor: serde_json::Value =
            serde_json::from_str(&descriptor).expect("valid schema descriptor JSON");
        let columns = descriptor["payload"]["columns"]
            .as_array()
            .expect("descriptor columns");
        let type_of = |name: &str| {
            columns
                .iter()
                .find(|column| column["name"] == name)
                .expect("declared column")["data_type"]["type"]
                .as_str()
                .expect("type tag")
        };
        assert_eq!(type_of("legacy"), "float");
        assert_eq!(type_of("precise"), "double_precision");
        assert_eq!(type_of("short_alias"), "double_precision");
        assert_eq!(type_of("real_alias"), "float");

        let database_descriptor: String = db.query_one("DESCRIBE DATABASE FORMAT JSON", ())?;
        assert!(
            database_descriptor.contains("\"sql_type\":\"DOUBLE PRECISION\""),
            "{database_descriptor}"
        );
        assert!(
            database_descriptor
                .matches("\"data_type\":{\"type\":\"double_precision\"}")
                .count()
                >= 2
        );
        let database_descriptor: serde_json::Value =
            serde_json::from_str(&database_descriptor).expect("valid database descriptor JSON");
        let view = database_descriptor["payload"]["views"]
            .as_array()
            .expect("database descriptor views")
            .iter()
            .find(|view| view["name"] == "measurement_view")
            .expect("measurement_view descriptor");
        assert_eq!(
            view["result_columns"][0]["data_type"]["type"],
            "double_precision"
        );

        db.execute("PRAGMA CHECKPOINT", ())?;
    }

    {
        let db = Database::open(&dsn)?;
        assert_eq!(
            db.query_one::<f64, _>("SELECT precise FROM measurements WHERE id = 1", ())?,
            1.25
        );
        assert_eq!(
            db.query_one::<i64, _>("SELECT id FROM measurements WHERE precise = 1.25", (),)?,
            1
        );
        let create_sql: String = db
            .query("SHOW CREATE TABLE measurements", ())?
            .next()
            .expect("SHOW CREATE TABLE row")?
            .get(1)?;
        assert!(create_sql.contains("\"precise\" DOUBLE PRECISION"));
    }

    Ok(())
}

#[test]
fn double_precision_rejects_type_modifiers() -> Result<()> {
    let db = Database::open("memory://invalid-double-precision")?;
    assert!(db
        .execute(
            "CREATE TABLE invalid_double (id INTEGER PRIMARY KEY, value DOUBLE PRECISION(1))",
            (),
        )
        .is_err());
    assert!(db.query("SELECT * FROM invalid_double", ()).is_err());
    Ok(())
}
