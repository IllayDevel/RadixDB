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

//! SQL type parser/executor contract tests.

use radixdb::Database;

fn describe_types(db: &Database, table: &str) -> Vec<(String, String)> {
    let sql = format!("DESCRIBE {table}");
    let mut rows = db.query(&sql, ()).expect("DESCRIBE should succeed");
    let mut types = Vec::new();
    while let Some(row) = rows.next().transpose().expect("row should be ok") {
        types.push((
            row.get::<String>(0).expect("field name"),
            row.get::<String>(1).expect("field type"),
        ));
    }
    types
}

#[test]
fn create_table_accepts_supported_type_aliases_end_to_end() {
    let db = Database::open("memory://type_aliases_create").expect("open database");

    db.execute(
        "CREATE TABLE typed (
            id INT PRIMARY KEY AUTO_INCREMENT,
            tiny TINYINT,
            small SMALLINT,
            big BIGINT,
            name VARCHAR NOT NULL,
            code CHAR,
            memo CLOB,
            amount DECIMAL,
            score NUMERIC,
            ratio DOUBLE,
            sample REAL,
            flag BOOL,
            created_on DATE,
            created_at DATETIME,
            clock TIME,
            uuid_pk UUID,
            payload JSONB,
            embedding VECTOR(3),
            raw BLOB,
            raw2 BINARY,
            raw3 VARBINARY
        )",
        (),
    )
    .expect("CREATE TABLE with aliases should succeed");

    let types = describe_types(&db, "typed");
    assert_eq!(
        types,
        vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("tiny".to_string(), "INTEGER".to_string()),
            ("small".to_string(), "INTEGER".to_string()),
            ("big".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "TEXT".to_string()),
            ("code".to_string(), "TEXT".to_string()),
            ("memo".to_string(), "TEXT".to_string()),
            ("amount".to_string(), "DECIMAL".to_string()),
            ("score".to_string(), "DECIMAL".to_string()),
            ("ratio".to_string(), "FLOAT".to_string()),
            ("sample".to_string(), "FLOAT".to_string()),
            ("flag".to_string(), "BOOLEAN".to_string()),
            ("created_on".to_string(), "DATE".to_string()),
            ("created_at".to_string(), "TIMESTAMP".to_string()),
            ("clock".to_string(), "TIMESTAMP".to_string()),
            ("uuid_pk".to_string(), "UUID".to_string()),
            ("payload".to_string(), "JSON".to_string()),
            ("embedding".to_string(), "VECTOR(3)".to_string()),
            ("raw".to_string(), "BYTES".to_string()),
            ("raw2".to_string(), "BYTES".to_string()),
            ("raw3".to_string(), "BYTES".to_string()),
        ]
    );
}

#[test]
fn malformed_type_arguments_fail_end_to_end() {
    let db = Database::open("memory://type_aliases_negative").expect("open database");

    for sql in [
        "CREATE TABLE bad_vector (embedding VECTOR(name))",
        "CREATE TABLE unsupported_varchar_modifier (name VARCHAR(255))",
        "CREATE TABLE bad_decimal_zero_precision (amount DECIMAL(0,0))",
        "CREATE TABLE bad_decimal_scale (amount DECIMAL(2,3))",
        "CREATE TABLE bad_decimal_precision (amount DECIMAL(39,2))",
        "CREATE TABLE bad_decimal_arity (amount DECIMAL(10,2,1))",
        "CREATE TABLE bad_trailing (name VARCHAR(255,))",
        "CREATE TABLE bad_empty (name VARCHAR())",
    ] {
        assert!(
            db.execute(sql, ()).is_err(),
            "malformed CREATE TABLE type arguments should fail: {sql}"
        );
    }

    db.execute("CREATE TABLE typed (id INTEGER PRIMARY KEY, name TEXT)", ())
        .expect("create baseline table");

    for sql in [
        "ALTER TABLE typed ADD COLUMN embedding VECTOR(name)",
        "ALTER TABLE typed ADD COLUMN amount DECIMAL(2,3)",
        "ALTER TABLE typed ADD COLUMN bad VARCHAR(255,)",
        "ALTER TABLE typed ADD COLUMN empty VARCHAR()",
        "ALTER TABLE typed MODIFY COLUMN name VECTOR(name)",
        "ALTER TABLE typed MODIFY COLUMN name VARCHAR(255,)",
        "ALTER TABLE typed MODIFY COLUMN name VARCHAR()",
    ] {
        assert!(
            db.execute(sql, ()).is_err(),
            "malformed ALTER TABLE type arguments should fail: {sql}"
        );
    }
}

#[test]
fn alter_table_accepts_supported_type_aliases_end_to_end() {
    let db = Database::open("memory://type_aliases_alter").expect("open database");

    db.execute(
        "CREATE TABLE typed (id INTEGER PRIMARY KEY AUTO_INCREMENT)",
        (),
    )
    .expect("create table");
    db.execute("ALTER TABLE typed ADD COLUMN name VARCHAR", ())
        .expect("ADD COLUMN VARCHAR");
    db.execute("ALTER TABLE typed ADD COLUMN amount DECIMAL", ())
        .expect("ADD COLUMN DECIMAL");
    db.execute("ALTER TABLE typed ADD COLUMN raw VARBINARY", ())
        .expect("ADD COLUMN VARBINARY");
    db.execute(
        "ALTER TABLE typed MODIFY COLUMN name VARCHAR NOT NULL DEFAULT 'anonymous'",
        (),
    )
    .expect("MODIFY COLUMN VARCHAR");

    let types = describe_types(&db, "typed");
    assert_eq!(
        types,
        vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "TEXT".to_string()),
            ("amount".to_string(), "DECIMAL".to_string()),
            ("raw".to_string(), "BYTES".to_string()),
        ]
    );
}
