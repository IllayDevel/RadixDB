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

//! UUID SQL type tests.

use radixdb::{DataType, Database, Value};

const UUID_A: &str = "550e8400-e29b-41d4-a716-446655440000";
const UUID_A_COMPACT: &str = "550e8400e29b41d4a716446655440000";
const UUID_B: &str = "550e8400-e29b-41d4-a716-446655440001";

#[test]
fn uuid_accepts_strings_and_roundtrips_as_16_byte_value() {
    let db = Database::open("memory://uuid_roundtrip").expect("open database");
    db.execute(
        "CREATE TABLE items (id UUID PRIMARY KEY, name TEXT NOT NULL)",
        (),
    )
    .expect("create table");

    db.execute(
        "INSERT INTO items (id, name) VALUES (?, ?)",
        (UUID_A, "hyphenated"),
    )
    .expect("insert hyphenated uuid");
    db.execute(
        "INSERT INTO items (id, name) VALUES (?, ?)",
        (UUID_B, "second"),
    )
    .expect("insert second uuid");

    let value: Value = db
        .query_one("SELECT id FROM items WHERE id = ?", (UUID_A_COMPACT,))
        .expect("select uuid by compact string");
    assert_eq!(value.data_type(), DataType::Uuid);
    assert_eq!(value.as_uuid_bytes().expect("uuid bytes").len(), 16);
    assert_eq!(value.as_string().as_deref(), Some(UUID_A));

    let text: String = db
        .query_one("SELECT CAST(id AS TEXT) FROM items WHERE id = ?", (UUID_A,))
        .expect("cast uuid to text");
    assert_eq!(text, UUID_A);
}

#[test]
fn uuid_primary_key_auto_increment_generates_uuidv7_on_null() {
    let db = Database::open("memory://uuid_auto_increment").expect("open database");
    db.execute(
        "CREATE TABLE events (id UUID PRIMARY KEY AUTO_INCREMENT, payload TEXT)",
        (),
    )
    .expect("create table");

    db.execute("INSERT INTO events (payload) VALUES ('first')", ())
        .expect("insert first");
    db.execute(
        "INSERT INTO events (id, payload) VALUES (NULL, 'second')",
        (),
    )
    .expect("insert explicit null");

    let mut rows = db
        .query("SELECT id FROM events ORDER BY payload", ())
        .expect("select generated ids");
    let first: Value = rows
        .next()
        .expect("first row")
        .expect("first row ok")
        .get(0)
        .unwrap();
    let second: Value = rows
        .next()
        .expect("second row")
        .expect("second row ok")
        .get(0)
        .unwrap();
    assert_ne!(first, second);
    assert_eq!(first.data_type(), DataType::Uuid);
    assert_eq!(second.data_type(), DataType::Uuid);
    assert!(first.as_string().unwrap().len() == 36);
    assert!(second.as_string().unwrap().len() == 36);
}

#[test]
fn uuid_primary_key_rejects_duplicates_and_invalid_strings() {
    let db = Database::open("memory://uuid_constraints").expect("open database");
    db.execute("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT)", ())
        .expect("create table");

    db.execute("INSERT INTO users VALUES (?, 'Alice')", (UUID_A,))
        .expect("insert first uuid");
    assert!(db
        .execute(
            "INSERT INTO users VALUES (?, 'Duplicate')",
            (UUID_A_COMPACT,)
        )
        .is_err());
    assert!(db
        .execute("INSERT INTO users VALUES ('not-a-uuid', 'Bad')", ())
        .is_err());
    assert!(db
        .execute("INSERT INTO users VALUES (NULL, 'No id')", ())
        .is_err());
}

#[test]
fn uuid_type_survives_file_reopen() {
    let dir = tempfile::tempdir().expect("temp dir");
    let dsn = format!(
        "file://{}?sync_mode=normal",
        dir.path().join("uuid_db").display()
    );

    {
        let db = Database::open(&dsn).expect("open database");
        db.execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)", ())
            .expect("create table");
        db.execute("INSERT INTO items VALUES (?, 'persisted')", (UUID_A,))
            .expect("insert uuid");
    }

    let db = Database::open(&dsn).expect("reopen database");
    let text: String = db
        .query_one("SELECT id FROM items WHERE id = ?", (UUID_A,))
        .expect("select persisted uuid");
    assert_eq!(text, UUID_A);
}
