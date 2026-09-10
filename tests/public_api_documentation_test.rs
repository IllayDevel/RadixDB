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

use radixdb::{named_params, params, Database, FromRow, Result, ResultRow};

#[derive(Debug, PartialEq, Eq)]
struct User {
    id: i64,
    name: String,
}

impl FromRow for User {
    fn from_row(row: &ResultRow) -> Result<Self> {
        Ok(Self {
            id: row.get(0)?,
            name: row.get_by_name("name")?,
        })
    }
}

#[test]
fn public_database_rows_statement_params_and_transaction_examples_execute() -> Result<()> {
    let db = Database::open_in_memory()?;
    db.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
        (),
    )?;

    let insert = db.prepare("INSERT INTO users VALUES ($1, $2, $3)")?;
    insert.execute((1, "Alice", 30))?;
    db.execute(
        "INSERT INTO users VALUES ($1, $2, $3)",
        params![2, "Bob", 25],
    )?;
    db.execute_named(
        "INSERT INTO users VALUES (:id, :name, :age)",
        named_params! { id: 3, name: "Charlie", age: 40 },
    )?;

    let users: Vec<User> = db.query_as("SELECT id, name FROM users ORDER BY id", ())?;
    assert_eq!(
        users,
        vec![
            User {
                id: 1,
                name: "Alice".to_string(),
            },
            User {
                id: 2,
                name: "Bob".to_string(),
            },
            User {
                id: 3,
                name: "Charlie".to_string(),
            },
        ]
    );

    let mut transaction = db.begin()?;
    transaction.execute("UPDATE users SET age = age + 1 WHERE id = $1", (1,))?;
    let age: i64 = transaction.query_one("SELECT age FROM users WHERE id = $1", (1,))?;
    assert_eq!(age, 31);
    transaction.commit()?;

    let missing: Option<String> = db.query_opt("SELECT name FROM users WHERE id = $1", (999,))?;
    assert_eq!(missing, None);
    Ok(())
}
