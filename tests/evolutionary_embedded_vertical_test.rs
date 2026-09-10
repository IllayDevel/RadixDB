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

//! Permanent embedded vertical oracle for the evolutionary crate migration.
//!
//! Keep this test expressed only through the public embedded API. Its job is
//! to preserve one end-to-end contract while internal owners move to dedicated
//! crates.

use radixdb::{ApiTransaction, Database, Result};

type JoinedOrder = (i64, String, i64, i64, String);

const JOIN_QUERY: &str = "
    SELECT a.id, a.name, a.balance, o.id, o.state
    FROM accounts a
    INNER JOIN orders o ON o.account_id = a.id
    ORDER BY a.id, o.id
";

fn database_joined_orders(db: &Database) -> Result<Vec<JoinedOrder>> {
    db.query(JOIN_QUERY, ())?
        .map(|row| {
            let row = row?;
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .collect()
}

fn transaction_joined_orders(tx: &mut ApiTransaction) -> Result<Vec<JoinedOrder>> {
    tx.query(JOIN_QUERY, ())?
        .map(|row| {
            let row = row?;
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .collect()
}

fn expected_committed_rows() -> Vec<JoinedOrder> {
    vec![
        (1, "alpha".to_string(), 800, 10, "paid".to_string()),
        (1, "alpha".to_string(), 800, 11, "open".to_string()),
    ]
}

fn assert_schema_identity(db: &Database) -> Result<()> {
    let mut rows = db.query("SHOW CREATE TABLE orders", ())?;
    let row = rows.next().expect("orders schema row must exist")?;
    let table_name: String = row.get(0)?;
    let definition: String = row.get(1)?;

    assert_eq!(table_name, "orders");
    assert!(
        definition.contains("account_id")
            && definition.contains("INTEGER")
            && definition.contains("NOT NULL"),
        "orders schema lost the typed non-null account column: {definition}"
    );
    assert!(
        definition.contains("FOREIGN KEY (\"account_id\")")
            && definition.contains("REFERENCES \"accounts\"(\"id\")"),
        "orders schema lost its account reference: {definition}"
    );
    Ok(())
}

#[test]
fn evo_00_embedded_vertical_contract_survives_checkpoint_and_reopen() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database_path = directory.path().join("embedded-vertical");
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=off",
        database_path.display()
    );

    {
        let db = Database::open(&dsn)?;
        db.execute(
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                balance INTEGER NOT NULL
            )",
            (),
        )?;
        db.execute(
            "CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                account_id INTEGER NOT NULL REFERENCES accounts(id),
                amount INTEGER NOT NULL,
                state TEXT NOT NULL
            )",
            (),
        )?;

        let mut committed = db.begin()?;
        committed.execute(
            "INSERT INTO accounts VALUES
                (1, 'alpha', 1000),
                (2, 'beta', 500)",
            (),
        )?;
        committed.execute(
            "INSERT INTO orders VALUES
                (10, 1, 200, 'open'),
                (11, 1, 300, 'open'),
                (20, 2, 150, 'cancelled')",
            (),
        )?;
        committed.execute(
            "UPDATE accounts SET balance = balance - 200 WHERE id = 1",
            (),
        )?;
        committed.execute("UPDATE orders SET state = 'paid' WHERE id = 10", ())?;
        committed.execute("DELETE FROM orders WHERE id = 20", ())?;

        assert_eq!(
            transaction_joined_orders(&mut committed)?,
            expected_committed_rows()
        );
        committed.commit()?;

        let mut rolled_back = db.begin()?;
        rolled_back.execute("INSERT INTO accounts VALUES (3, 'transient', 700)", ())?;
        rolled_back.execute("INSERT INTO orders VALUES (30, 3, 70, 'open')", ())?;
        rolled_back.execute("UPDATE accounts SET balance = 0 WHERE id = 1", ())?;
        rolled_back.execute("DELETE FROM orders WHERE id = 11", ())?;

        assert_eq!(
            rolled_back.query_one::<i64, _>("SELECT COUNT(*) FROM accounts", ())?,
            3
        );
        assert_eq!(transaction_joined_orders(&mut rolled_back)?.len(), 2);
        rolled_back.rollback()?;

        assert_eq!(database_joined_orders(&db)?, expected_committed_rows());
        assert_eq!(
            db.query_one::<i64, _>("SELECT COUNT(*) FROM accounts", ())?,
            2
        );
        assert_eq!(
            db.query_one::<i64, _>("SELECT COUNT(*) FROM orders", ())?,
            2
        );
        assert_schema_identity(&db)?;

        db.execute("PRAGMA CHECKPOINT", ())?;
        assert_eq!(database_joined_orders(&db)?, expected_committed_rows());
        db.close()?;
    }

    let reopened = Database::open(&dsn)?;
    assert_eq!(
        database_joined_orders(&reopened)?,
        expected_committed_rows()
    );
    assert_eq!(
        reopened.query_one::<i64, _>("SELECT COUNT(*) FROM accounts", ())?,
        2
    );
    assert_eq!(
        reopened.query_one::<i64, _>("SELECT COUNT(*) FROM orders", ())?,
        2
    );
    assert_eq!(
        reopened.query_one::<i64, _>("SELECT COUNT(*) FROM accounts WHERE id = 3", ())?,
        0
    );
    assert_schema_identity(&reopened)?;
    reopened.close()?;

    Ok(())
}
