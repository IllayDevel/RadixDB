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

use radixdb::Database;

#[test]
fn r3_l01_batch_b_failed_statement_does_not_commit_a_prefix() {
    let db =
        Database::open("memory://r3_l01_batch_b_statement_atomicity").expect("open test database");
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)",
        (),
    )
    .expect("create table");
    db.execute("INSERT INTO items VALUES (1, 10)", ())
        .expect("seed row");

    db.execute("BEGIN", ()).expect("begin transaction");
    let error = db
        .execute("INSERT INTO items VALUES (2, 20), (1, 99)", ())
        .expect_err("late duplicate must reject the whole statement");
    let message = error.to_string().to_ascii_lowercase();
    assert!(
        message.contains("unique")
            || message.contains("duplicate")
            || message.contains("primary key constraint"),
        "unexpected statement error: {error}"
    );
    let count_before_commit: i64 = db
        .query_one("SELECT COUNT(*) FROM items", ())
        .expect("count rows inside transaction after statement rollback");
    assert_eq!(
        count_before_commit, 1,
        "failed statement prefix remained visible inside the transaction"
    );
    db.execute("COMMIT", ())
        .expect("the explicit transaction remains commit-capable");

    let count: i64 = db
        .query_one("SELECT COUNT(*) FROM items", ())
        .expect("count rows after commit");
    let original: i64 = db
        .query_one("SELECT value FROM items WHERE id = 1", ())
        .expect("read original row");
    assert_eq!(count, 1, "a failed statement published an inserted prefix");
    assert_eq!(original, 10, "a failed statement replaced the seed row");
}

#[test]
fn r3_l01_batch_b_savepoint_identity_and_target_lifetime_are_stable() {
    let db =
        Database::open("memory://r3_l01_batch_b_savepoint_identity").expect("open test database");
    db.execute(
        "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)",
        (),
    )
    .expect("create table");
    db.execute("INSERT INTO accounts VALUES (1, 100)", ())
        .expect("seed row");

    db.execute("BEGIN", ()).expect("begin transaction");
    db.execute("SAVEPOINT MixedPoint", ())
        .expect("create mixed-case savepoint");
    db.execute("UPDATE accounts SET balance = 40 WHERE id = 1", ())
        .expect("first update");
    db.execute("ROLLBACK TO mixedpoint", ())
        .expect("unquoted savepoint identifiers are case-insensitive");
    let restored: i64 = db
        .query_one("SELECT balance FROM accounts WHERE id = 1", ())
        .expect("read restored value");
    assert_eq!(restored, 100);

    db.execute("UPDATE accounts SET balance = 25 WHERE id = 1", ())
        .expect("second update");
    db.execute("ROLLBACK TO MIXEDPOINT", ())
        .expect("ROLLBACK TO retains the target savepoint");
    db.execute("RELEASE SAVEPOINT mixedpoint", ())
        .expect("release normalized savepoint name");
    db.execute("COMMIT", ()).expect("commit transaction");

    let final_balance: i64 = db
        .query_one("SELECT balance FROM accounts WHERE id = 1", ())
        .expect("read final balance");
    assert_eq!(final_balance, 100);
}

#[test]
fn r3_l01_batch_b_public_transaction_exposes_savepoint_lifecycle() {
    let db =
        Database::open("memory://r3_l01_batch_b_public_savepoint").expect("open test database");
    db.execute(
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER)",
        (),
    )
    .expect("create table");
    db.execute("INSERT INTO ledger VALUES (1, 10)", ())
        .expect("seed row");

    let mut tx = db.begin().expect("begin public transaction");
    tx.savepoint("ExactRustName")
        .expect("create public savepoint");
    tx.execute("UPDATE ledger SET amount = 99 WHERE id = 1", ())
        .expect("update after savepoint");
    tx.rollback_to_savepoint("ExactRustName")
        .expect("rollback through public facade");
    let amount: i64 = tx
        .query_one("SELECT amount FROM ledger WHERE id = 1", ())
        .expect("read transaction-local restored value");
    assert_eq!(amount, 10);
    tx.release_savepoint("ExactRustName")
        .expect("release through public facade");
    tx.commit().expect("commit public transaction");
}

#[test]
fn r3_l01_batch_b_sql_commit_error_preserves_rollback_handle() {
    let db =
        Database::open("memory://r3_l01_batch_b_sql_commit_handle").expect("open test database");
    db.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ())
        .expect("create parent table");
    db.execute(
        "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, \
         FOREIGN KEY (parent_id) REFERENCES parents(id))",
        (),
    )
    .expect("create child table");
    db.execute("INSERT INTO parents VALUES (1)", ())
        .expect("seed parent");

    db.execute("BEGIN", ())
        .expect("begin SQL child transaction");
    db.execute("INSERT INTO children VALUES (1, 1)", ())
        .expect("stage child while parent is committed");

    let mut parent_delete = db.begin().expect("begin parent delete transaction");
    parent_delete
        .execute("DELETE FROM parents WHERE id = 1", ())
        .expect("stage parent delete");
    parent_delete.commit().expect("publish parent delete");

    db.execute("COMMIT", ())
        .expect_err("FK preflight must reject child COMMIT after parent deletion");
    db.execute("ROLLBACK", ())
        .expect("failed COMMIT must preserve rollback-capable SQL handle");

    let parent_count: i64 = db
        .query_one("SELECT COUNT(*) FROM parents", ())
        .expect("count parents");
    let child_count: i64 = db
        .query_one("SELECT COUNT(*) FROM children", ())
        .expect("count children");
    assert_eq!((parent_count, child_count), (0, 0));
}
