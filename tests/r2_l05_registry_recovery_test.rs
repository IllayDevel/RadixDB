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
use tempfile::tempdir;

#[test]
fn r2_l05_a_reopen_does_not_reuse_aborted_transaction_id() {
    let dir = tempdir().unwrap();
    let dsn = format!("file://{}", dir.path().display());

    let aborted_id = {
        let db = Database::open(&dsn).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", ())
            .unwrap();
        let mut transaction = db.begin().unwrap();
        let aborted_id = transaction.id();
        transaction
            .execute("INSERT INTO t VALUES (1, 'aborted')", ())
            .unwrap();
        transaction.rollback().unwrap();
        aborted_id
    };

    let reopened = Database::open(&dsn).unwrap();
    let next = reopened.begin().unwrap();
    assert!(
        next.id() > aborted_id,
        "reopen reused transaction ID {} from retained aborted WAL",
        aborted_id
    );
}
