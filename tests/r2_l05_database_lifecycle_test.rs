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

use radixdb::storage::mvcc::MVCCEngine;
use radixdb::{Database, Error};
use tempfile::tempdir;

#[test]
fn r2_l05_b_shared_handles_and_terminal_engine_have_one_lifecycle_owner() {
    let dir = tempdir().unwrap();
    let dsn = format!("file://{}", dir.path().join("shared").display());

    let original = Database::open(&dsn).unwrap();
    original
        .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, v TEXT)", ())
        .unwrap();
    let clone = original.clone();
    drop(original);

    clone
        .execute("INSERT INTO items VALUES (1, 'clone survives')", ())
        .expect("dropping one handle must not close the shared engine");
    let peer = clone.clone();
    clone.close().unwrap();
    assert!(
        peer.query_one::<i64, _>("SELECT COUNT(*) FROM items", ())
            .is_err(),
        "explicit close is terminal for every shared handle"
    );

    let reopened = Database::open(&dsn).expect("successful close must release the DSN owner");
    assert_eq!(
        reopened
            .query_one::<i64, _>("SELECT COUNT(*) FROM items", ())
            .unwrap(),
        1
    );
    reopened.close().unwrap();

    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();
    engine.close_engine().unwrap();
    assert!(matches!(engine.open_engine(), Err(Error::EngineNotOpen)));
}
