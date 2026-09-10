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

//! Exact DECIMAL predicate contract across hot, cold and mixed storage.

use radixdb::Database;

fn query_ids<P: radixdb::Params>(db: &Database, sql: &str, params: P) -> Vec<i64> {
    db.query(sql, params)
        .expect("DECIMAL predicate query succeeds")
        .map(|row| {
            row.expect("row succeeds")
                .get::<i64>(0)
                .expect("integer id")
        })
        .collect()
}

fn assert_base_predicates(db: &Database) {
    assert_eq!(
        query_ids(
            db,
            "SELECT id FROM prices WHERE amount = 50 ORDER BY id",
            ()
        ),
        vec![2]
    );
    assert_eq!(
        query_ids(
            db,
            "SELECT id FROM prices WHERE amount != 50 ORDER BY id",
            ()
        ),
        vec![1, 3, 5, 6]
    );
    assert_eq!(
        query_ids(
            db,
            "SELECT id FROM prices WHERE amount < 50 ORDER BY id",
            ()
        ),
        vec![1, 5]
    );
    assert_eq!(
        query_ids(
            db,
            "SELECT id FROM prices WHERE amount <= 50 ORDER BY id",
            ()
        ),
        vec![1, 2, 5]
    );
    assert_eq!(
        query_ids(
            db,
            "SELECT id FROM prices WHERE amount > 50 ORDER BY id",
            ()
        ),
        vec![3, 6]
    );
    assert_eq!(
        query_ids(
            db,
            "SELECT id FROM prices WHERE amount >= 50 ORDER BY id",
            ()
        ),
        vec![2, 3, 6]
    );

    // Text and FLOAT parameters are converted to the exact DECIMAL comparison
    // representation before the storage scan begins.
    assert_eq!(
        query_ids(
            db,
            "SELECT id FROM prices WHERE amount = ? ORDER BY id",
            ("50.0000",),
        ),
        vec![2]
    );
    assert_eq!(
        query_ids(
            db,
            "SELECT id FROM prices WHERE amount > ? ORDER BY id",
            (50.0,),
        ),
        vec![3, 6]
    );

    // SQL NULL comparison remains UNKNOWN; IS NULL has its normal SQL meaning.
    assert!(query_ids(db, "SELECT id FROM prices WHERE amount = NULL", ()).is_empty());
    assert_eq!(
        query_ids(db, "SELECT id FROM prices WHERE amount IS NULL", ()),
        vec![4]
    );

    // A 38-digit boundary is compared as an exact integer-scale decimal, not
    // through f64 and not lexicographically as Text.
    assert_eq!(
        query_ids(
            db,
            "SELECT id FROM prices WHERE amount = ?",
            ("99999999999999999999999999999999999999",),
        ),
        vec![6]
    );

    let plan = db
        .query(
            "EXPLAIN ANALYZE SELECT id FROM prices WHERE amount > 50 ORDER BY id",
            (),
        )
        .expect("EXPLAIN ANALYZE accepts DECIMAL predicate")
        .map(|row| row.expect("plan row").get::<String>(0).expect("plan text"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("actual time=")
            && plan.contains("amount")
            && (plan.contains("operator: Gt") || plan.contains("amount > 50")),
        "unexpected plan:\n{plan}"
    );
}

#[test]
fn decimal_predicates_are_exact_for_hot_cold_reopened_and_mixed_rows() {
    let dir = tempfile::tempdir().expect("temporary database directory");
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&sync_mode=none",
        dir.path().join("decimal_predicates").display()
    );

    {
        let db = Database::open(&dsn).expect("open hot database");
        db.execute(
            "CREATE TABLE prices (
                id INTEGER PRIMARY KEY,
                amount DECIMAL
             )",
            (),
        )
        .expect("create DECIMAL table");
        db.execute(
            "INSERT INTO prices VALUES
             (1, 49.99),
             (2, 50.0),
             (3, 50.01),
             (4, NULL),
             (5, -0.01)",
            (),
        )
        .expect("insert FLOAT literals into DECIMAL column");
        db.execute(
            "INSERT INTO prices (id, amount) VALUES (?, ?)",
            (6_i64, "99999999999999999999999999999999999999"),
        )
        .expect("insert exact 38-digit DECIMAL boundary");

        assert_base_predicates(&db);
        db.execute("PRAGMA CHECKPOINT", ())
            .expect("seal DECIMAL rows into cold artifact-backed storage");
        assert_base_predicates(&db);
        db.close().expect("close cold database");
    }

    {
        let db = Database::open(&dsn).expect("reopen cold database");
        assert_base_predicates(&db);

        db.execute("INSERT INTO prices VALUES (7, 75.25)", ())
            .expect("insert mixed hot DECIMAL row");
        assert_eq!(
            query_ids(
                &db,
                "SELECT id FROM prices WHERE amount > 50 ORDER BY id",
                ()
            ),
            vec![3, 6, 7]
        );
        assert_eq!(
            query_ids(&db, "SELECT id FROM prices WHERE amount = ?", (75.25,),),
            vec![7]
        );
    }
}
