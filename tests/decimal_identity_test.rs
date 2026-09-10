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

//! Public SQL oracles for canonical Decimal physical-key identity.

use radixdb::Database;

fn query_ids(db: &Database, sql: &str) -> Vec<i64> {
    db.query(sql, ())
        .unwrap()
        .map(|row| row.unwrap().get(0).unwrap())
        .collect()
}

fn explain(db: &Database, sql: &str) -> String {
    db.query(sql, ())
        .unwrap()
        .map(|row| row.unwrap().get::<String>(0).unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn decimal_scale_variants_share_group_distinct_join_and_unique_index_identity() {
    let db = Database::open("memory://decimal_identity").unwrap();
    db.execute(
        "CREATE TABLE left_values (id INTEGER PRIMARY KEY, amount DECIMAL)",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE right_values (id INTEGER PRIMARY KEY, amount DECIMAL)",
        (),
    )
    .unwrap();

    db.execute(
        "INSERT INTO left_values VALUES
         (1, CAST('1.0' AS DECIMAL)),
         (2, CAST('1.00' AS DECIMAL))",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO right_values VALUES
         (1, CAST('1.000' AS DECIMAL)),
         (2, CAST('1' AS DECIMAL))",
        (),
    )
    .unwrap();

    let group_counts: Vec<i64> = db
        .query(
            "SELECT amount, COUNT(*) FROM left_values GROUP BY amount",
            (),
        )
        .unwrap()
        .map(|row| row.unwrap().get(1).unwrap())
        .collect();
    assert_eq!(group_counts, vec![2]);

    let distinct: i64 = db
        .query_one("SELECT COUNT(DISTINCT amount) FROM left_values", ())
        .unwrap();
    assert_eq!(distinct, 1);

    let joined: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM left_values l JOIN right_values r
             ON l.amount = r.amount",
            (),
        )
        .unwrap();
    assert_eq!(joined, 4);

    db.execute(
        "CREATE UNIQUE INDEX right_amount_uq ON right_values (amount)",
        (),
    )
    .expect_err("scale-equivalent existing Decimal keys must violate uniqueness");
}

#[test]
fn decimal_persisted_hashes_fail_open_for_exact_and_ordered_lookups_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let dsn = format!("file://{}/decimal_index_compat", dir.path().display());
    let exact_sql =
        "SELECT id FROM measurements WHERE amount = CAST('1.0000' AS DECIMAL) ORDER BY id";
    let ordered_sql = "SELECT id FROM measurements
                       WHERE amount = CAST('1.0000' AS DECIMAL) AND sequence >= 10
                       ORDER BY sequence LIMIT 2";

    {
        let db = Database::open(&dsn).unwrap();
        db.execute(
            "CREATE TABLE measurements (
                id INTEGER PRIMARY KEY,
                amount DECIMAL NOT NULL,
                sequence INTEGER NOT NULL
            )",
            (),
        )
        .unwrap();
        db.execute("CREATE INDEX idx_amount ON measurements (amount)", ())
            .unwrap();
        db.execute(
            "CREATE INDEX idx_amount_sequence ON measurements (amount, sequence)",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO measurements VALUES
             (1, CAST('1.0' AS DECIMAL), 10),
             (2, CAST('1.00' AS DECIMAL), 20),
             (3, CAST('2.0' AS DECIMAL), 30)",
            (),
        )
        .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();

        assert_eq!(query_ids(&db, exact_sql), vec![1, 2]);
        assert_eq!(query_ids(&db, ordered_sql), vec![1, 2]);
        db.close().unwrap();
    }

    let db = Database::open(&dsn).unwrap();
    assert_eq!(query_ids(&db, exact_sql), vec![1, 2]);
    assert_eq!(query_ids(&db, ordered_sql), vec![1, 2]);

    let exact_plan = explain(&db, &format!("EXPLAIN {exact_sql}"));
    assert!(
        !exact_plan.contains("volume.exact_index")
            && !exact_plan.contains("volume.composite_exact_index"),
        "representation-sensitive persisted exact hashes must fail open:\n{exact_plan}"
    );
    let ordered_plan = explain(&db, &format!("EXPLAIN {ordered_sql}"));
    assert!(
        !ordered_plan.contains("volume.ordered_index")
            && !ordered_plan.contains("volume.composite_ordered_index"),
        "Decimal equality prefixes must not use representation-sensitive ordered hashes:\n{ordered_plan}"
    );
}
