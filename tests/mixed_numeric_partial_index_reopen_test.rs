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

//! File-backed evidence for exact mixed Integer/Float partial-index predicates.

use radixdb::Database;

const EXACT: i64 = 1_i64 << 53;

fn matching_ids(db: &Database) -> Vec<i64> {
    db.query(
        "SELECT id FROM samples
         WHERE metric > 9007199254740992.0
         ORDER BY id",
        (),
    )
    .expect("mixed numeric predicate query succeeds")
    .map(|row| {
        row.expect("row succeeds")
            .get::<i64>(0)
            .expect("integer id")
    })
    .collect()
}

fn assert_duplicate_matching_row_rejected(db: &Database, id: i64) {
    let error = db
        .execute(
            "INSERT INTO samples (id, metric, marker) VALUES (?, ?, 'same')",
            (id, EXACT + 2),
        )
        .expect_err("a second matching partial-index key must be rejected");
    assert!(
        error.to_string().contains("unique constraint"),
        "unexpected duplicate error: {error}"
    );
}

#[test]
fn mixed_numeric_partial_index_predicate_is_exact_hot_cold_and_reopened() {
    let dir = tempfile::tempdir().expect("temporary database directory");
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&sync_mode=none&checkpoint_on_close=off",
        dir.path().join("mixed_numeric_partial_index").display()
    );

    {
        let db = Database::open(&dsn).expect("open hot database");
        db.execute(
            "CREATE TABLE samples (
                id INTEGER PRIMARY KEY,
                metric INTEGER NOT NULL,
                marker TEXT NOT NULL
             )",
            (),
        )
        .expect("create samples table");
        db.execute(
            "CREATE UNIQUE INDEX samples_large_marker_idx
             ON samples (marker)
             WHERE metric > 9007199254740992.0",
            (),
        )
        .expect("create exact mixed numeric partial index");

        db.execute(
            "INSERT INTO samples (id, metric, marker) VALUES (?, ?, 'same')",
            (1_i64, EXACT),
        )
        .expect("rounded exact boundary is outside predicate");
        db.execute(
            "INSERT INTO samples (id, metric, marker) VALUES (?, ?, 'same')",
            (2_i64, EXACT + 1),
        )
        .expect("integer neighbor is strictly greater and enters partial index");

        assert_eq!(matching_ids(&db), vec![2], "hot predicate result");
        assert_duplicate_matching_row_rejected(&db, 3);

        db.execute("PRAGMA CHECKPOINT", ())
            .expect("seal rows into cold artifact-backed storage");
        assert_eq!(matching_ids(&db), vec![2], "cold predicate result");
        assert_duplicate_matching_row_rejected(&db, 4);
        db.close().expect("close without an implicit checkpoint");
    }

    {
        let db = Database::open(&dsn).expect("reopen cold database");
        assert_eq!(matching_ids(&db), vec![2], "reopened predicate result");
        assert_duplicate_matching_row_rejected(&db, 5);
    }
}

fn assert_fractional_integer_ranges(db: &Database, phase: &str) {
    let collect = |sql: &str| -> Vec<i64> {
        db.query(sql, ())
            .expect("ordered range query succeeds")
            .map(|row| row.expect("row succeeds").get(0).expect("integer id"))
            .collect()
    };

    assert_eq!(
        collect(
            "SELECT id FROM integer_ranges
             WHERE bucket = 1 AND id < 0.5
             ORDER BY id LIMIT 10",
        ),
        vec![-1, 0],
        "fractional upper bound during {phase}",
    );
    assert_eq!(
        collect(
            "SELECT id FROM integer_ranges
             WHERE bucket = 1 AND id > -0.5
             ORDER BY id LIMIT 10",
        ),
        vec![0, 1, i64::MAX],
        "fractional lower bound during {phase}",
    );
    assert_eq!(
        collect(
            "SELECT id FROM integer_ranges
             WHERE bucket = 1 AND id < 9223372036854775808.0
             ORDER BY id LIMIT 10",
        ),
        vec![-1, 0, 1, i64::MAX],
        "out-of-domain upper bound during {phase}",
    );
}

#[test]
fn cold_ordered_integer_range_declines_lossy_float_bounds() {
    let dir = tempfile::tempdir().expect("temporary database directory");
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&sync_mode=none&checkpoint_on_close=off",
        dir.path().join("mixed_numeric_ordered_range").display()
    );

    {
        let db = Database::open(&dsn).expect("open hot database");
        db.execute(
            "CREATE TABLE integer_ranges (
                id INTEGER PRIMARY KEY,
                bucket INTEGER NOT NULL
             )",
            (),
        )
        .expect("create integer range table");
        db.execute(
            "CREATE INDEX integer_ranges_bucket_id_idx
             ON integer_ranges (bucket, id)",
            (),
        )
        .expect("create composite ordered index");
        for id in [-1_i64, 0, 1, i64::MAX] {
            db.execute("INSERT INTO integer_ranges VALUES (?, 1)", (id,))
                .expect("insert integer boundary row");
        }

        assert_fractional_integer_ranges(&db, "hot state");
        db.execute("PRAGMA CHECKPOINT", ())
            .expect("seal ordered index into cold artifact-backed storage");
        assert_fractional_integer_ranges(&db, "checkpointed state");
        db.close().expect("close without implicit checkpoint");
    }

    let db = Database::open(&dsn).expect("reopen cold database");
    assert_fractional_integer_ranges(&db, "reopened state");
}

fn assert_nullable_integer_range(db: &Database, phase: &str) {
    let collect = |sql: &str| -> Vec<i64> {
        db.query(sql, ())
            .expect("nullable ordered range query succeeds")
            .map(|row| row.expect("row succeeds").get(0).expect("integer id"))
            .collect()
    };

    assert_eq!(
        collect(
            "SELECT id FROM nullable_ranges
             WHERE value >= 10
             ORDER BY value ASC LIMIT 2",
        ),
        vec![2, 3],
        "ascending range excludes NULL during {phase}",
    );
    assert_eq!(
        collect(
            "SELECT id FROM nullable_ranges
             WHERE value >= 10
             ORDER BY value DESC LIMIT 1",
        ),
        vec![3],
        "descending limit is applied after NULL exclusion during {phase}",
    );
}

#[test]
fn cold_ordered_integer_range_excludes_nullable_keys_before_limit() {
    let dir = tempfile::tempdir().expect("temporary database directory");
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&sync_mode=none&checkpoint_on_close=off",
        dir.path().join("nullable_ordered_range").display()
    );

    {
        let db = Database::open(&dsn).expect("open hot database");
        db.execute(
            "CREATE TABLE nullable_ranges (
                id INTEGER PRIMARY KEY,
                value INTEGER
             )",
            (),
        )
        .expect("create nullable range table");
        db.execute(
            "CREATE INDEX nullable_ranges_value_idx ON nullable_ranges (value)",
            (),
        )
        .expect("create ordered index");
        db.execute(
            "INSERT INTO nullable_ranges VALUES
             (1, NULL), (2, 10), (3, 20), (4, NULL)",
            (),
        )
        .expect("insert nullable range rows");

        assert_nullable_integer_range(&db, "hot state");
        db.execute("PRAGMA CHECKPOINT", ())
            .expect("seal ordered index into cold artifact-backed storage");
        assert_nullable_integer_range(&db, "checkpointed state");
        db.close().expect("close without implicit checkpoint");
    }

    let db = Database::open(&dsn).expect("reopen cold database");
    assert_nullable_integer_range(&db, "reopened state");
}
