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

//! NR-07 storage-mode contract for navigable-reference projection.

use radixdb::Database;

type LabelRow = (i64, Option<String>);

fn navigation_rows(db: &Database) -> Vec<LabelRow> {
    db.query(
        "SELECT r.id, r.target_id.label
         FROM roots r
         ORDER BY r.id",
        (),
    )
    .expect("navigation query")
    .map(|row| {
        let row = row.expect("navigation row");
        (row.get(0).unwrap(), row.get(1).unwrap())
    })
    .collect()
}

fn explicit_rows(db: &Database) -> Vec<LabelRow> {
    db.query(
        "SELECT r.id, t.label
         FROM roots r
         LEFT JOIN targets t ON r.target_id = t.id
         ORDER BY r.id",
        (),
    )
    .expect("explicit LEFT JOIN")
    .map(|row| {
        let row = row.expect("join row");
        (row.get(0).unwrap(), row.get(1).unwrap())
    })
    .collect()
}

fn navigation_filtered_rows(db: &Database) -> Vec<LabelRow> {
    db.query(
        "SELECT r.id, r.target_id.label
         FROM roots r
         WHERE r.target_id.label IN ('one', 'four') OR r.target_id.label IS NULL
         ORDER BY r.id",
        (),
    )
    .expect("navigation predicate query")
    .map(|row| {
        let row = row.expect("navigation predicate row");
        (row.get(0).unwrap(), row.get(1).unwrap())
    })
    .collect()
}

fn explicit_filtered_rows(db: &Database) -> Vec<LabelRow> {
    db.query(
        "SELECT r.id, t.label
         FROM roots r
         LEFT JOIN targets t ON r.target_id = t.id
         WHERE t.label IN ('one', 'four') OR t.label IS NULL
         ORDER BY r.id",
        (),
    )
    .expect("explicit predicate LEFT JOIN")
    .map(|row| {
        let row = row.expect("explicit predicate row");
        (row.get(0).unwrap(), row.get(1).unwrap())
    })
    .collect()
}

fn explain_analyze(db: &Database) -> String {
    db.query(
        "EXPLAIN ANALYZE
         SELECT r.id, r.target_id.label
         FROM roots r
         ORDER BY r.id",
        (),
    )
    .expect("EXPLAIN ANALYZE navigation")
    .map(|row| row.unwrap().get::<String>(0).unwrap())
    .collect::<Vec<_>>()
    .join("\n")
}

fn assert_parity(db: &Database, phase: &str) {
    let navigation = navigation_rows(db);
    let explicit = explicit_rows(db);
    assert_eq!(navigation, explicit, "{phase}: navigation/JOIN parity");
    assert_eq!(
        navigation_filtered_rows(db),
        explicit_filtered_rows(db),
        "{phase}: navigation predicate/JOIN parity"
    );
}

#[test]
fn reference_projection_matches_hot_cold_hybrid_and_wal_reopen() {
    let directory = tempfile::tempdir().expect("temporary database directory");
    let dsn = format!(
        "file://{}/navigation_nr07?sync_mode=normal&checkpoint_interval=3600&checkpoint_on_close=off",
        directory.path().display()
    );

    {
        let db = Database::open(&dsn).expect("open file database");
        db.execute(
            "CREATE TABLE targets (
                id INTEGER PRIMARY KEY,
                label TEXT NOT NULL,
                unused_payload TEXT NOT NULL
            )",
            (),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE roots (
                id INTEGER PRIMARY KEY,
                target_id INTEGER REFERENCES targets(id)
            )",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO targets VALUES
                (1, 'one', 'cold-payload-one'),
                (2, 'two', 'cold-payload-two'),
                (3, 'three', 'cold-payload-three'),
                (100, 'hundred', 'sparse-manual-primary-key')",
            (),
        )
        .unwrap();
        db.execute(
            "INSERT INTO roots VALUES
                (10, 1), (11, 1), (12, 2), (13, NULL), (20, 100)",
            (),
        )
        .unwrap();

        assert_parity(&db, "hot");
        let hot = explain_analyze(&db);
        assert!(hot.contains("Storage Mode: hot_mvcc"), "{hot}");
        assert!(hot.contains("Actual Strategy: index_nested_loop"), "{hot}");
        assert!(hot.contains("Target Projection Columns: 2"), "{hot}");

        db.execute("PRAGMA CHECKPOINT", ())
            .expect("seal checkpoint");
        assert_parity(&db, "cold");
        let cold = explain_analyze(&db);
        assert!(cold.contains("Storage Mode: cold_artifact"), "{cold}");
        assert!(
            cold.contains("Actual Strategy: index_nested_loop"),
            "cold INTEGER PK navigation must use the row-id batch probe:\n{cold}"
        );
        assert!(cold.contains("fallback=0"), "{cold}");
        assert!(cold.contains("Target Projection Columns: 2"), "{cold}");

        db.execute(
            "INSERT INTO targets VALUES (4, 'four', 'wal-tail-payload')",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO roots VALUES (14, 4)", ()).unwrap();
        assert_parity(&db, "hybrid");
        let hybrid = explain_analyze(&db);
        assert!(
            hybrid.contains("Storage Mode: mixed_cold_artifact_hot"),
            "{hybrid}"
        );

        db.close().expect("close with uncheckpointed WAL tail");
    }

    let reopened = Database::open(&dsn).expect("reopen from checkpoint plus WAL tail");
    assert_parity(&reopened, "reopened WAL tail");
    assert_eq!(
        navigation_rows(&reopened),
        vec![
            (10, Some("one".to_string())),
            (11, Some("one".to_string())),
            (12, Some("two".to_string())),
            (13, None),
            (14, Some("four".to_string())),
            (20, Some("hundred".to_string())),
        ]
    );
    reopened.close().expect("close reopened database");
}
