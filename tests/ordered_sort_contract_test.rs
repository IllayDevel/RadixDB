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

fn db(name: &str) -> Database {
    Database::open(&format!("memory://{name}")).expect("create database")
}

#[test]
fn ordered_aggregates_preserve_explicit_and_default_null_placement() {
    let db = db("ordered_aggregate_sort_contract");
    db.execute(
        "CREATE TABLE ordered_agg (id INTEGER PRIMARY KEY, label TEXT, sort_key INTEGER)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO ordered_agg VALUES (1, 'null-key', NULL), (2, 'low', 1), (3, 'high', 2)",
        (),
    )
    .unwrap();

    let row = db
        .query(
            "SELECT ARRAY_AGG(label ORDER BY sort_key DESC NULLS LAST), \
                    FIRST(label ORDER BY sort_key DESC NULLS LAST), \
                    LAST(label ORDER BY sort_key DESC NULLS LAST) \
             FROM ordered_agg",
            (),
        )
        .unwrap()
        .next()
        .expect("aggregate row")
        .unwrap();
    assert_eq!(
        row.get::<String>(0).unwrap(),
        "[\"high\",\"low\",\"null-key\"]"
    );
    assert_eq!(row.get::<String>(1).unwrap(), "high");
    assert_eq!(row.get::<String>(2).unwrap(), "null-key");

    let ordered: String = db
        .query_one(
            "SELECT ARRAY_AGG(label ORDER BY sort_key DESC) FROM ordered_agg",
            (),
        )
        .unwrap();
    assert_eq!(ordered, "[\"null-key\",\"high\",\"low\"]");
}

#[test]
fn ordered_aggregates_preserve_sort_contract_with_rollup() {
    let db = db("ordered_aggregate_rollup_sort_contract");
    db.execute(
        "CREATE TABLE ordered_rollup (id INTEGER PRIMARY KEY, region TEXT, label TEXT, sort_key INTEGER)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO ordered_rollup VALUES \
         (1, 'north', 'null-key', NULL), \
         (2, 'north', 'low', 1), \
         (3, 'north', 'high', 2)",
        (),
    )
    .unwrap();

    let rows = db
        .query(
            "SELECT ARRAY_AGG(label ORDER BY sort_key DESC NULLS LAST), \
                    FIRST(label ORDER BY sort_key DESC NULLS LAST), \
                    LAST(label ORDER BY sort_key DESC NULLS LAST) \
             FROM ordered_rollup GROUP BY ROLLUP(region)",
            (),
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(rows.len(), 2, "one region plus the ROLLUP grand total");
    for row in rows {
        assert_eq!(
            row.get::<String>(0).unwrap(),
            "[\"high\",\"low\",\"null-key\"]"
        );
        assert_eq!(row.get::<String>(1).unwrap(), "high");
        assert_eq!(row.get::<String>(2).unwrap(), "null-key");
    }
}

#[test]
fn ordered_aggregates_preserve_sort_contract_with_cube_and_grouping_sets() {
    let db = db("ordered_aggregate_grouping_modifiers_sort_contract");
    db.execute(
        "CREATE TABLE ordered_modifiers (\
             id INTEGER PRIMARY KEY, \
             region TEXT, \
             channel TEXT, \
             label TEXT, \
             major_key INTEGER, \
             decimal_key DECIMAL\
         )",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO ordered_modifiers VALUES \
         (1, 'one', 'web', 'decimal-null', 2, NULL), \
         (2, 'one', 'web', 'decimal-low', 2, 0.9), \
         (3, 'one', 'web', 'decimal-one-a', 2, 1.0), \
         (4, 'one', 'web', 'decimal-one-b', 2, 1.00), \
         (5, 'one', 'web', 'lower-major', 1, 0), \
         (6, 'one', 'web', 'null-major', NULL, 0)",
        (),
    )
    .unwrap();

    let cases = [
        ("CUBE(region, channel)", 4usize),
        ("GROUPING SETS ((region, channel), (region), ())", 3usize),
    ];
    let expected = "decimal-null|decimal-low|decimal-one-a|decimal-one-b|lower-major|null-major";

    for (modifier, expected_rows) in cases {
        let sql = format!(
            "SELECT \
                 STRING_AGG(label, '|' ORDER BY \
                     major_key DESC NULLS LAST, decimal_key ASC NULLS FIRST), \
                 GROUP_CONCAT(label, '|' ORDER BY \
                     major_key DESC NULLS LAST, decimal_key ASC NULLS FIRST) \
             FROM ordered_modifiers GROUP BY {modifier}"
        );
        let rows = db
            .query(&sql, ())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            rows.len(),
            expected_rows,
            "unexpected row count for {modifier}"
        );
        for row in rows {
            assert_eq!(row.get::<String>(0).unwrap(), expected, "{modifier}");
            assert_eq!(row.get::<String>(1).unwrap(), expected, "{modifier}");
        }
    }
}

#[test]
fn window_order_preserves_explicit_null_policy_with_secondary_index() {
    let db = db("ordered_window_sort_contract");
    db.execute(
        "CREATE TABLE ordered_window (id INTEGER PRIMARY KEY, sort_key INTEGER)",
        (),
    )
    .unwrap();
    db.execute(
        "INSERT INTO ordered_window VALUES (1, NULL), (2, 1), (3, 2)",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX idx_ordered_window_key ON ordered_window(sort_key)",
        (),
    )
    .unwrap();

    let rows = db
        .query(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY sort_key DESC NULLS LAST) AS rn \
             FROM ordered_window ORDER BY id",
            (),
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<i64>(1).unwrap(), 3);
    assert_eq!(rows[1].get::<i64>(1).unwrap(), 2);
    assert_eq!(rows[2].get::<i64>(1).unwrap(), 1);
}
