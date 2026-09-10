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

//! End-to-end regression oracles for the canonical physical key contract.

use radixdb::Database;

#[test]
fn hash_join_matches_exact_mixed_numeric_keys_without_rounding_neighbors() {
    let db = Database::open("memory://numeric_physical_hash_join").unwrap();
    db.execute(
        "CREATE TABLE int_keys (id INTEGER PRIMARY KEY, join_key INTEGER)",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE float_keys (id INTEGER PRIMARY KEY, join_key FLOAT)",
        (),
    )
    .unwrap();

    let boundary = 1_i64 << 53;
    for id in 0_i64..300 {
        let key = match id / 100 {
            0 => boundary,
            1 => boundary + 1,
            _ => 0,
        };
        db.execute("INSERT INTO int_keys VALUES (?, ?)", (id, key))
            .unwrap();
    }
    for id in 0_i64..120 {
        let key = if id < 60 { boundary as f64 } else { -0.0 };
        db.execute("INSERT INTO float_keys VALUES (?, ?)", (id, key))
            .unwrap();
    }

    db.execute("ANALYZE int_keys", ()).unwrap();
    db.execute("ANALYZE float_keys", ()).unwrap();

    let plan: Vec<String> = db
        .query(
            "EXPLAIN SELECT * FROM int_keys i JOIN float_keys f \
             ON i.join_key = f.join_key",
            (),
        )
        .unwrap()
        .map(|row| row.unwrap().get(0).unwrap())
        .collect();
    assert!(
        plan.iter().any(|line| line.contains("Hash Join")),
        "expected physical Hash Join, got:\n{}",
        plan.join("\n")
    );

    let count: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM int_keys i JOIN float_keys f \
             ON i.join_key = f.join_key",
            (),
        )
        .unwrap();
    assert_eq!(count, 12_000);
}

#[test]
fn group_and_count_distinct_share_canonical_mixed_numeric_identity() {
    let db = Database::open("memory://numeric_physical_grouping").unwrap();
    let source = "(VALUES \
        (9007199254740992), \
        (9007199254740992.0), \
        (9007199254740993), \
        (0), (0.0), (-0.0)) AS keys(value)";

    let mut counts: Vec<i64> = db
        .query(
            &format!("SELECT value, COUNT(*) FROM {source} GROUP BY value"),
            (),
        )
        .unwrap()
        .map(|row| row.unwrap().get(1).unwrap())
        .collect();
    counts.sort_unstable();
    assert_eq!(counts, vec![1, 2, 3]);

    let distinct: i64 = db
        .query_one(&format!("SELECT COUNT(DISTINCT value) FROM {source}"), ())
        .unwrap();
    assert_eq!(distinct, 3);
}
