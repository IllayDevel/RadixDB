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

#![cfg(feature = "stress-tests")]

use proptest::prelude::*;
use radixdb::{Database, Value};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ExactCell {
    Null(u8),
    Integer(i64),
    Float(u64),
    Boolean(bool),
    Text(String),
    Timestamp(String),
    Extension(Vec<u8>),
}

/// Creates a deterministic test database from a seed.
/// The table `t` has 20 rows with columns: id, a, b, category.
fn create_test_db(seed: u64) -> Database {
    let db = Database::open_in_memory().unwrap();
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, category TEXT)",
        (),
    )
    .unwrap();
    let categories = ["X", "Y", "Z"];
    for i in 0..20 {
        let a = ((seed.wrapping_mul(31).wrapping_add(i as u64)) % 20) as i64;
        let b = ((seed.wrapping_mul(37).wrapping_add(i as u64 * 7)) % 30) as i64;
        let cat = categories[(i % 3) as usize];
        db.execute(
            &format!("INSERT INTO t VALUES ({}, {}, {}, '{}')", i, a, b, cat),
            (),
        )
        .unwrap();
    }
    db
}

/// Execute one query without erasing SQL types, Float bits, typed NULLs or raw
/// Extension precision. Metamorphic forms must agree before presentation or
/// string formatting can hide a mismatch.
fn query_rows(db: &Database, sql: &str) -> Vec<Vec<ExactCell>> {
    let mut rows_result = db.query(sql, ()).unwrap();
    let num_cols = rows_result.columns().len();
    let mut result: Vec<Vec<ExactCell>> = Vec::new();

    for row in &mut rows_result {
        let row = row.unwrap();
        let mut typed_row = Vec::with_capacity(num_cols);
        for i in 0..num_cols {
            let cell = match row.get_value(i) {
                Some(Value::Null(data_type)) => ExactCell::Null(*data_type as u8),
                None => ExactCell::Null(radixdb::DataType::Null as u8),
                Some(Value::Integer(value)) => ExactCell::Integer(*value),
                Some(Value::Float(value)) => ExactCell::Float(value.to_bits()),
                Some(Value::Boolean(value)) => ExactCell::Boolean(*value),
                Some(Value::Text(value)) => ExactCell::Text(value.to_string()),
                Some(Value::Timestamp(value)) => ExactCell::Timestamp(value.to_rfc3339()),
                Some(Value::Extension(value)) => ExactCell::Extension(value.as_ref().to_vec()),
            };
            typed_row.push(cell);
        }
        result.push(typed_row);
    }
    result
}

/// Sorts rows lexicographically for order-independent comparison.
fn sorted_rows(mut rows: Vec<Vec<ExactCell>>) -> Vec<Vec<ExactCell>> {
    rows.sort();
    rows
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    /// Reordering AND predicates in a WHERE clause must not change results.
    #[test]
    fn prop_predicate_reorder(seed in 0u64..1000) {
        let db = create_test_db(seed);

        let r1 = sorted_rows(query_rows(&db, "SELECT * FROM t WHERE a > 5 AND b < 15 AND category = 'X'"));
        let r2 = sorted_rows(query_rows(&db, "SELECT * FROM t WHERE b < 15 AND a > 5 AND category = 'X'"));
        let r3 = sorted_rows(query_rows(&db, "SELECT * FROM t WHERE category = 'X' AND b < 15 AND a > 5"));

        prop_assert_eq!(&r1, &r2, "reorder 1 vs 2 failed for seed {}", seed);
        prop_assert_eq!(&r1, &r3, "reorder 1 vs 3 failed for seed {}", seed);
    }

    /// A CTE and a derived table (subquery in FROM) with the same logic must return the same results.
    #[test]
    fn prop_cte_subquery_equivalence(seed in 0u64..1000) {
        let db = create_test_db(seed);

        let cte_result = sorted_rows(query_rows(
            &db,
            "WITH sub AS (SELECT * FROM t WHERE a > 5) SELECT * FROM sub WHERE b < 15",
        ));
        let subquery_result = sorted_rows(query_rows(
            &db,
            "SELECT * FROM (SELECT * FROM t WHERE a > 5) AS sub WHERE b < 15",
        ));

        prop_assert_eq!(cte_result, subquery_result, "CTE vs subquery mismatch for seed {}", seed);
    }

    /// INNER JOIN is commutative: swapping the table order in FROM must not change results
    /// (given the same ON condition and the same SELECT column list).
    #[test]
    fn prop_join_commutativity(seed in 0u64..1000) {
        let db = create_test_db(seed);

        // Create a second table t2 with different seed-derived data.
        db.execute(
            "CREATE TABLE t2 (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, category TEXT)",
            (),
        ).unwrap();
        let categories = ["X", "Y", "Z"];
        for i in 0..20 {
            let a = ((seed.wrapping_mul(41).wrapping_add(i as u64 * 3)) % 20) as i64;
            let b = ((seed.wrapping_mul(43).wrapping_add(i as u64 * 11)) % 30) as i64;
            let cat = categories[((i + 1) % 3) as usize];
            db.execute(
                &format!("INSERT INTO t2 VALUES ({}, {}, {}, '{}')", i, a, b, cat),
                (),
            ).unwrap();
        }

        // Rename t -> t1 via a view so we can use t1/t2 naming consistently.
        // Actually, just use t and t2 directly.
        let r1 = sorted_rows(query_rows(
            &db,
            "SELECT t.id, t.a, t2.id, t2.a FROM t INNER JOIN t2 ON t.category = t2.category",
        ));
        let r2 = sorted_rows(query_rows(
            &db,
            "SELECT t.id, t.a, t2.id, t2.a FROM t2 INNER JOIN t ON t.category = t2.category",
        ));

        prop_assert_eq!(r1, r2, "join commutativity failed for seed {}", seed);
    }

    /// UNION (which deduplicates) of the same set with itself equals SELECT DISTINCT on that set.
    #[test]
    fn prop_union_idempotence(seed in 0u64..1000) {
        let db = create_test_db(seed);

        let union_result = sorted_rows(query_rows(
            &db,
            "SELECT id, a FROM t WHERE a > 5 UNION SELECT id, a FROM t WHERE a > 5",
        ));
        let distinct_result = sorted_rows(query_rows(
            &db,
            "SELECT DISTINCT id, a FROM t WHERE a > 5",
        ));

        prop_assert_eq!(union_result, distinct_result, "union idempotence failed for seed {}", seed);
    }

    /// COUNT(*) and SUM(1) must produce the same numeric value over any filtered set.
    #[test]
    fn prop_count_sum1_equivalence(seed in 0u64..1000) {
        let db = create_test_db(seed);

        let count_rows = query_rows(&db, "SELECT COUNT(*) FROM t WHERE a > 5");
        let sum_rows = query_rows(&db, "SELECT SUM(1) FROM t WHERE a > 5");

        // Both should return exactly one row with one column.
        prop_assert_eq!(count_rows.len(), 1, "count should return 1 row");
        prop_assert_eq!(sum_rows.len(), 1, "sum should return 1 row");
        prop_assert_eq!(count_rows[0].len(), 1, "count should return 1 column");
        prop_assert_eq!(sum_rows[0].len(), 1, "sum should return 1 column");

        prop_assert_eq!(&count_rows[0][0], &sum_rows[0][0], "COUNT(*) and SUM(1) differ for seed {}", seed);
    }

    /// Aggregates over an empty set must return well-defined values:
    /// COUNT(*) = 0, SUM/AVG/MIN/MAX = NULL.
    #[test]
    fn prop_empty_set_aggregates(seed in 0u64..1000) {
        let db = create_test_db(seed);

        let count = query_rows(&db, "SELECT COUNT(*) FROM t WHERE id = -999");
        let sum = query_rows(&db, "SELECT SUM(a) FROM t WHERE id = -999");
        let avg = query_rows(&db, "SELECT AVG(a) FROM t WHERE id = -999");
        let min = query_rows(&db, "SELECT MIN(a) FROM t WHERE id = -999");
        let max = query_rows(&db, "SELECT MAX(a) FROM t WHERE id = -999");

        prop_assert_eq!(&count[0][0], &ExactCell::Integer(0), "COUNT on empty set should be 0 for seed {}", seed);
        prop_assert!(matches!(sum[0][0], ExactCell::Null(_)), "SUM on empty set should be NULL for seed {}", seed);
        prop_assert!(matches!(avg[0][0], ExactCell::Null(_)), "AVG on empty set should be NULL for seed {}", seed);
        prop_assert!(matches!(min[0][0], ExactCell::Null(_)), "MIN on empty set should be NULL for seed {}", seed);
        prop_assert!(matches!(max[0][0], ExactCell::Null(_)), "MAX on empty set should be NULL for seed {}", seed);
    }

    /// Double negation: NOT NOT (predicate) is equivalent to the predicate itself.
    #[test]
    fn prop_double_negation(seed in 0u64..1000) {
        let db = create_test_db(seed);

        let r1 = sorted_rows(query_rows(&db, "SELECT * FROM t WHERE NOT NOT (a > 5)"));
        let r2 = sorted_rows(query_rows(&db, "SELECT * FROM t WHERE a > 5"));

        prop_assert_eq!(r1, r2, "double negation failed for seed {}", seed);
    }

    /// De Morgan's law: NOT (A AND B) is equivalent to (NOT A) OR (NOT B).
    /// We use the simplified form: NOT (a > 5 AND b < 15) <=> a <= 5 OR b >= 15.
    #[test]
    fn prop_de_morgan(seed in 0u64..1000) {
        let db = create_test_db(seed);

        let r1 = sorted_rows(query_rows(
            &db,
            "SELECT * FROM t WHERE NOT (a > 5 AND b < 15)",
        ));
        let r2 = sorted_rows(query_rows(
            &db,
            "SELECT * FROM t WHERE a <= 5 OR b >= 15",
        ));

        prop_assert_eq!(r1, r2, "De Morgan's law failed for seed {}", seed);
    }

    /// LIMIT N on an ordered result must return exactly the first N rows of the full ordered result.
    #[test]
    fn prop_limit_subset(seed in 0u64..1000) {
        let db = create_test_db(seed);

        // Don't sort these - order matters for prefix check.
        // Use , id tiebreaker to ensure deterministic order when a has duplicates.
        let limited = query_rows(&db, "SELECT id FROM t ORDER BY a, id LIMIT 5");
        let full = query_rows(&db, "SELECT id FROM t ORDER BY a, id");

        prop_assert!(
            full.len() >= 5,
            "table should have at least 5 rows, got {} for seed {}",
            full.len(), seed
        );
        prop_assert_eq!(
            limited.len(), 5,
            "LIMIT 5 should return exactly 5 rows, got {} for seed {}",
            limited.len(), seed
        );

        // The limited result must be the first 5 rows of the full result (prefix check).
        for i in 0..5 {
            prop_assert_eq!(
                &limited[i], &full[i],
                "LIMIT result row {} differs from full result for seed {}",
                i, seed
            );
        }
    }
}
