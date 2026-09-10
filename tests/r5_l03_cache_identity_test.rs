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

use radixdb::optimizer::fingerprint_predicate;
use radixdb::parser::{parse_sql, Expression, Statement};
use radixdb::{Database, Row, Value};

fn where_expression(sql: &str) -> Expression {
    let statements = parse_sql(sql).unwrap();
    let Statement::Select(select) = &statements[0] else {
        panic!("expected SELECT")
    };
    select.where_clause.as_deref().unwrap().clone()
}

#[test]
fn r5_l03_cache_identity_is_lexical_value_sensitive_and_shared_per_database() {
    let db = Database::open_in_memory().unwrap();

    let first: String = db.query_one("SELECT 'a  b'", ()).unwrap();
    let second: String = db.query_one("SELECT 'a b'", ()).unwrap();
    assert_eq!(first, "a  b");
    assert_eq!(second, "a b", "query-cache key changed a string literal");

    let p1 = where_expression("SELECT * FROM orders WHERE amount > 10");
    let p2 = where_expression("SELECT * FROM orders WHERE amount > 1000");
    assert_ne!(
        fingerprint_predicate("orders", &p1),
        fingerprint_predicate("orders", &p2),
        "feedback for different literal distributions must not alias"
    );

    db.execute(
        "CREATE TABLE cache_identity (id INTEGER PRIMARY KEY, value INTEGER)",
        (),
    )
    .unwrap();
    db.execute("INSERT INTO cache_identity VALUES (1, 10)", ())
        .unwrap();

    let reader = db.clone();
    let writer = db.clone();
    let before: i64 = reader
        .query_one("SELECT value FROM cache_identity WHERE id >= 1", ())
        .unwrap();
    assert_eq!(before, 10);

    writer
        .execute("UPDATE cache_identity SET value = 20 WHERE id = 1", ())
        .unwrap();
    let after: i64 = reader
        .query_one("SELECT value FROM cache_identity WHERE id >= 1", ())
        .unwrap();
    assert_eq!(after, 20, "a sibling connection returned stale cached rows");

    let cache = radixdb::executor::SemanticCache::new();
    let stale_generation = cache.generation();
    cache.invalidate_table("race");
    cache.insert_if_generation(
        stale_generation,
        "race",
        vec!["value".to_string()],
        vec![Row::from_values(vec![Value::Integer(1)])],
        None,
    );
    assert!(matches!(
        cache.lookup("race", &["value".to_string()], None),
        radixdb::executor::CacheLookupResult::Miss
    ));
}
