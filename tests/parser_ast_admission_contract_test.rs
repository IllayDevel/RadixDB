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

//! R5-L02 batch-C full-tree binding and fail-closed AST admission contract.

use std::sync::Arc;

use radixdb::executor::Executor;
use radixdb::parser::parse_sql;
use radixdb::MVCCEngine;

fn executor() -> Executor {
    let engine = MVCCEngine::in_memory();
    engine.open_engine().expect("open in-memory engine");
    Executor::new(Arc::new(engine))
}

#[test]
fn prepared_plan_counts_parameters_in_every_supported_ast_subtree() {
    let executor = executor();
    for (sql, expected) in [
        ("SELECT $1 LIKE $2 ESCAPE '!'", 2),
        ("SELECT ARRAY_AGG($1 ORDER BY $3) FILTER (WHERE $4)", 4),
        ("SELECT ROW_NUMBER() OVER (ORDER BY $5)", 5),
        ("SELECT * FROM (SELECT $6 AS v) AS s", 6),
        ("WITH c AS (SELECT $7 AS v) SELECT * FROM c", 7),
        ("SELECT 1 UNION SELECT $8", 8),
        ("UPDATE t SET x = $1 RETURNING $9", 9),
    ] {
        let plan = executor
            .get_or_create_plan(sql)
            .unwrap_or_else(|error| panic!("failed to plan {sql:?}: {error}"));
        assert!(plan.has_params(), "parameters missed in {sql:?}");
        assert_eq!(plan.param_count(), expected, "wrong count for {sql:?}");
    }
}

#[test]
fn window_frame_bounds_are_literal_nonnegative_and_ordered() {
    for sql in [
        "SELECT SUM(x) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
        "SELECT SUM(x) OVER (ORDER BY x ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING)",
    ] {
        assert!(parse_sql(sql).is_ok(), "valid frame failed: {sql}");
    }

    for sql in [
        "SELECT SUM(x) OVER (ORDER BY x ROWS -1 PRECEDING)",
        "SELECT SUM(x) OVER (ORDER BY x ROWS $1 PRECEDING)",
        "SELECT SUM(x) OVER (ORDER BY x ROWS UNBOUNDED FOLLOWING)",
        "SELECT SUM(x) OVER (ORDER BY x ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING)",
        "SELECT SUM(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING)",
        "SELECT SUM(x) OVER (ORDER BY x ROWS BETWEEN 2 FOLLOWING AND 1 FOLLOWING)",
    ] {
        assert!(parse_sql(sql).is_err(), "invalid frame passed: {sql}");
    }
}

#[test]
fn scalar_calls_reject_aggregate_only_modifiers() {
    let executor = executor();
    for sql in [
        "SELECT UPPER(DISTINCT 'x')",
        "SELECT ABS(1 ORDER BY 1)",
        "SELECT LOWER('x') FILTER (WHERE TRUE)",
    ] {
        assert!(
            executor.execute(sql).is_err(),
            "invalid scalar call ran: {sql}"
        );
    }
    assert!(executor.execute("SELECT COUNT(DISTINCT 1)").is_ok());
}

#[test]
fn generic_tuple_is_rejected_instead_of_truncated() {
    let executor = executor();
    assert!(executor.execute("SELECT (1, 2)").is_err());
    assert!(executor.execute("SELECT COALESCE((1, 2), 0)").is_err());
    assert!(executor
        .execute("SELECT 1 WHERE (1, 2) IN ((1, 2))")
        .is_ok());
}

#[test]
fn nulls_clause_and_keyword_window_names_are_admitted_exactly() {
    assert!(parse_sql("SELECT 1 ORDER BY 1 NULLS FIRST").is_ok());
    assert!(parse_sql("SELECT 1 ORDER BY 1 NULLS").is_err());

    let sql = "SELECT ROW_NUMBER() OVER range WINDOW range AS (ORDER BY 1)";
    assert!(parse_sql(sql).is_ok(), "keyword window reference failed");
}
