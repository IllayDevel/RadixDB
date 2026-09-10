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

//! R5-L02 batch-B syntax admission and public operator-inventory contract.

use radixdb::parser::{is_operator, parse_sql};

#[test]
fn positional_parameter_zero_is_rejected_at_parse_time() {
    assert!(parse_sql("SELECT $0").is_err());
    assert!(parse_sql("SELECT $1, $2").is_ok());
}

#[test]
fn all_any_accepts_only_comparison_operators() {
    for sql in [
        "SELECT 1 = ANY (SELECT 1)",
        "SELECT 1 <> ALL (SELECT 2)",
        "SELECT 1 >= SOME (SELECT 1)",
    ] {
        assert!(parse_sql(sql).is_ok(), "valid comparison failed: {sql}");
    }
    for sql in ["SELECT 1 + ANY (SELECT 1)", "SELECT 1 || ALL (SELECT 1)"] {
        assert!(parse_sql(sql).is_err(), "invalid comparison passed: {sql}");
    }
}

#[test]
fn cast_target_is_known_and_preserves_supported_modifiers() {
    for sql in [
        "SELECT CAST('1' AS INTEGER)",
        "SELECT CAST('1' AS VARCHAR(255))",
        "SELECT CAST('1.25' AS DECIMAL(10,2))",
        "SELECT CAST('1.25' AS DECIMAL(10))",
        "SELECT CAST('[1,2]' AS VECTOR(2))",
    ] {
        assert!(parse_sql(sql).is_ok(), "valid CAST failed: {sql}");
    }
    for sql in [
        "SELECT CAST(1 AS UNKNOWN_TYPE)",
        "SELECT CAST(1 AS INTEGER(2))",
        "SELECT CAST(1 AS DECIMAL(2,3))",
        "SELECT CAST(1 AS VECTOR(0))",
    ] {
        assert!(parse_sql(sql).is_err(), "invalid CAST passed: {sql}");
    }
}

#[test]
fn quoted_interval_has_exact_shape_and_valid_unit() {
    for sql in ["SELECT INTERVAL '1 month'", "SELECT INTERVAL '-2 days'"] {
        assert!(parse_sql(sql).is_ok(), "valid INTERVAL failed: {sql}");
    }
    for sql in [
        "SELECT INTERVAL '1 month ignored'",
        "SELECT INTERVAL '1 nonsense'",
        "SELECT INTERVAL 'month'",
    ] {
        assert!(parse_sql(sql).is_err(), "invalid INTERVAL passed: {sql}");
    }
}

#[test]
fn arbitrary_infix_not_is_rejected() {
    assert!(parse_sql("SELECT 1 NOT IN (2)").is_ok());
    assert!(parse_sql("SELECT 1 NOT BETWEEN 2 AND 3").is_ok());
    assert!(parse_sql("SELECT 1 NOT 2").is_err());
}

#[test]
fn public_operator_inventory_contains_only_reachable_operators() {
    for unreachable in ["#>", "#>>", "?", "?|", "?&"] {
        assert!(
            !is_operator(unreachable),
            "unreachable JSON operator remains advertised: {unreachable}"
        );
    }
    for reachable in ["=", "<=", "||", "->", "->>", "&&", "@>", "<@"] {
        assert!(
            is_operator(reachable),
            "reachable operator missing: {reachable}"
        );
        assert!(
            parse_sql(&format!("SELECT 1 {reachable} 2")).is_ok(),
            "advertised operator is not accepted by the expression parser: {reachable}"
        );
    }
}
