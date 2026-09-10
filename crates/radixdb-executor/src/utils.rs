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

//! Shared utility functions for the executor module.
//!
//! This module provides common utilities used across the executor:
//! - Token creation for internal AST construction
//! - Value-to-Expression conversion
//! - Row combination for JOIN operations
//! - Value hashing and comparison
//! - Column index map building

use std::cell::RefCell;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::{Arc, LazyLock};

use radixdb_core::CompactArc;

use lru::LruCache;
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};

use radixdb_core::StringMap;

use crate::operator::ColumnSource;
use radixdb_core::value::NULL_VALUE;
use radixdb_core::{DataType, Operator, Row, Value};
use radixdb_sql::ast::{
    BetweenExpression, BooleanLiteral, Expression, FloatLiteral, FunctionCall, Identifier,
    InExpression, InfixExpression, InfixOperator, IntegerLiteral, LikeExpression, ListExpression,
    NullLiteral, PrefixExpression, QualifiedIdentifier, StringLiteral, WindowFrameBound,
};
use radixdb_sql::token::{Position, Token, TokenType};

pub use crate::expression::{expression_to_string, string_to_datatype};
pub(crate) use crate::memory::RetainedRowsBudget;

// ============================================================================
// Token Creation Utilities
// ============================================================================

/// Static dummy token for internal AST construction - avoids allocation.
/// Token literal is not used during execution, only for display/errors.
static DUMMY_TOKEN: LazyLock<Token> =
    LazyLock::new(|| Token::new(TokenType::Identifier, String::new(), Position::default()));

/// Get a reference to a pre-allocated dummy token (zero allocation).
#[inline]
pub fn dummy_token_ref() -> &'static Token {
    &DUMMY_TOKEN
}

/// Clone the static dummy token (single String allocation for empty string).
#[inline]
pub fn dummy_token_clone() -> Token {
    DUMMY_TOKEN.clone()
}

/// Helper to create a dummy token for internal AST construction.
/// Use `dummy_token_clone()` when literal doesn't matter to avoid allocation.
#[inline]
pub fn dummy_token(literal: &str, token_type: TokenType) -> Token {
    Token::new(token_type, literal, Position::default())
}

// ============================================================================
// Value-to-Expression Conversion
// ============================================================================

/// Convert a Value to an Expression for use in subquery result replacement
/// and other internal AST manipulation.
pub fn value_to_expression(v: &Value) -> Expression {
    match v {
        Value::Integer(i) => Expression::IntegerLiteral(IntegerLiteral {
            token: dummy_token(&i.to_string(), TokenType::Integer),
            value: *i,
        }),
        Value::Float(f) => Expression::FloatLiteral(FloatLiteral {
            token: dummy_token(&f.to_string(), TokenType::Float),
            value: *f,
        }),
        Value::Text(s) => Expression::StringLiteral(StringLiteral {
            token: dummy_token(&format!("'{}'", s), TokenType::String),
            value: s.as_str().into(),
            type_hint: None,
        }),
        Value::Boolean(b) => Expression::BooleanLiteral(BooleanLiteral {
            token: dummy_token(if *b { "TRUE" } else { "FALSE" }, TokenType::Keyword),
            value: *b,
        }),
        Value::Null(_) => Expression::NullLiteral(NullLiteral {
            token: dummy_token("NULL", TokenType::Keyword),
        }),
        _ => Expression::BoundValue(Box::new(v.clone())),
    }
}

/// Substitute outer references in an expression with their actual values.
///
/// This is used for correlated subqueries to enable predicate pushdown.
/// When we have a WHERE clause like `o.user_id = u.id` where `u.id` is an outer
/// reference, this function replaces `u.id` with its actual value (e.g., 42),
/// allowing the expression `o.user_id = 42` to be pushed down to storage
/// for index usage.
///
/// # Arguments
/// * `expr` - The expression to transform
/// * `outer_row` - Map of outer column names to their values
///
/// # Returns
/// A new expression with outer references replaced by literal values.
/// Uses copy-on-write semantics: only clones when substitution is actually needed.
pub fn substitute_outer_references(
    expr: &Expression,
    outer_row: &FxHashMap<CompactArc<str>, Value>,
) -> Expression {
    // Use the internal function that returns Option for copy-on-write semantics
    substitute_outer_references_inner(expr, outer_row).unwrap_or_else(|| expr.clone())
}

/// Internal helper that returns None if no substitution was made (avoids cloning).
/// Returns Some(new_expr) only when a substitution occurred.
fn substitute_outer_references_inner(
    expr: &Expression,
    outer_row: &FxHashMap<CompactArc<str>, Value>,
) -> Option<Expression> {
    match expr {
        // Check if this is an outer reference
        Expression::QualifiedIdentifier(qid) => {
            // Try qualified name: "alias.column"
            // Use .as_str() for lookups since map now uses CompactArc<str> keys
            let qualified_name = format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower);
            if let Some(value) = outer_row.get(qualified_name.as_str()) {
                return Some(value_to_expression(value));
            }
            // A qualified name that does not match an outer qualifier belongs to
            // the local query. Falling back to the unqualified column would turn
            // `inner.id = outer.id` into `outer.id = outer.id` whenever both
            // scopes expose the same base name.
            None
        }

        // Check unqualified identifiers too
        Expression::Identifier(id) => {
            if let Some(value) = outer_row.get(id.value_lower.as_str()) {
                return Some(value_to_expression(value));
            }
            None
        }

        // Recursively handle infix expressions (AND, OR, comparisons)
        Expression::Infix(infix) => {
            let new_left = substitute_outer_references_inner(&infix.left, outer_row);
            let new_right = substitute_outer_references_inner(&infix.right, outer_row);

            // Only create new expression if something changed
            if new_left.is_some() || new_right.is_some() {
                Some(Expression::Infix(InfixExpression {
                    token: infix.token.clone(),
                    left: Box::new(new_left.unwrap_or_else(|| (*infix.left).clone())),
                    operator: infix.operator.clone(),
                    op_type: infix.op_type,
                    right: Box::new(new_right.unwrap_or_else(|| (*infix.right).clone())),
                }))
            } else {
                None
            }
        }

        // Recursively handle prefix expressions (NOT)
        Expression::Prefix(prefix) => substitute_outer_references_inner(&prefix.right, outer_row)
            .map(|new_right| {
                Expression::Prefix(PrefixExpression {
                    token: prefix.token.clone(),
                    operator: prefix.operator.clone(),
                    op_type: prefix.op_type,
                    right: Box::new(new_right),
                })
            }),

        // Handle IN expressions
        Expression::In(in_expr) => {
            let new_left = substitute_outer_references_inner(&in_expr.left, outer_row);
            let new_right = match &*in_expr.right {
                Expression::List(list) => {
                    // Check if any element changed
                    let mut any_changed = false;
                    let new_elements: Vec<Option<Expression>> = list
                        .elements
                        .iter()
                        .map(|e| {
                            let result = substitute_outer_references_inner(e, outer_row);
                            if result.is_some() {
                                any_changed = true;
                            }
                            result
                        })
                        .collect();

                    if any_changed {
                        Some(Expression::List(Box::new(ListExpression {
                            token: list.token.clone(),
                            elements: new_elements
                                .into_iter()
                                .zip(list.elements.iter())
                                .map(|(new, old)| new.unwrap_or_else(|| old.clone()))
                                .collect(),
                        })))
                    } else {
                        None
                    }
                }
                other => substitute_outer_references_inner(other, outer_row),
            };

            if new_left.is_some() || new_right.is_some() {
                Some(Expression::In(InExpression {
                    token: in_expr.token.clone(),
                    left: Box::new(new_left.unwrap_or_else(|| (*in_expr.left).clone())),
                    not: in_expr.not,
                    right: Box::new(new_right.unwrap_or_else(|| (*in_expr.right).clone())),
                }))
            } else {
                None
            }
        }

        // Handle BETWEEN expressions
        Expression::Between(between) => {
            let new_expr = substitute_outer_references_inner(&between.expr, outer_row);
            let new_lower = substitute_outer_references_inner(&between.lower, outer_row);
            let new_upper = substitute_outer_references_inner(&between.upper, outer_row);

            if new_expr.is_some() || new_lower.is_some() || new_upper.is_some() {
                Some(Expression::Between(BetweenExpression {
                    token: between.token.clone(),
                    expr: Box::new(new_expr.unwrap_or_else(|| (*between.expr).clone())),
                    not: between.not,
                    lower: Box::new(new_lower.unwrap_or_else(|| (*between.lower).clone())),
                    upper: Box::new(new_upper.unwrap_or_else(|| (*between.upper).clone())),
                }))
            } else {
                None
            }
        }

        // Handle LIKE expressions
        Expression::Like(like) => {
            let new_left = substitute_outer_references_inner(&like.left, outer_row);
            let new_pattern = substitute_outer_references_inner(&like.pattern, outer_row);
            let new_escape = like
                .escape
                .as_ref()
                .and_then(|e| substitute_outer_references_inner(e, outer_row));

            if new_left.is_some() || new_pattern.is_some() || new_escape.is_some() {
                Some(Expression::Like(LikeExpression {
                    token: like.token.clone(),
                    left: Box::new(new_left.unwrap_or_else(|| (*like.left).clone())),
                    operator: like.operator.clone(),
                    pattern: Box::new(new_pattern.unwrap_or_else(|| (*like.pattern).clone())),
                    escape: if new_escape.is_some() {
                        new_escape.map(Box::new)
                    } else {
                        like.escape.clone()
                    },
                }))
            } else {
                None
            }
        }

        // Handle function calls
        Expression::FunctionCall(func) => {
            // Check if any argument changed
            let mut any_changed = false;
            let new_args: Vec<Option<Expression>> = func
                .arguments
                .iter()
                .map(|arg| {
                    let result = substitute_outer_references_inner(arg, outer_row);
                    if result.is_some() {
                        any_changed = true;
                    }
                    result
                })
                .collect();

            let new_filter = func
                .filter
                .as_ref()
                .and_then(|f| substitute_outer_references_inner(f, outer_row));
            if new_filter.is_some() {
                any_changed = true;
            }

            if any_changed {
                Some(Expression::FunctionCall(Box::new(FunctionCall {
                    token: func.token.clone(),
                    function: func.function.clone(),
                    arguments: new_args
                        .into_iter()
                        .zip(func.arguments.iter())
                        .map(|(new, old)| new.unwrap_or_else(|| old.clone()))
                        .collect(),
                    is_distinct: func.is_distinct,
                    order_by: func.order_by.clone(),
                    filter: if new_filter.is_some() {
                        new_filter.map(Box::new)
                    } else {
                        func.filter.clone()
                    },
                })))
            } else {
                None
            }
        }

        // Literals and other expressions that don't need substitution
        _ => None,
    }
}

// ============================================================================
// Column Index Utilities
// ============================================================================

/// Build a column name to index map for fast column lookups.
/// Column names are lowercased for case-insensitive matching.
/// Also adds unqualified base names as fallbacks for qualified columns
/// (e.g., "t.val" also registers "val") when the base name is unambiguous.
pub fn build_column_index_map(columns: &[String]) -> StringMap<usize> {
    let mut map: StringMap<usize> = StringMap::with_capacity(columns.len());
    for (i, c) in columns.iter().enumerate() {
        map.insert(c.to_lowercase(), i);
    }
    // Add unqualified fallbacks for qualified column names (e.g., "t.val" → "val")
    // Only add when the base name is unambiguous (appears in exactly one table)
    // and doesn't conflict with an existing entry (e.g., an unqualified column)
    let mut base_count: StringMap<u8> = StringMap::default();
    for c in columns {
        let lower = c.to_lowercase();
        if let Some(dot_pos) = lower.rfind('.') {
            let base = &lower[dot_pos + 1..];
            let entry = base_count.entry(base.to_string()).or_insert(0);
            *entry = entry.saturating_add(1);
        }
    }
    for (i, c) in columns.iter().enumerate() {
        let lower = c.to_lowercase();
        if let Some(dot_pos) = lower.rfind('.') {
            let base = &lower[dot_pos + 1..];
            if base_count.get(base).copied().unwrap_or(0) == 1 && !map.contains_key(base) {
                map.insert(base.to_string(), i);
            }
        }
    }
    map
}

// ============================================================================
// Row Combination Utilities
// ============================================================================

/// Combine two rows into one for join output.
#[inline]
pub fn combine_rows(left: &Row, right: &Row, left_count: usize, right_count: usize) -> Vec<Value> {
    let mut combined = Vec::with_capacity(left_count + right_count);
    combined.extend(left.iter().cloned());
    combined.extend(right.iter().cloned());
    combined
}

/// Combine a row with NULLs for the other side (used in OUTER JOINs).
#[inline]
pub fn combine_rows_with_nulls(
    row: &Row,
    row_count: usize,
    null_count: usize,
    row_is_left: bool,
) -> Vec<Value> {
    let mut values = Vec::with_capacity(row_count + null_count);
    if row_is_left {
        values.extend(row.iter().cloned());
        values.resize(row_count + null_count, NULL_VALUE);
    } else {
        values.resize(null_count, NULL_VALUE);
        values.extend(row.iter().cloned());
    }
    values
}

// ============================================================================
// Hashing Utilities
// ============================================================================

/// Hash multiple key columns into a single hash value.
/// Used heavily in hash joins - called on every row during build and probe phases.
/// Uses FxHasher which is optimized for trusted keys in embedded database context.
#[inline]
pub fn hash_composite_key(row: &Row, key_indices: &[usize]) -> u64 {
    let mut hasher = FxHasher::default();

    for &idx in key_indices {
        if let Some(value) = row.get(idx) {
            hash_value_into(value, &mut hasher);
        } else {
            // NULL marker
            0xDEADBEEFu64.hash(&mut hasher);
        }
    }

    hasher.finish()
}

/// Hash a single value into an existing hasher using the canonical `Value` key contract.
#[inline]
pub fn hash_value_into<H: Hasher>(value: &Value, hasher: &mut H) {
    value.hash(hasher);
}

// ============================================================================
// Value Comparison Utilities
// ============================================================================

/// Compare two Values for SQL equi-join key equality.
#[inline]
pub fn values_equal(a: &Value, b: &Value) -> bool {
    // Structural Value equality is the canonical in-memory key contract, but
    // SQL equi-joins must still reject NULL = NULL.
    !a.is_null() && !b.is_null() && a == b
}

/// Compare two Values using canonical total ordering, with NULLs last.
pub fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.cmp(b),
    }
}

/// Verify that all composite key columns match (handles hash collisions).
#[inline]
pub fn verify_composite_key_equality(
    row1: &Row,
    row2: &Row,
    indices1: &[usize],
    indices2: &[usize],
) -> bool {
    debug_assert_eq!(indices1.len(), indices2.len());

    for (&idx1, &idx2) in indices1.iter().zip(indices2.iter()) {
        match (row1.get(idx1), row2.get(idx2)) {
            (Some(v1), Some(v2)) => {
                if !values_equal(v1, v2) {
                    return false;
                }
            }
            (None, None) => {
                // Both NULL - considered not equal in SQL join semantics
                return false;
            }
            _ => {
                // One NULL, one not - not equal
                return false;
            }
        }
    }
    true
}

// ============================================================================
// Row Utilities
// ============================================================================

/// Hash all values in a row into a single hash value.
/// Used for DISTINCT operations and set operations (UNION, INTERSECT, EXCEPT).
/// Uses FxHasher which is optimized for trusted keys in embedded database context.
#[inline]
pub fn hash_row(row: &Row) -> u64 {
    let mut hasher = FxHasher::default();
    for value in row.iter() {
        value.hash(&mut hasher);
    }
    hasher.finish()
}

/// Compare two rows for equality.
/// Returns true if both rows have the same length and all values are equal.
#[inline]
pub fn rows_equal(a: &Row, b: &Row) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for i in 0..a.len() {
        match (a.get(i), b.get(i)) {
            (Some(va), Some(vb)) if va == vb => continue,
            (None, None) => continue,
            _ => return false,
        }
    }
    true
}

// ============================================================================
// Expression Extraction Utilities
// ============================================================================

/// Extract the column name from an Identifier or QualifiedIdentifier expression.
/// Returns the column name without table qualifier.
#[inline]
pub fn extract_column_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Identifier(Identifier { value, .. }) => Some(value.to_string()),
        Expression::QualifiedIdentifier(QualifiedIdentifier { name, .. }) => {
            Some(name.value.to_string())
        }
        _ => None,
    }
}

/// Extract a literal value from an expression.
/// Converts AST literal expressions to runtime Values.
/// Note: double-quoted identifiers are NOT treated as literals here.
/// They may refer to actual column names, so pushdown should not assume
/// they are string constants. The VM/compiler handles them correctly via
/// column-resolution-first-then-string-fallback.
#[inline]
pub fn extract_literal_value(expr: &Expression) -> Option<Value> {
    match expr {
        Expression::IntegerLiteral(i) => Some(Value::Integer(i.value)),
        Expression::FloatLiteral(f) => Some(Value::Float(f.value)),
        Expression::StringLiteral(s) => Some(if let Some(type_hint) = &s.type_hint {
            match type_hint.to_uppercase().as_str() {
                "TIMESTAMP" | "DATETIME" => radixdb_core::value::parse_timestamp(&s.value)
                    .map(Value::Timestamp)
                    .unwrap_or_else(|_| Value::Text(s.value.clone())),
                "DATE" => radixdb_core::value::parse_date_days_since_unix_epoch(&s.value)
                    .map(Value::date)
                    .unwrap_or_else(|| Value::Text(s.value.clone())),
                _ => Value::Text(s.value.clone()),
            }
        } else {
            Value::Text(s.value.clone())
        }),
        Expression::BooleanLiteral(b) => Some(Value::Boolean(b.value)),
        Expression::NullLiteral(_) => Some(Value::Null(DataType::Text)),
        _ => None,
    }
}

/// Flip a comparison operator for when column and value are swapped.
/// E.g., `5 > col` becomes `col < 5`.
#[inline]
pub fn flip_operator(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::Lte => Operator::Gte,
        Operator::Gt => Operator::Lt,
        Operator::Gte => Operator::Lte,
        other => other, // Eq, Ne are symmetric
    }
}

/// Convert AST InfixOperator to core Operator.
/// Returns None for operators that don't map to comparison operators.
#[inline]
pub fn infix_to_operator(op: InfixOperator) -> Option<Operator> {
    match op {
        InfixOperator::Equal => Some(Operator::Eq),
        InfixOperator::NotEqual => Some(Operator::Ne),
        InfixOperator::LessThan => Some(Operator::Lt),
        InfixOperator::LessEqual => Some(Operator::Lte),
        InfixOperator::GreaterThan => Some(Operator::Gt),
        InfixOperator::GreaterEqual => Some(Operator::Gte),
        _ => None,
    }
}

// ============================================================================
// Column Name Utilities
// ============================================================================

/// Extract the base (unqualified) column name from a potentially qualified column name.
/// For "table.column" returns "column", for "column" returns "column".
/// The result is always lowercase for case-insensitive comparisons.
#[inline]
pub fn extract_base_column_name(col_name: &str) -> String {
    if let Some(dot_idx) = col_name.rfind('.') {
        col_name[dot_idx + 1..].to_lowercase()
    } else {
        col_name.to_lowercase()
    }
}

// ============================================================================
// Expression Analysis Utilities
// ============================================================================

/// Check if an expression contains any Parameter nodes ($1, $2, etc.)
/// Parameterized queries cannot be semantically cached because the cache
/// stores results tied to specific parameter values, but the AST only
/// contains parameter indices, not values.
pub fn expression_has_parameters(expr: &Expression) -> bool {
    match expr {
        Expression::Parameter(_) => true,
        Expression::Prefix(prefix) => expression_has_parameters(&prefix.right),
        Expression::Infix(infix) => {
            expression_has_parameters(&infix.left) || expression_has_parameters(&infix.right)
        }
        Expression::In(in_expr) => {
            expression_has_parameters(&in_expr.left)
                || match in_expr.right.as_ref() {
                    Expression::List(list) => list.elements.iter().any(expression_has_parameters),
                    Expression::ExpressionList(list) => {
                        list.expressions.iter().any(expression_has_parameters)
                    }
                    other => expression_has_parameters(other),
                }
        }
        Expression::Between(between) => {
            expression_has_parameters(&between.expr)
                || expression_has_parameters(&between.lower)
                || expression_has_parameters(&between.upper)
        }
        Expression::Like(like) => {
            expression_has_parameters(&like.left) || expression_has_parameters(&like.pattern)
        }
        Expression::Case(case) => {
            case.value
                .as_ref()
                .is_some_and(|e| expression_has_parameters(e))
                || case.when_clauses.iter().any(|wc| {
                    expression_has_parameters(&wc.condition)
                        || expression_has_parameters(&wc.then_result)
                })
                || case
                    .else_value
                    .as_ref()
                    .is_some_and(|e| expression_has_parameters(e))
        }
        Expression::FunctionCall(func) => func.arguments.iter().any(expression_has_parameters),
        Expression::Aliased(aliased) => expression_has_parameters(&aliased.expression),
        Expression::Cast(cast) => expression_has_parameters(&cast.expr),
        _ => false,
    }
}

/// Check if two expressions are structurally equivalent.
/// Used for semantic matching and predicate comparison.
pub fn expressions_equivalent(a: &Expression, b: &Expression) -> bool {
    match (a, b) {
        (Expression::Identifier(ia), Expression::Identifier(ib)) => {
            ia.value_lower == ib.value_lower
        }
        (Expression::QualifiedIdentifier(qa), Expression::QualifiedIdentifier(qb)) => {
            qa.qualifier.value_lower == qb.qualifier.value_lower
                && qa.name.value_lower == qb.name.value_lower
        }
        (Expression::IntegerLiteral(la), Expression::IntegerLiteral(lb)) => la.value == lb.value,
        (Expression::FloatLiteral(la), Expression::FloatLiteral(lb)) => {
            Value::Float(la.value) == Value::Float(lb.value)
        }
        (Expression::StringLiteral(la), Expression::StringLiteral(lb)) => la.value == lb.value,
        (Expression::BooleanLiteral(la), Expression::BooleanLiteral(lb)) => la.value == lb.value,
        (Expression::NullLiteral(_), Expression::NullLiteral(_)) => true,
        (Expression::Infix(ia), Expression::Infix(ib)) => {
            ia.op_type == ib.op_type
                && expressions_equivalent(&ia.left, &ib.left)
                && expressions_equivalent(&ia.right, &ib.right)
        }
        (Expression::Prefix(pa), Expression::Prefix(pb)) => {
            pa.operator == pb.operator && expressions_equivalent(&pa.right, &pb.right)
        }
        (Expression::Between(ba), Expression::Between(bb)) => {
            ba.not == bb.not
                && expressions_equivalent(&ba.expr, &bb.expr)
                && expressions_equivalent(&ba.lower, &bb.lower)
                && expressions_equivalent(&ba.upper, &bb.upper)
        }
        (Expression::In(ia), Expression::In(ib)) => {
            ia.not == ib.not
                && expressions_equivalent(&ia.left, &ib.left)
                && expressions_equivalent(&ia.right, &ib.right)
        }
        (Expression::ExpressionList(la), Expression::ExpressionList(lb)) => {
            la.expressions.len() == lb.expressions.len()
                && la
                    .expressions
                    .iter()
                    .zip(lb.expressions.iter())
                    .all(|(ae, be)| expressions_equivalent(ae, be))
        }
        (Expression::Like(la), Expression::Like(lb)) => {
            la.operator == lb.operator
                && expressions_equivalent(&la.left, &lb.left)
                && expressions_equivalent(&la.pattern, &lb.pattern)
                && match (&la.escape, &lb.escape) {
                    (None, None) => true,
                    (Some(ea), Some(eb)) => expressions_equivalent(ea, eb),
                    _ => false,
                }
        }
        (Expression::FunctionCall(fa), Expression::FunctionCall(fb)) => {
            function_calls_equivalent(fa, fb)
        }
        (Expression::Window(wa), Expression::Window(wb)) => {
            function_calls_equivalent(&wa.function, &wb.function)
                && wa.window_ref == wb.window_ref
                && wa.partition_by.len() == wb.partition_by.len()
                && wa
                    .partition_by
                    .iter()
                    .zip(wb.partition_by.iter())
                    .all(|(ae, be)| expressions_equivalent(ae, be))
                && wa.order_by.len() == wb.order_by.len()
                && wa.order_by.iter().zip(wb.order_by.iter()).all(|(oa, ob)| {
                    oa.ascending == ob.ascending
                        && oa.nulls_first == ob.nulls_first
                        && expressions_equivalent(&oa.expression, &ob.expression)
                })
                && match (&wa.frame, &wb.frame) {
                    (None, None) => true,
                    (Some(fa), Some(fb)) => {
                        fa.unit == fb.unit
                            && window_bounds_equivalent(&fa.start, &fb.start)
                            && match (&fa.end, &fb.end) {
                                (None, None) => true,
                                (Some(ea), Some(eb)) => window_bounds_equivalent(ea, eb),
                                _ => false,
                            }
                    }
                    _ => false,
                }
        }
        _ => false,
    }
}

/// Compare two FunctionCall structs for structural equivalence.
fn function_calls_equivalent(fa: &FunctionCall, fb: &FunctionCall) -> bool {
    fa.function.eq_ignore_ascii_case(&fb.function)
        && fa.is_distinct == fb.is_distinct
        && fa.arguments.len() == fb.arguments.len()
        && fa
            .arguments
            .iter()
            .zip(fb.arguments.iter())
            .all(|(ae, be)| expressions_equivalent(ae, be))
        && fa.order_by.len() == fb.order_by.len()
        && fa.order_by.iter().zip(fb.order_by.iter()).all(|(oa, ob)| {
            oa.ascending == ob.ascending
                && oa.nulls_first == ob.nulls_first
                && expressions_equivalent(&oa.expression, &ob.expression)
        })
        && match (&fa.filter, &fb.filter) {
            (None, None) => true,
            (Some(ea), Some(eb)) => expressions_equivalent(ea, eb),
            _ => false,
        }
}

/// Compare two WindowFrameBound values for structural equivalence.
fn window_bounds_equivalent(a: &WindowFrameBound, b: &WindowFrameBound) -> bool {
    match (a, b) {
        (WindowFrameBound::CurrentRow, WindowFrameBound::CurrentRow)
        | (WindowFrameBound::UnboundedPreceding, WindowFrameBound::UnboundedPreceding)
        | (WindowFrameBound::UnboundedFollowing, WindowFrameBound::UnboundedFollowing) => true,
        (WindowFrameBound::Preceding(ea), WindowFrameBound::Preceding(eb))
        | (WindowFrameBound::Following(ea), WindowFrameBound::Following(eb)) => {
            expressions_equivalent(ea, eb)
        }
        _ => false,
    }
}

// ============================================================================
// Predicate Manipulation Utilities
// ============================================================================

/// Flatten AND predicates into a list of individual predicates.
/// E.g., `a AND b AND c` becomes `[a, b, c]`.
pub fn flatten_and_predicates(expr: &Expression) -> Vec<Expression> {
    match expr {
        Expression::Infix(infix) if infix.operator.to_uppercase() == "AND" => {
            let mut result = flatten_and_predicates(&infix.left);
            result.extend(flatten_and_predicates(&infix.right));
            result
        }
        _ => vec![expr.clone()],
    }
}

/// Combine predicates with AND operator.
/// Returns None if the input is empty.
pub fn combine_predicates_with_and(preds: Vec<Expression>) -> Option<Expression> {
    if preds.is_empty() {
        return None;
    }

    let mut result = preds.into_iter();
    let first = result.next().unwrap();

    Some(result.fold(first, |acc, pred| {
        Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Keyword, "AND", Position::default()),
            Box::new(acc),
            "AND".to_string(),
            Box::new(pred),
        ))
    }))
}

/// Extract all AND-ed conditions from an expression as references.
/// Similar to flatten_and_predicates but returns references.
pub fn extract_and_conditions(expr: &Expression) -> Vec<&Expression> {
    let mut conditions = Vec::new();

    fn collect<'a>(expr: &'a Expression, out: &mut Vec<&'a Expression>) {
        if let Expression::Infix(infix) = expr {
            if matches!(infix.op_type, InfixOperator::And) {
                collect(&infix.left, out);
                collect(&infix.right, out);
                return;
            }
        }
        out.push(expr);
    }

    collect(expr, &mut conditions);
    conditions
}

// ============================================================================
// Table Qualifier Utilities
// ============================================================================

/// Extract all table qualifiers (aliases) referenced in an expression.
/// Returns a set of lowercase table names/aliases.
pub fn collect_table_qualifiers(expr: &Expression) -> FxHashSet<String> {
    let mut qualifiers = FxHashSet::default();
    collect_table_qualifiers_impl(expr, &mut qualifiers);
    qualifiers
}

fn collect_table_qualifiers_impl(expr: &Expression, qualifiers: &mut FxHashSet<String>) {
    match expr {
        Expression::QualifiedIdentifier(qi) => {
            qualifiers.insert(qi.qualifier.value_lower.to_string());
        }
        Expression::Infix(infix) => {
            collect_table_qualifiers_impl(&infix.left, qualifiers);
            collect_table_qualifiers_impl(&infix.right, qualifiers);
        }
        Expression::Prefix(prefix) => {
            collect_table_qualifiers_impl(&prefix.right, qualifiers);
        }
        Expression::In(in_expr) => {
            collect_table_qualifiers_impl(&in_expr.left, qualifiers);
            match in_expr.right.as_ref() {
                Expression::ExpressionList(el) => {
                    for elem in &el.expressions {
                        collect_table_qualifiers_impl(elem, qualifiers);
                    }
                }
                Expression::List(list) => {
                    for elem in &list.elements {
                        collect_table_qualifiers_impl(elem, qualifiers);
                    }
                }
                other => {
                    collect_table_qualifiers_impl(other, qualifiers);
                }
            }
        }
        Expression::Between(between) => {
            collect_table_qualifiers_impl(&between.expr, qualifiers);
            collect_table_qualifiers_impl(&between.lower, qualifiers);
            collect_table_qualifiers_impl(&between.upper, qualifiers);
        }
        Expression::Like(like) => {
            collect_table_qualifiers_impl(&like.left, qualifiers);
            collect_table_qualifiers_impl(&like.pattern, qualifiers);
        }
        Expression::FunctionCall(func) => {
            for arg in &func.arguments {
                collect_table_qualifiers_impl(arg, qualifiers);
            }
        }
        Expression::Aliased(aliased) => {
            collect_table_qualifiers_impl(&aliased.expression, qualifiers);
        }
        Expression::Cast(cast) => {
            collect_table_qualifiers_impl(&cast.expr, qualifiers);
        }
        Expression::Case(case) => {
            if let Some(ref val) = case.value {
                collect_table_qualifiers_impl(val, qualifiers);
            }
            for when in &case.when_clauses {
                collect_table_qualifiers_impl(&when.condition, qualifiers);
                collect_table_qualifiers_impl(&when.then_result, qualifiers);
            }
            if let Some(ref else_val) = case.else_value {
                collect_table_qualifiers_impl(else_val, qualifiers);
            }
        }
        _ => {}
    }
}

/// Get table alias from a table expression.
/// Returns the alias if specified, otherwise the table name.
pub fn get_table_alias_from_expr(expr: &Expression) -> Option<String> {
    match expr {
        Expression::TableSource(ts) => Some(
            ts.alias
                .as_ref()
                .map(|a| a.value.to_string())
                .unwrap_or_else(|| ts.name.value.to_string()),
        ),
        Expression::SubquerySource(ss) => ss.alias.as_ref().map(|a| a.value.to_string()),
        Expression::FunctionTableSource(fs) => Some(
            fs.alias
                .as_ref()
                .map(|a| a.value.to_string())
                .unwrap_or_else(|| fs.function.value.to_string()),
        ),
        Expression::ValuesSource(vs) => vs.alias.as_ref().map(|a| a.value.to_string()),
        Expression::CteReference(cr) => Some(
            cr.alias
                .as_ref()
                .map(|a| a.value.to_string())
                .unwrap_or_else(|| cr.name.value.to_string()),
        ),
        _ => None,
    }
}

/// Strip table qualifier from an expression, replacing qualified identifiers
/// with unqualified ones. Used when pushing filters to individual table scans.
pub fn strip_table_qualifier(expr: &Expression, table_alias: &str) -> Expression {
    let alias_lower = table_alias.to_lowercase();

    match expr {
        Expression::QualifiedIdentifier(qi) if qi.qualifier.value_lower.as_str() == alias_lower => {
            // Convert to simple identifier
            Expression::Identifier(Identifier::new(
                qi.name.token.clone(),
                qi.name.value.clone(),
            ))
        }
        Expression::Infix(infix) => Expression::Infix(InfixExpression::new(
            infix.token.clone(),
            Box::new(strip_table_qualifier(&infix.left, table_alias)),
            infix.operator.clone(),
            Box::new(strip_table_qualifier(&infix.right, table_alias)),
        )),
        Expression::Prefix(prefix) => Expression::Prefix(PrefixExpression::new(
            prefix.token.clone(),
            prefix.operator.clone(),
            Box::new(strip_table_qualifier(&prefix.right, table_alias)),
        )),
        Expression::In(in_expr) => {
            let new_left = strip_table_qualifier(&in_expr.left, table_alias);
            let new_right = match in_expr.right.as_ref() {
                Expression::List(list) => Expression::List(Box::new(ListExpression {
                    token: list.token.clone(),
                    elements: list
                        .elements
                        .iter()
                        .map(|e| strip_table_qualifier(e, table_alias))
                        .collect(),
                })),
                other => strip_table_qualifier(other, table_alias),
            };
            Expression::In(InExpression {
                token: in_expr.token.clone(),
                left: Box::new(new_left),
                right: Box::new(new_right),
                not: in_expr.not,
            })
        }
        Expression::Between(between) => Expression::Between(BetweenExpression {
            token: between.token.clone(),
            expr: Box::new(strip_table_qualifier(&between.expr, table_alias)),
            lower: Box::new(strip_table_qualifier(&between.lower, table_alias)),
            upper: Box::new(strip_table_qualifier(&between.upper, table_alias)),
            not: between.not,
        }),
        Expression::Like(like) => Expression::Like(LikeExpression {
            token: like.token.clone(),
            left: Box::new(strip_table_qualifier(&like.left, table_alias)),
            pattern: Box::new(strip_table_qualifier(&like.pattern, table_alias)),
            operator: like.operator.clone(),
            escape: like
                .escape
                .as_ref()
                .map(|e| Box::new(strip_table_qualifier(e, table_alias))),
        }),
        Expression::FunctionCall(func) => Expression::FunctionCall(Box::new(FunctionCall {
            token: func.token.clone(),
            function: func.function.clone(),
            arguments: func
                .arguments
                .iter()
                .map(|a| strip_table_qualifier(a, table_alias))
                .collect(),
            is_distinct: func.is_distinct,
            order_by: func.order_by.clone(),
            filter: func
                .filter
                .as_ref()
                .map(|f| Box::new(strip_table_qualifier(f, table_alias))),
        })),
        // Return unchanged for other expression types
        other => other.clone(),
    }
}

/// Add table qualifier to an expression, converting simple identifiers
/// to qualified ones. Used when applying filters post-join that were
/// originally stripped for pushdown.
pub fn add_table_qualifier(expr: &Expression, table_alias: &str) -> Expression {
    match expr {
        Expression::Identifier(id) => {
            // Convert to qualified identifier
            Expression::QualifiedIdentifier(QualifiedIdentifier {
                token: Token::new(TokenType::Identifier, table_alias, Position::default()),
                qualifier: Box::new(Identifier::new(
                    Token::new(TokenType::Identifier, table_alias, Position::default()),
                    table_alias.to_string(),
                )),
                intermediate: None,
                name: Box::new(id.clone()),
            })
        }
        Expression::Infix(infix) => Expression::Infix(InfixExpression::new(
            infix.token.clone(),
            Box::new(add_table_qualifier(&infix.left, table_alias)),
            infix.operator.clone(),
            Box::new(add_table_qualifier(&infix.right, table_alias)),
        )),
        Expression::Prefix(prefix) => Expression::Prefix(PrefixExpression::new(
            prefix.token.clone(),
            prefix.operator.clone(),
            Box::new(add_table_qualifier(&prefix.right, table_alias)),
        )),
        Expression::In(in_expr) => {
            let new_left = add_table_qualifier(&in_expr.left, table_alias);
            let new_right = match in_expr.right.as_ref() {
                Expression::List(list) => Expression::List(Box::new(ListExpression {
                    token: list.token.clone(),
                    elements: list
                        .elements
                        .iter()
                        .map(|e| add_table_qualifier(e, table_alias))
                        .collect(),
                })),
                other => add_table_qualifier(other, table_alias),
            };
            Expression::In(InExpression {
                token: in_expr.token.clone(),
                left: Box::new(new_left),
                right: Box::new(new_right),
                not: in_expr.not,
            })
        }
        Expression::Between(between) => Expression::Between(BetweenExpression {
            token: between.token.clone(),
            expr: Box::new(add_table_qualifier(&between.expr, table_alias)),
            lower: Box::new(add_table_qualifier(&between.lower, table_alias)),
            upper: Box::new(add_table_qualifier(&between.upper, table_alias)),
            not: between.not,
        }),
        Expression::Like(like) => Expression::Like(LikeExpression {
            token: like.token.clone(),
            left: Box::new(add_table_qualifier(&like.left, table_alias)),
            pattern: Box::new(add_table_qualifier(&like.pattern, table_alias)),
            operator: like.operator.clone(),
            escape: like
                .escape
                .as_ref()
                .map(|e| Box::new(add_table_qualifier(e, table_alias))),
        }),
        Expression::FunctionCall(func) => Expression::FunctionCall(Box::new(FunctionCall {
            token: func.token.clone(),
            function: func.function.clone(),
            arguments: func
                .arguments
                .iter()
                .map(|a| add_table_qualifier(a, table_alias))
                .collect(),
            is_distinct: func.is_distinct,
            order_by: func.order_by.clone(),
            filter: func
                .filter
                .as_ref()
                .map(|f| Box::new(add_table_qualifier(f, table_alias))),
        })),
        // Return unchanged for other expression types (literals, qualified identifiers, etc.)
        other => other.clone(),
    }
}

// ============================================================================
// Aggregate Function Utilities
// ============================================================================

/// Check if a function name is an aggregate function.
/// Uses the function registry to determine this.
#[inline]
pub fn is_aggregate_function(name: &str) -> bool {
    radixdb_functions::registry::global_registry().is_aggregate(name)
}

/// Check if an expression contains an aggregate function.
/// Used for detecting nested aggregates and determining query structure.
pub fn expression_contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::FunctionCall(func) => {
            if is_aggregate_function(&func.function) {
                return true;
            }
            // Check arguments recursively
            func.arguments.iter().any(expression_contains_aggregate)
        }
        Expression::Aliased(aliased) => expression_contains_aggregate(&aliased.expression),
        Expression::Infix(infix) => {
            expression_contains_aggregate(&infix.left)
                || expression_contains_aggregate(&infix.right)
        }
        Expression::Prefix(prefix) => expression_contains_aggregate(&prefix.right),
        Expression::Cast(cast) => expression_contains_aggregate(&cast.expr),
        Expression::Case(case) => {
            for when_clause in &case.when_clauses {
                if expression_contains_aggregate(&when_clause.condition)
                    || expression_contains_aggregate(&when_clause.then_result)
                {
                    return true;
                }
            }
            if let Some(ref else_val) = case.else_value {
                if expression_contains_aggregate(else_val) {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

// ============================================================================
// Column Index Utilities
// ============================================================================

/// Extract column name from identifier expression with optional qualifier.
/// Returns (qualifier, column_name) where qualifier is Some for qualified identifiers.
/// Column names are returned in lowercase for case-insensitive matching.
#[inline]
pub fn extract_column_name_with_qualifier(expr: &Expression) -> Option<(Option<String>, String)> {
    match expr {
        Expression::Identifier(id) => Some((None, id.value_lower.to_string())),
        Expression::QualifiedIdentifier(qid) => Some((
            Some(qid.qualifier.value_lower.to_string()),
            qid.name.value_lower.to_string(),
        )),
        _ => None,
    }
}

/// Find column index in column list, handling qualified names.
/// Supports exact match and qualified match (table.column).
///
/// IMPORTANT: When a qualifier is provided (e.g., "t2" for "t2.id"),
/// we ONLY match columns that have that exact qualifier. This prevents
/// incorrectly matching "t1.id" when looking for "t2.id".
pub fn find_column_index(col_info: &(Option<String>, String), columns: &[String]) -> Option<usize> {
    let (qualifier, col_name) = col_info;

    // Pre-compute qualified name if qualifier exists (avoid format! in loop)
    let qualified = qualifier.as_ref().map(|q| format!("{}.{}", q, col_name));

    // First pass: try exact or qualified match
    for (idx, column) in columns.iter().enumerate() {
        let col_lower = column.to_lowercase();

        // Try exact match (unqualified column name)
        if col_lower == *col_name {
            return Some(idx);
        }

        // Try qualified match (table.column)
        if let Some(ref q) = qualified {
            if col_lower == *q {
                return Some(idx);
            }
        }
    }

    // Second pass: ONLY if no qualifier was provided, try suffix match
    // This allows matching "id" against "t1.id" when the column ref is just "id"
    if qualifier.is_none() {
        // Pre-compute suffix pattern once (avoid format! in loop)
        let suffix_pattern = format!(".{}", col_name);
        for (idx, column) in columns.iter().enumerate() {
            let col_lower = column.to_lowercase();
            if col_lower.ends_with(&suffix_pattern) {
                return Some(idx);
            }
        }
    }

    None
}

// ============================================================================
// Type Conversion Utilities
// ============================================================================

/// Parse vector dimension from a type string like "VECTOR(768)".
/// Returns 0 if no dimension is specified.
pub fn parse_vector_dimension(type_str: &str) -> u16 {
    let upper = type_str.to_uppercase();
    if let Some(inner) = upper
        .strip_prefix("VECTOR(")
        .and_then(|s| s.strip_suffix(')'))
    {
        inner.trim().parse::<u16>().unwrap_or(0)
    } else {
        0
    }
}

// ============================================================================
// Expression Display Utilities
// ============================================================================

// ============================================================================
// Join Key Extraction
// ============================================================================

/// Extract equality join keys and residual conditions from a join condition.
///
/// Returns (left_indices, right_indices, residual_conditions) where residual
/// contains non-equality conditions that must be applied after the hash join.
pub fn extract_join_keys_and_residual(
    condition: &Expression,
    left_columns: &[String],
    right_columns: &[String],
) -> (Vec<usize>, Vec<usize>, Vec<Expression>) {
    let mut left_indices = Vec::new();
    let mut right_indices = Vec::new();
    let mut residual = Vec::new();

    extract_join_keys_recursive(
        condition,
        left_columns,
        right_columns,
        &mut left_indices,
        &mut right_indices,
        &mut residual,
    );

    (left_indices, right_indices, residual)
}

/// Recursively extract equality join keys from AND expressions.
fn extract_join_keys_recursive(
    condition: &Expression,
    left_columns: &[String],
    right_columns: &[String],
    left_indices: &mut Vec<usize>,
    right_indices: &mut Vec<usize>,
    residual: &mut Vec<Expression>,
) {
    match condition {
        Expression::Infix(infix) if infix.op_type == InfixOperator::And => {
            // Recurse into AND branches
            extract_join_keys_recursive(
                &infix.left,
                left_columns,
                right_columns,
                left_indices,
                right_indices,
                residual,
            );
            extract_join_keys_recursive(
                &infix.right,
                left_columns,
                right_columns,
                left_indices,
                right_indices,
                residual,
            );
        }
        Expression::Infix(infix) if infix.op_type == InfixOperator::Equal => {
            // Extract equality condition
            if let (Some(left_col), Some(right_col)) = (
                extract_column_name_with_qualifier(&infix.left),
                extract_column_name_with_qualifier(&infix.right),
            ) {
                // Case 1: left.col = right.col
                if let (Some(left_idx), Some(right_idx)) = (
                    find_column_index(&left_col, left_columns),
                    find_column_index(&right_col, right_columns),
                ) {
                    left_indices.push(left_idx);
                    right_indices.push(right_idx);
                    return;
                }

                // Case 2: right.col = left.col (swapped)
                if let (Some(left_idx), Some(right_idx)) = (
                    find_column_index(&right_col, left_columns),
                    find_column_index(&left_col, right_columns),
                ) {
                    left_indices.push(left_idx);
                    right_indices.push(right_idx);
                    return;
                }
            }
            // Non-join equality (e.g., a.x = 5) - add to residual
            residual.push(condition.clone());
        }
        _ => {
            // Non-equality condition - add to residual filters
            residual.push(condition.clone());
        }
    }
}

// ============================================================================
// Join Key Equivalence - Column Substitution
// ============================================================================

/// Recursively check if an expression contains a reference to a specific column.
/// This handles nested expressions including function calls, AND/OR, prefix, etc.
fn expression_contains_column(expr: &Expression, target_lower: &str) -> bool {
    match expr {
        // Direct column reference
        Expression::Identifier(ident) => {
            ident.value_lower.as_str() == target_lower
                || extract_base_column_name(&ident.value) == target_lower
        }
        Expression::QualifiedIdentifier(qi) => {
            qi.name.value_lower.as_str() == target_lower
                || extract_base_column_name(&qi.name.value) == target_lower
        }

        // Function calls - check all arguments (e.g., LOWER(col), COALESCE(col, 0))
        Expression::FunctionCall(fc) => fc
            .arguments
            .iter()
            .any(|arg| expression_contains_column(arg, target_lower)),

        // Infix expressions - check both sides (e.g., col + 1, col = value)
        Expression::Infix(infix) => {
            expression_contains_column(&infix.left, target_lower)
                || expression_contains_column(&infix.right, target_lower)
        }

        // Prefix expressions - check inner (e.g., NOT col, -col)
        Expression::Prefix(prefix) => expression_contains_column(&prefix.right, target_lower),

        // IN expression - check the left side
        Expression::In(in_expr) => expression_contains_column(&in_expr.left, target_lower),

        // BETWEEN expression - check the main expression
        Expression::Between(between) => expression_contains_column(&between.expr, target_lower),

        // LIKE expression - check the left side
        Expression::Like(like) => expression_contains_column(&like.left, target_lower),

        // CASE expression - check condition and all branches
        Expression::Case(case) => {
            let in_value = case
                .value
                .as_ref()
                .map(|e| expression_contains_column(e, target_lower))
                .unwrap_or(false);
            let in_branches = case.when_clauses.iter().any(|clause| {
                expression_contains_column(&clause.condition, target_lower)
                    || expression_contains_column(&clause.then_result, target_lower)
            });
            let in_else = case
                .else_value
                .as_ref()
                .map(|e| expression_contains_column(e, target_lower))
                .unwrap_or(false);
            in_value || in_branches || in_else
        }

        // Cast expression - check inner expression
        Expression::Cast(cast) => expression_contains_column(&cast.expr, target_lower),

        // Subqueries - don't recurse into subqueries for this optimization
        Expression::ScalarSubquery(_) | Expression::SubquerySource(_) => false,

        // Literals and other terminals - no column reference
        _ => false,
    }
}

/// Check if a filter expression references a specific column (the join key).
/// Returns true if the filter's main column matches the target column name.
/// Handles IN, comparison, BETWEEN, LIKE, function calls, and nested expressions.
///
/// This is used for join key equivalence optimization: when a filter on the
/// inner table's join key can be pushed to the outer table.
pub fn filter_references_column(expr: &Expression, target_col: &str) -> bool {
    let target_lower = target_col.to_lowercase();

    match expr {
        Expression::In(in_expr) => {
            // Check if IN expression references the target column (direct or nested)
            expression_contains_column(&in_expr.left, &target_lower)
        }
        Expression::Infix(infix) => {
            // Handle AND/OR by checking both sides recursively
            if infix.operator == "AND" || infix.operator == "OR" {
                return filter_references_column(&infix.left, target_col)
                    || filter_references_column(&infix.right, target_col);
            }

            // Check comparison expressions: col = value, LOWER(col) = 'x', etc.
            expression_contains_column(&infix.left, &target_lower)
                || expression_contains_column(&infix.right, &target_lower)
        }
        Expression::Between(between) => {
            // Check if BETWEEN expression references the target column
            expression_contains_column(&between.expr, &target_lower)
        }
        Expression::Like(like) => {
            // Check if LIKE expression references the target column
            expression_contains_column(&like.left, &target_lower)
        }
        Expression::Prefix(prefix) => {
            // Handle NOT expression by checking inner (e.g., NOT col IS NULL)
            filter_references_column(&prefix.right, target_col)
        }
        Expression::FunctionCall(fc) => {
            // Function call at top level (rare, but handle it)
            fc.arguments
                .iter()
                .any(|arg| expression_contains_column(arg, &target_lower))
        }
        _ => false,
    }
}

/// Substitute a column reference in a filter expression with a new column name.
/// This is used for join key equivalence: when filter `o.user_id IN (1,2,3)`
/// can be transformed to `u.id IN (1,2,3)` based on join condition `u.id = o.user_id`.
///
/// Only handles simple cases where the column is directly referenced.
/// Returns None if substitution is not possible.
pub fn substitute_filter_column(
    expr: &Expression,
    from_col: &str,
    to_col: &str,
) -> Option<Expression> {
    let from_lower = from_col.to_lowercase();
    let from_base = extract_base_column_name(from_col);

    match expr {
        Expression::In(in_expr) => {
            // Substitute column in IN expression
            if let Some(col_name) = extract_column_name(&in_expr.left) {
                let col_lower = col_name.to_lowercase();
                let col_base = extract_base_column_name(&col_name);

                if col_lower == from_lower || col_base == from_base {
                    // Create new identifier with the target column name
                    let new_left = create_column_identifier(to_col);
                    return Some(Expression::In(InExpression {
                        token: in_expr.token.clone(),
                        left: Box::new(new_left),
                        right: in_expr.right.clone(),
                        not: in_expr.not,
                    }));
                }
            }
        }
        Expression::Infix(infix) => {
            // Substitute column in comparison expression
            let left_col = extract_column_name(&infix.left);
            let right_col = extract_column_name(&infix.right);

            // Check if left side is the target column
            if let Some(col_name) = &left_col {
                let col_lower = col_name.to_lowercase();
                let col_base = extract_base_column_name(col_name);

                if col_lower == from_lower || col_base == from_base {
                    let new_left = create_column_identifier(to_col);
                    return Some(Expression::Infix(InfixExpression::new(
                        infix.token.clone(),
                        Box::new(new_left),
                        infix.operator.clone(),
                        infix.right.clone(),
                    )));
                }
            }

            // Check if right side is the target column (for value = col cases)
            if let Some(col_name) = &right_col {
                let col_lower = col_name.to_lowercase();
                let col_base = extract_base_column_name(col_name);

                if col_lower == from_lower || col_base == from_base {
                    let new_right = create_column_identifier(to_col);
                    return Some(Expression::Infix(InfixExpression::new(
                        infix.token.clone(),
                        infix.left.clone(),
                        infix.operator.clone(),
                        Box::new(new_right),
                    )));
                }
            }
        }
        Expression::Between(between) => {
            if let Some(col_name) = extract_column_name(&between.expr) {
                let col_lower = col_name.to_lowercase();
                let col_base = extract_base_column_name(&col_name);

                if col_lower == from_lower || col_base == from_base {
                    let new_expr = create_column_identifier(to_col);
                    return Some(Expression::Between(BetweenExpression {
                        token: between.token.clone(),
                        expr: Box::new(new_expr),
                        lower: between.lower.clone(),
                        upper: between.upper.clone(),
                        not: between.not,
                    }));
                }
            }
        }
        Expression::Like(like) => {
            if let Some(col_name) = extract_column_name(&like.left) {
                let col_lower = col_name.to_lowercase();
                let col_base = extract_base_column_name(&col_name);

                if col_lower == from_lower || col_base == from_base {
                    let new_left = create_column_identifier(to_col);
                    return Some(Expression::Like(LikeExpression {
                        token: like.token.clone(),
                        left: Box::new(new_left),
                        pattern: like.pattern.clone(),
                        operator: like.operator.clone(),
                        escape: like.escape.clone(),
                    }));
                }
            }
        }
        _ => {}
    }
    None
}

/// Create a column identifier expression from a column name.
/// Handles qualified names (table.column) and unqualified names (column).
fn create_column_identifier(col_name: &str) -> Expression {
    if let Some(dot_idx) = col_name.find('.') {
        let qualifier = &col_name[..dot_idx];
        let name = &col_name[dot_idx + 1..];
        Expression::QualifiedIdentifier(QualifiedIdentifier {
            token: dummy_token(col_name, TokenType::Identifier),
            qualifier: Box::new(Identifier::new(
                dummy_token(qualifier, TokenType::Identifier),
                qualifier.to_string(),
            )),
            intermediate: None,
            name: Box::new(Identifier::new(
                dummy_token(name, TokenType::Identifier),
                name.to_string(),
            )),
        })
    } else {
        Expression::Identifier(Identifier::new(
            dummy_token(col_name, TokenType::Identifier),
            col_name.to_string(),
        ))
    }
}

// ============================================================================
// Join Projection Utilities
// ============================================================================

/// Result of computing join projection indices.
/// Contains the column sources in SELECT order to satisfy the SELECT expressions.
#[derive(Clone)]
pub struct JoinProjectionIndices {
    /// Column sources in SELECT order (preserves original column ordering)
    pub columns: Vec<ColumnSource>,
    /// Output column names for the projected result
    pub output_columns: Vec<String>,
}

fn join_projection_source(combined_index: usize, outer_width: usize) -> ColumnSource {
    if combined_index < outer_width {
        ColumnSource::Outer(combined_index)
    } else {
        ColumnSource::Inner(combined_index - outer_width)
    }
}

type JoinProjectionLookupBucket = Vec<(Vec<String>, Arc<StringMap<usize>>)>;

thread_local! {
    static JOIN_PROJECTION_LOOKUP_CACHE: RefCell<LruCache<u64, JoinProjectionLookupBucket>> =
        RefCell::new(LruCache::new(NonZeroUsize::new(512).unwrap()));
}

pub fn clear_join_projection_lookup_cache() {
    JOIN_PROJECTION_LOOKUP_CACHE.with(|cache| cache.borrow_mut().clear());
}

fn build_join_projection_lookup(
    outer_columns: &[String],
    inner_columns: &[String],
) -> Arc<StringMap<usize>> {
    let mut hasher = FxHasher::default();
    outer_columns.len().hash(&mut hasher);
    inner_columns.len().hash(&mut hasher);
    for column in outer_columns.iter().chain(inner_columns) {
        column.hash(&mut hasher);
    }
    let key = hasher.finish();

    JOIN_PROJECTION_LOOKUP_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(bucket) = cache.get(&key) {
            if let Some((_, lookup)) = bucket.iter().find(|(columns, _)| {
                columns.len() == outer_columns.len() + inner_columns.len()
                    && columns
                        .iter()
                        .zip(outer_columns.iter().chain(inner_columns))
                        .all(|(cached, actual)| cached == actual)
            }) {
                return Arc::clone(lookup);
            }
        }

        let columns = outer_columns
            .iter()
            .chain(inner_columns)
            .cloned()
            .collect::<Vec<_>>();
        let lookup = Arc::new(build_column_index_map(&columns));
        if let Some(bucket) = cache.get_mut(&key) {
            bucket.push((columns, Arc::clone(&lookup)));
        } else {
            cache.put(key, vec![(columns, Arc::clone(&lookup))]);
        }
        lookup
    })
}

/// Compute projection indices for a join operator.
///
/// Analyzes SELECT expressions and determines which columns from the outer and inner
/// sides are needed. Returns columns in SELECT order (not outer-first/inner-second).
///
/// # Arguments
/// * `select_exprs` - The SELECT expressions to analyze
/// * `outer_columns` - Column names from the outer (left) side of the join
/// * `inner_columns` - Column names from the inner (right) side of the join
///
/// # Returns
/// Some(JoinProjectionIndices) if all expressions are simple column references,
/// None otherwise.
pub fn compute_join_projection(
    select_exprs: &[Expression],
    outer_columns: &[String],
    inner_columns: &[String],
) -> Option<JoinProjectionIndices> {
    let outer_width = outer_columns.len();
    let lookup = build_join_projection_lookup(outer_columns, inner_columns);
    let mut columns = Vec::new();
    let mut output_columns = Vec::new();

    for expr in select_exprs {
        match expr {
            // SELECT * - cannot push down projection
            Expression::Star(_) | Expression::QualifiedStar(_) => return None,

            Expression::Identifier(id) => {
                let col_lower = id.value_lower.as_str();
                let index = lookup.get(col_lower).copied()?;
                columns.push(join_projection_source(index, outer_width));
                output_columns.push(id.value.to_string());
            }

            Expression::QualifiedIdentifier(qid) => {
                let full_name = format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower);
                let index = lookup
                    .get(&full_name)
                    .copied()
                    .or_else(|| lookup.get(qid.name.value_lower.as_str()).copied())?;
                columns.push(join_projection_source(index, outer_width));
                output_columns.push(qid.name.value.to_string());
            }

            Expression::Aliased(aliased) => {
                // Handle aliased expressions - check if inner is a simple column
                let alias_name = aliased.alias.value.to_string();
                match &*aliased.expression {
                    Expression::Identifier(id) => {
                        let col_lower = id.value_lower.as_str();
                        let index = lookup.get(col_lower).copied()?;
                        columns.push(join_projection_source(index, outer_width));
                        output_columns.push(alias_name);
                    }
                    Expression::QualifiedIdentifier(qid) => {
                        let full_name =
                            format!("{}.{}", qid.qualifier.value_lower, qid.name.value_lower);
                        let index = lookup
                            .get(&full_name)
                            .copied()
                            .or_else(|| lookup.get(qid.name.value_lower.as_str()).copied())?;
                        columns.push(join_projection_source(index, outer_width));
                        output_columns.push(alias_name);
                    }
                    _ => return None, // Complex expression - cannot push down
                }
            }

            // Any other expression type cannot be pushed down
            _ => return None,
        }
    }

    Some(JoinProjectionIndices {
        columns,
        output_columns,
    })
}

include!("utils/tests.rs");
