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

//! Partial-index metadata and wrapper.
//!
//! A partial index stores entries only for rows matching a deterministic
//! row-local predicate from `CREATE INDEX ... WHERE ...`. The wrapper does not
//! try to infer predicate membership from `Index::add(values, ...)` because that
//! method sees only index-key values, not the complete row. Write paths must use
//! the predicate-aware helpers added around storage/index maintenance.

use std::fmt;
use std::sync::Arc;

use crate::expression::Expression as StorageExpression;
use crate::traits::Index;
use radixdb_core::I64Map;
use radixdb_core::{
    DataType, Error, IndexEntry, IndexType, Operator, Result, Row, RowIdVec, Schema, Value,
};

/// Serializable public metadata for a partial-index predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialIndexPredicateMetadata {
    canonical_sql: String,
}

impl PartialIndexPredicateMetadata {
    pub fn new(canonical_sql: impl Into<String>) -> Self {
        Self {
            canonical_sql: canonical_sql.into(),
        }
    }

    pub fn canonical_sql(&self) -> &str {
        &self.canonical_sql
    }
}

/// Prepared runtime predicate for a partial index.
#[derive(Clone)]
pub struct PartialIndexPredicate {
    canonical_sql: String,
    referenced_column_names: Vec<String>,
    referenced_column_ids: Vec<i32>,
    expression: Arc<dyn StorageExpression>,
}

impl PartialEq for PartialIndexPredicate {
    fn eq(&self, other: &Self) -> bool {
        self.canonical_sql == other.canonical_sql
            && self.referenced_column_names == other.referenced_column_names
            && self.referenced_column_ids == other.referenced_column_ids
    }
}

impl Eq for PartialIndexPredicate {}

/// Composition port used to rebuild a persisted partial-index predicate.
///
/// Storage owns the prepared result but never parses SQL. The facade supplies
/// the executor-owned binder before opening a persistent engine.
pub type PartialIndexPredicateBinder = fn(&str, &Schema) -> Result<PartialIndexPredicate>;

impl PartialIndexPredicate {
    pub fn new(
        canonical_sql: impl Into<String>,
        referenced_column_names: Vec<String>,
        mut expression: Box<dyn StorageExpression>,
        schema: &Schema,
    ) -> Result<Self> {
        let canonical_sql = canonical_sql.into();
        let mut referenced_column_ids = Vec::with_capacity(referenced_column_names.len());
        let column_map = schema.column_index_map();

        for column_name in &referenced_column_names {
            let key = column_name.to_lowercase();
            let Some(&idx) = column_map.get(key.as_str()) else {
                return Err(Error::ColumnNotFound(column_name.clone()));
            };
            referenced_column_ids.push(idx as i32);
        }

        expression.prepare_for_schema(schema);

        if !referenced_column_names.is_empty() {
            let mut prepared_indices = Vec::new();
            if !expression.collect_column_indices(&mut prepared_indices) {
                return Err(Error::invalid_argument(format!(
                    "partial index predicate references columns that cannot be resolved against table '{}': {}",
                    schema.table_name, canonical_sql
                )));
            }
        }

        Ok(Self {
            canonical_sql,
            referenced_column_names,
            referenced_column_ids,
            expression: Arc::from(expression),
        })
    }

    pub fn canonical_sql(&self) -> &str {
        &self.canonical_sql
    }

    pub fn referenced_column_names(&self) -> &[String] {
        &self.referenced_column_names
    }

    pub fn referenced_column_ids(&self) -> &[i32] {
        &self.referenced_column_ids
    }

    pub fn metadata(&self) -> PartialIndexPredicateMetadata {
        PartialIndexPredicateMetadata::new(self.canonical_sql.clone())
    }

    pub fn matches(&self, row: &Row) -> Result<bool> {
        self.expression.evaluate(row)
    }

    pub fn matches_fast(&self, row: &Row) -> bool {
        self.expression.evaluate_fast(row)
    }

    /// Returns true when a query predicate is proven to imply this partial
    /// index predicate.
    ///
    /// This is intentionally conservative. It recognizes exact row-local
    /// conjuncts (`col IS NULL`, `col = value`, etc.) and `AND` decomposition.
    /// Unknown shapes return false so the planner falls back to a full
    /// index/scan path rather than risking incomplete results.
    pub fn is_implied_by(&self, query: &dyn StorageExpression) -> bool {
        expression_implies(query, self.expression.as_ref())
    }
}

fn expression_implies(query: &dyn StorageExpression, predicate: &dyn StorageExpression) -> bool {
    if let Some(predicate_conjuncts) = predicate.get_and_operands() {
        return predicate_conjuncts
            .iter()
            .all(|part| expression_implies(query, part.as_ref()));
    }

    if expressions_equivalent(query, predicate) {
        return true;
    }

    if let Some(query_conjuncts) = query.get_and_operands() {
        return query_conjuncts
            .iter()
            .any(|part| expression_implies(part.as_ref(), predicate));
    }

    false
}

fn expressions_equivalent(left: &dyn StorageExpression, right: &dyn StorageExpression) -> bool {
    if let (Some((left_col, left_is_null)), Some((right_col, right_is_null))) =
        (left.get_null_check_info(), right.get_null_check_info())
    {
        return left_col.eq_ignore_ascii_case(right_col) && left_is_null == right_is_null;
    }

    if let (Some((left_col, left_op, left_value)), Some((right_col, right_op, right_value))) =
        (left.get_comparison_info(), right.get_comparison_info())
    {
        return left_col.eq_ignore_ascii_case(right_col)
            && left_op == right_op
            && left_value == right_value;
    }

    false
}

impl fmt::Debug for PartialIndexPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartialIndexPredicate")
            .field("canonical_sql", &self.canonical_sql)
            .field("referenced_column_names", &self.referenced_column_names)
            .field("referenced_column_ids", &self.referenced_column_ids)
            .finish_non_exhaustive()
    }
}

/// Metadata-carrying wrapper around a concrete index implementation.
pub struct PartialIndex {
    inner: Arc<dyn Index>,
    predicate: PartialIndexPredicate,
}

impl PartialIndex {
    pub fn new(inner: Arc<dyn Index>, predicate: PartialIndexPredicate) -> Self {
        Self { inner, predicate }
    }

    pub fn inner(&self) -> &Arc<dyn Index> {
        &self.inner
    }
}

impl Index for PartialIndex {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn table_name(&self) -> &str {
        self.inner.table_name()
    }

    fn build(&mut self) -> Result<()> {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.build()
        } else {
            Err(Error::NotSupported(
                "cannot exclusively build a shared partial index".to_string(),
            ))
        }
    }

    fn add(&self, values: &[Value], row_id: i64, ref_id: i64) -> Result<()> {
        self.inner.add(values, row_id, ref_id)
    }

    fn add_batch(&self, entries: &I64Map<Vec<Value>>) -> Result<()> {
        self.inner.add_batch(entries)
    }

    fn remove(&self, values: &[Value], row_id: i64, ref_id: i64) -> Result<()> {
        self.inner.remove(values, row_id, ref_id)
    }

    fn remove_batch(&self, entries: &I64Map<Vec<Value>>) -> Result<()> {
        self.inner.remove_batch(entries)
    }

    fn add_batch_slice(&self, entries: &[(i64, &[Value])]) -> Result<()> {
        self.inner.add_batch_slice(entries)
    }

    fn remove_batch_slice(&self, entries: &[(i64, &[Value])]) -> Result<()> {
        self.inner.remove_batch_slice(entries)
    }

    fn column_ids(&self) -> &[i32] {
        self.inner.column_ids()
    }

    fn column_names(&self) -> &[String] {
        self.inner.column_names()
    }

    fn data_types(&self) -> &[DataType] {
        self.inner.data_types()
    }

    fn index_type(&self) -> IndexType {
        self.inner.index_type()
    }

    fn is_unique(&self) -> bool {
        self.inner.is_unique()
    }

    fn partial_predicate(&self) -> Option<&PartialIndexPredicate> {
        Some(&self.predicate)
    }

    fn prepared_key_encoder(&self) -> Option<crate::index::PreparedIndexKeyEncoder> {
        self.inner.prepared_key_encoder()
    }

    fn all_entries(&self) -> Result<Vec<IndexEntry>> {
        self.inner.all_entries()
    }

    fn find(&self, values: &[Value]) -> Result<Vec<IndexEntry>> {
        self.inner.find(values)
    }

    fn find_range(
        &self,
        min: &[Value],
        max: &[Value],
        min_inclusive: bool,
        max_inclusive: bool,
    ) -> Result<Vec<IndexEntry>> {
        self.inner
            .find_range(min, max, min_inclusive, max_inclusive)
    }

    fn find_physical_range(
        &self,
        min: &[Value],
        max: &[Value],
        min_inclusive: bool,
        max_inclusive: bool,
    ) -> Result<Vec<IndexEntry>> {
        self.inner
            .find_physical_range(min, max, min_inclusive, max_inclusive)
    }

    fn find_with_operator(&self, op: Operator, values: &[Value]) -> Result<Vec<IndexEntry>> {
        self.inner.find_with_operator(op, values)
    }

    fn get_row_ids_equal_into(&self, values: &[Value], buffer: &mut Vec<i64>) -> Result<()> {
        self.inner.get_row_ids_equal_into(values, buffer)
    }

    fn get_row_ids_in_range_into(
        &self,
        min_value: &[Value],
        max_value: &[Value],
        include_min: bool,
        include_max: bool,
        buffer: &mut Vec<i64>,
    ) -> Result<()> {
        self.inner
            .get_row_ids_in_range_into(min_value, max_value, include_min, include_max, buffer)
    }

    fn get_row_ids_in_into(&self, value_list: &[Value], buffer: &mut Vec<i64>) -> Result<()> {
        self.inner.get_row_ids_in_into(value_list, buffer)
    }

    fn get_filtered_row_ids(&self, expr: &dyn StorageExpression) -> Result<RowIdVec> {
        self.inner.get_filtered_row_ids(expr)
    }

    fn get_min_value(&self) -> Option<Value> {
        self.inner.get_min_value()
    }

    fn get_max_value(&self) -> Option<Value> {
        self.inner.get_max_value()
    }

    fn get_all_values(&self) -> Vec<Value> {
        self.inner.get_all_values()
    }

    fn get_distinct_count_excluding_null(&self) -> Option<usize> {
        self.inner.get_distinct_count_excluding_null()
    }

    fn get_row_ids_ordered(
        &self,
        ascending: bool,
        limit: usize,
        offset: usize,
    ) -> Option<Vec<i64>> {
        self.inner.get_row_ids_ordered(ascending, limit, offset)
    }

    fn get_grouped_row_ids(&self) -> Option<Vec<(Value, Vec<i64>)>> {
        self.inner.get_grouped_row_ids()
    }

    fn for_each_group(
        &self,
        callback: &mut dyn FnMut(&Value, &[i64]) -> Result<bool>,
    ) -> Option<Result<()>> {
        self.inner.for_each_group(callback)
    }

    fn search_nearest(&self, query: &Value, k: usize, ef_search: usize) -> Option<Vec<(i64, f64)>> {
        self.inner.search_nearest(query, k, ef_search)
    }

    fn hnsw_distance_metric(&self) -> Option<u8> {
        self.inner.hnsw_distance_metric()
    }

    fn hnsw_m(&self) -> Option<u16> {
        self.inner.hnsw_m()
    }

    fn hnsw_ef_construction(&self) -> Option<u16> {
        self.inner.hnsw_ef_construction()
    }

    fn default_ef_search(&self) -> Option<usize> {
        self.inner.default_ef_search()
    }

    fn hnsw_graph_bytes(&self) -> Result<Option<Vec<u8>>> {
        self.inner.hnsw_graph_bytes()
    }

    fn hnsw_indexed_row_ids(&self) -> Option<Vec<i64>> {
        self.inner.hnsw_indexed_row_ids()
    }

    fn clear(&self) {
        self.inner.clear()
    }

    fn cleanup(&self) -> Result<()> {
        self.inner.cleanup()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.as_any()
    }

    fn close(&mut self) -> Result<()> {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.close()
        } else {
            Err(Error::NotSupported(
                "cannot exclusively close a shared partial index".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::NullCheckExpr;
    use crate::index::HashIndex;
    use radixdb_core::SchemaBuilder;

    fn schema() -> Schema {
        SchemaBuilder::new("users")
            .add_primary_key("id", DataType::Integer)
            .add("email", DataType::Text)
            .add_nullable("__raf_deleted_at", DataType::Timestamp)
            .build()
    }

    #[test]
    fn partial_index_predicate_matches_rows() {
        let schema = schema();
        let predicate = PartialIndexPredicate::new(
            "(__raf_deleted_at IS NULL)",
            vec!["__raf_deleted_at".to_string()],
            Box::new(NullCheckExpr::is_null("__raf_deleted_at")),
            &schema,
        )
        .expect("predicate");

        let active = Row::from_values(vec![
            Value::integer(1),
            Value::text("owner@example.test"),
            Value::Null(DataType::Timestamp),
        ]);
        let deleted = Row::from_values(vec![
            Value::integer(2),
            Value::text("owner@example.test"),
            Value::text("2026-08-07T00:00:00Z"),
        ]);

        assert!(predicate.matches(&active).expect("active eval"));
        assert!(!predicate.matches(&deleted).expect("deleted eval"));
        assert_eq!(predicate.canonical_sql(), "(__raf_deleted_at IS NULL)");
        assert_eq!(
            predicate.referenced_column_names(),
            &["__raf_deleted_at".to_string()]
        );
        assert_eq!(predicate.referenced_column_ids(), &[2]);
    }

    #[test]
    fn partial_index_predicate_rejects_unknown_columns() {
        let schema = schema();
        let err = PartialIndexPredicate::new(
            "(missing IS NULL)",
            vec!["missing".to_string()],
            Box::new(NullCheckExpr::is_null("missing")),
            &schema,
        )
        .expect_err("unknown column must be rejected");

        assert!(
            err.to_string().contains("missing"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn v2_r3_shared_partial_lifecycle_cannot_report_false_success() {
        let schema = schema();
        let predicate = PartialIndexPredicate::new(
            "(__raf_deleted_at IS NULL)",
            vec!["__raf_deleted_at".to_string()],
            Box::new(NullCheckExpr::is_null("__raf_deleted_at")),
            &schema,
        )
        .unwrap();
        let inner: Arc<dyn Index> = Arc::new(HashIndex::new(
            "idx_email".into(),
            "users".into(),
            vec!["email".into()],
            vec![1],
            vec![DataType::Text],
            false,
            0,
        ));
        let observer = Arc::clone(&inner);
        let mut partial = PartialIndex::new(inner, predicate);

        assert!(partial.build().is_err());
        assert!(partial.close().is_err());
        observer
            .add(&[Value::text("a@example.test")], 1, 1)
            .unwrap();
        assert_eq!(
            observer.find(&[Value::text("a@example.test")]).unwrap()[0].row_id,
            1
        );
    }
}
