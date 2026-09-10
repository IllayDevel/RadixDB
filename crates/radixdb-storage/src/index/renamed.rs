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

//! Metadata-only index wrapper.
//!
//! `ALTER INDEX ... RENAME TO ...` must not rebuild index data. Existing index
//! implementations also keep column names/ordinals immutable. This wrapper can
//! publish the post-DDL metadata while delegating all data operations to the
//! original index object.

use std::sync::Arc;

use crate::expression::Expression;
use crate::traits::Index;
use radixdb_core::I64Map;
use radixdb_core::{DataType, Error, IndexEntry, IndexType, Operator, Result, RowIdVec, Value};

pub struct RenamedIndex {
    name: String,
    column_ids: Vec<i32>,
    column_names: Vec<String>,
    inner: Arc<dyn Index>,
}

impl RenamedIndex {
    fn canonical_inner(mut inner: Arc<dyn Index>) -> Arc<dyn Index> {
        while let Some(next) = inner.metadata_inner() {
            inner = next;
        }
        inner
    }

    #[doc(hidden)]
    pub fn new(name: String, inner: Arc<dyn Index>) -> Self {
        let column_ids = inner.column_ids().to_vec();
        let column_names = inner.column_names().to_vec();
        Self {
            name,
            column_ids,
            column_names,
            inner: Self::canonical_inner(inner),
        }
    }

    #[doc(hidden)]
    pub fn with_columns(
        name: String,
        inner: Arc<dyn Index>,
        column_ids: Vec<i32>,
        column_names: Vec<String>,
    ) -> Self {
        Self {
            name,
            column_ids,
            column_names,
            inner: Self::canonical_inner(inner),
        }
    }
}

impl Index for RenamedIndex {
    fn name(&self) -> &str {
        &self.name
    }

    fn table_name(&self) -> &str {
        self.inner.table_name()
    }

    fn build(&mut self) -> Result<()> {
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.build()
        } else {
            Err(Error::NotSupported(
                "cannot exclusively build a shared renamed index".to_string(),
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
        &self.column_ids
    }

    fn column_names(&self) -> &[String] {
        &self.column_names
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

    fn metadata_inner(&self) -> Option<Arc<dyn Index>> {
        Some(Arc::clone(&self.inner))
    }

    fn partial_predicate(&self) -> Option<&crate::index::PartialIndexPredicate> {
        self.inner.partial_predicate()
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

    fn get_filtered_row_ids(&self, expr: &dyn Expression) -> Result<RowIdVec> {
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
                "cannot exclusively close a shared renamed index".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::HashIndex;

    #[test]
    fn v2_r3_shared_renamed_lifecycle_cannot_report_false_success() {
        let inner: Arc<dyn Index> = Arc::new(HashIndex::new(
            "idx_value".into(),
            "items".into(),
            vec!["value".into()],
            vec![0],
            vec![DataType::Integer],
            false,
            0,
        ));
        let observer = Arc::clone(&inner);
        let mut renamed = RenamedIndex::new("idx_value_v2".into(), inner);

        assert!(renamed.build().is_err());
        assert!(renamed.close().is_err());
        observer.add(&[Value::Integer(7)], 7, 7).unwrap();
        assert_eq!(observer.find(&[Value::Integer(7)]).unwrap()[0].row_id, 7);
    }
}
