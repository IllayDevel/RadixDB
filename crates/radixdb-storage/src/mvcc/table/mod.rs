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

//! MVCC Table implementation
//!
//! Provides MVCC isolation for table operations.
//!

use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::{Arc, RwLock};

use crate::expression::logical::OrExpr;
use crate::expression::Expression;
use crate::index::{
    BTreeIndex, BitmapIndex, HashIndex, HnswIndex, MultiColumnIndex, PartialIndex,
    PartialIndexPredicate,
};
use crate::mvcc::{get_fast_timestamp, TransactionVersionStore, VersionStore};
use crate::traits::table::DeferredSum;
use crate::traits::MVCCScanner;
use crate::traits::MemoryResult;
use crate::traits::{Index, QueryResult, ScanPlan, Scanner, Table};
use radixdb_core::{CompactArc, I64Set};
use radixdb_core::{
    DataType, Error, IndexType, Result, Row, RowIdVec, RowVec, Schema, SchemaColumn, Value,
};

#[derive(Clone)]
struct CompositeRangeBounds {
    min: Option<(Value, bool)>,
    max: Option<(Value, bool)>,
}

/// One executable composite-index access path shared by query execution and
/// EXPLAIN. Keeping the bound keys and the rendered columns in one object
/// prevents the planner from advertising a path the storage lookup cannot run
/// (or, as in RDB-0007, hiding a path execution already understands).
#[derive(Clone)]
struct CompositeIndexLookupPlan {
    index: Arc<dyn Index>,
    equality_values: Vec<Value>,
    range: Option<CompositeRangeBounds>,
    columns: Vec<String>,
    conditions: Vec<String>,
    covered_columns: FxHashSet<String>,
}

impl CompositeIndexLookupPlan {
    fn lookup_row_ids(&self) -> Result<Option<RowIdVec>> {
        if let Some(range) = &self.range {
            let mut min_key = self.equality_values.clone();
            let mut max_key = self.equality_values.clone();
            let min_inclusive = if let Some((value, inclusive)) = &range.min {
                min_key.push(value.clone());
                *inclusive
            } else {
                true
            };
            let max_inclusive = if let Some((value, inclusive)) = &range.max {
                max_key.push(value.clone());
                *inclusive
            } else {
                // Keep an equality prefix as an inclusive partial upper bound;
                // no prefix means an unbounded upper range.
                true
            };
            let entries =
                self.index
                    .find_range(&min_key, &max_key, min_inclusive, max_inclusive)?;
            let mut ids = RowIdVec::with_capacity(entries.len());
            ids.extend(entries.into_iter().map(|entry| entry.row_id));
            Ok(Some(ids))
        } else {
            Ok(Some(self.index.get_row_ids_equal(&self.equality_values)?))
        }
    }

    fn lookup_ordered_limited(
        &self,
        ascending: bool,
        limit: usize,
    ) -> Option<Vec<radixdb_core::IndexEntry>> {
        let range = self.range.as_ref()?;
        let mut min_key = self.equality_values.clone();
        let mut max_key = self.equality_values.clone();
        let min_inclusive = if let Some((value, inclusive)) = &range.min {
            min_key.push(value.clone());
            *inclusive
        } else {
            true
        };
        let max_inclusive = if let Some((value, inclusive)) = &range.max {
            max_key.push(value.clone());
            *inclusive
        } else {
            true
        };
        self.index
            .find_range_ordered_limited(
                &min_key,
                &max_key,
                min_inclusive,
                max_inclusive,
                ascending,
                limit,
            )
            .ok()
    }
}

/// MVCC Table wrapper that provides MVCC isolation for tables
pub(crate) struct MVCCTable {
    /// Transaction ID
    txn_id: i64,
    /// Reference to the version store
    version_store: Arc<VersionStore>,
    /// Transaction-local version store (shared between multiple MVCCTable instances for same txn+table)
    txn_versions: Arc<RwLock<TransactionVersionStore>>,
    /// Cached schema for returning references (Arc clone from version_store - O(1) instead of cloning)
    cached_schema: CompactArc<Schema>,
    /// Standalone storage tests may commit an owned table directly. Handles
    /// backed by an engine transaction must commit through `Transaction`,
    /// where cross-table CHECK/FK revalidation and the durable marker live.
    allow_direct_commit: bool,
}

mod inherent;
mod table_impl;

/// Helper function to convert Operator to string for display
fn operator_to_string(op: radixdb_core::Operator) -> &'static str {
    use radixdb_core::Operator;
    match op {
        Operator::Eq => "=",
        Operator::Ne => "!=",
        Operator::Lt => "<",
        Operator::Lte => "<=",
        Operator::Gt => ">",
        Operator::Gte => ">=",
        Operator::Like => "LIKE",
        Operator::In => "IN",
        Operator::NotIn => "NOT IN",
        Operator::IsNull => "IS NULL",
        Operator::IsNotNull => "IS NOT NULL",
    }
}

#[cfg(test)]
mod tests;
