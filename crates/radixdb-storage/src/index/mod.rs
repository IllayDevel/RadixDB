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

//! Index implementations for RadixDB
//!
//! This module provides all index structures used by the storage engine:
//!
//! - [`BTreeIndex`] - B-tree index for range queries and sorted access
//! - [`HashIndex`] - Hash index for O(1) equality lookups
//! - [`BitmapIndex`] - Bitmap index for low-cardinality columns
//! - [`HnswIndex`] - HNSW index for approximate nearest neighbor search
//! - [`MultiColumnIndex`] - Composite index for multi-column queries
//! - [`PkIndex`] - Primary key index (virtual, auto-created)

pub mod bitmap;
pub mod btree;
pub mod encoded;
pub mod hash;
pub mod hnsw;
pub mod multi_column;
pub mod partial;
pub mod pk;
pub mod renamed;

// Re-export main types
pub use bitmap::BitmapIndex;
pub use btree::{
    intersect_multiple_sorted_ids, intersect_sorted_ids, union_multiple_sorted_ids,
    union_sorted_ids, BTreeIndex,
};
pub use encoded::{EncodedIndex, PreparedIndexKeyEncoder};
pub use hash::HashIndex;
pub use hnsw::{
    default_ef_construction, default_ef_search, default_m_for_dims, HnswDistanceMetric, HnswIndex,
};
pub use multi_column::{CompositeKey, MultiColumnIndex};
pub use partial::{
    PartialIndex, PartialIndexPredicate, PartialIndexPredicateBinder, PartialIndexPredicateMetadata,
};
pub use pk::PkIndex;
pub use renamed::RenamedIndex;

#[doc(hidden)]
pub fn validate_index_metadata_shape(
    name: &str,
    table_name: &str,
    column_names: &[String],
    column_ids: &[i32],
    data_types: &[radixdb_core::DataType],
) -> radixdb_core::Result<()> {
    if name.trim().is_empty() || table_name.trim().is_empty() {
        return Err(radixdb_core::Error::invalid_argument(
            "index and table names must be non-empty",
        ));
    }
    if column_names.is_empty()
        || column_names.len() != column_ids.len()
        || column_names.len() != data_types.len()
    {
        return Err(radixdb_core::Error::invalid_argument(format!(
            "index metadata arity mismatch: names={}, ids={}, types={}",
            column_names.len(),
            column_ids.len(),
            data_types.len()
        )));
    }
    let mut seen_names = rustc_hash::FxHashSet::default();
    let mut seen_ids = rustc_hash::FxHashSet::default();
    for (column_name, column_id) in column_names.iter().zip(column_ids) {
        if column_name.trim().is_empty() || *column_id < 0 {
            return Err(radixdb_core::Error::invalid_argument(
                "index column names must be non-empty and IDs non-negative",
            ));
        }
        if !seen_names.insert(column_name.to_lowercase()) || !seen_ids.insert(*column_id) {
            return Err(radixdb_core::Error::invalid_argument(
                "index metadata contains duplicate columns",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod v2_r3_tests {
    use super::*;
    use radixdb_core::DataType;

    #[test]
    fn public_index_constructors_reject_incoherent_metadata() {
        assert!(HashIndex::try_new(
            "idx".into(),
            "items".into(),
            vec!["a".into(), "b".into()],
            vec![0],
            vec![DataType::Integer, DataType::Text],
            false,
            0,
        )
        .is_err());
        assert!(MultiColumnIndex::try_new(
            "idx".into(),
            "items".into(),
            vec!["a".into(), "a".into()],
            vec![0, 1],
            vec![DataType::Integer, DataType::Integer],
            false,
            0,
        )
        .is_err());
        assert!(BitmapIndex::try_new(
            "idx".into(),
            "items".into(),
            vec!["a".into(), "b".into()],
            vec![0, 1],
            vec![DataType::Integer, DataType::Integer],
            false,
            0,
        )
        .is_err());
        assert!(HashIndex::try_new(
            "".into(),
            "items".into(),
            vec!["a".into()],
            vec![-1],
            vec![DataType::Integer],
            false,
            0,
        )
        .is_err());
    }
}
