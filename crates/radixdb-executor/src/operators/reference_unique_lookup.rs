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

//! Shared bounded physical lookup for classic indexed JOIN and navigation.
//!
//! This module owns the storage-facing half of a `Reference/UniqueLookupJoin`
//! edge: bounded multi-key index resolution, candidate de-duplication,
//! projected row fetch and cancellation boundaries. SQL JOIN owns match/NULL
//! semantics; navigation additionally verifies its required-target contract.

use crate::lookup_key::exact_integer_pk_value;
use radixdb_core::CompactArc;
use radixdb_core::{Result, Row, RowVec, Value, ValueMap};
use radixdb_storage::traits::{Index, Table};

// Match the bounded physical row-group window. The storage candidate guard
// bisects a request when fan-out exceeds its 65K-row ceiling, so starting with
// 256 keys only multiplied authoritative artifact-backed rechecks without reducing the
// actual memory bound. Unique/reference edges normally complete in one call;
// wide non-unique edges converge to the largest safe sub-batches.
const UNIQUE_LOOKUP_KEYS_PER_BATCH: usize = 65_536;
const NON_UNIQUE_LOOKUP_KEYS_PER_BATCH: usize = 65_536;
// artifact-backed decodes one 64K row group per selected column. Fetching 256 row IDs at a
// time discarded the request-local decoded-block cache after every tiny slice
// and could decode the same group hundreds of times. Keep one bounded physical
// row group per fetch so keyed fan-out remains linear without retaining an
// unbounded candidate set.
const LOOKUP_FETCH_ROWS_PER_BATCH: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupEdgeCardinality {
    Unbounded,
    AtMostOne,
    ExactlyOne,
}

impl LookupEdgeCardinality {
    fn has_unique_bound(self) -> bool {
        matches!(self, Self::AtMostOne | Self::ExactlyOne)
    }
}

pub enum LookupEdgeFallback<'a> {
    None,
    IntegerPrimaryKey,
    SecondaryIndex(&'a dyn Index),
}

pub struct LookupEdgeBatchResult {
    pub rows: RowVec,
    pub lookup_calls: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniqueLookupIntegrity {
    Complete,
    Missing,
    NotUnique,
}

/// Result rows for one proven-unique physical edge.
///
/// Dense INTEGER keys avoid hashing on the common PK/reference path. Other
/// domains retain one hash slot per requested key. Empty slots represent a
/// legitimate LEFT JOIN miss for `AtMostOne` and an integrity failure for
/// `ExactlyOne`.
pub enum UniqueLookupRows {
    DenseInteger {
        base: i64,
        requested: Vec<bool>,
        rows: Vec<Row>,
        requested_count: usize,
        populated: usize,
    },
    Hash {
        rows: ValueMap<Row>,
        populated: usize,
    },
}

/// Immutable batch representation consumed by a JOIN operator.
///
/// The lookup owns each physical row once. Repeated outer keys retain only an
/// Arc + row index instead of cloning every inner value for every output row.
pub enum SharedUniqueLookupRows {
    DenseInteger {
        base: i64,
        requested: Vec<bool>,
        rows: CompactArc<Vec<Row>>,
    },
    Hash {
        row_indices: ValueMap<usize>,
        rows: CompactArc<Vec<Row>>,
    },
}

impl SharedUniqueLookupRows {
    pub fn get(&self, key: &Value) -> Option<(CompactArc<Vec<Row>>, usize)> {
        match self {
            Self::DenseInteger {
                base,
                requested,
                rows,
            } => {
                let index = UniqueLookupRows::dense_index(*base, requested.len(), key)?;
                requested[index]
                    .then_some(())
                    .filter(|()| !rows[index].is_empty())?;
                Some((CompactArc::clone(rows), index))
            }
            Self::Hash { row_indices, rows } => row_indices
                .get(key)
                .copied()
                .map(|index| (CompactArc::clone(rows), index)),
        }
    }
}

impl UniqueLookupRows {
    const MAX_DENSE_SLOTS: usize = 1_000_000;
    const MAX_DENSE_SPAN_PER_KEY: usize = 4;

    pub fn new(keys: &[Value]) -> Self {
        let mut integer_bounds: Option<(i64, i64)> = None;
        for key in keys {
            let Value::Integer(value) = key else {
                integer_bounds = None;
                break;
            };
            integer_bounds = Some(match integer_bounds {
                None => (*value, *value),
                Some((min, max)) => (min.min(*value), max.max(*value)),
            });
        }
        if let Some((min, max)) = integer_bounds {
            let span = i128::from(max) - i128::from(min) + 1;
            if let Ok(span) = usize::try_from(span) {
                if span <= Self::MAX_DENSE_SLOTS
                    && span <= keys.len().saturating_mul(Self::MAX_DENSE_SPAN_PER_KEY)
                {
                    let mut requested = vec![false; span];
                    let mut requested_count = 0usize;
                    for key in keys {
                        let Value::Integer(value) = key else {
                            unreachable!("integer bounds require integer keys");
                        };
                        let index = usize::try_from(i128::from(*value) - i128::from(min))
                            .expect("dense integer key lies inside its checked span");
                        if !requested[index] {
                            requested[index] = true;
                            requested_count += 1;
                        }
                    }
                    return Self::DenseInteger {
                        base: min,
                        requested,
                        rows: vec![Row::new(); span],
                        requested_count,
                        populated: 0,
                    };
                }
            }
        }

        Self::Hash {
            rows: keys.iter().cloned().map(|key| (key, Row::new())).collect(),
            populated: 0,
        }
    }

    fn dense_index(base: i64, len: usize, key: &Value) -> Option<usize> {
        let Value::Integer(value) = key else {
            return None;
        };
        let offset = i128::from(*value) - i128::from(base);
        usize::try_from(offset).ok().filter(|&index| index < len)
    }

    pub fn contains_key(&self, key: &Value) -> bool {
        match self {
            Self::DenseInteger {
                base, requested, ..
            } => {
                Self::dense_index(*base, requested.len(), key).is_some_and(|index| requested[index])
            }
            Self::Hash { rows, .. } => rows.contains_key(key),
        }
    }

    pub fn get(&self, key: &Value) -> Option<&[Value]> {
        match self {
            Self::DenseInteger {
                base,
                requested,
                rows,
                ..
            } => {
                let index = Self::dense_index(*base, requested.len(), key)?;
                requested[index]
                    .then_some(&rows[index])
                    .filter(|row| !row.is_empty())
                    .map(Row::as_slice)
            }
            Self::Hash { rows, .. } => rows
                .get(key)
                .filter(|row| !row.is_empty())
                .map(Row::as_slice),
        }
    }

    fn insert(&mut self, key: Value, row: Row) -> bool {
        debug_assert!(!row.is_empty());
        match self {
            Self::DenseInteger {
                base,
                requested,
                rows,
                populated,
                ..
            } => {
                let Some(index) = Self::dense_index(*base, requested.len(), &key) else {
                    return false;
                };
                if !requested[index] || !rows[index].is_empty() {
                    return false;
                }
                rows[index] = row;
                *populated += 1;
                true
            }
            Self::Hash { rows, populated } => {
                let Some(slot) = rows.get_mut(&key) else {
                    return false;
                };
                if !slot.is_empty() {
                    return false;
                }
                *slot = row;
                *populated += 1;
                true
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::DenseInteger { populated, .. } | Self::Hash { populated, .. } => *populated,
        }
    }

    /// Return whether the lookup contains no keyed rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn is_complete(&self) -> bool {
        match self {
            Self::DenseInteger {
                requested_count,
                populated,
                ..
            } => populated == requested_count,
            Self::Hash { rows, populated } => *populated == rows.len(),
        }
    }

    pub fn visit_values(&self, mut visit: impl FnMut(&[Value])) {
        match self {
            Self::DenseInteger { rows, .. } => {
                for row in rows.iter().filter(|row| !row.is_empty()) {
                    visit(row.as_slice());
                }
            }
            Self::Hash { rows, .. } => {
                for row in rows.values().filter(|row| !row.is_empty()) {
                    visit(row.as_slice());
                }
            }
        }
    }

    pub fn into_shared(self) -> SharedUniqueLookupRows {
        match self {
            Self::DenseInteger {
                base,
                requested,
                rows,
                ..
            } => SharedUniqueLookupRows::DenseInteger {
                base,
                requested,
                rows: CompactArc::new(rows),
            },
            Self::Hash { rows, populated } => {
                let mut shared_rows = Vec::with_capacity(populated);
                let mut row_indices = ValueMap::with_capacity(populated);
                for (key, row) in rows {
                    if row.is_empty() {
                        continue;
                    }
                    row_indices.insert(key, shared_rows.len());
                    shared_rows.push(row);
                }
                SharedUniqueLookupRows::Hash {
                    row_indices,
                    rows: CompactArc::new(shared_rows),
                }
            }
        }
    }
}

pub struct UniqueLookupJoinBatch {
    pub rows_by_key: UniqueLookupRows,
    pub lookup_calls: u64,
    pub candidate_rows: usize,
    pub integrity: UniqueLookupIntegrity,
}

fn assemble_unique_rows(
    requested_keys: &[Value],
    candidates: RowVec,
    key_index: usize,
    cardinality: LookupEdgeCardinality,
    mut admit: impl FnMut(&[Value]) -> Result<()>,
    mut check_cancelled: impl FnMut() -> Result<()>,
) -> Result<UniqueLookupJoinBatch> {
    let candidate_rows = candidates.len();
    let mut rows_by_key = UniqueLookupRows::new(requested_keys);
    for (index, (_, row)) in candidates.into_iter().enumerate() {
        if index & 0xff == 0 {
            check_cancelled()?;
        }
        let key = row.get(key_index).cloned().ok_or_else(|| {
            radixdb_core::Error::internal("unique lookup candidate omitted its key")
        })?;
        if !rows_by_key.contains_key(&key) {
            continue;
        }
        admit(row.as_slice())?;
        if !rows_by_key.insert(key, row) {
            return Ok(UniqueLookupJoinBatch {
                rows_by_key,
                lookup_calls: 0,
                candidate_rows,
                integrity: UniqueLookupIntegrity::NotUnique,
            });
        }
    }
    check_cancelled()?;
    let integrity =
        if cardinality == LookupEdgeCardinality::ExactlyOne && !rows_by_key.is_complete() {
            UniqueLookupIntegrity::Missing
        } else {
            UniqueLookupIntegrity::Complete
        };
    Ok(UniqueLookupJoinBatch {
        rows_by_key,
        lookup_calls: 0,
        candidate_rows,
        integrity,
    })
}

pub fn materialize_unique_lookup_candidates(
    requested_keys: &[Value],
    candidates: RowVec,
    key_index: usize,
    cardinality: LookupEdgeCardinality,
    admit: impl FnMut(&[Value]) -> Result<()>,
    check_cancelled: impl FnMut() -> Result<()>,
) -> Result<UniqueLookupJoinBatch> {
    debug_assert!(cardinality.has_unique_bound());
    assemble_unique_rows(
        requested_keys,
        candidates,
        key_index,
        cardinality,
        admit,
        check_cancelled,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_unique_lookup_join_batch(
    table: &dyn Table,
    key_column: &str,
    keys: &[Value],
    projection: Option<&[usize]>,
    projected_key_index: usize,
    cardinality: LookupEdgeCardinality,
    fallback: LookupEdgeFallback<'_>,
    admit: impl FnMut(&[Value]) -> Result<()>,
    mut check_cancelled: impl FnMut() -> Result<()>,
) -> Result<Option<UniqueLookupJoinBatch>> {
    let Some(batch) = lookup_edge_batch(
        table,
        key_column,
        keys,
        projection,
        cardinality,
        fallback,
        &mut check_cancelled,
    )?
    else {
        return Ok(None);
    };
    let lookup_calls = batch.lookup_calls;
    let mut result = assemble_unique_rows(
        keys,
        batch.rows,
        projected_key_index,
        cardinality,
        admit,
        check_cancelled,
    )?;
    result.lookup_calls = lookup_calls;
    Ok(Some(result))
}

/// Execute one bounded storage lookup edge.
///
/// `Ok(None)` means the table has no compatible exact lookup and the caller
/// supplied no physical fallback. Navigation may then choose its explicit
/// hash/merge/snapshot fallback; classic JOIN treats disappearance of its
/// selected segmented index as an error.
pub fn lookup_edge_batch(
    table: &dyn Table,
    key_column: &str,
    keys: &[Value],
    projection: Option<&[usize]>,
    cardinality: LookupEdgeCardinality,
    fallback: LookupEdgeFallback<'_>,
    mut check_cancelled: impl FnMut() -> Result<()>,
) -> Result<Option<LookupEdgeBatchResult>> {
    debug_assert!(!keys.is_empty());
    let key_batch_size = if cardinality.has_unique_bound() {
        UNIQUE_LOOKUP_KEYS_PER_BATCH
    } else {
        NON_UNIQUE_LOOKUP_KEYS_PER_BATCH
    };
    let mut row_ids = Vec::new();
    let mut rows = RowVec::new();
    let mut lookup_calls = 0u64;

    for key_chunk in keys.chunks(key_batch_size) {
        let mut pending = vec![key_chunk];
        while let Some(candidate_keys) = pending.pop() {
            check_cancelled()?;
            lookup_calls = lookup_calls.saturating_add(1);
            if let Some(result) =
                table.collect_rows_by_index_values_projected(key_column, candidate_keys, projection)
            {
                rows.extend(result?);
                continue;
            }

            // A persisted cold lookup may decline a multi-key request only
            // because its bounded candidate guard was exceeded. Retry smaller
            // physical batches before concluding that the index path is not
            // available. Push the right half first to preserve left-to-right
            // processing when it is popped from the stack.
            if candidate_keys.len() > 1 {
                let middle = candidate_keys.len() / 2;
                pending.push(&candidate_keys[middle..]);
                pending.push(&candidate_keys[..middle]);
                continue;
            }

            match fallback {
                LookupEdgeFallback::None => return Ok(None),
                LookupEdgeFallback::IntegerPrimaryKey => {
                    row_ids.extend(candidate_keys.iter().filter_map(exact_integer_pk_value));
                }
                LookupEdgeFallback::SecondaryIndex(index) => {
                    index.get_row_ids_equal_into(candidate_keys, &mut row_ids)?;
                }
            }
        }
    }

    row_ids.sort_unstable();
    row_ids.dedup();
    for row_id_chunk in row_ids.chunks(LOOKUP_FETCH_ROWS_PER_BATCH) {
        check_cancelled()?;
        let fetched = if let Some(projection) = projection {
            table.collect_rows_by_ids_projected(row_id_chunk, projection)?
        } else {
            table.collect_rows_by_ids(row_id_chunk)?
        };
        rows.extend(fetched);
    }
    rows.sort_unstable_by_key(|(row_id, _)| *row_id);
    rows.dedup_by(|left, right| left.0 == right.0);
    check_cancelled()?;

    Ok(Some(LookupEdgeBatchResult { rows, lookup_calls }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidates(rows: Vec<(i64, Vec<Value>)>) -> RowVec {
        RowVec::from_vec(
            rows.into_iter()
                .map(|(row_id, values)| (row_id, Row::from_values(values)))
                .collect(),
        )
    }

    #[test]
    fn unique_lookup_dense_exactly_one_is_complete() {
        let batch = materialize_unique_lookup_candidates(
            &[Value::Integer(10), Value::Integer(11)],
            candidates(vec![
                (1, vec![Value::Integer(10), Value::text("a")]),
                (2, vec![Value::Integer(11), Value::text("b")]),
            ]),
            0,
            LookupEdgeCardinality::ExactlyOne,
            |_| Ok(()),
            || Ok(()),
        )
        .unwrap();

        assert_eq!(batch.integrity, UniqueLookupIntegrity::Complete);
        assert_eq!(batch.rows_by_key.len(), 2);
        assert_eq!(
            batch.rows_by_key.get(&Value::Integer(11)),
            Some([Value::Integer(11), Value::text("b")].as_slice())
        );
    }

    #[test]
    fn unique_lookup_exactly_one_reports_missing_and_duplicate() {
        let missing = materialize_unique_lookup_candidates(
            &[Value::Integer(10)],
            RowVec::new(),
            0,
            LookupEdgeCardinality::ExactlyOne,
            |_| Ok(()),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(missing.integrity, UniqueLookupIntegrity::Missing);

        let duplicate = materialize_unique_lookup_candidates(
            &[Value::Integer(10)],
            candidates(vec![
                (1, vec![Value::Integer(10), Value::text("a")]),
                (2, vec![Value::Integer(10), Value::text("b")]),
            ]),
            0,
            LookupEdgeCardinality::ExactlyOne,
            |_| Ok(()),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(duplicate.integrity, UniqueLookupIntegrity::NotUnique);
    }

    #[test]
    fn unique_lookup_at_most_one_allows_missing_rows() {
        let batch = materialize_unique_lookup_candidates(
            &[Value::text("missing")],
            RowVec::new(),
            0,
            LookupEdgeCardinality::AtMostOne,
            |_| Ok(()),
            || Ok(()),
        )
        .unwrap();

        assert_eq!(batch.integrity, UniqueLookupIntegrity::Complete);
        assert_eq!(batch.rows_by_key.len(), 0);
        assert!(matches!(batch.rows_by_key, UniqueLookupRows::Hash { .. }));
    }
}
