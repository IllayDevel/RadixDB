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

//! SegmentedTable: wraps an MVCCTable (hot buffer) with immutable segments.
//!
//! Write operations delegate to the inner MVCCTable unchanged.
//! Read operations merge results from segments + hot buffer.
//! Aggregation pushdowns use pre-computed segment stats when possible.
//!
//! Key design: volume rows do NOT live in normal MVCC secondary indexes.
//! Hot indexes only cover hot rows. Cold data has its own immutable access
//! path via segment zone maps, binary search, dictionary pre-filters, and
//! immutable exact/ordered postings.
//!
//! This is the Table trait implementation that makes the executor unaware
//! of whether data lives in memory or frozen segments.

use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::expression::Expression;
use crate::timestamp::get_fast_timestamp;
use crate::traits::table::{
    index_values_for_row, AggregateOp, DeferredSum, GroupKey, GroupedAggregateResult,
    IntegerPrimaryKeyRange, ScanPlan,
};
use crate::traits::{Index, QueryResult, Scanner, Table};
use radixdb_core::{time_compat::Instant, CompactArc};
use radixdb_core::{DataType, IndexType, Result, Row, RowVec, Schema, Value, ValueMap, ValueSet};

use super::manifest::{ColdSegment, SegmentManager};
use super::scanner::VolumeScanner;
use super::writer::FrozenVolume;
use super::{
    column::{ColumnData, ZoneMap},
    writer::ColSource,
};
use crate::v6::ArtifactColumnBatch;

const COLD_INDEX_CURSOR_CANDIDATE_LIMIT: usize = 65_536;
/// Keep the metadata-only count bounded.  Wider ranges are still correct via
/// the generic scanner; this operator exists for selective PK equality/range
/// predicates and must never turn a broad COUNT into an unbounded temporary
/// row-id set.
const METADATA_PK_COUNT_CANDIDATE_LIMIT: usize = 65_536;
/// Maximum inclusive INTEGER/TIMESTAMP key domain width for the artifact GROUP BY
/// direct-array accumulator. Wider or sparse domains keep the hash-map path to
/// avoid turning an optimization into an accidental memory bomb.
const ARTIFACT_COLUMNAR_GROUP_DIRECT_ARRAY_MAX_WIDTH: u64 = 1_000_000;

type GroupKeyMap<V> = ahash::AHashMap<GroupKey, V>;

struct ColdAggregateProjection {
    columns: Vec<usize>,
    positions: Vec<Option<usize>>,
    aggregates: Vec<(AggregateOp, usize)>,
}

/// Exact, deliberately narrow contract for the artifact grouped-column operator.
///
/// It is separate from the generic `(AggregateOp, column)` API because this
/// operator must be able to decline a query without weakening SQL semantics.
/// In particular, it only runs against an unmodified cold artifact set with one
/// INTEGER/TIMESTAMP group key. Everything else keeps the scanner fallback.
struct ArtifactColumnarGroupPlan {
    group_data_type: DataType,
    group_projection_pos: usize,
    physical_projection: Vec<usize>,
    aggregates: Vec<ArtifactColumnarGroupAggregate>,
}

struct ArtifactColumnarGroupAggregate {
    operation: AggregateOp,
    data_type: DataType,
    /// `None` is COUNT(*), which deliberately has no physical input column.
    projection_pos: Option<usize>,
}

fn exact_integer_sum_value(sum: i128) -> Option<Value> {
    if let Ok(integer) = i64::try_from(sum) {
        return Some(Value::Integer(integer));
    }
    let precision = u8::try_from(sum.to_string().trim_start_matches('-').len()).ok()?;
    Value::try_decimal(sum, precision, 0).ok()
}

/// Typed state retained per group by the artifact operator. No `Value` or `Row`
/// exists in the input loop; `Value` is constructed only for final results.
#[derive(Clone)]
struct ArtifactColumnarAccum {
    count: i64,
    int_sum: i128,
    float_sum: f64,
    min_i64: Option<i64>,
    max_i64: Option<i64>,
    min_f64: Option<f64>,
    max_f64: Option<f64>,
}

impl Default for ArtifactColumnarAccum {
    fn default() -> Self {
        Self {
            count: 0,
            int_sum: 0,
            float_sum: 0.0,
            min_i64: None,
            max_i64: None,
            min_f64: None,
            max_f64: None,
        }
    }
}

enum ArtifactColumnarGroupState {
    HashMap {
        groups: FxHashMap<Option<i64>, Vec<ArtifactColumnarAccum>>,
    },
    DirectArray {
        min_key: i64,
        slots: Vec<Option<Vec<ArtifactColumnarAccum>>>,
        null_group: Option<Vec<ArtifactColumnarAccum>>,
        non_null_groups: usize,
    },
}

struct ArtifactColumnarGroupSegmentResult {
    groups: ArtifactColumnarGroupState,
    row_groups: u64,
    selected_blocks: u64,
    input_rows: u64,
}

impl ArtifactColumnarGroupState {
    fn new(bounds: Option<(i64, i64)>) -> Self {
        let Some((min_key, max_key)) = bounds else {
            return Self::hash_map();
        };
        let Some(width) = max_key
            .checked_sub(min_key)
            .and_then(|delta| delta.checked_add(1))
            .and_then(|width| u64::try_from(width).ok())
        else {
            return Self::hash_map();
        };
        if width == 0 || width > ARTIFACT_COLUMNAR_GROUP_DIRECT_ARRAY_MAX_WIDTH {
            return Self::hash_map();
        }
        let Ok(width) = usize::try_from(width) else {
            return Self::hash_map();
        };
        Self::DirectArray {
            min_key,
            slots: vec![None; width],
            null_group: None,
            non_null_groups: 0,
        }
    }

    fn hash_map() -> Self {
        Self::HashMap {
            groups: FxHashMap::default(),
        }
    }

    fn is_direct_array(&self) -> bool {
        matches!(self, Self::DirectArray { .. })
    }

    fn accums_for_key(
        &mut self,
        key: Option<i64>,
        aggregate_count: usize,
    ) -> Option<&mut Vec<ArtifactColumnarAccum>> {
        match self {
            Self::HashMap { groups } => Some(
                groups
                    .entry(key)
                    .or_insert_with(|| vec![ArtifactColumnarAccum::default(); aggregate_count]),
            ),
            Self::DirectArray {
                min_key,
                slots,
                null_group,
                non_null_groups,
            } => {
                let Some(key) = key else {
                    return Some(null_group.get_or_insert_with(|| {
                        vec![ArtifactColumnarAccum::default(); aggregate_count]
                    }));
                };
                let offset = key.checked_sub(*min_key)?;
                let offset = usize::try_from(offset).ok()?;
                let slot = slots.get_mut(offset)?;
                if slot.is_none() {
                    *non_null_groups = non_null_groups.saturating_add(1);
                    *slot = Some(vec![ArtifactColumnarAccum::default(); aggregate_count]);
                }
                slot.as_mut()
            }
        }
    }

    fn merge_from(&mut self, other: ArtifactColumnarGroupState, aggregate_count: usize) -> bool {
        for (key, source_accums) in other.into_key_accums() {
            let Some(target_accums) = self.accums_for_key(key, aggregate_count) else {
                return false;
            };
            if target_accums.len() != source_accums.len() {
                return false;
            }
            for (target, source) in target_accums.iter_mut().zip(source_accums) {
                target.merge_from(source);
            }
        }
        true
    }

    fn group_count(&self) -> usize {
        match self {
            Self::HashMap { groups } => groups.len(),
            Self::DirectArray {
                null_group,
                non_null_groups,
                ..
            } => non_null_groups.saturating_add(usize::from(null_group.is_some())),
        }
    }

    fn into_key_accums(self) -> Vec<(Option<i64>, Vec<ArtifactColumnarAccum>)> {
        match self {
            Self::HashMap { groups } => groups.into_iter().collect(),
            Self::DirectArray {
                min_key,
                slots,
                null_group,
                ..
            } => {
                let mut groups = Vec::with_capacity(
                    slots.iter().filter(|slot| slot.is_some()).count()
                        + usize::from(null_group.is_some()),
                );
                if let Some(accums) = null_group {
                    groups.push((None, accums));
                }
                for (offset, slot) in slots.into_iter().enumerate() {
                    if let Some(accums) = slot {
                        let Some(key) = i64::try_from(offset)
                            .ok()
                            .and_then(|offset| min_key.checked_add(offset))
                        else {
                            continue;
                        };
                        groups.push((Some(key), accums));
                    }
                }
                groups
            }
        }
    }
}

impl ArtifactColumnarAccum {
    fn merge_from(&mut self, other: ArtifactColumnarAccum) {
        self.count = self.count.saturating_add(other.count);
        self.int_sum = self.int_sum.saturating_add(other.int_sum);
        self.float_sum += other.float_sum;
        self.min_i64 = match (self.min_i64, other.min_i64) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (None, value) | (value, None) => value,
        };
        self.max_i64 = match (self.max_i64, other.max_i64) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (None, value) | (value, None) => value,
        };
        self.min_f64 = match (self.min_f64, other.min_f64) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (None, value) | (value, None) => value,
        };
        self.max_f64 = match (self.max_f64, other.max_f64) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (None, value) | (value, None) => value,
        };
    }
}

/// A table backed by immutable segments (historical) + an MVCCTable (hot buffer).
///
/// The executor sees a single Table interface. Reads merge across all sources.
/// Writes go exclusively to the hot buffer (MVCCTable).
///
/// Visibility: cold segment rows are skipped via per-volume skip sets built
/// from hot row_ids and tombstones. Newer volumes shadow older ones.
///
/// Normal secondary indexes exist only for hot rows. Volume rows are never
/// inserted into hot indexes. Constraint checks (PK/UNIQUE) against cold data
/// use segment metadata (zone maps, sorted columns, dictionary pre-filters).
pub struct SegmentedTable {
    /// The hot buffer (current in-memory MVCC table for writes)
    hot: Box<dyn Table>,
    /// Segment manager (shared, engine-owned): segments, tombstones, manifest
    segment_mgr: Arc<SegmentManager>,
    /// Snapshot sequence for snapshot isolation transactions.
    /// If Some(seq), only tombstones with commit_seq <= seq are visible,
    /// preserving the snapshot's point-in-time view of cold data.
    /// None for auto-commit transactions (all tombstones visible).
    snapshot_seq: Option<u64>,
}

struct ColdIndexRemovalStatementGuard {
    manager: Arc<SegmentManager>,
    txn_id: i64,
    checkpoint: usize,
    armed: bool,
}

impl ColdIndexRemovalStatementGuard {
    fn new(manager: Arc<SegmentManager>, txn_id: i64) -> Self {
        let checkpoint = manager.cold_index_removal_checkpoint(txn_id);
        Self {
            manager,
            txn_id,
            checkpoint,
            armed: true,
        }
    }

    fn finish(mut self) {
        self.armed = false;
    }
}

impl Drop for ColdIndexRemovalStatementGuard {
    fn drop(&mut self) {
        if self.armed {
            self.manager
                .rollback_cold_index_removals_to_checkpoint(self.txn_id, self.checkpoint);
        }
    }
}

#[derive(Clone)]
struct ColdCompositeExactPlan {
    index_name: String,
    declared_column_count: usize,
    columns: Vec<String>,
    column_indices: Vec<usize>,
    values: Vec<Value>,
    conditions: Vec<String>,
}

#[derive(Clone)]
struct ColdCompositeOrderedPlan {
    index_name: String,
    declared_column_count: usize,
    columns: Vec<String>,
    column_indices: Vec<usize>,
    equality_values: Vec<Value>,
    min: Option<(i64, bool)>,
    max: Option<(i64, bool)>,
    conditions: Vec<String>,
    covered_columns: FxHashSet<String>,
}

#[derive(Clone)]
struct ColdExactSetPlan {
    index_name: String,
    declared_column_count: usize,
    column: String,
    values: Vec<Value>,
}

struct ExactIndexCandidates {
    column_index: usize,
    requested_values: ValueSet,
    row_ids: Vec<i64>,
    requires_recheck: bool,
}

mod aggregate;
mod candidates;
mod constraints;
mod contract;
mod mutation;
mod read;

#[cfg(test)]
mod tests;
