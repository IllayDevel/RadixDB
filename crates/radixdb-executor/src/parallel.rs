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

//! Parallel Query Execution
//!
//! This module provides parallel execution strategies for CPU-intensive query operations:
//!
//! - **Parallel Scan + Filter**: Process table rows in parallel chunks with WHERE evaluation
//! - **Parallel Aggregation**: Already implemented in aggregation.rs
//! - **Parallel Join**: Parallel hash join build/probe phases
//!
//! # Architecture
//!
//! The parallel execution model works by:
//! 1. Collecting rows from storage (sequential - storage layer limitation)
//! 2. Splitting rows into chunks for parallel processing
//! 3. Processing each chunk independently using Rayon's work-stealing scheduler
//! 4. Merging results back together
//!
//! # Thresholds
//!
//! Parallelization has overhead, so we only use it when beneficial:
//! - Table scan + filter: 10,000+ rows
//! - Aggregation: 100,000+ rows (already in aggregation.rs)
//! - Hash join: 10,000+ build rows

#[cfg(feature = "parallel")]
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use radixdb_core::value::NULL_VALUE;
use radixdb_core::{CompactArc, CompactVec};
use radixdb_core::{Result, Row, RowVec, Value};
use radixdb_functions::FunctionRegistry;
use radixdb_sql::ast::Expression;

use super::context::{CancellationHandle, ExecutionContext};
use super::expression::ExpressionEval;
#[cfg(feature = "parallel")]
use super::expression::RowFilter;
use super::hash_table::{hash_keys_with, JoinMemoryReservation};
use super::operator::ColumnSource;
use super::operator::{ColumnInfo, Operator, OrderingProperty, RowRef};
use super::utils::{hash_composite_key, verify_composite_key_equality, RetainedRowsBudget};

/// Get the active Rayon worker count.
#[cfg(feature = "parallel")]
#[inline]
fn num_threads() -> usize {
    rayon::current_num_threads()
}

// Re-export JoinType from operators::hash_join - single source of truth
pub use super::operators::hash_join::JoinType;

#[doc(hidden)]
pub static PARALLEL_JOIN_CANCELLATION_OBSERVED: AtomicUsize = AtomicUsize::new(0);

#[inline]
fn check_parallel_cancellation(cancellation: Option<&CancellationHandle>) -> Result<()> {
    if cancellation.is_some_and(CancellationHandle::is_cancelled) {
        PARALLEL_JOIN_CANCELLATION_OBSERVED.fetch_add(1, Ordering::Relaxed);
        Err(radixdb_core::Error::QueryCancelled)
    } else {
        Ok(())
    }
}

// Default thresholds for parallel execution - single source of truth
// These are used by ParallelConfig.
pub const DEFAULT_PARALLEL_FILTER_THRESHOLD: usize = 10_000;
pub const DEFAULT_PARALLEL_JOIN_THRESHOLD: usize = 10_000;
pub const DEFAULT_PARALLEL_CHUNK_SIZE: usize = 2048;
pub const DEFAULT_PARALLEL_JOIN_OUTPUT_BATCH_ROWS: usize = 2048;
pub const DEFAULT_PARALLEL_JOIN_OUTPUT_BATCH_BYTES: usize = 16 * 1024 * 1024;

/// Configuration for parallel execution
#[derive(Clone, Debug)]
pub struct ParallelConfig {
    /// Whether parallel execution is enabled
    pub enabled: bool,
    /// Minimum rows to trigger parallel scan + filter
    pub min_rows_for_parallel_filter: usize,
    /// Minimum build rows to trigger parallel hash join
    pub min_rows_for_parallel_join: usize,
    /// Chunk size for parallel processing (rows per thread task)
    pub chunk_size: usize,
    /// Maximum probe rows retained by one parallel JOIN pull batch.
    pub join_output_batch_rows: usize,
    /// Maximum retained probe-row graph owned by one parallel JOIN pull batch.
    pub join_output_batch_bytes: usize,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_rows_for_parallel_filter: DEFAULT_PARALLEL_FILTER_THRESHOLD,
            min_rows_for_parallel_join: DEFAULT_PARALLEL_JOIN_THRESHOLD,
            // Optimal chunk size balances:
            // - Too small: excessive task scheduling overhead
            // - Too large: poor load balancing if chunks have varying filter selectivity
            // 2048 is a good default that works well with typical L2 cache sizes
            chunk_size: DEFAULT_PARALLEL_CHUNK_SIZE,
            join_output_batch_rows: DEFAULT_PARALLEL_JOIN_OUTPUT_BATCH_ROWS,
            join_output_batch_bytes: DEFAULT_PARALLEL_JOIN_OUTPUT_BATCH_BYTES,
        }
    }
}

impl ParallelConfig {
    /// Check if parallel filter should be used for the given row count
    #[inline]
    pub fn should_parallel_filter(&self, row_count: usize) -> bool {
        #[cfg(any(test, feature = "test-failpoints"))]
        {
            if radixdb_storage::test_failpoints::force_serial_execution() {
                return false;
            }
            if radixdb_storage::test_failpoints::force_parallel_execution() {
                return self.enabled && row_count > 0;
            }
        }
        self.enabled && row_count >= self.min_rows_for_parallel_filter
    }

    /// Check if parallel join should be used for the given build side row count
    #[inline]
    pub fn should_parallel_join(&self, build_rows: usize) -> bool {
        self.enabled && build_rows >= self.min_rows_for_parallel_join
    }
}

/// Parallel filter execution for WHERE clause evaluation
///
/// This function filters rows in parallel by:
/// 1. Splitting rows into chunks
/// 2. Evaluating the WHERE predicate on each chunk in parallel
/// 3. Collecting matching rows from all chunks
///
/// Works with `(i64, Row)` tuples, preserving the row ID throughout filtering.
///
/// # Performance
///
/// For a table with 1M rows and 50% selectivity:
/// - Sequential: ~500ms
/// - Parallel (8 cores): ~80ms (6x speedup)
///
/// The speedup depends on:
/// - Number of CPU cores
/// - Complexity of the WHERE predicate
/// - Selectivity (how many rows pass the filter)
pub fn parallel_filter(
    rows: RowVec,
    filter_expr: &Expression,
    columns: &[String],
    _function_registry: &FunctionRegistry,
    config: &ParallelConfig,
    ctx: &ExecutionContext,
) -> Result<RowVec> {
    #[cfg(feature = "parallel")]
    {
        let row_count = rows.len();
        if config.should_parallel_filter(row_count) {
            #[cfg(any(test, feature = "test-failpoints"))]
            radixdb_storage::test_failpoints::record_execution_path(7);
            // Pre-compile the filter expression once (RowFilter is Send+Sync)
            let columns_vec: Vec<String> = columns.to_vec();
            let filter = RowFilter::new(filter_expr, &columns_vec)?.with_context(ctx);

            // Mark-and-extract: compute a parallel boolean mask (1 byte per row),
            // then single-pass extract kept rows in original order.
            // This avoids materializing an N-sized Option<(i64,Row)> intermediate.
            let mut row_vec: Vec<(i64, Row)> = rows.into_vec();
            let keep: Result<Vec<bool>> = row_vec
                .par_iter()
                .map(|(_, row)| filter.matches_checked(row))
                .collect();
            let keep = keep?;

            // Single-pass extract: move kept rows into result, preserving order
            let kept_count = keep.iter().filter(|&&b| b).count();
            let mut filtered = Vec::with_capacity(kept_count);
            for (i, entry) in row_vec.drain(..).enumerate() {
                if keep[i] {
                    filtered.push(entry);
                }
            }

            // Wrap final result in RowVec (uses main thread's cache)
            return Ok(RowVec::from_vec(filtered));
        }
    }

    // Sequential fallback (always compiled)
    #[cfg(any(test, feature = "test-failpoints"))]
    radixdb_storage::test_failpoints::record_execution_path(6);
    let _ = config; // suppress unused warning when parallel feature is disabled
    sequential_filter(rows, filter_expr, columns, ctx)
}

/// Sequential filter for small datasets or when parallel is disabled
fn sequential_filter(
    rows: RowVec,
    filter_expr: &Expression,
    columns: &[String],
    ctx: &ExecutionContext,
) -> Result<RowVec> {
    let columns_vec: Vec<String> = columns.to_vec();
    let mut eval = ExpressionEval::compile(filter_expr, &columns_vec)?.with_context(ctx);

    let mut result = RowVec::with_capacity(rows.len());
    for (id, row) in rows {
        if eval.eval_bool_checked(&row)? {
            result.push((id, row));
        }
    }
    Ok(result)
}

// ============================================================================
// Parallel Hash Join
// ============================================================================

const EMPTY_PARALLEL_BUCKET: u32 = u32::MAX;

/// One fixed-width parallel hash entry. The row index is its position in the
/// entry array, so only the full hash and next link must be retained.
#[repr(C)]
struct ParallelHashEntry {
    hash: AtomicU64,
    next: AtomicU32,
}

impl ParallelHashEntry {
    fn empty() -> Self {
        Self {
            hash: AtomicU64::new(0),
            next: AtomicU32::new(EMPTY_PARALLEL_BUCKET),
        }
    }
}

/// Build side match tracking using atomic operations
///
/// Uses Vec<AtomicBool> for both sequential and parallel execution to ensure
/// the type is Sync and can be safely shared across threads. The atomic overhead
/// in sequential mode is minimal (~1-2 nanoseconds per operation).
struct BuildMatchedTracker {
    matched: Vec<AtomicBool>,
}

impl BuildMatchedTracker {
    /// Create a new tracker
    fn new(size: usize) -> Self {
        BuildMatchedTracker {
            matched: (0..size).map(|_| AtomicBool::new(false)).collect(),
        }
    }

    /// Mark a build row as matched
    ///
    /// Uses Release ordering in parallel mode for cross-thread visibility.
    /// In sequential mode, Relaxed would suffice, but we use Release uniformly
    /// for simplicity and the overhead is negligible.
    #[inline]
    fn mark_matched(&self, idx: usize) {
        self.matched[idx].store(true, Ordering::Release);
    }

    /// Check if a build row was matched
    ///
    /// Uses Acquire ordering to synchronize with Release stores from probe phase.
    #[inline]
    fn was_matched(&self, idx: usize) -> bool {
        self.matched[idx].load(Ordering::Acquire)
    }
}

/// Result of parallel hash table build phase
struct ParallelHashTable {
    bucket_heads: Vec<AtomicU32>,
    entries: Vec<ParallelHashEntry>,
    bucket_mask: u64,
}

impl ParallelHashTable {
    fn retained_bytes(row_count: usize) -> Option<usize> {
        crate::hash_table::JoinHashTable::estimated_retained_bytes(row_count)
    }

    fn with_capacity(row_count: usize, max_bytes: usize) -> Result<Self> {
        let retained_bytes = Self::retained_bytes(row_count).ok_or_else(|| {
            radixdb_core::Error::invalid_argument(
                "parallel join hash state exceeds its addressable row range",
            )
        })?;
        if retained_bytes > max_bytes {
            return Err(radixdb_core::Error::invalid_argument(format!(
                "parallel join hash state exceeds memory budget ({retained_bytes}/{max_bytes} bytes)"
            )));
        }

        let entry_bytes = row_count
            .checked_mul(std::mem::size_of::<ParallelHashEntry>())
            .ok_or_else(|| {
                radixdb_core::Error::invalid_argument(
                    "parallel join hash entry size exceeds addressable memory",
                )
            })?;
        let bucket_bytes = retained_bytes.checked_sub(entry_bytes).ok_or_else(|| {
            radixdb_core::Error::internal("parallel join hash retained-size contract mismatch")
        })?;
        let bucket_count = bucket_bytes / std::mem::size_of::<AtomicU32>();
        let bucket_heads = (0..bucket_count)
            .map(|_| AtomicU32::new(EMPTY_PARALLEL_BUCKET))
            .collect();
        let entries = (0..row_count).map(|_| ParallelHashEntry::empty()).collect();

        Ok(Self {
            bucket_heads,
            entries,
            bucket_mask: (bucket_count - 1) as u64,
        })
    }

    #[inline]
    fn insert(&self, hash: u64, row_idx: usize) {
        let row_idx = row_idx as u32;
        let bucket = (hash & self.bucket_mask) as usize;
        let previous = self.bucket_heads[bucket].swap(row_idx, Ordering::Relaxed);
        let entry = &self.entries[row_idx as usize];
        entry.hash.store(hash, Ordering::Relaxed);
        entry.next.store(previous, Ordering::Relaxed);
    }

    #[inline]
    fn for_each_match(&self, key: &u64, mut visit: impl FnMut(usize)) {
        let mut entry_idx =
            self.bucket_heads[(*key & self.bucket_mask) as usize].load(Ordering::Relaxed);
        while entry_idx != EMPTY_PARALLEL_BUCKET {
            let entry = &self.entries[entry_idx as usize];
            if entry.hash.load(Ordering::Relaxed) == *key {
                visit(entry_idx as usize);
            }
            entry_idx = entry.next.load(Ordering::Relaxed);
        }
    }

    #[inline]
    fn bucket_head(&self, hash: u64) -> u32 {
        self.bucket_heads[(hash & self.bucket_mask) as usize].load(Ordering::Relaxed)
    }
}

pub fn parallel_join_state_retained_bytes(
    build_rows: usize,
    track_build_matches: bool,
) -> Option<usize> {
    let hash_bytes = ParallelHashTable::retained_bytes(build_rows)?;
    let tracker_bytes = if track_build_matches {
        build_rows.checked_mul(std::mem::size_of::<AtomicBool>())?
    } else {
        0
    };
    hash_bytes.checked_add(tracker_bytes)
}

#[derive(Debug)]
struct ParallelProbeState {
    row: Option<RowRef>,
    hash: u64,
    next_entry: u32,
    matched: bool,
    done: bool,
}

#[derive(Debug, Clone, Copy)]
enum ParallelProbeCandidate {
    Match {
        probe_index: usize,
        build_index: usize,
    },
    UnmatchedProbe {
        probe_index: usize,
    },
}

#[derive(Debug)]
struct ParallelProbeRound {
    candidate: Option<ParallelProbeCandidate>,
    examined: u64,
}

/// Pull-based parallel hash join.
///
/// The build table is parallel and immutable. Probe work is admitted in a
/// bounded batch, and each Rayon round advances every live probe cursor by at
/// most one output match. The round therefore retains only O(batch rows)
/// fixed-width candidate descriptors, never a full JOIN result or an
/// unbounded per-key fan-out vector.
pub struct ParallelHashJoinOperator {
    probe: Box<dyn Operator>,
    build_rows: CompactArc<Vec<Row>>,
    build_key_indices: Vec<usize>,
    probe_key_indices: Vec<usize>,
    join_type: JoinType,
    build_is_left: bool,
    left_col_count: usize,
    right_col_count: usize,
    schema: Vec<ColumnInfo>,
    projection_columns: CompactArc<[ColumnSource]>,
    config: ParallelConfig,
    cancellation: CancellationHandle,
    hash_table: Option<ParallelHashTable>,
    build_matched: Option<BuildMatchedTracker>,
    probe_batch: Vec<ParallelProbeState>,
    ready: VecDeque<ParallelProbeCandidate>,
    probe_batch_budget: RetainedRowsBudget,
    probe_exhausted: bool,
    unmatched_build_index: usize,
    hash_state_bytes: usize,
    _hash_state_reservation: Option<JoinMemoryReservation>,
    _batch_reservation: Option<JoinMemoryReservation>,
    opened: bool,
    closed: bool,
    observed_probe_rows: u64,
    observed_candidate_rows: u64,
    #[cfg(test)]
    peak_probe_batch_rows: usize,
}

#[allow(clippy::too_many_arguments)]
impl ParallelHashJoinOperator {
    pub fn new(
        probe: Box<dyn Operator>,
        build_rows: CompactArc<Vec<Row>>,
        build_key_indices: Vec<usize>,
        probe_key_indices: Vec<usize>,
        join_type: JoinType,
        build_is_left: bool,
        left_col_count: usize,
        right_col_count: usize,
        columns: Vec<String>,
        projection: Option<Vec<ColumnSource>>,
        config: ParallelConfig,
        cancellation: CancellationHandle,
        hash_state_bytes: usize,
        hash_state_reservation: JoinMemoryReservation,
        batch_reservation: JoinMemoryReservation,
    ) -> Result<Self> {
        let projection = projection.unwrap_or_else(|| {
            let mut sources = Vec::with_capacity(left_col_count + right_col_count);
            sources.extend((0..left_col_count).map(ColumnSource::Outer));
            sources.extend((0..right_col_count).map(ColumnSource::Inner));
            sources
        });
        super::operator::JoinProjection {
            columns: projection.clone(),
        }
        .validate(left_col_count, right_col_count, columns.len())?;

        let batch_rows = config.join_output_batch_rows.max(1);
        let batch_bytes = config.join_output_batch_bytes.max(1);
        Ok(Self {
            probe,
            build_rows,
            build_key_indices,
            probe_key_indices,
            join_type,
            build_is_left,
            left_col_count,
            right_col_count,
            schema: columns.into_iter().map(ColumnInfo::new).collect(),
            projection_columns: CompactArc::from(projection),
            config,
            cancellation,
            hash_table: None,
            build_matched: None,
            probe_batch: Vec::with_capacity(batch_rows),
            ready: VecDeque::with_capacity(batch_rows),
            probe_batch_budget: RetainedRowsBudget::with_limits(
                "parallel JOIN probe batch",
                batch_rows,
                batch_bytes,
            ),
            probe_exhausted: false,
            unmatched_build_index: 0,
            hash_state_bytes,
            _hash_state_reservation: Some(hash_state_reservation),
            _batch_reservation: Some(batch_reservation),
            opened: false,
            closed: false,
            observed_probe_rows: 0,
            observed_candidate_rows: 0,
            #[cfg(test)]
            peak_probe_batch_rows: 0,
        })
    }

    pub fn observed_probe_rows(&self) -> u64 {
        self.observed_probe_rows
    }

    pub fn observed_candidate_rows(&self) -> u64 {
        self.observed_candidate_rows
    }

    fn fill_probe_batch(&mut self) -> Result<()> {
        debug_assert!(self.probe_batch.is_empty());
        while self.probe_batch.len() < self.config.join_output_batch_rows.max(1) {
            check_parallel_cancellation(Some(&self.cancellation))?;
            let Some(row) = self.probe.next()? else {
                self.probe_exhausted = true;
                break;
            };
            self.probe_batch_budget
                .admit_estimated_bytes(row.estimated_retained_bytes())?;
            let hash = hash_keys_with(&self.probe_key_indices, |index| row.get(index));
            let next_entry = self
                .hash_table
                .as_ref()
                .expect("opened parallel JOIN must own a hash table")
                .bucket_head(hash);
            self.probe_batch.push(ParallelProbeState {
                row: Some(row),
                hash,
                next_entry,
                matched: false,
                done: false,
            });
            #[cfg(test)]
            {
                self.peak_probe_batch_rows = self.peak_probe_batch_rows.max(self.probe_batch.len());
            }
            self.observed_probe_rows = self.observed_probe_rows.saturating_add(1);
        }
        Ok(())
    }

    fn advance_probe_state(
        state: &mut ParallelProbeState,
        probe_index: usize,
        hash_table: &ParallelHashTable,
        build_rows: &[Row],
        probe_key_indices: &[usize],
        build_key_indices: &[usize],
        build_matched: Option<&BuildMatchedTracker>,
        needs_unmatched_probe: bool,
        cancellation: &CancellationHandle,
    ) -> ParallelProbeRound {
        if state.done || cancellation.is_cancelled() {
            return ParallelProbeRound {
                candidate: None,
                examined: 0,
            };
        }

        let probe_row = state
            .row
            .as_ref()
            .expect("live parallel probe state must retain its row");
        let mut examined = 0_u64;
        while state.next_entry != EMPTY_PARALLEL_BUCKET {
            let build_index = state.next_entry as usize;
            let entry = &hash_table.entries[build_index];
            state.next_entry = entry.next.load(Ordering::Relaxed);
            examined = examined.saturating_add(1);
            if entry.hash.load(Ordering::Relaxed) != state.hash {
                continue;
            }
            let build_row = &build_rows[build_index];
            let matches = probe_key_indices.iter().zip(build_key_indices.iter()).all(
                |(&probe_index, &build_index)| {
                    let (Some(probe_value), Some(build_value)) =
                        (probe_row.get(probe_index), build_row.get(build_index))
                    else {
                        return false;
                    };
                    !probe_value.is_null() && !build_value.is_null() && probe_value == build_value
                },
            );
            if matches {
                state.matched = true;
                if let Some(tracker) = build_matched {
                    tracker.mark_matched(build_index);
                }
                if state.next_entry == EMPTY_PARALLEL_BUCKET {
                    state.done = true;
                }
                return ParallelProbeRound {
                    candidate: Some(ParallelProbeCandidate::Match {
                        probe_index,
                        build_index,
                    }),
                    examined,
                };
            }
        }

        state.done = true;
        ParallelProbeRound {
            candidate: (!state.matched && needs_unmatched_probe)
                .then_some(ParallelProbeCandidate::UnmatchedProbe { probe_index }),
            examined,
        }
    }

    fn run_probe_round(&mut self) -> Result<()> {
        check_parallel_cancellation(Some(&self.cancellation))?;
        let hash_table = self
            .hash_table
            .as_ref()
            .expect("opened parallel JOIN must own a hash table");
        let build_rows = self.build_rows.as_slice();
        let probe_key_indices = self.probe_key_indices.as_slice();
        let build_key_indices = self.build_key_indices.as_slice();
        let build_matched = self.build_matched.as_ref();
        let needs_unmatched_probe = self.join_type.needs_unmatched_probe(self.build_is_left);
        let cancellation = &self.cancellation;

        #[cfg(feature = "parallel")]
        let rounds: Vec<ParallelProbeRound> = self
            .probe_batch
            .par_iter_mut()
            .enumerate()
            .map(|(probe_index, state)| {
                Self::advance_probe_state(
                    state,
                    probe_index,
                    hash_table,
                    build_rows,
                    probe_key_indices,
                    build_key_indices,
                    build_matched,
                    needs_unmatched_probe,
                    cancellation,
                )
            })
            .collect();
        #[cfg(not(feature = "parallel"))]
        let rounds: Vec<ParallelProbeRound> = self
            .probe_batch
            .iter_mut()
            .enumerate()
            .map(|(probe_index, state)| {
                Self::advance_probe_state(
                    state,
                    probe_index,
                    hash_table,
                    build_rows,
                    probe_key_indices,
                    build_key_indices,
                    build_matched,
                    needs_unmatched_probe,
                    cancellation,
                )
            })
            .collect();

        check_parallel_cancellation(Some(&self.cancellation))?;
        for round in rounds {
            self.observed_candidate_rows =
                self.observed_candidate_rows.saturating_add(round.examined);
            if let Some(candidate) = round.candidate {
                self.ready.push_back(candidate);
            }
        }
        Ok(())
    }

    fn projected_row(&mut self, candidate: ParallelProbeCandidate) -> RowRef {
        match candidate {
            ParallelProbeCandidate::Match {
                probe_index,
                build_index,
            } => {
                let state = &mut self.probe_batch[probe_index];
                let probe = if state.done {
                    state
                        .row
                        .take()
                        .expect("completed parallel probe must retain its result row")
                } else {
                    state
                        .row
                        .as_ref()
                        .expect("live parallel probe must retain its row")
                        .clone()
                };
                let build = RowRef::shared(CompactArc::clone(&self.build_rows), build_index);
                let (left, right) = if self.build_is_left {
                    (build, probe)
                } else {
                    (probe, build)
                };
                RowRef::projected(left, right, CompactArc::clone(&self.projection_columns))
            }
            ParallelProbeCandidate::UnmatchedProbe { probe_index } => {
                let probe = self.probe_batch[probe_index]
                    .row
                    .take()
                    .expect("unmatched parallel probe must retain its row");
                let build_width = if self.build_is_left {
                    self.left_col_count
                } else {
                    self.right_col_count
                };
                let null_build = RowRef::owned(Row::from_values(vec![NULL_VALUE; build_width]));
                let (left, right) = if self.build_is_left {
                    (null_build, probe)
                } else {
                    (probe, null_build)
                };
                RowRef::projected(left, right, CompactArc::clone(&self.projection_columns))
            }
        }
    }

    fn next_unmatched_build(&mut self) -> Result<Option<RowRef>> {
        let Some(tracker) = self.build_matched.as_ref() else {
            return Ok(None);
        };
        while self.unmatched_build_index < self.build_rows.len() {
            if self.unmatched_build_index & 0xff == 0 {
                check_parallel_cancellation(Some(&self.cancellation))?;
            }
            let build_index = self.unmatched_build_index;
            self.unmatched_build_index += 1;
            if tracker.was_matched(build_index) {
                continue;
            }
            let build = RowRef::shared(CompactArc::clone(&self.build_rows), build_index);
            let probe_width = if self.build_is_left {
                self.right_col_count
            } else {
                self.left_col_count
            };
            let null_probe = RowRef::owned(Row::from_values(vec![NULL_VALUE; probe_width]));
            let (left, right) = if self.build_is_left {
                (build, null_probe)
            } else {
                (null_probe, build)
            };
            return Ok(Some(RowRef::projected(
                left,
                right,
                CompactArc::clone(&self.projection_columns),
            )));
        }
        Ok(None)
    }

    fn clear_probe_batch(&mut self) {
        self.probe_batch.clear();
        self.ready.clear();
        self.probe_batch_budget = RetainedRowsBudget::with_limits(
            "parallel JOIN probe batch",
            self.config.join_output_batch_rows.max(1),
            self.config.join_output_batch_bytes.max(1),
        );
    }
}

impl Operator for ParallelHashJoinOperator {
    fn open(&mut self) -> Result<()> {
        if self.opened {
            return Ok(());
        }
        check_parallel_cancellation(Some(&self.cancellation))?;
        let tracker_bytes = if self.join_type.needs_unmatched_build(self.build_is_left) {
            self.build_rows
                .len()
                .checked_mul(std::mem::size_of::<AtomicBool>())
                .ok_or_else(|| {
                    radixdb_core::Error::invalid_argument(
                        "parallel join match tracker exceeds addressable memory",
                    )
                })?
        } else {
            0
        };
        self.hash_table = Some(parallel_hash_build_inner(
            &self.build_rows,
            &self.build_key_indices,
            &self.config,
            Some(&self.cancellation),
            self.hash_state_bytes.saturating_sub(tracker_bytes),
        )?);
        if tracker_bytes > 0 {
            self.build_matched = Some(BuildMatchedTracker::new(self.build_rows.len()));
        }
        if let Err(error) = self.probe.open() {
            self.hash_table = None;
            self.build_matched = None;
            return Err(error);
        }
        self.opened = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<RowRef>> {
        if !self.opened || self.closed {
            return Ok(None);
        }
        loop {
            check_parallel_cancellation(Some(&self.cancellation))?;
            if let Some(candidate) = self.ready.pop_front() {
                return Ok(Some(self.projected_row(candidate)));
            }

            if !self.probe_batch.is_empty() {
                if self.probe_batch.iter().all(|state| state.done) {
                    self.clear_probe_batch();
                } else {
                    self.run_probe_round()?;
                    continue;
                }
            }

            if !self.probe_exhausted {
                self.fill_probe_batch()?;
                if !self.probe_batch.is_empty() {
                    continue;
                }
            }

            return self.next_unmatched_build();
        }
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        // A peer may disconnect while the cursor is idle between FETCH calls.
        // Observe that cancellation at the resource-release boundary as well as
        // inside active build/probe loops.
        let _ = check_parallel_cancellation(Some(&self.cancellation));
        self.closed = true;
        self.clear_probe_batch();
        self.hash_table = None;
        self.build_matched = None;
        self._hash_state_reservation = None;
        self._batch_reservation = None;
        self.probe.close()
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn estimated_rows(&self) -> Option<usize> {
        None
    }

    fn ordering(&self) -> OrderingProperty {
        OrderingProperty::Unknown
    }

    fn name(&self) -> &str {
        "ParallelHashJoin"
    }
}

#[cfg(test)]
pub fn parallel_join_state_fits_budget(
    build_rows: usize,
    track_build_matches: bool,
    max_bytes: usize,
) -> bool {
    parallel_join_state_retained_bytes(build_rows, track_build_matches)
        .is_some_and(|bytes| bytes <= max_bytes)
}

fn parallel_hash_build_inner(
    build_rows: &[Row],
    key_indices: &[usize],
    config: &ParallelConfig,
    cancellation: Option<&CancellationHandle>,
    max_bytes: usize,
) -> Result<ParallelHashTable> {
    check_parallel_cancellation(cancellation)?;
    let row_count = build_rows.len();
    let table = ParallelHashTable::with_capacity(row_count, max_bytes)?;

    #[cfg(feature = "parallel")]
    if config.should_parallel_join(row_count) {
        let n_threads = num_threads();
        let chunk_size = config.chunk_size.max(row_count / n_threads).max(1000);

        // Every row owns one fixed entry while bucket heads are published with
        // atomic swaps. No per-key Vec or allocator growth occurs during build.
        build_rows
            .par_chunks(chunk_size)
            .enumerate()
            .for_each(|(chunk_idx, chunk)| {
                if cancellation.is_some_and(CancellationHandle::is_cancelled) {
                    return;
                }
                let base_idx = chunk_idx * chunk_size;
                for (local_idx, row) in chunk.iter().enumerate() {
                    if local_idx & 0xff == 0
                        && cancellation.is_some_and(CancellationHandle::is_cancelled)
                    {
                        return;
                    }
                    // SAFETY: Check for index overflow (would require ~18 quintillion rows on 64-bit)
                    // Use debug_assert for zero runtime cost in release builds
                    debug_assert!(
                        base_idx.checked_add(local_idx).is_some(),
                        "Index overflow in parallel hash build: base_idx={} + local_idx={}",
                        base_idx,
                        local_idx
                    );
                    let global_idx = base_idx + local_idx;
                    let hash = hash_composite_key(row, key_indices);
                    table.insert(hash, global_idx);
                }
            });

        check_parallel_cancellation(cancellation)?;
        return Ok(table);
    }

    // Sequential build retains the same fixed representation, so switching the
    // worker count cannot change admission or memory shape.
    let _ = config; // suppress unused warning when parallel feature is disabled
    for (idx, row) in build_rows.iter().enumerate() {
        if idx & 0xff == 0 {
            check_parallel_cancellation(cancellation)?;
        }
        let hash = hash_composite_key(row, key_indices);
        table.insert(hash, idx);
    }
    Ok(table)
}

/// Hash a row using specific key column indices.
#[inline]
fn hash_row_by_keys(row: &Row, key_indices: &[usize]) -> u64 {
    hash_composite_key(row, key_indices)
}

/// Verify that two rows match on their respective key columns.
#[inline]
fn verify_key_match(
    probe_row: &Row,
    build_row: &Row,
    probe_key_indices: &[usize],
    build_key_indices: &[usize],
) -> bool {
    verify_composite_key_equality(probe_row, build_row, probe_key_indices, build_key_indices)
}

// JoinType is imported from operators::hash_join - single source of truth

/// Parallel hash join result
pub struct ParallelJoinResult {
    /// The joined rows
    pub rows: Vec<Row>,
}

/// Sequential probe phase for hash join (used when parallel is disabled or dataset is small)
#[allow(clippy::too_many_arguments)]
fn sequential_probe(
    probe_rows: &[Row],
    build_rows: &[Row],
    hash_table: &ParallelHashTable,
    probe_key_indices: &[usize],
    build_key_indices: &[usize],
    join_type: &JoinType,
    probe_col_count: usize,
    build_col_count: usize,
    swapped: bool,
    projection: Option<&[ColumnSource]>,
    build_matched: &Option<BuildMatchedTracker>,
    cancellation: Option<&CancellationHandle>,
) -> Result<(Vec<Row>, Vec<Row>)> {
    let mut matched_rows = Vec::new();
    let needs_unmatched_probe = join_type.needs_unmatched_probe(swapped);

    for (probe_index, probe_row) in probe_rows.iter().enumerate() {
        if probe_index & 0xff == 0 {
            check_parallel_cancellation(cancellation)?;
        }
        let hash = hash_row_by_keys(probe_row, probe_key_indices);
        let mut matched = false;

        hash_table.for_each_match(&hash, |build_idx| {
            let build_row = &build_rows[build_idx];
            if verify_key_match(probe_row, build_row, probe_key_indices, build_key_indices) {
                matched = true;
                if let Some(ref tracker) = build_matched {
                    tracker.mark_matched(build_idx);
                }
                let combined = combine_join_rows(
                    probe_row,
                    build_row,
                    probe_col_count,
                    build_col_count,
                    swapped,
                    projection,
                );
                matched_rows.push(Row::from_compact_vec(combined));
            }
        });

        if !matched && needs_unmatched_probe {
            let values = combine_with_nulls(
                probe_row,
                probe_col_count,
                build_col_count,
                swapped,
                projection,
            );
            matched_rows.push(Row::from_compact_vec(values));
        }
    }

    Ok((matched_rows, Vec::new()))
}

/// Test-only convenience wrapper around the cancellable production owner.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
fn parallel_hash_join(
    probe_rows: &[Row],
    build_rows: &[Row],
    probe_key_indices: &[usize],
    build_key_indices: &[usize],
    join_type: JoinType,
    probe_col_count: usize,
    build_col_count: usize,
    swapped: bool,
    config: &ParallelConfig,
) -> ParallelJoinResult {
    parallel_hash_join_inner(
        probe_rows,
        build_rows,
        probe_key_indices,
        build_key_indices,
        join_type,
        probe_col_count,
        build_col_count,
        swapped,
        None,
        config,
        None,
        usize::MAX,
    )
    .expect("uncancellable parallel hash join")
}

#[allow(clippy::too_many_arguments)]
pub fn parallel_hash_join_cancellable(
    probe_rows: &[Row],
    build_rows: &[Row],
    probe_key_indices: &[usize],
    build_key_indices: &[usize],
    join_type: JoinType,
    probe_col_count: usize,
    build_col_count: usize,
    swapped: bool,
    projection: Option<&[ColumnSource]>,
    config: &ParallelConfig,
    cancellation: &CancellationHandle,
    max_state_bytes: usize,
) -> Result<ParallelJoinResult> {
    parallel_hash_join_inner(
        probe_rows,
        build_rows,
        probe_key_indices,
        build_key_indices,
        join_type,
        probe_col_count,
        build_col_count,
        swapped,
        projection,
        config,
        Some(cancellation),
        max_state_bytes,
    )
}

#[allow(clippy::too_many_arguments)]
fn parallel_hash_join_inner(
    probe_rows: &[Row],
    build_rows: &[Row],
    probe_key_indices: &[usize],
    build_key_indices: &[usize],
    join_type: JoinType,
    probe_col_count: usize,
    build_col_count: usize,
    swapped: bool,
    projection: Option<&[ColumnSource]>,
    config: &ParallelConfig,
    cancellation: Option<&CancellationHandle>,
    max_state_bytes: usize,
) -> Result<ParallelJoinResult> {
    check_parallel_cancellation(cancellation)?;
    #[cfg(feature = "parallel")]
    let probe_count = probe_rows.len();
    let build_count = build_rows.len();

    // Determine if we should use parallel execution
    #[cfg(feature = "parallel")]
    let use_parallel =
        config.should_parallel_join(build_count) || config.should_parallel_join(probe_count);

    // For OUTER joins, we need to track which build rows were matched
    // Uses Vec<AtomicBool> for both sequential and parallel execution (minimal overhead)
    let track_build_matches = join_type.needs_unmatched_build(swapped);
    let retained_bytes = parallel_join_state_retained_bytes(build_count, track_build_matches)
        .ok_or_else(|| {
            radixdb_core::Error::invalid_argument(
                "parallel join state exceeds its addressable memory range",
            )
        })?;
    if retained_bytes > max_state_bytes {
        return Err(radixdb_core::Error::invalid_argument(format!(
            "parallel join state exceeds memory budget ({retained_bytes}/{max_state_bytes} bytes)"
        )));
    }
    let tracker_bytes = if track_build_matches {
        build_count
            .checked_mul(std::mem::size_of::<AtomicBool>())
            .ok_or_else(|| {
                radixdb_core::Error::invalid_argument(
                    "parallel join match tracker exceeds addressable memory",
                )
            })?
    } else {
        0
    };

    // Build phase: one fixed-width table whose allocation is admitted together
    // with the optional OUTER match tracker.
    let hash_table = parallel_hash_build_inner(
        build_rows,
        build_key_indices,
        config,
        cancellation,
        max_state_bytes.saturating_sub(tracker_bytes),
    )?;

    let build_matched: Option<BuildMatchedTracker> = if track_build_matches {
        Some(BuildMatchedTracker::new(build_count))
    } else {
        None
    };

    // Probe phase
    #[allow(unused_variables)]
    let (matched_rows, unmatched_probe_rows) = {
        #[cfg(feature = "parallel")]
        {
            if use_parallel && join_type == JoinType::Inner {
                // For INNER joins, we can fully parallelize the probe phase
                let matches: Vec<Row> = probe_rows
                    .par_chunks(config.chunk_size.max(1000))
                    .flat_map(|chunk| {
                        let mut local_results = Vec::new();
                        for (probe_index, probe_row) in chunk.iter().enumerate() {
                            if probe_index & 0xff == 0
                                && cancellation.is_some_and(CancellationHandle::is_cancelled)
                            {
                                break;
                            }
                            let hash = hash_row_by_keys(probe_row, probe_key_indices);
                            hash_table.for_each_match(&hash, |build_idx| {
                                let build_row = &build_rows[build_idx];
                                if verify_key_match(
                                    probe_row,
                                    build_row,
                                    probe_key_indices,
                                    build_key_indices,
                                ) {
                                    let combined = combine_join_rows(
                                        probe_row,
                                        build_row,
                                        probe_col_count,
                                        build_col_count,
                                        swapped,
                                        projection,
                                    );
                                    local_results.push(Row::from_compact_vec(combined));
                                }
                            });
                        }
                        local_results
                    })
                    .collect();
                check_parallel_cancellation(cancellation)?;
                (matches, Vec::new())
            } else if use_parallel {
                // For OUTER joins with parallel execution, use atomic tracking for build side
                // and collect unmatched probe rows directly in parallel
                let needs_unmatched_probe = join_type.needs_unmatched_probe(swapped);

                // Each chunk returns: (matched_rows, unmatched_probe_rows)
                let chunk_results: Vec<(Vec<Row>, Vec<Row>)> = probe_rows
                    .par_chunks(config.chunk_size.max(1000))
                    .map(|chunk| {
                        let mut matched_results = Vec::new();
                        let mut unmatched_results = Vec::new();

                        for (probe_index, probe_row) in chunk.iter().enumerate() {
                            if probe_index & 0xff == 0
                                && cancellation.is_some_and(CancellationHandle::is_cancelled)
                            {
                                break;
                            }
                            let mut matched = false;
                            let hash = hash_row_by_keys(probe_row, probe_key_indices);

                            hash_table.for_each_match(&hash, |build_idx| {
                                let build_row = &build_rows[build_idx];
                                if verify_key_match(
                                    probe_row,
                                    build_row,
                                    probe_key_indices,
                                    build_key_indices,
                                ) {
                                    matched = true;
                                    if let Some(ref tracker) = build_matched {
                                        tracker.mark_matched(build_idx);
                                    }
                                    let combined = combine_join_rows(
                                        probe_row,
                                        build_row,
                                        probe_col_count,
                                        build_col_count,
                                        swapped,
                                        projection,
                                    );
                                    matched_results.push(Row::from_compact_vec(combined));
                                }
                            });

                            if !matched && needs_unmatched_probe {
                                let values = combine_with_nulls(
                                    probe_row,
                                    probe_col_count,
                                    build_col_count,
                                    swapped,
                                    projection,
                                );
                                unmatched_results.push(Row::from_compact_vec(values));
                            }
                        }

                        (matched_results, unmatched_results)
                    })
                    .collect();
                check_parallel_cancellation(cancellation)?;

                let total_matched: usize = chunk_results.iter().map(|(m, _)| m.len()).sum();
                let total_unmatched: usize = chunk_results.iter().map(|(_, u)| u.len()).sum();

                let mut matched_rows = Vec::with_capacity(total_matched);
                let mut unmatched_rows = Vec::with_capacity(total_unmatched);

                for (matched, unmatched) in chunk_results {
                    matched_rows.extend(matched);
                    unmatched_rows.extend(unmatched);
                }

                // CRITICAL: Acquire fence for cross-thread visibility of build_matched[] writes.
                // Parallel probe stores use Release ordering; this fence ensures all stores
                // are visible before the sequential scan of unmatched build rows below.
                std::sync::atomic::fence(Ordering::Acquire);

                (matched_rows, unmatched_rows)
            } else {
                // Sequential execution for small datasets
                sequential_probe(
                    probe_rows,
                    build_rows,
                    &hash_table,
                    probe_key_indices,
                    build_key_indices,
                    &join_type,
                    probe_col_count,
                    build_col_count,
                    swapped,
                    projection,
                    &build_matched,
                    cancellation,
                )?
            }
        }
        #[cfg(not(feature = "parallel"))]
        {
            sequential_probe(
                probe_rows,
                build_rows,
                &hash_table,
                probe_key_indices,
                build_key_indices,
                &join_type,
                probe_col_count,
                build_col_count,
                swapped,
                projection,
                &build_matched,
                cancellation,
            )?
        }
    };

    let mut result_rows = matched_rows;
    result_rows.extend(unmatched_probe_rows);

    // Handle unmatched build rows for OUTER joins
    // The Acquire fence at line 794 ensures all parallel stores are visible
    if let Some(ref tracker) = build_matched {
        for (build_idx, build_row) in build_rows.iter().enumerate() {
            if build_idx & 0xff == 0 {
                check_parallel_cancellation(cancellation)?;
            }
            if !tracker.was_matched(build_idx) {
                let values = combine_build_with_nulls(
                    build_row,
                    build_col_count,
                    probe_col_count,
                    swapped,
                    projection,
                );
                result_rows.push(Row::from_compact_vec(values));
            }
        }
    }

    Ok(ParallelJoinResult { rows: result_rows })
}

/// Combine probe and build rows into a single row, respecting swap order
#[inline]
fn combine_join_rows(
    probe_row: &Row,
    build_row: &Row,
    probe_col_count: usize,
    build_col_count: usize,
    swapped: bool,
    projection: Option<&[ColumnSource]>,
) -> CompactVec<Value> {
    if let Some(projection) = projection {
        let (left, right) = if swapped {
            (build_row, probe_row)
        } else {
            (probe_row, build_row)
        };
        return project_join_rows(Some(left), Some(right), projection);
    }
    // Use CompactVec directly to avoid Vec→CompactVec conversion in Row::from_values
    let mut combined: CompactVec<Value> =
        CompactVec::with_capacity(probe_col_count + build_col_count);
    if swapped {
        // Build was originally left, probe was originally right
        for i in 0..build_col_count {
            combined.push(build_row.get(i).cloned().unwrap_or(NULL_VALUE));
        }
        for i in 0..probe_col_count {
            combined.push(probe_row.get(i).cloned().unwrap_or(NULL_VALUE));
        }
    } else {
        // Probe is left, build is right
        for i in 0..probe_col_count {
            combined.push(probe_row.get(i).cloned().unwrap_or(NULL_VALUE));
        }
        for i in 0..build_col_count {
            combined.push(build_row.get(i).cloned().unwrap_or(NULL_VALUE));
        }
    }
    combined
}

/// Combine probe row with NULLs for unmatched probe side in OUTER joins
#[inline]
fn combine_with_nulls(
    probe_row: &Row,
    probe_col_count: usize,
    build_col_count: usize,
    swapped: bool,
    projection: Option<&[ColumnSource]>,
) -> CompactVec<Value> {
    if let Some(projection) = projection {
        let (left, right) = if swapped {
            (None, Some(probe_row))
        } else {
            (Some(probe_row), None)
        };
        return project_join_rows(left, right, projection);
    }
    // Use CompactVec directly to avoid Vec→CompactVec conversion in Row::from_values
    let mut combined: CompactVec<Value> =
        CompactVec::with_capacity(probe_col_count + build_col_count);
    if swapped {
        // Build (left) is NULL, probe (right) has values
        combined.extend(std::iter::repeat_n(NULL_VALUE, build_col_count));
        for i in 0..probe_col_count {
            combined.push(probe_row.get(i).cloned().unwrap_or(NULL_VALUE));
        }
    } else {
        // Probe (left) has values, build (right) is NULL
        for i in 0..probe_col_count {
            combined.push(probe_row.get(i).cloned().unwrap_or(NULL_VALUE));
        }
        combined.extend(std::iter::repeat_n(NULL_VALUE, build_col_count));
    }
    combined
}

/// Combine build row with NULLs for unmatched build side in OUTER joins
#[inline]
fn combine_build_with_nulls(
    build_row: &Row,
    build_col_count: usize,
    probe_col_count: usize,
    swapped: bool,
    projection: Option<&[ColumnSource]>,
) -> CompactVec<Value> {
    if let Some(projection) = projection {
        let (left, right) = if swapped {
            (Some(build_row), None)
        } else {
            (None, Some(build_row))
        };
        return project_join_rows(left, right, projection);
    }
    // Use CompactVec directly to avoid Vec→CompactVec conversion in Row::from_values
    let mut combined: CompactVec<Value> =
        CompactVec::with_capacity(probe_col_count + build_col_count);
    if swapped {
        // Build (left) has values, probe (right) is NULL
        for i in 0..build_col_count {
            combined.push(build_row.get(i).cloned().unwrap_or(NULL_VALUE));
        }
        combined.extend(std::iter::repeat_n(NULL_VALUE, probe_col_count));
    } else {
        // Probe (left) is NULL, build (right) has values
        combined.extend(std::iter::repeat_n(NULL_VALUE, probe_col_count));
        for i in 0..build_col_count {
            combined.push(build_row.get(i).cloned().unwrap_or(NULL_VALUE));
        }
    }
    combined
}

#[inline]
fn project_join_rows(
    left: Option<&Row>,
    right: Option<&Row>,
    projection: &[ColumnSource],
) -> CompactVec<Value> {
    let mut values = CompactVec::with_capacity(projection.len());
    for source in projection {
        let value = match source {
            ColumnSource::Outer(index) => left.and_then(|row| row.get(*index)),
            ColumnSource::Inner(index) => right.and_then(|row| row.get(*index)),
        };
        values.push(value.cloned().unwrap_or(NULL_VALUE));
    }
    values
}

/// Distance metric for vector search
#[derive(Debug, Clone, Copy)]
pub enum DistanceMetric {
    L2,
    Cosine,
    InnerProduct,
}

/// Parallel brute-force k-NN vector search
///
/// Fuses distance computation + top-K heap selection into parallel chunks,
/// then merges chunk heaps. Zero-copy on Extension(Vector) values.
///
/// Returns (row_id, row, distance) sorted by distance (ascending).
pub fn parallel_topn_vector_search(
    rows: RowVec,
    vector_col_idx: usize,
    query_bytes: &[u8],
    k: usize,
    metric: DistanceMetric,
    config: &ParallelConfig,
) -> radixdb_core::Result<Vec<(i64, Row, f64)>> {
    use std::collections::BinaryHeap;

    if k == 0 || rows.is_empty() {
        return Ok(Vec::new());
    }

    let distance_fn: fn(&[u8], &[u8]) -> radixdb_core::Result<f64> = match metric {
        DistanceMetric::L2 => radixdb_functions::scalar::vector::l2_distance_bytes,
        DistanceMetric::Cosine => radixdb_functions::scalar::vector::cosine_distance_bytes,
        DistanceMetric::InnerProduct => radixdb_functions::scalar::vector::ip_distance_bytes,
    };

    // Extract vector bytes from a row value — zero-copy for Extension(Vector)
    #[inline]
    fn get_vector_bytes(row: &Row, col_idx: usize) -> Option<&[u8]> {
        match row.get(col_idx)? {
            Value::Extension(data)
                if data.first() == Some(&(radixdb_core::DataType::Vector as u8)) =>
            {
                Some(&data[1..])
            }
            _ => None,
        }
    }

    /// Max-heap entry: highest distance at top so we can efficiently evict the worst
    struct HeapEntry {
        distance: f64,
        idx: usize, // index into the chunk's collected (row_id, row) vec
    }

    impl PartialEq for HeapEntry {
        fn eq(&self, other: &Self) -> bool {
            self.distance == other.distance
        }
    }
    impl Eq for HeapEntry {}
    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.distance.total_cmp(&other.distance)
        }
    }

    let row_vec: Vec<(i64, Row)> = rows.into_vec();
    // Validate all shapes before entering parallel closures, where errors must
    // not be converted into infinite distances or panics.
    distance_fn(query_bytes, query_bytes)?;
    for (_, row) in &row_vec {
        if let Some(vector) = get_vector_bytes(row, vector_col_idx) {
            distance_fn(vector, query_bytes)?;
        }
    }

    #[cfg(feature = "parallel")]
    let use_parallel = config.should_parallel_filter(row_vec.len());
    #[cfg(not(feature = "parallel"))]
    let use_parallel = false;
    let _ = config;

    if use_parallel {
        #[cfg(feature = "parallel")]
        {
            let n_threads = num_threads();
            let chunk_size = (row_vec.len() / (n_threads * 4)).max(1024);

            // Each chunk computes top-K locally, returns (row_id, row, distance)
            let chunk_results: Vec<Vec<(i64, Row, f64)>> = row_vec
                .into_par_iter()
                .chunks(chunk_size)
                .map(|chunk| {
                    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(k + 1);
                    let mut entries: Vec<(i64, Row, f64)> = Vec::with_capacity(k + 1);

                    for (row_id, row) in chunk {
                        let dist = if let Some(vec_bytes) = get_vector_bytes(&row, vector_col_idx) {
                            if vec_bytes.len() == query_bytes.len() {
                                distance_fn(vec_bytes, query_bytes)
                                    .expect("vector shapes were validated before parallel search")
                            } else {
                                f64::INFINITY // Dimension mismatch → sort to end
                            }
                        } else {
                            f64::INFINITY // No vector → sort to end
                        };
                        if entries.len() < k {
                            let idx = entries.len();
                            entries.push((row_id, row, dist));
                            heap.push(HeapEntry {
                                distance: dist,
                                idx,
                            });
                        } else if let Some(worst) = heap.peek() {
                            if dist < worst.distance {
                                let evict_idx = worst.idx;
                                heap.pop();
                                entries[evict_idx] = (row_id, row, dist);
                                heap.push(HeapEntry {
                                    distance: dist,
                                    idx: evict_idx,
                                });
                            }
                        }
                    }

                    // Return only the live entries (some slots may have been reused)
                    let live_indices: FxHashSet<usize> = heap.into_iter().map(|e| e.idx).collect();
                    entries
                        .into_iter()
                        .enumerate()
                        .filter_map(|(i, e)| {
                            if live_indices.contains(&i) {
                                Some(e)
                            } else {
                                None
                            }
                        })
                        .collect()
                })
                .collect();

            // Merge: collect all chunk results, take global top-K
            let mut merged: Vec<(i64, Row, f64)> = chunk_results.into_iter().flatten().collect();
            merged.sort_unstable_by(|a, b| a.2.total_cmp(&b.2));
            merged.truncate(k);
            return Ok(merged);
        }
    }

    // Sequential fallback
    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(k + 1);
    let mut entries: Vec<(i64, Row, f64)> = Vec::with_capacity(k + 1);

    for (row_id, row) in row_vec {
        let dist = if let Some(vec_bytes) = get_vector_bytes(&row, vector_col_idx) {
            if vec_bytes.len() == query_bytes.len() {
                distance_fn(vec_bytes, query_bytes)
                    .expect("vector shapes were validated before sequential search")
            } else {
                f64::INFINITY // Dimension mismatch → sort to end
            }
        } else {
            f64::INFINITY // No vector → sort to end
        };
        if entries.len() < k {
            let idx = entries.len();
            entries.push((row_id, row, dist));
            heap.push(HeapEntry {
                distance: dist,
                idx,
            });
        } else if let Some(worst) = heap.peek() {
            if dist < worst.distance {
                let evict_idx = worst.idx;
                heap.pop();
                entries[evict_idx] = (row_id, row, dist);
                heap.push(HeapEntry {
                    distance: dist,
                    idx: evict_idx,
                });
            }
        }
    }

    let live_indices: FxHashSet<usize> = heap.into_iter().map(|e| e.idx).collect();
    let mut result: Vec<(i64, Row, f64)> = entries
        .into_iter()
        .enumerate()
        .filter_map(|(i, e)| {
            if live_indices.contains(&i) {
                Some(e)
            } else {
                None
            }
        })
        .collect();
    result.sort_unstable_by(|a, b| a.2.total_cmp(&b.2));
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_core::Value;

    #[test]
    fn test_parallel_config_thresholds() {
        let config = ParallelConfig::default();

        assert!(!config.should_parallel_filter(1000)); // Below threshold
        assert!(config.should_parallel_filter(20_000)); // Above threshold

        assert!(!config.should_parallel_join(1000)); // Below threshold
        assert!(config.should_parallel_join(20_000)); // Above threshold
    }

    #[test]
    fn test_parallel_hash_build() {
        // Build side: 10K rows with id as key
        let build_rows: Vec<Row> = (0..10_000)
            .map(|i| {
                Row::from_values(vec![
                    Value::Integer(i),
                    Value::Text(format!("build_{}", i).into()),
                ])
            })
            .collect();

        let config = ParallelConfig {
            min_rows_for_parallel_join: 1000,
            ..Default::default()
        };

        let retained = ParallelHashTable::retained_bytes(build_rows.len()).unwrap();
        let hash_table =
            parallel_hash_build_inner(&build_rows, &[0], &config, None, retained).unwrap();
        // Verify some lookups work
        let test_hash = hash_row_by_keys(&build_rows[500], &[0]);
        let mut found = false;
        hash_table.for_each_match(&test_hash, |index| found |= index == 500);
        assert!(found);
    }

    #[test]
    fn parallel_hash_state_has_one_fixed_entry_per_row_and_is_admitted_up_front() {
        assert_eq!(
            std::mem::size_of::<ParallelHashEntry>(),
            16,
            "parallel hash entries must remain fixed width"
        );

        let build_rows: Vec<Row> = (0..128)
            .map(|i| Row::from_values(vec![Value::Integer(i)]))
            .collect();
        let retained = ParallelHashTable::retained_bytes(build_rows.len()).unwrap();
        assert_eq!(
            retained,
            crate::hash_table::JoinHashTable::estimated_retained_bytes(build_rows.len()).unwrap()
        );
        assert!(parallel_join_state_fits_budget(
            build_rows.len(),
            false,
            retained
        ));
        assert!(!parallel_join_state_fits_budget(
            build_rows.len(),
            false,
            retained - 1
        ));

        let config = ParallelConfig {
            min_rows_for_parallel_join: 1,
            chunk_size: 8,
            ..Default::default()
        };
        let error = parallel_hash_build_inner(&build_rows, &[0], &config, None, retained - 1)
            .err()
            .expect("over-budget table must be rejected before build");
        assert!(error.to_string().contains("exceeds memory budget"));
    }

    #[test]
    fn outer_match_tracker_is_part_of_parallel_join_budget() {
        let rows = 128;
        let hash_bytes = ParallelHashTable::retained_bytes(rows).unwrap();
        let total = parallel_join_state_retained_bytes(rows, true).unwrap();
        assert_eq!(total, hash_bytes + rows * std::mem::size_of::<AtomicBool>());
        assert!(!parallel_join_state_fits_budget(rows, true, hash_bytes));
        assert!(parallel_join_state_fits_budget(rows, true, total));
    }

    #[test]
    fn parallel_pull_join_bounds_probe_batches_and_releases_request_memory() {
        let build_rows = CompactArc::new(vec![
            Row::from_values(vec![Value::Integer(1)]),
            Row::from_values(vec![Value::Integer(1)]),
            Row::from_values(vec![Value::Integer(1)]),
            Row::from_values(vec![Value::Integer(2)]),
        ]);
        let probe_rows = vec![
            Row::from_values(vec![Value::Integer(1)]),
            Row::from_values(vec![Value::Integer(2)]),
            Row::from_values(vec![Value::Integer(3)]),
            Row::from_values(vec![Value::Integer(1)]),
            Row::from_values(vec![Value::Integer(4)]),
        ];
        let probe = Box::new(crate::operator::MaterializedOperator::new(
            probe_rows,
            vec![ColumnInfo::new("probe.id")],
        ));
        let config = ParallelConfig {
            min_rows_for_parallel_join: 1,
            chunk_size: 1,
            join_output_batch_rows: 2,
            join_output_batch_bytes: 4096,
            ..Default::default()
        };
        let ctx = ExecutionContext::new();
        let state_bytes = parallel_join_state_retained_bytes(build_rows.len(), false).unwrap();
        let state_reservation = ctx.reserve_join_memory(state_bytes).unwrap();
        let batch_reservation = ctx
            .reserve_join_memory(config.join_output_batch_bytes)
            .unwrap();
        let cancellation = ctx.cancellation_handle();
        let mut operator = ParallelHashJoinOperator::new(
            probe,
            build_rows,
            vec![0],
            vec![0],
            JoinType::Left,
            false,
            1,
            1,
            vec!["probe.id".to_string(), "build.id".to_string()],
            None,
            config,
            cancellation,
            state_bytes,
            state_reservation,
            batch_reservation,
        )
        .unwrap();

        operator.open().unwrap();
        assert_eq!(operator.peak_probe_batch_rows, 0);
        let first = operator.next().unwrap().unwrap().into_owned();
        assert_eq!(first.get(0), Some(&Value::Integer(1)));
        assert_eq!(operator.peak_probe_batch_rows, 2);

        let mut output_rows = 1;
        while operator.next().unwrap().is_some() {
            output_rows += 1;
            assert!(operator.probe_batch.len() <= 2);
            assert!(operator.ready.len() <= 2);
        }
        assert_eq!(output_rows, 9);
        assert_eq!(operator.peak_probe_batch_rows, 2);
        assert!(ctx.retained_join_memory_bytes() > 0);
        operator.close().unwrap();
        assert_eq!(ctx.retained_join_memory_bytes(), 0);
    }

    #[test]
    fn parallel_pull_join_cancellation_closes_without_retained_state() {
        let build_rows = CompactArc::new(
            (0..128)
                .map(|value| Row::from_values(vec![Value::Integer(value)]))
                .collect::<Vec<_>>(),
        );
        let probe = Box::new(crate::operator::MaterializedOperator::new(
            vec![Row::from_values(vec![Value::Integer(1)])],
            vec![ColumnInfo::new("probe.id")],
        ));
        let config = ParallelConfig {
            min_rows_for_parallel_join: 1,
            join_output_batch_rows: 2,
            join_output_batch_bytes: 4096,
            ..Default::default()
        };
        let ctx = ExecutionContext::new();
        let state_bytes = parallel_join_state_retained_bytes(build_rows.len(), false).unwrap();
        let state_reservation = ctx.reserve_join_memory(state_bytes).unwrap();
        let batch_reservation = ctx
            .reserve_join_memory(config.join_output_batch_bytes)
            .unwrap();
        let cancellation = ctx.cancellation_handle();
        let mut operator = ParallelHashJoinOperator::new(
            probe,
            build_rows,
            vec![0],
            vec![0],
            JoinType::Inner,
            false,
            1,
            1,
            vec!["probe.id".to_string(), "build.id".to_string()],
            None,
            config,
            cancellation.clone(),
            state_bytes,
            state_reservation,
            batch_reservation,
        )
        .unwrap();

        operator.open().unwrap();
        cancellation.cancel();
        assert!(matches!(
            operator.next(),
            Err(radixdb_core::Error::QueryCancelled)
        ));
        operator.close().unwrap();
        assert_eq!(ctx.retained_join_memory_bytes(), 0);
    }

    #[test]
    fn test_verify_key_match() {
        let row1 = Row::from_values(vec![Value::Integer(1), Value::Text("a".to_string().into())]);
        let row2 = Row::from_values(vec![Value::Integer(1), Value::Text("b".to_string().into())]);
        let row3 = Row::from_values(vec![Value::Integer(2), Value::Text("a".to_string().into())]);

        // Same key column 0
        assert!(verify_key_match(&row1, &row2, &[0], &[0]));

        // Different key column 0
        assert!(!verify_key_match(&row1, &row3, &[0], &[0]));

        // Same value in column 1
        assert!(verify_key_match(&row1, &row3, &[1], &[1]));
    }

    #[test]
    fn test_hash_join_numeric_key_contract_sequential_and_parallel() {
        const EXACT: i64 = 1_i64 << 53;
        let build_rows = vec![
            Row::from_values(vec![Value::Integer(EXACT)]),
            Row::from_values(vec![Value::Integer(EXACT + 1)]),
            Row::from_values(vec![Value::Float(-0.0)]),
            Row::from_values(vec![Value::Float(f64::NAN)]),
        ];
        let probe_rows = vec![
            Row::from_values(vec![Value::Float(EXACT as f64)]),
            Row::from_values(vec![Value::Integer(0)]),
            Row::from_values(vec![Value::Float(f64::from_bits(0x7ff8_0000_0000_0042))]),
        ];

        for (name, config) in [
            (
                "sequential",
                ParallelConfig {
                    min_rows_for_parallel_join: usize::MAX,
                    ..Default::default()
                },
            ),
            (
                "parallel",
                ParallelConfig {
                    min_rows_for_parallel_join: 1,
                    chunk_size: 1,
                    ..Default::default()
                },
            ),
        ] {
            for swapped in [false, true] {
                let result = parallel_hash_join(
                    &probe_rows,
                    &build_rows,
                    &[0],
                    &[0],
                    JoinType::Inner,
                    1,
                    1,
                    swapped,
                    &config,
                );
                assert_eq!(
                    result.rows.len(),
                    3,
                    "{name} numeric join with swapped={swapped}"
                );
                let _ = name;
            }
        }
    }

    // ========================================================================
    // Edge Case Tests for Hash Joins
    // ========================================================================

    /// Test parallel hash join with hash collisions on join keys
    #[test]
    fn test_parallel_hash_join_collision_handling() {
        // Build side: rows with varying second columns but same join key
        let build_rows: Vec<Row> = vec![
            Row::from_values(vec![Value::Integer(1), Value::Text("build_a".into())]),
            Row::from_values(vec![Value::Integer(2), Value::Text("build_b".into())]),
            Row::from_values(vec![Value::Integer(3), Value::Text("build_c".into())]),
        ];

        // Probe side: rows that should match build side
        let probe_rows: Vec<Row> = vec![
            Row::from_values(vec![Value::Integer(1), Value::Text("probe_x".into())]),
            Row::from_values(vec![Value::Integer(2), Value::Text("probe_y".into())]),
            Row::from_values(vec![Value::Integer(4), Value::Text("probe_z".into())]), // No match
        ];

        let config = ParallelConfig {
            min_rows_for_parallel_join: 1, // Force parallel path
            ..Default::default()
        };

        // INNER JOIN on first column
        let result = parallel_hash_join(
            &probe_rows,
            &build_rows,
            &[0], // probe key
            &[0], // build key
            JoinType::Inner,
            2, // probe col count
            2, // build col count
            false,
            &config,
        );

        // Should have 2 matches (id=1 and id=2)
        assert_eq!(result.rows.len(), 2, "INNER JOIN should have 2 matches");

        // Verify the joined rows have correct values
        for row in &result.rows {
            // Combined row should have 4 columns (2 from probe + 2 from build)
            assert_eq!(row.len(), 4);
        }
    }

    /// Test LEFT OUTER join with unmatched probe rows
    #[test]
    fn test_parallel_left_join_unmatched() {
        let build_rows: Vec<Row> = vec![Row::from_values(vec![
            Value::Integer(1),
            Value::Text("match".into()),
        ])];

        let probe_rows: Vec<Row> = vec![
            Row::from_values(vec![Value::Integer(1), Value::Text("p1".into())]), // Matches
            Row::from_values(vec![Value::Integer(2), Value::Text("p2".into())]), // No match
            Row::from_values(vec![Value::Integer(3), Value::Text("p3".into())]), // No match
        ];

        let config = ParallelConfig {
            min_rows_for_parallel_join: 1,
            ..Default::default()
        };

        let result = parallel_hash_join(
            &probe_rows,
            &build_rows,
            &[0],
            &[0],
            JoinType::Left,
            2,
            2,
            false,
            &config,
        );

        // Should have 3 rows: 1 matched + 2 unmatched with NULL build columns
        assert_eq!(result.rows.len(), 3, "LEFT JOIN should have 3 rows");

        // Count rows with NULL in build columns (last 2 columns)
        let null_count = result
            .rows
            .iter()
            .filter(|r| {
                r.get(2).map(|v| v.is_null()).unwrap_or(false)
                    && r.get(3).map(|v| v.is_null()).unwrap_or(false)
            })
            .count();
        assert_eq!(
            null_count, 2,
            "Should have 2 unmatched rows with NULL build columns"
        );
    }

    #[test]
    fn test_parallel_left_join_fuses_projection_before_output_materialization() {
        let build_rows = vec![Row::from_values(vec![
            Value::Integer(1),
            Value::Text("build".into()),
            Value::Text("unused-build-payload".repeat(128).into()),
        ])];
        let probe_rows = vec![
            Row::from_values(vec![
                Value::Integer(1),
                Value::Text("matched".into()),
                Value::Text("unused-probe-payload".repeat(128).into()),
            ]),
            Row::from_values(vec![
                Value::Integer(2),
                Value::Text("unmatched".into()),
                Value::Text("unused-probe-payload".repeat(128).into()),
            ]),
        ];
        let projection = [ColumnSource::Outer(1), ColumnSource::Inner(1)];
        let config = ParallelConfig {
            min_rows_for_parallel_join: 1,
            ..Default::default()
        };

        let result = parallel_hash_join_inner(
            &probe_rows,
            &build_rows,
            &[0],
            &[0],
            JoinType::Left,
            3,
            3,
            false,
            Some(&projection),
            &config,
            None,
            usize::MAX,
        )
        .unwrap();

        assert_eq!(result.rows.len(), 2);
        assert!(result.rows.iter().all(|row| row.len() == 2));
        assert!(result.rows.iter().any(|row| {
            row.get(0) == Some(&Value::text("matched")) && row.get(1) == Some(&Value::text("build"))
        }));
        assert!(result.rows.iter().any(|row| {
            row.get(0) == Some(&Value::text("unmatched")) && row.get(1).is_some_and(Value::is_null)
        }));
    }

    /// Test RIGHT OUTER join with unmatched build rows
    #[test]
    fn test_parallel_right_join_unmatched() {
        let build_rows: Vec<Row> = vec![
            Row::from_values(vec![Value::Integer(1), Value::Text("b1".into())]), // Matches
            Row::from_values(vec![Value::Integer(2), Value::Text("b2".into())]), // No match
            Row::from_values(vec![Value::Integer(3), Value::Text("b3".into())]), // No match
        ];

        let probe_rows: Vec<Row> = vec![Row::from_values(vec![
            Value::Integer(1),
            Value::Text("p1".into()),
        ])];

        let config = ParallelConfig {
            min_rows_for_parallel_join: 1,
            ..Default::default()
        };

        let result = parallel_hash_join(
            &probe_rows,
            &build_rows,
            &[0],
            &[0],
            JoinType::Right,
            2,
            2,
            false,
            &config,
        );

        // Should have 3 rows: 1 matched + 2 unmatched with NULL probe columns
        assert_eq!(result.rows.len(), 3, "RIGHT JOIN should have 3 rows");

        // Count rows with NULL in probe columns (first 2 columns)
        let null_count = result
            .rows
            .iter()
            .filter(|r| {
                r.get(0).map(|v| v.is_null()).unwrap_or(false)
                    && r.get(1).map(|v| v.is_null()).unwrap_or(false)
            })
            .count();
        assert_eq!(
            null_count, 2,
            "Should have 2 unmatched rows with NULL probe columns"
        );
    }

    /// Test FULL OUTER join with unmatched rows on both sides
    #[test]
    fn test_parallel_full_outer_join() {
        let build_rows: Vec<Row> = vec![
            Row::from_values(vec![Value::Integer(1), Value::Text("b1".into())]), // Matches
            Row::from_values(vec![Value::Integer(3), Value::Text("b3".into())]), // No match
        ];

        let probe_rows: Vec<Row> = vec![
            Row::from_values(vec![Value::Integer(1), Value::Text("p1".into())]), // Matches
            Row::from_values(vec![Value::Integer(2), Value::Text("p2".into())]), // No match
        ];

        let config = ParallelConfig {
            min_rows_for_parallel_join: 1,
            ..Default::default()
        };

        let result = parallel_hash_join(
            &probe_rows,
            &build_rows,
            &[0],
            &[0],
            JoinType::Full,
            2,
            2,
            false,
            &config,
        );

        // Should have 3 rows:
        // 1 matched (id=1)
        // 1 unmatched probe (id=2, build NULL)
        // 1 unmatched build (id=3, probe NULL)
        assert_eq!(result.rows.len(), 3, "FULL OUTER JOIN should have 3 rows");
    }

    /// Test join with empty tables
    #[test]
    fn test_parallel_join_empty_tables() {
        let config = ParallelConfig::default();

        // Empty probe
        let result = parallel_hash_join(
            &[],
            &[Row::from_values(vec![Value::Integer(1)])],
            &[0],
            &[0],
            JoinType::Inner,
            1,
            1,
            false,
            &config,
        );
        assert_eq!(
            result.rows.len(),
            0,
            "Empty probe should give empty result for INNER"
        );

        // Empty build
        let result = parallel_hash_join(
            &[Row::from_values(vec![Value::Integer(1)])],
            &[],
            &[0],
            &[0],
            JoinType::Inner,
            1,
            1,
            false,
            &config,
        );
        assert_eq!(
            result.rows.len(),
            0,
            "Empty build should give empty result for INNER"
        );

        // LEFT JOIN with empty build should preserve all probe rows
        let result = parallel_hash_join(
            &[
                Row::from_values(vec![Value::Integer(1)]),
                Row::from_values(vec![Value::Integer(2)]),
            ],
            &[],
            &[0],
            &[0],
            JoinType::Left,
            1,
            1,
            false,
            &config,
        );
        assert_eq!(
            result.rows.len(),
            2,
            "LEFT JOIN with empty build should have all probe rows"
        );
    }

    /// Test join with swapped build/probe sides
    /// When swapped=true, the roles of probe and build are swapped, but the join type
    /// semantics stay the same. LEFT JOIN with swapped=true means:
    /// - Build side is actually "left" in the original query
    /// - Probe side is "right"
    /// - LEFT JOIN needs unmatched LEFT (build) rows, not probe rows
    #[test]
    fn test_parallel_join_swapped() {
        // When swapped=true:
        // - build_rows (1 row) = original left side
        // - probe_rows (2 rows) = original right side
        let build_rows: Vec<Row> = vec![
            Row::from_values(vec![Value::Integer(1), Value::Text("b1".into())]),
            Row::from_values(vec![Value::Integer(3), Value::Text("b3".into())]), // Unmatched left
        ];

        let probe_rows: Vec<Row> = vec![
            Row::from_values(vec![Value::Integer(1), Value::Text("p1".into())]),
            Row::from_values(vec![Value::Integer(2), Value::Text("p2".into())]), // Unmatched right
        ];

        let config = ParallelConfig {
            min_rows_for_parallel_join: 1,
            ..Default::default()
        };

        // LEFT JOIN with swapped=true:
        // - Build is "left", so unmatched build rows need NULL right columns
        // - Probe is "right", so unmatched probe rows are NOT included (LEFT JOIN only keeps left)
        let result = parallel_hash_join(
            &probe_rows,
            &build_rows,
            &[0],
            &[0],
            JoinType::Left,
            2,
            2,
            true, // swapped: build=left, probe=right
            &config,
        );

        // Should have 2 rows:
        // 1 matched row (id=1)
        // 1 unmatched build row (id=3) with NULL probe columns
        assert_eq!(result.rows.len(), 2, "LEFT JOIN swapped should have 2 rows");

        // Verify column order is correct (build columns first when swapped)
        // Row structure: [build_col0, build_col1, probe_col0, probe_col1]
        let matched_row = result
            .rows
            .iter()
            .find(|r| r.get(0) == Some(&Value::Integer(1)) && r.get(2) == Some(&Value::Integer(1)));
        assert!(matched_row.is_some(), "Should have a matched row with id=1");

        // Verify unmatched build row has NULL in probe columns
        let unmatched_row = result
            .rows
            .iter()
            .find(|r| r.get(0) == Some(&Value::Integer(3)));
        assert!(
            unmatched_row.is_some(),
            "Should have unmatched build row with id=3"
        );
        let unmatched = unmatched_row.unwrap();
        assert!(
            unmatched.get(2).map(|v| v.is_null()).unwrap_or(false),
            "Probe col should be NULL"
        );
        assert!(
            unmatched.get(3).map(|v| v.is_null()).unwrap_or(false),
            "Probe col should be NULL"
        );
    }
}
