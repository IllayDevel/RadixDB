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

//! Modern streaming JOIN executor using Volcano-style operators.
//!
//! This module provides high-performance JOIN execution with:
//! - **Hash Join**: Build smaller side, probe larger side with O(N+M) complexity
//! - **Merge Join**: O(N+M) when inputs are pre-sorted on join keys
//! - **Nested Loop**: O(N*M) fallback for non-equality joins or small tables
//! - **Early Termination**: LIMIT stops execution immediately
//! - **Residual Filters**: Non-equality conditions applied during streaming
//!
//! # Architecture
//!
//! ```text
//! JoinRequest
//!     │
//!     ▼
//! ┌─────────────────────────────────┐
//! │ JoinExecutor::execute()         │
//! │  1. Analyze join condition      │
//! │  2. Select optimal algorithm    │
//! │  3. Execute with streaming      │
//! │  4. Early terminate at LIMIT    │
//! └─────────────────────────────────┘
//!     │
//!     ▼
//! JoinResult { rows, columns }
//! ```
//!
//! # Design Decisions & Tradeoffs
//!
//! ## Hybrid Execution: Streaming vs Parallel
//!
//! This executor uses a **hybrid approach** that dynamically chooses between
//! Volcano-style streaming and parallel bulk processing based on query characteristics:
//!
//! ### Streaming Volcano Path (default for small datasets or small LIMIT)
//! - **When**: Build side < 10,000 rows OR LIMIT ≤ 1,000
//! - **Benefits**:
//!   - O(1) memory for probe side (streaming, not materialized)
//!   - Early termination: LIMIT 10 stops after 10 rows
//!   - Low latency to first row (important for interactive queries)
//!   - Composable operators (Filter → Join → Project → Limit)
//!
//! ### Parallel Hash Join Path (for large analytical queries)
//! - **When**: Build side ≥ 10,000 rows AND (no LIMIT or LIMIT > 1,000)
//! - **Benefits**:
//!   - Parallel hash build using a pre-admitted fixed-width atomic table
//!   - Parallel probe with Rayon work-stealing
//!   - 2-4x speedup on multi-core systems for large joins
//!   - Atomic tracking for OUTER join unmatched rows
//!
//! ### Why Not Always Parallel?
//! Parallel execution has overhead (task scheduling, synchronization). For:
//! - Small datasets: overhead exceeds benefit
//! - Small LIMIT: streaming stops early; parallel computes full result then truncates
//!
//! ## Merge Join Boundary
//!
//! The binary entry point currently receives materialized relation batches,
//! but `MergeJoinOperator` itself consumes its certified ordered inputs one row
//! at a time. Only matching duplicate-key groups are blocking state, and their
//! owner is bounded before execution:
//!
//! ```text
//! left ordered operator  ┐
//!                        ├→ bounded streaming MergeJoinOperator
//! right ordered operator ┘
//! ```
//!
//! ## Bloom Filter Optimization
//!
//! Bloom filters can accelerate hash joins by filtering probe rows that
//! definitely won't match before touching the hash table. This is particularly
//! effective for:
//! - High selectivity joins (few matches relative to probe size)
//! - Multi-way joins (filter cascades through the plan)
//!
//! Query planning can build a runtime bloom filter and wrap the probe input in
//! `BloomFilterOperator` before handing it to the streaming join executor.

use crate::context::ExecutionContext;
use crate::expression::{JoinFilter, RowFilter};
use crate::hash_table::JoinHashState;
use crate::operator::{ColumnInfo, MaterializedOperator, Operator, OrderingProperty, RowRef};
use crate::operators::hash_join::{HashJoinOperator, JoinSide, JoinType};
use crate::operators::merge_join::MergeJoinOperator;
use crate::operators::nested_loop_join::NestedLoopJoinOperator;
use crate::parallel::{
    parallel_hash_join_cancellable, parallel_join_state_retained_bytes, ParallelConfig,
    ParallelHashJoinOperator, DEFAULT_PARALLEL_JOIN_THRESHOLD,
};
use crate::result::OperatorExecutorResult;
use crate::utils::{extract_join_keys_and_residual, JoinProjectionIndices, RetainedRowsBudget};
use radixdb_core::value::NULL_VALUE;
use radixdb_core::{CompactArc, CompactVec};
use radixdb_core::{Result, Row, RowVec, Value};
use radixdb_functions::global_registry;
use radixdb_sql::ast::Expression;
use radixdb_storage::instrumentation::{self, JoinExecutionKind, JoinExecutionRecord};
use radixdb_storage::DeferredRow;

/// Runtime join algorithm selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeJoinAlgorithm {
    /// Hash join: O(N + M) for unsorted equality inputs.
    HashJoin,
    /// Merge join: O(N + M) when inputs are ordered on their keys.
    MergeJoin,
    /// Nested loop for small inputs or conditions without equality keys.
    NestedLoop,
}

/// Runtime join decision produced by the planner and consumed by physical JOIN.
#[derive(Debug, Clone)]
pub struct RuntimeJoinDecision {
    /// Selected join algorithm.
    pub algorithm: RuntimeJoinAlgorithm,
    /// Whether physical execution should swap the two inputs.
    pub swap_sides: bool,
    /// Human-readable decision evidence.
    pub explanation: String,
}

impl RuntimeJoinDecision {
    /// Check if hash join was selected.
    pub fn use_hash_join(&self) -> bool {
        self.algorithm == RuntimeJoinAlgorithm::HashJoin
    }

    /// Check if merge join was selected.
    pub fn use_merge_join(&self) -> bool {
        self.algorithm == RuntimeJoinAlgorithm::MergeJoin
    }

    /// Check if nested loop was selected.
    pub fn use_nested_loop(&self) -> bool {
        self.algorithm == RuntimeJoinAlgorithm::NestedLoop
    }
}

/// LIMIT threshold below which streaming execution is preferred over parallel.
///
/// When LIMIT is small (≤ this value), streaming Volcano-style execution benefits from
/// early termination - we can stop after producing just N rows without processing
/// the entire join. Parallel execution would compute the full join result before
/// truncating, wasting work.
///
/// When LIMIT is large (> this value), the early termination benefit is minimal,
/// so parallel execution's throughput advantage dominates.
const STREAMING_LIMIT_THRESHOLD: u64 = 1000;

/// Result of a streaming join execution.
#[derive(Debug)]
pub struct JoinResult {
    /// The joined rows with synthetic row IDs.
    pub rows: JoinRows,
    /// Column names for the combined result.
    pub columns: Vec<String>,
}

#[derive(Debug)]
pub enum JoinRows {
    Owned(RowVec),
    Deferred(Vec<DeferredRow>),
}

impl JoinRows {
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Owned(rows) => rows.len(),
            Self::Deferred(rows) => rows.len(),
        }
    }

    /// Return whether the physical JOIN produced no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    fn owned_len(&self) -> usize {
        match self {
            Self::Owned(rows) => rows.len(),
            Self::Deferred(_) => 0,
        }
    }

    pub fn into_owned(self) -> RowVec {
        match self {
            Self::Owned(rows) => rows,
            Self::Deferred(rows) => {
                let mut owned = RowVec::with_capacity(rows.len());
                for (row_id, row) in rows.into_iter().enumerate() {
                    owned.push((row_id as i64, row.into_owned()));
                }
                owned
            }
        }
    }

    pub fn into_deferred(self) -> Vec<DeferredRow> {
        match self {
            Self::Deferred(rows) => rows,
            Self::Owned(mut rows) => rows
                .drain_rows()
                .map(DeferredRow::owned)
                .collect::<Vec<_>>(),
        }
    }
}

/// Analysis of a join operation for algorithm selection.
#[derive(Debug, Clone)]
pub struct JoinAnalysis {
    /// Left side key column indices for equality join.
    pub left_key_indices: Vec<usize>,
    /// Right side key column indices for equality join.
    pub right_key_indices: Vec<usize>,
    /// Non-equality conditions to apply after hash matching.
    pub residual_conditions: Vec<Expression>,
    /// The parsed join type.
    pub join_type: JoinType,
    /// Join type as string (for compatibility).
    pub join_type_str: String,
}

/// Configuration for join execution.
/// This combines the algorithm choice with execution-specific config.
#[derive(Debug, Clone)]
struct JoinConfig {
    /// The algorithm to use.
    algorithm: RuntimeJoinAlgorithm,
    /// For hash joins: whether to build on the left side.
    build_left: bool,
}

/// Physical ordering certificates for both sides of one JOIN edge.
#[derive(Debug, Clone, Default)]
pub struct JoinInputOrderings {
    pub left: OrderingProperty,
    pub right: OrderingProperty,
}

impl JoinInputOrderings {
    pub fn new(left: OrderingProperty, right: OrderingProperty) -> Self {
        Self { left, right }
    }
}

struct MergeExecutionRequest<'a> {
    left_rows: CompactArc<Vec<Row>>,
    right_rows: CompactArc<Vec<Row>>,
    columns: (&'a [String], &'a [String]),
    analysis: &'a JoinAnalysis,
    ordering: JoinInputOrderings,
    limit: Option<u64>,
    ctx: &'a ExecutionContext,
}

/// Request to execute a join operation.
///
/// Uses `CompactArc<Vec<Row>>` to enable zero-copy sharing with CTE results.
/// When dropping, only decrements refcount (O(1)) instead of deallocating rows.
/// The caller should pass Arc-wrapped data for CTE sources.
pub struct JoinRequest<'a> {
    /// Left side rows (Arc for zero-copy sharing with CTE results).
    pub left_rows: CompactArc<Vec<Row>>,
    /// Right side rows (Arc for zero-copy sharing with CTE results).
    pub right_rows: CompactArc<Vec<Row>>,
    /// Left side column names.
    pub left_columns: &'a [String],
    /// Right side column names.
    pub right_columns: &'a [String],
    /// Join condition (if any).
    pub condition: Option<&'a Expression>,
    /// Join type string (INNER, LEFT, RIGHT, FULL, CROSS).
    pub join_type: &'a str,
    /// LIMIT for early termination.
    pub limit: Option<u64>,
    /// Execution context for expression evaluation.
    pub ctx: &'a ExecutionContext,
    /// Optional algorithm decision from QueryPlanner.
    /// When provided, the executor uses this instead of making its own decision.
    pub algorithm_hint: Option<&'a RuntimeJoinDecision>,
    /// Ordering certified by the physical producers of both inputs.
    pub ordering: JoinInputOrderings,
    /// Optional fused projection for the final joined row.
    ///
    /// Hash joins apply it only when no residual predicate still needs the full
    /// logical row. Nested-loop joins apply it after evaluating their condition
    /// against full left/right rows.
    pub projection: Option<&'a JoinProjectionIndices>,
}

/// Request to execute a streaming hash join.
///
/// Unlike `JoinRequest`, this takes a streaming operator for the probe side,
/// enabling true streaming without full materialization. This is optimal for
/// LIMIT queries where early termination can stop the probe scan early.
///
/// # Memory Model
///
/// - **Build side**: Fully materialized (required for hash table construction)
/// - **Probe side**: Streams row-by-row from the operator (O(1) memory)
///
/// # When to Use
///
/// Use `StreamingJoinRequest` when:
/// - Query has LIMIT (early termination benefit)
/// - Probe side is large (avoid full materialization)
/// - Join algorithm is Hash Join
pub struct StreamingJoinRequest<'a> {
    /// Build side rows (Arc for zero-copy sharing with CTE results).
    pub build_rows: CompactArc<Vec<Row>>,
    /// Build side column names.
    pub build_columns: &'a [String],
    /// Probe side as streaming operator (NOT materialized).
    pub probe_source: Box<dyn Operator>,
    /// Probe side column names.
    pub probe_columns: Vec<String>,
    /// Join condition (if any).
    pub condition: Option<&'a Expression>,
    /// Join type string (INNER, LEFT, RIGHT, FULL, CROSS).
    pub join_type: &'a str,
    /// Whether build side is left (false = build is right).
    pub build_is_left: bool,
    /// LIMIT for early termination.
    pub limit: Option<u64>,
    /// Execution context for expression evaluation.
    pub ctx: &'a ExecutionContext,
    /// Pre-built hash table (if available). When provided, skips the hash table
    /// build phase in HashJoinOperator, avoiding double iteration of build_rows.
    pub pre_built_hash_state: Option<JoinHashState>,
    /// Optional fused projection for the final joined row.
    ///
    /// This is applied inside HashJoinOperator only when the join condition has
    /// no residual predicates that need the full logical left+right row.
    pub projection: Option<&'a JoinProjectionIndices>,
}

pub type StreamingJoinResult = (
    Box<dyn radixdb_storage::QueryResult>,
    CompactArc<Vec<String>>,
);

struct PreparedStreamingHashJoin {
    operator: HashJoinOperator,
    columns: Vec<String>,
    started: std::time::Instant,
    build_row_count: u64,
    build_is_left: bool,
    left_width: u64,
    right_width: u64,
}

/// Publishes the same edge-level counters whether the operator is collected by
/// the legacy binary entry point or pulled lazily by another JOIN edge.
struct ObservedStreamingHashJoin {
    inner: HashJoinOperator,
    started: std::time::Instant,
    build_row_count: u64,
    build_is_left: bool,
    left_width: u64,
    right_width: u64,
    output_width: u64,
    output_rows: u64,
    recorded: bool,
}

/// Edge-level instrumentation wrapper for the bounded parallel pull operator.
struct ObservedParallelHashJoin {
    inner: ParallelHashJoinOperator,
    started: std::time::Instant,
    build_row_count: u64,
    build_is_left: bool,
    left_width: u64,
    right_width: u64,
    output_width: u64,
    output_rows: u64,
    recorded: bool,
}

impl ObservedParallelHashJoin {
    fn publish(&mut self) {
        if self.recorded {
            return;
        }
        let probe_rows = self.inner.observed_probe_rows();
        let (left_rows, right_rows) = if self.build_is_left {
            (self.build_row_count, probe_rows)
        } else {
            (probe_rows, self.build_row_count)
        };
        instrumentation::record_join_execution(
            JoinExecutionKind::HashParallel,
            JoinExecutionRecord {
                left_rows,
                right_rows,
                output_rows: self.output_rows,
                left_width: self.left_width,
                right_width: self.right_width,
                output_width: self.output_width,
                candidate_pairs: self.inner.observed_candidate_rows(),
                wall_nanos: self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                ..JoinExecutionRecord::default()
            },
        );
        self.recorded = true;
    }
}

impl Operator for ObservedParallelHashJoin {
    fn open(&mut self) -> Result<()> {
        self.inner.open()
    }

    fn next(&mut self) -> Result<Option<RowRef>> {
        let row = self.inner.next()?;
        if row.is_some() {
            self.output_rows = self.output_rows.saturating_add(1);
        }
        Ok(row)
    }

    fn close(&mut self) -> Result<()> {
        let result = self.inner.close();
        self.publish();
        result
    }

    fn schema(&self) -> &[ColumnInfo] {
        self.inner.schema()
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.inner.estimated_rows()
    }

    fn ordering(&self) -> OrderingProperty {
        self.inner.ordering()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
}

impl ObservedStreamingHashJoin {
    fn new(plan: PreparedStreamingHashJoin) -> (Self, Vec<String>) {
        let columns = plan.columns;
        let output_width = columns.len() as u64;
        (
            Self {
                inner: plan.operator,
                started: plan.started,
                build_row_count: plan.build_row_count,
                build_is_left: plan.build_is_left,
                left_width: plan.left_width,
                right_width: plan.right_width,
                output_width,
                output_rows: 0,
                recorded: false,
            },
            columns,
        )
    }

    fn publish(&mut self) {
        if self.recorded {
            return;
        }
        let probe_rows = self.inner.observed_probe_rows();
        let execution_kind = if self.inner.used_scan_fallback() {
            JoinExecutionKind::NestedLoop
        } else {
            JoinExecutionKind::HashStreaming
        };
        let (left_rows, right_rows) = if self.build_is_left {
            (self.build_row_count, probe_rows)
        } else {
            (probe_rows, self.build_row_count)
        };
        instrumentation::record_join_execution(
            execution_kind,
            JoinExecutionRecord {
                left_rows,
                right_rows,
                output_rows: self.output_rows,
                left_width: self.left_width,
                right_width: self.right_width,
                output_width: self.output_width,
                candidate_pairs: self.inner.observed_candidate_rows(),
                wall_nanos: self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                ..JoinExecutionRecord::default()
            },
        );
        self.recorded = true;
    }
}

impl Operator for ObservedStreamingHashJoin {
    fn open(&mut self) -> Result<()> {
        self.inner.open()
    }

    fn next(&mut self) -> Result<Option<RowRef>> {
        let row = self.inner.next()?;
        if row.is_some() {
            self.output_rows = self.output_rows.saturating_add(1);
        }
        Ok(row)
    }

    fn close(&mut self) -> Result<()> {
        let result = self.inner.close();
        self.publish();
        result
    }

    fn schema(&self) -> &[ColumnInfo] {
        self.inner.schema()
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.inner.estimated_rows()
    }

    fn ordering(&self) -> OrderingProperty {
        self.inner.ordering()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
}

/// Modern streaming join executor.
///
/// Uses Volcano-style operators for efficient join execution with:
/// - Streaming probe side (no full materialization)
/// - Early termination for LIMIT
/// - Residual filter application during iteration
pub struct JoinExecutor {}

/// Build-side width sampling is intentionally bounded. The inputs have already
/// crossed their predicate/dependency-projection boundaries, so a small sample
/// captures the physical row shape without adding another O(N) pass before the
/// hash build.
const HASH_BUILD_WIDTH_SAMPLE_ROWS: usize = 256;

impl JoinExecutor {
    /// Create a new join executor.
    pub fn new() -> Self {
        Self {}
    }

    /// Execute a join operation.
    ///
    /// This is the main entry point that:
    /// 1. Analyzes the join condition
    /// 2. Uses provided algorithm hint or selects optimal algorithm
    /// 3. Executes with streaming
    /// 4. Applies early termination
    pub fn execute(&self, request: JoinRequest<'_>) -> Result<JoinResult> {
        let started = std::time::Instant::now();
        let left_rows = request.left_rows.len() as u64;
        let right_rows = request.right_rows.len() as u64;
        let left_width = request.left_columns.len() as u64;
        let right_width = request.right_columns.len() as u64;
        // Build combined column list
        let mut all_columns = request.left_columns.to_vec();
        all_columns.extend(request.right_columns.iter().cloned());

        // Analyze the join (key extraction only - sort check is deferred)
        let analysis = self.analyze(
            request.left_columns,
            request.right_columns,
            request.condition,
            request.join_type,
        );
        let merge_ordering_certified = request
            .ordering
            .left
            .proves_ascending_nulls_last(&analysis.left_key_indices)
            && request
                .ordering
                .right
                .proves_ascending_nulls_last(&analysis.right_key_indices);

        // Select algorithm: use provided hint from QueryPlanner if available,
        // otherwise fall back to local heuristics
        let mut config = if let Some(hint) = request.algorithm_hint {
            self.convert_runtime_decision(hint, &analysis)
        } else {
            self.select_algorithm(
                &analysis,
                &request.left_rows,
                &request.right_rows,
                merge_ordering_certified,
            )
        };

        // MergeJoinOperator consumes equality keys only. The hash operator owns
        // complete ON match-state, including residual predicates, so an OUTER
        // equality edge never needs the O(NxM) nested-loop fallback.
        if config.algorithm == RuntimeJoinAlgorithm::MergeJoin
            && (!analysis.residual_conditions.is_empty() || !merge_ordering_certified)
        {
            config.algorithm = RuntimeJoinAlgorithm::HashJoin;
            config.build_left = match analysis.join_type {
                JoinType::Left | JoinType::Full => false,
                JoinType::Right => true,
                _ => left_rows <= right_rows,
            };
        }
        let mut merge_memory_reservation = None;
        if config.algorithm == RuntimeJoinAlgorithm::MergeJoin {
            let retained_bytes = MergeJoinOperator::matching_groups_retained_bytes(
                &request.left_rows,
                &request.right_rows,
                &analysis.left_key_indices,
                &analysis.right_key_indices,
                RetainedRowsBudget::DEFAULT_MAX_ROWS,
                request.ctx.join_hash_state_max_bytes(),
            )?;
            merge_memory_reservation =
                retained_bytes.and_then(|bytes| request.ctx.reserve_join_memory(bytes));
            if merge_memory_reservation.is_none() {
                config.algorithm = RuntimeJoinAlgorithm::HashJoin;
                config.build_left = match analysis.join_type {
                    JoinType::Left | JoinType::Full => false,
                    JoinType::Right => true,
                    _ => left_rows <= right_rows,
                };
            }
        }
        if config.algorithm == RuntimeJoinAlgorithm::HashJoin {
            config.build_left = Self::choose_hash_build_side(
                &analysis.join_type,
                &request.left_rows,
                &request.right_rows,
                config.build_left,
            );
        }

        let applied_projection = match config.algorithm {
            RuntimeJoinAlgorithm::HashJoin | RuntimeJoinAlgorithm::NestedLoop => request.projection,
            RuntimeJoinAlgorithm::MergeJoin => None,
        };

        // Execute join based on algorithm (takes ownership of rows)
        let (rows, execution_kind) = match config.algorithm {
            RuntimeJoinAlgorithm::HashJoin => self.execute_hash_join(
                request.left_rows,
                request.right_rows,
                &analysis,
                request.left_columns,
                request.right_columns,
                config.build_left,
                request.limit,
                request.ctx,
                applied_projection,
            )?,
            RuntimeJoinAlgorithm::MergeJoin => {
                let _memory_reservation = merge_memory_reservation
                    .take()
                    .expect("admitted merge plan must retain its memory reservation");
                self.execute_merge_join(MergeExecutionRequest {
                    left_rows: request.left_rows,
                    right_rows: request.right_rows,
                    columns: (request.left_columns, request.right_columns),
                    analysis: &analysis,
                    ordering: request.ordering,
                    limit: request.limit,
                    ctx: request.ctx,
                })
                .map(|rows| (JoinRows::Owned(rows), JoinExecutionKind::Merge))?
            }
            RuntimeJoinAlgorithm::NestedLoop => self
                .execute_nested_loop(
                    request.left_rows,
                    request.right_rows,
                    request.condition,
                    request.left_columns,
                    request.right_columns,
                    &analysis.join_type_str,
                    request.limit,
                    applied_projection,
                    request.ctx,
                )
                .map(|rows| (rows, JoinExecutionKind::NestedLoop))?,
        };

        let columns = applied_projection
            .map(|proj| proj.output_columns.clone())
            .unwrap_or(all_columns);

        instrumentation::record_join_rows_constructed(rows.owned_len() as u64);
        instrumentation::record_join_execution(
            execution_kind,
            JoinExecutionRecord {
                left_rows,
                right_rows,
                output_rows: rows.len() as u64,
                left_width,
                right_width,
                output_width: columns.len() as u64,
                candidate_pairs: if execution_kind == JoinExecutionKind::NestedLoop {
                    left_rows.saturating_mul(right_rows)
                } else {
                    0
                },
                wall_nanos: started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                ..JoinExecutionRecord::default()
            },
        );

        Ok(JoinResult { rows, columns })
    }

    /// Execute a streaming hash join where probe side streams from an operator.
    ///
    /// This is the optimized path for LIMIT queries:
    /// - Build side is materialized (required for hash table)
    /// - Probe side streams row-by-row (O(1) memory)
    /// - Early termination stops probe scan immediately when LIMIT is reached
    ///
    /// # Performance
    ///
    /// For `SELECT ... JOIN ... LIMIT 10`:
    /// - **Old path**: Materialize 10M + 10M rows, then return 10
    /// - **This path**: Materialize 10M rows, stream until 10 matches
    ///
    /// Memory usage is halved, and early termination actually stops work.
    pub fn execute_streaming(&self, request: StreamingJoinRequest<'_>) -> Result<JoinResult> {
        let limit = request.limit;
        let preserve_deferred = request.projection.is_some();
        let ctx = request.ctx;
        let plan = self.prepare_streaming_hash_join(request)?;
        let (mut join_op, columns) = ObservedStreamingHashJoin::new(plan);

        let rows =
            self.execute_operator_with_filter(&mut join_op, limit, &[], preserve_deferred, ctx)?;
        instrumentation::record_join_rows_constructed(rows.owned_len() as u64);

        Ok(JoinResult { rows, columns })
    }

    /// Return a pull cursor over one hash edge without collecting its output.
    /// The cursor itself enforces LIMIT and closes the whole child pipeline on
    /// EOF, cancellation, error, early limit, or drop.
    pub fn execute_streaming_result(
        &self,
        request: StreamingJoinRequest<'_>,
    ) -> Result<StreamingJoinResult> {
        let limit = request.limit;
        let cancellation = request.ctx.cancellation_handle();

        // A large equality edge uses the same pull boundary as the serial hash
        // path, but advances one bounded probe batch in parallel. No complete
        // result or materialized probe relation is created. Residual predicates
        // and semi/anti joins remain on the serial operator until their complete
        // match-state is represented by this parallel cursor as well.
        let (left_columns, right_columns) = if request.build_is_left {
            (
                request.build_columns.to_vec(),
                request.probe_columns.clone(),
            )
        } else {
            (
                request.probe_columns.clone(),
                request.build_columns.to_vec(),
            )
        };
        let analysis = self.analyze(
            &left_columns,
            &right_columns,
            request.condition,
            request.join_type,
        );
        let config = ParallelConfig::default();
        #[cfg(any(test, feature = "test-failpoints"))]
        let force_serial = radixdb_storage::test_failpoints::force_serial_execution();
        #[cfg(not(any(test, feature = "test-failpoints")))]
        let force_serial = false;
        #[cfg(any(test, feature = "test-failpoints"))]
        let threshold_allows = radixdb_storage::test_failpoints::force_parallel_execution()
            || config.should_parallel_join(request.build_rows.len());
        #[cfg(not(any(test, feature = "test-failpoints")))]
        let threshold_allows = config.should_parallel_join(request.build_rows.len());
        let parallel_eligible = !force_serial
            && request.pre_built_hash_state.is_none()
            && threshold_allows
            && limit.is_none_or(|limit| limit > STREAMING_LIMIT_THRESHOLD)
            && !analysis.left_key_indices.is_empty()
            && analysis.residual_conditions.is_empty()
            && matches!(
                analysis.join_type,
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
            );
        if parallel_eligible {
            let track_build_matches = analysis
                .join_type
                .needs_unmatched_build(request.build_is_left);
            let state_bytes =
                parallel_join_state_retained_bytes(request.build_rows.len(), track_build_matches)
                    .ok_or_else(|| {
                    radixdb_core::Error::invalid_argument(
                        "parallel join state exceeds its addressable memory range",
                    )
                })?;
            if let Some(state_reservation) = request.ctx.reserve_join_memory(state_bytes) {
                if let Some(batch_reservation) = request
                    .ctx
                    .reserve_join_memory(config.join_output_batch_bytes)
                {
                    let build_row_count = request.build_rows.len() as u64;
                    #[cfg(any(test, feature = "test-failpoints"))]
                    radixdb_storage::test_failpoints::record_execution_path(9);
                    let mut all_columns = left_columns.clone();
                    all_columns.extend(right_columns.iter().cloned());
                    let columns = request
                        .projection
                        .map(|projection| projection.output_columns.clone())
                        .unwrap_or(all_columns);
                    let (build_key_indices, probe_key_indices) = if request.build_is_left {
                        (
                            analysis.left_key_indices.clone(),
                            analysis.right_key_indices.clone(),
                        )
                    } else {
                        (
                            analysis.right_key_indices.clone(),
                            analysis.left_key_indices.clone(),
                        )
                    };
                    let operator = ParallelHashJoinOperator::new(
                        request.probe_source,
                        request.build_rows,
                        build_key_indices,
                        probe_key_indices,
                        analysis.join_type,
                        request.build_is_left,
                        left_columns.len(),
                        right_columns.len(),
                        columns.clone(),
                        request
                            .projection
                            .map(|projection| projection.columns.clone()),
                        config,
                        cancellation.clone(),
                        state_bytes,
                        state_reservation,
                        batch_reservation,
                    )?;
                    let operator = ObservedParallelHashJoin {
                        inner: operator,
                        started: std::time::Instant::now(),
                        build_row_count,
                        build_is_left: request.build_is_left,
                        left_width: left_columns.len() as u64,
                        right_width: right_columns.len() as u64,
                        output_width: columns.len() as u64,
                        output_rows: 0,
                        recorded: false,
                    };
                    let columns = CompactArc::new(columns);
                    let result = OperatorExecutorResult::open(
                        CompactArc::clone(&columns),
                        Box::new(operator),
                        cancellation,
                        limit,
                    )?;
                    return Ok((Box::new(result), columns));
                }
            }
        }

        #[cfg(any(test, feature = "test-failpoints"))]
        radixdb_storage::test_failpoints::record_execution_path(8);
        let plan = self.prepare_streaming_hash_join(request)?;
        let (join_op, columns) = ObservedStreamingHashJoin::new(plan);
        let columns = CompactArc::new(columns);
        let result = OperatorExecutorResult::open(
            CompactArc::clone(&columns),
            Box::new(join_op),
            cancellation,
            limit,
        )?;
        Ok((Box::new(result), columns))
    }

    fn prepare_streaming_hash_join(
        &self,
        request: StreamingJoinRequest<'_>,
    ) -> Result<PreparedStreamingHashJoin> {
        let started = std::time::Instant::now();
        let build_row_count = request.build_rows.len() as u64;
        // Build combined column list based on build side position
        let (left_columns, right_columns) = if request.build_is_left {
            (
                request.build_columns.to_vec(),
                request.probe_columns.clone(),
            )
        } else {
            (
                request.probe_columns.clone(),
                request.build_columns.to_vec(),
            )
        };

        let mut all_columns = left_columns.clone();
        all_columns.extend(right_columns.iter().cloned());

        // Analyze the join condition
        let analysis = self.analyze(
            &left_columns,
            &right_columns,
            request.condition,
            request.join_type,
        );

        // Probe side is already a streaming operator
        let probe_op = request.probe_source;

        let build_side = if request.build_is_left {
            JoinSide::Left
        } else {
            JoinSide::Right
        };

        let build_key_indices = if request.build_is_left {
            &analysis.left_key_indices
        } else {
            &analysis.right_key_indices
        };
        let build_batch = request.build_rows;
        let retain_hash_for_reuse = CompactArc::strong_count(&build_batch) > 1;
        let hash_state = match request.pre_built_hash_state {
            Some(state) => {
                if !state.matches(&build_batch, build_key_indices) {
                    return Err(radixdb_core::Error::internal(
                        "pre-built join hash state does not match build rows and keys",
                    ));
                }
                Some(state)
            }
            None if !build_key_indices.is_empty() => request.ctx.join_hash_state_for(
                CompactArc::clone(&build_batch),
                build_key_indices,
                retain_hash_for_reuse,
            ),
            None => None,
        };

        // Equality build rows cross this boundary exactly once. The immutable
        // state is built directly over their CompactArc and reused for every
        // probe row; the former path moved all rows through a second Vec before
        // it could construct the same table.
        let mut join_op = if let Some(hash_state) = hash_state {
            HashJoinOperator::with_prebuilt(
                probe_op,
                hash_state,
                analysis.join_type,
                analysis.left_key_indices.clone(),
                analysis.right_key_indices.clone(),
                request.build_is_left,
                request.build_columns.len(),
            )?
        } else {
            // Standard path: build hash table during open()
            let build_schema: Vec<ColumnInfo> =
                request.build_columns.iter().map(ColumnInfo::new).collect();
            // Unwrap CompactArc if sole owner, otherwise clone (MaterializedOperator needs Vec<Row>)
            let build_rows_vec =
                CompactArc::try_unwrap(build_batch).unwrap_or_else(|arc| (*arc).clone());
            let build_op = Box::new(MaterializedOperator::new(build_rows_vec, build_schema));

            let (left_op, right_op): (Box<dyn Operator>, Box<dyn Operator>) =
                if request.build_is_left {
                    (build_op, probe_op)
                } else {
                    (probe_op, build_op)
                };

            HashJoinOperator::new(
                left_op,
                right_op,
                analysis.join_type,
                analysis.left_key_indices.clone(),
                analysis.right_key_indices.clone(),
                build_side,
            )
            // The common request owner already declined reservation. Force the
            // operator's O(1) scan fallback instead of spending the full limit
            // a second time outside request accounting.
            .with_hash_state_max_bytes(0)
        };

        let residual_filters = analysis
            .residual_conditions
            .iter()
            .map(|condition| {
                JoinFilter::new(condition, &left_columns, &right_columns, global_registry())
                    .map(|filter| filter.with_context(request.ctx))
            })
            .collect::<Result<Vec<_>>>()?;
        join_op = join_op.with_residual_filters(residual_filters);

        if let Some(proj) = request.projection {
            let projected_schema: Vec<ColumnInfo> =
                proj.output_columns.iter().map(ColumnInfo::new).collect();
            join_op = join_op.with_projection(proj.columns.clone(), projected_schema);
        }

        let columns = if let Some(proj) = request.projection {
            proj.output_columns.clone()
        } else {
            all_columns
        };

        Ok(PreparedStreamingHashJoin {
            operator: join_op,
            columns,
            started,
            build_row_count,
            build_is_left: request.build_is_left,
            left_width: left_columns.len() as u64,
            right_width: right_columns.len() as u64,
        })
    }

    /// Analyze join for algorithm selection and key extraction.
    ///
    /// Ordering is not discovered here. Merge eligibility comes from an
    /// explicit physical property supplied with the request.
    fn analyze(
        &self,
        left_columns: &[String],
        right_columns: &[String],
        condition: Option<&Expression>,
        join_type_str: &str,
    ) -> JoinAnalysis {
        let join_type = JoinType::parse(join_type_str);

        // Extract equality keys and residual conditions
        let (left_key_indices, right_key_indices, residual_conditions) =
            if let Some(cond) = condition {
                extract_join_keys_and_residual(cond, left_columns, right_columns)
            } else {
                (Vec::new(), Vec::new(), Vec::new())
            };

        JoinAnalysis {
            left_key_indices,
            right_key_indices,
            residual_conditions,
            join_type,
            join_type_str: join_type_str.to_uppercase(),
        }
    }

    /// Select optimal join algorithm based on analysis and cardinalities.
    ///
    /// This is the fallback algorithm selection when QueryPlanner doesn't
    /// provide an algorithm hint. Materialized rows are never rescanned merely
    /// to discover sortedness.
    fn select_algorithm(
        &self,
        analysis: &JoinAnalysis,
        left_rows: &[Row],
        right_rows: &[Row],
        merge_ordering_certified: bool,
    ) -> JoinConfig {
        let has_equality_keys = !analysis.left_key_indices.is_empty();

        // No equality keys -> must use nested loop
        if !has_equality_keys {
            return JoinConfig {
                algorithm: RuntimeJoinAlgorithm::NestedLoop,
                build_left: false,
            };
        }

        // Merge is valid only when both physical producers certify the exact
        // leading key order required by this edge.
        if merge_ordering_certified {
            return JoinConfig {
                algorithm: RuntimeJoinAlgorithm::MergeJoin,
                build_left: false,
            };
        }

        // Use hash join with build on smaller side
        // Exception: OUTER joins have restrictions on build side
        let join_type = &analysis.join_type_str;
        let build_left = if join_type.contains("LEFT") || join_type.contains("FULL") {
            // LEFT/FULL OUTER: must build on right (left rows must be preserved)
            false
        } else if join_type.contains("RIGHT") {
            // RIGHT OUTER: must build on left (right rows must be preserved)
            true
        } else {
            // INNER/CROSS: build on smaller side
            left_rows.len() <= right_rows.len()
        };

        JoinConfig {
            algorithm: RuntimeJoinAlgorithm::HashJoin,
            build_left,
        }
    }

    /// Convert a RuntimeJoinDecision from QueryPlanner to JoinConfig.
    ///
    /// This bridges the gap between the QueryPlanner's cost-based decisions and
    /// the executor's algorithm implementation.
    fn convert_runtime_decision(
        &self,
        decision: &RuntimeJoinDecision,
        analysis: &JoinAnalysis,
    ) -> JoinConfig {
        let build_left = match decision.algorithm {
            RuntimeJoinAlgorithm::HashJoin => {
                // Use swap_sides hint from QueryPlanner, but respect OUTER join constraints
                let join_type = &analysis.join_type_str;
                if join_type.contains("LEFT") || join_type.contains("FULL") {
                    // LEFT/FULL OUTER: must build on right (left rows must be preserved)
                    false
                } else if join_type.contains("RIGHT") {
                    // RIGHT OUTER: must build on left (right rows must be preserved)
                    true
                } else {
                    // INNER/CROSS: use QueryPlanner's decision based on cost analysis
                    // swap_sides=true means swap, so if left was smaller, build_left=true normally
                    // QueryPlanner computes swap_sides = right < left, so:
                    // - swap_sides=false means left <= right, build on left
                    // - swap_sides=true means right < left, build on right (inverted)
                    !decision.swap_sides
                }
            }
            _ => false, // build_left not used for merge/nested loop
        };

        JoinConfig {
            algorithm: decision.algorithm,
            build_left,
        }
    }

    /// Refine a planner row-count hint using the actual post-pushdown row
    /// shapes that the physical hash operator will retain.
    ///
    /// OUTER sides retain their established fail-closed orientation. INNER
    /// joins choose the smaller estimated resident build batch, so one wide
    /// row does not beat several narrow rows merely because its cardinality is
    /// smaller. Equal estimates preserve the planner choice.
    fn choose_hash_build_side(
        join_type: &JoinType,
        left_rows: &[Row],
        right_rows: &[Row],
        planner_build_left: bool,
    ) -> bool {
        match join_type {
            JoinType::Left | JoinType::Full => return false,
            JoinType::Right => return true,
            _ => {}
        }

        let left_bytes = Self::estimate_post_pushdown_bytes(left_rows);
        let right_bytes = Self::estimate_post_pushdown_bytes(right_rows);
        match left_bytes.cmp(&right_bytes) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => planner_build_left,
        }
    }

    fn estimate_post_pushdown_bytes(rows: &[Row]) -> u128 {
        let sampled_rows = rows.len().min(HASH_BUILD_WIDTH_SAMPLE_ROWS);
        if sampled_rows == 0 {
            return 0;
        }
        let sampled_bytes = rows.iter().take(sampled_rows).fold(0_u128, |total, row| {
            total.saturating_add(RetainedRowsBudget::estimate_row_bytes(row) as u128)
        });
        sampled_bytes
            .saturating_mul(rows.len() as u128)
            .div_ceil(sampled_rows as u128)
    }

    /// Execute hash join using streaming HashJoinOperator or parallel execution.
    ///
    /// Chooses between:
    /// - **Parallel hash join**: When build side exceeds threshold (10,000 rows) and
    ///   LIMIT is absent or large (> 1,000). Better for bulk analytics.
    /// - **Streaming Volcano**: When LIMIT is small (early termination benefit) or
    ///   data is below parallel threshold. Better for interactive queries.
    #[allow(clippy::too_many_arguments)]
    fn execute_hash_join(
        &self,
        left_rows: CompactArc<Vec<Row>>,
        right_rows: CompactArc<Vec<Row>>,
        analysis: &JoinAnalysis,
        left_columns: &[String],
        right_columns: &[String],
        build_left: bool,
        limit: Option<u64>,
        ctx: &ExecutionContext,
        projection: Option<&JoinProjectionIndices>,
    ) -> Result<(JoinRows, JoinExecutionKind)> {
        // Use schema for column counts (not row data - handles empty tables correctly)
        let left_col_count = left_columns.len();
        let right_col_count = right_columns.len();

        // Build combined column list for residual filter compilation
        let mut all_columns = left_columns.to_vec();
        all_columns.extend(right_columns.iter().cloned());

        // Decide between parallel and streaming execution
        // Parallel is beneficial when:
        // 1. Build side row count exceeds threshold (parallel overhead worthwhile)
        // 2. No small LIMIT (streaming benefits from early termination)
        let build_row_count = if build_left {
            left_rows.len()
        } else {
            right_rows.len()
        };
        #[cfg(any(test, feature = "test-failpoints"))]
        let force_serial = radixdb_storage::test_failpoints::force_serial_execution();
        #[cfg(not(any(test, feature = "test-failpoints")))]
        let force_serial = false;
        #[cfg(any(test, feature = "test-failpoints"))]
        let threshold_allows = radixdb_storage::test_failpoints::force_parallel_execution()
            || build_row_count >= DEFAULT_PARALLEL_JOIN_THRESHOLD;
        #[cfg(not(any(test, feature = "test-failpoints")))]
        let threshold_allows = build_row_count >= DEFAULT_PARALLEL_JOIN_THRESHOLD;
        let parallel_eligible = !force_serial
            && analysis.residual_conditions.is_empty()
            && threshold_allows
            && limit.is_none_or(|l| l > STREAMING_LIMIT_THRESHOLD);
        let parallel_memory_reservation = parallel_eligible
            .then(|| {
                parallel_join_state_retained_bytes(
                    build_row_count,
                    analysis.join_type.needs_unmatched_build(build_left),
                )
                .and_then(|bytes| ctx.reserve_join_memory(bytes))
            })
            .flatten();

        if let Some(_memory_reservation) = parallel_memory_reservation {
            #[cfg(any(test, feature = "test-failpoints"))]
            radixdb_storage::test_failpoints::record_execution_path(9);
            // Parallel execution path
            self.execute_hash_join_parallel(
                left_rows,
                right_rows,
                analysis,
                left_col_count,
                right_col_count,
                &all_columns,
                build_left,
                limit,
                projection,
                ctx,
            )
            .map(|rows| (JoinRows::Owned(rows), JoinExecutionKind::HashParallel))
        } else {
            #[cfg(any(test, feature = "test-failpoints"))]
            radixdb_storage::test_failpoints::record_execution_path(8);
            // Shared immutable relations (notably repeated CTE references)
            // retain one request-local hash state across binary edges.
            let build_rows_reusable = if build_left {
                CompactArc::strong_count(&left_rows) > 1
            } else {
                CompactArc::strong_count(&right_rows) > 1
            };
            let prebuilt_hash_state = if build_left {
                ctx.join_hash_state_for(
                    CompactArc::clone(&left_rows),
                    &analysis.left_key_indices,
                    build_rows_reusable,
                )
            } else {
                ctx.join_hash_state_for(
                    CompactArc::clone(&right_rows),
                    &analysis.right_key_indices,
                    build_rows_reusable,
                )
            };
            // Streaming Volcano execution path
            self.execute_hash_join_streaming(
                left_rows,
                right_rows,
                analysis,
                left_columns,
                right_columns,
                build_left,
                limit,
                ctx,
                projection,
                prebuilt_hash_state,
            )
            .map(|(rows, fallback)| {
                let kind = if fallback {
                    JoinExecutionKind::NestedLoop
                } else {
                    JoinExecutionKind::HashStreaming
                };
                (rows, kind)
            })
        }
    }

    /// Execute hash join using a fixed-width atomic table + Rayon.
    ///
    /// Uses parallel hash build and probe phases for bulk analytics workloads.
    /// Better for large datasets without small LIMIT constraints.
    #[allow(clippy::too_many_arguments)]
    fn execute_hash_join_parallel(
        &self,
        left_rows: CompactArc<Vec<Row>>,
        right_rows: CompactArc<Vec<Row>>,
        analysis: &JoinAnalysis,
        left_col_count: usize,
        right_col_count: usize,
        all_columns: &[String],
        build_left: bool,
        limit: Option<u64>,
        projection: Option<&JoinProjectionIndices>,
        ctx: &ExecutionContext,
    ) -> Result<RowVec> {
        let config = ParallelConfig::default();

        // Determine probe and build sides - use Arc slices directly (zero-copy)
        let (probe_slice, build_slice, probe_key_indices, build_key_indices, swapped) =
            if build_left {
                // Build on left: probe is right, build is left
                (
                    right_rows.as_slice(),
                    left_rows.as_slice(),
                    &analysis.right_key_indices,
                    &analysis.left_key_indices,
                    true, // swapped: left is build, right is probe
                )
            } else {
                // Build on right (default): probe is left, build is right
                (
                    left_rows.as_slice(),
                    right_rows.as_slice(),
                    &analysis.left_key_indices,
                    &analysis.right_key_indices,
                    false, // not swapped: left is probe, right is build
                )
            };

        let (probe_col_count, build_col_count) = if swapped {
            (right_col_count, left_col_count)
        } else {
            (left_col_count, right_col_count)
        };

        // Execute parallel hash join
        let cancellation = ctx.cancellation_handle();
        let result = parallel_hash_join_cancellable(
            probe_slice,
            build_slice,
            probe_key_indices,
            build_key_indices,
            analysis.join_type,
            probe_col_count,
            build_col_count,
            swapped,
            projection.map(|projection| projection.columns.as_slice()),
            &config,
            &cancellation,
            ctx.join_hash_state_max_bytes(),
        )?;

        // Wrap with synthetic row IDs for join results
        let mut rows: RowVec = result
            .rows
            .into_iter()
            .enumerate()
            .map(|(i, row)| (i as i64, row))
            .collect();

        // Apply residual conditions FIRST (before LIMIT)
        // This ensures correct semantics: filter matching rows, then limit
        let is_inner = !analysis.join_type_str.contains("LEFT")
            && !analysis.join_type_str.contains("RIGHT")
            && !analysis.join_type_str.contains("FULL");

        if !analysis.residual_conditions.is_empty() {
            if is_inner {
                // For INNER joins, simply filter rows
                for cond in &analysis.residual_conditions {
                    let filter = RowFilter::new(cond, all_columns)?.with_context(ctx);
                    filter.retain_checked(&mut rows)?;
                }
            } else {
                // For OUTER joins, need special NULL-padding handling
                rows = self.apply_residual_post_join(
                    rows,
                    &analysis.residual_conditions,
                    all_columns,
                    &analysis.join_type_str,
                    left_col_count,
                    right_col_count,
                    ctx,
                )?;
            }
        }

        // Apply LIMIT after filtering (correct order)
        if let Some(max) = limit {
            rows.truncate(max as usize);
        }

        Ok(rows)
    }

    /// Execute hash join using streaming Volcano-style operators.
    ///
    /// Uses iterator-based execution for low latency to first row and
    /// early termination with LIMIT.
    #[allow(clippy::too_many_arguments)]
    fn execute_hash_join_streaming(
        &self,
        left_rows: CompactArc<Vec<Row>>,
        right_rows: CompactArc<Vec<Row>>,
        analysis: &JoinAnalysis,
        left_columns: &[String],
        right_columns: &[String],
        build_left: bool,
        limit: Option<u64>,
        ctx: &ExecutionContext,
        projection: Option<&JoinProjectionIndices>,
        prebuilt_hash_state: Option<JoinHashState>,
    ) -> Result<(JoinRows, bool)> {
        // Build schema for operators from column names
        let left_schema: Vec<ColumnInfo> = left_columns.iter().map(ColumnInfo::new).collect();
        let right_schema: Vec<ColumnInfo> = right_columns.iter().map(ColumnInfo::new).collect();

        let mut join_op = if let Some(hash_state) = prebuilt_hash_state {
            let (probe_rows, probe_schema, build_col_count) = if build_left {
                (right_rows, right_schema, left_columns.len())
            } else {
                (left_rows, left_schema, right_columns.len())
            };
            HashJoinOperator::with_prebuilt(
                Box::new(MaterializedOperator::from_arc(probe_rows, probe_schema)),
                hash_state,
                analysis.join_type,
                analysis.left_key_indices.clone(),
                analysis.right_key_indices.clone(),
                build_left,
                build_col_count,
            )?
        } else {
            // Create input operators from Arc (unwraps if sole owner, clones if shared).
            let left_op = Box::new(MaterializedOperator::from_arc(left_rows, left_schema));
            let right_op = Box::new(MaterializedOperator::from_arc(right_rows, right_schema));
            let build_side = if build_left {
                JoinSide::Left
            } else {
                JoinSide::Right
            };
            HashJoinOperator::new(
                left_op,
                right_op,
                analysis.join_type,
                analysis.left_key_indices.clone(),
                analysis.right_key_indices.clone(),
                build_side,
            )
            // `prebuilt_hash_state == None` means the common request owner did
            // not admit another hash allocation. Preserve correctness through
            // the bounded scan fallback.
            .with_hash_state_max_bytes(0)
        };

        let residual_filters = analysis
            .residual_conditions
            .iter()
            .map(|condition| {
                JoinFilter::new(condition, left_columns, right_columns, global_registry())
                    .map(|filter| filter.with_context(ctx))
            })
            .collect::<Result<Vec<_>>>()?;
        join_op = join_op.with_residual_filters(residual_filters);

        if let Some(proj) = projection {
            let projected_schema: Vec<ColumnInfo> =
                proj.output_columns.iter().map(ColumnInfo::new).collect();
            join_op = join_op.with_projection(proj.columns.clone(), projected_schema);
        }

        // Execute with Volcano model
        let rows =
            self.execute_operator_with_filter(&mut join_op, limit, &[], projection.is_some(), ctx)?;
        Ok((rows, join_op.used_scan_fallback()))
    }

    /// Execute merge join for pre-sorted inputs using MergeJoinOperator.
    fn execute_merge_join(&self, request: MergeExecutionRequest<'_>) -> Result<RowVec> {
        let (left_columns, right_columns) = request.columns;
        // Build schema for operators
        let left_schema: Vec<ColumnInfo> = left_columns.iter().map(ColumnInfo::new).collect();
        let right_schema: Vec<ColumnInfo> = right_columns.iter().map(ColumnInfo::new).collect();

        // Unwrap CompactArc if sole owner, otherwise clone (MaterializedOperator needs Vec<Row>)
        let left_vec =
            CompactArc::try_unwrap(request.left_rows).unwrap_or_else(|arc| (*arc).clone());
        let right_vec =
            CompactArc::try_unwrap(request.right_rows).unwrap_or_else(|arc| (*arc).clone());

        // Create input operators - takes ownership, no clone
        let left_op = Box::new(
            MaterializedOperator::new(left_vec, left_schema).with_ordering(request.ordering.left),
        );
        let right_op = Box::new(
            MaterializedOperator::new(right_vec, right_schema)
                .with_ordering(request.ordering.right),
        );

        // Create merge join operator
        let mut merge_op = MergeJoinOperator::new(
            left_op,
            right_op,
            request.analysis.join_type,
            request.analysis.left_key_indices.clone(),
            request.analysis.right_key_indices.clone(),
        )
        .with_group_budget(
            RetainedRowsBudget::DEFAULT_MAX_ROWS,
            request.ctx.join_hash_state_max_bytes(),
        );

        // Execute with Volcano model (no residual filters for merge join currently)
        self.execute_operator_with_filter(&mut merge_op, request.limit, &[], false, request.ctx)
            .map(JoinRows::into_owned)
    }

    /// Execute nested loop join using NestedLoopJoinOperator.
    #[allow(clippy::too_many_arguments)]
    fn execute_nested_loop(
        &self,
        left_rows: CompactArc<Vec<Row>>,
        right_rows: CompactArc<Vec<Row>>,
        condition: Option<&Expression>,
        left_columns: &[String],
        right_columns: &[String],
        join_type_str: &str,
        limit: Option<u64>,
        projection: Option<&JoinProjectionIndices>,
        ctx: &ExecutionContext,
    ) -> Result<JoinRows> {
        // Build schema for operators
        let left_schema: Vec<ColumnInfo> = left_columns.iter().map(ColumnInfo::new).collect();
        let right_schema: Vec<ColumnInfo> = right_columns.iter().map(ColumnInfo::new).collect();

        // Unwrap CompactArc if sole owner, otherwise clone (MaterializedOperator needs Vec<Row>)
        let left_vec = CompactArc::try_unwrap(left_rows).unwrap_or_else(|arc| (*arc).clone());
        let right_vec = CompactArc::try_unwrap(right_rows).unwrap_or_else(|arc| (*arc).clone());

        // Create input operators - takes ownership, no clone
        let left_op = Box::new(MaterializedOperator::new(left_vec, left_schema));
        let right_op = Box::new(MaterializedOperator::new(right_vec, right_schema));

        // Convert join type string to enum
        let join_type = if join_type_str.contains("CROSS") {
            JoinType::Cross
        } else if join_type_str.contains("FULL") {
            JoinType::Full
        } else if join_type_str.contains("RIGHT") {
            JoinType::Right
        } else if join_type_str.contains("LEFT") {
            JoinType::Left
        } else {
            JoinType::Inner
        };

        // Create nested loop join operator
        let mut nl_op =
            NestedLoopJoinOperator::new(left_op, right_op, join_type, condition.cloned());
        if let Some(proj) = projection {
            let projected_schema: Vec<ColumnInfo> =
                proj.output_columns.iter().map(ColumnInfo::new).collect();
            nl_op = nl_op.with_projection(proj.columns.clone(), projected_schema);
        }

        // Execute with Volcano model
        self.execute_operator_with_filter(&mut nl_op, limit, &[], projection.is_some(), ctx)
    }

    /// Execute operator with Volcano model and optional residual filter.
    fn execute_operator_with_filter(
        &self,
        op: &mut dyn Operator,
        limit: Option<u64>,
        residual_filters: &[RowFilter],
        preserve_deferred: bool,
        ctx: &ExecutionContext,
    ) -> Result<JoinRows> {
        ctx.check_cancelled()?;
        if let Err(error) = op.open() {
            let _ = op.close();
            return Err(error);
        }
        let execution_result = (|| {
            let max_rows = limit.map(|l| l as usize).unwrap_or(usize::MAX);
            let mut rows = RowVec::with_capacity(max_rows.min(1000));
            let mut deferred_rows =
                preserve_deferred.then(|| Vec::with_capacity(max_rows.min(1000)));
            let mut row_id = 0i64;
            let mut visited_rows = 0_usize;
            let has_filters = !residual_filters.is_empty();

            loop {
                if visited_rows & 0xff == 0 {
                    ctx.check_cancelled()?;
                }
                let Some(row_ref) = op.next()? else {
                    break;
                };
                visited_rows = visited_rows.saturating_add(1);
                // Apply residual filters - specialized unrolling for common cases (1-4 filters)
                if has_filters {
                    let pass = match residual_filters.len() {
                        1 => residual_filters[0].matches_row_ref_checked(&row_ref)?,
                        2 => {
                            residual_filters[0].matches_row_ref_checked(&row_ref)?
                                && residual_filters[1].matches_row_ref_checked(&row_ref)?
                        }
                        3 => {
                            residual_filters[0].matches_row_ref_checked(&row_ref)?
                                && residual_filters[1].matches_row_ref_checked(&row_ref)?
                                && residual_filters[2].matches_row_ref_checked(&row_ref)?
                        }
                        4 => {
                            residual_filters[0].matches_row_ref_checked(&row_ref)?
                                && residual_filters[1].matches_row_ref_checked(&row_ref)?
                                && residual_filters[2].matches_row_ref_checked(&row_ref)?
                                && residual_filters[3].matches_row_ref_checked(&row_ref)?
                        }
                        _ => {
                            let mut all_pass = true;
                            for filter in residual_filters {
                                if !filter.matches_row_ref_checked(&row_ref)? {
                                    all_pass = false;
                                    break;
                                }
                            }
                            all_pass
                        }
                    };
                    if !pass {
                        continue;
                    }
                }

                if let Some(deferred_rows) = deferred_rows.as_mut() {
                    deferred_rows.push(row_ref.into_deferred());
                    if deferred_rows.len() >= max_rows {
                        break;
                    }
                    continue;
                }

                let row = row_ref.into_owned();

                rows.push((row_id, row));
                row_id += 1;

                // Early termination
                if rows.len() >= max_rows {
                    break;
                }
            }

            Ok(deferred_rows.map_or(JoinRows::Owned(rows), JoinRows::Deferred))
        })();
        let close_result = op.close();
        match (execution_result, close_result) {
            (Ok(rows), Ok(())) => Ok(rows),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    /// Apply residual conditions for OUTER joins.
    ///
    /// For OUTER joins, residual conditions need special handling:
    /// matched rows that fail residual should produce NULL-padded output.
    #[allow(clippy::too_many_arguments)]
    fn apply_residual_post_join(
        &self,
        mut rows: RowVec,
        residual: &[Expression],
        all_columns: &[String],
        join_type: &str,
        left_col_count: usize,
        right_col_count: usize,
        ctx: &ExecutionContext,
    ) -> Result<RowVec> {
        let is_left_outer = join_type.contains("LEFT");
        let is_right_outer = join_type.contains("RIGHT");
        let is_full_outer = join_type.contains("FULL");

        for cond in residual {
            let filter = RowFilter::new(cond, all_columns)?.with_context(ctx);

            if is_left_outer || is_right_outer || is_full_outer {
                // For OUTER joins, replace non-matching rows with NULL-padded versions
                let mut new_rows = RowVec::with_capacity(rows.len());
                for (row_id, row) in rows {
                    if filter.matches_checked(&row)? {
                        new_rows.push((row_id, row));
                    } else {
                        // Convert to NULL-padded row
                        if is_left_outer {
                            // Keep left, NULL right
                            let mut new_values: CompactVec<Value> =
                                CompactVec::with_capacity(left_col_count + right_col_count);
                            new_values.extend(row.iter().take(left_col_count).cloned());
                            new_values.extend(std::iter::repeat_n(NULL_VALUE, right_col_count));
                            new_rows.push((row_id, Row::from_compact_vec(new_values)));
                        } else if is_right_outer {
                            // NULL left, keep right
                            let mut new_values: CompactVec<Value> =
                                CompactVec::with_capacity(left_col_count + right_col_count);
                            new_values.extend(std::iter::repeat_n(NULL_VALUE, left_col_count));
                            new_values.extend(row.iter().skip(left_col_count).cloned());
                            new_rows.push((row_id, Row::from_compact_vec(new_values)));
                        } else {
                            // FULL OUTER - keep original for now
                            new_rows.push((row_id, row));
                        }
                    }
                }
                rows = new_rows;
            } else {
                // INNER join - just filter
                filter.retain_checked(&mut rows)?;
            }
        }

        Ok(rows)
    }
}

impl Default for JoinExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::{ColumnSource, MaterializedOperator};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct CountingOperator {
        rows: Vec<Row>,
        schema: Vec<ColumnInfo>,
        next_row: usize,
        next_calls: Arc<AtomicUsize>,
        opened: bool,
    }

    impl CountingOperator {
        fn new(rows: Vec<Row>, columns: &[String], next_calls: Arc<AtomicUsize>) -> Self {
            Self {
                rows,
                schema: columns.iter().map(ColumnInfo::new).collect(),
                next_row: 0,
                next_calls,
                opened: false,
            }
        }
    }

    impl Operator for CountingOperator {
        fn open(&mut self) -> Result<()> {
            self.opened = true;
            Ok(())
        }

        fn next(&mut self) -> Result<Option<RowRef>> {
            assert!(self.opened);
            self.next_calls.fetch_add(1, Ordering::Relaxed);
            let Some(row) = self.rows.get(self.next_row).cloned() else {
                return Ok(None);
            };
            self.next_row += 1;
            Ok(Some(RowRef::owned(row)))
        }

        fn close(&mut self) -> Result<()> {
            self.opened = false;
            Ok(())
        }

        fn schema(&self) -> &[ColumnInfo] {
            &self.schema
        }

        fn estimated_rows(&self) -> Option<usize> {
            Some(self.rows.len())
        }

        fn name(&self) -> &str {
            "Counting"
        }
    }

    fn make_rows(data: Vec<Vec<i64>>) -> Vec<Row> {
        data.into_iter()
            .map(|vals| Row::from_values(vals.into_iter().map(Value::integer).collect()))
            .collect()
    }

    #[test]
    fn streaming_result_does_not_collect_probe_before_downstream_pull() {
        use radixdb_sql::ast::{Identifier, InfixExpression};
        use radixdb_sql::token::{Position, Token, TokenType};

        let executor = JoinExecutor::new();
        let ctx = ExecutionContext::new();
        let left_columns = vec!["a.id".to_string(), "a.value".to_string()];
        let right_columns = vec!["b.id".to_string(), "b.value".to_string()];
        let condition = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Operator, "=", Position::default()),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "a.id", Position::default()),
                "a.id".to_string(),
            ))),
            "=".to_string(),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "b.id", Position::default()),
                "b.id".to_string(),
            ))),
        ));
        let projection = JoinProjectionIndices {
            columns: vec![ColumnSource::Outer(1), ColumnSource::Inner(1)],
            output_columns: vec!["value".to_string(), "dictionary".to_string()],
        };
        let next_calls = Arc::new(AtomicUsize::new(0));

        radixdb_storage::instrumentation::begin_join_execution_probe();
        let (mut result, columns) = executor
            .execute_streaming_result(StreamingJoinRequest {
                build_rows: CompactArc::new(make_rows(vec![vec![1, 100], vec![2, 200]])),
                build_columns: &right_columns,
                probe_source: Box::new(CountingOperator::new(
                    make_rows(vec![vec![1, 10], vec![2, 20]]),
                    &left_columns,
                    Arc::clone(&next_calls),
                )),
                probe_columns: left_columns,
                condition: Some(&condition),
                join_type: "INNER",
                build_is_left: false,
                limit: None,
                ctx: &ctx,
                pre_built_hash_state: None,
                projection: Some(&projection),
            })
            .unwrap();

        assert_eq!(&*columns, &["value".to_string(), "dictionary".to_string()]);
        assert_eq!(next_calls.load(Ordering::Relaxed), 0);
        assert!(result.next());
        assert_eq!(next_calls.load(Ordering::Relaxed), 1);
        let row = result.take_deferred_row();
        assert!(row.is_deferred());
        assert_eq!(row.get(0), Some(&Value::integer(10)));
        assert_eq!(row.get(1), Some(&Value::integer(100)));
        drop(result);
        assert_eq!(ctx.retained_join_memory_bytes(), 0);
        let probe = radixdb_storage::instrumentation::end_join_execution_probe();
        assert_eq!(probe.hash_streaming_calls, 1);
        assert_eq!(probe.output_rows, 1);
    }

    #[test]
    fn hash_build_side_uses_bounded_post_pushdown_bytes_not_only_row_count() {
        let wide_left = vec![Row::from_values(vec![
            Value::integer(1),
            Value::text("x".repeat(32 * 1024)),
        ])];
        let narrow_right = make_rows(vec![vec![1], vec![2], vec![3], vec![4]]);

        assert!(!JoinExecutor::choose_hash_build_side(
            &JoinType::Inner,
            &wide_left,
            &narrow_right,
            true,
        ));

        let narrow_left = make_rows(vec![vec![1], vec![2]]);
        let wide_right = vec![Row::from_values(vec![
            Value::integer(1),
            Value::text("y".repeat(32 * 1024)),
        ])];
        assert!(JoinExecutor::choose_hash_build_side(
            &JoinType::Inner,
            &narrow_left,
            &wide_right,
            false,
        ));
    }

    #[test]
    fn hash_build_side_keeps_outer_orientation() {
        let wide = vec![Row::from_values(vec![Value::text("z".repeat(32 * 1024))])];
        let narrow = make_rows(vec![vec![1]]);

        assert!(!JoinExecutor::choose_hash_build_side(
            &JoinType::Left,
            &wide,
            &narrow,
            true,
        ));
        assert!(JoinExecutor::choose_hash_build_side(
            &JoinType::Right,
            &narrow,
            &wide,
            false,
        ));
        assert!(!JoinExecutor::choose_hash_build_side(
            &JoinType::Full,
            &wide,
            &narrow,
            true,
        ));
    }

    #[test]
    fn test_inner_join() {
        let executor = JoinExecutor::new();
        let ctx = ExecutionContext::new();

        let left = make_rows(vec![vec![1, 10], vec![2, 20], vec![3, 30]]);
        let right = make_rows(vec![vec![1, 100], vec![3, 300]]);

        let left_cols = vec!["a.id".to_string(), "a.val".to_string()];
        let right_cols = vec!["b.id".to_string(), "b.data".to_string()];

        // Create equality condition: a.id = b.id
        use radixdb_sql::ast::{Identifier, InfixExpression};
        use radixdb_sql::token::{Position, Token, TokenType};

        let cond = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Operator, "=", Position::default()),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "a.id", Position::default()),
                "a.id".to_string(),
            ))),
            "=".to_string(),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "b.id", Position::default()),
                "b.id".to_string(),
            ))),
        ));

        let request = JoinRequest {
            left_rows: CompactArc::new(left),
            right_rows: CompactArc::new(right),
            left_columns: &left_cols,
            right_columns: &right_cols,
            condition: Some(&cond),
            join_type: "INNER",
            limit: None,
            ctx: &ctx,
            algorithm_hint: None,
            ordering: JoinInputOrderings::default(),
            projection: None,
        };

        let before = radixdb_storage::instrumentation::snapshot();
        let result = executor.execute(request).unwrap();
        let after = radixdb_storage::instrumentation::snapshot();

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.columns.len(), 4);
        assert!(
            after.join_rows_constructed >= before.join_rows_constructed.saturating_add(2),
            "materialized join rows must reach the engine instrumentation owner"
        );
        assert!(
            after.join_operator_calls >= before.join_operator_calls.saturating_add(1),
            "one physical JOIN execution must be published"
        );
        assert!(after.join_left_input_rows >= before.join_left_input_rows.saturating_add(3));
        assert!(after.join_right_input_rows >= before.join_right_input_rows.saturating_add(2));
        assert!(after.join_output_rows >= before.join_output_rows.saturating_add(2));
        assert!(after.join_max_output_width >= 4);
    }

    #[test]
    fn merge_requires_physical_ordering_certificates_without_row_rescan() {
        use radixdb_sql::ast::{Identifier, InfixExpression};
        use radixdb_sql::token::{Position, Token, TokenType};

        let executor = JoinExecutor::new();
        let ctx = ExecutionContext::new();
        let left = make_rows(vec![vec![1, 10], vec![2, 20], vec![3, 30]]);
        let right = make_rows(vec![vec![1, 100], vec![3, 300]]);
        let left_cols = vec!["a.id".to_string(), "a.val".to_string()];
        let right_cols = vec!["b.id".to_string(), "b.data".to_string()];
        let condition = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Operator, "=", Position::default()),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "a.id", Position::default()),
                "a.id".to_string(),
            ))),
            "=".to_string(),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "b.id", Position::default()),
                "b.id".to_string(),
            ))),
        ));

        radixdb_storage::instrumentation::begin_join_execution_probe();
        let unknown_result = executor
            .execute(JoinRequest {
                left_rows: CompactArc::new(left.clone()),
                right_rows: CompactArc::new(right.clone()),
                left_columns: &left_cols,
                right_columns: &right_cols,
                condition: Some(&condition),
                join_type: "INNER",
                limit: None,
                ctx: &ctx,
                algorithm_hint: None,
                ordering: JoinInputOrderings::default(),
                projection: None,
            })
            .unwrap();
        let unknown_probe = radixdb_storage::instrumentation::end_join_execution_probe();
        assert_eq!(unknown_result.rows.len(), 2);
        assert_eq!(unknown_probe.merge_calls, 0);
        assert_eq!(unknown_probe.hash_streaming_calls, 1);

        radixdb_storage::instrumentation::begin_join_execution_probe();
        let certified_result = executor
            .execute(JoinRequest {
                left_rows: CompactArc::new(left),
                right_rows: CompactArc::new(right),
                left_columns: &left_cols,
                right_columns: &right_cols,
                condition: Some(&condition),
                join_type: "INNER",
                limit: None,
                ctx: &ctx,
                algorithm_hint: None,
                ordering: JoinInputOrderings::new(
                    OrderingProperty::ascending_nulls_last(vec![0]),
                    OrderingProperty::ascending_nulls_last(vec![0]),
                ),
                projection: None,
            })
            .unwrap();
        let certified_probe = radixdb_storage::instrumentation::end_join_execution_probe();
        assert_eq!(certified_result.rows.len(), 2);
        assert_eq!(certified_probe.merge_calls, 1);
        assert_eq!(certified_probe.hash_streaming_calls, 0);
    }

    #[test]
    fn merge_hint_cannot_bypass_missing_ordering_certificate() {
        use radixdb_sql::ast::{Identifier, InfixExpression};
        use radixdb_sql::token::{Position, Token, TokenType};

        let executor = JoinExecutor::new();
        let ctx = ExecutionContext::new();
        let left_cols = vec!["a.id".to_string()];
        let right_cols = vec!["b.id".to_string()];
        let condition = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Operator, "=", Position::default()),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "a.id", Position::default()),
                "a.id".to_string(),
            ))),
            "=".to_string(),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "b.id", Position::default()),
                "b.id".to_string(),
            ))),
        ));
        let hint = RuntimeJoinDecision {
            algorithm: RuntimeJoinAlgorithm::MergeJoin,
            swap_sides: false,
            explanation: "test untrusted merge hint".to_string(),
        };

        radixdb_storage::instrumentation::begin_join_execution_probe();
        executor
            .execute(JoinRequest {
                left_rows: CompactArc::new(make_rows(vec![vec![1], vec![2]])),
                right_rows: CompactArc::new(make_rows(vec![vec![1], vec![2]])),
                left_columns: &left_cols,
                right_columns: &right_cols,
                condition: Some(&condition),
                join_type: "INNER",
                limit: None,
                ctx: &ctx,
                algorithm_hint: Some(&hint),
                ordering: JoinInputOrderings::default(),
                projection: None,
            })
            .unwrap();
        let probe = radixdb_storage::instrumentation::end_join_execution_probe();
        assert_eq!(probe.merge_calls, 0);
        assert_eq!(probe.hash_streaming_calls, 1);
    }

    #[test]
    fn oversized_merge_duplicate_group_falls_back_to_bounded_hash() {
        use radixdb_sql::ast::{Identifier, InfixExpression};
        use radixdb_sql::token::{Position, Token, TokenType};

        let executor = JoinExecutor::new();
        let hash_budget = crate::hash_table::JoinHashTable::estimated_retained_bytes(10).unwrap();
        let ctx = crate::context::ExecutionContextBuilder::new()
            .join_hash_state_max_bytes(hash_budget)
            .build();
        let left_cols = vec!["a.id".to_string()];
        let right_cols = vec!["b.id".to_string()];
        let condition = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Operator, "=", Position::default()),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "a.id", Position::default()),
                "a.id".to_string(),
            ))),
            "=".to_string(),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "b.id", Position::default()),
                "b.id".to_string(),
            ))),
        ));
        let hint = RuntimeJoinDecision {
            algorithm: RuntimeJoinAlgorithm::MergeJoin,
            swap_sides: false,
            explanation: "test pathological duplicate group".to_string(),
        };
        let ordering = JoinInputOrderings::new(
            OrderingProperty::ascending_nulls_last(vec![0]),
            OrderingProperty::ascending_nulls_last(vec![0]),
        );
        let duplicates = || make_rows((0..10).map(|_| vec![1]).collect());

        radixdb_storage::instrumentation::begin_join_execution_probe();
        let result = executor
            .execute(JoinRequest {
                left_rows: CompactArc::new(duplicates()),
                right_rows: CompactArc::new(duplicates()),
                left_columns: &left_cols,
                right_columns: &right_cols,
                condition: Some(&condition),
                join_type: "INNER",
                limit: None,
                ctx: &ctx,
                algorithm_hint: Some(&hint),
                ordering,
                projection: None,
            })
            .unwrap();
        let probe = radixdb_storage::instrumentation::end_join_execution_probe();

        assert_eq!(result.rows.len(), 100);
        assert_eq!(probe.merge_calls, 0);
        assert_eq!(probe.hash_streaming_calls, 1);
    }

    #[test]
    fn test_left_join() {
        let executor = JoinExecutor::new();
        let ctx = ExecutionContext::new();

        let left = make_rows(vec![vec![1, 10], vec![2, 20], vec![3, 30]]);
        let right = make_rows(vec![vec![1, 100]]);

        let left_cols = vec!["a.id".to_string(), "a.val".to_string()];
        let right_cols = vec!["b.id".to_string(), "b.data".to_string()];

        use radixdb_sql::ast::{Identifier, InfixExpression};
        use radixdb_sql::token::{Position, Token, TokenType};

        let cond = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Operator, "=", Position::default()),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "a.id", Position::default()),
                "a.id".to_string(),
            ))),
            "=".to_string(),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "b.id", Position::default()),
                "b.id".to_string(),
            ))),
        ));

        let request = JoinRequest {
            left_rows: CompactArc::new(left),
            right_rows: CompactArc::new(right),
            left_columns: &left_cols,
            right_columns: &right_cols,
            condition: Some(&cond),
            join_type: "LEFT",
            limit: None,
            ctx: &ctx,
            algorithm_hint: None,
            ordering: JoinInputOrderings::default(),
            projection: None,
        };

        let result = executor.execute(request).unwrap();

        // All 3 left rows should be preserved
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_early_termination() {
        let executor = JoinExecutor::new();
        let ctx = ExecutionContext::new();

        let left = make_rows(vec![vec![1], vec![2], vec![3]]);
        let right = make_rows(vec![vec![1], vec![2], vec![3]]);

        let left_cols = vec!["a.id".to_string()];
        let right_cols = vec!["b.id".to_string()];

        use radixdb_sql::ast::{Identifier, InfixExpression};
        use radixdb_sql::token::{Position, Token, TokenType};

        let cond = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Operator, "=", Position::default()),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "a.id", Position::default()),
                "a.id".to_string(),
            ))),
            "=".to_string(),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "b.id", Position::default()),
                "b.id".to_string(),
            ))),
        ));

        let request = JoinRequest {
            left_rows: CompactArc::new(left),
            right_rows: CompactArc::new(right),
            left_columns: &left_cols,
            right_columns: &right_cols,
            condition: Some(&cond),
            join_type: "INNER",
            limit: Some(1), // Only need 1 row
            ctx: &ctx,
            algorithm_hint: None,
            ordering: JoinInputOrderings::default(),
            projection: None,
        };

        let result = executor.execute(request).unwrap();

        // Should stop after 1 row
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_streaming_hash_join_applies_projection_boundary() {
        let executor = JoinExecutor::new();
        let ctx = ExecutionContext::new();

        let left = make_rows(vec![
            vec![1, 10, 1000],
            vec![2, 20, 2000],
            vec![3, 30, 3000],
        ]);
        let right = make_rows(vec![vec![1, 100, 9000], vec![3, 300, 7000]]);

        let left_cols = vec![
            "a.id".to_string(),
            "a.val".to_string(),
            "a.unused".to_string(),
        ];
        let right_cols = vec![
            "b.id".to_string(),
            "b.data".to_string(),
            "b.unused".to_string(),
        ];

        use radixdb_sql::ast::{Identifier, InfixExpression};
        use radixdb_sql::token::{Position, Token, TokenType};

        let cond = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Operator, "=", Position::default()),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "a.id", Position::default()),
                "a.id".to_string(),
            ))),
            "=".to_string(),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "b.id", Position::default()),
                "b.id".to_string(),
            ))),
        ));

        let projection = JoinProjectionIndices {
            columns: vec![ColumnSource::Outer(1), ColumnSource::Inner(1)],
            output_columns: vec!["val".to_string(), "data".to_string()],
        };

        let left_schema = left_cols.iter().map(ColumnInfo::new).collect();
        let request = StreamingJoinRequest {
            build_rows: CompactArc::new(right),
            build_columns: &right_cols,
            probe_source: Box::new(MaterializedOperator::new(left, left_schema)),
            probe_columns: left_cols.clone(),
            condition: Some(&cond),
            join_type: "INNER",
            build_is_left: false,
            limit: Some(10),
            ctx: &ctx,
            pre_built_hash_state: None,
            projection: Some(&projection),
        };

        let result = executor.execute_streaming(request).unwrap();

        assert_eq!(result.columns, vec!["val".to_string(), "data".to_string()]);
        let rows = result.rows.into_owned();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|(_, row)| row.len() == 2));
        assert_eq!(rows[0].1.get(0), Some(&Value::integer(10)));
        assert_eq!(rows[0].1.get(1), Some(&Value::integer(100)));
        assert_eq!(rows[1].1.get(0), Some(&Value::integer(30)));
        assert_eq!(rows[1].1.get(1), Some(&Value::integer(300)));
    }

    #[test]
    fn streaming_hash_state_keeps_empty_build_schema_for_left_join() {
        let executor = JoinExecutor::new();
        let ctx = ExecutionContext::new();
        let left = make_rows(vec![vec![1, 10], vec![2, 20]]);
        let right = CompactArc::new(Vec::new());
        let left_cols = vec!["a.id".to_string(), "a.value".to_string()];
        let right_cols = vec!["b.id".to_string(), "b.value".to_string()];

        use radixdb_sql::ast::{Identifier, InfixExpression};
        use radixdb_sql::token::{Position, Token, TokenType};
        let condition = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Operator, "=", Position::default()),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "a.id", Position::default()),
                "a.id".to_string(),
            ))),
            "=".to_string(),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "b.id", Position::default()),
                "b.id".to_string(),
            ))),
        ));
        let left_schema = left_cols.iter().map(ColumnInfo::new).collect();

        let result = executor
            .execute_streaming(StreamingJoinRequest {
                build_rows: right,
                build_columns: &right_cols,
                probe_source: Box::new(MaterializedOperator::new(left, left_schema)),
                probe_columns: left_cols,
                condition: Some(&condition),
                join_type: "LEFT",
                build_is_left: false,
                limit: None,
                ctx: &ctx,
                pre_built_hash_state: None,
                projection: None,
            })
            .unwrap();

        assert_eq!(result.columns.len(), 4);
        let rows = result.rows.into_owned();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|(_, row)| row.len() == 4));
        assert!(rows.iter().all(|(_, row)| row.get(2).unwrap().is_null()));
        assert!(rows.iter().all(|(_, row)| row.get(3).unwrap().is_null()));
    }

    #[test]
    fn streaming_hash_state_rejects_different_physical_batch() {
        let executor = JoinExecutor::new();
        let ctx = ExecutionContext::new();
        let build_rows = CompactArc::new(make_rows(vec![vec![1], vec![2]]));
        let unrelated_rows = CompactArc::new((*build_rows).clone());
        let state = JoinHashState::build(build_rows, &[0]);
        let left_cols = vec!["a.id".to_string()];
        let right_cols = vec!["b.id".to_string()];

        use radixdb_sql::ast::{Identifier, InfixExpression};
        use radixdb_sql::token::{Position, Token, TokenType};
        let condition = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Operator, "=", Position::default()),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "a.id", Position::default()),
                "a.id".to_string(),
            ))),
            "=".to_string(),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "b.id", Position::default()),
                "b.id".to_string(),
            ))),
        ));
        let left_schema = left_cols.iter().map(ColumnInfo::new).collect();

        let error = executor
            .execute_streaming(StreamingJoinRequest {
                build_rows: unrelated_rows,
                build_columns: &right_cols,
                probe_source: Box::new(MaterializedOperator::new(
                    make_rows(vec![vec![1]]),
                    left_schema,
                )),
                probe_columns: left_cols,
                condition: Some(&condition),
                join_type: "INNER",
                build_is_left: false,
                limit: None,
                ctx: &ctx,
                pre_built_hash_state: Some(state),
                projection: None,
            })
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("does not match build rows and keys"));
    }

    #[test]
    fn request_local_hash_state_is_built_once_for_one_shared_relation() {
        let executor = JoinExecutor::new();
        let ctx = ExecutionContext::new();
        let shared_build = CompactArc::new(make_rows(vec![vec![1, 100], vec![2, 200]]));
        let left_cols = vec!["a.id".to_string()];
        let right_cols = vec!["b.id".to_string(), "b.value".to_string()];

        use radixdb_sql::ast::{Identifier, InfixExpression};
        use radixdb_sql::token::{Position, Token, TokenType};
        let condition = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Operator, "=", Position::default()),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "a.id", Position::default()),
                "a.id".to_string(),
            ))),
            "=".to_string(),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "b.id", Position::default()),
                "b.id".to_string(),
            ))),
        ));

        radixdb_storage::instrumentation::begin_join_execution_probe();
        for _ in 0..2 {
            let result = executor
                .execute_streaming(StreamingJoinRequest {
                    build_rows: CompactArc::clone(&shared_build),
                    build_columns: &right_cols,
                    probe_source: Box::new(MaterializedOperator::new(
                        make_rows(vec![vec![1], vec![2]]),
                        left_cols.iter().map(ColumnInfo::new).collect(),
                    )),
                    probe_columns: left_cols.clone(),
                    condition: Some(&condition),
                    join_type: "INNER",
                    build_is_left: false,
                    limit: None,
                    ctx: &ctx,
                    pre_built_hash_state: None,
                    projection: None,
                })
                .unwrap();
            assert_eq!(result.rows.len(), 2);
        }
        let probe = radixdb_storage::instrumentation::end_join_execution_probe();
        assert_eq!(probe.hash_state_builds, 1);
        assert_eq!(probe.hash_state_reuses, 1);
    }

    #[test]
    fn streaming_hash_budget_falls_back_without_changing_left_join_results() {
        let executor = JoinExecutor::new();
        let ctx = crate::context::ExecutionContextBuilder::new()
            .join_hash_state_max_bytes(0)
            .build();
        let left = make_rows(vec![vec![1, 10], vec![2, 20], vec![3, 30]]);
        let right = CompactArc::new(make_rows(vec![vec![1, 100], vec![3, 300]]));
        let left_cols = vec!["a.id".to_string(), "a.value".to_string()];
        let right_cols = vec!["b.id".to_string(), "b.value".to_string()];

        use radixdb_sql::ast::{Identifier, InfixExpression};
        use radixdb_sql::token::{Position, Token, TokenType};
        let condition = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Operator, "=", Position::default()),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "a.id", Position::default()),
                "a.id".to_string(),
            ))),
            "=".to_string(),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "b.id", Position::default()),
                "b.id".to_string(),
            ))),
        ));
        let left_schema = left_cols.iter().map(ColumnInfo::new).collect();

        radixdb_storage::instrumentation::begin_join_execution_probe();
        let result = executor
            .execute_streaming(StreamingJoinRequest {
                build_rows: right,
                build_columns: &right_cols,
                probe_source: Box::new(MaterializedOperator::new(left, left_schema)),
                probe_columns: left_cols,
                condition: Some(&condition),
                join_type: "LEFT",
                build_is_left: false,
                limit: None,
                ctx: &ctx,
                pre_built_hash_state: None,
                projection: None,
            })
            .unwrap();
        let probe = radixdb_storage::instrumentation::end_join_execution_probe();

        let rows = result.rows.into_owned();
        assert_eq!(rows.len(), 3);
        assert!(rows
            .iter()
            .find(|(_, row)| row.get(0) == Some(&Value::integer(2)))
            .unwrap()
            .1
            .get(2)
            .unwrap()
            .is_null());
        assert_eq!(probe.hash_streaming_calls, 0);
        assert_eq!(probe.nested_loop_calls, 1);
        assert_eq!(probe.candidate_pairs, 6);
    }

    #[test]
    fn test_materialized_hash_join_applies_projection_boundary() {
        let _path = radixdb_storage::test_failpoints::ExecutionPathControlGuard::install(
            radixdb_storage::test_failpoints::ExecutionPathMode::ForceParallel,
        );
        let executor = JoinExecutor::new();
        let ctx = ExecutionContext::new();

        let left = make_rows(vec![
            vec![2, 20, 2000],
            vec![1, 10, 1000],
            vec![3, 30, 3000],
        ]);
        let right = make_rows(vec![vec![1, 100, 9000], vec![3, 300, 7000]]);

        let left_cols = vec![
            "a.id".to_string(),
            "a.val".to_string(),
            "a.unused".to_string(),
        ];
        let right_cols = vec![
            "b.id".to_string(),
            "b.data".to_string(),
            "b.unused".to_string(),
        ];

        use radixdb_sql::ast::{Identifier, InfixExpression};
        use radixdb_sql::token::{Position, Token, TokenType};

        let cond = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Operator, "=", Position::default()),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "a.id", Position::default()),
                "a.id".to_string(),
            ))),
            "=".to_string(),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "b.id", Position::default()),
                "b.id".to_string(),
            ))),
        ));

        let projection = JoinProjectionIndices {
            columns: vec![ColumnSource::Outer(1), ColumnSource::Inner(1)],
            output_columns: vec!["val".to_string(), "data".to_string()],
        };
        let decision = RuntimeJoinDecision {
            algorithm: RuntimeJoinAlgorithm::HashJoin,
            swap_sides: false,
            explanation: "test forces hash join".to_string(),
        };

        let request = JoinRequest {
            left_rows: CompactArc::new(left),
            right_rows: CompactArc::new(right),
            left_columns: &left_cols,
            right_columns: &right_cols,
            condition: Some(&cond),
            join_type: "INNER",
            limit: None,
            ctx: &ctx,
            algorithm_hint: Some(&decision),
            ordering: JoinInputOrderings::default(),
            projection: Some(&projection),
        };

        radixdb_storage::instrumentation::begin_join_execution_probe();
        let result = executor.execute(request).unwrap();
        let probe = radixdb_storage::instrumentation::end_join_execution_probe();

        assert_eq!(result.columns, vec!["val".to_string(), "data".to_string()]);
        let rows = result.rows.into_owned();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|(_, row)| row.len() == 2));
        assert_eq!(probe.hash_parallel_calls, 1);
        assert_eq!(probe.hash_streaming_calls, 0);
    }

    #[test]
    fn test_materialized_hash_join_filters_residual_before_projecting_deferred_row() {
        let executor = JoinExecutor::new();
        let ctx = ExecutionContext::new();

        let left = make_rows(vec![vec![1, 10, 1000], vec![2, 20, 2000]]);
        let right = make_rows(vec![vec![1, 100, 9000], vec![2, 15, 8000]]);

        let left_cols = vec![
            "a.id".to_string(),
            "a.val".to_string(),
            "a.unused".to_string(),
        ];
        let right_cols = vec![
            "b.id".to_string(),
            "b.data".to_string(),
            "b.unused".to_string(),
        ];

        use radixdb_sql::ast::{Identifier, InfixExpression};
        use radixdb_sql::token::{Position, Token, TokenType};

        let eq = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Operator, "=", Position::default()),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "a.id", Position::default()),
                "a.id".to_string(),
            ))),
            "=".to_string(),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "b.id", Position::default()),
                "b.id".to_string(),
            ))),
        ));
        let residual = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Operator, "<", Position::default()),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "a.val", Position::default()),
                "a.val".to_string(),
            ))),
            "<".to_string(),
            Box::new(Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, "b.data", Position::default()),
                "b.data".to_string(),
            ))),
        ));
        let cond = Expression::Infix(InfixExpression::new(
            Token::new(TokenType::Keyword, "AND", Position::default()),
            Box::new(eq),
            "AND".to_string(),
            Box::new(residual),
        ));

        let projection = JoinProjectionIndices {
            columns: vec![ColumnSource::Outer(1), ColumnSource::Inner(1)],
            output_columns: vec!["val".to_string(), "data".to_string()],
        };
        let decision = RuntimeJoinDecision {
            algorithm: RuntimeJoinAlgorithm::HashJoin,
            swap_sides: false,
            explanation: "test forces hash join".to_string(),
        };

        let request = JoinRequest {
            left_rows: CompactArc::new(left),
            right_rows: CompactArc::new(right),
            left_columns: &left_cols,
            right_columns: &right_cols,
            condition: Some(&cond),
            join_type: "INNER",
            limit: None,
            ctx: &ctx,
            algorithm_hint: Some(&decision),
            ordering: JoinInputOrderings::default(),
            projection: Some(&projection),
        };

        let result = executor.execute(request).unwrap();

        assert_eq!(result.columns, vec!["val".to_string(), "data".to_string()]);
        assert!(matches!(result.rows, JoinRows::Deferred(_)));
        let rows = result.rows.into_owned();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.len(), 2);
        assert_eq!(rows[0].1.get(0), Some(&Value::integer(10)));
        assert_eq!(rows[0].1.get(1), Some(&Value::integer(100)));
    }

    #[test]
    fn inner_residual_merge_candidate_uses_hash_instead_of_quadratic_fallback() {
        let executor = JoinExecutor::new();
        let ctx = ExecutionContext::new();
        let left = make_rows(vec![vec![1, 10], vec![2, 20]]);
        let right = make_rows(vec![vec![1, 100], vec![2, 15]]);
        let left_cols = vec!["a.id".to_string(), "a.val".to_string()];
        let right_cols = vec!["b.id".to_string(), "b.data".to_string()];

        use radixdb_sql::ast::{Identifier, InfixExpression};
        use radixdb_sql::token::{Position, Token, TokenType};
        let column = |name: &str| {
            Expression::Identifier(Identifier::new(
                Token::new(TokenType::Identifier, name, Position::default()),
                name.to_string(),
            ))
        };
        let infix = |left: Expression, op: &str, right: Expression| {
            Expression::Infix(InfixExpression::new(
                Token::new(TokenType::Operator, op, Position::default()),
                Box::new(left),
                op.to_string(),
                Box::new(right),
            ))
        };
        let condition = infix(
            infix(column("a.id"), "=", column("b.id")),
            "AND",
            infix(column("a.val"), "<", column("b.data")),
        );
        let decision = RuntimeJoinDecision {
            algorithm: RuntimeJoinAlgorithm::MergeJoin,
            swap_sides: false,
            explanation: "test sorted-input merge candidate".to_string(),
        };

        radixdb_storage::instrumentation::begin_join_execution_probe();
        let result = executor
            .execute(JoinRequest {
                left_rows: CompactArc::new(left),
                right_rows: CompactArc::new(right),
                left_columns: &left_cols,
                right_columns: &right_cols,
                condition: Some(&condition),
                join_type: "INNER",
                limit: None,
                ctx: &ctx,
                algorithm_hint: Some(&decision),
                ordering: JoinInputOrderings::default(),
                projection: None,
            })
            .unwrap();
        let probe = radixdb_storage::instrumentation::end_join_execution_probe();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(probe.hash_streaming_calls, 1);
        assert_eq!(probe.nested_loop_calls, 0);
    }

    #[test]
    fn test_materialized_nested_loop_applies_projection_boundary() {
        let executor = JoinExecutor::new();
        let ctx = ExecutionContext::new();

        let left = make_rows(vec![vec![1, 10], vec![2, 20]]);
        let right = make_rows(vec![vec![100, 1000], vec![200, 2000]]);

        let left_cols = vec!["a.id".to_string(), "a.val".to_string()];
        let right_cols = vec!["b.id".to_string(), "b.data".to_string()];

        let projection = JoinProjectionIndices {
            columns: vec![ColumnSource::Inner(1), ColumnSource::Outer(1)],
            output_columns: vec!["data".to_string(), "val".to_string()],
        };

        let request = JoinRequest {
            left_rows: CompactArc::new(left),
            right_rows: CompactArc::new(right),
            left_columns: &left_cols,
            right_columns: &right_cols,
            condition: None,
            join_type: "CROSS",
            limit: None,
            ctx: &ctx,
            algorithm_hint: None,
            ordering: JoinInputOrderings::default(),
            projection: Some(&projection),
        };

        let result = executor.execute(request).unwrap();

        assert_eq!(result.columns, vec!["data".to_string(), "val".to_string()]);
        let rows = result.rows.into_owned();
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|(_, row)| row.len() == 2));
        assert_eq!(rows[0].1.get(0), Some(&Value::integer(1000)));
        assert_eq!(rows[0].1.get(1), Some(&Value::integer(10)));
    }

    #[test]
    fn test_cross_join() {
        let executor = JoinExecutor::new();
        let ctx = ExecutionContext::new();

        let left = make_rows(vec![vec![1], vec![2]]);
        let right = make_rows(vec![vec![10], vec![20]]);

        let left_cols = vec!["a.id".to_string()];
        let right_cols = vec!["b.val".to_string()];

        let request = JoinRequest {
            left_rows: CompactArc::new(left),
            right_rows: CompactArc::new(right),
            left_columns: &left_cols,
            right_columns: &right_cols,
            condition: None,
            join_type: "CROSS",
            limit: None,
            ctx: &ctx,
            algorithm_hint: None,
            ordering: JoinInputOrderings::default(),
            projection: None,
        };

        let result = executor.execute(request).unwrap();

        // 2 x 2 = 4 rows
        assert_eq!(result.rows.len(), 4);
    }
}
