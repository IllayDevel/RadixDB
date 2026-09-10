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

//! Schema-aware binding for read-only navigable-reference paths.
//!
//! This module is the only owner that assigns schema meaning to a parsed
//! multi-part identifier. It performs no lookup and reads no user rows.

use std::sync::{Arc, Mutex};

use rustc_hash::{FxHashMap, FxHashSet};

use crate::aggregation::{AggregationExecutorExt, AggregationHost};
use crate::context::ExecutionContext;
use crate::expression::RowFilter;
use crate::mutation::host::ActiveTransaction;
use crate::operators::reference_unique_lookup::{
    execute_unique_lookup_join_batch, materialize_unique_lookup_candidates, LookupEdgeCardinality,
    LookupEdgeFallback, UniqueLookupIntegrity, UniqueLookupRows,
};
use crate::result::ExecutorResult;
use crate::utils::{combine_predicates_with_and, flatten_and_predicates, RetainedRowsBudget};
use radixdb_core::{
    DataType, Error, NavigationErrorCode, ReferenceTargetKey, Result, Row, RowVec, SchemaColumnId,
    SchemaTableId, Value,
};
use radixdb_sql::ast::{
    Expression, GroupByClause, GroupByModifier, Identifier, InfixExpression, InfixOperator,
    JoinTableSource, NullLiteral, QualifiedIdentifier, SelectStatement, SimpleTableSource,
    Statement, WindowFrame, WindowFrameBound,
};
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::traits::{Engine, QueryResult, ScanPlan, Table};

mod binding;
mod execution;
mod rewrite;

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub type SourceMaterializedTestHook =
    Arc<dyn Fn(&ReferenceExpandPlan, &ExecutionContext) + Send + Sync>;

#[cfg(any(test, feature = "test-hooks"))]
static SOURCE_MATERIALIZED_TEST_HOOK: std::sync::LazyLock<
    Mutex<Option<SourceMaterializedTestHook>>,
> = std::sync::LazyLock::new(|| Mutex::new(None));

#[cfg(any(test, feature = "test-hooks"))]
static SOURCE_MATERIALIZED_TEST_HOOK_OWNER: Mutex<()> = Mutex::new(());

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn run_source_materialized_test_hook(plan: &ReferenceExpandPlan, context: &ExecutionContext) {
    let hook = SOURCE_MATERIALIZED_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    if let Some(hook) = hook {
        hook(plan, context);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub struct SourceMaterializedTestHookGuard {
    _owner: std::sync::MutexGuard<'static, ()>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl SourceMaterializedTestHookGuard {
    #[doc(hidden)]
    pub fn install(hook: SourceMaterializedTestHook) -> Self {
        let owner = SOURCE_MATERIALIZED_TEST_HOOK_OWNER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *SOURCE_MATERIALIZED_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
        Self { _owner: owner }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for SourceMaterializedTestHookGuard {
    fn drop(&mut self) {
        *SOURCE_MATERIALIZED_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

pub use binding::{
    bind_navigation_paths, bind_reference_expand_plan, bind_reference_expand_plan_for_execution,
    reject_navigation_in_write_statement,
};
#[doc(hidden)]
pub use execution::verify_unique_lookup_integrity;

/// Narrow composition contract for navigation execution. Binding, graph
/// planning and lookup policy remain in this crate; the host supplies only
/// recursive SELECT/projection callbacks and transaction ownership.
pub trait NavigationHost: AggregationHost {
    fn navigation_engine(&self) -> &Arc<MVCCEngine>;
    fn navigation_active_transaction(&self) -> &Mutex<Option<ActiveTransaction>>;
    fn navigation_execute_select(
        &self,
        statement: &SelectStatement,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>>;
    fn navigation_project_rows_with_alias(
        &self,
        select_expressions: &[Expression],
        rows: RowVec,
        columns: &[String],
        columns_lower: Option<&[String]>,
        context: &ExecutionContext,
        table_alias: Option<&str>,
    ) -> Result<RowVec>;
    fn navigation_source_materialized(
        &self,
        _plan: &ReferenceExpandPlan,
        _context: &ExecutionContext,
    ) {
    }
}

/// Single owner for navigation graph execution and reference lookups.
pub struct NavigationExecutor<'a, H: NavigationHost + ?Sized> {
    host: &'a H,
}

impl<'a, H: NavigationHost + ?Sized> NavigationExecutor<'a, H> {
    fn new(host: &'a H) -> Self {
        Self { host }
    }
}

/// Internal call surface between reference navigation and the SELECT owner.
pub trait NavigationExecutorExt: NavigationHost {
    fn execute_reference_projection(
        &self,
        select: &SelectStatement,
        plan: &ReferenceExpandPlan,
        context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        NavigationExecutor::new(self).execute_reference_projection(select, plan, context)
    }

    fn execute_reference_projection_with_metrics(
        &self,
        select: &SelectStatement,
        plan: &ReferenceExpandPlan,
        context: &ExecutionContext,
    ) -> Result<(Box<dyn QueryResult>, ReferenceExpandMetrics)> {
        NavigationExecutor::new(self)
            .execute_reference_projection_with_metrics(select, plan, context)
    }
}

impl<T: NavigationHost + ?Sized> NavigationExecutorExt for T {}

use execution::*;
use rewrite::*;

/// Query-local identity of one physical relation occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RootRelationInstance {
    ordinal: u32,
    table: SchemaTableId,
}

#[allow(dead_code)] // Later physical stages consume the full stable identity.
impl RootRelationInstance {
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }

    pub fn table(&self) -> &SchemaTableId {
        &self.table
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReferenceStepIdentity {
    source_column: SchemaColumnId,
    target_key_column: SchemaColumnId,
}

impl ReferenceExpandEdgeIdentity {
    /// Number of reference hops represented by this canonical edge.
    pub fn depth(&self) -> usize {
        self.steps.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceStep {
    identity: ReferenceStepIdentity,
    target_key: ReferenceTargetKey,
    source_nullable: bool,
}

#[allow(dead_code)] // Later physical stages consume nullable/source metadata.
impl ReferenceStep {
    pub fn source_column(&self) -> &SchemaColumnId {
        &self.identity.source_column
    }

    pub fn target_table(&self) -> &SchemaTableId {
        self.identity.target_key_column.table()
    }

    pub fn target_key_column(&self) -> &SchemaColumnId {
        &self.identity.target_key_column
    }

    pub fn target_key(&self) -> ReferenceTargetKey {
        self.target_key
    }

    pub fn source_nullable(&self) -> bool {
        self.source_nullable
    }
}

/// Canonical identity shared by all syntactic spellings of the same path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NavigationPathIdentity {
    root: RootRelationInstance,
    steps: Vec<ReferenceStepIdentity>,
    terminal_column: SchemaColumnId,
}

/// Bound, read-only navigation expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NavigationExpr {
    identity: NavigationPathIdentity,
    steps: Vec<ReferenceStep>,
    terminal_type: DataType,
    nullable: bool,
    display_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceSemantics {
    Left,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceIntegrityCheck {
    Required,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReferenceExpandEdgeIdentity {
    root: RootRelationInstance,
    steps: Vec<ReferenceStepIdentity>,
}

/// One deduplicated lookup edge in the logical navigation graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceExpandEdge {
    identity: ReferenceExpandEdgeIdentity,
    source_column: SchemaColumnId,
    target_key_column: SchemaColumnId,
    target_key: ReferenceTargetKey,
    required_columns: Vec<SchemaColumnId>,
    semantics: ReferenceSemantics,
    integrity_check: ReferenceIntegrityCheck,
}

#[allow(dead_code)] // EXPLAIN and later physical strategies consume all fields.
impl ReferenceExpandEdge {
    pub fn identity(&self) -> &ReferenceExpandEdgeIdentity {
        &self.identity
    }

    pub fn source_column(&self) -> &SchemaColumnId {
        &self.source_column
    }

    pub fn target_key_column(&self) -> &SchemaColumnId {
        &self.target_key_column
    }

    pub fn target_key(&self) -> ReferenceTargetKey {
        self.target_key
    }

    pub fn required_columns(&self) -> &[SchemaColumnId] {
        &self.required_columns
    }

    pub fn semantics(&self) -> ReferenceSemantics {
        self.semantics
    }

    pub fn integrity_check(&self) -> ReferenceIntegrityCheck {
        self.integrity_check
    }
}

/// One canonical terminal path and all equivalent syntactic occurrences.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceExpandPath {
    identity: NavigationPathIdentity,
    edge_indices: Vec<usize>,
    terminal_type: DataType,
    nullable: bool,
    display_paths: Vec<String>,
}

#[allow(dead_code)] // NR-09/NR-10 consume the remaining path metadata.
impl ReferenceExpandPath {
    pub fn identity(&self) -> &NavigationPathIdentity {
        &self.identity
    }

    pub fn edge_indices(&self) -> &[usize] {
        &self.edge_indices
    }

    pub fn terminal_type(&self) -> DataType {
        self.terminal_type
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn display_paths(&self) -> &[String] {
        &self.display_paths
    }
}

/// Logical semantic owner for all navigable-reference paths in one statement.
///
/// Construction performs only deterministic metadata folding: no target table
/// is opened and no key lookup is attempted.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceExpandPlan {
    schema_scope_id: u64,
    schema_generation: u64,
    paths: Vec<ReferenceExpandPath>,
    edges: Vec<ReferenceExpandEdge>,
}

#[doc(hidden)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum CachedReferenceExpand {
    #[default]
    Unknown,
    NoPaths {
        schema_scope_id: u64,
        schema_generation: u64,
    },
    Plan(ReferenceExpandPlan),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceLookupStrategy {
    DirectUnique,
    IndexNestedLoop,
    UniqueBatch,
    TargetHashScan,
    MergeJoin,
    SnapshotScanFallback,
}

impl ReferenceLookupStrategy {
    fn explain_name(self) -> &'static str {
        match self {
            Self::DirectUnique => "direct_unique_lookup",
            Self::IndexNestedLoop => "index_nested_loop",
            Self::UniqueBatch => "unique_batch_lookup",
            Self::TargetHashScan => "target_hash_scan",
            Self::MergeJoin => "merge_join",
            Self::SnapshotScanFallback => "snapshot_scan_fallback",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceStorageMode {
    HotMvcc,
    ColdArtifact,
    HybridArtifactHot,
}

impl ReferenceStorageMode {
    fn explain_name(self) -> &'static str {
        match self {
            Self::HotMvcc => "hot_mvcc",
            Self::ColdArtifact => "cold_artifact",
            Self::HybridArtifactHot => "mixed_cold_artifact_hot",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceExecutionDirection {
    SourceFirst,
    TargetFirst,
}

impl ReferenceExecutionDirection {
    fn explain_name(self) -> &'static str {
        match self {
            Self::SourceFirst => "source_first",
            Self::TargetFirst => "target_first",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReferenceEdgeExecution {
    edge_index: usize,
    strategy: ReferenceLookupStrategy,
    storage_mode: ReferenceStorageMode,
    direction: ReferenceExecutionDirection,
    distinct_keys: usize,
    projected_columns: usize,
    reverse_source_index_eligible: bool,
    target_predicate_pushdown: bool,
    left_to_inner: bool,
    rejected_distinct_keys: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReferenceExpandMetrics {
    paths_planned: usize,
    paths_executed: usize,
    source_rows: usize,
    null_source_keys: usize,
    distinct_keys: usize,
    repeated_keys_eliminated: usize,
    lookup_batches: usize,
    lookup_hits: usize,
    lookup_misses: usize,
    direct_edges: usize,
    index_nested_loop_edges: usize,
    batch_edges: usize,
    hash_edges: usize,
    merge_edges: usize,
    fallback_edges: usize,
    target_first_edges: usize,
    hot_edges: usize,
    cold_edges: usize,
    hybrid_edges: usize,
    target_predicate_edges: usize,
    left_to_inner_edges: usize,
    target_predicate_keys_rejected: usize,
    planner_left_join_edges: usize,
    edge_executions: Vec<ReferenceEdgeExecution>,
}

#[allow(dead_code)] // NR-15 publishes these counters; NR-06 tests them directly.
impl ReferenceExpandMetrics {
    pub fn paths_planned(&self) -> usize {
        self.paths_planned
    }

    pub fn paths_executed(&self) -> usize {
        self.paths_executed
    }

    pub fn source_rows(&self) -> usize {
        self.source_rows
    }

    pub fn null_source_keys(&self) -> usize {
        self.null_source_keys
    }

    pub fn distinct_keys(&self) -> usize {
        self.distinct_keys
    }

    pub fn repeated_keys_eliminated(&self) -> usize {
        self.repeated_keys_eliminated
    }

    pub fn lookup_batches(&self) -> usize {
        self.lookup_batches
    }

    pub fn lookup_hits(&self) -> usize {
        self.lookup_hits
    }

    pub fn lookup_misses(&self) -> usize {
        self.lookup_misses
    }

    pub fn direct_edges(&self) -> usize {
        self.direct_edges
    }

    pub fn batch_edges(&self) -> usize {
        self.batch_edges
    }

    pub fn index_nested_loop_edges(&self) -> usize {
        self.index_nested_loop_edges
    }

    pub fn hash_edges(&self) -> usize {
        self.hash_edges
    }

    pub fn merge_edges(&self) -> usize {
        self.merge_edges
    }

    pub fn fallback_edges(&self) -> usize {
        self.fallback_edges
    }

    pub fn target_first_edges(&self) -> usize {
        self.target_first_edges
    }

    pub fn target_predicate_edges(&self) -> usize {
        self.target_predicate_edges
    }

    pub fn left_to_inner_edges(&self) -> usize {
        self.left_to_inner_edges
    }

    pub fn target_predicate_keys_rejected(&self) -> usize {
        self.target_predicate_keys_rejected
    }

    pub fn planner_left_join_edges(&self) -> usize {
        self.planner_left_join_edges
    }

    pub fn hot_edges(&self) -> usize {
        self.hot_edges
    }

    pub fn edge_projected_columns(&self, index: usize) -> Option<usize> {
        self.edge_executions
            .get(index)
            .map(|edge| edge.projected_columns)
    }

    pub fn edge_reverse_source_index_eligible(&self, index: usize) -> Option<bool> {
        self.edge_executions
            .get(index)
            .map(|edge| edge.reverse_source_index_eligible)
    }

    fn instrumentation(&self) -> radixdb_storage::instrumentation::NavigationQueryCounters {
        radixdb_storage::instrumentation::NavigationQueryCounters {
            paths_planned: self.paths_planned as u64,
            paths_executed: self.paths_executed as u64,
            source_rows: self.source_rows as u64,
            distinct_source_keys: self.distinct_keys as u64,
            repeated_keys_eliminated: self.repeated_keys_eliminated as u64,
            lookup_batches: self.lookup_batches as u64,
            lookup_hits: self.lookup_hits as u64,
            lookup_misses: self.lookup_misses as u64,
            direct_edges: self.direct_edges as u64,
            index_nested_loop_edges: self.index_nested_loop_edges as u64,
            batch_edges: self.batch_edges as u64,
            hash_edges: self.hash_edges as u64,
            merge_edges: self.merge_edges as u64,
            fallback_edges: self.fallback_edges as u64,
            planner_left_join_edges: self.planner_left_join_edges as u64,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ReferenceGraphExecutionPlan {
    source_select: SelectStatement,
    augmented_columns: Vec<String>,
    source_column_positions: Vec<Option<usize>>,
    rewritten_where: Option<Expression>,
    rewritten_projection: Vec<Expression>,
    output_columns: Vec<String>,
    edge_key_positions: Vec<usize>,
    edge_key_types: Vec<DataType>,
    hidden_path_positions: Vec<usize>,
    target_predicates: Vec<Vec<Expression>>,
    table_alias: Option<String>,
    limit: Option<Box<Expression>>,
    offset: Option<Box<Expression>>,
    aggregation_select: Option<SelectStatement>,
}

const MAX_NAVIGATION_STEPS: usize = 8;
pub const MAX_NAVIGATION_PATHS: usize = 256;
const MAX_NAVIGATION_EDGES: usize = 512;

impl ReferenceExpandPlan {
    pub fn build(paths: Vec<NavigationExpr>) -> Result<Self> {
        if paths.len() > MAX_NAVIGATION_PATHS {
            return Err(Error::navigation(
                NavigationErrorCode::UnsupportedReferenceShape,
                format!(
                    "statement contains {} navigable paths; maximum is {MAX_NAVIGATION_PATHS}",
                    paths.len()
                ),
            ));
        }
        let schema_generation = paths
            .first()
            .map_or(0, |path| path.root_relation().table().schema_generation());
        let schema_scope_id = paths
            .first()
            .map_or(0, |path| path.root_relation().table().scope_id());
        let mut plan = Self {
            schema_scope_id,
            schema_generation,
            paths: Vec::new(),
            edges: Vec::new(),
        };
        let mut path_index = FxHashMap::default();
        let mut edge_index = FxHashMap::default();

        for path in paths {
            if path.root_relation().table().schema_generation() != schema_generation {
                return Err(Error::navigation(
                    NavigationErrorCode::SchemaChanged,
                    "bound navigation paths span more than one schema generation",
                ));
            }

            if let Some(&index) = path_index.get(path.identity()) {
                let existing: &mut ReferenceExpandPath = &mut plan.paths[index];
                if !existing.display_paths.contains(&path.display_path) {
                    existing.display_paths.push(path.display_path);
                }
                continue;
            }

            let mut edge_indices = Vec::with_capacity(path.steps.len());
            for step_index in 0..path.steps.len() {
                let identity = ReferenceExpandEdgeIdentity {
                    root: path.identity.root.clone(),
                    steps: path.identity.steps[..=step_index].to_vec(),
                };
                let required = if step_index + 1 < path.steps.len() {
                    path.steps[step_index + 1].source_column().clone()
                } else {
                    path.terminal_column().clone()
                };

                let index = if let Some(&index) = edge_index.get(&identity) {
                    let edge: &mut ReferenceExpandEdge = &mut plan.edges[index];
                    if !edge.required_columns.contains(&required) {
                        edge.required_columns.push(required);
                    }
                    index
                } else {
                    let step = &path.steps[step_index];
                    if plan.edges.len() == MAX_NAVIGATION_EDGES {
                        return Err(Error::navigation(
                            NavigationErrorCode::UnsupportedReferenceShape,
                            format!(
                                "navigation graph exceeds the {MAX_NAVIGATION_EDGES}-edge compile limit"
                            ),
                        ));
                    }
                    let index = plan.edges.len();
                    plan.edges.push(ReferenceExpandEdge {
                        identity: identity.clone(),
                        source_column: step.source_column().clone(),
                        target_key_column: step.target_key_column().clone(),
                        target_key: step.target_key(),
                        required_columns: vec![required],
                        semantics: ReferenceSemantics::Left,
                        integrity_check: ReferenceIntegrityCheck::Required,
                    });
                    edge_index.insert(identity, index);
                    index
                };
                edge_indices.push(index);
            }

            let identity = path.identity.clone();
            let index = plan.paths.len();
            plan.paths.push(ReferenceExpandPath {
                identity: identity.clone(),
                edge_indices,
                terminal_type: path.terminal_type,
                nullable: path.nullable,
                display_paths: vec![path.display_path],
            });
            path_index.insert(identity, index);
        }

        Ok(plan)
    }

    pub fn schema_generation(&self) -> u64 {
        self.schema_generation
    }

    pub fn schema_scope_id(&self) -> u64 {
        self.schema_scope_id
    }

    #[allow(dead_code)] // Test/EXPLAIN API retained for later path stages.
    pub fn paths(&self) -> &[ReferenceExpandPath] {
        &self.paths
    }

    #[allow(dead_code)] // NR-06 consumes the logical edge list.
    pub fn edges(&self) -> &[ReferenceExpandEdge] {
        &self.edges
    }

    pub fn validate(&self, engine: &dyn Engine) -> Result<()> {
        for edge in &self.edges {
            engine.validate_schema_table_id(edge.identity.root.table())?;
            engine.validate_schema_table_id(edge.source_column.table())?;
            engine.validate_schema_table_id(edge.target_key_column.table())?;
            for required in &edge.required_columns {
                engine.validate_schema_table_id(required.table())?;
            }
        }
        Ok(())
    }

    pub fn explain_lines(&self, engine: &dyn Engine) -> Result<Vec<String>> {
        self.explain_lines_with_metrics(engine, None)
    }

    pub fn explain_lines_with_metrics(
        &self,
        engine: &dyn Engine,
        metrics: Option<&ReferenceExpandMetrics>,
    ) -> Result<Vec<String>> {
        self.validate(engine)?;
        let mut lines = vec!["Reference Navigation".to_string()];
        lines.push(format!("  Schema Generation: {}", self.schema_generation));
        lines.push("  Semantics: LEFT".to_string());
        lines.push("  Snapshot: statement".to_string());
        lines.push("  Authorization: same_as_explicit_left_join".to_string());
        if let Some(metrics) = metrics {
            if metrics.planner_left_join_edges > 0 {
                lines.push(format!(
                    "  Counters: paths_planned={}, paths_executed={}, delegated_reference_edges={}",
                    metrics.paths_planned, metrics.paths_executed, metrics.planner_left_join_edges
                ));
                lines.push(format!(
                    "  Actual Work: delegated_to_join_executor, reference_edges={}",
                    metrics.planner_left_join_edges
                ));
            } else {
                lines.push(format!(
                    "  Counters: paths_planned={}, paths_executed={}, source_rows={}, distinct_source_keys={}, repeated_keys_eliminated={}, lookup_batches={}, target_lookup_hits={}, target_lookup_misses={}",
                    metrics.paths_planned,
                    metrics.paths_executed,
                    metrics.source_rows,
                    metrics.distinct_keys,
                    metrics.repeated_keys_eliminated,
                    metrics.lookup_batches,
                    metrics.lookup_hits,
                    metrics.lookup_misses
                ));
                lines.push(format!(
                    "  Strategies: direct={}, index_nested_loop={}, batch={}, hash={}, merge={}, fallback={}",
                    metrics.direct_edges,
                    metrics.index_nested_loop_edges,
                    metrics.batch_edges,
                    metrics.hash_edges,
                    metrics.merge_edges,
                    metrics.fallback_edges
                ));
                lines.push(format!(
                    "  Actual Work: source_rows={}, distinct_keys={}, lookup_batches={}, hits={}",
                    metrics.source_rows,
                    metrics.distinct_keys,
                    metrics.lookup_batches,
                    metrics.lookup_hits
                ));
                lines.push(format!(
                    "  Predicate Rewrite: target_edges={}, left_to_inner_edges={}, rejected_distinct_keys={}",
                    metrics.target_predicate_edges,
                    metrics.left_to_inner_edges,
                    metrics.target_predicate_keys_rejected
                ));
            }
        } else {
            lines.push(
                "  Physical Strategy: adaptive_unique_lookup (direct_unique_lookup | index_nested_loop | unique_batch_lookup | target_hash_scan)"
                    .to_string(),
            );
            lines.push("  Merge Join: disabled_without_ordering_certificate".to_string());
            lines.push("  Counters: available_with_explain_analyze".to_string());
        }
        for (index, edge) in self.edges.iter().enumerate() {
            let source_name = column_name(engine, edge.source_column())?;
            let target_name = column_name(engine, edge.target_key_column())?;
            let required = edge
                .required_columns()
                .iter()
                .map(|column| column_name(engine, column))
                .collect::<Result<Vec<_>>>()?;
            lines.push(format!(
                "  Edge {}: {}.{} -> {}.{}",
                index + 1,
                edge.source_column().table().table_name(),
                source_name,
                edge.target_key_column().table().table_name(),
                target_name
            ));
            lines.push(format!("    Required Columns: {}", required.join(", ")));
            lines.push("    Integrity Check: enabled".to_string());
            if let Some(execution) = metrics.and_then(|metrics| {
                metrics
                    .edge_executions
                    .iter()
                    .find(|execution| execution.edge_index == index)
            }) {
                lines.push(format!(
                    "    Actual Strategy: {}",
                    execution.strategy.explain_name()
                ));
                lines.push(format!(
                    "    Storage Mode: {}",
                    execution.storage_mode.explain_name()
                ));
                lines.push(format!(
                    "    Physical Direction: {}",
                    execution.direction.explain_name()
                ));
                lines.push(format!("    Distinct Keys: {}", execution.distinct_keys));
                lines.push(format!(
                    "    Target Projection Columns: {}",
                    execution.projected_columns
                ));
                lines.push(format!(
                    "    Reverse Source Index: {}",
                    if execution.reverse_source_index_eligible {
                        "eligible_but_not_used_without_target_predicate"
                    } else {
                        "not_eligible"
                    }
                ));
                lines.push(format!(
                    "    Target Predicate Pushdown: {}",
                    if execution.target_predicate_pushdown {
                        "enabled"
                    } else {
                        "disabled"
                    }
                ));
                lines.push(format!(
                    "    LEFT-to-INNER: {}",
                    if execution.left_to_inner {
                        "proven_null_rejecting"
                    } else {
                        "disabled"
                    }
                ));
                if execution.target_predicate_pushdown {
                    lines.push(format!(
                        "    Rejected Distinct Keys: {}",
                        execution.rejected_distinct_keys
                    ));
                }
            } else if metrics.is_some_and(|metrics| metrics.planner_left_join_edges > 0) {
                lines.push("    Actual Strategy: planner_left_join".to_string());
                lines.push("    Storage Mode: delegated_to_join_executor".to_string());
            }
        }
        for (index, path) in self.paths.iter().enumerate() {
            lines.push(format!("  Path {}: {}", index + 1, path.display_paths()[0]));
            lines.push(format!(
                "    Steps: {}",
                path.edge_indices
                    .iter()
                    .map(|edge| (edge + 1).to_string())
                    .collect::<Vec<_>>()
                    .join(" -> ")
            ));
        }
        Ok(lines)
    }

    fn path_index_for_display(&self, display: &str) -> Option<usize> {
        self.paths
            .iter()
            .position(|path| path.display_paths.iter().any(|item| item == display))
    }

    fn prepare_graph_execution(
        &self,
        select: &SelectStatement,
        engine: &dyn Engine,
    ) -> Result<ReferenceGraphExecutionPlan> {
        let navigable_where = select
            .where_clause
            .as_deref()
            .filter(|expression| expression_contains_bound_navigation(expression, self));
        let graph_aggregation = self.can_execute_graph_aggregation(select);
        if !graph_aggregation
            && (select.distinct
                || !select.distinct_on.is_empty()
                || !select.group_by.columns.is_empty()
                || !matches!(select.group_by.modifier, GroupByModifier::None)
                || select.having.is_some()
                || !select.window_defs.is_empty()
                || !select.set_operations.is_empty()
                || select
                    .columns
                    .iter()
                    .any(crate::utils::expression_contains_aggregate))
        {
            return Err(Error::NotSupported(
                "navigable predicates in aggregate, DISTINCT, window, or set contexts require NR-10"
                    .to_string(),
            ));
        }
        if navigable_where.is_some_and(expression_contains_subquery) {
            return Err(Error::NotSupported(
                "subqueries combined with navigable predicates require NR-10".to_string(),
            ));
        }

        let table = single_root_table_source(select)?;
        let root = self
            .paths
            .first()
            .ok_or_else(|| Error::internal("ReferenceExpand plan has no paths"))?
            .identity
            .root
            .table();
        if !table.name.value().eq_ignore_ascii_case(root.table_name()) {
            return Err(Error::navigation(
                NavigationErrorCode::SchemaChanged,
                "navigation root no longer matches the physical source table",
            ));
        }
        let schema = engine.get_table_schema(root.table_name())?;
        let source_columns = schema.column_names_owned().to_vec();
        let visible_root = table
            .alias
            .as_ref()
            .unwrap_or(&table.name)
            .value()
            .to_string();
        let table_alias = table.alias.as_ref().map(|alias| alias.value().to_string());
        let projection_aliases: FxHashSet<String> = select
            .columns
            .iter()
            .filter_map(|expression| match expression {
                Expression::Aliased(aliased) => Some(aliased.alias.value_lower().to_string()),
                _ => None,
            })
            .collect();
        for order in &select.order_by {
            if matches!(order.expression, Expression::IntegerLiteral(_))
                || matches!(
                    &order.expression,
                    Expression::Identifier(identifier)
                        if projection_aliases.contains(identifier.value_lower())
                )
            {
                return Err(Error::NotSupported(
                    "ORDER BY projection alias/position with a navigable predicate requires NR-10"
                        .to_string(),
                ));
            }
        }

        // NR-08 owns WHERE only. A bound path in any remaining SELECT context
        // must still fail closed until the corresponding NR-10 lowering exists.
        let mut outside_where_and_projection = select.clone();
        outside_where_and_projection.columns.clear();
        outside_where_and_projection.where_clause = None;
        if graph_aggregation {
            outside_where_and_projection.group_by = GroupByClause::default();
            outside_where_and_projection.having = None;
        }
        let mut unsupported_path = None;
        radixdb_sql::ast::walk_select_tree(&outside_where_and_projection, &mut |expression| {
            if unsupported_path.is_none() {
                if let Expression::QualifiedIdentifier(path) = expression {
                    let display = path.to_string();
                    if self.path_index_for_display(&display).is_some() {
                        unsupported_path = Some(display);
                    }
                }
            }
        });
        if let Some(path) = unsupported_path {
            return Err(Error::NotSupported(format!(
                "navigable reference path '{path}' outside projection/WHERE requires NR-10"
            )));
        }

        let mut edge_key_names = Vec::with_capacity(self.edges.len());
        let mut occupied: FxHashSet<String> = source_columns
            .iter()
            .map(|column| column.to_lowercase())
            .collect();
        for edge_index in 0..self.edges.len() {
            let mut suffix = 0usize;
            let hidden = loop {
                let candidate = if suffix == 0 {
                    format!(
                        "__radix_reference_edge_{}_{}",
                        self.schema_scope_id, edge_index
                    )
                } else {
                    format!(
                        "__radix_reference_edge_{}_{}_{}",
                        self.schema_scope_id, edge_index, suffix
                    )
                };
                if occupied.insert(candidate.to_lowercase()) {
                    break candidate;
                }
                suffix = suffix.saturating_add(1);
            };
            edge_key_names.push(hidden);
        }
        let mut hidden_names = Vec::with_capacity(self.paths.len());
        for path_index in 0..self.paths.len() {
            let mut suffix = 0usize;
            let hidden = loop {
                let candidate = if suffix == 0 {
                    format!("__radix_reference_{}_{}", self.schema_scope_id, path_index)
                } else {
                    format!(
                        "__radix_reference_{}_{}_{}",
                        self.schema_scope_id, path_index, suffix
                    )
                };
                if occupied.insert(candidate.to_lowercase()) {
                    break candidate;
                }
                suffix = suffix.saturating_add(1);
            };
            hidden_names.push(hidden);
        }
        let rewritten_where = navigable_where
            .map(|where_clause| {
                rewrite_navigation_expression(where_clause, self, &hidden_names, &visible_root)
            })
            .transpose()?;

        let mut rewritten_projection = Vec::new();
        let mut output_columns = Vec::new();
        for (index, expression) in select.columns.iter().enumerate() {
            match expression {
                Expression::Star(_) => {
                    append_root_projection(
                        &mut rewritten_projection,
                        &mut output_columns,
                        &source_columns,
                        &select.token,
                    );
                }
                Expression::QualifiedStar(star)
                    if star.qualifier.eq_ignore_ascii_case(&visible_root) =>
                {
                    append_root_projection(
                        &mut rewritten_projection,
                        &mut output_columns,
                        &source_columns,
                        &select.token,
                    );
                }
                Expression::QualifiedStar(_) => {
                    return Err(Error::NotSupported(
                        "qualified star outside the navigation root requires NR-10".to_string(),
                    ));
                }
                _ => {
                    if !graph_aggregation
                        && expression_contains_bound_navigation(expression, self)
                        && !is_direct_navigation_projection(expression, self)
                    {
                        return Err(Error::NotSupported(
                            "navigable references inside projection expressions require NR-10"
                                .to_string(),
                        ));
                    }
                    rewritten_projection.push(rewrite_navigation_expression(
                        expression,
                        self,
                        &hidden_names,
                        &visible_root,
                    )?);
                    let output_name = match expression {
                        Expression::QualifiedIdentifier(path)
                            if self.path_index_for_display(&path.to_string()).is_some() =>
                        {
                            path.to_string()
                        }
                        _ => reference_output_name(expression, index),
                    };
                    output_columns.push(output_name);
                }
            }
        }

        let conjuncts = navigable_where
            .map(flatten_and_predicates)
            .unwrap_or_default();
        let source_predicates = conjuncts
            .iter()
            .filter(|predicate| !expression_contains_bound_navigation(predicate, self))
            .cloned()
            .collect();
        let mut target_predicates = vec![Vec::new(); self.edges.len()];
        for predicate in &conjuncts {
            if let Some(edge_index) = null_rejecting_target_edge(predicate, self) {
                target_predicates[edge_index].push(rewrite_navigation_expression(
                    predicate,
                    self,
                    &hidden_names,
                    &visible_root,
                )?);
            }
        }

        let edge_key_types = self
            .edges
            .iter()
            .map(|edge| {
                let schema = engine.get_table_schema(edge.source_column.table().table_name())?;
                schema
                    .get_column(edge.source_column.ordinal())
                    .map(|column| column.data_type)
                    .ok_or_else(|| {
                        Error::navigation(
                            NavigationErrorCode::SchemaChanged,
                            "navigation source column moved while preparing execution",
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        let aggregation_select = if graph_aggregation {
            let mut rewritten = select.clone();
            rewritten.columns = rewritten_projection.clone();
            rewritten.where_clause = None;
            for expression in &mut rewritten.group_by.columns {
                *expression =
                    rewrite_navigation_expression(expression, self, &hidden_names, &visible_root)?;
            }
            if let GroupByModifier::GroupingSets(sets) = &mut rewritten.group_by.modifier {
                for set in sets {
                    for expression in set {
                        *expression = rewrite_navigation_expression(
                            expression,
                            self,
                            &hidden_names,
                            &visible_root,
                        )?;
                    }
                }
            }
            if let Some(having) = &mut rewritten.having {
                **having =
                    rewrite_navigation_expression(having, self, &hidden_names, &visible_root)?;
            }
            Some(rewritten)
        } else {
            None
        };

        let source_projection_ordinals = if graph_aggregation {
            graph_aggregation_source_ordinals(
                &source_columns,
                &visible_root,
                root.table_name(),
                &self.edges,
                aggregation_select
                    .as_ref()
                    .expect("graph aggregation owns its rewritten SELECT"),
                rewritten_where.as_ref(),
            )
        } else {
            (0..source_columns.len()).collect()
        };
        let projected_source_columns = source_projection_ordinals
            .iter()
            .map(|&ordinal| source_columns[ordinal].clone())
            .collect::<Vec<_>>();
        let mut source_column_positions = vec![None; source_columns.len()];
        for (position, &ordinal) in source_projection_ordinals.iter().enumerate() {
            source_column_positions[ordinal] = Some(position);
        }
        let mut augmented_columns = projected_source_columns.clone();
        let edge_key_positions = edge_key_names
            .iter()
            .map(|name| {
                let position = augmented_columns.len();
                augmented_columns.push(name.clone());
                position
            })
            .collect::<Vec<_>>();
        let hidden_path_positions = hidden_names
            .iter()
            .map(|name| {
                let position = augmented_columns.len();
                augmented_columns.push(name.clone());
                position
            })
            .collect::<Vec<_>>();

        let mut source_select = select.clone();
        source_select.columns = projected_source_columns
            .iter()
            .map(|column| {
                Expression::Identifier(radixdb_sql::ast::Identifier::new(
                    select.token.clone(),
                    column.clone(),
                ))
            })
            .collect();
        if navigable_where.is_some() {
            source_select.where_clause =
                combine_predicates_with_and(source_predicates).map(Box::new);
        }
        source_select.limit = None;
        source_select.offset = None;
        if graph_aggregation {
            source_select.group_by = GroupByClause::default();
            source_select.having = None;
            source_select.order_by.clear();
        }

        Ok(ReferenceGraphExecutionPlan {
            source_select,
            augmented_columns,
            source_column_positions,
            rewritten_where,
            rewritten_projection,
            output_columns,
            edge_key_positions,
            edge_key_types,
            hidden_path_positions,
            target_predicates,
            table_alias,
            limit: select.limit.clone(),
            offset: select.offset.clone(),
            aggregation_select,
        })
    }

    /// Diagnostic projection width used by integration contracts without
    /// exposing the internal graph representation.
    #[doc(hidden)]
    pub fn graph_source_projection_len(
        &self,
        select: &SelectStatement,
        engine: &dyn Engine,
    ) -> Result<usize> {
        self.prepare_graph_execution(select, engine)
            .map(|graph| graph.source_select.columns.len())
    }

    fn can_execute_graph_aggregation(&self, select: &SelectStatement) -> bool {
        let has_aggregation = !select.group_by.columns.is_empty()
            || !matches!(select.group_by.modifier, GroupByModifier::None)
            || select.having.is_some()
            || select
                .columns
                .iter()
                .any(crate::utils::expression_contains_aggregate);
        if !has_aggregation
            || select.with.is_some()
            || !select.set_operations.is_empty()
            || !matches!(
                select.table_expr.as_deref(),
                Some(Expression::TableSource(_))
            )
            || select.distinct
            || !select.distinct_on.is_empty()
            || !select.window_defs.is_empty()
            || !select.order_by.is_empty()
            || select.limit.is_some()
            || select.offset.is_some()
        {
            return false;
        }

        !select.columns.iter().any(expression_contains_subquery)
            && !select
                .group_by
                .columns
                .iter()
                .any(expression_contains_subquery)
            && !matches!(
                &select.group_by.modifier,
                GroupByModifier::GroupingSets(sets)
                    if sets.iter().flatten().any(expression_contains_subquery)
            )
            && !select
                .having
                .as_deref()
                .is_some_and(expression_contains_subquery)
            && !select
                .where_clause
                .as_deref()
                .is_some_and(expression_contains_subquery)
    }

    fn requires_planner_left_join(&self, select: &SelectStatement) -> bool {
        if select.with.is_some()
            || !select.set_operations.is_empty()
            || !matches!(
                select.table_expr.as_deref(),
                Some(Expression::TableSource(_))
            )
            || select.distinct
            || !select.distinct_on.is_empty()
            || !select.group_by.columns.is_empty()
            || !matches!(select.group_by.modifier, GroupByModifier::None)
            || select.having.is_some()
            || !select.window_defs.is_empty()
            || select
                .columns
                .iter()
                .any(crate::utils::expression_contains_aggregate)
            || select.columns.iter().any(|expression| {
                expression_contains_bound_navigation(expression, self)
                    && !is_direct_navigation_projection(expression, self)
            })
            || select.where_clause.as_deref().is_some_and(|expression| {
                expression_contains_subquery(expression)
                    && expression_contains_bound_navigation(expression, self)
            })
        {
            return true;
        }

        let navigation_aliases: FxHashSet<String> = select
            .columns
            .iter()
            .enumerate()
            .filter(|(_, expression)| expression_contains_bound_navigation(expression, self))
            .map(|(index, expression)| match expression {
                Expression::Aliased(aliased) => aliased.alias.value_lower().to_string(),
                _ => (index + 1).to_string(),
            })
            .collect();
        select.order_by.iter().any(|order| {
            expression_contains_bound_navigation(&order.expression, self)
                || matches!(
                    &order.expression,
                    Expression::Identifier(identifier)
                        if navigation_aliases.contains(identifier.value_lower())
                )
                || matches!(
                    &order.expression,
                    Expression::IntegerLiteral(position)
                        if position.value > 0
                            && navigation_aliases.contains(&position.value.to_string())
                )
        })
    }

    fn lower_to_planner_left_joins(
        &self,
        select: &SelectStatement,
        engine: &dyn Engine,
    ) -> Result<SelectStatement> {
        self.validate(engine)?;
        let mut lowered = select.clone();
        let needs_correlated_visibility = select_has_correlated_navigation(select, self);
        let source = lowered.table_expr.take().ok_or_else(|| {
            Error::navigation(
                NavigationErrorCode::UnsupportedReferenceShape,
                "navigable references require a physical FROM root",
            )
        })?;
        let aliases = reference_edge_aliases(source.as_ref(), self, &select.token);
        let parent_edges = reference_parent_edges(self)?;
        let mut relation_ordinal = 0u32;
        lowered.table_expr = Some(Box::new(attach_reference_edges_to_sources(
            *source,
            self,
            engine,
            &aliases,
            &parent_edges,
            &mut relation_ordinal,
        )?));

        add_canonical_navigation_result_aliases(&mut lowered.columns, self)?;
        rewrite_select_navigation_current_scope(&mut lowered, self, engine, &aliases)?;
        if needs_correlated_visibility {
            retain_correlated_navigation_columns(&mut lowered, self, engine, &aliases)?;
        }
        Ok(lowered)
    }
}
