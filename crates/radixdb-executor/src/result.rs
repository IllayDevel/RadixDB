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

//! Execution Result Types
//!
//! This module provides result types for SQL query execution.

use crate::optimizer::workload::{global_workload_learner, QueryPattern};
use radixdb_core::CompactArc;
use radixdb_core::{Error, Result, Row, RowVec, Value};
use radixdb_sql::ast::Expression;
use radixdb_storage::traits::{
    DeferredRow, QueryResult, TypedBatchFallbackReason, TypedColumnBatch,
};
use rustc_hash::{FxHashMap, FxHasher};
use std::cell::OnceCell;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use super::context::{CancellationHandle, TimeoutGuard};
use super::expression::RowFilter;
use super::operator::Operator;
use crate::memory::RetainedRowsBudget;

/// Internal streaming output of SQL execution.
///
/// The public embedded cursor adapts this value at the API boundary; API row
/// and cursor types never implement the lower-level execution contract.
pub type ExecutionResult = Box<dyn QueryResult>;

// Compatibility path for existing executor consumers. The implementation is
// storage-owned because it adapts only Scanner to QueryResult.
pub use radixdb_storage::traits::{AliasedResult, ScannerResult};

/// Keeps a statement timeout registered until the returned cursor is closed
/// or dropped. Planning a streaming result is not query completion: rows may
/// still execute storage and operator work while the caller consumes them.
#[doc(hidden)]
pub struct TimedQueryResult {
    inner: Box<dyn QueryResult>,
    timeout_guard: Option<TimeoutGuard>,
    cancellation: CancellationHandle,
    cancelled_error_returned: bool,
    workload: Option<WorkloadObservation>,
    workload_started_at: radixdb_core::time_compat::Instant,
    workload_rows: u64,
}

#[derive(Clone, Copy)]
struct WorkloadObservation {
    fingerprint: u64,
    pattern: QueryPattern,
}

impl TimedQueryResult {
    #[cfg(test)]
    pub fn wrap(
        inner: ExecutionResult,
        timeout_guard: Option<TimeoutGuard>,
        cancellation: CancellationHandle,
    ) -> ExecutionResult {
        Box::new(Self {
            inner,
            timeout_guard,
            cancellation,
            cancelled_error_returned: false,
            workload: None,
            workload_started_at: radixdb_core::time_compat::Instant::now(),
            workload_rows: 0,
        })
    }

    pub fn wrap_with_workload(
        inner: ExecutionResult,
        timeout_guard: Option<TimeoutGuard>,
        cancellation: CancellationHandle,
        sql: &str,
    ) -> ExecutionResult {
        let upper = sql.to_ascii_uppercase();
        let pattern = if upper.trim_start().starts_with("INSERT") {
            QueryPattern::InsertHeavy
        } else if upper.trim_start().starts_with("UPDATE")
            || upper.trim_start().starts_with("DELETE")
        {
            QueryPattern::UpdateHeavy
        } else if upper.contains(" JOIN ") {
            QueryPattern::JoinHeavy
        } else if ["COUNT(", "SUM(", "AVG(", "MIN(", "MAX(", "GROUP BY"]
            .iter()
            .any(|needle| upper.contains(needle))
        {
            QueryPattern::Aggregation
        } else if upper.trim_start().starts_with("SELECT") {
            QueryPattern::FullScan
        } else {
            QueryPattern::Unknown
        };
        let mut hasher = FxHasher::default();
        sql.hash(&mut hasher);

        Box::new(Self {
            inner,
            timeout_guard,
            cancellation,
            cancelled_error_returned: false,
            workload: Some(WorkloadObservation {
                fingerprint: hasher.finish(),
                pattern,
            }),
            workload_started_at: radixdb_core::time_compat::Instant::now(),
            workload_rows: 0,
        })
    }

    fn finish_workload_observation(&mut self) {
        let Some(observation) = self.workload.take() else {
            return;
        };
        let affected = self.inner.rows_affected().max(0) as u64;
        let rows = self.workload_rows.max(affected);
        global_workload_learner().record_query(
            observation.fingerprint,
            observation.pattern,
            self.workload_started_at.elapsed(),
            0,
            rows,
            rows,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
    }
}

impl QueryResult for TimedQueryResult {
    fn columns(&self) -> &[String] {
        self.inner.columns()
    }

    fn columns_arc(&self) -> Option<CompactArc<Vec<String>>> {
        self.inner.columns_arc()
    }

    fn next(&mut self) -> bool {
        if self.cancellation.is_cancelled() {
            return false;
        }
        let has_row = self.inner.next();
        if has_row {
            self.workload_rows = self.workload_rows.saturating_add(1);
        } else {
            self.finish_workload_observation();
        }
        has_row
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        self.inner.scan(dest)
    }

    fn row(&self) -> &Row {
        self.inner.row()
    }

    fn take_row(&mut self) -> Row {
        self.inner.take_row()
    }

    fn take_deferred_row(&mut self) -> DeferredRow {
        self.inner.take_deferred_row()
    }

    fn preserves_deferred_rows(&self) -> bool {
        self.inner.preserves_deferred_rows()
    }

    fn close(&mut self) -> Result<()> {
        let result = self.inner.close();
        self.finish_workload_observation();
        self.timeout_guard.take();
        result
    }

    fn rows_affected(&self) -> i64 {
        self.inner.rows_affected()
    }

    fn last_insert_id(&self) -> i64 {
        self.inner.last_insert_id()
    }

    fn try_into_arc_rows(&mut self) -> Option<CompactArc<Vec<Row>>> {
        let rows = self.inner.try_into_arc_rows();
        if let Some(rows) = rows.as_ref() {
            self.workload_rows = self.workload_rows.saturating_add(rows.len() as u64);
            self.finish_workload_observation();
        }
        rows
    }

    fn estimated_count(&self) -> Option<usize> {
        self.inner.estimated_count()
    }

    fn supports_typed_batches(&self) -> bool {
        self.inner.supports_typed_batches()
    }

    fn typed_batch_fallback_reason(&self) -> Option<TypedBatchFallbackReason> {
        self.inner.typed_batch_fallback_reason()
    }

    fn next_typed_batch(&mut self) -> Result<Option<TypedColumnBatch>> {
        if self.cancellation.is_cancelled() {
            return Err(radixdb_core::Error::QueryCancelled);
        }
        let batch = self.inner.next_typed_batch()?;
        if let Some(batch) = batch.as_ref() {
            self.workload_rows = self.workload_rows.saturating_add(batch.row_count() as u64);
        } else {
            self.finish_workload_observation();
        }
        Ok(batch)
    }

    fn last_error(&mut self) -> Option<radixdb_core::Error> {
        if self.cancellation.is_cancelled() && !self.cancelled_error_returned {
            self.cancelled_error_returned = true;
            Some(radixdb_core::Error::QueryCancelled)
        } else {
            self.inner.last_error()
        }
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        let Self {
            inner,
            timeout_guard,
            cancellation,
            cancelled_error_returned,
            workload,
            workload_started_at,
            workload_rows,
        } = *self;
        Box::new(Self {
            inner: inner.with_aliases(aliases),
            timeout_guard,
            cancellation,
            cancelled_error_returned,
            workload,
            workload_started_at,
            workload_rows,
        })
    }
}

/// Execution result for DML operations (INSERT, UPDATE, DELETE)
///
/// This result type tracks the number of rows affected and the last insert ID
/// for auto-increment columns.
///
/// OPTIMIZATION: Uses static empty values for columns and empty_row to avoid
/// allocations on every DML operation (was causing 40K+ allocations per benchmark).
pub struct ExecResult {
    /// Number of rows affected
    affected: i64,
    /// Last insert ID (for auto-increment)
    insert_id: i64,
}

/// Static empty columns for ExecResult (avoids Vec allocation)
static EMPTY_COLUMNS: &[String] = &[];

/// Static empty row for ExecResult (avoids Row allocation)
static EMPTY_ROW: std::sync::OnceLock<Row> = std::sync::OnceLock::new();

#[inline]
fn get_empty_row() -> &'static Row {
    EMPTY_ROW.get_or_init(Row::new)
}

impl ExecResult {
    /// Create a new execution result
    #[inline]
    pub fn new(rows_affected: i64, last_insert_id: i64) -> Self {
        Self {
            affected: rows_affected,
            insert_id: last_insert_id,
        }
    }

    /// Create an empty result (for DDL statements)
    #[inline]
    pub fn empty() -> Self {
        Self::new(0, 0)
    }

    /// Create a result with just rows affected
    #[inline]
    pub fn with_rows_affected(rows_affected: i64) -> Self {
        Self::new(rows_affected, 0)
    }

    /// Create a result with rows affected and last insert ID
    #[inline]
    pub fn with_last_insert_id(rows_affected: i64, last_insert_id: i64) -> Self {
        Self::new(rows_affected, last_insert_id)
    }
}

impl QueryResult for ExecResult {
    fn columns(&self) -> &[String] {
        EMPTY_COLUMNS
    }

    fn next(&mut self) -> bool {
        // DML results have no rows
        false
    }

    fn scan(&self, _dest: &mut [Value]) -> Result<()> {
        Err(radixdb_core::Error::internal(
            "scan() called on exec result",
        ))
    }

    fn row(&self) -> &Row {
        get_empty_row()
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn rows_affected(&self) -> i64 {
        self.affected
    }

    fn last_insert_id(&self) -> i64 {
        self.insert_id
    }

    fn with_aliases(self: Box<Self>, _aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        self
    }
}

/// Memory-based result for SELECT queries
///
/// This result type stores all rows in memory, suitable for
/// small to medium result sets.
/// Storage for result rows - either owned (pooled) or shared via Arc
enum RowStorage {
    /// Owned rows - pooled, returns to thread-local cache on drop
    Owned(RowVec),
    /// Shared rows from cache - read-only, clone on take
    Shared(CompactArc<Vec<Row>>),
}

impl RowStorage {
    #[inline]
    fn len(&self) -> usize {
        match self {
            RowStorage::Owned(rv) => rv.len(),
            RowStorage::Shared(rows) => rows.len(),
        }
    }

    #[inline]
    fn get(&self, index: usize) -> Option<&Row> {
        match self {
            RowStorage::Owned(rv) => rv.get(index).map(|(_, row)| row),
            RowStorage::Shared(rows) => rows.get(index),
        }
    }

    #[inline]
    fn take(&mut self, index: usize) -> Row {
        match self {
            RowStorage::Owned(rv) => std::mem::take(&mut rv[index].1),
            // For shared storage, we must clone since we can't take ownership
            RowStorage::Shared(rows) => rows[index].clone(),
        }
    }
}

pub struct ExecutorResult {
    /// Column names (Arc for zero-copy sharing with API layer)
    columns: CompactArc<Vec<String>>,
    /// Result rows - either owned or shared
    rows: RowStorage,
    /// Cached row count to avoid repeated match in next()
    len: usize,
    /// Current row index (None before first next())
    current_index: Option<usize>,
    /// Whether the result is closed
    closed: bool,
    /// Rows affected (0 for SELECT)
    affected: i64,
    /// Last insert ID (0 for SELECT)
    insert_id: i64,
}

/// Internal result that carries compact JOIN rows to the next recursive edge.
///
/// Public consumers keep the ordinary QueryResult contract: `row()` and
/// `take_row()` materialize at most once. QueryResultOperator instead consumes
/// `take_deferred_row()` and preserves the row graph without cloning payload
/// columns between binary JOIN nodes.
#[doc(hidden)]
pub struct DeferredExecutorResult {
    columns: CompactArc<Vec<String>>,
    rows: Vec<Option<DeferredRow>>,
    current_index: Option<usize>,
    current_materialized: OnceCell<Row>,
    closed: bool,
}

/// Attach a physical ordering certificate to a result without changing rows.
///
/// Construction is restricted to executor paths that obtained rows from an
/// ordered storage/index API. The wrapper deliberately performs no runtime
/// sortedness check.
#[doc(hidden)]
pub struct CertifiedOrderedResult {
    inner: Box<dyn QueryResult>,
    ordering: Vec<usize>,
}

impl CertifiedOrderedResult {
    pub fn ascending_nulls_last(inner: Box<dyn QueryResult>, ordering: Vec<usize>) -> Self {
        Self { inner, ordering }
    }
}

impl QueryResult for CertifiedOrderedResult {
    fn columns(&self) -> &[String] {
        self.inner.columns()
    }

    fn columns_arc(&self) -> Option<CompactArc<Vec<String>>> {
        self.inner.columns_arc()
    }

    fn next(&mut self) -> bool {
        self.inner.next()
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        self.inner.scan(dest)
    }

    fn row(&self) -> &Row {
        self.inner.row()
    }

    fn take_row(&mut self) -> Row {
        self.inner.take_row()
    }

    fn take_deferred_row(&mut self) -> DeferredRow {
        self.inner.take_deferred_row()
    }

    fn preserves_deferred_rows(&self) -> bool {
        self.inner.preserves_deferred_rows()
    }

    fn ascending_nulls_last_ordering(&self) -> Option<Vec<usize>> {
        Some(self.ordering.clone())
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    fn rows_affected(&self) -> i64 {
        self.inner.rows_affected()
    }

    fn last_insert_id(&self) -> i64 {
        self.inner.last_insert_id()
    }

    fn last_error(&mut self) -> Option<radixdb_core::Error> {
        self.inner.last_error()
    }

    fn estimated_count(&self) -> Option<usize> {
        self.inner.estimated_count()
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        let Self { inner, ordering } = *self;
        Box::new(Self {
            inner: inner.with_aliases(aliases),
            ordering,
        })
    }
}

impl DeferredExecutorResult {
    pub fn with_arc_columns(columns: CompactArc<Vec<String>>, rows: Vec<DeferredRow>) -> Self {
        radixdb_storage::instrumentation::record_join_deferred_boundary_rows(rows.len() as u64);
        Self {
            columns,
            rows: rows.into_iter().map(Some).collect(),
            current_index: None,
            current_materialized: OnceCell::new(),
            closed: false,
        }
    }

    fn current_deferred(&self) -> &DeferredRow {
        let index = self
            .current_index
            .expect("row access without successful next()");
        self.rows[index]
            .as_ref()
            .expect("row already consumed from deferred result")
    }
}

impl QueryResult for DeferredExecutorResult {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn columns_arc(&self) -> Option<CompactArc<Vec<String>>> {
        Some(CompactArc::clone(&self.columns))
    }

    fn next(&mut self) -> bool {
        if self.closed {
            return false;
        }
        self.current_materialized = OnceCell::new();
        let next_index = self.current_index.map_or(0, |index| index + 1);
        if next_index < self.rows.len() {
            self.current_index = Some(next_index);
            true
        } else {
            false
        }
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        let row = self.row();
        if dest.len() != row.len() {
            return Err(radixdb_core::Error::internal(format!(
                "scan destination has {} values but row has {} columns",
                dest.len(),
                row.len()
            )));
        }
        dest.clone_from_slice(row.as_slice());
        Ok(())
    }

    fn row(&self) -> &Row {
        self.current_materialized
            .get_or_init(|| self.current_deferred().to_owned())
    }

    fn take_row(&mut self) -> Row {
        let index = self
            .current_index
            .expect("take_row() called without successful next()");
        let deferred = self.rows[index]
            .take()
            .expect("take_row() called after current row was consumed");
        self.current_materialized
            .take()
            .unwrap_or_else(|| deferred.into_owned())
    }

    fn take_deferred_row(&mut self) -> DeferredRow {
        let index = self
            .current_index
            .expect("take_deferred_row() called without successful next()");
        let deferred = self.rows[index]
            .take()
            .expect("take_deferred_row() called after current row was consumed");
        self.current_materialized
            .take()
            .map_or(deferred, DeferredRow::owned)
    }

    fn preserves_deferred_rows(&self) -> bool {
        true
    }

    fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }

    fn rows_affected(&self) -> i64 {
        0
    }

    fn last_insert_id(&self) -> i64 {
        0
    }

    fn estimated_count(&self) -> Option<usize> {
        if self.closed {
            return Some(0);
        }
        let consumed = self
            .current_index
            .map_or(0, |index| index.saturating_add(1));
        Some(self.rows.len().saturating_sub(consumed))
    }

    fn with_aliases(
        mut self: Box<Self>,
        aliases: FxHashMap<String, String>,
    ) -> Box<dyn QueryResult> {
        let columns = CompactArc::make_mut(&mut self.columns);
        for column in columns {
            if let Some((alias, _)) = aliases.iter().find(|(_, original)| *original == column) {
                *column = alias.clone();
            }
        }
        self
    }
}

/// Internal cursor backed directly by an opened Volcano operator.
///
/// It retains only the current deferred row. Recursive JOIN edges can wrap it
/// in `QueryResultOperator`, so the downstream edge pulls upstream work with
/// natural backpressure instead of crossing a `Vec<DeferredRow>` boundary.
#[doc(hidden)]
pub struct OperatorExecutorResult {
    columns: CompactArc<Vec<String>>,
    operator: Box<dyn Operator>,
    cancellation: CancellationHandle,
    current: Option<DeferredRow>,
    current_materialized: OnceCell<Row>,
    pending_error: Option<radixdb_core::Error>,
    estimated_rows: Option<usize>,
    emitted_rows: usize,
    remaining_limit: Option<usize>,
    ordering: Option<Vec<usize>>,
    closed: bool,
}

impl OperatorExecutorResult {
    pub fn open(
        columns: CompactArc<Vec<String>>,
        mut operator: Box<dyn Operator>,
        cancellation: CancellationHandle,
        limit: Option<u64>,
    ) -> Result<Self> {
        let estimated_rows = operator.estimated_rows().map(|rows| {
            limit.map_or(rows, |limit| {
                rows.min(usize::try_from(limit).unwrap_or(usize::MAX))
            })
        });
        let ordering = match operator.ordering() {
            super::operator::OrderingProperty::AscendingNullsLast(keys) => Some(keys),
            super::operator::OrderingProperty::Unknown => None,
        };
        if let Err(error) = operator.open() {
            let _ = operator.close();
            return Err(error);
        }
        Ok(Self {
            columns,
            operator,
            cancellation,
            current: None,
            current_materialized: OnceCell::new(),
            pending_error: None,
            estimated_rows,
            emitted_rows: 0,
            remaining_limit: limit.map(|limit| usize::try_from(limit).unwrap_or(usize::MAX)),
            ordering,
            closed: false,
        })
    }

    fn finish(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.operator.close()
    }

    fn fail(&mut self, error: radixdb_core::Error) -> bool {
        self.pending_error = Some(error);
        if let Err(close_error) = self.finish() {
            if self.pending_error.is_none() {
                self.pending_error = Some(close_error);
            }
        }
        false
    }

    fn current_deferred(&self) -> &DeferredRow {
        self.current
            .as_ref()
            .expect("row access without successful next()")
    }
}

impl Drop for OperatorExecutorResult {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

impl QueryResult for OperatorExecutorResult {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn columns_arc(&self) -> Option<CompactArc<Vec<String>>> {
        Some(CompactArc::clone(&self.columns))
    }

    fn next(&mut self) -> bool {
        if self.closed || self.pending_error.is_some() {
            return false;
        }
        self.current = None;
        self.current_materialized = OnceCell::new();
        if self.remaining_limit == Some(0) {
            return match self.finish() {
                Ok(()) => false,
                Err(error) => self.fail(error),
            };
        }
        if self.cancellation.is_cancelled() {
            return self.fail(radixdb_core::Error::QueryCancelled);
        }

        match self.operator.next() {
            Ok(Some(row)) => {
                self.current = Some(row.into_deferred());
                self.emitted_rows = self.emitted_rows.saturating_add(1);
                if let Some(remaining) = self.remaining_limit.as_mut() {
                    *remaining = remaining.saturating_sub(1);
                }
                true
            }
            Ok(None) => match self.finish() {
                Ok(()) => false,
                Err(error) => self.fail(error),
            },
            Err(error) => self.fail(error),
        }
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        let row = self.row();
        if dest.len() != row.len() {
            return Err(radixdb_core::Error::internal(format!(
                "scan destination has {} values but row has {} columns",
                dest.len(),
                row.len()
            )));
        }
        dest.clone_from_slice(row.as_slice());
        Ok(())
    }

    fn row(&self) -> &Row {
        self.current_materialized
            .get_or_init(|| self.current_deferred().to_owned())
    }

    fn take_row(&mut self) -> Row {
        let deferred = self
            .current
            .take()
            .expect("take_row() called without successful next()");
        self.current_materialized
            .take()
            .unwrap_or_else(|| deferred.into_owned())
    }

    fn take_deferred_row(&mut self) -> DeferredRow {
        let deferred = self
            .current
            .take()
            .expect("take_deferred_row() called without successful next()");
        self.current_materialized
            .take()
            .map_or(deferred, DeferredRow::owned)
    }

    fn preserves_deferred_rows(&self) -> bool {
        true
    }

    fn ascending_nulls_last_ordering(&self) -> Option<Vec<usize>> {
        self.ordering.clone()
    }

    fn close(&mut self) -> Result<()> {
        self.current = None;
        self.current_materialized = OnceCell::new();
        self.finish()
    }

    fn rows_affected(&self) -> i64 {
        0
    }

    fn last_insert_id(&self) -> i64 {
        0
    }

    fn estimated_count(&self) -> Option<usize> {
        if self.closed {
            return Some(0);
        }
        self.estimated_rows
            .map(|rows| rows.saturating_sub(self.emitted_rows))
    }

    fn last_error(&mut self) -> Option<radixdb_core::Error> {
        self.pending_error.take()
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

/// Query result backed by a fallible row iterator.
///
/// This is the common backpressure bridge for producers such as table-valued
/// functions: construction retains no rows and each `next()` owns at most the
/// current row.
#[doc(hidden)]
pub struct StreamingRowsResult {
    columns: Vec<String>,
    rows: Box<dyn Iterator<Item = Result<(i64, Row)>> + Send>,
    current: Option<Row>,
    pending_error: Option<radixdb_core::Error>,
}

impl StreamingRowsResult {
    pub fn new(
        columns: Vec<String>,
        rows: Box<dyn Iterator<Item = Result<(i64, Row)>> + Send>,
    ) -> Self {
        Self {
            columns,
            rows,
            current: None,
            pending_error: None,
        }
    }
}

impl QueryResult for StreamingRowsResult {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn next(&mut self) -> bool {
        self.current = None;
        match self.rows.next() {
            Some(Ok((_, row))) => {
                self.current = Some(row);
                true
            }
            Some(Err(error)) => {
                self.pending_error = Some(error);
                false
            }
            None => false,
        }
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        let row = self.row();
        if dest.len() != row.len() {
            return Err(radixdb_core::Error::internal(format!(
                "scan destination has {} values but row has {} columns",
                dest.len(),
                row.len()
            )));
        }
        dest.clone_from_slice(row.as_slice());
        Ok(())
    }

    fn row(&self) -> &Row {
        self.current
            .as_ref()
            .expect("row() called without successful next()")
    }

    fn rows_affected(&self) -> i64 {
        0
    }

    fn last_insert_id(&self) -> i64 {
        0
    }

    fn take_row(&mut self) -> Row {
        self.current
            .take()
            .expect("take_row() called without successful next()")
    }

    fn last_error(&mut self) -> Option<radixdb_core::Error> {
        self.pending_error.take()
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

impl ExecutorResult {
    /// Create a new memory result with columns and pooled rows
    pub fn new(columns: Vec<String>, rows: RowVec) -> Self {
        let len = rows.len();
        Self {
            columns: CompactArc::new(columns),
            rows: RowStorage::Owned(rows),
            len,
            current_index: None,
            closed: false,
            affected: 0,
            insert_id: 0,
        }
    }

    /// Create a new memory result with Arc columns (zero-copy)
    pub fn with_arc_columns(columns: CompactArc<Vec<String>>, rows: RowVec) -> Self {
        let len = rows.len();
        Self {
            columns,
            rows: RowStorage::Owned(rows),
            len,
            current_index: None,
            closed: false,
            affected: 0,
            insert_id: 0,
        }
    }

    /// Create a new memory result with shared rows from cache (zero-copy for rows)
    /// This avoids cloning the entire `Vec<Row>` when reading from semantic cache
    pub fn with_shared_rows(columns: Vec<String>, rows: CompactArc<Vec<Row>>) -> Self {
        let len = rows.len();
        Self {
            columns: CompactArc::new(columns),
            rows: RowStorage::Shared(rows),
            len,
            current_index: None,
            closed: false,
            affected: 0,
            insert_id: 0,
        }
    }

    /// Create a new memory result with Arc columns and shared rows (zero-copy for both)
    pub fn with_arc_columns_shared_rows(
        columns: CompactArc<Vec<String>>,
        rows: CompactArc<Vec<Row>>,
    ) -> Self {
        let len = rows.len();
        Self {
            columns,
            rows: RowStorage::Shared(rows),
            len,
            current_index: None,
            closed: false,
            affected: 0,
            insert_id: 0,
        }
    }

    /// Create an empty memory result
    pub fn empty() -> Self {
        Self::new(Vec::new(), RowVec::new())
    }

    /// Create with columns only (no rows yet)
    pub fn with_columns(columns: Vec<String>) -> Self {
        Self::new(columns, RowVec::new())
    }

    /// Add a row to the result
    pub fn add_row(&mut self, row: Row) {
        // Convert from shared to owned if needed
        let shared_rows = match &self.rows {
            RowStorage::Owned(_) => None,
            RowStorage::Shared(arc_rows) => {
                let mut rows = RowVec::with_capacity(arc_rows.len() + 1);
                for (index, row) in arc_rows.iter().enumerate() {
                    rows.push((index as i64, row.clone()));
                }
                Some(rows)
            }
        };
        if let Some(rows) = shared_rows {
            self.rows = RowStorage::Owned(rows);
        }
        if let RowStorage::Owned(rv) = &mut self.rows {
            rv.push((self.len as i64, row));
            self.len += 1;
        }
    }

    /// Get the number of rows
    #[inline]
    pub fn row_count(&self) -> usize {
        self.len
    }

    /// Get row by index
    #[inline]
    pub fn get_row(&self, index: usize) -> Option<&Row> {
        self.rows.get(index)
    }

    /// Take ownership of all rows (extracts Row from RowVec)
    pub fn into_rows(self) -> Vec<Row> {
        match self.rows {
            RowStorage::Owned(mut rv) => rv.drain_rows().collect(),
            RowStorage::Shared(rows) => {
                // Must clone if shared
                CompactArc::try_unwrap(rows).unwrap_or_else(|arc| (*arc).clone())
            }
        }
    }

    /// Take rows as CompactArc for zero-copy sharing with joins
    /// Returns `CompactArc<Vec<Row>>` - wraps owned rows or clones `CompactArc` for shared
    pub fn into_arc_rows(self) -> CompactArc<Vec<Row>> {
        match self.rows {
            RowStorage::Owned(mut rv) => CompactArc::new(rv.drain_rows().collect()),
            RowStorage::Shared(rows) => rows,
        }
    }

    /// Reset the cursor to the beginning
    pub fn reset(&mut self) {
        self.current_index = None;
    }

    /// Set rows affected (for modification results)
    pub fn set_rows_affected(&mut self, count: i64) {
        self.affected = count;
    }

    /// Set last insert ID
    pub fn set_last_insert_id(&mut self, id: i64) {
        self.insert_id = id;
    }
}

impl QueryResult for ExecutorResult {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn columns_arc(&self) -> Option<CompactArc<Vec<String>>> {
        Some(CompactArc::clone(&self.columns))
    }

    #[inline]
    fn next(&mut self) -> bool {
        if self.closed {
            return false;
        }

        let next_index = match self.current_index {
            None => 0,
            Some(i) => i + 1,
        };

        if next_index < self.len {
            self.current_index = Some(next_index);
            true
        } else {
            false
        }
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        let row = self.row();

        if dest.len() != row.len() {
            return Err(radixdb_core::Error::internal(format!(
                "scan destination has {} values but row has {} columns",
                dest.len(),
                row.len()
            )));
        }

        for (i, value) in row.iter().enumerate() {
            dest[i] = value.clone();
        }

        Ok(())
    }

    fn row(&self) -> &Row {
        match self.current_index {
            Some(i) => self
                .rows
                .get(i)
                .expect("row() called without successful next()"),
            _ => panic!("row() called without successful next()"),
        }
    }

    fn take_row(&mut self) -> Row {
        match self.current_index {
            Some(i) if i < self.rows.len() => self.rows.take(i),
            _ => panic!("take_row() called without successful next()"),
        }
    }

    fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }

    fn rows_affected(&self) -> i64 {
        self.affected
    }

    fn last_insert_id(&self) -> i64 {
        self.insert_id
    }

    fn try_into_arc_rows(&mut self) -> Option<CompactArc<Vec<Row>>> {
        // Take ownership of rows and return as Arc
        let rows = std::mem::replace(&mut self.rows, RowStorage::Owned(RowVec::new()));
        self.closed = true; // Mark as consumed
        match rows {
            RowStorage::Owned(mut rv) => Some(CompactArc::new(rv.drain_rows().collect())),
            RowStorage::Shared(arc) => Some(arc),
        }
    }

    fn estimated_count(&self) -> Option<usize> {
        if self.closed {
            return Some(0);
        }
        let consumed = self
            .current_index
            .map_or(0, |index| index.saturating_add(1));
        Some(self.len.saturating_sub(consumed))
    }

    fn with_aliases(
        mut self: Box<Self>,
        aliases: FxHashMap<String, String>,
    ) -> Box<dyn QueryResult> {
        // Apply aliases to column names (use CompactArc::make_mut for copy-on-write)
        let columns = CompactArc::make_mut(&mut self.columns);
        for col in columns {
            // Find if this column has an alias (reverse lookup)
            for (alias, original) in &aliases {
                if col == original {
                    *col = alias.clone();
                    break;
                }
            }
        }
        self
    }
}

/// Filtered result that applies a WHERE clause to an underlying result
///
/// This struct owns a pre-compiled RowFilter, avoiding per-row compilation.
/// The filter is compiled once during construction and reused for every row.
pub struct FilteredResult {
    /// Underlying result
    inner: Box<dyn QueryResult>,
    /// Pre-compiled row filter (thread-safe, reusable)
    filter: RowFilter,
    /// Current row (cached after filter passes)
    current_row: Option<Row>,
    /// Columns cached
    columns: Vec<String>,
    /// Pending error from filter evaluation (e.g. invalid REGEXP pattern)
    pending_error: Option<radixdb_core::Error>,
}

/// Streaming post-operator filter that preserves deferred JOIN rows.
///
/// Unlike [`FilteredResult`], this wrapper evaluates the predicate directly
/// against `DeferredRow` and therefore does not publish an owned `Row` merely
/// to decide whether it passes a post-JOIN WHERE clause. This is the required
/// boundary for OUTER JOIN predicates that cannot be pushed below the join.
#[doc(hidden)]
pub struct DeferredFilteredResult {
    inner: Box<dyn QueryResult>,
    filter: RowFilter,
    current: Option<DeferredRow>,
    current_materialized: OnceCell<Row>,
    columns: CompactArc<Vec<String>>,
    pending_error: Option<radixdb_core::Error>,
}

impl DeferredFilteredResult {
    pub fn from_filter(inner: Box<dyn QueryResult>, filter: RowFilter) -> Self {
        let columns = inner
            .columns_arc()
            .unwrap_or_else(|| CompactArc::new(inner.columns().to_vec()));
        Self {
            inner,
            filter,
            current: None,
            current_materialized: OnceCell::new(),
            columns,
            pending_error: None,
        }
    }

    fn current_deferred(&self) -> &DeferredRow {
        self.current
            .as_ref()
            .expect("row access without successful next()")
    }
}

impl QueryResult for DeferredFilteredResult {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn columns_arc(&self) -> Option<CompactArc<Vec<String>>> {
        Some(CompactArc::clone(&self.columns))
    }

    fn next(&mut self) -> bool {
        self.current = None;
        self.current_materialized = OnceCell::new();
        while self.inner.next() {
            let row = self.inner.take_deferred_row();
            match self.filter.matches_deferred_checked(&row) {
                Ok(true) => {
                    self.current = Some(row);
                    return true;
                }
                Ok(false) => {}
                Err(error) => {
                    self.pending_error = Some(error);
                    return false;
                }
            }
        }
        false
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        let row = self.row();
        if dest.len() != row.len() {
            return Err(radixdb_core::Error::internal(format!(
                "scan destination has {} values but row has {} columns",
                dest.len(),
                row.len()
            )));
        }
        dest.clone_from_slice(row.as_slice());
        Ok(())
    }

    fn row(&self) -> &Row {
        self.current_materialized
            .get_or_init(|| self.current_deferred().to_owned())
    }

    fn take_row(&mut self) -> Row {
        let deferred = self
            .current
            .take()
            .expect("take_row() called without successful next()");
        self.current_materialized
            .take()
            .unwrap_or_else(|| deferred.into_owned())
    }

    fn take_deferred_row(&mut self) -> DeferredRow {
        let deferred = self
            .current
            .take()
            .expect("take_deferred_row() called without successful next()");
        self.current_materialized
            .take()
            .map_or(deferred, DeferredRow::owned)
    }

    fn preserves_deferred_rows(&self) -> bool {
        true
    }

    fn ascending_nulls_last_ordering(&self) -> Option<Vec<usize>> {
        self.inner.ascending_nulls_last_ordering()
    }

    fn close(&mut self) -> Result<()> {
        self.current = None;
        self.current_materialized = OnceCell::new();
        self.inner.close()
    }

    fn rows_affected(&self) -> i64 {
        self.inner.rows_affected()
    }

    fn last_insert_id(&self) -> i64 {
        self.inner.last_insert_id()
    }

    fn last_error(&mut self) -> Option<radixdb_core::Error> {
        self.pending_error
            .take()
            .or_else(|| self.inner.last_error())
    }

    fn estimated_count(&self) -> Option<usize> {
        self.inner.estimated_count()
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

impl FilteredResult {
    /// Create a new expression-filtered result
    ///
    /// # Arguments
    /// * `inner` - The source result to filter
    /// * `filter_expr` - The WHERE clause expression
    ///
    /// Returns an error if the filter expression cannot be compiled.
    pub fn new(inner: Box<dyn QueryResult>, filter_expr: &Expression) -> Result<Self> {
        let columns = inner.columns().to_vec();
        let filter = RowFilter::new(filter_expr, &columns)?;

        Ok(Self {
            inner,
            filter,
            current_row: None,
            columns,
            pending_error: None,
        })
    }

    /// Create from a pre-built RowFilter
    ///
    /// Use this when you have a RowFilter that was constructed with specific
    /// context (e.g., with_context for correlated subqueries).
    pub fn from_filter(inner: Box<dyn QueryResult>, filter: RowFilter) -> Self {
        let columns = inner.columns().to_vec();
        Self {
            inner,
            filter,
            current_row: None,
            columns,
            pending_error: None,
        }
    }

    /// Create with default function registry (static lifetime)
    pub fn with_defaults(inner: Box<dyn QueryResult>, filter_expr: Expression) -> Result<Self> {
        let columns = inner.columns().to_vec();
        let filter = RowFilter::new(&filter_expr, &columns)?;

        Ok(Self {
            inner,
            filter,
            current_row: None,
            columns,
            pending_error: None,
        })
    }
}

impl QueryResult for FilteredResult {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn next(&mut self) -> bool {
        // Keep advancing until we find a row that passes the filter
        while self.inner.next() {
            let row = self.inner.row();
            // Use checked filter to propagate runtime errors (e.g. invalid REGEXP)
            match self.filter.matches_checked(row) {
                Ok(true) => {
                    self.current_row = Some(self.inner.take_row());
                    return true;
                }
                Ok(false) => continue,
                Err(e) => {
                    self.pending_error = Some(e);
                    self.current_row = None;
                    return false;
                }
            }
        }
        self.current_row = None;
        false
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        if let Some(ref row) = self.current_row {
            if dest.len() != row.len() {
                return Err(radixdb_core::Error::internal(format!(
                    "scan destination has {} values but row has {} columns",
                    dest.len(),
                    row.len()
                )));
            }
            for (i, value) in row.iter().enumerate() {
                dest[i] = value.clone();
            }
            Ok(())
        } else {
            Err(radixdb_core::Error::internal(
                "scan() called without successful next()",
            ))
        }
    }

    fn row(&self) -> &Row {
        self.current_row
            .as_ref()
            .expect("row() called without successful next()")
    }

    fn take_row(&mut self) -> Row {
        self.current_row
            .take()
            .expect("take_row() called without successful next()")
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    fn rows_affected(&self) -> i64 {
        self.inner.rows_affected()
    }

    fn last_insert_id(&self) -> i64 {
        self.inner.last_insert_id()
    }

    fn last_error(&mut self) -> Option<radixdb_core::Error> {
        // Return own error first, then check inner for nested FilteredResult chains
        self.pending_error
            .take()
            .or_else(|| self.inner.last_error())
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

/// A result wrapper that returns one row already consumed from `inner` before
/// continuing with the remaining source rows.
///
/// Optimizers sometimes need to inspect the first row to select a physical
/// path. If they reject that path after inspection, this wrapper preserves the
/// consumed row so the caller can continue with the same source instead of
/// executing the source query again.
#[doc(hidden)]
pub struct PrefetchedResult {
    inner: Box<dyn QueryResult>,
    columns: Vec<String>,
    prefetched: Option<Row>,
    current_row: Option<Row>,
}

impl PrefetchedResult {
    pub fn new(prefetched: Row, inner: Box<dyn QueryResult>) -> Self {
        let columns = inner.columns().to_vec();
        Self {
            inner,
            columns,
            prefetched: Some(prefetched),
            current_row: None,
        }
    }
}

impl QueryResult for PrefetchedResult {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn next(&mut self) -> bool {
        self.current_row = None;
        if let Some(row) = self.prefetched.take() {
            self.current_row = Some(row);
            return true;
        }
        if self.inner.next() {
            self.current_row = Some(self.inner.take_row());
            return true;
        }
        false
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        let row = self.current_row.as_ref().ok_or_else(|| {
            radixdb_core::Error::internal("scan() called without successful next()")
        })?;
        if dest.len() != row.len() {
            return Err(radixdb_core::Error::internal(format!(
                "scan destination has {} values but row has {} columns",
                dest.len(),
                row.len()
            )));
        }
        for (index, value) in row.iter().enumerate() {
            dest[index] = value.clone();
        }
        Ok(())
    }

    fn row(&self) -> &Row {
        self.current_row
            .as_ref()
            .expect("row() called without successful next()")
    }

    fn take_row(&mut self) -> Row {
        self.current_row
            .take()
            .expect("take_row() called without successful next()")
    }

    fn close(&mut self) -> Result<()> {
        self.prefetched = None;
        self.current_row = None;
        self.inner.close()
    }

    fn rows_affected(&self) -> i64 {
        self.inner.rows_affected()
    }

    fn last_insert_id(&self) -> i64 {
        self.inner.last_insert_id()
    }

    fn estimated_count(&self) -> Option<usize> {
        self.inner
            .estimated_count()
            .map(|count| count.saturating_add(usize::from(self.prefetched.is_some())))
    }

    fn last_error(&mut self) -> Option<radixdb_core::Error> {
        self.inner.last_error()
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

/// Pre-compiled projection that can be either a Star expansion or a compiled expression.
enum CompiledProjection {
    /// Expand all columns from source (SELECT *)
    Star,
    /// Expand columns for specific table/alias (SELECT t.*)
    QualifiedStar {
        /// Lowercase qualifier for matching (e.g., "t") - no format! allocation
        qualifier_lower: String,
    },
    /// A pre-compiled expression program
    Compiled(super::expression::SharedProgram),
}

/// Expression-based mapped result with pre-compiled projections
///
/// This struct pre-compiles all expressions during construction, providing
/// efficient per-row evaluation through the Expression VM.
pub struct ExprMappedResult {
    /// Underlying result
    inner: Box<dyn QueryResult>,
    /// Pre-compiled projections (one per output column)
    projections: Vec<CompiledProjection>,
    /// VM instance for expression execution (reused)
    vm: super::expression::ExprVM,
    /// Current mapped row
    current_row: Row,
    /// Output column names
    output_columns: Vec<String>,
    /// Pre-computed lowercase source columns (avoids per-row to_lowercase())
    source_columns_lower: Vec<String>,
    /// Pending error from projection evaluation.
    pending_error: Option<radixdb_core::Error>,
    params: Vec<Value>,
    named_params: FxHashMap<String, Value>,
    transaction_id: Option<u64>,
    stored_function_invoker: Option<Arc<dyn super::context::StoredFunctionInvoker>>,
    ordering: Option<Vec<usize>>,
}

impl ExprMappedResult {
    fn direct_source_index(expression: &Expression, source_columns: &[String]) -> Option<usize> {
        let expression = match expression {
            Expression::Aliased(aliased) => aliased.expression.as_ref(),
            other => other,
        };
        match expression {
            Expression::QualifiedIdentifier(identifier) => {
                let qualified = identifier.to_string();
                source_columns
                    .iter()
                    .position(|column| column.eq_ignore_ascii_case(&qualified))
            }
            Expression::Identifier(identifier) => {
                let mut matches = source_columns.iter().enumerate().filter(|(_, column)| {
                    column.eq_ignore_ascii_case(identifier.value.as_str())
                        || column.rsplit_once('.').is_some_and(|(_, base)| {
                            base.eq_ignore_ascii_case(identifier.value.as_str())
                        })
                });
                let (index, _) = matches.next()?;
                matches.next().is_none().then_some(index)
            }
            _ => None,
        }
    }

    /// Create a new expression-mapped result
    ///
    /// # Arguments
    /// * `inner` - The source result to project
    /// * `expressions` - The projection expressions
    /// * `output_columns` - Names for the output columns
    ///
    /// Returns an error if any expression cannot be compiled.
    pub fn new(
        inner: Box<dyn QueryResult>,
        expressions: Vec<Expression>,
        output_columns: Vec<String>,
    ) -> Result<Self> {
        Self::new_with_optional_context(inner, expressions, output_columns, None)
    }

    pub fn with_context(
        inner: Box<dyn QueryResult>,
        expressions: Vec<Expression>,
        output_columns: Vec<String>,
        ctx: &super::context::ExecutionContext,
    ) -> Result<Self> {
        Self::new_with_optional_context(inner, expressions, output_columns, Some(ctx))
    }

    fn new_with_optional_context(
        inner: Box<dyn QueryResult>,
        expressions: Vec<Expression>,
        output_columns: Vec<String>,
        ctx: Option<&super::context::ExecutionContext>,
    ) -> Result<Self> {
        use super::expression::compile_expression;

        let source_columns = inner.columns().to_vec();
        let source_ordering = inner.ascending_nulls_last_ordering();

        // Pre-compile all expressions
        let mut projections = Vec::with_capacity(expressions.len());
        for expr in &expressions {
            let projection = match expr {
                Expression::Star(_) => CompiledProjection::Star,
                Expression::QualifiedStar(qs) => CompiledProjection::QualifiedStar {
                    qualifier_lower: qs.qualifier.to_lowercase().to_string(),
                },
                _ => {
                    let program = compile_expression(expr, &source_columns)?;
                    CompiledProjection::Compiled(program)
                }
            };
            projections.push(projection);
        }

        // Pre-compute lowercase source columns to avoid per-row to_lowercase() calls
        let source_columns_lower: Vec<String> =
            source_columns.iter().map(|c| c.to_lowercase()).collect();
        let ordering = source_ordering.and_then(|keys| {
            let mut remapped = Vec::with_capacity(keys.len());
            for key in keys {
                let output = expressions.iter().position(|expression| {
                    Self::direct_source_index(expression, &source_columns) == Some(key)
                })?;
                remapped.push(output);
            }
            (!remapped.is_empty()).then_some(remapped)
        });

        // Pre-allocate buffer with capacity for reuse
        let capacity = projections.len();
        Ok(Self {
            inner,
            projections,
            vm: super::expression::ExprVM::new(),
            current_row: Row::with_capacity(capacity),
            output_columns,
            source_columns_lower,
            pending_error: None,
            params: ctx.map_or_else(Vec::new, |ctx| ctx.params().to_vec()),
            named_params: ctx.map_or_else(FxHashMap::default, |ctx| ctx.named_params().clone()),
            transaction_id: ctx.and_then(super::context::ExecutionContext::transaction_id),
            stored_function_invoker: ctx.and_then(|ctx| ctx.stored_function_invoker().cloned()),
            ordering,
        })
    }

    /// Create with default function registry (static lifetime)
    pub fn with_defaults(
        inner: Box<dyn QueryResult>,
        expressions: Vec<Expression>,
        output_columns: Vec<String>,
    ) -> Result<Self> {
        Self::new(inner, expressions, output_columns)
    }
}

impl QueryResult for ExprMappedResult {
    fn columns(&self) -> &[String] {
        &self.output_columns
    }

    fn next(&mut self) -> bool {
        use super::expression::ExecuteContext;

        if self.inner.next() {
            let source_row = self.inner.row();

            // OPTIMIZATION: Lazy capacity reservation - only allocate when Row was taken
            // This avoids allocation in take_row() when caller drops the Row
            self.current_row.reserve_inline(self.projections.len());
            // Reuse buffer with inline storage - clear_inline preserves capacity
            self.current_row.clear_inline();
            for projection in &self.projections {
                match projection {
                    CompiledProjection::Star => {
                        // Expand all columns from source
                        for value in source_row.iter() {
                            self.current_row.push_inline(value.clone());
                        }
                    }
                    CompiledProjection::QualifiedStar { qualifier_lower } => {
                        // Expand columns for specific table/alias
                        // Use pre-computed lowercase columns to avoid per-row to_lowercase()
                        let qualifier_len = qualifier_lower.len();
                        for (idx, col_lower) in self.source_columns_lower.iter().enumerate() {
                            // Inline prefix check: "qualifier." without format! allocation
                            if col_lower.len() > qualifier_len
                                && col_lower.starts_with(qualifier_lower.as_str())
                                && col_lower.as_bytes()[qualifier_len] == b'.'
                                && idx < source_row.len()
                            {
                                self.current_row.push_inline(source_row[idx].clone());
                            }
                        }
                    }
                    CompiledProjection::Compiled(program) => {
                        let ctx = ExecuteContext::with_common_params(
                            source_row,
                            &self.params,
                            (!self.named_params.is_empty()).then_some(&self.named_params),
                            self.transaction_id,
                        )
                        .with_stored_function_invoker(self.stored_function_invoker.as_ref());
                        let value = self.vm.execute_cow(program, &ctx);
                        match value {
                            Ok(value) => self.current_row.push_inline(value),
                            Err(error) => {
                                self.pending_error = Some(error);
                                self.current_row.clear_inline();
                                return false;
                            }
                        }
                    }
                }
            }
            true
        } else {
            false
        }
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        if dest.len() != self.current_row.len() {
            return Err(radixdb_core::Error::internal(format!(
                "scan destination has {} values but row has {} columns",
                dest.len(),
                self.current_row.len()
            )));
        }
        for (i, value) in self.current_row.iter().enumerate() {
            dest[i] = value.clone();
        }
        Ok(())
    }

    fn row(&self) -> &Row {
        &self.current_row
    }

    fn take_row(&mut self) -> Row {
        // Don't pre-allocate here - let next() do lazy initialization
        // This eliminates one allocation per row when the caller drops the row
        std::mem::take(&mut self.current_row)
    }

    fn ascending_nulls_last_ordering(&self) -> Option<Vec<usize>> {
        self.ordering.clone()
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    fn rows_affected(&self) -> i64 {
        0
    }

    fn last_insert_id(&self) -> i64 {
        0
    }

    fn last_error(&mut self) -> Option<radixdb_core::Error> {
        self.pending_error
            .take()
            .or_else(|| self.inner.last_error())
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

mod ordering;
pub use ordering::{LimitedResult, OrderedResult, RadixOrderSpec, TopNResult};
#[cfg(test)]
use ordering::{ORDERED_RUN_MAX_BYTES, ORDERED_RUN_MAX_ROWS};

/// Streaming distinct result that removes duplicate rows on-the-fly
///
/// This streams rows and only stores seen row values for deduplication. This enables:
/// - Early termination with LIMIT (no need to scan all rows)
/// - Lower latency to first row
/// - Streaming output
pub struct DistinctResult {
    /// Underlying result source
    inner: Box<dyn QueryResult>,
    /// Columns from inner result
    columns: Vec<String>,
    /// Number of columns to consider for distinctness
    /// (may be less than total columns when ORDER BY adds extra columns)
    distinct_column_count: usize,
    /// Seen rows for deduplication: hash -> list of row values (for collision handling)
    /// We only store the distinct columns, not the full row
    seen: FxHashMap<u64, Vec<Vec<Value>>>,
    /// Current row (stored for row() method)
    current_row: Row,
    /// Whether we have a valid current row
    has_current: bool,
    budget: RetainedRowsBudget,
    terminal_error: Option<radixdb_core::Error>,
}

impl DistinctResult {
    /// Create a new streaming distinct result
    pub fn new(inner: Box<dyn QueryResult>) -> Self {
        Self::with_column_count(inner, None)
    }

    /// Create a distinct result that only considers the first `distinct_columns` columns
    /// for uniqueness comparison. This is used when ORDER BY references columns not in SELECT.
    ///
    /// For example: SELECT DISTINCT a FROM t ORDER BY b
    /// - The result has columns [a, b] for sorting
    /// - But distinctness should only compare column `a`
    pub fn with_column_count(inner: Box<dyn QueryResult>, distinct_columns: Option<usize>) -> Self {
        let columns = inner.columns().to_vec();
        let distinct_column_count = distinct_columns.unwrap_or(columns.len());

        Self {
            inner,
            columns,
            distinct_column_count,
            seen: FxHashMap::default(),
            current_row: Row::new(),
            has_current: false,
            budget: RetainedRowsBudget::new("DISTINCT"),
            terminal_error: None,
        }
    }

    /// Compute hash for the distinct columns of a row
    fn hash_row(&self, row: &Row) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = FxHasher::default();
        for value in row.iter().take(self.distinct_column_count) {
            value.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Extract distinct column values from a row
    fn extract_distinct_values(&self, row: &Row) -> Vec<Value> {
        row.iter()
            .take(self.distinct_column_count)
            .cloned()
            .collect()
    }
}

impl QueryResult for DistinctResult {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn next(&mut self) -> bool {
        // Keep getting rows until we find a non-duplicate
        while self.inner.next() {
            let row = self.inner.row();
            let hash = self.hash_row(row);

            // Extract distinct values first for duplicate check
            let values = self.extract_distinct_values(row);

            // Check if we've seen this combination before
            let is_dup = if let Some(seen_rows) = self.seen.get(&hash) {
                seen_rows.contains(&values)
            } else {
                false
            };

            if !is_dup {
                // New unique row found - take ownership and mark as seen
                if let Err(error) = self.budget.admit_values(&values) {
                    self.terminal_error = Some(error);
                    self.has_current = false;
                    return false;
                }
                self.current_row = self.inner.take_row();
                self.seen.entry(hash).or_default().push(values);
                self.has_current = true;
                return true;
            }
            // Duplicate - continue to next row
        }

        // No more rows
        self.has_current = false;
        false
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        if !self.has_current {
            return Err(radixdb_core::Error::internal(
                "scan() called without successful next()",
            ));
        }
        if dest.len() != self.current_row.len() {
            return Err(radixdb_core::Error::internal(format!(
                "scan destination has {} values but row has {} columns",
                dest.len(),
                self.current_row.len()
            )));
        }
        for (i, v) in self.current_row.iter().enumerate() {
            dest[i] = v.clone();
        }
        Ok(())
    }

    fn row(&self) -> &Row {
        &self.current_row
    }

    fn take_row(&mut self) -> Row {
        self.has_current = false;
        std::mem::take(&mut self.current_row)
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    fn rows_affected(&self) -> i64 {
        0
    }

    fn last_insert_id(&self) -> i64 {
        0
    }

    fn last_error(&mut self) -> Option<radixdb_core::Error> {
        self.terminal_error
            .take()
            .or_else(|| self.inner.last_error())
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

/// DISTINCT ON result — keeps only the first row per unique combination
/// of the specified key columns. Uses hash-based deduplication so the input
/// does not need to be sorted by the key columns (ORDER BY controls which
/// row is "first" because the input stream preserves ORDER BY order).
/// Memory usage is O(groups) where groups is the number of unique key
/// combinations, not total rows.
pub struct DistinctOnResult {
    inner: Box<dyn QueryResult>,
    columns: Vec<String>,
    /// Column indices that form the DISTINCT ON key
    key_indices: Vec<usize>,
    /// Seen keys: hash -> list of key values (for collision handling)
    seen: FxHashMap<u64, Vec<Vec<Value>>>,
    current_row: Row,
    has_current: bool,
    budget: RetainedRowsBudget,
    terminal_error: Option<radixdb_core::Error>,
}

impl DistinctOnResult {
    pub fn new(inner: Box<dyn QueryResult>, key_indices: Vec<usize>) -> Self {
        let columns = inner.columns().to_vec();
        Self {
            inner,
            columns,
            key_indices,
            seen: FxHashMap::default(),
            current_row: Row::new(),
            has_current: false,
            budget: RetainedRowsBudget::new("DISTINCT ON"),
            terminal_error: None,
        }
    }

    fn extract_key(&self, row: &Row) -> Vec<Value> {
        self.key_indices
            .iter()
            .map(|&i| row.get(i).cloned().unwrap_or_else(Value::null_unknown))
            .collect()
    }

    fn hash_key(&self, key: &[Value]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = FxHasher::default();
        for value in key {
            value.hash(&mut hasher);
        }
        hasher.finish()
    }
}

impl QueryResult for DistinctOnResult {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn next(&mut self) -> bool {
        while self.inner.next() {
            let row = self.inner.row();
            let key = self.extract_key(row);
            let hash = self.hash_key(&key);

            // Check if we've seen this key before
            let is_dup = if let Some(seen_keys) = self.seen.get(&hash) {
                seen_keys.contains(&key)
            } else {
                false
            };

            if is_dup {
                continue; // already emitted a row for this group
            }

            if let Err(error) = self.budget.admit_values(&key) {
                self.terminal_error = Some(error);
                self.has_current = false;
                return false;
            }
            self.seen.entry(hash).or_default().push(key);
            self.current_row = self.inner.take_row();
            self.has_current = true;
            return true;
        }
        self.has_current = false;
        false
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        if !self.has_current {
            return Err(radixdb_core::Error::internal(
                "scan() called without successful next()",
            ));
        }
        if dest.len() != self.current_row.len() {
            return Err(radixdb_core::Error::internal(format!(
                "scan destination has {} values but row has {} columns",
                dest.len(),
                self.current_row.len()
            )));
        }
        for (i, v) in self.current_row.iter().enumerate() {
            dest[i] = v.clone();
        }
        Ok(())
    }

    fn row(&self) -> &Row {
        &self.current_row
    }

    fn take_row(&mut self) -> Row {
        self.has_current = false;
        std::mem::take(&mut self.current_row)
    }

    fn close(&mut self) -> Result<()> {
        self.seen.clear();
        self.inner.close()
    }

    fn rows_affected(&self) -> i64 {
        0
    }

    fn last_insert_id(&self) -> i64 {
        0
    }

    fn last_error(&mut self) -> Option<radixdb_core::Error> {
        self.terminal_error
            .take()
            .or_else(|| self.inner.last_error())
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

/// Projected result that removes extra columns (e.g., ORDER BY columns not in SELECT)
///
/// This result type projects rows to only include the first N columns,
/// removing any extra columns that were added for sorting purposes.
pub struct ProjectedResult {
    inner: Box<dyn QueryResult>,
    /// Number of columns to keep
    keep_columns: usize,
    /// Cached projected columns
    projected_columns: Vec<String>,
    /// Cached projected row
    current_row: Row,
}

impl ProjectedResult {
    /// Create a new projected result
    pub fn new(inner: Box<dyn QueryResult>, keep_columns: usize) -> Self {
        let projected_columns: Vec<String> =
            inner.columns().iter().take(keep_columns).cloned().collect();

        // Pre-allocate Inline storage - no Arc overhead for intermediate results
        Self {
            inner,
            keep_columns,
            projected_columns,
            current_row: Row::with_capacity(keep_columns),
        }
    }
}

impl QueryResult for ProjectedResult {
    fn columns(&self) -> &[String] {
        &self.projected_columns
    }

    fn next(&mut self) -> bool {
        if self.inner.next() {
            // Project the row to keep only the first N columns
            // OPTIMIZATION: Use Inline storage - no Arc overhead for intermediate results
            self.current_row.reserve_inline(self.keep_columns);
            self.current_row.clear_inline();
            let full_row = self.inner.row();
            for i in 0..self.keep_columns {
                self.current_row
                    .push_inline(full_row.get(i).cloned().unwrap_or(Value::null_unknown()));
            }
            true
        } else {
            false
        }
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        for (i, val) in dest.iter_mut().enumerate().take(self.keep_columns) {
            *val = self
                .current_row
                .get(i)
                .cloned()
                .unwrap_or(Value::null_unknown());
        }
        Ok(())
    }

    fn row(&self) -> &Row {
        &self.current_row
    }

    fn take_row(&mut self) -> Row {
        std::mem::take(&mut self.current_row)
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    fn rows_affected(&self) -> i64 {
        0
    }

    fn last_insert_id(&self) -> i64 {
        0
    }

    fn last_error(&mut self) -> Option<radixdb_core::Error> {
        self.inner.last_error()
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

/// Streaming projection result that projects columns row-by-row
///
/// This allows streaming projection without materializing all rows into a Vec.
/// Uses simple column index-based projection for performance.
pub struct StreamingProjectionResult {
    /// Underlying result
    inner: Box<dyn QueryResult>,
    /// Column indices to project (from source to output)
    column_indices: Vec<usize>,
    /// Output column names
    output_columns: Vec<String>,
    /// Current projected row
    current_row: Row,
}

impl StreamingProjectionResult {
    /// Create a new streaming projection result
    ///
    /// # Arguments
    /// * `inner` - The source result
    /// * `column_indices` - Indices of columns to keep from the source
    /// * `output_columns` - Names for the output columns
    pub fn new(
        inner: Box<dyn QueryResult>,
        column_indices: Vec<usize>,
        output_columns: Vec<String>,
    ) -> Self {
        // Pre-allocate Inline storage - no Arc overhead for intermediate results
        let capacity = column_indices.len();
        Self {
            inner,
            column_indices,
            output_columns,
            current_row: Row::with_capacity(capacity),
        }
    }
}

impl QueryResult for StreamingProjectionResult {
    fn columns(&self) -> &[String] {
        &self.output_columns
    }

    fn next(&mut self) -> bool {
        if self.inner.next() {
            // OPTIMIZATION: Use Inline storage - no Arc overhead for intermediate results
            self.current_row.reserve_inline(self.column_indices.len());
            self.current_row.clear_inline();
            let source_row = self.inner.row();
            for &idx in &self.column_indices {
                self.current_row.push_inline(
                    source_row
                        .get(idx)
                        .cloned()
                        .unwrap_or(Value::null_unknown()),
                );
            }
            true
        } else {
            false
        }
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        if dest.len() != self.current_row.len() {
            return Err(radixdb_core::Error::internal(format!(
                "scan destination has {} values but row has {} columns",
                dest.len(),
                self.current_row.len()
            )));
        }
        for (i, value) in self.current_row.iter().enumerate() {
            dest[i] = value.clone();
        }
        Ok(())
    }

    fn row(&self) -> &Row {
        &self.current_row
    }

    fn take_row(&mut self) -> Row {
        std::mem::take(&mut self.current_row)
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    fn rows_affected(&self) -> i64 {
        0
    }

    fn last_insert_id(&self) -> i64 {
        0
    }

    fn last_error(&mut self) -> Option<radixdb_core::Error> {
        self.inner.last_error()
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

/// Columnar result that stores data column-major and materializes rows lazily.
///
/// This is optimized for window functions and other operations that naturally
/// produce column-major output. Instead of allocating millions of Row objects
/// upfront, it stores data as `Vec<Value>` per column and materializes rows
/// on-demand during iteration.
///
/// Key benefits:
/// - Reduces allocations from O(num_rows) to O(num_columns)
/// - Reuses a single Row buffer during iteration (zero per-row allocation)
/// - Natural fit for window function results
///
/// The returned row from `row()` is valid only until the next `next()` call.
pub struct ColumnarResult {
    /// Column names
    columns: CompactArc<Vec<String>>,
    /// Column-major storage: data[col_idx][row_idx]
    data: Vec<Vec<Value>>,
    /// Number of rows (cached for fast access)
    num_rows: usize,
    /// Current row index (None before first next())
    current_index: Option<usize>,
    /// Reusable row buffer - avoids allocation per row
    current_row: Row,
    /// Whether the result is closed
    closed: bool,
}

impl ColumnarResult {
    /// Create a new columnar result from column-major data
    ///
    /// # Arguments
    /// * `columns` - Column names (must match data.len())
    /// * `data` - Column-major data where `data[col_idx]` contains all values for that column
    ///
    /// # Panics
    /// Panics if columns.len() != data.len() or if columns have different lengths
    pub fn new(columns: Vec<String>, data: Vec<Vec<Value>>) -> Self {
        debug_assert!(
            columns.len() == data.len(),
            "columns.len() ({}) != data.len() ({})",
            columns.len(),
            data.len()
        );

        let num_rows = data.first().map(|c| c.len()).unwrap_or(0);

        // Verify all columns have the same length
        #[cfg(debug_assertions)]
        for (i, col) in data.iter().enumerate() {
            debug_assert!(
                col.len() == num_rows,
                "column {} has {} rows but expected {}",
                i,
                col.len(),
                num_rows
            );
        }

        // Pre-allocate the row buffer with capacity for all columns
        let num_cols = columns.len();

        Self {
            columns: CompactArc::new(columns),
            data,
            num_rows,
            current_index: None,
            current_row: Row::with_capacity(num_cols),
            closed: false,
        }
    }

    /// Create with CompactArc columns (zero-copy)
    pub fn with_arc_columns(columns: CompactArc<Vec<String>>, data: Vec<Vec<Value>>) -> Self {
        let num_rows = data.first().map(|c| c.len()).unwrap_or(0);
        let num_cols = columns.len();

        Self {
            columns,
            data,
            num_rows,
            current_index: None,
            current_row: Row::with_capacity(num_cols),
            closed: false,
        }
    }

    /// Get the number of rows
    #[inline]
    pub fn row_count(&self) -> usize {
        self.num_rows
    }

    /// Get the number of columns
    #[inline]
    pub fn column_count(&self) -> usize {
        self.data.len()
    }
}

impl QueryResult for ColumnarResult {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn columns_arc(&self) -> Option<CompactArc<Vec<String>>> {
        Some(CompactArc::clone(&self.columns))
    }

    #[inline]
    fn next(&mut self) -> bool {
        if self.closed {
            return false;
        }

        let next_idx = match self.current_index {
            None => 0,
            Some(i) => i + 1,
        };

        if next_idx >= self.num_rows {
            return false;
        }

        self.current_index = Some(next_idx);

        // OPTIMIZATION: Lazy capacity reservation - only allocate when Row was taken
        self.current_row.reserve_inline(self.data.len());
        // Materialize row from column data - clear_inline preserves capacity
        self.current_row.clear_inline();
        for col_data in &self.data {
            // Safety: we verified all columns have num_rows elements
            self.current_row.push_inline(col_data[next_idx].clone());
        }

        true
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        if dest.len() != self.current_row.len() {
            return Err(radixdb_core::Error::internal(format!(
                "scan destination has {} values but row has {} columns",
                dest.len(),
                self.current_row.len()
            )));
        }
        for (i, value) in self.current_row.iter().enumerate() {
            dest[i] = value.clone();
        }
        Ok(())
    }

    #[inline]
    fn row(&self) -> &Row {
        &self.current_row
    }

    fn take_row(&mut self) -> Row {
        // Don't pre-allocate here - let next() do lazy initialization
        // This eliminates one allocation per row when the caller drops the Row
        std::mem::take(&mut self.current_row)
    }

    fn close(&mut self) -> Result<()> {
        self.closed = true;
        // Clear data to free memory
        self.data.clear();
        Ok(())
    }

    fn rows_affected(&self) -> i64 {
        0
    }

    fn last_insert_id(&self) -> i64 {
        0
    }

    fn estimated_count(&self) -> Option<usize> {
        Some(self.num_rows)
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

#[cfg(test)]
mod tests;
