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

//! Result trait for query results
//!

use rustc_hash::FxHashMap;

use crate::traits::{Scanner, TypedBatchFallbackReason, TypedColumnBatch};
use radixdb_core::value::NULL_VALUE;
use radixdb_core::CompactArc;
use radixdb_core::{Result, Row, Value};

/// Source slot retained by an internal deferred row projection.
///
/// This is an executor/storage bridge, not part of the public SQL result
/// contract. It lets a recursive JOIN hand its compact row representation to
/// the next operator without first constructing a complete owned [`Row`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeferredColumnSource {
    Left(usize),
    Right(usize),
}

/// Compact row representation carried across an internal [`QueryResult`]
/// boundary.
///
/// Normal consumers still observe the established owned [`Row`] contract via
/// [`QueryResult::row`] and [`QueryResult::take_row`]. Only executor adapters
/// opt into [`QueryResult::take_deferred_row`].
#[doc(hidden)]
#[derive(Debug, Clone)]
pub enum DeferredRow {
    Owned(Row),
    Shared {
        rows: CompactArc<Vec<Row>>,
        row_index: usize,
    },
    Projected {
        left: Box<DeferredRow>,
        right: Box<DeferredRow>,
        columns: CompactArc<[DeferredColumnSource]>,
    },
    Remapped {
        row: Box<DeferredRow>,
        columns: CompactArc<[usize]>,
    },
}

impl DeferredRow {
    #[inline]
    pub fn owned(row: Row) -> Self {
        Self::Owned(row)
    }

    #[inline]
    pub fn shared(rows: CompactArc<Vec<Row>>, row_index: usize) -> Self {
        assert!(
            row_index < rows.len(),
            "deferred shared row index outside batch"
        );
        Self::Shared { rows, row_index }
    }

    #[inline]
    pub fn projected(
        left: DeferredRow,
        right: DeferredRow,
        columns: CompactArc<[DeferredColumnSource]>,
    ) -> Self {
        Self::Projected {
            left: Box::new(left),
            right: Box::new(right),
            columns,
        }
    }

    #[inline]
    pub fn remapped(row: DeferredRow, columns: CompactArc<[usize]>) -> Self {
        Self::Remapped {
            row: Box::new(row),
            columns,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Owned(row) => row.len(),
            Self::Shared { rows, row_index } => rows[*row_index].len(),
            Self::Projected { columns, .. } => columns.len(),
            Self::Remapped { columns, .. } => columns.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn is_deferred(&self) -> bool {
        !matches!(self, Self::Owned(_))
    }

    /// Conservative retained-size estimate for request-local backpressure.
    ///
    /// Shared batches are charged by their owning scan/hash state, so a shared
    /// row charges only its handle. Owned rows charge their variable-width
    /// payload, while projected/remapped rows charge the compact graph they
    /// actually retain. This deliberately avoids materializing a deferred JOIN
    /// row merely to decide whether another parallel probe batch may be pulled.
    pub fn estimated_retained_bytes(&self) -> usize {
        fn values_bytes(values: &[Value]) -> usize {
            values.iter().fold(0_usize, |total, value| {
                let payload = match value {
                    Value::Text(text) => text.len(),
                    Value::Extension(bytes) => bytes.len(),
                    _ => 0,
                };
                total
                    .saturating_add(std::mem::size_of::<Value>())
                    .saturating_add(payload)
            })
        }

        match self {
            Self::Owned(row) => {
                std::mem::size_of::<Self>().saturating_add(values_bytes(row.as_slice()))
            }
            Self::Shared { .. } => std::mem::size_of::<Self>(),
            Self::Projected {
                left,
                right,
                columns,
            } => std::mem::size_of::<Self>()
                .saturating_add(left.estimated_retained_bytes())
                .saturating_add(right.estimated_retained_bytes())
                .saturating_add(
                    columns
                        .len()
                        .saturating_mul(std::mem::size_of::<DeferredColumnSource>()),
                ),
            Self::Remapped { row, columns } => std::mem::size_of::<Self>()
                .saturating_add(row.estimated_retained_bytes())
                .saturating_add(columns.len().saturating_mul(std::mem::size_of::<usize>())),
        }
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&Value> {
        match self {
            Self::Owned(row) => row.get(index),
            Self::Shared { rows, row_index } => rows[*row_index].get(index),
            Self::Projected {
                left,
                right,
                columns,
            } => match columns.get(index)? {
                DeferredColumnSource::Left(source_index) => left.get(*source_index),
                DeferredColumnSource::Right(source_index) => right.get(*source_index),
            },
            Self::Remapped { row, columns } => row.get(*columns.get(index)?),
        }
    }

    pub fn to_owned(&self) -> Row {
        let row = match self {
            Self::Owned(row) => row.clone(),
            Self::Shared { rows, row_index } => rows[*row_index].clone(),
            Self::Projected { columns, .. } => {
                let mut row = Row::with_capacity(columns.len());
                for index in 0..columns.len() {
                    row.push(self.get(index).cloned().unwrap_or(NULL_VALUE));
                }
                row
            }
            Self::Remapped { columns, .. } => {
                let mut row = Row::with_capacity(columns.len());
                for index in 0..columns.len() {
                    row.push(self.get(index).cloned().unwrap_or(NULL_VALUE));
                }
                row
            }
        };
        crate::instrumentation::record_join_value_copies(row.as_slice());
        row
    }

    #[inline]
    pub fn into_owned(self) -> Row {
        match self {
            Self::Owned(row) => row,
            Self::Shared { rows, row_index } => {
                let row = rows[row_index].clone();
                crate::instrumentation::record_join_value_copies(row.as_slice());
                row
            }
            deferred @ (Self::Projected { .. } | Self::Remapped { .. }) => deferred.to_owned(),
        }
    }
}

/// Lower-level streaming result shared by storage and execution operators.
///
/// This is not the public embedded cursor or public row facade. The API layer
/// adapts it once into its own cursor and typed row contracts. The trait
/// provides internal iteration, direct row access, column metadata and
/// aliasing without depending on the executor or public API.
///
/// # Example
///
/// ```ignore
/// let result = transaction.select("users", &["id", "name"], None)?;
/// println!("Columns: {:?}", result.columns());
/// while result.next() {
///     let row = result.row();
///     // Process row...
/// }
/// result.close()?;
/// ```
pub trait QueryResult: Send {
    /// Returns the column names in the result
    ///
    /// If aliases are set, this returns the aliased column names.
    fn columns(&self) -> &[String];

    /// Returns column names as Arc for zero-copy sharing
    ///
    /// Upper-layer adapters can use this to avoid cloning column names. The
    /// default returns `None`, so an adapter falls back to `columns()`.
    fn columns_arc(&self) -> Option<CompactArc<Vec<String>>> {
        None
    }

    /// Moves the cursor to the next row
    ///
    /// Returns `true` if there is another row available, `false` otherwise.
    fn next(&mut self) -> bool;

    /// Scans the current row into the provided values
    ///
    /// The number of destination values must match the number of columns.
    /// Values are converted to the destination types where possible.
    fn scan(&self, dest: &mut [Value]) -> Result<()>;

    /// Returns the current row directly without copying
    ///
    /// This is a high-performance method to access raw column values.
    /// The returned row is valid until the next call to `next()` or `close()`.
    fn row(&self) -> &Row;

    /// Takes ownership of the current row (avoids clone)
    ///
    /// This is a high-performance method that moves the row data out of the result.
    /// After calling this, `row()` will return an empty row until `next()` is called.
    /// The default implementation clones the row for backward compatibility.
    fn take_row(&mut self) -> Row {
        self.row().clone()
    }

    /// Takes the current row while preserving an internal deferred JOIN shape
    /// when the result supports it.
    ///
    /// The default keeps every existing result implementation source-compatible
    /// and simply wraps its normal owned row. Executor-only results override it.
    #[doc(hidden)]
    fn take_deferred_row(&mut self) -> DeferredRow {
        DeferredRow::owned(self.take_row())
    }

    /// Whether [`QueryResult::take_deferred_row`] can return an executor row
    /// graph instead of merely wrapping the ordinary owned row.
    ///
    /// JOIN planning uses this capability to keep a deferred recursive side on
    /// the streaming/probe path. The default is deliberately false so existing
    /// public result implementations retain their established contract.
    #[doc(hidden)]
    fn preserves_deferred_rows(&self) -> bool {
        false
    }

    /// Physical ascending, NULLS LAST ordering guaranteed by this result.
    ///
    /// The indices address [`QueryResult::columns`]. This is an executor
    /// certificate, never an inference from the current row contents. Results
    /// that filter or limit without reordering may forward it; projections must
    /// remap it, while every other wrapper keeps the fail-closed default.
    #[doc(hidden)]
    fn ascending_nulls_last_ordering(&self) -> Option<Vec<usize>> {
        None
    }

    /// Closes the result set and releases resources
    ///
    /// Default implementation does nothing. Override if cleanup is needed.
    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    /// Returns the number of rows affected by an INSERT, UPDATE, or DELETE
    ///
    fn rows_affected(&self) -> i64;

    /// Returns the last inserted ID for an INSERT operation
    ///
    fn last_insert_id(&self) -> i64;

    /// Try to extract all rows as `CompactArc<Vec<Row>>` for zero-copy joins
    ///
    /// Returns None if the result cannot provide Arc-wrapped rows.
    /// This consumes the result - after calling, iteration will yield no more rows.
    /// Default implementation returns None.
    fn try_into_arc_rows(&mut self) -> Option<CompactArc<Vec<Row>>> {
        None
    }

    /// Returns an estimate of the total number of rows in the result.
    ///
    /// This is used for pre-allocating vectors to avoid reallocations.
    /// Returns None if the count is unknown. Default implementation returns None.
    fn estimated_count(&self) -> Option<usize> {
        None
    }

    /// Whether the result can yield decoded typed column batches without
    /// constructing a row/value object for every result record.
    ///
    /// This is deliberately opt-in. Any executor wrapper that changes row
    /// order, filtering, projection, or value semantics inherits the safe
    /// default (`false`) and continues through the established row contract.
    fn supports_typed_batches(&self) -> bool {
        false
    }

    /// If `supports_typed_batches()` is false, return the best semantic reason.
    ///
    /// This is a diagnostics contract. It must not alter cursor state.
    fn typed_batch_fallback_reason(&self) -> Option<TypedBatchFallbackReason> {
        Some(TypedBatchFallbackReason::UnsupportedResultShape)
    }

    /// Advance to the next typed column batch.
    ///
    /// Callers must first observe `supports_typed_batches() == true`. `None`
    /// is EOF. A returned batch is in the same order and has the same
    /// projected columns as normal row iteration.
    fn next_typed_batch(&mut self) -> Result<Option<TypedColumnBatch>> {
        Ok(None)
    }

    /// Returns a pending error from the last `next()` call, if any.
    ///
    /// When `next()` returns false due to a runtime error (e.g. invalid REGEXP
    /// pattern), this method returns the error so callers can surface it.
    /// Default returns None (no error).
    fn last_error(&mut self) -> Option<radixdb_core::Error> {
        None
    }

    /// Sets column aliases for this result
    ///
    /// The map keys are alias names, values are original column names.
    /// Returns a new result with the aliases applied.
    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult>;
}

/// Neutral result wrapper that changes column presentation only.
pub struct AliasedResult {
    inner: Box<dyn QueryResult>,
    aliased_columns: Vec<String>,
}

impl AliasedResult {
    pub fn new(inner: Box<dyn QueryResult>, aliases: FxHashMap<String, String>) -> Self {
        let aliased_columns = inner
            .columns()
            .iter()
            .map(|column| {
                aliases
                    .iter()
                    .find(|(_, original)| *original == column)
                    .map_or_else(|| column.clone(), |(alias, _)| alias.clone())
            })
            .collect();
        Self {
            inner,
            aliased_columns,
        }
    }
}

impl QueryResult for AliasedResult {
    fn columns(&self) -> &[String] {
        &self.aliased_columns
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
        self.inner.ascending_nulls_last_ordering()
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

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(Self::new(self, aliases))
    }
}

/// Neutral query-result adapter backed directly by a storage scanner.
///
/// The adapter owns no SQL or executor policy. It only exposes the existing
/// row/typed-batch scanner contract through [`QueryResult`].
pub struct ScannerResult {
    scanner: Box<dyn Scanner>,
    columns: Vec<String>,
    current_row: Row,
    has_current: bool,
}

impl ScannerResult {
    pub fn new(scanner: Box<dyn Scanner>, columns: Vec<String>) -> Self {
        Self {
            scanner,
            columns,
            current_row: Row::new(),
            has_current: false,
        }
    }
}

impl QueryResult for ScannerResult {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn next(&mut self) -> bool {
        if self.scanner.next() {
            self.current_row = self.scanner.take_row();
            self.has_current = true;
            true
        } else {
            self.has_current = false;
            false
        }
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
        for (dest, value) in dest.iter_mut().zip(self.current_row.iter()) {
            *dest = value.clone();
        }
        Ok(())
    }

    fn row(&self) -> &Row {
        assert!(self.has_current, "row() called without successful next()");
        &self.current_row
    }

    fn take_row(&mut self) -> Row {
        assert!(
            self.has_current,
            "take_row() called without successful next()"
        );
        std::mem::take(&mut self.current_row)
    }

    fn close(&mut self) -> Result<()> {
        self.scanner.close()
    }

    fn last_error(&mut self) -> Option<radixdb_core::Error> {
        self.scanner.err().cloned()
    }

    fn rows_affected(&self) -> i64 {
        0
    }

    fn last_insert_id(&self) -> i64 {
        0
    }

    fn estimated_count(&self) -> Option<usize> {
        self.scanner.estimated_count()
    }

    fn supports_typed_batches(&self) -> bool {
        !self.has_current && self.scanner.supports_typed_batches()
    }

    fn typed_batch_fallback_reason(&self) -> Option<TypedBatchFallbackReason> {
        if self.supports_typed_batches() {
            None
        } else if self.has_current {
            Some(TypedBatchFallbackReason::RowAlreadyFetched)
        } else {
            self.scanner.typed_batch_fallback_reason()
        }
    }

    fn next_typed_batch(&mut self) -> Result<Option<TypedColumnBatch>> {
        if self.has_current {
            return Err(radixdb_core::Error::internal(
                "typed batch requested after row-oriented scanner advance",
            ));
        }
        self.scanner.next_typed_batch()
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

/// A simple in-memory query result (useful for testing and simple results)
pub struct MemoryResult {
    columns: Vec<String>,
    rows: Vec<Row>,
    current_index: Option<usize>,
    rows_affected: i64,
    last_insert_id: i64,
    closed: bool,
}

impl MemoryResult {
    /// Creates a new empty result with the given columns
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
            current_index: None,
            rows_affected: 0,
            last_insert_id: 0,
            closed: false,
        }
    }

    /// Creates a result with columns and rows
    pub fn with_rows(columns: Vec<String>, rows: Vec<Row>) -> Self {
        Self {
            columns,
            rows,
            current_index: None,
            rows_affected: 0,
            last_insert_id: 0,
            closed: false,
        }
    }

    /// Creates a result for a modification operation (INSERT/UPDATE/DELETE)
    pub fn for_modification(rows_affected: i64, last_insert_id: i64) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            current_index: None,
            rows_affected,
            last_insert_id,
            closed: false,
        }
    }

    /// Adds a row to the result
    pub fn add_row(&mut self, row: Row) {
        self.rows.push(row);
    }

    /// Sets the rows affected count
    pub fn set_rows_affected(&mut self, count: i64) {
        self.rows_affected = count;
    }

    /// Sets the last insert ID
    pub fn set_last_insert_id(&mut self, id: i64) {
        self.last_insert_id = id;
    }
}

impl QueryResult for MemoryResult {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn estimated_count(&self) -> Option<usize> {
        Some(self.rows.len())
    }

    fn next(&mut self) -> bool {
        if self.closed {
            return false;
        }

        let next_index = match self.current_index {
            None => 0,
            Some(i) => i + 1,
        };

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

        for (i, value) in row.iter().enumerate() {
            dest[i] = value.clone();
        }

        Ok(())
    }

    fn row(&self) -> &Row {
        match self.current_index {
            Some(i) if i < self.rows.len() => &self.rows[i],
            _ => panic!("row() called without successful next()"),
        }
    }

    /// Optimized take_row that swaps out the row instead of cloning
    fn take_row(&mut self) -> Row {
        match self.current_index {
            Some(i) if i < self.rows.len() => std::mem::take(&mut self.rows[i]),
            _ => panic!("take_row() called without successful next()"),
        }
    }

    fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }

    fn rows_affected(&self) -> i64 {
        self.rows_affected
    }

    fn last_insert_id(&self) -> i64 {
        self.last_insert_id
    }

    fn with_aliases(
        mut self: Box<Self>,
        aliases: FxHashMap<String, String>,
    ) -> Box<dyn QueryResult> {
        // Apply aliases to column names
        for col in &mut self.columns {
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

/// An empty result that returns no rows
pub struct EmptyResult {
    columns: Vec<String>,
    rows_affected: i64,
    last_insert_id: i64,
}

impl EmptyResult {
    /// Creates a new empty result
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            rows_affected: 0,
            last_insert_id: 0,
        }
    }

    /// Creates an empty result for a modification operation
    pub fn for_modification(rows_affected: i64, last_insert_id: i64) -> Self {
        Self {
            columns: Vec::new(),
            rows_affected,
            last_insert_id,
        }
    }
}

impl Default for EmptyResult {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryResult for EmptyResult {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn next(&mut self) -> bool {
        false
    }

    fn scan(&self, _dest: &mut [Value]) -> Result<()> {
        Err(radixdb_core::Error::internal(
            "scan() called on empty result",
        ))
    }

    fn row(&self) -> &Row {
        panic!("row() called on empty result")
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn rows_affected(&self) -> i64 {
        self.rows_affected
    }

    fn last_insert_id(&self) -> i64 {
        self.last_insert_id
    }

    fn with_aliases(self: Box<Self>, _aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::VecScanner;

    #[test]
    fn test_memory_result_empty() {
        let mut result = MemoryResult::new(vec!["id".to_string(), "name".to_string()]);

        assert_eq!(result.columns(), &["id", "name"]);
        assert!(!result.next());
        assert_eq!(result.rows_affected(), 0);
        assert_eq!(result.last_insert_id(), 0);
    }

    #[test]
    fn test_memory_result_with_rows() {
        let rows = vec![
            Row::from_values(vec![Value::Integer(1), Value::text("Alice")]),
            Row::from_values(vec![Value::Integer(2), Value::text("Bob")]),
        ];

        let mut result = MemoryResult::with_rows(vec!["id".to_string(), "name".to_string()], rows);

        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(1)));

        assert!(result.next());
        assert_eq!(result.row().get(0), Some(&Value::Integer(2)));

        assert!(!result.next());
    }

    #[test]
    fn test_memory_result_scan() {
        let rows = vec![Row::from_values(vec![
            Value::Integer(42),
            Value::text("test"),
        ])];

        let mut result = MemoryResult::with_rows(vec!["id".to_string(), "name".to_string()], rows);

        assert!(result.next());

        let mut dest = vec![Value::null_unknown(), Value::null_unknown()];
        result.scan(&mut dest).unwrap();

        assert_eq!(dest[0], Value::Integer(42));
        assert_eq!(dest[1], Value::text("test"));
    }

    #[test]
    fn test_memory_result_for_modification() {
        let result = MemoryResult::for_modification(5, 100);

        assert_eq!(result.rows_affected(), 5);
        assert_eq!(result.last_insert_id(), 100);
    }

    #[test]
    fn test_memory_result_close() {
        let rows = vec![Row::from_values(vec![Value::Integer(1)])];
        let mut result = MemoryResult::with_rows(vec!["id".to_string()], rows);

        assert!(result.next());
        assert!(result.close().is_ok());
        assert!(!result.next()); // After close, next returns false
    }

    #[test]
    fn test_memory_result_with_aliases() {
        let rows = vec![Row::from_values(vec![Value::Integer(1)])];
        let result = Box::new(MemoryResult::with_rows(vec!["user_id".to_string()], rows));

        let mut aliases = FxHashMap::default();
        aliases.insert("id".to_string(), "user_id".to_string());

        let aliased = result.with_aliases(aliases);
        assert_eq!(aliased.columns(), &["id"]);
    }

    #[test]
    fn test_empty_result() {
        let mut result = EmptyResult::new();

        assert!(result.columns().is_empty());
        assert!(!result.next());
        assert_eq!(result.rows_affected(), 0);
        assert!(result.close().is_ok());
    }

    #[test]
    fn test_empty_result_for_modification() {
        let result = EmptyResult::for_modification(10, 0);

        assert_eq!(result.rows_affected(), 10);
        assert_eq!(result.last_insert_id(), 0);
    }

    #[test]
    fn scanner_result_streams_rows_and_preserves_alias_wrapper_contract() {
        let scanner = VecScanner::new(vec![
            Row::from_values(vec![Value::Integer(1), Value::text("one")]),
            Row::from_values(vec![Value::Integer(2), Value::text("two")]),
        ]);
        let result: Box<dyn QueryResult> = Box::new(ScannerResult::new(
            Box::new(scanner),
            vec!["id".to_string(), "payload".to_string()],
        ));
        let mut aliases = FxHashMap::default();
        aliases.insert("value".to_string(), "payload".to_string());
        let mut result = result.with_aliases(aliases);

        assert_eq!(result.columns(), &["id", "value"]);
        assert!(result.next());
        assert_eq!(result.take_row().get(0), Some(&Value::Integer(1)));
        assert!(result.next());
        assert_eq!(result.row().get(1), Some(&Value::text("two")));
        assert!(!result.next());
        assert!(result.last_error().is_none());
        result.close().unwrap();
    }

    #[test]
    fn scanner_result_surfaces_scanner_terminal_error() {
        let scanner = VecScanner::with_error(radixdb_core::Error::internal("scanner terminal"));
        let mut result = ScannerResult::new(Box::new(scanner), vec!["id".to_string()]);

        assert!(!result.next());
        assert!(result
            .last_error()
            .is_some_and(|error| error.to_string().contains("scanner terminal")));
    }
}
