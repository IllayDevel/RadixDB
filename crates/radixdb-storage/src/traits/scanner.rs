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

//! Scanner trait for iterating over table rows
//!

use crate::volume::column::ColumnData;
use radixdb_core::{Result, Row};

/// One decoded, output-ordered typed column batch.
///
/// This is intentionally a storage-level value: it carries no protocol types
/// and does not construct `Row`/`Value` objects. Consumers that cannot retain
/// a columnar representation simply use the existing row iterator methods.
pub struct TypedColumnBatch {
    row_count: usize,
    columns: Vec<ColumnData>,
}

/// Why a scanner/result cannot currently yield `TypedColumnBatch`.
///
/// This is intentionally a semantic reason, not a tuning hint. The server can
/// fall back to the row protocol safely, but diagnostics and benchmark reports
/// must still show *why* the columnar path was not used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypedBatchFallbackReason {
    /// A scanner has already observed a pending storage/runtime error.
    PendingError,
    /// A row-oriented cursor already holds the current row.
    RowAlreadyFetched,
    /// The scanner has already advanced through the row API.
    RowIterationStarted,
    /// The result/cursor is already closed.
    Closed,
    /// The storage source is not backed by an immutable DATA artifact.
    NotArtifactBacked,
    /// A row-level filter is still required.
    RowFilter,
    /// A dictionary filter is still required.
    DictionaryFilter,
    /// An index/matching-row selection is still required.
    IndexSelection,
    /// Typed predicates are still a scanner-side row selection boundary.
    TypedPredicate,
    /// An exact typed filter is still required.
    ExactTypedFilter,
    /// A filter was covered by typed predicates but the batch contract has not
    /// been proven for that mixed path.
    FilterCoveredByTypedPredicates,
    /// Row-group skip metadata changes the selected group stream.
    RowGroupSkips,
    /// The projection has no columns.
    EmptyProjection,
    /// The projection repeats the same output column.
    DuplicateProjection,
    /// Schema evolution mapping does not contain the requested output column.
    SchemaMappingMissingColumn,
    /// A stored physical column type has no typed batch representation yet.
    UnsupportedStorageType,
    /// A schema-evolved default value has no typed batch representation yet.
    UnsupportedSchemaDefault,
    /// The scanner range is internally inconsistent.
    InvalidRange,
    /// A merged scanner contains at least one row-only source.
    MergedSource,
    /// A merged scanner contains both typed-batch capable and row-only sources.
    MixedTypedAndRowSources,
    /// The query result shape is row-only by contract.
    UnsupportedResultShape,
}

impl TypedBatchFallbackReason {
    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PendingError => "pending-error",
            Self::RowAlreadyFetched => "row-already-fetched",
            Self::RowIterationStarted => "row-iteration-started",
            Self::Closed => "closed",
            Self::NotArtifactBacked => "not-artifact-backed",
            Self::RowFilter => "row-filter",
            Self::DictionaryFilter => "dictionary-filter",
            Self::IndexSelection => "index-selection",
            Self::TypedPredicate => "typed-predicate",
            Self::ExactTypedFilter => "exact-typed-filter",
            Self::FilterCoveredByTypedPredicates => "filter-covered-by-typed-predicates",
            Self::RowGroupSkips => "row-group-skips",
            Self::EmptyProjection => "empty-projection",
            Self::DuplicateProjection => "duplicate-projection",
            Self::SchemaMappingMissingColumn => "schema-mapping-missing-column",
            Self::UnsupportedStorageType => "unsupported-storage-type",
            Self::UnsupportedSchemaDefault => "unsupported-schema-default",
            Self::InvalidRange => "invalid-range",
            Self::MergedSource => "merged-source",
            Self::MixedTypedAndRowSources => "mixed-typed-and-row-sources",
            Self::UnsupportedResultShape => "unsupported-result-shape",
        }
    }
}

impl TypedColumnBatch {
    pub fn new(row_count: usize, columns: Vec<ColumnData>) -> Self {
        debug_assert!(columns.iter().all(|column| column.len() == row_count));
        Self { row_count, columns }
    }

    #[inline]
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    #[inline]
    pub fn columns(&self) -> &[ColumnData] {
        &self.columns
    }

    #[inline]
    pub fn into_columns(self) -> Vec<ColumnData> {
        self.columns
    }
}

/// Scanner provides an iterator over rows in a table
///
/// This trait is the primary way to read rows from a table. It follows
/// an iterator pattern where `next()` advances to the next row and
/// `row()` returns the current row.
///
/// # Example
///
/// ```ignore
/// let scanner = table.scan(&[0, 1], None)?;
/// while scanner.next() {
///     let row = scanner.row();
///     // Process row...
/// }
/// if let Some(err) = scanner.err() {
///     // Handle error...
/// }
/// scanner.close()?;
/// ```
pub trait Scanner: Send {
    /// Advances the scanner to the next row
    ///
    /// Returns `true` if there is another row available, `false` otherwise.
    /// After returning `false`, the caller should check `err()` to see if
    /// iteration stopped due to an error.
    fn next(&mut self) -> bool;

    /// Returns the current row
    ///
    /// The returned row is valid until the next call to `next()` or `close()`.
    /// Calling this before `next()` or after `next()` returns `false` is undefined.
    fn row(&self) -> &Row;

    /// Returns any error that occurred during scanning
    ///
    /// Should be called after `next()` returns `false` to check if iteration
    /// stopped due to an error or simply because there are no more rows.
    fn err(&self) -> Option<&radixdb_core::Error>;

    /// Closes the scanner and releases any resources
    ///
    /// This should be called when done with the scanner, even if `next()`
    /// returned `false` due to reaching the end of the data.
    fn close(&mut self) -> Result<()>;

    /// Takes ownership of the current row (avoids clone)
    ///
    /// This is more efficient than `row().clone()` when you need to move
    /// the row data out of the scanner. After calling this, the internal
    /// row buffer may be empty until `next()` is called again.
    /// The default implementation clones the row for backward compatibility.
    fn take_row(&mut self) -> Row {
        self.row().clone()
    }

    /// Returns an estimate of the total number of rows in the scan.
    ///
    /// This is used for pre-allocating vectors to avoid reallocations.
    /// Returns None if the count is unknown. Default implementation returns None.
    fn estimated_count(&self) -> Option<usize> {
        None
    }

    /// Takes ownership of the current row along with its row ID
    ///
    /// This is more efficient than `row().clone()` when you need to move
    /// the row data out of the scanner with its associated ID. After calling this,
    /// the internal row buffer may be empty until `next()` is called again.
    fn take_row_with_id(&mut self) -> Result<(i64, Row)> {
        Err(radixdb_core::Error::internal(
            "scanner does not expose physical row identity",
        ))
    }

    /// Returns the current row ID
    ///
    /// Returns the internal row ID for the current row. This is used for
    /// row-level operations that need to track row identity.
    fn current_row_id(&self) -> Result<i64> {
        Err(radixdb_core::Error::internal(
            "scanner does not expose physical row identity",
        ))
    }

    /// Collect all remaining matching row IDs without constructing row
    /// payloads when the concrete scanner has an exact bulk implementation.
    ///
    /// `Ok(false)` means the scanner was not advanced and the caller must use
    /// the normal `next()` contract. Implementations returning `Ok(true)` have
    /// consumed the scanner through EOF and appended IDs in scan order.
    fn collect_remaining_row_ids(&mut self, _output: &mut Vec<i64>) -> Result<bool> {
        Ok(false)
    }

    /// Best-effort non-blocking/low-blocking prefetch hook.
    ///
    /// Implementations may start bounded background work for future rows, but
    /// must not advance scanner position or change the externally visible row
    /// order. The default is a no-op for scanners that have no useful warmup.
    fn warmup(&mut self) {}

    /// Whether this scanner can advance in decoded typed column batches.
    ///
    /// Returning false is always safe and preserves the row contract. A
    /// scanner must return true only when every remaining row is represented
    /// by its batches in exactly the normal scan order.
    fn supports_typed_batches(&self) -> bool {
        false
    }

    /// If `supports_typed_batches()` is false, return the best semantic reason.
    ///
    /// Implementations should keep this cheap and side-effect free. The
    /// default marks old row-only scanners explicitly instead of silently
    /// hiding them behind a bare `false`.
    fn typed_batch_fallback_reason(&self) -> Option<TypedBatchFallbackReason> {
        Some(TypedBatchFallbackReason::UnsupportedResultShape)
    }

    /// Advance to the next typed column batch.
    ///
    /// Only callers that first observed `supports_typed_batches() == true`
    /// may call this. `Ok(None)` is EOF; all row-oriented scanner state is
    /// advanced past the returned batch.
    fn next_typed_batch(&mut self) -> Result<Option<TypedColumnBatch>> {
        Ok(None)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn is_warmed_for_test(&self) -> bool {
        false
    }
}

/// An empty scanner that immediately returns no rows
pub struct EmptyScanner {
    empty_row: Row,
    closed: bool,
}

impl EmptyScanner {
    /// Creates a new empty scanner
    pub fn new() -> Self {
        Self {
            empty_row: Row::new(),
            closed: false,
        }
    }
}

impl Default for EmptyScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl Scanner for EmptyScanner {
    fn next(&mut self) -> bool {
        false
    }

    fn row(&self) -> &Row {
        // This should never be called since next() always returns false
        &self.empty_row
    }

    fn err(&self) -> Option<&radixdb_core::Error> {
        None
    }

    fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }

    fn estimated_count(&self) -> Option<usize> {
        Some(0)
    }
}

/// A scanner over a vector of rows (useful for testing)
pub struct VecScanner {
    rows: Vec<Row>,
    current_index: Option<usize>,
    error: Option<radixdb_core::Error>,
    closed: bool,
}

impl VecScanner {
    /// Creates a new scanner over the given rows
    pub fn new(rows: Vec<Row>) -> Self {
        Self {
            rows,
            current_index: None,
            error: None,
            closed: false,
        }
    }

    /// Creates a scanner that will return an error
    pub fn with_error(error: radixdb_core::Error) -> Self {
        Self {
            rows: Vec::new(),
            current_index: None,
            error: Some(error),
            closed: false,
        }
    }
}

impl Scanner for VecScanner {
    fn next(&mut self) -> bool {
        if self.closed || self.error.is_some() {
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

    fn row(&self) -> &Row {
        match self.current_index {
            Some(i) if i < self.rows.len() => &self.rows[i],
            _ => {
                // Panic in debug mode, return first row or panic in release
                // This is a programming error - row() called without next()
                panic!("row() called without successful next()")
            }
        }
    }

    fn err(&self) -> Option<&radixdb_core::Error> {
        self.error.as_ref()
    }

    fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }

    fn estimated_count(&self) -> Option<usize> {
        Some(self.rows.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_core::Value;

    #[test]
    fn test_empty_scanner() {
        let mut scanner = EmptyScanner::new();
        assert!(!scanner.next());
        assert!(scanner.err().is_none());
        assert!(scanner.close().is_ok());
    }

    #[test]
    fn test_vec_scanner_empty() {
        let mut scanner = VecScanner::new(vec![]);
        assert!(!scanner.next());
        assert!(scanner.err().is_none());
    }

    #[test]
    fn test_vec_scanner_with_rows() {
        let rows = vec![
            Row::from_values(vec![Value::Integer(1), Value::text("a")]),
            Row::from_values(vec![Value::Integer(2), Value::text("b")]),
            Row::from_values(vec![Value::Integer(3), Value::text("c")]),
        ];

        let mut scanner = VecScanner::new(rows);

        assert!(scanner.next());
        assert_eq!(scanner.row().get(0), Some(&Value::Integer(1)));

        assert!(scanner.next());
        assert_eq!(scanner.row().get(0), Some(&Value::Integer(2)));

        assert!(scanner.next());
        assert_eq!(scanner.row().get(0), Some(&Value::Integer(3)));

        assert!(!scanner.next());
        assert!(scanner.err().is_none());
    }

    #[test]
    fn test_vec_scanner_with_error() {
        let mut scanner = VecScanner::with_error(radixdb_core::Error::internal("test error"));
        assert!(!scanner.next());
        assert!(scanner.err().is_some());
    }

    #[test]
    fn test_vec_scanner_close() {
        let rows = vec![Row::from_values(vec![Value::Integer(1)])];
        let mut scanner = VecScanner::new(rows);

        assert!(scanner.next());
        assert!(scanner.close().is_ok());

        // After close, next should return false
        assert!(!scanner.next());
    }
}
