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

//! Rows iterator for query results - Rust idiomatic API
//!
//! # Example
//!
//! ```no_run
//! use radixdb_api::{Database, FromRow, ResultRow};
//! use radixdb_core::Result;
//! # fn main() -> Result<()> {
//! # let db = Database::open_in_memory()?;
//! # db.execute("CREATE TABLE users (id INTEGER, name TEXT)", ())?;
//! // Idiomatic Rust iteration
//! for row in db.query("SELECT * FROM users", ())? {
//!     let row = row?;
//!     let id: i64 = row.get(0)?;
//!     let name: String = row.get(1)?;
//!     println!("{}: {}", id, name);
//! }
//!
//! // With combinators
//! let names: Vec<String> = db.query("SELECT name FROM users", ())?
//!     .map(|r| r.and_then(|row| row.get(0)))
//!     .collect::<Result<_>>()?;
//!
//! // With FromRow for struct mapping
//! struct User { id: i64, name: String }
//!
//! impl FromRow for User {
//!     fn from_row(row: &ResultRow) -> Result<Self> {
//!         Ok(User {
//!             id: row.get(0)?,
//!             name: row.get(1)?,
//!         })
//!     }
//! }
//!
//! let users: Vec<User> = db.query_as("SELECT id, name FROM users", ())?;
//! # assert!(users.is_empty());
//! # Ok(())
//! # }
//! ```

use radixdb_core::CompactArc;
use radixdb_core::{Error, Result, Row, Value};
use radixdb_executor::result::ExecutionResult;
use rustc_hash::FxHashMap;

use super::database::FromValue;
use super::result_adapter::ApiResultCursor;

/// Trait for converting a database row into a Rust struct
///
/// Implement this trait for your structs to enable automatic mapping
/// from query results using `query_as`.
///
/// # Example
///
/// ```ignore
/// use radixdb::{Database, FromRow, ResultRow, Result};
///
/// struct User {
///     id: i64,
///     name: String,
///     email: Option<String>,
/// }
///
/// impl FromRow for User {
///     fn from_row(row: &ResultRow) -> Result<Self> {
///         Ok(User {
///             id: row.get(0)?,
///             name: row.get(1)?,
///             email: row.get(2)?,  // Option<T> handles NULL
///         })
///     }
/// }
///
/// // Now you can use query_as
/// let db = Database::open("memory://")?;
/// let users: Vec<User> = db.query_as("SELECT id, name, email FROM users", ())?;
/// ```
///
/// # Using column names
///
/// You can also use column names for more robust mapping:
///
/// ```ignore
/// impl FromRow for User {
///     fn from_row(row: &ResultRow) -> Result<Self> {
///         Ok(User {
///             id: row.get_by_name("id")?,
///             name: row.get_by_name("name")?,
///             email: row.get_by_name("email")?,
///         })
///     }
/// }
/// ```
pub trait FromRow: Sized {
    /// Convert a result row into Self
    fn from_row(row: &ResultRow) -> Result<Self>;
}

/// A single row from a query result with typed accessors
#[derive(Debug, Clone)]
pub struct ResultRow {
    row: Row,
    /// Shared column names (Arc avoids per-row allocation)
    columns: CompactArc<Vec<String>>,
    /// Shared case-folded name map built once per result cursor.
    column_lookup: CompactArc<FxHashMap<String, Vec<usize>>>,
}

impl ResultRow {
    /// Create a new ResultRow
    pub(crate) fn new(
        row: Row,
        columns: CompactArc<Vec<String>>,
        column_lookup: CompactArc<FxHashMap<String, Vec<usize>>>,
    ) -> Self {
        Self {
            row,
            columns,
            column_lookup,
        }
    }

    /// Get a column value by index with type conversion
    ///
    /// # Example
    ///
    /// ```ignore
    /// let id: i64 = row.get(0)?;
    /// let name: String = row.get(1)?;
    /// let score: Option<f64> = row.get(2)?;
    /// ```
    pub fn get<T: FromValue>(&self, index: usize) -> Result<T> {
        let value = self
            .row
            .get(index)
            .ok_or(Error::ColumnIndexOutOfBounds { index })?;
        T::from_value(value)
    }

    /// Get a column value by name with type conversion
    ///
    /// # Example
    ///
    /// ```ignore
    /// let name: String = row.get_by_name("name")?;
    /// let age: i64 = row.get_by_name("age")?;
    /// ```
    pub fn get_by_name<T: FromValue>(&self, name: &str) -> Result<T> {
        let index = self.column_index(name)?;
        self.get(index)
    }

    /// Get the raw Value at an index
    pub fn get_value(&self, index: usize) -> Option<&Value> {
        self.row.get(index)
    }

    /// Get the underlying Row
    pub fn into_inner(self) -> Row {
        self.row
    }

    /// Get a reference to the underlying Row
    pub fn as_row(&self) -> &Row {
        &self.row
    }

    /// Get the column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Get the number of columns
    pub fn len(&self) -> usize {
        self.row.len()
    }

    /// Check if row is empty
    pub fn is_empty(&self) -> bool {
        self.row.len() == 0
    }

    /// Check if a column value is NULL
    pub fn is_null(&self, index: usize) -> Result<bool> {
        self.row
            .get(index)
            .map(Value::is_null)
            .ok_or(Error::ColumnIndexOutOfBounds { index })
    }

    /// Get the index of a column by name (case-insensitive)
    fn column_index(&self, name: &str) -> Result<usize> {
        let name_lower = name.to_lowercase();
        let matches = self
            .column_lookup
            .get(&name_lower)
            .ok_or_else(|| Error::ColumnNotFound(name.to_string()))?;
        if matches.len() > 1 {
            Err(Error::AmbiguousColumn(name.to_string()))
        } else {
            Ok(matches[0])
        }
    }
}

/// Iterator over query result rows
///
/// Implements `Iterator<Item = Result<ResultRow>>` for idiomatic Rust usage.
///
/// # Example
///
/// ```ignore
/// // Standard for loop
/// for row in db.query("SELECT * FROM users")? {
///     let row = row?;
///     println!("{:?}", row.get::<String>(0)?);
/// }
///
/// // Collect into Vec
/// let rows: Vec<ResultRow> = db.query("SELECT * FROM users")?
///     .collect::<Result<Vec<_>, _>>()?;
///
/// // Filter and map
/// let adults: Vec<String> = db.query("SELECT name, age FROM users")?
///     .filter_map(|r| {
///         let row = r.ok()?;
///         let age: i64 = row.get(1).ok()?;
///         if age >= 18 {
///             row.get::<String>(0).ok()
///         } else {
///             None
///         }
///     })
///     .collect();
/// ```
pub struct Rows {
    result: ApiResultCursor,
    /// Shared column names (Arc to avoid cloning per row)
    columns: CompactArc<Vec<String>>,
    column_lookup: CompactArc<FxHashMap<String, Vec<usize>>>,
    closed: bool,
    positioned: bool,
    /// Pending error from a filter runtime failure (e.g., invalid REGEXP)
    pending_error: Option<radixdb_core::Error>,
    close_error: Option<radixdb_core::Error>,
    iterator_close_error_yielded: bool,
}

impl Rows {
    /// Adapt an internal execution result into the public row cursor.
    pub(crate) fn new(result: ExecutionResult) -> Self {
        let result = ApiResultCursor::new(result);
        // Use columns_arc() if available (zero-copy), otherwise clone
        let columns = result
            .columns_arc()
            .unwrap_or_else(|| CompactArc::new(result.columns().to_vec()));
        let mut column_lookup: FxHashMap<String, Vec<usize>> = FxHashMap::default();
        for (index, column) in columns.iter().enumerate() {
            column_lookup
                .entry(column.to_lowercase())
                .or_default()
                .push(index);
        }
        Self {
            result,
            columns,
            column_lookup: CompactArc::new(column_lookup),
            closed: false,
            positioned: false,
            pending_error: None,
            close_error: None,
            iterator_close_error_yielded: false,
        }
    }

    /// Get the column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get the number of rows affected (for DML statements)
    pub fn rows_affected(&self) -> i64 {
        self.result.rows_affected()
    }

    /// Get the last generated id from an AUTO_INCREMENT insert.
    pub fn last_insert_id(&self) -> i64 {
        self.result.last_insert_id()
    }

    /// Whether this result can be fetched as decoded typed column batches.
    ///
    /// The method is intended for the server transport boundary. Ordinary API
    /// callers keep using `advance()` / `Iterator` and therefore retain the
    /// historic row-oriented contract.
    #[inline]
    pub fn supports_server_column_batches(&self) -> bool {
        !self.closed && self.result.supports_typed_batches()
    }

    /// If typed batches are unavailable, return a stable diagnostics reason.
    #[inline]
    pub fn server_column_batch_fallback(&self) -> Option<super::ServerBatchFallback> {
        if self.supports_server_column_batches() {
            None
        } else if self.closed {
            Some(super::ServerBatchFallback::RowState)
        } else {
            self.result
                .typed_batch_fallback_reason()
                .map(super::ServerBatchFallback::from_storage)
        }
    }

    /// Return the next decoded typed batch, closing the result at EOF.
    pub fn next_server_column_batch(&mut self) -> Result<Option<super::ServerColumnBatch>> {
        if self.closed {
            return self.close_error.clone().map_or(Ok(None), Err);
        }
        match self.result.next_typed_batch() {
            Ok(Some(batch)) => super::ServerColumnBatch::from_storage(batch).map(Some),
            Ok(None) => {
                self.close_internal()?;
                Ok(None)
            }
            Err(error) => {
                let _ = self.close_internal();
                Err(error)
            }
        }
    }

    /// Advance the cursor to the next row.
    ///
    /// Returns `true` if a row is available, `false` when exhausted.
    /// Use `current_row()` to access the row by reference (no clone).
    ///
    /// This is faster than the Iterator interface for bulk serialization
    /// because it avoids `take_row()` which clones the row.
    #[inline]
    pub fn advance(&mut self) -> bool {
        if self.closed {
            return false;
        }
        if self.result.next() {
            self.positioned = true;
            return true;
        }
        self.positioned = false;
        // Check for runtime filter errors (e.g., invalid REGEXP)
        if let Some(err) = self.result.last_error() {
            self.pending_error = Some(err);
        }
        // Clean up resources (scanner close, etc.) now that iteration is done
        let _ = self.close_internal();
        false
    }

    /// Return any runtime error that caused `advance()` to return false.
    ///
    /// After `advance()` returns `false`, call this to distinguish between
    /// normal end-of-stream (returns `None`) and a runtime filter error
    /// like an invalid parameterized REGEXP (returns `Some(error)`).
    #[inline]
    pub fn error(&mut self) -> Option<radixdb_core::Error> {
        if let Some(error) = self.pending_error.take() {
            return Some(error);
        }
        let error = self.close_error.clone();
        if error.is_some() {
            self.iterator_close_error_yielded = true;
        }
        error
    }

    /// Get a reference to the current row (after a successful `advance()`).
    ///
    /// Returns `&Row` directly — no clone, no ResultRow wrapper.
    #[inline]
    pub fn current_row(&self) -> Result<&Row> {
        if self.closed || !self.positioned {
            Err(Error::CursorNotPositioned)
        } else {
            Ok(self.result.row())
        }
    }

    /// Collect all rows into a Vec
    ///
    /// # Example
    ///
    /// ```ignore
    /// let rows = db.query("SELECT * FROM users")?.collect_vec()?;
    /// for row in rows {
    ///     println!("{:?}", row);
    /// }
    /// ```
    pub fn collect_vec(self) -> Result<Vec<ResultRow>> {
        self.collect()
    }

    /// Close the result set explicitly
    ///
    /// This is called automatically when the Rows is dropped.
    pub fn close(&mut self) -> Result<()> {
        let result = self.close_internal();
        if result.is_err() {
            self.iterator_close_error_yielded = true;
        }
        result
    }

    fn close_internal(&mut self) -> Result<()> {
        if !self.closed {
            let result = self.result.close();
            self.closed = true;
            self.positioned = false;
            if let Err(error) = result {
                self.close_error = Some(error);
            }
        }
        self.close_error.clone().map_or(Ok(()), Err)
    }
}

impl Iterator for Rows {
    type Item = Result<ResultRow>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.closed {
            if !self.iterator_close_error_yielded {
                if let Some(error) = self.close_error.clone() {
                    self.iterator_close_error_yielded = true;
                    return Some(Err(error));
                }
            }
            return None;
        }

        self.positioned = false;

        if self.result.next() {
            // Use take_row() to avoid cloning - moves the row out of the result
            let row = self.result.take_row();
            // Arc clone is O(1) - just increments reference count
            Some(Ok(ResultRow::new(
                row,
                CompactArc::clone(&self.columns),
                CompactArc::clone(&self.column_lookup),
            )))
        } else if let Some(err) = self.result.last_error() {
            // Surface runtime errors (e.g. invalid REGEXP pattern).
            // close() forwards to result.close() for proper scanner cleanup
            let _ = self.close_internal();
            Some(Err(err))
        } else {
            match self.close_internal() {
                Ok(()) => None,
                Err(error) => {
                    self.iterator_close_error_yielded = true;
                    Some(Err(error))
                }
            }
        }
    }
}

impl Drop for Rows {
    fn drop(&mut self) {
        let _ = self.close_internal();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result_adapter::close_failing_result;
    use radixdb_storage::traits::MemoryResult;

    fn create_test_rows() -> Rows {
        let columns = vec!["id".to_string(), "name".to_string(), "value".to_string()];
        let rows = vec![
            Row::from_values(vec![
                Value::Integer(1),
                Value::text("Alice"),
                Value::Float(10.5),
            ]),
            Row::from_values(vec![
                Value::Integer(2),
                Value::text("Bob"),
                Value::Float(20.0),
            ]),
        ];

        let result = MemoryResult::with_rows(columns, rows);
        Rows::new(Box::new(result))
    }

    #[test]
    fn test_iterator_for_loop() {
        let rows = create_test_rows();
        let mut count = 0;

        for row in rows {
            let row = row.unwrap();
            assert!(row.get::<i64>(0).is_ok());
            count += 1;
        }

        assert_eq!(count, 2);
    }

    #[test]
    fn r6_l01_b_rows_close_surfaces_cleanup_failure() {
        let mut rows = Rows::new(close_failing_result());
        let error = rows.close().expect_err("close failure must be observable");
        assert!(error.to_string().contains("injected close failure"));
        assert!(rows.close().is_err(), "close failure remains observable");
    }

    #[test]
    fn r8_iterator_surfaces_cleanup_failure_at_eof_once() {
        let mut rows = Rows::new(close_failing_result());
        let error = rows
            .next()
            .expect("terminal cleanup error")
            .expect_err("cleanup must fail");
        assert!(error.to_string().contains("injected close failure"));
        assert!(rows.next().is_none());
    }

    #[test]
    fn r8_advance_exposes_terminal_cleanup_failure() {
        let mut rows = Rows::new(close_failing_result());
        assert!(!rows.advance());
        let error = rows.error().expect("cleanup error must be recoverable");
        assert!(error.to_string().contains("injected close failure"));
    }

    #[test]
    fn test_iterator_collect() {
        let rows = create_test_rows();
        let collected: Vec<ResultRow> = rows.collect::<std::result::Result<Vec<_>, _>>().unwrap();

        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].get::<i64>(0).unwrap(), 1);
        assert_eq!(collected[1].get::<i64>(0).unwrap(), 2);
    }

    #[test]
    fn test_iterator_map() {
        let rows = create_test_rows();
        let names: Vec<String> = rows
            .map(|r| r.and_then(|row| row.get(1)))
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(names, vec!["Alice", "Bob"]);
    }

    #[test]
    fn test_iterator_filter() {
        let rows = create_test_rows();
        let filtered: Vec<ResultRow> = rows
            .filter_map(|r| {
                let row = r.ok()?;
                let id: i64 = row.get(0).ok()?;
                if id > 1 {
                    Some(row)
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].get::<String>(1).unwrap(), "Bob");
    }

    #[test]
    fn test_result_row_get() {
        let rows = create_test_rows();
        let row = rows.into_iter().next().unwrap().unwrap();

        assert_eq!(row.get::<i64>(0).unwrap(), 1);
        assert_eq!(row.get::<String>(1).unwrap(), "Alice");
        assert_eq!(row.get::<f64>(2).unwrap(), 10.5);
    }

    #[test]
    fn test_result_row_get_by_name() {
        let rows = create_test_rows();
        let row = rows.into_iter().next().unwrap().unwrap();

        assert_eq!(row.get_by_name::<i64>("id").unwrap(), 1);
        assert_eq!(row.get_by_name::<String>("name").unwrap(), "Alice");
        assert_eq!(row.get_by_name::<f64>("value").unwrap(), 10.5);

        // Case insensitive
        assert_eq!(row.get_by_name::<i64>("ID").unwrap(), 1);
        assert_eq!(row.get_by_name::<String>("NAME").unwrap(), "Alice");
    }

    #[test]
    fn r8_l01_batch_j_result_rows_share_case_folded_column_lookup() {
        let result = MemoryResult::with_rows(
            vec!["Name".to_string(), "NAME".to_string(), "id".to_string()],
            vec![
                Row::from_values(vec![Value::text("a"), Value::text("b"), Value::Integer(1)]),
                Row::from_values(vec![Value::text("c"), Value::text("d"), Value::Integer(2)]),
            ],
        );
        let mut rows = Rows::new(Box::new(result));
        let first = rows.next().unwrap().unwrap();
        let second = rows.next().unwrap().unwrap();

        assert_eq!(first.get_by_name::<i64>("ID").unwrap(), 1);
        assert_eq!(second.get_by_name::<i64>("id").unwrap(), 2);
        assert!(matches!(
            first.get_by_name::<String>("name"),
            Err(Error::AmbiguousColumn(column)) if column == "name"
        ));
    }

    #[test]
    fn test_result_row_columns() {
        let rows = create_test_rows();
        let row = rows.into_iter().next().unwrap().unwrap();

        assert_eq!(row.columns(), &["id", "name", "value"]);
        assert_eq!(row.len(), 3);
        assert!(!row.is_empty());
    }

    #[test]
    fn test_rows_columns() {
        let rows = create_test_rows();
        assert_eq!(rows.columns(), &["id", "name", "value"]);
        assert_eq!(rows.column_count(), 3);
    }

    #[test]
    fn test_collect_vec() {
        let rows = create_test_rows();
        let collected = rows.collect_vec().unwrap();

        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn test_out_of_bounds() {
        let rows = create_test_rows();
        let row = rows.into_iter().next().unwrap().unwrap();

        assert!(row.get::<i64>(10).is_err());
    }

    #[test]
    fn test_column_not_found() {
        let rows = create_test_rows();
        let row = rows.into_iter().next().unwrap().unwrap();

        assert!(row.get_by_name::<String>("nonexistent").is_err());
    }

    #[test]
    fn test_advance_and_current_row() {
        let mut rows = create_test_rows();

        // First row
        assert!(rows.advance());
        let row = rows.current_row().unwrap();
        assert_eq!(row.get(0), Some(&Value::Integer(1)));
        assert_eq!(row.get(1), Some(&Value::text("Alice")));

        // Second row
        assert!(rows.advance());
        let row = rows.current_row().unwrap();
        assert_eq!(row.get(0), Some(&Value::Integer(2)));
        assert_eq!(row.get(1), Some(&Value::text("Bob")));

        // Exhausted
        assert!(!rows.advance());
    }

    #[test]
    fn test_advance_on_closed_rows() {
        let mut rows = create_test_rows();
        rows.close().unwrap();
        assert!(!rows.advance());
    }

    #[test]
    fn test_advance_full_scan() {
        let mut rows = create_test_rows();
        let mut count = 0;
        while rows.advance() {
            let _row = rows.current_row();
            count += 1;
        }
        assert_eq!(count, 2);
    }
}
