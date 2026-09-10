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

//! MVCC Scanner implementations
//!
//! Provides scanner implementations for MVCC query results.
//!

use crate::traits::Scanner;
use radixdb_core::CompactArc;
use radixdb_core::{Error, Result, Row, RowVec, Schema};

/// MVCC Scanner for iterating over versioned rows
pub struct MVCCScanner {
    /// Source rows with their IDs (cached - returns to pool on drop)
    rows: RowVec,
    /// Current index in the rows vector
    current_index: isize,
    /// Column indices to include in projection
    column_indices: Vec<usize>,
    /// Whether the scanner has been closed
    closed: bool,
}

impl MVCCScanner {
    #[inline]
    fn projection_is_full_identity(column_indices: &[usize], num_schema_cols: usize) -> bool {
        column_indices.len() == num_schema_cols
            && column_indices.iter().enumerate().all(|(i, &idx)| i == idx)
    }

    /// Project a row vector to the scan contract while preserving row IDs.
    ///
    /// Contract:
    /// - empty `column_indices` means all columns;
    /// - full identity projection keeps rows as-is;
    /// - every other projection returns rows with exactly the requested columns.
    #[doc(hidden)]
    pub fn project_rows_for_scan(
        rows: RowVec,
        num_schema_cols: usize,
        column_indices: &[usize],
    ) -> RowVec {
        let needs_projection = !column_indices.is_empty()
            && !Self::projection_is_full_identity(column_indices, num_schema_cols);

        if !needs_projection {
            return rows;
        }

        rows.into_iter()
            .map(|(id, row)| {
                let projected_values: Vec<radixdb_core::Value> = column_indices
                    .iter()
                    .map(|&idx| {
                        row.get(idx)
                            .cloned()
                            .unwrap_or_else(radixdb_core::Value::null_unknown)
                    })
                    .collect();
                (id, Row::from_values(projected_values))
            })
            .collect()
    }

    /// Project a row vector using exact projection semantics.
    ///
    /// Unlike `project_rows_for_scan()`, an empty projection remains empty
    /// instead of meaning "all columns".
    #[doc(hidden)]
    pub fn project_rows_for_scan_exact(
        rows: RowVec,
        num_schema_cols: usize,
        column_indices: &[usize],
    ) -> RowVec {
        let needs_projection = !Self::projection_is_full_identity(column_indices, num_schema_cols);

        if !needs_projection {
            return rows;
        }

        rows.into_iter()
            .map(|(id, row)| {
                let projected_values: Vec<radixdb_core::Value> = column_indices
                    .iter()
                    .map(|&idx| {
                        row.get(idx)
                            .cloned()
                            .unwrap_or_else(radixdb_core::Value::null_unknown)
                    })
                    .collect();
                (id, Row::from_values(projected_values))
            })
            .collect()
    }

    /// Creates scanner from RowVec
    #[inline]
    pub fn from_rows(rows: RowVec, schema: CompactArc<Schema>, column_indices: Vec<usize>) -> Self {
        let num_schema_cols = schema.columns.len();
        let projected_rows = Self::project_rows_for_scan(rows, num_schema_cols, &column_indices);
        let rows_are_projected = !column_indices.is_empty()
            && !Self::projection_is_full_identity(&column_indices, num_schema_cols);

        Self {
            rows: projected_rows,
            current_index: -1,
            column_indices: if rows_are_projected {
                vec![]
            } else {
                column_indices
            }, // Clear if already projected
            closed: false,
        }
    }

    /// Creates scanner from RowVec with exact projection semantics.
    ///
    /// Empty `column_indices` returns zero-width rows.
    pub fn from_rows_exact_projection(
        rows: RowVec,
        schema: CompactArc<Schema>,
        column_indices: Vec<usize>,
    ) -> Self {
        let num_schema_cols = schema.columns.len();
        let projected_rows =
            Self::project_rows_for_scan_exact(rows, num_schema_cols, &column_indices);
        let rows_are_projected =
            !Self::projection_is_full_identity(&column_indices, num_schema_cols);

        Self {
            rows: projected_rows,
            current_index: -1,
            column_indices: if rows_are_projected {
                vec![]
            } else {
                column_indices
            },
            closed: false,
        }
    }

    /// Creates an empty scanner
    #[inline]
    pub fn empty(_schema: CompactArc<Schema>, column_indices: Vec<usize>) -> Self {
        Self {
            rows: RowVec::new(),
            current_index: -1,
            column_indices,
            closed: false,
        }
    }

    /// Returns the number of rows in the scanner
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns true if the scanner has no rows
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl Scanner for MVCCScanner {
    fn next(&mut self) -> bool {
        if self.closed {
            return false;
        }

        self.current_index += 1;

        (self.current_index as usize) < self.rows.len()
    }

    fn row(&self) -> &Row {
        if self.current_index < 0 || (self.current_index as usize) >= self.rows.len() {
            // Return a static empty row for safety
            static EMPTY_ROW: std::sync::OnceLock<Row> = std::sync::OnceLock::new();
            return EMPTY_ROW.get_or_init(|| Row::from_values(vec![]));
        }

        let (_, ref source_row) = self.rows[self.current_index as usize];

        // If no column projection, return the row directly
        if self.column_indices.is_empty() {
            return source_row;
        }

        // Production row-vector constructors precompute projection. Compatibility
        // constructors retain their existing source-row behavior here.
        if self.column_indices.len() == source_row.len() {
            let all_match = self
                .column_indices
                .iter()
                .enumerate()
                .all(|(i, &idx)| i == idx);
            if all_match {
                return source_row;
            }
        }

        // Otherwise return source row - projection handled in caller
        source_row
    }

    fn err(&self) -> Option<&radixdb_core::Error> {
        None
    }

    fn close(&mut self) -> Result<()> {
        self.closed = true;
        self.rows.clear();
        Ok(())
    }

    fn take_row(&mut self) -> Row {
        if self.current_index < 0 || (self.current_index as usize) >= self.rows.len() {
            return Row::new();
        }

        // Swap out the row with an empty one to avoid cloning
        let idx = self.current_index as usize;
        std::mem::take(&mut self.rows[idx].1)
    }

    fn take_row_with_id(&mut self) -> Result<(i64, Row)> {
        if self.current_index < 0 || (self.current_index as usize) >= self.rows.len() {
            return Err(Error::internal(
                "row identity requested without a current MVCC row",
            ));
        }

        let idx = self.current_index as usize;
        let row_id = self.rows[idx].0;
        let row = std::mem::take(&mut self.rows[idx].1);
        Ok((row_id, row))
    }

    fn current_row_id(&self) -> Result<i64> {
        if self.current_index < 0 || (self.current_index as usize) >= self.rows.len() {
            return Err(Error::internal(
                "row identity requested without a current MVCC row",
            ));
        }

        Ok(self.rows[self.current_index as usize].0)
    }

    fn estimated_count(&self) -> Option<usize> {
        Some(self.rows.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_core::{DataType, SchemaBuilder, Value};

    fn test_schema() -> CompactArc<Schema> {
        CompactArc::new(
            SchemaBuilder::new("test")
                .column("id", DataType::Integer, false, false)
                .build(),
        )
    }

    fn wide_test_schema() -> CompactArc<Schema> {
        CompactArc::new(
            SchemaBuilder::new("wide_test")
                .column("id", DataType::Integer, false, false)
                .column("name", DataType::Text, false, false)
                .column("price", DataType::Float, false, false)
                .build(),
        )
    }

    #[test]
    fn test_mvcc_scanner_empty() {
        let schema = test_schema();

        let mut scanner = MVCCScanner::empty(schema, vec![0]);

        assert!(!scanner.next());
        assert!(scanner.is_empty());
    }

    #[test]
    fn test_mvcc_scanner_multiple_rows() {
        let schema = test_schema();

        let mut rows = RowVec::new();
        rows.push((1, Row::from_values(vec![Value::Integer(1)])));
        rows.push((2, Row::from_values(vec![Value::Integer(2)])));
        rows.push((3, Row::from_values(vec![Value::Integer(3)])));

        let mut scanner = MVCCScanner::from_rows(rows, schema, vec![0]);

        assert_eq!(scanner.len(), 3);

        // Check all rows
        assert!(scanner.next());
        assert_eq!(scanner.row().get(0), Some(&Value::Integer(1)));

        assert!(scanner.next());
        assert_eq!(scanner.row().get(0), Some(&Value::Integer(2)));

        assert!(scanner.next());
        assert_eq!(scanner.row().get(0), Some(&Value::Integer(3)));

        assert!(!scanner.next());
    }

    #[test]
    fn test_mvcc_scanner_projects_prefix_subset() {
        let schema = wide_test_schema();

        let mut rows = RowVec::new();
        rows.push((
            1,
            Row::from_values(vec![
                Value::Integer(1),
                Value::text("apple"),
                Value::Float(1.5),
            ]),
        ));

        let mut scanner = MVCCScanner::from_rows(rows, schema, vec![0, 1]);

        assert!(scanner.next());
        assert_eq!(scanner.row().len(), 2);
        assert_eq!(scanner.row().get(0), Some(&Value::Integer(1)));
        assert_eq!(scanner.row().get(1), Some(&Value::text("apple")));
        assert!(scanner.row().get(2).is_none());
    }

    #[test]
    fn test_mvcc_scanner_projects_reordered_subset() {
        let schema = wide_test_schema();

        let mut rows = RowVec::new();
        rows.push((
            1,
            Row::from_values(vec![
                Value::Integer(1),
                Value::text("apple"),
                Value::Float(1.5),
            ]),
        ));

        let mut scanner = MVCCScanner::from_rows(rows, schema, vec![2, 0]);

        assert!(scanner.next());
        assert_eq!(scanner.row().len(), 2);
        assert_eq!(scanner.row().get(0), Some(&Value::Float(1.5)));
        assert_eq!(scanner.row().get(1), Some(&Value::Integer(1)));
    }

    #[test]
    fn test_mvcc_scanner_preserves_duplicate_projection_indices() {
        let schema = wide_test_schema();

        let mut rows = RowVec::new();
        rows.push((
            1,
            Row::from_values(vec![
                Value::Integer(1),
                Value::text("apple"),
                Value::Float(1.5),
            ]),
        ));

        let mut scanner = MVCCScanner::from_rows(rows, schema, vec![1, 1, 0]);

        assert!(scanner.next());
        assert_eq!(scanner.row().len(), 3);
        assert_eq!(scanner.row().get(0), Some(&Value::text("apple")));
        assert_eq!(scanner.row().get(1), Some(&Value::text("apple")));
        assert_eq!(scanner.row().get(2), Some(&Value::Integer(1)));
    }

    #[test]
    fn test_mvcc_scanner_exposes_current_row_id() {
        let schema = test_schema();

        let mut rows = RowVec::new();
        rows.push((41, Row::from_values(vec![Value::Integer(1)])));
        rows.push((42, Row::from_values(vec![Value::Integer(2)])));

        let mut scanner = MVCCScanner::from_rows(rows, schema, vec![0]);

        assert!(scanner.current_row_id().is_err());
        assert!(scanner.next());
        assert_eq!(scanner.current_row_id().unwrap(), 41);
        assert!(scanner.next());
        assert_eq!(scanner.current_row_id().unwrap(), 42);
        assert!(!scanner.next());
        assert!(scanner.current_row_id().is_err());
    }

    #[test]
    fn test_mvcc_scanner_take_row_with_id_preserves_identity_and_projection() {
        let schema = wide_test_schema();

        let mut rows = RowVec::new();
        rows.push((
            77,
            Row::from_values(vec![
                Value::Integer(7),
                Value::text("seven"),
                Value::Float(7.7),
            ]),
        ));

        let mut scanner = MVCCScanner::from_rows(rows, schema, vec![0, 1]);

        assert!(scanner.next());
        let (row_id, row) = scanner.take_row_with_id().unwrap();
        assert_eq!(row_id, 77);
        assert_eq!(row.len(), 2);
        assert_eq!(row.get(0), Some(&Value::Integer(7)));
        assert_eq!(row.get(1), Some(&Value::text("seven")));
    }

    #[test]
    fn test_mvcc_scanner_close() {
        let schema = test_schema();

        let mut rows = RowVec::new();
        rows.push((1, Row::from_values(vec![Value::Integer(1)])));

        let mut scanner = MVCCScanner::from_rows(rows, schema, vec![0]);

        assert!(scanner.next());
        assert!(scanner.close().is_ok());

        // After close, next should return false
        assert!(!scanner.next());
    }
}
