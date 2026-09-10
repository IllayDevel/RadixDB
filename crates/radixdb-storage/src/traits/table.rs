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

//! Table trait for database tables
//!

use rustc_hash::FxHashMap;
use std::fmt;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::expression::Expression;
use crate::traits::index::IndexKeyRange;
use crate::traits::{Index, QueryResult, Scanner};
use radixdb_core::{
    CompactArc, DataType, Error, IndexType, Operator, Result, Row, RowVec, Schema, Value,
};

/// Operation requested from a storage-level aggregate pushdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateOp {
    Count,
    CountStar,
    Sum,
    Min,
    Max,
    Avg,
}

/// Stable group identity shared by hot and cold aggregate implementations.
#[derive(Clone, Debug)]
pub enum GroupKey {
    Single(CompactArc<Value>),
    Multi(Vec<CompactArc<Value>>),
}

impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Single(left), Self::Single(right)) => **left == **right,
            (Self::Multi(left), Self::Multi(right)) => {
                left.len() == right.len() && left.iter().zip(right.iter()).all(|(a, b)| **a == **b)
            }
            _ => false,
        }
    }
}

impl Eq for GroupKey {}

impl std::hash::Hash for GroupKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Single(value) => (**value).hash(state),
            Self::Multi(values) => {
                for value in values {
                    (**value).hash(state);
                }
            }
        }
    }
}

/// Result of one storage-level grouped aggregation.
#[derive(Debug, Clone)]
pub struct GroupedAggregateResult {
    pub group_values: Vec<Value>,
    pub aggregate_values: Vec<Value>,
}

/// Resolve the physical index key for one logical row.
///
/// Keeping this in the neutral table contract prevents partial-index
/// evaluation from diverging between hot MVCC and immutable segments.
#[doc(hidden)]
pub fn index_values_for_row(index: &dyn Index, row: &Row) -> Result<Option<Vec<Value>>> {
    let column_ids = index.column_ids();
    if column_ids.is_empty() {
        return Ok(None);
    }
    if let Some(predicate) = index.partial_predicate() {
        if !predicate.matches(row)? {
            return Ok(None);
        }
    }
    Ok(Some(
        column_ids
            .iter()
            .map(|&column_id| {
                row.get(column_id as usize)
                    .cloned()
                    .unwrap_or(Value::Null(DataType::Null))
            })
            .collect(),
    ))
}

/// Exact intermediate state for deferred SUM/AVG pushdown.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DeferredSum {
    integer_sum: i128,
    float_sum: f64,
    integer_count: usize,
    float_count: usize,
    overflowed: bool,
}

impl DeferredSum {
    pub const fn new() -> Self {
        Self {
            integer_sum: 0,
            float_sum: 0.0,
            integer_count: 0,
            float_count: 0,
            overflowed: false,
        }
    }

    pub fn add_integer(&mut self, value: i128, count: usize) {
        match (
            self.integer_sum.checked_add(value),
            self.integer_count.checked_add(count),
        ) {
            (Some(sum), Some(total)) => {
                self.integer_sum = sum;
                self.integer_count = total;
            }
            _ => self.overflowed = true,
        }
    }

    pub fn add_float(&mut self, value: f64, count: usize) {
        self.float_sum += value;
        self.float_count = match self.float_count.checked_add(count) {
            Some(total) => total,
            None => {
                self.overflowed = true;
                self.float_count
            }
        };
    }

    pub fn add_value(&mut self, value: &Value) {
        match value {
            Value::Integer(value) => self.add_integer(*value as i128, 1),
            Value::Float(value) => self.add_float(*value, 1),
            _ => {}
        }
    }

    pub fn merge(&mut self, other: Self) {
        self.overflowed |= other.overflowed;
        self.add_integer(other.integer_sum, other.integer_count);
        self.add_float(other.float_sum, other.float_count);
    }

    pub fn count(&self) -> usize {
        self.integer_count.saturating_add(self.float_count)
    }

    pub fn as_f64(&self) -> f64 {
        self.integer_sum as f64 + self.float_sum
    }

    pub fn into_value(self) -> Result<Value> {
        if self.overflowed {
            return Err(Error::invalid_argument("deferred SUM accumulator overflow"));
        }
        if self.float_count == 0 {
            if let Ok(value) = i64::try_from(self.integer_sum) {
                return Ok(Value::Integer(value));
            }
            let digits = self.integer_sum.to_string().trim_start_matches('-').len();
            let precision = u8::try_from(digits)
                .ok()
                .filter(|precision| *precision <= 38)
                .ok_or_else(|| Error::invalid_argument("deferred SUM exceeds DECIMAL(38)"))?;
            return Value::try_decimal(self.integer_sum, precision, 0);
        }
        Ok(Value::Float(self.as_f64()))
    }
}

impl Default for DeferredSum {
    fn default() -> Self {
        Self::new()
    }
}

/// Describes the access method that will be used for a table scan
///
/// This is used by EXPLAIN to show users how their queries will be executed.
#[derive(Debug, Clone)]
pub enum ScanPlan {
    /// Sequential scan - reads all rows and applies filter in memory
    SeqScan {
        table: String,
        filter: Option<String>,
    },
    /// Parallel sequential scan - reads all rows and filters in parallel across workers
    ParallelSeqScan {
        table: String,
        filter: Option<String>,
        workers: usize,
    },
    /// Primary key lookup - O(1) direct access by primary key
    PkLookup {
        table: String,
        pk_column: String,
        pk_value: String,
    },
    /// Index scan - uses an index to find matching rows
    IndexScan {
        table: String,
        index_name: String,
        column: String,
        condition: String,
        /// Non-indexed predicates applied as in-memory filter after index lookup
        filter: Option<String>,
    },
    /// Multi-index scan - uses multiple indexes with AND/OR operations
    MultiIndexScan {
        table: String,
        indexes: Vec<(String, String, String)>, // (index_name, column, condition)
        operation: String,                      // "AND" or "OR"
        /// Non-indexed predicates applied as in-memory filter after index lookup
        filter: Option<String>,
    },
    /// Union/intersection over persisted artifact-backed postings plus hot MVCC indexes.
    SegmentedMultiIndexScan {
        table: String,
        indexes: Vec<(String, String, String)>,
        operation: String,
        filter: Option<String>,
        cold_segments: usize,
        cold_rows_hint: usize,
        hot_rows_hint: usize,
    },
    /// Composite index scan - uses a multi-column index
    CompositeIndexScan {
        table: String,
        index_name: String,
        columns: Vec<String>,
        conditions: Vec<String>,
        /// Non-indexed predicates applied as in-memory filter after index lookup
        filter: Option<String>,
    },
    /// Exact composite equality lookup over persisted artifact-backed postings plus the hot
    /// MVCC index. This is separate from `CompositeIndexScan` so EXPLAIN does
    /// not describe a cold source as a hot-only index.
    SegmentedCompositeIndexScan {
        table: String,
        index_name: String,
        columns: Vec<String>,
        conditions: Vec<String>,
        filter: Option<String>,
        cold_segments: usize,
        cold_rows_hint: usize,
        hot_rows_hint: usize,
        ordered: bool,
        /// True when the physical declaration has multiple columns, even if
        /// this lookup uses only one persisted leading prefix column.
        composite: bool,
    },
    /// HNSW approximate nearest neighbor search
    VectorSearch {
        table: String,
        index_name: String,
        vector_column: String,
        metric: String,
        k: usize,
        ef_search: usize,
        filter: Option<String>,
    },
    /// Brute-force parallel vector distance scan
    VectorBruteForce {
        table: String,
        vector_column: String,
        metric: String,
        k: usize,
        filter: Option<String>,
    },
    /// Segment-backed table scan - merges immutable cold segments with hot rows.
    ///
    /// This is deliberately separate from SeqScan/IndexScan: cold rows do not
    /// participate in the hot MVCC secondary indexes. Cold data is read from
    /// descriptor-backed artifact-backed row-group blocks.
    SegmentedScan {
        table: String,
        filter: Option<String>,
        cold_segments: usize,
        cold_rows_hint: usize,
        cold_row_groups_hint: usize,
        cold_selected_segments: usize,
        cold_selected_rows_hint: usize,
        cold_selected_row_groups_hint: usize,
        cold_metadata_pruned_segments: usize,
        cold_metadata_pruned_rows_hint: usize,
        hot_rows_hint: usize,
    },
}

impl ScanPlan {
    /// Stable, machine-readable access path identifier for EXPLAIN/debug output.
    ///
    /// Human-readable `Display` output is intentionally kept stable for users and
    /// existing tests. This identifier is the contract benchmark reports and
    /// planner assertions should use.
    pub fn access_path_id(&self) -> &'static str {
        match self {
            ScanPlan::SeqScan { .. } => "scan.seq",
            ScanPlan::ParallelSeqScan { .. } => "scan.parallel_seq",
            ScanPlan::PkLookup { .. } => "scan.pk",
            ScanPlan::IndexScan { .. } => "scan.index",
            ScanPlan::MultiIndexScan { .. } | ScanPlan::SegmentedMultiIndexScan { .. } => {
                "scan.multi_index"
            }
            ScanPlan::CompositeIndexScan { .. } => "scan.composite_index",
            ScanPlan::SegmentedCompositeIndexScan { composite, .. } if !composite => "scan.index",
            ScanPlan::SegmentedCompositeIndexScan { .. } => "scan.composite_index",
            ScanPlan::VectorSearch { .. } => "scan.vector_hnsw",
            ScanPlan::VectorBruteForce { .. } => "scan.vector_bruteforce",
            ScanPlan::SegmentedScan { hot_rows_hint, .. } => {
                if *hot_rows_hint == 0 {
                    "scan.cold_artifact"
                } else {
                    "scan.mixed_cold_artifact_hot"
                }
            }
        }
    }

    /// Stable EXPLAIN lines emitted below the human-readable scan node.
    pub fn stable_explain_lines(&self) -> Vec<String> {
        let mut lines = vec![format!("Access Path: {}", self.access_path_id())];
        // Stage 8 will introduce real RAM accelerator lifecycle. Until then,
        // make the absence explicit so EXPLAIN/debug output never implies an
        // invisible accelerator path.
        lines.push("RAM Accelerator: none".to_string());

        match self {
            ScanPlan::SeqScan { filter, .. } => {
                lines.push("Access Source: hot_mvcc".to_string());
                lines.push("Hot Access Path: version_store.seq_scan".to_string());
                if filter.is_some() {
                    lines.push("Access Filter: executor_or_storage_residual".to_string());
                    lines.push("Partial Index Eligibility: no_proven_partial_index".to_string());
                }
            }
            ScanPlan::ParallelSeqScan { filter, .. } => {
                lines.push("Access Source: hot_mvcc".to_string());
                lines.push("Hot Access Path: version_store.parallel_seq_scan".to_string());
                if filter.is_some() {
                    lines.push("Access Filter: executor_or_storage_residual".to_string());
                    lines.push("Partial Index Eligibility: no_proven_partial_index".to_string());
                }
            }
            ScanPlan::PkLookup { pk_column, .. } => {
                lines.push("Access Source: hot_mvcc".to_string());
                lines.push("Hot Access Path: version_store.primary_key".to_string());
                lines.push(format!("Access Key: primary_key({})", pk_column));
            }
            ScanPlan::IndexScan {
                index_name,
                column,
                filter,
                ..
            } => {
                lines.push("Access Source: hot_mvcc".to_string());
                lines.push("Hot Access Path: version_store.secondary_index".to_string());
                lines.push(format!("Access Key: index({}.{})", index_name, column));
                if filter.is_some() {
                    lines.push("Access Filter: post_index_residual".to_string());
                }
            }
            ScanPlan::MultiIndexScan {
                indexes,
                operation,
                filter,
                ..
            } => {
                lines.push("Access Source: hot_mvcc".to_string());
                lines.push("Hot Access Path: version_store.multi_index".to_string());
                lines.push(format!(
                    "Access Key: multi_index({}; {})",
                    operation,
                    indexes
                        .iter()
                        .map(|(idx, col, _)| format!("{}.{}", idx, col))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                if filter.is_some() {
                    lines.push("Access Filter: post_index_residual".to_string());
                }
            }
            ScanPlan::SegmentedMultiIndexScan {
                indexes,
                operation,
                filter,
                cold_segments,
                cold_rows_hint,
                hot_rows_hint,
                ..
            } => {
                lines.push("Access Source: cold(artifact_index_postings)+hot".to_string());
                lines.push("Cold Access Path: volume.multi_index_union".to_string());
                lines.push("Hot Access Path: version_store.multi_index".to_string());
                lines.push(format!(
                    "Access Key: multi_index({}; {})",
                    operation,
                    indexes
                        .iter()
                        .map(|(idx, col, _)| format!("{}.{}", idx, col))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                lines.push(format!("Cold Segments: {}", cold_segments));
                lines.push(format!("Cold Rows Hint: {}", cold_rows_hint));
                lines.push(format!("Hot Rows Hint: {}", hot_rows_hint));
                if filter.is_some() {
                    lines.push("Access Filter: post_index_residual".to_string());
                }
            }
            ScanPlan::CompositeIndexScan {
                index_name,
                columns,
                filter,
                ..
            } => {
                lines.push("Access Source: hot_mvcc".to_string());
                lines.push("Hot Access Path: version_store.composite_index".to_string());
                lines.push(format!(
                    "Access Key: composite_index({}.({}))",
                    index_name,
                    columns.join(", ")
                ));
                if filter.is_some() {
                    lines.push("Access Filter: post_index_residual".to_string());
                }
            }
            ScanPlan::SegmentedCompositeIndexScan {
                index_name,
                columns,
                cold_segments,
                cold_rows_hint,
                hot_rows_hint,
                filter,
                ordered,
                composite,
                ..
            } => {
                let single_column = !composite;
                lines.push(if *ordered {
                    "Access Source: cold(artifact_ordered_postings)+hot".to_string()
                } else {
                    "Access Source: cold(artifact_exact_postings)+hot".to_string()
                });
                lines.push(if *ordered && single_column {
                    "Cold Access Path: volume.ordered_index".to_string()
                } else if *ordered {
                    "Cold Access Path: volume.composite_ordered_index".to_string()
                } else if single_column {
                    "Cold Access Path: volume.exact_index".to_string()
                } else {
                    "Cold Access Path: volume.composite_exact_index".to_string()
                });
                lines.push(if single_column {
                    "Hot Access Path: version_store.secondary_index".to_string()
                } else {
                    "Hot Access Path: version_store.composite_index".to_string()
                });
                lines.push(if single_column {
                    format!("Access Key: index({}.{})", index_name, columns[0])
                } else {
                    format!(
                        "Access Key: composite_index({}.({}))",
                        index_name,
                        columns.join(", ")
                    )
                });
                lines.push(format!("Cold Segments: {}", cold_segments));
                lines.push(format!("Cold Rows Hint: {}", cold_rows_hint));
                lines.push(format!("Hot Rows Hint: {}", hot_rows_hint));
                if filter.is_some() {
                    lines.push("Access Filter: post_index_residual".to_string());
                }
            }
            ScanPlan::VectorSearch {
                index_name,
                vector_column,
                metric,
                ..
            } => {
                lines.push("Access Source: hot_mvcc".to_string());
                lines.push("Hot Access Path: version_store.vector_hnsw".to_string());
                lines.push(format!(
                    "Access Key: hnsw({}.{}, metric={})",
                    index_name, vector_column, metric
                ));
            }
            ScanPlan::VectorBruteForce {
                vector_column,
                metric,
                ..
            } => {
                lines.push("Access Source: hot_mvcc".to_string());
                lines.push("Hot Access Path: version_store.vector_bruteforce".to_string());
                lines.push(format!(
                    "Access Key: vector_bruteforce({}, metric={})",
                    vector_column, metric
                ));
            }
            ScanPlan::SegmentedScan {
                filter,
                cold_segments,
                cold_rows_hint,
                cold_row_groups_hint,
                cold_selected_segments,
                cold_selected_rows_hint,
                cold_selected_row_groups_hint,
                cold_metadata_pruned_segments,
                cold_metadata_pruned_rows_hint,
                hot_rows_hint,
                ..
            } => {
                lines.push("Access Source: cold(artifact_blocks)+hot".to_string());
                lines.push("Hot Access Path: version_store.snapshot_scan".to_string());
                lines.push("Cold Metadata Path: zone_map+bloom+row_group_metadata".to_string());
                lines.push(format!("Cold Segments: {}", cold_segments));
                lines.push(format!("Cold Rows Hint: {}", cold_rows_hint));
                lines.push(format!("Cold Row Groups Hint: {}", cold_row_groups_hint));
                lines.push(format!(
                    "Cold Metadata Selected Segments: {}",
                    cold_selected_segments
                ));
                lines.push(format!(
                    "Cold Metadata Selected Rows Hint: {}",
                    cold_selected_rows_hint
                ));
                lines.push(format!(
                    "Cold Metadata Selected Row Groups Hint: {}",
                    cold_selected_row_groups_hint
                ));
                lines.push(format!(
                    "Cold Metadata Pruned Segments: {}",
                    cold_metadata_pruned_segments
                ));
                lines.push(format!(
                    "Cold Metadata Pruned Rows Hint: {}",
                    cold_metadata_pruned_rows_hint
                ));
                lines.push(format!("Hot Rows Hint: {}", hot_rows_hint));
                if filter.is_some() {
                    lines.push(
                        "Access Filter: cold_scan_predicate+hot_snapshot_residual".to_string(),
                    );
                }
            }
        }

        lines
    }
}

impl fmt::Display for ScanPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScanPlan::SeqScan { table, filter } => {
                write!(f, "Seq Scan on {}", table)?;
                if let Some(flt) = filter {
                    write!(f, "\n  Filter: {}", flt)?;
                }
                Ok(())
            }
            ScanPlan::ParallelSeqScan {
                table,
                filter,
                workers,
            } => {
                write!(f, "Parallel Seq Scan on {} (workers={})", table, workers)?;
                if let Some(flt) = filter {
                    write!(f, "\n  Filter: {}", flt)?;
                }
                Ok(())
            }
            ScanPlan::PkLookup {
                table,
                pk_column,
                pk_value,
            } => {
                write!(f, "PK Lookup on {}\n  {} = {}", table, pk_column, pk_value)
            }
            ScanPlan::IndexScan {
                table,
                index_name,
                column,
                condition,
                filter,
            } => {
                write!(
                    f,
                    "Index Scan using {} on {}\n  Index Cond: {} {}",
                    index_name, table, column, condition
                )?;
                if let Some(flt) = filter {
                    write!(f, "\n  Filter: {}", flt)?;
                }
                Ok(())
            }
            ScanPlan::MultiIndexScan {
                table,
                indexes,
                operation,
                filter,
            } => {
                write!(f, "Multi-Index Scan on {} ({})", table, operation)?;
                for (idx_name, col, cond) in indexes {
                    write!(f, "\n  -> {} on {}: {}", idx_name, col, cond)?;
                }
                if let Some(flt) = filter {
                    write!(f, "\n  Filter: {}", flt)?;
                }
                Ok(())
            }
            ScanPlan::SegmentedMultiIndexScan {
                table,
                indexes,
                operation,
                filter,
                cold_segments,
                cold_rows_hint,
                hot_rows_hint,
            } => {
                write!(f, "Segmented Multi-Index Scan on {} ({})", table, operation)?;
                for (idx_name, col, cond) in indexes {
                    write!(f, "\n  -> {} on {}: {}", idx_name, col, cond)?;
                }
                write!(
                    f,
                    "\n  Cold Segments: {}\n  Cold Rows Hint: {}\n  Hot Rows Hint: {}",
                    cold_segments, cold_rows_hint, hot_rows_hint
                )?;
                if let Some(flt) = filter {
                    write!(f, "\n  Filter: {}", flt)?;
                }
                Ok(())
            }
            ScanPlan::CompositeIndexScan {
                table,
                index_name,
                columns,
                conditions,
                filter,
            } => {
                write!(
                    f,
                    "Composite Index Scan using {} on {}\n  Columns: ({})",
                    index_name,
                    table,
                    columns.join(", ")
                )?;
                for (col, cond) in columns.iter().zip(conditions.iter()) {
                    write!(f, "\n  {} {}", col, cond)?;
                }
                if let Some(flt) = filter {
                    write!(f, "\n  Filter: {}", flt)?;
                }
                Ok(())
            }
            ScanPlan::SegmentedCompositeIndexScan {
                table,
                index_name,
                columns,
                conditions,
                filter,
                cold_segments,
                cold_rows_hint,
                hot_rows_hint,
                ordered: _,
                composite,
            } => {
                let node_name = if !composite {
                    "Segmented Index Scan"
                } else {
                    "Segmented Composite Index Scan"
                };
                write!(
                    f,
                    "{} using {} on {}\n  Columns: ({})\n  Cold Segments: {}\n  Cold Rows Hint: {}\n  Hot Rows Hint: {}",
                    node_name,
                    index_name,
                    table,
                    columns.join(", "),
                    cold_segments,
                    cold_rows_hint,
                    hot_rows_hint
                )?;
                for (column, condition) in columns.iter().zip(conditions.iter()) {
                    write!(f, "\n  {} {}", column, condition)?;
                }
                if let Some(residual) = filter {
                    write!(f, "\n  Filter: {}", residual)?;
                }
                Ok(())
            }
            ScanPlan::VectorSearch {
                table,
                index_name,
                vector_column,
                metric,
                k,
                ef_search,
                filter,
            } => {
                write!(
                    f,
                    "HNSW Index Scan using {} on {}\n  Column: {}\n  Metric: {}\n  K: {}\n  EF Search: {}",
                    index_name, table, vector_column, metric, k, ef_search
                )?;
                if let Some(flt) = filter {
                    write!(f, "\n  Filter: {}", flt)?;
                }
                Ok(())
            }
            ScanPlan::VectorBruteForce {
                table,
                vector_column,
                metric,
                k,
                filter,
            } => {
                write!(
                    f,
                    "Vector Scan on {}\n  Column: {}\n  Metric: {}\n  K: {}",
                    table, vector_column, metric, k
                )?;
                if let Some(flt) = filter {
                    write!(f, "\n  Filter: {}", flt)?;
                }
                Ok(())
            }
            ScanPlan::SegmentedScan {
                table,
                filter,
                cold_segments,
                cold_rows_hint,
                cold_selected_segments,
                cold_selected_rows_hint,
                hot_rows_hint,
                ..
            } => {
                write!(
                    f,
                    "Segmented Scan on {}\n  Cold Segments: {}\n  Cold Rows Hint: {}\n  Cold Selected Segments: {}\n  Cold Selected Rows Hint: {}\n  Hot Rows Hint: {}",
                    table,
                    cold_segments,
                    cold_rows_hint,
                    cold_selected_segments,
                    cold_selected_rows_hint,
                    hot_rows_hint
                )?;
                if let Some(flt) = filter {
                    write!(f, "\n  Filter: {}", flt)?;
                }
                Ok(())
            }
        }
    }
}

/// Table represents a database table
///
/// This trait defines the interface for interacting with a table,
/// including schema management, data manipulation (CRUD), and scanning.
///
/// # Example
///
/// ```ignore
/// let table = transaction.get_table("users")?;
/// println!("Table: {}", table.name());
/// println!("Schema: {:?}", table.schema());
///
/// // Insert a row
/// let row = Row::from_values(vec![Value::Integer(1), Value::text("Alice")]);
/// table.insert(row)?;
///
/// // Scan all rows
/// let scanner = table.scan(&[0, 1], None)?;
/// while scanner.next() {
///     println!("{:?}", scanner.row());
/// }
/// ```
/// A normalized contiguous predicate over an INTEGER primary key.
///
/// The range is deliberately a storage contract rather than a parser detail:
/// a caller that has proven its WHERE clause consists only of conjunctive
/// comparisons on the current INTEGER PRIMARY KEY can ask storage for an
/// exact metadata-only count.  Any other shape must use the normal scan path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IntegerPrimaryKeyRange {
    lower: Option<(i64, bool)>,
    upper: Option<(i64, bool)>,
}

impl IntegerPrimaryKeyRange {
    /// Build a range from simple AND-combined comparisons.
    ///
    /// When `require_all_columns` is true every supplied comparison must target
    /// `pk_name`; this is the mode for an exact COUNT operator.  Scanner
    /// refinement may pass false and use only the PK portion as a safe prune.
    pub fn from_conjunctive_comparisons(
        comparisons: &[(&str, Operator, &Value)],
        pk_name: &str,
        require_all_columns: bool,
    ) -> Option<Self> {
        fn unqualified(name: &str) -> &str {
            name.rsplit('.').next().unwrap_or(name).trim_matches('"')
        }

        let mut range = Self::default();
        let mut found = false;
        for &(column, operator, value) in comparisons {
            if !unqualified(column).eq_ignore_ascii_case(pk_name) {
                if require_all_columns {
                    return None;
                }
                continue;
            }

            // Row-ID metadata has the exact INTEGER physical domain. A
            // Float/Decimal bound cannot be truncated or saturated here:
            // doing so may exclude a valid row before the canonical mixed
            // numeric predicate gets its residual check (for example
            // `id < 0.5` must retain row ID 0). Decline the optimization unless
            // the bound already has the exact physical representation.
            let Value::Integer(value) = value else {
                return None;
            };
            if !range.apply_comparison(operator, *value) {
                return None;
            }
            found = true;
        }

        found.then_some(range)
    }

    /// True when no INTEGER can satisfy both normalized bounds.
    #[inline]
    pub fn is_empty(&self) -> bool {
        match (self.lower, self.upper) {
            (Some((lower, lower_inclusive)), Some((upper, upper_inclusive))) => {
                lower > upper || (lower == upper && (!lower_inclusive || !upper_inclusive))
            }
            _ => false,
        }
    }

    /// Normalized lower bound as `(value, inclusive)`.
    #[inline]
    pub fn lower_bound(&self) -> Option<(i64, bool)> {
        self.lower
    }

    /// Normalized upper bound as `(value, inclusive)`.
    #[inline]
    pub fn upper_bound(&self) -> Option<(i64, bool)> {
        self.upper
    }

    /// Return whether one row id belongs to this range.
    #[inline]
    pub fn contains(&self, row_id: i64) -> bool {
        if self.is_empty() {
            return false;
        }
        if let Some((lower, inclusive)) = self.lower {
            if row_id < lower || (row_id == lower && !inclusive) {
                return false;
            }
        }
        if let Some((upper, inclusive)) = self.upper {
            if row_id > upper || (row_id == upper && !inclusive) {
                return false;
            }
        }
        true
    }

    /// Return the exact half-open slice of sorted row IDs matching this range.
    #[inline]
    pub fn slice_bounds(&self, row_ids: &[i64]) -> (usize, usize) {
        self.bounds_with(row_ids.len(), |index| row_ids[index])
    }

    /// Return bounds over any sorted row-id owner without materializing a
    /// temporary slice. Cold artifact-backed metadata uses this with compact row-id runs.
    pub fn bounds_with(
        &self,
        len: usize,
        mut row_id_at: impl FnMut(usize) -> i64,
    ) -> (usize, usize) {
        if self.is_empty() {
            return (0, 0);
        }
        let mut partition = |mut predicate: Box<dyn FnMut(i64) -> bool>| {
            let mut left = 0usize;
            let mut right = len;
            while left < right {
                let middle = left + (right - left) / 2;
                if predicate(row_id_at(middle)) {
                    left = middle + 1;
                } else {
                    right = middle;
                }
            }
            left
        };
        let start = match self.lower {
            Some((value, true)) => partition(Box::new(move |row_id| row_id < value)),
            Some((value, false)) => partition(Box::new(move |row_id| row_id <= value)),
            None => 0,
        };
        let end = match self.upper {
            Some((value, true)) => partition(Box::new(move |row_id| row_id <= value)),
            Some((value, false)) => partition(Box::new(move |row_id| row_id < value)),
            None => len,
        };
        (start.min(end), end)
    }

    fn apply_comparison(&mut self, operator: Operator, value: i64) -> bool {
        match operator {
            Operator::Eq => {
                self.tighten_lower(value, true);
                self.tighten_upper(value, true);
            }
            Operator::Gt => self.tighten_lower(value, false),
            Operator::Gte => self.tighten_lower(value, true),
            Operator::Lt => self.tighten_upper(value, false),
            Operator::Lte => self.tighten_upper(value, true),
            _ => return false,
        }
        true
    }

    fn tighten_lower(&mut self, value: i64, inclusive: bool) {
        if self.lower.is_none_or(|(current, current_inclusive)| {
            value > current || (value == current && !inclusive && current_inclusive)
        }) {
            self.lower = Some((value, inclusive));
        }
    }

    fn tighten_upper(&mut self, value: i64, inclusive: bool) {
        if self.upper.is_none_or(|(current, current_inclusive)| {
            value < current || (value == current && !inclusive && current_inclusive)
        }) {
            self.upper = Some((value, inclusive));
        }
    }
}

pub trait Table: Send + Sync {
    /// Returns the name of the table
    fn name(&self) -> &str;

    /// Returns the schema of the table
    fn schema(&self) -> &Schema;

    /// Returns the transaction ID this table handle belongs to.
    /// Used by FK enforcement to participate in the caller's transaction.
    fn txn_id(&self) -> i64;

    /// Stage removal of an index entry owned by an immutable storage tier.
    /// Composite storage implementations use this hidden hook to join cold
    /// index changes to the MVCC commit transition instead of mutating shared
    /// indexes before commit.
    #[doc(hidden)]
    fn stage_external_index_removal(
        &mut self,
        _index: Arc<dyn Index>,
        _values: Vec<Value>,
        _row_id: i64,
    ) -> Result<()> {
        Err(Error::NotSupported(
            "table does not support transaction-private external index removals".to_string(),
        ))
    }

    /// Creates a new column in the table
    ///
    /// # Arguments
    /// * `name` - The name of the column
    /// * `column_type` - The data type of the column
    /// * `nullable` - Whether the column can contain NULL values
    fn create_column(&mut self, name: &str, column_type: DataType, nullable: bool) -> Result<()>;

    /// Creates a new column in the table with default expression
    ///
    /// # Arguments
    /// * `name` - The name of the column
    /// * `column_type` - The data type of the column
    /// * `nullable` - Whether the column can contain NULL values
    /// * `default_expr` - Default value expression as string (to be evaluated during INSERT)
    fn create_column_with_default(
        &mut self,
        name: &str,
        column_type: DataType,
        nullable: bool,
        default_expr: Option<String>,
    ) -> Result<()> {
        if default_expr.is_some() {
            return Err(radixdb_core::Error::NotSupported(
                "table implementation does not support column defaults".to_string(),
            ));
        }
        self.create_column(name, column_type, nullable)
    }

    /// Creates a new column with both expression and pre-computed default value
    ///
    /// The pre-computed default value is used for schema evolution (backfilling existing rows)
    /// while the expression string is used for new inserts.
    ///
    /// # Arguments
    /// * `name` - The name of the column
    /// * `column_type` - The data type of the column
    /// * `nullable` - Whether the column can contain NULL values
    /// * `default_expr` - Default value expression as string (for INSERT)
    /// * `default_value` - Pre-computed default value (for schema evolution)
    fn create_column_with_default_value(
        &mut self,
        name: &str,
        column_type: DataType,
        nullable: bool,
        default_expr: Option<String>,
        default_value: Option<radixdb_core::Value>,
    ) -> Result<()> {
        if default_value.is_some() {
            return Err(radixdb_core::Error::NotSupported(
                "table implementation does not support schema backfill values".to_string(),
            ));
        }
        self.create_column_with_default(name, column_type, nullable, default_expr)
    }

    /// Drops a column from the table
    ///
    /// # Arguments
    /// * `name` - The name of the column to drop
    fn drop_column(&mut self, name: &str) -> Result<()>;

    /// Materializes storage-owned values that must exist before statement-level
    /// constraints are evaluated (currently AUTO_INCREMENT INTEGER/UUID keys).
    ///
    /// The default is a no-op for table implementations without generated
    /// values. Calling this more than once for the same row must be idempotent.
    fn materialize_insert_values(&mut self, _row: &mut Row) -> Result<()> {
        Ok(())
    }

    /// Inserts a single row into the table
    ///
    /// # Arguments
    /// * `row` - The row to insert
    ///
    /// # Returns
    /// The inserted row (with AUTO_INCREMENT values applied)
    fn insert(&mut self, row: Row) -> Result<Row>;

    /// Inserts a single row without returning it (avoids clone overhead)
    ///
    /// Use this when the RETURNING clause is not needed.
    /// This is ~7ms faster per 1000 rows by avoiding Row::clone().
    ///
    /// # Arguments
    /// * `row` - The row to insert
    fn insert_discard(&mut self, row: Row) -> Result<()> {
        self.insert(row)?;
        Ok(())
    }

    /// Inserts multiple rows into the table in a single batch operation
    ///
    /// This is more efficient than calling `insert` multiple times.
    ///
    /// # Arguments
    /// * `rows` - The rows to insert
    fn insert_batch(&mut self, rows: Vec<Row>) -> Result<()>;

    /// Updates rows matching the given expression
    ///
    /// # Arguments
    /// * `where_expr` - Expression to filter rows to update (None means all rows)
    /// * `setter` - Function that transforms a row in place, returns true if changed
    ///
    /// # Returns
    /// The number of rows updated
    fn update(
        &mut self,
        where_expr: Option<&dyn Expression>,
        setter: &mut dyn FnMut(Row) -> Result<(Row, bool)>,
    ) -> Result<i32>;

    /// Updates rows by their row IDs directly (O(k) lookup instead of O(n) scan).
    ///
    /// This is an optimization for UPDATE with IN subquery on INTEGER PRIMARY KEY.
    /// Instead of scanning all rows and filtering, we directly look up the specific row IDs.
    ///
    /// # Arguments
    /// * `row_ids` - The row IDs to update (should be sorted for cache locality)
    /// * `setter` - Function that transforms a row in place, returns true if changed
    ///
    /// # Returns
    /// The number of rows updated
    fn update_by_row_ids(
        &mut self,
        row_ids: &[i64],
        setter: &mut dyn FnMut(Row) -> Result<(Row, bool)>,
    ) -> Result<i32>;

    /// Deletes rows by their row IDs directly (O(k) lookup instead of O(n) scan).
    ///
    /// This is an optimization for DELETE with IN subquery on INTEGER PRIMARY KEY.
    ///
    /// # Arguments
    /// * `row_ids` - The row IDs to delete (should be sorted for cache locality)
    ///
    /// # Returns
    /// The number of rows deleted
    fn delete_by_row_ids(&mut self, row_ids: &[i64]) -> Result<i32>;

    /// Discovers the internal row IDs matched by one DELETE predicate.
    ///
    /// This is the read half of the DML access plan. An exact empty projection
    /// preserves row identity while allowing storage implementations to read
    /// only predicate columns. Scanner implementations may override
    /// `collect_remaining_row_ids` to avoid constructing output rows entirely.
    fn collect_delete_candidate_row_ids(
        &self,
        where_expr: Option<&dyn Expression>,
    ) -> Result<Vec<i64>> {
        let mut scanner = self.scan_exact_projection(&[], where_expr)?;
        let mut row_ids = Vec::new();
        if !scanner.collect_remaining_row_ids(&mut row_ids)? {
            while scanner.next() {
                row_ids.push(scanner.current_row_id()?);
            }
        }
        if let Some(error) = scanner.err() {
            return Err(error.clone());
        }
        scanner.close()?;
        row_ids.sort_unstable();
        row_ids.dedup();
        Ok(row_ids)
    }

    /// Applies the mutation half of a DELETE access plan to a bounded row-ID
    /// set. Storage implementations that can revalidate a predicate after
    /// acquiring write claims should override this method.
    fn delete_candidate_row_ids(
        &mut self,
        row_ids: &[i64],
        _recheck_expr: Option<&dyn Expression>,
    ) -> Result<i32> {
        self.delete_by_row_ids(row_ids)
    }

    /// Applies a DELETE candidate batch and reports the exact physical IDs
    /// that were staged. RETURNING uses this outcome to avoid issuing one
    /// storage mutation per row while preserving concurrent recheck semantics.
    fn delete_candidate_row_ids_collect(
        &mut self,
        row_ids: &[i64],
        recheck_expr: Option<&dyn Expression>,
        deleted_row_ids: &mut Vec<i64>,
    ) -> Result<i32> {
        let deleted = self.delete_candidate_row_ids(row_ids, recheck_expr)?;
        if deleted as usize == row_ids.len() {
            deleted_row_ids.extend_from_slice(row_ids);
        }
        Ok(deleted)
    }

    /// Returns all active row IDs visible to the current transaction.
    /// Used for NOT IN (anti-join) optimization.
    fn get_active_row_ids(&self) -> Vec<i64>;

    /// Populate a FxHashSet with all hot row_ids. Avoids the intermediate Vec
    /// allocation of get_active_row_ids() when building skip sets.
    fn collect_hot_row_ids_into(&self, dest: &mut rustc_hash::FxHashSet<i64>) {
        for id in self.get_active_row_ids() {
            dest.insert(id);
        }
    }

    /// Populate the logical shadow set used while preparing a mixed hot/cold
    /// scan. Only hot versions visible to this table transaction may shadow an
    /// older cold version; transaction-local writes/deletes must also be
    /// included. The caller owns the table's membership fence for the complete
    /// hot+cold snapshot.
    #[doc(hidden)]
    fn collect_shadow_row_ids_into(&self, dest: &mut rustc_hash::FxHashSet<i64>) {
        self.collect_hot_row_ids_into(dest);
    }

    /// Check if a specific row_id exists in the hot buffer.
    /// O(log n) lookup instead of collecting all row_ids.
    fn has_row_id(&self, _row_id: i64) -> bool {
        false
    }

    /// Returns the per-table publication fence, when the storage implementation
    /// has one.
    ///
    /// A writer holds the exclusive side from publication of hot MVCC versions
    /// through publication of matching cold tombstones and transaction
    /// visibility. A membership probe holds the shared side for its complete
    /// hot+cold observation. Implementations without an MVCC/cold split may
    /// leave the default `None` value.
    #[doc(hidden)]
    fn membership_fence(&self) -> Option<Arc<RwLock<()>>> {
        None
    }

    /// Scan while restricting candidates to stable row-ID ranges produced by
    /// a non-stale zone-map generation. Implementations that cannot consume
    /// the ranges safely retain the correctness-first ordinary scan fallback.
    #[doc(hidden)]
    fn scan_with_row_id_ranges(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
        _row_id_ranges: &[(i64, i64)],
    ) -> Result<Box<dyn Scanner>> {
        self.scan(column_indices, where_expr)
    }

    /// Exact-projection counterpart of [`Table::scan_with_row_id_ranges`].
    #[doc(hidden)]
    fn scan_exact_projection_with_row_id_ranges(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
        _row_id_ranges: &[(i64, i64)],
    ) -> Result<Box<dyn Scanner>> {
        self.scan_exact_projection(column_indices, where_expr)
    }

    /// Probe row IDs for visibility in the current transaction.
    ///
    /// `matches[i]` corresponds exactly to `row_ids[i]`. Duplicate row IDs are
    /// intentionally preserved, so the returned count includes every matching
    /// input position. Implementations must overwrite every output slot.
    ///
    /// The default implementation is a correctness fallback for table types
    /// without a metadata-only lookup path. It collects the table's visible
    /// rows and therefore may materialize payload data. Storage implementations
    /// with MVCC/segment metadata should override this method.
    fn probe_visible_row_ids(&self, row_ids: &[i64], matches: &mut [bool]) -> Result<usize> {
        let fence = self.membership_fence();
        let _publication_guard = fence.as_ref().map(|fence| fence.read());
        self.probe_visible_row_ids_unfenced(row_ids, matches)
    }

    /// Implementation hook for [`Table::probe_visible_row_ids`].
    ///
    /// The public method acquires [`Table::membership_fence`] before calling
    /// this hook. Composite table implementations that need to hold one fence
    /// across several storage sources may call it after acquiring that fence.
    /// Callers outside the storage layer must use `probe_visible_row_ids`.
    #[doc(hidden)]
    fn probe_visible_row_ids_unfenced(
        &self,
        row_ids: &[i64],
        matches: &mut [bool],
    ) -> Result<usize> {
        if row_ids.len() != matches.len() {
            return Err(Error::invalid_argument(format!(
                "row ID probe output length mismatch: expected {}, got {}",
                row_ids.len(),
                matches.len()
            )));
        }

        matches.fill(false);
        if row_ids.is_empty() {
            return Ok(0);
        }

        let requested: rustc_hash::FxHashSet<i64> = row_ids.iter().copied().collect();
        let visible_rows = self.collect_all_rows(None)?;
        let mut visible_ids =
            rustc_hash::FxHashSet::with_capacity_and_hasher(requested.len(), Default::default());
        for (row_id, _) in visible_rows {
            if requested.contains(&row_id) {
                visible_ids.insert(row_id);
            }
        }

        let mut count = 0;
        for (row_id, matched) in row_ids.iter().zip(matches.iter_mut()) {
            *matched = visible_ids.contains(row_id);
            count += usize::from(*matched);
        }
        Ok(count)
    }

    /// Count visible rows in an exact range of the current INTEGER PRIMARY
    /// KEY without loading row payload blocks.
    ///
    /// `None` means that this storage implementation cannot prove the answer
    /// from metadata for the present MVCC/segment state and the executor must
    /// use its regular filtered aggregate path. `Some(Err(_))` is a real
    /// storage error and must not be silently converted into a scan.
    fn count_visible_integer_primary_key_range(
        &self,
        _range: &IntegerPrimaryKeyRange,
    ) -> Option<Result<usize>> {
        None
    }

    /// Claim a row for update to prevent concurrent cold-row modifications.
    /// When two transactions update the same cold row, both mirror it into hot
    /// via insert_discard. Without claiming, neither detects the other because
    /// the row starts absent from hot. This method uses the VersionStore's
    /// uncommitted_writes map to serialize access.
    fn try_claim_row(&self, _row_id: i64) -> Result<()> {
        Ok(())
    }

    /// Claims several rows in one deterministic mutation boundary.
    ///
    /// Implementations with MVCC should override this method to sort/deduplicate
    /// candidates and acquire the underlying write claims without repeating
    /// transaction-store locks for every row. The default preserves the
    /// contract for simpler table implementations.
    fn try_claim_rows(&self, row_ids: &[i64]) -> Result<()> {
        for &row_id in row_ids {
            self.try_claim_row(row_id)?;
        }
        Ok(())
    }

    /// Claims DELETE candidates while preserving DELETE provenance in MVCC
    /// implementations. Simple tables may use the generic claim contract.
    fn try_claim_rows_for_delete(&self, row_ids: &[i64]) -> Result<()> {
        self.try_claim_rows(row_ids)
    }

    /// Deletes rows matching the given expression
    ///
    /// # Arguments
    /// * `where_expr` - Expression to filter rows to delete (None means all rows)
    ///
    /// # Returns
    /// The number of rows deleted
    fn delete(&mut self, where_expr: Option<&dyn Expression>) -> Result<i32>;

    /// Truncates the table, removing all rows efficiently.
    /// Unlike DELETE, this drops storage directly instead of creating delete versions.
    /// Default implementation falls back to delete(None).
    fn truncate(&mut self) -> Result<i32> {
        self.delete(None)
    }

    /// Validate implementation-specific TRUNCATE preconditions, execute one
    /// coordinated storage publication, then clear the table. Hybrid storage
    /// overrides this so a failed hot preflight cannot expose a cold-only
    /// partial outcome.
    fn truncate_after(&mut self, before_clear: &mut dyn FnMut() -> Result<()>) -> Result<i32> {
        before_clear()?;
        self.truncate()
    }

    /// Scans the table and returns a scanner over matching rows
    ///
    /// # Arguments
    /// * `column_indices` - Indices of columns to include in the scan
    /// * `where_expr` - Expression to filter rows (None means all rows)
    ///
    /// # Returns
    /// A scanner that iterates over the matching rows
    fn scan(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
    ) -> Result<Box<dyn Scanner>>;

    /// Visit the visible full-row snapshot without requiring one owning
    /// `RowVec` for the complete table.
    ///
    /// The default keeps third-party table implementations source-compatible
    /// by consuming their scanner. Native MVCC/segmented tables override this
    /// boundary so maintenance operations such as `ANALYZE` can retain only
    /// bounded per-column state while walking a large table.
    fn visit_visible_rows(&self, visitor: &mut dyn FnMut(i64, Row) -> Result<()>) -> Result<()> {
        let projection: Vec<usize> = (0..self.schema().columns.len()).collect();
        let mut scanner = self.scan(&projection, None)?;
        while scanner.next() {
            let (row_id, row) = scanner.take_row_with_id()?;
            visitor(row_id, row)?;
        }
        if let Some(error) = scanner.err().cloned() {
            let _ = scanner.close();
            return Err(error);
        }
        scanner.close()
    }

    /// Scans the table with exact projection semantics.
    ///
    /// Unlike `scan()`, an empty `column_indices` slice means "return zero-width
    /// rows", not `SELECT *`. This boundary is for COUNT(*) and other
    /// columnless paths that need row visibility/cardinality without reading
    /// user payload columns.
    fn scan_exact_projection(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
    ) -> Result<Box<dyn Scanner>> {
        if column_indices.is_empty() {
            return Err(radixdb_core::Error::NotSupported(
                "table implementation does not support exact empty projection".to_string(),
            ));
        }
        self.scan(column_indices, where_expr)
    }

    /// Exact-projection scan used after the caller has already acquired this
    /// table's exclusive membership fence.
    ///
    /// Native hot+cold tables override this hook to avoid recursively taking
    /// the non-reentrant shared side while transactional commit validation owns
    /// the exclusive side. Callers outside the storage commit path must use
    /// [`Table::scan_exact_projection`].
    #[doc(hidden)]
    fn scan_exact_projection_unfenced(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
    ) -> Result<Box<dyn Scanner>> {
        self.scan_exact_projection(column_indices, where_expr)
    }

    /// Collects all rows matching the expression without intermediate cloning
    ///
    /// This is more efficient than using scan() when you need all rows at once,
    /// as it avoids the double-clone overhead of the scanner interface.
    ///
    /// # Arguments
    /// * `where_expr` - Expression to filter rows (None means all rows)
    ///
    /// # Returns
    /// Cached row vector - returns to cache on drop for reuse
    fn collect_all_rows(&self, where_expr: Option<&dyn Expression>) -> Result<RowVec>;

    /// Collects rows with an optional limit (LIMIT pushdown optimization)
    ///
    /// This enables early termination when only a limited number of rows are needed,
    /// avoiding the cost of scanning the entire table.
    ///
    /// # Arguments
    /// * `where_expr` - Optional filter expression
    /// * `limit` - Maximum number of rows to return
    /// * `offset` - Number of rows to skip before returning
    ///
    /// # Returns
    /// A RowVec containing rows up to the limit (after offset) with row IDs
    fn collect_rows_with_limit(
        &self,
        where_expr: Option<&dyn Expression>,
        limit: usize,
        offset: usize,
    ) -> Result<RowVec> {
        // Default implementation: collect all and apply limit/offset
        let all_rows = self.collect_all_rows(where_expr)?;
        Ok(all_rows.into_iter().skip(offset).take(limit).collect())
    }

    /// Collects rows with LIMIT/OFFSET without guaranteeing deterministic order.
    ///
    /// This is an optimization for queries with LIMIT but without ORDER BY.
    /// Since SQL doesn't guarantee order for LIMIT without ORDER BY, we can
    /// skip sorting and return rows in arbitrary order. This provides significant
    /// speedup by enabling true early termination.
    ///
    /// # Arguments
    /// * `where_expr` - Optional filter expression
    /// * `limit` - Maximum number of rows to return
    /// * `offset` - Number of rows to skip before returning
    ///
    /// # Returns
    /// A RowVec containing rows up to the limit (after offset), in arbitrary order
    fn collect_rows_with_limit_unordered(
        &self,
        where_expr: Option<&dyn Expression>,
        limit: usize,
        offset: usize,
    ) -> Result<RowVec> {
        // Default implementation: delegate to ordered version
        self.collect_rows_with_limit(where_expr, limit, offset)
    }

    /// Collects projected rows with LIMIT/OFFSET without guaranteeing deterministic order.
    ///
    /// This is the projected variant of `collect_rows_with_limit_unordered`.
    /// The default implementation preserves each table's existing LIMIT behavior
    /// and trims rows at the table boundary; storage implementations with column
    /// blocks should override this to avoid reading unused columns.
    fn collect_rows_with_limit_unordered_projected(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
        limit: usize,
        offset: usize,
    ) -> Result<RowVec> {
        let rows = self.collect_rows_with_limit_unordered(where_expr, limit, offset)?;
        let num_schema_cols = self.schema().columns.len();
        let needs_projection = !column_indices.is_empty()
            && !(column_indices.len() == num_schema_cols
                && column_indices.iter().enumerate().all(|(i, &idx)| i == idx));

        if !needs_projection {
            return Ok(rows);
        }

        Ok(rows
            .into_iter()
            .map(|(row_id, row)| {
                let values = column_indices
                    .iter()
                    .map(|&idx| row.get(idx).cloned().unwrap_or_else(Value::null_unknown))
                    .collect();
                (row_id, Row::from_values(values))
            })
            .collect())
    }

    /// Collects projected rows with exact projection semantics and LIMIT/OFFSET.
    ///
    /// Unlike `collect_rows_with_limit_unordered_projected()`, an empty
    /// projection means zero-width rows. This is intended for expression
    /// projection/dependency plans where the dependency set may legitimately be
    /// empty.
    fn collect_rows_with_limit_unordered_exact_projected(
        &self,
        column_indices: &[usize],
        where_expr: Option<&dyn Expression>,
        limit: usize,
        offset: usize,
    ) -> Result<RowVec> {
        let mut scanner = self.scan_exact_projection(column_indices, where_expr)?;
        let mut rows = RowVec::with_capacity(limit.min(1024));
        let mut skipped = 0usize;

        while scanner.next() {
            if skipped < offset {
                skipped += 1;
                continue;
            }
            if rows.len() >= limit {
                break;
            }
            rows.push(scanner.take_row_with_id()?);
        }
        if let Some(err) = scanner.err() {
            return Err(err.clone());
        }
        scanner.close()?;
        Ok(rows)
    }

    /// Collects all rows WITHOUT guaranteeing deterministic order.
    ///
    /// This is an optimization for GROUP BY queries where row order doesn't matter.
    /// By skipping the O(n log n) sort, this provides significant speedup for large tables.
    ///
    /// # Returns
    /// Cached row vector in arbitrary (storage iteration) order
    fn collect_all_rows_unsorted(&self) -> Result<RowVec> {
        // Default implementation: delegate to ordered version
        self.collect_all_rows(None)
    }

    /// Collects rows for specific row IDs.
    ///
    /// Used by HNSW index search to fetch rows after approximate nearest neighbor lookup.
    /// The returned rows preserve the order of the input row_ids.
    fn collect_rows_by_ids(&self, _row_ids: &[i64]) -> Result<RowVec> {
        Err(Error::NotSupported(
            "collect_rows_by_ids not implemented".to_string(),
        ))
    }

    /// Collects exact projected rows for specific row IDs.
    ///
    /// This is an unfiltered lookup boundary intended for operators that have
    /// already proven visibility/key eligibility and only need a subset of row
    /// columns. An empty projection means exactly zero columns, not `SELECT *`.
    fn collect_rows_by_ids_projected(
        &self,
        row_ids: &[i64],
        column_indices: &[usize],
    ) -> Result<RowVec> {
        let rows = self.collect_rows_by_ids(row_ids)?;
        rows.into_iter()
            .map(|(row_id, row)| {
                row.take_columns(column_indices)
                    .map(|projected| (row_id, projected))
            })
            .collect()
    }

    /// Collect rows with ORDER BY + LIMIT using deferred materialization
    ///
    /// This is an optimization for `SELECT * FROM t ORDER BY col LIMIT n`:
    /// - Loads only the sort column values (not full rows)
    /// - Sorts indices by those values
    /// - Materializes only the top N rows
    ///
    /// # Arguments
    /// * `sort_col_idx` - Column index to sort by
    /// * `ascending` - Sort direction (true = ASC, false = DESC)
    /// * `limit` - Maximum rows to return
    /// * `offset` - Rows to skip before collecting
    ///
    /// # Returns
    /// A vector of rows sorted by the specified column
    fn collect_rows_sorted_with_limit(
        &self,
        sort_col_idx: usize,
        ascending: bool,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Row>> {
        // Default implementation: collect all, sort, take limit
        // Concrete implementations can override with deferred materialization
        let mut rows = self.collect_all_rows(None)?;
        rows.sort_by(|(_, a), (_, b)| {
            let va = a.get(sort_col_idx);
            let vb = b.get(sort_col_idx);
            let cmp = match (va, vb) {
                (None, None) => std::cmp::Ordering::Equal,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (Some(va), Some(vb)) => va.compare(vb).unwrap_or(std::cmp::Ordering::Equal),
            };
            if ascending {
                cmp
            } else {
                cmp.reverse()
            }
        });
        Ok(rows
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|(_, row)| row)
            .collect())
    }

    /// Closes the table and releases any resources
    fn close(&mut self) -> Result<()>;

    /// Commits any pending changes in this table's transaction
    ///
    /// This applies the table's local transaction changes to the global store,
    /// making them visible to other transactions.
    fn commit(&mut self) -> Result<()>;

    /// Rolls back any pending changes in this table's transaction
    ///
    /// This discards any local changes that have not been committed.
    fn rollback(&mut self);

    /// Rolls back changes to a specific timestamp (for savepoint support)
    ///
    /// Discards all local changes with timestamps greater than the specified timestamp.
    /// This is used by ROLLBACK TO SAVEPOINT to partially undo transaction changes.
    ///
    /// # Arguments
    /// * `timestamp` - The timestamp to roll back to (in nanoseconds since epoch)
    fn rollback_to_timestamp(&self, timestamp: i64);

    /// Returns true if this table has uncommitted local changes
    ///
    /// This is used by the transaction to determine if the two-phase commit
    /// protocol needs to be executed.
    fn has_local_changes(&self) -> bool;

    /// Returns the pending versions to be committed for WAL logging
    ///
    /// Returns a list of (row_id, row_data, is_deleted, txn_id, create_time) tuples representing
    /// all uncommitted changes in this table. This is called before commit()
    /// to capture changes for WAL persistence.
    ///
    /// # Returns
    /// Vec of (row_id, row_data, is_deleted, txn_id, create_time) tuples
    fn get_pending_versions(&self) -> Vec<(i64, Row, bool, i64, i64)> {
        Vec::new() // Default implementation returns empty - override in concrete tables
    }

    // ---- Index Operations ----

    /// Creates an index on the table
    ///
    /// # Arguments
    /// * `name` - The name of the index
    /// * `columns` - The column names to include in the index
    /// * `is_unique` - Whether this is a unique index
    fn create_index(&self, name: &str, columns: &[&str], is_unique: bool) -> Result<()>;

    /// Creates an index on the table with a specific index type
    ///
    /// # Arguments
    /// * `name` - The name of the index
    /// * `columns` - The column names to include in the index
    /// * `is_unique` - Whether this is a unique index
    /// * `index_type` - Optional index type (Hash, BTree, Bitmap). If None, auto-selects based on column types.
    ///
    /// # Type-Based Index Selection (when index_type is None):
    /// - TEXT/JSON columns → Hash index (avoids O(strlen) comparisons)
    /// - BOOLEAN columns → Bitmap index (only 2 values, fast AND/OR)
    /// - INTEGER/FLOAT/TIMESTAMP columns → BTree index (supports range queries)
    fn create_index_with_type(
        &self,
        name: &str,
        columns: &[&str],
        is_unique: bool,
        index_type: Option<IndexType>,
    ) -> Result<()> {
        // Default implementation calls create_index (ignores index_type)
        let _ = index_type;
        self.create_index(name, columns, is_unique)
    }

    /// Create a core-owned index whose logical external value is transformed
    /// by a composition-layer encoder before reaching the access method.
    #[doc(hidden)]
    fn create_index_with_key_encoder(
        &self,
        name: &str,
        columns: &[&str],
        is_unique: bool,
        index_type: IndexType,
        encoder: crate::index::PreparedIndexKeyEncoder,
    ) -> Result<()> {
        let _ = (name, columns, is_unique, index_type, encoder);
        Err(Error::NotSupported(
            "encoded index construction is not supported by this table".to_owned(),
        ))
    }

    /// Builds a complete index object without publishing it to table readers.
    /// Segmented storage uses this internal hook to add cold rows before the
    /// index becomes discoverable. Ordinary implementations may decline it.
    #[doc(hidden)]
    fn build_index_with_type_detached(
        &self,
        name: &str,
        columns: &[&str],
        is_unique: bool,
        index_type: Option<IndexType>,
    ) -> Result<Arc<dyn Index>> {
        let _ = (name, columns, is_unique, index_type);
        Err(Error::NotSupported(
            "detached index construction is not supported by this table".to_string(),
        ))
    }

    /// Builds and publishes only the hot portion of an index while immutable
    /// cold coverage is prepared by the physical artifact publisher.
    ///
    /// The method is intentionally separate from ordinary CREATE INDEX: a
    /// caller must either publish complete cold coverage before readers are
    /// released or call `complete_deferred_cold_index` as its failure path.
    #[doc(hidden)]
    fn create_index_with_deferred_cold_backfill(
        &self,
        name: &str,
        columns: &[&str],
        is_unique: bool,
        index_type: Option<IndexType>,
    ) -> Result<()> {
        let _ = (name, columns, is_unique, index_type);
        Err(Error::NotSupported(
            "deferred cold index construction is not supported by this table".to_string(),
        ))
    }

    /// Completes a previously deferred cold index in memory. This is the
    /// correctness fallback when immutable accelerator publication cannot be
    /// completed after the logical DDL commit became durable.
    #[doc(hidden)]
    fn complete_deferred_cold_index(&self, name: &str, columns: &[&str]) -> Result<()> {
        let _ = (name, columns);
        Err(Error::NotSupported(
            "deferred cold index completion is not supported by this table".to_string(),
        ))
    }

    /// Publishes one fully built detached index as a single catalog action.
    #[doc(hidden)]
    fn publish_detached_index(&self, index: Arc<dyn Index>) -> Result<()> {
        let _ = index;
        Err(Error::NotSupported(
            "detached index publication is not supported by this table".to_string(),
        ))
    }

    /// Creates a partial index on the table.
    fn create_partial_index_with_type(
        &self,
        name: &str,
        columns: &[&str],
        is_unique: bool,
        index_type: Option<IndexType>,
        predicate: crate::index::PartialIndexPredicate,
    ) -> Result<()> {
        let _ = (name, columns, is_unique, index_type, predicate);
        Err(Error::invalid_argument(
            "partial indexes are not supported by this table",
        ))
    }

    /// Creates an HNSW index with custom parameters
    ///
    /// # Arguments
    /// * `name` - The name of the index
    /// * `column` - The vector column to index
    /// * `is_unique` - Whether this is a unique index
    /// * `m` - Max connections per node per layer
    /// * `ef_construction` - Build-time beam width
    /// * `ef_search` - Search-time beam width
    /// * `metric` - Distance metric (L2, Cosine, InnerProduct)
    #[allow(clippy::too_many_arguments)]
    fn create_hnsw_index(
        &self,
        name: &str,
        column: &str,
        is_unique: bool,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        metric: crate::index::HnswDistanceMetric,
    ) -> Result<()> {
        let _ = (
            name,
            column,
            is_unique,
            m,
            ef_construction,
            ef_search,
            metric,
        );
        Err(radixdb_core::Error::internal(
            "HNSW index not supported by this storage engine".to_string(),
        ))
    }

    /// Drops an index from the table
    ///
    /// # Arguments
    /// * `name` - The name of the index to drop
    fn drop_index(&self, name: &str) -> Result<()>;

    /// Renames an index without rebuilding index data.
    ///
    /// # Arguments
    /// * `old_name` - Existing index name
    /// * `new_name` - New index name
    fn rename_index(&self, old_name: &str, new_name: &str) -> Result<()>;

    /// Creates a btree index on a column
    ///
    /// # Arguments
    /// * `column_name` - The column to index
    /// * `is_unique` - Whether this is a unique index
    /// * `custom_name` - Optional custom name for the index
    fn create_btree_index(
        &self,
        column_name: &str,
        is_unique: bool,
        custom_name: Option<&str>,
    ) -> Result<()>;

    /// Drops a btree index from the table
    ///
    /// # Arguments
    /// * `column_name` - The column whose index to drop
    fn drop_btree_index(&self, column_name: &str) -> Result<()>;

    /// Creates a multi-column index on the table
    ///
    /// # Arguments
    /// * `name` - The name of the index
    /// * `columns` - The column names to include in the index
    /// * `is_unique` - Whether this is a unique index
    fn create_multi_column_index(
        &self,
        name: &str,
        columns: &[&str],
        is_unique: bool,
    ) -> Result<()> {
        let _ = (name, columns, is_unique);
        Err(Error::NotSupported(
            "Multi-column indexes not supported by this table type".to_string(),
        ))
    }

    /// Checks if an index exists on a specific column
    ///
    /// # Arguments
    /// * `column_name` - The column to check for an index
    ///
    /// # Returns
    /// true if an index exists on the column, false otherwise
    fn has_index_on_column(&self, column_name: &str) -> bool {
        let _ = column_name;
        false // Default implementation - override in concrete tables
    }

    /// Gets the index on a specific column (if any exists)
    ///
    /// # Arguments
    /// * `column_name` - The column to get the index for
    ///
    /// # Returns
    /// Some(index) if an index exists on the column, None otherwise
    fn get_index_on_column(&self, column_name: &str) -> Option<std::sync::Arc<dyn Index>> {
        let _ = column_name;
        None // Default implementation - override in concrete tables
    }

    /// Resolve exact row-id candidates for several values of one indexed
    /// column. Implementations with immutable storage must include every
    /// physical source or return `None`; callers may then use an honest scan
    /// fallback instead of treating a hot-only index as complete.
    fn collect_row_ids_by_index_values(
        &self,
        column_name: &str,
        values: &[Value],
    ) -> Option<Result<Vec<i64>>> {
        // Transaction-local inserts and index-key updates are deliberately not
        // published to shared indexes before commit. A shared index is
        // therefore not a complete candidate source while local changes exist.
        if self.has_local_changes() {
            return None;
        }
        let index = self.get_index_on_column(column_name)?;
        let (_, column) = self.schema().find_column(column_name)?;
        if crate::expression::in_list::has_cross_numeric_physical_variant(column.data_type, values)
        {
            return None;
        }
        let mut row_ids = Vec::new();
        for value in values {
            let value = value.coerce_to_type(column.data_type);
            if value.is_null() {
                continue;
            }
            if let Err(error) =
                index.get_row_ids_equal_into(std::slice::from_ref(&value), &mut row_ids)
            {
                return Some(Err(error));
            }
        }
        row_ids.sort_unstable();
        row_ids.dedup();
        Some(Ok(row_ids))
    }

    /// Resolve and materialize exact indexed candidates in one physical
    /// operation. Immutable tables may override this to fuse their
    /// authoritative key recheck with the requested projection; the default
    /// preserves the existing two-step contract.
    fn collect_rows_by_index_values_projected(
        &self,
        column_name: &str,
        values: &[Value],
        projection: Option<&[usize]>,
    ) -> Option<Result<RowVec>> {
        let row_ids = match self.collect_row_ids_by_index_values(column_name, values)? {
            Ok(row_ids) => row_ids,
            Err(error) => return Some(Err(error)),
        };
        Some(match projection {
            Some(columns) => self.collect_rows_by_ids_projected(&row_ids, columns),
            None => self.collect_rows_by_ids(&row_ids),
        })
    }

    /// Resolve the union of several inclusive ranges through one named
    /// core-owned index. `None` means the index cannot prove complete coverage
    /// for this transaction/snapshot and the executor must use a full scan.
    fn collect_row_ids_by_index_ranges(
        &self,
        index_name: &str,
        ranges: &[IndexKeyRange],
    ) -> Option<Result<Vec<i64>>> {
        if self.has_local_changes() {
            return None;
        }
        let index = self.get_index(index_name)?;
        let mut row_ids = Vec::new();
        for range in ranges {
            let entries = match index.find_physical_range(
                std::slice::from_ref(&range.start),
                std::slice::from_ref(&range.end),
                true,
                true,
            ) {
                Ok(entries) => entries,
                Err(error) => return Some(Err(error)),
            };
            row_ids.extend(entries.into_iter().map(|entry| entry.row_id));
        }
        row_ids.sort_unstable();
        row_ids.dedup();
        Some(Ok(row_ids))
    }

    /// Returns true when this table has immutable cold segment rows in addition
    /// to the hot MVCC store.
    ///
    /// Normal secondary indexes are hot-MVCC indexes unless a concrete table
    /// explicitly documents and maintains cold-populated entries. Optimizers
    /// must not assume that a secondary index covers cold rows merely because
    /// `get_index_on_column()` returns an index handle.
    fn has_cold_segments(&self) -> bool {
        false
    }

    /// Gets all unique indexes on the table (for constraint checking).
    ///
    /// Returns a list of (index_name, column_names) for each unique index.
    /// Used by SegmentedTable to check volume data during inserts.
    fn get_unique_indexes(&self) -> Vec<(String, Vec<String>)> {
        Vec::new() // Default: no unique indexes
    }

    /// Gets all index handles known to this table.
    ///
    /// Most callers should prefer narrower helpers. Volume-backed DML uses this
    /// to maintain entries that were populated from cold rows for special index
    /// families such as partial indexes.
    fn get_indexes(&self) -> Vec<Arc<dyn Index>> {
        Vec::new()
    }

    /// Finds a conflicting row ID for a unique-key lookup.
    ///
    /// Volume-backed tables can override this to probe cold storage directly
    /// after the hot index path misses, avoiding a full table scan on upserts.
    fn find_unique_conflict_row_id(
        &self,
        _index_name: &str,
        _column_name: &str,
        _row_values: &[Value],
    ) -> Result<Option<i64>> {
        Ok(None)
    }

    /// Iterate unique non-PK indexes by reference, avoiding per-call String allocations.
    ///
    /// The callback receives `(&str, &[String])` — the index name and its column names —
    /// without cloning.  Implementations that hold an `RwLock<FxHashMap<String, Arc<dyn Index>>>`
    /// can iterate the lock guard directly.
    ///
    /// The default implementation falls back to `get_unique_indexes()`.
    fn for_each_unique_non_pk_index(
        &self,
        f: &mut dyn FnMut(&str, &[String]) -> Result<()>,
    ) -> Result<()> {
        for (name, cols) in self.get_unique_indexes() {
            f(&name, &cols)?;
        }
        Ok(())
    }

    /// Gets unique non-PK index handles, preserving index metadata such as
    /// CREATE INDEX ... WHERE predicates.
    ///
    /// The older `(name, columns)` helper is still useful for allocation-free
    /// full-index checks, but volume-backed constraint checks need the actual
    /// index object to avoid treating partial unique indexes as full unique
    /// indexes.
    fn get_unique_non_pk_indexes(&self) -> Vec<Arc<dyn Index>> {
        Vec::new()
    }

    /// Check if the table has any unique indexes that are NOT the PK column.
    /// Used as a fast bail-out to avoid allocating get_unique_indexes().
    fn has_unique_non_pk_indexes(&self) -> bool {
        false // Default: no
    }

    /// Gets an index by name
    ///
    /// # Arguments
    /// * `name` - The name of the index
    ///
    /// # Returns
    /// Some(index) if found, None otherwise
    fn get_index(&self, name: &str) -> Option<std::sync::Arc<dyn Index>> {
        let _ = name;
        None // Default implementation - override in concrete tables
    }

    /// Find the best multi-column index that matches a set of predicate columns.
    /// Returns the index if it covers a prefix of the given columns.
    /// For example, an index on (a, b, c) can be used for queries on (a), (a, b), or (a, b, c).
    ///
    /// # Arguments
    /// * `predicate_columns` - The columns used in WHERE clause predicates
    ///
    /// # Returns
    /// Some((index, matched_columns)) if found, None otherwise
    fn get_multi_column_index(
        &self,
        predicate_columns: &[&str],
    ) -> Option<(std::sync::Arc<dyn Index>, usize)> {
        let _ = predicate_columns;
        None // Default implementation - override in concrete tables
    }

    /// Gets the minimum value from an indexed column (O(1) or O(log n) instead of O(n) scan)
    ///
    /// # Arguments
    /// * `column_name` - The column to get the minimum value from
    ///
    /// # Returns
    /// Some(Value) if the column has an index with min/max support, None otherwise
    fn get_index_min_value(&self, column_name: &str) -> Option<Value> {
        let _ = column_name;
        None // Default implementation - override in concrete tables
    }

    /// Gets the maximum value from an indexed column (O(1) or O(log n) instead of O(n) scan)
    ///
    /// # Arguments
    /// * `column_name` - The column to get the maximum value from
    ///
    /// # Returns
    /// Some(Value) if the column has an index with min/max support, None otherwise
    fn get_index_max_value(&self, column_name: &str) -> Option<Value> {
        let _ = column_name;
        None // Default implementation - override in concrete tables
    }

    /// Gets the count of rows in the table (COUNT(*) pushdown optimization)
    ///
    /// This enables O(1) row counting instead of O(n) scan for `SELECT COUNT(*) FROM table`
    /// without WHERE clause.
    ///
    /// # Returns
    /// The number of visible rows in the table
    fn row_count(&self) -> usize {
        0 // Default implementation - override in concrete tables
    }

    /// Fast O(1) row count hint for optimizer decisions
    ///
    /// Returns an upper bound estimate without expensive visibility checks.
    /// Use for cache eligibility and similar decisions where exact count isn't needed.
    fn row_count_hint(&self) -> usize {
        self.row_count() // Default falls back to row_count
    }

    /// Fast O(1) exact row count for COUNT(*) queries
    ///
    /// Returns Some(count) if the fast path can be used (no local changes, sees all committed data),
    /// Returns None if the caller should fall back to the full row_count() method.
    ///
    /// This is different from row_count_hint() because it returns the EXACT count,
    /// not an estimate. It's designed for COUNT(*) without WHERE clause.
    fn fast_row_count(&self) -> Option<usize> {
        None // Default: no fast path available
    }

    /// Collects rows sorted by an indexed column with limit (ORDER BY + LIMIT pushdown)
    ///
    /// For queries like `SELECT * FROM table ORDER BY col LIMIT 10`, this uses the
    /// index to get rows in sorted order, stopping after the limit is reached.
    /// This is O(limit) instead of O(n log n) for full sort.
    ///
    /// # Arguments
    /// * `column_name` - The indexed column to order by
    /// * `ascending` - True for ASC, false for DESC
    /// * `limit` - Maximum number of rows to return
    /// * `offset` - Number of rows to skip
    ///
    /// # Returns
    /// Some(RowVec) if the column has an index, None otherwise
    fn collect_rows_ordered_by_index(
        &self,
        column_name: &str,
        ascending: bool,
        limit: usize,
        offset: usize,
    ) -> Option<RowVec> {
        let _ = (column_name, ascending, limit, offset);
        None // Default implementation - override in concrete tables
    }

    /// Bounded ordered lookup for a composite index shaped as equality prefix
    /// plus one range/order column. `None` means the storage layer cannot prove
    /// this physical path for the current hot/cold state; `Some(Err(_))` is a
    /// real execution error and must not be converted into a scan silently.
    fn collect_rows_composite_ordered_range(
        &self,
        where_expr: &dyn Expression,
        order_column: &str,
        ascending: bool,
        limit: usize,
        offset: usize,
    ) -> Option<Result<RowVec>> {
        let _ = (where_expr, order_column, ascending, limit, offset);
        None
    }

    /// Keyset pagination optimization for PRIMARY KEY columns
    ///
    /// For queries like `WHERE id > X ORDER BY id LIMIT Y`, this uses the PK's
    /// natural ordering to start iteration from X and return only Y rows.
    /// This provides O(limit) complexity instead of O(n) for full table scans.
    ///
    /// # Arguments
    /// * `start_after` - For `id > X` (exclusive bound)
    /// * `start_from` - For `id >= X` (inclusive bound)
    /// * `ascending` - True for ASC, false for DESC
    /// * `limit` - Maximum number of rows to return
    ///
    /// # Returns
    /// Some(RowVec) if the table has an INTEGER PRIMARY KEY, None otherwise
    fn collect_rows_pk_keyset(
        &self,
        start_after: Option<i64>,
        start_from: Option<i64>,
        ascending: bool,
        limit: usize,
    ) -> Option<RowVec> {
        let _ = (start_after, start_from, ascending, limit);
        None // Default implementation - override in concrete tables
    }

    /// Collects rows grouped by an indexed partition column (PARTITION BY optimization)
    ///
    /// For window functions with `PARTITION BY col` where col is indexed, this uses the
    /// index to iterate through unique values and collect rows already grouped by partition.
    /// This avoids O(n) hash-based grouping in window function execution.
    ///
    /// # Arguments
    /// * `column_name` - The indexed column to partition by
    ///
    /// # Returns
    /// Some(Vec<(Value, RowVec)>) where each tuple is (partition_value, rows_in_partition)
    /// Returns None if the column has no index
    fn collect_rows_grouped_by_partition(&self, column_name: &str) -> Option<Vec<(Value, RowVec)>> {
        let _ = column_name;
        None // Default implementation - override in concrete tables
    }

    /// Get distinct partition values from an indexed column.
    /// Used for LIMIT pushdown in window functions - allows fetching partitions one at a time.
    ///
    /// # Arguments
    /// * `column_name` - The indexed column to get partition values from
    ///
    /// # Returns
    /// `Some(Vec<Value>)` with distinct values, or `None` if column has no index
    fn get_partition_values(&self, column_name: &str) -> Option<Vec<Value>> {
        let _ = column_name;
        None // Default implementation - override in concrete tables
    }

    /// Compute distinct non-null values for a column by exploiting cold volume
    /// metadata. For dictionary-encoded TEXT columns with no tombstones, this
    /// extracts dictionary entries directly (O(unique values) per volume, no
    /// row scan). Falls back to None when the fast path is not applicable.
    ///
    /// # Arguments
    /// * `col_idx` - Schema column index
    ///
    /// # Returns
    /// `Some(Vec<Value>)` with distinct non-null values, or `None` if fast path unavailable
    fn compute_distinct_values(&self, _col_idx: usize) -> Option<Vec<Value>> {
        None
    }

    /// Get the count of distinct non-null values from an indexed column.
    /// Used for COUNT(DISTINCT col) optimization without cloning all values.
    ///
    /// # Arguments
    /// * `column_name` - The indexed column to count distinct values from
    ///
    /// # Returns
    /// Some(count) excluding NULL values, or None if column has no index
    fn get_partition_count(&self, column_name: &str) -> Option<usize> {
        let _ = column_name;
        None // Default implementation - override in concrete tables
    }

    /// Get rows for a specific partition value.
    /// Used for LIMIT pushdown in window functions - fetches only one partition at a time.
    ///
    /// # Arguments
    /// * `column_name` - The indexed column
    /// * `partition_value` - The specific partition value to fetch rows for
    ///
    /// # Returns
    /// Some(RowVec) with rows matching the partition value, or None if column has no index
    fn get_rows_for_partition_value(
        &self,
        column_name: &str,
        partition_value: &Value,
    ) -> Option<RowVec> {
        let _ = (column_name, partition_value);
        None // Default implementation - override in concrete tables
    }

    /// Fetch rows by their row IDs with an optional filter.
    ///
    /// # Arguments
    /// * `row_ids` - The row IDs to fetch
    /// * `filter` - Filter expression to apply to fetched rows
    ///
    /// # Returns
    /// A checked row vector in input-ID order. Storage/materialization and
    /// expression failures remain distinguishable from a legitimate non-match.
    fn fetch_rows_by_ids(&self, row_ids: &[i64], filter: &dyn Expression) -> Result<RowVec> {
        let candidates = self.collect_rows_by_ids(row_ids)?;
        let mut results = RowVec::with_capacity(candidates.len());
        for (row_id, row) in candidates {
            if filter.evaluate(&row)? {
                results.push((row_id, row));
            }
        }
        Ok(results)
    }

    /// Fetch rows into a reusable RowVec buffer
    fn fetch_rows_by_ids_into(
        &self,
        row_ids: &[i64],
        filter: &dyn Expression,
        buffer: &mut RowVec,
    ) -> Result<()> {
        buffer.extend(self.fetch_rows_by_ids(row_ids, filter)?);
        Ok(())
    }

    // ---- Additional Column Operations ----

    /// Renames a column in the table
    ///
    /// # Arguments
    /// * `old_name` - Current column name
    /// * `new_name` - New column name
    fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<()>;

    /// Modifies a column's definition
    ///
    /// # Arguments
    /// * `name` - The column name
    /// * `column_type` - The new data type
    /// * `nullable` - Whether the column can contain NULL values
    fn modify_column(&mut self, name: &str, column_type: DataType, nullable: bool) -> Result<()>;

    /// Modifies a column's definition and replaces its default expression.
    ///
    /// The default implementation preserves existing behavior for table
    /// implementations that do not store default metadata.
    fn modify_column_with_default(
        &mut self,
        name: &str,
        column_type: DataType,
        nullable: bool,
        default_expr: Option<String>,
        default_value: Option<Value>,
    ) -> Result<()> {
        if default_expr.is_some() || default_value.is_some() {
            return Err(radixdb_core::Error::NotSupported(
                "table implementation does not support replacing column defaults".to_string(),
            ));
        }
        self.modify_column(name, column_type, nullable)
    }

    // ---- Query Operations ----

    /// Executes a SELECT query on the table
    ///
    /// # Arguments
    /// * `columns` - Column names to include in the result
    /// * `expr` - Optional filter expression
    ///
    /// # Returns
    /// A QueryResult with the matching rows
    fn select(
        &self,
        columns: &[&str],
        expr: Option<&dyn Expression>,
    ) -> Result<Box<dyn QueryResult>>;

    /// Executes a SELECT query with column aliases
    ///
    /// # Arguments
    /// * `columns` - Column names to include in the result
    /// * `expr` - Optional filter expression
    /// * `aliases` - Map from alias names to original column names
    ///
    /// # Returns
    /// A QueryResult with the matching rows and aliased column names
    fn select_with_aliases(
        &self,
        columns: &[&str],
        expr: Option<&dyn Expression>,
        aliases: &FxHashMap<String, String>,
    ) -> Result<Box<dyn QueryResult>>;

    /// Executes a temporal SELECT query as of a specific point in time
    ///
    /// # Arguments
    /// * `columns` - Column names to include in the result
    /// * `expr` - Optional filter expression
    /// * `temporal_type` - Either "TRANSACTION" or "TIMESTAMP"
    /// * `temporal_value` - Transaction ID or timestamp in nanoseconds
    ///
    /// # Returns
    /// A QueryResult with rows as they were at the specified point
    fn select_as_of(
        &self,
        columns: &[&str],
        expr: Option<&dyn Expression>,
        temporal_type: &str,
        temporal_value: i64,
    ) -> Result<Box<dyn QueryResult>>;

    /// Explains what access method would be used for a scan
    ///
    /// This method analyzes the WHERE expression and returns a ScanPlan
    /// describing how the query would be executed (without actually executing it).
    /// Used by EXPLAIN to show users the query execution strategy.
    ///
    /// # Arguments
    /// * `where_expr` - Optional filter expression to analyze
    ///
    /// # Returns
    /// A ScanPlan describing the access method that would be used
    fn explain_scan(&self, where_expr: Option<&dyn Expression>) -> ScanPlan {
        // Default implementation returns SeqScan
        ScanPlan::SeqScan {
            table: self.name().to_string(),
            filter: where_expr.map(|e| format!("{:?}", e)),
        }
    }

    // ---- Zone Map Operations (Statistics for Segment Pruning) ----

    /// Sets the zone maps for this table
    ///
    /// Zone maps contain min/max statistics per segment, enabling the query
    /// executor to skip entire segments when predicates fall outside the range.
    /// This is typically called by ANALYZE.
    ///
    /// # Arguments
    /// * `zone_maps` - The zone map statistics for the table
    fn set_zone_maps(&self, _zone_maps: crate::volume::zonemap::TableZoneMap) {
        // Default implementation does nothing - override in concrete tables
    }

    /// Data/schema generation captured before an ANALYZE build. Concrete MVCC
    /// tables override this with a monotonic publication owner.
    #[doc(hidden)]
    fn zone_map_generation(&self) -> u64 {
        0
    }

    /// Gets the zone maps for this table
    ///
    /// Returns None if zone maps have not been built (ANALYZE not run)
    /// Uses Arc to avoid expensive cloning on high QPS workloads
    fn get_zone_maps(&self) -> Option<std::sync::Arc<crate::volume::zonemap::TableZoneMap>> {
        None // Default implementation - override in concrete tables
    }

    /// Gets the segments that need to be scanned for a given predicate
    ///
    /// Uses zone maps to determine which segments can be pruned (skipped)
    /// based on the predicate's column, operator, and value.
    ///
    /// # Arguments
    /// * `column` - The column name in the predicate
    /// * `operator` - The comparison operator
    /// * `value` - The value being compared against
    ///
    /// # Returns
    /// Some(Vec<segment_ids>) if zone maps exist, None otherwise
    fn get_segments_to_scan(
        &self,
        _column: &str,
        _operator: radixdb_core::Operator,
        _value: &Value,
    ) -> Option<Vec<u32>> {
        None // Default implementation - override in concrete tables
    }

    // ---- Deferred Aggregation Methods ----
    // These methods enable aggregation pushdown to avoid full row materialization.
    // For `SELECT SUM(col) FROM table`, we can compute SUM directly from arena data
    // without cloning any rows.

    /// Compute SUM of a column without materializing rows (deferred aggregation)
    ///
    /// Returns (sum, count_non_null) for proper NULL handling.
    /// Returns None if the optimization is not available.
    ///
    /// # Arguments
    /// * `col_idx` - Column index to sum
    fn sum_column(&self, _col_idx: usize) -> Option<DeferredSum> {
        None // Default implementation - override in concrete tables
    }

    /// Compute AVG of a column without materializing rows (deferred aggregation)
    ///
    /// Returns (sum, count_non_null) for computing average as sum/count.
    /// Returns None if the optimization is not available.
    ///
    /// # Arguments
    /// * `col_idx` - Column index to average
    fn avg_column(&self, _col_idx: usize) -> Option<DeferredSum> {
        // Default: use sum_column if available
        self.sum_column(_col_idx)
    }

    /// Compute MIN of a column without materializing rows (deferred aggregation)
    ///
    /// Returns the minimum value, or None if no non-NULL values exist.
    /// Returns None for the outer Option if the optimization is not available.
    ///
    /// # Arguments
    /// * `col_idx` - Column index to find minimum
    fn min_column(&self, _col_idx: usize) -> Option<Option<Value>> {
        None // Default implementation - override in concrete tables
    }

    /// Compute MAX of a column without materializing rows (deferred aggregation)
    ///
    /// Returns the maximum value, or None if no non-NULL values exist.
    /// Returns None for the outer Option if the optimization is not available.
    ///
    /// # Arguments
    /// * `col_idx` - Column index to find maximum
    fn max_column(&self, _col_idx: usize) -> Option<Option<Value>> {
        None // Default implementation - override in concrete tables
    }

    /// Compute aggregates with a WHERE filter at the storage level.
    ///
    /// This pushes filtered aggregation (e.g. `SELECT SUM(col) FROM t WHERE x > 5`)
    /// directly into the storage layer, scanning only rows that match the predicate
    /// and computing aggregates without materializing full Row objects in the executor.
    ///
    /// # Arguments
    /// * `aggregates` - List of (operation, column_index) pairs
    /// * `where_expr` - Storage expression representing the WHERE clause filter
    ///
    /// # Returns
    /// Some(values) with one Value per aggregate if optimization is available, None otherwise
    fn compute_filtered_aggregates(
        &self,
        _aggregates: &[(AggregateOp, usize)],
        _where_expr: &dyn Expression,
    ) -> Option<Vec<radixdb_core::Value>> {
        None // Default: not supported
    }

    /// Compute grouped aggregates at the storage level.
    ///
    /// This performs GROUP BY aggregation directly on arena storage without
    /// materializing Row objects, significantly reducing memory allocations.
    ///
    /// # Arguments
    /// * `group_by_indices` - Column indices to group by
    /// * `aggregates` - List of (operation, column_index) pairs
    ///
    /// # Returns
    /// Some(results) if optimization is available, None otherwise
    fn compute_grouped_aggregates(
        &self,
        _group_by_indices: &[usize],
        _aggregates: &[(AggregateOp, usize)],
    ) -> Option<Vec<GroupedAggregateResult>> {
        None // Default: not supported
    }

    /// Compute grouped aggregates with a WHERE filter at the storage level.
    ///
    /// This is the GROUP BY counterpart of `compute_filtered_aggregates`: the
    /// storage layer applies the predicate, groups matching rows, and computes
    /// aggregate values without forcing the executor to materialize all input
    /// rows first.
    ///
    /// # Arguments
    /// * `group_by_indices` - Column indices to group by
    /// * `aggregates` - List of (operation, column_index) pairs
    /// * `where_expr` - Storage expression representing the WHERE clause filter
    ///
    /// # Returns
    /// Some(results) if optimization is available, None otherwise
    fn compute_filtered_grouped_aggregates(
        &self,
        _group_by_indices: &[usize],
        _aggregates: &[(AggregateOp, usize)],
        _where_expr: &dyn Expression,
    ) -> Option<Vec<GroupedAggregateResult>> {
        None // Default: not supported
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deferred_sum_preserves_values_beyond_float_integer_precision() {
        let mut sum = DeferredSum::new();
        sum.add_integer(9_007_199_254_740_992, 1);
        sum.add_integer(1, 1);
        assert_eq!(
            sum.into_value().unwrap(),
            Value::Integer(9_007_199_254_740_993)
        );

        let mut wide = DeferredSum::new();
        wide.add_integer(i64::MAX as i128, 1);
        wide.add_integer(1, 1);
        assert_eq!(
            wide.into_value().unwrap().as_decimal_parts(),
            Some((i64::MAX as i128 + 1, 19, 0))
        );
    }

    // Verify trait is object-safe
    fn _assert_object_safe(_: &dyn Table) {}

    #[test]
    fn integer_primary_key_range_normalizes_bounds_and_exactness() {
        let lower = Value::Integer(3);
        let upper = Value::Integer(7);
        let comparisons = [
            ("events.id", Operator::Gte, &lower),
            ("id", Operator::Lt, &upper),
        ];
        let range = IntegerPrimaryKeyRange::from_conjunctive_comparisons(&comparisons, "id", true)
            .expect("conjunctive PK range");

        assert!(range.contains(3));
        assert!(range.contains(6));
        assert!(!range.contains(7));
        assert_eq!(range.slice_bounds(&[1, 3, 4, 6, 7, 9]), (1, 4));

        let other = Value::Integer(1);
        let mixed = [
            ("id", Operator::Eq, &lower),
            ("status", Operator::Eq, &other),
        ];
        assert!(IntegerPrimaryKeyRange::from_conjunctive_comparisons(&mixed, "id", true).is_none());
        assert!(
            IntegerPrimaryKeyRange::from_conjunctive_comparisons(&mixed, "id", false).is_some()
        );

        let fractional = Value::Float(3.5);
        let fractional_comparison = [("id", Operator::Eq, &fractional)];
        assert!(IntegerPrimaryKeyRange::from_conjunctive_comparisons(
            &fractional_comparison,
            "id",
            true,
        )
        .is_none());

        let not_equal = [("id", Operator::Ne, &lower)];
        assert!(
            IntegerPrimaryKeyRange::from_conjunctive_comparisons(&not_equal, "id", true).is_none()
        );
    }

    #[test]
    fn integer_primary_key_range_marks_contradictions_empty() {
        let lower = Value::Integer(9);
        let upper = Value::Integer(9);
        let comparisons = [("id", Operator::Gt, &lower), ("id", Operator::Lte, &upper)];
        let range = IntegerPrimaryKeyRange::from_conjunctive_comparisons(&comparisons, "id", true)
            .expect("supported comparisons");
        assert!(range.is_empty());
        assert_eq!(range.slice_bounds(&[1, 9, 10]), (0, 0));
    }

    #[test]
    fn integer_primary_key_range_handles_i64_edges_without_overflow() {
        let min = Value::Integer(i64::MIN);
        let max = Value::Integer(i64::MAX);
        let row_ids = [i64::MIN, -1, 0, i64::MAX];

        let all_comparisons = [("id", Operator::Gte, &min), ("id", Operator::Lte, &max)];
        let all =
            IntegerPrimaryKeyRange::from_conjunctive_comparisons(&all_comparisons, "id", true)
                .unwrap();
        assert_eq!(all.slice_bounds(&row_ids), (0, row_ids.len()));

        let above_max = [("id", Operator::Gt, &max)];
        let above_max =
            IntegerPrimaryKeyRange::from_conjunctive_comparisons(&above_max, "id", true).unwrap();
        assert_eq!(
            above_max.slice_bounds(&row_ids),
            (row_ids.len(), row_ids.len())
        );

        let below_min = [("id", Operator::Lt, &min)];
        let below_min =
            IntegerPrimaryKeyRange::from_conjunctive_comparisons(&below_min, "id", true).unwrap();
        assert_eq!(below_min.slice_bounds(&row_ids), (0, 0));
    }

    // ScanPlan Display tests

    #[test]
    fn test_seq_scan_display_without_filter() {
        let plan = ScanPlan::SeqScan {
            table: "users".to_string(),
            filter: None,
        };
        let display = format!("{}", plan);
        assert_eq!(display, "Seq Scan on users");
    }

    #[test]
    fn test_seq_scan_display_with_filter() {
        let plan = ScanPlan::SeqScan {
            table: "orders".to_string(),
            filter: Some("amount > 100".to_string()),
        };
        let display = format!("{}", plan);
        assert!(display.contains("Seq Scan on orders"));
        assert!(display.contains("Filter: amount > 100"));
    }

    #[test]
    fn test_parallel_seq_scan_display_without_filter() {
        let plan = ScanPlan::ParallelSeqScan {
            table: "products".to_string(),
            filter: None,
            workers: 4,
        };
        let display = format!("{}", plan);
        assert_eq!(display, "Parallel Seq Scan on products (workers=4)");
    }

    #[test]
    fn test_parallel_seq_scan_display_with_filter() {
        let plan = ScanPlan::ParallelSeqScan {
            table: "items".to_string(),
            filter: Some("price < 50".to_string()),
            workers: 8,
        };
        let display = format!("{}", plan);
        assert!(display.contains("Parallel Seq Scan on items (workers=8)"));
        assert!(display.contains("Filter: price < 50"));
    }

    #[test]
    fn test_pk_lookup_display() {
        let plan = ScanPlan::PkLookup {
            table: "users".to_string(),
            pk_column: "id".to_string(),
            pk_value: "42".to_string(),
        };
        let display = format!("{}", plan);
        assert!(display.contains("PK Lookup on users"));
        assert!(display.contains("id = 42"));
    }

    #[test]
    fn test_index_scan_display() {
        let plan = ScanPlan::IndexScan {
            table: "orders".to_string(),
            index_name: "idx_customer_id".to_string(),
            column: "customer_id".to_string(),
            condition: "= 123".to_string(),
            filter: None,
        };
        let display = format!("{}", plan);
        assert!(display.contains("Index Scan using idx_customer_id on orders"));
        assert!(display.contains("Index Cond: customer_id = 123"));
    }

    #[test]
    fn test_multi_index_scan_display_and() {
        let plan = ScanPlan::MultiIndexScan {
            table: "products".to_string(),
            indexes: vec![
                (
                    "idx_category".to_string(),
                    "category".to_string(),
                    "= 'electronics'".to_string(),
                ),
                (
                    "idx_price".to_string(),
                    "price".to_string(),
                    "> 100".to_string(),
                ),
            ],
            operation: "AND".to_string(),
            filter: None,
        };
        let display = format!("{}", plan);
        assert!(display.contains("Multi-Index Scan on products (AND)"));
        assert!(display.contains("idx_category on category: = 'electronics'"));
        assert!(display.contains("idx_price on price: > 100"));
    }

    #[test]
    fn test_multi_index_scan_display_or() {
        let plan = ScanPlan::MultiIndexScan {
            table: "items".to_string(),
            indexes: vec![
                ("idx_a".to_string(), "col_a".to_string(), "= 1".to_string()),
                ("idx_b".to_string(), "col_b".to_string(), "= 2".to_string()),
            ],
            operation: "OR".to_string(),
            filter: None,
        };
        let display = format!("{}", plan);
        assert!(display.contains("Multi-Index Scan on items (OR)"));
    }

    #[test]
    fn test_composite_index_scan_display() {
        let plan = ScanPlan::CompositeIndexScan {
            table: "orders".to_string(),
            index_name: "idx_cust_date".to_string(),
            columns: vec!["customer_id".to_string(), "order_date".to_string()],
            conditions: vec!["= 100".to_string(), "> '2024-01-01'".to_string()],
            filter: None,
        };
        let display = format!("{}", plan);
        assert!(display.contains("Composite Index Scan using idx_cust_date on orders"));
        assert!(display.contains("Columns: (customer_id, order_date)"));
        assert!(display.contains("customer_id = 100"));
        assert!(display.contains("order_date > '2024-01-01'"));
    }

    #[test]
    fn test_scan_plan_debug() {
        let plan = ScanPlan::SeqScan {
            table: "test".to_string(),
            filter: Some("x > 1".to_string()),
        };
        let debug = format!("{:?}", plan);
        assert!(debug.contains("SeqScan"));
        assert!(debug.contains("test"));
    }

    #[test]
    fn test_scan_plan_clone() {
        let plan = ScanPlan::PkLookup {
            table: "users".to_string(),
            pk_column: "id".to_string(),
            pk_value: "1".to_string(),
        };
        let cloned = plan.clone();
        match cloned {
            ScanPlan::PkLookup {
                table,
                pk_column,
                pk_value,
            } => {
                assert_eq!(table, "users");
                assert_eq!(pk_column, "id");
                assert_eq!(pk_value, "1");
            }
            _ => panic!("Expected PkLookup"),
        }
    }

    #[test]
    fn test_multi_index_scan_empty_indexes() {
        let plan = ScanPlan::MultiIndexScan {
            table: "empty".to_string(),
            indexes: vec![],
            operation: "AND".to_string(),
            filter: None,
        };
        let display = format!("{}", plan);
        assert!(display.contains("Multi-Index Scan on empty (AND)"));
    }

    #[test]
    fn test_composite_index_scan_single_column() {
        let plan = ScanPlan::CompositeIndexScan {
            table: "single".to_string(),
            index_name: "idx_single".to_string(),
            columns: vec!["id".to_string()],
            conditions: vec!["= 1".to_string()],
            filter: None,
        };
        let display = format!("{}", plan);
        assert!(display.contains("Composite Index Scan using idx_single on single"));
        assert!(display.contains("Columns: (id)"));
        assert!(display.contains("id = 1"));
    }

    #[test]
    fn test_parallel_seq_scan_single_worker() {
        let plan = ScanPlan::ParallelSeqScan {
            table: "small".to_string(),
            filter: None,
            workers: 1,
        };
        let display = format!("{}", plan);
        assert_eq!(display, "Parallel Seq Scan on small (workers=1)");
    }

    #[test]
    fn test_segmented_scan_stable_access_path_cold_artifact() {
        let plan = ScanPlan::SegmentedScan {
            table: "events".to_string(),
            filter: None,
            cold_segments: 2,
            cold_rows_hint: 100,
            cold_row_groups_hint: 4,
            cold_selected_segments: 2,
            cold_selected_rows_hint: 100,
            cold_selected_row_groups_hint: 4,
            cold_metadata_pruned_segments: 0,
            cold_metadata_pruned_rows_hint: 0,
            hot_rows_hint: 0,
        };

        assert_eq!(plan.access_path_id(), "scan.cold_artifact");
        let lines = plan.stable_explain_lines();
        assert!(lines.contains(&"Access Path: scan.cold_artifact".to_string()));
        assert!(lines.contains(&"Access Source: cold(artifact_blocks)+hot".to_string()));
        assert!(
            lines.contains(&"Cold Metadata Path: zone_map+bloom+row_group_metadata".to_string())
        );
        assert!(lines.contains(&"Cold Segments: 2".to_string()));
        assert!(lines.contains(&"Cold Rows Hint: 100".to_string()));
        assert!(lines.contains(&"Cold Row Groups Hint: 4".to_string()));
        assert!(lines.contains(&"Cold Metadata Selected Segments: 2".to_string()));
        assert!(lines.contains(&"Cold Metadata Selected Rows Hint: 100".to_string()));
        assert!(lines.contains(&"Cold Metadata Pruned Segments: 0".to_string()));
        assert!(lines.contains(&"Hot Rows Hint: 0".to_string()));
    }

    #[test]
    fn test_segmented_scan_stable_access_path_mixed_artifact_hot() {
        let plan = ScanPlan::SegmentedScan {
            table: "events".to_string(),
            filter: Some("id > 10".to_string()),
            cold_segments: 1,
            cold_rows_hint: 50,
            cold_row_groups_hint: 1,
            cold_selected_segments: 1,
            cold_selected_rows_hint: 50,
            cold_selected_row_groups_hint: 1,
            cold_metadata_pruned_segments: 0,
            cold_metadata_pruned_rows_hint: 0,
            hot_rows_hint: 3,
        };

        assert_eq!(plan.access_path_id(), "scan.mixed_cold_artifact_hot");
        let lines = plan.stable_explain_lines();
        assert!(lines.contains(&"Access Path: scan.mixed_cold_artifact_hot".to_string()));
        assert!(
            lines.contains(&"Access Filter: cold_scan_predicate+hot_snapshot_residual".to_string())
        );
        let display = format!("{}", plan);
        assert!(display.contains("Segmented Scan on events"));
        assert!(display.contains("Filter: id > 10"));
    }
}
