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

//! Query Planner - Integrates statistics and cost-based optimization
//!
//! This module provides the QueryPlanner which coordinates between:
//! - Table statistics stored in system tables (sys_table_stats, sys_column_stats)
//! - Cost estimator for choosing access methods
//! - Zone maps for segment pruning
//! - Index selection for efficient access paths
//!
//! The planner is used by the executor to make informed decisions about:
//! - Whether to use an index vs sequential scan
//! - Which join algorithm to use
//! - Which segments can be skipped using zone maps

use std::sync::Arc;

use radixdb_core::StringMap;

use crate::optimizer::feedback::{fingerprint_predicate, FeedbackCache};
use crate::optimizer::workload::{EdgeAwarePlanner, EdgeJoinRecommendation};
use radixdb_core::{DataType, Operator, Result, Schema, Value};
use radixdb_sql::ast::Expression;
use radixdb_storage::mvcc::engine::MVCCEngine;
use radixdb_storage::statistics::{
    decode_statistics_value, Histogram, HistogramOp, TableStats, SYS_COLUMN_STATS, SYS_TABLE_STATS,
};
use radixdb_storage::traits::{Engine, Table, Transaction};
use radixdb_storage::volume::zonemap::TableZoneMap;

/// Query planner that integrates statistics-based optimization
pub struct QueryPlanner {
    /// Reference to the storage engine for reading statistics
    engine: Arc<MVCCEngine>,
    /// Cache of table statistics to avoid repeated lookups
    stats_cache: std::sync::RwLock<StringMap<CachedStats>>,
    /// Feedback is scoped to one engine owner and invalidated with its data.
    feedback_cache: Arc<FeedbackCache>,
}

/// Default TTL for cached statistics (5 minutes)
/// After this time, stats are considered potentially stale and will be refreshed
const STATS_CACHE_TTL_SECS: u64 = 300;

/// Maximum number of tables to cache statistics for (LRU eviction threshold)
/// This prevents unbounded memory growth for databases with many tables
const MAX_STATS_CACHE_SIZE: usize = 1000;
const STORAGE_PAGE_BYTES: u64 = 4096;

#[doc(hidden)]
pub fn estimated_schema_column_width(data_type: DataType, vector_dimensions: u16) -> u64 {
    match data_type {
        DataType::Null => 1,
        DataType::Boolean => 1,
        DataType::Date => 4,
        DataType::Integer | DataType::Float | DataType::Timestamp => 8,
        DataType::Uuid => 16,
        DataType::Decimal => 24,
        DataType::Text | DataType::Json | DataType::Bytes => 32,
        DataType::Vector => u64::from(vector_dimensions).saturating_mul(4).max(16),
    }
}

#[doc(hidden)]
pub fn estimated_schema_row_width(schema: &Schema) -> u64 {
    schema
        .columns
        .iter()
        .map(|column| {
            estimated_schema_column_width(column.data_type, column.vector_dimensions)
                .saturating_add(u64::from(column.nullable))
        })
        .sum::<u64>()
        .max(1)
}

#[inline]
fn decode_nonnegative_stat(value: Option<&Value>) -> Option<u64> {
    match value {
        Some(Value::Integer(value)) => u64::try_from(*value).ok(),
        _ => None,
    }
}

/// Cached statistics for a table
#[derive(Clone)]
struct CachedStats {
    table_stats: TableStats,
    column_stats: StringMap<ColumnStatsCache>,
    /// Timestamp when this cache entry was created
    cached_at: radixdb_core::time_compat::Instant,
    /// Timestamp of last access (for LRU eviction)
    last_accessed: radixdb_core::time_compat::Instant,
}

impl CachedStats {
    /// Check if this cache entry is stale (older than TTL)
    fn is_stale(&self) -> bool {
        self.cached_at.elapsed().as_secs() > STATS_CACHE_TTL_SECS
    }

    /// Update last accessed time
    fn touch(&mut self) {
        self.last_accessed = radixdb_core::time_compat::Instant::now();
    }
}

/// Cached column stats (simplified for internal use)
#[derive(Clone)]
pub struct ColumnStatsCache {
    /// Number of null values in the column
    pub null_count: u64,
    /// Number of distinct values in the column
    pub distinct_count: u64,
    /// Minimum value in the column
    pub min_value: Option<Value>,
    /// Maximum value in the column
    pub max_value: Option<Value>,
    /// Histogram for range selectivity estimation
    pub histogram: Option<Histogram>,
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new(engine: Arc<MVCCEngine>) -> Self {
        Self::with_feedback_cache(engine, Arc::new(FeedbackCache::new()))
    }

    #[doc(hidden)]
    pub fn with_feedback_cache(
        engine: Arc<MVCCEngine>,
        feedback_cache: Arc<FeedbackCache>,
    ) -> Self {
        Self {
            engine,
            stats_cache: std::sync::RwLock::new(StringMap::new()),
            feedback_cache,
        }
    }

    /// Invalidate cached statistics for a table
    ///
    /// Call this after ANALYZE to ensure fresh statistics are used.
    pub fn invalidate_stats_cache(&self, table_name: &str) {
        let mut cache = self.stats_cache.write().unwrap();
        cache.remove(&table_name.to_lowercase());
    }

    /// Clear all cached statistics
    pub fn clear_stats_cache(&self) {
        let mut cache = self.stats_cache.write().unwrap();
        cache.clear();
    }

    /// Get or load statistics for a table
    ///
    /// If cached stats are stale (older than TTL), they will be refreshed
    /// from the system tables.
    ///
    /// Returns None if:
    /// - No statistics have been collected (ANALYZE not run)
    /// - Statistics have row_count == 0 (empty/invalid stats)
    pub fn get_table_stats(&self, table_name: &str) -> Option<TableStats> {
        let key = table_name.to_lowercase();

        // Check cache first - use read lock then upgrade to write for LRU touch
        {
            let cache = self.stats_cache.read().unwrap();
            if let Some(cached) = cache.get(&key) {
                // Return cached stats if still fresh and valid (row_count > 0)
                if !cached.is_stale() && cached.table_stats.row_count > 0 {
                    let result = cached.table_stats.clone();
                    // We need to touch the entry - drop read lock first
                    drop(cache);
                    // Update last_accessed for LRU
                    if let Ok(mut write_cache) = self.stats_cache.write() {
                        if let Some(entry) = write_cache.get_mut(&key) {
                            entry.touch();
                        }
                    }
                    return Some(result);
                }
                // Stats are stale or invalid, will reload below
            }
        }

        // Load from system tables (will update cache)
        // Only return stats if they have valid row_count > 0
        self.load_stats_from_system_tables(table_name)
            .ok()
            .filter(|stats| stats.row_count > 0)
    }

    /// Get table statistics with fallback to runtime estimation
    ///
    /// If ANALYZE hasn't been run, computes basic statistics from the table.
    /// This ensures the optimizer always has some statistics to work with.
    pub fn get_table_stats_with_fallback(&self, table: &dyn Table) -> TableStats {
        let table_name = table.name();

        // Try to get analyzed stats first
        if let Some(stats) = self.get_table_stats(table_name) {
            if stats.row_count > 0 {
                return stats;
            }
        }

        // Fallback: compute basic stats from table (use hint for O(1))
        let row_count = table.row_count_hint() as u64;
        let avg_row_size = estimated_schema_row_width(table.schema());
        TableStats {
            table_name: table_name.to_string(),
            row_count,
            page_count: row_count
                .saturating_mul(avg_row_size)
                .div_ceil(STORAGE_PAGE_BYTES)
                .max(1),
            avg_row_size,
        }
    }

    /// Get column statistics
    ///
    /// If cached stats are stale (older than TTL), they will be refreshed.
    pub fn get_column_stats(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Option<ColumnStatsCache> {
        let table_key = table_name.to_lowercase();
        let col_key = column_name.to_lowercase();

        // Check cache first
        let should_reload = {
            let cache = self.stats_cache.read().unwrap();
            if let Some(cached) = cache.get(&table_key) {
                if !cached.is_stale() {
                    let result = cached.column_stats.get(&col_key).cloned();
                    // Touch the entry for LRU
                    drop(cache);
                    if let Ok(mut write_cache) = self.stats_cache.write() {
                        if let Some(entry) = write_cache.get_mut(&table_key) {
                            entry.touch();
                        }
                    }
                    return result;
                }
                true // Stale, need to reload
            } else {
                true // Not in cache, need to load
            }
        };

        if should_reload {
            // Load stats which will populate cache
            let _ = self.load_stats_from_system_tables(table_name);
        }

        // Try cache again
        let cache = self.stats_cache.read().unwrap();
        let result = cache
            .get(&table_key)
            .and_then(|c| c.column_stats.get(&col_key).cloned());

        // Touch the entry for LRU if found
        if result.is_some() {
            drop(cache);
            if let Ok(mut write_cache) = self.stats_cache.write() {
                if let Some(entry) = write_cache.get_mut(&table_key) {
                    entry.touch();
                }
            }
        }

        result
    }

    /// Get zone maps for a table (from table, not system tables)
    /// Uses Arc to avoid cloning on high QPS workloads
    pub fn get_zone_maps(&self, table: &dyn Table) -> Option<std::sync::Arc<TableZoneMap>> {
        table.get_zone_maps()
    }

    /// Load statistics from system tables
    fn load_stats_from_system_tables(&self, table_name: &str) -> Result<TableStats> {
        let tx = self.engine.begin_transaction()?;

        // Check if system tables exist
        let tables = tx.list_tables()?;
        let has_table_stats = tables
            .iter()
            .any(|t| t.eq_ignore_ascii_case(SYS_TABLE_STATS));
        let has_column_stats = tables
            .iter()
            .any(|t| t.eq_ignore_ascii_case(SYS_COLUMN_STATS));

        if !has_table_stats {
            // No statistics available - return default
            return Ok(TableStats::default());
        }

        // Read table statistics
        let table_stats = self.read_table_stats(&*tx, table_name)?;

        // Read column statistics if available
        let column_stats = if has_column_stats {
            self.read_column_stats(&*tx, table_name, table_stats.row_count)?
        } else {
            StringMap::new()
        };

        // Cache the stats with current timestamp
        {
            let mut cache = self.stats_cache.write().unwrap();

            // LRU eviction: if cache is full, remove least recently used entries
            if cache.len() >= MAX_STATS_CACHE_SIZE {
                // Find the least recently used entry (oldest last_accessed time)
                if let Some(lru_key) = cache
                    .iter()
                    .min_by_key(|(_, v)| v.last_accessed)
                    .map(|(k, _)| k.clone())
                {
                    cache.remove(&lru_key);
                }
            }

            let now = radixdb_core::time_compat::Instant::now();
            cache.insert(
                table_name.to_lowercase(),
                CachedStats {
                    table_stats: table_stats.clone(),
                    column_stats,
                    cached_at: now,
                    last_accessed: now,
                },
            );
        }

        Ok(table_stats)
    }

    /// Read table statistics from sys_table_stats
    ///
    /// Table schema is:
    /// id (0), table_name (1), row_count (2), page_count (3), avg_row_size (4), last_analyzed (5)
    fn read_table_stats(&self, tx: &dyn Transaction, table_name: &str) -> Result<TableStats> {
        let stats_table = match tx.get_table(SYS_TABLE_STATS) {
            Ok(t) => t,
            Err(_) => return Ok(TableStats::default()),
        };

        // Scan for this table's stats (all columns, no filter)
        let mut result = stats_table.scan(&[], None)?;
        while result.next() {
            let row = result.row();
            // Check if this row is for our table (table_name is column 1)
            if let Some(Value::Text(name)) = row.get(1) {
                if name.eq_ignore_ascii_case(table_name) {
                    let Some(row_count) = decode_nonnegative_stat(row.get(2)) else {
                        return Ok(TableStats::default());
                    };
                    let Some(page_count) = decode_nonnegative_stat(row.get(3)) else {
                        return Ok(TableStats::default());
                    };
                    let Some(avg_row_size) =
                        decode_nonnegative_stat(row.get(4)).filter(|value| *value > 0)
                    else {
                        return Ok(TableStats::default());
                    };
                    if row_count > 0 && page_count == 0 {
                        return Ok(TableStats::default());
                    }
                    return Ok(TableStats {
                        table_name: table_name.to_string(),
                        row_count,
                        page_count,
                        avg_row_size,
                    });
                }
            }
        }

        // No stats found - return default
        Ok(TableStats::default())
    }

    /// Read column statistics from sys_column_stats
    ///
    /// Table schema is:
    /// id (0), table_name (1), column_name (2), null_count (3), distinct_count (4),
    /// min_value (5), max_value (6), avg_width (7), histogram (8)
    fn read_column_stats(
        &self,
        tx: &dyn Transaction,
        table_name: &str,
        table_row_count: u64,
    ) -> Result<StringMap<ColumnStatsCache>> {
        let mut stats = StringMap::new();

        let stats_table = match tx.get_table(SYS_COLUMN_STATS) {
            Ok(t) => t,
            Err(_) => return Ok(stats),
        };

        // Scan for this table's column stats (all columns, no filter)
        let mut result = stats_table.scan(&[], None)?;
        while result.next() {
            let row = result.row();
            // Check if this row is for our table (table_name is column 1)
            if let Some(Value::Text(name)) = row.get(1) {
                if name.eq_ignore_ascii_case(table_name) {
                    if let Some(Value::Text(col_name)) = row.get(2) {
                        let null_count = decode_nonnegative_stat(row.get(3));
                        let distinct_count = decode_nonnegative_stat(row.get(4));
                        let (Some(null_count), Some(distinct_count)) = (null_count, distinct_count)
                        else {
                            continue;
                        };
                        if null_count > table_row_count || distinct_count > table_row_count {
                            continue;
                        }
                        // Parse histogram from JSON string if available
                        let histogram = row
                            .get(8)
                            .and_then(|v| match v {
                                Value::Text(s) => Some(s.to_string()),
                                _ => None,
                            })
                            .and_then(|s| Histogram::from_json(&s));

                        let col_stats = ColumnStatsCache {
                            null_count,
                            distinct_count,
                            min_value: row.get(5).and_then(|value| match value {
                                Value::Text(encoded) => decode_statistics_value(encoded),
                                Value::Null(_) => None,
                                value => Some(value.clone()),
                            }),
                            max_value: row.get(6).and_then(|value| match value {
                                Value::Text(encoded) => decode_statistics_value(encoded),
                                Value::Null(_) => None,
                                value => Some(value.clone()),
                            }),
                            histogram,
                        };
                        stats.insert(col_name.to_lowercase().to_string(), col_stats);
                    }
                }
            }
        }

        Ok(stats)
    }

    /// Estimate selectivity for a predicate
    fn estimate_selectivity(
        &self,
        op: Option<Operator>,
        value: Option<&Value>,
        col_stats: Option<&ColumnStatsCache>,
        table_stats: &TableStats,
    ) -> f64 {
        match (op, value, col_stats) {
            (Some(Operator::Eq), _, Some(stats)) if stats.distinct_count > 0 => {
                // Equality: 1/distinct_count
                1.0 / stats.distinct_count as f64
            }
            (Some(Operator::Eq), _, _) => {
                // Default equality selectivity
                0.1
            }
            (Some(Operator::Ne), _, Some(stats)) if stats.distinct_count > 0 => {
                // Not equal: 1 - 1/distinct_count
                1.0 - (1.0 / stats.distinct_count as f64)
            }
            (Some(Operator::Ne), _, _) => 0.9,
            (
                Some(Operator::Lt | Operator::Lte | Operator::Gt | Operator::Gte),
                Some(val),
                Some(stats),
            ) => {
                // Use histogram for accurate range selectivity if available
                if let Some(ref histogram) = stats.histogram {
                    let hist_op = match op {
                        Some(Operator::Lt) => HistogramOp::LessThan,
                        Some(Operator::Lte) => HistogramOp::LessThanOrEqual,
                        Some(Operator::Gt) => HistogramOp::GreaterThan,
                        Some(Operator::Gte) => HistogramOp::GreaterThanOrEqual,
                        _ => HistogramOp::Equal,
                    };
                    return histogram.estimate_selectivity(val, hist_op);
                }

                // Fall back to min/max based heuristic
                if let (Some(min), Some(max)) = (&stats.min_value, &stats.max_value) {
                    if min < max {
                        // Estimate position in range using linear interpolation
                        let position = Self::estimate_value_position(val, min, max);
                        match op {
                            Some(Operator::Lt | Operator::Lte) => {
                                if val <= min {
                                    0.01
                                } else if val >= max {
                                    0.99
                                } else {
                                    position.clamp(0.01, 0.99)
                                }
                            }
                            Some(Operator::Gt | Operator::Gte) => {
                                if val >= max {
                                    0.01
                                } else if val <= min {
                                    0.99
                                } else {
                                    (1.0 - position).clamp(0.01, 0.99)
                                }
                            }
                            _ => 0.33,
                        }
                    } else {
                        0.33
                    }
                } else {
                    0.33
                }
            }
            (Some(Operator::Lt | Operator::Lte | Operator::Gt | Operator::Gte), _, _) => {
                // Default range selectivity
                0.33
            }
            (Some(Operator::Like), _, _) => {
                // LIKE selectivity depends on pattern
                0.25
            }
            (Some(Operator::In), _, _) => {
                // IN selectivity
                0.2
            }
            (Some(Operator::NotIn), _, _) => {
                // NOT IN selectivity
                0.8
            }
            (Some(Operator::IsNull), _, Some(stats)) if table_stats.row_count > 0 => {
                stats.null_count as f64 / table_stats.row_count as f64
            }
            (Some(Operator::IsNotNull), _, Some(stats)) if table_stats.row_count > 0 => {
                1.0 - (stats.null_count as f64 / table_stats.row_count as f64)
            }
            _ => 1.0, // No selectivity reduction
        }
    }

    /// Estimate the position of a value within a range (0.0 to 1.0)
    /// Used for linear interpolation when histogram is not available
    fn estimate_value_position(value: &Value, min: &Value, max: &Value) -> f64 {
        match (min, max, value) {
            (Value::Integer(lo), Value::Integer(hi), Value::Integer(v)) => {
                if hi == lo {
                    0.5
                } else {
                    ((*v - *lo) as f64 / (*hi - *lo) as f64).clamp(0.0, 1.0)
                }
            }
            (Value::Float(lo), Value::Float(hi), Value::Float(v)) => {
                if (hi - lo).abs() < f64::EPSILON {
                    0.5
                } else {
                    ((v - lo) / (hi - lo)).clamp(0.0, 1.0)
                }
            }
            // Handle mixed integer/float comparisons
            (Value::Integer(lo), Value::Integer(hi), Value::Float(v)) => {
                let lo_f = *lo as f64;
                let hi_f = *hi as f64;
                if (hi_f - lo_f).abs() < f64::EPSILON {
                    0.5
                } else {
                    ((v - lo_f) / (hi_f - lo_f)).clamp(0.0, 1.0)
                }
            }
            (Value::Float(lo), Value::Float(hi), Value::Integer(v)) => {
                let v_f = *v as f64;
                if (hi - lo).abs() < f64::EPSILON {
                    0.5
                } else {
                    ((v_f - lo) / (hi - lo)).clamp(0.0, 1.0)
                }
            }
            _ => 0.5, // Default for non-comparable types
        }
    }

    /// Check if zone maps indicate that no rows can possibly match the expression
    ///
    /// Returns true if the entire scan can be skipped (zone maps show no match possible).
    /// Returns false if:
    /// - Zone maps are not available
    /// - Some segments might match
    /// - Expression cannot be evaluated against zone maps
    ///
    /// This enables early exit optimization for range queries on ordered data.
    pub fn can_prune_entire_scan(
        &self,
        table: &dyn Table,
        expr: &dyn radixdb_storage::expression::Expression,
    ) -> bool {
        let zone_maps = match table.get_zone_maps() {
            Some(zm) => zm,
            None => return false, // No zone maps, cannot prune
        };

        // Check if zone maps are stale
        if zone_maps.is_stale() {
            return false; // Stale zone maps, don't trust them
        }

        // Extract all comparisons from the expression
        let comparisons = expr.collect_comparisons();
        if comparisons.is_empty() {
            return false; // No simple comparisons to check
        }

        // For AND expressions: ALL comparisons must show no possible match
        // For a single comparison: check if any segment could match
        for (column, op, value) in comparisons {
            if let Some(segments) = zone_maps.get_segments_to_scan(column, op, value) {
                if !segments.is_empty() {
                    return false; // At least one segment might match
                }
            } else {
                return false; // Cannot evaluate this comparison
            }
        }

        // All comparisons indicate no segments match - can skip entire scan
        true
    }

    /// Get overall health of statistics for a table
    pub fn stats_health(&self, table_name: &str) -> StatsHealth {
        let key = table_name.to_lowercase();
        if let Some(cached) = self.stats_cache.read().unwrap().get(&key) {
            return Self::classify_stats_health(cached.table_stats.row_count, cached.is_stale());
        }

        let table_stats = self.get_table_stats(table_name);

        match table_stats {
            Some(stats) => Self::classify_stats_health(stats.row_count, false),
            None => StatsHealth::Missing,
        }
    }

    fn classify_stats_health(row_count: u64, stale: bool) -> StatsHealth {
        if row_count == 0 {
            StatsHealth::Missing
        } else if stale {
            StatsHealth::Stale
        } else {
            StatsHealth::Current
        }
    }

    // =========================================================================
    // Cardinality Estimation for Scans
    // =========================================================================

    /// Estimate the number of rows that will be returned by a scan with a predicate
    ///
    /// This method uses table statistics and column statistics to estimate
    /// selectivity of predicates. It also applies cardinality feedback corrections
    /// if available from previous query executions.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table being scanned
    /// * `predicate` - Optional WHERE clause predicate
    ///
    /// # Returns
    /// Estimated number of rows, or None if stats are unavailable
    pub fn estimate_scan_rows(
        &self,
        table_name: &str,
        predicate: Option<&Expression>,
    ) -> Option<u64> {
        let table_stats = self.get_table_stats(table_name)?;
        let base_rows = table_stats.row_count;

        if base_rows == 0 {
            return Some(0);
        }

        let predicate = match predicate {
            Some(p) => p,
            None => return Some(base_rows), // Full table scan
        };

        // Estimate selectivity from the predicate
        let selectivity = self.estimate_predicate_selectivity(table_name, predicate, &table_stats);
        let estimated = ((base_rows as f64) * selectivity).max(1.0) as u64;

        // Apply feedback correction
        Some(self.estimate_with_feedback(table_name, Some(predicate), estimated))
    }

    /// Estimate selectivity of a predicate expression
    fn estimate_predicate_selectivity(
        &self,
        table_name: &str,
        expr: &Expression,
        table_stats: &TableStats,
    ) -> f64 {
        use radixdb_sql::ast::{InfixOperator, PrefixOperator};

        match expr {
            // Infix expressions (a AND b, a OR b, a = b, a IS NULL, etc.)
            Expression::Infix(infix) => {
                match infix.op_type {
                    // AND: multiply selectivities (assuming independence)
                    InfixOperator::And => {
                        let left_sel = self.estimate_predicate_selectivity(
                            table_name,
                            &infix.left,
                            table_stats,
                        );
                        let right_sel = self.estimate_predicate_selectivity(
                            table_name,
                            &infix.right,
                            table_stats,
                        );
                        left_sel * right_sel
                    }
                    // OR: use inclusion-exclusion principle
                    InfixOperator::Or => {
                        let left_sel = self.estimate_predicate_selectivity(
                            table_name,
                            &infix.left,
                            table_stats,
                        );
                        let right_sel = self.estimate_predicate_selectivity(
                            table_name,
                            &infix.right,
                            table_stats,
                        );
                        // P(A or B) = P(A) + P(B) - P(A and B)
                        (left_sel + right_sel - left_sel * right_sel).min(1.0)
                    }
                    // IS NULL
                    InfixOperator::Is => {
                        // Check if right side is NULL
                        if matches!(infix.right.as_ref(), Expression::NullLiteral(_)) {
                            let col_name = self.extract_column_name(&infix.left);
                            let col_stats =
                                col_name.and_then(|name| self.get_column_stats(table_name, &name));
                            self.estimate_selectivity(
                                Some(Operator::IsNull),
                                None,
                                col_stats.as_ref(),
                                table_stats,
                            )
                        } else {
                            0.5
                        }
                    }
                    // IS NOT NULL
                    InfixOperator::IsNot => {
                        if matches!(infix.right.as_ref(), Expression::NullLiteral(_)) {
                            let col_name = self.extract_column_name(&infix.left);
                            let col_stats =
                                col_name.and_then(|name| self.get_column_stats(table_name, &name));
                            self.estimate_selectivity(
                                Some(Operator::IsNotNull),
                                None,
                                col_stats.as_ref(),
                                table_stats,
                            )
                        } else {
                            0.5
                        }
                    }
                    // Comparison operators
                    InfixOperator::Equal => {
                        let col_name = self
                            .extract_column_name(&infix.left)
                            .or_else(|| self.extract_column_name(&infix.right));
                        let value = self
                            .extract_value(&infix.right)
                            .or_else(|| self.extract_value(&infix.left));
                        let col_stats =
                            col_name.and_then(|name| self.get_column_stats(table_name, &name));
                        self.estimate_selectivity(
                            Some(Operator::Eq),
                            value.as_ref(),
                            col_stats.as_ref(),
                            table_stats,
                        )
                    }
                    InfixOperator::NotEqual => {
                        let col_name = self
                            .extract_column_name(&infix.left)
                            .or_else(|| self.extract_column_name(&infix.right));
                        let value = self
                            .extract_value(&infix.right)
                            .or_else(|| self.extract_value(&infix.left));
                        let col_stats =
                            col_name.and_then(|name| self.get_column_stats(table_name, &name));
                        self.estimate_selectivity(
                            Some(Operator::Ne),
                            value.as_ref(),
                            col_stats.as_ref(),
                            table_stats,
                        )
                    }
                    InfixOperator::LessThan => {
                        let col_name = self.extract_column_name(&infix.left);
                        let value = self.extract_value(&infix.right);
                        let col_stats =
                            col_name.and_then(|name| self.get_column_stats(table_name, &name));
                        self.estimate_selectivity(
                            Some(Operator::Lt),
                            value.as_ref(),
                            col_stats.as_ref(),
                            table_stats,
                        )
                    }
                    InfixOperator::LessEqual => {
                        let col_name = self.extract_column_name(&infix.left);
                        let value = self.extract_value(&infix.right);
                        let col_stats =
                            col_name.and_then(|name| self.get_column_stats(table_name, &name));
                        self.estimate_selectivity(
                            Some(Operator::Lte),
                            value.as_ref(),
                            col_stats.as_ref(),
                            table_stats,
                        )
                    }
                    InfixOperator::GreaterThan => {
                        let col_name = self.extract_column_name(&infix.left);
                        let value = self.extract_value(&infix.right);
                        let col_stats =
                            col_name.and_then(|name| self.get_column_stats(table_name, &name));
                        self.estimate_selectivity(
                            Some(Operator::Gt),
                            value.as_ref(),
                            col_stats.as_ref(),
                            table_stats,
                        )
                    }
                    InfixOperator::GreaterEqual => {
                        let col_name = self.extract_column_name(&infix.left);
                        let value = self.extract_value(&infix.right);
                        let col_stats =
                            col_name.and_then(|name| self.get_column_stats(table_name, &name));
                        self.estimate_selectivity(
                            Some(Operator::Gte),
                            value.as_ref(),
                            col_stats.as_ref(),
                            table_stats,
                        )
                    }
                    InfixOperator::Like | InfixOperator::ILike => {
                        let pattern_str = self.extract_string_value(&infix.right);
                        match pattern_str {
                            Some(p) if !p.starts_with('%') => 0.1, // Prefix match is more selective
                            Some(_) => 0.25,                       // Suffix or contains
                            None => 0.25,
                        }
                    }
                    InfixOperator::NotLike | InfixOperator::NotILike => {
                        let pattern_str = self.extract_string_value(&infix.right);
                        let like_sel = match pattern_str {
                            Some(p) if !p.starts_with('%') => 0.1,
                            Some(_) => 0.25,
                            None => 0.25,
                        };
                        1.0 - like_sel
                    }
                    // Default for other operators
                    _ => 0.5,
                }
            }
            // IN expression
            Expression::In(in_expr) => {
                let col_name = self.extract_column_name(&in_expr.left);
                let col_stats = col_name.and_then(|name| self.get_column_stats(table_name, &name));

                // Get list size from the right side
                let list_size = match in_expr.right.as_ref() {
                    Expression::List(list) => list.elements.len() as f64,
                    Expression::ExpressionList(list) => list.expressions.len() as f64,
                    _ => 5.0, // Default assumption
                };
                let distinct = col_stats
                    .map(|s| s.distinct_count.max(1) as f64)
                    .unwrap_or(100.0);
                let in_selectivity = (list_size / distinct).min(1.0);

                if in_expr.not {
                    1.0 - in_selectivity
                } else {
                    in_selectivity
                }
            }
            // BETWEEN expression
            Expression::Between(between) => {
                let col_name = self.extract_column_name(&between.expr);
                let col_stats = col_name.and_then(|name| self.get_column_stats(table_name, &name));
                let low_val = self.extract_value(&between.lower);
                let high_val = self.extract_value(&between.upper);

                // Estimate as (high - low) / (max - min)
                let range_sel = if let (Some(ref stats), Some(low_v), Some(high_v)) =
                    (&col_stats, low_val, high_val)
                {
                    if let (Some(min), Some(max)) = (&stats.min_value, &stats.max_value) {
                        let low_pos = Self::estimate_value_position(&low_v, min, max);
                        let high_pos = Self::estimate_value_position(&high_v, min, max);
                        (high_pos - low_pos).abs().clamp(0.01, 0.99)
                    } else {
                        0.25 // Default BETWEEN selectivity
                    }
                } else {
                    0.25
                };

                if between.not {
                    1.0 - range_sel
                } else {
                    range_sel
                }
            }
            // LIKE expression (standalone)
            Expression::Like(like_expr) => {
                let is_negated = like_expr.operator.to_uppercase().contains("NOT");
                let pattern_str = self.extract_string_value(&like_expr.pattern);
                let base_sel = match pattern_str {
                    Some(p) if !p.starts_with('%') => 0.1,
                    Some(_) => 0.25,
                    None => 0.25,
                };
                if is_negated {
                    1.0 - base_sel
                } else {
                    base_sel
                }
            }
            // Prefix expressions (NOT x, -x)
            Expression::Prefix(prefix) => match prefix.op_type {
                PrefixOperator::Not => {
                    1.0 - self.estimate_predicate_selectivity(
                        table_name,
                        &prefix.right,
                        table_stats,
                    )
                }
                _ => 0.5,
            },
            // Unknown expressions - conservative estimate
            _ => 0.5,
        }
    }

    /// Extract column name from an expression
    fn extract_column_name(&self, expr: &Expression) -> Option<String> {
        match expr {
            Expression::Identifier(id) => Some(id.value_lower.to_string()),
            Expression::QualifiedIdentifier(qid) => Some(qid.name.value_lower.to_string()),
            _ => None,
        }
    }

    /// Extract a Value from a literal expression
    fn extract_value(&self, expr: &Expression) -> Option<Value> {
        match expr {
            Expression::IntegerLiteral(lit) => Some(Value::Integer(lit.value)),
            Expression::FloatLiteral(lit) => Some(Value::Float(lit.value)),
            Expression::StringLiteral(lit) => Some(Value::Text(lit.value.to_string().into())),
            Expression::BooleanLiteral(lit) => Some(Value::Boolean(lit.value)),
            Expression::NullLiteral(_) => None, // NULL doesn't have a comparable value
            _ => None,
        }
    }

    /// Extract string value from an expression
    fn extract_string_value(&self, expr: &Expression) -> Option<String> {
        match expr {
            Expression::StringLiteral(lit) => Some(lit.value.to_string()),
            _ => None,
        }
    }

    // =========================================================================
    // Cardinality Feedback Integration
    // =========================================================================

    /// Estimate row count with cardinality feedback correction
    ///
    /// This method combines statistics-based estimation with learned corrections
    /// from previous query executions. When similar predicates have been executed
    /// before, the correction factor improves accuracy.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table being scanned
    /// * `predicate` - The WHERE clause predicate (for fingerprinting)
    /// * `base_estimate` - Initial row count estimate from statistics
    ///
    /// # Returns
    /// Corrected row count estimate
    pub fn estimate_with_feedback(
        &self,
        table_name: &str,
        predicate: Option<&Expression>,
        base_estimate: u64,
    ) -> u64 {
        let predicate = match predicate {
            Some(p) => p,
            None => return base_estimate, // No predicate, no feedback
        };

        // Get fingerprint for this predicate pattern
        let fingerprint = fingerprint_predicate(table_name, predicate);

        // Look up and apply any learned correction
        self.feedback_cache
            .apply_correction(table_name, fingerprint, base_estimate)
    }

    /// Record cardinality feedback after query execution
    ///
    /// This method stores the difference between estimated and actual row counts,
    /// enabling future queries with similar predicates to benefit from the correction.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table that was scanned
    /// * `predicate` - The WHERE clause predicate (for fingerprinting)
    /// * `column_name` - Optional column name for more specific feedback
    /// * `estimated_rows` - Row count estimate used during planning
    /// * `actual_rows` - Actual row count observed during execution
    pub fn record_feedback(
        &self,
        table_name: &str,
        predicate: &Expression,
        column_name: Option<String>,
        estimated_rows: u64,
        actual_rows: u64,
    ) {
        // Only record if there's meaningful difference (avoid noise from perfect estimates)
        if estimated_rows == actual_rows {
            return;
        }

        // Only record if actual rows are significant (avoid learning from tiny results)
        if actual_rows < 10 && estimated_rows < 10 {
            return;
        }

        let fingerprint = fingerprint_predicate(table_name, predicate);
        self.feedback_cache.record_feedback(
            table_name,
            fingerprint,
            column_name,
            estimated_rows,
            actual_rows,
        );
    }

    /// Get the correction factor for a predicate (for debugging/EXPLAIN)
    ///
    /// Returns 1.0 if no feedback is available or if feedback is not yet reliable.
    pub fn get_feedback_correction(&self, table_name: &str, predicate: &Expression) -> f64 {
        let fingerprint = fingerprint_predicate(table_name, predicate);
        self.feedback_cache.get_correction(table_name, fingerprint)
    }
}

/// Health status of table statistics
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StatsHealth {
    /// Statistics are current (recently analyzed)
    Current,
    /// Statistics exist but may be stale
    Stale,
    /// No statistics available
    Missing,
}

pub use crate::join_executor::{RuntimeJoinAlgorithm, RuntimeJoinDecision};

/// Cost input for choosing between a bounded indexed lookup and the general
/// scan/hash path for one equality JOIN edge.
///
/// Costs are expressed as comparable byte-work units.  This is deliberately a
/// physical input contract: the caller supplies the visible inner cardinality,
/// storage pages, measured/schema-derived widths and the number of outer rows
/// that can reach this edge.  No fixed "bytes per row" guess is hidden here.
#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub struct IndexedJoinCostInput {
    pub outer_rows: u64,
    pub inner_rows: u64,
    pub inner_pages: u64,
    pub inner_distinct_keys: Option<u64>,
    pub inner_row_width: u64,
    pub projected_inner_width: u64,
    pub lookup_unique: bool,
    pub limit: Option<u64>,
}

/// Costed physical access decision for one indexed JOIN edge.
#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct IndexedJoinCostDecision {
    pub use_index_lookup: bool,
    pub lookup_cost: u64,
    pub scan_hash_cost: u64,
    pub expected_matches: u64,
    pub explanation: String,
}

impl QueryPlanner {
    /// Compare a deduplicated batch lookup with materializing the complete
    /// inner relation and building/probing the general equality JOIN.
    ///
    /// The model intentionally charges one metadata page plus one random page
    /// per distinct outer key.  Batch lookup can therefore lose for tiny inner
    /// relations, while a selective root wins as the unrelated inner relation
    /// grows.  Non-unique fan-out comes from ANALYZE distinctness when present;
    /// without it the square-root estimate is conservative and deterministic.
    #[doc(hidden)]
    pub fn plan_indexed_join_access(&self, input: IndexedJoinCostInput) -> IndexedJoinCostDecision {
        let outer_rows = match input.limit {
            Some(limit) if input.lookup_unique => input.outer_rows.min(limit.saturating_mul(2)),
            _ => input.outer_rows,
        };
        let distinct_inner = if input.lookup_unique {
            input.inner_rows
        } else {
            input
                .inner_distinct_keys
                .filter(|count| *count > 0)
                .unwrap_or_else(|| input.inner_rows.max(1).isqrt())
                .min(input.inner_rows.max(1))
        };
        let fanout = if input.lookup_unique || input.inner_rows == 0 {
            u64::from(input.inner_rows > 0)
        } else {
            input.inner_rows.div_ceil(distinct_inner.max(1)).max(1)
        };
        let distinct_probes = outer_rows.min(distinct_inner.max(1));
        let expected_matches = outer_rows.saturating_mul(fanout);

        let lookup_cost = STORAGE_PAGE_BYTES
            .saturating_add(distinct_probes.saturating_mul(STORAGE_PAGE_BYTES))
            .saturating_add(expected_matches.saturating_mul(input.inner_row_width))
            .saturating_add(expected_matches.saturating_mul(input.projected_inner_width));
        let scan_hash_cost = input
            .inner_pages
            .saturating_mul(STORAGE_PAGE_BYTES)
            .saturating_add(input.inner_rows.saturating_mul(input.projected_inner_width))
            .saturating_add(outer_rows.saturating_mul(input.projected_inner_width.max(1)));

        // An empty outer input needs no inner scan.  Otherwise ties stay on the
        // general path because it has less index/batch setup work.
        let use_index_lookup = outer_rows == 0 || lookup_cost < scan_hash_cost;
        let selected = if use_index_lookup {
            "batch index lookup"
        } else {
            "scan/hash"
        };
        IndexedJoinCostDecision {
            use_index_lookup,
            lookup_cost,
            scan_hash_cost,
            expected_matches,
            explanation: format!(
                "{selected}: lookup_cost={lookup_cost}, scan_hash_cost={scan_hash_cost}, outer_rows={outer_rows}, inner_rows={}, distinct_keys={distinct_inner}, expected_matches={expected_matches}",
                input.inner_rows
            ),
        }
    }

    /// Make a runtime join algorithm decision based on actual row counts
    ///
    /// This is called during execution with the actual materialized row counts,
    /// enabling adaptive decisions that account for runtime conditions.
    /// Also consults the EdgeAwarePlanner for workload-learned hints.
    ///
    /// # Arguments
    /// * `left_rows` - Actual row count from left side
    /// * `right_rows` - Actual row count from right side
    /// * `has_equality_keys` - Whether join has equality conditions (a.x = b.x)
    ///
    /// # Returns
    /// Decision on which algorithm to use and whether to swap sides
    pub fn plan_runtime_join(
        &self,
        left_rows: usize,
        right_rows: usize,
        has_equality_keys: bool,
    ) -> RuntimeJoinDecision {
        self.plan_runtime_join_with_sort_info(
            left_rows,
            right_rows,
            has_equality_keys,
            false,
            false,
        )
    }

    /// Make a runtime join algorithm decision with sort information
    ///
    /// Extended version that also considers whether inputs are pre-sorted,
    /// which enables merge join optimization.
    ///
    /// # Arguments
    /// * `left_rows` - Actual row count from left side
    /// * `right_rows` - Actual row count from right side
    /// * `has_equality_keys` - Whether join has equality conditions (a.x = b.x)
    /// * `left_sorted` - Whether left input is sorted on join keys
    /// * `right_sorted` - Whether right input is sorted on join keys
    pub fn plan_runtime_join_with_sort_info(
        &self,
        left_rows: usize,
        right_rows: usize,
        has_equality_keys: bool,
        left_sorted: bool,
        right_sorted: bool,
    ) -> RuntimeJoinDecision {
        // For small tables, nested loop is faster (no hash table overhead)
        // PostgreSQL uses similar thresholds
        const NESTED_LOOP_MAX: usize = 200;
        const HASH_JOIN_MIN_BENEFIT: usize = 50;
        const ESTIMATED_BYTES_PER_ROW: u64 = 100;
        // Merge join is preferred over hash when both inputs are sorted
        // and tables are large enough to benefit from avoiding hash overhead
        const MERGE_JOIN_MIN_ROWS: usize = 500;

        let total_rows = left_rows + right_rows;
        let product = left_rows.saturating_mul(right_rows);

        // Case 1: No equality keys - must use nested loop
        if !has_equality_keys {
            return RuntimeJoinDecision {
                algorithm: RuntimeJoinAlgorithm::NestedLoop,
                swap_sides: false,
                explanation: "Nested loop: no equality join keys".to_string(),
            };
        }

        // Consult EdgeAwarePlanner for workload-learned hints
        let edge_planner = EdgeAwarePlanner::from_global();
        let (build_rows_u64, probe_rows_u64) = if right_rows < left_rows {
            (right_rows as u64, left_rows as u64)
        } else {
            (left_rows as u64, right_rows as u64)
        };

        let edge_recommendation = edge_planner.recommend_join_for_edge(
            build_rows_u64,
            probe_rows_u64,
            ESTIMATED_BYTES_PER_ROW,
        );

        // Check if edge constraints force a specific algorithm
        match edge_recommendation {
            EdgeJoinRecommendation::ForceNestedLoop { reason } => {
                return RuntimeJoinDecision {
                    algorithm: RuntimeJoinAlgorithm::NestedLoop,
                    swap_sides: false,
                    explanation: format!("Nested loop (edge constraint): {}", reason),
                };
            }
            EdgeJoinRecommendation::PreferNestedLoop { .. } => {
                // A learned interactive-workload preference must not override
                // the physical equality-join cost.  In particular, long
                // selective chains frequently keep both sides below a few
                // hundred rows; choosing nested loop at every edge turns that
                // useful selectivity into repeated O(N*M) JoinFilter work.
                // The cardinality cost below remains authoritative; the hint
                // may influence future first-row/index alternatives, but not
                // replace an equality operator with repeated expression-VM
                // evaluation.
            }
            EdgeJoinRecommendation::PreferHashJoin { .. } => {
                // Edge mode prefers hash join - skip nested loop checks for medium tables
                // But still consider merge join if both inputs are sorted
                if total_rows > NESTED_LOOP_MAX && !(left_sorted && right_sorted) {
                    let swap = right_rows < left_rows;
                    let (build, probe) = if swap {
                        (right_rows, left_rows)
                    } else {
                        (left_rows, right_rows)
                    };
                    return RuntimeJoinDecision {
                        algorithm: RuntimeJoinAlgorithm::HashJoin,
                        swap_sides: swap,
                        explanation: format!(
                            "Hash join (batch workload): build {} rows, probe {} rows",
                            build, probe
                        ),
                    };
                }
            }
            EdgeJoinRecommendation::UseDefault => {
                // Fall through to standard cost-based decision
            }
        }

        // Case 2: An empty input never benefits from building hash state. This
        // must precede tiny/tiny because zero is also "tiny".
        if left_rows == 0 || right_rows == 0 {
            return RuntimeJoinDecision {
                algorithm: RuntimeJoinAlgorithm::NestedLoop,
                swap_sides: false,
                explanation: "Nested loop: one side empty".to_string(),
            };
        }

        // Case 3: Both sides tiny - use hash join (still faster than nested loop)
        // RATIONALE: Even for small datasets, hash join with equality keys is O(N+M)
        // while nested loop is O(N*M) with JoinFilter VM evaluation per comparison.
        // The hash table overhead is minimal for small tables, and we avoid expensive
        // per-comparison expression evaluation.
        if left_rows <= NESTED_LOOP_MAX && right_rows <= NESTED_LOOP_MAX {
            let swap = right_rows < left_rows;
            return RuntimeJoinDecision {
                algorithm: RuntimeJoinAlgorithm::HashJoin,
                swap_sides: swap,
                explanation: format!(
                    "Hash join: small tables ({} + {} = {} ops vs {} comparisons)",
                    left_rows, right_rows, total_rows, product
                ),
            };
        }

        // Case 4: Merge join when both inputs are already sorted
        // Merge join is O(N + M) like hash join, but avoids hash table overhead
        // It's optimal when both inputs are pre-sorted on join keys
        if left_sorted && right_sorted && total_rows >= MERGE_JOIN_MIN_ROWS {
            return RuntimeJoinDecision {
                algorithm: RuntimeJoinAlgorithm::MergeJoin,
                swap_sides: false,
                explanation: format!(
                    "Merge join: both inputs sorted ({} + {} rows)",
                    left_rows, right_rows
                ),
            };
        }

        // Case 5: Hash join cost analysis
        // Hash join is O(N + M) vs nested loop O(N * M)
        // But hash join has setup cost, so only use when beneficial
        let hash_cost = total_rows as f64;
        let nested_cost = product as f64;

        if nested_cost < hash_cost + HASH_JOIN_MIN_BENEFIT as f64 {
            // Nested loop is cheaper even accounting for setup
            return RuntimeJoinDecision {
                algorithm: RuntimeJoinAlgorithm::NestedLoop,
                swap_sides: false,
                explanation: format!(
                    "Nested loop: cheaper than hash ({} < {} + setup)",
                    product, total_rows
                ),
            };
        }

        // Case 6: Use hash join with smaller side as build side
        let swap = right_rows < left_rows;
        let (build_rows, probe_rows) = if swap {
            (right_rows, left_rows)
        } else {
            (left_rows, right_rows)
        };

        RuntimeJoinDecision {
            algorithm: RuntimeJoinAlgorithm::HashJoin,
            swap_sides: swap,
            explanation: format!(
                "Hash join: build {} rows, probe {} rows (swap={})",
                build_rows, probe_rows, swap
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_core::SchemaBuilder;

    #[test]
    fn fallback_width_is_schema_derived_instead_of_fixed() {
        let narrow = SchemaBuilder::new("narrow")
            .add_primary_key("id", DataType::Integer)
            .add("active", DataType::Boolean)
            .build();
        let wide = SchemaBuilder::new("wide")
            .add_primary_key("id", DataType::Integer)
            .add_nullable("payload", DataType::Text)
            .add("uuid", DataType::Uuid)
            .build();

        assert_eq!(estimated_schema_row_width(&narrow), 9);
        assert_eq!(estimated_schema_row_width(&wide), 57);
        assert_ne!(estimated_schema_row_width(&wide), 100);
    }

    #[test]
    fn v2_r5_catalog_statistics_reject_negative_values() {
        assert_eq!(decode_nonnegative_stat(Some(&Value::Integer(0))), Some(0));
        assert_eq!(decode_nonnegative_stat(Some(&Value::Integer(42))), Some(42));
        assert_eq!(decode_nonnegative_stat(Some(&Value::Integer(-1))), None);
        assert_eq!(
            decode_nonnegative_stat(Some(&Value::Text("1".into()))),
            None
        );
    }

    #[test]
    fn r8_l01_batch_h_empty_join_bypasses_tiny_hash_plan() {
        let planner = QueryPlanner::new(Arc::new(MVCCEngine::in_memory()));
        for (left, right) in [(0, 0), (0, 17), (17, 0)] {
            let decision = planner.plan_runtime_join(left, right, true);
            assert!(decision.use_nested_loop(), "{left} x {right}: {decision:?}");
        }
    }

    #[test]
    fn test_selectivity_estimation() {
        let planner = QueryPlanner::new(Arc::new(MVCCEngine::in_memory()));

        // Test equality selectivity with distinct count
        let col_stats = ColumnStatsCache {
            null_count: 0,
            distinct_count: 100,
            min_value: Some(Value::Integer(1)),
            max_value: Some(Value::Integer(100)),
            histogram: None,
        };
        let table_stats = TableStats::default();

        let sel = planner.estimate_selectivity(
            Some(Operator::Eq),
            Some(&Value::Integer(50)),
            Some(&col_stats),
            &table_stats,
        );
        assert!((sel - 0.01).abs() < 0.001); // 1/100 = 0.01

        // Test no column stats
        let sel_no_stats = planner.estimate_selectivity(
            Some(Operator::Eq),
            Some(&Value::Integer(50)),
            None,
            &table_stats,
        );
        assert!((sel_no_stats - 0.1).abs() < 0.001); // Default
    }

    #[test]
    fn test_estimate_value_position_integers() {
        // Value in middle of range
        let pos = QueryPlanner::estimate_value_position(
            &Value::Integer(50),
            &Value::Integer(0),
            &Value::Integer(100),
        );
        assert!((pos - 0.5).abs() < 0.001);

        // Value at start
        let pos = QueryPlanner::estimate_value_position(
            &Value::Integer(0),
            &Value::Integer(0),
            &Value::Integer(100),
        );
        assert!(pos.abs() < 0.001);

        // Value at end
        let pos = QueryPlanner::estimate_value_position(
            &Value::Integer(100),
            &Value::Integer(0),
            &Value::Integer(100),
        );
        assert!((pos - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_estimate_value_position_floats() {
        let pos = QueryPlanner::estimate_value_position(
            &Value::Float(0.75),
            &Value::Float(0.0),
            &Value::Float(1.0),
        );
        assert!((pos - 0.75).abs() < 0.001);
    }

    #[test]
    fn test_estimate_value_position_equal_bounds() {
        // Equal bounds should return 0.5
        let pos = QueryPlanner::estimate_value_position(
            &Value::Integer(50),
            &Value::Integer(50),
            &Value::Integer(50),
        );
        assert!((pos - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_estimate_value_position_clamping() {
        // Value below range should clamp to 0
        let pos = QueryPlanner::estimate_value_position(
            &Value::Integer(-10),
            &Value::Integer(0),
            &Value::Integer(100),
        );
        assert!(pos.abs() < 0.001);

        // Value above range should clamp to 1
        let pos = QueryPlanner::estimate_value_position(
            &Value::Integer(200),
            &Value::Integer(0),
            &Value::Integer(100),
        );
        assert!((pos - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_runtime_join_decision_hash_join() {
        let planner = QueryPlanner::new(Arc::new(MVCCEngine::in_memory()));

        // For equality joins with reasonable sizes, hash join should be selected
        let decision = planner.plan_runtime_join(1000, 1000, true);
        assert!(decision.use_hash_join());
        assert!(!decision.use_merge_join());
        assert!(!decision.use_nested_loop());
    }

    #[test]
    fn test_runtime_join_decision_nested_loop() {
        let planner = QueryPlanner::new(Arc::new(MVCCEngine::in_memory()));

        // Non-equality joins should use nested loop
        let decision = planner.plan_runtime_join(100, 100, false);
        assert!(decision.use_nested_loop());
        assert!(!decision.use_hash_join());
        assert!(!decision.use_merge_join());
    }

    #[test]
    fn test_runtime_join_decision_small_tables() {
        let planner = QueryPlanner::new(Arc::new(MVCCEngine::in_memory()));

        // Very small tables might still use hash join for equality
        let decision = planner.plan_runtime_join(10, 10, true);
        // Small tables with equality should still use efficient algorithm
        assert!(decision.use_hash_join() || decision.use_nested_loop());
    }

    #[test]
    fn test_runtime_join_decision_merge_join() {
        let planner = QueryPlanner::new(Arc::new(MVCCEngine::in_memory()));

        // When both sides are sorted, merge join might be preferred
        let decision = planner.plan_runtime_join_with_sort_info(10000, 10000, true, true, true);
        assert!(decision.use_merge_join() || decision.use_hash_join());
    }

    #[test]
    fn test_selectivity_range_operators() {
        let planner = QueryPlanner::new(Arc::new(MVCCEngine::in_memory()));

        let col_stats = ColumnStatsCache {
            null_count: 0,
            distinct_count: 100,
            min_value: Some(Value::Integer(1)),
            max_value: Some(Value::Integer(100)),
            histogram: None,
        };
        let table_stats = TableStats::default();

        // Test Greater Than - should be about 50% for value in middle
        let sel = planner.estimate_selectivity(
            Some(Operator::Gt),
            Some(&Value::Integer(50)),
            Some(&col_stats),
            &table_stats,
        );
        assert!(sel > 0.0 && sel < 1.0);

        // Test Less Than
        let sel = planner.estimate_selectivity(
            Some(Operator::Lt),
            Some(&Value::Integer(50)),
            Some(&col_stats),
            &table_stats,
        );
        assert!(sel > 0.0 && sel < 1.0);
    }

    #[test]
    fn test_selectivity_no_operator() {
        let planner = QueryPlanner::new(Arc::new(MVCCEngine::in_memory()));
        let table_stats = TableStats::default();

        // No operator should return default selectivity
        let sel = planner.estimate_selectivity(None, Some(&Value::Integer(50)), None, &table_stats);
        assert!((sel - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_stats_health_missing() {
        let planner = QueryPlanner::new(Arc::new(MVCCEngine::in_memory()));

        // Non-existent table should return Missing
        let health = planner.stats_health("non_existent_table");
        assert!(matches!(health, StatsHealth::Missing));
    }

    #[test]
    fn r5_l04_batch_g_stats_health_classifies_stale_cache_entries() {
        assert_eq!(
            QueryPlanner::classify_stats_health(42, true),
            StatsHealth::Stale
        );
        assert_eq!(
            QueryPlanner::classify_stats_health(42, false),
            StatsHealth::Current
        );
        assert_eq!(
            QueryPlanner::classify_stats_health(0, true),
            StatsHealth::Missing
        );
    }

    #[test]
    fn test_estimate_value_position_mixed_types() {
        // Integer min/max with float value
        let pos = QueryPlanner::estimate_value_position(
            &Value::Float(50.5),
            &Value::Integer(0),
            &Value::Integer(100),
        );
        assert!(pos > 0.49 && pos < 0.52);

        // Float min/max with integer value
        let pos = QueryPlanner::estimate_value_position(
            &Value::Integer(75),
            &Value::Float(0.0),
            &Value::Float(100.0),
        );
        assert!((pos - 0.75).abs() < 0.001);
    }

    #[test]
    fn test_runtime_join_decision_explanation() {
        let planner = QueryPlanner::new(Arc::new(MVCCEngine::in_memory()));

        let decision = planner.plan_runtime_join(1000, 1000, true);
        // Explanation should not be empty
        assert!(!decision.explanation.is_empty());
    }

    #[test]
    fn indexed_join_cost_prefers_scan_for_tiny_inner_relation() {
        let planner = QueryPlanner::new(Arc::new(MVCCEngine::in_memory()));
        let decision = planner.plan_indexed_join_access(IndexedJoinCostInput {
            outer_rows: 3,
            inner_rows: 4,
            inner_pages: 1,
            inner_distinct_keys: Some(4),
            inner_row_width: 24,
            projected_inner_width: 16,
            lookup_unique: true,
            limit: None,
        });

        assert!(!decision.use_index_lookup, "{}", decision.explanation);
        assert!(decision.scan_hash_cost < decision.lookup_cost);
    }

    #[test]
    fn indexed_join_cost_prefers_lookup_for_selective_large_inner_relation() {
        let planner = QueryPlanner::new(Arc::new(MVCCEngine::in_memory()));
        let decision = planner.plan_indexed_join_access(IndexedJoinCostInput {
            outer_rows: 1,
            inner_rows: 10_000,
            inner_pages: 196,
            inner_distinct_keys: Some(10_000),
            inner_row_width: 80,
            projected_inner_width: 16,
            lookup_unique: true,
            limit: None,
        });

        assert!(decision.use_index_lookup, "{}", decision.explanation);
        assert!(decision.lookup_cost < decision.scan_hash_cost);
        assert_eq!(decision.expected_matches, 1);
    }

    #[test]
    fn indexed_join_cost_accounts_for_non_unique_fanout() {
        let planner = QueryPlanner::new(Arc::new(MVCCEngine::in_memory()));
        let low_fanout = planner.plan_indexed_join_access(IndexedJoinCostInput {
            outer_rows: 4,
            inner_rows: 100_000,
            inner_pages: 2_000,
            inner_distinct_keys: Some(100_000),
            inner_row_width: 80,
            projected_inner_width: 16,
            lookup_unique: false,
            limit: None,
        });
        let high_fanout = planner.plan_indexed_join_access(IndexedJoinCostInput {
            inner_distinct_keys: Some(1),
            ..IndexedJoinCostInput {
                outer_rows: 4,
                inner_rows: 100_000,
                inner_pages: 2_000,
                inner_distinct_keys: None,
                inner_row_width: 80,
                projected_inner_width: 16,
                lookup_unique: false,
                limit: None,
            }
        });

        assert!(low_fanout.use_index_lookup, "{}", low_fanout.explanation);
        assert!(!high_fanout.use_index_lookup, "{}", high_fanout.explanation);
        assert!(high_fanout.expected_matches > low_fanout.expected_matches);
    }

    #[test]
    fn indexed_join_cost_uses_safe_limit_for_unique_edge() {
        let planner = QueryPlanner::new(Arc::new(MVCCEngine::in_memory()));
        let without_limit = planner.plan_indexed_join_access(IndexedJoinCostInput {
            outer_rows: 1_000,
            inner_rows: 10_000,
            inner_pages: 100,
            inner_distinct_keys: Some(10_000),
            inner_row_width: 80,
            projected_inner_width: 100,
            lookup_unique: true,
            limit: None,
        });
        let with_limit = planner.plan_indexed_join_access(IndexedJoinCostInput {
            limit: Some(10),
            ..IndexedJoinCostInput {
                outer_rows: 1_000,
                inner_rows: 10_000,
                inner_pages: 100,
                inner_distinct_keys: Some(10_000),
                inner_row_width: 80,
                projected_inner_width: 100,
                lookup_unique: true,
                limit: None,
            }
        });

        assert!(
            !without_limit.use_index_lookup,
            "{}",
            without_limit.explanation
        );
        assert!(with_limit.use_index_lookup, "{}", with_limit.explanation);
        assert!(with_limit.lookup_cost < without_limit.lookup_cost);
    }
}
