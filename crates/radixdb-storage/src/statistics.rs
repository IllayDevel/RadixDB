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

//! Statistics infrastructure for query optimization
//!
//! This module provides statistics collection and storage for cost-based
//! query optimization. Statistics are stored in system tables and collected
//! via the ANALYZE command.
//!
//! ## System Tables
//!
//! - `_sys_table_stats` - Table-level statistics (row count, page count, etc.)
//! - `_sys_column_stats` - Column-level statistics (distinct count, min/max, histogram)
//!
//! ## Usage
//!
//! Statistics are collected by running `ANALYZE table_name` which scans the table
//! and populates the system tables. The query planner retrieves these statistics
//! to estimate cardinalities and choose optimal access paths.

use radixdb_core::Value;

const STAT_VALUE_PREFIX: &str = "rdbs1:";

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn hex_decode(encoded: &str) -> Option<Vec<u8>> {
    if !encoded.len().is_multiple_of(2) {
        return None;
    }
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16)?;
            let low = (pair[1] as char).to_digit(16)?;
            Some(((high << 4) | low) as u8)
        })
        .collect()
}

/// Encode a statistics scalar without losing its physical Value variant.
///
/// Statistics are regenerable internal metadata, so the system-table columns
/// remain TEXT for compatibility. The payload itself is versioned and tagged;
/// planner comparisons therefore never infer a scalar type from display text.
#[doc(hidden)]
pub fn encode_statistics_value(value: &Value) -> String {
    match value {
        Value::Null(data_type) => format!("{STAT_VALUE_PREFIX}n:{:02x}", *data_type as u8),
        Value::Boolean(value) => format!("{STAT_VALUE_PREFIX}b:{}", u8::from(*value)),
        Value::Integer(value) => format!("{STAT_VALUE_PREFIX}i:{value}"),
        Value::Float(value) => format!("{STAT_VALUE_PREFIX}f:{:016x}", value.to_bits()),
        Value::Text(value) => format!("{STAT_VALUE_PREFIX}s:{}", hex_encode(value.as_bytes())),
        Value::Timestamp(value) => format!(
            "{STAT_VALUE_PREFIX}t:{}:{}",
            value.timestamp(),
            value.timestamp_subsec_nanos()
        ),
        Value::Extension(value) => {
            format!("{STAT_VALUE_PREFIX}x:{}", hex_encode(value.as_ref()))
        }
    }
}

/// Decode the versioned scalar representation used by persistent statistics.
#[doc(hidden)]
pub fn decode_statistics_value(encoded: &str) -> Option<Value> {
    let payload = encoded.strip_prefix(STAT_VALUE_PREFIX)?;
    let (tag, value) = payload.split_once(':')?;
    match tag {
        "n" => {
            let raw = u8::from_str_radix(value, 16).ok()?;
            let data_type = radixdb_core::DataType::from_u8(raw)?;
            Some(Value::Null(data_type))
        }
        "b" => match value {
            "0" => Some(Value::Boolean(false)),
            "1" => Some(Value::Boolean(true)),
            _ => None,
        },
        "i" => value.parse().ok().map(Value::Integer),
        "f" => u64::from_str_radix(value, 16)
            .ok()
            .map(|bits| Value::Float(f64::from_bits(bits))),
        "s" => String::from_utf8(hex_decode(value)?).ok().map(Value::text),
        "t" => {
            let (seconds, nanos) = value.split_once(':')?;
            let seconds = seconds.parse().ok()?;
            let nanos = nanos.parse().ok()?;
            chrono::DateTime::from_timestamp(seconds, nanos).map(Value::timestamp)
        }
        "x" => Some(Value::Extension(hex_decode(value)?.into())),
        _ => None,
    }
}

/// System table name for table-level statistics
pub const SYS_TABLE_STATS: &str = "_sys_table_stats";

/// System table name for column-level statistics
pub const SYS_COLUMN_STATS: &str = "_sys_column_stats";

/// SQL to create the table statistics system table
/// Note: RadixDB requires INTEGER PRIMARY KEY, so we use an auto-increment id
/// and a unique index on table_name
pub const CREATE_TABLE_STATS_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS _sys_table_stats (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    table_name TEXT NOT NULL UNIQUE,
    row_count INTEGER NOT NULL DEFAULT 0,
    page_count INTEGER NOT NULL DEFAULT 0,
    avg_row_size INTEGER NOT NULL DEFAULT 0,
    last_analyzed TIMESTAMP
)
"#;

/// SQL to create the column statistics system table
/// Note: We rely on DELETE before INSERT to maintain uniqueness on (table_name, column_name)
pub const CREATE_COLUMN_STATS_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS _sys_column_stats (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    null_count INTEGER NOT NULL DEFAULT 0,
    distinct_count INTEGER NOT NULL DEFAULT 0,
    min_value TEXT,
    max_value TEXT,
    avg_width INTEGER NOT NULL DEFAULT 0,
    histogram TEXT
)
"#;

/// Number of histogram buckets (default)
/// Using a small number for edge computing efficiency
pub const DEFAULT_HISTOGRAM_BUCKETS: usize = 32;

/// Equi-depth histogram for range selectivity estimation
///
/// Each bucket contains approximately the same number of values.
/// This provides better selectivity estimates for skewed data distributions.
#[derive(Debug, Clone)]
pub struct Histogram {
    /// Bucket boundaries (n+1 values for n buckets). The first value is the
    /// global lower bound and every following value is the inclusive upper
    /// bound of the corresponding bucket.
    boundaries: Vec<Value>,
    /// Legacy/equilibrium bucket width retained for old persisted statistics.
    rows_per_bucket: u64,
    /// Actual frequency retained by each bucket. Unlike `rows_per_bucket`,
    /// this survives quantile-boundary collapse around duplicate-heavy keys.
    bucket_counts: Vec<u64>,
    /// Frequency of the inclusive upper-bound value in each bucket.
    upper_repeats: Vec<u64>,
    /// Total number of values represented
    total_rows: u64,
}

impl Histogram {
    fn validate_parts(
        boundaries: &[Value],
        rows_per_bucket: u64,
        bucket_counts: &[u64],
        upper_repeats: &[u64],
        total_rows: u64,
    ) -> bool {
        let bucket_len = boundaries.len().saturating_sub(1);
        if boundaries.len() < 2
            || rows_per_bucket == 0
            || total_rows == 0
            || bucket_counts.len() != bucket_len
            || upper_repeats.len() != bucket_len
            || bucket_counts.contains(&0)
            || upper_repeats
                .iter()
                .zip(bucket_counts)
                .any(|(repeats, count)| repeats > count)
            || bucket_counts
                .iter()
                .try_fold(0u64, |sum, count| sum.checked_add(*count))
                != Some(total_rows)
        {
            return false;
        }
        let data_type = boundaries[0].data_type();
        boundaries
            .iter()
            .all(|value| !value.is_null() && value.data_type() == data_type)
            && boundaries.windows(2).all(|pair| {
                pair[0]
                    .compare(&pair[1])
                    .is_ok_and(|ordering| ordering != std::cmp::Ordering::Greater)
            })
    }

    pub fn boundaries(&self) -> &[Value] {
        &self.boundaries
    }

    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Build an equi-depth histogram from sorted values
    ///
    /// The input values must be sorted in ascending order.
    pub fn from_sorted_values(values: &[Value], num_buckets: usize) -> Option<Self> {
        Self::from_sorted_sample(
            values,
            num_buckets,
            values.iter().filter(|v| !v.is_null()).count() as u64,
        )
    }

    /// Build a bounded equi-depth sample whose frequencies are scaled to the
    /// full non-NULL cardinality domain.
    pub fn from_sorted_sample(
        values: &[Value],
        num_buckets: usize,
        represented_rows: u64,
    ) -> Option<Self> {
        if values.is_empty() || num_buckets == 0 {
            return None;
        }

        // Skip nulls - they're counted separately
        let non_null_values: Vec<_> = values.iter().filter(|v| !v.is_null()).collect();
        if non_null_values.is_empty() {
            return None;
        }
        let data_type = non_null_values[0].data_type();
        if non_null_values
            .iter()
            .any(|value| value.data_type() != data_type)
            || non_null_values.windows(2).any(|pair| {
                !pair[0]
                    .compare(pair[1])
                    .is_ok_and(|ordering| ordering != std::cmp::Ordering::Greater)
            })
        {
            return None;
        }

        let sample_rows = non_null_values.len() as u64;
        let total_rows = represented_rows.max(sample_rows);
        let rows_per_bucket = total_rows.div_ceil(num_buckets as u64).max(1);
        let sample_target = sample_rows.div_ceil(num_buckets as u64).max(1);
        let mut boundaries = vec![non_null_values[0].clone()];
        let mut sample_counts = Vec::with_capacity(num_buckets);
        let mut sample_upper_repeats = Vec::with_capacity(num_buckets);
        let mut current_count = 0u64;
        let mut last_run_count = 0u64;
        let mut index = 0usize;

        // Equal runs are indivisible: closing only between runs preserves the
        // frequency mass that the old duplicate-boundary collapse discarded.
        while index < non_null_values.len() {
            let value = non_null_values[index];
            let mut run_end = index + 1;
            while run_end < non_null_values.len() && non_null_values[run_end] == value {
                run_end += 1;
            }
            let run_count = (run_end - index) as u64;
            current_count += run_count;
            last_run_count = run_count;
            if sample_counts.len() + 1 < num_buckets && current_count >= sample_target {
                boundaries.push(value.clone());
                sample_counts.push(current_count);
                sample_upper_repeats.push(run_count);
                current_count = 0;
            }
            index = run_end;
        }
        if current_count > 0 {
            boundaries.push((*non_null_values.last().unwrap()).clone());
            sample_counts.push(current_count);
            sample_upper_repeats.push(last_run_count);
        }

        let mut bucket_counts = Vec::with_capacity(sample_counts.len());
        let mut upper_repeats = Vec::with_capacity(sample_counts.len());
        let mut assigned = 0u64;
        let mut sample_assigned = 0u64;
        for (index, sample_count) in sample_counts.iter().copied().enumerate() {
            sample_assigned += sample_count;
            let scaled_cumulative = if index + 1 == sample_counts.len() {
                total_rows
            } else {
                ((sample_assigned as u128 * total_rows as u128) / sample_rows as u128) as u64
            };
            bucket_counts.push(scaled_cumulative.saturating_sub(assigned));
            assigned = scaled_cumulative;
            let scaled_repeat = ((sample_upper_repeats[index] as u128 * total_rows as u128)
                / sample_rows as u128) as u64;
            upper_repeats.push(scaled_repeat.max(1).min(bucket_counts[index]));
        }

        let histogram = Self {
            boundaries,
            rows_per_bucket,
            bucket_counts,
            upper_repeats,
            total_rows,
        };
        Self::validate_parts(
            &histogram.boundaries,
            histogram.rows_per_bucket,
            &histogram.bucket_counts,
            &histogram.upper_repeats,
            histogram.total_rows,
        )
        .then_some(histogram)
    }

    /// Estimate selectivity for a range predicate using the histogram
    ///
    /// Returns the fraction of rows that satisfy:
    /// - For Lt/Le: value < bound or value <= bound
    /// - For Gt/Ge: value > bound or value >= bound
    /// - For Eq: value = bound (uses bucket containing the value)
    pub fn estimate_selectivity(&self, value: &Value, operator: HistogramOp) -> f64 {
        if self.boundaries.is_empty() || self.total_rows == 0 {
            return 0.5; // Fallback
        }

        let bucket_idx = self.find_bucket(value);

        match operator {
            HistogramOp::Equal => {
                let upper = &self.boundaries[(bucket_idx + 1).min(self.boundaries.len() - 1)];
                if value == upper && self.upper_repeat(bucket_idx) > 0 {
                    self.upper_repeat(bucket_idx) as f64 / self.total_rows as f64
                } else {
                    1.0 / self.bucket_count(bucket_idx).max(1) as f64
                }
            }
            HistogramOp::LessThan => self.cumulative_fraction(value, false),
            HistogramOp::LessThanOrEqual => self.cumulative_fraction(value, true),
            HistogramOp::GreaterThan => 1.0 - self.cumulative_fraction(value, true),
            HistogramOp::GreaterThanOrEqual => 1.0 - self.cumulative_fraction(value, false),
        }
    }

    /// Find which bucket contains a value (binary search)
    fn find_bucket(&self, value: &Value) -> usize {
        if self.boundaries.is_empty() {
            return 0;
        }

        let bucket_count = self.boundaries.len().saturating_sub(1);
        if bucket_count == 0 {
            return 0;
        }
        let mut low = 0usize;
        let mut high = bucket_count;
        while low < high {
            let mid = (low + high) / 2;
            if &self.boundaries[mid + 1] < value {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low.min(bucket_count - 1)
    }

    /// Estimate what fraction of a bucket is below a given value
    fn fraction_in_bucket(&self, value: &Value, bucket_idx: usize) -> f64 {
        if bucket_idx >= self.boundaries.len().saturating_sub(1) {
            return 1.0;
        }

        // Get bucket bounds
        let lower = &self.boundaries[bucket_idx];
        let upper = if bucket_idx + 1 < self.boundaries.len() {
            &self.boundaries[bucket_idx + 1]
        } else {
            return 1.0;
        };

        // Estimate fraction based on value position within bucket
        // For numeric types, use linear interpolation
        match (lower, upper, value) {
            (Value::Integer(lo), Value::Integer(hi), Value::Integer(v)) => {
                if hi == lo {
                    if v < lo {
                        0.0
                    } else {
                        1.0
                    }
                } else {
                    let numerator = *v as i128 - *lo as i128;
                    let denominator = *hi as i128 - *lo as i128;
                    (numerator as f64 / denominator as f64).clamp(0.0, 1.0)
                }
            }
            (Value::Float(lo), Value::Float(hi), Value::Float(v)) => {
                if (hi - lo).abs() < f64::EPSILON {
                    if v < lo {
                        0.0
                    } else {
                        1.0
                    }
                } else {
                    let fraction = (v - lo) / (hi - lo);
                    if fraction.is_finite() {
                        fraction.clamp(0.0, 1.0)
                    } else {
                        0.5
                    }
                }
            }
            _ if value <= lower => 0.0,
            _ if value >= upper => 1.0,
            _ => 0.5,
        }
    }

    fn bucket_count(&self, bucket_idx: usize) -> u64 {
        self.bucket_counts
            .get(bucket_idx)
            .copied()
            .unwrap_or(self.rows_per_bucket)
    }

    fn upper_repeat(&self, bucket_idx: usize) -> u64 {
        self.upper_repeats.get(bucket_idx).copied().unwrap_or(0)
    }

    fn cumulative_fraction(&self, value: &Value, inclusive: bool) -> f64 {
        if self.boundaries.len() < 2 || self.total_rows == 0 {
            return 0.5;
        }
        if value < &self.boundaries[0] {
            return 0.0;
        }
        if value > self.boundaries.last().unwrap() {
            return 1.0;
        }
        let bucket_idx = self.find_bucket(value);
        let before: u128 = (0..bucket_idx)
            .map(|index| self.bucket_count(index) as u128)
            .sum();
        let lower = &self.boundaries[bucket_idx];
        let upper = &self.boundaries[bucket_idx + 1];
        let bucket_count = self.bucket_count(bucket_idx);
        let upper_repeat = self.upper_repeat(bucket_idx).min(bucket_count);
        let within = if value == upper {
            if inclusive {
                bucket_count as f64
            } else {
                bucket_count.saturating_sub(upper_repeat) as f64
            }
        } else {
            self.fraction_in_bucket(value, bucket_idx)
                * bucket_count.saturating_sub(upper_repeat) as f64
        };
        let _ = lower;
        let fraction = (before as f64 + within) / self.total_rows as f64;
        if fraction.is_finite() {
            fraction.clamp(0.0, 1.0)
        } else {
            0.5
        }
    }

    /// Estimate selectivity for a BETWEEN range predicate
    ///
    /// Returns the fraction of rows where low <= value <= high.
    /// Uses bucket walk algorithm for accurate estimation.
    pub fn estimate_range_selectivity(&self, low: &Value, high: &Value) -> f64 {
        if self.boundaries.is_empty() || self.total_rows == 0 {
            return 0.33; // Default range selectivity
        }

        if low > high {
            return 0.0;
        }
        (self.cumulative_fraction(high, true) - self.cumulative_fraction(low, false))
            .clamp(0.0001, 1.0)
    }

    /// Serialize histogram to JSON string for storage
    pub fn to_json(&self) -> String {
        let boundary_strs: Vec<String> = self
            .boundaries
            .iter()
            .map(encode_statistics_value)
            .collect();
        format!(
            r#"{{"boundaries":[{}],"rows_per_bucket":{},"bucket_counts":[{}],"upper_repeats":[{}],"total_rows":{}}}"#,
            boundary_strs
                .iter()
                .map(|s| format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")))
                .collect::<Vec<_>>()
                .join(","),
            self.rows_per_bucket,
            self.bucket_counts
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(","),
            self.upper_repeats
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(","),
            self.total_rows
        )
    }

    /// Parse histogram from JSON string
    pub fn from_json(json: &str) -> Option<Self> {
        // Simple JSON parsing for histogram format
        // Format: {"boundaries":["v1","v2",...],"rows_per_bucket":N,"total_rows":N}
        let json = json.trim();
        if !json.starts_with('{') || !json.ends_with('}') {
            return None;
        }

        // Extract rows_per_bucket
        let rows_per_bucket = extract_number(json, "rows_per_bucket")?;
        let total_rows = extract_number(json, "total_rows")?;

        // Extract boundaries array
        let boundaries = extract_value_array(json, "boundaries")?;
        let bucket_len = boundaries.len().saturating_sub(1);
        let bucket_counts = extract_number_array(json, "bucket_counts").unwrap_or_else(|| {
            let mut counts = vec![rows_per_bucket; bucket_len];
            if let Some(last) = counts.last_mut() {
                let assigned = rows_per_bucket.saturating_mul(bucket_len.saturating_sub(1) as u64);
                *last = total_rows.saturating_sub(assigned).max(1);
            }
            counts
        });
        let upper_repeats =
            extract_number_array(json, "upper_repeats").unwrap_or_else(|| vec![0; bucket_len]);
        if bucket_counts.len() != bucket_len || upper_repeats.len() != bucket_len {
            return None;
        }

        Self::validate_parts(
            &boundaries,
            rows_per_bucket,
            &bucket_counts,
            &upper_repeats,
            total_rows,
        )
        .then_some(Self {
            boundaries,
            rows_per_bucket,
            bucket_counts,
            upper_repeats,
            total_rows,
        })
    }
}

/// Helper function to extract a number from simple JSON
fn extract_number(json: &str, key: &str) -> Option<u64> {
    let key_pattern = format!("\"{}\":", key);
    let start = json.find(&key_pattern)? + key_pattern.len();
    let rest = &json[start..];
    let end = rest.find([',', '}'])?;
    rest[..end].trim().parse().ok()
}

fn extract_number_array(json: &str, key: &str) -> Option<Vec<u64>> {
    let key_pattern = format!("\"{}\":[", key);
    let start = json.find(&key_pattern)? + key_pattern.len();
    let rest = &json[start..];
    let end = rest.find(']')?;
    let content = rest[..end].trim();
    if content.is_empty() {
        return Some(Vec::new());
    }
    content
        .split(',')
        .map(|value| value.trim().parse().ok())
        .collect()
}

/// Helper function to extract a Value array from simple JSON
fn extract_value_array(json: &str, key: &str) -> Option<Vec<Value>> {
    let key_pattern = format!("\"{}\":[", key);
    let start = json.find(&key_pattern)? + key_pattern.len();
    let rest = &json[start..];
    let end = rest.find(']')?;
    let array_content = &rest[..end];

    let mut values = Vec::new();
    for item in array_content.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        // Remove surrounding quotes
        let item = item.trim_matches('"');
        values.push(decode_statistics_value(item)?);
    }

    Some(values)
}

/// Histogram comparison operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistogramOp {
    Equal,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

/// Maximum number of rows to sample for statistics
/// For large tables, we sample instead of scanning everything
pub const DEFAULT_SAMPLE_SIZE: usize = 10000;

/// Table-level statistics (in-memory representation)
#[derive(Debug, Clone, Default)]
pub struct TableStats {
    /// Table name
    pub table_name: String,
    /// Estimated row count
    pub row_count: u64,
    /// Number of pages/blocks (for I/O cost estimation)
    pub page_count: u64,
    /// Average row size in bytes
    pub avg_row_size: u64,
}

impl TableStats {
    /// Create new empty table statistics
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            row_count: 0,
            page_count: 0,
            avg_row_size: 0,
        }
    }

    /// Get the selectivity for an equality predicate
    /// Returns 1/row_count or 0.1 as default
    pub fn equality_selectivity(&self, distinct_count: u64) -> f64 {
        if distinct_count > 0 {
            1.0 / distinct_count as f64
        } else if self.row_count > 0 {
            1.0 / self.row_count as f64
        } else {
            0.1
        }
    }
}

/// Column-level statistics (in-memory representation)
#[derive(Debug, Clone, Default)]
pub struct ColumnStats {
    /// Column name
    pub column_name: String,
    /// Number of NULL values
    pub null_count: u64,
    /// Number of distinct values (approximate)
    pub distinct_count: u64,
    /// Minimum value (for range estimation)
    pub min_value: Option<Value>,
    /// Maximum value (for range estimation)
    pub max_value: Option<Value>,
    /// Average value width in bytes (for memory estimation)
    pub avg_width: u32,
    /// Histogram buckets as JSON string
    pub histogram: Option<String>,
}

impl ColumnStats {
    /// Create new empty column statistics
    pub fn new(column_name: String) -> Self {
        Self {
            column_name,
            null_count: 0,
            distinct_count: 0,
            min_value: None,
            max_value: None,
            avg_width: 0,
            histogram: None,
        }
    }

    /// Check if statistics are empty (never analyzed)
    pub fn is_empty(&self) -> bool {
        self.distinct_count == 0 && self.min_value.is_none() && self.max_value.is_none()
    }

    /// Parse and return the histogram if available
    pub fn parsed_histogram(&self) -> Option<Histogram> {
        self.histogram
            .as_ref()
            .and_then(|json| Histogram::from_json(json))
    }

    /// Set histogram from a Histogram struct
    pub fn set_histogram(&mut self, histogram: &Histogram) {
        self.histogram = Some(histogram.to_json());
    }
}

/// Selectivity estimation utilities
pub struct SelectivityEstimator;

impl SelectivityEstimator {
    /// Estimate selectivity for equality predicate (column = value)
    /// Formula: 1 / distinct_count
    pub fn equality(distinct_count: u64) -> f64 {
        if distinct_count > 0 {
            1.0 / distinct_count as f64
        } else {
            0.1 // default
        }
    }

    /// Estimate selectivity for range predicate (column > value, column < value)
    /// Using uniform distribution assumption: 1/3 for range predicates
    pub fn range() -> f64 {
        0.33
    }

    /// Estimate selectivity for range predicate using histogram
    ///
    /// If a histogram is available, uses it for accurate estimates.
    /// Otherwise falls back to uniform distribution assumption.
    pub fn range_with_histogram(col_stats: &ColumnStats, value: &Value, op: HistogramOp) -> f64 {
        // Try to use histogram if available
        if let Some(histogram) = col_stats.parsed_histogram() {
            return histogram.estimate_selectivity(value, op);
        }

        // Fall back to min/max based estimation if available
        if let (Some(min_val), Some(max_val)) = (&col_stats.min_value, &col_stats.max_value) {
            // Use linear interpolation between min and max
            let fraction = Self::estimate_position(value, min_val, max_val);

            return match op {
                HistogramOp::Equal => 1.0 / col_stats.distinct_count.max(1) as f64,
                HistogramOp::LessThan | HistogramOp::LessThanOrEqual => fraction,
                HistogramOp::GreaterThan | HistogramOp::GreaterThanOrEqual => 1.0 - fraction,
            };
        }

        // No statistics available - use default
        match op {
            HistogramOp::Equal => 0.1,
            _ => 0.33,
        }
    }

    /// Estimate position of a value between min and max (0.0 to 1.0)
    fn estimate_position(value: &Value, min: &Value, max: &Value) -> f64 {
        match (min, max, value) {
            (Value::Integer(lo), Value::Integer(hi), Value::Integer(v)) => {
                if hi == lo {
                    0.5
                } else {
                    let numerator = *v as i128 - *lo as i128;
                    let denominator = *hi as i128 - *lo as i128;
                    (numerator as f64 / denominator as f64).clamp(0.0, 1.0)
                }
            }
            (Value::Float(lo), Value::Float(hi), Value::Float(v)) => {
                if (hi - lo).abs() < f64::EPSILON {
                    0.5
                } else {
                    let fraction = (v - lo) / (hi - lo);
                    if fraction.is_finite() {
                        fraction.clamp(0.0, 1.0)
                    } else {
                        0.5
                    }
                }
            }
            _ => 0.5, // Default for non-comparable types
        }
    }

    /// Estimate selectivity for LIKE predicate
    /// Prefix patterns are more selective than suffix/infix
    pub fn like(pattern: &str, distinct_count: u64) -> f64 {
        // Prefix-only patterns (e.g., 'abc%') are more selective
        if !pattern.starts_with('%') && pattern.ends_with('%') {
            let prefix_len = pattern.len() - 1;
            if distinct_count > 0 {
                // Estimate based on prefix length
                let prefix_selectivity = (26.0_f64).powi(-(prefix_len as i32));
                return prefix_selectivity.max(1.0 / distinct_count as f64);
            }
            return 0.1;
        }

        // Suffix or infix patterns are less selective
        if pattern.starts_with('%') {
            return 0.25;
        }

        0.15 // Default for mixed patterns
    }

    /// Estimate selectivity for IN list predicate
    /// Formula: list_size / distinct_count
    pub fn in_list(list_size: usize, distinct_count: u64) -> f64 {
        if distinct_count > 0 {
            (list_size as f64 / distinct_count as f64).min(1.0)
        } else {
            (list_size as f64 * 0.1).min(1.0)
        }
    }

    /// Estimate selectivity for IS NULL predicate
    /// Formula: null_count / row_count
    pub fn is_null(null_count: u64, row_count: u64) -> f64 {
        if row_count > 0 {
            null_count as f64 / row_count as f64
        } else {
            0.01
        }
    }

    /// Estimate selectivity for IS NOT NULL predicate
    pub fn is_not_null(null_count: u64, row_count: u64) -> f64 {
        1.0 - Self::is_null(null_count, row_count)
    }

    /// Estimate join cardinality
    /// Formula: |R| * |S| / max(distinct(R.col), distinct(S.col))
    pub fn join_cardinality(
        left_rows: u64,
        right_rows: u64,
        left_distinct: u64,
        right_distinct: u64,
    ) -> u64 {
        let max_distinct = left_distinct.max(right_distinct).max(1);
        let cardinality = left_rows as u128 * right_rows as u128 / max_distinct as u128;
        cardinality.min(u64::MAX as u128) as u64
    }
}

/// Check if a table name is a system statistics table
pub fn is_stats_table(table_name: &str) -> bool {
    let lower = table_name.to_lowercase();
    lower == SYS_TABLE_STATS || lower == SYS_COLUMN_STATS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_stats_new() {
        let stats = TableStats::new("test_table".to_string());
        assert_eq!(stats.table_name, "test_table");
        assert_eq!(stats.row_count, 0);
    }

    #[test]
    fn test_equality_selectivity() {
        // With 100 distinct values, selectivity = 1/100 = 0.01
        let sel = SelectivityEstimator::equality(100);
        assert!((sel - 0.01).abs() < 0.001);

        // With 0 distinct values, use default
        let sel_default = SelectivityEstimator::equality(0);
        assert!((sel_default - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_in_list_selectivity() {
        // IN list with 2 values out of 5 distinct = 0.4
        let sel = SelectivityEstimator::in_list(2, 5);
        assert!((sel - 0.4).abs() < 0.001);

        // Large list should cap at 1.0
        let sel_large = SelectivityEstimator::in_list(10, 5);
        assert!((sel_large - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_null_selectivity() {
        // 100 nulls out of 1000 rows = 0.1
        let sel = SelectivityEstimator::is_null(100, 1000);
        assert!((sel - 0.1).abs() < 0.001);

        // Not null selectivity should be 0.9
        let sel_not_null = SelectivityEstimator::is_not_null(100, 1000);
        assert!((sel_not_null - 0.9).abs() < 0.001);
    }

    #[test]
    fn test_join_cardinality() {
        // 10000 orders, 1000 users, 1000 distinct user_ids
        // Join cardinality = 10000 * 1000 / max(1000, 1000) = 10000
        let join_card = SelectivityEstimator::join_cardinality(10000, 1000, 1000, 1000);
        assert_eq!(join_card, 10000);
    }

    #[test]
    fn test_like_selectivity() {
        // Prefix pattern
        let sel_prefix = SelectivityEstimator::like("abc%", 1000);
        assert!(sel_prefix < 0.1);

        // Suffix pattern (less selective)
        let sel_suffix = SelectivityEstimator::like("%abc", 1000);
        assert!((sel_suffix - 0.25).abs() < 0.001);
    }

    #[test]
    fn test_is_stats_table() {
        assert!(is_stats_table("_sys_table_stats"));
        assert!(is_stats_table("_SYS_TABLE_STATS"));
        assert!(is_stats_table("_sys_column_stats"));
        assert!(!is_stats_table("users"));
        assert!(!is_stats_table("_sys_other"));
    }

    #[test]
    fn test_column_stats_is_empty() {
        let stats = ColumnStats::new("test".to_string());
        assert!(stats.is_empty());

        let mut stats2 = ColumnStats::new("test".to_string());
        stats2.distinct_count = 10;
        assert!(!stats2.is_empty());
    }

    #[test]
    fn test_histogram_from_sorted_values() {
        // Create sorted integer values
        let values: Vec<Value> = (0..100).map(Value::Integer).collect();
        let histogram = Histogram::from_sorted_values(&values, 10).unwrap();

        // Should have boundaries
        assert!(!histogram.boundaries.is_empty());
        assert_eq!(histogram.total_rows, 100);
        assert_eq!(histogram.rows_per_bucket, 10);

        // First boundary should be minimum
        assert_eq!(histogram.boundaries[0], Value::Integer(0));
    }

    #[test]
    fn test_histogram_selectivity_estimation() {
        // Create uniformly distributed values 0-99
        let values: Vec<Value> = (0..100).map(Value::Integer).collect();
        let histogram = Histogram::from_sorted_values(&values, 10).unwrap();

        // LessThan 50 should be approximately 0.5
        let sel_lt_50 = histogram.estimate_selectivity(&Value::Integer(50), HistogramOp::LessThan);
        assert!(
            sel_lt_50 > 0.4 && sel_lt_50 < 0.6,
            "Expected ~0.5, got {}",
            sel_lt_50
        );

        // LessThan 10 should be approximately 0.1
        let sel_lt_10 = histogram.estimate_selectivity(&Value::Integer(10), HistogramOp::LessThan);
        assert!(
            sel_lt_10 > 0.05 && sel_lt_10 < 0.2,
            "Expected ~0.1, got {}",
            sel_lt_10
        );

        // GreaterThan 90 should be approximately 0.1
        let sel_gt_90 =
            histogram.estimate_selectivity(&Value::Integer(90), HistogramOp::GreaterThan);
        assert!(
            sel_gt_90 > 0.0 && sel_gt_90 < 0.2,
            "Expected ~0.1, got {}",
            sel_gt_90
        );
    }

    #[test]
    fn test_histogram_json_round_trip() {
        let values: Vec<Value> = (0..100).map(Value::Integer).collect();
        let histogram = Histogram::from_sorted_values(&values, 10).unwrap();

        // Serialize to JSON
        let json = histogram.to_json();

        // Parse back
        let parsed = Histogram::from_json(&json).expect("Failed to parse histogram JSON");

        // Verify key properties match
        assert_eq!(parsed.total_rows, histogram.total_rows);
        assert_eq!(parsed.rows_per_bucket, histogram.rows_per_bucket);
        assert_eq!(parsed.boundaries.len(), histogram.boundaries.len());
    }

    #[test]
    fn r7_l01_legacy_histogram_boundaries_are_rejected() {
        let current = Histogram {
            boundaries: vec![Value::text("001"), Value::text("1e3")],
            rows_per_bucket: 1,
            bucket_counts: vec![2],
            upper_repeats: vec![1],
            total_rows: 2,
        };
        assert_eq!(
            Histogram::from_json(&current.to_json()).unwrap().boundaries,
            current.boundaries
        );

        let legacy = r#"{"boundaries":["001","1e3"],"rows_per_bucket":1,"total_rows":2}"#;
        assert!(Histogram::from_json(legacy).is_none());
    }

    #[test]
    fn test_range_with_histogram() {
        // Create column stats with histogram
        let values: Vec<Value> = (0..100).map(Value::Integer).collect();
        let histogram = Histogram::from_sorted_values(&values, 10).unwrap();

        let mut col_stats = ColumnStats::new("test".to_string());
        col_stats.set_histogram(&histogram);
        col_stats.min_value = Some(Value::Integer(0));
        col_stats.max_value = Some(Value::Integer(99));
        col_stats.distinct_count = 100;

        // Use histogram-based estimation
        let sel = SelectivityEstimator::range_with_histogram(
            &col_stats,
            &Value::Integer(50),
            HistogramOp::LessThan,
        );
        assert!(sel > 0.4 && sel < 0.6, "Expected ~0.5, got {}", sel);
    }

    #[test]
    fn test_histogram_empty_values() {
        let values: Vec<Value> = vec![];
        let histogram = Histogram::from_sorted_values(&values, 10);
        assert!(histogram.is_none());
    }

    #[test]
    fn test_histogram_with_nulls() {
        use radixdb_core::DataType;

        // Histogram should skip null values
        let mut values: Vec<Value> = (0..50).map(Value::Integer).collect();
        values.extend((0..10).map(|_| Value::Null(DataType::Integer)));
        values.extend((50..100).map(Value::Integer));

        let histogram = Histogram::from_sorted_values(&values, 10).unwrap();
        assert_eq!(histogram.total_rows, 100); // Only non-null values counted
    }

    // =========================================================================
    // HISTOGRAM BETWEEN RANGE SELECTIVITY TESTS
    // =========================================================================

    #[test]
    fn test_histogram_between_range_selectivity() {
        // Create uniformly distributed values 0-99
        let values: Vec<Value> = (0..100).map(Value::Integer).collect();
        let histogram = Histogram::from_sorted_values(&values, 10).unwrap();

        // BETWEEN 25 AND 75 should be approximately 0.5
        let sel_25_75 =
            histogram.estimate_range_selectivity(&Value::Integer(25), &Value::Integer(75));
        assert!(
            sel_25_75 > 0.4 && sel_25_75 < 0.65,
            "Expected ~0.5 for BETWEEN 25 AND 75, got {}",
            sel_25_75
        );

        // BETWEEN 0 AND 10 should be approximately 0.1
        let sel_0_10 =
            histogram.estimate_range_selectivity(&Value::Integer(0), &Value::Integer(10));
        assert!(
            sel_0_10 > 0.05 && sel_0_10 < 0.2,
            "Expected ~0.1 for BETWEEN 0 AND 10, got {}",
            sel_0_10
        );

        // BETWEEN 90 AND 100 should be approximately 0.1
        let sel_90_100 =
            histogram.estimate_range_selectivity(&Value::Integer(90), &Value::Integer(100));
        assert!(
            sel_90_100 > 0.05 && sel_90_100 < 0.2,
            "Expected ~0.1 for BETWEEN 90 AND 100, got {}",
            sel_90_100
        );

        // BETWEEN 0 AND 100 should be approximately 1.0
        let sel_full =
            histogram.estimate_range_selectivity(&Value::Integer(0), &Value::Integer(100));
        assert!(
            sel_full > 0.9,
            "Expected ~1.0 for full range, got {}",
            sel_full
        );
    }

    #[test]
    fn test_histogram_between_single_bucket() {
        // Create uniformly distributed values 0-99
        let values: Vec<Value> = (0..100).map(Value::Integer).collect();
        let histogram = Histogram::from_sorted_values(&values, 10).unwrap();

        // Small range within single bucket
        let sel_5_8 = histogram.estimate_range_selectivity(&Value::Integer(5), &Value::Integer(8));
        assert!(
            sel_5_8 > 0.0 && sel_5_8 < 0.15,
            "Expected small selectivity for narrow range, got {}",
            sel_5_8
        );
    }

    #[test]
    fn test_histogram_between_float_values() {
        // Create float values 0.0 to 99.0
        let values: Vec<Value> = (0..100).map(|i| Value::Float(i as f64)).collect();
        let histogram = Histogram::from_sorted_values(&values, 10).unwrap();

        // BETWEEN 25.0 AND 75.0 should be approximately 0.5
        let sel = histogram.estimate_range_selectivity(&Value::Float(25.0), &Value::Float(75.0));
        assert!(
            sel > 0.4 && sel < 0.65,
            "Expected ~0.5 for BETWEEN 25.0 AND 75.0, got {}",
            sel
        );
    }

    #[test]
    fn r3_l04_batch_c_histogram_roundtrip_preserves_scalar_types() {
        let domains = vec![
            vec![Value::Integer(i64::MIN), Value::Integer(i64::MAX)],
            vec![Value::Float(-0.0), Value::Float(1.0)],
            vec![
                Value::timestamp(
                    chrono::DateTime::from_timestamp_millis(1_700_000_000_123).unwrap(),
                ),
                Value::timestamp(
                    chrono::DateTime::from_timestamp_millis(1_700_000_000_124).unwrap(),
                ),
            ],
            vec![Value::uuid([0x5a; 16]), Value::uuid([0x5b; 16])],
            vec![Value::decimal(12345, 8, 2), Value::decimal(12346, 8, 2)],
            vec![Value::text("42"), Value::text("43")],
        ];
        for boundaries in domains {
            let histogram = Histogram {
                boundaries: boundaries.clone(),
                rows_per_bucket: 7,
                bucket_counts: vec![7],
                upper_repeats: vec![1],
                total_rows: 7,
            };
            let decoded = Histogram::from_json(&histogram.to_json())
                .expect("typed statistics histogram must decode");
            assert_eq!(decoded.boundaries, boundaries);
        }

        let mixed = Histogram {
            boundaries: vec![Value::Integer(1), Value::text("2")],
            rows_per_bucket: 1,
            bucket_counts: vec![1],
            upper_repeats: vec![0],
            total_rows: 1,
        };
        assert!(Histogram::from_json(&mixed.to_json()).is_none());
    }

    #[test]
    fn histogram_extremes_and_malformed_shapes_fail_closed() {
        let histogram =
            Histogram::from_sorted_values(&[Value::Integer(i64::MIN), Value::Integer(i64::MAX)], 1)
                .unwrap();
        let estimate = histogram.estimate_selectivity(&Value::Integer(0), HistogramOp::LessThan);
        assert!(estimate.is_finite() && (0.0..=1.0).contains(&estimate));
        assert_eq!(
            SelectivityEstimator::join_cardinality(u64::MAX, u64::MAX, 1, 1),
            u64::MAX
        );

        for malformed in [
            r#"{"boundaries":["I:1","I:2"],"rows_per_bucket":0,"bucket_counts":[1],"upper_repeats":[0],"total_rows":1}"#,
            r#"{"boundaries":["I:2","I:1"],"rows_per_bucket":1,"bucket_counts":[1],"upper_repeats":[0],"total_rows":1}"#,
            r#"{"boundaries":["I:1","I:2"],"rows_per_bucket":1,"bucket_counts":[2],"upper_repeats":[3],"total_rows":1}"#,
        ] {
            assert!(Histogram::from_json(malformed).is_none());
        }
    }

    #[test]
    fn r8_l01_batch_g_histogram_retains_duplicate_frequency_mass() {
        let mut values = vec![Value::Integer(0); 90];
        values.extend((1..=10).map(Value::Integer));
        let histogram = Histogram::from_sorted_values(&values, 10).unwrap();

        assert_eq!(histogram.bucket_counts.iter().sum::<u64>(), 100);
        assert_eq!(histogram.upper_repeats[0], 90);
        assert_eq!(
            histogram.estimate_selectivity(&Value::Integer(0), HistogramOp::LessThan),
            0.0
        );
        assert!(
            (histogram.estimate_selectivity(&Value::Integer(0), HistogramOp::Equal) - 0.9).abs()
                < 0.0001
        );
        assert!(
            (histogram.estimate_selectivity(&Value::Integer(0), HistogramOp::LessThanOrEqual)
                - 0.9)
                .abs()
                < 0.0001
        );

        let decoded = Histogram::from_json(&histogram.to_json()).unwrap();
        assert_eq!(decoded.bucket_counts, histogram.bucket_counts);
        assert_eq!(decoded.upper_repeats, histogram.upper_repeats);
    }
}
