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

//! Typed column storage for frozen volumes.
//!
//! Each column is stored as a contiguous typed array with a null bitmap.
//! This avoids the 16-byte Value enum overhead during scans — integer columns
//! are raw `[i64]`, float columns are raw `[f64]`, etc.
//!
//! Text columns use dictionary encoding: values are stored as `[u32]` IDs
//! referencing a deduplicated string table, reducing storage for repeated
//! values (e.g., exchange names, symbols) from N bytes to 4 bytes.

use std::sync::Arc;

use ahash::AHashMap;
use radixdb_core::SmartString;
use radixdb_core::{DataType, Error, LogicalTypeRef, Result, Value};

/// Typed column data stored contiguously for cache-friendly access.
///
/// Each variant stores a flat array of the native type plus a null bitmap.
/// The null bitmap uses one byte per row (not bit-packed) for simplicity
/// and fast random access. For 1M rows this is 1MB overhead.
#[derive(Clone)]
pub enum ColumnData {
    /// 64-bit signed integers with null bitmap.
    /// Used for INTEGER columns and auto-increment IDs.
    Int64 { values: Vec<i64>, nulls: Vec<bool> },

    /// 64-bit floating point with null bitmap.
    /// Used for FLOAT/DOUBLE columns.
    Float64 { values: Vec<f64>, nulls: Vec<bool> },

    /// Timestamps stored as nanoseconds since Unix epoch.
    /// Preserves sub-second precision while enabling integer comparison
    /// and binary search without chrono overhead.
    TimestampNanos { values: Vec<i64>, nulls: Vec<bool> },

    /// Booleans stored as bytes with null bitmap.
    Boolean { values: Vec<bool>, nulls: Vec<bool> },

    /// Dictionary-encoded text.
    /// Each value is a u32 index into the dictionary Vec.
    /// Repeated strings (common in categorical data) share a single
    /// dictionary entry, reducing storage from O(n * avg_len) to O(n * 4 + unique * avg_len).
    Dictionary {
        /// Per-row dictionary IDs
        ids: Vec<u32>,
        /// Deduplicated string table (Arc for cheap cloning across row groups)
        dictionary: Arc<[SmartString]>,
        /// Null bitmap
        nulls: Vec<bool>,
    },

    /// Raw bytes for Extension types (JSON, Vector, etc.)
    /// Falls back to per-value serialization.
    Bytes {
        /// Concatenated byte data for all rows
        data: Vec<u8>,
        /// (offset, length) pairs for each row
        offsets: Vec<(u64, u64)>,
        /// The extension DataType tag (e.g., DataType::Json)
        ext_type: DataType,
        /// Null bitmap
        nulls: Vec<bool>,
    },
    /// Canonical payloads for one catalog-bound external scalar type.
    External {
        data: Vec<u8>,
        offsets: Vec<(u64, u64)>,
        type_ref: radixdb_core::ExternalTypeRef,
        nulls: Vec<bool>,
    },
}

/// Per-column zone map for segment-level pruning.
///
/// Stores the min and max values seen in the column. A query predicate
/// like `WHERE time >= X` can skip the entire volume if `zone_max < X`.
///
/// For dictionary-encoded columns, min/max reference the dictionary values,
/// not the IDs.
#[derive(Debug, Clone)]
pub struct ZoneMap {
    /// Minimum non-null value in the column
    pub min: Value,
    /// Maximum non-null value in the column
    pub max: Value,
    /// Number of null values
    pub null_count: u32,
    /// Total number of rows
    pub row_count: u32,
}

/// Default number of rows per row group for sub-volume zone map pruning.
pub const ROW_GROUP_SIZE: usize = 65536; // 64K rows

/// Zone map metadata for a single row group within a volume.
/// Each row group covers a contiguous index range [start_idx, end_idx).
/// Column data stays contiguous in memory; row groups are a logical overlay.
#[derive(Debug, Clone)]
pub struct RowGroupMeta {
    /// Start index (inclusive) within the volume's column arrays.
    pub start_idx: u32,
    /// End index (exclusive).
    pub end_idx: u32,
    /// Per-column zone maps for this row group. Length = number of columns.
    pub zone_maps: Vec<ZoneMap>,
}

impl ColumnData {
    /// Build one decoded runtime column from a checked physical row-group.
    ///
    /// Artifact readers deliberately return canonical [`Value`] objects so
    /// their format validation stays independent from the MVCC row adapter.
    /// This boundary converts exactly one bounded row group back into the
    /// typed arrays consumed by scans and vectorized operators. It never
    /// materializes a complete cold segment.
    pub fn try_from_values(data_type: DataType, values: &[Value]) -> Result<Self> {
        if data_type == DataType::Null {
            return Err(Error::invalid_argument(
                "NULL is not a physical column data type",
            ));
        }

        let mut nulls = Vec::with_capacity(values.len());
        match data_type {
            DataType::Integer => {
                let mut decoded = Vec::with_capacity(values.len());
                for value in values {
                    match value {
                        Value::Null(actual) if *actual == data_type => {
                            decoded.push(0);
                            nulls.push(true);
                        }
                        Value::Integer(value) => {
                            decoded.push(*value);
                            nulls.push(false);
                        }
                        _ => return Err(column_value_type_error(data_type, value)),
                    }
                }
                Ok(Self::Int64 {
                    values: decoded,
                    nulls,
                })
            }
            DataType::Float => {
                let mut decoded = Vec::with_capacity(values.len());
                for value in values {
                    match value {
                        Value::Null(actual) if *actual == data_type => {
                            decoded.push(0.0);
                            nulls.push(true);
                        }
                        Value::Float(value) => {
                            decoded.push(*value);
                            nulls.push(false);
                        }
                        _ => return Err(column_value_type_error(data_type, value)),
                    }
                }
                Ok(Self::Float64 {
                    values: decoded,
                    nulls,
                })
            }
            DataType::Timestamp => {
                let mut decoded = Vec::with_capacity(values.len());
                for value in values {
                    match value {
                        Value::Null(actual) if *actual == data_type => {
                            decoded.push(0);
                            nulls.push(true);
                        }
                        Value::Timestamp(value) => {
                            let nanos = value.timestamp_nanos_opt().ok_or_else(|| {
                                Error::invalid_argument(
                                    "physical timestamp is outside the exact nanosecond range",
                                )
                            })?;
                            decoded.push(nanos);
                            nulls.push(false);
                        }
                        _ => return Err(column_value_type_error(data_type, value)),
                    }
                }
                Ok(Self::TimestampNanos {
                    values: decoded,
                    nulls,
                })
            }
            DataType::Boolean => {
                let mut decoded = Vec::with_capacity(values.len());
                for value in values {
                    match value {
                        Value::Null(actual) if *actual == data_type => {
                            decoded.push(false);
                            nulls.push(true);
                        }
                        Value::Boolean(value) => {
                            decoded.push(*value);
                            nulls.push(false);
                        }
                        _ => return Err(column_value_type_error(data_type, value)),
                    }
                }
                Ok(Self::Boolean {
                    values: decoded,
                    nulls,
                })
            }
            DataType::Text => {
                let mut ids = Vec::with_capacity(values.len());
                let mut dictionary = Vec::new();
                let mut dictionary_ids = AHashMap::new();
                for value in values {
                    match value {
                        Value::Null(actual) if *actual == data_type => {
                            ids.push(0);
                            nulls.push(true);
                        }
                        Value::Text(value) => {
                            let id = if let Some(id) = dictionary_ids.get(value).copied() {
                                id
                            } else {
                                let id = u32::try_from(dictionary.len()).map_err(|_| {
                                    Error::invalid_argument(
                                        "row-group text dictionary exceeds u32 identifiers",
                                    )
                                })?;
                                dictionary.push(value.clone());
                                dictionary_ids.insert(value.clone(), id);
                                id
                            };
                            ids.push(id);
                            nulls.push(false);
                        }
                        _ => return Err(column_value_type_error(data_type, value)),
                    }
                }
                Ok(Self::Dictionary {
                    ids,
                    dictionary: Arc::from(dictionary),
                    nulls,
                })
            }
            DataType::Json
            | DataType::Vector
            | DataType::Uuid
            | DataType::Decimal
            | DataType::Date
            | DataType::Bytes => {
                let mut data = Vec::new();
                let mut offsets = Vec::with_capacity(values.len());
                for value in values {
                    match value {
                        Value::Null(actual) if *actual == data_type => {
                            offsets.push((0, 0));
                            nulls.push(true);
                        }
                        Value::Extension(encoded) if value.data_type() == data_type => {
                            value.validate_shape()?;
                            let payload = encoded.get(1..).ok_or_else(|| {
                                Error::invalid_argument(
                                    "physical extension value is missing its type tag",
                                )
                            })?;
                            let offset = u64::try_from(data.len()).map_err(|_| {
                                Error::invalid_argument(
                                    "row-group extension payload offset exceeds u64",
                                )
                            })?;
                            let length = u64::try_from(payload.len()).map_err(|_| {
                                Error::invalid_argument(
                                    "row-group extension payload length exceeds u64",
                                )
                            })?;
                            data.extend_from_slice(payload);
                            offsets.push((offset, length));
                            nulls.push(false);
                        }
                        _ => return Err(column_value_type_error(data_type, value)),
                    }
                }
                Ok(Self::Bytes {
                    data,
                    offsets,
                    ext_type: data_type,
                    nulls,
                })
            }
            DataType::Null => unreachable!("NULL rejected above"),
        }
    }

    /// Adopt one validated DATA row-group column without a `Vec<Value>`
    /// intermediate. The format owner has already checked type, NULL bitmap,
    /// offsets and extension shapes; this boundary only transfers ownership
    /// into the runtime representation.
    pub(crate) fn from_artifact(column: crate::v6::DecodedColumn) -> Self {
        match column {
            crate::v6::DecodedColumn::Int64 { values, nulls } => Self::Int64 { values, nulls },
            crate::v6::DecodedColumn::Float64 { values, nulls } => Self::Float64 { values, nulls },
            crate::v6::DecodedColumn::TimestampNanos { values, nulls } => {
                Self::TimestampNanos { values, nulls }
            }
            crate::v6::DecodedColumn::Boolean { values, nulls } => Self::Boolean { values, nulls },
            crate::v6::DecodedColumn::Text {
                ids,
                dictionary,
                nulls,
            } => Self::Dictionary {
                ids,
                dictionary: Arc::from(dictionary),
                nulls,
            },
            crate::v6::DecodedColumn::Bytes {
                data,
                offsets,
                data_type,
                nulls,
            } => Self::Bytes {
                data,
                offsets,
                ext_type: data_type,
                nulls,
            },
            crate::v6::DecodedColumn::External {
                data,
                offsets,
                type_ref,
                nulls,
            } => Self::External {
                data,
                offsets,
                type_ref,
                nulls,
            },
        }
    }

    #[inline]
    fn bool_vec_allocation_bytes(values: &Vec<bool>) -> usize {
        values.capacity().div_ceil(8)
    }

    fn allocated_memory_size(&self) -> usize {
        use std::mem::size_of;

        match self {
            ColumnData::Int64 { values, nulls } => {
                values.capacity() * size_of::<i64>() + Self::bool_vec_allocation_bytes(nulls)
            }
            ColumnData::Float64 { values, nulls } => {
                values.capacity() * size_of::<f64>() + Self::bool_vec_allocation_bytes(nulls)
            }
            ColumnData::TimestampNanos { values, nulls } => {
                values.capacity() * size_of::<i64>() + Self::bool_vec_allocation_bytes(nulls)
            }
            ColumnData::Boolean { values, nulls } => {
                Self::bool_vec_allocation_bytes(values) + Self::bool_vec_allocation_bytes(nulls)
            }
            ColumnData::Dictionary {
                ids,
                dictionary,
                nulls,
            } => {
                let dictionary_bytes = dictionary.len() * size_of::<SmartString>()
                    + dictionary.iter().map(|value| value.len()).sum::<usize>();
                ids.capacity() * size_of::<u32>()
                    + dictionary_bytes
                    + Self::bool_vec_allocation_bytes(nulls)
            }
            ColumnData::Bytes {
                data,
                offsets,
                nulls,
                ..
            } => {
                data.capacity()
                    + offsets.capacity() * size_of::<(u64, u64)>()
                    + Self::bool_vec_allocation_bytes(nulls)
            }
            ColumnData::External {
                data,
                offsets,
                nulls,
                ..
            } => {
                data.capacity()
                    + offsets.capacity() * size_of::<(u64, u64)>()
                    + Self::bool_vec_allocation_bytes(nulls)
            }
        }
    }

    /// Estimate retained allocations owned by this decoded column.
    pub fn memory_size(&self) -> usize {
        self.allocated_memory_size()
    }

    /// Compute a zone map (min/max) for a range [start, end) of this column.
    /// Uses typed comparisons directly on the underlying arrays to avoid
    /// constructing Value objects for every row.
    pub fn zone_map_for_range(&self, start: usize, end: usize) -> ZoneMap {
        let mut null_count = 0u32;
        let row_count = (end - start) as u32;
        match self {
            ColumnData::Int64 { values, nulls } => {
                let mut min_val = i64::MAX;
                let mut max_val = i64::MIN;
                let mut has_value = false;
                for i in start..end {
                    if nulls[i] {
                        null_count += 1;
                    } else {
                        let v = values[i];
                        if !has_value || v < min_val {
                            min_val = v;
                        }
                        if !has_value || v > max_val {
                            max_val = v;
                        }
                        has_value = true;
                    }
                }
                if has_value {
                    ZoneMap {
                        min: Value::Integer(min_val),
                        max: Value::Integer(max_val),
                        null_count,
                        row_count,
                    }
                } else {
                    ZoneMap {
                        min: Value::Null(DataType::Integer),
                        max: Value::Null(DataType::Integer),
                        null_count,
                        row_count,
                    }
                }
            }
            ColumnData::Float64 { values, nulls } => {
                let mut min_val = f64::INFINITY;
                let mut max_val = f64::NEG_INFINITY;
                let mut has_value = false;
                for i in start..end {
                    if nulls[i] {
                        null_count += 1;
                    } else {
                        let v = values[i];
                        // Skip NaN: NaN comparisons are always false, so a NaN
                        // in min/max would make the zone map reject valid rows.
                        if v.is_nan() {
                            continue;
                        }
                        if !has_value || v < min_val {
                            min_val = v;
                        }
                        if !has_value || v > max_val {
                            max_val = v;
                        }
                        has_value = true;
                    }
                }
                if has_value {
                    ZoneMap {
                        min: Value::Float(min_val),
                        max: Value::Float(max_val),
                        null_count,
                        row_count,
                    }
                } else {
                    ZoneMap {
                        min: Value::Null(DataType::Float),
                        max: Value::Null(DataType::Float),
                        null_count,
                        row_count,
                    }
                }
            }
            ColumnData::TimestampNanos { values, nulls } => {
                let mut min_val = i64::MAX;
                let mut max_val = i64::MIN;
                let mut has_value = false;
                for i in start..end {
                    if nulls[i] {
                        null_count += 1;
                    } else {
                        let v = values[i];
                        if !has_value || v < min_val {
                            min_val = v;
                        }
                        if !has_value || v > max_val {
                            max_val = v;
                        }
                        has_value = true;
                    }
                }
                if has_value {
                    let to_ts = |nanos: i64| -> Value {
                        let secs = nanos.div_euclid(1_000_000_000);
                        let sub = nanos.rem_euclid(1_000_000_000) as u32;
                        match chrono::TimeZone::timestamp_opt(&chrono::Utc, secs, sub) {
                            chrono::LocalResult::Single(dt) => Value::Timestamp(dt),
                            _ => Value::Null(DataType::Timestamp),
                        }
                    };
                    ZoneMap {
                        min: to_ts(min_val),
                        max: to_ts(max_val),
                        null_count,
                        row_count,
                    }
                } else {
                    ZoneMap {
                        min: Value::Null(DataType::Timestamp),
                        max: Value::Null(DataType::Timestamp),
                        null_count,
                        row_count,
                    }
                }
            }
            ColumnData::Boolean { values, nulls } => {
                let mut has_true = false;
                let mut has_false = false;
                for i in start..end {
                    if nulls[i] {
                        null_count += 1;
                    } else if values[i] {
                        has_true = true;
                    } else {
                        has_false = true;
                    }
                }
                let (min, max) = if has_false && has_true {
                    (Value::Boolean(false), Value::Boolean(true))
                } else if has_false {
                    (Value::Boolean(false), Value::Boolean(false))
                } else if has_true {
                    (Value::Boolean(true), Value::Boolean(true))
                } else {
                    (
                        Value::Null(DataType::Boolean),
                        Value::Null(DataType::Boolean),
                    )
                };
                ZoneMap {
                    min,
                    max,
                    null_count,
                    row_count,
                }
            }
            ColumnData::Dictionary {
                ids,
                dictionary,
                nulls,
            } => {
                let mut min_str: Option<&str> = None;
                let mut max_str: Option<&str> = None;
                for i in start..end {
                    if nulls[i] {
                        null_count += 1;
                    } else {
                        let s = dictionary[ids[i] as usize].as_str();
                        min_str = Some(match min_str {
                            Some(cur) if cur <= s => cur,
                            _ => s,
                        });
                        max_str = Some(match max_str {
                            Some(cur) if cur >= s => cur,
                            _ => s,
                        });
                    }
                }
                if let (Some(mn), Some(mx)) = (min_str, max_str) {
                    ZoneMap {
                        min: Value::text(mn),
                        max: Value::text(mx),
                        null_count,
                        row_count,
                    }
                } else {
                    ZoneMap {
                        min: Value::Null(DataType::Text),
                        max: Value::Null(DataType::Text),
                        null_count,
                        row_count,
                    }
                }
            }
            ColumnData::Bytes {
                nulls, ext_type, ..
            } => {
                // Extension types: no meaningful min/max ordering
                for is_null in &nulls[start..end] {
                    if *is_null {
                        null_count += 1;
                    }
                }
                ZoneMap {
                    min: Value::Null(*ext_type),
                    max: Value::Null(*ext_type),
                    null_count,
                    row_count,
                }
            }
            ColumnData::External { nulls, .. } => {
                for is_null in &nulls[start..end] {
                    if *is_null {
                        null_count += 1;
                    }
                }
                ZoneMap {
                    min: Value::null_unknown(),
                    max: Value::null_unknown(),
                    null_count,
                    row_count,
                }
            }
        }
    }

    /// Get the number of rows in this column.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Int64 { values, .. } => values.len(),
            ColumnData::Float64 { values, .. } => values.len(),
            ColumnData::TimestampNanos { values, .. } => values.len(),
            ColumnData::Boolean { values, .. } => values.len(),
            ColumnData::Dictionary { ids, .. } => ids.len(),
            ColumnData::Bytes { offsets, .. } => offsets.len(),
            ColumnData::External { offsets, .. } => offsets.len(),
        }
    }

    /// Get the DataType for this column.
    #[inline]
    pub fn data_type(&self) -> DataType {
        match self {
            ColumnData::Int64 { .. } => DataType::Integer,
            ColumnData::Float64 { .. } => DataType::Float,
            ColumnData::TimestampNanos { .. } => DataType::Timestamp,
            ColumnData::Boolean { .. } => DataType::Boolean,
            ColumnData::Dictionary { .. } => DataType::Text,
            ColumnData::Bytes { ext_type, .. } => *ext_type,
            ColumnData::External { .. } => DataType::Null,
        }
    }

    /// Return the exact logical type identity carried by this column.
    #[inline]
    pub fn logical_type(&self) -> LogicalTypeRef {
        match self {
            ColumnData::External { type_ref, .. } => LogicalTypeRef::External(*type_ref),
            _ => LogicalTypeRef::Builtin(self.data_type()),
        }
    }

    /// Check if the column is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Build a constant decoded column for schema-evolution defaults.
    ///
    /// This keeps `ALTER TABLE ADD COLUMN ... DEFAULT` readable through artifact-backed
    /// typed batches when the default value has a supported columnar
    /// representation. Unsupported extension-like types return `None` so the
    /// caller can stay on the semantic row fallback.
    pub fn constant(value: &Value, row_count: usize) -> Option<Self> {
        value.validate_shape().ok()?;
        match value {
            Value::Null(DataType::Integer) => Some(ColumnData::Int64 {
                values: vec![0; row_count],
                nulls: vec![true; row_count],
            }),
            Value::Integer(value) => Some(ColumnData::Int64 {
                values: vec![*value; row_count],
                nulls: vec![false; row_count],
            }),
            Value::Null(DataType::Float) => Some(ColumnData::Float64 {
                values: vec![0.0; row_count],
                nulls: vec![true; row_count],
            }),
            Value::Float(value) => Some(ColumnData::Float64 {
                values: vec![*value; row_count],
                nulls: vec![false; row_count],
            }),
            Value::Null(DataType::Timestamp) => Some(ColumnData::TimestampNanos {
                values: vec![0; row_count],
                nulls: vec![true; row_count],
            }),
            Value::Timestamp(value) => {
                let nanos = Value::Timestamp(*value).artifact_timestamp_nanos()?;
                Some(ColumnData::TimestampNanos {
                    values: vec![nanos; row_count],
                    nulls: vec![false; row_count],
                })
            }
            Value::Null(DataType::Boolean) => Some(ColumnData::Boolean {
                values: vec![false; row_count],
                nulls: vec![true; row_count],
            }),
            Value::Boolean(value) => Some(ColumnData::Boolean {
                values: vec![*value; row_count],
                nulls: vec![false; row_count],
            }),
            Value::Null(DataType::Text) => Some(ColumnData::Dictionary {
                ids: vec![0; row_count],
                dictionary: Arc::<[SmartString]>::from(Vec::<SmartString>::new()),
                nulls: vec![true; row_count],
            }),
            Value::Text(value) => Some(ColumnData::Dictionary {
                ids: vec![0; row_count],
                dictionary: Arc::<[SmartString]>::from(vec![value.clone()]),
                nulls: vec![false; row_count],
            }),
            Value::Null(DataType::Bytes) | Value::Null(DataType::Json) => {
                let ext_type = value.data_type();
                Some(ColumnData::Bytes {
                    data: Vec::new(),
                    offsets: vec![(0, 0); row_count],
                    ext_type,
                    nulls: vec![true; row_count],
                })
            }
            Value::Extension(data)
                if matches!(value.data_type(), DataType::Bytes | DataType::Json)
                    && data.len() > 1 =>
            {
                let payload = &data[1..];
                let mut bytes = Vec::with_capacity(payload.len().saturating_mul(row_count));
                let mut offsets = Vec::with_capacity(row_count);
                for _ in 0..row_count {
                    let offset = bytes.len() as u64;
                    bytes.extend_from_slice(payload);
                    offsets.push((offset, payload.len() as u64));
                }
                Some(ColumnData::Bytes {
                    data: bytes,
                    offsets,
                    ext_type: value.data_type(),
                    nulls: vec![false; row_count],
                })
            }
            _ => None,
        }
    }

    /// Return a row-range slice of this decoded column.
    ///
    /// The returned column starts at row zero and owns only the selected values.
    /// Dictionary columns keep sharing the immutable dictionary; bytes-like
    /// columns copy the selected byte spans into a compact contiguous buffer
    /// and rewrite offsets accordingly.
    pub fn slice_range(&self, start: usize, end: usize) -> Self {
        assert!(start <= end, "column slice start must be <= end");
        assert!(end <= self.len(), "column slice end must fit column length");
        match self {
            ColumnData::Int64 { values, nulls } => ColumnData::Int64 {
                values: values[start..end].to_vec(),
                nulls: nulls[start..end].to_vec(),
            },
            ColumnData::Float64 { values, nulls } => ColumnData::Float64 {
                values: values[start..end].to_vec(),
                nulls: nulls[start..end].to_vec(),
            },
            ColumnData::TimestampNanos { values, nulls } => ColumnData::TimestampNanos {
                values: values[start..end].to_vec(),
                nulls: nulls[start..end].to_vec(),
            },
            ColumnData::Boolean { values, nulls } => ColumnData::Boolean {
                values: values[start..end].to_vec(),
                nulls: nulls[start..end].to_vec(),
            },
            ColumnData::Dictionary {
                ids,
                dictionary,
                nulls,
            } => ColumnData::Dictionary {
                ids: ids[start..end].to_vec(),
                dictionary: Arc::clone(dictionary),
                nulls: nulls[start..end].to_vec(),
            },
            ColumnData::Bytes {
                data,
                offsets,
                ext_type,
                nulls,
            } => {
                let mut sliced_data = Vec::new();
                let mut sliced_offsets = Vec::with_capacity(end - start);
                for &(offset, len) in &offsets[start..end] {
                    let offset = usize::try_from(offset).expect("column byte offset fits usize");
                    let len = usize::try_from(len).expect("column byte length fits usize");
                    let byte_end = offset
                        .checked_add(len)
                        .expect("column byte range length does not overflow");
                    let new_offset = sliced_data.len() as u64;
                    sliced_data.extend_from_slice(&data[offset..byte_end]);
                    sliced_offsets.push((new_offset, len as u64));
                }
                ColumnData::Bytes {
                    data: sliced_data,
                    offsets: sliced_offsets,
                    ext_type: *ext_type,
                    nulls: nulls[start..end].to_vec(),
                }
            }
            ColumnData::External {
                data,
                offsets,
                type_ref,
                nulls,
            } => {
                let (data, offsets) = slice_byte_ranges(data, offsets, start..end);
                ColumnData::External {
                    data,
                    offsets,
                    type_ref: *type_ref,
                    nulls: nulls[start..end].to_vec(),
                }
            }
        }
    }

    /// Return a row-selection slice of this decoded column.
    ///
    /// `indices` are local row indices in this column. This is used by typed
    /// artifact-backed paths after row-id/visibility pruning: selected rows stay columnar,
    /// but they no longer need to be contiguous.
    pub fn select_indices(&self, indices: &[usize]) -> Self {
        debug_assert!(
            indices.iter().all(|idx| *idx < self.len()),
            "column selection indices must fit column length"
        );
        match self {
            ColumnData::Int64 { values, nulls } => ColumnData::Int64 {
                values: indices.iter().map(|&idx| values[idx]).collect(),
                nulls: indices.iter().map(|&idx| nulls[idx]).collect(),
            },
            ColumnData::Float64 { values, nulls } => ColumnData::Float64 {
                values: indices.iter().map(|&idx| values[idx]).collect(),
                nulls: indices.iter().map(|&idx| nulls[idx]).collect(),
            },
            ColumnData::TimestampNanos { values, nulls } => ColumnData::TimestampNanos {
                values: indices.iter().map(|&idx| values[idx]).collect(),
                nulls: indices.iter().map(|&idx| nulls[idx]).collect(),
            },
            ColumnData::Boolean { values, nulls } => ColumnData::Boolean {
                values: indices.iter().map(|&idx| values[idx]).collect(),
                nulls: indices.iter().map(|&idx| nulls[idx]).collect(),
            },
            ColumnData::Dictionary {
                ids,
                dictionary,
                nulls,
            } => ColumnData::Dictionary {
                ids: indices.iter().map(|&idx| ids[idx]).collect(),
                dictionary: Arc::clone(dictionary),
                nulls: indices.iter().map(|&idx| nulls[idx]).collect(),
            },
            ColumnData::Bytes {
                data,
                offsets,
                ext_type,
                nulls,
            } => {
                let mut selected_data = Vec::new();
                let mut selected_offsets = Vec::with_capacity(indices.len());
                for &idx in indices {
                    let (offset, len) = offsets[idx];
                    let offset = usize::try_from(offset).expect("column byte offset fits usize");
                    let len = usize::try_from(len).expect("column byte length fits usize");
                    let byte_end = offset
                        .checked_add(len)
                        .expect("column byte range length does not overflow");
                    let new_offset = selected_data.len() as u64;
                    selected_data.extend_from_slice(&data[offset..byte_end]);
                    selected_offsets.push((new_offset, len as u64));
                }
                ColumnData::Bytes {
                    data: selected_data,
                    offsets: selected_offsets,
                    ext_type: *ext_type,
                    nulls: indices.iter().map(|&idx| nulls[idx]).collect(),
                }
            }
            ColumnData::External {
                data,
                offsets,
                type_ref,
                nulls,
            } => {
                let (data, offsets) = select_byte_ranges(data, offsets, indices);
                ColumnData::External {
                    data,
                    offsets,
                    type_ref: *type_ref,
                    nulls: indices.iter().map(|&idx| nulls[idx]).collect(),
                }
            }
        }
    }

    /// Check if a specific row is null.
    #[inline]
    pub fn is_null(&self, idx: usize) -> bool {
        match self {
            ColumnData::Int64 { nulls, .. }
            | ColumnData::Float64 { nulls, .. }
            | ColumnData::TimestampNanos { nulls, .. }
            | ColumnData::Boolean { nulls, .. }
            | ColumnData::Dictionary { nulls, .. }
            | ColumnData::Bytes { nulls, .. }
            | ColumnData::External { nulls, .. } => nulls[idx],
        }
    }

    /// Reconstruct a Value for row `idx`.
    ///
    /// This is the "slow path" used when the executor needs a full Value
    /// (e.g., for projection into result rows). Aggregation and filtering
    /// should use the typed accessors (`get_i64`, `get_f64`) instead.
    pub fn get_value(&self, idx: usize) -> Value {
        match self {
            ColumnData::Int64 { values, nulls } => {
                if nulls[idx] {
                    Value::Null(DataType::Integer)
                } else {
                    Value::Integer(values[idx])
                }
            }
            ColumnData::Float64 { values, nulls } => {
                if nulls[idx] {
                    Value::Null(DataType::Float)
                } else {
                    Value::Float(values[idx])
                }
            }
            ColumnData::TimestampNanos { values, nulls } => {
                if nulls[idx] {
                    Value::Null(DataType::Timestamp)
                } else {
                    let nanos = values[idx];
                    let secs = nanos.div_euclid(1_000_000_000);
                    let sub_nanos = nanos.rem_euclid(1_000_000_000) as u32;
                    match chrono::TimeZone::timestamp_opt(&chrono::Utc, secs, sub_nanos) {
                        chrono::LocalResult::Single(dt) => Value::Timestamp(dt),
                        _ => Value::Null(DataType::Timestamp),
                    }
                }
            }
            ColumnData::Boolean { values, nulls } => {
                if nulls[idx] {
                    Value::Null(DataType::Boolean)
                } else {
                    Value::Boolean(values[idx])
                }
            }
            ColumnData::Dictionary {
                ids,
                dictionary,
                nulls,
            } => {
                if nulls[idx] {
                    Value::Null(DataType::Text)
                } else {
                    Value::Text(dictionary[ids[idx] as usize].clone())
                }
            }
            ColumnData::Bytes {
                data,
                offsets,
                ext_type,
                nulls,
            } => {
                if nulls[idx] {
                    Value::Null(*ext_type)
                } else {
                    let (off, len) = offsets[idx];
                    let bytes = &data[off as usize..(off + len) as usize];
                    // Reconstruct Extension value: prepend type tag
                    let mut tagged = Vec::with_capacity(1 + bytes.len());
                    tagged.push(*ext_type as u8);
                    tagged.extend_from_slice(bytes);
                    Value::Extension(radixdb_core::CompactArc::from(tagged))
                }
            }
            ColumnData::External {
                data,
                offsets,
                type_ref,
                nulls,
            } => {
                if nulls[idx] {
                    Value::null_unknown()
                } else {
                    let (off, len) = offsets[idx];
                    let bytes = &data[off as usize..(off + len) as usize];
                    Value::try_external(*type_ref, bytes)
                        .expect("validated external column payload")
                }
            }
        }
    }

    // =========================================================================
    // Fast typed accessors (no Value construction)
    // =========================================================================

    /// Get raw i64 value. Works for Int64 and TimestampNanos columns.
    /// Returns 0 on type mismatch (callers must guard with type checks).
    #[inline]
    pub fn get_i64(&self, idx: usize) -> i64 {
        match self {
            ColumnData::Int64 { values, .. } | ColumnData::TimestampNanos { values, .. } => {
                values[idx]
            }
            _ => 0,
        }
    }

    /// Get raw f64 value. Returns 0.0 on type mismatch.
    #[inline]
    pub fn get_f64(&self, idx: usize) -> f64 {
        match self {
            ColumnData::Float64 { values, .. } => values[idx],
            _ => 0.0,
        }
    }

    /// Get raw bool value. Returns false on type mismatch.
    #[inline]
    pub fn get_bool(&self, idx: usize) -> bool {
        match self {
            ColumnData::Boolean { values, .. } => values[idx],
            _ => false,
        }
    }

    /// Get dictionary string reference. Returns empty string on type mismatch.
    #[inline]
    pub fn get_str(&self, idx: usize) -> &str {
        match self {
            ColumnData::Dictionary {
                ids, dictionary, ..
            } => &dictionary[ids[idx] as usize],
            _ => "",
        }
    }

    /// Get the dictionary ID for a row. Returns u32::MAX on type mismatch.
    #[inline]
    pub fn get_dict_id(&self, idx: usize) -> u32 {
        match self {
            ColumnData::Dictionary { ids, .. } => ids[idx],
            _ => u32::MAX,
        }
    }

    // =========================================================================
    // Search operations
    // =========================================================================

    /// Binary search on sorted i64 data (timestamps, integer PKs).
    /// Returns the index of the first element >= target.
    pub fn binary_search_ge(&self, target: i64) -> usize {
        match self {
            ColumnData::Int64 { values, .. } | ColumnData::TimestampNanos { values, .. } => {
                values.partition_point(|v| *v < target)
            }
            _ => 0,
        }
    }

    /// Binary search on sorted i64 data.
    /// Returns the index of the first element > target.
    pub fn binary_search_gt(&self, target: i64) -> usize {
        match self {
            ColumnData::Int64 { values, .. } | ColumnData::TimestampNanos { values, .. } => {
                values.partition_point(|v| *v <= target)
            }
            _ => 0,
        }
    }
}

fn slice_byte_ranges(
    data: &[u8],
    offsets: &[(u64, u64)],
    range: std::ops::Range<usize>,
) -> (Vec<u8>, Vec<(u64, u64)>) {
    select_byte_ranges(data, offsets, range.collect::<Vec<_>>().as_slice())
}

fn select_byte_ranges(
    data: &[u8],
    offsets: &[(u64, u64)],
    indices: &[usize],
) -> (Vec<u8>, Vec<(u64, u64)>) {
    let mut selected_data = Vec::new();
    let mut selected_offsets = Vec::with_capacity(indices.len());
    for &index in indices {
        let (offset, length) = offsets[index];
        let start = offset as usize;
        let end = start + length as usize;
        let selected_offset = selected_data.len() as u64;
        selected_data.extend_from_slice(&data[start..end]);
        selected_offsets.push((selected_offset, length));
    }
    (selected_data, selected_offsets)
}

fn column_value_type_error(expected: DataType, actual: &Value) -> Error {
    Error::invalid_argument(format!(
        "physical column expects {expected}, got {}",
        actual.data_type()
    ))
}

/// Simple bloom filter for fast membership testing on column values.
///
/// Used to quickly determine if a value MIGHT exist in a volume column.
/// False positives are possible, false negatives are not.
#[derive(Debug, Clone)]
pub struct ColumnBloomFilter {
    /// Bitset
    bits: Vec<u64>,
    /// Number of bits
    num_bits: usize,
    /// Value-level membership must fail open when the filter has no type
    /// provenance or contains a representation with canonical aliases.
    ///
    /// This flag is intentionally not persisted. Decoded filters always set it
    /// because the historical byte format carries no column type. Engine
    /// pruning uses the separately guarded raw-hash API.
    value_api_fail_open: bool,
}

impl ColumnBloomFilter {
    /// Default cap for persisted bloom filters.
    ///
    /// A pure `10 bits * row_count` policy becomes target-sized metadata on
    /// 100M/1B volumes. The cap preserves correctness (never false-negative)
    /// while preventing bloom metadata from scaling linearly without bound.
    pub const DEFAULT_MAX_BITS: usize = 8 * 1024 * 1024;

    /// Whether current writers should create a bloom for this column type.
    #[inline]
    pub fn supports_bloom_creation(data_type: DataType) -> bool {
        matches!(
            data_type,
            DataType::Integer | DataType::Timestamp | DataType::Boolean | DataType::Text
        )
    }

    /// Whether a persisted bloom negative is authoritative for this probe
    /// under the current scalar equality contract.
    ///
    /// Float bloom bytes historically hash raw IEEE bits, so canonical-equal
    /// signed zero and NaN payloads can occupy different bits. Mixed-domain
    /// scalar comparison can also equate differently hashed representations.
    /// Old artifacts remain readable, but only exact storage/probe domains may
    /// use a bloom negative to prune a volume.
    #[inline]
    pub fn supports_definitive_pruning(data_type: DataType, probe: &Value) -> bool {
        matches!(
            (data_type, probe),
            (DataType::Integer, Value::Integer(_))
                | (DataType::Timestamp, Value::Timestamp(_))
                | (DataType::Boolean, Value::Boolean(_))
                | (DataType::Text, Value::Text(_))
        )
    }

    /// Estimate in-memory size of this bloom filter in bytes.
    pub fn memory_size(&self) -> usize {
        if self.is_disabled() {
            0
        } else {
            self.bits.capacity() * std::mem::size_of::<u64>()
        }
    }

    /// Create a bloom filter sized for the expected number of elements.
    /// Uses ~10 bits per element with 3 hash functions (~1.7% false positive rate).
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn new(expected_elements: usize) -> Self {
        let num_bits = expected_elements.saturating_mul(10).max(64);
        Self::with_num_bits(num_bits)
    }

    /// Create a bloom filter sized for the expected number of elements, capped
    /// to a maximum bit count.
    ///
    /// When the cap is hit, false positives increase but correctness is
    /// unchanged: bloom filters are only allowed to say "definitely not" when
    /// the bitset proves it.
    pub fn new_capped(expected_elements: usize, max_bits: usize) -> Self {
        if max_bits < 64 {
            return Self::always_maybe();
        }
        let requested_bits = expected_elements.saturating_mul(10).max(64);
        Self::with_num_bits(requested_bits.min(max_bits))
    }

    fn with_num_bits(num_bits: usize) -> Self {
        let num_words = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; num_words],
            num_bits,
            value_api_fail_open: false,
        }
    }

    /// Create a disabled bloom filter that never prunes.
    ///
    /// This is useful for extension/opaque columns where the current persisted
    /// hash cannot distinguish individual values. Returning "maybe" for every
    /// probe preserves correctness while avoiding a large but ineffective
    /// per-row bitset in metadata.
    pub fn always_maybe() -> Self {
        Self {
            bits: Vec::new(),
            num_bits: 0,
            value_api_fail_open: true,
        }
    }

    #[inline]
    pub fn is_disabled(&self) -> bool {
        self.num_bits == 0 || self.bits.is_empty()
    }

    /// Add raw bytes with a type tag to the bloom filter (avoids Value allocation).
    fn add_raw(&mut self, tag: u8, bytes: &[u8]) {
        if self.is_disabled() {
            return;
        }

        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;
        let mut h = FNV_OFFSET;
        h ^= tag as u64;
        h = h.wrapping_mul(FNV_PRIME);
        for &b in bytes {
            h ^= b as u64;
            h = h.wrapping_mul(FNV_PRIME);
        }
        self.insert_hash(h);
    }

    /// Add an i64 value (for Integer and TimestampNanos columns).
    pub fn add_i64(&mut self, val: i64) {
        self.add_raw(1, &val.to_le_bytes());
    }

    /// Add an f64 value (for Float columns).
    pub fn add_f64(&mut self, val: f64) {
        self.value_api_fail_open = true;
        self.add_raw(2, &val.to_bits().to_le_bytes());
    }

    /// Add a string value (for Dictionary/Text columns).
    pub fn add_str(&mut self, val: &str) {
        self.add_raw(3, val.as_bytes());
    }

    /// Add a bool value (tag 4, matching hash_value for Value::Boolean).
    pub fn add_bool(&mut self, val: bool) {
        self.add_raw(4, &[val as u8]);
    }

    /// Add a timestamp value as nanoseconds (tag 5, matching hash_value for Value::Timestamp).
    pub fn add_timestamp_nanos(&mut self, nanos: i64) {
        self.add_raw(5, &nanos.to_le_bytes());
    }

    fn insert_hash(&mut self, h: u64) {
        if self.is_disabled() {
            return;
        }

        let h1 = h as usize;
        let h2 = (h >> 32) as usize;
        for i in 0..3usize {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2))) % self.num_bits;
            self.bits[bit / 64] |= 1u64 << (bit % 64);
        }
    }

    /// Add a value to the bloom filter.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn add(&mut self, value: &Value) {
        if matches!(value, Value::Float(_)) || value.as_decimal_parts().is_some() {
            self.value_api_fail_open = true;
        }
        let h = Self::hash_value(value);
        self.insert_hash(h);
    }

    /// Check if a value MIGHT exist in the filter.
    /// Returns false only if the value is definitely not present.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn might_contain(&self, value: &Value) -> bool {
        if self.value_api_fail_open
            || matches!(value, Value::Float(_))
            || value.as_decimal_parts().is_some()
        {
            return true;
        }
        self.might_contain_hash(Self::hash_value(value))
    }

    /// Check bloom filter using a pre-computed hash.
    /// Use `hash_value_static` to compute the hash once, then pass it to
    /// multiple volumes to avoid redundant hashing of the same value.
    #[inline]
    pub fn might_contain_hash(&self, h: u64) -> bool {
        if self.is_disabled() {
            return true;
        }

        let h1 = h as usize;
        let h2 = (h >> 32) as usize;
        for i in 0..3usize {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2))) % self.num_bits;
            if self.bits[bit / 64] & (1u64 << (bit % 64)) == 0 {
                return false;
            }
        }
        true
    }

    /// Compute bloom filter hash for a Value. Can be called once and reused
    /// across multiple volumes via `might_contain_hash`.
    pub fn hash_value_static(value: &Value) -> u64 {
        Self::hash_value(value)
    }

    /// Return the logical number of bits in the filter (needed for serialization).
    #[inline]
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Serialize the bitset to bytes in little-endian format.
    pub fn bits_as_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.bits.len() * 8);
        for &word in &self.bits {
            out.extend_from_slice(&word.to_le_bytes());
        }
        out
    }

    /// Reconstruct a bloom filter from serialized parts.
    ///
    /// `num_bits` is the logical bit count; `bytes` is the raw bitset written
    /// by `bits_as_bytes()` (little-endian u64 words).
    pub fn from_parts(num_bits: usize, bytes: &[u8]) -> Self {
        if num_bits == 0 || bytes.is_empty() {
            return Self::always_maybe();
        }

        let num_words = bytes.len() / 8;
        let mut bits = Vec::with_capacity(num_words);
        for i in 0..num_words {
            let offset = i * 8;
            bits.push(u64::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
                bytes[offset + 5],
                bytes[offset + 6],
                bytes[offset + 7],
            ]));
        }
        // Clamp num_bits to the actual backing storage to prevent OOB in might_contain.
        let max_bits = num_words * 64;
        let safe_num_bits = if num_bits > max_bits {
            max_bits
        } else {
            num_bits
        };
        Self {
            bits,
            num_bits: safe_num_bits,
            value_api_fail_open: true,
        }
    }

    /// Stable FNV-1a hash that is deterministic across Rust versions.
    ///
    /// Bloom filter bits are persisted to disk, so the hash function MUST
    /// produce identical output regardless of Rust toolchain version.
    /// `std::collections::hash_map::DefaultHasher` does NOT guarantee this.
    fn hash_value(value: &Value) -> u64 {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let mut h = FNV_OFFSET;

        #[inline(always)]
        fn mix(h: &mut u64, bytes: &[u8]) {
            for &b in bytes {
                *h ^= b as u64;
                *h = h.wrapping_mul(FNV_PRIME);
            }
        }

        match value {
            Value::Integer(i) => {
                mix(&mut h, &[1]);
                mix(&mut h, &i.to_le_bytes());
            }
            Value::Float(f) => {
                mix(&mut h, &[2]);
                mix(&mut h, &f.to_bits().to_le_bytes());
            }
            Value::Text(s) => {
                mix(&mut h, &[3]);
                mix(&mut h, s.as_bytes());
            }
            Value::Boolean(b) => {
                mix(&mut h, &[4]);
                mix(&mut h, &[*b as u8]);
            }
            Value::Timestamp(ts) => {
                mix(&mut h, &[5]);
                let nanos = ts.timestamp_nanos_opt().unwrap_or_else(|| {
                    ts.timestamp()
                        .wrapping_mul(1_000_000_000)
                        .wrapping_add(ts.timestamp_subsec_nanos() as i64)
                });
                mix(&mut h, &nanos.to_le_bytes());
            }
            _ => {
                // Extension (JSON, Vector) and Null all hash to tag 0.
                // New extension columns use disabled/always-maybe bloom filters
                // instead of a dense no-op bitset. Keep this hash stable so old
                // persisted volumes that already have tag-0 extension bloom bits
                // remain readable with the same pruning semantics.
                mix(&mut h, &[0]);
            }
        }
        h
    }
}

impl ZoneMap {
    /// Float zone maps in existing artifact-backed artifacts do not record whether NaN was
    /// present because min/max construction historically skipped NaN values.
    /// Under the canonical scalar order NaN is a real, canonical-last value,
    /// so no Float min/max interval can be used as definitive negative
    /// evidence without an explicit `has_nan` bit. Fail open while preserving
    /// the persisted layout.
    #[inline]
    fn float_metadata_is_non_definitive(&self, probe: &Value) -> bool {
        matches!(self.min, Value::Float(_))
            || matches!(self.max, Value::Float(_))
            || matches!(probe, Value::Float(_))
    }

    /// A typed NULL bound can mean either "all rows are NULL" or "this
    /// physical column has no order-preserving metadata". Extension-backed
    /// columns (UUID/Decimal/Date/Bytes/JSON/Vector) use the latter form for
    /// row-group metadata. Only `null_count == row_count` is definitive proof
    /// that no scalar predicate can match.
    #[inline]
    fn missing_bounds_are_non_definitive(&self) -> bool {
        (self.min.is_null() || self.max.is_null()) && self.null_count < self.row_count
    }

    /// Check if a predicate `column >= value` can possibly match any row.
    /// Returns false if we can definitively skip this volume.
    #[inline]
    pub fn may_contain_gte(&self, value: &Value) -> bool {
        if self.float_metadata_is_non_definitive(value) || self.missing_bounds_are_non_definitive()
        {
            return true;
        }
        if self.max.is_null() {
            return false; // all nulls
        }
        // max >= value means some rows might match
        self.max
            .compare(value)
            .map(|o| o != std::cmp::Ordering::Less)
            .unwrap_or(true) // on comparison error, don't skip
    }

    /// Check if a predicate `column > value` can possibly match any row.
    #[inline]
    pub fn may_contain_gt(&self, value: &Value) -> bool {
        if self.float_metadata_is_non_definitive(value) || self.missing_bounds_are_non_definitive()
        {
            return true;
        }
        if self.max.is_null() {
            return false;
        }
        self.max
            .compare(value)
            .map(|ordering| ordering == std::cmp::Ordering::Greater)
            .unwrap_or(true)
    }

    /// Check if a predicate `column <= value` can possibly match any row.
    #[inline]
    pub fn may_contain_lte(&self, value: &Value) -> bool {
        if self.float_metadata_is_non_definitive(value) || self.missing_bounds_are_non_definitive()
        {
            return true;
        }
        if self.min.is_null() {
            return false;
        }
        self.min
            .compare(value)
            .map(|o| o != std::cmp::Ordering::Greater)
            .unwrap_or(true)
    }

    /// Check if a predicate `column < value` can possibly match any row.
    #[inline]
    pub fn may_contain_lt(&self, value: &Value) -> bool {
        if self.float_metadata_is_non_definitive(value) || self.missing_bounds_are_non_definitive()
        {
            return true;
        }
        if self.min.is_null() {
            return false;
        }
        self.min
            .compare(value)
            .map(|ordering| ordering == std::cmp::Ordering::Less)
            .unwrap_or(true)
    }

    /// Check if a predicate `column = value` can possibly match any row.
    #[inline]
    pub fn may_contain_eq(&self, value: &Value) -> bool {
        if self.float_metadata_is_non_definitive(value) || self.missing_bounds_are_non_definitive()
        {
            return true;
        }
        if self.min.is_null() {
            return false;
        }
        let above_min = self
            .min
            .compare(value)
            .map(|o| o != std::cmp::Ordering::Greater)
            .unwrap_or(true);
        let below_max = self
            .max
            .compare(value)
            .map(|o| o != std::cmp::Ordering::Less)
            .unwrap_or(true);
        above_min && below_max
    }

    /// Check if a predicate `column BETWEEN low AND high` can possibly match.
    #[inline]
    pub fn may_contain_between(&self, low: &Value, high: &Value) -> bool {
        self.may_contain_gte(low) && self.may_contain_lte(high)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoded_row_group_converts_to_typed_columns_without_full_segment_state() {
        let cases = vec![
            (
                DataType::Integer,
                vec![Value::Integer(7), Value::Null(DataType::Integer)],
            ),
            (
                DataType::Float,
                vec![Value::Float(1.5), Value::Null(DataType::Float)],
            ),
            (
                DataType::Text,
                vec![
                    Value::text("same"),
                    Value::text("same"),
                    Value::Null(DataType::Text),
                ],
            ),
            (
                DataType::Boolean,
                vec![Value::Boolean(true), Value::Null(DataType::Boolean)],
            ),
            (
                DataType::Timestamp,
                vec![
                    Value::Timestamp(chrono::DateTime::from_timestamp(17, 23).unwrap()),
                    Value::Null(DataType::Timestamp),
                ],
            ),
            (
                DataType::Bytes,
                vec![Value::bytes(vec![1, 2, 3]), Value::Null(DataType::Bytes)],
            ),
        ];

        for (data_type, values) in cases {
            let column = ColumnData::try_from_values(data_type, &values).unwrap();
            assert_eq!(column.len(), values.len());
            for (index, expected) in values.iter().enumerate() {
                assert_eq!(column.get_value(index), *expected);
            }
        }
    }

    #[test]
    fn decoded_row_group_rejects_wrong_or_untyped_null_values() {
        assert!(ColumnData::try_from_values(DataType::Integer, &[Value::Float(1.0)]).is_err());
        assert!(
            ColumnData::try_from_values(DataType::Integer, &[Value::Null(DataType::Null)]).is_err()
        );
        assert!(ColumnData::try_from_values(DataType::Null, &[]).is_err());
    }

    #[test]
    fn test_disabled_bloom_filter_is_always_maybe_and_zero_sized() {
        let bloom = ColumnBloomFilter::always_maybe();
        assert!(bloom.is_disabled());
        assert_eq!(bloom.memory_size(), 0);
        assert_eq!(bloom.num_bits(), 0);
        assert!(bloom.bits_as_bytes().is_empty());
        assert!(bloom.might_contain(&Value::Integer(42)));
        assert!(bloom.might_contain(&Value::text("missing")));

        let decoded = ColumnBloomFilter::from_parts(0, &[]);
        assert!(decoded.is_disabled());
        assert!(decoded.might_contain(&Value::Float(3.125)));
    }

    #[test]
    fn test_column_data_slice_range_rewrites_bytes_offsets() {
        let col = ColumnData::Bytes {
            data: b"alphabetaomega".to_vec(),
            offsets: vec![(0, 5), (5, 4), (9, 0), (9, 5)],
            ext_type: DataType::Bytes,
            nulls: vec![false, false, true, false],
        };

        let sliced = col.slice_range(1, 4);
        match sliced {
            ColumnData::Bytes {
                data,
                offsets,
                ext_type,
                nulls,
            } => {
                assert_eq!(data, b"betaomega");
                assert_eq!(offsets, vec![(0, 4), (4, 0), (4, 5)]);
                assert_eq!(ext_type, DataType::Bytes);
                assert_eq!(nulls, vec![false, true, false]);
            }
            _ => panic!("sliced bytes column must stay bytes"),
        }

        let empty = col.slice_range(2, 2);
        match empty {
            ColumnData::Bytes {
                data,
                offsets,
                nulls,
                ..
            } => {
                assert!(data.is_empty());
                assert!(offsets.is_empty());
                assert!(nulls.is_empty());
            }
            _ => panic!("empty bytes slice must stay bytes"),
        }
    }

    #[test]
    fn test_column_data_select_indices_rewrites_bytes_offsets() {
        let col = ColumnData::Bytes {
            data: b"alphabetaomega".to_vec(),
            offsets: vec![(0, 5), (5, 4), (9, 0), (9, 5)],
            ext_type: DataType::Json,
            nulls: vec![false, false, true, false],
        };

        let selected = col.select_indices(&[3, 0, 2]);
        match selected {
            ColumnData::Bytes {
                data,
                offsets,
                ext_type,
                nulls,
            } => {
                assert_eq!(data, b"omegaalpha");
                assert_eq!(offsets, vec![(0, 5), (5, 5), (10, 0)]);
                assert_eq!(ext_type, DataType::Json);
                assert_eq!(nulls, vec![false, false, true]);
            }
            _ => panic!("selected bytes column must stay bytes"),
        }
    }

    #[test]
    fn test_column_data_constant_builds_supported_default_columns() {
        match ColumnData::constant(&Value::Integer(42), 3).expect("integer constant") {
            ColumnData::Int64 { values, nulls } => {
                assert_eq!(values, vec![42, 42, 42]);
                assert_eq!(nulls, vec![false, false, false]);
            }
            _ => panic!("integer default must build Int64 column"),
        }

        match ColumnData::constant(&Value::Null(DataType::Text), 2).expect("null text constant") {
            ColumnData::Dictionary {
                ids,
                dictionary,
                nulls,
            } => {
                assert_eq!(ids, vec![0, 0]);
                assert!(dictionary.is_empty());
                assert_eq!(nulls, vec![true, true]);
            }
            _ => panic!("NULL TEXT default must build dictionary column"),
        }

        match ColumnData::constant(&Value::json(r#"{"kind":"default"}"#), 2).expect("json constant")
        {
            ColumnData::Bytes {
                data,
                offsets,
                ext_type,
                nulls,
            } => {
                assert_eq!(data, br#"{"kind":"default"}{"kind":"default"}"#);
                assert_eq!(offsets, vec![(0, 18), (18, 18)]);
                assert_eq!(ext_type, DataType::Json);
                assert_eq!(nulls, vec![false, false]);
            }
            _ => panic!("JSON default must build bytes-like column"),
        }

        assert!(
            ColumnData::constant(&Value::uuid([1; 16]), 1).is_none(),
            "unsupported extension defaults must stay on row fallback"
        );
    }

    #[test]
    fn test_capped_bloom_filter_limits_metadata_without_false_negative() {
        let mut bloom = ColumnBloomFilter::new_capped(1_000_000, 1024);
        assert!(!bloom.is_disabled());
        assert_eq!(bloom.num_bits(), 1024);
        bloom.add_i64(42);
        assert!(bloom.might_contain(&Value::Integer(42)));

        let disabled = ColumnBloomFilter::new_capped(1_000_000, 0);
        assert!(disabled.is_disabled());
        assert!(disabled.might_contain(&Value::Integer(42)));
    }

    #[test]
    fn test_value_membership_fails_open_for_canonical_aliases_and_decoded_filters() {
        let mut float_bloom = ColumnBloomFilter::new(4096);
        float_bloom.add_f64(-0.0);
        assert!(float_bloom.might_contain(&Value::Float(0.0)));
        assert!(float_bloom.might_contain(&Value::Integer(0)));

        let mut integer_bloom = ColumnBloomFilter::new(4096);
        integer_bloom.add_i64(42);
        assert!(integer_bloom.might_contain(&Value::Float(42.0)));

        let mut decimal_bloom = ColumnBloomFilter::new(4096);
        decimal_bloom.add(&Value::decimal(10, 2, 1));
        assert!(decimal_bloom.might_contain(&Value::decimal(100, 3, 2)));
        assert!(decimal_bloom.might_contain(&Value::Integer(1)));

        let decoded = ColumnBloomFilter::from_parts(64, &[0; 8]);
        assert!(
            !decoded.might_contain_hash(ColumnBloomFilter::hash_value_static(&Value::Integer(7)))
        );
        assert!(decoded.might_contain(&Value::Integer(7)));
    }

    #[test]
    fn test_int64_column() {
        let col = ColumnData::Int64 {
            values: vec![10, 20, 30, 0, 50],
            nulls: vec![false, false, false, true, false],
        };
        assert_eq!(col.len(), 5);
        assert_eq!(col.get_i64(0), 10);
        assert_eq!(col.get_i64(2), 30);
        assert!(col.is_null(3));
        assert!(!col.is_null(0));

        // get_value
        assert_eq!(col.get_value(0), Value::Integer(10));
        assert!(col.get_value(3).is_null());
    }

    #[test]
    fn test_float64_column() {
        let col = ColumnData::Float64 {
            values: vec![1.5, 2.5, 0.0],
            nulls: vec![false, false, true],
        };
        assert_eq!(col.get_f64(0), 1.5);
        assert_eq!(col.get_value(2), Value::Null(DataType::Float));
    }

    #[test]
    fn test_dictionary_column() {
        let col = ColumnData::Dictionary {
            ids: vec![0, 1, 0, 1, 0],
            dictionary: Arc::from(vec![
                SmartString::from("binance"),
                SmartString::from("coinbase"),
            ]),
            nulls: vec![false, false, false, false, false],
        };
        assert_eq!(col.get_str(0), "binance");
        assert_eq!(col.get_str(1), "coinbase");
        assert_eq!(col.get_str(2), "binance");
        assert_eq!(col.get_dict_id(0), 0);
        assert_eq!(col.get_dict_id(1), 1);
    }

    #[test]
    fn test_binary_search() {
        let col = ColumnData::TimestampNanos {
            values: vec![100, 200, 300, 400, 500],
            nulls: vec![false; 5],
        };
        assert_eq!(col.binary_search_ge(250), 2); // first >= 250 is index 2 (300)
        assert_eq!(col.binary_search_ge(300), 2); // first >= 300 is index 2 (300)
        assert_eq!(col.binary_search_ge(100), 0); // first >= 100 is index 0
        assert_eq!(col.binary_search_ge(600), 5); // nothing >= 600
        assert_eq!(col.binary_search_gt(300), 3); // first > 300 is index 3 (400)
    }

    #[test]
    fn test_zone_map_pruning() {
        let zm = ZoneMap {
            min: Value::Integer(10),
            max: Value::Integer(100),
            null_count: 0,
            row_count: 50,
        };
        assert!(zm.may_contain_gte(&Value::Integer(50))); // 100 >= 50
        assert!(zm.may_contain_gte(&Value::Integer(100))); // 100 >= 100
        assert!(!zm.may_contain_gte(&Value::Integer(101))); // 100 < 101
        assert!(zm.may_contain_gt(&Value::Integer(99))); // 100 > 99
        assert!(!zm.may_contain_gt(&Value::Integer(100))); // 100 is not > 100
        assert!(zm.may_contain_lte(&Value::Integer(50))); // 10 <= 50
        assert!(zm.may_contain_lte(&Value::Integer(10))); // 10 <= 10
        assert!(!zm.may_contain_lte(&Value::Integer(9))); // 10 > 9
        assert!(zm.may_contain_lt(&Value::Integer(11))); // 10 < 11
        assert!(!zm.may_contain_lt(&Value::Integer(10))); // 10 is not < 10
        assert!(zm.may_contain_eq(&Value::Integer(50)));
        assert!(!zm.may_contain_eq(&Value::Integer(5)));
        assert!(!zm.may_contain_eq(&Value::Integer(101)));
    }

    #[test]
    fn test_float_zone_map_is_fail_open_without_persisted_nan_presence() {
        let zm = ZoneMap {
            min: Value::Float(1.0),
            max: Value::Float(2.0),
            null_count: 0,
            row_count: 3,
        };

        // The third row may have been NaN and omitted from historical min/max
        // metadata. Canonical NaN ordering/equality therefore makes every
        // Float negative non-definitive.
        let nan = Value::Float(f64::from_bits(0x7ff8_0000_0000_0042));
        assert!(zm.may_contain_eq(&nan));
        assert!(zm.may_contain_gte(&Value::Float(3.0)));
        assert!(zm.may_contain_gt(&Value::Float(3.0)));
        assert!(zm.may_contain_lte(&Value::Float(0.0)));
        assert!(zm.may_contain_lt(&Value::Float(0.0)));
        assert!(zm.may_contain_eq(&Value::Integer(3)));

        let historical_all_nan = ZoneMap {
            min: Value::Null(DataType::Float),
            max: Value::Null(DataType::Float),
            null_count: 0,
            row_count: 1,
        };
        assert!(historical_all_nan.may_contain_eq(&nan));
        assert!(historical_all_nan.may_contain_gte(&Value::Float(0.0)));
        assert!(historical_all_nan.may_contain_gt(&Value::Float(0.0)));
    }

    #[test]
    fn extension_zone_map_without_bounds_is_fail_open_unless_all_null() {
        let uuid = Value::uuid([7; 16]);
        let non_null_extension_group = ZoneMap {
            min: Value::Null(DataType::Uuid),
            max: Value::Null(DataType::Uuid),
            null_count: 0,
            row_count: 65_536,
        };
        assert!(non_null_extension_group.may_contain_eq(&uuid));
        assert!(non_null_extension_group.may_contain_gte(&uuid));
        assert!(non_null_extension_group.may_contain_gt(&uuid));
        assert!(non_null_extension_group.may_contain_lte(&uuid));
        assert!(non_null_extension_group.may_contain_lt(&uuid));

        let all_null_group = ZoneMap {
            min: Value::Null(DataType::Uuid),
            max: Value::Null(DataType::Uuid),
            null_count: 65_536,
            row_count: 65_536,
        };
        assert!(!all_null_group.may_contain_eq(&uuid));
        assert!(!all_null_group.may_contain_gte(&uuid));
        assert!(!all_null_group.may_contain_gt(&uuid));
        assert!(!all_null_group.may_contain_lte(&uuid));
        assert!(!all_null_group.may_contain_lt(&uuid));
    }

    #[test]
    fn timestamp_constant_rejects_values_outside_artifact_nanoseconds() {
        let far_future = chrono::TimeZone::timestamp_opt(&chrono::Utc, 253_402_300_799, 0)
            .single()
            .unwrap();
        assert!(ColumnData::constant(&Value::Timestamp(far_future), 1).is_none());
    }
}
