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

//! Value type for RadixDB - runtime values with type information
//!
//! This module provides a unified Value enum that represents SQL values
//! with full type information and conversion capabilities.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use uuid::Uuid;

use super::error::{Error, Result};
use super::types::{DataType, ExternalTypeRef, LogicalTypeRef};
use crate::{CompactArc, SmartString};

const EXTERNAL_VALUE_MARKER: u8 = 0xff;
const EXTERNAL_VALUE_HEADER_BYTES: usize = 1 + 16 + 4;
pub const MAX_EXTERNAL_VALUE_BYTES: usize = 16 * 1024 * 1024;

/// Borrowed view of the canonical payload carried by an external value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternalValueRef<'a> {
    type_ref: ExternalTypeRef,
    payload: &'a [u8],
}

impl<'a> ExternalValueRef<'a> {
    pub const fn type_ref(self) -> ExternalTypeRef {
        self.type_ref
    }

    pub const fn payload(self) -> &'a [u8] {
        self.payload
    }
}

/// Timestamp formats supported for parsing
/// Order matters - more specific formats first
const TIMESTAMP_FORMATS: &[&str] = &[
    "%Y-%m-%dT%H:%M:%S%.f%:z", // RFC3339 with fractional seconds
    "%Y-%m-%dT%H:%M:%S%:z",    // RFC3339
    "%Y-%m-%dT%H:%M:%S%.fZ",   // RFC3339 UTC with fractional seconds
    "%Y-%m-%dT%H:%M:%SZ",      // RFC3339 UTC
    "%Y-%m-%dT%H:%M:%S%.f",    // ISO with fractional seconds, no timezone
    "%Y-%m-%dT%H:%M:%S",       // ISO without timezone
    "%Y-%m-%d %H:%M:%S%.f",    // SQL-style with fractional seconds
    "%Y-%m-%d %H:%M:%S",       // SQL-style
    "%Y-%m-%d",                // Date only
    "%Y/%m/%d %H:%M:%S",       // Alternative with slashes
    "%Y/%m/%d",                // Alternative date only
    "%m/%d/%Y",                // US format
    "%d/%m/%Y",                // European format
];

const TIME_FORMATS: &[&str] = &[
    "%H:%M:%S%.f", // High precision
    "%H:%M:%S",    // Standard
    "%H:%M",       // Hours and minutes only
];

/// A runtime value with type information
///
/// Each variant carries its data directly, avoiding the need for interface
/// indirection or separate value references.
///
/// ## Memory Layout (16 bytes)
///
/// Value is exactly 16 bytes due to niche optimization:
/// - Text(SmartString): 16 bytes with niches in tag byte (values 17-255 unused)
/// - Extension(CompactArc<[u8]>): 8 bytes (thin pointer), leaving niche bytes free
/// - Rust stores Value's discriminant in SmartString's niche values
///
/// ## Extension Variant
///
/// The Extension variant is a catch-all for all complex types (JSON, Vector, Blob, etc.)
/// It stores a single `CompactArc<[u8]>` (8 bytes) where `byte[0]` is the `DataType` tag
/// and byte[1..] is the payload. This keeps Value at exactly 7 variants forever —
/// new types are added by extending DataType (a 1-byte `#[repr(u8)]` enum).
///
/// Note: Text uses SmartString for inline storage of strings up to 15 bytes.
/// Longer strings use `Arc<str>` for O(1) clone and sharing.
#[derive(Debug, Clone)]
pub enum Value {
    /// NULL value with optional type hint
    Null(DataType),

    /// 64-bit signed integer
    Integer(i64),

    /// 64-bit floating point
    Float(f64),

    /// UTF-8 text string (SmartString: inline ≤15 bytes, Arc for larger)
    Text(SmartString),

    /// Boolean value
    Boolean(bool),

    /// Timestamp (UTC)
    Timestamp(DateTime<Utc>),

    /// Extension type: `byte[0]` = `DataType` tag, `byte[1..]` = payload
    /// - Json: `byte[0]=6`, `byte[1..]`=UTF-8 bytes (access via `as_json()`)
    /// - Vector: `byte[0]=7`, `byte[1..]`=packed LE f32 bytes (access via `as_vector_f32()`)
    /// - Future types (Blob, Array, etc.) add DataType variants, not Value variants
    Extension(CompactArc<[u8]>),
}

/// Static NULL value for zero-cost reuse
pub const NULL_VALUE: Value = Value::Null(DataType::Null);

impl Value {
    // =========================================================================
    // Constructors
    // =========================================================================

    /// Create a NULL value with a type hint
    #[inline]
    pub fn null(data_type: DataType) -> Self {
        Value::Null(data_type)
    }

    /// Create a NULL value with unknown type
    #[inline(always)]
    pub fn null_unknown() -> Self {
        Value::Null(DataType::Null)
    }

    /// Create an integer value
    pub fn integer(value: i64) -> Self {
        Value::Integer(value)
    }

    /// Create a float value
    pub fn float(value: f64) -> Self {
        Value::Float(value)
    }

    /// Create a text value
    ///
    /// Uses SmartString::from_string_shared() for heap strings to enable
    /// O(1) clone via `Arc<str>`. This allows string sharing between
    /// Arena, Index, and VersionStore.
    pub fn text(value: impl Into<String>) -> Self {
        Value::Text(SmartString::from_string_shared(value.into()))
    }

    /// Create a text value from `Arc<str>` (zero-copy for heap strings)
    ///
    /// Preserves the Arc reference for O(1) clone and sharing.
    pub fn text_arc(value: Arc<str>) -> Self {
        Value::Text(SmartString::from(value))
    }

    /// Create a boolean value
    pub fn boolean(value: bool) -> Self {
        Value::Boolean(value)
    }

    /// Create a timestamp value
    pub fn timestamp(value: DateTime<Utc>) -> Self {
        Value::Timestamp(value)
    }

    /// Create a validated JSON value.
    pub fn try_json(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        serde_json::from_str::<serde_json::Value>(&value)
            .map_err(|error| Error::invalid_argument(format!("invalid JSON value: {error}")))?;
        Ok(Self::json_unchecked(value))
    }

    /// Build JSON from a document already validated or generated by the engine.
    fn json_unchecked(value: impl Into<String>) -> Self {
        let s_bytes = value.into().into_bytes();
        let mut bytes = Vec::with_capacity(1 + s_bytes.len());
        bytes.push(DataType::Json as u8);
        bytes.extend_from_slice(&s_bytes);
        Value::Extension(CompactArc::from(bytes))
    }

    #[inline]
    #[doc(hidden)]
    pub fn json(value: impl Into<String>) -> Self {
        Self::json_unchecked(value)
    }

    /// Create a vector value from f32 data (stored as packed LE f32 bytes in Extension)
    pub fn vector(data: Vec<f32>) -> Self {
        let mut bytes = Vec::with_capacity(1 + data.len() * 4);
        bytes.push(DataType::Vector as u8);
        for f in &data {
            bytes.extend_from_slice(&f.to_le_bytes());
        }
        Value::Extension(CompactArc::from(bytes))
    }

    /// Create a vector value from pre-packed little-endian `f32` bytes.
    pub fn try_vector_from_bytes(raw_f32_bytes: CompactArc<[u8]>) -> Result<Self> {
        if !raw_f32_bytes
            .len()
            .is_multiple_of(std::mem::size_of::<f32>())
        {
            return Err(Error::invalid_argument(format!(
                "VECTOR payload has {} bytes, expected a multiple of 4",
                raw_f32_bytes.len()
            )));
        }
        Ok(Self::vector_from_bytes_unchecked(raw_f32_bytes))
    }

    fn vector_from_bytes_unchecked(raw_f32_bytes: CompactArc<[u8]>) -> Self {
        let mut bytes = Vec::with_capacity(1 + raw_f32_bytes.len());
        bytes.push(DataType::Vector as u8);
        bytes.extend_from_slice(&raw_f32_bytes);
        Value::Extension(CompactArc::from(bytes))
    }

    /// Create a UUID value from raw 16-byte UUID storage.
    pub fn uuid(bytes: [u8; 16]) -> Self {
        let mut data = Vec::with_capacity(17);
        data.push(DataType::Uuid as u8);
        data.extend_from_slice(&bytes);
        Value::Extension(CompactArc::from(data))
    }

    /// Create a new UUIDv7 value.
    ///
    /// UUIDv7 is time-ordered, which makes it much friendlier for primary-key
    /// B-tree indexes than fully random UUIDv4 values.
    pub fn uuid_v7() -> Self {
        Value::uuid(*Uuid::now_v7().as_bytes())
    }

    /// Create an exact decimal value.
    ///
    /// Payload layout:
    /// - byte 0: [`DataType::Decimal`] tag;
    /// - bytes 1..17: little-endian `i128` unscaled integer;
    /// - byte 17: precision;
    /// - byte 18: scale.
    pub fn try_decimal(unscaled: i128, precision: u8, scale: u8) -> Result<Self> {
        validate_decimal_shape(unscaled, precision, scale)?;
        Ok(Self::decimal_unchecked(unscaled, precision, scale))
    }

    fn decimal_unchecked(unscaled: i128, precision: u8, scale: u8) -> Self {
        let mut data = Vec::with_capacity(19);
        data.push(DataType::Decimal as u8);
        data.extend_from_slice(&unscaled.to_le_bytes());
        data.push(precision);
        data.push(scale);
        Value::Extension(CompactArc::from(data))
    }

    #[inline]
    #[doc(hidden)]
    pub fn decimal(unscaled: i128, precision: u8, scale: u8) -> Self {
        Self::decimal_unchecked(unscaled, precision, scale)
    }

    /// Create a calendar date value from days since Unix epoch.
    pub fn date(days_since_unix_epoch: i32) -> Self {
        let mut data = Vec::with_capacity(5);
        data.push(DataType::Date as u8);
        data.extend_from_slice(&days_since_unix_epoch.to_le_bytes());
        Value::Extension(CompactArc::from(data))
    }

    /// Create a raw byte-string value.
    pub fn bytes(bytes: Vec<u8>) -> Self {
        let mut data = Vec::with_capacity(1 + bytes.len());
        data.push(DataType::Bytes as u8);
        data.extend_from_slice(&bytes);
        Value::Extension(CompactArc::from(data))
    }

    /// Create a typed external value from canonical plugin codec bytes.
    ///
    /// Semantic codec validation is performed by the admitted plugin host at
    /// SQL/wire ingress. This constructor enforces the context-free envelope
    /// bounds shared by WAL and immutable artifact readers.
    pub fn try_external(type_ref: ExternalTypeRef, payload: impl AsRef<[u8]>) -> Result<Self> {
        let payload = payload.as_ref();
        if payload.len() > MAX_EXTERNAL_VALUE_BYTES {
            return Err(Error::invalid_argument(format!(
                "external value payload has {} bytes, limit is {}",
                payload.len(),
                MAX_EXTERNAL_VALUE_BYTES
            )));
        }
        let mut data = Vec::with_capacity(EXTERNAL_VALUE_HEADER_BYTES + payload.len());
        data.push(EXTERNAL_VALUE_MARKER);
        data.extend_from_slice(&type_ref.type_object_id());
        data.extend_from_slice(&type_ref.codec_version().to_le_bytes());
        data.extend_from_slice(payload);
        Ok(Value::Extension(CompactArc::from(data)))
    }

    // =========================================================================
    // Type accessors
    // =========================================================================

    /// Returns the data type of this value
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null(dt) => *dt,
            Value::Integer(_) => DataType::Integer,
            Value::Float(_) => DataType::Float,
            Value::Text(_) => DataType::Text,
            Value::Boolean(_) => DataType::Boolean,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Extension(data) => data
                .first()
                .and_then(|&b| DataType::from_u8(b))
                .unwrap_or(DataType::Null),
        }
    }

    /// Return the complete built-in or external logical type identity.
    pub fn logical_type(&self) -> LogicalTypeRef {
        self.as_external()
            .map(|value| LogicalTypeRef::External(value.type_ref()))
            .unwrap_or_else(|| LogicalTypeRef::Builtin(self.data_type()))
    }

    /// Return whether this value carries the reserved external-type envelope.
    ///
    /// This intentionally checks only the marker. Full envelope validation is
    /// owned by [`Value::as_external`] and the row/WAL/file admission paths.
    #[inline]
    pub fn is_external(&self) -> bool {
        matches!(self, Value::Extension(data) if data.first() == Some(&EXTERNAL_VALUE_MARKER))
    }

    /// Borrow the external envelope without exposing its compact storage.
    pub fn as_external(&self) -> Option<ExternalValueRef<'_>> {
        let Value::Extension(data) = self else {
            return None;
        };
        if data.first() != Some(&EXTERNAL_VALUE_MARKER) || data.len() < EXTERNAL_VALUE_HEADER_BYTES
        {
            return None;
        }
        let type_object_id = data[1..17].try_into().ok()?;
        let codec_version = u32::from_le_bytes(data[17..21].try_into().ok()?);
        let type_ref = ExternalTypeRef::new(type_object_id, codec_version).ok()?;
        Some(ExternalValueRef {
            type_ref,
            payload: &data[EXTERNAL_VALUE_HEADER_BYTES..],
        })
    }

    /// Validate the physical shape of a value before it crosses a row, WAL or
    /// file-format admission boundary.
    pub fn validate_shape(&self) -> Result<()> {
        let Value::Extension(data) = self else {
            return Ok(());
        };
        if data.first() == Some(&EXTERNAL_VALUE_MARKER) {
            let external = self.as_external().ok_or_else(|| {
                Error::invalid_argument("external value has an invalid typed envelope")
            })?;
            if external.payload().len() > MAX_EXTERNAL_VALUE_BYTES {
                return Err(Error::invalid_argument(
                    "external value payload exceeds 16 MiB",
                ));
            }
            return Ok(());
        }
        let Some(tag) = data.first().and_then(|tag| DataType::from_u8(*tag)) else {
            return Err(Error::invalid_argument(
                "extension value has an unknown or missing tag",
            ));
        };
        Self::validate_extension_payload(tag, &data[1..])
    }

    /// Validate an untagged extension payload read from a typed physical column.
    #[doc(hidden)]
    pub fn validate_extension_payload(tag: DataType, payload: &[u8]) -> Result<()> {
        match tag {
            DataType::Json => {
                let json = std::str::from_utf8(payload).map_err(|error| {
                    Error::invalid_argument(format!("invalid JSON UTF-8: {error}"))
                })?;
                serde_json::from_str::<serde_json::Value>(json).map_err(|error| {
                    Error::invalid_argument(format!("invalid JSON document: {error}"))
                })?;
            }
            DataType::Vector => {
                if !payload.len().is_multiple_of(std::mem::size_of::<f32>()) {
                    return Err(Error::invalid_argument(format!(
                        "VECTOR payload has {} bytes, expected a multiple of 4",
                        payload.len()
                    )));
                }
            }
            DataType::Uuid => {
                if payload.len() != 16 {
                    return Err(Error::invalid_argument(
                        "UUID payload must contain exactly 16 bytes",
                    ));
                }
            }
            DataType::Decimal => {
                if payload.len() != 18 {
                    return Err(Error::invalid_argument(
                        "DECIMAL payload must contain exactly 18 bytes",
                    ));
                }
                let unscaled = i128::from_le_bytes(
                    payload[..16]
                        .try_into()
                        .map_err(|_| Error::invalid_argument("invalid DECIMAL coefficient"))?,
                );
                validate_decimal_shape(unscaled, payload[16], payload[17])?;
            }
            DataType::Date => {
                if payload.len() != 4 {
                    return Err(Error::invalid_argument(
                        "DATE payload must contain exactly 4 bytes",
                    ));
                }
            }
            DataType::Bytes => {}
            _ => {
                return Err(Error::invalid_argument(format!(
                    "data type {tag} is not a valid extension payload tag"
                )));
            }
        }
        Ok(())
    }

    /// Returns true if this value is NULL
    #[inline(always)]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null(_))
    }

    // =========================================================================
    // Value extractors
    // =========================================================================

    /// Extract as i64, with type coercion
    ///
    /// Returns None if:
    /// - Value is NULL
    /// - Conversion is not possible
    pub fn as_int64(&self) -> Option<i64> {
        match self {
            Value::Null(_) => None,
            Value::Integer(v) => Some(*v),
            Value::Float(v) => checked_float_to_i64(*v),
            Value::Text(s) => parse_text_to_i64(s),
            Value::Boolean(b) => Some(if *b { 1 } else { 0 }),
            Value::Timestamp(t) => t.timestamp_nanos_opt(),
            Value::Extension(_) => None,
        }
    }

    /// Return the exact canonical INTEGER identity of this numeric value.
    ///
    /// Unlike [`Value::as_int64`], this never truncates fractional values and
    /// never accepts a saturating float-to-integer conversion. It follows the
    /// same cross-domain equality contract as [`Value::compare`], so an
    /// exactly integral FLOAT or DECIMAL can safely probe an INTEGER index.
    #[doc(hidden)]
    pub fn exact_integer_identity(&self) -> Option<i64> {
        match self {
            Value::Integer(integer) => Some(*integer),
            Value::Float(float) if float.is_finite() && float.fract() == 0.0 => {
                let integer = *float as i64;
                (Value::Integer(integer) == *self).then_some(integer)
            }
            Value::Extension(data) if data.first() == Some(&(DataType::Decimal as u8)) => {
                let (unscaled, _, scale) = self.as_decimal_parts()?;
                DecimalIdentity::from_parts(unscaled, scale).exact_i64()
            }
            _ => None,
        }
    }

    /// Extract as f64, with type coercion
    pub fn as_float64(&self) -> Option<f64> {
        match self {
            Value::Null(_) => None,
            Value::Integer(v) => Some(*v as f64),
            Value::Float(v) => Some(*v),
            Value::Text(s) => s.parse::<f64>().ok(),
            Value::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
            Value::Timestamp(_) | Value::Extension(_) => None,
        }
    }

    /// Extract as boolean, with type coercion
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Value::Null(_) => None,
            Value::Integer(v) => Some(*v != 0),
            Value::Float(v) => Some(*v != 0.0),
            Value::Text(s) => {
                // OPTIMIZATION: Use eq_ignore_ascii_case to avoid allocation
                let s_ref: &str = s.as_ref();
                if s_ref.eq_ignore_ascii_case("true")
                    || s_ref.eq_ignore_ascii_case("t")
                    || s_ref.eq_ignore_ascii_case("yes")
                    || s_ref.eq_ignore_ascii_case("y")
                    || s_ref == "1"
                {
                    Some(true)
                } else if s_ref.eq_ignore_ascii_case("false")
                    || s_ref.eq_ignore_ascii_case("f")
                    || s_ref.eq_ignore_ascii_case("no")
                    || s_ref.eq_ignore_ascii_case("n")
                    || s_ref == "0"
                    || s_ref.is_empty()
                {
                    Some(false)
                } else {
                    s_ref.parse::<f64>().ok().map(|f| f != 0.0)
                }
            }
            Value::Boolean(b) => Some(*b),
            Value::Timestamp(_) | Value::Extension(_) => None,
        }
    }

    /// Extract as String, with type coercion
    pub fn as_string(&self) -> Option<String> {
        match self {
            Value::Null(_) => None,
            Value::Integer(v) => Some(v.to_string()),
            Value::Float(v) => Some(format_float(*v)),
            Value::Text(s) => Some(s.to_string()),
            Value::Boolean(b) => Some(if *b { "true" } else { "false" }.to_string()),
            Value::Timestamp(t) => Some(t.to_rfc3339()),
            Value::Extension(data) if data.first() == Some(&(DataType::Json as u8)) => {
                // SAFETY: Json data is always stored as valid UTF-8
                Some(std::str::from_utf8(&data[1..]).unwrap_or("").to_string())
            }
            Value::Extension(data) if data.first() == Some(&(DataType::Vector as u8)) => {
                Some(format_vector_bytes(&data[1..]))
            }
            Value::Extension(data) if data.first() == Some(&(DataType::Uuid as u8)) => {
                format_uuid_bytes(&data[1..])
            }
            Value::Extension(data) if data.first() == Some(&(DataType::Decimal as u8)) => self
                .as_decimal_parts()
                .map(|(unscaled, _, scale)| format_decimal_parts(unscaled, scale)),
            Value::Extension(data) if data.first() == Some(&(DataType::Date as u8)) => self
                .as_date_days()
                .and_then(format_date_days_since_unix_epoch),
            Value::Extension(data) if data.first() == Some(&(DataType::Bytes as u8)) => {
                Some(format_bytes_hex(&data[1..]))
            }
            Value::Extension(data) => {
                // Generic fallback: try payload as UTF-8
                if data.len() > 1 {
                    std::str::from_utf8(&data[1..]).ok().map(|s| s.to_string())
                } else {
                    None
                }
            }
        }
    }

    /// Extract as string reference (avoids clone for Text/Json)
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s.as_str()),
            Value::Extension(data) if data.first() == Some(&(DataType::Json as u8)) => {
                let json = std::str::from_utf8(&data[1..]).ok()?;
                serde_json::from_str::<serde_json::Value>(json).ok()?;
                Some(json)
            }
            _ => None,
        }
    }

    /// Extract as `DateTime<Utc>`
    pub fn as_timestamp(&self) -> Option<DateTime<Utc>> {
        match self {
            Value::Null(_) => None,
            Value::Timestamp(t) => Some(*t),
            Value::Text(s) => parse_timestamp(s).ok(),
            Value::Integer(nanos) => {
                // Interpret as nanoseconds since Unix epoch
                datetime_from_epoch_nanos(*nanos)
            }
            _ => None,
        }
    }

    /// Return the exact artifact-backed timestamp representation when it is available.
    ///
    /// artifact-backed stores timestamps as signed nanoseconds since the Unix epoch. Chrono
    /// supports a wider public range, so callers at a persistence boundary must
    /// reject `None` instead of narrowing or wrapping the value.
    #[inline]
    #[doc(hidden)]
    pub fn artifact_timestamp_nanos(&self) -> Option<i64> {
        match self {
            Value::Timestamp(timestamp) => timestamp.timestamp_nanos_opt(),
            _ => None,
        }
    }

    /// Extract as JSON string
    pub fn as_json(&self) -> Option<&str> {
        match self {
            Value::Null(_) => None,
            // SAFETY: Json data is always stored as valid UTF-8 (tag at [0], payload at [1..])
            Value::Extension(data) if data.first() == Some(&(DataType::Json as u8)) => {
                let json = std::str::from_utf8(&data[1..]).ok()?;
                serde_json::from_str::<serde_json::Value>(json).ok()?;
                Some(json)
            }
            _ => None,
        }
    }

    /// Extract vector as `Vec<f32>` (reads packed LE f32 bytes from Extension payload)
    pub fn as_vector_f32(&self) -> Option<Vec<f32>> {
        match self {
            Value::Extension(data) if data.first() == Some(&(DataType::Vector as u8)) => {
                let payload = &data[1..];
                if payload.len() % std::mem::size_of::<f32>() != 0 {
                    return None;
                }
                let len = payload.len() / 4;
                let mut result = Vec::with_capacity(len);
                for i in 0..len {
                    let bytes = [
                        payload[i * 4],
                        payload[i * 4 + 1],
                        payload[i * 4 + 2],
                        payload[i * 4 + 3],
                    ];
                    result.push(f32::from_le_bytes(bytes));
                }
                Some(result)
            }
            _ => None,
        }
    }

    /// Extract UUID as raw 16 bytes.
    pub fn as_uuid_bytes(&self) -> Option<[u8; 16]> {
        match self {
            Value::Extension(data)
                if data.first() == Some(&(DataType::Uuid as u8)) && data.len() == 17 =>
            {
                data[1..].try_into().ok()
            }
            _ => None,
        }
    }

    /// Extract exact decimal parts: `(unscaled, precision, scale)`.
    pub fn as_decimal_parts(&self) -> Option<(i128, u8, u8)> {
        match self {
            Value::Extension(data)
                if data.first() == Some(&(DataType::Decimal as u8)) && data.len() == 19 =>
            {
                let unscaled = i128::from_le_bytes(data[1..17].try_into().ok()?);
                validate_decimal_shape(unscaled, data[17], data[18]).ok()?;
                Some((unscaled, data[17], data[18]))
            }
            _ => None,
        }
    }

    /// Extract calendar date as days since Unix epoch.
    pub fn as_date_days(&self) -> Option<i32> {
        match self {
            Value::Extension(data)
                if data.first() == Some(&(DataType::Date as u8)) && data.len() == 5 =>
            {
                Some(i32::from_le_bytes(data[1..5].try_into().ok()?))
            }
            _ => None,
        }
    }

    /// Extract raw bytes from a BYTES/BLOB/BINARY value.
    pub fn as_bytes_value(&self) -> Option<&[u8]> {
        match self {
            Value::Extension(data) if data.first() == Some(&(DataType::Bytes as u8)) => {
                Some(&data[1..])
            }
            _ => None,
        }
    }

    // =========================================================================
    // Comparison
    // =========================================================================

    /// Compare two values for ordering
    ///
    /// Returns:
    /// - Ok(Ordering::Less) if self < other
    /// - Ok(Ordering::Equal) if self == other
    /// - Ok(Ordering::Greater) if self > other
    /// - Err if comparison is not possible
    pub fn compare(&self, other: &Value) -> Result<Ordering> {
        // Handle NULL comparisons
        if self.is_null() || other.is_null() {
            if self.is_null() && other.is_null() {
                return Ok(Ordering::Equal);
            }
            return Err(Error::NullComparison);
        }

        // Integer, Float and Decimal share one canonical numeric identity.
        // Decimal keeps its exact physical payload; only comparison/key
        // semantics ignore redundant precision and trailing scale zeroes.
        if let Some(ordering) = compare_canonical_numeric(self, other) {
            return Ok(ordering);
        }

        // Same type comparison (most efficient path)
        if self.data_type() == other.data_type() {
            return self.compare_same_type(other);
        }

        // Cross-domain coercion belongs to SQL binding, not Value identity.
        Err(Error::IncomparableTypes)
    }

    /// Compare values of the same type
    fn compare_same_type(&self, other: &Value) -> Result<Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => Ok(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => Ok(compare_floats(*a, *b)),
            (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b)),
            (Value::Boolean(a), Value::Boolean(b)) => Ok(a.cmp(b)),
            (Value::Timestamp(a), Value::Timestamp(b)) => Ok(a.cmp(b)),
            (Value::Extension(a), Value::Extension(b)) => {
                // External equality and ordering are explicit plugin
                // capabilities. Core structural bytes are never a semantic
                // comparison fallback.
                if a.first() == Some(&EXTERNAL_VALUE_MARKER)
                    || b.first() == Some(&EXTERNAL_VALUE_MARKER)
                {
                    return Err(Error::IncomparableTypes);
                }
                // Extension: tag byte is [0], so same-tag comparison is data equality
                if a.first() != b.first() {
                    return Err(Error::IncomparableTypes);
                }
                if a.first() == Some(&(DataType::Uuid as u8)) {
                    return if a.len() == 17 && b.len() == 17 {
                        Ok(a[1..].cmp(&b[1..]))
                    } else {
                        Err(Error::IncomparableTypes)
                    };
                }
                if a.first() == Some(&(DataType::Decimal as u8)) {
                    return match (self.as_decimal_parts(), other.as_decimal_parts()) {
                        (
                            Some((left_unscaled, _, left_scale)),
                            Some((right_unscaled, _, right_scale)),
                        ) => Ok(compare_decimal_parts(
                            left_unscaled,
                            left_scale,
                            right_unscaled,
                            right_scale,
                        )),
                        _ => Err(Error::IncomparableTypes),
                    };
                }
                if a.first() == Some(&(DataType::Date as u8)) {
                    return match (self.as_date_days(), other.as_date_days()) {
                        (Some(left), Some(right)) => Ok(left.cmp(&right)),
                        _ => Err(Error::IncomparableTypes),
                    };
                }
                if a.first() == Some(&(DataType::Bytes as u8)) {
                    return Ok(a[1..].cmp(&b[1..]));
                }
                // All extension types: equality only (not orderable)
                if a == b {
                    Ok(Ordering::Equal)
                } else {
                    Err(Error::IncomparableTypes)
                }
            }
            _ => Err(Error::IncomparableTypes),
        }
    }

    // =========================================================================
    // Construction from typed values
    // =========================================================================

    /// Create a Value from a typed value with explicit data type
    pub fn from_typed(value: Option<&dyn std::any::Any>, data_type: DataType) -> Result<Self> {
        let has_value = value.is_some();
        let result = match value {
            None => Value::Null(data_type),
            Some(v) => {
                // Try to downcast based on expected type
                match data_type {
                    DataType::Integer => {
                        if let Some(&i) = v.downcast_ref::<i64>() {
                            Value::Integer(i)
                        } else if let Some(&i) = v.downcast_ref::<i32>() {
                            Value::Integer(i as i64)
                        } else if let Some(s) = v.downcast_ref::<String>() {
                            s.parse::<i64>()
                                .map(Value::Integer)
                                .unwrap_or(Value::Null(data_type))
                        } else {
                            Value::Null(data_type)
                        }
                    }
                    DataType::Float => {
                        if let Some(&f) = v.downcast_ref::<f64>() {
                            Value::Float(f)
                        } else if let Some(&i) = v.downcast_ref::<i64>() {
                            Value::Float(i as f64)
                        } else if let Some(s) = v.downcast_ref::<String>() {
                            s.parse::<f64>()
                                .map(Value::Float)
                                .unwrap_or(Value::Null(data_type))
                        } else {
                            Value::Null(data_type)
                        }
                    }
                    DataType::Text => {
                        if let Some(s) = v.downcast_ref::<String>() {
                            Value::Text(SmartString::new(s))
                        } else if let Some(&s) = v.downcast_ref::<&str>() {
                            Value::Text(SmartString::from(s))
                        } else {
                            Value::Null(data_type)
                        }
                    }
                    DataType::Boolean => {
                        if let Some(&b) = v.downcast_ref::<bool>() {
                            Value::Boolean(b)
                        } else if let Some(&i) = v.downcast_ref::<i64>() {
                            Value::Boolean(i != 0)
                        } else {
                            Value::Null(data_type)
                        }
                    }
                    DataType::Timestamp => {
                        if let Some(&t) = v.downcast_ref::<DateTime<Utc>>() {
                            Value::Timestamp(t)
                        } else if let Some(s) = v.downcast_ref::<String>() {
                            parse_timestamp(s)
                                .map(Value::Timestamp)
                                .unwrap_or(Value::Null(data_type))
                        } else {
                            Value::Null(data_type)
                        }
                    }
                    DataType::Json => {
                        if let Some(s) = v.downcast_ref::<String>() {
                            // Validate JSON
                            if serde_json::from_str::<serde_json::Value>(s).is_ok() {
                                Value::json_unchecked(s)
                            } else {
                                Value::Null(data_type)
                            }
                        } else {
                            Value::Null(data_type)
                        }
                    }
                    DataType::Uuid => {
                        if let Some(&bytes) = v.downcast_ref::<[u8; 16]>() {
                            Value::uuid(bytes)
                        } else if let Some(s) = v.downcast_ref::<String>() {
                            parse_uuid_str(s)
                                .map(Value::uuid)
                                .unwrap_or(Value::Null(data_type))
                        } else if let Some(&s) = v.downcast_ref::<&str>() {
                            parse_uuid_str(s)
                                .map(Value::uuid)
                                .unwrap_or(Value::Null(data_type))
                        } else {
                            Value::Null(data_type)
                        }
                    }
                    DataType::Vector => {
                        if let Some(vec) = v.downcast_ref::<Vec<f32>>() {
                            Value::vector(vec.clone())
                        } else {
                            Value::Null(data_type)
                        }
                    }
                    DataType::Decimal => {
                        if let Some(&(unscaled, precision, scale)) =
                            v.downcast_ref::<(i128, u8, u8)>()
                        {
                            Value::decimal_unchecked(unscaled, precision, scale)
                        } else if let Some(&i) = v.downcast_ref::<i64>() {
                            Value::decimal_unchecked(
                                i as i128,
                                decimal_precision_for_unscaled(i),
                                0,
                            )
                        } else if let Some(s) = v.downcast_ref::<String>() {
                            parse_decimal_str(s)
                                .map(|(unscaled, precision, scale)| {
                                    Value::decimal_unchecked(unscaled, precision, scale)
                                })
                                .unwrap_or(Value::Null(data_type))
                        } else if let Some(&s) = v.downcast_ref::<&str>() {
                            parse_decimal_str(s)
                                .map(|(unscaled, precision, scale)| {
                                    Value::decimal_unchecked(unscaled, precision, scale)
                                })
                                .unwrap_or(Value::Null(data_type))
                        } else {
                            Value::Null(data_type)
                        }
                    }
                    DataType::Date => {
                        if let Some(&days) = v.downcast_ref::<i32>() {
                            Value::date(days)
                        } else if let Some(s) = v.downcast_ref::<String>() {
                            parse_date_days_since_unix_epoch(s)
                                .map(Value::date)
                                .unwrap_or(Value::Null(data_type))
                        } else if let Some(&s) = v.downcast_ref::<&str>() {
                            parse_date_days_since_unix_epoch(s)
                                .map(Value::date)
                                .unwrap_or(Value::Null(data_type))
                        } else {
                            Value::Null(data_type)
                        }
                    }
                    DataType::Bytes => {
                        if let Some(bytes) = v.downcast_ref::<Vec<u8>>() {
                            Value::bytes(bytes.clone())
                        } else if let Some(s) = v.downcast_ref::<String>() {
                            Value::bytes(s.as_bytes().to_vec())
                        } else if let Some(&s) = v.downcast_ref::<&str>() {
                            Value::bytes(s.as_bytes().to_vec())
                        } else {
                            Value::Null(data_type)
                        }
                    }
                    DataType::Null => Value::Null(DataType::Null),
                }
            }
        };
        result.validate_shape()?;
        if has_value && result.is_null() && data_type != DataType::Null {
            return Err(Error::type_conversion("typed value", data_type.to_string()));
        }
        Ok(result)
    }

    // =========================================================================
    // Type coercion
    // =========================================================================

    /// Coerce this value to the target data type
    ///
    /// Type coercion rules:
    /// - Integer column receiving Float → converts to Integer
    /// - Float column receiving Integer → converts to Float
    /// - Text column receiving any type → converts to Text
    /// - Timestamp column receiving String → parses timestamp
    /// - JSON column receiving valid JSON string → stores as JSON
    /// - Boolean column receiving Integer/String → converts to Boolean
    ///
    /// Returns the coerced value, or NULL if coercion fails.
    pub fn coerce_to_type(&self, target_type: DataType) -> Value {
        // NULL stays NULL (with target type hint)
        if self.is_null() {
            return Value::Null(target_type);
        }

        if self.validate_shape().is_err() {
            return Value::Null(target_type);
        }

        // Same type - no conversion needed
        if self.data_type() == target_type {
            return self.clone();
        }

        match target_type {
            DataType::Integer => {
                // Convert to INTEGER
                match self {
                    Value::Integer(v) => Value::Integer(*v),
                    Value::Float(v) => checked_float_to_i64(*v)
                        .map(Value::Integer)
                        .unwrap_or(Value::Null(target_type)),
                    Value::Text(s) => parse_text_to_i64(s)
                        .map(Value::Integer)
                        .unwrap_or(Value::Null(target_type)),
                    Value::Boolean(b) => Value::Integer(if *b { 1 } else { 0 }),
                    Value::Extension(data) if data.first() == Some(&(DataType::Decimal as u8)) => {
                        self.as_decimal_parts()
                            .and_then(|(unscaled, _, scale)| {
                                decimal_scale_factor(scale)
                                    .and_then(|factor| i64::try_from(unscaled / factor).ok())
                            })
                            .map(Value::Integer)
                            .unwrap_or(Value::Null(target_type))
                    }
                    _ => Value::Null(target_type),
                }
            }
            DataType::Float => {
                // Convert to FLOAT
                match self {
                    Value::Float(v) => Value::Float(*v),
                    Value::Integer(v) => Value::Float(*v as f64),
                    Value::Text(s) => s
                        .parse::<f64>()
                        .map(Value::Float)
                        .unwrap_or(Value::Null(target_type)),
                    Value::Boolean(b) => Value::Float(if *b { 1.0 } else { 0.0 }),
                    Value::Extension(data) if data.first() == Some(&(DataType::Decimal as u8)) => {
                        self.as_string()
                            .and_then(|value| value.parse::<f64>().ok())
                            .map(Value::Float)
                            .unwrap_or(Value::Null(target_type))
                    }
                    _ => Value::Null(target_type),
                }
            }
            DataType::Text => {
                // Convert to TEXT - everything can become text
                match self {
                    Value::Text(s) => Value::Text(s.clone()),
                    Value::Integer(v) => Value::Text(SmartString::from_string(v.to_string())),
                    Value::Float(v) => Value::Text(SmartString::from_string(format_float(*v))),
                    Value::Boolean(b) => {
                        Value::Text(SmartString::new(if *b { "true" } else { "false" }))
                    }
                    Value::Timestamp(t) => Value::Text(SmartString::from_string(t.to_rfc3339())),
                    Value::Extension(data) if data.first() == Some(&(DataType::Json as u8)) => {
                        Value::Text(SmartString::new(
                            std::str::from_utf8(&data[1..]).unwrap_or(""),
                        ))
                    }
                    Value::Extension(data) if data.first() == Some(&(DataType::Vector as u8)) => {
                        Value::Text(SmartString::from_string(format_vector_bytes(&data[1..])))
                    }
                    Value::Extension(data) if data.first() == Some(&(DataType::Uuid as u8)) => {
                        format_uuid_bytes(&data[1..])
                            .map(|s| Value::Text(SmartString::from_string(s)))
                            .unwrap_or(Value::Null(target_type))
                    }
                    Value::Extension(data) if data.first() == Some(&(DataType::Decimal as u8)) => {
                        self.as_string()
                            .map(|value| Value::Text(SmartString::from_string(value)))
                            .unwrap_or(Value::Null(target_type))
                    }
                    Value::Extension(_) => Value::Null(target_type),
                    Value::Null(_) => Value::Null(target_type),
                }
            }
            DataType::Boolean => {
                // Convert to BOOLEAN
                match self {
                    Value::Boolean(b) => Value::Boolean(*b),
                    Value::Integer(v) => Value::Boolean(*v != 0),
                    Value::Float(v) => Value::Boolean(*v != 0.0),
                    Value::Text(s) => {
                        // OPTIMIZATION: Use eq_ignore_ascii_case to avoid allocation
                        let s_ref: &str = s.as_ref();
                        if s_ref.eq_ignore_ascii_case("true")
                            || s_ref.eq_ignore_ascii_case("t")
                            || s_ref.eq_ignore_ascii_case("yes")
                            || s_ref.eq_ignore_ascii_case("y")
                            || s_ref == "1"
                        {
                            Value::Boolean(true)
                        } else if s_ref.eq_ignore_ascii_case("false")
                            || s_ref.eq_ignore_ascii_case("f")
                            || s_ref.eq_ignore_ascii_case("no")
                            || s_ref.eq_ignore_ascii_case("n")
                            || s_ref == "0"
                        {
                            Value::Boolean(false)
                        } else {
                            Value::Null(target_type)
                        }
                    }
                    _ => Value::Null(target_type),
                }
            }
            DataType::Timestamp => {
                // Convert to TIMESTAMP
                match self {
                    Value::Timestamp(t) => Value::Timestamp(*t),
                    Value::Extension(data)
                        if data.first() == Some(&(DataType::Date as u8)) && data.len() == 5 =>
                    {
                        self.as_date_days()
                            .and_then(|days| {
                                NaiveDate::from_ymd_opt(1970, 1, 1)?
                                    .checked_add_signed(chrono::Duration::days(i64::from(days)))?
                                    .and_hms_opt(0, 0, 0)
                            })
                            .map(|value| {
                                Value::Timestamp(DateTime::<Utc>::from_naive_utc_and_offset(
                                    value, Utc,
                                ))
                            })
                            .unwrap_or(Value::Null(target_type))
                    }
                    Value::Text(s) => parse_timestamp(s)
                        .map(Value::Timestamp)
                        .unwrap_or(Value::Null(target_type)),
                    Value::Integer(nanos) => {
                        // Interpret as nanoseconds since Unix epoch
                        datetime_from_epoch_nanos(*nanos)
                            .map(Value::Timestamp)
                            .unwrap_or(Value::Null(target_type))
                    }
                    _ => Value::Null(target_type),
                }
            }
            DataType::Json => {
                // Convert to JSON
                match self {
                    Value::Extension(data) if data.first() == Some(&(DataType::Json as u8)) => {
                        self.clone()
                    }
                    Value::Text(s) => {
                        // Validate JSON
                        if serde_json::from_str::<serde_json::Value>(s.as_str()).is_ok() {
                            Value::json_unchecked(s.as_str())
                        } else {
                            Value::Null(target_type)
                        }
                    }
                    // Convert other types to JSON representation
                    Value::Integer(v) => Value::json_unchecked(v.to_string()),
                    Value::Float(v) if v.is_finite() => Value::json_unchecked(format_float(*v)),
                    Value::Boolean(b) => Value::json_unchecked(if *b { "true" } else { "false" }),
                    Value::Timestamp(timestamp) => {
                        Value::json_unchecked(format!("\"{}\"", timestamp.to_rfc3339()))
                    }
                    _ => Value::Null(target_type),
                }
            }
            DataType::Vector => match self {
                Value::Extension(data) if data.first() == Some(&(DataType::Vector as u8)) => {
                    self.clone()
                }
                Value::Text(s) => {
                    if let Some(floats) = parse_vector_str(s.as_str()) {
                        Value::vector(floats)
                    } else {
                        Value::Null(target_type)
                    }
                }
                _ => Value::Null(target_type),
            },
            DataType::Uuid => match self {
                Value::Extension(data) if data.first() == Some(&(DataType::Uuid as u8)) => {
                    if data.len() == 17 {
                        self.clone()
                    } else {
                        Value::Null(target_type)
                    }
                }
                Value::Text(s) => parse_uuid_str(s.as_str())
                    .map(Value::uuid)
                    .unwrap_or(Value::Null(target_type)),
                _ => Value::Null(target_type),
            },
            DataType::Decimal => match self {
                Value::Extension(data) if data.first() == Some(&(DataType::Decimal as u8)) => {
                    if self.as_decimal_parts().is_some() {
                        self.clone()
                    } else {
                        Value::Null(target_type)
                    }
                }
                Value::Integer(value) => {
                    let digits = decimal_precision_for_unscaled(*value);
                    Value::decimal_unchecked(*value as i128, digits, 0)
                }
                Value::Float(value) => parse_decimal_f64(*value)
                    .map(|(unscaled, precision, scale)| {
                        Value::decimal_unchecked(unscaled, precision, scale)
                    })
                    .unwrap_or(Value::Null(target_type)),
                Value::Text(s) => parse_decimal_str(s.as_str())
                    .map(|(unscaled, precision, scale)| {
                        Value::decimal_unchecked(unscaled, precision, scale)
                    })
                    .unwrap_or(Value::Null(target_type)),
                _ => Value::Null(target_type),
            },
            DataType::Date => match self {
                Value::Extension(data) if data.first() == Some(&(DataType::Date as u8)) => {
                    if data.len() == 5 {
                        self.clone()
                    } else {
                        Value::Null(target_type)
                    }
                }
                Value::Text(s) => parse_date_days_since_unix_epoch(s.as_str())
                    .map(Value::date)
                    .unwrap_or(Value::Null(target_type)),
                Value::Timestamp(timestamp) => {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                        .expect("Unix epoch is a valid calendar date");
                    i32::try_from(
                        timestamp
                            .date_naive()
                            .signed_duration_since(epoch)
                            .num_days(),
                    )
                    .map(Value::date)
                    .unwrap_or(Value::Null(target_type))
                }
                _ => Value::Null(target_type),
            },
            DataType::Bytes => match self {
                Value::Extension(data) if data.first() == Some(&(DataType::Bytes as u8)) => {
                    self.clone()
                }
                Value::Text(s) => Value::bytes(s.as_bytes().to_vec()),
                _ => Value::Null(target_type),
            },
            DataType::Null => Value::Null(DataType::Null),
        }
    }

    /// Checked coercion to the target data type.
    ///
    /// This is the strict counterpart to [`Value::coerce_to_type`]. It preserves
    /// the normal SQL rule that NULL casts to typed NULL, but treats
    /// non-NULL-to-NULL conversion as a runtime type error. Use this for explicit
    /// SQL `CAST` and other user-visible expression evaluation boundaries where
    /// silently producing NULL would hide bad data.
    pub fn try_coerce_to_type(&self, target_type: DataType) -> Result<Value> {
        self.validate_shape()?;
        let coerced = self.coerce_to_type(target_type);
        if !self.is_null() && coerced.is_null() && target_type != DataType::Null {
            return Err(Error::Type(format!(
                "cannot convert value '{}' from {:?} to {:?}",
                self,
                self.data_type(),
                target_type
            )));
        }
        Ok(coerced)
    }

    /// Coerce value to target type, consuming self
    /// OPTIMIZATION: Avoids clone when types already match
    #[inline]
    pub fn into_coerce_to_type(self, target_type: DataType) -> Value {
        // NULL stays NULL (with target type hint)
        if self.is_null() {
            return Value::Null(target_type);
        }

        if self.validate_shape().is_err() {
            return Value::Null(target_type);
        }

        // Same type - no conversion needed, return self directly
        if self.data_type() == target_type {
            return self;
        }

        match target_type {
            DataType::Integer => match &self {
                Value::Integer(v) => Value::Integer(*v),
                Value::Float(v) => checked_float_to_i64(*v)
                    .map(Value::Integer)
                    .unwrap_or(Value::Null(target_type)),
                Value::Text(s) => parse_text_to_i64(s)
                    .map(Value::Integer)
                    .unwrap_or(Value::Null(target_type)),
                Value::Boolean(b) => Value::Integer(if *b { 1 } else { 0 }),
                Value::Extension(ref data) if data.first() == Some(&(DataType::Decimal as u8)) => {
                    self.as_decimal_parts()
                        .and_then(|(unscaled, _, scale)| {
                            decimal_scale_factor(scale)
                                .and_then(|factor| i64::try_from(unscaled / factor).ok())
                        })
                        .map(Value::Integer)
                        .unwrap_or(Value::Null(target_type))
                }
                _ => Value::Null(target_type),
            },
            DataType::Float => match &self {
                Value::Float(v) => Value::Float(*v),
                Value::Integer(v) => Value::Float(*v as f64),
                Value::Text(s) => s
                    .parse::<f64>()
                    .map(Value::Float)
                    .unwrap_or(Value::Null(target_type)),
                Value::Boolean(b) => Value::Float(if *b { 1.0 } else { 0.0 }),
                Value::Extension(ref data) if data.first() == Some(&(DataType::Decimal as u8)) => {
                    self.as_string()
                        .and_then(|value| value.parse::<f64>().ok())
                        .map(Value::Float)
                        .unwrap_or(Value::Null(target_type))
                }
                _ => Value::Null(target_type),
            },
            DataType::Text => match self {
                Value::Text(s) => Value::Text(s),
                Value::Integer(v) => Value::Text(SmartString::from_string(v.to_string())),
                Value::Float(v) => Value::Text(SmartString::from_string(format_float(v))),
                Value::Boolean(b) => {
                    Value::Text(SmartString::new(if b { "true" } else { "false" }))
                }
                Value::Timestamp(t) => Value::Text(SmartString::from_string(t.to_rfc3339())),
                Value::Extension(data) if data.first() == Some(&(DataType::Json as u8)) => {
                    Value::Text(SmartString::new(
                        std::str::from_utf8(&data[1..]).unwrap_or(""),
                    ))
                }
                Value::Extension(data) if data.first() == Some(&(DataType::Vector as u8)) => {
                    Value::Text(SmartString::from_string(format_vector_bytes(&data[1..])))
                }
                Value::Extension(data) if data.first() == Some(&(DataType::Uuid as u8)) => {
                    format_uuid_bytes(&data[1..])
                        .map(|s| Value::Text(SmartString::from_string(s)))
                        .unwrap_or(Value::Null(target_type))
                }
                Value::Extension(ref data) if data.first() == Some(&(DataType::Decimal as u8)) => {
                    self.as_string()
                        .map(|value| Value::Text(SmartString::from_string(value)))
                        .unwrap_or(Value::Null(target_type))
                }
                Value::Extension(_) | Value::Null(_) => Value::Null(target_type),
            },
            DataType::Boolean => match &self {
                Value::Boolean(b) => Value::Boolean(*b),
                Value::Integer(v) => Value::Boolean(*v != 0),
                Value::Float(v) => Value::Boolean(*v != 0.0),
                Value::Text(s) => {
                    // OPTIMIZATION: Use eq_ignore_ascii_case to avoid allocation
                    let s_ref: &str = s.as_ref();
                    if s_ref.eq_ignore_ascii_case("true")
                        || s_ref.eq_ignore_ascii_case("t")
                        || s_ref.eq_ignore_ascii_case("yes")
                        || s_ref.eq_ignore_ascii_case("y")
                        || s_ref == "1"
                    {
                        Value::Boolean(true)
                    } else if s_ref.eq_ignore_ascii_case("false")
                        || s_ref.eq_ignore_ascii_case("f")
                        || s_ref.eq_ignore_ascii_case("no")
                        || s_ref.eq_ignore_ascii_case("n")
                        || s_ref == "0"
                    {
                        Value::Boolean(false)
                    } else {
                        Value::Null(target_type)
                    }
                }
                _ => Value::Null(target_type),
            },
            DataType::Timestamp => match self {
                Value::Timestamp(t) => Value::Timestamp(t),
                Value::Text(s) => parse_timestamp(&s)
                    .map(Value::Timestamp)
                    .unwrap_or(Value::Null(target_type)),
                Value::Integer(nanos) => datetime_from_epoch_nanos(nanos)
                    .map(Value::Timestamp)
                    .unwrap_or(Value::Null(target_type)),
                _ => Value::Null(target_type),
            },
            DataType::Json => match self {
                Value::Extension(ref data) if data.first() == Some(&(DataType::Json as u8)) => self,
                Value::Text(s) => {
                    if serde_json::from_str::<serde_json::Value>(s.as_str()).is_ok() {
                        Value::json_unchecked(s.as_str())
                    } else {
                        Value::Null(target_type)
                    }
                }
                Value::Integer(v) => Value::json_unchecked(v.to_string()),
                Value::Float(v) if v.is_finite() => Value::json_unchecked(format_float(v)),
                Value::Boolean(b) => Value::json_unchecked(if b { "true" } else { "false" }),
                Value::Timestamp(timestamp) => {
                    Value::json_unchecked(format!("\"{}\"", timestamp.to_rfc3339()))
                }
                _ => Value::Null(target_type),
            },
            DataType::Vector => match self {
                Value::Extension(ref data) if data.first() == Some(&(DataType::Vector as u8)) => {
                    self
                }
                Value::Text(s) => {
                    if let Some(floats) = parse_vector_str(s.as_str()) {
                        Value::vector(floats)
                    } else {
                        Value::Null(target_type)
                    }
                }
                _ => Value::Null(target_type),
            },
            DataType::Uuid => match self {
                Value::Extension(ref data) if data.first() == Some(&(DataType::Uuid as u8)) => {
                    if data.len() == 17 {
                        self
                    } else {
                        Value::Null(target_type)
                    }
                }
                Value::Text(s) => parse_uuid_str(s.as_str())
                    .map(Value::uuid)
                    .unwrap_or(Value::Null(target_type)),
                _ => Value::Null(target_type),
            },
            DataType::Decimal => match self {
                Value::Extension(ref data) if data.first() == Some(&(DataType::Decimal as u8)) => {
                    if self.as_decimal_parts().is_some() {
                        self
                    } else {
                        Value::Null(target_type)
                    }
                }
                Value::Integer(value) => {
                    let digits = decimal_precision_for_unscaled(value);
                    Value::decimal_unchecked(value as i128, digits, 0)
                }
                Value::Float(value) => parse_decimal_f64(value)
                    .map(|(unscaled, precision, scale)| {
                        Value::decimal_unchecked(unscaled, precision, scale)
                    })
                    .unwrap_or(Value::Null(target_type)),
                Value::Text(s) => parse_decimal_str(s.as_str())
                    .map(|(unscaled, precision, scale)| {
                        Value::decimal_unchecked(unscaled, precision, scale)
                    })
                    .unwrap_or(Value::Null(target_type)),
                _ => Value::Null(target_type),
            },
            DataType::Date => match self {
                Value::Extension(ref data) if data.first() == Some(&(DataType::Date as u8)) => {
                    if data.len() == 5 {
                        self
                    } else {
                        Value::Null(target_type)
                    }
                }
                Value::Text(s) => parse_date_days_since_unix_epoch(s.as_str())
                    .map(Value::date)
                    .unwrap_or(Value::Null(target_type)),
                _ => Value::Null(target_type),
            },
            DataType::Bytes => match self {
                Value::Extension(ref data) if data.first() == Some(&(DataType::Bytes as u8)) => {
                    self
                }
                Value::Text(s) => Value::bytes(s.as_bytes().to_vec()),
                _ => Value::Null(target_type),
            },
            DataType::Null => Value::Null(DataType::Null),
        }
    }
}

// =========================================================================
// Trait implementations
// =========================================================================

impl Default for Value {
    fn default() -> Self {
        Value::Null(DataType::Null)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null(_) => write!(f, "NULL"),
            Value::Integer(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", format_float(*v)),
            Value::Text(s) => write!(f, "{}", s),
            Value::Boolean(b) => write!(f, "{}", if *b { "true" } else { "false" }),
            Value::Timestamp(t) => write!(f, "{}", t.to_rfc3339()),
            Value::Extension(data) => {
                let tag = data.first().copied().unwrap_or(0);
                if tag == DataType::Json as u8 {
                    write!(f, "{}", std::str::from_utf8(&data[1..]).unwrap_or(""))
                } else if tag == DataType::Vector as u8 {
                    write!(f, "{}", format_vector_bytes(&data[1..]))
                } else if tag == DataType::Uuid as u8 {
                    match format_uuid_bytes(&data[1..]) {
                        Some(uuid) => write!(f, "{}", uuid),
                        None => write!(f, "<invalid-uuid>"),
                    }
                } else if tag == DataType::Decimal as u8 {
                    match self.as_decimal_parts() {
                        Some((unscaled, _, scale)) => {
                            write!(f, "{}", format_decimal_parts(unscaled, scale))
                        }
                        None => write!(f, "<invalid-decimal>"),
                    }
                } else if tag == DataType::Date as u8 {
                    match self
                        .as_date_days()
                        .and_then(format_date_days_since_unix_epoch)
                    {
                        Some(date) => write!(f, "{}", date),
                        None => write!(f, "<invalid-date>"),
                    }
                } else if tag == DataType::Bytes as u8 {
                    write!(f, "{}", format_bytes_hex(&data[1..]))
                } else {
                    write!(f, "<extension:{}>", tag)
                }
            }
        }
    }
}

impl PartialEq for Value {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        if let Some(ordering) = compare_canonical_numeric(self, other) {
            return ordering == Ordering::Equal;
        }

        // Single match handles NULL and all type comparisons without redundant is_null() calls
        match (self, other) {
            // NULL handling: NULL == NULL (SQL equality semantics for grouping)
            (Value::Null(_), Value::Null(_)) => true,
            // NULL != any non-NULL value
            (Value::Null(_), _) | (_, Value::Null(_)) => false,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::Extension(a), Value::Extension(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

/// Compare an exact i64 with an f64 without converting the integer to f64.
///
/// NaNs form the existing canonical class ordered after every numeric value.
/// `2^63` is the exclusive upper bound because `i64::MAX as f64` rounds to it;
/// `-2^63` is exactly representable and therefore remains inclusive.
#[inline]
fn compare_integer_float(integer: i64, float: f64) -> Ordering {
    const I64_EXCLUSIVE_UPPER_F64: f64 = 9_223_372_036_854_775_808.0;
    const I64_INCLUSIVE_LOWER_F64: f64 = -9_223_372_036_854_775_808.0;

    if float.is_nan() {
        return Ordering::Less;
    }
    if float >= I64_EXCLUSIVE_UPPER_F64 {
        return Ordering::Less;
    }
    if float < I64_INCLUSIVE_LOWER_F64 {
        return Ordering::Greater;
    }

    // The range checks make this saturating cast an exact truncation toward
    // zero into i64. Comparing with that integer decides every case except a
    // fractional float whose truncation is exactly `integer`.
    let truncated = float as i64;
    match integer.cmp(&truncated) {
        Ordering::Equal => {
            let fraction = float.fract();
            if fraction == 0.0 {
                Ordering::Equal
            } else if fraction.is_sign_negative() {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        ordering => ordering,
    }
}

/// WyHash-style 128-bit multiply mixing function.
/// Provides excellent avalanche properties - small input changes produce
/// completely different outputs. This pre-mixes values before the hasher
/// sees them, fixing collision problems with simple hashers like FxHash.
#[inline(always)]
fn wymix(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

// WyHash prime constants for mixing
const WY_P1: u64 = 0xa0761d6478bd642f;
const WY_P2: u64 = 0xe7037ed1a0b428db;

#[inline(always)]
fn integer_hash_word(value: i64) -> u64 {
    wymix(1 ^ (value as u64), WY_P1)
}

#[inline(always)]
fn float_hash_word(value: f64) -> u64 {
    if value.is_nan() {
        return wymix(6 ^ f64::NAN.to_bits(), WY_P1);
    }

    let integer = value as i64;
    if compare_integer_float(integer, value) == Ordering::Equal {
        // Exactly integral, in-range floats share the Integer domain,
        // including signed zero and -2^63.
        integer_hash_word(integer)
    } else {
        wymix(6 ^ value.to_bits(), WY_P1)
    }
}

impl Hash for Value {
    #[inline(always)]
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Pre-mix strategy: Instead of writing raw values that may have poor
        // distribution (causing collisions in simple hashers like FxHash),
        // we pre-mix everything using WyHash-style 128-bit multiply mixing.
        // This gives ANY hasher well-distributed inputs.
        //
        // Constraint: Integer(5) == Float(5.0) must have equal hashes.
        // We handle this by using the exact integer domain only for integral
        // floats that compare equal to a mathematical i64.
        match self {
            Value::Null(_) => {
                // All NULLs hash the same
                state.write_u64(0);
            }
            Value::Integer(v) => {
                // Every integer keeps its exact i64 identity.
                state.write_u64(integer_hash_word(*v));
            }
            Value::Float(v) => {
                state.write_u64(float_hash_word(*v));
            }
            Value::Text(s) => {
                // Pre-hash string with WyHash-style mixing, write single u64
                let bytes = s.as_bytes();
                let len = bytes.len();
                let mut h = wymix(2 ^ (len as u64), WY_P1);

                // Process 8 bytes at a time
                let chunks = len / 8;
                let ptr = bytes.as_ptr();
                for i in 0..chunks {
                    // SAFETY: We iterate i from 0..chunks where chunks = len/8.
                    // So i*8 is always < len, and we read 8 bytes which is valid
                    // since (i+1)*8 <= chunks*8 <= len. read_unaligned handles alignment.
                    let chunk = unsafe { (ptr.add(i * 8) as *const u64).read_unaligned() };
                    h = wymix(h ^ chunk, WY_P2);
                }

                // Handle tail bytes (0-7)
                let tail_start = chunks * 8;
                if tail_start < len {
                    let mut tail = 0u64;
                    for (j, &b) in bytes[tail_start..].iter().enumerate() {
                        tail |= (b as u64) << (j * 8);
                    }
                    h = wymix(h ^ tail, WY_P1);
                }

                state.write_u64(h);
            }
            Value::Boolean(b) => {
                // Pre-mixed boolean
                state.write_u64(wymix(if *b { 5 } else { 4 }, WY_P1));
            }
            Value::Timestamp(t) => {
                // Hash at full nanosecond precision. Volume segments now store
                // timestamps as i64 nanoseconds, so no precision loss occurs.
                let nanos = t
                    .timestamp_nanos_opt()
                    .unwrap_or_else(|| t.timestamp().saturating_mul(1_000_000_000));
                state.write_u64(wymix(3 ^ (nanos as u64), WY_P1));
            }
            Value::Extension(data) => {
                if let Some((unscaled, _, scale)) = self.as_decimal_parts() {
                    state.write_u64(decimal_hash_word(unscaled, scale));
                    return;
                }

                // Pre-hash extension data with WyHash-style mixing
                // Tag byte is included in data, so discriminant is embedded
                let bytes: &[u8] = data;
                let len = bytes.len();
                let mut h = wymix(10 ^ (len as u64), WY_P1);

                let chunks = len / 8;
                let ptr = bytes.as_ptr();
                for i in 0..chunks {
                    // SAFETY: We iterate i from 0..chunks where chunks = len/8.
                    // So i*8 is always < len, and we read 8 bytes which is valid
                    // since (i+1)*8 <= chunks*8 <= len. read_unaligned handles alignment.
                    let chunk = unsafe { (ptr.add(i * 8) as *const u64).read_unaligned() };
                    h = wymix(h ^ chunk, WY_P2);
                }

                let tail_start = chunks * 8;
                if tail_start < len {
                    let mut tail = 0u64;
                    for (j, &b) in bytes[tail_start..].iter().enumerate() {
                        tail |= (b as u64) << (j * 8);
                    }
                    h = wymix(h ^ tail, WY_P1);
                }

                state.write_u64(h);
            }
        }
    }
}

// Note: PartialOrd intentionally differs from Ord for SQL semantics
// - PartialOrd: SQL comparison (NULL returns None, cross-type numeric comparison)
// - Ord: BTreeMap ordering (NULLs first, type discriminant ordering)
#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Use the original compare method for semantic correctness in SQL operations
        // This preserves NULL comparison semantics (returning None for NULL comparisons)
        // and proper cross-type numeric comparison (Integer vs Float)
        self.compare(other).ok()
    }
}

/// Total ordering implementation for Value
///
/// This is required for using Value as a key in BTreeMap/BTreeSet.
/// The ordering is defined as follows:
/// 1. NULLs are always ordered first (smallest)
/// 2. Numeric types (Integer, Float) are compared by numeric value (consistent with PartialEq)
/// 3. Other different data types are ordered by their type discriminant
/// 4. Same data types use their natural ordering
///
/// IMPORTANT: This ordering MUST be consistent with PartialEq. Since Integer(5) == Float(5.0)
/// per PartialEq, we must ensure Integer(5).cmp(&Float(5.0)) == Ordering::Equal.
/// Violating this contract causes BTreeMap corruption.
///
/// Note: This differs from SQL NULL semantics where NULL comparisons
/// return UNKNOWN. This ordering is only for internal index structure.
impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        // Handle NULL comparisons - NULLs are ordered first
        match (self.is_null(), other.is_null()) {
            (true, true) => return Ordering::Equal,
            (true, false) => return Ordering::Less,
            (false, true) => return Ordering::Greater,
            (false, false) => {} // Continue to value comparison
        }

        if let Some(ordering) = compare_canonical_numeric(self, other) {
            return ordering;
        }

        // Helper function to get type discriminant for ordering
        fn type_discriminant(v: &Value) -> u8 {
            match v {
                Value::Null(_) => 0,
                Value::Boolean(_) => 1,
                // Integer, Float and valid Decimal share the numeric domain.
                Value::Integer(_) | Value::Float(_) => 2,
                Value::Extension(_) if v.as_decimal_parts().is_some() => 2,
                Value::Text(_) => 3,
                Value::Timestamp(_) => 4,
                Value::Extension(_) => 5,
            }
        }

        let self_disc = type_discriminant(self);
        let other_disc = type_discriminant(other);

        // Different types: order by type discriminant
        if self_disc != other_disc {
            return self_disc.cmp(&other_disc);
        }

        // Same type comparison
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => {
                // Handle NaN: NaN is ordered last
                match (a.is_nan(), b.is_nan()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    (false, false) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
                }
            }
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::Extension(a), Value::Extension(b)) => a.cmp(b),
            _ => Ordering::Equal, // Should not reach here
        }
    }
}

// =========================================================================
// From implementations for convenient construction
// =========================================================================

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Integer(v as i64)
    }
}

impl From<i16> for Value {
    fn from(v: i16) -> Self {
        Value::Integer(v as i64)
    }
}

impl From<i8> for Value {
    fn from(v: i8) -> Self {
        Value::Integer(v as i64)
    }
}

impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Value::Integer(v as i64)
    }
}

impl From<u16> for Value {
    fn from(v: u16) -> Self {
        Value::Integer(v as i64)
    }
}

impl From<u8> for Value {
    fn from(v: u8) -> Self {
        Value::Integer(v as i64)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Float(v as f64)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(SmartString::from_string(v))
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(SmartString::from(v))
    }
}

impl From<Arc<str>> for Value {
    fn from(v: Arc<str>) -> Self {
        Value::Text(SmartString::from(v.as_ref()))
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Boolean(v)
    }
}

impl From<DateTime<Utc>> for Value {
    fn from(v: DateTime<Utc>) -> Self {
        Value::Timestamp(v)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(val) => val.into(),
            None => Value::Null(DataType::Null),
        }
    }
}

// =========================================================================
// Helper functions
// =========================================================================

/// Parse a timestamp string with multiple format support
pub fn parse_timestamp(s: &str) -> Result<DateTime<Utc>> {
    let s = s.trim();

    // Try each timestamp format
    for format in TIMESTAMP_FORMATS {
        if let Ok(dt) = DateTime::parse_from_str(s, format) {
            return Ok(dt.with_timezone(&Utc));
        }
        // Try parsing as naive datetime and assume UTC
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, format) {
            return Ok(Utc.from_utc_datetime(&ndt));
        }
    }

    // Try date-only formats
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let datetime = date.and_hms_opt(0, 0, 0).unwrap();
        return Ok(Utc.from_utc_datetime(&datetime));
    }

    // Try time-only formats (use today's date)
    for format in TIME_FORMATS {
        if let Ok(time) = NaiveTime::parse_from_str(s, format) {
            let today = Utc::now().date_naive();
            let datetime = today.and_time(time);
            return Ok(Utc.from_utc_datetime(&datetime));
        }
    }

    Err(Error::parse(format!("invalid timestamp format: {}", s)))
}

/// Format a float value consistently
fn format_float(v: f64) -> String {
    // Handle special cases
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v.is_sign_positive() {
            "Infinity"
        } else {
            "-Infinity"
        }
        .to_string();
    }

    let abs_v = v.abs();

    // Use scientific notation for very large or very small numbers
    if abs_v != 0.0 && !(1e-4..1e15).contains(&abs_v) {
        // Use scientific notation with up to 15 significant digits
        let s = format!("{:e}", v);
        // Clean up trailing zeros in mantissa
        if let Some(e_pos) = s.find('e') {
            let (mantissa, exp) = s.split_at(e_pos);
            let clean_mantissa = if mantissa.contains('.') {
                mantissa
                    .trim_end_matches('0')
                    .trim_end_matches('.')
                    .to_string()
            } else {
                mantissa.to_string()
            };
            return format!("{}{}", clean_mantissa, exp);
        }
        return s;
    }

    if v.fract() == 0.0 {
        // Integer-like float, format without decimal
        format!("{:.0}", v)
    } else {
        // Use standard representation for normal range
        let s = format!("{:?}", v);
        // Remove trailing zeros after decimal point
        if s.contains('.') && !s.contains('e') && !s.contains('E') {
            s.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            s
        }
    }
}

/// Format packed LE f32 bytes as "[1.0, 2.0, 3.0]" string
pub fn format_vector_bytes(data: &[u8]) -> String {
    let len = data.len() / 4;
    let mut s = String::with_capacity(len * 8 + 2);
    s.push('[');
    for i in 0..len {
        if i > 0 {
            s.push_str(", ");
        }
        let f = f32::from_le_bytes([
            data[i * 4],
            data[i * 4 + 1],
            data[i * 4 + 2],
            data[i * 4 + 3],
        ]);
        use std::fmt::Write;
        if f.fract() == 0.0 && f.is_finite() {
            let _ = write!(s, "{:.1}", f);
        } else {
            let _ = write!(s, "{}", f);
        }
    }
    s.push(']');
    s
}

/// Parse a UUID string into raw 16-byte UUID storage.
///
/// Accepts the standard hyphenated form and the compact 32-hex form supported
/// by the `uuid` crate. Short strings are rejected; no implicit zero-padding is
/// part of the SQL contract.
pub fn parse_uuid_str(s: &str) -> Option<[u8; 16]> {
    Uuid::parse_str(s.trim()).ok().map(|uuid| *uuid.as_bytes())
}

/// Format raw 16-byte UUID storage as canonical lowercase hyphenated text.
pub fn format_uuid_bytes(data: &[u8]) -> Option<String> {
    let bytes: [u8; 16] = data.try_into().ok()?;
    Some(Uuid::from_bytes(bytes).hyphenated().to_string())
}

pub const MAX_DECIMAL_PRECISION: u8 = 38;

/// Validate the declared physical DECIMAL payload.
pub fn validate_decimal_shape(unscaled: i128, precision: u8, scale: u8) -> Result<()> {
    if !(1..=MAX_DECIMAL_PRECISION).contains(&precision) {
        return Err(Error::invalid_argument(format!(
            "DECIMAL precision {precision} is outside supported range 1..={MAX_DECIMAL_PRECISION}"
        )));
    }
    if scale > precision {
        return Err(Error::invalid_argument(format!(
            "DECIMAL scale {scale} exceeds declared precision {precision}"
        )));
    }
    let digits = unscaled.unsigned_abs().to_string().len().max(1);
    if digits > usize::from(precision) {
        return Err(Error::invalid_argument(format!(
            "DECIMAL coefficient has {digits} digits but precision is {precision}"
        )));
    }
    Ok(())
}

#[inline]
fn checked_float_to_i64(value: f64) -> Option<i64> {
    const I64_EXCLUSIVE_UPPER_F64: f64 = 9_223_372_036_854_775_808.0;
    const I64_INCLUSIVE_LOWER_F64: f64 = -9_223_372_036_854_775_808.0;
    (value.is_finite() && (I64_INCLUSIVE_LOWER_F64..I64_EXCLUSIVE_UPPER_F64).contains(&value))
        .then_some(value as i64)
}

#[inline]
fn parse_text_to_i64(value: &str) -> Option<i64> {
    value
        .parse::<i64>()
        .ok()
        .or_else(|| value.parse::<f64>().ok().and_then(checked_float_to_i64))
}

#[inline]
fn datetime_from_epoch_nanos(nanos: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(
        nanos.div_euclid(1_000_000_000),
        nanos.rem_euclid(1_000_000_000) as u32,
    )
}

/// Infer precision for an integer decimal payload.
fn decimal_precision_for_unscaled(value: i64) -> u8 {
    decimal_precision_for_unscaled_i128(value as i128)
}

fn decimal_precision_for_unscaled_i128(value: i128) -> u8 {
    let digits = value.unsigned_abs().to_string().len().max(1);
    digits.min(MAX_DECIMAL_PRECISION as usize) as u8
}

#[inline]
fn decimal_scale_factor(scale: u8) -> Option<i128> {
    (scale <= MAX_DECIMAL_PRECISION)
        .then(|| 10_i128.checked_pow(scale as u32))
        .flatten()
}

/// Canonical numeric identity for an exact base-10 value.
///
/// `coefficient * 10^exponent` has no trailing coefficient zeroes. Precision
/// metadata is deliberately absent: it remains in the physical Decimal bytes,
/// but it is not part of numeric equality, hashing or ordering.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DecimalIdentity {
    negative: bool,
    coefficient: u128,
    exponent: i32,
}

impl DecimalIdentity {
    fn new(negative: bool, mut coefficient: u128, mut exponent: i32) -> Self {
        if coefficient == 0 {
            return Self {
                negative: false,
                coefficient: 0,
                exponent: 0,
            };
        }

        while coefficient.is_multiple_of(10) {
            coefficient /= 10;
            exponent += 1;
        }

        Self {
            negative,
            coefficient,
            exponent,
        }
    }

    fn from_parts(unscaled: i128, scale: u8) -> Self {
        Self::new(
            unscaled.is_negative(),
            unscaled.unsigned_abs(),
            -i32::from(scale),
        )
    }

    /// Parse Rust's shortest round-trippable finite FLOAT rendering into the
    /// same exact base-10 identity used by Decimal.
    fn from_float(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }

        // LowerExp keeps the significand bounded to the f64 round-trip digit
        // count even for values near f64::{MIN_POSITIVE, MAX}.
        let rendered = format!("{value:e}");
        let (mantissa, exponent) = rendered.split_once('e')?;
        let exponent = exponent.parse::<i32>().ok()?;
        let (negative, mantissa) = mantissa
            .strip_prefix('-')
            .map_or((false, mantissa), |unsigned| (true, unsigned));
        let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let digits = format!("{integer}{fraction}");
        let coefficient = digits.parse::<u128>().ok()?;
        let fraction_len = i32::try_from(fraction.len()).ok()?;

        Some(Self::new(
            negative,
            coefficient,
            exponent.checked_sub(fraction_len)?,
        ))
    }

    fn exact_i64(&self) -> Option<i64> {
        if self.coefficient == 0 {
            return Some(0);
        }
        let exponent = u32::try_from(self.exponent).ok()?;
        let magnitude = self
            .coefficient
            .checked_mul(10_u128.checked_pow(exponent)?)?;
        if self.negative {
            if magnitude == (i64::MAX as u128) + 1 {
                Some(i64::MIN)
            } else {
                i64::try_from(magnitude).ok().map(|value| -value)
            }
        } else {
            i64::try_from(magnitude).ok()
        }
    }

    fn cmp_magnitude(&self, other: &Self) -> Ordering {
        debug_assert!(self.coefficient != 0 && other.coefficient != 0);

        let left_digits = self.coefficient.to_string();
        let right_digits = other.coefficient.to_string();
        let left_order = i32::try_from(left_digits.len())
            .unwrap_or(i32::MAX)
            .saturating_add(self.exponent);
        let right_order = i32::try_from(right_digits.len())
            .unwrap_or(i32::MAX)
            .saturating_add(other.exponent);
        match left_order.cmp(&right_order) {
            Ordering::Equal => {}
            ordering => return ordering,
        }

        let width = left_digits.len().max(right_digits.len());
        for index in 0..width {
            let left = left_digits.as_bytes().get(index).copied().unwrap_or(b'0');
            let right = right_digits.as_bytes().get(index).copied().unwrap_or(b'0');
            match left.cmp(&right) {
                Ordering::Equal => {}
                ordering => return ordering,
            }
        }
        Ordering::Equal
    }

    fn cmp_numeric(&self, other: &Self) -> Ordering {
        if self == other {
            return Ordering::Equal;
        }

        match (self.coefficient == 0, other.coefficient == 0) {
            (true, true) => return Ordering::Equal,
            (true, false) => {
                return if other.negative {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            (false, true) => {
                return if self.negative {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            (false, false) => {}
        }

        match self.negative.cmp(&other.negative) {
            Ordering::Less => Ordering::Greater,
            Ordering::Greater => Ordering::Less,
            Ordering::Equal if self.negative => self.cmp_magnitude(other).reverse(),
            Ordering::Equal => self.cmp_magnitude(other),
        }
    }
}

fn compare_decimal_float(decimal: &DecimalIdentity, float: f64) -> Ordering {
    if float.is_nan() || float == f64::INFINITY {
        return Ordering::Less;
    }
    if float == f64::NEG_INFINITY {
        return Ordering::Greater;
    }

    decimal.cmp_numeric(
        &DecimalIdentity::from_float(float)
            .expect("every finite f64 has a shortest decimal identity"),
    )
}

/// Single owner for structural numeric identity across Integer, Float and
/// valid Decimal payloads. `None` means at least one operand is non-numeric.
fn compare_canonical_numeric(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Integer(left), Value::Integer(right)) => Some(left.cmp(right)),
        (Value::Float(left), Value::Float(right)) => Some(compare_floats(*left, *right)),
        (Value::Integer(integer), Value::Float(float)) => {
            Some(compare_integer_float(*integer, *float))
        }
        (Value::Float(float), Value::Integer(integer)) => {
            Some(compare_integer_float(*integer, *float).reverse())
        }
        _ => {
            let left_decimal = left
                .as_decimal_parts()
                .map(|(unscaled, _, scale)| DecimalIdentity::from_parts(unscaled, scale));
            let right_decimal = right
                .as_decimal_parts()
                .map(|(unscaled, _, scale)| DecimalIdentity::from_parts(unscaled, scale));

            match (left_decimal, right_decimal, left, right) {
                (Some(left), Some(right), _, _) => Some(left.cmp_numeric(&right)),
                (Some(left), None, _, Value::Integer(right)) => {
                    Some(left.cmp_numeric(&DecimalIdentity::from_parts(*right as i128, 0)))
                }
                (None, Some(right), Value::Integer(left), _) => {
                    Some(DecimalIdentity::from_parts(*left as i128, 0).cmp_numeric(&right))
                }
                (Some(left), None, _, Value::Float(right)) => {
                    Some(compare_decimal_float(&left, *right))
                }
                (None, Some(right), Value::Float(left), _) => {
                    Some(compare_decimal_float(&right, *left).reverse())
                }
                _ => None,
            }
        }
    }
}

fn decimal_hash_word(unscaled: i128, scale: u8) -> u64 {
    let identity = DecimalIdentity::from_parts(unscaled, scale);
    if let Some(integer) = identity.exact_i64() {
        return integer_hash_word(integer);
    }

    // A Decimal equals a finite Float only when that Float's shortest
    // round-trip decimal identity is exactly the same. Preserve the existing
    // Float hash domain for that equality class without changing Float's hot
    // hashing path.
    if let Ok(float) = format_decimal_parts(unscaled, scale).parse::<f64>() {
        if float.is_finite() && DecimalIdentity::from_float(float).as_ref() == Some(&identity) {
            return float_hash_word(float);
        }
    }

    let low = identity.coefficient as u64;
    let high = (identity.coefficient >> 64) as u64;
    let sign = u64::from(identity.negative);
    let exponent = identity.exponent as i64 as u64;
    let mut hash = wymix(7 ^ low, WY_P1);
    hash = wymix(hash ^ high, WY_P2);
    hash = wymix(hash ^ exponent, WY_P1);
    wymix(hash ^ sign, WY_P2)
}

/// Convert a finite binary FLOAT through Rust's shortest round-trippable
/// decimal representation. This defines the mixed FLOAT/DECIMAL contract
/// without importing the binary approximation itself into exact DECIMAL
/// storage.
fn parse_decimal_f64(value: f64) -> Option<(i128, u8, u8)> {
    if !value.is_finite() {
        return None;
    }

    let rendered = value.to_string();
    let Some(exponent_offset) = rendered.find(['e', 'E']) else {
        return parse_decimal_str(&rendered);
    };

    let (mantissa, exponent_with_marker) = rendered.split_at(exponent_offset);
    let exponent = exponent_with_marker[1..].parse::<i32>().ok()?;
    let (mut unscaled, _, mantissa_scale) = parse_decimal_str(mantissa)?;
    let resulting_scale = i32::from(mantissa_scale).checked_sub(exponent)?;

    let scale = if resulting_scale < 0 {
        let power = u8::try_from(resulting_scale.checked_neg()?).ok()?;
        unscaled = unscaled.checked_mul(decimal_scale_factor(power)?)?;
        0
    } else {
        u8::try_from(resulting_scale).ok()?
    };
    if scale > MAX_DECIMAL_PRECISION
        || unscaled.unsigned_abs().to_string().len() > MAX_DECIMAL_PRECISION as usize
    {
        return None;
    }

    Some((
        unscaled,
        decimal_precision_for_unscaled_i128(unscaled),
        scale,
    ))
}

/// Parse a plain decimal string into exact wire-style decimal parts.
///
/// This intentionally accepts only ordinary fixed-point forms such as
/// `12`, `-12.30` and `.5`. Exponent notation belongs to FLOAT, not exact
/// DECIMAL, until the SQL decimal surface is expanded deliberately.
pub fn parse_decimal_str(s: &str) -> Option<(i128, u8, u8)> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }

    let (negative, body) = match trimmed.as_bytes()[0] {
        b'-' => (true, &trimmed[1..]),
        b'+' => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };
    if body.is_empty() {
        return None;
    }

    let mut parts = body.split('.');
    let int_part = parts.next().unwrap_or("");
    let frac_part = parts.next();
    if parts.next().is_some() {
        return None;
    }

    let frac = frac_part.unwrap_or("");
    if int_part.is_empty() && frac.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit()) || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }

    let scale = u8::try_from(frac.len()).ok()?;
    if scale > MAX_DECIMAL_PRECISION {
        return None;
    }

    let digits = format!("{int_part}{frac}");
    let normalized = digits.trim_start_matches('0');
    let precision_len = normalized.len().max(usize::from(scale)).max(1);
    if precision_len > MAX_DECIMAL_PRECISION as usize {
        return None;
    }
    let precision = precision_len as u8;

    let magnitude = if normalized.is_empty() {
        0
    } else {
        normalized.parse::<i128>().ok()?
    };
    Some((
        if negative { -magnitude } else { magnitude },
        precision,
        scale,
    ))
}

pub fn format_decimal_parts(unscaled: i128, scale: u8) -> String {
    if scale == 0 {
        return unscaled.to_string();
    }

    let negative = unscaled.is_negative();
    let mut digits = unscaled.unsigned_abs().to_string();
    let scale_len = scale as usize;
    if digits.len() <= scale_len {
        let mut padded = String::with_capacity(scale_len + 1);
        padded.push_str(&"0".repeat(scale_len + 1 - digits.len()));
        padded.push_str(&digits);
        digits = padded;
    }
    let split = digits.len() - scale_len;
    let mut out = String::with_capacity(digits.len() + 2);
    if negative {
        out.push('-');
    }
    out.push_str(&digits[..split]);
    out.push('.');
    out.push_str(&digits[split..]);
    out
}

#[doc(hidden)]
pub fn compare_decimal_parts(
    left_unscaled: i128,
    left_scale: u8,
    right_unscaled: i128,
    right_scale: u8,
) -> Ordering {
    DecimalIdentity::from_parts(left_unscaled, left_scale)
        .cmp_numeric(&DecimalIdentity::from_parts(right_unscaled, right_scale))
}

pub fn parse_date_days_since_unix_epoch(s: &str) -> Option<i32> {
    let date = NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok()?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    i32::try_from(date.signed_duration_since(epoch).num_days()).ok()
}

pub fn format_date_days_since_unix_epoch(days: i32) -> Option<String> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    epoch
        .checked_add_signed(chrono::Duration::days(days as i64))
        .map(|date| date.format("%Y-%m-%d").to_string())
}

pub fn format_bytes_hex(data: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(2 + data.len() * 2);
    out.push_str("0x");
    for byte in data {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Parse a vector string in [f32, f32, ...] format
pub fn parse_vector_str(s: &str) -> Option<Vec<f32>> {
    let s = s.trim();
    let inner = s.strip_prefix('[')?.strip_suffix(']')?;
    if inner.trim().is_empty() {
        return Some(Vec::new());
    }
    let mut result = Vec::new();
    for part in inner.split(',') {
        let val: f32 = part.trim().parse().ok()?;
        result.push(val);
    }
    Some(result)
}

/// Compare two floats with proper NaN handling
fn compare_floats(a: f64, b: f64) -> Ordering {
    // Handle NaN: treat as greater than all other values for consistency
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
    }
}

#[cfg(test)]
mod tests;
