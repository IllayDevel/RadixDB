//! Versioned binary wire contract shared by RadixDB clients and servers.
//!
//! This crate owns only messages, values, validation and bounded framing. It
//! has no dependency on the client transport, server runtime, SQL executor or
//! storage engine.

use std::{
    collections::BTreeMap,
    io::{Read, Write},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

/// Version 17 adds catalog-bound external scalar values. Version 16 added
/// observable stock Job scheduler counters. Version 15 added
/// database-bound catalog Principal authentication. Older peers are
/// rejected at handshake because bincode enum layouts are not self-describing.
///
/// A client must know whether a failed COMMIT remains rollback-capable or was
/// atomically aborted by a durability failure. Older bincode decoders do not
/// know `TransactionFailed`, so the handshake rejects them explicitly.
pub const PROTOCOL_VERSION: u16 = 17;
pub const DEFAULT_MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024;
/// A validated endpoint must be able to carry every mandatory handshake/control
/// response even when the data-frame limit is configured aggressively low.
pub const MIN_CONTROL_FRAME_BYTES: u32 = 256;
/// Maximum memory bincode may claim while decoding one frame. This is distinct
/// from the encoded frame limit: container element sizes are charged before
/// allocation, preventing a compact length claim from expanding without bound.
pub const DEFAULT_MAX_DECODED_BYTES: usize = 256 * 1024 * 1024;
/// Hard protocol ceiling for one canonical external scalar payload. A catalog
/// type may declare a lower bound, which the server enforces before execution.
pub const MAX_EXTERNAL_VALUE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientProtocolCountersSnapshot {
    pub frame_decode_calls: u64,
    pub frame_decode_bytes: u64,
    pub frame_decode_nanos: u64,
}

struct ClientProtocolCounters {
    frame_decode_calls: AtomicU64,
    frame_decode_bytes: AtomicU64,
    frame_decode_nanos: AtomicU64,
}

impl ClientProtocolCounters {
    const fn new() -> Self {
        Self {
            frame_decode_calls: AtomicU64::new(0),
            frame_decode_bytes: AtomicU64::new(0),
            frame_decode_nanos: AtomicU64::new(0),
        }
    }

    fn reset(&self) {
        self.frame_decode_calls.store(0, Ordering::Relaxed);
        self.frame_decode_bytes.store(0, Ordering::Relaxed);
        self.frame_decode_nanos.store(0, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ClientProtocolCountersSnapshot {
        ClientProtocolCountersSnapshot {
            frame_decode_calls: self.frame_decode_calls.load(Ordering::Relaxed),
            frame_decode_bytes: self.frame_decode_bytes.load(Ordering::Relaxed),
            frame_decode_nanos: self.frame_decode_nanos.load(Ordering::Relaxed),
        }
    }
}

static CLIENT_PROTOCOL_COUNTERS: ClientProtocolCounters = ClientProtocolCounters::new();
pub fn client_protocol_counters_snapshot() -> ClientProtocolCountersSnapshot {
    CLIENT_PROTOCOL_COUNTERS.snapshot()
}

pub fn reset_client_protocol_counters() {
    CLIENT_PROTOCOL_COUNTERS.reset();
}

#[derive(Debug)]
pub enum ProtocolError {
    Io(std::io::Error),
    Encode(bincode::error::EncodeError),
    Decode(bincode::error::DecodeError),
    FrameTooLarge { actual: u32, limit: u32 },
    InvalidFrameLength,
    InvalidBatchShape(String),
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::Encode(error) => error.fmt(formatter),
            Self::Decode(error) => error.fmt(formatter),
            Self::FrameTooLarge { actual, limit } => {
                write!(
                    formatter,
                    "binary frame size {actual} exceeds negotiated limit {limit}"
                )
            }
            Self::InvalidFrameLength => formatter.write_str("invalid binary frame length"),
            Self::InvalidBatchShape(message) => {
                write!(formatter, "invalid result batch shape: {message}")
            }
        }
    }
}

impl std::error::Error for ProtocolError {}

impl From<std::io::Error> for ProtocolError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ClientMessage {
    Handshake {
        protocol_version: u16,
        max_frame_bytes: u32,
        capabilities: Vec<ProtocolCapability>,
    },
    Authenticate {
        login: String,
        password: Option<String>,
    },
    /// Authenticate against the durable security catalog of one database and
    /// atomically select it. The server, not the client, resolves the stable
    /// Principal ID used by every later request.
    AuthenticatePrincipal {
        database: String,
        login: String,
        password: String,
    },
    SelectDatabase {
        database: String,
    },
    Execute {
        request_id: u64,
        sql: String,
        positional: Vec<WireValue>,
        named: BTreeMap<String, WireValue>,
    },
    Prepare {
        sql: String,
    },
    ExecutePrepared {
        request_id: u64,
        statement_id: u64,
        positional: Vec<WireValue>,
        named: BTreeMap<String, WireValue>,
    },
    ClosePrepared {
        statement_id: u64,
    },
    /// Out-of-band cancellation. Send this from another authenticated
    /// connection while `request_id` executes on its owner connection.
    CancelExecution {
        request_id: u64,
    },
    Fetch {
        cursor_id: u64,
    },
    CloseCursor {
        cursor_id: u64,
    },
    Cancel {
        cursor_id: u64,
    },
    BeginTransaction {
        isolation: TransactionIsolation,
    },
    CommitTransaction,
    RollbackTransaction,
    CreateSavepoint {
        name: String,
    },
    RollbackToSavepoint {
        name: String,
    },
    ReleaseSavepoint {
        name: String,
    },
    CloseDatabase {
        database: String,
    },
    /// Fetch an active cursor as one decoded typed column batch when the
    /// negotiated capability and the query's exact semantics permit it.
    /// The server falls back to `RowBatch` for all other cursor shapes.
    FetchColumnBatch {
        cursor_id: u64,
    },
    ServerStatus {
        database: Option<String>,
    },
}

/// Isolation level requested for a dedicated wire transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionIsolation {
    ReadCommitted,
    Snapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ServerMessage {
    HandshakeAccepted {
        protocol_version: u16,
        max_frame_bytes: u32,
        capabilities: Vec<ProtocolCapability>,
    },
    AuthenticationAccepted,
    DatabaseSelected {
        database: String,
    },
    Prepared {
        statement_id: u64,
    },
    PreparedClosed {
        statement_id: u64,
    },
    ExecutionCancelled {
        request_id: u64,
        found: bool,
    },
    CommandComplete {
        affected_rows: u64,
        last_insert_id: u64,
    },
    CursorOpened {
        cursor_id: u64,
        columns: Vec<Column>,
    },
    RowBatch {
        cursor_id: u64,
        rows: Vec<Row>,
        eof: bool,
    },
    CursorClosed {
        cursor_id: u64,
    },
    CursorFailed {
        cursor_id: u64,
        failure: ProtocolFailure,
        active: bool,
    },
    TransactionBegan,
    TransactionCommitted,
    TransactionRolledBack,
    SavepointCreated {
        name: String,
    },
    SavepointRolledBack {
        name: String,
    },
    SavepointReleased {
        name: String,
    },
    DatabaseClosed {
        database: String,
    },
    TransactionFailed {
        failure: ProtocolFailure,
        active: bool,
    },
    Error(ProtocolFailure),
    /// A decoded typed result batch. Column order is exactly the order from
    /// `CursorOpened`; each column has `row_count` values and null markers.
    ColumnBatch {
        cursor_id: u64,
        columns: Vec<WireColumn>,
        row_count: u32,
        eof: bool,
    },
    ServerStatus(Box<ServerStatus>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ProtocolCapability {
    /// Enables `FetchColumnBatch` / `ColumnBatch` for eligible artifact-backed scans.
    ColumnBatchV1,
    /// Includes a machine-readable build identity card in `ServerStatus`.
    BuildIdentityV1,
    /// Admits typed external scalar values and external column batches.
    ExternalValueV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerStatus {
    /// Present only when `BuildIdentityV1` was negotiated.
    pub build: Option<BuildIdentity>,
    pub lifecycle: ServerLifecycleState,
    pub ready: bool,
    pub message: String,
    pub databases: Vec<DatabaseStatus>,
    pub runtime: ServerRuntimeStatus,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerRuntimeStatus {
    /// Databases with a live, ready engine instance.
    pub open_databases: u64,
    /// All registry entries, including opening and retryable failed entries.
    pub retained_databases: u64,
    pub max_databases: u64,
    pub active_connections: u64,
    pub max_connections: u64,
    pub inflight_frame_bytes: u64,
    pub max_inflight_frame_bytes: u64,
    pub job_scheduler_cycles: u64,
    pub job_attempts_started: u64,
    pub job_attempts_succeeded: u64,
    pub job_attempts_failed: u64,
    pub job_attempts_active: u64,
    pub job_scheduler_last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildIdentity {
    pub semantic_version: String,
    pub git_revision: String,
    pub protocol_version: u16,
    pub build_profile: String,
    pub target: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseStatus {
    pub name: String,
    pub lifecycle: ServerLifecycleState,
    pub ready: bool,
    pub message: String,
    pub artifacts: DatabaseArtifactSummary,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseArtifactSummary {
    /// True only when every directory entry and file type was inspected.
    pub complete: bool,
    /// True when an explicit traversal budget stopped the inventory.
    pub truncated: bool,
    /// Retained snapshot trees are intentionally excluded from readiness polling.
    pub snapshots_omitted: bool,
    /// Monotonic process-local identity of this inventory sample.
    pub sequence: u64,
    /// Wall-clock capture time used by operators to detect a stale sample.
    pub sampled_unix_millis: u64,
    /// Entries whose type was inspected before completion or truncation.
    pub entries_visited: u64,
    /// Number of filesystem traversal errors observed while collecting.
    pub scan_errors: u64,
    pub table_dirs: u64,
    pub wal_files: u64,
    /// Immutable `.data` and `.idx` artifacts.
    pub artifact_files: u64,
    pub snapshot_files: u64,
    /// Number of committed CONTROL slots.
    pub checkpoint_files: u64,
    /// Number of database/table manifest and catalog files.
    pub manifest_files: u64,
    pub other_files: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerLifecycleState {
    Starting,
    Recovering,
    Opening,
    Warming,
    Ready,
    Degraded,
    Emergency,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolFailure {
    pub code: ProtocolErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolErrorCode {
    ProtocolViolation,
    AuthenticationFailed,
    AuthorizationDenied,
    DatabaseNotFound,
    SqlError,
    CursorNotFound,
    CommandsOutOfSync,
    TransactionState,
    ServerError,
    CompactionBackpressure,
    UnsupportedType,
}

impl ProtocolErrorCode {
    /// The server proved that the failed logical action did not publish and
    /// may be submitted again after a transient condition changes.
    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::CompactionBackpressure)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    /// Stable catalog identity and codec of an external type. Built-in columns
    /// must publish `None`; an external value is never disguised as `BYTES`.
    #[serde(default)]
    pub external_type: Option<ExternalTypeRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalTypeRef {
    pub type_object_id: [u8; 16],
    pub codec_version: u32,
}

impl ExternalTypeRef {
    fn validate(self) -> Result<(), String> {
        if self.type_object_id == [0; 16] {
            return Err("external type object ID must not be zero".to_string());
        }
        if self.codec_version == 0 {
            return Err("external codec version must be at least 1".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<WireValue>,
}

/// Column-major representation used by the optional artifact-backed result path.
///
/// Values intentionally retain the same public value domain as `WireValue`:
/// timestamps are UTC nanoseconds, dictionary text remains UTF-8, binary
/// bytes stay raw, and JSON text is UTF-8 bytes. The layout avoids allocating
/// one `Row` and one `WireValue` per cell.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WireColumn {
    Int64 {
        values: Vec<i64>,
        nulls: Vec<bool>,
    },
    Float64 {
        values: Vec<f64>,
        nulls: Vec<bool>,
    },
    Boolean {
        values: Vec<bool>,
        nulls: Vec<bool>,
    },
    TimestampNanos {
        values: Vec<i64>,
        nulls: Vec<bool>,
    },
    DictionaryText {
        ids: Vec<u32>,
        dictionary: Vec<String>,
        nulls: Vec<bool>,
    },
    Bytes {
        data: Vec<u8>,
        offsets: Vec<(u64, u64)>,
        nulls: Vec<bool>,
    },
    JsonText {
        data: Vec<u8>,
        offsets: Vec<(u64, u64)>,
        nulls: Vec<bool>,
    },
    External {
        type_object_id: [u8; 16],
        codec_version: u32,
        data: Vec<u8>,
        offsets: Vec<u32>,
        nulls: Vec<bool>,
    },
}

/// Scalar values admitted by protocol v17. Recursive collection/object values
/// are intentionally absent: no SQL/server owner supported them, and removing
/// them gives the unauthenticated decoder a fixed nesting depth.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WireValue {
    Null,
    Bool(bool),
    Int(i64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    UInt(u64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    Float64(f64),
    Decimal {
        unscaled: i128,
        precision: u8,
        scale: u8,
    },
    String(String),
    Bytes(Vec<u8>),
    Date {
        days_since_unix_epoch: i32,
    },
    DateTime {
        millis_since_unix_epoch_utc: i64,
    },
    TimestampNanos {
        nanos_since_unix_epoch_utc: i64,
    },
    Json(String),
    /// Packed little-endian IEEE-754 `f32` elements.
    Vector(Vec<u8>),
    Uuid([u8; 16]),
    External {
        type_object_id: [u8; 16],
        codec_version: u32,
        payload: Vec<u8>,
    },
}

/// Validate the physical DECIMAL payload before it becomes an engine value.
pub fn validate_decimal_shape(unscaled: i128, precision: u8, scale: u8) -> Result<(), String> {
    if precision == 0 || precision > 38 {
        return Err(format!(
            "Decimal precision {precision} is outside supported range 1..=38"
        ));
    }
    if scale > precision {
        return Err(format!(
            "Decimal scale {scale} exceeds declared precision {precision}"
        ));
    }
    let digits = unscaled.unsigned_abs().to_string().len().max(1);
    if digits > usize::from(precision) {
        return Err(format!(
            "Decimal coefficient has {digits} digits but precision is {precision}"
        ));
    }
    Ok(())
}

/// Validate a row-major response against the metadata published at cursor open.
pub fn validate_row_batch(rows: &[Row], columns: &[Column]) -> Result<(), ProtocolError> {
    let expected_columns = columns.len();
    if let Some((row_index, row)) = rows
        .iter()
        .enumerate()
        .find(|(_, row)| row.values.len() != expected_columns)
    {
        return Err(ProtocolError::InvalidBatchShape(format!(
            "row {row_index} has {} values, expected {expected_columns}",
            row.values.len()
        )));
    }
    for (row_index, row) in rows.iter().enumerate() {
        for (column_index, (value, column)) in row.values.iter().zip(columns).enumerate() {
            validate_wire_value(value, column).map_err(|message| {
                ProtocolError::InvalidBatchShape(format!(
                    "row {row_index} column {column_index}: {message}"
                ))
            })?;
        }
    }
    Ok(())
}

/// Validate a column-major response before it becomes caller-visible.
pub fn validate_column_batch(
    columns: &[WireColumn],
    row_count: u32,
    expected_columns: &[Column],
    eof: bool,
) -> Result<(), ProtocolError> {
    if columns.is_empty() && row_count == 0 && eof {
        return Ok(());
    }
    if columns.len() != expected_columns.len() {
        return Err(ProtocolError::InvalidBatchShape(format!(
            "batch has {} columns, expected {}",
            columns.len(),
            expected_columns.len()
        )));
    }
    let row_count = usize::try_from(row_count).map_err(|_| {
        ProtocolError::InvalidBatchShape("row_count does not fit usize".to_string())
    })?;
    for (index, column) in columns.iter().enumerate() {
        column.validate_shape(index, row_count)?;
        column.validate_contract(index, &expected_columns[index])?;
    }
    Ok(())
}

fn normalized_type_name(name: &str) -> String {
    name.trim().to_ascii_uppercase()
}

fn validate_wire_value(value: &WireValue, column: &Column) -> Result<(), String> {
    if matches!(value, WireValue::Null) {
        return column
            .nullable
            .then_some(())
            .ok_or_else(|| format!("NULL violates non-nullable `{}`", column.name));
    }
    if let WireValue::External {
        type_object_id,
        codec_version,
        payload,
    } = value
    {
        let value_type = ExternalTypeRef {
            type_object_id: *type_object_id,
            codec_version: *codec_version,
        };
        value_type.validate()?;
        if payload.len() > MAX_EXTERNAL_VALUE_BYTES {
            return Err(format!(
                "external payload has {} bytes, maximum is {MAX_EXTERNAL_VALUE_BYTES}",
                payload.len()
            ));
        }
        return (column.external_type == Some(value_type))
            .then_some(())
            .ok_or_else(|| {
                format!(
                    "external value identity is incompatible with published type {}",
                    column.type_name
                )
            });
    }
    if column.external_type.is_some() {
        return Err(format!(
            "built-in wire value is incompatible with external type {}",
            column.type_name
        ));
    }
    let ty = normalized_type_name(&column.type_name);
    if ty == "UNKNOWN" {
        return Ok(());
    }
    let compatible = match ty.as_str() {
        "BOOLEAN" | "BOOL" => matches!(value, WireValue::Bool(_)),
        "TINYINT" => matches!(value, WireValue::Int8(_)),
        "SMALLINT" => matches!(value, WireValue::Int16(_)),
        "INTEGER" | "INT" | "BIGINT" => matches!(value, WireValue::Int(_) | WireValue::Int32(_)),
        "UNSIGNED" | "UINT" => matches!(
            value,
            WireValue::UInt(_) | WireValue::UInt8(_) | WireValue::UInt16(_) | WireValue::UInt32(_)
        ),
        "FLOAT" | "DOUBLE" | "REAL" => matches!(value, WireValue::Float64(_)),
        name if name.starts_with("DECIMAL") => matches!(value, WireValue::Decimal { .. }),
        "TEXT" | "STRING" | "VARCHAR" => matches!(value, WireValue::String(_)),
        "BYTES" | "BLOB" | "BINARY" => matches!(value, WireValue::Bytes(_)),
        "DATE" => matches!(value, WireValue::Date { .. }),
        "DATETIME" => matches!(value, WireValue::DateTime { .. }),
        "TIMESTAMP" => matches!(value, WireValue::TimestampNanos { .. }),
        "JSON" => {
            matches!(value, WireValue::Json(text) if serde_json::from_str::<serde_json::Value>(text).is_ok())
        }
        "VECTOR" => matches!(value, WireValue::Vector(bytes) if bytes.len() % 4 == 0),
        "UUID" => matches!(value, WireValue::Uuid(_)),
        _ => true,
    };
    compatible.then_some(()).ok_or_else(|| {
        format!(
            "value is incompatible with published type {}",
            column.type_name
        )
    })
}

impl WireColumn {
    fn validate_shape(&self, index: usize, row_count: usize) -> Result<(), ProtocolError> {
        let invalid = |message: String| {
            ProtocolError::InvalidBatchShape(format!("column {index}: {message}"))
        };
        let (values_len, nulls) = match self {
            Self::Int64 { values, nulls } | Self::TimestampNanos { values, nulls } => {
                (values.len(), nulls)
            }
            Self::Float64 { values, nulls } => (values.len(), nulls),
            Self::Boolean { values, nulls } => (values.len(), nulls),
            Self::DictionaryText {
                ids,
                dictionary,
                nulls,
            } => {
                if let Some((row, id)) = ids.iter().enumerate().find(|(row, id)| {
                    !nulls.get(*row).copied().unwrap_or(false)
                        && usize::try_from(**id).map_or(true, |id| id >= dictionary.len())
                }) {
                    return Err(invalid(format!(
                        "row {row} dictionary id {id} is outside {} entries",
                        dictionary.len()
                    )));
                }
                (ids.len(), nulls)
            }
            Self::Bytes {
                data,
                offsets,
                nulls,
            }
            | Self::JsonText {
                data,
                offsets,
                nulls,
            } => {
                for (row, (offset, length)) in offsets.iter().copied().enumerate() {
                    let start = usize::try_from(offset)
                        .map_err(|_| invalid(format!("row {row} offset does not fit usize")))?;
                    let length = usize::try_from(length)
                        .map_err(|_| invalid(format!("row {row} length does not fit usize")))?;
                    let end = start
                        .checked_add(length)
                        .ok_or_else(|| invalid(format!("row {row} byte range overflows")))?;
                    if end > data.len() {
                        return Err(invalid(format!(
                            "row {row} byte range {start}..{end} exceeds {} bytes",
                            data.len()
                        )));
                    }
                }
                (offsets.len(), nulls)
            }
            Self::External {
                type_object_id,
                codec_version,
                data,
                offsets,
                nulls,
            } => {
                ExternalTypeRef {
                    type_object_id: *type_object_id,
                    codec_version: *codec_version,
                }
                .validate()
                .map_err(invalid)?;
                if offsets.len() != row_count.saturating_add(1) {
                    return Err(invalid(format!(
                        "has {} offsets, expected {}",
                        offsets.len(),
                        row_count.saturating_add(1)
                    )));
                }
                if offsets.first().copied() != Some(0) {
                    return Err(invalid("first external offset must be zero".to_string()));
                }
                let data_len = u32::try_from(data.len())
                    .map_err(|_| invalid("external data length does not fit u32".to_string()))?;
                if offsets.last().copied() != Some(data_len) {
                    return Err(invalid(format!(
                        "last external offset must equal data length {}",
                        data.len()
                    )));
                }
                for row in 0..row_count {
                    let start = offsets[row];
                    let end = offsets[row + 1];
                    if end < start {
                        return Err(invalid(format!(
                            "row {row} external offsets are not monotonic"
                        )));
                    }
                    let length = usize::try_from(end - start)
                        .map_err(|_| invalid(format!("row {row} length does not fit usize")))?;
                    if length > MAX_EXTERNAL_VALUE_BYTES {
                        return Err(invalid(format!(
                            "row {row} external payload has {length} bytes, maximum is {MAX_EXTERNAL_VALUE_BYTES}"
                        )));
                    }
                    if nulls.get(row).copied().unwrap_or(false) && start != end {
                        return Err(invalid(format!(
                            "row {row} is NULL but its external byte slice is not empty"
                        )));
                    }
                }
                (row_count, nulls)
            }
        };
        if values_len != row_count || nulls.len() != row_count {
            return Err(invalid(format!(
                "has {values_len} values and {} null markers, expected {row_count}",
                nulls.len()
            )));
        }
        Ok(())
    }

    fn validate_contract(&self, index: usize, contract: &Column) -> Result<(), ProtocolError> {
        let invalid = |message: String| {
            ProtocolError::InvalidBatchShape(format!("column {index}: {message}"))
        };
        let nulls = match self {
            Self::Int64 { nulls, .. }
            | Self::Float64 { nulls, .. }
            | Self::Boolean { nulls, .. }
            | Self::TimestampNanos { nulls, .. }
            | Self::DictionaryText { nulls, .. }
            | Self::Bytes { nulls, .. }
            | Self::JsonText { nulls, .. }
            | Self::External { nulls, .. } => nulls,
        };
        if !contract.nullable && nulls.iter().any(|value| *value) {
            return Err(invalid(format!(
                "NULL violates non-nullable `{}`",
                contract.name
            )));
        }
        if let Self::External {
            type_object_id,
            codec_version,
            ..
        } = self
        {
            let wire_type = ExternalTypeRef {
                type_object_id: *type_object_id,
                codec_version: *codec_version,
            };
            wire_type.validate().map_err(invalid)?;
            if contract.external_type != Some(wire_type) {
                return Err(invalid(format!(
                    "external wire column identity is incompatible with published type {}",
                    contract.type_name
                )));
            }
            return Ok(());
        }
        if contract.external_type.is_some() {
            return Err(invalid(format!(
                "built-in wire column is incompatible with external type {}",
                contract.type_name
            )));
        }
        let ty = normalized_type_name(&contract.type_name);
        if ty != "UNKNOWN" {
            let compatible = match self {
                Self::Int64 { .. } => matches!(ty.as_str(), "INTEGER" | "INT" | "BIGINT"),
                Self::Float64 { .. } => matches!(ty.as_str(), "FLOAT" | "DOUBLE" | "REAL"),
                Self::Boolean { .. } => matches!(ty.as_str(), "BOOLEAN" | "BOOL"),
                Self::TimestampNanos { .. } => ty == "TIMESTAMP",
                Self::DictionaryText { .. } => matches!(ty.as_str(), "TEXT" | "STRING" | "VARCHAR"),
                Self::Bytes { .. } => matches!(ty.as_str(), "BYTES" | "BLOB" | "BINARY"),
                Self::JsonText { .. } => ty == "JSON",
                Self::External { .. } => unreachable!("external branch returned above"),
            };
            if !compatible {
                return Err(invalid(format!(
                    "wire column is incompatible with published type {}",
                    contract.type_name
                )));
            }
        }
        if let Self::JsonText {
            data,
            offsets,
            nulls,
        } = self
        {
            for (row, (offset, length)) in offsets.iter().copied().enumerate() {
                if nulls[row] {
                    continue;
                }
                let start = usize::try_from(offset)
                    .map_err(|_| invalid(format!("row {row} offset does not fit usize")))?;
                let length = usize::try_from(length)
                    .map_err(|_| invalid(format!("row {row} length does not fit usize")))?;
                let end = start
                    .checked_add(length)
                    .ok_or_else(|| invalid(format!("row {row} byte range overflows")))?;
                let text = std::str::from_utf8(&data[start..end])
                    .map_err(|_| invalid(format!("row {row} JSON is not UTF-8")))?;
                serde_json::from_str::<serde_json::Value>(text)
                    .map_err(|_| invalid(format!("row {row} JSON document is invalid")))?;
            }
        }
        Ok(())
    }
}

pub fn write_frame<T: Serialize>(
    writer: &mut impl Write,
    message: &T,
    max_frame_bytes: u32,
) -> Result<(), ProtocolError> {
    let payload = encode_payload(message)?;
    write_frame_payload(writer, &payload, max_frame_bytes)
}

/// Write an already encoded binary message as one framed payload.
///
/// Server-side column batches use this to avoid serialising a validated batch
/// once to measure it and a second time to send it. The caller owns the exact
/// bincode payload and this function performs only frame-limit validation and
/// socket writes.
pub fn write_frame_payload(
    writer: &mut impl Write,
    payload: &[u8],
    max_frame_bytes: u32,
) -> Result<(), ProtocolError> {
    let length = frame_length_prefix(payload.len(), max_frame_bytes)?;
    writer.write_all(&length)?;
    writer.write_all(payload)?;
    writer.flush()?;
    Ok(())
}

/// Exact binary payload length used by `write_frame`, excluding its four-byte
/// length prefix. Server batch limits must measure this format, not Rust heap
/// sizes, so the configured boundary matches bytes written to the socket.
pub fn encoded_payload_len<T: Serialize>(message: &T) -> Result<usize, ProtocolError> {
    struct CountingWriter(usize);
    impl Write for CountingWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.checked_add(bytes.len()).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "encoded length overflow")
            })?;
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut writer = CountingWriter(0);
    let encoded =
        bincode::serde::encode_into_std_write(message, &mut writer, bincode::config::standard())
            .map_err(ProtocolError::Encode)?;
    debug_assert_eq!(encoded, writer.0);
    Ok(writer.0)
}

/// Encode a message without its four-byte frame prefix.
///
/// Most callers should use [`write_frame`]. This public primitive exists for
/// a bounded server response that must retain the exact bytes after checking
/// its configured batch limit.
pub fn encode_payload<T: Serialize>(message: &T) -> Result<Vec<u8>, ProtocolError> {
    bincode::serde::encode_to_vec(message, bincode::config::standard())
        .map_err(ProtocolError::Encode)
}

/// Validate an encoded payload length and return the canonical big-endian
/// frame prefix shared by blocking and asynchronous transports.
#[doc(hidden)]
pub fn frame_length_prefix(
    payload_len: usize,
    max_frame_bytes: u32,
) -> Result<[u8; 4], ProtocolError> {
    let length = u32::try_from(payload_len).map_err(|_| ProtocolError::FrameTooLarge {
        actual: u32::MAX,
        limit: max_frame_bytes,
    })?;
    validate_frame_length(length, max_frame_bytes)?;
    Ok(length.to_be_bytes())
}

/// Validate a frame length received from any transport before allocation.
#[doc(hidden)]
pub fn validate_frame_length(length: u32, max_frame_bytes: u32) -> Result<usize, ProtocolError> {
    if length == 0 {
        return Err(ProtocolError::InvalidFrameLength);
    }
    if length > max_frame_bytes {
        return Err(ProtocolError::FrameTooLarge {
            actual: length,
            limit: max_frame_bytes,
        });
    }
    Ok(length as usize)
}

/// Decode one already framed payload through the canonical bincode contract.
/// Transport implementations must validate the four-byte length prefix before
/// allocating and calling this function.
#[doc(hidden)]
pub fn decode_payload<T: for<'de> Deserialize<'de>>(payload: &[u8]) -> Result<T, ProtocolError> {
    let started = Instant::now();
    let decoded = bincode::serde::decode_from_slice(
        payload,
        bincode::config::standard().with_limit::<DEFAULT_MAX_DECODED_BYTES>(),
    );
    record_client_frame_decode(payload.len() as u64, started.elapsed());
    let (message, consumed) = decoded.map_err(ProtocolError::Decode)?;
    if consumed != payload.len() {
        return Err(ProtocolError::InvalidFrameLength);
    }
    Ok(message)
}

pub fn read_frame<T: for<'de> Deserialize<'de>>(
    reader: &mut impl Read,
    max_frame_bytes: u32,
) -> Result<T, ProtocolError> {
    read_frame_with_payload_hook(reader, max_frame_bytes, |_| Ok(()))
}

/// Reads one frame and invokes `payload_hook` after the validated length
/// prefix, immediately before reading or allocating its payload. A socket
/// server uses this boundary to distinguish idle command waits from a stalled
/// frame payload without duplicating the wire decoder.
pub fn read_frame_with_payload_hook<T, R, F>(
    reader: &mut R,
    max_frame_bytes: u32,
    payload_hook: F,
) -> Result<T, ProtocolError>
where
    T: for<'de> Deserialize<'de>,
    R: Read,
    F: FnOnce(&mut R) -> std::io::Result<()>,
{
    read_frame_with_payload_len_hook(reader, max_frame_bytes, |reader, _| payload_hook(reader))
}

/// Variant of [`read_frame_with_payload_hook`] that also exposes the exact
/// validated payload length from the frame header without re-encoding the
/// message. Streaming servers use it to enforce byte budgets with no second
/// serialization pass on the hot path.
pub fn read_frame_with_payload_len_hook<T, R, F>(
    reader: &mut R,
    max_frame_bytes: u32,
    payload_hook: F,
) -> Result<T, ProtocolError>
where
    T: for<'de> Deserialize<'de>,
    R: Read,
    F: FnOnce(&mut R, u32) -> std::io::Result<()>,
{
    let mut length = [0_u8; 4];
    reader.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length);
    let payload_len = validate_frame_length(length, max_frame_bytes)?;
    payload_hook(reader, length)?;
    let mut payload = vec![0_u8; payload_len];
    reader.read_exact(&mut payload)?;
    decode_payload(&payload)
}

fn record_client_frame_decode(bytes: u64, elapsed: Duration) {
    CLIENT_PROTOCOL_COUNTERS
        .frame_decode_calls
        .fetch_add(1, Ordering::Relaxed);
    CLIENT_PROTOCOL_COUNTERS
        .frame_decode_bytes
        .fetch_add(bytes, Ordering::Relaxed);
    CLIENT_PROTOCOL_COUNTERS.frame_decode_nanos.fetch_add(
        elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
        Ordering::Relaxed,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn protocol_error_is_invalid_batch_shape(result: Result<(), ProtocolError>) {
        assert!(matches!(result, Err(ProtocolError::InvalidBatchShape(_))));
    }

    #[test]
    fn decoded_budget_rejects_compact_unbounded_container_claim() {
        let payload = bincode::serde::encode_to_vec(u64::MAX, bincode::config::standard()).unwrap();
        assert!(matches!(
            decode_payload::<Vec<u64>>(&payload),
            Err(ProtocolError::Decode(_))
        ));
    }

    #[test]
    fn decimal_admission_rejects_impossible_shapes() {
        assert!(validate_decimal_shape(123, 3, 2).is_ok());
        assert!(validate_decimal_shape(1, 2, 3).is_err());
        assert!(validate_decimal_shape(1234, 3, 1).is_err());
        assert!(validate_decimal_shape(0, 0, 0).is_err());
    }

    #[test]
    fn result_batch_shape_is_fail_closed() {
        let integer = Column {
            name: "id".into(),
            type_name: "INTEGER".into(),
            nullable: false,
            external_type: None,
        };
        let two_columns = vec![integer.clone(), integer.clone()];
        protocol_error_is_invalid_batch_shape(validate_row_batch(
            &[Row {
                values: vec![WireValue::Int(1)],
            }],
            &two_columns,
        ));
        protocol_error_is_invalid_batch_shape(validate_column_batch(
            &[WireColumn::Int64 {
                values: vec![1, 2],
                nulls: vec![false],
            }],
            2,
            std::slice::from_ref(&integer),
            false,
        ));
        protocol_error_is_invalid_batch_shape(validate_column_batch(
            &[WireColumn::DictionaryText {
                ids: vec![1],
                dictionary: vec!["only-zero".to_string()],
                nulls: vec![false],
            }],
            1,
            &[Column {
                name: "name".into(),
                type_name: "TEXT".into(),
                nullable: false,
                external_type: None,
            }],
            false,
        ));
        protocol_error_is_invalid_batch_shape(validate_column_batch(
            &[WireColumn::Bytes {
                data: vec![1],
                offsets: vec![(1, u64::MAX)],
                nulls: vec![false],
            }],
            1,
            &[Column {
                name: "data".into(),
                type_name: "BYTES".into(),
                nullable: false,
                external_type: None,
            }],
            false,
        ));
        assert!(validate_column_batch(&[], 0, &two_columns, true).is_ok());
        protocol_error_is_invalid_batch_shape(validate_column_batch(&[], 0, &two_columns, false));
    }

    #[test]
    fn published_column_semantics_are_fail_closed() {
        let integer = Column {
            name: "id".into(),
            type_name: "INTEGER".into(),
            nullable: false,
            external_type: None,
        };
        protocol_error_is_invalid_batch_shape(validate_row_batch(
            &[Row {
                values: vec![WireValue::String("wrong".into())],
            }],
            std::slice::from_ref(&integer),
        ));
        protocol_error_is_invalid_batch_shape(validate_row_batch(
            &[Row {
                values: vec![WireValue::Null],
            }],
            std::slice::from_ref(&integer),
        ));
        let json = Column {
            name: "doc".into(),
            type_name: "JSON".into(),
            nullable: false,
            external_type: None,
        };
        protocol_error_is_invalid_batch_shape(validate_row_batch(
            &[Row {
                values: vec![WireValue::Json("not-json".into())],
            }],
            std::slice::from_ref(&json),
        ));
        protocol_error_is_invalid_batch_shape(validate_column_batch(
            &[WireColumn::JsonText {
                data: b"not-json".to_vec(),
                offsets: vec![(0, 8)],
                nulls: vec![false],
            }],
            1,
            std::slice::from_ref(&json),
            false,
        ));
        protocol_error_is_invalid_batch_shape(validate_column_batch(
            &[WireColumn::Int64 {
                values: vec![1],
                nulls: vec![true],
            }],
            1,
            std::slice::from_ref(&integer),
            false,
        ));
        let unknown = Column {
            name: "future".into(),
            type_name: "UNKNOWN".into(),
            nullable: true,
            external_type: None,
        };
        assert!(validate_row_batch(
            &[Row {
                values: vec![WireValue::String("ok".into())]
            }],
            &[unknown]
        )
        .is_ok());
    }

    #[test]
    fn mandatory_handshake_fits_versioned_control_budget() {
        let response = ServerMessage::HandshakeAccepted {
            protocol_version: PROTOCOL_VERSION,
            max_frame_bytes: MIN_CONTROL_FRAME_BYTES,
            capabilities: vec![
                ProtocolCapability::ColumnBatchV1,
                ProtocolCapability::BuildIdentityV1,
                ProtocolCapability::ExternalValueV1,
            ],
        };
        assert!(encoded_payload_len(&response).unwrap() <= MIN_CONTROL_FRAME_BYTES as usize);
    }

    #[test]
    fn protocol_v17_scalar_fixture_preserves_v16_prefix_layout() {
        let values = [
            ("null", WireValue::Null),
            ("bool", WireValue::Bool(true)),
            ("int", WireValue::Int(-7)),
            ("float", WireValue::Float64(1.5)),
            (
                "decimal",
                WireValue::Decimal {
                    unscaled: 123,
                    precision: 3,
                    scale: 2,
                },
            ),
            ("string", WireValue::String("v12".to_string())),
            ("bytes", WireValue::Bytes(vec![0, 255])),
            (
                "timestamp",
                WireValue::TimestampNanos {
                    nanos_since_unix_epoch_utc: 1_234_567_890,
                },
            ),
            ("json", WireValue::Json("{\"v\":12}".to_string())),
            ("vector", WireValue::Vector(1.25_f32.to_le_bytes().to_vec())),
            ("uuid", WireValue::Uuid([0x5a; 16])),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect();
        let message = ClientMessage::Execute {
            request_id: 7,
            sql: "SELECT :v12".to_string(),
            positional: Vec::new(),
            named: values,
        };
        let payload = encode_payload(&message).unwrap();
        let encoded = payload
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            encoded,
            "04070b53454c454354203a763132000b04626f6f6c01010562797465730d0200ff07646563696d616c0bf6030205666c6f61740a000000000000f83f03696e74020d046a736f6e11087b2276223a31327d046e756c6c0006737472696e670c037631320974696d657374616d7010fca4052c930475756964135a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a06766563746f7212040000a03f",
            "existing scalar layout changed; bump PROTOCOL_VERSION and product version together"
        );
        assert_eq!(decode_payload::<ClientMessage>(&payload).unwrap(), message);
    }

    #[test]
    fn protocol_v17_external_tags_and_identity_are_frozen() {
        let external_type = ExternalTypeRef {
            type_object_id: [0x2a; 16],
            codec_version: 7,
        };
        let column = Column {
            name: "location".into(),
            type_name: "geo.point".into(),
            nullable: true,
            external_type: Some(external_type),
        };
        let value = WireValue::External {
            type_object_id: external_type.type_object_id,
            codec_version: external_type.codec_version,
            payload: vec![1, 2, 3],
        };
        assert!(validate_row_batch(
            &[Row {
                values: vec![value.clone()],
            }],
            std::slice::from_ref(&column),
        )
        .is_ok());
        assert_eq!(encode_payload(&value).unwrap()[0], 20);
        assert_eq!(
            encode_payload(&ProtocolCapability::ExternalValueV1).unwrap(),
            vec![2]
        );
        assert_eq!(
            encode_payload(&ProtocolErrorCode::UnsupportedType).unwrap(),
            vec![10]
        );

        let batch = WireColumn::External {
            type_object_id: external_type.type_object_id,
            codec_version: external_type.codec_version,
            data: vec![1, 2, 3],
            offsets: vec![0, 3, 3],
            nulls: vec![false, true],
        };
        assert!(validate_column_batch(std::slice::from_ref(&batch), 2, &[column], false).is_ok());
        assert_eq!(encode_payload(&batch).unwrap()[0], 7);
    }

    #[test]
    fn malformed_external_values_and_offsets_fail_closed() {
        let contract = Column {
            name: "location".into(),
            type_name: "geo.point".into(),
            nullable: true,
            external_type: Some(ExternalTypeRef {
                type_object_id: [1; 16],
                codec_version: 1,
            }),
        };
        protocol_error_is_invalid_batch_shape(validate_row_batch(
            &[Row {
                values: vec![WireValue::External {
                    type_object_id: [2; 16],
                    codec_version: 1,
                    payload: vec![],
                }],
            }],
            std::slice::from_ref(&contract),
        ));
        protocol_error_is_invalid_batch_shape(validate_column_batch(
            &[WireColumn::External {
                type_object_id: [1; 16],
                codec_version: 1,
                data: vec![1],
                offsets: vec![0, 1],
                nulls: vec![true],
            }],
            1,
            &[contract],
            false,
        ));
    }

    #[test]
    fn client_protocol_counters_record_frame_decode_work() {
        // The counters are process-global diagnostics. Other protocol tests
        // may decode frames concurrently, so this assertion checks the
        // monotonic contribution of this operation instead of resetting
        // shared state and requiring an invalid test-only exclusivity.
        let before = client_protocol_counters_snapshot();
        let mut frame = Vec::new();
        write_frame(
            &mut frame,
            &ServerMessage::CommandComplete {
                affected_rows: 7,
                last_insert_id: 3,
            },
            DEFAULT_MAX_FRAME_BYTES,
        )
        .unwrap();

        let decoded: ServerMessage = read_frame(&mut frame.as_slice(), DEFAULT_MAX_FRAME_BYTES)
            .expect("decode command frame");
        assert!(matches!(
            decoded,
            ServerMessage::CommandComplete {
                affected_rows: 7,
                last_insert_id: 3,
            }
        ));

        let counters = client_protocol_counters_snapshot();
        assert!(counters.frame_decode_calls > before.frame_decode_calls);
        assert!(
            counters.frame_decode_bytes
                >= before.frame_decode_bytes + u64::try_from(frame.len() - 4).unwrap()
        );
    }

    #[test]
    fn transaction_failure_roundtrip_preserves_server_activity_state() {
        for active in [false, true] {
            let expected = ServerMessage::TransactionFailed {
                failure: ProtocolFailure {
                    code: ProtocolErrorCode::SqlError,
                    message: "unique constraint failed".to_string(),
                },
                active,
            };
            let mut frame = Vec::new();
            write_frame(&mut frame, &expected, DEFAULT_MAX_FRAME_BYTES).unwrap();
            let decoded: ServerMessage =
                read_frame(&mut frame.as_slice(), DEFAULT_MAX_FRAME_BYTES).unwrap();
            assert_eq!(decoded, expected);
        }
    }

    #[test]
    fn compaction_backpressure_roundtrip_preserves_retryable_identity() {
        let expected = ServerMessage::Error(ProtocolFailure {
            code: ProtocolErrorCode::CompactionBackpressure,
            message: "compaction debt is at the hard boundary".to_string(),
        });
        let mut frame = Vec::new();
        write_frame(&mut frame, &expected, DEFAULT_MAX_FRAME_BYTES).unwrap();
        let decoded: ServerMessage =
            read_frame(&mut frame.as_slice(), DEFAULT_MAX_FRAME_BYTES).unwrap();
        assert_eq!(decoded, expected);
        let ServerMessage::Error(failure) = decoded else {
            panic!("error response expected");
        };
        assert!(failure.code.is_retryable());
    }

    #[test]
    fn shared_frame_helpers_preserve_limits_and_payload_contract() {
        let message = ClientMessage::BeginTransaction {
            isolation: TransactionIsolation::Snapshot,
        };
        let payload = encode_payload(&message).unwrap();
        let prefix = frame_length_prefix(payload.len(), DEFAULT_MAX_FRAME_BYTES).unwrap();
        assert_eq!(u32::from_be_bytes(prefix) as usize, payload.len());
        let decoded: ClientMessage = decode_payload(&payload).unwrap();
        assert_eq!(decoded, message);

        assert!(matches!(
            validate_frame_length(0, DEFAULT_MAX_FRAME_BYTES),
            Err(ProtocolError::InvalidFrameLength)
        ));
        assert!(matches!(
            validate_frame_length(9, 8),
            Err(ProtocolError::FrameTooLarge {
                actual: 9,
                limit: 8
            })
        ));
    }

    #[test]
    fn r8_l01_batch_j_frame_limit_is_checked_before_output_allocation() {
        let message = ClientMessage::Execute {
            request_id: 1,
            sql: "x".repeat(1_024),
            positional: Vec::new(),
            named: Default::default(),
        };
        let payload = encode_payload(&message).unwrap();
        assert_eq!(encoded_payload_len(&message).unwrap(), payload.len());

        let mut output = Vec::new();
        assert!(matches!(
            write_frame(&mut output, &message, 16),
            Err(ProtocolError::FrameTooLarge { .. })
        ));
        assert!(output.is_empty(), "rejected frame must not write a prefix");
    }
}
