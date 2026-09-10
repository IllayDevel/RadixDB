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

//! Canonical error types for RadixDB
//!
//! This module defines all error types used throughout the storage engine.

use std::fmt;

use thiserror::Error;

mod code;
mod context;

pub use code::{ErrorCategory, ErrorCode};
pub use context::ErrorContext;

/// Stable category for navigable-reference binding and execution failures.
///
/// The textual code is part of the SQL error contract. Details may gain
/// context, but callers can classify the error without parsing prose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NavigationErrorCode {
    UnknownRoot,
    AmbiguousRoot,
    NotAReference,
    UnsupportedReferenceShape,
    TargetColumnNotFound,
    ReadOnly,
    SchemaChanged,
    TargetMissing,
    TargetNotUnique,
}

impl NavigationErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownRoot => "NAVIGATION_UNKNOWN_ROOT",
            Self::AmbiguousRoot => "NAVIGATION_AMBIGUOUS_ROOT",
            Self::NotAReference => "NAVIGATION_NOT_A_REFERENCE",
            Self::UnsupportedReferenceShape => "NAVIGATION_UNSUPPORTED_REFERENCE_SHAPE",
            Self::TargetColumnNotFound => "NAVIGATION_TARGET_COLUMN_NOT_FOUND",
            Self::ReadOnly => "NAVIGATION_READ_ONLY",
            Self::SchemaChanged => "NAVIGATION_SCHEMA_CHANGED",
            Self::TargetMissing => "REFERENCE_TARGET_MISSING",
            Self::TargetNotUnique => "REFERENCE_TARGET_NOT_UNIQUE",
        }
    }
}

impl fmt::Display for NavigationErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Result type alias for RadixDB operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for RadixDB storage operations
///
/// This enum covers all error cases including both sentinel errors
/// and structured errors with context.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum Error {
    // =========================================================================
    // Table errors
    // =========================================================================
    /// Table not found in the database
    #[error("table '{0}' not found")]
    TableNotFound(String),

    /// Table already exists when trying to create
    #[error("table '{0}' already exists")]
    TableAlreadyExists(String),

    /// Table has been closed and cannot be used
    #[error("table closed")]
    TableClosed,

    /// Table column count mismatch
    #[error("table columns don't match, expected {expected}, got {got}")]
    TableColumnsNotMatch { expected: usize, got: usize },

    /// Cannot truncate table because other transactions hold uncommitted writes
    #[error("cannot truncate table: active transactions have uncommitted changes")]
    TableHasActiveTransactions,

    // =========================================================================
    // Column errors
    // =========================================================================
    /// Column not found in table schema
    #[error("column '{0}' not found")]
    ColumnNotFound(String),

    /// A result label resolves to more than one projected column.
    #[error("column '{0}' is ambiguous")]
    AmbiguousColumn(String),

    /// Invalid column type for operation
    #[error("invalid column type")]
    InvalidColumnType,

    /// Vector dimension mismatch
    #[error("Vector dimension mismatch: expected {expected}, got {got}")]
    VectorDimensionMismatch { expected: u16, got: u16 },

    /// Duplicate column name in schema
    #[error("duplicate column")]
    DuplicateColumn,

    // =========================================================================
    // Value errors
    // =========================================================================
    /// Invalid value for operation
    #[error("invalid value")]
    InvalidValue,

    /// Invalid argument for function
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Authenticated Principal lacks authority for the requested operation.
    #[error("authorization denied: {0}")]
    AuthorizationDenied(String),

    /// Value exceeds maximum length
    #[error("value for column {column} is too long, max {max}, got {got}")]
    ValueTooLong {
        column: String,
        max: usize,
        got: usize,
    },

    // =========================================================================
    // Constraint errors
    // =========================================================================
    /// NOT NULL constraint violation
    #[error("not null constraint failed for column {column}")]
    NotNullConstraint { column: String },

    /// Primary key constraint violation
    #[error("primary key constraint failed with {row_id} already exists in this table")]
    PrimaryKeyConstraint { row_id: i64 },

    /// Unique constraint violation
    #[error("unique constraint failed for index {index} on column {column} with value {value}")]
    UniqueConstraint {
        index: String,
        column: String,
        value: String,
        /// Row ID of the conflicting row (-1 if unknown)
        row_id: i64,
    },

    /// CHECK constraint violation
    #[error("CHECK constraint failed for column {column}: {expression}")]
    CheckConstraintViolation { column: String, expression: String },

    /// Foreign key constraint violation
    #[error("foreign key constraint violation: column '{column}' in table '{table}' references '{ref_table}({ref_column})' — {detail}")]
    ForeignKeyViolation {
        table: String,
        column: String,
        ref_table: String,
        ref_column: String,
        detail: String,
    },

    // =========================================================================
    // Transaction errors
    // =========================================================================
    /// Transaction has not been started
    #[error("transaction not started")]
    TransactionNotStarted,

    /// Transaction has already been started
    #[error("transaction already started")]
    TransactionAlreadyStarted,

    /// Transaction has already ended (committed or rolled back)
    #[error("transaction already ended")]
    TransactionEnded,

    /// Transaction was aborted
    #[error("transaction aborted")]
    TransactionAborted,

    /// Transaction has already been committed
    #[error("transaction already committed")]
    TransactionCommitted,

    /// Transaction has been closed
    #[error("transaction already closed")]
    TransactionClosed,

    /// A registry transition was requested for a missing transaction or from
    /// a state that cannot legally perform that transition.
    #[error("invalid transaction {txn_id} transition: expected {expected}, found {actual}")]
    InvalidTransactionTransition {
        txn_id: i64,
        expected: String,
        actual: String,
    },

    /// Adding a row wait would close a cycle in the transaction wait graph.
    #[error(
        "transaction serialization conflict while acquiring row {row_id}; retry the transaction"
    )]
    TransactionSerializationConflict { row_id: i64 },

    /// A row writer exceeded the bounded claim wait.
    #[error("transaction timed out while waiting to update row {row_id} after {timeout_ms} ms")]
    RowLockTimeout { row_id: i64, timeout_ms: u64 },

    /// A table accumulated more uncompacted L0 work than the configured hard
    /// admission limit. The transaction has not published and may be retried.
    #[error(
        "COMPACTION_BACKPRESSURE: table '{table}' has L0 debt {segments} segments/{physical_bytes} bytes; hard limit {hard_segments} segments/{hard_bytes} bytes; retry after compaction"
    )]
    CompactionBackpressure {
        table: String,
        segments: u64,
        physical_bytes: u64,
        hard_segments: u64,
        hard_bytes: u64,
    },

    // =========================================================================
    // Index errors
    // =========================================================================
    /// Index not found
    #[error("index '{0}' not found")]
    IndexNotFound(String),

    /// Index already exists
    #[error("index '{0}' already exists")]
    IndexAlreadyExists(String),

    /// Column for index not found
    #[error("index column not found")]
    IndexColumnNotFound,

    /// Index is closed
    #[error("index is closed")]
    IndexClosed,

    // =========================================================================
    // Engine errors
    // =========================================================================
    /// Engine is not open
    #[error("engine is not open")]
    EngineNotOpen,

    /// Engine is already open
    #[error("engine is already open")]
    EngineAlreadyOpen,

    // =========================================================================
    // View errors
    // =========================================================================
    /// View already exists
    #[error("view '{0}' already exists")]
    ViewAlreadyExists(String),

    /// View not found
    #[error("view '{0}' not found")]
    ViewNotFound(String),

    // =========================================================================
    // Lock errors
    // =========================================================================
    /// Failed to acquire lock
    #[error("failed to acquire lock: {0}")]
    LockAcquisitionFailed(String),

    // =========================================================================
    // Query result errors
    // =========================================================================
    /// Query returned no rows
    #[error("query returned no rows")]
    NoRowsReturned,

    /// No statements to execute
    #[error("no statements to execute")]
    NoStatementsToExecute,

    /// Column index out of bounds
    #[error("column index {index} out of bounds")]
    ColumnIndexOutOfBounds { index: usize },

    /// A streaming cursor has no current row before its first successful
    /// advance or after end-of-stream.
    #[error("cursor is not positioned on a row")]
    CursorNotPositioned,

    // =========================================================================
    // WAL errors
    // =========================================================================
    /// WAL manager is not running
    #[error("WAL manager is not running")]
    WalNotRunning,

    /// WAL file is closed
    #[error("WAL file is closed")]
    WalFileClosed,

    /// WAL bytes containing an outcome were written, but their durability
    /// could not be established. The transaction must be treated as terminal
    /// and callers must not assume rollback or safely retry the logical action.
    #[error("WAL durability outcome is uncertain: {detail}")]
    WalDurabilityUncertain { detail: String },

    /// A bounded multi-transaction operation failed after publishing a
    /// durable prefix. Callers must not blindly retry the whole operation.
    #[error("{operation} failed after committing {committed_rows} rows: {cause}")]
    PartialCommit {
        operation: String,
        committed_rows: i64,
        cause: String,
    },

    /// One atomic COPY would exceed its configured resident-memory envelope.
    #[error(
        "COPY transaction memory budget exceeded at row {row}: limit {limit_bytes} bytes, attempted {attempted_bytes} bytes"
    )]
    CopyTransactionMemoryLimit {
        row: i64,
        limit_bytes: usize,
        attempted_bytes: usize,
    },

    /// WAL not initialized
    #[error("WAL not initialized")]
    WalNotInitialized,

    // =========================================================================
    // Database errors
    // =========================================================================
    /// Database is locked by another process
    #[error("database is locked by another process")]
    DatabaseLocked,

    /// Cannot drop primary key column
    #[error("cannot drop primary key column")]
    CannotDropPrimaryKey,

    // =========================================================================
    // Comparison errors
    // =========================================================================
    /// Cannot compare NULL with non-NULL value
    #[error("cannot compare NULL with non-NULL value")]
    NullComparison,

    /// Cannot compare incompatible types
    #[error("cannot compare incompatible types")]
    IncomparableTypes,

    // =========================================================================
    // Other errors
    // =========================================================================
    /// Operation not supported
    #[error("not supported: {0}")]
    NotSupported(String),

    /// A native extension function failed. Arguments and plugin-provided
    /// detail are deliberately excluded from this public diagnostic.
    #[error("native function '{function}' failed with plugin status {status}")]
    NativeFunction { function: String, status: u32 },

    /// Navigable-reference contract failure with a stable machine category.
    #[error("{code}: {detail}")]
    Navigation {
        code: NavigationErrorCode,
        detail: String,
    },

    /// Segment not found (internal storage error)
    #[error("segment not found")]
    SegmentNotFound,

    /// Expression evaluation failed
    #[error("expression evaluation failed")]
    ExpressionEvaluation,

    /// Expression evaluation failed with message
    #[error("expression evaluation failed: {message}")]
    ExpressionEvaluationWithMessage { message: String },

    /// Type conversion error
    #[error("type conversion error: cannot convert {from} to {to}")]
    TypeConversion { from: String, to: String },

    /// Parse error
    #[error("parse error: {0}")]
    Parse(String),

    /// IO error (wrapped)
    #[error("IO error: {message}")]
    Io { message: String },

    /// Internal error for unexpected conditions
    #[error("{message}")]
    Internal { message: String },

    // =========================================================================
    // Executor errors
    // =========================================================================
    /// Table or view not found (with name)
    #[error("table or view '{0}' not found")]
    TableOrViewNotFound(String),

    /// Type error
    #[error("type error: {0}")]
    Type(String),

    /// Division by zero
    #[error("division by zero")]
    DivisionByZero,

    /// Query cancelled
    #[error("query cancelled")]
    QueryCancelled,
}

impl Error {
    /// Return a stable machine-readable code without parsing presentation text.
    pub fn code(&self) -> ErrorCode {
        let code = match self {
            Self::TableNotFound(_) => "TABLE_NOT_FOUND",
            Self::TableAlreadyExists(_) => "TABLE_ALREADY_EXISTS",
            Self::TableClosed => "TABLE_CLOSED",
            Self::TableColumnsNotMatch { .. } => "TABLE_COLUMNS_NOT_MATCH",
            Self::TableHasActiveTransactions => "TABLE_HAS_ACTIVE_TRANSACTIONS",
            Self::ColumnNotFound(_) => "COLUMN_NOT_FOUND",
            Self::AmbiguousColumn(_) => "AMBIGUOUS_COLUMN",
            Self::InvalidColumnType => "INVALID_COLUMN_TYPE",
            Self::VectorDimensionMismatch { .. } => "VECTOR_DIMENSION_MISMATCH",
            Self::DuplicateColumn => "DUPLICATE_COLUMN",
            Self::InvalidValue => "INVALID_VALUE",
            Self::InvalidArgument(_) => "INVALID_ARGUMENT",
            Self::AuthorizationDenied(_) => "AUTHORIZATION_DENIED",
            Self::ValueTooLong { .. } => "VALUE_TOO_LONG",
            Self::NotNullConstraint { .. } => "NOT_NULL_CONSTRAINT",
            Self::PrimaryKeyConstraint { .. } => "PRIMARY_KEY_CONSTRAINT",
            Self::UniqueConstraint { .. } => "UNIQUE_CONSTRAINT",
            Self::CheckConstraintViolation { .. } => "CHECK_CONSTRAINT",
            Self::ForeignKeyViolation { .. } => "FOREIGN_KEY_CONSTRAINT",
            Self::TransactionNotStarted => "TRANSACTION_NOT_STARTED",
            Self::TransactionAlreadyStarted => "TRANSACTION_ALREADY_STARTED",
            Self::TransactionEnded => "TRANSACTION_ENDED",
            Self::TransactionAborted => "TRANSACTION_ABORTED",
            Self::TransactionCommitted => "TRANSACTION_COMMITTED",
            Self::TransactionClosed => "TRANSACTION_CLOSED",
            Self::InvalidTransactionTransition { .. } => "INVALID_TRANSACTION_TRANSITION",
            Self::TransactionSerializationConflict { .. } => "TRANSACTION_SERIALIZATION_CONFLICT",
            Self::RowLockTimeout { .. } => "ROW_LOCK_TIMEOUT",
            Self::CompactionBackpressure { .. } => "COMPACTION_BACKPRESSURE",
            Self::IndexNotFound(_) => "INDEX_NOT_FOUND",
            Self::IndexAlreadyExists(_) => "INDEX_ALREADY_EXISTS",
            Self::IndexColumnNotFound => "INDEX_COLUMN_NOT_FOUND",
            Self::IndexClosed => "INDEX_CLOSED",
            Self::EngineNotOpen => "ENGINE_NOT_OPEN",
            Self::EngineAlreadyOpen => "ENGINE_ALREADY_OPEN",
            Self::ViewAlreadyExists(_) => "VIEW_ALREADY_EXISTS",
            Self::ViewNotFound(_) => "VIEW_NOT_FOUND",
            Self::LockAcquisitionFailed(_) => "LOCK_ACQUISITION_FAILED",
            Self::NoRowsReturned => "NO_ROWS_RETURNED",
            Self::NoStatementsToExecute => "NO_STATEMENTS_TO_EXECUTE",
            Self::ColumnIndexOutOfBounds { .. } => "COLUMN_INDEX_OUT_OF_BOUNDS",
            Self::CursorNotPositioned => "CURSOR_NOT_POSITIONED",
            Self::WalNotRunning => "WAL_NOT_RUNNING",
            Self::WalFileClosed => "WAL_FILE_CLOSED",
            Self::WalDurabilityUncertain { .. } => "WAL_DURABILITY_UNCERTAIN",
            Self::PartialCommit { .. } => "PARTIAL_COMMIT",
            Self::CopyTransactionMemoryLimit { .. } => "COPY_TRANSACTION_MEMORY_LIMIT",
            Self::WalNotInitialized => "WAL_NOT_INITIALIZED",
            Self::DatabaseLocked => "DATABASE_LOCKED",
            Self::CannotDropPrimaryKey => "CANNOT_DROP_PRIMARY_KEY",
            Self::NullComparison => "NULL_COMPARISON",
            Self::IncomparableTypes => "INCOMPARABLE_TYPES",
            Self::NotSupported(_) => "NOT_SUPPORTED",
            Self::NativeFunction { .. } => "NATIVE_FUNCTION_ERROR",
            Self::Navigation { code, .. } => code.as_str(),
            Self::SegmentNotFound => "SEGMENT_NOT_FOUND",
            Self::ExpressionEvaluation => "EXPRESSION_EVALUATION",
            Self::ExpressionEvaluationWithMessage { .. } => "EXPRESSION_EVALUATION",
            Self::TypeConversion { .. } => "TYPE_CONVERSION",
            Self::Parse(_) => "PARSE_ERROR",
            Self::Io { .. } => "IO_ERROR",
            Self::Internal { .. } => "INTERNAL_ERROR",
            Self::TableOrViewNotFound(_) => "TABLE_OR_VIEW_NOT_FOUND",
            Self::Type(_) => "TYPE_ERROR",
            Self::DivisionByZero => "DIVISION_BY_ZERO",
            Self::QueryCancelled => "QUERY_CANCELLED",
        };
        ErrorCode::new(code)
    }

    /// Return neutral classification for protocol, API, and diagnostic mapping.
    pub fn context(&self) -> ErrorContext {
        use ErrorCategory as Category;

        let category = match self {
            Self::TableNotFound(_)
            | Self::TableAlreadyExists(_)
            | Self::TableClosed
            | Self::TableColumnsNotMatch { .. }
            | Self::TableHasActiveTransactions
            | Self::ColumnNotFound(_)
            | Self::AmbiguousColumn(_)
            | Self::DuplicateColumn
            | Self::ViewAlreadyExists(_)
            | Self::ViewNotFound(_)
            | Self::CannotDropPrimaryKey
            | Self::Navigation { .. }
            | Self::TableOrViewNotFound(_) => Category::Catalog,
            Self::InvalidColumnType
            | Self::VectorDimensionMismatch { .. }
            | Self::InvalidValue
            | Self::InvalidArgument(_)
            | Self::ValueTooLong { .. }
            | Self::NullComparison
            | Self::IncomparableTypes
            | Self::TypeConversion { .. }
            | Self::Type(_) => Category::Value,
            Self::AuthorizationDenied(_) => Category::Security,
            Self::NotNullConstraint { .. }
            | Self::PrimaryKeyConstraint { .. }
            | Self::UniqueConstraint { .. }
            | Self::CheckConstraintViolation { .. }
            | Self::ForeignKeyViolation { .. } => Category::Constraint,
            Self::TransactionNotStarted
            | Self::TransactionAlreadyStarted
            | Self::TransactionEnded
            | Self::TransactionAborted
            | Self::TransactionCommitted
            | Self::TransactionClosed
            | Self::InvalidTransactionTransition { .. }
            | Self::TransactionSerializationConflict { .. }
            | Self::RowLockTimeout { .. }
            | Self::CompactionBackpressure { .. }
            | Self::PartialCommit { .. }
            | Self::CopyTransactionMemoryLimit { .. } => Category::Transaction,
            Self::IndexNotFound(_)
            | Self::IndexAlreadyExists(_)
            | Self::IndexColumnNotFound
            | Self::IndexClosed => Category::Index,
            Self::EngineNotOpen | Self::EngineAlreadyOpen | Self::LockAcquisitionFailed(_) => {
                Category::Engine
            }
            Self::NoRowsReturned
            | Self::NoStatementsToExecute
            | Self::ColumnIndexOutOfBounds { .. }
            | Self::CursorNotPositioned
            | Self::NotSupported(_)
            | Self::NativeFunction { .. }
            | Self::QueryCancelled => Category::Query,
            Self::WalNotRunning
            | Self::WalFileClosed
            | Self::WalDurabilityUncertain { .. }
            | Self::WalNotInitialized
            | Self::SegmentNotFound => Category::Durability,
            Self::DatabaseLocked => Category::Database,
            Self::ExpressionEvaluation
            | Self::ExpressionEvaluationWithMessage { .. }
            | Self::DivisionByZero => Category::Evaluation,
            Self::Parse(_) => Category::Syntax,
            Self::Io { .. } => Category::Io,
            Self::Internal { .. } => Category::Internal,
        };
        ErrorContext::new(
            self.code(),
            category,
            self.is_retryable(),
            self.is_not_found(),
            self.is_constraint_violation(),
        )
    }

    /// Create a new TableColumnsNotMatch error
    pub fn table_columns_not_match(expected: usize, got: usize) -> Self {
        Error::TableColumnsNotMatch { expected, got }
    }

    /// Create a new ValueTooLong error
    pub fn value_too_long(column: impl Into<String>, max: usize, got: usize) -> Self {
        Error::ValueTooLong {
            column: column.into(),
            max,
            got,
        }
    }

    /// Create a new NotNullConstraint error
    pub fn not_null_constraint(column: impl Into<String>) -> Self {
        Error::NotNullConstraint {
            column: column.into(),
        }
    }

    /// Create a new PrimaryKeyConstraint error
    pub fn primary_key_constraint(row_id: i64) -> Self {
        Error::PrimaryKeyConstraint { row_id }
    }

    /// Create a new UniqueConstraint error
    pub fn unique_constraint(
        index: impl Into<String>,
        column: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Error::UniqueConstraint {
            index: index.into(),
            column: column.into(),
            value: value.into(),
            row_id: -1,
        }
    }

    /// Create a new ForeignKeyViolation error
    pub fn foreign_key_violation(
        table: impl Into<String>,
        column: impl Into<String>,
        ref_table: impl Into<String>,
        ref_column: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Error::ForeignKeyViolation {
            table: table.into(),
            column: column.into(),
            ref_table: ref_table.into(),
            ref_column: ref_column.into(),
            detail: detail.into(),
        }
    }

    /// Create a new TypeConversion error
    pub fn type_conversion(from: impl Into<String>, to: impl Into<String>) -> Self {
        Error::TypeConversion {
            from: from.into(),
            to: to.into(),
        }
    }

    /// Create a new Parse error
    pub fn parse(message: impl Into<String>) -> Self {
        Error::Parse(message.into())
    }

    /// Create a new IO error
    pub fn io(message: impl Into<String>) -> Self {
        Error::Io {
            message: message.into(),
        }
    }

    /// Create a new Internal error
    pub fn internal(message: impl Into<String>) -> Self {
        Error::Internal {
            message: message.into(),
        }
    }

    /// Create a new ExpressionEvaluationWithMessage error
    pub fn expression_evaluation(message: impl Into<String>) -> Self {
        Error::ExpressionEvaluationWithMessage {
            message: message.into(),
        }
    }

    /// Create a new InvalidArgument error
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Error::InvalidArgument(message.into())
    }

    /// Create a stable authorization failure without exposing credentials or
    /// parameter values.
    pub fn authorization_denied(message: impl Into<String>) -> Self {
        Error::AuthorizationDenied(message.into())
    }

    /// Create a stable navigable-reference error.
    pub fn navigation(code: NavigationErrorCode, detail: impl Into<String>) -> Self {
        Error::Navigation {
            code,
            detail: detail.into(),
        }
    }

    /// Return the stable navigable-reference category, if this is one.
    pub fn navigation_code(&self) -> Option<NavigationErrorCode> {
        match self {
            Error::Navigation { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// Check if this is a "not found" type error
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            Error::TableNotFound(_)
                | Error::ColumnNotFound(_)
                | Error::IndexNotFound(_)
                | Error::IndexColumnNotFound
                | Error::SegmentNotFound
                | Error::ViewNotFound(_)
                | Error::TableOrViewNotFound(_)
        )
    }

    /// Check if this is a constraint violation error
    pub fn is_constraint_violation(&self) -> bool {
        matches!(
            self,
            Error::NotNullConstraint { .. }
                | Error::PrimaryKeyConstraint { .. }
                | Error::UniqueConstraint { .. }
                | Error::CheckConstraintViolation { .. }
                | Error::ForeignKeyViolation { .. }
        )
    }

    /// PK or UNIQUE violation only (excludes NOT NULL / FK).
    pub fn is_pk_or_unique_violation(&self) -> bool {
        matches!(
            self,
            Error::PrimaryKeyConstraint { .. } | Error::UniqueConstraint { .. }
        )
    }

    /// Check if this is a transaction-related error
    pub fn is_transaction_error(&self) -> bool {
        matches!(
            self,
            Error::TransactionNotStarted
                | Error::TransactionAlreadyStarted
                | Error::TransactionEnded
                | Error::TransactionAborted
                | Error::TransactionCommitted
                | Error::TransactionClosed
                | Error::TransactionSerializationConflict { .. }
                | Error::RowLockTimeout { .. }
                | Error::CompactionBackpressure { .. }
        )
    }

    /// The failed logical action did not publish and may be retried after the
    /// reported transient condition changes.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::TransactionSerializationConflict { .. }
                | Error::RowLockTimeout { .. }
                | Error::CompactionBackpressure { .. }
        )
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io {
            message: err.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        assert_eq!(
            Error::TableNotFound("users".to_string()).to_string(),
            "table 'users' not found"
        );
        assert_eq!(
            Error::TableAlreadyExists("users".to_string()).to_string(),
            "table 'users' already exists"
        );
        assert_eq!(
            Error::ColumnNotFound("email".to_string()).to_string(),
            "column 'email' not found"
        );
        assert_eq!(Error::InvalidValue.to_string(), "invalid value");
        assert_eq!(
            Error::TransactionNotStarted.to_string(),
            "transaction not started"
        );
        assert_eq!(
            Error::IndexNotFound("idx_email".to_string()).to_string(),
            "index 'idx_email' not found"
        );
        assert_eq!(
            Error::NullComparison.to_string(),
            "cannot compare NULL with non-NULL value"
        );
    }

    #[test]
    fn test_structured_error_display() {
        let err = Error::table_columns_not_match(5, 3);
        assert_eq!(
            err.to_string(),
            "table columns don't match, expected 5, got 3"
        );

        let err = Error::value_too_long("name", 100, 150);
        assert_eq!(
            err.to_string(),
            "value for column name is too long, max 100, got 150"
        );

        let err = Error::not_null_constraint("email");
        assert_eq!(
            err.to_string(),
            "not null constraint failed for column email"
        );

        let err = Error::primary_key_constraint(42);
        assert_eq!(
            err.to_string(),
            "primary key constraint failed with 42 already exists in this table"
        );

        let err = Error::unique_constraint("idx_email", "email", "test@example.com");
        assert_eq!(
            err.to_string(),
            "unique constraint failed for index idx_email on column email with value test@example.com"
        );
    }

    #[test]
    fn test_error_classification() {
        assert!(Error::TableNotFound("t".to_string()).is_not_found());
        assert!(Error::ColumnNotFound("c".to_string()).is_not_found());
        assert!(Error::IndexNotFound("i".to_string()).is_not_found());
        assert!(!Error::InvalidValue.is_not_found());

        assert!(Error::not_null_constraint("col").is_constraint_violation());
        assert!(Error::primary_key_constraint(1).is_constraint_violation());
        assert!(Error::unique_constraint("idx", "col", "val").is_constraint_violation());
        assert!(Error::CheckConstraintViolation {
            column: "<table:t>".to_string(),
            expression: "a > b".to_string(),
        }
        .is_constraint_violation());
        assert!(!Error::TableNotFound("t".to_string()).is_constraint_violation());

        assert!(Error::TransactionNotStarted.is_transaction_error());
        assert!(Error::TransactionCommitted.is_transaction_error());
        assert!(!Error::TableNotFound("t".to_string()).is_transaction_error());
    }

    #[test]
    fn test_transaction_classifier_covers_retryable_transaction_variants() {
        let transaction_errors = [
            Error::TransactionNotStarted,
            Error::TransactionAlreadyStarted,
            Error::TransactionEnded,
            Error::TransactionAborted,
            Error::TransactionCommitted,
            Error::TransactionClosed,
            Error::TransactionSerializationConflict { row_id: 7 },
            Error::RowLockTimeout {
                row_id: 7,
                timeout_ms: 250,
            },
            Error::CompactionBackpressure {
                table: "items".to_string(),
                segments: 32,
                physical_bytes: 1024,
                hard_segments: 32,
                hard_bytes: 2048,
            },
        ];

        for error in transaction_errors {
            assert!(
                error.is_transaction_error(),
                "transaction classifier rejected {error:?}"
            );
        }

        assert!(Error::CompactionBackpressure {
            table: "items".to_string(),
            segments: 32,
            physical_bytes: 1024,
            hard_segments: 32,
            hard_bytes: 2048,
        }
        .is_retryable());
    }

    #[test]
    fn test_error_equality() {
        assert_eq!(
            Error::TableNotFound("t".to_string()),
            Error::TableNotFound("t".to_string())
        );
        assert_ne!(
            Error::TableNotFound("t".to_string()),
            Error::TableAlreadyExists("t".to_string())
        );

        let err1 = Error::table_columns_not_match(5, 3);
        let err2 = Error::table_columns_not_match(5, 3);
        let err3 = Error::table_columns_not_match(5, 4);
        assert_eq!(err1, err2);
        assert_ne!(err1, err3);
    }

    #[test]
    fn navigation_errors_have_stable_machine_categories() {
        let error = Error::navigation(
            NavigationErrorCode::TargetColumnNotFound,
            "target column 'profiles.missing' does not exist",
        );
        assert_eq!(
            error.navigation_code(),
            Some(NavigationErrorCode::TargetColumnNotFound)
        );
        assert_eq!(
            error.to_string(),
            "NAVIGATION_TARGET_COLUMN_NOT_FOUND: target column 'profiles.missing' does not exist"
        );
    }

    #[test]
    fn neutral_context_does_not_depend_on_presentation_text() {
        let error = Error::RowLockTimeout {
            row_id: 17,
            timeout_ms: 250,
        };
        let context = error.context();
        assert_eq!(context.code().as_str(), "ROW_LOCK_TIMEOUT");
        assert_eq!(context.category(), ErrorCategory::Transaction);
        assert!(context.retryable());
        assert!(!context.not_found());
        assert!(!context.constraint_violation());

        let error = Error::ColumnNotFound("missing".to_owned());
        let context = error.context();
        assert_eq!(context.code().as_str(), "COLUMN_NOT_FOUND");
        assert_eq!(context.category(), ErrorCategory::Catalog);
        assert!(context.not_found());
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io { .. }));
        assert!(err.to_string().contains("file not found"));
    }
}
