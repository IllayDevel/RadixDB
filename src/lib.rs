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

#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

//! # RadixDB — embedded API and server composition facade
//!
//! RadixDB is a modern embedded SQL database written in pure Rust. It provides
//! full ACID transactions with MVCC, a rule- and statistics-informed runtime
//! planner, and a hybrid row/column storage engine.
//!
//! ## Key Features
//!
//! - **MVCC Transactions** - Full multi-version concurrency control with snapshot isolation
//! - **Multiple Index Types** - B-tree, Hash, and Bitmap indexes (auto-selected by data type)
//! - **Multi-Column Indexes** - Composite indexes for complex query patterns
//! - **Runtime Query Planning** - Index, join, pushdown, cache-feedback, and
//!   parallel execution decisions over current schema and statistics
//! - **Parallel Query Execution** - Automatic parallelization using Rayon
//! - **Semantic Query Caching** - Intelligent result caching with predicate subsumption
//! - **AS OF Temporal Queries** - Built-in time-travel queries
//! - **Window Functions** - ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE, etc.
//! - **CTEs** - Common Table Expressions including recursive CTEs
//! - **101+ Built-in Functions** - String, math, date/time, JSON, aggregate, window
//!
//! ## Quick Start
//!
//! ```rust
//! use radixdb::api::Database;
//!
//! // Open in-memory database
//! let db = Database::open_in_memory().unwrap();
//!
//! // Create a table
//! db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", ()).unwrap();
//!
//! // Insert data
//! db.execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)", ()).unwrap();
//!
//! // Query with window function
//! let rows = db.query("
//!     SELECT name, age, RANK() OVER (ORDER BY age DESC) as rank
//!     FROM users
//! ", ()).unwrap();
//! ```
//!
//! ## Supported public surface
//!
//! Embedded applications use the top-level handles ([`Database`],
//! [`Statement`], [`Transaction`], [`Rows`]) or their definitions in [`api`].
//! Server deployments use [`server`], and logical backup/restore tooling uses
//! [`sql_dump`]. TCP applications should depend on the standalone
//! `radixdb-client` crate.
//!
//! The historical `core`, `storage`, `parser`, `functions`, `executor`,
//! `optimizer`, `client`, and `protocol` namespaces are direct aliases of their
//! canonical crates. They have no mirror trees in this crate, are hidden from
//! rustdoc, and are not independent public extension APIs. Prefer the top-level
//! re-exports when a public type is available there.

// Source support is intentionally Linux-only. Platform-specific fallback
// implementations are not retained behind this admission guard.
#[cfg(not(target_os = "linux"))]
compile_error!("RadixDB v1 release artifacts support Linux targets only");

#[cfg(feature = "test-mutations")]
pub mod test_mutations;

// Use mimalloc as global allocator when feature is enabled
// (but not when dhat-heap is enabled, as it needs its own allocator)
#[cfg(all(feature = "mimalloc", not(feature = "dhat-heap")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub use radixdb_api as api;
#[cfg(feature = "cli")]
#[doc(hidden)]
pub mod cli;
#[doc(hidden)]
pub use radixdb_client as client;
#[doc(hidden)]
pub mod common;
#[doc(hidden)]
pub use radixdb_core as core;
#[doc(hidden)]
pub use radixdb_executor as executor;
#[doc(hidden)]
pub use radixdb_executor::optimizer;
#[doc(hidden)]
pub use radixdb_functions as functions;
#[doc(hidden)]
pub use radixdb_protocol as protocol;
#[doc(hidden)]
pub use radixdb_sql as parser;
pub mod server;
pub mod sql_dump;
#[doc(hidden)]
pub use radixdb_storage as storage;

// Re-export main types for convenience
pub use core::{
    DataType, Error, ErrorCategory, ErrorCode, ErrorContext, IndexEntry, IndexType, IsolationLevel,
    NavigationErrorCode, Operator, ReferenceDescriptor, ReferenceTargetKey, Result, Row, Schema,
    SchemaBuilder, SchemaColumn, SchemaColumnId, SchemaTableId, Value,
};

// Re-export common utilities
pub use common::{BufferPool, I64Map, I64Set, PoolStats, SemVer, SmartString};
pub use radixdb_api::{named_params, params};
pub use radixdb_core::compact_vec;
pub use radixdb_core::row;

// Re-export storage/expression types
pub use storage::{
    AndExpr, BetweenExpr, CastExpr, ComparisonExpr, CompoundExpr, Expression, InListExpr, NotExpr,
    NullCheckExpr, OrExpr, RangeExpr,
};

// Re-export config types
pub use storage::{Config, PersistenceConfig, SyncMode};

// Re-export storage traits
pub use radixdb_storage::traits::{
    EmptyResult, EmptyScanner, Engine, Index, MemoryResult, PhysicalSnapshotIdentity, QueryResult,
    Scanner, Table, TemporalType, Transaction as StorageTransaction, VecScanner,
};

// Re-export MVCC types
pub use radixdb_storage::mvcc::{
    MVCCEngine, MvccTransaction, RowVersion, TransactionEngineOperations, TransactionRegistry,
    TransactionState, TransactionVersionStore, VersionStore, VisibilityChecker, WriteSetEntry,
    INVALID_TRANSACTION_ID, RECOVERY_TRANSACTION_ID,
};
pub use radixdb_storage::traits::MVCCScanner;
pub use radixdb_storage::BTreeIndex;

// Re-export WAL types
pub use radixdb_storage::mvcc::{
    WALEntry, WALManager, WALOperationType, DEFAULT_WAL_BUFFER_SIZE, DEFAULT_WAL_FLUSH_TRIGGER,
    DEFAULT_WAL_MAX_SIZE,
};

// Re-export Persistence types
pub use radixdb_storage::mvcc::{
    deserialize_row_version, deserialize_value, serialize_row_version, serialize_value,
    serialize_value_into, IndexMetadata, PersistenceManager, PersistenceMeta,
    DEFAULT_CHECKPOINT_INTERVAL, DEFAULT_KEEP_SNAPSHOTS,
};

// Re-export function types
pub use functions::{
    AggregateFunction, FunctionDataType, FunctionInfo, FunctionRegistry, FunctionSignature,
    FunctionType, ScalarFunction, WindowFunction,
};
pub use radixdb_functions::aggregate::{
    AvgFunction, CountFunction, FirstFunction, LastFunction, MaxFunction, MinFunction, SumFunction,
};
pub use radixdb_functions::validate_arg_count;

// Re-export specific function implementations
pub use radixdb_functions::scalar::{
    AbsFunction, CeilingFunction, CoalesceFunction, ConcatFunction, FloorFunction, IfNullFunction,
    LengthFunction, LowerFunction, NowFunction, NullIfFunction, RoundFunction, SubstringFunction,
    UpperFunction,
};
pub use radixdb_functions::window::{
    DenseRankFunction, LagFunction, LeadFunction, NtileFunction, RankFunction, RowNumberFunction,
};

// Re-export executor types
pub use radixdb_executor::context::ExecutionContext;
pub use radixdb_executor::planner::{ColumnStatsCache, QueryPlanner, StatsHealth};
pub use radixdb_executor::query_cache::{CacheStats, CachedPlanRef, CachedQueryPlan, QueryCache};
pub use radixdb_executor::result::{ExecResult, ExecutorResult};
pub use radixdb_executor::Executor;

// Re-export API types
pub use api::{
    ApplicationRelationIdentity, ApplicationRetentionOutcome, ApplicationRetentionPolicy,
    AuditEvent, Database, DecimalValue, FromRow, FromValue, NamedParams, ObjectId, OutboxClaim,
    OutboxCompletion, OutboxMessage, OutboxRetryDisposition, ParamVec, Params, ResultRow, Rows,
    Statement, ToParam, Transaction, AUDIT_RELATION_NAME, OUTBOX_RELATION_NAME,
};

/// Backward-compatible spelling for the high-level embedded transaction.
pub use api::Transaction as ApiTransaction;

#[cfg(any(test, feature = "test-failpoints"))]
pub use radixdb_storage::test_failpoints;

#[cfg(test)]
mod size_tests {
    #[test]
    fn check_ast_sizes() {
        use std::mem::size_of;
        println!("\n=== AST Type Sizes ===");
        println!("Token: {} bytes", size_of::<crate::parser::token::Token>());
        println!(
            "Identifier: {} bytes",
            size_of::<crate::parser::ast::Identifier>()
        );
        println!(
            "Expression: {} bytes",
            size_of::<crate::parser::ast::Expression>()
        );
        println!(
            "Statement: {} bytes",
            size_of::<crate::parser::ast::Statement>()
        );
    }
}
