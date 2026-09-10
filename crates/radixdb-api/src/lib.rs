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

//! Top-level Database API
//!
//! This module provides the high-level database interface for RadixDB.
//!
//! # Quick Start
//!
//! ```no_run
//! use radixdb_api::{Database, params};
//! # fn main() -> radixdb_core::Result<()> {
//!
//! // Open an in-memory database
//! let db = Database::open_in_memory()?;
//!
//! // Create a table
//! db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", ())?;
//!
//! // Insert data with parameters
//! db.execute("INSERT INTO users VALUES ($1, $2, $3)", (1, "Alice", 30))?;
//! db.execute("INSERT INTO users VALUES ($1, $2, $3)", params![2, "Bob", 25])?;
//!
//! // Query data
//! for row in db.query("SELECT * FROM users WHERE age > $1", (20,))? {
//!     let row = row?;
//!     let id: i64 = row.get(0)?;
//!     let name: String = row.get(1)?;
//!     println!("{}: {}", id, name);
//! }
//!
//! // Query single value
//! let count: i64 = db.query_one("SELECT COUNT(*) FROM users", ())?;
//!
//! // Transactions
//! let mut tx = db.begin()?;
//! tx.execute("UPDATE users SET age = age + 1", ())?;
//! tx.commit()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Parameter Binding
//!
//! Parameters can be passed in several ways:
//!
//! ```ignore
//! // Empty tuple for no parameters
//! db.execute("CREATE TABLE foo (id INTEGER)", ())?;
//!
//! // Tuple syntax for inline parameters
//! db.execute("INSERT INTO foo VALUES ($1, $2)", (1, "Alice"))?;
//!
//! // params! macro for explicit parameter list
//! db.execute("INSERT INTO foo VALUES ($1, $2)", params![1, "Alice"])?;
//!
//! // Optional values
//! let name: Option<&str> = Some("Alice");
//! db.execute("INSERT INTO foo VALUES ($1, $2)", (1, name))?;
//! ```
//!
//! # Prepared Statements
//!
//! ```ignore
//! let stmt = db.prepare("SELECT * FROM users WHERE id = $1")?;
//!
//! // Execute multiple times with different parameters
//! for id in 1..=10 {
//!     for row in stmt.query((id,))? {
//!         // ...
//!     }
//! }
//! ```

pub mod application;
pub mod database;
pub mod orm;
pub mod params;
pub mod public_read;
mod result_adapter;
pub mod rows;
#[doc(hidden)]
pub mod server_runtime;
pub mod statement;
pub mod transaction;
mod value;

pub use application::{
    ApplicationRelationIdentity, ApplicationRetentionOutcome, ApplicationRetentionPolicy,
    AuditEvent, ObjectId, OutboxClaim, OutboxCompletion, OutboxMessage, OutboxRetryDisposition,
    AUDIT_RELATION_NAME, OUTBOX_RELATION_NAME,
};
pub use database::{Database, FromValue};
pub use orm::{
    EmbeddedAlterTableRequest, EmbeddedCreateTableRequest, EmbeddedDdlRequest,
    EmbeddedDescribeDatabaseRequest, EmbeddedDescribeTableRequest, EmbeddedListTablesRequest,
    EmbeddedSchemaClient, EmbeddedTableColumnsRequest, EmbeddedTableConstraintsRequest,
    EmbeddedTableIndexesRequest, EmbeddedTableSchemaClient, OrmError, OrmResult,
};
pub use params::{DecimalValue, NamedParams, ParamVec, Params, ToParam};
pub use public_read::{
    PublicReadCursor, PublicReadCursorKey, PublicReadError, PublicReadErrorCode, PublicReadPage,
    PublicReadRequest, PublicReadResult,
};
pub use radixdb_executor::{
    BoundPublicReadPolicy, PublicReadColumnBinding, PublicReadLimits, PublicReadRelationBinding,
    PublicReadRelationSpec, QueryOutputColumn,
};
pub use rows::{FromRow, ResultRow, Rows};
#[doc(hidden)]
pub use server_runtime::sql_contains_transaction_control;
#[doc(hidden)]
pub use server_runtime::{
    DatabaseRuntimeState, ServerBatchFallback, ServerCancellation, ServerColumnBatch,
    ServerColumnData, ServerCredentialContract, ServerExecutionContext, ServerJobAttemptMetadata,
    ServerJobAttemptOutcome, ServerJobDiagnostic, ServerJobDiagnosticKind, ServerRuntimeMetrics,
    ServerScheduledJobDefinition, ServerScheduledJobSchedule, ServerStorageContract,
};
pub use statement::Statement;
pub use transaction::Transaction;
