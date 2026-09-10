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

//! Narrow library contract used by the network runtime.
//!
//! The TCP state machine owns frames, authentication and cursors. This module
//! prevents it from also depending on executor contexts, MVCC lifecycle enums,
//! physical column storage or the instrumentation implementation.

use std::time::Duration;

use crate::ObjectId;
use crate::{Database, NamedParams, ParamVec};
use radixdb_core::{Error, Result};
use radixdb_executor::context::{CancellationHandle, ExecutionContext};
#[doc(hidden)]
pub use radixdb_executor::procedural::{
    Diagnostic as ServerJobDiagnostic, DiagnosticKind as ServerJobDiagnosticKind,
    JobAttemptMetadata as ServerJobAttemptMetadata, JobAttemptOutcome as ServerJobAttemptOutcome,
    ScheduledJobDefinition as ServerScheduledJobDefinition,
    ScheduledJobSchedule as ServerScheduledJobSchedule,
};
use radixdb_storage::instrumentation::ProtocolColumnBatchFallback;
use radixdb_storage::traits::TypedBatchFallbackReason;
use radixdb_storage::volume::column::ColumnData;

/// Opaque cancellation signal shared by a server session and its requests.
#[derive(Clone, Debug)]
pub struct ServerCancellation {
    pub(crate) inner: CancellationHandle,
}

impl ServerCancellation {
    /// Create an independent cancellation signal.
    pub fn new() -> Self {
        Self {
            inner: ExecutionContext::new().cancellation_handle(),
        }
    }

    /// Request cancellation.
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    /// Return whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }
}

impl Default for ServerCancellation {
    fn default() -> Self {
        Self::new()
    }
}

/// Opaque request context accepted by embedded facade methods used by server.
pub struct ServerExecutionContext {
    pub(crate) inner: ExecutionContext,
}

impl ServerExecutionContext {
    /// Construct a positional-parameter request context.
    pub fn positional(params: ParamVec) -> Self {
        Self {
            inner: ExecutionContext::with_params(params),
        }
    }

    /// Construct a named-parameter request context.
    pub fn named(params: NamedParams) -> Self {
        Self {
            inner: ExecutionContext::with_named_params(params.into_inner()),
        }
    }

    /// Bind the authenticated session identity and immutable protocol request
    /// ID after user parameters have been admitted.
    pub fn bind_request_identity(&mut self, principal_id: ObjectId, request_id: u64) -> Result<()> {
        self.inner = self.inner.with_principal_id(principal_id);
        self.inner.set_request_id(request_id)
    }

    /// Inherit session/server shutdown cancellation without sharing the
    /// request-local cancellation bit.
    pub fn bind_parent_cancellation(&mut self, parent: &ServerCancellation) {
        self.inner.bind_parent_cancellation(&parent.inner);
    }

    /// Obtain the request-local signal registered under the protocol request ID.
    pub fn cancellation(&self) -> ServerCancellation {
        ServerCancellation {
            inner: self.inner.cancellation_handle(),
        }
    }

    pub(crate) fn inner(&self) -> &ExecutionContext {
        &self.inner
    }

    pub(crate) fn into_inner(self) -> ExecutionContext {
        self.inner
    }
}

/// Stable facade view of a database engine lifecycle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatabaseRuntimeState {
    Closed,
    Opening,
    Ready,
    Closing,
    CloseFailed(Error),
    Failed(Error),
}

impl Database {
    /// Snapshot enabled durable Job definitions for the stock scheduler host.
    #[doc(hidden)]
    pub fn scheduled_jobs_snapshot(&self) -> Result<Vec<ServerScheduledJobDefinition>> {
        self.with_connection_executor(|executor| executor.scheduled_jobs_snapshot())
    }

    /// Execute one scheduler-owned attempt through the ordinary procedural
    /// transaction boundary without exposing the executor to the stock server.
    #[doc(hidden)]
    pub fn execute_scheduled_job_attempt(
        &self,
        job_id: ObjectId,
        metadata: ServerJobAttemptMetadata,
        cancellation: &ServerCancellation,
    ) -> std::result::Result<ServerJobAttemptOutcome, ServerJobDiagnostic> {
        self.with_connection_executor(|executor| {
            let mut context = ExecutionContext::new();
            context.bind_parent_cancellation(&cancellation.inner);
            Ok(executor.execute_job_attempt(job_id, metadata, &context))
        })
        .map_err(|error| {
            ServerJobDiagnostic::new(
                ServerJobDiagnosticKind::RuntimeInvalidState,
                error.to_string(),
            )
        })?
    }

    /// Return lifecycle state without exposing the storage engine handle.
    pub fn runtime_state(&self) -> DatabaseRuntimeState {
        match self.engine().lifecycle_state() {
            radixdb_storage::mvcc::engine::EngineLifecycleState::Closed => {
                DatabaseRuntimeState::Closed
            }
            radixdb_storage::mvcc::engine::EngineLifecycleState::Opening => {
                DatabaseRuntimeState::Opening
            }
            radixdb_storage::mvcc::engine::EngineLifecycleState::Ready => {
                DatabaseRuntimeState::Ready
            }
            radixdb_storage::mvcc::engine::EngineLifecycleState::Closing => {
                DatabaseRuntimeState::Closing
            }
            radixdb_storage::mvcc::engine::EngineLifecycleState::CloseFailed(error) => {
                DatabaseRuntimeState::CloseFailed(error)
            }
            radixdb_storage::mvcc::engine::EngineLifecycleState::Failed(error) => {
                DatabaseRuntimeState::Failed(error)
            }
        }
    }
}

/// Typed column value crossing the embedded facade into a transport runtime.
pub enum ServerColumnData {
    Int64 {
        values: Vec<i64>,
        nulls: Vec<bool>,
    },
    Float64 {
        values: Vec<f64>,
        nulls: Vec<bool>,
    },
    TimestampNanos {
        values: Vec<i64>,
        nulls: Vec<bool>,
    },
    Boolean {
        values: Vec<bool>,
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
        data: Vec<u8>,
        offsets: Vec<(u64, u64)>,
        type_ref: radixdb_core::ExternalTypeRef,
        nulls: Vec<bool>,
    },
}

/// One decoded, output-ordered column batch independent of storage types.
pub struct ServerColumnBatch {
    row_count: usize,
    columns: Vec<ServerColumnData>,
}

impl ServerColumnBatch {
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn into_columns(self) -> Vec<ServerColumnData> {
        self.columns
    }

    pub(crate) fn from_storage(batch: radixdb_storage::traits::TypedColumnBatch) -> Result<Self> {
        let row_count = batch.row_count();
        let columns = batch
            .into_columns()
            .into_iter()
            .map(server_column_from_storage)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { row_count, columns })
    }
}

fn server_column_from_storage(column: ColumnData) -> Result<ServerColumnData> {
    match column {
        ColumnData::Int64 { values, nulls } => Ok(ServerColumnData::Int64 { values, nulls }),
        ColumnData::Float64 { values, nulls } => Ok(ServerColumnData::Float64 { values, nulls }),
        ColumnData::TimestampNanos { values, nulls } => {
            Ok(ServerColumnData::TimestampNanos { values, nulls })
        }
        ColumnData::Boolean { values, nulls } => Ok(ServerColumnData::Boolean { values, nulls }),
        ColumnData::Dictionary {
            ids,
            dictionary,
            nulls,
        } => Ok(ServerColumnData::DictionaryText {
            ids,
            dictionary: dictionary.iter().map(ToString::to_string).collect(),
            nulls,
        }),
        ColumnData::Bytes {
            data,
            offsets,
            ext_type: radixdb_core::DataType::Bytes,
            nulls,
        } => Ok(ServerColumnData::Bytes {
            data,
            offsets,
            nulls,
        }),
        ColumnData::Bytes {
            data,
            offsets,
            ext_type: radixdb_core::DataType::Json,
            nulls,
        } => Ok(ServerColumnData::JsonText {
            data,
            offsets,
            nulls,
        }),
        ColumnData::Bytes { ext_type, .. } => Err(Error::NotSupported(format!(
            "typed column protocol does not yet support {ext_type}"
        ))),
        ColumnData::External {
            data,
            offsets,
            type_ref,
            nulls,
        } => Ok(ServerColumnData::External {
            data,
            offsets,
            type_ref,
            nulls,
        }),
    }
}

/// Coarse transport-facing reason why a typed column batch is unavailable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerBatchFallback {
    RowState,
    QueryShape,
    StorageShape,
    Schema,
}

impl ServerBatchFallback {
    pub(crate) fn from_storage(reason: TypedBatchFallbackReason) -> Self {
        match reason {
            TypedBatchFallbackReason::RowAlreadyFetched
            | TypedBatchFallbackReason::RowIterationStarted
            | TypedBatchFallbackReason::Closed
            | TypedBatchFallbackReason::PendingError => Self::RowState,
            TypedBatchFallbackReason::RowFilter
            | TypedBatchFallbackReason::DictionaryFilter
            | TypedBatchFallbackReason::IndexSelection
            | TypedBatchFallbackReason::TypedPredicate
            | TypedBatchFallbackReason::ExactTypedFilter
            | TypedBatchFallbackReason::FilterCoveredByTypedPredicates
            | TypedBatchFallbackReason::RowGroupSkips
            | TypedBatchFallbackReason::EmptyProjection
            | TypedBatchFallbackReason::DuplicateProjection
            | TypedBatchFallbackReason::MergedSource
            | TypedBatchFallbackReason::MixedTypedAndRowSources
            | TypedBatchFallbackReason::UnsupportedResultShape => Self::QueryShape,
            TypedBatchFallbackReason::NotArtifactBacked
            | TypedBatchFallbackReason::UnsupportedStorageType
            | TypedBatchFallbackReason::InvalidRange => Self::StorageShape,
            TypedBatchFallbackReason::SchemaMappingMissingColumn
            | TypedBatchFallbackReason::UnsupportedSchemaDefault => Self::Schema,
        }
    }
}

/// Instrumentation adapter kept outside the network state machine.
pub struct ServerRuntimeMetrics;

impl ServerRuntimeMetrics {
    pub fn connection_opened() {
        radixdb_storage::instrumentation::record_server_connection_opened();
    }

    pub fn connection_closed() {
        radixdb_storage::instrumentation::record_server_connection_closed();
    }

    pub fn execution_started() {
        radixdb_storage::instrumentation::record_server_execution_started();
    }

    pub fn execution_finished() {
        radixdb_storage::instrumentation::record_server_execution_finished();
    }

    pub fn replace_session_owners(
        before_sessions: u64,
        after_sessions: u64,
        before_cursors: u64,
        after_cursors: u64,
        before_prepared: u64,
        after_prepared: u64,
    ) {
        radixdb_storage::instrumentation::replace_server_session_owners(
            before_sessions,
            after_sessions,
            before_cursors,
            after_cursors,
            before_prepared,
            after_prepared,
        );
    }

    pub fn flush_thread_local() {
        radixdb_storage::instrumentation::flush_thread_local_counters();
    }

    pub fn protocol_encode(bytes: u64, elapsed: Duration) {
        radixdb_storage::instrumentation::record_protocol_encode(bytes, elapsed);
    }

    pub fn protocol_socket_write(bytes: u64, elapsed: Duration) {
        radixdb_storage::instrumentation::record_protocol_socket_write(bytes, elapsed);
    }

    pub fn protocol_round_trip(elapsed: Duration) {
        radixdb_storage::instrumentation::record_protocol_round_trip(elapsed);
    }

    pub fn protocol_row_adapter(values: u64) {
        radixdb_storage::instrumentation::record_protocol_row_adapter(values);
    }

    pub fn protocol_result_rows(rows: u64) {
        radixdb_storage::instrumentation::record_protocol_result_rows(rows);
    }

    pub fn protocol_row_batch(rows: u64) {
        radixdb_storage::instrumentation::record_protocol_row_batch(rows);
    }

    pub fn protocol_column_batch_fallback(reason: ServerBatchFallback) {
        let reason = match reason {
            ServerBatchFallback::RowState => ProtocolColumnBatchFallback::RowState,
            ServerBatchFallback::QueryShape => ProtocolColumnBatchFallback::QueryShape,
            ServerBatchFallback::StorageShape => ProtocolColumnBatchFallback::StorageShape,
            ServerBatchFallback::Schema => ProtocolColumnBatchFallback::Schema,
        };
        radixdb_storage::instrumentation::record_protocol_column_batch_fallback(reason);
    }

    pub fn column_batch_pending_opened(rows: u64, retained_bytes: u64) {
        radixdb_storage::instrumentation::record_protocol_column_batch_pending_opened(
            rows,
            retained_bytes,
        );
    }

    pub fn column_batch_pending_completed(rows: u64, retained_bytes: u64) {
        radixdb_storage::instrumentation::record_protocol_column_batch_pending_completed(
            rows,
            retained_bytes,
        );
    }

    pub fn column_batch_pending_dropped(rows: u64, retained_bytes: u64) {
        radixdb_storage::instrumentation::record_protocol_column_batch_pending_dropped(
            rows,
            retained_bytes,
        );
    }
}

/// Storage tuning values admitted by server configuration without exposing
/// the physical storage module to the network runtime.
pub struct ServerStorageContract;

impl ServerStorageContract {
    pub const DEFAULT_COPY_MAX_TRANSACTION_BYTES: usize =
        radixdb_storage::config::DEFAULT_COPY_MAX_TRANSACTION_BYTES;
    pub const DEFAULT_MAX_COMPACTION_JOBS: usize =
        radixdb_storage::config::DEFAULT_MAX_COMPACTION_JOBS;
    pub const MAX_COMPACTION_JOBS: usize = radixdb_storage::config::MAX_COMPACTION_JOBS;
    pub const DEFAULT_STORAGE_CPU_WORKERS: usize =
        radixdb_storage::config::DEFAULT_STORAGE_CPU_WORKERS;
    pub const DEFAULT_PAGE_CACHE_LEVEL: u8 = radixdb_storage::config::DEFAULT_PAGE_CACHE_LEVEL;
    pub const MAX_PAGE_CACHE_LEVEL: u8 = radixdb_storage::config::MAX_PAGE_CACHE_LEVEL;
    pub const DEFAULT_PAGE_CACHE_MAX_BYTES: u64 =
        radixdb_storage::config::DEFAULT_PAGE_CACHE_MAX_BYTES;
    pub const DEFAULT_PAGE_CACHE_MEMORY_RESERVE: u64 =
        radixdb_storage::config::DEFAULT_PAGE_CACHE_MEMORY_RESERVE;
}

/// Credential primitives admitted by server configuration without exposing
/// the executor implementation crate to the network runtime.
pub struct ServerCredentialContract;

impl ServerCredentialContract {
    pub fn validate_password_verifier(encoded: &str) -> Result<()> {
        radixdb_executor::credentials::validate_password_verifier(encoded)
    }

    pub fn verify_password_verifier(encoded: &str, password: &str) -> bool {
        radixdb_executor::credentials::verify_password_verifier(encoded, password)
    }

    pub fn hash_password_verifier(password: &str) -> Result<String> {
        radixdb_executor::credentials::hash_password_verifier(password)
    }
}

/// Classify transaction-lifecycle SQL without granting the server parser/AST ownership.
pub fn sql_contains_transaction_control(sql: &str) -> bool {
    radixdb_executor::program_contains_transaction_control(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_typed_sources_are_a_query_shape_fallback() {
        assert_eq!(
            ServerBatchFallback::from_storage(TypedBatchFallbackReason::MixedTypedAndRowSources),
            ServerBatchFallback::QueryShape
        );
    }
}
