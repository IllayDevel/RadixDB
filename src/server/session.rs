use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Instant,
};

use crate::api::{
    sql_contains_transaction_control, DatabaseRuntimeState, ServerBatchFallback,
    ServerCancellation, ServerColumnBatch, ServerColumnData, ServerExecutionContext,
    ServerRuntimeMetrics,
};
use crate::protocol::{
    encode_payload, encoded_payload_len, read_frame_with_payload_len_hook, write_frame_payload,
    ClientMessage, Column, DatabaseArtifactSummary, DatabaseStatus, ProtocolCapability,
    ProtocolErrorCode, ProtocolFailure, Row, ServerLifecycleState, ServerMessage, ServerStatus,
    TransactionIsolation, WireColumn, WireValue, DEFAULT_MAX_FRAME_BYTES, PROTOCOL_VERSION,
};
use crate::{
    ApiTransaction, Database, Error as DatabaseError, IsolationLevel, ObjectId, Rows, Statement,
};
use radixdb_plugin_host::PluginRegistry;

use super::value_codec::{
    radixdb_value_to_wire, slice_external_bytes, wire_column_len, wire_column_retained_bytes,
    wire_parameters_to_named, wire_value_to_radixdb,
};
use super::{build_identity, ServerConfig};
use status::{collect_database_artifacts, server_status, unavailable_artifact_summary};

#[cfg(test)]
static EMPTY_PLUGIN_REGISTRY: std::sync::LazyLock<Arc<PluginRegistry>> =
    std::sync::LazyLock::new(|| Arc::new(PluginRegistry::empty()));

mod status;

#[cfg(test)]
type ArtifactScanTestHook = std::sync::Arc<dyn Fn(&std::path::Path) + Send + Sync>;

#[cfg(test)]
static ARTIFACT_SCAN_TEST_HOOK: std::sync::LazyLock<Mutex<Option<ArtifactScanTestHook>>> =
    std::sync::LazyLock::new(|| Mutex::new(None));
#[cfg(test)]
static ARTIFACT_SCAN_TEST_HOOK_OWNER: Mutex<()> = Mutex::new(());

#[cfg(test)]
type ExecuteSqlTestHook = std::sync::Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
static EXECUTE_SQL_TEST_HOOK: std::sync::LazyLock<Mutex<Option<ExecuteSqlTestHook>>> =
    std::sync::LazyLock::new(|| Mutex::new(None));
#[cfg(test)]
static EXECUTE_SQL_TEST_HOOK_OWNER: Mutex<()> = Mutex::new(());

#[cfg(test)]
fn replace_execute_sql_test_hook(hook: Option<ExecuteSqlTestHook>) {
    *EXECUTE_SQL_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = hook;
}

#[cfg(test)]
pub(crate) struct ExecuteSqlTestHookGuard {
    _owner: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl ExecuteSqlTestHookGuard {
    pub(crate) fn install(hook: ExecuteSqlTestHook) -> Self {
        let owner = EXECUTE_SQL_TEST_HOOK_OWNER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        replace_execute_sql_test_hook(Some(hook));
        Self { _owner: owner }
    }
}

#[cfg(test)]
impl Drop for ExecuteSqlTestHookGuard {
    fn drop(&mut self) {
        replace_execute_sql_test_hook(None);
    }
}

#[cfg(test)]
struct ArtifactScanTestHookGuard {
    _owner: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl ArtifactScanTestHookGuard {
    fn install(hook: ArtifactScanTestHook) -> Self {
        let owner = ARTIFACT_SCAN_TEST_HOOK_OWNER
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *ARTIFACT_SCAN_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
        Self { _owner: owner }
    }
}

#[cfg(test)]
impl Drop for ArtifactScanTestHookGuard {
    fn drop(&mut self) {
        *ARTIFACT_SCAN_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

#[derive(Clone)]
pub enum DatabaseRegistryEntry {
    Opening {
        artifacts: DatabaseArtifactSummary,
    },
    Ready {
        database: Database,
        artifacts: DatabaseArtifactSummary,
        sessions: usize,
    },
    Failed {
        error: DatabaseError,
        artifacts: DatabaseArtifactSummary,
        retry_after: Instant,
    },
}

pub(crate) struct RuntimeState {
    pub(crate) active_connections: AtomicUsize,
    pub(crate) inflight_frame_bytes: AtomicUsize,
    frame_budget_gate: Mutex<()>,
    frame_budget_ready: Condvar,
    executions: Mutex<BTreeMap<u64, ServerCancellation>>,
    pub(crate) job_scheduler: super::job_scheduler::JobSchedulerRuntime,
}

impl RuntimeState {
    pub(crate) fn new() -> Self {
        Self {
            active_connections: AtomicUsize::new(0),
            inflight_frame_bytes: AtomicUsize::new(0),
            frame_budget_gate: Mutex::new(()),
            frame_budget_ready: Condvar::new(),
            executions: Mutex::new(BTreeMap::new()),
            job_scheduler: super::job_scheduler::JobSchedulerRuntime::default(),
        }
    }

    fn acquire_frame(&self, bytes: usize, limit: usize) -> std::io::Result<FramePermit<'_>> {
        if bytes > limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("frame payload {bytes} exceeds global frame budget {limit}"),
            ));
        }
        let mut gate = self
            .frame_budget_gate
            .lock()
            .map_err(|_| std::io::Error::other("global frame-budget admission lock is poisoned"))?;
        loop {
            if self
                .inflight_frame_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current.checked_add(bytes).filter(|next| *next <= limit)
                })
                .is_ok()
            {
                return Ok(FramePermit {
                    runtime: self,
                    bytes,
                });
            }
            gate = self.frame_budget_ready.wait(gate).map_err(|_| {
                std::io::Error::other("global frame-budget admission lock is poisoned")
            })?;
        }
    }

    fn register_execution(
        &self,
        request_id: u64,
        cancellation: ServerCancellation,
    ) -> Result<ExecutionRegistration<'_>, String> {
        if request_id == 0 {
            return Err("request_id must not be zero".into());
        }
        let mut executions = self
            .executions
            .lock()
            .map_err(|_| "execution registry is poisoned".to_string())?;
        if executions.contains_key(&request_id) {
            return Err(format!("request_id {request_id} is already active"));
        }
        executions.insert(request_id, cancellation);
        ServerRuntimeMetrics::execution_started();
        Ok(ExecutionRegistration {
            runtime: self,
            request_id,
        })
    }

    fn cancel_execution(&self, request_id: u64) -> Result<bool, String> {
        let executions = self
            .executions
            .lock()
            .map_err(|_| "execution registry is poisoned".to_string())?;
        if let Some(handle) = executions.get(&request_id) {
            handle.cancel();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[cfg(test)]
    pub(crate) fn active_execution_count(&self) -> usize {
        self.executions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
}

struct FramePermit<'a> {
    runtime: &'a RuntimeState,
    bytes: usize,
}
impl Drop for FramePermit<'_> {
    fn drop(&mut self) {
        self.runtime
            .inflight_frame_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
        if let Ok(_gate) = self.runtime.frame_budget_gate.lock() {
            self.runtime.frame_budget_ready.notify_all();
        }
    }
}
struct ExecutionRegistration<'a> {
    runtime: &'a RuntimeState,
    request_id: u64,
}
impl Drop for ExecutionRegistration<'_> {
    fn drop(&mut self) {
        if let Ok(mut executions) = self.runtime.executions.lock() {
            executions.remove(&self.request_id);
        }
        ServerRuntimeMetrics::execution_finished();
    }
}

#[derive(Debug)]
pub(super) enum OpenDatabaseError {
    InvalidName(String),
    Opening(String),
    Startup(DatabaseError),
    Server(String),
}

impl OpenDatabaseError {
    fn protocol_code(&self) -> ProtocolErrorCode {
        match self {
            Self::InvalidName(_) => ProtocolErrorCode::ProtocolViolation,
            Self::Opening(_) | Self::Startup(_) | Self::Server(_) => ProtocolErrorCode::ServerError,
        }
    }
}

impl std::fmt::Display for OpenDatabaseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidName(message) | Self::Opening(message) | Self::Server(message) => {
                formatter.write_str(message)
            }
            Self::Startup(error) => error.fmt(formatter),
        }
    }
}

#[derive(Debug)]
pub enum ServerSessionError {
    Protocol(crate::protocol::ProtocolError),
    Io(std::io::Error),
}

impl std::fmt::Display for ServerSessionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Protocol(error) => error.fmt(formatter),
            Self::Io(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ServerSessionError {}

impl From<crate::protocol::ProtocolError> for ServerSessionError {
    fn from(error: crate::protocol::ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl From<std::io::Error> for ServerSessionError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

// `ReadySession` is the hot state of a long-lived TCP session. Keep it inline:
// boxing would save enum size but add heap indirection to every ready session.
#[allow(clippy::large_enum_variant)]
enum SessionState {
    AwaitHandshake,
    AwaitAuthentication {
        column_batch_v1: bool,
        build_identity_v1: bool,
        external_value_v1: bool,
    },
    Ready(ReadySession),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PublishedSessionOwners {
    sessions: u64,
    cursors: u64,
    prepared: u64,
}

impl PublishedSessionOwners {
    fn from_state(state: &SessionState) -> Self {
        match state {
            SessionState::Ready(session) => Self {
                sessions: 1,
                cursors: u64::from(session.cursor.is_some()),
                prepared: session.prepared.len() as u64,
            },
            SessionState::AwaitHandshake | SessionState::AwaitAuthentication { .. } => {
                Self::default()
            }
        }
    }
}

#[derive(Default)]
struct SessionOwnerPublication {
    current: PublishedSessionOwners,
}

impl SessionOwnerPublication {
    fn publish(&mut self, state: &SessionState) {
        let next = PublishedSessionOwners::from_state(state);
        ServerRuntimeMetrics::replace_session_owners(
            self.current.sessions,
            next.sessions,
            self.current.cursors,
            next.cursors,
            self.current.prepared,
            next.prepared,
        );
        self.current = next;
    }
}

impl Drop for SessionOwnerPublication {
    fn drop(&mut self) {
        ServerRuntimeMetrics::replace_session_owners(
            self.current.sessions,
            0,
            self.current.cursors,
            0,
            self.current.prepared,
            0,
        );
    }
}

/// A server response that may already own its exact wire payload.
///
/// Only a bounded `ColumnBatch` uses `Encoded`: it is serialised once while
/// enforcing the configured cursor/frame limits, then written unchanged. All
/// ordinary protocol messages retain the generic `write_frame` path.
enum ServerResponse {
    Message(ServerMessage),
    Encoded { payload: Vec<u8> },
}

impl ServerResponse {
    fn write_to<S: Write>(
        self,
        stream: &mut S,
        max_frame_bytes: u32,
        runtime: &RuntimeState,
        global_limit: usize,
    ) -> Result<(), crate::protocol::ProtocolError> {
        match self {
            Self::Message(message) => {
                let encode_start = Instant::now();
                let payload = encode_payload(&message)?;
                ServerRuntimeMetrics::protocol_encode(payload.len() as u64, encode_start.elapsed());
                let _permit = runtime.acquire_frame(payload.len(), global_limit)?;
                write_payload_with_timing(stream, &payload, max_frame_bytes)
            }
            Self::Encoded { payload } => {
                let _permit = runtime.acquire_frame(payload.len(), global_limit)?;
                write_payload_with_timing(stream, &payload, max_frame_bytes)
            }
        }
    }
}

fn write_payload_with_timing<S: Write>(
    stream: &mut S,
    payload: &[u8],
    max_frame_bytes: u32,
) -> Result<(), crate::protocol::ProtocolError> {
    let write_start = Instant::now();
    write_frame_payload(stream, payload, max_frame_bytes)?;
    ServerRuntimeMetrics::protocol_socket_write(
        (payload.len() as u64).saturating_add(4),
        write_start.elapsed(),
    );
    Ok(())
}

/// Makes thread-local instrumentation visible even when a session exits on an
/// I/O error or a client disconnects between request boundaries.
struct ThreadLocalCounterFlushGuard;

impl Drop for ThreadLocalCounterFlushGuard {
    fn drop(&mut self) {
        ServerRuntimeMetrics::flush_thread_local();
    }
}

struct ReadySession {
    principal_id: ObjectId,
    /// Durable Principal identities belong to exactly one database catalog.
    /// `None` is the loopback-only bootstrap recovery session.
    authenticated_database: Option<String>,
    selected_database_name: Option<String>,
    selected_database: Option<Database>,
    cursor: Option<SessionCursor>,
    transaction: Option<ApiTransaction>,
    next_cursor_id: u64,
    prepared: BTreeMap<u64, Statement>,
    next_statement_id: u64,
    column_batch_v1: bool,
    build_identity_v1: bool,
    external_value_v1: bool,
}

struct WireBindings {
    positional: Vec<WireValue>,
    named: BTreeMap<String, WireValue>,
}

struct SessionCursor {
    id: u64,
    rows: Rows,
    /// Rows retained only when one prepared legacy RowBatch exceeded the
    /// configured byte/frame limit. The bounded tail is exhausted before the
    /// storage iterator advances again.
    pending_row_batch: Option<PendingRowBatch>,
    /// One decoded typed storage group being adapted to legacy RowBatch wire
    /// frames. This bypasses intermediate storage Row/Value materialization.
    pending_row_columns: Option<Box<PendingRowColumns>>,
    /// A decoded artifact-backed group that did not fit into one bounded protocol frame.
    ///
    /// The underlying scanner has already advanced past this group, so its
    /// remainder must be sent before asking it for another batch.
    pending_column_batch: Option<PendingColumnBatch>,
    /// Prevent mixing `Fetch` after a scanner has advanced through the typed
    /// path. Doing so would otherwise silently skip the consumed artifact-backed group.
    typed_batches_started: bool,
    /// Prevent switching from a legacy Fetch that consumed typed storage
    /// groups to FetchColumnBatch on the same cursor.
    row_typed_batches_started: bool,
}

struct PendingRowBatch {
    rows: Vec<Row>,
    /// Whether the storage iterator was exhausted behind this retained tail.
    eof: bool,
}

struct PendingRowColumns {
    columns: Vec<WireColumn>,
    row_count: usize,
    next_row: usize,
}

impl PendingRowColumns {
    fn new(columns: Vec<WireColumn>, row_count: usize) -> Self {
        debug_assert!(columns
            .iter()
            .all(|column| wire_column_len(column) == row_count));
        Self {
            columns,
            row_count,
            next_row: 0,
        }
    }

    fn is_complete(&self) -> bool {
        self.next_row == self.row_count
    }
}

/// One decoded typed batch retained only when it must be split to honour the
/// configured byte/frame limit. Normal-size artifact-backed groups bypass this structure
/// and retain the zero-copy column move into the prepared response.
struct PendingColumnBatch {
    columns: Vec<WireColumn>,
    row_count: usize,
    next_row: usize,
    retained_bytes: u64,
}

impl PendingColumnBatch {
    fn new(columns: Vec<WireColumn>, row_count: usize) -> Self {
        debug_assert!(columns
            .iter()
            .all(|column| wire_column_len(column) == row_count));
        let retained_bytes = wire_columns_retained_bytes(&columns);
        ServerRuntimeMetrics::column_batch_pending_opened(row_count as u64, retained_bytes);
        Self {
            columns,
            row_count,
            next_row: 0,
            retained_bytes,
        }
    }

    fn remaining_rows(&self) -> usize {
        self.row_count.saturating_sub(self.next_row)
    }

    fn is_complete(&self) -> bool {
        self.next_row == self.row_count
    }
}

impl Drop for PendingColumnBatch {
    fn drop(&mut self) {
        if self.is_complete() {
            ServerRuntimeMetrics::column_batch_pending_completed(
                self.row_count as u64,
                self.retained_bytes,
            );
        } else {
            ServerRuntimeMetrics::column_batch_pending_dropped(
                self.row_count as u64,
                self.retained_bytes,
            );
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionReadPhase {
    Idle,
    FramePayload,
}

#[cfg(test)]
pub(crate) fn serve_connection_with_databases_and_limits<S, F>(
    stream: &mut S,
    config: &ServerConfig,
    databases: &Mutex<BTreeMap<String, DatabaseRegistryEntry>>,
    configure_read_phase: F,
) -> Result<(), ServerSessionError>
where
    S: Read + Write,
    F: FnMut(&mut S, SessionReadPhase) -> std::io::Result<()>,
{
    let runtime = RuntimeState::new();
    serve_connection_with_databases_limits_and_cancellation(
        stream,
        config,
        databases,
        Arc::clone(&EMPTY_PLUGIN_REGISTRY),
        ServerCancellation::new(),
        &runtime,
        configure_read_phase,
    )
}

pub(crate) fn serve_connection_with_databases_limits_and_cancellation<S, F>(
    stream: &mut S,
    config: &ServerConfig,
    databases: &Mutex<BTreeMap<String, DatabaseRegistryEntry>>,
    plugin_registry: Arc<PluginRegistry>,
    cancellation: ServerCancellation,
    runtime: &RuntimeState,
    mut configure_read_phase: F,
) -> Result<(), ServerSessionError>
where
    S: Read + Write,
    F: FnMut(&mut S, SessionReadPhase) -> std::io::Result<()>,
{
    let _counter_flush_guard = ThreadLocalCounterFlushGuard;
    let mut state = SessionState::AwaitHandshake;
    let mut owner_publication = SessionOwnerPublication::default();
    let mut max_frame_bytes = config.max_frame_bytes.min(DEFAULT_MAX_FRAME_BYTES);
    let result = (|| loop {
        let mut frame_permit = None;
        let session_ready = matches!(state, SessionState::Ready(_));
        if session_ready {
            configure_read_phase(stream, SessionReadPhase::Idle)?;
        }
        let message = match read_frame_with_payload_len_hook(
            stream,
            max_frame_bytes,
            |stream, payload_bytes| {
                frame_permit = Some(
                    runtime
                        .acquire_frame(payload_bytes as usize, config.max_inflight_frame_bytes)?,
                );
                if session_ready {
                    configure_read_phase(stream, SessionReadPhase::FramePayload)?;
                }
                Ok(())
            },
        ) {
            Ok(message) => message,
            Err(crate::protocol::ProtocolError::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::WouldBlock
                ) =>
            {
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        #[cfg(feature = "bench-harness")]
        let round_trip_started = Instant::now();
        let session_runtime = SessionRuntimeContext {
            databases,
            plugin_registry: &plugin_registry,
            cancellation: &cancellation,
            runtime,
        };
        let response = handle_message_with_cancellation(
            &mut state,
            message,
            config,
            &session_runtime,
            &mut max_frame_bytes,
        );
        owner_publication.publish(&state);
        drop(frame_permit.take());
        ServerRuntimeMetrics::flush_thread_local();
        #[cfg(any(test, feature = "test-failpoints"))]
        if matches!(
            &response,
            ServerResponse::Message(ServerMessage::TransactionCommitted)
        ) {
            crate::test_failpoints::interleave(
                crate::test_failpoints::InterleavePoint::CommitAckReady,
                0,
            );
        }
        let write_result = response.write_to(
            stream,
            max_frame_bytes,
            runtime,
            config.max_inflight_frame_bytes,
        );
        #[cfg(feature = "bench-harness")]
        ServerRuntimeMetrics::protocol_round_trip(round_trip_started.elapsed());
        write_result?;
    })();
    if let SessionState::Ready(session) = &mut state {
        release_selected_database(session, databases);
    }
    result
}

#[cfg(test)]
fn handle_message(
    state: &mut SessionState,
    message: ClientMessage,
    config: &ServerConfig,
    databases: &Mutex<BTreeMap<String, DatabaseRegistryEntry>>,
    max_frame_bytes: &mut u32,
) -> ServerResponse {
    let cancellation = ServerCancellation::new();
    let runtime = RuntimeState::new();
    let session_runtime = SessionRuntimeContext {
        databases,
        plugin_registry: &EMPTY_PLUGIN_REGISTRY,
        cancellation: &cancellation,
        runtime: &runtime,
    };
    handle_message_with_cancellation(state, message, config, &session_runtime, max_frame_bytes)
}

struct SessionRuntimeContext<'a> {
    databases: &'a Mutex<BTreeMap<String, DatabaseRegistryEntry>>,
    plugin_registry: &'a Arc<PluginRegistry>,
    cancellation: &'a ServerCancellation,
    runtime: &'a RuntimeState,
}

fn handle_message_with_cancellation(
    state: &mut SessionState,
    message: ClientMessage,
    config: &ServerConfig,
    session_runtime: &SessionRuntimeContext<'_>,
    max_frame_bytes: &mut u32,
) -> ServerResponse {
    let SessionRuntimeContext {
        databases,
        plugin_registry,
        ..
    } = session_runtime;
    match state {
        SessionState::AwaitHandshake => match message {
            ClientMessage::Handshake {
                protocol_version,
                max_frame_bytes: client_max_frame_bytes,
                capabilities,
            } => {
                if protocol_version != PROTOCOL_VERSION {
                    return ServerResponse::Message(protocol_error(format!(
                        "unsupported protocol version {protocol_version}; expected {PROTOCOL_VERSION}"
                    )));
                }
                if client_max_frame_bytes == 0 {
                    return ServerResponse::Message(protocol_error(
                        "client max_frame_bytes must be greater than zero",
                    ));
                }
                let negotiated_max_frame_bytes = client_max_frame_bytes
                    .min(config.max_frame_bytes)
                    .min(DEFAULT_MAX_FRAME_BYTES);
                let column_batch_v1 = capabilities.contains(&ProtocolCapability::ColumnBatchV1);
                let build_identity_v1 = capabilities.contains(&ProtocolCapability::BuildIdentityV1);
                let external_value_v1 = capabilities.contains(&ProtocolCapability::ExternalValueV1);
                let mut accepted_capabilities = Vec::with_capacity(3);
                if column_batch_v1 {
                    accepted_capabilities.push(ProtocolCapability::ColumnBatchV1);
                }
                if build_identity_v1 {
                    accepted_capabilities.push(ProtocolCapability::BuildIdentityV1);
                }
                if external_value_v1 {
                    accepted_capabilities.push(ProtocolCapability::ExternalValueV1);
                }
                let accepted = ServerMessage::HandshakeAccepted {
                    protocol_version: PROTOCOL_VERSION,
                    max_frame_bytes: negotiated_max_frame_bytes,
                    capabilities: accepted_capabilities,
                };
                if encoded_payload_len(&accepted)
                    .map_or(true, |size| size > negotiated_max_frame_bytes as usize)
                {
                    return ServerResponse::Message(protocol_error(format!(
                        "client max_frame_bytes {client_max_frame_bytes} cannot carry the handshake response"
                    )));
                }
                *state = SessionState::AwaitAuthentication {
                    column_batch_v1,
                    build_identity_v1,
                    external_value_v1,
                };
                *max_frame_bytes = negotiated_max_frame_bytes;
                ServerResponse::Message(accepted)
            }
            _ => ServerResponse::Message(protocol_error("first client message must be Handshake")),
        },
        SessionState::AwaitAuthentication {
            column_batch_v1,
            build_identity_v1,
            external_value_v1,
        } => match message {
            ClientMessage::Authenticate { login, password } => {
                match authenticate_client(config, &login, password.as_deref()) {
                    Ok(()) => {
                        *state = SessionState::Ready(ReadySession {
                            principal_id: ObjectId::BOOTSTRAP_OWNER,
                            authenticated_database: None,
                            selected_database_name: None,
                            selected_database: None,
                            cursor: None,
                            transaction: None,
                            next_cursor_id: 1,
                            prepared: BTreeMap::new(),
                            next_statement_id: 1,
                            column_batch_v1: *column_batch_v1,
                            build_identity_v1: *build_identity_v1,
                            external_value_v1: *external_value_v1,
                        });
                        ServerResponse::Message(ServerMessage::AuthenticationAccepted)
                    }
                    Err(message) => ServerResponse::Message(authentication_failed(message)),
                }
            }
            ClientMessage::AuthenticatePrincipal {
                database,
                login,
                password,
            } => match open_database_with_plugin_registry(
                config,
                databases,
                &database,
                Arc::clone(plugin_registry),
            ) {
                Ok(db) => match db.authenticate_principal(&login, &password) {
                    Ok(principal_id) => {
                        *state = SessionState::Ready(ReadySession {
                            principal_id,
                            authenticated_database: Some(database.clone()),
                            selected_database_name: Some(database),
                            selected_database: Some(db),
                            cursor: None,
                            transaction: None,
                            next_cursor_id: 1,
                            prepared: BTreeMap::new(),
                            next_statement_id: 1,
                            column_batch_v1: *column_batch_v1,
                            build_identity_v1: *build_identity_v1,
                            external_value_v1: *external_value_v1,
                        });
                        ServerResponse::Message(ServerMessage::AuthenticationAccepted)
                    }
                    Err(_) => {
                        let _ = release_database_lease(databases, &database);
                        ServerResponse::Message(authentication_failed("authentication failed"))
                    }
                },
                Err(_) => ServerResponse::Message(authentication_failed("authentication failed")),
            },
            _ => ServerResponse::Message(protocol_error(
                "client must authenticate before issuing commands",
            )),
        },
        SessionState::Ready(session) => {
            handle_ready_message(session, message, config, session_runtime, *max_frame_bytes)
        }
    }
}

fn handle_ready_message(
    session: &mut ReadySession,
    message: ClientMessage,
    config: &ServerConfig,
    session_runtime: &SessionRuntimeContext<'_>,
    negotiated_max_frame_bytes: u32,
) -> ServerResponse {
    let SessionRuntimeContext {
        databases,
        plugin_registry,
        runtime,
        ..
    } = session_runtime;
    let effective_config = effective_session_config(config, negotiated_max_frame_bytes);
    let config = &effective_config;
    match message {
        ClientMessage::Handshake { .. }
        | ClientMessage::Authenticate { .. }
        | ClientMessage::AuthenticatePrincipal { .. } => ServerResponse::Message(protocol_error(
            "handshake and authentication are already complete",
        )),
        ClientMessage::ServerStatus { database } => ServerResponse::Message(server_status(
            config,
            databases,
            database.as_deref(),
            session.build_identity_v1,
            runtime,
        )),
        ClientMessage::SelectDatabase { database } => {
            if session
                .authenticated_database
                .as_deref()
                .is_some_and(|authenticated| authenticated != database)
            {
                ServerResponse::Message(authentication_failed(
                    "catalog Principal sessions cannot switch databases; reconnect and authenticate",
                ))
            } else if session.selected_database_name.as_deref() == Some(database.as_str()) {
                // Principal authentication selects the database atomically. Re-selecting that
                // same database is an idempotent protocol operation and must not acquire a
                // second registry lease for the same session.
                ServerResponse::Message(ServerMessage::DatabaseSelected { database })
            } else if session.cursor.is_some() {
                ServerResponse::Message(out_of_sync(
                    "close the active cursor before selecting another database",
                ))
            } else if session.transaction.is_some() {
                ServerResponse::Message(transaction_error(
                    "cannot select database while a transaction is active",
                ))
            } else if session
                .selected_database
                .as_ref()
                .is_some_and(|database| database.has_active_sql_transaction().unwrap_or(true))
            {
                ServerResponse::Message(transaction_error(
                    "cannot select database while a SQL transaction is active",
                ))
            } else {
                ServerResponse::Message(
                    match open_database_with_plugin_registry(
                        config,
                        databases,
                        &database,
                        Arc::clone(plugin_registry),
                    ) {
                        Ok(db) => {
                            if let Some(previous) = session.selected_database_name.as_deref() {
                                if let Err(message) = release_database_lease(databases, previous) {
                                    let _ = release_database_lease(databases, &database);
                                    return ServerResponse::Message(protocol_error(message));
                                }
                            }
                            if session.selected_database_name.as_deref() != Some(database.as_str())
                            {
                                session.prepared.clear();
                            }
                            session.selected_database_name = Some(database.clone());
                            session.selected_database = Some(db);
                            ServerMessage::DatabaseSelected { database }
                        }
                        Err(error) => ServerMessage::Error(ProtocolFailure {
                            code: error.protocol_code(),
                            message: error.to_string(),
                        }),
                    },
                )
            }
        }
        ClientMessage::Execute {
            request_id,
            sql,
            positional,
            named,
        } => {
            if session.cursor.is_some() {
                ServerResponse::Message(out_of_sync(
                    "close or fetch the active cursor before executing another query",
                ))
            } else {
                ServerResponse::Message(execute_sql(
                    session,
                    request_id,
                    &sql,
                    WireBindings { positional, named },
                    config,
                    session_runtime,
                ))
            }
        }
        ClientMessage::Prepare { sql } => ServerResponse::Message(prepare_statement(session, &sql)),
        ClientMessage::ExecutePrepared {
            request_id,
            statement_id,
            positional,
            named,
        } => {
            if session.cursor.is_some() {
                ServerResponse::Message(out_of_sync(
                    "close or fetch the active cursor before executing another query",
                ))
            } else {
                ServerResponse::Message(execute_prepared_statement(
                    session,
                    request_id,
                    statement_id,
                    WireBindings { positional, named },
                    config,
                    session_runtime,
                ))
            }
        }
        ClientMessage::ClosePrepared { statement_id } => {
            ServerResponse::Message(if session.prepared.remove(&statement_id).is_some() {
                ServerMessage::PreparedClosed { statement_id }
            } else {
                protocol_error(format!("prepared statement {statement_id} not found"))
            })
        }
        ClientMessage::CancelExecution { request_id } => {
            ServerResponse::Message(match runtime.cancel_execution(request_id) {
                Ok(found) => ServerMessage::ExecutionCancelled { request_id, found },
                Err(message) => protocol_error(message),
            })
        }
        ClientMessage::Fetch { cursor_id } => fetch_cursor(session, cursor_id, config),
        ClientMessage::FetchColumnBatch { cursor_id } => {
            if !session.column_batch_v1 {
                ServerResponse::Message(protocol_error(
                    "ColumnBatchV1 was not negotiated during handshake",
                ))
            } else {
                fetch_column_batch(session, cursor_id, config)
            }
        }
        ClientMessage::CloseCursor { cursor_id } | ClientMessage::Cancel { cursor_id } => {
            ServerResponse::Message(close_cursor(session, cursor_id))
        }
        ClientMessage::BeginTransaction { isolation } => {
            ServerResponse::Message(begin_transaction(session, isolation))
        }
        ClientMessage::CommitTransaction => ServerResponse::Message(commit_transaction(session)),
        ClientMessage::RollbackTransaction => {
            ServerResponse::Message(rollback_transaction(session))
        }
        ClientMessage::CreateSavepoint { name } => {
            ServerResponse::Message(savepoint(session, name, 0))
        }
        ClientMessage::RollbackToSavepoint { name } => {
            ServerResponse::Message(savepoint(session, name, 1))
        }
        ClientMessage::ReleaseSavepoint { name } => {
            ServerResponse::Message(savepoint(session, name, 2))
        }
        ClientMessage::CloseDatabase { database } => {
            ServerResponse::Message(close_database(session, databases, database))
        }
    }
}

fn effective_session_config(
    config: &ServerConfig,
    negotiated_max_frame_bytes: u32,
) -> ServerConfig {
    let mut effective = config.clone();
    effective.max_frame_bytes = negotiated_max_frame_bytes.min(config.max_frame_bytes);
    effective.cursor_batch_max_bytes = config
        .cursor_batch_max_bytes
        .min(effective.max_frame_bytes as usize);
    effective
}

fn authenticate_client(
    config: &ServerConfig,
    login: &str,
    password: Option<&str>,
) -> Result<(), String> {
    if login != "root" {
        return Err(format!(
            "unsupported login `{login}`; use a database principal or configured root authentication"
        ));
    }

    if config.root_password_is_configured() {
        return match password {
            Some(password) if config.root_password_matches(password) => Ok(()),
            _ => Err("root authentication failed".to_string()),
        };
    }

    if password.is_none() && config.root_passwordless_is_permitted() {
        return Ok(());
    }

    if password.is_none() {
        return Err(
            "passwordless root authentication is permitted only on a loopback plaintext endpoint"
                .to_string(),
        );
    }

    if password.is_some() {
        return Err(
            "password authentication is not configured; only passwordless root on loopback is supported"
                .to_string(),
        );
    }

    Err("root authentication failed".to_string())
}

#[cfg(test)]
pub(super) fn open_database(
    config: &ServerConfig,
    databases: &Mutex<BTreeMap<String, DatabaseRegistryEntry>>,
    name: &str,
) -> Result<Database, OpenDatabaseError> {
    open_database_with_plugin_registry(config, databases, name, Arc::clone(&EMPTY_PLUGIN_REGISTRY))
}

pub(super) fn open_database_with_plugin_registry(
    config: &ServerConfig,
    databases: &Mutex<BTreeMap<String, DatabaseRegistryEntry>>,
    name: &str,
    plugin_registry: Arc<PluginRegistry>,
) -> Result<Database, OpenDatabaseError> {
    validate_database_name(name, config.max_database_name_bytes)
        .map_err(OpenDatabaseError::InvalidName)?;
    let database_dir = config.data_dir.join("databases").join(name);

    {
        let mut guard = databases
            .lock()
            .map_err(|_| OpenDatabaseError::Server("database registry is poisoned".to_string()))?;
        match guard.get_mut(name) {
            Some(DatabaseRegistryEntry::Ready {
                database, sessions, ..
            }) => {
                *sessions = sessions.checked_add(1).ok_or_else(|| {
                    OpenDatabaseError::Server("database session count exhausted".to_string())
                })?;
                return Ok(database.clone());
            }
            Some(DatabaseRegistryEntry::Opening { .. }) => {
                return Err(OpenDatabaseError::Opening(format!(
                    "database `{name}` is opening/recovering; retry later"
                )));
            }
            Some(DatabaseRegistryEntry::Failed {
                error, retry_after, ..
            }) if Instant::now() < *retry_after => {
                return Err(OpenDatabaseError::Startup(error.clone()));
            }
            Some(DatabaseRegistryEntry::Failed { .. }) => {
                // A previous startup failure is diagnostic state, not a
                // permanent negative cache. The Opening marker below provides
                // bounded single-owner retry after an operator repairs disk.
                guard.remove(name);
                guard.insert(
                    name.to_string(),
                    DatabaseRegistryEntry::Opening {
                        artifacts: unavailable_artifact_summary(),
                    },
                );
            }
            None => {
                if guard.len() >= config.max_databases {
                    return Err(OpenDatabaseError::Server(format!(
                        "database limit ({}) reached; close an idle database before opening another",
                        config.max_databases
                    )));
                }
                guard.insert(
                    name.to_string(),
                    DatabaseRegistryEntry::Opening {
                        artifacts: unavailable_artifact_summary(),
                    },
                );
            }
        }
    }

    if let Err(error) = std::fs::create_dir_all(&database_dir) {
        let error = DatabaseError::io(format!(
            "failed to create database directory {}: {error}",
            database_dir.display()
        ));
        let artifacts = collect_database_artifacts(&database_dir);
        record_failed_database(databases, name, error.clone(), artifacts);
        return Err(OpenDatabaseError::Startup(error));
    }
    let dsn = format!(
        "file://{}?copy_max_transaction_bytes={}&max_compaction_jobs={}&storage_cpu_workers={}&page_cache_level={}&page_cache_max_bytes={}&page_cache_memory_reserve={}&target_volume_rows={}&seal_hot_bytes_threshold={}&seal_incremental_hot_bytes_threshold={}&read_queue_depth={}",
        database_dir.display(),
        config.copy_max_transaction_bytes,
        config.max_compaction_jobs,
        config.storage_cpu_workers,
        config.page_cache_level,
        config.page_cache_max_bytes,
        config.page_cache_memory_reserve,
        config.target_volume_rows,
        config.seal_hot_bytes_threshold,
        config.seal_incremental_hot_bytes_threshold,
        config.read_queue_depth
    );
    let started = Instant::now();
    let artifacts = collect_database_artifacts(&database_dir);
    update_opening_artifacts(databases, name, artifacts.clone());
    eprintln!(
        "radixdb-server: database `{name}` state=opening path={} phase=database_open table_dirs={} wal_files={} artifact_files={} snapshot_files={} control_files={} metadata_files={} other_files={}",
        database_dir.display(),
        artifacts.table_dirs,
        artifacts.wal_files,
        artifacts.artifact_files,
        artifacts.snapshot_files,
        artifacts.checkpoint_files,
        artifacts.manifest_files,
        artifacts.other_files
    );
    let database = match Database::open_with_plugin_registry(&dsn, plugin_registry) {
        Ok(database) => database,
        Err(error) => {
            record_failed_database(databases, name, error.clone(), artifacts.clone());
            eprintln!(
                "radixdb-server: database `{name}` state=failed phase=database_open elapsed_ms={:.3} table_dirs={} wal_files={} artifact_files={} snapshot_files={} control_files={} metadata_files={} other_files={} error={error}",
                started.elapsed().as_secs_f64() * 1000.0,
                artifacts.table_dirs,
                artifacts.wal_files,
                artifacts.artifact_files,
                artifacts.snapshot_files,
                artifacts.checkpoint_files,
                artifacts.manifest_files,
                artifacts.other_files
            );
            return Err(OpenDatabaseError::Startup(error));
        }
    };
    match database.runtime_state() {
        DatabaseRuntimeState::Ready => {}
        DatabaseRuntimeState::CloseFailed(error) => {
            record_failed_database(databases, name, error.clone(), artifacts.clone());
            return Err(OpenDatabaseError::Startup(error));
        }
        DatabaseRuntimeState::Failed(error) => {
            record_failed_database(databases, name, error.clone(), artifacts.clone());
            return Err(OpenDatabaseError::Startup(error));
        }
        state => {
            let error = DatabaseError::internal(format!(
                "database `{name}` returned from open in non-ready engine state: {state:?}"
            ));
            record_failed_database(databases, name, error.clone(), artifacts.clone());
            return Err(OpenDatabaseError::Startup(error));
        }
    }
    let session_database = database.clone();
    let ready_artifacts = collect_database_artifacts(&database_dir);
    let mut guard = databases
        .lock()
        .map_err(|_| OpenDatabaseError::Server("database registry is poisoned".to_string()))?;
    guard.insert(
        name.to_string(),
        DatabaseRegistryEntry::Ready {
            database,
            artifacts: ready_artifacts.clone(),
            sessions: 1,
        },
    );
    drop(guard);
    eprintln!(
        "radixdb-server: database `{name}` state=ready phase=database_open elapsed_ms={:.3} table_dirs={} wal_files={} artifact_files={} snapshot_files={} control_files={} metadata_files={} other_files={}",
        started.elapsed().as_secs_f64() * 1000.0,
        ready_artifacts.table_dirs,
        ready_artifacts.wal_files,
        ready_artifacts.artifact_files,
        ready_artifacts.snapshot_files,
        ready_artifacts.checkpoint_files,
        ready_artifacts.manifest_files,
        ready_artifacts.other_files
    );
    Ok(session_database)
}

fn record_failed_database(
    databases: &Mutex<BTreeMap<String, DatabaseRegistryEntry>>,
    name: &str,
    error: DatabaseError,
    artifacts: DatabaseArtifactSummary,
) {
    if let Ok(mut guard) = databases.lock() {
        if matches!(guard.get(name), Some(DatabaseRegistryEntry::Opening { .. })) {
            guard.insert(
                name.to_string(),
                DatabaseRegistryEntry::Failed {
                    error,
                    artifacts,
                    retry_after: Instant::now() + std::time::Duration::from_millis(100),
                },
            );
        }
    }
}

fn update_opening_artifacts(
    databases: &Mutex<BTreeMap<String, DatabaseRegistryEntry>>,
    name: &str,
    artifacts: DatabaseArtifactSummary,
) {
    if let Ok(mut guard) = databases.lock() {
        if matches!(guard.get(name), Some(DatabaseRegistryEntry::Opening { .. })) {
            guard.insert(
                name.to_string(),
                DatabaseRegistryEntry::Opening { artifacts },
            );
        }
    }
}

fn validate_database_name(name: &str, max_bytes: usize) -> Result<(), String> {
    if name.is_empty() {
        return Err("database name must not be empty".to_string());
    }
    if name.len() > max_bytes {
        return Err(format!(
            "database name is {} bytes; maximum is {max_bytes}",
            name.len()
        ));
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(format!(
            "database name `{name}` contains unsupported characters"
        ));
    }
    Ok(())
}

pub(super) fn release_database_lease(
    databases: &Mutex<BTreeMap<String, DatabaseRegistryEntry>>,
    name: &str,
) -> Result<(), String> {
    let mut guard = databases
        .lock()
        .map_err(|_| "database registry is poisoned".to_string())?;
    let Some(DatabaseRegistryEntry::Ready { sessions, .. }) = guard.get_mut(name) else {
        return Err(format!(
            "selected database `{name}` is missing from the registry"
        ));
    };
    *sessions = sessions
        .checked_sub(1)
        .ok_or_else(|| format!("database `{name}` session count underflow"))?;
    Ok(())
}

fn release_selected_database(
    session: &mut ReadySession,
    databases: &Mutex<BTreeMap<String, DatabaseRegistryEntry>>,
) {
    if let Some(name) = session.selected_database_name.take() {
        if let Err(error) = release_database_lease(databases, &name) {
            eprintln!("radixdb-server: failed to release database `{name}` lease: {error}");
        }
    }
    session.selected_database = None;
    session.prepared.clear();
}

fn begin_transaction(session: &mut ReadySession, isolation: TransactionIsolation) -> ServerMessage {
    let Some(database) = &session.selected_database else {
        return database_required();
    };
    if session.cursor.is_some() {
        return out_of_sync("close the active cursor before beginning a transaction");
    }
    if session.transaction.is_some() {
        return transaction_error("a transaction is already active");
    }
    match database.has_active_sql_transaction() {
        Ok(true) => return transaction_error("a SQL transaction is already active"),
        Ok(false) => {}
        Err(error) => return database_error(error),
    }
    let isolation = match isolation {
        TransactionIsolation::ReadCommitted => IsolationLevel::ReadCommitted,
        TransactionIsolation::Snapshot => IsolationLevel::SnapshotIsolation,
    };
    match database.begin_with_isolation(isolation) {
        Ok(transaction) => {
            session.transaction = Some(transaction);
            #[cfg(any(test, feature = "test-failpoints"))]
            crate::test_failpoints::interleave(
                crate::test_failpoints::InterleavePoint::ActiveTransactionBegan,
                0,
            );
            ServerMessage::TransactionBegan
        }
        Err(error) => database_error(error),
    }
}

fn commit_transaction(session: &mut ReadySession) -> ServerMessage {
    if session.cursor.is_some() {
        return out_of_sync("close the active cursor before committing");
    }
    let Some(mut transaction) = session.transaction.take() else {
        return transaction_error("no active transaction to commit");
    };
    match transaction.commit() {
        Ok(()) => ServerMessage::TransactionCommitted,
        Err(error) => {
            let active = transaction.is_active();
            if active {
                session.transaction = Some(transaction);
            }
            ServerMessage::TransactionFailed {
                failure: database_failure(&error),
                active,
            }
        }
    }
}

fn rollback_transaction(session: &mut ReadySession) -> ServerMessage {
    if session.cursor.is_some() {
        return out_of_sync("close the active cursor before rolling back");
    }
    let Some(mut transaction) = session.transaction.take() else {
        return transaction_error("no active transaction to roll back");
    };
    match transaction.rollback() {
        Ok(()) => ServerMessage::TransactionRolledBack,
        Err(error) => ServerMessage::TransactionFailed {
            failure: database_failure(&error),
            active: false,
        },
    }
}

fn savepoint(session: &mut ReadySession, name: String, operation: u8) -> ServerMessage {
    if session.cursor.is_some() {
        return out_of_sync("close the active cursor before a savepoint operation");
    }
    if name.is_empty() || name.len() > 128 {
        return protocol_error("savepoint name must contain 1..=128 bytes");
    }
    let Some(transaction) = session.transaction.as_mut() else {
        return transaction_error("no transaction is active");
    };
    let result = match operation {
        0 => transaction.savepoint(&name),
        1 => transaction.rollback_to_savepoint(&name),
        _ => transaction.release_savepoint(&name),
    };
    match result {
        Ok(()) if operation == 0 => ServerMessage::SavepointCreated { name },
        Ok(()) if operation == 1 => ServerMessage::SavepointRolledBack { name },
        Ok(()) => ServerMessage::SavepointReleased { name },
        Err(error) => database_error(error),
    }
}

fn close_database(
    session: &mut ReadySession,
    databases: &Mutex<BTreeMap<String, DatabaseRegistryEntry>>,
    name: String,
) -> ServerMessage {
    if session.cursor.is_some() {
        return out_of_sync("close the active cursor before closing a database");
    }
    if session.transaction.is_some() {
        return transaction_error("finish the active transaction before closing a database");
    }
    let owns_lease = session.selected_database_name.as_deref() == Some(name.as_str());
    let entry = match databases.lock() {
        Ok(mut guard) => {
            match guard.get(&name) {
                Some(DatabaseRegistryEntry::Opening { .. }) => {
                    return out_of_sync(format!("database `{name}` is still opening"));
                }
                Some(DatabaseRegistryEntry::Ready { sessions, .. })
                    if *sessions > usize::from(owns_lease) =>
                {
                    return out_of_sync(format!(
                        "database `{name}` is selected by another live session"
                    ));
                }
                _ => {}
            }
            let entry = guard.remove(&name);
            if let Some(DatabaseRegistryEntry::Ready { artifacts, .. }) = &entry {
                // Keep a registry barrier while the engine owner drains and
                // closes.  Without it, the stock Job scheduler can discover
                // the directory between `remove` and `Database::close`, race
                // a foreground reopen, and poison the shared retry state with
                // the transient internal `Closing` lifecycle state.
                guard.insert(
                    name.clone(),
                    DatabaseRegistryEntry::Opening {
                        artifacts: artifacts.clone(),
                    },
                );
            }
            entry
        }
        Err(_) => return protocol_error("database registry is poisoned"),
    };
    if owns_lease {
        session.selected_database = None;
        session.selected_database_name = None;
        session.prepared.clear();
    }
    if let Some(DatabaseRegistryEntry::Ready { database, .. }) = entry {
        if let Err(error) = database.close() {
            record_failed_database(
                databases,
                &name,
                error.clone(),
                unavailable_artifact_summary(),
            );
            return database_error(error);
        }
        if let Ok(mut guard) = databases.lock() {
            if matches!(
                guard.get(&name),
                Some(DatabaseRegistryEntry::Opening { .. })
            ) {
                guard.remove(&name);
            }
        } else {
            return protocol_error("database registry is poisoned");
        }
    }
    ServerMessage::DatabaseClosed { database: name }
}

fn execute_sql(
    session: &mut ReadySession,
    request_id: u64,
    sql: &str,
    bindings: WireBindings,
    config: &ServerConfig,
    session_runtime: &SessionRuntimeContext<'_>,
) -> ServerMessage {
    let SessionRuntimeContext {
        plugin_registry,
        cancellation,
        runtime,
        ..
    } = session_runtime;
    let WireBindings { positional, named } = bindings;
    if !session.external_value_v1
        && positional
            .iter()
            .chain(named.values())
            .any(|value| matches!(value, WireValue::External { .. }))
    {
        return unsupported_type(
            "external parameters require negotiated ExternalValueV1 capability",
        );
    }
    if sql_contains_transaction_control(sql) {
        return transaction_error("transaction control SQL is not accepted by generic Execute; use the dedicated transaction/savepoint messages");
    }
    if !positional.is_empty() && !named.is_empty() {
        return protocol_error("positional and named parameters cannot be mixed in one execution");
    }
    let mut context = match wire_execution_context(positional, named, plugin_registry) {
        Ok(context) => context,
        Err(message) => return protocol_error(message),
    };
    if let Err(message) = context.bind_request_identity(session.principal_id, request_id) {
        return protocol_error(message.to_string());
    }
    context.bind_parent_cancellation(cancellation);
    let execution_cancellation = context.cancellation();
    let _registration = match runtime.register_execution(request_id, execution_cancellation) {
        Ok(registration) => registration,
        Err(message) => return protocol_error(message),
    };
    #[cfg(test)]
    {
        let hook = EXECUTE_SQL_TEST_HOOK
            .lock()
            .expect("execute SQL test hook lock")
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    // Bind cursor metadata before execution from the same schema/type graph
    // used by CTAS. Unsupported expressions remain explicitly UNKNOWN rather
    // than making every result look unknown and nullable.
    let described = if let Some(transaction) = session.transaction.as_ref() {
        transaction.describe_query_output(sql).ok().flatten()
    } else {
        session
            .selected_database
            .as_ref()
            .and_then(|database| database.describe_query_output(sql).ok().flatten())
    };

    let result = if let Some(transaction) = session.transaction.as_mut() {
        transaction.query_for_server(sql, context)
    } else {
        let Some(database) = &session.selected_database else {
            return database_required();
        };
        database.query_for_server(sql, &context)
    };

    let rows = match result {
        Ok(rows) => rows,
        Err(error) => return database_error(error),
    };

    rows_to_response(session, rows, described, config)
}

fn rows_to_response(
    session: &mut ReadySession,
    rows: Rows,
    described: Option<Vec<crate::api::QueryOutputColumn>>,
    config: &ServerConfig,
) -> ServerMessage {
    if !session.external_value_v1
        && described.as_ref().is_some_and(|columns| {
            columns.iter().any(|column| {
                matches!(
                    column.logical_type,
                    radixdb_core::LogicalTypeRef::External(_)
                )
            })
        })
    {
        return unsupported_type(
            "external result columns require negotiated ExternalValueV1 capability",
        );
    }
    let columns = rows
        .columns()
        .iter()
        .enumerate()
        .map(|(index, name)| {
            described
                .as_ref()
                .filter(|columns| columns.len() == rows.column_count())
                .and_then(|columns| columns.get(index))
                .map_or_else(
                    || Column {
                        name: name.clone(),
                        type_name: "UNKNOWN".to_string(),
                        nullable: true,
                        external_type: None,
                    },
                    |described| Column {
                        name: name.clone(),
                        type_name: described.type_name.clone(),
                        nullable: described.nullable,
                        external_type: match described.logical_type {
                            radixdb_core::LogicalTypeRef::Builtin(_) => None,
                            radixdb_core::LogicalTypeRef::External(type_ref) => {
                                Some(crate::protocol::ExternalTypeRef {
                                    type_object_id: type_ref.type_object_id(),
                                    codec_version: type_ref.codec_version(),
                                })
                            }
                        },
                    },
                )
        })
        .collect::<Vec<_>>();

    if columns.is_empty() {
        return ServerMessage::CommandComplete {
            affected_rows: rows.rows_affected().max(0) as u64,
            last_insert_id: rows.last_insert_id().max(0) as u64,
        };
    }

    let cursor_id = session.next_cursor_id;
    session.next_cursor_id = match session.next_cursor_id.checked_add(1) {
        Some(next) => next,
        None => {
            return protocol_error("cursor id space exhausted; reconnect to create a new session")
        }
    };
    session.cursor = Some(SessionCursor {
        id: cursor_id,
        rows,
        pending_row_batch: None,
        pending_row_columns: None,
        pending_column_batch: None,
        typed_batches_started: false,
        row_typed_batches_started: false,
    });
    #[cfg(any(test, feature = "test-failpoints"))]
    crate::test_failpoints::interleave(
        crate::test_failpoints::InterleavePoint::CursorPublished,
        cursor_id as i64,
    );
    let response = ServerMessage::CursorOpened { cursor_id, columns };
    match encoded_payload_len(&response) {
        Ok(size) if size <= config.max_frame_bytes as usize => response,
        Ok(_) => {
            session.cursor = None;
            protocol_error("cursor metadata exceeds negotiated frame limit")
        }
        Err(error) => {
            session.cursor = None;
            protocol_error(error.to_string())
        }
    }
}

fn wire_execution_context(
    positional: Vec<WireValue>,
    named: BTreeMap<String, WireValue>,
    plugin_registry: &PluginRegistry,
) -> Result<ServerExecutionContext, String> {
    if !positional.is_empty() {
        let values = positional
            .into_iter()
            .map(|value| wire_value_to_radixdb(value, plugin_registry))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ServerExecutionContext::positional(values.into()))
    } else {
        Ok(ServerExecutionContext::named(wire_parameters_to_named(
            named,
            plugin_registry,
        )?))
    }
}

fn prepare_statement(session: &mut ReadySession, sql: &str) -> ServerMessage {
    let Some(database) = session.selected_database.as_ref() else {
        return database_required();
    };
    if sql_contains_transaction_control(sql) {
        return transaction_error(
            "transaction control SQL cannot be prepared; use dedicated protocol messages",
        );
    }
    let statement = match database.prepare(sql) {
        Ok(statement) => statement,
        Err(error) => return database_error(error),
    };
    let statement_id = session.next_statement_id;
    session.next_statement_id = match statement_id.checked_add(1) {
        Some(next) => next,
        None => return protocol_error("prepared statement id space exhausted; reconnect"),
    };
    session.prepared.insert(statement_id, statement);
    ServerMessage::Prepared { statement_id }
}

fn execute_prepared_statement(
    session: &mut ReadySession,
    request_id: u64,
    statement_id: u64,
    bindings: WireBindings,
    config: &ServerConfig,
    session_runtime: &SessionRuntimeContext<'_>,
) -> ServerMessage {
    let SessionRuntimeContext {
        plugin_registry,
        cancellation,
        runtime,
        ..
    } = session_runtime;
    let WireBindings { positional, named } = bindings;
    if !session.external_value_v1
        && positional
            .iter()
            .chain(named.values())
            .any(|value| matches!(value, WireValue::External { .. }))
    {
        return unsupported_type(
            "external parameters require negotiated ExternalValueV1 capability",
        );
    }
    if !positional.is_empty() && !named.is_empty() {
        return protocol_error("positional and named parameters cannot be mixed in one execution");
    }
    let Some(statement) = session.prepared.get(&statement_id).cloned() else {
        return protocol_error(format!("prepared statement {statement_id} not found"));
    };
    let mut context = match wire_execution_context(positional, named, plugin_registry) {
        Ok(context) => context,
        Err(message) => return protocol_error(message),
    };
    if let Err(message) = context.bind_request_identity(session.principal_id, request_id) {
        return protocol_error(message.to_string());
    }
    context.bind_parent_cancellation(cancellation);
    let execution_cancellation = context.cancellation();
    let _registration = match runtime.register_execution(request_id, execution_cancellation) {
        Ok(registration) => registration,
        Err(message) => return protocol_error(message),
    };
    let described = session.selected_database.as_ref().and_then(|database| {
        database
            .describe_query_output(statement.sql())
            .ok()
            .flatten()
    });
    let result = if let Some(transaction) = session.transaction.as_mut() {
        transaction.query_prepared_for_server(&statement, context)
    } else {
        statement.query_for_server(context)
    };
    match result {
        Ok(rows) => rows_to_response(session, rows, described, config),
        Err(error) => database_error(error),
    }
}

fn fetch_cursor(
    session: &mut ReadySession,
    cursor_id: u64,
    config: &ServerConfig,
) -> ServerResponse {
    let Some(cursor) = session.cursor.as_mut() else {
        return ServerResponse::Message(cursor_not_found(cursor_id));
    };
    if cursor.id != cursor_id {
        return ServerResponse::Message(cursor_not_found(cursor_id));
    }
    if cursor.typed_batches_started {
        return ServerResponse::Message(out_of_sync(
            "continue a typed cursor with FetchColumnBatch, or close/cancel it before Fetch",
        ));
    }

    if let Some(pending) = cursor.pending_row_batch.take() {
        return emit_row_batch(session, cursor_id, pending.rows, pending.eof, config);
    }
    if cursor.pending_row_columns.is_some() {
        return fetch_pending_row_columns(session, cursor_id, config);
    }
    if cursor.rows.supports_server_column_batches() {
        let typed = match cursor.rows.next_server_column_batch() {
            Ok(Some(batch)) => batch,
            Ok(None) => {
                return emit_row_batch(session, cursor_id, Vec::new(), true, config);
            }
            Err(error) => {
                session.cursor = None;
                return ServerResponse::Message(cursor_failed(
                    cursor_id,
                    ProtocolErrorCode::SqlError,
                    error.to_string(),
                ));
            }
        };
        let pending = match pending_row_columns_from_typed(typed) {
            Ok(pending) => pending,
            Err(message) => {
                session.cursor = None;
                return ServerResponse::Message(cursor_failed(
                    cursor_id,
                    ProtocolErrorCode::SqlError,
                    message,
                ));
            }
        };
        let cursor = session
            .cursor
            .as_mut()
            .expect("row cursor was checked above");
        cursor.row_typed_batches_started = true;
        cursor.pending_row_columns = Some(Box::new(pending));
        return fetch_pending_row_columns(session, cursor_id, config);
    }

    let cursor = session
        .cursor
        .as_mut()
        .expect("row cursor was checked above");
    let mut batch = Vec::with_capacity(config.cursor_batch_max_rows);
    let mut eof = false;
    loop {
        if batch.len() >= config.cursor_batch_max_rows {
            break;
        }
        let next_row = cursor.rows.next().map(|result| {
            result.map_err(|error| error.to_string()).and_then(|row| {
                let values = row.into_inner();
                values
                    .into_iter()
                    .map(|value| radixdb_value_to_wire(&value))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|values| Row { values })
            })
        });
        let Some(next_row) = next_row else {
            eof = true;
            break;
        };
        let row = match next_row {
            Ok(row) => row,
            Err(message) => {
                session.cursor = None;
                return ServerResponse::Message(cursor_failed(
                    cursor_id,
                    ProtocolErrorCode::SqlError,
                    message,
                ));
            }
        };
        ServerRuntimeMetrics::protocol_row_adapter(row.values.len() as u64);
        batch.push(row);
    }
    emit_row_batch(session, cursor_id, batch, eof, config)
}

fn emit_row_batch(
    session: &mut ReadySession,
    cursor_id: u64,
    batch: Vec<Row>,
    source_eof: bool,
    config: &ServerConfig,
) -> ServerResponse {
    match prepare_row_batch(cursor_id, batch, source_eof, config) {
        Ok(prepared) => {
            let emitted_rows = prepared.emitted_rows;
            if let Some(pending) = prepared.pending {
                session
                    .cursor
                    .as_mut()
                    .expect("row cursor remains active while a tail is pending")
                    .pending_row_batch = Some(pending);
            } else if prepared.eof {
                session.cursor = None;
            }
            ServerRuntimeMetrics::protocol_result_rows(emitted_rows as u64);
            ServerRuntimeMetrics::protocol_row_batch(emitted_rows as u64);
            ServerResponse::Encoded {
                payload: prepared.payload,
            }
        }
        Err(message) => {
            session.cursor = None;
            ServerResponse::Message(cursor_failed(
                cursor_id,
                ProtocolErrorCode::ProtocolViolation,
                message,
            ))
        }
    }
}

fn fetch_pending_row_columns(
    session: &mut ReadySession,
    cursor_id: u64,
    config: &ServerConfig,
) -> ServerResponse {
    let prepared = (|| -> Result<(Vec<Row>, bool), String> {
        let cursor = session
            .cursor
            .as_mut()
            .expect("pending typed row columns require an active cursor");
        let pending = cursor
            .pending_row_columns
            .as_mut()
            .expect("pending typed row columns were checked above");
        let end = pending
            .next_row
            .saturating_add(config.cursor_batch_max_rows)
            .min(pending.row_count);
        let mut batch = Vec::with_capacity(end.saturating_sub(pending.next_row));
        for row_idx in pending.next_row..end {
            let values = pending
                .columns
                .iter()
                .map(|column| wire_value_from_column(column, row_idx))
                .collect::<Result<Vec<_>, _>>()?;
            batch.push(Row { values });
        }
        pending.next_row = end;
        Ok((batch, pending.is_complete()))
    })();
    let (batch, complete) = match prepared {
        Ok(prepared) => prepared,
        Err(message) => {
            session.cursor = None;
            return ServerResponse::Message(cursor_failed(
                cursor_id,
                ProtocolErrorCode::ProtocolViolation,
                message,
            ));
        }
    };
    let mut source_eof = false;
    if complete {
        let cursor = session
            .cursor
            .as_mut()
            .expect("row cursor remains active after a typed group");
        cursor.pending_row_columns = None;
        loop {
            match cursor.rows.next_server_column_batch() {
                Ok(Some(next)) if next.row_count() == 0 => continue,
                Ok(Some(next)) => match pending_row_columns_from_typed(next) {
                    Ok(pending) => cursor.pending_row_columns = Some(Box::new(pending)),
                    Err(message) => {
                        session.cursor = None;
                        return ServerResponse::Message(cursor_failed(
                            cursor_id,
                            ProtocolErrorCode::SqlError,
                            message,
                        ));
                    }
                },
                Ok(None) => source_eof = true,
                Err(error) => {
                    session.cursor = None;
                    return ServerResponse::Message(cursor_failed(
                        cursor_id,
                        ProtocolErrorCode::SqlError,
                        error.to_string(),
                    ));
                }
            }
            break;
        }
    }
    emit_row_batch(session, cursor_id, batch, source_eof, config)
}

fn pending_row_columns_from_typed(batch: ServerColumnBatch) -> Result<PendingRowColumns, String> {
    let row_count = batch.row_count();
    let columns = batch
        .into_columns()
        .into_iter()
        .map(column_data_to_wire_column)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PendingRowColumns::new(columns, row_count))
}

fn wire_value_from_column(column: &WireColumn, row_idx: usize) -> Result<WireValue, String> {
    match column {
        WireColumn::Int64 { values, nulls } => {
            if nulls[row_idx] {
                Ok(WireValue::Null)
            } else {
                Ok(WireValue::Int(values[row_idx]))
            }
        }
        WireColumn::Float64 { values, nulls } => {
            if nulls[row_idx] {
                Ok(WireValue::Null)
            } else {
                Ok(WireValue::Float64(values[row_idx]))
            }
        }
        WireColumn::Boolean { values, nulls } => {
            if nulls[row_idx] {
                Ok(WireValue::Null)
            } else {
                Ok(WireValue::Bool(values[row_idx]))
            }
        }
        WireColumn::TimestampNanos { values, nulls } => {
            if nulls[row_idx] {
                Ok(WireValue::Null)
            } else {
                Ok(WireValue::TimestampNanos {
                    nanos_since_unix_epoch_utc: values[row_idx],
                })
            }
        }
        WireColumn::DictionaryText {
            ids,
            dictionary,
            nulls,
        } => {
            if nulls[row_idx] {
                return Ok(WireValue::Null);
            }
            dictionary
                .get(ids[row_idx] as usize)
                .cloned()
                .map(WireValue::String)
                .ok_or_else(|| "typed dictionary row contains an invalid dictionary id".to_string())
        }
        WireColumn::Bytes {
            data,
            offsets,
            nulls,
        } => {
            if nulls[row_idx] {
                Ok(WireValue::Null)
            } else {
                wire_column_bytes_at(data, offsets, row_idx)
                    .map(|bytes| WireValue::Bytes(bytes.to_vec()))
            }
        }
        WireColumn::JsonText {
            data,
            offsets,
            nulls,
        } => {
            if nulls[row_idx] {
                Ok(WireValue::Null)
            } else {
                let bytes = wire_column_bytes_at(data, offsets, row_idx)?;
                std::str::from_utf8(bytes)
                    .map(str::to_owned)
                    .map(WireValue::Json)
                    .map_err(|error| error.to_string())
            }
        }
        WireColumn::External {
            type_object_id,
            codec_version,
            data,
            offsets,
            nulls,
        } => {
            if nulls[row_idx] {
                Ok(WireValue::Null)
            } else {
                let start = offsets[row_idx] as usize;
                let end = offsets[row_idx + 1] as usize;
                Ok(WireValue::External {
                    type_object_id: *type_object_id,
                    codec_version: *codec_version,
                    payload: data[start..end].to_vec(),
                })
            }
        }
    }
}

fn wire_column_bytes_at<'a>(
    data: &'a [u8],
    offsets: &[(u64, u64)],
    row_idx: usize,
) -> Result<&'a [u8], String> {
    let &(offset, len) = offsets
        .get(row_idx)
        .ok_or_else(|| "typed byte column row is outside its offsets".to_string())?;
    let offset = usize::try_from(offset).map_err(|_| "typed byte offset exceeds usize")?;
    let len = usize::try_from(len).map_err(|_| "typed byte length exceeds usize")?;
    let end = offset
        .checked_add(len)
        .ok_or_else(|| "typed byte range overflows usize".to_string())?;
    data.get(offset..end)
        .ok_or_else(|| "typed byte range exceeds its payload".to_string())
}

/// Fetch an eligible immutable artifact-backed scan without creating one storage `Row` and
/// one protocol `WireValue` per cell. All non-trivial cursor shapes fall back
/// to the established row protocol, so this path cannot weaken visibility,
/// filtering or schema-evolution semantics.
fn fetch_column_batch(
    session: &mut ReadySession,
    cursor_id: u64,
    config: &ServerConfig,
) -> ServerResponse {
    let Some(cursor) = session.cursor.as_ref() else {
        return ServerResponse::Message(cursor_not_found(cursor_id));
    };
    if cursor.id != cursor_id {
        return ServerResponse::Message(cursor_not_found(cursor_id));
    }
    if cursor.row_typed_batches_started {
        return ServerResponse::Message(out_of_sync(
            "continue a row cursor with Fetch, or close/cancel it before FetchColumnBatch",
        ));
    }
    if cursor.pending_column_batch.is_some() {
        return fetch_pending_column_batch(session, cursor_id, config);
    }
    if !cursor.rows.supports_server_column_batches() {
        let reason = cursor
            .rows
            .server_column_batch_fallback()
            .unwrap_or(ServerBatchFallback::QueryShape);
        ServerRuntimeMetrics::protocol_column_batch_fallback(reason);
        return fetch_cursor(session, cursor_id, config);
    }

    let batch = match session
        .cursor
        .as_mut()
        .expect("cursor was checked above")
        .rows
        .next_server_column_batch()
    {
        Ok(Some(batch)) => batch,
        Ok(None) => {
            session.cursor = None;
            return encode_column_batch_response(
                ServerMessage::ColumnBatch {
                    cursor_id,
                    columns: Vec::new(),
                    row_count: 0,
                    eof: true,
                },
                config,
            );
        }
        Err(error) => {
            session.cursor = None;
            return ServerResponse::Message(cursor_failed(
                cursor_id,
                ProtocolErrorCode::SqlError,
                error.to_string(),
            ));
        }
    };
    let row_count = match u32::try_from(batch.row_count()) {
        Ok(row_count) => row_count,
        Err(_) => {
            session.cursor = None;
            return ServerResponse::Message(cursor_failed(
                cursor_id,
                ProtocolErrorCode::ProtocolViolation,
                "typed column batch row count exceeds u32",
            ));
        }
    };
    let columns = match batch
        .into_columns()
        .into_iter()
        .map(column_data_to_wire_column)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(columns) => columns,
        Err(message) => {
            session.cursor = None;
            return ServerResponse::Message(cursor_failed(
                cursor_id,
                ProtocolErrorCode::SqlError,
                message,
            ));
        }
    };
    let response = ServerMessage::ColumnBatch {
        cursor_id,
        columns,
        row_count,
        // A artifact-backed group is atomic. The next fetch returns the terminal empty
        // batch when this was the final group, just like a row cursor may need
        // one final fetch to observe EOF at a batch boundary.
        eof: false,
    };
    match encode_column_batch_payload(&response, config) {
        Ok(Some(payload)) => {
            session
                .cursor
                .as_mut()
                .expect("cursor was checked above")
                .typed_batches_started = true;
            ServerRuntimeMetrics::protocol_result_rows(row_count as u64);
            ServerResponse::Encoded { payload }
        }
        Ok(None) => {
            let ServerMessage::ColumnBatch {
                columns, row_count, ..
            } = response
            else {
                unreachable!("constructed a column batch response")
            };
            let mut pending = PendingColumnBatch::new(columns, row_count as usize);
            let (response, emitted_rows) =
                match encode_next_pending_column_batch(&mut pending, cursor_id, config) {
                    Ok(result) => result,
                    Err(message) => {
                        session.cursor = None;
                        return ServerResponse::Message(cursor_failed(
                            cursor_id,
                            ProtocolErrorCode::ProtocolViolation,
                            message,
                        ));
                    }
                };
            let cursor = session.cursor.as_mut().expect("cursor was checked above");
            cursor.typed_batches_started = true;
            if !pending.is_complete() {
                cursor.pending_column_batch = Some(pending);
            }
            ServerRuntimeMetrics::protocol_result_rows(emitted_rows as u64);
            response
        }
        Err(message) => {
            session.cursor = None;
            ServerResponse::Message(cursor_failed(
                cursor_id,
                ProtocolErrorCode::ProtocolViolation,
                message,
            ))
        }
    }
}

/// Continue a artifact-backed group that was split because its direct `ColumnBatch` frame
/// exceeded the configured byte limit. The scanner is deliberately not
/// advanced here: the retained group is exhausted first, preserving order.
fn fetch_pending_column_batch(
    session: &mut ReadySession,
    cursor_id: u64,
    config: &ServerConfig,
) -> ServerResponse {
    let (response, emitted_rows, complete) = {
        let cursor = session
            .cursor
            .as_mut()
            .expect("pending batch requires an active cursor");
        let pending = cursor
            .pending_column_batch
            .as_mut()
            .expect("pending batch was checked above");
        match encode_next_pending_column_batch(pending, cursor_id, config) {
            Ok((response, emitted_rows)) => (response, emitted_rows, pending.is_complete()),
            Err(message) => {
                session.cursor = None;
                return ServerResponse::Message(cursor_failed(
                    cursor_id,
                    ProtocolErrorCode::ProtocolViolation,
                    message,
                ));
            }
        }
    };
    let cursor = session
        .cursor
        .as_mut()
        .expect("cursor remains active after a bounded batch");
    cursor.typed_batches_started = true;
    if complete {
        cursor.pending_column_batch = None;
    }
    ServerRuntimeMetrics::protocol_result_rows(emitted_rows as u64);
    response
}

/// Encode a complete immutable column batch exactly once. The resulting bytes
/// are reused by `ServerResponse::write_to`, so frame validation and socket
/// output observe the identical payload.
fn encode_column_batch_response(response: ServerMessage, config: &ServerConfig) -> ServerResponse {
    match encode_column_batch_payload(&response, config) {
        Ok(Some(payload)) => ServerResponse::Encoded { payload },
        Ok(None) => ServerResponse::Message(protocol_error(
            "typed column batch exceeds configured cursor batch or frame limit",
        )),
        Err(message) => ServerResponse::Message(protocol_error(message)),
    }
}

/// Encode one complete `ColumnBatch`, retaining the exact bytes only if they
/// fit both server-side limits. Borrowing `response` lets an oversized batch
/// be split without losing its already decoded storage columns.
fn encode_column_batch_payload(
    response: &ServerMessage,
    config: &ServerConfig,
) -> Result<Option<Vec<u8>>, String> {
    let encode_start = Instant::now();
    let payload = encode_payload(response).map_err(|error| error.to_string())?;
    ServerRuntimeMetrics::protocol_encode(payload.len() as u64, encode_start.elapsed());
    Ok((payload.len() <= config.cursor_batch_max_bytes
        && payload.len() <= config.max_frame_bytes as usize)
        .then_some(payload))
}

/// Emit the largest consecutive range that fits one columnar protocol frame.
///
/// This slow allocation path runs only after the direct artifact-backed group failed the
/// byte limit. It preserves the normal path's direct column move while making
/// configured limits strict and preventing a consumed artifact-backed group from vanishing.
fn encode_next_pending_column_batch(
    pending: &mut PendingColumnBatch,
    cursor_id: u64,
    config: &ServerConfig,
) -> Result<(ServerResponse, usize), String> {
    let remaining_rows = pending.remaining_rows();
    if remaining_rows == 0 {
        return Err("internal error: attempted to emit an exhausted typed column batch".into());
    }

    let limit = config
        .cursor_batch_max_bytes
        .min(config.max_frame_bytes as usize);
    let empty = ServerMessage::ColumnBatch {
        cursor_id,
        columns: slice_wire_columns(&pending.columns, pending.next_row, pending.next_row),
        row_count: 0,
        eof: false,
    };
    let empty_len = encoded_payload_len(&empty).map_err(|error| error.to_string())?;
    let mut estimated = empty_len;
    let mut byte_payload_lengths = vec![0_usize; pending.columns.len()];
    let mut take = 0usize;
    for row in pending.next_row..pending.row_count {
        let next_take = take + 1;
        let mut delta = encoded_payload_len(&(next_take as u32))
            .map_err(|error| error.to_string())?
            .saturating_sub(
                encoded_payload_len(&(take as u32)).map_err(|error| error.to_string())?,
            );
        for (column_index, column) in pending.columns.iter().enumerate() {
            delta = delta
                .checked_add(wire_column_row_encoded_delta(
                    column,
                    row,
                    take,
                    next_take,
                    byte_payload_lengths[column_index],
                )?)
                .ok_or_else(|| "typed column batch encoded length overflow".to_string())?;
        }
        let next = estimated
            .checked_add(delta)
            .ok_or_else(|| "typed column batch encoded length overflow".to_string())?;
        if next > limit {
            break;
        }
        estimated = next;
        take = next_take;
        for (column_index, column) in pending.columns.iter().enumerate() {
            if let Some(length) = wire_byte_column_row_length(column, row)? {
                byte_payload_lengths[column_index] = byte_payload_lengths[column_index]
                    .checked_add(length)
                    .ok_or_else(|| "typed byte payload length overflow".to_string())?;
            }
        }
    }
    if take == 0 {
        return Err("single typed row exceeds configured cursor batch or frame limit".into());
    }
    let response = ServerMessage::ColumnBatch {
        cursor_id,
        columns: slice_wire_columns(&pending.columns, pending.next_row, pending.next_row + take),
        row_count: u32::try_from(take)
            .map_err(|_| "typed column batch split row count exceeds u32")?,
        eof: false,
    };
    let payload = encode_column_batch_payload(&response, config)?
        .ok_or_else(|| "typed batch sizing estimate exceeded the configured limit".to_string())?;
    pending.next_row += take;
    Ok((ServerResponse::Encoded { payload }, take))
}

fn encoded_sequence_prefix_len(length: usize) -> Result<usize, String> {
    let length = u64::try_from(length).map_err(|_| "sequence length exceeds u64".to_string())?;
    encoded_payload_len(&length).map_err(|error| error.to_string())
}

fn sequence_prefix_growth(current: usize, next: usize) -> Result<usize, String> {
    Ok(encoded_sequence_prefix_len(next)?.saturating_sub(encoded_sequence_prefix_len(current)?))
}

fn wire_encoded_len<T: serde::Serialize>(value: &T) -> Result<usize, String> {
    encoded_payload_len(value).map_err(|error| error.to_string())
}

fn checked_encoded_sum(parts: &[usize], context: &str) -> Result<usize, String> {
    parts.iter().try_fold(0_usize, |sum, part| {
        sum.checked_add(*part)
            .ok_or_else(|| format!("{context} encoded length overflow"))
    })
}

fn wire_column_row_encoded_delta(
    column: &WireColumn,
    row: usize,
    current_rows: usize,
    next_rows: usize,
    current_byte_payload: usize,
) -> Result<usize, String> {
    let row_prefix = sequence_prefix_growth(current_rows, next_rows)?;
    let two_vectors = row_prefix
        .checked_mul(2)
        .ok_or_else(|| "typed vector prefix growth overflow".to_string())?;
    let null_length = |nulls: &[bool]| -> Result<usize, String> {
        let value = nulls
            .get(row)
            .ok_or_else(|| "typed null bitmap is shorter than its row count".to_string())?;
        wire_encoded_len(value)
    };
    match column {
        WireColumn::Int64 { values, nulls } | WireColumn::TimestampNanos { values, nulls } => {
            let value = values
                .get(row)
                .ok_or_else(|| "typed integer column is shorter than its row count".to_string())?;
            checked_encoded_sum(
                &[two_vectors, wire_encoded_len(value)?, null_length(nulls)?],
                "typed integer column",
            )
        }
        WireColumn::Float64 { values, nulls } => {
            let value = values
                .get(row)
                .ok_or_else(|| "typed float column is shorter than its row count".to_string())?;
            checked_encoded_sum(
                &[two_vectors, wire_encoded_len(value)?, null_length(nulls)?],
                "typed float column",
            )
        }
        WireColumn::Boolean { values, nulls } => {
            let value = values
                .get(row)
                .ok_or_else(|| "typed boolean column is shorter than its row count".to_string())?;
            checked_encoded_sum(
                &[two_vectors, wire_encoded_len(value)?, null_length(nulls)?],
                "typed boolean column",
            )
        }
        WireColumn::DictionaryText { ids, nulls, .. } => {
            let id = ids
                .get(row)
                .ok_or_else(|| "typed dictionary ids are shorter than its row count".to_string())?;
            checked_encoded_sum(
                &[two_vectors, wire_encoded_len(id)?, null_length(nulls)?],
                "typed dictionary column",
            )
        }
        WireColumn::Bytes { offsets, nulls, .. } | WireColumn::JsonText { offsets, nulls, .. } => {
            let &(_, byte_length) = offsets
                .get(row)
                .ok_or_else(|| "typed byte offsets are shorter than its row count".to_string())?;
            let byte_length = usize::try_from(byte_length)
                .map_err(|_| "typed byte length exceeds usize".to_string())?;
            let next_byte_payload = current_byte_payload
                .checked_add(byte_length)
                .ok_or_else(|| "typed byte payload length overflow".to_string())?;
            let data_prefix = sequence_prefix_growth(current_byte_payload, next_byte_payload)?;
            let offset = (
                u64::try_from(current_byte_payload)
                    .map_err(|_| "typed byte offset exceeds u64".to_string())?,
                u64::try_from(byte_length)
                    .map_err(|_| "typed byte length exceeds u64".to_string())?,
            );
            checked_encoded_sum(
                &[
                    two_vectors,
                    data_prefix,
                    byte_length,
                    wire_encoded_len(&offset)?,
                    null_length(nulls)?,
                ],
                "typed byte column",
            )
        }
        WireColumn::External { offsets, nulls, .. } => {
            let start = *offsets
                .get(row)
                .ok_or_else(|| "external offsets are shorter than its row count".to_string())?;
            let end = *offsets
                .get(row + 1)
                .ok_or_else(|| "external offsets are shorter than its row count".to_string())?;
            let byte_length = usize::try_from(end.saturating_sub(start))
                .map_err(|_| "external byte length exceeds usize".to_string())?;
            let next_byte_payload = current_byte_payload
                .checked_add(byte_length)
                .ok_or_else(|| "external byte payload length overflow".to_string())?;
            checked_encoded_sum(
                &[
                    sequence_prefix_growth(current_rows + 1, next_rows + 1)?,
                    sequence_prefix_growth(current_rows, next_rows)?,
                    sequence_prefix_growth(current_byte_payload, next_byte_payload)?,
                    byte_length,
                    wire_encoded_len(&end)?,
                    null_length(nulls)?,
                ],
                "external column",
            )
        }
    }
}

fn wire_byte_column_row_length(column: &WireColumn, row: usize) -> Result<Option<usize>, String> {
    match column {
        WireColumn::Bytes { offsets, .. } | WireColumn::JsonText { offsets, .. } => offsets
            .get(row)
            .ok_or_else(|| "typed byte offsets are shorter than its row count".to_string())
            .and_then(|&(_, length)| {
                usize::try_from(length)
                    .map(Some)
                    .map_err(|_| "typed byte length exceeds usize".to_string())
            }),
        WireColumn::External { offsets, .. } => {
            let start = offsets
                .get(row)
                .copied()
                .ok_or_else(|| "external offsets are shorter than its row count".to_string())?;
            let end = offsets
                .get(row + 1)
                .copied()
                .ok_or_else(|| "external offsets are shorter than its row count".to_string())?;
            usize::try_from(end.saturating_sub(start))
                .map(Some)
                .map_err(|_| "external byte length exceeds usize".to_string())
        }
        _ => Ok(None),
    }
}

fn slice_wire_columns(columns: &[WireColumn], start: usize, end: usize) -> Vec<WireColumn> {
    debug_assert!(start <= end);
    columns
        .iter()
        .map(|column| slice_wire_column(column, start, end))
        .collect()
}

fn slice_wire_column(column: &WireColumn, start: usize, end: usize) -> WireColumn {
    debug_assert!(end <= wire_column_len(column));
    match column {
        WireColumn::Int64 { values, nulls } => WireColumn::Int64 {
            values: values[start..end].to_vec(),
            nulls: nulls[start..end].to_vec(),
        },
        WireColumn::Float64 { values, nulls } => WireColumn::Float64 {
            values: values[start..end].to_vec(),
            nulls: nulls[start..end].to_vec(),
        },
        WireColumn::Boolean { values, nulls } => WireColumn::Boolean {
            values: values[start..end].to_vec(),
            nulls: nulls[start..end].to_vec(),
        },
        WireColumn::TimestampNanos { values, nulls } => WireColumn::TimestampNanos {
            values: values[start..end].to_vec(),
            nulls: nulls[start..end].to_vec(),
        },
        WireColumn::DictionaryText {
            ids,
            dictionary,
            nulls,
        } => WireColumn::DictionaryText {
            ids: ids[start..end].to_vec(),
            dictionary: dictionary.clone(),
            nulls: nulls[start..end].to_vec(),
        },
        WireColumn::Bytes {
            data,
            offsets,
            nulls,
        } => {
            let (data, offsets) = slice_wire_bytes(data, offsets, start, end);
            WireColumn::Bytes {
                data,
                offsets,
                nulls: nulls[start..end].to_vec(),
            }
        }
        WireColumn::JsonText {
            data,
            offsets,
            nulls,
        } => {
            let (data, offsets) = slice_wire_bytes(data, offsets, start, end);
            WireColumn::JsonText {
                data,
                offsets,
                nulls: nulls[start..end].to_vec(),
            }
        }
        WireColumn::External {
            type_object_id,
            codec_version,
            data,
            offsets,
            nulls,
        } => {
            let (data, offsets) = slice_external_bytes(data, offsets, start, end);
            WireColumn::External {
                type_object_id: *type_object_id,
                codec_version: *codec_version,
                data,
                offsets,
                nulls: nulls[start..end].to_vec(),
            }
        }
    }
}

fn slice_wire_bytes(
    data: &[u8],
    offsets: &[(u64, u64)],
    start: usize,
    end: usize,
) -> (Vec<u8>, Vec<(u64, u64)>) {
    let mut sliced_data = Vec::new();
    let mut sliced_offsets = Vec::with_capacity(end.saturating_sub(start));
    for &(offset, len) in &offsets[start..end] {
        let offset = usize::try_from(offset).expect("wire byte offset fits usize");
        let len = usize::try_from(len).expect("wire byte length fits usize");
        let end = offset
            .checked_add(len)
            .expect("wire byte range length does not overflow");
        let new_offset = sliced_data.len() as u64;
        sliced_data.extend_from_slice(&data[offset..end]);
        sliced_offsets.push((new_offset, len as u64));
    }
    (sliced_data, sliced_offsets)
}

fn wire_columns_retained_bytes(columns: &[WireColumn]) -> u64 {
    columns
        .iter()
        .map(wire_column_retained_bytes)
        .fold(0_u64, u64::saturating_add)
}

/// Convert facade-owned typed columns directly into the public wire layout.
///
/// The scanner advertises typed batches only for storage types that have an
/// explicit `WireColumn` representation. Keeping the exhaustive match here
/// makes an accidental future widening fail closed instead of changing the
/// meaning of an existing row result.
fn column_data_to_wire_column(column: ServerColumnData) -> Result<WireColumn, String> {
    match column {
        ServerColumnData::Int64 { values, nulls } => Ok(WireColumn::Int64 { values, nulls }),
        ServerColumnData::Float64 { values, nulls } => Ok(WireColumn::Float64 { values, nulls }),
        ServerColumnData::Boolean { values, nulls } => Ok(WireColumn::Boolean { values, nulls }),
        ServerColumnData::TimestampNanos { values, nulls } => {
            Ok(WireColumn::TimestampNanos { values, nulls })
        }
        ServerColumnData::DictionaryText {
            ids,
            dictionary,
            nulls,
        } => Ok(WireColumn::DictionaryText {
            ids,
            dictionary,
            nulls,
        }),
        ServerColumnData::Bytes {
            data,
            offsets,
            nulls,
        } => Ok(WireColumn::Bytes {
            data,
            offsets,
            nulls,
        }),
        ServerColumnData::JsonText {
            data,
            offsets,
            nulls,
        } => Ok(WireColumn::JsonText {
            data,
            offsets,
            nulls,
        }),
        ServerColumnData::External {
            data,
            offsets,
            type_ref,
            nulls,
        } => {
            let mut wire_offsets = Vec::with_capacity(offsets.len() + 1);
            let mut wire_data = Vec::with_capacity(data.len());
            wire_offsets.push(0);
            for (row, ((offset, length), is_null)) in offsets.into_iter().zip(&nulls).enumerate() {
                if *is_null {
                    if length != 0 {
                        return Err(format!("NULL external storage row {row} owns a byte range"));
                    }
                } else {
                    let start = usize::try_from(offset)
                        .map_err(|_| "external storage offset exceeds usize".to_string())?;
                    let length = usize::try_from(length)
                        .map_err(|_| "external storage length exceeds usize".to_string())?;
                    let end = start
                        .checked_add(length)
                        .ok_or_else(|| "external storage byte range overflows".to_string())?;
                    let value = data
                        .get(start..end)
                        .ok_or_else(|| "external storage byte range exceeds payload".to_string())?;
                    wire_data.extend_from_slice(value);
                }
                wire_offsets.push(
                    u32::try_from(wire_data.len())
                        .map_err(|_| "external storage offset exceeds u32".to_string())?,
                );
            }
            Ok(WireColumn::External {
                type_object_id: type_ref.type_object_id(),
                codec_version: type_ref.codec_version(),
                data: wire_data,
                offsets: wire_offsets,
                nulls,
            })
        }
    }
}

struct PreparedRowBatch {
    payload: Vec<u8>,
    emitted_rows: usize,
    eof: bool,
    pending: Option<PendingRowBatch>,
}

/// Prepare one bounded legacy RowBatch and retain its exact wire payload.
///
/// The normal path serialises the complete batch once. Only a batch that is
/// actually larger than a configured limit enters the slow split path; its
/// unconsumed tail remains bounded by `cursor_batch_max_rows` and is emitted
/// before the storage iterator advances again.
fn prepare_row_batch(
    cursor_id: u64,
    rows: Vec<Row>,
    source_eof: bool,
    config: &ServerConfig,
) -> Result<PreparedRowBatch, String> {
    let response = ServerMessage::RowBatch {
        cursor_id,
        rows,
        eof: source_eof,
    };
    let payload = encode_row_batch_payload(&response)?;
    if row_batch_payload_fits(&payload, config) {
        let ServerMessage::RowBatch { rows, eof, .. } = response else {
            unreachable!("constructed a row batch response")
        };
        return Ok(PreparedRowBatch {
            emitted_rows: rows.len(),
            payload,
            eof,
            pending: None,
        });
    }

    let ServerMessage::RowBatch { mut rows, .. } = response else {
        unreachable!("constructed a row batch response")
    };
    if rows.is_empty() {
        return Err("empty row batch exceeds configured cursor batch or frame limit".to_string());
    }
    if rows.len() == 1 {
        return Err("single row exceeds configured cursor batch or frame limit".to_string());
    }

    // One sizing pass over borrowed rows, then one materialization and encode.
    // Vec's bincode payload is its length prefix followed by each Row payload.
    let empty = ServerMessage::RowBatch {
        cursor_id,
        rows: Vec::new(),
        eof: false,
    };
    let empty_len = encoded_payload_len(&empty).map_err(|error| error.to_string())?;
    let zero_len = encoded_payload_len(&0_u64).map_err(|error| error.to_string())?;
    let limit = config
        .cursor_batch_max_bytes
        .min(config.max_frame_bytes as usize);
    let mut payload_len = empty_len.saturating_sub(zero_len);
    let mut take = 0usize;
    for row in &rows {
        let row_len = encoded_payload_len(row).map_err(|error| error.to_string())?;
        let next_take = take + 1;
        let count_len =
            encoded_payload_len(&(next_take as u64)).map_err(|error| error.to_string())?;
        let next_len = payload_len.saturating_add(row_len);
        if next_len.saturating_add(count_len) > limit {
            break;
        }
        payload_len = next_len;
        take = next_take;
    }
    if take == 0 {
        return Err("single row exceeds configured cursor batch or frame limit".to_string());
    }
    let candidate = ServerMessage::RowBatch {
        cursor_id,
        rows: rows[..take].to_vec(),
        eof: false,
    };
    let payload = encode_row_batch_payload(&candidate)?;
    if !row_batch_payload_fits(&payload, config) {
        return Err("row batch sizing invariant exceeded configured limit".to_string());
    }
    let tail = rows.split_off(take);
    let pending = (!tail.is_empty()).then_some(PendingRowBatch {
        rows: tail,
        eof: source_eof,
    });
    Ok(PreparedRowBatch {
        payload,
        emitted_rows: take,
        eof: pending.is_none() && source_eof,
        pending,
    })
}

fn encode_row_batch_payload(response: &ServerMessage) -> Result<Vec<u8>, String> {
    let encode_start = Instant::now();
    let payload = encode_payload(response).map_err(|error| error.to_string())?;
    ServerRuntimeMetrics::protocol_encode(payload.len() as u64, encode_start.elapsed());
    Ok(payload)
}

fn row_batch_payload_fits(payload: &[u8], config: &ServerConfig) -> bool {
    payload.len() <= config.cursor_batch_max_bytes
        && payload.len() <= config.max_frame_bytes as usize
}

#[cfg(test)]
mod tests;

fn close_cursor(session: &mut ReadySession, cursor_id: u64) -> ServerMessage {
    let Some(cursor) = session.cursor.take() else {
        return cursor_not_found(cursor_id);
    };
    if cursor.id != cursor_id {
        session.cursor = Some(cursor);
        return cursor_not_found(cursor_id);
    }
    ServerMessage::CursorClosed { cursor_id }
}

fn protocol_error(message: impl Into<String>) -> ServerMessage {
    ServerMessage::Error(ProtocolFailure {
        code: ProtocolErrorCode::ProtocolViolation,
        message: message.into(),
    })
}

fn unsupported_type(message: impl Into<String>) -> ServerMessage {
    ServerMessage::Error(ProtocolFailure {
        code: ProtocolErrorCode::UnsupportedType,
        message: message.into(),
    })
}

fn authentication_failed(message: impl Into<String>) -> ServerMessage {
    ServerMessage::Error(ProtocolFailure {
        code: ProtocolErrorCode::AuthenticationFailed,
        message: message.into(),
    })
}

fn out_of_sync(message: impl Into<String>) -> ServerMessage {
    ServerMessage::Error(ProtocolFailure {
        code: ProtocolErrorCode::CommandsOutOfSync,
        message: message.into(),
    })
}

fn transaction_error(message: impl Into<String>) -> ServerMessage {
    ServerMessage::Error(ProtocolFailure {
        code: ProtocolErrorCode::TransactionState,
        message: message.into(),
    })
}

fn database_failure(error: &DatabaseError) -> ProtocolFailure {
    ProtocolFailure {
        code: match error {
            DatabaseError::AuthorizationDenied(_) => ProtocolErrorCode::AuthorizationDenied,
            DatabaseError::CompactionBackpressure { .. } => {
                ProtocolErrorCode::CompactionBackpressure
            }
            _ => ProtocolErrorCode::SqlError,
        },
        message: error.to_string(),
    }
}

fn database_error(error: DatabaseError) -> ServerMessage {
    ServerMessage::Error(database_failure(&error))
}

fn database_required() -> ServerMessage {
    ServerMessage::Error(ProtocolFailure {
        code: ProtocolErrorCode::DatabaseNotFound,
        message: "select a database before executing SQL".to_string(),
    })
}

fn cursor_not_found(cursor_id: u64) -> ServerMessage {
    ServerMessage::Error(ProtocolFailure {
        code: ProtocolErrorCode::CursorNotFound,
        message: format!("cursor {cursor_id} not found"),
    })
}

fn cursor_failed(
    cursor_id: u64,
    code: ProtocolErrorCode,
    message: impl Into<String>,
) -> ServerMessage {
    ServerMessage::CursorFailed {
        cursor_id,
        failure: ProtocolFailure {
            code,
            message: message.into(),
        },
        active: false,
    }
}
