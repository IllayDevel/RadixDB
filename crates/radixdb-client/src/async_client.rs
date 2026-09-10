//! Tokio-native RadixDB client transport.
//!
//! The state machine mirrors [`crate::Connection`] while all socket I/O uses
//! `tokio::net::TcpStream`. Protocol messages and the binary codec remain
//! owned by [`crate::protocol`].

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpStream, ToSocketAddrs},
    time::timeout,
};
use tokio_rustls::client::TlsStream;

use crate::{
    protocol::{
        decode_payload, encode_payload, frame_length_prefix, validate_column_batch,
        validate_frame_length, validate_row_batch, ClientMessage, ServerMessage,
        DEFAULT_MAX_FRAME_BYTES,
    },
    ClientError, ColumnCursorBatch, Cursor, CursorBatch, CursorFetchMode, ExecuteResult,
    PreparedStatement, ProtocolCapability, ProtocolError, ServerStatus, TimeoutOperation,
    TlsClientConfig, TransactionIsolation, WireValue, PROTOCOL_VERSION,
};

/// Independent deadlines for asynchronous connection and frame I/O.
///
/// Every label in a resulting [`ClientError::Timeout`] is static and contains
/// no SQL, parameters, credentials or database names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AsyncTimeouts {
    pub connect: Duration,
    pub read: Duration,
    pub write: Duration,
    pub shutdown: Duration,
}

impl AsyncTimeouts {
    pub fn new(connect: Duration, read: Duration, write: Duration) -> Self {
        Self {
            connect,
            read,
            write,
            shutdown: write,
        }
    }

    pub fn with_shutdown(mut self, shutdown: Duration) -> Self {
        self.shutdown = shutdown;
        self
    }
}

impl Default for AsyncTimeouts {
    fn default() -> Self {
        Self::new(
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(30),
        )
    }
}

/// Explicit client-side knowledge of the server transaction outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AsyncTransactionState {
    Inactive = 0,
    Active = 1,
    /// A cancelled, timed out or incomplete command may have changed the
    /// server state. The connection is poisoned and must not be pooled.
    Unknown = 2,
}

impl AsyncTransactionState {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Inactive,
            1 => Self::Active,
            _ => Self::Unknown,
        }
    }
}

struct SharedConnectionState {
    poisoned: AtomicBool,
    closed: AtomicBool,
    transaction: AtomicU8,
}

impl SharedConnectionState {
    fn new() -> Self {
        Self {
            poisoned: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            transaction: AtomicU8::new(AsyncTransactionState::Inactive as u8),
        }
    }

    fn poison(&self) {
        self.transaction
            .store(AsyncTransactionState::Unknown as u8, Ordering::Release);
        self.poisoned.store(true, Ordering::Release);
    }

    fn transaction_state(&self) -> AsyncTransactionState {
        AsyncTransactionState::from_u8(self.transaction.load(Ordering::Acquire))
    }

    fn set_transaction_state(&self, state: AsyncTransactionState) {
        self.transaction.store(state as u8, Ordering::Release);
    }
}

/// Marks a connection unusable if a future is dropped after frame I/O may
/// have started and before the complete response is decoded.
struct CommandGuard {
    state: Arc<SharedConnectionState>,
    armed: bool,
}

impl CommandGuard {
    fn new(state: Arc<SharedConnectionState>) -> Self {
        Self {
            state,
            armed: false,
        }
    }

    fn arm(&mut self) {
        self.armed = true;
    }

    fn complete(&mut self) {
        self.armed = false;
    }
}

impl Drop for CommandGuard {
    fn drop(&mut self) {
        if self.armed {
            self.state.poison();
        }
    }
}

/// A Tokio-native, single-command-at-a-time RadixDB connection.
///
/// `&mut self` on every protocol operation enforces one in-flight command per
/// socket. The type is `Send` when its stream is `Send`; it intentionally does
/// not provide internal multiplexing, hidden threads or a command queue.
pub struct AsyncConnection<S = TcpStream> {
    stream: S,
    timeouts: AsyncTimeouts,
    max_frame_bytes: u32,
    capabilities: Vec<ProtocolCapability>,
    owner_id: u64,
    active_cursor: Option<u64>,
    state: Arc<SharedConnectionState>,
}

impl AsyncConnection<TcpStream> {
    /// Connect with explicit deadlines, enable `TCP_NODELAY`, and complete the
    /// protocol handshake before returning the connection.
    pub async fn connect(
        address: impl ToSocketAddrs,
        timeouts: AsyncTimeouts,
    ) -> Result<Self, ClientError> {
        let stream = timeout(timeouts.connect, TcpStream::connect(address))
            .await
            .map_err(|_| ClientError::Timeout {
                operation: TimeoutOperation::Connect,
            })??;
        stream.set_nodelay(true)?;
        Self::from_stream(stream, timeouts).await
    }
}

impl AsyncConnection<TlsStream<TcpStream>> {
    /// Connect through verified direct TLS and complete the protocol
    /// handshake only after certificate and server-name validation succeeds.
    pub async fn connect_tls(
        address: impl ToSocketAddrs,
        tls: &TlsClientConfig,
        timeouts: AsyncTimeouts,
    ) -> Result<Self, ClientError> {
        let stream = timeout(timeouts.connect, TcpStream::connect(address))
            .await
            .map_err(|_| ClientError::Timeout {
                operation: TimeoutOperation::Connect,
            })??;
        stream.set_nodelay(true)?;
        let connector = tokio_rustls::TlsConnector::from(Arc::clone(&tls.config));
        let stream = timeout(
            timeouts.connect,
            connector.connect(tls.server_name.clone(), stream),
        )
        .await
        .map_err(|_| ClientError::Timeout {
            operation: TimeoutOperation::Connect,
        })?
        .map_err(|error| ClientError::Tls(error.to_string()))?;
        Self::from_stream(stream, timeouts).await
    }
}

impl<S> AsyncConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Construct over an existing asynchronous stream and perform the same
    /// handshake/capability negotiation as the blocking client.
    pub async fn from_stream(stream: S, timeouts: AsyncTimeouts) -> Result<Self, ClientError> {
        let owner_id = crate::request_id::next_client_request_id().ok_or_else(|| {
            ClientError::Protocol(ProtocolError::InvalidBatchShape(
                "connection identity space exhausted".into(),
            ))
        })?;
        let mut connection = Self {
            stream,
            timeouts,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            capabilities: Vec::new(),
            owner_id,
            active_cursor: None,
            state: Arc::new(SharedConnectionState::new()),
        };

        let response = connection
            .round_trip(ClientMessage::Handshake {
                protocol_version: PROTOCOL_VERSION,
                max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
                capabilities: vec![
                    ProtocolCapability::ColumnBatchV1,
                    ProtocolCapability::BuildIdentityV1,
                    ProtocolCapability::ExternalValueV1,
                ],
            })
            .await?;

        match response {
            ServerMessage::HandshakeAccepted {
                protocol_version,
                max_frame_bytes,
                capabilities,
            } if protocol_version == PROTOCOL_VERSION && max_frame_bytes > 0 => {
                connection.max_frame_bytes = max_frame_bytes.min(DEFAULT_MAX_FRAME_BYTES);
                connection.capabilities = capabilities;
                Ok(connection)
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => connection.unexpected("server did not accept protocol handshake"),
        }
    }

    pub async fn authenticate(
        &mut self,
        login: impl Into<String>,
        password: Option<String>,
    ) -> Result<(), ClientError> {
        match self
            .round_trip(ClientMessage::Authenticate {
                login: login.into(),
                password,
            })
            .await?
        {
            ServerMessage::AuthenticationAccepted => Ok(()),
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not return authentication response"),
        }
    }

    pub async fn authenticate_database(
        &mut self,
        database: impl Into<String>,
        login: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<(), ClientError> {
        match self
            .round_trip(ClientMessage::AuthenticatePrincipal {
                database: database.into(),
                login: login.into(),
                password: password.into(),
            })
            .await?
        {
            ServerMessage::AuthenticationAccepted => Ok(()),
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not return authentication response"),
        }
    }

    pub async fn select_database(
        &mut self,
        database: impl Into<String>,
    ) -> Result<(), ClientError> {
        self.ensure_no_active_cursor()?;
        if self.transaction_active()? {
            return Err(ClientError::TransactionState(
                "cannot select a database while a transaction is active",
            ));
        }
        let database = database.into();
        match self
            .round_trip(ClientMessage::SelectDatabase {
                database: database.clone(),
            })
            .await?
        {
            ServerMessage::DatabaseSelected { database: selected } if selected == database => {
                Ok(())
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not select requested database"),
        }
    }

    pub async fn server_status(&mut self) -> Result<ServerStatus, ClientError> {
        self.request_server_status(None).await
    }

    pub async fn database_status(
        &mut self,
        database: impl Into<String>,
    ) -> Result<ServerStatus, ClientError> {
        self.request_server_status(Some(database.into())).await
    }

    async fn request_server_status(
        &mut self,
        database: Option<String>,
    ) -> Result<ServerStatus, ClientError> {
        self.ensure_no_active_cursor()?;
        match self
            .round_trip(ClientMessage::ServerStatus { database })
            .await?
        {
            ServerMessage::ServerStatus(status) => Ok(*status),
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not return status response"),
        }
    }

    pub async fn execute(&mut self, sql: impl Into<String>) -> Result<ExecuteResult, ClientError> {
        self.execute_with_parameters(sql, BTreeMap::new()).await
    }

    pub async fn execute_with_parameters(
        &mut self,
        sql: impl Into<String>,
        parameters: BTreeMap<String, WireValue>,
    ) -> Result<ExecuteResult, ClientError> {
        self.execute_with_bindings(sql, Vec::new(), parameters)
            .await
    }

    pub async fn execute_with_positional_parameters(
        &mut self,
        sql: impl Into<String>,
        positional: Vec<WireValue>,
    ) -> Result<ExecuteResult, ClientError> {
        self.execute_with_bindings(sql, positional, BTreeMap::new())
            .await
    }

    pub fn reserve_request_id(&mut self) -> Result<u64, ClientError> {
        crate::request_id::next_client_request_id().ok_or(ClientError::ConnectionPoisoned)
    }

    pub async fn execute_with_bindings(
        &mut self,
        sql: impl Into<String>,
        positional: Vec<WireValue>,
        named: BTreeMap<String, WireValue>,
    ) -> Result<ExecuteResult, ClientError> {
        self.ensure_no_active_cursor()?;
        let request_id = self.reserve_request_id()?;
        self.execute_with_request_id(request_id, sql, positional, named)
            .await
    }

    pub async fn execute_with_request_id(
        &mut self,
        request_id: u64,
        sql: impl Into<String>,
        positional: Vec<WireValue>,
        named: BTreeMap<String, WireValue>,
    ) -> Result<ExecuteResult, ClientError> {
        self.ensure_no_active_cursor()?;
        match self
            .round_trip(ClientMessage::Execute {
                request_id,
                sql: sql.into(),
                positional,
                named,
            })
            .await?
        {
            ServerMessage::CommandComplete {
                affected_rows,
                last_insert_id,
            } => Ok(ExecuteResult::CommandComplete {
                affected_rows,
                last_insert_id,
            }),
            ServerMessage::CursorOpened { cursor_id, columns } => {
                self.active_cursor = Some(cursor_id);
                Ok(ExecuteResult::Cursor(Cursor {
                    id: cursor_id,
                    columns,
                }))
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not return execute response"),
        }
    }

    pub async fn prepare(
        &mut self,
        sql: impl Into<String>,
    ) -> Result<PreparedStatement, ClientError> {
        self.ensure_no_active_cursor()?;
        match self
            .round_trip(ClientMessage::Prepare { sql: sql.into() })
            .await?
        {
            ServerMessage::Prepared { statement_id } => Ok(PreparedStatement {
                id: statement_id,
                owner_id: self.owner_id,
            }),
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not prepare statement"),
        }
    }

    pub async fn execute_prepared(
        &mut self,
        statement: &PreparedStatement,
        positional: Vec<WireValue>,
    ) -> Result<ExecuteResult, ClientError> {
        self.execute_prepared_with_bindings(statement, positional, BTreeMap::new())
            .await
    }

    pub async fn execute_prepared_with_bindings(
        &mut self,
        statement: &PreparedStatement,
        positional: Vec<WireValue>,
        named: BTreeMap<String, WireValue>,
    ) -> Result<ExecuteResult, ClientError> {
        self.ensure_no_active_cursor()?;
        if statement.owner_id != self.owner_id {
            return Err(ClientError::PreparedStatementOwnerMismatch);
        }
        let request_id = self.reserve_request_id()?;
        match self
            .round_trip(ClientMessage::ExecutePrepared {
                request_id,
                statement_id: statement.id,
                positional,
                named,
            })
            .await?
        {
            ServerMessage::CommandComplete {
                affected_rows,
                last_insert_id,
            } => Ok(ExecuteResult::CommandComplete {
                affected_rows,
                last_insert_id,
            }),
            ServerMessage::CursorOpened { cursor_id, columns } => {
                self.active_cursor = Some(cursor_id);
                Ok(ExecuteResult::Cursor(Cursor {
                    id: cursor_id,
                    columns,
                }))
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not execute prepared statement"),
        }
    }

    pub async fn close_prepared(
        &mut self,
        statement: PreparedStatement,
    ) -> Result<(), ClientError> {
        self.ensure_no_active_cursor()?;
        if statement.owner_id != self.owner_id {
            return Err(ClientError::PreparedStatementOwnerMismatch);
        }
        match self
            .round_trip(ClientMessage::ClosePrepared {
                statement_id: statement.id,
            })
            .await?
        {
            ServerMessage::PreparedClosed { statement_id } if statement_id == statement.id => {
                Ok(())
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not close prepared statement"),
        }
    }

    pub async fn cancel_execution(&mut self, request_id: u64) -> Result<bool, ClientError> {
        self.ensure_no_active_cursor()?;
        match self
            .round_trip(ClientMessage::CancelExecution { request_id })
            .await?
        {
            ServerMessage::ExecutionCancelled {
                request_id: actual,
                found,
            } if actual == request_id => Ok(found),
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not acknowledge execution cancellation"),
        }
    }

    pub async fn fetch(&mut self, cursor: &Cursor) -> Result<CursorBatch, ClientError> {
        self.ensure_cursor(cursor.id)?;
        match self
            .round_trip(ClientMessage::Fetch {
                cursor_id: cursor.id,
            })
            .await?
        {
            ServerMessage::RowBatch {
                cursor_id,
                rows,
                eof,
            } if cursor_id == cursor.id => {
                validate_row_batch(&rows, &cursor.columns).map_err(|error| {
                    self.state.poison();
                    ClientError::Protocol(error)
                })?;
                if eof {
                    self.active_cursor = None;
                }
                Ok(CursorBatch { rows, eof })
            }
            ServerMessage::CursorFailed {
                cursor_id,
                failure,
                active,
            } if cursor_id == cursor.id => {
                if !active {
                    self.active_cursor = None;
                }
                Err(ClientError::Server(failure))
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not return cursor batch"),
        }
    }

    pub async fn fetch_batch(
        &mut self,
        cursor: &Cursor,
        mode: CursorFetchMode,
    ) -> Result<ColumnCursorBatch, ClientError> {
        match mode {
            CursorFetchMode::Rows => self.fetch(cursor).await.map(ColumnCursorBatch::Rows),
            CursorFetchMode::Columnar => self.fetch_column_batch(cursor).await,
            CursorFetchMode::Auto => {
                if self
                    .capabilities
                    .contains(&ProtocolCapability::ColumnBatchV1)
                {
                    self.fetch_column_batch(cursor).await
                } else {
                    self.fetch(cursor).await.map(ColumnCursorBatch::Rows)
                }
            }
        }
    }

    pub async fn fetch_column_batch(
        &mut self,
        cursor: &Cursor,
    ) -> Result<ColumnCursorBatch, ClientError> {
        self.ensure_cursor(cursor.id)?;
        if !self
            .capabilities
            .contains(&ProtocolCapability::ColumnBatchV1)
        {
            return Err(ClientError::CapabilityUnavailable(
                ProtocolCapability::ColumnBatchV1,
            ));
        }
        match self
            .round_trip(ClientMessage::FetchColumnBatch {
                cursor_id: cursor.id,
            })
            .await?
        {
            ServerMessage::ColumnBatch {
                cursor_id,
                columns,
                row_count,
                eof,
            } if cursor_id == cursor.id => {
                validate_column_batch(&columns, row_count, &cursor.columns, eof).map_err(
                    |error| {
                        self.state.poison();
                        ClientError::Protocol(error)
                    },
                )?;
                if eof {
                    self.active_cursor = None;
                }
                Ok(ColumnCursorBatch::Columnar {
                    columns,
                    row_count,
                    eof,
                })
            }
            ServerMessage::RowBatch {
                cursor_id,
                rows,
                eof,
            } if cursor_id == cursor.id => {
                validate_row_batch(&rows, &cursor.columns).map_err(|error| {
                    self.state.poison();
                    ClientError::Protocol(error)
                })?;
                if eof {
                    self.active_cursor = None;
                }
                Ok(ColumnCursorBatch::Rows(CursorBatch { rows, eof }))
            }
            ServerMessage::CursorFailed {
                cursor_id,
                failure,
                active,
            } if cursor_id == cursor.id => {
                if !active {
                    self.active_cursor = None;
                }
                Err(ClientError::Server(failure))
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not return column or row cursor batch"),
        }
    }

    pub async fn close_cursor(&mut self, cursor: Cursor) -> Result<(), ClientError> {
        self.ensure_cursor(cursor.id)?;
        match self
            .round_trip(ClientMessage::CloseCursor {
                cursor_id: cursor.id,
            })
            .await?
        {
            ServerMessage::CursorClosed { cursor_id } if cursor_id == cursor.id => {
                self.active_cursor = None;
                Ok(())
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not close cursor"),
        }
    }

    pub async fn cancel(&mut self, cursor: Cursor) -> Result<(), ClientError> {
        self.ensure_cursor(cursor.id)?;
        match self
            .round_trip(ClientMessage::Cancel {
                cursor_id: cursor.id,
            })
            .await?
        {
            ServerMessage::CursorClosed { cursor_id } if cursor_id == cursor.id => {
                self.active_cursor = None;
                Ok(())
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not cancel cursor"),
        }
    }

    pub async fn discard_active_cursor(&mut self) -> Result<bool, ClientError> {
        let Some(cursor_id) = self.active_cursor else {
            return Ok(false);
        };
        match self
            .round_trip(ClientMessage::CloseCursor { cursor_id })
            .await?
        {
            ServerMessage::CursorClosed { cursor_id: actual } if actual == cursor_id => {
                self.active_cursor = None;
                Ok(true)
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not discard active cursor"),
        }
    }

    pub async fn begin(&mut self) -> Result<(), ClientError> {
        self.begin_with_isolation(TransactionIsolation::ReadCommitted)
            .await
    }

    pub async fn begin_with_isolation(
        &mut self,
        isolation: TransactionIsolation,
    ) -> Result<(), ClientError> {
        self.ensure_no_active_cursor()?;
        if self.transaction_active()? {
            return Err(ClientError::TransactionState(
                "a transaction is already active",
            ));
        }
        match self
            .round_trip(ClientMessage::BeginTransaction { isolation })
            .await?
        {
            ServerMessage::TransactionBegan => {
                self.state
                    .set_transaction_state(AsyncTransactionState::Active);
                Ok(())
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not begin transaction"),
        }
    }

    pub async fn commit(&mut self) -> Result<(), ClientError> {
        self.finish_transaction(
            ClientMessage::CommitTransaction,
            ServerMessage::TransactionCommitted,
            "server did not commit transaction",
        )
        .await
    }

    pub async fn rollback(&mut self) -> Result<(), ClientError> {
        self.finish_transaction(
            ClientMessage::RollbackTransaction,
            ServerMessage::TransactionRolledBack,
            "server did not roll back transaction",
        )
        .await
    }

    pub async fn savepoint(&mut self, name: impl Into<String>) -> Result<(), ClientError> {
        self.savepoint_round_trip(ClientMessage::CreateSavepoint { name: name.into() }, 0)
            .await
    }

    pub async fn rollback_to_savepoint(
        &mut self,
        name: impl Into<String>,
    ) -> Result<(), ClientError> {
        self.savepoint_round_trip(ClientMessage::RollbackToSavepoint { name: name.into() }, 1)
            .await
    }

    pub async fn release_savepoint(&mut self, name: impl Into<String>) -> Result<(), ClientError> {
        self.savepoint_round_trip(ClientMessage::ReleaseSavepoint { name: name.into() }, 2)
            .await
    }

    async fn savepoint_round_trip(
        &mut self,
        request: ClientMessage,
        kind: u8,
    ) -> Result<(), ClientError> {
        self.ensure_no_active_cursor()?;
        if !self.transaction_active()? {
            return Err(ClientError::TransactionState("no transaction is active"));
        }
        let expected_name = match &request {
            ClientMessage::CreateSavepoint { name }
            | ClientMessage::RollbackToSavepoint { name }
            | ClientMessage::ReleaseSavepoint { name } => name.clone(),
            _ => return self.unexpected("invalid local savepoint request"),
        };
        match (kind, self.round_trip(request).await?) {
            (0, ServerMessage::SavepointCreated { name })
            | (1, ServerMessage::SavepointRolledBack { name })
            | (2, ServerMessage::SavepointReleased { name })
                if name == expected_name =>
            {
                Ok(())
            }
            (_, ServerMessage::Error(error)) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not complete savepoint operation"),
        }
    }

    pub async fn close_database(&mut self, database: impl Into<String>) -> Result<(), ClientError> {
        self.ensure_no_active_cursor()?;
        if self.transaction_active()? {
            return Err(ClientError::TransactionState(
                "cannot close database while a transaction is active",
            ));
        }
        let database = database.into();
        match self
            .round_trip(ClientMessage::CloseDatabase {
                database: database.clone(),
            })
            .await?
        {
            ServerMessage::DatabaseClosed { database: actual } if actual == database => Ok(()),
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected("server did not close database"),
        }
    }

    async fn finish_transaction(
        &mut self,
        request: ClientMessage,
        expected: ServerMessage,
        unexpected: &'static str,
    ) -> Result<(), ClientError> {
        self.ensure_no_active_cursor()?;
        if !self.transaction_active()? {
            return Err(ClientError::TransactionState("no transaction is active"));
        }
        match self.round_trip(request).await? {
            response if response == expected => {
                self.state
                    .set_transaction_state(AsyncTransactionState::Inactive);
                Ok(())
            }
            ServerMessage::TransactionFailed { failure, active } => {
                self.state.set_transaction_state(if active {
                    AsyncTransactionState::Active
                } else {
                    AsyncTransactionState::Inactive
                });
                Err(ClientError::Server(failure))
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => self.unexpected(unexpected),
        }
    }

    /// Returns the last server-confirmed transaction state.
    pub fn transaction_state(&self) -> AsyncTransactionState {
        self.state.transaction_state()
    }

    /// Return a truthful active/inactive answer or an explicit poisoned error
    /// when cancellation made the server outcome unknowable.
    pub fn transaction_active(&self) -> Result<bool, ClientError> {
        match self.transaction_state() {
            AsyncTransactionState::Inactive => Ok(false),
            AsyncTransactionState::Active => Ok(true),
            AsyncTransactionState::Unknown => Err(ClientError::ConnectionPoisoned),
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.transaction_state() != AsyncTransactionState::Inactive
    }

    pub fn capabilities(&self) -> &[ProtocolCapability] {
        &self.capabilities
    }

    pub fn max_frame_bytes(&self) -> u32 {
        self.max_frame_bytes
    }

    pub fn has_active_cursor(&self) -> bool {
        self.active_cursor.is_some()
    }

    pub fn is_poisoned(&self) -> bool {
        self.state.poisoned.load(Ordering::Acquire)
    }

    pub fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }

    /// A pool may reuse only a healthy, open connection with no cursor and no
    /// explicit transaction.
    pub fn is_reusable(&self) -> bool {
        !self.is_poisoned()
            && !self.is_closed()
            && self.active_cursor.is_none()
            && self.transaction_state() == AsyncTransactionState::Inactive
    }

    /// Close the asynchronous stream. This method remains available for a
    /// poisoned connection so callers can discard it deterministically.
    pub async fn shutdown(&mut self) -> Result<(), ClientError> {
        if self.is_closed() {
            return Ok(());
        }
        let result = timeout(self.timeouts.shutdown, self.stream.shutdown()).await;
        self.state.closed.store(true, Ordering::Release);
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                self.state.poison();
                Err(ClientError::Io(error))
            }
            Err(_) => {
                self.state.poison();
                Err(ClientError::Timeout {
                    operation: TimeoutOperation::Shutdown,
                })
            }
        }
    }

    async fn round_trip(&mut self, message: ClientMessage) -> Result<ServerMessage, ClientError> {
        self.ensure_usable()?;
        let payload = encode_payload(&message)?;
        let prefix = frame_length_prefix(payload.len(), self.max_frame_bytes)?;
        let mut guard = CommandGuard::new(Arc::clone(&self.state));
        guard.arm();

        self.write_frame(&prefix, &payload).await?;
        let response = self.read_frame().await?;
        guard.complete();
        Ok(response)
    }

    async fn write_frame(&mut self, prefix: &[u8; 4], payload: &[u8]) -> Result<(), ClientError> {
        let write = async {
            self.stream.write_all(prefix).await?;
            self.stream.write_all(payload).await?;
            self.stream.flush().await?;
            Ok::<(), std::io::Error>(())
        };
        match timeout(self.timeouts.write, write).await {
            Ok(result) => result.map_err(ClientError::Io),
            Err(_) => Err(ClientError::Timeout {
                operation: TimeoutOperation::Write,
            }),
        }
    }

    async fn read_frame(&mut self) -> Result<ServerMessage, ClientError> {
        let max_frame_bytes = self.max_frame_bytes;
        let read = async {
            let mut prefix = [0_u8; 4];
            self.stream.read_exact(&mut prefix).await?;
            let payload_len = validate_frame_length(u32::from_be_bytes(prefix), max_frame_bytes)?;
            let mut payload = vec![0_u8; payload_len];
            self.stream.read_exact(&mut payload).await?;
            decode_payload(&payload).map_err(ClientError::Protocol)
        };
        match timeout(self.timeouts.read, read).await {
            Ok(result) => result,
            Err(_) => Err(ClientError::Timeout {
                operation: TimeoutOperation::Read,
            }),
        }
    }

    fn ensure_usable(&self) -> Result<(), ClientError> {
        if self.is_poisoned() {
            Err(ClientError::ConnectionPoisoned)
        } else if self.is_closed() {
            Err(ClientError::ConnectionClosed)
        } else {
            Ok(())
        }
    }

    fn ensure_no_active_cursor(&self) -> Result<(), ClientError> {
        self.ensure_usable()?;
        if self.active_cursor.is_some() {
            Err(ClientError::CommandsOutOfSync)
        } else {
            Ok(())
        }
    }

    fn ensure_cursor(&self, id: u64) -> Result<(), ClientError> {
        self.ensure_usable()?;
        if self.active_cursor == Some(id) {
            Ok(())
        } else {
            Err(ClientError::CommandsOutOfSync)
        }
    }

    fn unexpected<T>(&mut self, message: &'static str) -> Result<T, ClientError> {
        self.state.poison();
        Err(ClientError::UnexpectedResponse(message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        protocol::ProtocolFailure, Column, ProtocolError, ProtocolErrorCode, Row, WireColumn,
    };
    use tokio::{io::DuplexStream, sync::oneshot};

    fn assert_send<T: Send>() {}

    async fn read_message(stream: &mut DuplexStream) -> ClientMessage {
        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix).await.unwrap();
        let len =
            validate_frame_length(u32::from_be_bytes(prefix), DEFAULT_MAX_FRAME_BYTES).unwrap();
        let mut payload = vec![0_u8; len];
        stream.read_exact(&mut payload).await.unwrap();
        decode_payload(&payload).unwrap()
    }

    async fn write_message(stream: &mut DuplexStream, message: &ServerMessage) {
        let payload = encode_payload(message).unwrap();
        let prefix = frame_length_prefix(payload.len(), DEFAULT_MAX_FRAME_BYTES).unwrap();
        stream.write_all(&prefix).await.unwrap();
        stream.write_all(&payload).await.unwrap();
        stream.flush().await.unwrap();
    }

    async fn accept_handshake(stream: &mut DuplexStream) {
        assert!(matches!(
            read_message(stream).await,
            ClientMessage::Handshake {
                protocol_version: PROTOCOL_VERSION,
                ..
            }
        ));
        write_message(
            stream,
            &ServerMessage::HandshakeAccepted {
                protocol_version: PROTOCOL_VERSION,
                max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
                capabilities: vec![ProtocolCapability::ColumnBatchV1],
            },
        )
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn core_lifecycle_uses_one_async_stream() {
        assert_send::<AsyncConnection<TcpStream>>();
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        let server_task = tokio::spawn(async move {
            accept_handshake(&mut server).await;

            assert!(matches!(
                read_message(&mut server).await,
                ClientMessage::Authenticate { .. }
            ));
            write_message(&mut server, &ServerMessage::AuthenticationAccepted).await;

            assert!(matches!(
                read_message(&mut server).await,
                ClientMessage::Execute { .. }
            ));
            write_message(
                &mut server,
                &ServerMessage::CursorOpened {
                    cursor_id: 7,
                    columns: vec![Column {
                        name: "id".to_string(),
                        type_name: "INTEGER".to_string(),
                        nullable: false,
                        external_type: None,
                    }],
                },
            )
            .await;

            assert_eq!(
                read_message(&mut server).await,
                ClientMessage::Fetch { cursor_id: 7 }
            );
            write_message(
                &mut server,
                &ServerMessage::RowBatch {
                    cursor_id: 7,
                    rows: vec![Row {
                        values: vec![WireValue::Int(11)],
                    }],
                    eof: true,
                },
            )
            .await;

            assert_eq!(
                read_message(&mut server).await,
                ClientMessage::BeginTransaction {
                    isolation: TransactionIsolation::ReadCommitted,
                }
            );
            write_message(&mut server, &ServerMessage::TransactionBegan).await;
            assert_eq!(
                read_message(&mut server).await,
                ClientMessage::CommitTransaction
            );
            write_message(&mut server, &ServerMessage::TransactionCommitted).await;
        });

        let mut connection = AsyncConnection::from_stream(
            client,
            AsyncTimeouts::new(
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
        )
        .await
        .unwrap();
        connection.authenticate("root", None).await.unwrap();
        let cursor = match connection.execute("SELECT id FROM t").await.unwrap() {
            ExecuteResult::Cursor(cursor) => cursor,
            other => panic!("unexpected result: {other:?}"),
        };
        let batch = connection.fetch(&cursor).await.unwrap();
        assert!(batch.eof);
        assert_eq!(batch.rows[0].values, vec![WireValue::Int(11)]);
        connection.begin().await.unwrap();
        assert_eq!(
            connection.transaction_state(),
            AsyncTransactionState::Active
        );
        connection.commit().await.unwrap();
        assert!(connection.is_reusable());
        server_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_positional_savepoint_and_close_database_roundtrip() {
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        let server_task = tokio::spawn(async move {
            accept_handshake(&mut server).await;
            assert_eq!(
                read_message(&mut server).await,
                ClientMessage::Prepare {
                    sql: "INSERT INTO t VALUES ($1)".into()
                }
            );
            write_message(&mut server, &ServerMessage::Prepared { statement_id: 31 }).await;
            assert!(matches!(
                read_message(&mut server).await,
                ClientMessage::ExecutePrepared {
                    statement_id: 31,
                    positional,
                    ..
                } if positional == vec![WireValue::Int(7)]
            ));
            write_message(
                &mut server,
                &ServerMessage::CommandComplete {
                    affected_rows: 1,
                    last_insert_id: 0,
                },
            )
            .await;
            assert_eq!(
                read_message(&mut server).await,
                ClientMessage::BeginTransaction {
                    isolation: TransactionIsolation::ReadCommitted,
                }
            );
            write_message(&mut server, &ServerMessage::TransactionBegan).await;
            for (request, response) in [
                (
                    ClientMessage::CreateSavepoint { name: "s1".into() },
                    ServerMessage::SavepointCreated { name: "s1".into() },
                ),
                (
                    ClientMessage::RollbackToSavepoint { name: "s1".into() },
                    ServerMessage::SavepointRolledBack { name: "s1".into() },
                ),
                (
                    ClientMessage::ReleaseSavepoint { name: "s1".into() },
                    ServerMessage::SavepointReleased { name: "s1".into() },
                ),
            ] {
                assert_eq!(read_message(&mut server).await, request);
                write_message(&mut server, &response).await;
            }
            assert_eq!(
                read_message(&mut server).await,
                ClientMessage::RollbackTransaction
            );
            write_message(&mut server, &ServerMessage::TransactionRolledBack).await;
            assert_eq!(
                read_message(&mut server).await,
                ClientMessage::ClosePrepared { statement_id: 31 }
            );
            write_message(
                &mut server,
                &ServerMessage::PreparedClosed { statement_id: 31 },
            )
            .await;
            assert_eq!(
                read_message(&mut server).await,
                ClientMessage::CloseDatabase {
                    database: "db".into()
                }
            );
            write_message(
                &mut server,
                &ServerMessage::DatabaseClosed {
                    database: "db".into(),
                },
            )
            .await;
        });

        let mut connection = AsyncConnection::from_stream(
            client,
            AsyncTimeouts::new(
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
        )
        .await
        .unwrap();
        let statement = connection
            .prepare("INSERT INTO t VALUES ($1)")
            .await
            .unwrap();
        assert!(matches!(
            connection
                .execute_prepared(&statement, vec![WireValue::Int(7)])
                .await
                .unwrap(),
            ExecuteResult::CommandComplete {
                affected_rows: 1,
                ..
            }
        ));
        connection.begin().await.unwrap();
        connection.savepoint("s1").await.unwrap();
        connection.rollback_to_savepoint("s1").await.unwrap();
        connection.release_savepoint("s1").await.unwrap();
        connection.rollback().await.unwrap();
        connection.close_prepared(statement).await.unwrap();
        connection.close_database("db").await.unwrap();
        assert!(connection.is_reusable());
        server_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn malformed_async_column_batch_is_rejected_and_poisoned() {
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        let server_task = tokio::spawn(async move {
            accept_handshake(&mut server).await;
            assert!(matches!(
                read_message(&mut server).await,
                ClientMessage::Execute { .. }
            ));
            write_message(
                &mut server,
                &ServerMessage::CursorOpened {
                    cursor_id: 8,
                    columns: vec![Column {
                        name: "id".to_string(),
                        type_name: "INTEGER".to_string(),
                        nullable: false,
                        external_type: None,
                    }],
                },
            )
            .await;
            assert!(matches!(
                read_message(&mut server).await,
                ClientMessage::FetchColumnBatch { cursor_id: 8 }
            ));
            write_message(
                &mut server,
                &ServerMessage::ColumnBatch {
                    cursor_id: 8,
                    columns: vec![WireColumn::Int64 {
                        values: vec![1],
                        nulls: Vec::new(),
                    }],
                    row_count: 1,
                    eof: true,
                },
            )
            .await;
        });
        let mut connection = AsyncConnection::from_stream(
            client,
            AsyncTimeouts::new(
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
        )
        .await
        .unwrap();
        let ExecuteResult::Cursor(cursor) = connection.execute("SELECT id FROM t").await.unwrap()
        else {
            panic!("cursor expected")
        };
        assert!(matches!(
            connection.fetch_column_batch(&cursor).await,
            Err(ClientError::Protocol(ProtocolError::InvalidBatchShape(_)))
        ));
        assert!(connection.is_poisoned());
        server_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_async_cursor_failure_releases_the_local_cursor() {
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        let server_task = tokio::spawn(async move {
            accept_handshake(&mut server).await;
            assert!(matches!(
                read_message(&mut server).await,
                ClientMessage::Execute { .. }
            ));
            write_message(
                &mut server,
                &ServerMessage::CursorOpened {
                    cursor_id: 21,
                    columns: vec![Column {
                        name: "id".to_string(),
                        type_name: "INTEGER".to_string(),
                        nullable: false,
                        external_type: None,
                    }],
                },
            )
            .await;
            assert_eq!(
                read_message(&mut server).await,
                ClientMessage::Fetch { cursor_id: 21 }
            );
            write_message(
                &mut server,
                &ServerMessage::CursorFailed {
                    cursor_id: 21,
                    failure: ProtocolFailure {
                        code: ProtocolErrorCode::ServerError,
                        message: "terminal storage failure".to_string(),
                    },
                    active: false,
                },
            )
            .await;
        });
        let mut connection = AsyncConnection::from_stream(
            client,
            AsyncTimeouts::new(
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
        )
        .await
        .unwrap();
        let ExecuteResult::Cursor(cursor) = connection.execute("SELECT id FROM t").await.unwrap()
        else {
            panic!("cursor expected")
        };
        assert!(matches!(
            connection.fetch(&cursor).await,
            Err(ClientError::Server(_))
        ));
        assert!(!connection.discard_active_cursor().await.unwrap());
        assert!(connection.is_reusable());
        server_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_read_poisons_connection_and_transaction_state() {
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        let (request_seen_tx, request_seen_rx) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            accept_handshake(&mut server).await;
            assert!(matches!(
                read_message(&mut server).await,
                ClientMessage::Execute { .. }
            ));
            let _ = request_seen_tx.send(());
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let mut connection = AsyncConnection::from_stream(
            client,
            AsyncTimeouts::new(
                Duration::from_secs(1),
                Duration::from_secs(60),
                Duration::from_secs(1),
            ),
        )
        .await
        .unwrap();

        {
            let operation = connection.execute("SELECT 1");
            tokio::pin!(operation);
            tokio::select! {
                _ = &mut operation => panic!("server unexpectedly answered"),
                _ = request_seen_rx => {}
            }
        }

        assert!(connection.is_poisoned());
        assert!(!connection.is_reusable());
        assert_eq!(
            connection.transaction_state(),
            AsyncTransactionState::Unknown
        );
        assert!(
            connection.in_transaction(),
            "unknown must not look inactive"
        );
        assert!(matches!(
            connection.transaction_active(),
            Err(ClientError::ConnectionPoisoned)
        ));
        assert!(matches!(
            connection.execute("SELECT 2").await,
            Err(ClientError::ConnectionPoisoned)
        ));
        server_task.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn transaction_failure_preserves_server_reported_state() {
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        let server_task = tokio::spawn(async move {
            accept_handshake(&mut server).await;
            assert_eq!(
                read_message(&mut server).await,
                ClientMessage::BeginTransaction {
                    isolation: TransactionIsolation::ReadCommitted,
                }
            );
            write_message(&mut server, &ServerMessage::TransactionBegan).await;
            assert_eq!(
                read_message(&mut server).await,
                ClientMessage::CommitTransaction
            );
            write_message(
                &mut server,
                &ServerMessage::TransactionFailed {
                    failure: ProtocolFailure {
                        code: ProtocolErrorCode::SqlError,
                        message: "commit rejected".to_string(),
                    },
                    active: true,
                },
            )
            .await;
            assert_eq!(
                read_message(&mut server).await,
                ClientMessage::RollbackTransaction
            );
            write_message(&mut server, &ServerMessage::TransactionRolledBack).await;
        });

        let mut connection = AsyncConnection::from_stream(
            client,
            AsyncTimeouts::new(
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
        )
        .await
        .unwrap();
        connection.begin().await.unwrap();
        assert!(matches!(
            connection.commit().await,
            Err(ClientError::Server(_))
        ));
        assert!(connection.transaction_active().unwrap());
        connection.rollback().await.unwrap();
        assert!(!connection.transaction_active().unwrap());
        server_task.await.unwrap();
    }
}
