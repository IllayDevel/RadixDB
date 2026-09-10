//! Reusable RadixDB binary protocol client.
//!
//! It deliberately does not depend on server implementation, storage, SQL
//! executor, or a copied protocol implementation.

mod orm;
pub mod protocol;
mod request_id;

pub use orm::{
    AlterTableRequest, CreateTableRequest, DdlRequest, DescribeDatabaseRequest,
    DescribeTableRequest, ListTablesRequest, OrmClientError, SchemaClient, TableColumnsRequest,
    TableConstraintsRequest, TableIndexesRequest, TableSchemaClient,
};
#[cfg(feature = "tokio")]
pub use orm::{
    AsyncAlterTableRequest, AsyncCreateTableRequest, AsyncDdlRequest, AsyncDescribeDatabaseRequest,
    AsyncDescribeTableRequest, AsyncListTablesRequest, AsyncSchemaClient, AsyncTableColumnsRequest,
    AsyncTableConstraintsRequest, AsyncTableIndexesRequest, AsyncTableSchemaClient,
};

#[cfg(feature = "tokio")]
mod async_client;

#[cfg(feature = "tokio")]
pub use async_client::{AsyncConnection, AsyncTimeouts, AsyncTransactionState};

use std::{
    collections::BTreeMap,
    fs::File,
    io::BufReader,
    io::{Read, Write},
    net::{Shutdown, TcpStream, ToSocketAddrs},
    path::Path,
    sync::Arc,
    time::Duration,
};

use rustls::{
    pki_types::ServerName, ClientConfig as RustlsClientConfig, ClientConnection, RootCertStore,
    StreamOwned,
};

use crate::protocol::{
    read_frame, validate_column_batch, validate_row_batch, write_frame, ClientMessage,
    ProtocolError, ServerMessage, DEFAULT_MAX_FRAME_BYTES,
};

pub use crate::protocol::{
    BuildIdentity, Column, DatabaseArtifactSummary, DatabaseStatus, ProtocolCapability,
    ProtocolErrorCode, ProtocolFailure, Row, ServerLifecycleState, ServerRuntimeStatus,
    ServerStatus, TransactionIsolation, WireColumn, WireValue, PROTOCOL_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Inactive,
    Active,
    Unknown,
}

/// Stable operation labels used by asynchronous timeout errors. They never
/// contain SQL text, parameters, credentials or database names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutOperation {
    Connect,
    Write,
    Read,
    Shutdown,
}

#[derive(Debug)]
pub enum ClientError {
    Io(std::io::Error),
    Protocol(ProtocolError),
    Server(ProtocolFailure),
    UnexpectedResponse(&'static str),
    CommandsOutOfSync,
    TransactionState(&'static str),
    Timeout { operation: TimeoutOperation },
    ConnectionPoisoned,
    ConnectionClosed,
    PreparedStatementOwnerMismatch,
    CapabilityUnavailable(ProtocolCapability),
    TlsConfiguration(String),
    Tls(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::Protocol(error) => error.fmt(formatter),
            Self::Server(error) => write!(formatter, "server {:?}: {}", error.code, error.message),
            Self::UnexpectedResponse(message) => formatter.write_str(message),
            Self::CommandsOutOfSync => formatter
                .write_str("the active cursor must finish or be closed before another command"),
            Self::TransactionState(message) => formatter.write_str(message),
            Self::Timeout { operation } => {
                write!(formatter, "client {operation:?} deadline elapsed")
            }
            Self::ConnectionPoisoned => formatter
                .write_str("connection outcome is unknown after a cancelled or incomplete command"),
            Self::ConnectionClosed => formatter.write_str("connection is closed"),
            Self::PreparedStatementOwnerMismatch => {
                formatter.write_str("prepared statement belongs to another connection")
            }
            Self::CapabilityUnavailable(capability) => {
                write!(
                    formatter,
                    "server did not negotiate capability {capability:?}"
                )
            }
            Self::TlsConfiguration(message) => write!(formatter, "TLS configuration: {message}"),
            Self::Tls(message) => write!(formatter, "TLS transport: {message}"),
        }
    }
}

/// Verified TLS client policy. The server name is immutable and certificate
/// roots are supplied explicitly; native/platform roots are never guessed.
#[derive(Clone)]
pub struct TlsClientConfig {
    config: Arc<RustlsClientConfig>,
    server_name: ServerName<'static>,
}

impl std::fmt::Debug for TlsClientConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TlsClientConfig")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

impl TlsClientConfig {
    pub fn from_ca_pem(
        certificate_authority: impl AsRef<Path>,
        server_name: impl Into<String>,
    ) -> Result<Self, ClientError> {
        let path = certificate_authority.as_ref();
        let file = File::open(path).map_err(ClientError::Io)?;
        let mut reader = BufReader::new(file);
        let certificates = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(ClientError::Io)?;
        if certificates.is_empty() {
            return Err(ClientError::TlsConfiguration(
                "certificate authority PEM contains no certificates".to_owned(),
            ));
        }
        let mut roots = RootCertStore::empty();
        for certificate in certificates {
            roots.add(certificate).map_err(|error| {
                ClientError::TlsConfiguration(format!("invalid trust anchor: {error}"))
            })?;
        }
        let server_name = ServerName::try_from(server_name.into())
            .map_err(|_| ClientError::TlsConfiguration("invalid TLS server name".to_owned()))?;
        let config = RustlsClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Self {
            config: Arc::new(config),
            server_name,
        })
    }
}

impl std::error::Error for ClientError {}

impl ClientError {
    /// True only for an explicit server classification whose operation is
    /// known not to have published. Transport failures remain outcome-unknown
    /// and must never be retried automatically.
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Server(failure) if failure.code.is_retryable())
    }
}

impl From<std::io::Error> for ClientError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ProtocolError> for ClientError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecuteResult {
    CommandComplete {
        affected_rows: u64,
        last_insert_id: u64,
    },
    Cursor(Cursor),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cursor {
    id: u64,
    columns: Vec<Column>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedStatement {
    pub(crate) id: u64,
    pub(crate) owner_id: u64,
}

impl PreparedStatement {
    pub fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CursorBatch {
    pub rows: Vec<Row>,
    pub eof: bool,
}

/// Result of an optional column-batch cursor fetch.
///
/// `Rows` is a semantic fallback for any query that needs filtering, MVCC
/// overlays, schema mapping, ordering or a value type not yet represented by
/// `WireColumn`. Callers can therefore use the extension without changing
/// query correctness assumptions.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnCursorBatch {
    Columnar {
        columns: Vec<WireColumn>,
        row_count: u32,
        eof: bool,
    },
    Rows(CursorBatch),
}

/// Transport requested for the next cursor batch.
///
/// `Auto` is the production default for new code: it uses `ColumnBatchV1`
/// when the handshake negotiated it and accepts the server's semantic row
/// fallback. `Rows` preserves the legacy row protocol explicitly, while
/// `Columnar` requires the capability and otherwise returns
/// `CapabilityUnavailable`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CursorFetchMode {
    #[default]
    Auto,
    Rows,
    Columnar,
}

impl Cursor {
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
}

pub struct Connection<S = TcpStream> {
    stream: S,
    max_frame_bytes: u32,
    capabilities: Vec<ProtocolCapability>,
    owner_id: u64,
    poisoned: bool,
    closed: bool,
    active_cursor: Option<u64>,
    transaction: TransactionState,
}

impl Connection<TcpStream> {
    pub fn connect(address: impl ToSocketAddrs) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(address)?;
        stream.set_nodelay(true)?;
        Self::from_stream(stream)
    }

    pub fn connect_with_timeouts(
        address: impl ToSocketAddrs,
        connect_timeout: Duration,
        read_timeout: Duration,
        write_timeout: Duration,
    ) -> Result<Self, ClientError> {
        let addresses = address.to_socket_addrs()?.collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(invalid_address("address resolved to no socket addresses"));
        }
        let mut last_error = None;
        for address in addresses {
            match TcpStream::connect_timeout(&address, connect_timeout) {
                Ok(stream) => {
                    stream.set_nodelay(true)?;
                    stream.set_read_timeout(Some(read_timeout))?;
                    stream.set_write_timeout(Some(write_timeout))?;
                    return Self::from_stream(stream);
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(ClientError::Io(last_error.unwrap_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                "no address was attempted",
            )
        })))
    }

    pub fn shutdown(&mut self) -> Result<(), ClientError> {
        if self.closed {
            return Ok(());
        }
        let result = self.stream.shutdown(Shutdown::Both);
        self.closed = true;
        result.map_err(ClientError::Io)
    }
}

pub type TlsConnection = Connection<StreamOwned<ClientConnection, TcpStream>>;

impl Connection<StreamOwned<ClientConnection, TcpStream>> {
    pub fn connect_tls(
        address: impl ToSocketAddrs,
        tls: &TlsClientConfig,
    ) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(address)?;
        stream.set_nodelay(true)?;
        let connection = ClientConnection::new(Arc::clone(&tls.config), tls.server_name.clone())
            .map_err(|error| ClientError::Tls(error.to_string()))?;
        Self::from_stream(StreamOwned::new(connection, stream))
    }

    pub fn connect_tls_with_timeouts(
        address: impl ToSocketAddrs,
        tls: &TlsClientConfig,
        connect_timeout: Duration,
        read_timeout: Duration,
        write_timeout: Duration,
    ) -> Result<Self, ClientError> {
        let addresses = address.to_socket_addrs()?.collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(invalid_address("address resolved to no socket addresses"));
        }
        let mut last_error = None;
        for address in addresses {
            match TcpStream::connect_timeout(&address, connect_timeout) {
                Ok(stream) => {
                    stream.set_nodelay(true)?;
                    stream.set_read_timeout(Some(read_timeout))?;
                    stream.set_write_timeout(Some(write_timeout))?;
                    let connection =
                        ClientConnection::new(Arc::clone(&tls.config), tls.server_name.clone())
                            .map_err(|error| ClientError::Tls(error.to_string()))?;
                    return Self::from_stream(StreamOwned::new(connection, stream));
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(ClientError::Io(last_error.unwrap_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                "no address was attempted",
            )
        })))
    }

    pub fn shutdown(&mut self) -> Result<(), ClientError> {
        if self.closed {
            return Ok(());
        }
        self.stream.conn.send_close_notify();
        let result = self.stream.sock.shutdown(Shutdown::Both);
        self.closed = true;
        result.map_err(ClientError::Io)
    }
}

fn invalid_address(message: &'static str) -> ClientError {
    ClientError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message,
    ))
}

impl<S: Read + Write> Connection<S> {
    pub fn from_stream(stream: S) -> Result<Self, ClientError> {
        let owner_id = crate::request_id::next_client_request_id().ok_or_else(|| {
            ClientError::Protocol(ProtocolError::InvalidBatchShape(
                "connection identity space exhausted".into(),
            ))
        })?;
        let mut connection = Self {
            stream,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            capabilities: Vec::new(),
            owner_id,
            poisoned: false,
            closed: false,
            active_cursor: None,
            transaction: TransactionState::Inactive,
        };
        connection.send(&ClientMessage::Handshake {
            protocol_version: PROTOCOL_VERSION,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            capabilities: vec![
                ProtocolCapability::ColumnBatchV1,
                ProtocolCapability::BuildIdentityV1,
                ProtocolCapability::ExternalValueV1,
            ],
        })?;
        match connection.receive()? {
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
            _ => Err(connection.poison_unexpected("server did not accept protocol handshake")),
        }
    }

    pub fn authenticate(
        &mut self,
        login: impl Into<String>,
        password: Option<String>,
    ) -> Result<(), ClientError> {
        self.send(&ClientMessage::Authenticate {
            login: login.into(),
            password,
        })?;
        match self.receive()? {
            ServerMessage::AuthenticationAccepted => Ok(()),
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => Err(self.poison_unexpected("server did not return authentication response")),
        }
    }

    /// Authenticate a durable catalog Principal and atomically select its
    /// database. This is the production identity boundary.
    pub fn authenticate_database(
        &mut self,
        database: impl Into<String>,
        login: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<(), ClientError> {
        self.send(&ClientMessage::AuthenticatePrincipal {
            database: database.into(),
            login: login.into(),
            password: password.into(),
        })?;
        match self.receive()? {
            ServerMessage::AuthenticationAccepted => Ok(()),
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => Err(self.poison_unexpected("server did not return authentication response")),
        }
    }

    pub fn select_database(&mut self, database: impl Into<String>) -> Result<(), ClientError> {
        if self.has_active_stream() {
            return Err(ClientError::CommandsOutOfSync);
        }
        if self.transaction_active()? {
            return Err(ClientError::TransactionState(
                "cannot select a database while a transaction is active",
            ));
        }
        let database = database.into();
        self.send(&ClientMessage::SelectDatabase {
            database: database.clone(),
        })?;
        match self.receive()? {
            ServerMessage::DatabaseSelected { database: selected } if selected == database => {
                Ok(())
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => Err(self.poison_unexpected("server did not select requested database")),
        }
    }

    pub fn server_status(&mut self) -> Result<ServerStatus, ClientError> {
        self.request_server_status(None)
    }

    pub fn database_status(
        &mut self,
        database: impl Into<String>,
    ) -> Result<ServerStatus, ClientError> {
        self.request_server_status(Some(database.into()))
    }

    fn request_server_status(
        &mut self,
        database: Option<String>,
    ) -> Result<ServerStatus, ClientError> {
        if self.has_active_stream() {
            return Err(ClientError::CommandsOutOfSync);
        }
        self.send(&ClientMessage::ServerStatus { database })?;
        match self.receive()? {
            ServerMessage::ServerStatus(status) => Ok(*status),
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => Err(self.poison_unexpected("server did not return status response")),
        }
    }

    pub fn execute(&mut self, sql: impl Into<String>) -> Result<ExecuteResult, ClientError> {
        self.execute_with_parameters(sql, BTreeMap::new())
    }

    pub fn execute_with_parameters(
        &mut self,
        sql: impl Into<String>,
        parameters: BTreeMap<String, WireValue>,
    ) -> Result<ExecuteResult, ClientError> {
        if self.has_active_stream() {
            return Err(ClientError::CommandsOutOfSync);
        }
        self.execute_with_bindings(sql, Vec::new(), parameters)
    }

    pub fn execute_with_positional_parameters(
        &mut self,
        sql: impl Into<String>,
        positional: Vec<WireValue>,
    ) -> Result<ExecuteResult, ClientError> {
        self.execute_with_bindings(sql, positional, BTreeMap::new())
    }

    pub fn reserve_request_id(&mut self) -> Result<u64, ClientError> {
        crate::request_id::next_client_request_id().ok_or_else(|| {
            self.poisoned = true;
            ClientError::Protocol(ProtocolError::InvalidBatchShape(
                "request id space exhausted".into(),
            ))
        })
    }

    pub fn execute_with_bindings(
        &mut self,
        sql: impl Into<String>,
        positional: Vec<WireValue>,
        named: BTreeMap<String, WireValue>,
    ) -> Result<ExecuteResult, ClientError> {
        if self.has_active_stream() {
            return Err(ClientError::CommandsOutOfSync);
        }
        let request_id = self.reserve_request_id()?;
        self.execute_with_request_id(request_id, sql, positional, named)
    }

    pub fn execute_with_request_id(
        &mut self,
        request_id: u64,
        sql: impl Into<String>,
        positional: Vec<WireValue>,
        named: BTreeMap<String, WireValue>,
    ) -> Result<ExecuteResult, ClientError> {
        if self.has_active_stream() {
            return Err(ClientError::CommandsOutOfSync);
        }
        self.send(&ClientMessage::Execute {
            request_id,
            sql: sql.into(),
            positional,
            named,
        })?;
        self.receive_execute_result()
    }

    fn receive_execute_result(&mut self) -> Result<ExecuteResult, ClientError> {
        match self.receive()? {
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
            _ => Err(self.poison_unexpected("server did not return execute response")),
        }
    }

    pub fn prepare(&mut self, sql: impl Into<String>) -> Result<PreparedStatement, ClientError> {
        if self.has_active_stream() {
            return Err(ClientError::CommandsOutOfSync);
        }
        self.send(&ClientMessage::Prepare { sql: sql.into() })?;
        match self.receive()? {
            ServerMessage::Prepared { statement_id } => Ok(PreparedStatement {
                id: statement_id,
                owner_id: self.owner_id,
            }),
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => Err(self.poison_unexpected("server did not prepare statement")),
        }
    }

    pub fn execute_prepared(
        &mut self,
        statement: &PreparedStatement,
        positional: Vec<WireValue>,
    ) -> Result<ExecuteResult, ClientError> {
        self.execute_prepared_with_bindings(statement, positional, BTreeMap::new())
    }

    pub fn execute_prepared_with_bindings(
        &mut self,
        statement: &PreparedStatement,
        positional: Vec<WireValue>,
        named: BTreeMap<String, WireValue>,
    ) -> Result<ExecuteResult, ClientError> {
        if self.has_active_stream() {
            return Err(ClientError::CommandsOutOfSync);
        }
        if statement.owner_id != self.owner_id {
            return Err(ClientError::PreparedStatementOwnerMismatch);
        }
        let request_id = self.reserve_request_id()?;
        self.send(&ClientMessage::ExecutePrepared {
            request_id,
            statement_id: statement.id,
            positional,
            named,
        })?;
        self.receive_execute_result()
    }

    pub fn close_prepared(&mut self, statement: PreparedStatement) -> Result<(), ClientError> {
        if self.has_active_stream() {
            return Err(ClientError::CommandsOutOfSync);
        }
        if statement.owner_id != self.owner_id {
            return Err(ClientError::PreparedStatementOwnerMismatch);
        }
        self.send(&ClientMessage::ClosePrepared {
            statement_id: statement.id,
        })?;
        match self.receive()? {
            ServerMessage::PreparedClosed { statement_id } if statement_id == statement.id => {
                Ok(())
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => Err(self.poison_unexpected("server did not close prepared statement")),
        }
    }

    pub fn cancel_execution(&mut self, request_id: u64) -> Result<bool, ClientError> {
        if self.has_active_stream() {
            return Err(ClientError::CommandsOutOfSync);
        }
        self.send(&ClientMessage::CancelExecution { request_id })?;
        match self.receive()? {
            ServerMessage::ExecutionCancelled {
                request_id: actual,
                found,
            } if actual == request_id => Ok(found),
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => Err(self.poison_unexpected("server did not acknowledge execution cancellation")),
        }
    }

    pub fn fetch(&mut self, cursor: &Cursor) -> Result<CursorBatch, ClientError> {
        if self.active_cursor != Some(cursor.id) {
            return Err(ClientError::CommandsOutOfSync);
        }
        self.send(&ClientMessage::Fetch {
            cursor_id: cursor.id,
        })?;
        match self.receive()? {
            ServerMessage::RowBatch {
                cursor_id,
                rows,
                eof,
            } if cursor_id == cursor.id => {
                validate_row_batch(&rows, &cursor.columns).map_err(|error| {
                    self.poisoned = true;
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
            _ => Err(self.poison_unexpected("server did not return cursor batch")),
        }
    }

    /// Fetch one cursor batch using an explicit transport policy.
    ///
    /// This is the preferred high-level cursor boundary for new clients. It
    /// exposes typed columns without converting them back into millions of
    /// `Row`/`WireValue` objects, while retaining a first-class row fallback
    /// for query/storage shapes that are not columnar-safe.
    pub fn fetch_batch(
        &mut self,
        cursor: &Cursor,
        mode: CursorFetchMode,
    ) -> Result<ColumnCursorBatch, ClientError> {
        match mode {
            CursorFetchMode::Rows => self.fetch(cursor).map(ColumnCursorBatch::Rows),
            CursorFetchMode::Columnar => self.fetch_column_batch(cursor),
            CursorFetchMode::Auto => {
                if self
                    .capabilities
                    .contains(&ProtocolCapability::ColumnBatchV1)
                {
                    self.fetch_column_batch(cursor)
                } else {
                    self.fetch(cursor).map(ColumnCursorBatch::Rows)
                }
            }
        }
    }

    /// Fetch one cursor batch through the negotiated columnar protocol.
    ///
    /// This does not alter `fetch()`: callers that need the traditional row
    /// API keep receiving only `CursorBatch`. When a query is not eligible for
    /// direct typed transport, the server returns `ColumnCursorBatch::Rows`.
    pub fn fetch_column_batch(
        &mut self,
        cursor: &Cursor,
    ) -> Result<ColumnCursorBatch, ClientError> {
        if self.active_cursor != Some(cursor.id) {
            return Err(ClientError::CommandsOutOfSync);
        }
        if !self
            .capabilities
            .contains(&ProtocolCapability::ColumnBatchV1)
        {
            return Err(ClientError::CapabilityUnavailable(
                ProtocolCapability::ColumnBatchV1,
            ));
        }
        self.send(&ClientMessage::FetchColumnBatch {
            cursor_id: cursor.id,
        })?;
        match self.receive()? {
            ServerMessage::ColumnBatch {
                cursor_id,
                columns,
                row_count,
                eof,
            } if cursor_id == cursor.id => {
                validate_column_batch(&columns, row_count, &cursor.columns, eof).map_err(
                    |error| {
                        self.poisoned = true;
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
                    self.poisoned = true;
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
            _ => Err(self.poison_unexpected("server did not return column or row cursor batch")),
        }
    }

    pub fn close_cursor(&mut self, cursor: Cursor) -> Result<(), ClientError> {
        if self.active_cursor != Some(cursor.id) {
            return Err(ClientError::CommandsOutOfSync);
        }
        self.send(&ClientMessage::CloseCursor {
            cursor_id: cursor.id,
        })?;
        match self.receive()? {
            ServerMessage::CursorClosed { cursor_id } if cursor_id == cursor.id => {
                self.active_cursor = None;
                Ok(())
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => Err(self.poison_unexpected("server did not close cursor")),
        }
    }

    pub fn cancel(&mut self, cursor: Cursor) -> Result<(), ClientError> {
        if self.active_cursor != Some(cursor.id) {
            return Err(ClientError::CommandsOutOfSync);
        }
        self.send(&ClientMessage::Cancel {
            cursor_id: cursor.id,
        })?;
        match self.receive()? {
            ServerMessage::CursorClosed { cursor_id } if cursor_id == cursor.id => {
                self.active_cursor = None;
                Ok(())
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => Err(self.poison_unexpected("server did not cancel cursor")),
        }
    }

    pub fn discard_active_cursor(&mut self) -> Result<bool, ClientError> {
        let Some(cursor_id) = self.active_cursor else {
            return Ok(false);
        };
        self.send(&ClientMessage::CloseCursor { cursor_id })?;
        match self.receive()? {
            ServerMessage::CursorClosed { cursor_id: actual } if actual == cursor_id => {
                self.active_cursor = None;
                Ok(true)
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => Err(self.poison_unexpected("server did not discard active cursor")),
        }
    }

    pub fn begin(&mut self) -> Result<(), ClientError> {
        self.begin_with_isolation(TransactionIsolation::ReadCommitted)
    }

    pub fn begin_with_isolation(
        &mut self,
        isolation: TransactionIsolation,
    ) -> Result<(), ClientError> {
        if self.has_active_stream() {
            return Err(ClientError::CommandsOutOfSync);
        }
        if self.transaction_active()? {
            return Err(ClientError::TransactionState(
                "a transaction is already active",
            ));
        }
        self.send(&ClientMessage::BeginTransaction { isolation })?;
        match self.receive()? {
            ServerMessage::TransactionBegan => {
                self.transaction = TransactionState::Active;
                Ok(())
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => Err(self.poison_unexpected("server did not begin transaction")),
        }
    }

    pub fn commit(&mut self) -> Result<(), ClientError> {
        self.finish_transaction(
            ClientMessage::CommitTransaction,
            ServerMessage::TransactionCommitted,
            "server did not commit transaction",
        )
    }

    pub fn rollback(&mut self) -> Result<(), ClientError> {
        self.finish_transaction(
            ClientMessage::RollbackTransaction,
            ServerMessage::TransactionRolledBack,
            "server did not roll back transaction",
        )
    }

    pub fn savepoint(&mut self, name: impl Into<String>) -> Result<(), ClientError> {
        self.savepoint_round_trip(ClientMessage::CreateSavepoint { name: name.into() }, 0)
    }

    pub fn rollback_to_savepoint(&mut self, name: impl Into<String>) -> Result<(), ClientError> {
        self.savepoint_round_trip(ClientMessage::RollbackToSavepoint { name: name.into() }, 1)
    }

    pub fn release_savepoint(&mut self, name: impl Into<String>) -> Result<(), ClientError> {
        self.savepoint_round_trip(ClientMessage::ReleaseSavepoint { name: name.into() }, 2)
    }

    fn savepoint_round_trip(
        &mut self,
        request: ClientMessage,
        kind: u8,
    ) -> Result<(), ClientError> {
        if self.has_active_stream() {
            return Err(ClientError::CommandsOutOfSync);
        }
        if !self.transaction_active()? {
            return Err(ClientError::TransactionState("no transaction is active"));
        }
        let expected_name = match &request {
            ClientMessage::CreateSavepoint { name }
            | ClientMessage::RollbackToSavepoint { name }
            | ClientMessage::ReleaseSavepoint { name } => name.clone(),
            _ => return Err(self.poison_unexpected("invalid local savepoint request")),
        };
        self.send(&request)?;
        match (kind, self.receive()?) {
            (0, ServerMessage::SavepointCreated { name })
            | (1, ServerMessage::SavepointRolledBack { name })
            | (2, ServerMessage::SavepointReleased { name })
                if name == expected_name =>
            {
                Ok(())
            }
            (_, ServerMessage::Error(error)) => Err(ClientError::Server(error)),
            _ => Err(self.poison_unexpected("server did not complete savepoint operation")),
        }
    }

    pub fn close_database(&mut self, database: impl Into<String>) -> Result<(), ClientError> {
        if self.has_active_stream() {
            return Err(ClientError::CommandsOutOfSync);
        }
        if self.transaction_active()? {
            return Err(ClientError::TransactionState(
                "cannot close a database while a transaction is active",
            ));
        }
        let database = database.into();
        self.send(&ClientMessage::CloseDatabase {
            database: database.clone(),
        })?;
        match self.receive()? {
            ServerMessage::DatabaseClosed { database: actual } if actual == database => Ok(()),
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => Err(self.poison_unexpected("server did not close database")),
        }
    }

    pub fn transaction_state(&self) -> TransactionState {
        self.transaction
    }

    pub fn transaction_active(&self) -> Result<bool, ClientError> {
        match self.transaction {
            TransactionState::Inactive => Ok(false),
            TransactionState::Active => Ok(true),
            TransactionState::Unknown => Err(ClientError::ConnectionPoisoned),
        }
    }

    /// Conservative compatibility accessor: `true` means active *or unknown*.
    pub fn in_transaction(&self) -> bool {
        self.transaction != TransactionState::Inactive
    }

    pub fn capabilities(&self) -> &[ProtocolCapability] {
        &self.capabilities
    }

    /// Whether an incomplete transport round trip left the server outcome
    /// unknown. Poisoned connections must be closed instead of pooled/reused.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Whether this connection can safely be returned to a pool.
    pub fn is_reusable(&self) -> bool {
        !self.poisoned
            && !self.closed
            && !self.has_active_stream()
            && self.transaction == TransactionState::Inactive
    }

    fn finish_transaction(
        &mut self,
        request: ClientMessage,
        expected: ServerMessage,
        unexpected: &'static str,
    ) -> Result<(), ClientError> {
        if self.has_active_stream() {
            return Err(ClientError::CommandsOutOfSync);
        }
        if !self.transaction_active()? {
            return Err(ClientError::TransactionState("no transaction is active"));
        }
        self.send(&request)?;
        match self.receive()? {
            response if response == expected => {
                self.transaction = TransactionState::Inactive;
                Ok(())
            }
            ServerMessage::TransactionFailed { failure, active } => {
                self.transaction = if active {
                    TransactionState::Active
                } else {
                    TransactionState::Inactive
                };
                Err(ClientError::Server(failure))
            }
            ServerMessage::Error(error) => Err(ClientError::Server(error)),
            _ => Err(self.poison_unexpected(unexpected)),
        }
    }

    fn send(&mut self, message: &ClientMessage) -> Result<(), ClientError> {
        if self.closed {
            return Err(ClientError::ConnectionClosed);
        }
        if self.poisoned {
            return Err(ClientError::ConnectionPoisoned);
        }
        match write_frame(&mut self.stream, message, self.max_frame_bytes) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.poisoned = true;
                self.transaction = TransactionState::Unknown;
                Err(error.into())
            }
        }
    }

    fn receive(&mut self) -> Result<ServerMessage, ClientError> {
        if self.closed {
            return Err(ClientError::ConnectionClosed);
        }
        if self.poisoned {
            return Err(ClientError::ConnectionPoisoned);
        }
        match read_frame(&mut self.stream, self.max_frame_bytes) {
            Ok(message) => Ok(message),
            Err(error) => {
                self.poisoned = true;
                self.transaction = TransactionState::Unknown;
                Err(error.into())
            }
        }
    }

    fn poison_unexpected(&mut self, message: &'static str) -> ClientError {
        self.poisoned = true;
        self.transaction = TransactionState::Unknown;
        ClientError::UnexpectedResponse(message)
    }

    fn has_active_stream(&self) -> bool {
        self.active_cursor.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    struct ScriptedStream {
        input: Cursor<Vec<u8>>,
        output: Vec<u8>,
    }

    impl Read for ScriptedStream {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.input.read(buffer)
        }
    }

    impl Write for ScriptedStream {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.output.extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn r6_l01_b_unexpected_blocking_response_poisons_connection() {
        let mut input = Vec::new();
        write_frame(
            &mut input,
            &ServerMessage::HandshakeAccepted {
                protocol_version: PROTOCOL_VERSION,
                max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
                capabilities: Vec::new(),
            },
            DEFAULT_MAX_FRAME_BYTES,
        )
        .unwrap();
        write_frame(
            &mut input,
            &ServerMessage::CommandComplete {
                affected_rows: 0,
                last_insert_id: 0,
            },
            DEFAULT_MAX_FRAME_BYTES,
        )
        .unwrap();

        let stream = ScriptedStream {
            input: Cursor::new(input),
            output: Vec::new(),
        };
        let mut connection = Connection::from_stream(stream).unwrap();
        assert!(matches!(
            connection.server_status(),
            Err(ClientError::UnexpectedResponse(_))
        ));
        assert!(connection.is_poisoned());
        assert!(!connection.is_reusable());
        assert!(
            connection.in_transaction(),
            "unknown must not look inactive"
        );
        assert!(matches!(
            connection.transaction_active(),
            Err(ClientError::ConnectionPoisoned)
        ));
        assert!(matches!(
            connection.server_status(),
            Err(ClientError::ConnectionPoisoned)
        ));
    }

    #[test]
    fn malformed_blocking_row_batch_is_rejected_and_poisoned() {
        let mut input = Vec::new();
        for message in [
            ServerMessage::HandshakeAccepted {
                protocol_version: PROTOCOL_VERSION,
                max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
                capabilities: Vec::new(),
            },
            ServerMessage::CursorOpened {
                cursor_id: 9,
                columns: vec![Column {
                    name: "id".to_string(),
                    type_name: "INTEGER".to_string(),
                    nullable: false,
                    external_type: None,
                }],
            },
            ServerMessage::RowBatch {
                cursor_id: 9,
                rows: vec![Row { values: Vec::new() }],
                eof: true,
            },
        ] {
            write_frame(&mut input, &message, DEFAULT_MAX_FRAME_BYTES).unwrap();
        }
        let stream = ScriptedStream {
            input: Cursor::new(input),
            output: Vec::new(),
        };
        let mut connection = Connection::from_stream(stream).unwrap();
        let ExecuteResult::Cursor(cursor) = connection.execute("SELECT id FROM t").unwrap() else {
            panic!("cursor expected")
        };
        assert!(matches!(
            connection.fetch(&cursor),
            Err(ClientError::Protocol(ProtocolError::InvalidBatchShape(_)))
        ));
        assert!(connection.is_poisoned());
    }

    #[test]
    fn terminal_blocking_cursor_failure_releases_the_local_cursor() {
        let mut input = Vec::new();
        for message in [
            ServerMessage::HandshakeAccepted {
                protocol_version: PROTOCOL_VERSION,
                max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
                capabilities: Vec::new(),
            },
            ServerMessage::CursorOpened {
                cursor_id: 19,
                columns: vec![Column {
                    name: "id".to_string(),
                    type_name: "INTEGER".to_string(),
                    nullable: false,
                    external_type: None,
                }],
            },
            ServerMessage::CursorFailed {
                cursor_id: 19,
                failure: ProtocolFailure {
                    code: ProtocolErrorCode::ServerError,
                    message: "terminal storage failure".to_string(),
                },
                active: false,
            },
        ] {
            write_frame(&mut input, &message, DEFAULT_MAX_FRAME_BYTES).unwrap();
        }
        let stream = ScriptedStream {
            input: Cursor::new(input),
            output: Vec::new(),
        };
        let mut connection = Connection::from_stream(stream).unwrap();
        let ExecuteResult::Cursor(cursor) = connection.execute("SELECT id FROM t").unwrap() else {
            panic!("cursor expected")
        };
        assert!(matches!(
            connection.fetch(&cursor),
            Err(ClientError::Server(_))
        ));
        assert!(!connection.discard_active_cursor().unwrap());
        assert!(connection.is_reusable());
    }

    #[test]
    fn prepared_statement_handle_cannot_cross_connection_owners() {
        let handshake = ServerMessage::HandshakeAccepted {
            protocol_version: PROTOCOL_VERSION,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            capabilities: Vec::new(),
        };
        let connection = |message: &ServerMessage| {
            let mut input = Vec::new();
            write_frame(&mut input, message, DEFAULT_MAX_FRAME_BYTES).unwrap();
            Connection::from_stream(ScriptedStream {
                input: Cursor::new(input),
                output: Vec::new(),
            })
            .unwrap()
        };
        let first = connection(&handshake);
        let mut second = connection(&handshake);
        let foreign = PreparedStatement {
            id: 1,
            owner_id: first.owner_id,
        };
        assert!(matches!(
            second.execute_prepared(&foreign, Vec::new()),
            Err(ClientError::PreparedStatementOwnerMismatch)
        ));
        assert!(matches!(
            second.close_prepared(foreign),
            Err(ClientError::PreparedStatementOwnerMismatch)
        ));
        assert!(second.is_reusable());
    }

    #[test]
    fn rollback_failure_uses_server_reported_inactive_state() {
        let mut input = Vec::new();
        for message in [
            ServerMessage::HandshakeAccepted {
                protocol_version: PROTOCOL_VERSION,
                max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
                capabilities: Vec::new(),
            },
            ServerMessage::TransactionBegan,
            ServerMessage::TransactionFailed {
                failure: ProtocolFailure {
                    code: ProtocolErrorCode::SqlError,
                    message: "rollback cleanup failed".to_string(),
                },
                active: false,
            },
        ] {
            write_frame(&mut input, &message, DEFAULT_MAX_FRAME_BYTES).unwrap();
        }
        let stream = ScriptedStream {
            input: Cursor::new(input),
            output: Vec::new(),
        };
        let mut connection = Connection::from_stream(stream).unwrap();

        connection.begin().unwrap();
        assert!(matches!(connection.rollback(), Err(ClientError::Server(_))));
        assert!(!connection.in_transaction());
        assert!(connection.is_reusable());
    }

    #[test]
    fn client_retry_classifier_requires_explicit_server_identity() {
        let retryable = ClientError::Server(ProtocolFailure {
            code: ProtocolErrorCode::CompactionBackpressure,
            message: "retry later".to_string(),
        });
        assert!(retryable.is_retryable());
        assert!(!ClientError::Server(ProtocolFailure {
            code: ProtocolErrorCode::SqlError,
            message: "COMPACTION_BACKPRESSURE in unstructured text".to_string(),
        })
        .is_retryable());
        assert!(!ClientError::Timeout {
            operation: TimeoutOperation::Read,
        }
        .is_retryable());
    }
}
