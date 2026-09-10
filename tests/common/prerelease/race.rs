#![cfg(all(feature = "stress-tests", feature = "test-failpoints"))]

use std::{
    collections::BTreeMap,
    net::{Shutdown, SocketAddr, TcpStream},
    sync::Arc,
    time::Duration,
};

use radixdb::test_failpoints::{InterleaveArrival, InterleaveController, InterleavePoint};
use radixdb_client::protocol::{read_frame, write_frame, ClientMessage, ServerMessage};
use radixdb_client::{ProtocolCapability, TransactionIsolation, WireValue, PROTOCOL_VERSION};

/// Deterministic scheduler for prerelease race profiles. Production threads
/// report named boundaries and the test releases one exact arrival at a time.
pub struct DeterministicScheduler {
    controller: Arc<InterleaveController>,
    timeout: Duration,
    steps: Vec<(InterleavePoint, i64)>,
}

impl DeterministicScheduler {
    pub fn new(controller: Arc<InterleaveController>) -> Self {
        Self {
            controller,
            timeout: Duration::from_secs(10),
            steps: Vec::new(),
        }
    }

    pub fn wait(&mut self, point: InterleavePoint, subject: Option<i64>) -> InterleaveArrival {
        let arrival = self
            .controller
            .wait_for(point, subject, self.timeout)
            .unwrap_or_else(|error| panic!("deterministic schedule stalled: {error}"));
        self.steps.push((arrival.point, arrival.subject));
        arrival
    }

    pub fn release(&self, arrival: InterleaveArrival) {
        self.controller.release(arrival);
    }

    pub fn step(&mut self, point: InterleavePoint, subject: Option<i64>) {
        let arrival = self.wait(point, subject);
        self.release(arrival);
    }

    pub fn trace(&self) -> &[(InterleavePoint, i64)] {
        &self.steps
    }
}

/// Minimal raw wire client used only to cut the transport after COMMIT was
/// sent but before its acknowledgement was observed.
pub struct RawProtocolClient {
    stream: TcpStream,
    max_frame_bytes: u32,
    next_request_id: u64,
}

impl RawProtocolClient {
    pub fn connect(address: SocketAddr, database: &str) -> Result<Self, String> {
        let stream = TcpStream::connect_timeout(&address, Duration::from_secs(3))
            .map_err(|error| error.to_string())?;
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .map_err(|error| error.to_string())?;
        stream
            .set_write_timeout(Some(Duration::from_secs(10)))
            .map_err(|error| error.to_string())?;
        stream
            .set_nodelay(true)
            .map_err(|error| error.to_string())?;
        let mut client = Self {
            stream,
            max_frame_bytes: radixdb_client::protocol::DEFAULT_MAX_FRAME_BYTES,
            next_request_id: 1,
        };
        client.send(&ClientMessage::Handshake {
            protocol_version: PROTOCOL_VERSION,
            max_frame_bytes: client.max_frame_bytes,
            capabilities: vec![ProtocolCapability::BuildIdentityV1],
        })?;
        match client.receive()? {
            ServerMessage::HandshakeAccepted {
                protocol_version,
                max_frame_bytes,
                ..
            } if protocol_version == PROTOCOL_VERSION => {
                client.max_frame_bytes = max_frame_bytes;
            }
            other => return Err(format!("unexpected handshake response: {other:?}")),
        }
        client.send(&ClientMessage::Authenticate {
            login: "root".to_string(),
            password: None,
        })?;
        if client.receive()? != ServerMessage::AuthenticationAccepted {
            return Err("authentication was not accepted".to_string());
        }
        client.send(&ClientMessage::SelectDatabase {
            database: database.to_string(),
        })?;
        match client.receive()? {
            ServerMessage::DatabaseSelected { database: selected }
                if selected.eq_ignore_ascii_case(database) => {}
            other => return Err(format!("unexpected database response: {other:?}")),
        }
        Ok(client)
    }

    pub fn begin(&mut self) -> Result<(), String> {
        self.send(&ClientMessage::BeginTransaction {
            isolation: TransactionIsolation::ReadCommitted,
        })?;
        match self.receive()? {
            ServerMessage::TransactionBegan => Ok(()),
            other => Err(format!("unexpected BEGIN response: {other:?}")),
        }
    }

    pub fn execute(&mut self, sql: impl Into<String>) -> Result<(), String> {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        self.send(&ClientMessage::Execute {
            request_id,
            sql: sql.into(),
            positional: Vec::<WireValue>::new(),
            named: BTreeMap::new(),
        })?;
        match self.receive()? {
            ServerMessage::CommandComplete { .. } => Ok(()),
            ServerMessage::Error(error) => Err(error.message),
            other => Err(format!("unexpected execute response: {other:?}")),
        }
    }

    pub fn send_commit(&mut self) -> Result<(), String> {
        self.send(&ClientMessage::CommitTransaction)
    }

    pub fn cut_transport(&self) -> Result<(), String> {
        self.stream
            .shutdown(Shutdown::Both)
            .map_err(|error| error.to_string())
    }

    fn send(&mut self, message: &ClientMessage) -> Result<(), String> {
        write_frame(&mut self.stream, message, self.max_frame_bytes)
            .map_err(|error| error.to_string())
    }

    fn receive(&mut self) -> Result<ServerMessage, String> {
        read_frame(&mut self.stream, self.max_frame_bytes).map_err(|error| error.to_string())
    }
}
