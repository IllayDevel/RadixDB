use std::{
    collections::{BTreeMap, HashMap},
    io::{self, Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{
    auth::BasicAuth,
    status::{EventBuffer, RunEvent, StatusSnapshot, WatchdogState},
};

const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_CONNECTIONS: usize = 16;
const AUTH_FAILURE_LIMIT: u32 = 10;
const AUTH_WINDOW: Duration = Duration::from_secs(60);
const DASHBOARD: &str = include_str!("../assets/index.html");

#[derive(Clone)]
pub struct StatusHub {
    snapshot: Arc<RwLock<StatusSnapshot>>,
    events: Arc<Mutex<EventBuffer>>,
}

impl StatusHub {
    pub fn new(snapshot: StatusSnapshot, event_capacity: usize) -> Result<Self, String> {
        snapshot.validate()?;
        Ok(Self {
            snapshot: Arc::new(RwLock::new(snapshot)),
            events: Arc::new(Mutex::new(EventBuffer::new(event_capacity)?)),
        })
    }

    pub fn snapshot(&self) -> StatusSnapshot {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or_else(
                |_| self.snapshot.read().unwrap().updated_unix_millis,
                |value| value.as_millis().min(u128::from(u64::MAX)) as u64,
            );
        self.snapshot_at(now)
    }

    pub fn snapshot_at(&self, unix_millis: u64) -> StatusSnapshot {
        let mut snapshot = self.snapshot.read().unwrap().clone();
        let now = unix_millis.max(snapshot.updated_unix_millis);
        snapshot.watchdog_silence_millis = now.saturating_sub(snapshot.last_progress_unix_millis);
        snapshot.watchdog_state = if snapshot.state.is_terminal() {
            WatchdogState::Terminal
        } else if snapshot.watchdog_silence_millis > snapshot.watchdog_timeout_millis {
            WatchdogState::Stalled
        } else {
            WatchdogState::Healthy
        };
        snapshot
    }

    pub fn update(&self, operation: impl FnOnce(&mut StatusSnapshot)) -> Result<(), String> {
        let mut snapshot = self.snapshot.write().unwrap();
        let predecessor = snapshot.clone();
        operation(&mut snapshot);
        if let Err(error) = snapshot.validate() {
            *snapshot = predecessor;
            return Err(error);
        }
        Ok(())
    }

    pub fn push_event(
        &self,
        unix_millis: u64,
        kind: &str,
        detail: &str,
    ) -> Result<RunEvent, String> {
        self.events.lock().unwrap().push(unix_millis, kind, detail)
    }

    fn events_after(&self, sequence: u64) -> Vec<RunEvent> {
        self.events.lock().unwrap().after(sequence)
    }
}

pub struct StatusServer {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<io::Result<()>>>,
}

impl StatusServer {
    pub fn start(bind: SocketAddr, auth: BasicAuth, hub: StatusHub) -> io::Result<Self> {
        let listener = TcpListener::bind(bind)?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let active = Arc::new(AtomicUsize::new(0));
        let worker = thread::Builder::new()
            .name("radixdb-soak-http".into())
            .spawn(move || serve(listener, auth, hub, worker_stop, active))?;
        Ok(Self {
            address,
            stop,
            worker: Some(worker),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }

    pub fn shutdown(mut self) -> io::Result<()> {
        self.stop.store(true, Ordering::Release);
        let wake = if self.address.ip().is_unspecified() {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), self.address.port())
        } else {
            self.address
        };
        let _ = TcpStream::connect_timeout(&wake, Duration::from_millis(100));
        match self.worker.take().unwrap().join() {
            Ok(result) => result,
            Err(_) => Err(io::Error::other("status server thread panicked")),
        }
    }
}

impl Drop for StatusServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

#[derive(Clone, Copy)]
struct AuthWindowState {
    started: Instant,
    failures: u32,
}

fn serve(
    listener: TcpListener,
    auth: BasicAuth,
    hub: StatusHub,
    stop: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
) -> io::Result<()> {
    let auth = Arc::new(auth);
    let limits = Arc::new(Mutex::new(HashMap::<IpAddr, AuthWindowState>::new()));
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, peer)) => {
                if active
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                        (value < MAX_CONNECTIONS).then_some(value + 1)
                    })
                    .is_err()
                {
                    let _ = respond_busy(stream);
                    continue;
                }
                let auth = Arc::clone(&auth);
                let hub = hub.clone();
                let limits = Arc::clone(&limits);
                let active = Arc::clone(&active);
                thread::Builder::new()
                    .name("radixdb-soak-http-client".into())
                    .spawn(move || {
                        let _guard = ActiveConnection(active);
                        let _ = handle_connection(stream, peer.ip(), &auth, &hub, &limits);
                    })?;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

struct ActiveConnection(Arc<AtomicUsize>);

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn handle_connection(
    mut stream: TcpStream,
    peer: IpAddr,
    auth: &BasicAuth,
    hub: &StatusHub,
    limits: &Mutex<HashMap<IpAddr, AuthWindowState>>,
) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let request = match read_request(&mut stream) {
        Ok(request) => request,
        Err(_) => {
            return write_response(
                &mut stream,
                "400 Bad Request",
                "text/plain",
                b"bad request\n",
                false,
                &[],
            )
        }
    };
    if is_rate_limited(limits, peer) {
        return write_response(
            &mut stream,
            "429 Too Many Requests",
            "text/plain",
            b"too many authentication failures\n",
            request.head,
            &[("Retry-After", "60")],
        );
    }
    if !auth.accepts_header(request.headers.get("authorization").map(String::as_str)) {
        record_auth_failure(limits, peer);
        let _ = hub.update(|status| {
            status.counters.auth_failures = status.counters.auth_failures.saturating_add(1);
        });
        return write_response(
            &mut stream,
            "401 Unauthorized",
            "text/plain",
            b"authentication required\n",
            request.head,
            &[("WWW-Authenticate", "Basic realm=\"RadixDB soak\"")],
        );
    }
    clear_auth_failures(limits, peer);
    route(&mut stream, request, hub)
}

struct Request {
    method: String,
    path: String,
    query: Option<String>,
    headers: BTreeMap<String, String>,
    head: bool,
}

fn read_request(stream: &mut TcpStream) -> io::Result<Request> {
    let mut buffer = Vec::with_capacity(1024);
    let mut byte = [0u8; 1024];
    while !buffer.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut byte)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "request ended",
            ));
        }
        buffer.extend_from_slice(&byte[..read]);
        if buffer.len() > MAX_HEADER_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "headers too large",
            ));
        }
    }
    let text = std::str::from_utf8(&buffer)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "headers are not UTF-8"))?;
    let mut lines = text.split("\r\n");
    let mut first = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request line"))?
        .split_ascii_whitespace();
    let method = first.next().unwrap_or_default().to_string();
    let target = first.next().unwrap_or_default();
    let version = first.next().unwrap_or_default();
    if first.next().is_some()
        || method.is_empty()
        || !target.starts_with('/')
        || !matches!(version, "HTTP/1.0" | "HTTP/1.1")
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid request line",
        ));
    }
    let (path, query) = target
        .split_once('?')
        .map_or((target, None), |(path, query)| {
            (path, Some(query.to_string()))
        });
    let mut headers = BTreeMap::new();
    for line in lines.take_while(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid header"))?;
        let name = name.trim().to_ascii_lowercase();
        if name.is_empty() || headers.insert(name, value.trim().to_string()).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "duplicate or empty header",
            ));
        }
    }
    Ok(Request {
        head: method == "HEAD",
        method,
        path: path.to_string(),
        query,
        headers,
    })
}

fn route(stream: &mut TcpStream, request: Request, hub: &StatusHub) -> io::Result<()> {
    if !matches!(request.method.as_str(), "GET" | "HEAD") {
        return write_response(
            stream,
            "405 Method Not Allowed",
            "text/plain",
            b"read-only endpoint\n",
            request.head,
            &[("Allow", "GET, HEAD")],
        );
    }
    let (content_type, body) = match request.path.as_str() {
        "/" => ("text/html; charset=utf-8", DASHBOARD.as_bytes().to_vec()),
        "/healthz" => ("text/plain; charset=utf-8", b"ok\n".to_vec()),
        "/api/v1/status" => (
            "application/json",
            serde_json::to_vec(&hub.snapshot()).map_err(io::Error::other)?,
        ),
        "/api/v1/metrics" => {
            let status = hub.snapshot();
            let value = serde_json::json!({
                "counters": status.counters,
                "latency": status.latency,
                "resources": status.resources,
                "resource_slopes": status.resource_slopes,
                "server_runtime": status.server_runtime,
                "active_clients": status.active_clients,
                "target_clients": status.target_clients,
                "current_tps": status.current_tps,
            });
            (
                "application/json",
                serde_json::to_vec(&value).map_err(io::Error::other)?,
            )
        }
        "/api/v1/invariants" => (
            "application/json",
            serde_json::to_vec(&hub.snapshot().invariants).map_err(io::Error::other)?,
        ),
        "/api/v1/events" => {
            let after = parse_after(request.query.as_deref())?;
            (
                "application/json",
                serde_json::to_vec(&hub.events_after(after)).map_err(io::Error::other)?,
            )
        }
        "/metrics" => (
            "text/plain; version=0.0.4; charset=utf-8",
            prometheus(&hub.snapshot()).into_bytes(),
        ),
        _ => {
            return write_response(
                stream,
                "404 Not Found",
                "text/plain",
                b"not found\n",
                request.head,
                &[],
            )
        }
    };
    write_response(stream, "200 OK", content_type, &body, request.head, &[])
}

fn parse_after(query: Option<&str>) -> io::Result<u64> {
    let Some(query) = query else {
        return Ok(0);
    };
    let Some(value) = query.strip_prefix("after=") else {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid query"));
    };
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid sequence",
        ));
    }
    value
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "sequence overflow"))
}

fn prometheus(status: &StatusSnapshot) -> String {
    format!(
        concat!(
            "# TYPE radixdb_soak_up gauge\n",
            "radixdb_soak_up 1\n",
            "# TYPE radixdb_soak_operations_total counter\n",
            "radixdb_soak_operations_total {}\n",
            "# TYPE radixdb_soak_commits_total counter\n",
            "radixdb_soak_commits_total {}\n",
            "# TYPE radixdb_soak_invariant_failures_total counter\n",
            "radixdb_soak_invariant_failures_total {}\n",
            "# TYPE radixdb_soak_checkpoint_deferred_total counter\n",
            "radixdb_soak_checkpoint_deferred_total {}\n",
            "# TYPE radixdb_soak_active_clients gauge\n",
            "radixdb_soak_active_clients {}\n",
            "# TYPE radixdb_soak_tps gauge\n",
            "radixdb_soak_tps {}\n",
            "# TYPE radixdb_soak_rss_bytes gauge\n",
            "radixdb_soak_rss_bytes {}\n",
            "# TYPE radixdb_soak_database_bytes gauge\n",
            "radixdb_soak_database_bytes {}\n",
            "# TYPE radixdb_soak_database_data_bytes gauge\n",
            "radixdb_soak_database_data_bytes {}\n",
            "# TYPE radixdb_soak_database_index_bytes gauge\n",
            "radixdb_soak_database_index_bytes {}\n",
            "# TYPE radixdb_soak_database_metadata_bytes gauge\n",
            "radixdb_soak_database_metadata_bytes {}\n",
            "# TYPE radixdb_soak_database_wal_bytes gauge\n",
            "radixdb_soak_database_wal_bytes {}\n",
            "# TYPE radixdb_soak_database_other_bytes gauge\n",
            "radixdb_soak_database_other_bytes {}\n",
            "# TYPE radixdb_soak_server_connections gauge\n",
            "radixdb_soak_server_connections {}\n"
        ),
        status.counters.operations,
        status.counters.transactions_committed,
        status.counters.invariant_failures,
        status.counters.checkpoint_deferred,
        status.active_clients,
        status.current_tps,
        status.resources.rss_bytes,
        status.resources.database_bytes,
        status.resources.database_data_bytes,
        status.resources.database_index_bytes,
        status.resources.database_metadata_bytes,
        status.resources.database_wal_bytes,
        status.resources.database_other_bytes,
        status.server_runtime.active_connections,
    )
}

fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
    head: bool,
    extra_headers: &[(&str, &str)],
) -> io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\n",
        body.len()
    )?;
    for (name, value) in extra_headers {
        write!(stream, "{name}: {value}\r\n")?;
    }
    write!(stream, "\r\n")?;
    if !head {
        stream.write_all(body)?;
    }
    stream.flush()
}

fn respond_busy(mut stream: TcpStream) -> io::Result<()> {
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    write_response(
        &mut stream,
        "503 Service Unavailable",
        "text/plain",
        b"status server busy\n",
        false,
        &[("Retry-After", "1")],
    )
}

fn is_rate_limited(limits: &Mutex<HashMap<IpAddr, AuthWindowState>>, peer: IpAddr) -> bool {
    let mut limits = limits.lock().unwrap();
    let Some(state) = limits.get_mut(&peer) else {
        return false;
    };
    if state.started.elapsed() >= AUTH_WINDOW {
        limits.remove(&peer);
        return false;
    }
    state.failures >= AUTH_FAILURE_LIMIT
}

fn record_auth_failure(limits: &Mutex<HashMap<IpAddr, AuthWindowState>>, peer: IpAddr) {
    let mut limits = limits.lock().unwrap();
    let now = Instant::now();
    let state = limits.entry(peer).or_insert(AuthWindowState {
        started: now,
        failures: 0,
    });
    if state.started.elapsed() >= AUTH_WINDOW {
        *state = AuthWindowState {
            started: now,
            failures: 0,
        };
    }
    state.failures = state.failures.saturating_add(1);
}

fn clear_auth_failures(limits: &Mutex<HashMap<IpAddr, AuthWindowState>>, peer: IpAddr) {
    limits.lock().unwrap().remove(&peer);
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, io::Read};

    use base64::{engine::general_purpose::STANDARD, Engine};

    use super::*;
    use crate::status::{
        BuildIdentity, Counters, LatencySnapshot, ResourceSlopes, ResourceSnapshot, RunState,
        ServerRuntimeSnapshot, WatchdogState,
    };

    fn snapshot() -> StatusSnapshot {
        StatusSnapshot {
            format: 1,
            run_id: "test-run".into(),
            profile: "smoke".into(),
            seed: 7,
            state: RunState::Running,
            phase: "workload".into(),
            started_unix_millis: 1,
            updated_unix_millis: 2,
            elapsed_millis: 1,
            remaining_millis: 9,
            last_progress_unix_millis: 2,
            watchdog_timeout_millis: 60_000,
            watchdog_silence_millis: 0,
            watchdog_state: WatchdogState::Healthy,
            active_clients: 1,
            target_clients: 1,
            current_tps: 2.0,
            identity: BuildIdentity {
                soak: "soak".into(),
                server: Some("server".into()),
                cargo_lock_sha256: "a".repeat(64),
            },
            counters: Counters::default(),
            latency: LatencySnapshot::default(),
            resources: ResourceSnapshot::default(),
            resource_slopes: ResourceSlopes::default(),
            server_runtime: ServerRuntimeSnapshot::default(),
            invariants: BTreeMap::new(),
            logical_digest: None,
            failure: None,
        }
    }

    fn request(address: SocketAddr, method: &str, path: &str, auth: Option<&str>) -> String {
        let mut stream = TcpStream::connect(address).unwrap();
        write!(stream, "{method} {path} HTTP/1.1\r\nHost: localhost\r\n").unwrap();
        if let Some(auth) = auth {
            write!(stream, "Authorization: {auth}\r\n").unwrap();
        }
        write!(stream, "Connection: close\r\n\r\n").unwrap();
        stream.shutdown(std::net::Shutdown::Write).unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
    }

    #[test]
    fn server_requires_auth_and_is_read_only() {
        let auth = BasicAuth::parse(
            "RADIXDB_SOAK_HTTP_USER=observer\nRADIXDB_SOAK_HTTP_PASSWORD=secret\n",
        )
        .unwrap();
        let hub = StatusHub::new(snapshot(), 16).unwrap();
        hub.push_event(2, "started", "ok").unwrap();
        let server = StatusServer::start("127.0.0.1:0".parse().unwrap(), auth, hub).unwrap();
        let address = server.local_addr();
        let header = format!("Basic {}", STANDARD.encode("observer:secret"));

        assert!(request(address, "GET", "/api/v1/status", None).starts_with("HTTP/1.1 401"));
        let status = request(address, "GET", "/api/v1/status", Some(&header));
        assert!(status.starts_with("HTTP/1.1 200"));
        assert!(status.contains("\"run_id\":\"test-run\""));
        assert!(
            request(address, "POST", "/api/v1/status", Some(&header)).starts_with("HTTP/1.1 405")
        );
        assert!(
            request(address, "GET", "/api/v1/events?after=0", Some(&header))
                .contains("\"kind\":\"started\"")
        );
        let metrics = request(address, "GET", "/metrics", Some(&header));
        assert!(metrics.contains("radixdb_soak_database_data_bytes 0"));
        assert!(metrics.contains("radixdb_soak_database_index_bytes 0"));
        assert!(metrics.contains("radixdb_soak_database_metadata_bytes 0"));
        assert!(metrics.contains("radixdb_soak_database_wal_bytes 0"));
        server.shutdown().unwrap();
    }

    #[test]
    fn workload_dashboard_links_to_intelligent_diagnostics() {
        assert!(DASHBOARD.contains("Open intelligent diagnostics"));
        assert!(DASHBOARD.contains(":18089/"));
        assert!(DASHBOARD.contains("database_data_bytes"));
        assert!(DASHBOARD.contains("database_index_bytes"));
        assert!(DASHBOARD.contains("database_metadata_bytes"));
        assert!(DASHBOARD.contains("database_wal_bytes"));
    }

    #[test]
    fn heartbeat_updates_cannot_mask_semantic_stall() {
        let mut initial = snapshot();
        initial.started_unix_millis = 1_000;
        initial.updated_unix_millis = 1_000;
        initial.last_progress_unix_millis = 1_000;
        initial.watchdog_timeout_millis = 100;
        let hub = StatusHub::new(initial, 16).unwrap();

        hub.update(|status| status.updated_unix_millis = 1_200)
            .unwrap();
        let stalled = hub.snapshot_at(1_200);

        assert_eq!(stalled.updated_unix_millis, 1_200);
        assert_eq!(stalled.last_progress_unix_millis, 1_000);
        assert_eq!(stalled.watchdog_silence_millis, 200);
        assert_eq!(stalled.watchdog_state, WatchdogState::Stalled);
    }

    #[test]
    fn live_http_reports_frozen_workload_while_sampler_heartbeat_is_alive() {
        let auth = BasicAuth::parse(
            "RADIXDB_SOAK_HTTP_USER=observer\nRADIXDB_SOAK_HTTP_PASSWORD=secret\n",
        )
        .unwrap();
        let mut initial = snapshot();
        initial.started_unix_millis = 1_000;
        initial.updated_unix_millis = 1_000;
        initial.last_progress_unix_millis = 1_000;
        initial.watchdog_timeout_millis = 100;
        let hub = StatusHub::new(initial, 16).unwrap();
        let server =
            StatusServer::start("127.0.0.1:0".parse().unwrap(), auth, hub.clone()).unwrap();
        let header = format!("Basic {}", STANDARD.encode("observer:secret"));

        // This is a live sampler/agent heartbeat without any workload epoch.
        hub.update(|status| status.updated_unix_millis = 1_200)
            .unwrap();
        let response = request(server.local_addr(), "GET", "/api/v1/status", Some(&header));
        let body = response.split("\r\n\r\n").nth(1).unwrap();
        let status: StatusSnapshot = serde_json::from_str(body).unwrap();

        assert_eq!(status.updated_unix_millis, 1_200);
        assert_eq!(status.last_progress_unix_millis, 1_000);
        assert_eq!(status.watchdog_state, WatchdogState::Stalled);
        server.shutdown().unwrap();
    }
}
