use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, RwLock,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use serde::Serialize;

use crate::{
    auth::BasicAuth,
    diagnostics::{DiagnosticAlertV2, DiagnosticIncidentV2, DiagnosticMetricFrameV2},
};

const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_CONNECTIONS: usize = 16;
const DASHBOARD: &str = include_str!("../../assets/diagnostics.html");

#[derive(Clone, Debug, Default, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverHttpSnapshot {
    pub status: Option<serde_json::Value>,
    pub latest_metrics: Option<DiagnosticMetricFrameV2>,
    pub timeline: VecDeque<DiagnosticMetricFrameV2>,
    pub alerts: Vec<DiagnosticAlertV2>,
    pub incidents: Vec<DiagnosticIncidentV2>,
}

#[derive(Debug)]
struct ObserverHttpState {
    snapshot: ObserverHttpSnapshot,
    timeline_capacity: usize,
}

#[derive(Clone)]
pub struct ObserverHttpHub(Arc<RwLock<ObserverHttpState>>);

impl Default for ObserverHttpHub {
    fn default() -> Self {
        Self::with_timeline_capacity(512)
    }
}

impl ObserverHttpHub {
    pub fn with_timeline_capacity(timeline_capacity: usize) -> Self {
        Self(Arc::new(RwLock::new(ObserverHttpState {
            snapshot: ObserverHttpSnapshot::default(),
            timeline_capacity: timeline_capacity.max(1),
        })))
    }

    pub fn publish<T: Serialize>(
        &self,
        status: &T,
        latest: Option<&DiagnosticMetricFrameV2>,
        alerts: &[DiagnosticAlertV2],
        incidents: &[DiagnosticIncidentV2],
    ) -> Result<(), String> {
        let mut state = self.0.write().unwrap();
        let timeline_capacity = state.timeline_capacity;
        let snapshot = &mut state.snapshot;
        snapshot.status = Some(serde_json::to_value(status).map_err(|error| error.to_string())?);
        snapshot.latest_metrics = latest.cloned();
        if let Some(latest) = latest {
            let new_sequence = snapshot
                .timeline
                .back()
                .is_none_or(|previous| previous.sequence < latest.sequence);
            if new_sequence {
                if snapshot.timeline.len() == timeline_capacity {
                    snapshot.timeline.pop_front();
                }
                snapshot.timeline.push_back(latest.clone());
            }
        }
        if snapshot.alerts.as_slice() != alerts {
            snapshot.alerts.clear();
            snapshot.alerts.extend_from_slice(alerts);
        }
        if snapshot.incidents.as_slice() != incidents {
            snapshot.incidents.clear();
            snapshot.incidents.extend_from_slice(incidents);
        }
        Ok(())
    }

    pub fn seed_timeline(&self, frames: impl IntoIterator<Item = DiagnosticMetricFrameV2>) {
        let mut state = self.0.write().unwrap();
        let capacity = state.timeline_capacity;
        state.snapshot.timeline.clear();
        for frame in frames {
            if state.snapshot.timeline.len() == capacity {
                state.snapshot.timeline.pop_front();
            }
            state.snapshot.timeline.push_back(frame);
        }
    }

    fn status(&self) -> Option<serde_json::Value> {
        self.0.read().unwrap().snapshot.status.clone()
    }

    fn latest_metrics(&self) -> Option<DiagnosticMetricFrameV2> {
        self.0.read().unwrap().snapshot.latest_metrics.clone()
    }

    fn timeline(&self, window_seconds: Option<u64>) -> Vec<DiagnosticMetricFrameV2> {
        bounded_timeline(&self.0.read().unwrap().snapshot.timeline, window_seconds)
            .into_iter()
            .cloned()
            .collect()
    }

    fn alerts(&self) -> Vec<DiagnosticAlertV2> {
        self.0.read().unwrap().snapshot.alerts.clone()
    }

    fn incidents(&self) -> Vec<DiagnosticIncidentV2> {
        self.0.read().unwrap().snapshot.incidents.clone()
    }
}

pub struct ObserverHttpServer {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<io::Result<()>>>,
}

impl ObserverHttpServer {
    pub fn start(bind: SocketAddr, auth: BasicAuth, hub: ObserverHttpHub) -> io::Result<Self> {
        let listener = TcpListener::bind(bind)?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let active = Arc::new(AtomicUsize::new(0));
        let worker = thread::Builder::new()
            .name("radixdb-soak-observer-http".into())
            .spawn(move || serve(listener, Arc::new(auth), hub, worker_stop, active))?;
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
            Err(_) => Err(io::Error::other("observer HTTP thread panicked")),
        }
    }
}

impl Drop for ObserverHttpServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

fn serve(
    listener: TcpListener,
    auth: Arc<BasicAuth>,
    hub: ObserverHttpHub,
    stop: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
) -> io::Result<()> {
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => {
                if active
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                        (value < MAX_CONNECTIONS).then_some(value + 1)
                    })
                    .is_err()
                {
                    continue;
                }
                let auth = Arc::clone(&auth);
                let hub = hub.clone();
                let active = Arc::clone(&active);
                thread::Builder::new()
                    .name("radixdb-soak-observer-http-client".into())
                    .spawn(move || {
                        let _guard = ActiveConnection(active);
                        let _ = handle(stream, &auth, &hub);
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

fn handle(mut stream: TcpStream, auth: &BasicAuth, hub: &ObserverHttpHub) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let request = read_request(&mut stream)?;
    if !auth.accepts_header(request.authorization.as_deref()) {
        return respond(
            &mut stream,
            "401 Unauthorized",
            "text/plain",
            b"authentication required\n",
            request.head,
            &[("WWW-Authenticate", "Basic realm=\"RadixDB soak observer\"")],
        );
    }
    if !matches!(request.method.as_str(), "GET" | "HEAD") {
        return respond(
            &mut stream,
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
        "/api/v2/status" => json(&hub.status())?,
        "/api/v2/metrics" => json(&hub.latest_metrics())?,
        "/api/v2/timeline" => {
            let window = match parse_window(request.query.as_deref()) {
                Ok(window) => window,
                Err(error) => {
                    return respond(
                        &mut stream,
                        "400 Bad Request",
                        "text/plain",
                        format!("{error}\n").as_bytes(),
                        request.head,
                        &[],
                    );
                }
            };
            let timeline = hub.timeline(window);
            json(&timeline)?
        }
        "/api/v2/alerts" => json(&hub.alerts())?,
        "/api/v2/incidents" => json(&hub.incidents())?,
        "/metrics" => {
            let latest = hub.latest_metrics();
            (
                "text/plain; version=0.0.4; charset=utf-8",
                prometheus(latest.as_ref()).into_bytes(),
            )
        }
        _ => {
            return respond(
                &mut stream,
                "404 Not Found",
                "text/plain",
                b"not found\n",
                request.head,
                &[],
            )
        }
    };
    respond(
        &mut stream,
        "200 OK",
        content_type,
        &body,
        request.head,
        &[],
    )
}

struct Request {
    method: String,
    path: String,
    query: Option<String>,
    authorization: Option<String>,
    head: bool,
}

fn read_request(stream: &mut TcpStream) -> io::Result<Request> {
    let mut bytes = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut chunk)?;
        if read == 0 || bytes.len().saturating_add(read) > MAX_HEADER_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid HTTP headers",
            ));
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "headers are not UTF-8"))?;
    let mut lines = text.split("\r\n");
    let mut first = lines.next().unwrap_or_default().split_ascii_whitespace();
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
    let mut authorization = None;
    for line in lines.take_while(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid header"))?;
        if name.eq_ignore_ascii_case("authorization")
            && authorization.replace(value.trim().to_string()).is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "duplicate authorization header",
            ));
        }
    }
    Ok(Request {
        head: method == "HEAD",
        method,
        path: path.into(),
        query,
        authorization,
    })
}

fn parse_window(query: Option<&str>) -> io::Result<Option<u64>> {
    let Some(query) = query else {
        return Ok(None);
    };
    let Some(value) = query.strip_prefix("window=") else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "timeline accepts only the window parameter",
        ));
    };
    if value.contains('&') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "timeline accepts exactly one window parameter",
        ));
    }
    let seconds: u64 = value
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid timeline window"))?;
    if !(1..=1800).contains(&seconds) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "timeline window must be in 1..=1800 seconds",
        ));
    }
    Ok(Some(seconds))
}

fn bounded_timeline(
    timeline: &VecDeque<DiagnosticMetricFrameV2>,
    window_seconds: Option<u64>,
) -> Vec<&DiagnosticMetricFrameV2> {
    let Some(window_seconds) = window_seconds else {
        return timeline.iter().collect();
    };
    let latest = timeline.back().map_or(0, |frame| frame.monotonic_millis);
    let cutoff = latest.saturating_sub(window_seconds.saturating_mul(1_000));
    timeline
        .iter()
        .filter(|frame| frame.monotonic_millis >= cutoff)
        .collect()
}

fn json<T: Serialize>(value: &T) -> io::Result<(&'static str, Vec<u8>)> {
    Ok((
        "application/json",
        serde_json::to_vec(value).map_err(io::Error::other)?,
    ))
}

fn prometheus(frame: Option<&DiagnosticMetricFrameV2>) -> String {
    let mut output =
        String::from("# TYPE radixdb_soak_observer_up gauge\nradixdb_soak_observer_up 1\n");
    if let Some(frame) = frame {
        for (name, value) in &frame.gauges {
            let name = prometheus_name(name);
            output.push_str(&format!("radixdb_soak_{name} {value}\n"));
        }
        for (name, value) in &frame.counters {
            let name = prometheus_name(name);
            output.push_str(&format!("radixdb_soak_{name} {value}\n"));
        }
    }
    output
}

fn prometheus_name(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn respond(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
    head: bool,
    extra: &[(&str, &str)],
) -> io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\n",
        body.len()
    )?;
    for (name, value) in extra {
        write!(stream, "{name}: {value}\r\n")?;
    }
    write!(stream, "\r\n")?;
    if !head {
        stream.write_all(body)?;
    }
    stream.flush()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use base64::{engine::general_purpose::STANDARD, Engine};

    use super::*;
    use crate::diagnostics::{
        DiagnosticConfidence, DiagnosticSeverity, DiagnosticState, EvidenceSignal,
        HeartbeatSnapshot, SemanticProgressSnapshot, DIAGNOSTIC_FORMAT_V2,
    };

    fn metric_frame(sequence: u64, monotonic_millis: u64) -> DiagnosticMetricFrameV2 {
        DiagnosticMetricFrameV2 {
            format: DIAGNOSTIC_FORMAT_V2,
            sequence,
            monotonic_millis,
            unix_millis: monotonic_millis,
            boot_id: "boot".into(),
            run_id: "run".into(),
            heartbeats: HeartbeatSnapshot::default(),
            progress: SemanticProgressSnapshot {
                format: DIAGNOSTIC_FORMAT_V2,
                sequence: 1,
                phase_epoch: 1,
                phase: "clients-16".into(),
                phase_started_unix_millis: 1,
                workload_epoch: 1,
                workload_units: 1,
                last_workload_progress_unix_millis: 1,
                operation_epoch: 0,
                active_operation: None,
                last_operation_progress_unix_millis: 1,
                planned_silence: None,
            },
            counters: BTreeMap::new(),
            gauges: BTreeMap::new(),
        }
    }

    #[test]
    fn observer_http_is_authenticated_and_read_only() {
        let auth = BasicAuth::parse(
            "RADIXDB_SOAK_HTTP_USER=observer\nRADIXDB_SOAK_HTTP_PASSWORD=secret\n",
        )
        .unwrap();
        let hub = ObserverHttpHub::default();
        let mut frame = metric_frame(1, 1);
        frame
            .gauges
            .insert("progress.actual_units_per_second".into(), 42.0);
        let alert = DiagnosticAlertV2 {
            format: DIAGNOSTIC_FORMAT_V2,
            id: "storage-bound-1".into(),
            kind: "storage_bound_engine".into(),
            severity: DiagnosticSeverity::Incident,
            confidence: DiagnosticConfidence::Probable,
            state: DiagnosticState::Active,
            first_unix_millis: 1,
            last_unix_millis: 1,
            first_frame_sequence: 1,
            last_frame_sequence: 1,
            evidence: vec![EvidenceSignal {
                name: "storage_pressure".into(),
                observed: 99.0,
                expected: Some(10.0),
                unit: "psi".into(),
                detail: "I/O pressure is elevated".into(),
            }],
            counter_evidence: Vec::new(),
        };
        hub.publish(
            &serde_json::json!({"state":"running"}),
            Some(&frame),
            std::slice::from_ref(&alert),
            &[],
        )
        .unwrap();
        let server = ObserverHttpServer::start("127.0.0.1:0".parse().unwrap(), auth, hub).unwrap();
        let header = format!("Basic {}", STANDARD.encode("observer:secret"));
        assert!(
            request(server.local_addr(), "GET", "/api/v2/status", None).starts_with("HTTP/1.1 401")
        );
        assert!(
            request(server.local_addr(), "GET", "/api/v2/status", Some(&header))
                .contains("\"state\":\"running\"")
        );
        assert!(
            request(server.local_addr(), "GET", "/api/v2/metrics", Some(&header))
                .contains("progress.actual_units_per_second")
        );
        assert!(
            request(server.local_addr(), "GET", "/api/v2/alerts", Some(&header))
                .contains("storage_bound_engine")
        );
        assert!(
            request(server.local_addr(), "POST", "/api/v2/status", Some(&header))
                .starts_with("HTTP/1.1 405")
        );
        assert!(request(
            server.local_addr(),
            "GET",
            "/api/v2/timeline?other=1",
            Some(&header)
        )
        .starts_with("HTTP/1.1 400"));
        server.shutdown().unwrap();
    }

    #[test]
    fn dashboard_exposes_causal_and_resource_state_without_raw_logs() {
        for required in [
            "Liveness and semantic progress",
            "Actual rate",
            "Expected rate",
            "WAL pending",
            "Memory PSI",
            "Disk await",
            "Causal diagnosis",
            "counter_evidence",
        ] {
            assert!(DASHBOARD.contains(required), "dashboard misses {required}");
        }
    }

    #[test]
    fn timeline_window_is_strict_and_actually_filters_history() {
        assert_eq!(parse_window(None).unwrap(), None);
        assert_eq!(parse_window(Some("window=1")).unwrap(), Some(1));
        assert!(parse_window(Some("other=1")).is_err());
        assert!(parse_window(Some("window=1&other=2")).is_err());
        assert!(parse_window(Some("window=1801")).is_err());

        let frames = VecDeque::from(vec![
            metric_frame(1, 1_000),
            metric_frame(2, 2_000),
            metric_frame(3, 3_000),
        ]);
        let selected = bounded_timeline(&frames, Some(1));
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].sequence, 2);
        assert_eq!(selected[1].sequence, 3);
    }

    #[test]
    fn hub_keeps_only_the_configured_incremental_timeline() {
        let hub = ObserverHttpHub::with_timeline_capacity(2);
        hub.seed_timeline((1..=2).map(|sequence| metric_frame(sequence, sequence * 1_000)));
        for sequence in 3..=4 {
            let frame = metric_frame(sequence, sequence * 1_000);
            hub.publish(
                &serde_json::json!({"sequence":sequence}),
                Some(&frame),
                &[],
                &[],
            )
            .unwrap();
        }
        let timeline = hub.timeline(None);
        assert_eq!(
            timeline
                .iter()
                .map(|frame| frame.sequence)
                .collect::<Vec<_>>(),
            [3, 4]
        );
        assert_eq!(hub.latest_metrics().unwrap().sequence, 4);
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
}
