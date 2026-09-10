use rustls::{ServerConfig as RustlsServerConfig, ServerConnection, StreamOwned};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::{
    collections::BTreeMap,
    fs::File,
    io::BufReader,
    io::ErrorKind,
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    thread,
    time::Duration,
};

use crate::api::{ServerCancellation, ServerRuntimeMetrics};
use crate::protocol::{write_frame, ProtocolErrorCode, ProtocolFailure, ServerMessage};
use crate::Error as RadixDBError;
use radixdb_plugin_host::{PluginRegistry, PluginRegistryStatus};

use super::{
    session::{
        serve_connection_with_databases_limits_and_cancellation, DatabaseRegistryEntry,
        RuntimeState, SessionReadPhase,
    },
    ServerConfig, ServerConfigError, ServerSessionError, ServerTransportConfig,
};

#[derive(Debug)]
pub enum ServerStartError {
    Config(ServerConfigError),
    Io(std::io::Error),
    Session(ServerSessionError),
    Database(RadixDBError),
    WorkerPanic,
    Tls(String),
}

impl std::fmt::Display for ServerStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(error) => error.fmt(formatter),
            Self::Io(error) => error.fmt(formatter),
            Self::Session(error) => error.fmt(formatter),
            Self::Database(error) => error.fmt(formatter),
            Self::WorkerPanic => formatter.write_str("server session worker panicked"),
            Self::Tls(message) => write!(formatter, "TLS configuration: {message}"),
        }
    }
}

impl std::error::Error for ServerStartError {}

impl From<ServerConfigError> for ServerStartError {
    fn from(error: ServerConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<std::io::Error> for ServerStartError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ServerSessionError> for ServerStartError {
    fn from(error: ServerSessionError) -> Self {
        Self::Session(error)
    }
}

impl From<RadixDBError> for ServerStartError {
    fn from(error: RadixDBError) -> Self {
        Self::Database(error)
    }
}

pub struct Server {
    listener: TcpListener,
    config: ServerConfig,
    databases: Mutex<BTreeMap<String, DatabaseRegistryEntry>>,
    runtime: Arc<RuntimeState>,
    served: AtomicBool,
    tls: Option<RwLock<TlsRuntime>>,
    plugin_registry: Arc<PluginRegistry>,
}

struct TlsRuntime {
    config: Arc<RustlsServerConfig>,
    generation: u64,
}

/// Observable certificate generation. Existing sessions keep their accepted
/// generation; an atomic reload affects only connections accepted afterwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TlsRuntimeStatus {
    pub enabled: bool,
    pub generation: u64,
}

struct ConnectionPermit<'a> {
    runtime: &'a RuntimeState,
}

struct SessionRegistration<'a> {
    id: u64,
    sessions: &'a Mutex<BTreeMap<u64, SessionControl>>,
}

struct SessionControl {
    stream: TcpStream,
    cancellation: ServerCancellation,
}

#[cfg(test)]
static PEER_DISCONNECT_CANCELLATIONS: AtomicUsize = AtomicUsize::new(0);

#[cfg(all(test, unix))]
fn peer_stream_reached_eof(stream: &TcpStream) -> bool {
    use std::os::fd::AsRawFd;
    let mut byte = 0_u8;
    unsafe {
        libc::recv(
            stream.as_raw_fd(),
            (&mut byte as *mut u8).cast(),
            1,
            libc::MSG_PEEK | libc::MSG_DONTWAIT,
        ) == 0
    }
}

fn monitor_peer_disconnect(stream: &TcpStream, cancellation: &ServerCancellation) {
    let mut byte = [0_u8; 1];
    loop {
        match stream.peek(&mut byte) {
            Ok(0) => {
                cancellation.cancel();
                #[cfg(test)]
                PEER_DISCONNECT_CANCELLATIONS.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Ok(_) => thread::yield_now(),
            Err(error) if matches!(error.kind(), ErrorKind::Interrupted) => continue,
            Err(error) if matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock) => {
                if cancellation.is_cancelled() {
                    return;
                }
            }
            Err(_) => {
                cancellation.cancel();
                return;
            }
        }
    }
}

impl Drop for SessionRegistration<'_> {
    fn drop(&mut self) {
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.remove(&self.id);
        }
    }
}

impl Drop for ConnectionPermit<'_> {
    fn drop(&mut self) {
        #[cfg(feature = "test-mutations")]
        if crate::test_mutations::session_cleanup_disabled() {
            return;
        }
        self.runtime
            .active_connections
            .fetch_sub(1, Ordering::AcqRel);
        ServerRuntimeMetrics::connection_closed();
    }
}

fn acquire_connection(
    runtime: &RuntimeState,
    max_connections: usize,
) -> Option<ConnectionPermit<'_>> {
    runtime
        .active_connections
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current < max_connections).then_some(current + 1)
        })
        .ok()
        .map(|_| {
            ServerRuntimeMetrics::connection_opened();
            ConnectionPermit { runtime }
        })
}

fn register_session<'a>(
    sessions: &'a Mutex<BTreeMap<u64, SessionControl>>,
    next_session_id: &AtomicU64,
    stream: &TcpStream,
    cancellation: ServerCancellation,
) -> Result<SessionRegistration<'a>, std::io::Error> {
    let id = next_session_id
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .map_err(|_| std::io::Error::other("server session id space exhausted"))?;
    let replaced = sessions
        .lock()
        .map_err(|_| std::io::Error::other("server session registry is poisoned"))?
        .insert(
            id,
            SessionControl {
                stream: stream.try_clone()?,
                cancellation,
            },
        );
    if replaced.is_some() {
        return Err(std::io::Error::other("server session id collision"));
    }
    Ok(SessionRegistration { id, sessions })
}

fn shutdown_sessions(
    sessions: &Mutex<BTreeMap<u64, SessionControl>>,
) -> Result<(), std::io::Error> {
    let sessions = sessions
        .lock()
        .map_err(|_| std::io::Error::other("server session registry is poisoned"))?;
    let mut first_error = None;
    for session in sessions.values() {
        session.cancellation.cancel();
        if let Err(error) = session.stream.shutdown(Shutdown::Both) {
            if !matches!(error.kind(), ErrorKind::NotConnected) && first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn write_connection_limit(
    stream: &mut impl std::io::Write,
    config: &ServerConfig,
) -> Result<(), crate::protocol::ProtocolError> {
    write_frame(
        stream,
        &ServerMessage::Error(ProtocolFailure {
            code: ProtocolErrorCode::ServerError,
            message: format!(
                "server connection limit ({}) reached",
                config.max_connections
            ),
        }),
        config.max_frame_bytes,
    )
}

fn load_tls_config(
    certificate_chain: &std::path::Path,
    private_key: &std::path::Path,
) -> Result<Arc<RustlsServerConfig>, ServerStartError> {
    use std::os::unix::fs::PermissionsExt;

    let key_mode = std::fs::metadata(private_key)?.permissions().mode() & 0o777;
    if key_mode & 0o077 != 0 {
        return Err(ServerStartError::Tls(format!(
            "private key permissions must deny group/other access (expected 0600 or stricter, found {key_mode:04o})"
        )));
    }
    let mut certificate_reader = BufReader::new(File::open(certificate_chain)?);
    let certificates =
        rustls_pemfile::certs(&mut certificate_reader).collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(ServerStartError::Tls(
            "certificate chain PEM contains no certificates".to_owned(),
        ));
    }
    let mut key_reader = BufReader::new(File::open(private_key)?);
    let key = rustls_pemfile::private_key(&mut key_reader)?.ok_or_else(|| {
        ServerStartError::Tls("private key PEM contains no supported key".to_owned())
    })?;
    let config = RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(|error| ServerStartError::Tls(error.to_string()))?;
    Ok(Arc::new(config))
}

impl Server {
    pub fn bind(config: &ServerConfig) -> Result<Self, ServerStartError> {
        config.validate()?;
        Self::bind_validated(
            config.clone(),
            config.port,
            Arc::new(PluginRegistry::empty()),
        )
    }

    /// Bind with one fully admitted immutable plugin registry generation.
    pub fn bind_with_plugin_registry(
        config: &ServerConfig,
        plugin_registry: Arc<PluginRegistry>,
    ) -> Result<Self, ServerStartError> {
        config.validate()?;
        Self::bind_validated(config.clone(), config.port, plugin_registry)
    }

    /// Bind an OS-assigned loopback port without a reserve-and-release race.
    ///
    /// This is intended for isolated test and embedded-server lifecycles. The
    /// resulting runtime config records the actual bound port; ordinary config
    /// validation continues to reject `port = 0` for server configuration files.
    pub fn bind_ephemeral(config: &ServerConfig) -> Result<Self, ServerStartError> {
        config.validate_for_ephemeral_bind()?;
        Self::bind_validated(config.clone(), 0, Arc::new(PluginRegistry::empty()))
    }

    fn bind_validated(
        mut config: ServerConfig,
        bind_port: u16,
        plugin_registry: Arc<PluginRegistry>,
    ) -> Result<Self, ServerStartError> {
        let tls = match &config.transport {
            ServerTransportConfig::Plaintext => None,
            ServerTransportConfig::Tls {
                certificate_chain,
                private_key,
            } => Some(RwLock::new(TlsRuntime {
                config: load_tls_config(certificate_chain, private_key)?,
                generation: 1,
            })),
        };
        std::fs::create_dir_all(config.data_dir.join("databases"))?;
        let listener = TcpListener::bind(SocketAddr::new(config.bind_ip, bind_port))?;
        config.port = listener.local_addr()?.port();
        Ok(Self {
            listener,
            config,
            databases: Mutex::new(BTreeMap::new()),
            runtime: Arc::new(RuntimeState::new()),
            served: AtomicBool::new(false),
            tls,
            plugin_registry,
        })
    }

    pub fn plugin_registry_status(&self) -> PluginRegistryStatus {
        self.plugin_registry.status()
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ServerStartError> {
        Ok(self.listener.local_addr()?)
    }

    pub fn tls_status(&self) -> Result<TlsRuntimeStatus, ServerStartError> {
        let Some(runtime) = &self.tls else {
            return Ok(TlsRuntimeStatus {
                enabled: false,
                generation: 0,
            });
        };
        let runtime = runtime
            .read()
            .map_err(|_| ServerStartError::Tls("TLS runtime lock is poisoned".to_owned()))?;
        Ok(TlsRuntimeStatus {
            enabled: true,
            generation: runtime.generation,
        })
    }

    /// Atomically reload certificate material for future connections. A bad
    /// replacement leaves the previous generation active.
    pub fn reload_tls(&self) -> Result<TlsRuntimeStatus, ServerStartError> {
        let ServerTransportConfig::Tls {
            certificate_chain,
            private_key,
        } = &self.config.transport
        else {
            return Err(ServerStartError::Tls(
                "cannot reload TLS while the server transport is plaintext".to_owned(),
            ));
        };
        let replacement = load_tls_config(certificate_chain, private_key)?;
        let runtime = self
            .tls
            .as_ref()
            .ok_or_else(|| ServerStartError::Tls("TLS runtime is unavailable".to_owned()))?;
        let mut runtime = runtime
            .write()
            .map_err(|_| ServerStartError::Tls("TLS runtime lock is poisoned".to_owned()))?;
        let generation = runtime
            .generation
            .checked_add(1)
            .ok_or_else(|| ServerStartError::Tls("TLS generation exhausted".to_owned()))?;
        runtime.config = replacement;
        runtime.generation = generation;
        Ok(TlsRuntimeStatus {
            enabled: true,
            generation,
        })
    }

    pub fn serve_one(&self) -> Result<(), ServerStartError> {
        self.begin_serving()?;
        let (stream, _) = self.listener.accept()?;
        let _permit = acquire_connection(&self.runtime, self.config.max_connections)
            .ok_or_else(|| std::io::Error::other("server connection limit reached"))?;
        let cancellation = ServerCancellation::new();
        let result = thread::scope(|scope| {
            let scheduler_stop = Arc::new(AtomicBool::new(false));
            let scheduler_cancellation = ServerCancellation::new();
            let worker_stop = Arc::clone(&scheduler_stop);
            let worker_cancellation = scheduler_cancellation.clone();
            let scheduler = scope.spawn(move || {
                super::job_scheduler::run_loop(
                    &self.config,
                    &self.databases,
                    Arc::clone(&self.plugin_registry),
                    worker_stop.as_ref(),
                    &worker_cancellation,
                    &self.runtime.job_scheduler,
                )
            });
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.serve_stream(stream, cancellation)
            }))
            .unwrap_or(Err(ServerStartError::WorkerPanic));
            scheduler_stop.store(true, Ordering::Release);
            scheduler_cancellation.cancel();
            result.and(
                scheduler
                    .join()
                    .map_err(|_| ServerStartError::WorkerPanic)
                    .map(|_| ()),
            )
        });
        let close_result = self.close_databases();
        result.and(close_result)
    }

    fn serve_stream(
        &self,
        mut stream: TcpStream,
        cancellation: ServerCancellation,
    ) -> Result<(), ServerStartError> {
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(self.config.connect_timeout_secs)))?;
        stream.set_write_timeout(Some(Duration::from_secs(
            self.config.net_write_timeout_secs,
        )))?;
        let monitor = stream.try_clone()?;
        if let Some(tls) = &self.tls {
            let tls_config = Arc::clone(
                &tls.read()
                    .map_err(|_| ServerStartError::Tls("TLS runtime lock is poisoned".to_owned()))?
                    .config,
            );
            let connection = ServerConnection::new(tls_config)
                .map_err(|error| ServerStartError::Tls(error.to_string()))?;
            let mut stream = StreamOwned::new(connection, stream);
            self.serve_tls_protocol_stream(&mut stream, monitor, cancellation)
        } else {
            self.serve_plain_protocol_stream(&mut stream, monitor, cancellation)
        }
    }

    fn serve_plain_protocol_stream(
        &self,
        stream: &mut TcpStream,
        monitor: TcpStream,
        cancellation: ServerCancellation,
    ) -> Result<(), ServerStartError> {
        let net_read_timeout = Duration::from_secs(self.config.net_read_timeout_secs);
        let idle_timeout = Duration::from_secs(self.config.connection_idle_timeout_secs);
        thread::scope(|scope| {
            let monitor_cancellation = cancellation.clone();
            let monitor_worker =
                scope.spawn(move || monitor_peer_disconnect(&monitor, &monitor_cancellation));
            let result = serve_connection_with_databases_limits_and_cancellation(
                stream,
                &self.config,
                &self.databases,
                Arc::clone(&self.plugin_registry),
                cancellation.clone(),
                &self.runtime,
                move |stream, phase| {
                    let timeout = match phase {
                        SessionReadPhase::Idle => idle_timeout,
                        SessionReadPhase::FramePayload => net_read_timeout,
                    };
                    stream.set_read_timeout(Some(timeout))
                },
            );
            cancellation.cancel();
            let _ = stream.shutdown(Shutdown::Read);
            let _ = monitor_worker.join();
            result.map_err(ServerStartError::from)
        })
    }

    fn serve_tls_protocol_stream(
        &self,
        stream: &mut StreamOwned<ServerConnection, TcpStream>,
        monitor: TcpStream,
        cancellation: ServerCancellation,
    ) -> Result<(), ServerStartError> {
        let net_read_timeout = Duration::from_secs(self.config.net_read_timeout_secs);
        let idle_timeout = Duration::from_secs(self.config.connection_idle_timeout_secs);
        thread::scope(|scope| {
            let monitor_cancellation = cancellation.clone();
            let monitor_worker =
                scope.spawn(move || monitor_peer_disconnect(&monitor, &monitor_cancellation));
            let result = serve_connection_with_databases_limits_and_cancellation(
                stream,
                &self.config,
                &self.databases,
                Arc::clone(&self.plugin_registry),
                cancellation.clone(),
                &self.runtime,
                move |stream, phase| {
                    let timeout = match phase {
                        SessionReadPhase::Idle => idle_timeout,
                        SessionReadPhase::FramePayload => net_read_timeout,
                    };
                    stream.sock.set_read_timeout(Some(timeout))
                },
            );
            cancellation.cancel();
            let _ = stream.sock.shutdown(Shutdown::Read);
            let _ = monitor_worker.join();
            result.map_err(ServerStartError::from)
        })
    }

    fn reject_connection_limit(&self, mut stream: TcpStream) -> Result<(), ServerStartError> {
        stream.set_write_timeout(Some(Duration::from_secs(
            self.config.net_write_timeout_secs,
        )))?;
        if let Some(tls) = &self.tls {
            let tls_config = Arc::clone(
                &tls.read()
                    .map_err(|_| ServerStartError::Tls("TLS runtime lock is poisoned".to_owned()))?
                    .config,
            );
            let connection = ServerConnection::new(tls_config)
                .map_err(|error| ServerStartError::Tls(error.to_string()))?;
            let mut stream = StreamOwned::new(connection, stream);
            return write_connection_limit(&mut stream, &self.config)
                .map_err(ServerSessionError::from)
                .map_err(ServerStartError::from);
        }
        write_connection_limit(&mut stream, &self.config)
            .map_err(ServerSessionError::from)
            .map_err(ServerStartError::from)
    }

    pub fn run(&self) -> Result<(), ServerStartError> {
        self.run_until(&AtomicBool::new(false))
    }

    pub fn run_until(&self, shutdown: &AtomicBool) -> Result<(), ServerStartError> {
        self.begin_serving()?;
        self.listener.set_nonblocking(true)?;
        let next_session_id = AtomicU64::new(1);
        let sessions = Mutex::new(BTreeMap::new());
        let worker_panicked = AtomicBool::new(false);
        let run_result = thread::scope(|scope| -> Result<(), ServerStartError> {
            let scheduler_stop = Arc::new(AtomicBool::new(false));
            let scheduler_cancellation = ServerCancellation::new();
            let worker_stop = Arc::clone(&scheduler_stop);
            let worker_cancellation = scheduler_cancellation.clone();
            let scheduler_worker = scope.spawn(move || {
                super::job_scheduler::run_loop(
                    &self.config,
                    &self.databases,
                    Arc::clone(&self.plugin_registry),
                    worker_stop.as_ref(),
                    &worker_cancellation,
                    &self.runtime.job_scheduler,
                )
            });
            let accept_result = (|| -> Result<(), ServerStartError> {
                while !shutdown.load(Ordering::Acquire) {
                    if worker_panicked.load(Ordering::Acquire) {
                        return Err(ServerStartError::WorkerPanic);
                    }
                    let (stream, _) = match self.listener.accept() {
                        Ok(accepted) => accepted,
                        Err(error) if error.kind() == ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    };
                    let Some(permit) =
                        acquire_connection(&self.runtime, self.config.max_connections)
                    else {
                        if let Err(error) = self.reject_connection_limit(stream) {
                            eprintln!(
                                "radixdb-server: failed to reject excess connection: {error}"
                            );
                        }
                        continue;
                    };
                    let cancellation = ServerCancellation::new();
                    let registration = register_session(
                        &sessions,
                        &next_session_id,
                        &stream,
                        cancellation.clone(),
                    )?;
                    let sessions_ref = &sessions;
                    let worker_panicked_ref = &worker_panicked;
                    scope.spawn(move || {
                        let _permit = permit;
                        let _registration = registration;
                        let outcome =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                self.serve_stream(stream, cancellation)
                            }));
                        if outcome.is_err() {
                            worker_panicked_ref.store(true, Ordering::Release);
                            let _ = shutdown_sessions(sessions_ref);
                        }
                        if let Ok(Err(error)) = outcome {
                            if !shutdown.load(Ordering::Acquire) {
                                eprintln!("radixdb-server: connection failed: {error}");
                            }
                        }
                    });
                }
                Ok(())
            })();
            scheduler_stop.store(true, Ordering::Release);
            scheduler_cancellation.cancel();
            let shutdown_result = shutdown_sessions(&sessions).map_err(ServerStartError::from);
            let scheduler_result = scheduler_worker
                .join()
                .map_err(|_| ServerStartError::WorkerPanic)
                .map(|_| ());
            accept_result.and(shutdown_result).and(scheduler_result)
        });
        let close_result = self.close_databases();
        let reset_result = self
            .listener
            .set_nonblocking(false)
            .map_err(ServerStartError::from);
        run_result.and(close_result).and(reset_result)
    }

    fn begin_serving(&self) -> Result<(), ServerStartError> {
        self.served.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| std::io::Error::new(ErrorKind::AlreadyExists, "Server lifecycle methods consume one bound instance; bind a new Server to restart").into())
    }

    fn close_databases(&self) -> Result<(), ServerStartError> {
        let mut databases = self
            .databases
            .lock()
            .map_err(|_| std::io::Error::other("database registry is poisoned"))?;
        let mut first_error = None;
        for entry in databases.values() {
            if let DatabaseRegistryEntry::Ready { database, .. } = entry {
                if let Err(error) = database.close() {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        databases.clear();
        first_error.map_or(Ok(()), |error| Err(error.into()))
    }
}

#[cfg(test)]
mod tests;
