#![cfg(feature = "stress-tests")]

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    thread,
    time::Duration,
};

use radixdb::server::{
    default_copy_max_transaction_bytes, default_target_volume_rows, Server, ServerConfig,
};
use radixdb_client::{Connection, ExecuteResult, Row, WireValue};

static SERVER_GATE: Mutex<()> = Mutex::new(());

struct ShutdownOnDrop<'a>(&'a AtomicBool);

impl Drop for ShutdownOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

pub fn tcp_server_config(data_dir: PathBuf, max_connections: usize) -> ServerConfig {
    ServerConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 0,
        data_dir,
        transport: Default::default(),
        authentication: Default::default(),
        max_connections,
        max_inflight_frame_bytes: radixdb::server::default_max_inflight_frame_bytes(),
        max_databases: radixdb::server::default_max_databases(),
        max_database_name_bytes: radixdb::server::default_max_database_name_bytes(),
        connect_timeout_secs: 5,
        connection_idle_timeout_secs: 30,
        net_read_timeout_secs: 30,
        net_write_timeout_secs: 30,
        cursor_batch_max_rows: 256,
        cursor_batch_max_bytes: 2 * 1024 * 1024,
        max_frame_bytes: 8 * 1024 * 1024,
        copy_max_transaction_bytes: default_copy_max_transaction_bytes(),
        max_compaction_jobs: radixdb::server::default_max_compaction_jobs(),
        storage_cpu_workers: radixdb::server::default_storage_cpu_workers(),
        page_cache_level: radixdb::server::default_page_cache_level(),
        page_cache_max_bytes: radixdb::server::default_page_cache_max_bytes(),
        page_cache_memory_reserve: radixdb::server::default_page_cache_memory_reserve(),
        target_volume_rows: default_target_volume_rows(),
        seal_hot_bytes_threshold: 16 * 1024,
        seal_incremental_hot_bytes_threshold: 4 * 1024,
        read_queue_depth: 2,
    }
}

pub fn with_tcp_server<T>(
    data_dir: PathBuf,
    max_connections: usize,
    operation: impl FnOnce(SocketAddr) -> T,
) -> T {
    let _gate = SERVER_GATE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let config = tcp_server_config(data_dir, max_connections);
    let server = Server::bind_ephemeral(&config).expect("bind prerelease TCP server");
    let address = server.local_addr().expect("prerelease TCP server address");
    let shutdown = AtomicBool::new(false);
    thread::scope(|scope| {
        let _shutdown_on_unwind = ShutdownOnDrop(&shutdown);
        let worker = scope.spawn(|| server.run_until(&shutdown));
        let result = operation(address);
        shutdown.store(true, Ordering::Release);
        worker
            .join()
            .expect("prerelease server thread joins")
            .expect("prerelease server stops cleanly");
        result
    })
}

pub fn tcp_connect(address: SocketAddr, database: &str) -> Result<Connection, String> {
    tcp_connect_with_read_timeout(address, database, Duration::from_secs(30))
}

/// Connect a harness client whose one server-side command may legitimately
/// outlive the ordinary interactive timeout.
///
/// Large atomic COPY remains one statement and one transaction, so splitting
/// it merely to keep a 30-second socket alive would weaken the contract under
/// test. Callers must opt in explicitly; ordinary chaos clients retain the
/// production-like 30-second timeout above.
pub fn tcp_connect_with_read_timeout(
    address: SocketAddr,
    database: &str,
    read_timeout: Duration,
) -> Result<Connection, String> {
    let mut connection = Connection::connect_with_timeouts(
        address,
        Duration::from_secs(3),
        read_timeout,
        Duration::from_secs(10),
    )
    .map_err(|error| format!("connect: {error}"))?;
    connection
        .authenticate("root", None)
        .map_err(|error| format!("authenticate: {error}"))?;
    connection
        .select_database(database)
        .map_err(|error| format!("select database `{database}`: {error}"))?;
    Ok(connection)
}

pub fn tcp_command(connection: &mut Connection, sql: impl Into<String>) -> Result<(), String> {
    let sql = sql.into();
    match connection
        .execute(sql.clone())
        .map_err(|error| format!("execute `{sql}`: {error}"))?
    {
        ExecuteResult::CommandComplete { .. } => Ok(()),
        ExecuteResult::Cursor(cursor) => {
            loop {
                let batch = connection
                    .fetch(&cursor)
                    .map_err(|error| format!("fetch `{sql}`: {error}"))?;
                if batch.eof {
                    break;
                }
            }
            Ok(())
        }
    }
}

pub fn tcp_rows(connection: &mut Connection, sql: &str) -> Result<Vec<Row>, String> {
    let ExecuteResult::Cursor(cursor) = connection
        .execute(sql)
        .map_err(|error| format!("open cursor for `{sql}`: {error}"))?
    else {
        return Err(format!("query did not open a cursor: {sql}"));
    };
    let mut result = Vec::new();
    loop {
        let batch = connection
            .fetch(&cursor)
            .map_err(|error| format!("fetch cursor for `{sql}`: {error}"))?;
        result.extend(batch.rows);
        if batch.eof {
            return Ok(result);
        }
    }
}

pub fn tcp_scalar_i64(connection: &mut Connection, sql: &str) -> Result<i64, String> {
    let mut result = tcp_rows(connection, sql)?;
    if result.len() != 1 || result[0].values.len() != 1 {
        return Err(format!(
            "expected one scalar row for `{sql}`, got {result:?}"
        ));
    }
    match result.remove(0).values.remove(0) {
        WireValue::Int(value) => Ok(value),
        other => Err(format!(
            "expected INTEGER scalar for `{sql}`, got {other:?}"
        )),
    }
}
