use std::{
    collections::BTreeMap,
    sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use radixdb_client::{ExecuteResult, WireValue};
use serde::{Deserialize, Serialize};

use crate::{
    config::{DatabaseEngine, ResolvedDatabaseConfig},
    database,
};

use super::DIAGNOSTIC_FORMAT_V2;

const ENGINE_RUNTIME_FORMAT_V2: u64 = 2;
const MAX_ENGINE_SNAPSHOT_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineSampleV2 {
    pub format: u32,
    pub sequence: u64,
    pub monotonic_millis: u64,
    pub unix_millis: u64,
    pub query_millis: u64,
    pub snapshot: Option<serde_json::Value>,
    pub error: Option<String>,
}

impl EngineSampleV2 {
    pub fn available(&self) -> bool {
        self.snapshot.is_some() && self.error.is_none()
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.format != DIAGNOSTIC_FORMAT_V2 || self.sequence == 0 {
            return Err("invalid engine sample identity".into());
        }
        match (&self.snapshot, &self.error) {
            (Some(snapshot), None) => validate_enriched_snapshot(snapshot),
            (None, Some(error)) if !error.is_empty() && error.len() <= 1_024 => Ok(()),
            _ => Err("engine sample must contain exactly one snapshot or error".into()),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct SnapshotRequest {
    sequence: u64,
    monotonic_millis: u64,
    unix_millis: u64,
}

enum WorkerRequest {
    Snapshot(SnapshotRequest),
    Stop,
}

/// One isolated diagnostic connection owner.
///
/// The observer sampling loop only performs non-blocking channel operations.
/// A stuck server can consume this one worker until the socket deadline, but
/// cannot stop procfs/disk sampling or spawn an unbounded set of query threads.
pub struct EngineSnapshotWorker {
    requests: SyncSender<WorkerRequest>,
    results: Receiver<EngineSampleV2>,
    handle: Option<JoinHandle<()>>,
    inflight: bool,
}

impl EngineSnapshotWorker {
    pub fn start(config: ResolvedDatabaseConfig, timeout: Duration) -> Result<Self, String> {
        if timeout.is_zero() {
            return Err("engine snapshot timeout must be non-zero".into());
        }
        let (request_tx, request_rx) = mpsc::sync_channel(1);
        // At most one request is in flight, so an unbounded result channel can
        // hold at most one unpublished sample and makes shutdown join-safe.
        let (result_tx, result_rx) = mpsc::channel();
        let handle = thread::Builder::new()
            .name("radixdb-soak-engine-observer".into())
            .spawn(move || {
                while let Ok(request) = request_rx.recv() {
                    let WorkerRequest::Snapshot(request) = request else {
                        break;
                    };
                    let started = Instant::now();
                    let result = collect_snapshot(&config, timeout, request.sequence);
                    let (snapshot, error) = match result {
                        Ok(snapshot) => (Some(snapshot), None),
                        Err(error) => (None, Some(bounded_error(error))),
                    };
                    let sample = EngineSampleV2 {
                        format: DIAGNOSTIC_FORMAT_V2,
                        sequence: request.sequence,
                        monotonic_millis: request.monotonic_millis,
                        unix_millis: request.unix_millis,
                        query_millis: started.elapsed().as_millis().min(u128::from(u64::MAX))
                            as u64,
                        snapshot,
                        error,
                    };
                    if result_tx.send(sample).is_err() {
                        break;
                    }
                }
            })
            .map_err(|error| format!("spawn engine snapshot worker: {error}"))?;
        Ok(Self {
            requests: request_tx,
            results: result_rx,
            handle: Some(handle),
            inflight: false,
        })
    }

    /// Queue one snapshot without waiting. `false` means that the previous
    /// bounded request is still in flight.
    pub fn try_request(
        &mut self,
        sequence: u64,
        monotonic_millis: u64,
        unix_millis: u64,
    ) -> Result<bool, String> {
        if self.inflight {
            return Ok(false);
        }
        let request = WorkerRequest::Snapshot(SnapshotRequest {
            sequence,
            monotonic_millis,
            unix_millis,
        });
        match self.requests.try_send(request) {
            Ok(()) => {
                self.inflight = true;
                Ok(true)
            }
            Err(TrySendError::Full(_)) => Ok(false),
            Err(TrySendError::Disconnected(_)) => {
                Err("engine snapshot worker stopped unexpectedly".into())
            }
        }
    }

    pub fn poll(&mut self) -> Result<Option<EngineSampleV2>, String> {
        match self.results.try_recv() {
            Ok(sample) => {
                self.inflight = false;
                sample.validate()?;
                Ok(Some(sample))
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => {
                Err("engine snapshot result channel disconnected".into())
            }
        }
    }

    pub fn shutdown(mut self) -> Result<Vec<EngineSampleV2>, String> {
        let _ = self.requests.send(WorkerRequest::Stop);
        if let Some(handle) = self.handle.take() {
            handle
                .join()
                .map_err(|_| "engine snapshot worker panicked".to_string())?;
        }
        let samples = self.results.try_iter().collect::<Vec<_>>();
        for sample in &samples {
            sample.validate()?;
        }
        Ok(samples)
    }
}

fn collect_snapshot(
    config: &ResolvedDatabaseConfig,
    timeout: Duration,
    request_sequence: u64,
) -> Result<serde_json::Value, String> {
    let mut bounded = config.clone();
    bounded.connect_timeout = timeout.min(config.connect_timeout);
    bounded.read_timeout = timeout.min(config.read_timeout);
    bounded.write_timeout = timeout.min(config.write_timeout);
    let mut connection =
        database::connect(&bounded).map_err(|error| format!("diagnostic connect: {error}"))?;
    let server_runtime = database::server_runtime(&mut connection)?;
    if config.engine == DatabaseEngine::Postgresql {
        return collect_postgresql_snapshot(connection, server_runtime, request_sequence);
    }
    let connection = connection
        .radixdb_mut()
        .ok_or_else(|| "diagnostic expected RadixDB connection".to_string())?;
    let cursor = match connection
        .execute("PRAGMA RUNTIME_STATS")
        .map_err(|error| format!("diagnostic runtime query: {error}"))?
    {
        ExecuteResult::Cursor(cursor) => cursor,
        ExecuteResult::CommandComplete { .. } => {
            return Err("PRAGMA RUNTIME_STATS returned no cursor".into())
        }
    };
    let batch = connection
        .fetch(&cursor)
        .map_err(|error| format!("diagnostic runtime fetch: {error}"))?;
    if !batch.eof || batch.rows.len() != 1 || batch.rows[0].values.len() != 1 {
        return Err("PRAGMA RUNTIME_STATS returned an invalid bounded shape".into());
    }
    let WireValue::String(payload) = &batch.rows[0].values[0] else {
        return Err("PRAGMA RUNTIME_STATS returned a non-text payload".into());
    };
    let mut snapshot = parse_snapshot_payload(payload)?;
    let server_runtime = serde_json::to_value(server_runtime)
        .map_err(|error| format!("encode diagnostic server runtime: {error}"))?;
    snapshot
        .as_object_mut()
        .expect("validated engine snapshot is an object")
        .insert("server_runtime".into(), server_runtime);
    validate_enriched_snapshot(&snapshot)?;
    Ok(snapshot)
}

fn collect_postgresql_snapshot(
    mut connection: database::DatabaseConnection,
    server_runtime: crate::status::ServerRuntimeSnapshot,
    sequence: u64,
) -> Result<serde_json::Value, String> {
    let started = Instant::now();
    let database = query_json(
        &mut connection,
        "SELECT row_to_json(s)::text FROM (
           SELECT * FROM pg_stat_database WHERE datname = current_database()
         ) s",
    )?;
    let bgwriter = query_json(
        &mut connection,
        "SELECT row_to_json(s)::text FROM (SELECT * FROM pg_stat_bgwriter) s",
    )?;
    let checkpointer = optional_postgresql_stat(
        &mut connection,
        "pg_stat_checkpointer",
        "SELECT row_to_json(s)::text FROM (SELECT * FROM pg_stat_checkpointer) s",
    )?;
    let wal = query_json(
        &mut connection,
        "SELECT row_to_json(s)::text FROM (SELECT * FROM pg_stat_wal) s",
    )?;
    let io = optional_postgresql_stat(
        &mut connection,
        "pg_stat_io",
        "SELECT json_build_object(
           'reads', COALESCE(SUM(reads), 0),
           'writes', COALESCE(SUM(writes), 0),
           'writebacks', COALESCE(SUM(writebacks), 0),
           'extends', COALESCE(SUM(extends), 0),
           'hits', COALESCE(SUM(hits), 0),
           'evictions', COALESCE(SUM(evictions), 0),
           'reuses', COALESCE(SUM(reuses), 0),
           'fsyncs', COALESCE(SUM(fsyncs), 0)
         )::text FROM pg_stat_io",
    )?;
    let activity = query_json(
        &mut connection,
        "SELECT json_build_object(
           'connections', COUNT(*),
           'active', COUNT(*) FILTER (WHERE state = 'active'),
           'idle', COUNT(*) FILTER (WHERE state = 'idle'),
           'idle_in_transaction', COUNT(*) FILTER (WHERE state = 'idle in transaction'),
           'waiting', COUNT(*) FILTER (WHERE wait_event IS NOT NULL),
           'autovacuum_workers', COUNT(*) FILTER (WHERE backend_type = 'autovacuum worker'),
           'oldest_transaction_millis', COALESCE(MAX(
             EXTRACT(EPOCH FROM (clock_timestamp() - xact_start)) * 1000
           ) FILTER (WHERE xact_start IS NOT NULL), 0)::bigint
         )::text
         FROM pg_stat_activity WHERE datname = current_database()",
    )?;
    let database_bytes =
        connection.scalar_i64("SELECT pg_database_size(current_database())::bigint")?;
    let wal_lsn_bytes =
        connection.scalar_i64("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint")?;
    let mut counters = BTreeMap::<String, u64>::new();
    let mut gauges = BTreeMap::<String, u64>::new();
    copy_numeric_fields(
        &database,
        &mut counters,
        &mut gauges,
        &["numbackends"],
        "database",
    );
    copy_numeric_fields(&bgwriter, &mut counters, &mut gauges, &[], "bgwriter");
    copy_numeric_fields(
        &checkpointer,
        &mut counters,
        &mut gauges,
        &[],
        "checkpointer",
    );
    copy_numeric_fields(&wal, &mut counters, &mut gauges, &[], "wal");
    copy_numeric_fields(&io, &mut counters, &mut gauges, &[], "io");
    copy_numeric_fields(
        &activity,
        &mut counters,
        &mut gauges,
        &[
            "connections",
            "active",
            "idle",
            "idle_in_transaction",
            "waiting",
            "autovacuum_workers",
            "oldest_transaction_millis",
        ],
        "activity",
    );
    gauges.insert(
        "database_bytes".into(),
        u64::try_from(database_bytes).map_err(|_| "negative PostgreSQL database size")?,
    );
    gauges.insert(
        "wal_current_lsn_bytes".into(),
        u64::try_from(wal_lsn_bytes).map_err(|_| "negative PostgreSQL WAL LSN")?,
    );
    let snapshot = serde_json::json!({
        "format": ENGINE_RUNTIME_FORMAT_V2,
        "sequence": sequence,
        "snapshot_nanos": started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
        "engine_kind": "postgresql",
        "postgres": {
            "counters": counters,
            "gauges": gauges,
        },
        "server_runtime": server_runtime,
    });
    validate_enriched_snapshot(&snapshot)?;
    Ok(snapshot)
}

fn optional_postgresql_stat(
    connection: &mut database::DatabaseConnection,
    relation: &str,
    sql: &str,
) -> Result<serde_json::Value, String> {
    let exists = connection.scalar_i64(&format!(
        "SELECT COUNT(*) FROM pg_catalog.pg_class \
         WHERE oid = to_regclass('pg_catalog.{relation}')"
    ))?;
    if exists == 0 {
        Ok(serde_json::json!({}))
    } else {
        query_json(connection, sql)
    }
}

fn query_json(
    connection: &mut database::DatabaseConnection,
    sql: &str,
) -> Result<serde_json::Value, String> {
    let payload = connection.query_text(sql)?;
    if payload.len() > MAX_ENGINE_SNAPSHOT_BYTES {
        return Err("PostgreSQL diagnostic row exceeds bounded snapshot contract".into());
    }
    serde_json::from_str(&payload).map_err(|error| format!("parse PostgreSQL statistics: {error}"))
}

fn copy_numeric_fields(
    source: &serde_json::Value,
    counters: &mut BTreeMap<String, u64>,
    gauges: &mut BTreeMap<String, u64>,
    gauge_names: &[&str],
    prefix: &str,
) {
    let Some(source) = source.as_object() else {
        return;
    };
    for (name, value) in source {
        let Some(value) = json_u64(value) else {
            continue;
        };
        let name = format!("{prefix}.{name}");
        if gauge_names
            .iter()
            .any(|candidate| name.ends_with(candidate))
        {
            gauges.insert(name, value);
        } else {
            counters.insert(name, value);
        }
    }
}

fn json_u64(value: &serde_json::Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
        .or_else(|| {
            value
                .as_f64()
                .filter(|value| value.is_finite() && *value >= 0.0)
                .map(|value| value.round().min(u64::MAX as f64) as u64)
        })
}

fn parse_snapshot_payload(payload: &str) -> Result<serde_json::Value, String> {
    if payload.len() > MAX_ENGINE_SNAPSHOT_BYTES {
        return Err(format!(
            "engine snapshot exceeds {} byte contract",
            MAX_ENGINE_SNAPSHOT_BYTES
        ));
    }
    let snapshot: serde_json::Value = serde_json::from_str(payload)
        .map_err(|error| format!("parse engine runtime snapshot: {error}"))?;
    validate_snapshot(&snapshot)?;
    Ok(snapshot)
}

fn validate_snapshot(snapshot: &serde_json::Value) -> Result<(), String> {
    let object = snapshot
        .as_object()
        .ok_or_else(|| "engine runtime snapshot must be an object".to_string())?;
    if object.get("format").and_then(serde_json::Value::as_u64) != Some(ENGINE_RUNTIME_FORMAT_V2) {
        return Err("unsupported engine runtime snapshot format".into());
    }
    if object
        .get("sequence")
        .and_then(serde_json::Value::as_u64)
        .is_none_or(|sequence| sequence == 0)
    {
        return Err("engine runtime snapshot sequence must be non-zero".into());
    }
    if object
        .get("engine_kind")
        .and_then(serde_json::Value::as_str)
        == Some("postgresql")
    {
        if object
            .get("snapshot_nanos")
            .and_then(serde_json::Value::as_u64)
            .is_none()
            || !snapshot
                .pointer("/postgres/counters")
                .is_some_and(serde_json::Value::is_object)
            || !snapshot
                .pointer("/postgres/gauges")
                .is_some_and(serde_json::Value::is_object)
        {
            return Err("invalid PostgreSQL runtime snapshot".into());
        }
        return Ok(());
    }
    for required in [
        "snapshot_nanos",
        "active_transactions",
        "hot_rows",
        "hot_bytes",
        "cold_rows",
        "cold_resident_bytes",
        "wal_current_lsn",
        "wal_current_file_bytes",
        "wal_pending_durability_bytes",
    ] {
        if object
            .get(required)
            .and_then(serde_json::Value::as_u64)
            .is_none()
        {
            return Err(format!("engine runtime snapshot is missing `{required}`"));
        }
    }
    if !object
        .get("counters")
        .is_some_and(serde_json::Value::is_object)
    {
        return Err("engine runtime snapshot counters must be an object".into());
    }
    if let Some(server_runtime) = object.get("server_runtime") {
        if !server_runtime.is_object() {
            return Err("engine server runtime must be an object".into());
        }
    }
    if !object
        .get("runtime_owners")
        .is_some_and(serde_json::Value::is_object)
    {
        return Err("engine runtime owners must be an object".into());
    }
    if !object
        .get("maintenance")
        .is_some_and(serde_json::Value::is_object)
    {
        return Err("engine runtime maintenance must be an object".into());
    }
    Ok(())
}

fn validate_enriched_snapshot(snapshot: &serde_json::Value) -> Result<(), String> {
    validate_snapshot(snapshot)?;
    if !snapshot
        .get("server_runtime")
        .is_some_and(serde_json::Value::is_object)
    {
        return Err("engine server runtime must be an object".into());
    }
    Ok(())
}

fn bounded_error(mut error: String) -> String {
    if error.len() > 1_024 {
        error.truncate(1_024);
    }
    error
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_payload_contract_is_versioned_and_bounded() {
        let payload = serde_json::json!({
            "format": 2,
            "sequence": 1,
            "snapshot_nanos": 10,
            "active_transactions": 0,
            "hot_rows": 1,
            "hot_bytes": 128,
            "cold_rows": 0,
            "cold_resident_bytes": 0,
            "wal_current_lsn": 1,
            "wal_current_file_bytes": 0,
            "wal_pending_durability_bytes": 0,
            "runtime_owners": {},
            "maintenance": {},
            "counters": {}
        })
        .to_string();
        assert!(parse_snapshot_payload(&payload).is_ok());

        let oversized = "x".repeat(MAX_ENGINE_SNAPSHOT_BYTES + 1);
        assert!(parse_snapshot_payload(&oversized)
            .unwrap_err()
            .contains("exceeds"));
    }
}
