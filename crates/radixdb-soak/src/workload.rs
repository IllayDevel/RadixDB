use std::{
    collections::VecDeque,
    fs::{self, OpenOptions},
    io::{BufWriter, Write},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use rand::{rngs::SmallRng, Rng, SeedableRng};
use sha2::{Digest, Sha256};

use crate::{
    config::{DatabaseEngine, ResolvedDatabaseConfig},
    database,
    runtime::RuntimeMetrics,
};

pub use crate::database::DatabaseConnection;

const MAX_ACTIVE_PER_WORKER: usize = 64;
const ID_BASE: i64 = 1_000_000_000_000;
const COLD_COPY_CHUNK_ROWS: u64 = 250_000;
const COLD_COPY_BACKPRESSURE_RETRY_PAUSE: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeedMilestone {
    RowsLoaded(SeedChunkMetrics),
    CopyBackpressure {
        loaded_rows: u64,
        retry_attempt: u64,
        elapsed_nanos: u64,
    },
    CheckpointStarted(u64),
    CheckpointCompleted(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeedChunkMetrics {
    pub loaded_rows: u64,
    pub chunk_rows: u64,
    pub csv_nanos: u64,
    pub copy_nanos: u64,
    pub total_nanos: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SeedEngineCounters {
    pub volume_read_bytes: u64,
    pub volume_read_nanos: u64,
    pub wal_write_bytes: u64,
    pub wal_write_nanos: u64,
    pub wal_sync_nanos: u64,
    pub wal_generation_validation_bytes: u64,
    pub wal_generation_validation_nanos: u64,
    pub wal_retention_identity_checks: u64,
    pub wal_retention_files_deleted: u64,
    pub wal_retention_nanos: u64,
    pub seal_rows: u64,
    pub seal_output_bytes: u64,
    pub seal_nanos: u64,
    pub compaction_nanos: u64,
    pub copy_parse_nanos: u64,
    pub copy_commit_nanos: u64,
    pub copy_total_nanos: u64,
    pub cold_constraint_nanos: u64,
    pub cold_pk_nanos: u64,
    pub runtime_wait_nanos: u64,
    pub manifest_publication_nanos: u64,
    pub backpressure_wait_millis: u64,
    pub cold_segments: u64,
    pub storage_cpu_workers_effective: u64,
    pub storage_cpu_workers_in_use: u64,
    pub storage_cpu_peak_workers_in_use: u64,
    pub storage_cpu_workers_reserved: u64,
    pub storage_cpu_peak_workers_reserved: u64,
    pub storage_cpu_leases: u64,
    pub storage_cpu_parallel_leases: u64,
}

impl SeedEngineCounters {
    pub fn delta(self, before: Self) -> Self {
        macro_rules! delta {
            ($field:ident) => {
                self.$field.saturating_sub(before.$field)
            };
        }
        Self {
            volume_read_bytes: delta!(volume_read_bytes),
            volume_read_nanos: delta!(volume_read_nanos),
            wal_write_bytes: delta!(wal_write_bytes),
            wal_write_nanos: delta!(wal_write_nanos),
            wal_sync_nanos: delta!(wal_sync_nanos),
            wal_generation_validation_bytes: delta!(wal_generation_validation_bytes),
            wal_generation_validation_nanos: delta!(wal_generation_validation_nanos),
            wal_retention_identity_checks: delta!(wal_retention_identity_checks),
            wal_retention_files_deleted: delta!(wal_retention_files_deleted),
            wal_retention_nanos: delta!(wal_retention_nanos),
            seal_rows: delta!(seal_rows),
            seal_output_bytes: delta!(seal_output_bytes),
            seal_nanos: delta!(seal_nanos),
            compaction_nanos: delta!(compaction_nanos),
            copy_parse_nanos: delta!(copy_parse_nanos),
            copy_commit_nanos: delta!(copy_commit_nanos),
            copy_total_nanos: delta!(copy_total_nanos),
            cold_constraint_nanos: delta!(cold_constraint_nanos),
            cold_pk_nanos: delta!(cold_pk_nanos),
            runtime_wait_nanos: delta!(runtime_wait_nanos),
            manifest_publication_nanos: delta!(manifest_publication_nanos),
            backpressure_wait_millis: delta!(backpressure_wait_millis),
            // Gauge, not a cumulative counter.
            cold_segments: self.cold_segments,
            storage_cpu_workers_effective: self.storage_cpu_workers_effective,
            storage_cpu_workers_in_use: self.storage_cpu_workers_in_use,
            storage_cpu_peak_workers_in_use: self.storage_cpu_peak_workers_in_use,
            storage_cpu_workers_reserved: self.storage_cpu_workers_reserved,
            storage_cpu_peak_workers_reserved: self.storage_cpu_peak_workers_reserved,
            storage_cpu_leases: delta!(storage_cpu_leases),
            storage_cpu_parallel_leases: delta!(storage_cpu_parallel_leases),
        }
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

pub fn connect(config: &ResolvedDatabaseConfig) -> Result<DatabaseConnection, String> {
    database::connect(config)
}

pub fn seed_engine_counters(
    connection: &mut DatabaseConnection,
) -> Result<SeedEngineCounters, String> {
    let Some(connection) = connection.radixdb_mut() else {
        return Ok(SeedEngineCounters::default());
    };
    let radixdb_client::ExecuteResult::Cursor(cursor) = connection
        .execute("PRAGMA RUNTIME_STATS")
        .map_err(|error| format!("seed runtime stats: {error}"))?
    else {
        return Err("PRAGMA RUNTIME_STATS returned no cursor".into());
    };
    let batch = connection
        .fetch(&cursor)
        .map_err(|error| format!("fetch seed runtime stats: {error}"))?;
    if !batch.eof || batch.rows.len() != 1 || batch.rows[0].values.len() != 1 {
        return Err("PRAGMA RUNTIME_STATS returned an invalid shape".into());
    }
    let radixdb_client::WireValue::String(payload) = &batch.rows[0].values[0] else {
        return Err("PRAGMA RUNTIME_STATS returned a non-text payload".into());
    };
    let value: serde_json::Value = serde_json::from_str(payload)
        .map_err(|error| format!("parse seed runtime stats: {error}"))?;
    let number = |pointer: &str| -> Result<u64, String> {
        value
            .pointer(pointer)
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| format!("seed runtime stats missing `{pointer}`"))
    };
    Ok(SeedEngineCounters {
        volume_read_bytes: number("/counters/volume_read_bytes")?,
        volume_read_nanos: number("/counters/volume_read_nanos")?,
        wal_write_bytes: number("/counters/wal_write_bytes")?,
        wal_write_nanos: number("/counters/wal_write_nanos")?,
        wal_sync_nanos: number("/counters/wal_sync_nanos")?,
        wal_generation_validation_bytes: number("/counters/wal_generation_validation_bytes")?,
        wal_generation_validation_nanos: number("/counters/wal_generation_validation_nanos")?,
        wal_retention_identity_checks: number("/counters/wal_retention_identity_checks")?,
        wal_retention_files_deleted: number("/counters/wal_retention_files_deleted")?,
        wal_retention_nanos: number("/counters/wal_retention_nanos")?,
        seal_rows: number("/counters/seal_rows")?,
        seal_output_bytes: number("/counters/seal_output_bytes")?,
        seal_nanos: number("/counters/seal_nanos")?,
        compaction_nanos: number("/counters/compaction_nanos")?,
        copy_parse_nanos: number("/counters/copy_parse_nanos")?,
        copy_commit_nanos: number("/counters/copy_commit_nanos")?,
        copy_total_nanos: number("/counters/copy_total_nanos")?,
        cold_constraint_nanos: number("/counters/cold_constraint_batch_nanos")?,
        cold_pk_nanos: number("/counters/cold_pk_batch_nanos")?,
        runtime_wait_nanos: number("/counters/runtime_profile/wait_nanos")?,
        manifest_publication_nanos: number(
            "/maintenance/compaction_cost/total_manifest_publication_nanos",
        )?,
        backpressure_wait_millis: number("/compaction_soft_backpressure_wait_millis")?,
        cold_segments: number("/cold_segments")?,
        storage_cpu_workers_effective: number("/storage_cpu_workers_effective")?,
        storage_cpu_workers_in_use: number("/storage_cpu_workers_in_use")?,
        storage_cpu_peak_workers_in_use: number("/storage_cpu_peak_workers_in_use")?,
        storage_cpu_workers_reserved: number("/storage_cpu_workers_reserved")?,
        storage_cpu_peak_workers_reserved: number("/storage_cpu_peak_workers_reserved")?,
        storage_cpu_leases: number("/storage_cpu_leases")?,
        storage_cpu_parallel_leases: number("/storage_cpu_parallel_leases")?,
    })
}

pub fn server_identity(config: &ResolvedDatabaseConfig) -> Result<String, String> {
    database::server_identity(config)
}

pub fn install_schema(
    connection: &mut DatabaseConnection,
    maximum_workers: usize,
) -> Result<(), String> {
    for sql in [
        "CREATE TABLE soak_organizations (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )",
        "CREATE TABLE soak_departments (
            id INTEGER PRIMARY KEY,
            organization_id INTEGER NOT NULL REFERENCES soak_organizations(id),
            name TEXT NOT NULL,
            UNIQUE (organization_id, name)
        )",
        "CREATE TABLE soak_employees (
            id INTEGER PRIMARY KEY,
            department_id INTEGER NOT NULL REFERENCES soak_departments(id),
            name TEXT NOT NULL
        )",
        "CREATE TABLE soak_transactions (
            id INTEGER PRIMARY KEY,
            worker_id INTEGER NOT NULL,
            sequence INTEGER NOT NULL,
            revision INTEGER NOT NULL CHECK (revision >= 0),
            amount INTEGER NOT NULL CHECK (amount >= 0),
            UNIQUE (worker_id, sequence)
        )",
        "CREATE TABLE soak_documents (
            id INTEGER PRIMARY KEY,
            transaction_id INTEGER NOT NULL UNIQUE REFERENCES soak_transactions(id),
            employee_id INTEGER NOT NULL REFERENCES soak_employees(id),
            revision INTEGER NOT NULL CHECK (revision >= 0),
            amount INTEGER NOT NULL CHECK (amount >= 0)
        )",
        "CREATE TABLE soak_lines (
            id INTEGER PRIMARY KEY,
            document_id INTEGER NOT NULL UNIQUE REFERENCES soak_documents(id),
            revision INTEGER NOT NULL CHECK (revision >= 0),
            amount INTEGER NOT NULL CHECK (amount >= 0)
        )",
        "CREATE TABLE soak_worker_state (
            worker_id INTEGER PRIMARY KEY,
            value INTEGER NOT NULL
        )",
        "CREATE TABLE soak_contention (
            id INTEGER PRIMARY KEY,
            value INTEGER NOT NULL
        )",
        "CREATE TABLE soak_accounts (
            id INTEGER PRIMARY KEY,
            employee_id INTEGER NOT NULL UNIQUE REFERENCES soak_employees(id),
            balance INTEGER NOT NULL
        )",
        "CREATE TABLE soak_postings (
            id INTEGER PRIMARY KEY,
            transaction_id INTEGER NOT NULL UNIQUE REFERENCES soak_transactions(id),
            account_id INTEGER NOT NULL REFERENCES soak_accounts(id),
            amount INTEGER NOT NULL CHECK (amount >= 0)
        )",
        "CREATE TABLE soak_messages (
            id INTEGER PRIMARY KEY,
            employee_id INTEGER NOT NULL REFERENCES soak_employees(id),
            revision INTEGER NOT NULL CHECK (revision >= 0)
        )",
        "CREATE TABLE soak_outbox (
            id INTEGER PRIMARY KEY,
            message_id INTEGER NOT NULL UNIQUE REFERENCES soak_messages(id),
            state INTEGER NOT NULL CHECK (state >= 0)
        )",
        "CREATE TABLE soak_sync_events (
            id INTEGER PRIMARY KEY,
            message_id INTEGER NOT NULL UNIQUE REFERENCES soak_messages(id),
            revision INTEGER NOT NULL CHECK (revision >= 0)
        )",
        "CREATE TABLE soak_reactions (
            id INTEGER PRIMARY KEY,
            message_id INTEGER NOT NULL UNIQUE REFERENCES soak_messages(id),
            employee_id INTEGER NOT NULL REFERENCES soak_employees(id)
        )",
        "CREATE TABLE soak_ledger (
            id INTEGER PRIMARY KEY,
            worker_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            action TEXT NOT NULL
        )",
        "CREATE TABLE soak_cold_rows (
            id INTEGER PRIMARY KEY,
            bucket INTEGER NOT NULL,
            value INTEGER NOT NULL,
            CHECK (bucket >= 0)
        )",
        "CREATE INDEX soak_cold_rows_bucket ON soak_cold_rows(bucket)",
        "CREATE VIEW soak_document_view AS
            SELECT d.id, d.revision, d.amount,
                   t.revision AS transaction_revision,
                   l.revision AS line_revision,
                   l.amount AS line_amount,
                   e.department_id
            FROM soak_documents d
            LEFT JOIN soak_transactions t ON d.transaction_id = t.id
            LEFT JOIN soak_lines l ON l.document_id = d.id
            LEFT JOIN soak_employees e ON d.employee_id = e.id",
        "CREATE VIEW soak_document_totals AS
            SELECT employee_id, COUNT(*) AS document_count, SUM(amount) AS total_amount
            FROM soak_documents GROUP BY employee_id",
        "CREATE VIEW soak_document_chain AS
            SELECT id, revision, line_revision, amount, line_amount
            FROM soak_document_view",
    ] {
        if connection.engine() == DatabaseEngine::Postgresql && sql.starts_with("CREATE TABLE") {
            command(connection, &sql.replace(" INTEGER", " BIGINT"))?;
        } else {
            command(connection, sql)?;
        }
    }
    command(
        connection,
        "INSERT INTO soak_organizations VALUES (1, 'root')",
    )?;
    command(
        connection,
        "INSERT INTO soak_departments VALUES (1, 1, 'operations')",
    )?;
    for worker in 0..maximum_workers {
        let id = worker + 1;
        command(
            connection,
            &format!("INSERT INTO soak_employees VALUES ({id}, 1, 'worker-{id}')"),
        )?;
        command(
            connection,
            &format!("INSERT INTO soak_worker_state VALUES ({id}, 0)"),
        )?;
        command(
            connection,
            &format!("INSERT INTO soak_accounts VALUES ({id}, {id}, 0)"),
        )?;
    }
    for id in 0..4 {
        command(
            connection,
            &format!("INSERT INTO soak_contention VALUES ({id}, 0)"),
        )?;
    }
    Ok(())
}

pub fn seed_cold_rows(
    connection: &mut DatabaseConnection,
    data_dir: &std::path::Path,
    total_rows: u64,
    copy_retry_budget: Duration,
    mut progress: impl FnMut(&mut DatabaseConnection, SeedMilestone) -> Result<(), String>,
) -> Result<(), String> {
    if total_rows == 0 {
        return Ok(());
    }
    fs::create_dir_all(data_dir).map_err(|error| format!("create import directory: {error}"))?;
    let path = data_dir.join(format!("soak-import-{}.csv", std::process::id()));
    if path.to_string_lossy().contains('\'') {
        return Err("soak import path contains an SQL quote".into());
    }
    let result = (|| {
        let mut loaded = 0u64;
        while loaded < total_rows {
            let chunk_started = Instant::now();
            let end = loaded.saturating_add(COLD_COPY_CHUNK_ROWS).min(total_rows);
            let csv_started = Instant::now();
            let file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .map_err(|error| format!("create cold-row import: {error}"))?;
            let mut writer = BufWriter::new(file);
            writer
                .write_all(b"id,bucket,value\n")
                .map_err(|error| error.to_string())?;
            for id in loaded..end {
                writeln!(writer, "{},{},{}", id + 1, id % 4096, id % 1_000_003)
                    .map_err(|error| error.to_string())?;
            }
            writer.flush().map_err(|error| error.to_string())?;
            writer
                .get_ref()
                .sync_all()
                .map_err(|error| error.to_string())?;
            let csv_elapsed = csv_started.elapsed();
            let copy_started = Instant::now();
            copy_chunk_with_retry(
                connection,
                copy_retry_budget,
                COLD_COPY_BACKPRESSURE_RETRY_PAUSE,
                |connection| connection.copy_csv(&path),
                |connection, retry_attempt, retry_elapsed| {
                    progress(
                        connection,
                        SeedMilestone::CopyBackpressure {
                            loaded_rows: loaded,
                            retry_attempt,
                            elapsed_nanos: duration_nanos(retry_elapsed),
                        },
                    )
                },
                loaded,
            )?;
            let copy_elapsed = copy_started.elapsed();
            fs::remove_file(&path).map_err(|error| error.to_string())?;
            let chunk_rows = end.saturating_sub(loaded);
            loaded = end;
            progress(
                connection,
                SeedMilestone::RowsLoaded(SeedChunkMetrics {
                    loaded_rows: loaded,
                    chunk_rows,
                    csv_nanos: duration_nanos(csv_elapsed),
                    copy_nanos: duration_nanos(copy_elapsed),
                    total_nanos: duration_nanos(chunk_started.elapsed()),
                }),
            )?;
        }
        // Durability oracle only. Memory flow control belongs to COPY/seal and
        // descriptor-backed postings; checkpoint-per-million was a workaround
        // that concealed linear cold-index heap growth.
        progress(connection, SeedMilestone::CheckpointStarted(loaded))?;
        checkpoint(connection)?;
        progress(connection, SeedMilestone::CheckpointCompleted(loaded))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&path);
    }
    result
}

fn copy_chunk_with_retry<T>(
    target: &mut T,
    retry_budget: Duration,
    retry_pause: Duration,
    mut copy: impl FnMut(&mut T) -> Result<(), database::CopyCsvError>,
    mut on_retry: impl FnMut(&mut T, u64, Duration) -> Result<(), String>,
    loaded_rows: u64,
) -> Result<(), String> {
    let started = Instant::now();
    let mut retry_attempt = 0_u64;
    loop {
        match copy(target) {
            Ok(()) => return Ok(()),
            Err(error) if error.is_retryable() => {
                retry_attempt = retry_attempt.saturating_add(1);
                let elapsed = started.elapsed();
                on_retry(target, retry_attempt, elapsed)?;
                if elapsed >= retry_budget {
                    return Err(format!(
                        "COPY chunk at {loaded_rows} rows exhausted the {:?} retry budget after {retry_attempt} explicit backpressure response(s): {error}",
                        retry_budget
                    ));
                }
                std::thread::sleep(retry_pause.min(retry_budget.saturating_sub(elapsed)));
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn worker_loop(
    config: ResolvedDatabaseConfig,
    worker_id: usize,
    seed: u64,
    sequence_start: u64,
    history_path: std::path::PathBuf,
    deadline: Instant,
    stop: Arc<AtomicBool>,
    recovering: Arc<AtomicBool>,
    recovery_epoch: Arc<AtomicU64>,
    metrics: Arc<RuntimeMetrics>,
) -> Result<(), String> {
    let history_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&history_path)
        .map_err(|error| format!("create worker history {}: {error}", history_path.display()))?;
    let mut history = BufWriter::new(history_file);
    history
        .write_all(b"worker,sequence,transaction_id,variant,terminal,outcome,latency_micros\n")
        .map_err(|error| error.to_string())?;
    let (mut connection, mut connection_epoch) =
        connect_stable(&config, &stop, &recovering, &recovery_epoch, deadline)?;
    let mut sequence = sequence_start;
    let mut random = SmallRng::seed_from_u64(
        seed ^ (worker_id as u64).rotate_left(23) ^ sequence_start.rotate_left(41),
    );
    let mut active = VecDeque::<i64>::new();
    while Instant::now() < deadline && !stop.load(Ordering::Acquire) {
        if recovery_boundary_changed(&recovering, &recovery_epoch, connection_epoch) {
            let _ = connection.shutdown();
            thread_wait_for_recovery(&recovering, &stop, deadline);
            if stop.load(Ordering::Acquire) || Instant::now() >= deadline {
                break;
            }
            (connection, connection_epoch) =
                connect_stable(&config, &stop, &recovering, &recovery_epoch, deadline)?;
        }
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| "worker sequence exhausted".to_string())?;
        metrics.planned();
        let started = Instant::now();
        let attempt_epoch = connection_epoch;
        let terminal = random.random_range(0..17);
        let variant = random.random::<u64>();
        let result = execute_transaction(&mut connection, worker_id, sequence, variant, &active);
        let outcome = match result {
            Ok(body) if terminal == 0 => {
                metrics.add_operations(body.operations);
                if let Err(error) = connection.shutdown() {
                    if !recovery_boundary_changed(&recovering, &recovery_epoch, attempt_epoch) {
                        return Err(error.to_string());
                    }
                    thread_wait_for_recovery(&recovering, &stop, deadline);
                }
                metrics.disconnected();
                if stop.load(Ordering::Acquire) || Instant::now() >= deadline {
                    break;
                }
                (connection, connection_epoch) =
                    connect_stable(&config, &stop, &recovering, &recovery_epoch, deadline)?;
                "disconnect_rollback"
            }
            Ok(body) if terminal == 1 || terminal == 2 => {
                metrics.add_operations(body.operations);
                match connection.rollback() {
                    Ok(()) => {
                        metrics.rolled_back();
                        "rollback"
                    }
                    Err(_)
                        if recovery_boundary_changed(
                            &recovering,
                            &recovery_epoch,
                            attempt_epoch,
                        ) =>
                    {
                        let _ = connection.shutdown();
                        thread_wait_for_recovery(&recovering, &stop, deadline);
                        (connection, connection_epoch) =
                            connect_stable(&config, &stop, &recovering, &recovery_epoch, deadline)?;
                        metrics.rolled_back();
                        "recovery_rollback"
                    }
                    Err(error) => return Err(error.to_string()),
                }
            }
            Ok(TransactionBody {
                id,
                deleted,
                operations,
            }) => match connection.commit() {
                Ok(()) => {
                    metrics.add_operations(operations);
                    if deleted.is_some() {
                        active.pop_front();
                    }
                    active.push_back(id);
                    metrics.committed();
                    "commit"
                }
                Err(error) if expected_conflict_text(&error) => {
                    rollback_if_active(&mut connection)?;
                    metrics.conflict();
                    "expected_conflict"
                }
                Err(_)
                    if recovery_boundary_changed(&recovering, &recovery_epoch, attempt_epoch) =>
                {
                    let (reconnected, committed) = resolve_after_reopen(
                        &config,
                        &stop,
                        &recovering,
                        &recovery_epoch,
                        deadline,
                        &mut active,
                        id,
                        deleted,
                        &metrics,
                    )?;
                    connection = reconnected.0;
                    connection_epoch = reconnected.1;
                    if committed {
                        "ambiguous_committed"
                    } else {
                        "ambiguous_rolled_back"
                    }
                }
                Err(error) => return Err(format!("worker {worker_id} commit: {error}")),
            },
            Err(error) if expected_conflict_text(&error) => {
                rollback_if_active(&mut connection)?;
                metrics.conflict();
                "expected_conflict"
            }
            Err(_) if recovery_boundary_changed(&recovering, &recovery_epoch, attempt_epoch) => {
                let id = transaction_id(worker_id, sequence)?;
                let deleted = (active.len() >= MAX_ACTIVE_PER_WORKER)
                    .then(|| active.front().copied())
                    .flatten();
                let (reconnected, committed) = resolve_after_reopen(
                    &config,
                    &stop,
                    &recovering,
                    &recovery_epoch,
                    deadline,
                    &mut active,
                    id,
                    deleted,
                    &metrics,
                )?;
                connection = reconnected.0;
                connection_epoch = reconnected.1;
                if committed {
                    "ambiguous_committed"
                } else {
                    "ambiguous_rolled_back"
                }
            }
            Err(error) => return Err(format!("worker {worker_id} transaction: {error}")),
        };
        let latency = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        metrics.record_latency_micros(latency);
        writeln!(
            history,
            "{worker_id},{sequence},{},{variant},{terminal},{outcome},{latency}",
            transaction_id(worker_id, sequence)?,
        )
        .map_err(|error| error.to_string())?;
        if sequence.is_multiple_of(4096) {
            history.flush().map_err(|error| error.to_string())?;
            history
                .get_ref()
                .sync_data()
                .map_err(|error| error.to_string())?;
        }
    }
    history.flush().map_err(|error| error.to_string())?;
    history
        .get_ref()
        .sync_data()
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn recovery_boundary_changed(
    recovering: &AtomicBool,
    recovery_epoch: &AtomicU64,
    connection_epoch: u64,
) -> bool {
    recovering.load(Ordering::Acquire) || recovery_epoch.load(Ordering::Acquire) != connection_epoch
}

fn thread_wait_for_recovery(recovering: &AtomicBool, stop: &AtomicBool, deadline: Instant) {
    while recovering.load(Ordering::Acquire)
        && !stop.load(Ordering::Acquire)
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn connect_stable(
    config: &ResolvedDatabaseConfig,
    stop: &AtomicBool,
    recovering: &AtomicBool,
    recovery_epoch: &AtomicU64,
    deadline: Instant,
) -> Result<(DatabaseConnection, u64), String> {
    connect_stable_with(stop, recovering, recovery_epoch, deadline, || {
        connect(config)
    })
}

fn connect_stable_with<T>(
    stop: &AtomicBool,
    recovering: &AtomicBool,
    recovery_epoch: &AtomicU64,
    deadline: Instant,
    mut connect_once: impl FnMut() -> Result<T, String>,
) -> Result<(T, u64), String> {
    let mut last_error = "recovery deadline expired".to_string();
    while !stop.load(Ordering::Acquire) && Instant::now() < deadline {
        thread_wait_for_recovery(recovering, stop, deadline);
        if stop.load(Ordering::Acquire) || Instant::now() >= deadline {
            break;
        }
        let attempt_epoch = recovery_epoch.load(Ordering::Acquire);
        match connect_once() {
            Ok(connection)
                if !recovery_boundary_changed(recovering, recovery_epoch, attempt_epoch) =>
            {
                return Ok((connection, attempt_epoch));
            }
            Ok(_) => {}
            Err(error) => last_error = error,
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(last_error)
}

#[allow(clippy::too_many_arguments)]
fn resolve_after_reopen(
    config: &ResolvedDatabaseConfig,
    stop: &AtomicBool,
    recovering: &AtomicBool,
    recovery_epoch: &AtomicU64,
    deadline: Instant,
    active: &mut VecDeque<i64>,
    id: i64,
    deleted: Option<i64>,
    metrics: &RuntimeMetrics,
) -> Result<((DatabaseConnection, u64), bool), String> {
    thread_wait_for_recovery(recovering, stop, deadline);
    let (mut connection, connection_epoch) =
        connect_stable(config, stop, recovering, recovery_epoch, deadline)?;
    let present = scalar_i64(
        &mut connection,
        &format!("SELECT COUNT(*) FROM soak_transactions WHERE id = {id}"),
    )?;
    let committed = match present {
        0 => {
            metrics.rolled_back();
            false
        }
        1 => {
            if deleted.is_some() {
                active.pop_front();
            }
            active.push_back(id);
            metrics.committed();
            true
        }
        other => return Err(format!("ambiguous transaction id {id} has {other} rows")),
    };
    metrics.ambiguous_resolved();
    Ok(((connection, connection_epoch), committed))
}

struct TransactionBody {
    id: i64,
    deleted: Option<i64>,
    operations: u64,
}

fn execute_transaction(
    connection: &mut DatabaseConnection,
    worker_id: usize,
    sequence: u64,
    variant: u64,
    active: &VecDeque<i64>,
) -> Result<TransactionBody, String> {
    connection.begin().map_err(|error| error.to_string())?;
    let id = transaction_id(worker_id, sequence)?;
    let employee_id = worker_id + 1;
    let revision = (sequence % 7) as i64;
    let amount = i64::try_from(sequence % 1_000_000).unwrap() + 1;
    let deleted = (active.len() >= MAX_ACTIVE_PER_WORKER)
        .then(|| active.front().copied())
        .flatten();

    let mut operations = 0u64;
    if let Some(old) = deleted {
        let old_amount = scalar_i64(
            connection,
            &format!("SELECT amount FROM soak_postings WHERE id = {old}"),
        )?;
        operations += 1;
        for sql in [
            format!("DELETE FROM soak_reactions WHERE id = {old}"),
            format!("DELETE FROM soak_sync_events WHERE id = {old}"),
            format!("DELETE FROM soak_outbox WHERE id = {old}"),
            format!("DELETE FROM soak_messages WHERE id = {old}"),
            format!("DELETE FROM soak_postings WHERE id = {old}"),
            format!("DELETE FROM soak_lines WHERE document_id = {old}"),
            format!("DELETE FROM soak_documents WHERE id = {old}"),
            format!("DELETE FROM soak_transactions WHERE id = {old}"),
        ] {
            command_exactly_one(connection, &sql)?;
            operations += 1;
        }
        command_exactly_one(
            connection,
            &format!(
                "UPDATE soak_accounts SET balance = balance - {old_amount} WHERE id = {employee_id}"
            ),
        )?;
        operations += 1;
    }
    for sql in [
        format!(
            "INSERT INTO soak_transactions VALUES ({id}, {}, {sequence}, {revision}, {amount})",
            worker_id + 1
        ),
        format!(
            "INSERT INTO soak_documents VALUES ({id}, {id}, {employee_id}, {revision}, {amount})"
        ),
        format!("INSERT INTO soak_lines VALUES ({id}, {id}, {revision}, {amount})"),
        format!("INSERT INTO soak_postings VALUES ({id}, {id}, {employee_id}, {amount})"),
        format!("UPDATE soak_accounts SET balance = balance + {amount} WHERE id = {employee_id}"),
        format!("INSERT INTO soak_messages VALUES ({id}, {employee_id}, {revision})"),
        format!("INSERT INTO soak_outbox VALUES ({id}, {id}, 0)"),
        format!("INSERT INTO soak_sync_events VALUES ({id}, {id}, {revision})"),
        format!("INSERT INTO soak_reactions VALUES ({id}, {id}, {employee_id})"),
        format!(
            "INSERT INTO soak_ledger VALUES ({id}, {}, {amount}, 'insert')",
            worker_id + 1
        ),
        format!(
            "INSERT INTO soak_worker_state VALUES ({employee_id}, 1)
             ON CONFLICT (worker_id) DO UPDATE SET value = soak_worker_state.value + 1"
        ),
    ] {
        command_exactly_one(connection, &sql)?;
        operations += 1;
    }
    if variant.is_multiple_of(3) {
        let next = revision + 1;
        for sql in [
            format!("UPDATE soak_transactions SET revision = {next} WHERE id = {id}"),
            format!("UPDATE soak_documents SET revision = {next} WHERE id = {id}"),
            format!("UPDATE soak_lines SET revision = {next} WHERE id = {id}"),
            format!("UPDATE soak_messages SET revision = {next} WHERE id = {id}"),
            format!("UPDATE soak_sync_events SET revision = {next} WHERE id = {id}"),
        ] {
            command_exactly_one(connection, &sql)?;
            operations += 1;
        }
    }
    if variant.is_multiple_of(5) {
        command_exactly_one(
            connection,
            &format!(
                "UPDATE soak_contention SET value = value + 1 WHERE id = {}",
                worker_id % 4
            ),
        )?;
        operations += 1;
    }
    Ok(TransactionBody {
        id,
        deleted,
        operations,
    })
}

fn transaction_id(worker_id: usize, sequence: u64) -> Result<i64, String> {
    let worker = i64::try_from(worker_id).map_err(|_| "worker id does not fit i64")?;
    let sequence = i64::try_from(sequence).map_err(|_| "sequence does not fit i64")?;
    ID_BASE
        .checked_add(
            worker
                .checked_mul(1_000_000_000)
                .ok_or("worker id overflow")?,
        )
        .and_then(|base| base.checked_add(sequence))
        .ok_or_else(|| "transaction id overflow".to_string())
}

fn rollback_if_active(connection: &mut DatabaseConnection) -> Result<(), String> {
    if connection.in_transaction() {
        connection.rollback().map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn expected_conflict_text(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("sqlstate 40001")
        || error.contains("sqlstate 40p01")
        || error.contains("serialization conflict")
        || error.contains("write conflict")
        || error.contains("timed out while waiting")
        || error.contains("unique constraint")
        || error.contains("could not serialize access")
        || error.contains("deadlock detected")
}

pub fn check_invariants(
    connection: &mut DatabaseConnection,
    expected_cold_rows: u64,
) -> Result<Vec<InvariantObservation>, String> {
    connection
        .begin_snapshot()
        .map_err(|error| format!("begin invariant snapshot: {error}"))?;
    let result = check_invariants_at_current_snapshot(connection, expected_cold_rows);
    let rollback = connection
        .rollback()
        .map_err(|error| format!("rollback invariant snapshot: {error}"));
    match (result, rollback) {
        (Ok(observations), Ok(())) => Ok(observations),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(rollback)) => Err(rollback),
        (Err(error), Err(rollback)) => Err(format!("{error}; {rollback}")),
    }
}

fn check_invariants_at_current_snapshot(
    connection: &mut DatabaseConnection,
    expected_cold_rows: u64,
) -> Result<Vec<InvariantObservation>, String> {
    let snapshot_start_documents = scalar_i64(connection, "SELECT COUNT(*) FROM soak_documents")?;
    let checks = [
        (
            "document_has_transaction",
            "SELECT COUNT(*) FROM soak_documents d
             LEFT JOIN soak_transactions t ON d.transaction_id = t.id
             WHERE t.id IS NULL",
        ),
        (
            "transaction_has_document",
            "SELECT COUNT(*) FROM soak_transactions t
             LEFT JOIN soak_documents d ON d.transaction_id = t.id
             WHERE d.id IS NULL",
        ),
        (
            "document_has_line",
            "SELECT COUNT(*) FROM soak_documents d
             LEFT JOIN soak_lines l ON l.document_id = d.id
             WHERE l.id IS NULL",
        ),
        (
            "revisions_match",
            "SELECT COUNT(*) FROM soak_document_view
             WHERE revision <> transaction_revision OR revision <> line_revision OR amount <> line_amount",
        ),
        (
            "message_graph_complete",
            "SELECT COUNT(*) FROM soak_messages m
             LEFT JOIN soak_outbox o ON o.message_id = m.id
             LEFT JOIN soak_sync_events s ON s.message_id = m.id
             LEFT JOIN soak_reactions r ON r.message_id = m.id
             WHERE o.id IS NULL OR s.id IS NULL OR r.id IS NULL",
        ),
        (
            "posting_graph_complete",
            "SELECT COUNT(*) FROM soak_transactions t
             LEFT JOIN soak_postings p ON p.transaction_id = t.id
             WHERE p.id IS NULL",
        ),
    ];
    let mut observations = Vec::with_capacity(checks.len() + 2);
    for (name, sql) in checks {
        let failures = scalar_i64(connection, sql)?;
        let detail = if name == "posting_graph_complete" && failures > 0 {
            let first_missing = scalar_i64(
                connection,
                "SELECT COALESCE(MIN(t.id), 0) FROM soak_transactions t
                 LEFT JOIN soak_postings p ON p.transaction_id = t.id
                 WHERE p.id IS NULL",
            )?;
            let transaction_visible = scalar_i64(
                connection,
                &format!("SELECT COUNT(*) FROM soak_transactions WHERE id = {first_missing}"),
            )?;
            let posting_by_id = scalar_i64(
                connection,
                &format!("SELECT COUNT(*) FROM soak_postings WHERE id = {first_missing}"),
            )?;
            let posting_by_transaction = scalar_i64(
                connection,
                &format!(
                    "SELECT COUNT(*) FROM soak_postings WHERE transaction_id = {first_missing}"
                ),
            )?;
            format!(
                "violating_rows={failures} first_missing_transaction={first_missing} \
                 transaction_visible={transaction_visible} posting_by_id={posting_by_id} \
                 posting_by_transaction={posting_by_transaction}"
            )
        } else {
            format!("violating_rows={failures}")
        };
        observations.push(InvariantObservation {
            name: name.into(),
            failures: u64::try_from(failures).map_err(|_| "negative invariant count")?,
            detail,
        });
    }
    let base = scalar_i64(connection, "SELECT COUNT(*) FROM soak_documents")?;
    let view = scalar_i64(connection, "SELECT COUNT(*) FROM soak_document_view")?;
    let view_detail = if base != view {
        let base_count_id = scalar_i64(connection, "SELECT COUNT(id) FROM soak_documents")?;
        let base_filtered = scalar_i64(
            connection,
            "SELECT COUNT(d.id) FROM soak_documents d WHERE d.id >= 0",
        )?;
        let base_sum = scalar_i64(
            connection,
            "SELECT COALESCE(SUM(id), 0) FROM soak_documents",
        )?;
        let view_sum = scalar_i64(
            connection,
            "SELECT COALESCE(SUM(id), 0) FROM soak_document_view",
        )?;
        format!(
            "base={base} view={view} base_count_id={base_count_id} \
             base_filtered={base_filtered} base_sum={base_sum} view_sum={view_sum}"
        )
    } else {
        format!("base={base} view={view}")
    };
    observations.push(InvariantObservation {
        name: "view_cardinality".into(),
        failures: u64::from(base != view),
        detail: view_detail,
    });
    let document_chain = scalar_i64(connection, "SELECT COUNT(*) FROM soak_document_chain")?;
    observations.push(InvariantObservation {
        name: "dependent_view_cardinality".into(),
        failures: u64::from(base != document_chain),
        detail: format!("base={base} dependent_view={document_chain}"),
    });
    let balances = scalar_i64(
        connection,
        "SELECT COALESCE(SUM(balance), 0) FROM soak_accounts",
    )?;
    let postings = scalar_i64(
        connection,
        "SELECT COALESCE(SUM(amount), 0) FROM soak_postings",
    )?;
    observations.push(InvariantObservation {
        name: "account_balance".into(),
        failures: u64::from(balances != postings),
        detail: format!("balances={balances} postings={postings}"),
    });
    let classic = scalar_i64(
        connection,
        "SELECT COUNT(*) FROM soak_documents d
         LEFT JOIN soak_employees e ON d.employee_id = e.id
         LEFT JOIN soak_departments p ON e.department_id = p.id
         LEFT JOIN soak_organizations o ON p.organization_id = o.id
         WHERE o.name = 'root'",
    )?;
    let navigation_sql = if connection.engine() == DatabaseEngine::Radixdb {
        "SELECT COUNT(*) FROM soak_documents d WHERE d.employee_id.department_id.organization_id.name = 'root'"
    } else {
        "SELECT COUNT(*) FROM soak_documents d
         LEFT JOIN soak_employees e ON d.employee_id = e.id
         LEFT JOIN soak_departments p ON e.department_id = p.id
         LEFT JOIN soak_organizations o ON p.organization_id = o.id
         WHERE o.name = 'root'"
    };
    let navigation = scalar_i64(connection, navigation_sql)?;
    observations.push(InvariantObservation {
        name: "navigation_classic_parity".into(),
        failures: u64::from(classic != navigation),
        detail: format!("classic={classic} navigation={navigation}"),
    });
    let cold_rows = scalar_i64(connection, "SELECT COUNT(*) FROM soak_cold_rows")?;
    let expected_cold_rows = i64::try_from(expected_cold_rows)
        .map_err(|_| "expected cold row count does not fit i64")?;
    observations.push(InvariantObservation {
        name: "cold_fixture_cardinality".into(),
        failures: u64::from(cold_rows != expected_cold_rows),
        detail: format!("expected={expected_cold_rows} actual={cold_rows}"),
    });
    let snapshot_end_documents = scalar_i64(connection, "SELECT COUNT(*) FROM soak_documents")?;
    observations.push(InvariantObservation {
        name: "snapshot_repeatable_read".into(),
        failures: u64::from(snapshot_start_documents != snapshot_end_documents),
        detail: format!("start={snapshot_start_documents} end={snapshot_end_documents}"),
    });
    Ok(observations)
}

#[derive(Clone, Debug)]
pub struct InvariantObservation {
    pub name: String,
    pub failures: u64,
    pub detail: String,
}

pub fn checkpoint(connection: &mut DatabaseConnection) -> Result<(), String> {
    let sql = match connection.engine() {
        DatabaseEngine::Radixdb => "PRAGMA CHECKPOINT",
        DatabaseEngine::Postgresql => "CHECKPOINT",
    };
    command(connection, sql)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckpointOutcome {
    Completed,
    Deferred(String),
}

pub fn checkpoint_during_load(
    connection: &mut DatabaseConnection,
) -> Result<CheckpointOutcome, String> {
    match checkpoint(connection) {
        Ok(()) => Ok(CheckpointOutcome::Completed),
        Err(error) if expected_checkpoint_busy(&error) => Ok(CheckpointOutcome::Deferred(error)),
        Err(error) => Err(error),
    }
}

fn expected_checkpoint_busy(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("forced checkpoint left committed hot rows unsealed")
        || error.contains("checkpoint timed out acquiring the commit fence")
}

pub fn snapshot(connection: &mut DatabaseConnection) -> Result<(), String> {
    if connection.engine() != DatabaseEngine::Radixdb {
        return Err("native snapshot is not part of the PostgreSQL comparison contract".into());
    }
    command(connection, "PRAGMA SNAPSHOT")
}

pub fn restore(connection: &mut DatabaseConnection) -> Result<(), String> {
    if connection.engine() != DatabaseEngine::Radixdb {
        return Err("native restore is not part of the PostgreSQL comparison contract".into());
    }
    command(connection, "PRAGMA RESTORE")
}

pub fn logical_digest(connection: &mut DatabaseConnection) -> Result<String, String> {
    let queries = [
        "SELECT COUNT(*) FROM soak_cold_rows",
        "SELECT COALESCE(SUM(value), 0) FROM soak_cold_rows",
        "SELECT COUNT(*) FROM soak_transactions",
        "SELECT COALESCE(SUM(amount), 0) FROM soak_transactions",
        "SELECT COUNT(*) FROM soak_documents",
        "SELECT COALESCE(SUM(revision), 0) FROM soak_documents",
        "SELECT COUNT(*) FROM soak_lines",
        "SELECT COUNT(*) FROM soak_messages",
        "SELECT COUNT(*) FROM soak_outbox",
        "SELECT COUNT(*) FROM soak_sync_events",
        "SELECT COUNT(*) FROM soak_reactions",
        "SELECT COUNT(*) FROM soak_ledger",
        "SELECT COALESCE(SUM(balance), 0) FROM soak_accounts",
        "SELECT COALESCE(SUM(value), 0) FROM soak_worker_state",
        "SELECT COALESCE(SUM(value), 0) FROM soak_contention",
        "SELECT COUNT(*) FROM soak_document_view",
        "SELECT COUNT(*) FROM soak_document_totals",
        "SELECT COUNT(*) FROM soak_document_chain",
    ];
    let mut digest = Sha256::new();
    for query in queries {
        let value = scalar_i64(connection, query)?;
        digest.update((query.len() as u64).to_le_bytes());
        digest.update(query.as_bytes());
        digest.update(value.to_le_bytes());
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn command(connection: &mut DatabaseConnection, sql: &str) -> Result<(), String> {
    connection.command(sql)
}

fn command_exactly_one(connection: &mut DatabaseConnection, sql: &str) -> Result<(), String> {
    connection.command_exactly_one(sql)
}

fn scalar_i64(connection: &mut DatabaseConnection, sql: &str) -> Result<i64, String> {
    connection.scalar_i64(sql)
}

pub fn phase_deadline(duration: Duration) -> Instant {
    Instant::now() + duration
}

pub fn address_for_log(address: SocketAddr) -> String {
    address.to_string()
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        time::{Duration, Instant},
    };

    use super::{
        connect_stable_with, copy_chunk_with_retry, expected_checkpoint_busy,
        expected_conflict_text, recovery_boundary_changed,
    };

    use crate::database::CopyCsvError;

    #[test]
    fn conflict_classifier_uses_postgresql_sqlstate_not_localized_text() {
        assert!(expected_conflict_text(
            "PostgreSQL SQLSTATE 40001: не удалось сериализовать доступ"
        ));
        assert!(expected_conflict_text(
            "PostgreSQL SQLSTATE 40P01: обнаружена взаимоблокировка"
        ));
        assert!(expected_conflict_text(
            "PostgreSQL SQLSTATE 40001: could not serialize access"
        ));
        assert!(!expected_conflict_text(
            "PostgreSQL SQLSTATE 23503: foreign key violation"
        ));
    }

    #[test]
    fn checkpoint_busy_classifier_is_fail_closed() {
        assert!(expected_checkpoint_busy(
            "server SqlError: forced checkpoint left committed hot rows unsealed"
        ));
        assert!(expected_checkpoint_busy(
            "server SqlError: checkpoint timed out acquiring the commit fence"
        ));
        assert!(!expected_checkpoint_busy(
            "server SqlError: checksum mismatch"
        ));
        assert!(!expected_checkpoint_busy("database busy"));
    }

    #[test]
    fn completed_recovery_epoch_still_invalidates_an_old_connection() {
        let recovering = AtomicBool::new(false);
        let epoch = AtomicU64::new(0);
        let connection_epoch = epoch.load(Ordering::Acquire);

        recovering.store(true, Ordering::Release);
        epoch.fetch_add(1, Ordering::AcqRel);
        epoch.fetch_add(1, Ordering::AcqRel);
        recovering.store(false, Ordering::Release);

        assert!(recovery_boundary_changed(
            &recovering,
            &epoch,
            connection_epoch
        ));
        assert!(!recovery_boundary_changed(
            &recovering,
            &epoch,
            epoch.load(Ordering::Acquire)
        ));
    }

    #[test]
    fn connection_opened_across_recovery_is_discarded_and_retried() {
        let stop = AtomicBool::new(false);
        let recovering = AtomicBool::new(false);
        let epoch = AtomicU64::new(0);
        let attempts = AtomicUsize::new(0);

        let (connection, connection_epoch) = connect_stable_with(
            &stop,
            &recovering,
            &epoch,
            Instant::now() + Duration::from_secs(1),
            || {
                let attempt = attempts.fetch_add(1, Ordering::AcqRel);
                if attempt == 0 {
                    recovering.store(true, Ordering::Release);
                    epoch.fetch_add(1, Ordering::AcqRel);
                    epoch.fetch_add(1, Ordering::AcqRel);
                    recovering.store(false, Ordering::Release);
                }
                Ok(attempt)
            },
        )
        .unwrap();

        assert_eq!(connection, 1);
        assert_eq!(attempts.load(Ordering::Acquire), 2);
        assert_eq!(connection_epoch, epoch.load(Ordering::Acquire));
    }

    #[test]
    fn copy_chunk_retries_only_explicit_backpressure_without_advancing_input() {
        let mut submitted = 0_u64;
        let mut retries = Vec::new();
        copy_chunk_with_retry(
            &mut submitted,
            Duration::from_secs(1),
            Duration::ZERO,
            |submitted| {
                *submitted += 1;
                if *submitted <= 2 {
                    Err(CopyCsvError::Retryable("hard L0 debt".to_string()))
                } else {
                    Ok(())
                }
            },
            |_, attempt, _| {
                retries.push(attempt);
                Ok(())
            },
            250_000,
        )
        .unwrap();
        assert_eq!(submitted, 3);
        assert_eq!(retries, vec![1, 2]);
    }

    #[test]
    fn copy_chunk_never_retries_an_unclassified_failure() {
        let mut submitted = 0_u64;
        let error = copy_chunk_with_retry(
            &mut submitted,
            Duration::from_secs(1),
            Duration::ZERO,
            |submitted| {
                *submitted += 1;
                Err(CopyCsvError::Fatal("outcome unknown".to_string()))
            },
            |_, _, _| panic!("fatal failure must not emit a retry milestone"),
            250_000,
        )
        .unwrap_err();
        assert_eq!(submitted, 1);
        assert_eq!(error, "outcome unknown");
    }

    #[test]
    fn copy_chunk_reports_retry_budget_exhaustion() {
        let mut submitted = 0_u64;
        let error = copy_chunk_with_retry(
            &mut submitted,
            Duration::ZERO,
            Duration::ZERO,
            |submitted| {
                *submitted += 1;
                Err(CopyCsvError::Retryable("hard L0 debt".to_string()))
            },
            |_, attempt, _| {
                assert_eq!(attempt, 1);
                Ok(())
            },
            250_000,
        )
        .unwrap_err();
        assert_eq!(submitted, 1);
        assert!(error.contains("exhausted"));
        assert!(error.contains("explicit backpressure"));
    }
}
