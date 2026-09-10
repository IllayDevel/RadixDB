//! Stock durable Job scheduler host.
//!
//! The catalog remains the authority for definitions. Two ordinary protected
//! relations are the durable execution ledger and the bounded public history
//! surface. The storage process lock supplies the cross-process ownership
//! boundary; the conditional state update supplies the worker boundary.

#[cfg(test)]
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use crate::api::{
    ServerCancellation, ServerJobAttemptMetadata, ServerJobDiagnostic,
    ServerScheduledJobDefinition, ServerScheduledJobSchedule,
};
use crate::{Database, Error, Result};

use super::session::{
    open_database_with_plugin_registry, release_database_lease, DatabaseRegistryEntry,
};
use super::ServerConfig;
use radixdb_plugin_host::PluginRegistry;
use std::sync::Arc;

pub const JOB_STATE_RELATION: &str = "radix_system_job_state";
pub const JOB_HISTORY_RELATION: &str = "radix_system_job_history";
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const ATTEMPT_LEASE_NS: i64 = 300_000_000_000;
const MAX_ATTEMPTS: i64 = 5;
const HISTORY_PER_JOB: i64 = 256;
const RETENTION_DELETE_BATCH: usize = 256;

#[derive(Default)]
pub(crate) struct JobSchedulerRuntime {
    pub cycles: std::sync::atomic::AtomicU64,
    pub attempts_started: std::sync::atomic::AtomicU64,
    pub attempts_succeeded: std::sync::atomic::AtomicU64,
    pub attempts_failed: std::sync::atomic::AtomicU64,
    pub active_attempts: std::sync::atomic::AtomicU64,
    pub last_error: Mutex<Option<String>>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JobSchedulerCycle {
    pub started: u64,
    pub succeeded: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DurableJobState {
    definition_version: i64,
    planned_ns: i64,
    attempt: i64,
    state: String,
    retry_after_ns: i64,
    lease_until_ns: i64,
    idempotency_key: String,
}

pub(crate) fn run_loop(
    config: &ServerConfig,
    databases: &Mutex<std::collections::BTreeMap<String, DatabaseRegistryEntry>>,
    plugin_registry: Arc<PluginRegistry>,
    stop: &AtomicBool,
    cancellation: &ServerCancellation,
    runtime: &JobSchedulerRuntime,
) {
    while !stop.load(Ordering::Acquire) && !cancellation.is_cancelled() {
        // Give foreground sessions the first opportunity to open a database.
        // Opening is single-owner and intentionally reports a retryable state;
        // racing every startup/restart from the background scheduler only adds
        // avoidable admission latency without improving Job timeliness.
        thread::sleep(POLL_INTERVAL);
        if stop.load(Ordering::Acquire) || cancellation.is_cancelled() {
            break;
        }
        discover_databases(config, databases, Arc::clone(&plugin_registry), runtime);
        let ready = match databases.lock() {
            Ok(guard) => guard
                .iter()
                .filter_map(|(name, entry)| match entry {
                    DatabaseRegistryEntry::Ready { database, .. } => {
                        Some((name.clone(), database.clone()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>(),
            Err(_) => {
                record_error(runtime, "database registry is poisoned".to_string());
                break;
            }
        };
        for (name, database) in ready {
            if stop.load(Ordering::Acquire) || cancellation.is_cancelled() {
                break;
            }
            runtime.cycles.fetch_add(1, Ordering::Relaxed);
            match run_cycle(&database, cancellation, Some(runtime)) {
                Ok(_) => {}
                Err(error) => record_error(runtime, format!("database {name}: {error}")),
            }
        }
    }
}

fn discover_databases(
    config: &ServerConfig,
    databases: &Mutex<std::collections::BTreeMap<String, DatabaseRegistryEntry>>,
    plugin_registry: Arc<PluginRegistry>,
    runtime: &JobSchedulerRuntime,
) {
    let root = config.data_dir.join("databases");
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let should_open = databases
            .lock()
            .map(|guard| match guard.get(&name) {
                None => true,
                Some(DatabaseRegistryEntry::Failed { retry_after, .. }) => {
                    Instant::now() >= *retry_after
                }
                Some(DatabaseRegistryEntry::Opening { .. })
                | Some(DatabaseRegistryEntry::Ready { .. }) => false,
            })
            .unwrap_or(false);
        if !should_open {
            continue;
        }
        match open_database_with_plugin_registry(
            config,
            databases,
            &name,
            Arc::clone(&plugin_registry),
        ) {
            Ok(_) => {
                if let Err(error) = release_database_lease(databases, &name) {
                    record_error(runtime, format!("database {name}: {error}"));
                }
            }
            Err(error) => record_error(runtime, format!("database {name}: {error}")),
        }
    }
}

fn record_error(runtime: &JobSchedulerRuntime, mut error: String) {
    if error.len() > 4096 {
        let mut boundary = 4096;
        while !error.is_char_boundary(boundary) {
            boundary -= 1;
        }
        error.truncate(boundary);
    }
    if let Ok(mut slot) = runtime.last_error.lock() {
        *slot = Some(error);
    }
}

pub(crate) fn run_cycle(
    database: &Database,
    cancellation: &ServerCancellation,
    runtime: Option<&JobSchedulerRuntime>,
) -> Result<JobSchedulerCycle> {
    let jobs = database.scheduled_jobs_snapshot()?;
    if jobs.is_empty() {
        return Ok(JobSchedulerCycle::default());
    }
    ensure_ledger(database)?;
    let now = unix_now_ns()?;
    let mut cycle = JobSchedulerCycle::default();
    for job in jobs {
        if cancellation.is_cancelled() {
            break;
        }
        match service_job(database, cancellation, &job, now, runtime)? {
            ServiceOutcome::NotDue => {}
            ServiceOutcome::Succeeded => {
                cycle.started += 1;
                cycle.succeeded += 1;
            }
            ServiceOutcome::Failed => {
                cycle.started += 1;
                cycle.failed += 1;
            }
        }
    }
    Ok(cycle)
}

fn ensure_ledger(database: &Database) -> Result<()> {
    database.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS {JOB_STATE_RELATION} (\
             job_id UUID PRIMARY KEY, definition_version INTEGER NOT NULL, \
             planned_ns INTEGER NOT NULL, attempt INTEGER NOT NULL, state TEXT NOT NULL, \
             retry_after_ns INTEGER NOT NULL, lease_until_ns INTEGER NOT NULL, \
             idempotency_key TEXT NOT NULL, updated_ns INTEGER NOT NULL)"
        ),
        (),
    )?;
    database.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS {JOB_HISTORY_RELATION} (\
             sequence INTEGER PRIMARY KEY AUTO_INCREMENT, job_id UUID NOT NULL, \
             planned_ns INTEGER NOT NULL, attempt INTEGER NOT NULL, \
             idempotency_key TEXT NOT NULL, started_ns INTEGER NOT NULL, \
             finished_ns INTEGER NOT NULL, outcome TEXT NOT NULL, \
             cause_kind TEXT NOT NULL, detail TEXT NOT NULL)"
        ),
        (),
    )?;
    Ok(())
}

fn service_job(
    database: &Database,
    cancellation: &ServerCancellation,
    job: &ServerScheduledJobDefinition,
    now: i64,
    runtime: Option<&JobSchedulerRuntime>,
) -> Result<ServiceOutcome> {
    let job_id = job.job_id.to_string();
    let job_uuid = uuid::Uuid::from_bytes(job.job_id.into_bytes());
    let expected_version = i64::from(job.definition_version);
    let mut state = match load_state(database, job_uuid)? {
        Some(state) if state.definition_version == expected_version => state,
        Some(_) => {
            reset_state(
                database,
                job_uuid,
                expected_version,
                initial_planned(job, now)?,
                now,
            )?;
            load_state(database, job_uuid)?.expect("reset state must exist")
        }
        None => {
            insert_state(
                database,
                job_uuid,
                expected_version,
                initial_planned(job, now)?,
                now,
            )?;
            load_state(database, job_uuid)?.expect("inserted state must exist")
        }
    };

    if state.state == "complete" || state.state == "failed" {
        return Ok(ServiceOutcome::NotDue);
    }
    if state.state == "running" && state.lease_until_ns > now {
        return Ok(ServiceOutcome::NotDue);
    }
    if state.state == "retry" && state.retry_after_ns > now {
        return Ok(ServiceOutcome::NotDue);
    }

    if state.state == "idle" {
        state.planned_ns = coalesced_planned(job, state.planned_ns, now)?;
        if state.planned_ns > now {
            return Ok(ServiceOutcome::NotDue);
        }
    }
    let attempt = if state.state == "idle" {
        1
    } else {
        state.attempt + 1
    };
    if attempt > MAX_ATTEMPTS {
        finalize_exhausted(database, job_uuid, job, &state, now)?;
        return Ok(ServiceOutcome::NotDue);
    }
    let idempotency_key = if state.idempotency_key.is_empty() || state.state == "idle" {
        format!("{job_id}/{}", state.planned_ns)
    } else {
        state.idempotency_key.clone()
    };
    if !claim_attempt(
        database,
        job_uuid,
        expected_version,
        &state,
        attempt,
        &idempotency_key,
        now,
    )? {
        return Ok(ServiceOutcome::NotDue);
    }

    if let Some(runtime) = runtime {
        runtime.attempts_started.fetch_add(1, Ordering::Relaxed);
        runtime.active_attempts.fetch_add(1, Ordering::AcqRel);
    }
    let outcome = database.execute_scheduled_job_attempt(
        job.job_id,
        ServerJobAttemptMetadata {
            scheduled_at_unix_ns: state.planned_ns,
            attempt: u32::try_from(attempt)
                .map_err(|_| Error::internal("scheduler attempt number overflow"))?,
            idempotency_key: idempotency_key.clone(),
        },
        cancellation,
    );
    if let Some(runtime) = runtime {
        runtime.active_attempts.fetch_sub(1, Ordering::AcqRel);
    }
    let finished = unix_now_ns()?;
    let service_outcome = match outcome {
        Ok(_) => {
            finalize_success(
                database,
                job_uuid,
                job,
                state.planned_ns,
                attempt,
                &idempotency_key,
                finished,
            )?;
            if let Some(runtime) = runtime {
                runtime.attempts_succeeded.fetch_add(1, Ordering::Relaxed);
            }
            ServiceOutcome::Succeeded
        }
        Err(diagnostic) => {
            finalize_failure(
                database,
                job_uuid,
                job,
                state.planned_ns,
                attempt,
                &idempotency_key,
                finished,
                &diagnostic,
            )?;
            if let Some(runtime) = runtime {
                runtime.attempts_failed.fetch_add(1, Ordering::Relaxed);
                record_error(runtime, diagnostic.to_string());
            }
            ServiceOutcome::Failed
        }
    };
    prune_history(database, job_uuid)?;
    Ok(service_outcome)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServiceOutcome {
    NotDue,
    Succeeded,
    Failed,
}

fn initial_planned(job: &ServerScheduledJobDefinition, now: i64) -> Result<i64> {
    match job.schedule {
        ServerScheduledJobSchedule::EveryNs(interval) => now
            .checked_add(i64::try_from(interval).map_err(|_| {
                Error::invalid_argument("job interval exceeds scheduler timestamp range")
            })?)
            .ok_or_else(|| Error::invalid_argument("initial job schedule overflow")),
        ServerScheduledJobSchedule::AtUnixNs(timestamp) => Ok(timestamp),
    }
}

fn coalesced_planned(job: &ServerScheduledJobDefinition, planned: i64, now: i64) -> Result<i64> {
    let ServerScheduledJobSchedule::EveryNs(interval) = job.schedule else {
        return Ok(planned);
    };
    if planned >= now {
        return Ok(planned);
    }
    let interval = i64::try_from(interval)
        .map_err(|_| Error::invalid_argument("job interval exceeds scheduler timestamp range"))?;
    let missed = (now - planned) / interval;
    planned
        .checked_add(missed.saturating_mul(interval))
        .ok_or_else(|| Error::invalid_argument("coalesced job schedule overflow"))
}

fn next_planned(job: &ServerScheduledJobDefinition, planned: i64) -> Result<Option<i64>> {
    match job.schedule {
        ServerScheduledJobSchedule::EveryNs(interval) => Ok(Some(
            planned
                .checked_add(i64::try_from(interval).map_err(|_| {
                    Error::invalid_argument("job interval exceeds scheduler timestamp range")
                })?)
                .ok_or_else(|| Error::invalid_argument("next job schedule overflow"))?,
        )),
        ServerScheduledJobSchedule::AtUnixNs(_) => Ok(None),
    }
}

fn load_state(database: &Database, job_id: uuid::Uuid) -> Result<Option<DurableJobState>> {
    let mut rows = database.query(
        &format!(
            "SELECT definition_version, planned_ns, attempt, state, retry_after_ns, \
             lease_until_ns, idempotency_key FROM {JOB_STATE_RELATION} WHERE job_id = $1"
        ),
        (job_id,),
    )?;
    let Some(row) = rows.next() else {
        return Ok(None);
    };
    let row = row?;
    Ok(Some(DurableJobState {
        definition_version: row.get(0)?,
        planned_ns: row.get(1)?,
        attempt: row.get(2)?,
        state: row.get(3)?,
        retry_after_ns: row.get(4)?,
        lease_until_ns: row.get(5)?,
        idempotency_key: row.get(6)?,
    }))
}

fn insert_state(
    database: &Database,
    job_id: uuid::Uuid,
    version: i64,
    planned: i64,
    now: i64,
) -> Result<()> {
    database.execute(
        &format!(
            "INSERT INTO {JOB_STATE_RELATION} VALUES \
             ($1, $2, $3, 0, 'idle', 0, 0, '', $4)"
        ),
        (job_id, version, planned, now),
    )?;
    Ok(())
}

fn reset_state(
    database: &Database,
    job_id: uuid::Uuid,
    version: i64,
    planned: i64,
    now: i64,
) -> Result<()> {
    database.execute(
        &format!(
            "UPDATE {JOB_STATE_RELATION} SET definition_version = $1, planned_ns = $2, \
             attempt = 0, state = 'idle', retry_after_ns = 0, lease_until_ns = 0, \
             idempotency_key = '', updated_ns = $3 WHERE job_id = $4"
        ),
        (version, planned, now, job_id),
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn claim_attempt(
    database: &Database,
    job_id: uuid::Uuid,
    version: i64,
    state: &DurableJobState,
    attempt: i64,
    key: &str,
    now: i64,
) -> Result<bool> {
    let lease_until = now
        .checked_add(ATTEMPT_LEASE_NS)
        .ok_or_else(|| Error::invalid_argument("job lease timestamp overflow"))?;
    let mut transaction = database.begin()?;
    let affected = transaction.execute(
        &format!(
            "UPDATE {JOB_STATE_RELATION} SET planned_ns = $1, attempt = $2, state = 'running', \
             retry_after_ns = 0, lease_until_ns = $3, idempotency_key = $4, updated_ns = $5 \
             WHERE job_id = $6 AND definition_version = $7 AND state = $8 \
             AND attempt = $9 AND planned_ns = $10"
        ),
        (
            state.planned_ns,
            attempt,
            lease_until,
            key,
            now,
            job_id,
            version,
            state.state.as_str(),
            state.attempt,
            state.planned_ns,
        ),
    )?;
    if affected != 1 {
        transaction.rollback()?;
        return Ok(false);
    }
    transaction.execute(
        &format!(
            "INSERT INTO {JOB_HISTORY_RELATION} \
             (job_id, planned_ns, attempt, idempotency_key, started_ns, finished_ns, outcome, cause_kind, detail) \
             VALUES ($1, $2, $3, $4, $5, 0, 'running', '', '')"
        ),
        (job_id, state.planned_ns, attempt, key, now),
    )?;
    transaction.commit()?;
    Ok(true)
}

fn finalize_success(
    database: &Database,
    job_id: uuid::Uuid,
    job: &ServerScheduledJobDefinition,
    planned: i64,
    attempt: i64,
    key: &str,
    finished: i64,
) -> Result<()> {
    let next = next_planned(job, planned)?;
    finalize_attempt(
        database,
        job_id,
        planned,
        attempt,
        key,
        finished,
        "succeeded",
        "",
        "",
        next.map_or("complete", |_| "idle"),
        next.unwrap_or(planned),
        0,
        0,
        "",
    )
}

#[allow(clippy::too_many_arguments)]
fn finalize_failure(
    database: &Database,
    job_id: uuid::Uuid,
    job: &ServerScheduledJobDefinition,
    planned: i64,
    attempt: i64,
    key: &str,
    finished: i64,
    diagnostic: &ServerJobDiagnostic,
) -> Result<()> {
    let retryable = diagnostic.details().iter().any(|detail| {
        detail.key == "scheduler_retryable" && detail.value.eq_ignore_ascii_case("true")
    });
    let cause = diagnostic
        .cause()
        .map_or_else(String::new, |kind| kind.as_str().to_string());
    if retryable && attempt < MAX_ATTEMPTS {
        let shift = u32::try_from(attempt.saturating_sub(1))
            .unwrap_or(31)
            .min(7);
        let backoff_ns = 100_000_000_i64.saturating_mul(1_i64 << shift);
        let retry_after = finished.saturating_add(backoff_ns.min(10_000_000_000));
        return finalize_attempt(
            database,
            job_id,
            planned,
            attempt,
            key,
            finished,
            "failed",
            &cause,
            &diagnostic.to_string(),
            "retry",
            planned,
            retry_after,
            0,
            key,
        );
    }
    let next = next_planned(job, planned)?;
    finalize_attempt(
        database,
        job_id,
        planned,
        attempt,
        key,
        finished,
        "failed",
        &cause,
        &diagnostic.to_string(),
        next.map_or("failed", |_| "idle"),
        next.unwrap_or(planned),
        0,
        0,
        "",
    )
}

fn finalize_exhausted(
    database: &Database,
    job_id: uuid::Uuid,
    job: &ServerScheduledJobDefinition,
    state: &DurableJobState,
    now: i64,
) -> Result<()> {
    let next = next_planned(job, state.planned_ns)?;
    database.execute(
        &format!(
            "UPDATE {JOB_STATE_RELATION} SET planned_ns = $1, attempt = 0, state = $2, \
             retry_after_ns = 0, lease_until_ns = 0, idempotency_key = '', updated_ns = $3 \
             WHERE job_id = $4"
        ),
        (
            next.unwrap_or(state.planned_ns),
            next.map_or("failed", |_| "idle"),
            now,
            job_id,
        ),
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn finalize_attempt(
    database: &Database,
    job_id: uuid::Uuid,
    planned: i64,
    attempt: i64,
    key: &str,
    finished: i64,
    outcome: &str,
    cause_kind: &str,
    detail: &str,
    state: &str,
    next_planned: i64,
    retry_after: i64,
    lease_until: i64,
    retained_key: &str,
) -> Result<()> {
    let mut transaction = database.begin()?;
    let history = transaction.execute(
        &format!(
            "UPDATE {JOB_HISTORY_RELATION} SET finished_ns = $1, outcome = $2, \
             cause_kind = $3, detail = $4 WHERE job_id = $5 AND planned_ns = $6 \
             AND attempt = $7 AND idempotency_key = $8 AND outcome = 'running'"
        ),
        (
            finished, outcome, cause_kind, detail, job_id, planned, attempt, key,
        ),
    )?;
    let state_rows = transaction.execute(
        &format!(
            "UPDATE {JOB_STATE_RELATION} SET planned_ns = $1, attempt = $2, state = $3, \
             retry_after_ns = $4, lease_until_ns = $5, idempotency_key = $6, updated_ns = $7 \
             WHERE job_id = $8 AND state = 'running' AND planned_ns = $9 \
             AND attempt = $10 AND idempotency_key = $11"
        ),
        (
            next_planned,
            if state == "retry" { attempt } else { 0 },
            state,
            retry_after,
            lease_until,
            retained_key,
            finished,
            job_id,
            planned,
            attempt,
            key,
        ),
    )?;
    if history != 1 || state_rows != 1 {
        transaction.rollback()?;
        return Err(Error::internal(format!(
            "job finalization lost its durable claim: history={history}, state={state_rows}"
        )));
    }
    transaction.commit()
}

fn prune_history(database: &Database, job_id: uuid::Uuid) -> Result<()> {
    let rows = database.query(
        &format!(
            "SELECT sequence FROM {JOB_HISTORY_RELATION} WHERE job_id = $1 \
             ORDER BY sequence DESC LIMIT {RETENTION_DELETE_BATCH} OFFSET {HISTORY_PER_JOB}"
        ),
        (job_id,),
    )?;
    let mut stale = Vec::new();
    for row in rows {
        stale.push(row?.get::<i64>(0)?);
    }
    for sequence in stale {
        database.execute(
            &format!("DELETE FROM {JOB_HISTORY_RELATION} WHERE sequence = $1"),
            (sequence,),
        )?;
    }
    Ok(())
}

fn unix_now_ns() -> Result<i64> {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| Error::internal("system clock is outside nanosecond timestamp range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_catalog_does_not_create_scheduler_ledger() {
        let database = Database::open_in_memory().unwrap();
        let cancellation = ServerCancellation::new();

        assert_eq!(
            run_cycle(&database, &cancellation, None).unwrap(),
            JobSchedulerCycle::default()
        );
        assert!(!database.table_exists(JOB_STATE_RELATION).unwrap());
        assert!(!database.table_exists(JOB_HISTORY_RELATION).unwrap());
    }

    #[test]
    fn interval_and_one_time_jobs_execute_with_durable_bounded_history() {
        let database = Database::open_in_memory().unwrap();
        database
            .execute("CREATE TABLE job_effects (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        database
            .execute(
                "CREATE PROCEDURE add_effect(IN input_id INTEGER NOT NULL) LANGUAGE RADIX \
                 SECURITY INVOKER AS BEGIN INSERT INTO job_effects VALUES (:input_id); END;",
                (),
            )
            .unwrap();
        let now = unix_now_ns().unwrap();
        let at =
            chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(now - 1_000_000).to_rfc3339();
        database
            .execute(
                &format!(
                    "CREATE JOB once_job SCHEDULE AT TIMESTAMP '{at}' RUN AS radix_system \
                     CALL add_effect(1) ENABLE;"
                ),
                (),
            )
            .unwrap();
        let cancellation = ServerCancellation::new();
        let cycle = run_cycle(&database, &cancellation, None).unwrap();
        assert_eq!(cycle.started, 1);
        assert_eq!(
            database.query_one::<i64, _>("SELECT COUNT(*) FROM job_effects", ()),
            Ok(1)
        );
        assert_eq!(
            database
                .query_one::<String, _>(&format!("SELECT state FROM {JOB_STATE_RELATION}"), (),),
            Ok("complete".to_string())
        );
        assert_eq!(
            database
                .query_one::<i64, _>(&format!("SELECT COUNT(*) FROM {JOB_HISTORY_RELATION}"), (),),
            Ok(1)
        );
        let second = run_cycle(&database, &cancellation, None).unwrap();
        assert_eq!(second.started, 0);
        assert_eq!(
            database.query_one::<i64, _>("SELECT COUNT(*) FROM job_effects", ()),
            Ok(1)
        );
    }

    #[test]
    fn retry_uses_one_idempotency_key_and_never_overlaps_a_live_lease() {
        let database = Database::open_in_memory().unwrap();
        database
            .execute(
                "CREATE PROCEDURE retry_job() LANGUAGE RADIX SECURITY INVOKER AS \
                 BEGIN RAISE conflict('retry'); END;",
                (),
            )
            .unwrap();
        database
            .execute(
                "CREATE JOB retry_schedule SCHEDULE AT TIMESTAMP '2020-01-01T00:00:00Z' \
                 RUN AS radix_system CALL retry_job() ENABLE;",
                (),
            )
            .unwrap();
        let cancellation = ServerCancellation::new();
        let first = run_cycle(&database, &cancellation, None).unwrap();
        assert_eq!(first.started, 1);
        let mut keys = BTreeSet::new();
        for row in database
            .query(
                &format!("SELECT idempotency_key FROM {JOB_HISTORY_RELATION}"),
                (),
            )
            .unwrap()
        {
            keys.insert(row.unwrap().get::<String>(0).unwrap());
        }
        assert_eq!(keys.len(), 1);
        let state = load_state(
            &database,
            uuid::Uuid::from_bytes(
                database.scheduled_jobs_snapshot().unwrap()[0]
                    .job_id
                    .into_bytes(),
            ),
        )
        .unwrap()
        .unwrap();
        if state.state == "retry" {
            database
                .execute(
                    &format!(
                        "UPDATE {JOB_STATE_RELATION} SET retry_after_ns = 0 WHERE job_id = $1"
                    ),
                    (database.scheduled_jobs_snapshot().unwrap()[0]
                        .job_id
                        .to_string(),),
                )
                .unwrap();
            let second = run_cycle(&database, &cancellation, None).unwrap();
            assert_eq!(second.started, 1);
        }
        let distinct: i64 = database
            .query_one(
                &format!("SELECT COUNT(DISTINCT idempotency_key) FROM {JOB_HISTORY_RELATION}"),
                (),
            )
            .unwrap();
        assert_eq!(distinct, 1);
    }
}
