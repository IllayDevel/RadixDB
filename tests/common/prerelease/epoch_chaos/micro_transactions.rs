use std::{sync::atomic::Ordering, thread, time::Instant};

use crate::common::prerelease::{tcp_command, tcp_connect_with_read_timeout, tcp_scalar_i64};

use super::{
    config::DATABASE,
    journal::{JournalShard, JournalSummary, RecordState},
    model::{
        merge_journals, operation_id, LatencyHistogram, LayerContext, LayerSummary,
        MaintenanceActor,
    },
};

const LAYER_TAG: u8 = 3;
const INSERT_BASE: i64 = 7_000_000_000;
const DELETE_BASE: i64 = 8_000_000_000;

pub fn run(context: &LayerContext) -> Result<LayerSummary, String> {
    let started = Instant::now();
    let workers = if context.profile.is_acceptance() {
        64
    } else {
        4
    };
    seed(context, workers)?;
    let maintenance =
        MaintenanceActor::start(context, (context.profile.micro_transactions / 32).max(100))?;
    let mut handles = Vec::with_capacity(workers);
    for actor in 0..workers {
        let context = context.clone();
        handles.push(
            thread::Builder::new()
                .name(format!("epoch-micro-{actor:02}"))
                .spawn(move || run_worker(&context, actor, workers))
                .map_err(|error| error.to_string())?,
        );
    }
    let mut results = Vec::with_capacity(workers);
    for handle in handles {
        results.push(
            handle
                .join()
                .map_err(|_| "micro transaction worker panicked".to_string())??,
        );
    }
    let maintenance = maintenance.stop()?;
    let journal = merge_journals(results.iter().map(|result| result.journal.clone()))?;
    let committed_transactions = results.iter().map(|result| result.committed).sum();
    let rolled_back_transactions = results.iter().map(|result| result.rolled_back).sum();
    let mut summary = LayerSummary {
        name: "micro-transactions".to_string(),
        semantic_operations: journal.terminal.count,
        transactions: context.profile.micro_transactions,
        committed_transactions,
        rolled_back_transactions,
        journal,
        ..LayerSummary::default()
    };
    summary.merge_maintenance(maintenance);
    for result in &results {
        summary.transaction_latency.merge(&result.latency);
    }
    summary.active_clients_high_watermark = workers as u64;
    summary.finalize_timing(started.elapsed());
    summary
        .details
        .insert("workers".to_string(), workers.to_string());
    summary.details.insert(
        "committed_insert_update_delete".to_string(),
        format!(
            "{}/{}/{}",
            results
                .iter()
                .map(|result| result.committed_inserts)
                .sum::<u64>(),
            results
                .iter()
                .map(|result| result.committed_updates)
                .sum::<u64>(),
            results
                .iter()
                .map(|result| result.committed_deletes)
                .sum::<u64>()
        ),
    );
    verify(context, &results)?;
    summary.validate()?;
    Ok(summary)
}

fn seed(context: &LayerContext, workers: usize) -> Result<(), String> {
    let mut connection =
        tcp_connect_with_read_timeout(context.address, DATABASE, context.profile.stage_timeout)?;
    let values = (0..workers)
        .map(|actor| format!("({}, 0, 0)", row_id(actor)))
        .collect::<Vec<_>>()
        .join(",");
    tcp_command(
        &mut connection,
        format!("INSERT INTO micro_cells VALUES {values}"),
    )?;
    let delete_rows = delete_operation_count(context.profile.micro_transactions);
    for chunk_start in (0..delete_rows).step_by(250) {
        let chunk_end = (chunk_start + 250).min(delete_rows);
        let values = (chunk_start..chunk_end)
            .map(|index| {
                let ordinal = 3 + index * 1_000;
                let id = DELETE_BASE + ordinal as i64;
                format!("({id}, 0, {id})")
            })
            .collect::<Vec<_>>()
            .join(",");
        tcp_command(
            &mut connection,
            format!("INSERT INTO micro_cells VALUES {values}"),
        )?;
    }
    Ok(())
}

#[derive(Clone)]
struct WorkerResult {
    actor: usize,
    committed: u64,
    rolled_back: u64,
    committed_inserts: u64,
    committed_updates: u64,
    committed_deletes: u64,
    latency: LatencyHistogram,
    journal: JournalSummary,
}

fn run_worker(
    context: &LayerContext,
    actor: usize,
    workers: usize,
) -> Result<WorkerResult, String> {
    let total = context.profile.micro_transactions;
    let base = total / workers as u64;
    let count = base + u64::from((actor as u64) < total % workers as u64);
    let start = actor as u64 * base + (actor as u64).min(total % workers as u64);
    let mut connection =
        tcp_connect_with_read_timeout(context.address, DATABASE, context.profile.stage_timeout)?;
    let mut journal = JournalShard::create(&context.journal_root, "micro", actor)?;
    let mut committed = 0;
    let mut rolled_back = 0;
    let mut committed_inserts = 0;
    let mut committed_updates = 0;
    let mut committed_deletes = 0;
    let mut latency = LatencyHistogram::default();
    let row = row_id(actor);
    for local in 0..count {
        let ordinal = start + local;
        let op_id = operation_id(LAYER_TAG, actor, ordinal + 1);
        let operation = micro_operation(ordinal, row);
        journal.plan_and_start(op_id, operation.target())?;
        let transaction_started = Instant::now();
        connection.begin().map_err(|error| error.to_string())?;
        operation.execute(&mut connection)?;
        let state = if ordinal.is_multiple_of(5) {
            connection.rollback().map_err(|error| error.to_string())?;
            rolled_back += 1;
            RecordState::RolledBack
        } else {
            connection.commit().map_err(|error| error.to_string())?;
            committed += 1;
            match operation {
                MicroOperation::Insert(_) => committed_inserts += 1,
                MicroOperation::Update(_) => committed_updates += 1,
                MicroOperation::Delete(_) => committed_deletes += 1,
            }
            RecordState::Committed
        };
        latency.record(transaction_started.elapsed());
        journal.terminal(op_id, operation.target(), state)?;
        context.progress.fetch_add(1, Ordering::Release);
    }
    Ok(WorkerResult {
        actor,
        committed,
        rolled_back,
        committed_inserts,
        committed_updates,
        committed_deletes,
        latency,
        journal: journal.seal()?,
    })
}

fn verify(context: &LayerContext, results: &[WorkerResult]) -> Result<(), String> {
    let mut connection =
        tcp_connect_with_read_timeout(context.address, DATABASE, context.profile.stage_timeout)?;
    for result in results {
        let row = row_id(result.actor);
        let version = tcp_scalar_i64(
            &mut connection,
            &format!("SELECT version FROM micro_cells WHERE id = {row}"),
        )?;
        let checksum = tcp_scalar_i64(
            &mut connection,
            &format!("SELECT checksum FROM micro_cells WHERE id = {row}"),
        )?;
        if version != result.committed_updates as i64
            || checksum != result.committed_updates as i64 * row
        {
            return Err(format!(
                "micro actor {} durable state differs: version={version} checksum={checksum} committed_updates={}",
                result.actor, result.committed_updates
            ));
        }
    }
    let committed_inserts = results
        .iter()
        .map(|result| result.committed_inserts)
        .sum::<u64>() as i64;
    let inserted = tcp_scalar_i64(
        &mut connection,
        &format!(
            "SELECT COUNT(*) FROM micro_cells WHERE id >= {INSERT_BASE} AND id < {DELETE_BASE}"
        ),
    )?;
    if inserted != committed_inserts {
        return Err(format!(
            "micro committed INSERT authority differs: expected {committed_inserts}, got {inserted}"
        ));
    }
    let committed_deletes = results
        .iter()
        .map(|result| result.committed_deletes)
        .sum::<u64>() as i64;
    let expected_delete_rows =
        delete_operation_count(context.profile.micro_transactions) as i64 - committed_deletes;
    let delete_rows = tcp_scalar_i64(
        &mut connection,
        &format!("SELECT COUNT(*) FROM micro_cells WHERE id >= {DELETE_BASE} AND id < 9000000000"),
    )?;
    if delete_rows != expected_delete_rows {
        return Err(format!(
            "micro committed DELETE authority differs: expected {expected_delete_rows}, got {delete_rows}"
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum MicroOperation {
    Insert(i64),
    Update(i64),
    Delete(i64),
}

impl MicroOperation {
    fn target(self) -> i64 {
        match self {
            Self::Insert(id) | Self::Update(id) | Self::Delete(id) => id,
        }
    }

    fn execute(self, connection: &mut radixdb_client::Connection) -> Result<(), String> {
        match self {
            Self::Insert(id) => tcp_command(
                connection,
                format!("INSERT INTO micro_cells VALUES ({id}, 1, {id})"),
            ),
            Self::Update(id) => tcp_command(
                connection,
                format!(
                    "UPDATE micro_cells SET version = version + 1, checksum = checksum + id WHERE id = {id}"
                ),
            ),
            Self::Delete(id) => tcp_command(
                connection,
                format!("DELETE FROM micro_cells WHERE id = {id}"),
            ),
        }
    }
}

fn micro_operation(ordinal: u64, update_row: i64) -> MicroOperation {
    match ordinal % 1_000 {
        2 => MicroOperation::Insert(INSERT_BASE + ordinal as i64),
        3 => MicroOperation::Delete(DELETE_BASE + ordinal as i64),
        _ => MicroOperation::Update(update_row),
    }
}

fn delete_operation_count(total: u64) -> u64 {
    if total <= 3 {
        0
    } else {
        (total - 4) / 1_000 + 1
    }
}

fn row_id(actor: usize) -> i64 {
    3_000_000_000 + actor as i64
}
