use std::{
    sync::{atomic::Ordering, Arc, Condvar, Mutex},
    thread,
    time::{Duration, Instant},
};

use crate::common::prerelease::{tcp_command, tcp_connect_with_read_timeout, tcp_scalar_i64};

use super::{
    config::DATABASE,
    journal::{JournalShard, JournalSummary, RecordState},
    model::{
        expected_busy, is_compaction_backpressure, is_row_lock_timeout, merge_journals,
        operation_id, LayerContext, LayerSummary,
    },
};

const LAYER_TAG: u8 = 2;
const SHARED_ROW: i64 = 1_999_999_999;

pub fn run(context: &LayerContext) -> Result<LayerSummary, String> {
    let started = Instant::now();
    seed(context)?;
    let gate = Arc::new(FinalGate::new(context.profile.large_workers));
    let mut workers = Vec::with_capacity(context.profile.large_workers);
    for actor in 0..context.profile.large_workers {
        let context = context.clone();
        let gate = Arc::clone(&gate);
        workers.push(
            thread::Builder::new()
                .name(format!("epoch-large-{actor:02}"))
                .spawn(move || run_worker(&context, actor, gate))
                .map_err(|error| error.to_string())?,
        );
    }

    let mut results = Vec::with_capacity(workers.len());
    for worker in workers {
        results.push(
            worker
                .join()
                .map_err(|_| "large transaction worker panicked".to_string())??,
        );
    }
    let journal = merge_journals(results.iter().map(|result| result.journal.clone()))?;
    let mut summary = LayerSummary {
        name: "large-transactions".to_string(),
        semantic_operations: journal.terminal.count,
        transactions: results.len() as u64,
        committed_transactions: results
            .iter()
            .filter(|result| result.state == RecordState::Committed)
            .count() as u64,
        rolled_back_transactions: results
            .iter()
            .filter(|result| result.state == RecordState::RolledBack)
            .count() as u64,
        conflicted_transactions: results
            .iter()
            .filter(|result| result.state == RecordState::Conflicted)
            .count() as u64,
        row_lock_timeout_conflicts: results
            .iter()
            .filter(|result| result.row_lock_timeout_conflict)
            .count() as u64,
        compaction_backpressure_conflicts: results
            .iter()
            .filter(|result| result.compaction_backpressure_conflict)
            .count() as u64,
        journal,
        ..LayerSummary::default()
    };
    for result in &results {
        summary.transaction_latency.record(result.latency);
    }
    summary.active_clients_high_watermark = context.profile.large_workers as u64;
    summary.finalize_timing(started.elapsed());
    summary.details.insert(
        "actions_per_transaction".to_string(),
        context.profile.large_actions_per_worker.to_string(),
    );
    summary.details.insert(
        "shared_final_contenders".to_string(),
        results
            .iter()
            .filter(|result| result.contended)
            .count()
            .to_string(),
    );
    verify(context, &results)?;
    summary.validate()?;
    Ok(summary)
}

fn seed(context: &LayerContext) -> Result<(), String> {
    let mut connection =
        tcp_connect_with_read_timeout(context.address, DATABASE, context.profile.stage_timeout)?;
    let mut rows = (0..context.profile.large_workers)
        .map(|actor| {
            let id = actor_row(actor);
            format!("({id}, {actor}, 0, 'large-{actor}', 'large-key-{actor}')")
        })
        .collect::<Vec<_>>();
    rows.push(format!(
        "({SHARED_ROW}, -1, 0, 'shared', 'large-key-shared')"
    ));
    tcp_command(
        &mut connection,
        format!("INSERT INTO large_cells VALUES {}", rows.join(",")),
    )
}

#[derive(Clone)]
struct WorkerResult {
    actor: usize,
    state: RecordState,
    contended: bool,
    row_lock_timeout_conflict: bool,
    compaction_backpressure_conflict: bool,
    latency: Duration,
    journal: JournalSummary,
}

fn run_worker(
    context: &LayerContext,
    actor: usize,
    gate: Arc<FinalGate>,
) -> Result<WorkerResult, String> {
    let started = Instant::now();
    let mut connection =
        tcp_connect_with_read_timeout(context.address, DATABASE, context.profile.stage_timeout)?;
    connection.begin().map_err(|error| error.to_string())?;
    let budget = context.profile.large_actions_per_worker;
    let mut journal = JournalShard::create(&context.journal_root, "large", actor)?;
    let mut completed = Vec::with_capacity(budget as usize);
    let own_row = actor_row(actor);
    let contended = actor >= context.profile.large_workers / 2;
    let mut conflict = false;
    let mut row_lock_timeout_conflict = false;
    let mut compaction_backpressure_conflict = false;

    for ordinal in 0..budget {
        let op_id = operation_id(LAYER_TAG, actor, ordinal + 1);
        journal.plan_and_start(op_id, own_row)?;
        let result = if ordinal + 1 == budget {
            gate.arrive_and_wait(context.profile.stage_timeout)?;
            let target = if contended { SHARED_ROW } else { own_row };
            tcp_command(
                &mut connection,
                format!("UPDATE large_cells SET version = version + 1 WHERE id = {target}"),
            )
        } else {
            execute_action(&mut connection, actor, own_row, ordinal)
        };
        match result {
            Ok(()) => completed.push(op_id),
            Err(error) if contended && expected_busy(&error) => {
                completed.push(op_id);
                conflict = true;
                row_lock_timeout_conflict = is_row_lock_timeout(&error);
                compaction_backpressure_conflict = is_compaction_backpressure(&error);
                context.progress.fetch_add(1, Ordering::Release);
                break;
            }
            Err(error) => return Err(format!("large actor {actor} op {ordinal}: {error}")),
        }
        context.progress.fetch_add(1, Ordering::Release);
    }
    if completed.len() as u64 != budget {
        return Err(format!(
            "large actor {actor} completed {} of {budget} actions",
            completed.len()
        ));
    }

    let state = if conflict {
        if connection.in_transaction() {
            connection.rollback().map_err(|error| error.to_string())?;
        }
        RecordState::Conflicted
    } else if actor.is_multiple_of(8) {
        connection.rollback().map_err(|error| error.to_string())?;
        RecordState::RolledBack
    } else {
        match connection.commit() {
            Ok(()) => RecordState::Committed,
            Err(error) if contended && expected_busy(&error.to_string()) => {
                row_lock_timeout_conflict = is_row_lock_timeout(&error.to_string());
                compaction_backpressure_conflict = is_compaction_backpressure(&error.to_string());
                if connection.in_transaction() {
                    connection.rollback().map_err(|error| error.to_string())?;
                }
                RecordState::Conflicted
            }
            Err(error) => return Err(format!("large actor {actor} commit: {error}")),
        }
    };
    for op_id in completed {
        journal.terminal(op_id, own_row, state)?;
    }
    Ok(WorkerResult {
        actor,
        state,
        contended,
        row_lock_timeout_conflict,
        compaction_backpressure_conflict,
        latency: started.elapsed(),
        journal: journal.seal()?,
    })
}

fn execute_action(
    connection: &mut radixdb_client::Connection,
    actor: usize,
    own_row: i64,
    ordinal: u64,
) -> Result<(), String> {
    let scratch = 2_000_000_000_i64
        .saturating_add(actor as i64 * 100_000)
        .saturating_add(ordinal as i64);
    match ordinal % 4 {
        0 => tcp_command(
            connection,
            format!("UPDATE large_cells SET version = version + 1 WHERE id = {own_row}"),
        ),
        1 => {
            let count = tcp_scalar_i64(
                connection,
                &format!("SELECT COUNT(*) FROM large_cells WHERE id = {own_row}"),
            )?;
            if count == 1 {
                Ok(())
            } else {
                Err(format!("large actor {actor} lost own row"))
            }
        }
        2 => tcp_command(
            connection,
            format!(
                "INSERT INTO large_scratch VALUES ({scratch}, {actor}, {ordinal}) ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value"
            ),
        ),
        _ => tcp_command(
            connection,
            format!("DELETE FROM large_scratch WHERE id = {}", scratch - 1),
        ),
    }
}

fn verify(context: &LayerContext, results: &[WorkerResult]) -> Result<(), String> {
    let mut connection =
        tcp_connect_with_read_timeout(context.address, DATABASE, context.profile.stage_timeout)?;
    for result in results {
        let version = tcp_scalar_i64(
            &mut connection,
            &format!(
                "SELECT version FROM large_cells WHERE id = {}",
                actor_row(result.actor)
            ),
        )?;
        match result.state {
            RecordState::Committed if version <= 0 => {
                return Err(format!(
                    "committed large actor {} has version {version}",
                    result.actor
                ))
            }
            RecordState::RolledBack | RecordState::Conflicted if version != 0 => {
                return Err(format!(
                    "aborted large actor {} leaked version {version}",
                    result.actor
                ))
            }
            _ => {}
        }
    }
    let shared_version = tcp_scalar_i64(
        &mut connection,
        &format!("SELECT version FROM large_cells WHERE id = {SHARED_ROW}"),
    )?;
    if shared_version <= 0 {
        return Err("no contended large transaction committed".to_string());
    }
    Ok(())
}

fn actor_row(actor: usize) -> i64 {
    1_500_000_000 + actor as i64
}

struct FinalGate {
    expected: usize,
    state: Mutex<GateState>,
    condition: Condvar,
}

#[derive(Default)]
struct GateState {
    arrived: usize,
    released: bool,
    failed: bool,
}

impl FinalGate {
    fn new(expected: usize) -> Self {
        Self {
            expected,
            state: Mutex::new(GateState::default()),
            condition: Condvar::new(),
        }
    }

    fn arrive_and_wait(&self, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.arrived += 1;
        if state.arrived == self.expected {
            state.released = true;
            self.condition.notify_all();
            return Ok(());
        }
        while !state.released && !state.failed {
            let now = Instant::now();
            if now >= deadline {
                state.failed = true;
                self.condition.notify_all();
                return Err(format!(
                    "large final gate saw {}/{} actors",
                    state.arrived, self.expected
                ));
            }
            let (next, wait) = self
                .condition
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
            if wait.timed_out() {
                state.failed = true;
                self.condition.notify_all();
            }
        }
        if state.failed {
            Err("large final gate failed".to_string())
        } else {
            Ok(())
        }
    }
}
