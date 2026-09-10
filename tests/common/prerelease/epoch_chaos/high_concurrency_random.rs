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
        operation_id, LatencyHistogram, LayerContext, LayerSummary, MaintenanceActor,
    },
};

const LAYER_TAG: u8 = 4;
const SHARED_ROW: i64 = 4_999_999_999;
const RUNG_SEED_SALTS: [u64; 5] = [
    0x9e37_79b9_7f4a_7c15,
    0xbf58_476d_1ce4_e5b9,
    0x94d0_49bb_1331_11eb,
    0xd6e8_feb8_6659_fd93,
    0xa076_1d64_78bd_642f,
];

pub fn run(context: &LayerContext) -> Result<LayerSummary, String> {
    let rungs = context
        .profile
        .random_rungs
        .iter()
        .copied()
        .enumerate()
        .collect::<Vec<_>>();
    run_selected(context, "high-concurrency-random", &rungs)
}

pub fn run_capacity(
    context: &LayerContext,
    rung_index: usize,
    clients: usize,
) -> Result<LayerSummary, String> {
    run_selected(
        context,
        &format!("high-concurrency-random-capacity-{clients}"),
        &[(rung_index, clients)],
    )
}

fn run_selected(
    context: &LayerContext,
    layer_name: &str,
    rungs: &[(usize, usize)],
) -> Result<LayerSummary, String> {
    if rungs.is_empty() {
        return Err("random layer has no selected client rung".to_string());
    }
    let started = Instant::now();
    let max_clients = rungs.iter().map(|(_, clients)| *clients).max().unwrap_or(0);
    seed(context, max_clients)?;
    let maintenance = MaintenanceActor::start(
        context,
        (context.profile.random_operations_per_rung / 16).max(100),
    )?;
    let mut all_results = Vec::new();
    for (rung_index, clients) in rungs.iter().copied() {
        all_results.extend(run_rung(context, rung_index, clients)?);
    }
    let maintenance = maintenance.stop()?;
    let journal = merge_journals(all_results.iter().map(|result| result.journal.clone()))?;
    let mut summary = LayerSummary {
        name: layer_name.to_string(),
        semantic_operations: journal.terminal.count,
        transactions: all_results.iter().map(|result| result.transactions).sum(),
        committed_transactions: all_results.iter().map(|result| result.committed).sum(),
        rolled_back_transactions: all_results.iter().map(|result| result.rolled_back).sum(),
        disconnected_transactions: all_results.iter().map(|result| result.disconnected).sum(),
        conflicted_transactions: all_results.iter().map(|result| result.conflicted).sum(),
        row_lock_timeout_conflicts: all_results
            .iter()
            .map(|result| result.row_lock_timeout_conflicts)
            .sum(),
        compaction_backpressure_conflicts: all_results
            .iter()
            .map(|result| result.compaction_backpressure_conflicts)
            .sum(),
        reconnects: all_results.iter().map(|result| result.reconnects).sum(),
        journal,
        ..LayerSummary::default()
    };
    summary.merge_maintenance(maintenance);
    for result in &all_results {
        summary.transaction_latency.merge(&result.latency);
    }
    summary.active_clients_high_watermark = max_clients as u64;
    summary.finalize_timing(started.elapsed());
    summary.details.insert(
        "client_rungs".to_string(),
        rungs
            .iter()
            .map(|(_, clients)| clients.to_string())
            .collect::<Vec<_>>()
            .join(","),
    );
    summary.details.insert(
        "operations_per_rung".to_string(),
        context.profile.random_operations_per_rung.to_string(),
    );
    summary.details.insert(
        "rung_seeds".to_string(),
        rungs
            .iter()
            .map(|(index, clients)| format!("{clients}:{}", rung_seed(context.seed, *index)))
            .collect::<Vec<_>>()
            .join(","),
    );
    summary.details.insert(
        "multi_operation_transactions".to_string(),
        all_results
            .iter()
            .map(|result| result.multi_operation_transactions)
            .sum::<u64>()
            .to_string(),
    );
    summary.details.insert(
        "max_transaction_operations".to_string(),
        all_results
            .iter()
            .map(|result| result.max_transaction_operations)
            .max()
            .unwrap_or(0)
            .to_string(),
    );
    verify(context, max_clients)?;
    summary.validate()?;
    Ok(summary)
}

fn seed(context: &LayerContext, max_clients: usize) -> Result<(), String> {
    let mut connection =
        tcp_connect_with_read_timeout(context.address, DATABASE, context.profile.stage_timeout)?;
    let mut values = (0..max_clients)
        .map(|actor| {
            let id = actor_row(actor);
            format!("({id}, 1, 0, 'random-{actor}')")
        })
        .collect::<Vec<_>>();
    values.push(format!("({SHARED_ROW}, 1, 0, 'shared')"));
    for chunk in values.chunks(250) {
        tcp_command(
            &mut connection,
            format!(
                "INSERT INTO random_cells VALUES {} ON CONFLICT (id) DO NOTHING",
                chunk.join(",")
            ),
        )?;
    }
    Ok(())
}

#[derive(Clone)]
struct WorkerResult {
    transactions: u64,
    committed: u64,
    rolled_back: u64,
    disconnected: u64,
    conflicted: u64,
    row_lock_timeout_conflicts: u64,
    compaction_backpressure_conflicts: u64,
    reconnects: u64,
    multi_operation_transactions: u64,
    max_transaction_operations: u64,
    latency: LatencyHistogram,
    journal: JournalSummary,
}

fn run_rung(
    context: &LayerContext,
    rung_index: usize,
    clients: usize,
) -> Result<Vec<WorkerResult>, String> {
    let gate = Arc::new(ActivationGate::new(clients));
    let mut workers = Vec::with_capacity(clients);
    for actor in 0..clients {
        let context = context.clone();
        let gate = Arc::clone(&gate);
        workers.push(
            thread::Builder::new()
                .name(format!("epoch-random-{clients}-{actor}"))
                .stack_size(512 * 1024)
                .spawn(move || run_worker(&context, rung_index, clients, actor, gate))
                .map_err(|error| error.to_string())?,
        );
    }
    let mut results = Vec::with_capacity(clients);
    for worker in workers {
        results.push(
            worker
                .join()
                .map_err(|_| format!("random {clients}-client worker panicked"))??,
        );
    }
    Ok(results)
}

fn run_worker(
    context: &LayerContext,
    rung_index: usize,
    clients: usize,
    actor: usize,
    gate: Arc<ActivationGate>,
) -> Result<WorkerResult, String> {
    let total = context.profile.random_operations_per_rung;
    let base = total / clients as u64;
    let count = base + u64::from((actor as u64) < total % clients as u64);
    let start = actor as u64 * base + (actor as u64).min(total % clients as u64);
    let mut connection =
        tcp_connect_with_read_timeout(context.address, DATABASE, context.profile.stage_timeout)?;
    gate.arrive_and_wait(Duration::from_secs(120))?;
    let shard = rung_index * 1024 + actor;
    let mut journal = JournalShard::create(&context.journal_root, "random", shard)?;
    let mut random = Random::new(rung_seed(context.seed, rung_index) ^ actor_seed(actor));
    let mut result = WorkerResult {
        transactions: 0,
        committed: 0,
        rolled_back: 0,
        disconnected: 0,
        conflicted: 0,
        row_lock_timeout_conflicts: 0,
        compaction_backpressure_conflicts: 0,
        reconnects: 0,
        multi_operation_transactions: 0,
        max_transaction_operations: 0,
        latency: LatencyHistogram::default(),
        journal: JournalSummary::default(),
    };
    let mut local = 0;
    while local < count {
        let requested = 1 + random.next() % 8;
        let transaction_operations = requested.min(count - local);
        result.transactions += 1;
        result.max_transaction_operations = result
            .max_transaction_operations
            .max(transaction_operations);
        result.multi_operation_transactions += u64::from(transaction_operations > 1);
        let transaction_started = Instant::now();
        connection.begin().map_err(|error| error.to_string())?;
        let mut operations = Vec::with_capacity(transaction_operations as usize);
        let mut conflict = false;
        for _ in 0..transaction_operations {
            let global = start + local;
            let op_id = operation_id(LAYER_TAG, shard, rung_index as u64 * total + global + 1);
            let choice = random.next() % 8;
            journal.plan_and_start(op_id, actor as i64)?;
            operations.push((op_id, global, choice));
            match execute_operation(
                &mut connection,
                context,
                rung_index,
                actor,
                global,
                choice,
            ) {
                Ok(()) => {}
                Err(error) if expected_busy(&error) => {
                    result.row_lock_timeout_conflicts += u64::from(is_row_lock_timeout(&error));
                    result.compaction_backpressure_conflicts +=
                        u64::from(is_compaction_backpressure(&error));
                    conflict = true;
                }
                Err(error) => {
                    return Err(format!(
                        "random rung={clients} actor={actor} operation={global} choice={choice}: {error}"
                    ))
                }
            }
            local += 1;
            if conflict {
                break;
            }
        }
        let state = if conflict {
            if connection.in_transaction() {
                connection.rollback().map_err(|error| error.to_string())?;
            }
            result.conflicted += 1;
            RecordState::Conflicted
        } else if random.next().is_multiple_of(50) {
            drop(connection);
            result.disconnected += 1;
            result.reconnects += 1;
            connection = tcp_connect_with_read_timeout(
                context.address,
                DATABASE,
                context.profile.stage_timeout,
            )?;
            RecordState::Disconnected
        } else if random.next().is_multiple_of(10) {
            connection.rollback().map_err(|error| error.to_string())?;
            result.rolled_back += 1;
            RecordState::RolledBack
        } else {
            match connection.commit() {
                Ok(()) => {
                    result.committed += 1;
                    RecordState::Committed
                }
                Err(error) if expected_busy(&error.to_string()) => {
                    result.row_lock_timeout_conflicts +=
                        u64::from(is_row_lock_timeout(&error.to_string()));
                    result.compaction_backpressure_conflicts +=
                        u64::from(is_compaction_backpressure(&error.to_string()));
                    if connection.in_transaction() {
                        connection.rollback().map_err(|error| error.to_string())?;
                    }
                    result.conflicted += 1;
                    RecordState::Conflicted
                }
                Err(error) => return Err(format!("random commit: {error}")),
            }
        };
        result.latency.record(transaction_started.elapsed());
        for (op_id, _, _) in operations.iter().copied() {
            journal.terminal(op_id, actor as i64, state)?;
        }
        context
            .progress
            .fetch_add(operations.len() as u64, Ordering::Release);
    }
    result.journal = journal.seal()?;
    Ok(result)
}

fn rung_seed(base: u64, rung_index: usize) -> u64 {
    base ^ RUNG_SEED_SALTS[rung_index % RUNG_SEED_SALTS.len()]
}

fn actor_seed(actor: usize) -> u64 {
    (actor as u64)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .rotate_left(31)
}

fn execute_operation(
    connection: &mut radixdb_client::Connection,
    context: &LayerContext,
    rung_index: usize,
    actor: usize,
    ordinal: u64,
    choice: u64,
) -> Result<(), String> {
    let own = actor_row(actor);
    let scratch = 6_000_000_000_i64
        .saturating_add(rung_index as i64 * 10_000_000)
        .saturating_add(ordinal as i64);
    match choice {
        0 => expect_positive(
            connection,
            &format!("SELECT COUNT(*) FROM random_cells WHERE id = {own}"),
        ),
        1 => tcp_command(
            connection,
            format!("UPDATE random_cells SET version = version + 1 WHERE id = {own}"),
        ),
        2 => tcp_command(
            connection,
            format!("INSERT INTO random_cells VALUES ({scratch}, 1, 0, 'scratch-{scratch}') ON CONFLICT (id) DO UPDATE SET version = random_cells.version + 1"),
        ),
        3 => tcp_command(
            connection,
            format!("DELETE FROM random_cells WHERE id = {scratch}"),
        ),
        4 => expect_positive(
            connection,
            &format!("SELECT COUNT(*) FROM random_cells r JOIN users u ON u.id = r.owner_id WHERE r.id = {own}"),
        ),
        5 => tcp_command(
            connection,
            format!("UPDATE random_cells SET version = version + 1 WHERE id = {SHARED_ROW}"),
        ),
        6 => {
            let cold = ordinal % context.profile.cold_rows + 1;
            tcp_command(
                connection,
                format!("UPDATE chaos_cells SET version = version + 1 WHERE id = {cold}"),
            )
        }
        _ => expect_positive(
            connection,
            &format!(
                "SELECT COUNT(*) FROM chaos_cells WHERE id BETWEEN {} AND {}",
                ordinal % context.profile.cold_rows + 1,
                (ordinal % context.profile.cold_rows + 1).saturating_add(31)
            ),
        ),
    }
}

fn expect_positive(connection: &mut radixdb_client::Connection, sql: &str) -> Result<(), String> {
    let count = tcp_scalar_i64(connection, sql)?;
    if count > 0 {
        Ok(())
    } else {
        Err(format!("random read returned no rows: {sql}"))
    }
}

fn verify(context: &LayerContext, max_clients: usize) -> Result<(), String> {
    let mut connection =
        tcp_connect_with_read_timeout(context.address, DATABASE, context.profile.stage_timeout)?;
    let expected = max_clients as i64 + 1;
    let actual = tcp_scalar_i64(
        &mut connection,
        "SELECT COUNT(*) FROM random_cells WHERE id BETWEEN 4000000000 AND 4999999999",
    )?;
    if actual != expected {
        return Err(format!(
            "random base rows differ: expected {expected}, got {actual}"
        ));
    }
    Ok(())
}

fn actor_row(actor: usize) -> i64 {
    4_000_000_000 + actor as i64
}

struct ActivationGate {
    expected: usize,
    state: Mutex<ActivationState>,
    condition: Condvar,
}

#[derive(Default)]
struct ActivationState {
    arrived: usize,
    released: bool,
    failed: bool,
}

impl ActivationGate {
    fn new(expected: usize) -> Self {
        Self {
            expected,
            state: Mutex::new(ActivationState::default()),
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
        }
        while !state.released && !state.failed {
            let now = Instant::now();
            if now >= deadline {
                state.failed = true;
                self.condition.notify_all();
                break;
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
            Err(format!(
                "only {}/{} random clients became active",
                state.arrived, self.expected
            ))
        } else {
            Ok(())
        }
    }
}

struct Random(u64);

impl Random {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }
}
