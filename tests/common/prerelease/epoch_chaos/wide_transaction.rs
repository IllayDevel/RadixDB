use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
    time::Instant,
};

use crate::common::prerelease::{tcp_command, tcp_connect_with_read_timeout, tcp_scalar_i64};

use super::{
    config::DATABASE,
    journal::{JournalShard, RecordState},
    model::{operation_id, LayerContext, LayerSummary},
};

const LAYER_TAG: u8 = 1;
const COMMIT_BASE: i64 = 1_000_000_000;
const ROLLBACK_BASE: i64 = 1_100_000_000;

pub fn run(context: &LayerContext) -> Result<LayerSummary, String> {
    let started = Instant::now();
    let mut summary = LayerSummary {
        name: "wide-transaction".to_string(),
        ..LayerSummary::default()
    };

    let committed = run_branch(context, 0, COMMIT_BASE, true)?;
    summary.journal.merge(&committed.journal);
    summary.semantic_operations += committed.operations;
    summary.rejected_statements += committed.rejected;
    summary.reader_checks += committed.reader_checks;
    summary.transactions += 1;
    summary.committed_transactions += 1;
    summary.transaction_latency.record(committed.latency);

    let rolled_back = run_branch(context, 1, ROLLBACK_BASE, false)?;
    summary.journal.merge(&rolled_back.journal);
    summary.semantic_operations += rolled_back.operations;
    summary.rejected_statements += rolled_back.rejected;
    summary.reader_checks += rolled_back.reader_checks;
    summary.transactions += 1;
    summary.rolled_back_transactions += 1;
    summary.transaction_latency.record(rolled_back.latency);

    let mut disconnected =
        tcp_connect_with_read_timeout(context.address, DATABASE, context.profile.stage_timeout)?;
    let disconnect_started = Instant::now();
    disconnected.begin().map_err(|error| error.to_string())?;
    let disconnect_id = operation_id(LAYER_TAG, 2, 1);
    let mut disconnect_journal = JournalShard::create(&context.journal_root, "wide", 2)?;
    disconnect_journal.plan_and_start(disconnect_id, 1)?;
    tcp_command(
        &mut disconnected,
        "INSERT INTO wide_cells VALUES (1200000000, 1, 0, 'disconnect')",
    )?;
    drop(disconnected);
    disconnect_journal.terminal(disconnect_id, 1, RecordState::Disconnected)?;
    let disconnect_journal = disconnect_journal.seal()?;
    summary.journal.merge(&disconnect_journal);
    summary.semantic_operations += 1;
    summary.transactions += 1;
    summary.disconnected_transactions += 1;
    summary
        .transaction_latency
        .record(disconnect_started.elapsed());
    context.progress.fetch_add(1, Ordering::Release);

    let mut verifier =
        tcp_connect_with_read_timeout(context.address, DATABASE, context.profile.stage_timeout)?;
    if tcp_scalar_i64(
        &mut verifier,
        "SELECT COUNT(*) FROM wide_cells WHERE id >= 1100000000",
    )? != 0
    {
        return Err("wide rollback/disconnect state became visible".to_string());
    }

    summary.active_clients_high_watermark = 2;
    summary.finalize_timing(started.elapsed());
    summary.details.insert(
        "commit_visible_rows".to_string(),
        committed.visible_rows.to_string(),
    );
    summary.details.insert(
        "actions_per_primary_branch".to_string(),
        context.profile.wide_actions.to_string(),
    );
    summary.validate()?;
    Ok(summary)
}

struct BranchSummary {
    operations: u64,
    rejected: u64,
    reader_checks: u64,
    visible_rows: i64,
    latency: std::time::Duration,
    journal: super::journal::JournalSummary,
}

fn run_branch(
    context: &LayerContext,
    shard: usize,
    base: i64,
    commit: bool,
) -> Result<BranchSummary, String> {
    let branch_started = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let (ready_tx, ready_rx) = mpsc::channel();
    let reader_stop = Arc::clone(&stop);
    let address = context.address;
    let timeout = context.profile.stage_timeout;
    let reader = thread::Builder::new()
        .name(format!("epoch-wide-reader-{shard}"))
        .spawn(move || -> Result<u64, String> {
            let mut connection = tcp_connect_with_read_timeout(address, DATABASE, timeout)?;
            ready_tx.send(()).map_err(|error| error.to_string())?;
            let mut checks = 0;
            while !reader_stop.load(Ordering::Acquire) {
                let count = tcp_scalar_i64(
                    &mut connection,
                    &format!(
                        "SELECT COUNT(*) FROM wide_cells WHERE id >= {base} AND id < {}",
                        base + 50_000_000
                    ),
                )?;
                if count != 0 {
                    return Err(format!(
                        "parallel reader observed {count} uncommitted wide rows"
                    ));
                }
                checks += 1;
                thread::yield_now();
            }
            Ok(checks)
        })
        .map_err(|error| error.to_string())?;
    ready_rx.recv().map_err(|error| error.to_string())?;

    let mut connection =
        tcp_connect_with_read_timeout(context.address, DATABASE, context.profile.stage_timeout)?;
    connection.begin().map_err(|error| error.to_string())?;
    let budget = context.profile.wide_actions;
    let invalid_actions = 10.min(budget / 20).max(1);
    let messenger_bundles = 100
        .min((budget.saturating_sub(invalid_actions)) / 1000)
        .max(1);
    let messenger_actions = messenger_bundles * 5;
    let generic_actions = budget
        .checked_sub(invalid_actions + messenger_actions)
        .ok_or_else(|| "wide smoke budget is too small".to_string())?;
    let mut journal = JournalShard::create(&context.journal_root, "wide", shard)?;
    let mut successful = Vec::with_capacity(budget as usize);
    let mut rejected = Vec::with_capacity(invalid_actions as usize);

    for ordinal in 0..generic_actions {
        let op_id = operation_id(LAYER_TAG, shard, ordinal + 1);
        journal.plan_and_start(op_id, base)?;
        execute_generic(&mut connection, base, ordinal)?;
        successful.push(op_id);
        context.progress.fetch_add(1, Ordering::Release);
    }
    for bundle in 0..messenger_bundles {
        for part in 0..5_u64 {
            let ordinal = generic_actions + bundle * 5 + part;
            let op_id = operation_id(LAYER_TAG, shard, ordinal + 1);
            journal.plan_and_start(op_id, base)?;
            execute_messenger(&mut connection, base, bundle, part)?;
            successful.push(op_id);
            context.progress.fetch_add(1, Ordering::Release);
        }
    }
    for invalid in 0..invalid_actions {
        let ordinal = generic_actions + messenger_actions + invalid;
        let op_id = operation_id(LAYER_TAG, shard, ordinal + 1);
        journal.plan_and_start(op_id, base)?;
        let result = tcp_command(
            &mut connection,
            format!(
                "INSERT INTO messages VALUES ({}, 999999999, {}, 1, 'invalid', false)",
                base + 40_000_000 + invalid as i64,
                base + 40_000_000 + invalid as i64
            ),
        );
        match result {
            Err(error) if error.to_ascii_lowercase().contains("foreign key") => {}
            Err(error) => return Err(format!("unexpected wide constraint error: {error}")),
            Ok(()) => return Err("wide invalid statement unexpectedly succeeded".to_string()),
        }
        rejected.push(op_id);
        context.progress.fetch_add(1, Ordering::Release);
    }
    if successful.len() as u64 + rejected.len() as u64 != budget {
        return Err("wide action accounting differs from frozen budget".to_string());
    }

    stop.store(true, Ordering::Release);
    let reader_checks = reader
        .join()
        .map_err(|_| "wide reader panicked".to_string())??;
    if reader_checks == 0 {
        return Err("wide reader performed no isolation checks".to_string());
    }

    let successful_state = if commit {
        connection.commit().map_err(|error| error.to_string())?;
        RecordState::Committed
    } else {
        connection.rollback().map_err(|error| error.to_string())?;
        RecordState::RolledBack
    };
    for op_id in successful {
        journal.terminal(op_id, base, successful_state)?;
    }
    for op_id in rejected {
        journal.terminal(op_id, base, RecordState::Rejected)?;
    }
    let journal = journal.seal()?;

    let mut verifier =
        tcp_connect_with_read_timeout(context.address, DATABASE, context.profile.stage_timeout)?;
    let visible_rows = tcp_scalar_i64(
        &mut verifier,
        &format!(
            "SELECT COUNT(*) FROM wide_cells WHERE id >= {base} AND id < {}",
            base + 50_000_000
        ),
    )?;
    if commit {
        if visible_rows <= 0 {
            return Err("wide commit published no durable rows".to_string());
        }
    } else if visible_rows != 0 {
        return Err(format!("wide rollback published {visible_rows} rows"));
    }
    Ok(BranchSummary {
        operations: budget,
        rejected: invalid_actions,
        reader_checks,
        visible_rows,
        latency: branch_started.elapsed(),
        journal,
    })
}

fn execute_generic(
    connection: &mut radixdb_client::Connection,
    base: i64,
    ordinal: u64,
) -> Result<(), String> {
    let group = ordinal / 5;
    let id = base + group as i64;
    match ordinal % 5 {
        0 => tcp_command(
            connection,
            format!("INSERT INTO wide_cells VALUES ({id}, 1, 0, 'wide-{id}')"),
        ),
        1 => expect_one(connection, id),
        2 => tcp_command(
            connection,
            format!("UPDATE wide_cells SET version = version + 1 WHERE id = {id}"),
        ),
        3 => {
            let count = tcp_scalar_i64(
                connection,
                &format!(
                    "SELECT COUNT(*) FROM wide_cells w JOIN users u ON u.id = w.owner_id WHERE w.id = {id}"
                ),
            )?;
            if count != 1 {
                return Err(format!("wide JOIN read-your-writes returned {count}"));
            }
            Ok(())
        }
        _ if group.is_multiple_of(2) => tcp_command(
            connection,
            format!("DELETE FROM wide_cells WHERE id = {id}"),
        ),
        _ => expect_one(connection, id),
    }
}

fn expect_one(connection: &mut radixdb_client::Connection, id: i64) -> Result<(), String> {
    let count = tcp_scalar_i64(
        connection,
        &format!("SELECT COUNT(*) FROM wide_cells WHERE id = {id}"),
    )?;
    if count == 1 {
        Ok(())
    } else {
        Err(format!("wide read-your-writes returned {count} for {id}"))
    }
}

fn execute_messenger(
    connection: &mut radixdb_client::Connection,
    base: i64,
    bundle: u64,
    part: u64,
) -> Result<(), String> {
    let id = base + 20_000_000 + bundle as i64;
    let outbox = base + 25_000_000 + bundle as i64;
    let sync = base + 30_000_000 + bundle as i64;
    let command = base + 35_000_000 + bundle as i64;
    match part {
        0 => tcp_command(
            connection,
            format!("INSERT INTO messages VALUES ({id}, 10, {id}, 1, 'wide-message-{id}', false)"),
        ),
        1 => tcp_command(
            connection,
            format!("INSERT INTO outbox_jobs VALUES ({outbox}, {id}, 'pending', 0, NULL, true)"),
        ),
        2 => tcp_command(
            connection,
            format!("INSERT INTO sync_events VALUES ({sync}, 2, {id}, {id}, {outbox}, 'message.created')"),
        ),
        3 => tcp_command(
            connection,
            format!("INSERT INTO command_results VALUES ({command}, 1, 'wide-command-{id}', {id})"),
        ),
        _ => {
            let count = tcp_scalar_i64(
                connection,
                &format!("SELECT COUNT(*) FROM messages m JOIN outbox_jobs o ON o.message_id = m.id JOIN sync_events s ON s.outbox_job_id = o.id WHERE m.id = {id}"),
            )?;
            if count == 1 {
                Ok(())
            } else {
                Err(format!("wide messenger JOIN returned {count}"))
            }
        }
    }
}
