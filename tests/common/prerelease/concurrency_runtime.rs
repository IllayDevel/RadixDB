#![cfg(feature = "stress-tests")]

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Barrier,
    },
    thread,
    time::Duration,
};

use radixdb_client::Connection;
use radixdb_orm::{table_as, Expr, OrmBuilder, QueryBuilder};

use super::{
    messenger_schema_plan, messenger_small_seed_plan, messenger_view_plan, tcp_command,
    tcp_connect, tcp_connect_with_read_timeout, tcp_rows, tcp_scalar_i64, ConcurrencyPlan,
    ConcurrencyTransaction, KeyDistribution, MessengerSeedPlan, TransactionCohort,
    TransactionTerminal,
};

pub const CONCURRENCY_DATABASE: &str = "prerelease_concurrency";
pub const LARGE_CONCURRENCY_QUERY_TIMEOUT: Duration = Duration::from_secs(5 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConcurrencyKeyspace {
    pub message_base: i64,
    pub disjoint_user_base: i64,
    pub disjoint_conversation_base: i64,
    pub disjoint_membership_base: i64,
    pub expected_tenants: i64,
}

impl ConcurrencyKeyspace {
    pub const fn small() -> Self {
        Self {
            message_base: 10_000_000,
            disjoint_user_base: 10_000,
            disjoint_conversation_base: 20_000,
            disjoint_membership_base: 30_000,
            expected_tenants: 2,
        }
    }

    pub fn after_large_fixture(plan: &MessengerSeedPlan) -> Result<Self, String> {
        let users = i64::try_from(plan.users).map_err(|_| "large user count exceeds i64")?;
        let conversations = i64::try_from(plan.conversations)
            .map_err(|_| "large conversation count exceeds i64")?;
        let memberships = i64::try_from(plan.conversation_members)
            .map_err(|_| "large membership count exceeds i64")?;
        let tenants = i64::try_from(plan.tenants).map_err(|_| "large tenant count exceeds i64")?;
        Ok(Self {
            // The seeded fixture owns message ids 1..=46M.  Keep the entire
            // transactional family, including the +800M child ids, in a
            // disjoint and easy-to-audit range.
            message_base: 1_000_000_000,
            disjoint_user_base: users.saturating_add(10_000),
            disjoint_conversation_base: conversations.saturating_add(20_000),
            disjoint_membership_base: memberships.saturating_add(30_000),
            expected_tenants: tenants,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConcurrencyRuntimeSummary {
    pub committed_ids: BTreeSet<i64>,
    pub rejected_ids: BTreeSet<i64>,
    pub ambiguous_ids: BTreeSet<i64>,
    pub rolled_back: usize,
    pub disconnected: usize,
    pub invalid_rejected: usize,
    pub conflicts: usize,
    pub retries_resolved: usize,
    pub disjoint_commits: usize,
    pub reader_checks: usize,
    pub maintenance_busy: usize,
    pub overload_accepted: usize,
    pub overload_refused: usize,
}

impl ConcurrencyRuntimeSummary {
    fn merge(&mut self, mut other: Self) {
        self.committed_ids.append(&mut other.committed_ids);
        self.rejected_ids.append(&mut other.rejected_ids);
        self.ambiguous_ids.append(&mut other.ambiguous_ids);
        self.rolled_back += other.rolled_back;
        self.disconnected += other.disconnected;
        self.invalid_rejected += other.invalid_rejected;
        self.conflicts += other.conflicts;
        self.retries_resolved += other.retries_resolved;
        self.disjoint_commits += other.disjoint_commits;
        self.reader_checks += other.reader_checks;
        self.maintenance_busy += other.maintenance_busy;
        self.overload_accepted += other.overload_accepted;
        self.overload_refused += other.overload_refused;
    }
}

pub fn initialize_concurrency_fixture(address: SocketAddr, clients: usize) -> Result<(), String> {
    let mut connection = tcp_connect(address, CONCURRENCY_DATABASE)?;
    for plan in [
        messenger_schema_plan(),
        messenger_small_seed_plan(),
        messenger_view_plan(),
    ] {
        for statement in plan.statements {
            tcp_command(&mut connection, statement.sql)
                .map_err(|error| format!("fixture statement {}: {error}", statement.id))?;
        }
    }
    initialize_concurrency_actor_rows_on_connection(
        &mut connection,
        clients,
        ConcurrencyKeyspace::small(),
    )?;
    tcp_command(&mut connection, "PRAGMA CHECKPOINT")?;
    Ok(())
}

pub fn initialize_concurrency_actor_rows(
    address: SocketAddr,
    clients: usize,
    keyspace: ConcurrencyKeyspace,
) -> Result<(), String> {
    let mut connection = tcp_connect(address, CONCURRENCY_DATABASE)?;
    initialize_concurrency_actor_rows_on_connection(&mut connection, clients, keyspace)
}

fn initialize_concurrency_actor_rows_on_connection(
    connection: &mut Connection,
    clients: usize,
    keyspace: ConcurrencyKeyspace,
) -> Result<(), String> {
    connection.begin().map_err(|error| error.to_string())?;
    for actor_id in 0..clients {
        let actor_id = i64::try_from(actor_id).map_err(|_| "actor id exceeds i64")?;
        let user_id = keyspace.disjoint_user_base.saturating_add(actor_id);
        let conversation_id = keyspace.disjoint_conversation_base.saturating_add(actor_id);
        tcp_command(
            connection,
            format!(
                "INSERT INTO users VALUES ({user_id}, 1, 'ladder-{user_id}', 'Ladder {actor_id}', 1, false)"
            ),
        )?;
        tcp_command(
            connection,
            format!(
                "INSERT INTO conversations VALUES ({conversation_id}, 1, {user_id}, 'direct', 'Ladder {actor_id}', true)"
            ),
        )?;
        tcp_command(
            connection,
            format!(
                "INSERT INTO conversation_members VALUES ({}, {conversation_id}, {user_id}, 'owner', 0, false)",
                keyspace.disjoint_membership_base.saturating_add(actor_id)
            ),
        )?;
    }
    connection.commit().map_err(|error| error.to_string())?;
    Ok(())
}

pub fn run_concurrency_step(
    address: SocketAddr,
    plan: &ConcurrencyPlan,
) -> Result<ConcurrencyRuntimeSummary, String> {
    run_concurrency_step_in_keyspace(address, plan, ConcurrencyKeyspace::small())
}

pub fn run_concurrency_step_in_keyspace(
    address: SocketAddr,
    plan: &ConcurrencyPlan,
    keyspace: ConcurrencyKeyspace,
) -> Result<ConcurrencyRuntimeSummary, String> {
    run_concurrency_step_in_keyspace_with_query_timeout(
        address,
        plan,
        keyspace,
        Duration::from_secs(30),
    )
}

pub fn run_concurrency_step_in_keyspace_with_query_timeout(
    address: SocketAddr,
    plan: &ConcurrencyPlan,
    keyspace: ConcurrencyKeyspace,
    query_timeout: Duration,
) -> Result<ConcurrencyRuntimeSummary, String> {
    if query_timeout.is_zero() {
        return Err("concurrency query timeout must not be zero".to_string());
    }
    plan.validate()?;
    let readers = 4usize;
    let participants = plan.clients + readers + 1 + 1;
    let start = Arc::new(Barrier::new(participants));
    let active_sessions = Arc::new(AtomicUsize::new(0));
    let peak_sessions = Arc::new(AtomicUsize::new(0));
    let mut actor_handles = Vec::with_capacity(plan.clients);

    for actor_id in 0..plan.clients {
        let transactions: Vec<_> = plan
            .transactions
            .iter()
            .filter(|transaction| transaction.actor_id == actor_id)
            .cloned()
            .collect();
        let start = Arc::clone(&start);
        let active_sessions = Arc::clone(&active_sessions);
        let peak_sessions = Arc::clone(&peak_sessions);
        actor_handles.push(thread::spawn(move || {
            start.wait();
            let current = active_sessions.fetch_add(1, Ordering::AcqRel) + 1;
            peak_sessions.fetch_max(current, Ordering::AcqRel);
            let result = actor_workload(address, actor_id, &transactions, keyspace, query_timeout);
            active_sessions.fetch_sub(1, Ordering::AcqRel);
            result
        }));
    }

    let orm_sql = OrmBuilder::to_sql(
        &QueryBuilder::from_relation(table_as("messages", "m"))
            .select([Expr::qualified("m", "id")])
            .limit(8),
    )
    .map_err(|error| error.to_string())?
    .sql;
    let mut reader_handles = Vec::with_capacity(readers);
    for _ in 0..readers {
        let start = Arc::clone(&start);
        let orm_sql = orm_sql.clone();
        reader_handles.push(thread::spawn(move || {
            start.wait();
            reader_workload(address, &orm_sql, query_timeout)
        }));
    }

    let maintenance_start = Arc::clone(&start);
    let maintenance = thread::spawn(move || {
        maintenance_start.wait();
        maintenance_workload(address, query_timeout)
    });
    start.wait();

    let mut summary = ConcurrencyRuntimeSummary::default();
    for (actor_id, handle) in actor_handles.into_iter().enumerate() {
        let actor_summary = handle
            .join()
            .map_err(|_| format!("concurrency actor {actor_id} panicked"))?
            .map_err(|error| format!("concurrency actor {actor_id}: {error}"))?;
        summary.merge(actor_summary);
    }
    for (reader_id, handle) in reader_handles.into_iter().enumerate() {
        summary.reader_checks += handle
            .join()
            .map_err(|_| format!("concurrency reader {reader_id} panicked"))?
            .map_err(|error| format!("concurrency reader {reader_id}: {error}"))?;
    }
    summary.maintenance_busy += maintenance
        .join()
        .map_err(|_| "concurrency maintenance actor panicked".to_string())??;
    if active_sessions.load(Ordering::Acquire) != 0 {
        return Err("actor session accounting did not return to zero".to_string());
    }
    if peak_sessions.load(Ordering::Acquire) < plan.clients {
        return Err(format!(
            "only {} of {} physical clients overlapped",
            peak_sessions.load(Ordering::Acquire),
            plan.clients
        ));
    }

    resolve_ambiguous_commits(address, &mut summary)?;
    verify_concurrency_state_in_keyspace(address, &summary, keyspace)?;
    Ok(summary)
}

fn actor_workload(
    address: SocketAddr,
    actor_id: usize,
    transactions: &[ConcurrencyTransaction],
    keyspace: ConcurrencyKeyspace,
    query_timeout: Duration,
) -> Result<ConcurrencyRuntimeSummary, String> {
    let mut summary = ConcurrencyRuntimeSummary::default();
    let mut connection =
        tcp_connect_with_read_timeout(address, CONCURRENCY_DATABASE, query_timeout)?;
    for transaction in transactions {
        let candidate_id = message_id(transaction.id, keyspace.message_base)?;
        connection
            .begin()
            .map_err(|error| format!("transaction {} begin: {error}", transaction.id))?;
        match execute_transaction_body(
            &mut connection,
            actor_id,
            transaction,
            candidate_id,
            keyspace,
        ) {
            Ok(BodyOutcome::InvalidRejected) => {
                connection.rollback().map_err(|error| {
                    format!(
                        "transaction {} rollback invalid cohort: {error}",
                        transaction.id
                    )
                })?;
                summary.invalid_rejected += 1;
                summary.rejected_ids.insert(candidate_id);
                continue;
            }
            Ok(BodyOutcome::Valid) => {}
            Err(error) if expected_conflict(&error) => {
                if connection.in_transaction() {
                    connection.rollback().map_err(|rollback| {
                        format!(
                            "transaction {} rollback after conflict `{error}` failed: {rollback}",
                            transaction.id
                        )
                    })?;
                }
                summary.conflicts += 1;
                summary.rejected_ids.insert(candidate_id);
                continue;
            }
            Err(error) => {
                return Err(format!("transaction {} body: {error}", transaction.id));
            }
        }

        match transaction.terminal {
            TransactionTerminal::Commit => match connection.commit() {
                Ok(()) => {
                    summary.committed_ids.insert(candidate_id);
                    if transaction.distribution == KeyDistribution::Disjoint {
                        summary.disjoint_commits += 1;
                    }
                }
                Err(error) if expected_conflict(&error.to_string()) => {
                    if connection.in_transaction() {
                        connection
                            .rollback()
                            .map_err(|rollback| rollback.to_string())?;
                    }
                    summary.conflicts += 1;
                    summary.rejected_ids.insert(candidate_id);
                }
                Err(error) => {
                    return Err(format!("transaction {} commit: {error}", transaction.id));
                }
            },
            TransactionTerminal::Rollback => {
                connection
                    .rollback()
                    .map_err(|error| format!("transaction {} rollback: {error}", transaction.id))?;
                summary.rolled_back += 1;
                summary.rejected_ids.insert(candidate_id);
            }
            TransactionTerminal::Disconnect => {
                drop(connection);
                summary.disconnected += 1;
                summary.rejected_ids.insert(candidate_id);
                connection =
                    tcp_connect_with_read_timeout(address, CONCURRENCY_DATABASE, query_timeout)
                        .map_err(|error| {
                            format!(
                                "transaction {} reconnect after disconnect: {error}",
                                transaction.id
                            )
                        })?;
            }
            TransactionTerminal::IdempotentRetry => match connection.commit() {
                Ok(()) => {
                    // Deliberately discard the local commit decision. The
                    // durable command ledger below is the sole authority.
                    summary.ambiguous_ids.insert(candidate_id);
                }
                Err(error) if expected_conflict(&error.to_string()) => {
                    if connection.in_transaction() {
                        connection
                            .rollback()
                            .map_err(|rollback| rollback.to_string())?;
                    }
                    summary.conflicts += 1;
                    summary.rejected_ids.insert(candidate_id);
                }
                Err(error) => {
                    return Err(format!(
                        "transaction {} idempotent commit: {error}",
                        transaction.id
                    ));
                }
            },
        }
    }
    Ok(summary)
}

enum BodyOutcome {
    Valid,
    InvalidRejected,
}

fn execute_transaction_body(
    connection: &mut Connection,
    actor_id: usize,
    transaction: &ConcurrencyTransaction,
    message_id: i64,
    keyspace: ConcurrencyKeyspace,
) -> Result<BodyOutcome, String> {
    let (user_id, conversation_id) = match transaction.distribution {
        KeyDistribution::SharedHot => (1, 10),
        KeyDistribution::Disjoint => (
            keyspace.disjoint_user_base.saturating_add(actor_id as i64),
            keyspace
                .disjoint_conversation_base
                .saturating_add(actor_id as i64),
        ),
        KeyDistribution::Cold => (3, 20),
    };
    tcp_command(
        connection,
        format!("UPDATE users SET revision = revision + 1 WHERE id = {user_id}"),
    )?;
    tcp_command(
        connection,
        format!(
            "INSERT INTO messages VALUES ({message_id}, {conversation_id}, {message_id}, {user_id}, 'message-{message_id}', false)"
        ),
    )?;
    let message_count = tcp_scalar_i64(
        connection,
        &format!("SELECT COUNT(*) FROM messages WHERE id = {message_id}"),
    )?;
    if message_count != 1 {
        let direct_rows = tcp_rows(
            connection,
            &format!("SELECT id FROM messages WHERE id = {message_id}"),
        )?;
        return Err(format!(
            "read-your-writes lost the inserted message: count={message_count} direct_rows={direct_rows:?}"
        ));
    }

    let outbox_id = message_id + 100_000_000;
    if transaction.cohort == TransactionCohort::InvalidFinalState {
        let invalid = tcp_command(
            connection,
            format!(
                "INSERT INTO outbox_jobs VALUES ({outbox_id}, {message_id}, 'pending', -1, NULL, true)"
            ),
        );
        return match invalid {
            Ok(()) => Err("invalid-final-state transaction unexpectedly succeeded".to_string()),
            Err(error) if error.to_ascii_lowercase().contains("check") => {
                Ok(BodyOutcome::InvalidRejected)
            }
            Err(error) => Err(format!("unexpected invalid cohort error: {error}")),
        };
    }

    let command_key = format!("command-{}", transaction.id);
    for sql in [
        format!(
            "INSERT INTO message_versions VALUES ({}, {message_id}, 1, 'message-{message_id}')",
            message_id + 200_000_000
        ),
        format!(
            "INSERT INTO attachments VALUES ({}, {message_id}, 'blob-{message_id}', 1024)",
            message_id + 300_000_000
        ),
        format!(
            "INSERT INTO reactions VALUES ({}, {message_id}, {user_id}, 'stress')",
            message_id + 400_000_000
        ),
        format!(
            "INSERT INTO receipts VALUES ({}, {message_id}, {user_id}, {message_id}, {message_id})",
            message_id + 500_000_000
        ),
        format!(
            "INSERT INTO outbox_jobs VALUES ({outbox_id}, {message_id}, 'pending', 0, NULL, true)"
        ),
        format!(
            "INSERT INTO sync_events VALUES ({}, {user_id}, {message_id}, {message_id}, {outbox_id}, 'message.created')",
            message_id + 600_000_000
        ),
        format!(
            "INSERT INTO command_results VALUES ({}, {user_id}, '{command_key}', {message_id})",
            message_id + 700_000_000
        ),
        format!(
            "INSERT INTO audit_log VALUES ({}, 1, {user_id}, 'message', {message_id}, 'insert')",
            message_id + 800_000_000
        ),
    ] {
        tcp_command(connection, sql)?;
    }

    for operation in &transaction.operations {
        let scratch_id = message_id
            .saturating_mul(100)
            .saturating_add(operation.ordinal as i64)
            .saturating_add(2_000_000_000);
        match operation.kind {
            super::ConcurrencyOperationKind::Insert => tcp_command(
                connection,
                format!(
                    "INSERT INTO audit_log VALUES ({scratch_id}, 1, {user_id}, 'operation', {message_id}, 'insert') ON CONFLICT (id) DO NOTHING"
                ),
            )?,
            super::ConcurrencyOperationKind::Update => tcp_command(
                connection,
                format!("UPDATE conversations SET title = 'tx-{}' WHERE id = {conversation_id}", transaction.id),
            )?,
            super::ConcurrencyOperationKind::Delete => {
                tcp_command(
                    connection,
                    format!(
                        "INSERT INTO audit_log VALUES ({scratch_id}, 1, {user_id}, 'operation', {message_id}, 'delete') ON CONFLICT (id) DO NOTHING"
                    ),
                )?;
                tcp_command(
                    connection,
                    format!("DELETE FROM audit_log WHERE id = {scratch_id}"),
                )?;
            }
            super::ConcurrencyOperationKind::Upsert => tcp_command(
                connection,
                format!(
                    "INSERT INTO audit_log VALUES ({scratch_id}, 1, {user_id}, 'operation', {message_id}, 'upsert') ON CONFLICT (id) DO UPDATE SET action = EXCLUDED.action"
                ),
            )?,
            super::ConcurrencyOperationKind::ReadYourWrites => {
                if tcp_scalar_i64(
                    connection,
                    &format!("SELECT COUNT(*) FROM outbox_jobs WHERE message_id = {message_id}"),
                )? != 1
                {
                    return Err("read-your-writes lost the outbox row".to_string());
                }
            }
        }
    }
    Ok(BodyOutcome::Valid)
}

fn reader_workload(
    address: SocketAddr,
    orm_sql: &str,
    query_timeout: Duration,
) -> Result<usize, String> {
    let mut checks = 0usize;
    for _ in 0..8 {
        let mut connection =
            tcp_connect_with_read_timeout(address, CONCURRENCY_DATABASE, query_timeout)?;
        for sql in [
            "SELECT COUNT(*) FROM messages",
            "SELECT COUNT(*) FROM pending_outbox_v",
            "SELECT COUNT(*) FROM outbox_jobs o LEFT JOIN messages m ON m.id = o.message_id WHERE m.id IS NULL",
            "SELECT COUNT(*) FROM sync_events s LEFT JOIN messages m ON m.id = s.message_id WHERE s.message_id IS NOT NULL AND m.id IS NULL",
            "SELECT o.id, o.message_id.body FROM outbox_jobs o ORDER BY o.id LIMIT 8",
            orm_sql,
        ] {
            let rows = tcp_rows(&mut connection, sql)?;
            if sql.contains("WHERE m.id IS NULL") {
                let value = rows
                    .first()
                    .and_then(|row| row.values.first())
                    .ok_or_else(|| format!("reader query returned no scalar: {sql}"))?;
                if !matches!(value, radixdb_client::WireValue::Int(0)) {
                    return Err(format!("reader observed impossible cross-table state: {sql}"));
                }
            }
            checks += 1;
        }
    }
    Ok(checks)
}

fn maintenance_workload(address: SocketAddr, query_timeout: Duration) -> Result<usize, String> {
    let mut busy = 0usize;
    for ordinal in 0..3 {
        thread::sleep(Duration::from_millis(15));
        let mut connection =
            tcp_connect_with_read_timeout(address, CONCURRENCY_DATABASE, query_timeout)?;
        tcp_rows(&mut connection, "DESCRIBE DATABASE FORMAT JSON")?;
        drop(connection);
        for sql in if ordinal == 1 {
            ["PRAGMA CHECKPOINT", "PRAGMA SNAPSHOT"].as_slice()
        } else {
            ["PRAGMA CHECKPOINT"].as_slice()
        } {
            // A full 100M snapshot copies and fsyncs the entire database. Its
            // lifecycle is intentionally longer than an interactive query;
            // keep the ordinary query timeout for checkpoints but give the
            // snapshot a bounded maintenance window. Disconnect cancellation
            // remains active and is tested independently.
            let command_timeout = if *sql == "PRAGMA SNAPSHOT" {
                query_timeout.max(Duration::from_secs(30 * 60))
            } else {
                query_timeout
            };
            let mut connection =
                tcp_connect_with_read_timeout(address, CONCURRENCY_DATABASE, command_timeout)?;
            if let Err(error) = tcp_command(&mut connection, *sql) {
                let normalized = error.to_ascii_lowercase();
                if is_expected_maintenance_busy(&normalized) {
                    busy += 1;
                } else {
                    return Err(format!("maintenance `{sql}` failed: {error}"));
                }
            }
        }
    }
    Ok(busy)
}

fn is_expected_maintenance_busy(normalized_error: &str) -> bool {
    // Only named, pre-publication maintenance outcomes are admissible here.
    // Broad tokens such as `lock`, `busy` or `active transaction` would also
    // hide corruption, poisoned ownership or recovery defects.
    normalized_error.contains("forced checkpoint left committed hot rows unsealed")
        // The checkpoint fence is deliberately time-bounded so maintenance
        // cannot starve foreground commits. Under the chaos workload this is
        // the explicit busy outcome, not a failed or partial checkpoint.
        || normalized_error.contains("checkpoint timed out acquiring the commit fence")
        // Snapshot publication uses the same bounded, fail-closed policy. A
        // timeout before the fence is acquired publishes no snapshot and is
        // therefore the explicit maintenance-busy outcome under write chaos.
        || normalized_error.contains("pragma snapshot timed out waiting for commit publication fence")
}

fn resolve_ambiguous_commits(
    address: SocketAddr,
    summary: &mut ConcurrencyRuntimeSummary,
) -> Result<(), String> {
    let mut connection = tcp_connect(address, CONCURRENCY_DATABASE)?;
    let ambiguous: Vec<_> = summary.ambiguous_ids.iter().copied().collect();
    for message_id in ambiguous {
        let count = tcp_scalar_i64(
            &mut connection,
            &format!("SELECT COUNT(*) FROM command_results WHERE message_id = {message_id}"),
        )?;
        match count {
            1 => {
                summary.committed_ids.insert(message_id);
                summary.retries_resolved += 1;
            }
            0 => {
                summary.rejected_ids.insert(message_id);
                summary.retries_resolved += 1;
            }
            other => return Err(format!("ambiguous command ledger has multiplicity {other}")),
        }
    }
    Ok(())
}

pub fn verify_concurrency_state(
    address: SocketAddr,
    summary: &ConcurrencyRuntimeSummary,
) -> Result<(), String> {
    verify_concurrency_state_in_keyspace(address, summary, ConcurrencyKeyspace::small())
}

pub fn verify_concurrency_state_in_keyspace(
    address: SocketAddr,
    summary: &ConcurrencyRuntimeSummary,
    keyspace: ConcurrencyKeyspace,
) -> Result<(), String> {
    let mut connection = tcp_connect(address, CONCURRENCY_DATABASE)?;
    let actual: BTreeSet<i64> = tcp_rows(
        &mut connection,
        &format!(
            "SELECT id FROM messages WHERE id >= {} ORDER BY id",
            keyspace.message_base
        ),
    )?
    .into_iter()
    .map(|row| match row.values.first() {
        Some(radixdb_client::WireValue::Int(value)) => Ok(*value),
        other => Err(format!("unexpected message identity {other:?}")),
    })
    .collect::<Result<_, _>>()?;
    if actual != summary.committed_ids {
        return Err(format!(
            "durable message set differs: expected {:?}, got {:?}",
            summary.committed_ids, actual
        ));
    }
    if !summary.rejected_ids.is_disjoint(&actual) {
        return Err("rollback/failed/disconnected transaction published a message".to_string());
    }
    for (table, column) in [
        ("message_versions", "message_id"),
        ("attachments", "message_id"),
        ("reactions", "message_id"),
        ("receipts", "message_id"),
        ("outbox_jobs", "message_id"),
        ("sync_events", "message_id"),
        ("command_results", "message_id"),
    ] {
        let count = tcp_scalar_i64(
            &mut connection,
            &format!(
                "SELECT COUNT(*) FROM {table} WHERE {column} >= {}",
                keyspace.message_base
            ),
        )?;
        if count != summary.committed_ids.len() as i64 {
            return Err(format!(
                "{table} contains {count} stress rows for {} committed messages",
                summary.committed_ids.len()
            ));
        }
    }
    for sql in [
        format!(
            "SELECT COUNT(*) FROM messages m LEFT JOIN outbox_jobs o ON o.message_id = m.id WHERE m.id >= {} AND o.id IS NULL",
            keyspace.message_base
        ),
        format!(
            "SELECT COUNT(*) FROM sync_events s LEFT JOIN messages m ON m.id = s.message_id WHERE s.message_id >= {} AND m.id IS NULL",
            keyspace.message_base
        ),
    ] {
        if tcp_scalar_i64(&mut connection, &sql)? != 0 {
            return Err(format!("cross-table invariant failed: {sql}"));
        }
    }
    if summary.disjoint_commits == 0 {
        return Err("disjoint cohort made no commit progress".to_string());
    }
    Ok(())
}

pub fn run_overload_wave(
    address: SocketAddr,
    max_connections: usize,
) -> Result<(usize, usize), String> {
    run_overload_wave_for_fixture(address, max_connections, 2)
}

pub fn run_overload_wave_for_fixture(
    address: SocketAddr,
    max_connections: usize,
    expected_tenants: i64,
) -> Result<(usize, usize), String> {
    let attempts = max_connections.saturating_add(16);
    let (accepted, refused) = run_connection_wave(address, attempts, expected_tenants)?;
    if accepted > max_connections || refused == 0 {
        return Err(format!(
            "overload accounting invalid: accepted={accepted} refused={refused} max={max_connections}"
        ));
    }
    Ok((accepted, refused))
}

pub fn run_capacity_probe(
    address: SocketAddr,
    attempted_clients: usize,
    expected_tenants: i64,
) -> Result<(usize, usize), String> {
    if attempted_clients == 0 {
        return Err("capacity probe requires at least one client".to_string());
    }
    run_connection_wave(address, attempted_clients, expected_tenants)
}

fn run_connection_wave(
    address: SocketAddr,
    attempts: usize,
    expected_tenants: i64,
) -> Result<(usize, usize), String> {
    let release = Arc::new(AtomicBool::new(false));
    let (sender, receiver) = mpsc::channel();
    let mut handles = Vec::with_capacity(attempts);
    for _ in 0..attempts {
        let sender = sender.clone();
        let release = Arc::clone(&release);
        handles.push(thread::spawn(move || {
            let connection = tcp_connect(address, CONCURRENCY_DATABASE);
            sender.send(connection.is_ok()).ok();
            if let Ok(connection) = connection {
                while !release.load(Ordering::Acquire) {
                    thread::sleep(Duration::from_millis(2));
                }
                drop(connection);
            }
        }));
    }
    drop(sender);
    let mut accepted = 0usize;
    let mut refused = 0usize;
    let mut receive_error = None;
    for _ in 0..attempts {
        match receiver.recv_timeout(Duration::from_secs(15)) {
            Ok(true) => accepted += 1,
            Ok(false) => refused += 1,
            Err(error) => {
                receive_error = Some(format!(
                    "overload connector accounting stopped before {attempts} results: {error}"
                ));
                break;
            }
        }
    }
    // Accepted clients deliberately retain their permits until every connect
    // attempt has reported.  Release them before joining; waiting for channel
    // closure here would deadlock because those threads still own senders.
    release.store(true, Ordering::Release);
    for handle in handles {
        handle
            .join()
            .map_err(|_| "overload connector panicked".to_string())?;
    }
    if let Some(error) = receive_error {
        return Err(error);
    }
    for _ in 0..16 {
        let mut health = tcp_connect(address, CONCURRENCY_DATABASE)?;
        if tcp_scalar_i64(&mut health, "SELECT COUNT(*) FROM tenants")? != expected_tenants {
            return Err("post-overload health query changed the fixture".to_string());
        }
    }
    Ok((accepted, refused))
}

fn message_id(transaction_id: u64, message_base: i64) -> Result<i64, String> {
    i64::try_from(transaction_id)
        .map(|transaction_id| message_base.saturating_add(transaction_id))
        .map_err(|_| "transaction id does not fit message identity".to_string())
}

fn expected_conflict(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();
    normalized.contains("serialization conflict")
        || normalized.contains("write conflict")
        || normalized.contains("timed out while waiting")
        || normalized.contains("unique constraint")
}

#[cfg(test)]
mod maintenance_busy_tests {
    use super::is_expected_maintenance_busy;

    #[test]
    fn classifies_only_explicit_maintenance_fence_timeouts_as_busy() {
        assert!(is_expected_maintenance_busy(
            "execute `pragma checkpoint`: server sqlerror: checkpoint timed out acquiring the commit fence"
        ));
        assert!(is_expected_maintenance_busy(
            "execute `pragma snapshot`: server sqlerror: pragma snapshot timed out waiting for commit publication fence"
        ));
        assert!(!is_expected_maintenance_busy(
            "execute `pragma checkpoint`: checksum mismatch"
        ));
        assert!(!is_expected_maintenance_busy(
            "execute `pragma snapshot`: database lock is corrupted"
        ));
        assert!(!is_expected_maintenance_busy(
            "execute `pragma checkpoint`: busy checksum writer"
        ));
    }
}
