// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![cfg(feature = "stress-tests")]

//! Deterministic TCP chaos workload modelled after the Mozaic messenger.
//!
//! The workload deliberately combines shared conversation sequence rows,
//! multi-table message publication, explicit commit/rollback, peer disconnect
//! with an active transaction, short-lived readers, unauthenticated socket
//! churn, concurrent checkpoints and two server restarts. Acceptance is based
//! on exact durable sets and cross-table invariants, not merely server liveness.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier,
    },
    thread,
    time::Duration,
};

use radixdb::server::{default_target_volume_rows, Server, ServerConfig};
use radixdb_client::{Connection, ExecuteResult, Row, WireValue};

const DATABASE: &str = "messenger_chaos";
const USERS: usize = 32;
const CONVERSATIONS: usize = 8;
const WRITERS: usize = 32;
const TRANSACTIONS_PER_WRITER: usize = 8;
const READERS: usize = 8;
const READS_PER_READER: usize = 16;
const CHURN_THREADS: usize = 2;
const CHURN_CONNECTIONS_PER_THREAD: usize = 64;
const CHECKPOINTS_PER_PHASE: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoricalTerminalDecision {
    Commit,
    Rollback,
    Disconnect,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoricalWorkloadSignature {
    pub transaction_attempts: usize,
    pub planned_commits: usize,
    pub planned_rollbacks: usize,
    pub planned_disconnects: usize,
    pub reader_connections: usize,
    pub raw_disconnects: usize,
    pub checkpoint_attempts: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoricalChaosSummary {
    pub committed: usize,
    pub rolled_back: usize,
    pub disconnected: usize,
    pub conflicts: usize,
    pub checkpoint_busy: usize,
}

pub fn historical_terminal_decision(
    phase: usize,
    writer: usize,
    iteration: usize,
) -> HistoricalTerminalDecision {
    match (phase + writer + iteration) % 5 {
        0 => HistoricalTerminalDecision::Rollback,
        1 => HistoricalTerminalDecision::Disconnect,
        _ => HistoricalTerminalDecision::Commit,
    }
}

pub fn historical_workload_signature() -> HistoricalWorkloadSignature {
    let mut planned_commits = 0;
    let mut planned_rollbacks = 0;
    let mut planned_disconnects = 0;
    for phase in 0..2 {
        for writer in 0..WRITERS {
            for iteration in 0..TRANSACTIONS_PER_WRITER {
                match historical_terminal_decision(phase, writer, iteration) {
                    HistoricalTerminalDecision::Commit => planned_commits += 1,
                    HistoricalTerminalDecision::Rollback => planned_rollbacks += 1,
                    HistoricalTerminalDecision::Disconnect => planned_disconnects += 1,
                }
            }
        }
    }
    HistoricalWorkloadSignature {
        transaction_attempts: WRITERS * TRANSACTIONS_PER_WRITER * 2,
        planned_commits,
        planned_rollbacks,
        planned_disconnects,
        reader_connections: READERS * READS_PER_READER * 2,
        raw_disconnects: CHURN_THREADS * CHURN_CONNECTIONS_PER_THREAD * 2,
        checkpoint_attempts: CHECKPOINTS_PER_PHASE * 2 + 2,
    }
}

struct ShutdownOnDrop<'a>(&'a AtomicBool);

impl Drop for ShutdownOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CommittedMessage {
    id: i64,
    conversation_id: i64,
    sequence: i64,
}

#[derive(Debug, Default)]
struct PhaseReport {
    committed: Vec<CommittedMessage>,
    rolled_back: usize,
    disconnected: usize,
    conflicts: usize,
    max_missing_outbox: i64,
    max_orphan_sync: i64,
    checkpoint_busy: usize,
}

impl PhaseReport {
    fn merge(&mut self, mut other: Self) {
        self.committed.append(&mut other.committed);
        self.rolled_back += other.rolled_back;
        self.disconnected += other.disconnected;
        self.conflicts += other.conflicts;
        self.max_missing_outbox = self.max_missing_outbox.max(other.max_missing_outbox);
        self.max_orphan_sync = self.max_orphan_sync.max(other.max_orphan_sync);
        self.checkpoint_busy += other.checkpoint_busy;
    }
}

fn server_config(data_dir: std::path::PathBuf) -> ServerConfig {
    ServerConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 0,
        data_dir,
        transport: Default::default(),
        authentication: Default::default(),
        max_connections: 128,
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
        copy_max_transaction_bytes: radixdb::server::default_copy_max_transaction_bytes(),
        max_compaction_jobs: radixdb::server::default_max_compaction_jobs(),
        storage_cpu_workers: radixdb::server::default_storage_cpu_workers(),
        page_cache_level: radixdb::server::default_page_cache_level(),
        page_cache_max_bytes: radixdb::server::default_page_cache_max_bytes(),
        page_cache_memory_reserve: radixdb::server::default_page_cache_memory_reserve(),
        target_volume_rows: default_target_volume_rows(),
        // Force the small stress fixture through hot-to-cold publication.
        seal_hot_bytes_threshold: 16 * 1024,
        seal_incremental_hot_bytes_threshold: 4 * 1024,
        read_queue_depth: 2,
    }
}

fn connect(address: SocketAddr) -> Result<Connection, String> {
    let mut connection = Connection::connect_with_timeouts(
        address,
        Duration::from_secs(3),
        Duration::from_secs(30),
        Duration::from_secs(10),
    )
    .map_err(|error| format!("connect: {error}"))?;
    connection
        .authenticate("root", None)
        .map_err(|error| format!("authenticate: {error}"))?;
    connection
        .select_database(DATABASE)
        .map_err(|error| format!("select database: {error}"))?;
    Ok(connection)
}

fn command(connection: &mut Connection, sql: impl Into<String>) -> Result<(), String> {
    match connection.execute(sql).map_err(|error| error.to_string())? {
        ExecuteResult::CommandComplete { .. } => Ok(()),
        ExecuteResult::Cursor(cursor) => {
            loop {
                let batch = connection
                    .fetch(&cursor)
                    .map_err(|error| error.to_string())?;
                if batch.eof {
                    break;
                }
            }
            Ok(())
        }
    }
}

fn rows(connection: &mut Connection, sql: &str) -> Result<Vec<Row>, String> {
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

fn int(value: &WireValue) -> Result<i64, String> {
    match value {
        WireValue::Int(value) => Ok(*value),
        other => Err(format!("expected INTEGER, got {other:?}")),
    }
}

fn scalar_i64(connection: &mut Connection, sql: &str) -> Result<i64, String> {
    let mut result = rows(connection, sql)?;
    if result.len() != 1 || result[0].values.len() != 1 {
        return Err(format!(
            "expected one scalar row for `{sql}`, got {result:?}"
        ));
    }
    int(&result.remove(0).values[0])
}

fn with_server<T>(data_dir: std::path::PathBuf, operation: impl FnOnce(SocketAddr) -> T) -> T {
    let config = server_config(data_dir);
    let server = Server::bind_ephemeral(&config).expect("bind chaos server");
    let address = server.local_addr().expect("chaos server address");
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let _shutdown_on_unwind = ShutdownOnDrop(&shutdown);
        let worker = scope.spawn(|| server.run_until(&shutdown));
        let result = operation(address);
        shutdown.store(true, Ordering::Release);
        worker
            .join()
            .expect("chaos server thread joins")
            .expect("chaos server stops cleanly");
        result
    })
}

fn create_schema(address: SocketAddr) {
    let mut connection = connect(address).expect("connect schema owner");
    for ddl in [
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            normalized_username TEXT NOT NULL,
            profile_revision INTEGER NOT NULL CHECK (profile_revision >= 1),
            disabled BOOLEAN NOT NULL
        )",
        "CREATE UNIQUE INDEX users_name_uidx ON users(normalized_username)",
        "CREATE TABLE devices (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            platform TEXT NOT NULL CHECK (platform IN ('android', 'web', 'linux', 'windows')),
            revision INTEGER NOT NULL CHECK (revision >= 1)
        )",
        "CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            created_by INTEGER NOT NULL REFERENCES users(id),
            kind TEXT NOT NULL CHECK (kind IN ('direct', 'group')),
            next_seq INTEGER NOT NULL CHECK (next_seq >= 0),
            committed_count INTEGER NOT NULL CHECK (committed_count >= 0),
            revision INTEGER NOT NULL CHECK (revision >= 1)
        )",
        "CREATE TABLE conversation_members (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL REFERENCES conversations(id),
            user_id INTEGER NOT NULL REFERENCES users(id),
            role TEXT NOT NULL CHECK (role IN ('member', 'admin', 'owner')),
            last_delivered_seq INTEGER NOT NULL CHECK (last_delivered_seq >= 0),
            last_read_seq INTEGER NOT NULL CHECK (last_read_seq >= 0),
            CHECK (last_read_seq <= last_delivered_seq)
        )",
        "CREATE UNIQUE INDEX members_scope_uidx
            ON conversation_members(conversation_id, user_id)",
        "CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL REFERENCES conversations(id),
            conversation_seq INTEGER NOT NULL CHECK (conversation_seq >= 1),
            sender_user_id INTEGER NOT NULL REFERENCES users(id),
            sender_device_id INTEGER NOT NULL REFERENCES devices(id),
            client_message_key TEXT NOT NULL,
            body TEXT NOT NULL,
            revision INTEGER NOT NULL CHECK (revision >= 1)
        )",
        "CREATE UNIQUE INDEX messages_sequence_uidx
            ON messages(conversation_id, conversation_seq)",
        "CREATE UNIQUE INDEX messages_idempotency_uidx
            ON messages(sender_user_id, client_message_key)",
        "CREATE TABLE outbox_jobs (
            id INTEGER PRIMARY KEY,
            message_id INTEGER NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('pending', 'leased', 'processed', 'dead')),
            attempts INTEGER NOT NULL CHECK (attempts >= 0),
            revision INTEGER NOT NULL CHECK (revision >= 1)
        )",
        "CREATE UNIQUE INDEX outbox_message_uidx ON outbox_jobs(message_id)",
        "CREATE INDEX outbox_state_idx ON outbox_jobs(state, id)",
        "CREATE TABLE sync_events (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            cursor INTEGER NOT NULL CHECK (cursor >= 1),
            message_id INTEGER NOT NULL REFERENCES messages(id),
            outbox_job_id INTEGER NOT NULL REFERENCES outbox_jobs(id),
            event_type TEXT NOT NULL
        )",
        "CREATE UNIQUE INDEX sync_user_cursor_uidx ON sync_events(user_id, cursor)",
        "CREATE UNIQUE INDEX sync_outbox_user_uidx ON sync_events(outbox_job_id, user_id)",
        "CREATE TABLE message_reactions (
            id INTEGER PRIMARY KEY,
            message_id INTEGER NOT NULL REFERENCES messages(id),
            user_id INTEGER NOT NULL REFERENCES users(id),
            emoji TEXT NOT NULL
        )",
        "CREATE UNIQUE INDEX reactions_scope_uidx
            ON message_reactions(message_id, user_id, emoji)",
        "CREATE TABLE message_receipts (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL REFERENCES conversations(id),
            user_id INTEGER NOT NULL REFERENCES users(id),
            delivered_seq INTEGER NOT NULL CHECK (delivered_seq >= 0),
            read_seq INTEGER NOT NULL CHECK (read_seq >= 0),
            CHECK (read_seq <= delivered_seq)
        )",
        "CREATE UNIQUE INDEX receipts_scope_uidx
            ON message_receipts(conversation_id, user_id)",
        "CREATE TABLE command_results (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            command_key TEXT NOT NULL,
            message_id INTEGER NOT NULL REFERENCES messages(id)
        )",
        "CREATE UNIQUE INDEX command_results_scope_uidx
            ON command_results(user_id, command_key)",
    ] {
        command(&mut connection, ddl)
            .unwrap_or_else(|error| panic!("DDL failed for `{ddl}`: {error}"));
    }

    connection.begin().expect("begin fixture transaction");
    for user in 1..=USERS as i64 {
        command(
            &mut connection,
            format!("INSERT INTO users VALUES ({user}, 'user-{user}', 1, false)"),
        )
        .expect("insert user fixture");
        let platform = match user % 4 {
            0 => "android",
            1 => "web",
            2 => "linux",
            _ => "windows",
        };
        command(
            &mut connection,
            format!("INSERT INTO devices VALUES ({user}, {user}, '{platform}', 1)"),
        )
        .expect("insert device fixture");
    }
    for conversation in 1..=CONVERSATIONS as i64 {
        let kind = if conversation % 2 == 0 {
            "group"
        } else {
            "direct"
        };
        command(
            &mut connection,
            format!("INSERT INTO conversations VALUES ({conversation}, 1, '{kind}', 0, 0, 1)"),
        )
        .expect("insert conversation fixture");
        for user in 1..=USERS as i64 {
            let membership_id = conversation * 1_000 + user;
            let role = if user == 1 { "owner" } else { "member" };
            command(
                &mut connection,
                format!(
                    "INSERT INTO conversation_members VALUES \
                     ({membership_id}, {conversation}, {user}, '{role}', 0, 0)"
                ),
            )
            .expect("insert membership fixture");
            command(
                &mut connection,
                format!(
                    "INSERT INTO message_receipts VALUES \
                     ({membership_id}, {conversation}, {user}, 0, 0)"
                ),
            )
            .expect("insert receipt fixture");
        }
    }
    connection.commit().expect("commit fixture transaction");
}

fn update_returning_sequence(
    connection: &mut Connection,
    conversation_id: i64,
) -> Result<i64, String> {
    scalar_i64(
        connection,
        &format!(
            "UPDATE conversations
             SET next_seq = next_seq + 1,
                 committed_count = committed_count + 1,
                 revision = revision + 1
             WHERE id = {conversation_id}
             RETURNING next_seq"
        ),
    )
}

fn populate_message_transaction(
    connection: &mut Connection,
    phase: usize,
    writer: usize,
    iteration: usize,
) -> Result<CommittedMessage, String> {
    let user_id = writer as i64 + 1;
    let recipient_id = user_id % USERS as i64 + 1;
    let conversation_id = ((writer + iteration * 3 + phase) % CONVERSATIONS) as i64 + 1;
    let message_id = (phase as i64 + 1) * 1_000_000 + writer as i64 * 1_000 + iteration as i64 + 1;
    let sequence = update_returning_sequence(connection, conversation_id)?;
    let command_key = format!("phase-{phase}-writer-{writer}-iteration-{iteration}");

    command(
        connection,
        format!(
            "INSERT INTO messages VALUES \
             ({message_id}, {conversation_id}, {sequence}, {user_id}, {user_id}, \
              '{command_key}', 'chaos-message-{message_id}', 1)"
        ),
    )?;
    command(
        connection,
        format!("INSERT INTO outbox_jobs VALUES ({message_id}, {message_id}, 'pending', 0, 1)"),
    )?;
    for (ordinal, recipient) in [(1_i64, user_id), (2_i64, recipient_id)] {
        let event_id = message_id * 10 + ordinal;
        command(
            connection,
            format!(
                "INSERT INTO sync_events VALUES \
                 ({event_id}, {recipient}, {event_id}, {message_id}, {message_id}, 'message.created')"
            ),
        )?;
    }
    command(
        connection,
        format!(
            "INSERT INTO message_reactions VALUES \
             ({message_id}, {message_id}, {user_id}, 'chaos')"
        ),
    )?;
    command(
        connection,
        format!(
            "INSERT INTO command_results VALUES \
             ({message_id}, {user_id}, '{command_key}', {message_id})"
        ),
    )?;
    let membership_id = conversation_id * 1_000 + user_id;
    command(
        connection,
        format!(
            "UPDATE conversation_members
             SET last_delivered_seq = {sequence}
             WHERE id = {membership_id}"
        ),
    )?;
    command(
        connection,
        format!(
            "UPDATE message_receipts
             SET delivered_seq = {sequence}
             WHERE id = {membership_id}"
        ),
    )?;

    Ok(CommittedMessage {
        id: message_id,
        conversation_id,
        sequence,
    })
}

fn expected_conflict(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();
    normalized.contains("serialization conflict")
        || normalized.contains("write conflict")
        || normalized.contains("timed out while waiting")
}

fn rollback_after_conflict(connection: &mut Connection, error: &str) -> Result<(), String> {
    if !expected_conflict(error) {
        return Err(format!("unexpected transaction error: {error}"));
    }
    if connection.in_transaction() {
        connection
            .rollback()
            .map_err(|rollback| format!("rollback after `{error}` failed: {rollback}"))?;
    }
    Ok(())
}

fn writer_workload(
    address: SocketAddr,
    phase: usize,
    writer: usize,
    start: Arc<Barrier>,
) -> Result<PhaseReport, String> {
    start.wait();
    let mut report = PhaseReport::default();
    for iteration in 0..TRANSACTIONS_PER_WRITER {
        let mut connection = connect(address)?;
        connection.begin().map_err(|error| error.to_string())?;
        let candidate =
            match populate_message_transaction(&mut connection, phase, writer, iteration) {
                Ok(candidate) => candidate,
                Err(error) => {
                    rollback_after_conflict(&mut connection, &error)?;
                    report.conflicts += 1;
                    continue;
                }
            };

        match historical_terminal_decision(phase, writer, iteration) {
            HistoricalTerminalDecision::Rollback => {
                connection.rollback().map_err(|error| error.to_string())?;
                report.rolled_back += 1;
            }
            HistoricalTerminalDecision::Disconnect => {
                // Deliberately omit a transaction terminal frame. Dropping the
                // socket must release every claim and discard the write set.
                drop(connection);
                report.disconnected += 1;
            }
            HistoricalTerminalDecision::Commit => match connection.commit() {
                Ok(()) => report.committed.push(candidate),
                Err(error) => {
                    let error = error.to_string();
                    rollback_after_conflict(&mut connection, &error)?;
                    report.conflicts += 1;
                }
            },
        }
    }
    Ok(report)
}

fn reader_workload(address: SocketAddr, start: Arc<Barrier>) -> Result<(i64, i64), String> {
    start.wait();
    let mut max_missing_outbox = 0;
    let mut max_orphan_sync = 0;
    for _ in 0..READS_PER_READER {
        let mut connection = connect(address)?;
        let missing_outbox = scalar_i64(
            &mut connection,
            "SELECT COUNT(*)
             FROM messages m LEFT JOIN outbox_jobs o ON o.message_id = m.id
             WHERE o.id IS NULL",
        )?;
        let orphan_sync = scalar_i64(
            &mut connection,
            "SELECT COUNT(*)
             FROM sync_events s LEFT JOIN messages m ON m.id = s.message_id
             WHERE m.id IS NULL",
        )?;
        max_missing_outbox = max_missing_outbox.max(missing_outbox);
        max_orphan_sync = max_orphan_sync.max(orphan_sync);
    }
    Ok((max_missing_outbox, max_orphan_sync))
}

fn historical_mixed_epoch_oracle(missing_outbox: i64, orphan_sync: i64) -> Result<(), String> {
    if missing_outbox != 0 {
        return Err(format!(
            "a reader observed {missing_outbox} message(s) without same-transaction outbox rows"
        ));
    }
    if orphan_sync != 0 {
        return Err(format!(
            "a reader observed {orphan_sync} sync event(s) without same-transaction message rows"
        ));
    }
    Ok(())
}

fn churn_workload(address: SocketAddr, start: Arc<Barrier>) -> Result<(), String> {
    start.wait();
    for _ in 0..CHURN_CONNECTIONS_PER_THREAD {
        let stream = TcpStream::connect_timeout(&address, Duration::from_secs(3))
            .map_err(|error| format!("raw churn connect: {error}"))?;
        drop(stream);
    }
    Ok(())
}

fn checkpoint_workload(address: SocketAddr, start: Arc<Barrier>) -> Result<usize, String> {
    start.wait();
    let mut busy = 0;
    for _ in 0..CHECKPOINTS_PER_PHASE {
        thread::sleep(Duration::from_millis(25));
        let mut connection = connect(address)?;
        if let Err(error) = command(&mut connection, "PRAGMA CHECKPOINT") {
            if error.contains("forced checkpoint left committed hot rows unsealed") {
                busy += 1;
            } else {
                return Err(error);
            }
        }
    }
    Ok(busy)
}

fn run_phase(address: SocketAddr, phase: usize) -> PhaseReport {
    let participants = WRITERS + READERS + CHURN_THREADS + 1 + 1;
    let start = Arc::new(Barrier::new(participants));
    let mut writers = Vec::with_capacity(WRITERS);
    for writer in 0..WRITERS {
        let start = Arc::clone(&start);
        writers.push(thread::spawn(move || {
            writer_workload(address, phase, writer, start)
        }));
    }
    let mut readers = Vec::with_capacity(READERS);
    for _ in 0..READERS {
        let start = Arc::clone(&start);
        readers.push(thread::spawn(move || reader_workload(address, start)));
    }
    let mut churners = Vec::with_capacity(CHURN_THREADS);
    for _ in 0..CHURN_THREADS {
        let start = Arc::clone(&start);
        churners.push(thread::spawn(move || churn_workload(address, start)));
    }
    let checkpoint_start = Arc::clone(&start);
    let checkpoint = thread::spawn(move || checkpoint_workload(address, checkpoint_start));
    start.wait();

    let mut report = PhaseReport::default();
    for writer in writers {
        report.merge(
            writer
                .join()
                .expect("chaos writer joins")
                .expect("chaos writer succeeds"),
        );
    }
    for reader in readers {
        let (missing_outbox, orphan_sync) = reader
            .join()
            .expect("chaos reader joins")
            .expect("chaos reader succeeds");
        report.max_missing_outbox = report.max_missing_outbox.max(missing_outbox);
        report.max_orphan_sync = report.max_orphan_sync.max(orphan_sync);
    }
    for churner in churners {
        churner
            .join()
            .expect("socket churn worker joins")
            .expect("socket churn succeeds");
    }
    report.checkpoint_busy += checkpoint
        .join()
        .expect("checkpoint worker joins")
        .expect("concurrent checkpoints have only documented busy outcomes");

    // Repeated health connections prove that peer churn and active-transaction
    // disconnects released both session registrations and connection permits.
    for _ in 0..16 {
        let mut health = connect(address).expect("connect after chaos phase");
        assert_eq!(
            scalar_i64(&mut health, "SELECT COUNT(*) FROM conversations").unwrap(),
            CONVERSATIONS as i64
        );
    }
    report
}

fn query_i64_set(connection: &mut Connection, sql: &str) -> BTreeSet<i64> {
    rows(connection, sql)
        .expect("query durable identifier set")
        .into_iter()
        .map(|row| {
            assert_eq!(row.values.len(), 1);
            int(&row.values[0]).expect("identifier is integer")
        })
        .collect()
}

fn verify_database(address: SocketAddr, expected: &[CommittedMessage]) {
    let mut connection = connect(address).expect("connect invariant verifier");
    let expected_ids: BTreeSet<_> = expected.iter().map(|message| message.id).collect();
    let expected_tuples: BTreeSet<_> = expected
        .iter()
        .map(|message| (message.conversation_id, message.sequence, message.id))
        .collect();

    let message_ids = query_i64_set(&mut connection, "SELECT id FROM messages ORDER BY id");
    assert_eq!(
        message_ids, expected_ids,
        "commit/rollback/drop durable set"
    );
    for sql in [
        "SELECT message_id FROM outbox_jobs ORDER BY message_id",
        "SELECT message_id FROM message_reactions ORDER BY message_id",
        "SELECT message_id FROM command_results ORDER BY message_id",
    ] {
        assert_eq!(
            query_i64_set(&mut connection, sql),
            expected_ids,
            "cross-table durable identity mismatch for {sql}"
        );
    }

    assert_eq!(
        scalar_i64(&mut connection, "SELECT COUNT(*) FROM sync_events").unwrap(),
        (expected.len() * 2) as i64
    );
    assert_eq!(
        scalar_i64(
            &mut connection,
            "SELECT COUNT(*)
             FROM messages m
             JOIN outbox_jobs o ON o.message_id = m.id
             JOIN command_results c ON c.message_id = m.id"
        )
        .unwrap(),
        expected.len() as i64
    );
    assert_eq!(
        scalar_i64(
            &mut connection,
            "SELECT COUNT(*)
             FROM messages m LEFT JOIN outbox_jobs o ON o.message_id = m.id
             WHERE o.id IS NULL"
        )
        .unwrap(),
        0
    );
    assert_eq!(
        scalar_i64(
            &mut connection,
            "SELECT SUM(committed_count) FROM conversations"
        )
        .unwrap(),
        expected.len() as i64
    );

    let actual_tuples: BTreeSet<_> = rows(
        &mut connection,
        "SELECT conversation_id, conversation_seq, id
         FROM messages ORDER BY conversation_id, conversation_seq",
    )
    .expect("query committed message tuples")
    .into_iter()
    .map(|row| {
        assert_eq!(row.values.len(), 3);
        (
            int(&row.values[0]).unwrap(),
            int(&row.values[1]).unwrap(),
            int(&row.values[2]).unwrap(),
        )
    })
    .collect();
    assert_eq!(actual_tuples, expected_tuples);

    let mut sequences_by_conversation: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for message in expected {
        sequences_by_conversation
            .entry(message.conversation_id)
            .or_default()
            .push(message.sequence);
    }
    let conversation_rows = rows(
        &mut connection,
        "SELECT id, next_seq, committed_count FROM conversations ORDER BY id",
    )
    .expect("query conversation heads");
    assert_eq!(conversation_rows.len(), CONVERSATIONS);
    for row in conversation_rows {
        let conversation_id = int(&row.values[0]).unwrap();
        let next_seq = int(&row.values[1]).unwrap();
        let committed_count = int(&row.values[2]).unwrap();
        let sequences = sequences_by_conversation
            .entry(conversation_id)
            .or_default();
        sequences.sort_unstable();
        let expected_sequences: Vec<_> = (1..=sequences.len() as i64).collect();
        assert_eq!(
            sequences.as_slice(),
            expected_sequences.as_slice(),
            "conversation {conversation_id} has a gap or duplicate sequence"
        );
        assert_eq!(committed_count, sequences.len() as i64);
        assert_eq!(next_seq, committed_count);
    }

    let sync_multiplicity = rows(
        &mut connection,
        "SELECT message_id, COUNT(*) FROM sync_events
         GROUP BY message_id ORDER BY message_id",
    )
    .expect("query sync fanout");
    assert_eq!(sync_multiplicity.len(), expected.len());
    for row in sync_multiplicity {
        assert!(expected_ids.contains(&int(&row.values[0]).unwrap()));
        assert_eq!(int(&row.values[1]).unwrap(), 2);
    }
}

pub fn run_historical_messenger_chaos() -> HistoricalChaosSummary {
    let temp = tempfile::tempdir().expect("chaos tempdir");
    let data_dir = temp.path().join("data");

    let first = with_server(data_dir.clone(), |address| {
        create_schema(address);
        let report = run_phase(address, 0);
        verify_database(address, &report.committed);
        let mut checkpoint = connect(address).expect("connect final phase-one checkpoint");
        command(&mut checkpoint, "PRAGMA CHECKPOINT").expect("phase-one checkpoint");
        report
    });

    let second = with_server(data_dir.clone(), |address| {
        verify_database(address, &first.committed);
        let report = run_phase(address, 1);
        let mut combined = PhaseReport::default();
        combined.merge(PhaseReport {
            committed: first.committed.clone(),
            rolled_back: first.rolled_back,
            disconnected: first.disconnected,
            conflicts: first.conflicts,
            max_missing_outbox: first.max_missing_outbox,
            max_orphan_sync: first.max_orphan_sync,
            checkpoint_busy: first.checkpoint_busy,
        });
        combined.merge(report);
        verify_database(address, &combined.committed);
        let mut checkpoint = connect(address).expect("connect final phase-two checkpoint");
        command(&mut checkpoint, "PRAGMA CHECKPOINT").expect("phase-two checkpoint");
        combined
    });

    with_server(data_dir, |address| {
        verify_database(address, &second.committed);
    });

    assert!(
        second.committed.len() >= 128,
        "insufficient commit progress"
    );
    assert!(
        second.rolled_back >= 64,
        "rollback branch was not exercised"
    );
    assert!(
        second.disconnected >= 64,
        "active-transaction disconnect branch was not exercised"
    );
    assert!(
        second.conflicts < WRITERS * TRANSACTIONS_PER_WRITER,
        "contention made no durable progress"
    );
    historical_mixed_epoch_oracle(second.max_missing_outbox, second.max_orphan_sync)
        .expect("historical mixed-epoch oracle rejected the run");
    println!(
        "messenger chaos: committed={} rolled_back={} disconnected={} conflicts={} \
         transaction_connections={} reader_connections={} raw_disconnects={} checkpoints={} \
         checkpoint_busy={}",
        second.committed.len(),
        second.rolled_back,
        second.disconnected,
        second.conflicts,
        WRITERS * TRANSACTIONS_PER_WRITER * 2,
        READERS * READS_PER_READER * 2,
        CHURN_THREADS * CHURN_CONNECTIONS_PER_THREAD * 2,
        CHECKPOINTS_PER_PHASE * 2 + 2,
        second.checkpoint_busy,
    );
    HistoricalChaosSummary {
        committed: second.committed.len(),
        rolled_back: second.rolled_back,
        disconnected: second.disconnected,
        conflicts: second.conflicts,
        checkpoint_busy: second.checkpoint_busy,
    }
}

#[cfg(feature = "test-mutations")]
pub fn prove_historical_mixed_epoch_oracle_rejects_disabled_fence() {
    use radixdb::test_mutations::{JoinBetweenSourcesPause, StatementVisibilityFenceMutation};

    let temp = tempfile::tempdir().expect("mutation fixture tempdir");
    let data_dir = temp.path().join("data");
    with_server(data_dir, |address| {
        create_schema(address);

        let mut seed = connect(address).expect("connect mutation seed");
        seed.begin().expect("begin mutation seed");
        command(
            &mut seed,
            "INSERT INTO messages VALUES (9000001, 1, 1, 1, 1, 'mutation-seed', 'seed', 1)",
        )
        .expect("insert mutation message");
        command(
            &mut seed,
            "INSERT INTO outbox_jobs VALUES (9000001, 9000001, 'pending', 0, 1)",
        )
        .expect("insert mutation outbox");
        seed.commit().expect("commit mutation seed");

        let _disabled = StatementVisibilityFenceMutation::disable()
            .expect("disable statement visibility fence mutation");
        let pause = JoinBetweenSourcesPause::install().expect("install join pause mutation");
        let query = thread::spawn(move || {
            let mut reader = connect(address).expect("connect mutation reader");
            scalar_i64(
                &mut reader,
                "SELECT COUNT(m.id)
                 FROM messages m LEFT JOIN outbox_jobs o ON o.message_id = m.id
                 WHERE o.id IS NULL",
            )
            .expect("execute mutation reader")
        });

        pause
            .wait_until_reached(Duration::from_secs(5))
            .expect("reader paused between join sources");
        let mut writer = connect(address).expect("connect mutation writer");
        writer.begin().expect("begin mutation delete");
        command(&mut writer, "DELETE FROM outbox_jobs WHERE id = 9000001")
            .expect("delete mutation outbox");
        command(&mut writer, "DELETE FROM messages WHERE id = 9000001")
            .expect("delete mutation message");
        writer.commit().expect("commit mutation delete");
        pause.release();

        let missing_outbox = query.join().expect("mutation reader joins");
        assert_eq!(pause.hit_count(), 1, "mutation hook was not exact-once");
        let error = historical_mixed_epoch_oracle(missing_outbox, 0)
            .expect_err("disabled fence did not trigger the historical oracle");
        assert!(error.contains("without same-transaction outbox"));

        let mut final_reader = connect(address).expect("connect final mutation reader");
        assert_eq!(
            scalar_i64(
                &mut final_reader,
                "SELECT COUNT(m.id)
                 FROM messages m LEFT JOIN outbox_jobs o ON o.message_id = m.id
                 WHERE o.id IS NULL",
            )
            .expect("verify final consistent state"),
            0,
            "mutation fixture ended in a genuinely inconsistent durable state"
        );
    });
}

/// Execute the real mixed-epoch invariant with the normal statement fence.
/// The B10 runner selects the test-only visibility mutation through the
/// process environment; with that mutation active the final `expect` below is
/// the single designated red oracle.
#[cfg(feature = "test-mutations")]
pub fn run_historical_mixed_epoch_invariant() {
    use radixdb::test_mutations::JoinBetweenSourcesPause;
    use std::sync::mpsc;

    let temp = tempfile::tempdir().expect("B10 visibility fixture tempdir");
    let data_dir = temp.path().join("data");
    with_server(data_dir, |address| {
        create_schema(address);

        let mut seed = connect(address).expect("connect B10 visibility seed");
        seed.begin().expect("begin B10 visibility seed");
        command(
            &mut seed,
            "INSERT INTO messages VALUES (9000010, 1, 1, 1, 1, 'b10-seed', 'seed', 1)",
        )
        .expect("insert B10 visibility message");
        command(
            &mut seed,
            "INSERT INTO outbox_jobs VALUES (9000010, 9000010, 'pending', 0, 1)",
        )
        .expect("insert B10 visibility outbox");
        seed.commit().expect("commit B10 visibility seed");

        let pause = JoinBetweenSourcesPause::install().expect("install B10 join pause");
        let query = thread::spawn(move || {
            let mut reader = connect(address).expect("connect B10 visibility reader");
            scalar_i64(
                &mut reader,
                "SELECT COUNT(m.id)
                 FROM messages m LEFT JOIN outbox_jobs o ON o.message_id = m.id
                 WHERE o.id IS NULL",
            )
            .expect("execute B10 visibility reader")
        });

        pause
            .wait_until_reached(Duration::from_secs(5))
            .expect("B10 reader paused between join sources");

        // Replace one complete parent/child pair with another complete pair.
        // A mixed epoch then reports one orphan whether the physical join
        // reads outer→inner or builds the inner key set before scanning outer.
        let (committed_tx, committed_rx) = mpsc::sync_channel(1);
        let writer = thread::spawn(move || {
            let mut writer = connect(address).expect("connect B10 visibility writer");
            writer.begin().expect("begin B10 visibility epoch swap");
            command(&mut writer, "DELETE FROM outbox_jobs WHERE id = 9000010")
                .expect("delete B10 visibility outbox");
            command(&mut writer, "DELETE FROM messages WHERE id = 9000010")
                .expect("delete B10 visibility message");
            command(
                &mut writer,
                "INSERT INTO messages VALUES (9000011, 1, 1, 1, 1, 'b10-next', 'next', 1)",
            )
            .expect("insert next B10 visibility message");
            command(
                &mut writer,
                "INSERT INTO outbox_jobs VALUES (9000011, 9000011, 'pending', 0, 1)",
            )
            .expect("insert next B10 visibility outbox");
            writer.commit().expect("commit B10 visibility epoch swap");
            let _ = committed_tx.send(());
        });

        // With the real fence the writer cannot cross COMMIT while the reader
        // is paused between sources. The selected mutation makes it cross.
        let committed_before_release = committed_rx
            .recv_timeout(Duration::from_millis(250))
            .is_ok();
        pause.release();

        let missing_outbox = query.join().expect("B10 visibility reader joins");
        writer.join().expect("B10 visibility writer joins");
        assert_eq!(pause.hit_count(), 1, "B10 join pause was not exact-once");
        if committed_before_release {
            assert_eq!(
                missing_outbox, 1,
                "visibility mutation crossed the fence without exposing the expected mixed epoch"
            );
        }
        historical_mixed_epoch_oracle(missing_outbox, 0)
            .expect("PRV-B10 invariant visibility_fence: one SELECT observed two committed epochs");
    });
}
