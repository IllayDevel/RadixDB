use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    net::SocketAddr,
    path::Path,
    time::Duration,
};

use serde::{Deserialize, Serialize};

use super::{
    config::{ChaosProfile, DATABASE},
    server::ServerSupervisor,
};
use crate::common::prerelease::{
    messenger_schema_plan, messenger_small_seed_plan, messenger_view_plan, tcp_command,
    tcp_connect_with_read_timeout, tcp_scalar_i64,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FixtureEvidence {
    pub cold_rows: u64,
    pub cold_copy_chunks: u64,
    pub hot_rows: u64,
    pub cold_messenger_bundles: u64,
    pub hot_messenger_bundles: u64,
    pub checksum: i64,
}

pub fn initialize(
    supervisor: &mut ServerSupervisor<'_>,
    run_root: &Path,
    profile: &ChaosProfile,
) -> Result<FixtureEvidence, String> {
    let address = supervisor.address()?;
    let mut connection = tcp_connect_with_read_timeout(address, DATABASE, profile.stage_timeout)?;
    for statement in messenger_schema_plan().statements {
        tcp_command(&mut connection, statement.sql)
            .map_err(|error| format!("fixture schema {}: {error}", statement.id))?;
    }
    for statement in messenger_small_seed_plan().statements {
        tcp_command(&mut connection, statement.sql)
            .map_err(|error| format!("fixture seed {}: {error}", statement.id))?;
    }
    for sql in [
        "CREATE TABLE chaos_cells (id INTEGER PRIMARY KEY, owner_id INTEGER NOT NULL REFERENCES users(id), partition_id INTEGER NOT NULL, version INTEGER NOT NULL CHECK (version >= 0), payload TEXT NOT NULL, live BOOLEAN NOT NULL)",
        "CREATE INDEX chaos_cells_partition_idx ON chaos_cells (partition_id, id)",
        "CREATE TABLE wide_cells (id INTEGER PRIMARY KEY, owner_id INTEGER NOT NULL REFERENCES users(id), version INTEGER NOT NULL, payload TEXT NOT NULL)",
        "CREATE TABLE large_cells (id INTEGER PRIMARY KEY, actor_id INTEGER NOT NULL, version INTEGER NOT NULL, payload TEXT NOT NULL, unique_key TEXT NOT NULL UNIQUE)",
        "CREATE INDEX large_cells_actor_idx ON large_cells (actor_id, id)",
        "CREATE TABLE large_scratch (id INTEGER PRIMARY KEY, actor_id INTEGER NOT NULL, value INTEGER NOT NULL)",
        "CREATE TABLE micro_cells (id INTEGER PRIMARY KEY, version INTEGER NOT NULL, checksum INTEGER NOT NULL)",
        "CREATE TABLE random_cells (id INTEGER PRIMARY KEY, owner_id INTEGER NOT NULL REFERENCES users(id), version INTEGER NOT NULL, payload TEXT NOT NULL)",
        "CREATE INDEX random_cells_owner_idx ON random_cells (owner_id, id)",
        "CREATE TABLE crash_ledger (id INTEGER PRIMARY KEY, layer TEXT NOT NULL, value INTEGER NOT NULL)",
    ] {
        tcp_command(&mut connection, sql)?;
    }

    let cold_copy_chunks = copy_cold_cells(&mut connection, run_root, profile.cold_rows)?;

    let cold_messenger_bundles = (profile.cold_rows / 100).clamp(50, 10_000);
    append_messenger_bundles(&mut connection, 0, cold_messenger_bundles)?;
    tcp_command(&mut connection, "PRAGMA CHECKPOINT")?;

    append_hot_cells(&mut connection, profile.cold_rows, profile.hot_rows)?;
    let hot_messenger_bundles = (profile.hot_rows / 10).clamp(50, 2_500);
    append_messenger_bundles(
        &mut connection,
        cold_messenger_bundles,
        hot_messenger_bundles,
    )?;
    for statement in messenger_view_plan().statements {
        tcp_command(&mut connection, statement.sql)
            .map_err(|error| format!("fixture view {}: {error}", statement.id))?;
    }

    let expected_rows = profile.cold_rows + profile.hot_rows;
    let actual_rows = tcp_scalar_i64(&mut connection, "SELECT COUNT(*) FROM chaos_cells")?;
    if actual_rows != expected_rows as i64 {
        return Err(format!(
            "chaos fixture row count differs: expected {expected_rows}, got {actual_rows}"
        ));
    }
    let checksum = tcp_scalar_i64(
        &mut connection,
        "SELECT SUM(id + version + partition_id) FROM chaos_cells",
    )?;
    Ok(FixtureEvidence {
        cold_rows: profile.cold_rows,
        cold_copy_chunks,
        hot_rows: profile.hot_rows,
        cold_messenger_bundles,
        hot_messenger_bundles,
        checksum,
    })
}

fn copy_cold_cells(
    connection: &mut radixdb_client::Connection,
    run_root: &Path,
    rows: u64,
) -> Result<u64, String> {
    const COPY_ROWS: u64 = 250_000;
    let mut chunks = 0;
    for first in (1..=rows).step_by(COPY_ROWS as usize) {
        let last = first.saturating_add(COPY_ROWS - 1).min(rows);
        let csv = run_root.join(format!("cold-chaos-cells-{chunks:04}.csv"));
        write_cold_csv(&csv, first, last)?;
        let escaped = csv
            .to_str()
            .ok_or_else(|| "cold fixture path is not UTF-8".to_string())?;
        if escaped.contains('\'') {
            return Err("cold fixture path contains an SQL quote".to_string());
        }
        tcp_command(
            connection,
            format!("COPY chaos_cells FROM '{escaped}' WITH (FORMAT CSV, HEADER true)"),
        )?;
        fs::remove_file(&csv).map_err(|error| error.to_string())?;
        chunks += 1;
    }
    Ok(chunks)
}

fn write_cold_csv(path: &Path, first: u64, last: u64) -> Result<(), String> {
    let file = File::create(path).map_err(|error| error.to_string())?;
    let mut writer = BufWriter::with_capacity(4 * 1024 * 1024, file);
    writer
        .write_all(b"id,owner_id,partition_id,version,payload,live\n")
        .map_err(|error| error.to_string())?;
    for id in first..=last {
        writeln!(writer, "{id},1,{},0,cold-{id},true", id % 257)
            .map_err(|error| error.to_string())?;
    }
    writer.flush().map_err(|error| error.to_string())?;
    writer
        .get_ref()
        .sync_all()
        .map_err(|error| error.to_string())
}

fn append_hot_cells(
    connection: &mut radixdb_client::Connection,
    cold_rows: u64,
    hot_rows: u64,
) -> Result<(), String> {
    for chunk_start in (0..hot_rows).step_by(500) {
        let chunk_end = (chunk_start + 500).min(hot_rows);
        let values = (chunk_start..chunk_end)
            .map(|offset| {
                let id = cold_rows + offset + 1;
                format!("({id}, 1, {}, 0, 'hot-{id}', true)", id % 257)
            })
            .collect::<Vec<_>>()
            .join(",");
        tcp_command(
            connection,
            format!("INSERT INTO chaos_cells VALUES {values}"),
        )?;
    }
    Ok(())
}

fn append_messenger_bundles(
    connection: &mut radixdb_client::Connection,
    offset: u64,
    bundles: u64,
) -> Result<(), String> {
    for chunk_start in (0..bundles).step_by(100) {
        let chunk_end = (chunk_start + 100).min(bundles);
        let mut messages = Vec::new();
        let mut outbox = Vec::new();
        let mut sync = Vec::new();
        let mut commands = Vec::new();
        for local in chunk_start..chunk_end {
            let ordinal = offset + local + 1;
            let message_id = 10_000_000 + ordinal as i64;
            let outbox_id = 20_000_000 + ordinal as i64;
            let sync_id = 30_000_000 + ordinal as i64;
            let command_id = 40_000_000 + ordinal as i64;
            let sequence = 10_000 + ordinal as i64;
            messages.push(format!(
                "({message_id}, 10, {sequence}, 1, 'fixture-{ordinal}', false)"
            ));
            outbox.push(format!(
                "({outbox_id}, {message_id}, 'pending', 0, NULL, true)"
            ));
            sync.push(format!(
                "({sync_id}, 2, {sequence}, {message_id}, {outbox_id}, 'message.created')"
            ));
            commands.push(format!(
                "({command_id}, 1, 'fixture-command-{ordinal}', {message_id})"
            ));
        }
        for (table, values) in [
            ("messages", messages),
            ("outbox_jobs", outbox),
            ("sync_events", sync),
            ("command_results", commands),
        ] {
            tcp_command(
                connection,
                format!("INSERT INTO {table} VALUES {}", values.join(",")),
            )?;
        }
    }
    Ok(())
}

pub fn address(supervisor: &ServerSupervisor<'_>) -> Result<SocketAddr, String> {
    supervisor.address()
}
