#![cfg(feature = "stress-tests")]

use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{BufWriter, Write},
    net::SocketAddr,
    ops::Range,
    path::Path,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    messenger_schema_plan, messenger_view_plan, tcp_command, tcp_connect_with_read_timeout,
    tcp_scalar_i64, MessengerFixtureScale, MessengerSeedPlan, MessengerSeedTable,
    MessengerStorageTier, OwnedFixture, CONCURRENCY_DATABASE,
};

/// Keep fixture creation representative without making one transaction own a
/// material fraction of the host memory.  Transaction-size/atomicity stress is
/// covered independently by B5.
const LARGE_COPY_CHUNK_ROWS: u64 = 250_000;
const LARGE_COLD_CHECKPOINT_CHUNKS: u64 = 4;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LargeFixtureTableEvidence {
    pub table: MessengerSeedTable,
    pub cold_rows: u64,
    pub hot_rows: u64,
    pub verified_rows: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LargeFixtureEvidence {
    pub seed: u64,
    pub total_rows: u64,
    pub cold_rows: u64,
    pub hot_rows: u64,
    pub copy_chunks: u64,
    pub max_copy_chunk_rows: u64,
    pub cold_checkpoints: u64,
    pub logical_sha256: String,
    pub tables: Vec<LargeFixtureTableEvidence>,
}

impl LargeFixtureEvidence {
    pub fn validate(&self) -> Result<(), String> {
        if self.total_rows < 100_000_000 {
            return Err("large fixture contains fewer than 100 million rows".to_string());
        }
        if self.logical_sha256.len() != 64
            || !self
                .logical_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err("large fixture logical checksum is invalid".to_string());
        }
        if self.tables.len() != MessengerSeedTable::ALL.len() {
            return Err("large fixture does not cover every messenger table".to_string());
        }
        if self.copy_chunks == 0
            || self.max_copy_chunk_rows == 0
            || self.max_copy_chunk_rows > LARGE_COPY_CHUNK_ROWS
            || self.cold_checkpoints == 0
        {
            return Err("large fixture COPY chunk accounting is invalid".to_string());
        }
        let actual_tables: BTreeSet<_> = self.tables.iter().map(|table| table.table).collect();
        let expected_tables: BTreeSet<_> = MessengerSeedTable::ALL.into_iter().collect();
        if actual_tables != expected_tables {
            return Err("large fixture table identities are incomplete or duplicated".to_string());
        }
        if self
            .tables
            .iter()
            .any(|table| table.cold_rows.saturating_add(table.hot_rows) != table.verified_rows)
        {
            return Err("large fixture per-table row accounting is inconsistent".to_string());
        }
        let cold = self.tables.iter().map(|table| table.cold_rows).sum::<u64>();
        let hot = self.tables.iter().map(|table| table.hot_rows).sum::<u64>();
        let verified = self
            .tables
            .iter()
            .map(|table| table.verified_rows)
            .sum::<u64>();
        if cold != self.cold_rows
            || hot != self.hot_rows
            || cold.saturating_add(hot) != self.total_rows
            || verified != self.total_rows
        {
            return Err("large fixture row accounting is inconsistent".to_string());
        }
        Ok(())
    }
}

/// Materialize the dedicated prerelease messenger fixture.
///
/// The accepted 100M benchmark database is never opened or modified.  CSV
/// staging files live under the caller-owned temporary fixture, one bounded
/// COPY transaction at a time.  The cold 90% of every table is loaded first
/// and checkpointed; the hot tail is loaded afterwards so the final database
/// deliberately contains a mixed hot/cold topology.
pub fn materialize_large_concurrency_fixture(
    address: SocketAddr,
    fixture: &OwnedFixture,
    seed: u64,
) -> Result<LargeFixtureEvidence, String> {
    let plan = MessengerSeedPlan::new(MessengerFixtureScale::Large, seed);
    let evidence = materialize_concurrency_fixture(address, fixture, &plan)?;
    evidence.validate()?;
    Ok(evidence)
}

fn materialize_concurrency_fixture(
    address: SocketAddr,
    fixture: &OwnedFixture,
    plan: &MessengerSeedPlan,
) -> Result<LargeFixtureEvidence, String> {
    let import_root = fixture.child("large-import")?;
    fs::create_dir_all(&import_root).map_err(|error| error.to_string())?;

    let mut connection = tcp_connect_with_read_timeout(
        address,
        CONCURRENCY_DATABASE,
        Duration::from_secs(2 * 60 * 60),
    )?;
    for statement in messenger_schema_plan().statements {
        tcp_command(&mut connection, statement.sql)
            .map_err(|error| format!("large fixture schema {}: {error}", statement.id))?;
    }

    let mut logical = Sha256::new();
    let mut copy_chunks = 0_u64;
    let mut max_copy_chunk_rows = 0_u64;
    let mut cold_checkpoints = 0_u64;
    let mut cold_chunks_since_checkpoint = 0_u64;
    for tier in [MessengerStorageTier::Cold, MessengerStorageTier::Hot] {
        for table in MessengerSeedTable::ALL {
            for (chunk_index, range) in table_tier_chunks(plan, table, tier).into_iter().enumerate()
            {
                let path = import_root.join(format!(
                    "{}-{}-{chunk_index:04}.csv",
                    table.name(),
                    match tier {
                        MessengerStorageTier::Cold => "cold",
                        MessengerStorageTier::Hot => "hot",
                    }
                ));
                let chunk_rows = range.end - range.start;
                write_table_csv_range(&path, plan, table, range, &mut logical, chunk_index == 0)?;
                let path_text = path
                    .to_str()
                    .ok_or_else(|| "large fixture path is not valid UTF-8".to_string())?;
                if path_text.contains('\'') {
                    return Err("large fixture path contains an SQL quote".to_string());
                }
                tcp_command(
                    &mut connection,
                    format!(
                        "COPY {} FROM '{}' WITH (FORMAT CSV, HEADER true)",
                        table.name(),
                        path_text
                    ),
                )
                .map_err(|error| {
                    format!(
                        "large fixture COPY {} {:?} chunk {chunk_index} failed: {error}; input preserved at {}",
                        table.name(),
                        tier,
                        path.display()
                    )
                })?;
                fs::remove_file(&path).map_err(|error| error.to_string())?;
                copy_chunks += 1;
                max_copy_chunk_rows = max_copy_chunk_rows.max(chunk_rows);
                if tier == MessengerStorageTier::Cold {
                    cold_chunks_since_checkpoint += 1;
                    if cold_chunks_since_checkpoint >= LARGE_COLD_CHECKPOINT_CHUNKS {
                        tcp_command(&mut connection, "PRAGMA CHECKPOINT")?;
                        cold_checkpoints += 1;
                        cold_chunks_since_checkpoint = 0;
                    }
                }
            }
        }
        if tier == MessengerStorageTier::Cold {
            tcp_command(&mut connection, "PRAGMA CHECKPOINT")?;
            cold_checkpoints += 1;
            cold_chunks_since_checkpoint = 0;
        }
    }

    for statement in messenger_view_plan().statements {
        tcp_command(&mut connection, statement.sql)
            .map_err(|error| format!("large fixture view {}: {error}", statement.id))?;
    }

    let mut tables = Vec::with_capacity(MessengerSeedTable::ALL.len());
    for table in MessengerSeedTable::ALL {
        let verified_rows = u64::try_from(tcp_scalar_i64(
            &mut connection,
            &format!("SELECT COUNT(*) FROM {}", table.name()),
        )?)
        .map_err(|_| format!("{} returned a negative row count", table.name()))?;
        let expected = plan.row_count(table);
        if verified_rows != expected {
            return Err(format!(
                "{} row count differs after materialization: expected {expected}, got {verified_rows}",
                table.name()
            ));
        }
        tables.push(LargeFixtureTableEvidence {
            table,
            cold_rows: plan.cold_rows(table),
            hot_rows: plan.hot_rows(table),
            verified_rows,
        });
    }

    let evidence = LargeFixtureEvidence {
        seed: plan.seed,
        total_rows: plan.total_rows(),
        cold_rows: tables.iter().map(|table| table.cold_rows).sum(),
        hot_rows: tables.iter().map(|table| table.hot_rows).sum(),
        copy_chunks,
        max_copy_chunk_rows,
        cold_checkpoints,
        logical_sha256: format!("{:x}", logical.finalize()),
        tables,
    };
    Ok(evidence)
}

fn table_tier_chunks(
    plan: &MessengerSeedPlan,
    table: MessengerSeedTable,
    tier: MessengerStorageTier,
) -> Vec<Range<u64>> {
    let cold_end = plan.cold_rows(table);
    let range = match tier {
        MessengerStorageTier::Cold => 0..cold_end,
        MessengerStorageTier::Hot => cold_end..plan.row_count(table),
    };
    bounded_ranges(range, LARGE_COPY_CHUNK_ROWS)
}

fn bounded_ranges(range: Range<u64>, max_rows: u64) -> Vec<Range<u64>> {
    assert!(max_rows > 0, "COPY chunk size must be positive");
    let mut chunks = Vec::new();
    let mut start = range.start;
    while start < range.end {
        let end = start.saturating_add(max_rows).min(range.end);
        chunks.push(start..end);
        start = end;
    }
    chunks
}

fn write_table_csv_range(
    path: &Path,
    plan: &MessengerSeedPlan,
    table: MessengerSeedTable,
    range: Range<u64>,
    logical: &mut Sha256,
    hash_header: bool,
) -> Result<(), String> {
    let file = File::create(path).map_err(|error| error.to_string())?;
    let mut writer = BufWriter::with_capacity(4 * 1024 * 1024, file);
    if hash_header {
        write_hashed_line(&mut writer, logical, table_header(table))?;
    } else {
        write_line(&mut writer, table_header(table))?;
    }
    for ordinal in range {
        let row = render_table_row(plan, table, ordinal)?;
        write_hashed_line(&mut writer, logical, &row)?;
    }
    writer.flush().map_err(|error| error.to_string())?;
    writer
        .get_ref()
        .sync_all()
        .map_err(|error| error.to_string())
}

fn write_line(writer: &mut BufWriter<File>, line: &str) -> Result<(), String> {
    writer
        .write_all(line.as_bytes())
        .and_then(|()| writer.write_all(b"\n"))
        .map_err(|error| error.to_string())
}

fn write_hashed_line(
    writer: &mut BufWriter<File>,
    logical: &mut Sha256,
    line: &str,
) -> Result<(), String> {
    logical.update(line.as_bytes());
    logical.update(b"\n");
    writer
        .write_all(line.as_bytes())
        .and_then(|()| writer.write_all(b"\n"))
        .map_err(|error| error.to_string())
}

fn table_header(table: MessengerSeedTable) -> &'static str {
    match table {
        MessengerSeedTable::Tenants => "id,slug,active",
        MessengerSeedTable::Users => "id,tenant_id,username,display_name,revision,disabled",
        MessengerSeedTable::Devices => "id,user_id,device_key,platform",
        MessengerSeedTable::Sessions => "id,user_id,device_id,active,revision",
        MessengerSeedTable::Conversations => "id,tenant_id,created_by,kind,title,active",
        MessengerSeedTable::ConversationMembers => {
            "id,conversation_id,user_id,role,last_read_seq,muted"
        }
        MessengerSeedTable::Messages => "id,conversation_id,sequence,sender_id,body,deleted",
        MessengerSeedTable::MessageVersions => "id,message_id,revision,body",
        MessengerSeedTable::ForwardedMessages => {
            "id,message_id,source_message_id,source_conversation_id"
        }
        MessengerSeedTable::Attachments => "id,message_id,storage_key,byte_size",
        MessengerSeedTable::ForwardedAttachmentAccess => {
            "id,forward_id,attachment_id,user_id,access_kind"
        }
        MessengerSeedTable::Reactions => "id,message_id,user_id,emoji",
        MessengerSeedTable::Receipts => "id,message_id,user_id,delivered_seq,read_seq",
        MessengerSeedTable::OutboxJobs => "id,message_id,state,retry_count,lease_owner,visible",
        MessengerSeedTable::SyncEvents => "id,user_id,sequence,message_id,outbox_job_id,event_type",
        MessengerSeedTable::CommandResults => "id,user_id,idempotency_key,message_id",
        MessengerSeedTable::RefreshTokens => "id,user_id,token_hash,revoked",
        MessengerSeedTable::AuditLog => "id,tenant_id,user_id,entity_kind,entity_id,action",
    }
}

fn render_table_row(
    plan: &MessengerSeedPlan,
    table: MessengerSeedTable,
    ordinal: u64,
) -> Result<String, String> {
    if ordinal >= plan.row_count(table) {
        return Err(format!(
            "{} ordinal {ordinal} exceeds its seed plan",
            table.name()
        ));
    }
    let id = ordinal + 1;
    let tenants = plan.cold_rows(MessengerSeedTable::Tenants).max(1);
    let users = plan.cold_rows(MessengerSeedTable::Users).max(1);
    let devices = plan.cold_rows(MessengerSeedTable::Devices).max(1);
    let conversations = plan.cold_rows(MessengerSeedTable::Conversations).max(1);
    let tenant = ordinal % tenants + 1;
    let user = ordinal % users + 1;
    let conversation = ordinal % conversations + 1;

    Ok(match table {
        MessengerSeedTable::Tenants => format!("{id},tenant-{id},true"),
        MessengerSeedTable::Users => {
            format!("{id},{tenant},user-{id},User-{id},1,false")
        }
        MessengerSeedTable::Devices => {
            format!("{id},{user},device-{id},desktop")
        }
        MessengerSeedTable::Sessions => {
            let device = ordinal % devices + 1;
            format!("{id},{user},{device},true,1")
        }
        MessengerSeedTable::Conversations => {
            format!("{id},{tenant},{user},group,Conversation-{id},true")
        }
        MessengerSeedTable::ConversationMembers => {
            let member_user = ordinal / conversations + 1;
            if member_user > plan.users {
                return Err("conversation member seed exhausted unique pair space".to_string());
            }
            format!("{id},{conversation},{member_user},member,0,false")
        }
        MessengerSeedTable::Messages => {
            let sequence = ordinal / conversations + 1;
            format!("{id},{conversation},{sequence},{user},message-{id},false")
        }
        MessengerSeedTable::MessageVersions => format!("{id},{id},1,message-version-{id}"),
        MessengerSeedTable::ForwardedMessages => {
            let source = id % plan.messages + 1;
            format!("{id},{id},{source},{conversation}")
        }
        MessengerSeedTable::Attachments => format!("{id},{id},blob-{id},1024"),
        MessengerSeedTable::ForwardedAttachmentAccess => {
            format!("{id},{id},{id},{user},read")
        }
        MessengerSeedTable::Reactions => format!("{id},{id},{user},stress"),
        MessengerSeedTable::Receipts => format!("{id},{id},{user},{id},{id}"),
        MessengerSeedTable::OutboxJobs => {
            format!("{id},{id},pending,0,worker-{id},true")
        }
        MessengerSeedTable::SyncEvents => {
            let sequence = ordinal / users + 1;
            let sync_message = ordinal % plan.messages + 1;
            let outbox_job = ordinal % plan.outbox_jobs + 1;
            format!("{id},{user},{sequence},{sync_message},{outbox_job},message.created")
        }
        MessengerSeedTable::CommandResults => {
            format!("{id},{user},command-{id},{id}")
        }
        MessengerSeedTable::RefreshTokens => {
            format!("{id},{user},token-{id},false")
        }
        MessengerSeedTable::AuditLog => {
            format!("{id},{tenant},{user},message,{id},insert")
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_materializer_smoke_loads_every_table_in_cold_and_hot_phases() {
        let fixture = OwnedFixture::new("radixdb-prerelease-large-smoke-").unwrap();
        let data_dir = fixture.child("server-data").unwrap();
        let plan = MessengerSeedPlan::new(MessengerFixtureScale::Small, 0x5eed_4004);
        let evidence = super::super::with_tcp_server(data_dir, 8, |address| {
            materialize_concurrency_fixture(address, &fixture, &plan).unwrap()
        });
        assert_eq!(evidence.total_rows, plan.total_rows());
        assert_eq!(evidence.tables.len(), MessengerSeedTable::ALL.len());
        assert_eq!(
            evidence
                .tables
                .iter()
                .map(|table| table.verified_rows)
                .sum::<u64>(),
            plan.total_rows()
        );
        assert!(evidence.cold_rows > 0);
        assert!(evidence.hot_rows > 0);
        assert!(evidence.copy_chunks >= MessengerSeedTable::ALL.len() as u64);
        assert!(evidence.max_copy_chunk_rows <= LARGE_COPY_CHUNK_ROWS);
        assert!(evidence.cold_checkpoints > 0);
        assert_ne!(evidence.logical_sha256, "0".repeat(64));
    }

    #[test]
    fn large_fixture_copy_chunks_are_bounded_gap_free_and_exact() {
        let plan = MessengerSeedPlan::new(MessengerFixtureScale::Large, 0x5eed_4004);
        let mut covered = 0_u64;
        for table in MessengerSeedTable::ALL {
            let mut next = 0_u64;
            for tier in [MessengerStorageTier::Cold, MessengerStorageTier::Hot] {
                for range in table_tier_chunks(&plan, table, tier) {
                    assert_eq!(range.start, next, "{} has a COPY gap", table.name());
                    assert!(range.end > range.start);
                    assert!(range.end - range.start <= LARGE_COPY_CHUNK_ROWS);
                    covered += range.end - range.start;
                    next = range.end;
                }
            }
            assert_eq!(
                next,
                plan.row_count(table),
                "{} is incomplete",
                table.name()
            );
        }
        assert_eq!(covered, 100_000_128);
    }

    #[test]
    fn large_fixture_renderer_covers_all_tables_and_both_storage_tiers() {
        let plan = MessengerSeedPlan::new(MessengerFixtureScale::Large, 0x5eed_4004);
        assert_eq!(plan.total_rows(), 100_000_128);
        for table in MessengerSeedTable::ALL {
            let cold_last = plan.cold_rows(table) - 1;
            let hot_first = plan.cold_rows(table);
            let final_row = plan.row_count(table) - 1;
            for ordinal in [0, cold_last, hot_first, final_row] {
                let row = render_table_row(&plan, table, ordinal).unwrap();
                assert!(!row.is_empty(), "{} ordinal {ordinal}", table.name());
                assert_eq!(
                    row.split(',').count(),
                    table_header(table).split(',').count(),
                    "{} ordinal {ordinal}",
                    table.name()
                );
            }
        }
    }

    #[test]
    fn large_fixture_evidence_rejects_incomplete_accounting() {
        let mut tables = Vec::new();
        for table in MessengerSeedTable::ALL {
            tables.push(LargeFixtureTableEvidence {
                table,
                cold_rows: 9,
                hot_rows: 1,
                verified_rows: 10,
            });
        }
        let evidence = LargeFixtureEvidence {
            seed: 1,
            total_rows: 180,
            cold_rows: 162,
            hot_rows: 18,
            copy_chunks: 36,
            max_copy_chunk_rows: 9,
            cold_checkpoints: 1,
            logical_sha256: "0".repeat(64),
            tables,
        };
        assert!(evidence.validate().is_err());
    }
}
