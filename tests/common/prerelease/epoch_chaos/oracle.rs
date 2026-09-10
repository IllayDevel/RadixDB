use std::{net::SocketAddr, time::Duration};

use serde::{Deserialize, Serialize};

use crate::common::prerelease::{tcp_connect_with_read_timeout, tcp_scalar_i64};

use super::config::DATABASE;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct OracleSnapshot {
    pub users: i64,
    pub conversations: i64,
    pub messages: i64,
    pub outbox: i64,
    pub sync_events: i64,
    pub reactions: i64,
    pub receipts: i64,
    pub command_results: i64,
    pub chaos_cells: i64,
    pub crash_ledger: i64,
    pub orphan_messages: i64,
    pub orphan_outbox: i64,
    pub orphan_sync_messages: i64,
    pub orphan_sync_outbox: i64,
    pub orphan_reactions: i64,
    pub orphan_receipts: i64,
    pub orphan_commands: i64,
}

impl OracleSnapshot {
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("orphan_messages", self.orphan_messages),
            ("orphan_outbox", self.orphan_outbox),
            ("orphan_sync_messages", self.orphan_sync_messages),
            ("orphan_sync_outbox", self.orphan_sync_outbox),
            ("orphan_reactions", self.orphan_reactions),
            ("orphan_receipts", self.orphan_receipts),
            ("orphan_commands", self.orphan_commands),
        ] {
            if value != 0 {
                return Err(format!("cross-table oracle {name} returned {value}"));
            }
        }
        if self.users <= 0 || self.conversations <= 0 || self.chaos_cells <= 0 {
            return Err(format!("fixture authority disappeared: {self:?}"));
        }
        Ok(())
    }
}

pub fn capture(address: SocketAddr, timeout: Duration) -> Result<OracleSnapshot, String> {
    let mut connection = tcp_connect_with_read_timeout(address, DATABASE, timeout)?;
    let snapshot = OracleSnapshot {
        users: scalar(&mut connection, "SELECT COUNT(*) FROM users")?,
        conversations: scalar(&mut connection, "SELECT COUNT(*) FROM conversations")?,
        messages: scalar(&mut connection, "SELECT COUNT(*) FROM messages")?,
        outbox: scalar(&mut connection, "SELECT COUNT(*) FROM outbox_jobs")?,
        sync_events: scalar(&mut connection, "SELECT COUNT(*) FROM sync_events")?,
        reactions: scalar(&mut connection, "SELECT COUNT(*) FROM reactions")?,
        receipts: scalar(&mut connection, "SELECT COUNT(*) FROM receipts")?,
        command_results: scalar(&mut connection, "SELECT COUNT(*) FROM command_results")?,
        chaos_cells: scalar(&mut connection, "SELECT COUNT(*) FROM chaos_cells")?,
        crash_ledger: scalar(&mut connection, "SELECT COUNT(*) FROM crash_ledger")?,
        orphan_messages: scalar(
            &mut connection,
            "SELECT COUNT(*) FROM messages m LEFT JOIN conversations c ON c.id = m.conversation_id LEFT JOIN users u ON u.id = m.sender_id WHERE c.id IS NULL OR u.id IS NULL",
        )?,
        orphan_outbox: scalar(
            &mut connection,
            "SELECT COUNT(*) FROM outbox_jobs o LEFT JOIN messages m ON m.id = o.message_id WHERE m.id IS NULL",
        )?,
        orphan_sync_messages: scalar(
            &mut connection,
            "SELECT COUNT(*) FROM sync_events s LEFT JOIN messages m ON m.id = s.message_id WHERE s.message_id IS NOT NULL AND m.id IS NULL",
        )?,
        orphan_sync_outbox: scalar(
            &mut connection,
            "SELECT COUNT(*) FROM sync_events s LEFT JOIN outbox_jobs o ON o.id = s.outbox_job_id WHERE s.outbox_job_id IS NOT NULL AND o.id IS NULL",
        )?,
        orphan_reactions: scalar(
            &mut connection,
            "SELECT COUNT(*) FROM reactions r LEFT JOIN messages m ON m.id = r.message_id LEFT JOIN users u ON u.id = r.user_id WHERE m.id IS NULL OR u.id IS NULL",
        )?,
        orphan_receipts: scalar(
            &mut connection,
            "SELECT COUNT(*) FROM receipts r LEFT JOIN messages m ON m.id = r.message_id LEFT JOIN users u ON u.id = r.user_id WHERE m.id IS NULL OR u.id IS NULL",
        )?,
        orphan_commands: scalar(
            &mut connection,
            "SELECT COUNT(*) FROM command_results c LEFT JOIN users u ON u.id = c.user_id LEFT JOIN messages m ON m.id = c.message_id WHERE u.id IS NULL OR (c.message_id IS NOT NULL AND m.id IS NULL)",
        )?,
    };
    snapshot.validate()?;
    Ok(snapshot)
}

fn scalar(connection: &mut radixdb_client::Connection, sql: &str) -> Result<i64, String> {
    tcp_scalar_i64(connection, sql).map_err(|error| format!("oracle `{sql}`: {error}"))
}
