use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaStatement {
    pub id: String,
    pub sql: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaPlan {
    pub version: u32,
    pub statements: Vec<SchemaStatement>,
}

impl SchemaPlan {
    pub fn validate(&self) -> Result<(), String> {
        if self.version == 0 {
            return Err("schema plan version must be greater than zero".to_string());
        }
        if self.statements.is_empty() {
            return Err("schema plan must contain at least one statement".to_string());
        }
        let mut ids = BTreeSet::new();
        for statement in &self.statements {
            if statement.id.is_empty()
                || !statement
                    .id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return Err(format!("invalid schema statement id `{}`", statement.id));
            }
            if !ids.insert(statement.id.as_str()) {
                return Err(format!("duplicate schema statement id `{}`", statement.id));
            }
            let sql = statement.sql.trim();
            if sql.is_empty() || sql.contains('\0') {
                return Err(format!(
                    "schema statement `{}` has invalid SQL",
                    statement.id
                ));
            }
        }
        Ok(())
    }

    pub fn fingerprint(&self) -> Result<String, String> {
        self.validate()?;
        let encoded = serde_json::to_vec(self).map_err(|error| error.to_string())?;
        let mut digest = Sha256::new();
        digest.update(encoded);
        Ok(format!("{:x}", digest.finalize()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessengerFixtureScale {
    Small,
    Medium,
    Large,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessengerSeedTable {
    Tenants,
    Users,
    Devices,
    Sessions,
    Conversations,
    ConversationMembers,
    Messages,
    MessageVersions,
    ForwardedMessages,
    Attachments,
    ForwardedAttachmentAccess,
    Reactions,
    Receipts,
    OutboxJobs,
    SyncEvents,
    CommandResults,
    RefreshTokens,
    AuditLog,
}

impl MessengerSeedTable {
    pub const ALL: [Self; 18] = [
        Self::Tenants,
        Self::Users,
        Self::Devices,
        Self::Sessions,
        Self::Conversations,
        Self::ConversationMembers,
        Self::Messages,
        Self::MessageVersions,
        Self::ForwardedMessages,
        Self::Attachments,
        Self::ForwardedAttachmentAccess,
        Self::Reactions,
        Self::Receipts,
        Self::OutboxJobs,
        Self::SyncEvents,
        Self::CommandResults,
        Self::RefreshTokens,
        Self::AuditLog,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Tenants => "tenants",
            Self::Users => "users",
            Self::Devices => "devices",
            Self::Sessions => "sessions",
            Self::Conversations => "conversations",
            Self::ConversationMembers => "conversation_members",
            Self::Messages => "messages",
            Self::MessageVersions => "message_versions",
            Self::ForwardedMessages => "forwarded_messages",
            Self::Attachments => "attachments",
            Self::ForwardedAttachmentAccess => "forwarded_attachment_access",
            Self::Reactions => "reactions",
            Self::Receipts => "receipts",
            Self::OutboxJobs => "outbox_jobs",
            Self::SyncEvents => "sync_events",
            Self::CommandResults => "command_results",
            Self::RefreshTokens => "refresh_tokens",
            Self::AuditLog => "audit_log",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessengerStorageTier {
    Cold,
    Hot,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessengerSeedRecord {
    pub table: MessengerSeedTable,
    pub ordinal: u64,
    pub primary_key: u64,
    pub tenant_id: u64,
    pub owner_id: u64,
    pub conversation_id: u64,
    pub storage_tier: MessengerStorageTier,
}

pub struct MessengerSeedRows<'a> {
    plan: &'a MessengerSeedPlan,
    table: MessengerSeedTable,
    next_ordinal: u64,
    row_count: u64,
}

impl Iterator for MessengerSeedRows<'_> {
    type Item = MessengerSeedRecord;

    fn next(&mut self) -> Option<Self::Item> {
        let record = self.plan.record(self.table, self.next_ordinal)?;
        self.next_ordinal += 1;
        Some(record)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.row_count.saturating_sub(self.next_ordinal);
        let lower = usize::try_from(remaining).unwrap_or(usize::MAX);
        (lower, Some(lower))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessengerSeedPlan {
    pub scale: MessengerFixtureScale,
    pub seed: u64,
    pub tenants: u64,
    pub users: u64,
    pub devices: u64,
    pub sessions: u64,
    pub conversations: u64,
    pub conversation_members: u64,
    pub messages: u64,
    pub message_versions: u64,
    pub forwarded_messages: u64,
    pub attachments: u64,
    pub forwarded_attachment_access: u64,
    pub reactions: u64,
    pub receipts: u64,
    pub outbox_jobs: u64,
    pub sync_events: u64,
    pub command_results: u64,
    pub refresh_tokens: u64,
    pub audit_events: u64,
}

impl MessengerSeedPlan {
    pub fn new(scale: MessengerFixtureScale, seed: u64) -> Self {
        let counts = match scale {
            MessengerFixtureScale::Small => [2, 4, 2, 2, 3, 4, 2, 2, 1, 1, 1, 1, 1, 2, 3, 1, 1, 4],
            MessengerFixtureScale::Medium => [
                8, 4_096, 4_096, 4_096, 2_048, 8_192, 131_072, 32_768, 4_096, 32_768, 4_096,
                32_768, 32_768, 16_384, 32_768, 4_096, 4_096, 65_536,
            ],
            // Exactly 100,000,128 rows across all 18 related tables. The
            // distribution intentionally leaves a sizeable hot tail while
            // keeping most of the immutable fixture sealable as cold data.
            MessengerFixtureScale::Large => [
                128, 1_000_000, 1_000_000, 1_000_000, 2_000_000, 4_000_000, 46_000_000, 15_000_000,
                1_000_000, 4_000_000, 1_000_000, 5_000_000, 5_000_000, 5_000_000, 3_000_000,
                1_000_000, 1_000_000, 4_000_000,
            ],
        };
        Self {
            scale,
            seed,
            tenants: counts[0],
            users: counts[1],
            devices: counts[2],
            sessions: counts[3],
            conversations: counts[4],
            conversation_members: counts[5],
            messages: counts[6],
            message_versions: counts[7],
            forwarded_messages: counts[8],
            attachments: counts[9],
            forwarded_attachment_access: counts[10],
            reactions: counts[11],
            receipts: counts[12],
            outbox_jobs: counts[13],
            sync_events: counts[14],
            command_results: counts[15],
            refresh_tokens: counts[16],
            audit_events: counts[17],
        }
    }

    pub fn total_rows(&self) -> u64 {
        MessengerSeedTable::ALL
            .into_iter()
            .map(|table| self.row_count(table))
            .fold(0u64, u64::saturating_add)
    }

    pub const fn row_count(&self, table: MessengerSeedTable) -> u64 {
        match table {
            MessengerSeedTable::Tenants => self.tenants,
            MessengerSeedTable::Users => self.users,
            MessengerSeedTable::Devices => self.devices,
            MessengerSeedTable::Sessions => self.sessions,
            MessengerSeedTable::Conversations => self.conversations,
            MessengerSeedTable::ConversationMembers => self.conversation_members,
            MessengerSeedTable::Messages => self.messages,
            MessengerSeedTable::MessageVersions => self.message_versions,
            MessengerSeedTable::ForwardedMessages => self.forwarded_messages,
            MessengerSeedTable::Attachments => self.attachments,
            MessengerSeedTable::ForwardedAttachmentAccess => self.forwarded_attachment_access,
            MessengerSeedTable::Reactions => self.reactions,
            MessengerSeedTable::Receipts => self.receipts,
            MessengerSeedTable::OutboxJobs => self.outbox_jobs,
            MessengerSeedTable::SyncEvents => self.sync_events,
            MessengerSeedTable::CommandResults => self.command_results,
            MessengerSeedTable::RefreshTokens => self.refresh_tokens,
            MessengerSeedTable::AuditLog => self.audit_events,
        }
    }

    pub fn cold_rows(&self, table: MessengerSeedTable) -> u64 {
        let total = self.row_count(table);
        if total <= 1 {
            return 0;
        }
        let (numerator, denominator) = match self.scale {
            MessengerFixtureScale::Small => (1, 2),
            MessengerFixtureScale::Medium => (3, 4),
            MessengerFixtureScale::Large => (9, 10),
        };
        (total.saturating_mul(numerator) / denominator).clamp(1, total - 1)
    }

    pub fn hot_rows(&self, table: MessengerSeedTable) -> u64 {
        self.row_count(table).saturating_sub(self.cold_rows(table))
    }

    pub fn rows(&self, table: MessengerSeedTable) -> MessengerSeedRows<'_> {
        MessengerSeedRows {
            plan: self,
            table,
            next_ordinal: 0,
            row_count: self.row_count(table),
        }
    }

    pub fn record(&self, table: MessengerSeedTable, ordinal: u64) -> Option<MessengerSeedRecord> {
        if ordinal >= self.row_count(table) {
            return None;
        }
        let owner_id = self.deterministic_owner(ordinal);
        Some(MessengerSeedRecord {
            table,
            ordinal,
            primary_key: ordinal + 1,
            tenant_id: (owner_id - 1) % self.tenants.max(1) + 1,
            owner_id,
            conversation_id: self.deterministic_conversation(ordinal),
            storage_tier: if ordinal < self.cold_rows(table) {
                MessengerStorageTier::Cold
            } else {
                MessengerStorageTier::Hot
            },
        })
    }

    pub fn deterministic_owner(&self, ordinal: u64) -> u64 {
        let mixed = ordinal
            .wrapping_add(self.seed)
            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
            .rotate_left(17);
        mixed % self.users.max(1) + 1
    }

    pub fn deterministic_conversation(&self, ordinal: u64) -> u64 {
        let mixed = ordinal
            .wrapping_add(self.seed.rotate_left(11))
            .wrapping_mul(0xbf58_476d_1ce4_e5b9)
            .rotate_left(29);
        mixed % self.conversations.max(1) + 1
    }
}

pub fn messenger_schema_plan() -> SchemaPlan {
    let statements = [
        ("tenants", "CREATE TABLE tenants (id INTEGER PRIMARY KEY, slug TEXT NOT NULL UNIQUE, active BOOLEAN NOT NULL)"),
        ("users", "CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL REFERENCES tenants(id), username TEXT NOT NULL, display_name TEXT, revision INTEGER NOT NULL CHECK (revision >= 1), disabled BOOLEAN NOT NULL, UNIQUE (tenant_id, username))"),
        ("devices", "CREATE TABLE devices (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), device_key TEXT NOT NULL UNIQUE, platform TEXT NOT NULL CHECK (platform IN ('android', 'ios', 'web', 'desktop')))"),
        ("sessions", "CREATE TABLE sessions (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), device_id INTEGER REFERENCES devices(id), active BOOLEAN NOT NULL, revision INTEGER NOT NULL CHECK (revision >= 1))"),
        ("conversations", "CREATE TABLE conversations (id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL REFERENCES tenants(id), created_by INTEGER NOT NULL REFERENCES users(id), kind TEXT NOT NULL CHECK (kind IN ('direct', 'group')), title TEXT, active BOOLEAN NOT NULL)"),
        ("conversation_members", "CREATE TABLE conversation_members (id INTEGER PRIMARY KEY, conversation_id INTEGER NOT NULL REFERENCES conversations(id), user_id INTEGER NOT NULL REFERENCES users(id), role TEXT NOT NULL CHECK (role IN ('member', 'admin', 'owner')), last_read_seq INTEGER NOT NULL CHECK (last_read_seq >= 0), muted BOOLEAN NOT NULL, UNIQUE (conversation_id, user_id))"),
        ("messages", "CREATE TABLE messages (id INTEGER PRIMARY KEY, conversation_id INTEGER NOT NULL REFERENCES conversations(id), sequence INTEGER NOT NULL CHECK (sequence >= 1), sender_id INTEGER NOT NULL REFERENCES users(id), body TEXT NOT NULL, deleted BOOLEAN NOT NULL, UNIQUE (conversation_id, sequence))"),
        ("message_versions", "CREATE TABLE message_versions (id INTEGER PRIMARY KEY, message_id INTEGER NOT NULL REFERENCES messages(id), revision INTEGER NOT NULL CHECK (revision >= 1), body TEXT NOT NULL, UNIQUE (message_id, revision))"),
        ("forwarded_messages", "CREATE TABLE forwarded_messages (id INTEGER PRIMARY KEY, message_id INTEGER NOT NULL REFERENCES messages(id), source_message_id INTEGER REFERENCES messages(id), source_conversation_id INTEGER REFERENCES conversations(id))"),
        ("attachments", "CREATE TABLE attachments (id INTEGER PRIMARY KEY, message_id INTEGER REFERENCES messages(id), storage_key TEXT NOT NULL UNIQUE, byte_size INTEGER NOT NULL CHECK (byte_size >= 0))"),
        ("forwarded_attachment_access", "CREATE TABLE forwarded_attachment_access (id INTEGER PRIMARY KEY, forward_id INTEGER NOT NULL REFERENCES forwarded_messages(id), attachment_id INTEGER NOT NULL REFERENCES attachments(id), user_id INTEGER REFERENCES users(id), access_kind TEXT NOT NULL CHECK (access_kind IN ('read', 'download')), UNIQUE (forward_id, attachment_id, user_id))"),
        ("reactions", "CREATE TABLE reactions (id INTEGER PRIMARY KEY, message_id INTEGER NOT NULL REFERENCES messages(id), user_id INTEGER NOT NULL REFERENCES users(id), emoji TEXT NOT NULL, UNIQUE (message_id, user_id, emoji))"),
        ("receipts", "CREATE TABLE receipts (id INTEGER PRIMARY KEY, message_id INTEGER NOT NULL REFERENCES messages(id), user_id INTEGER NOT NULL REFERENCES users(id), delivered_seq INTEGER NOT NULL CHECK (delivered_seq >= 0), read_seq INTEGER NOT NULL CHECK (read_seq >= 0), UNIQUE (message_id, user_id))"),
        ("outbox_jobs", "CREATE TABLE outbox_jobs (id INTEGER PRIMARY KEY, message_id INTEGER NOT NULL UNIQUE REFERENCES messages(id), state TEXT NOT NULL CHECK (state IN ('pending', 'leased', 'done')), retry_count INTEGER NOT NULL CHECK (retry_count >= 0), lease_owner TEXT, visible BOOLEAN NOT NULL)"),
        ("sync_events", "CREATE TABLE sync_events (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), sequence INTEGER NOT NULL CHECK (sequence >= 1), message_id INTEGER REFERENCES messages(id), outbox_job_id INTEGER REFERENCES outbox_jobs(id), event_type TEXT NOT NULL, UNIQUE (user_id, sequence))"),
        ("command_results", "CREATE TABLE command_results (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), idempotency_key TEXT NOT NULL, message_id INTEGER REFERENCES messages(id), UNIQUE (user_id, idempotency_key))"),
        ("refresh_tokens", "CREATE TABLE refresh_tokens (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), token_hash TEXT NOT NULL UNIQUE, revoked BOOLEAN NOT NULL)"),
        ("audit_log", "CREATE TABLE audit_log (id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL REFERENCES tenants(id), user_id INTEGER REFERENCES users(id), entity_kind TEXT NOT NULL, entity_id INTEGER NOT NULL, action TEXT NOT NULL)"),
        ("messages_sender_idx", "CREATE INDEX messages_sender_idx ON messages (sender_id, conversation_id)"),
        ("sync_message_idx", "CREATE INDEX sync_message_idx ON sync_events (message_id, user_id)"),
        ("outbox_state_idx", "CREATE INDEX outbox_state_idx ON outbox_jobs (state, visible, id)"),
        ("audit_entity_idx", "CREATE INDEX audit_entity_idx ON audit_log (tenant_id, entity_kind, entity_id)"),
    ]
    .into_iter()
    .map(|(id, sql)| SchemaStatement {
        id: id.to_string(),
        sql: sql.to_string(),
    })
    .collect();
    SchemaPlan {
        version: 1,
        statements,
    }
}

pub fn messenger_view_plan() -> SchemaPlan {
    let statements = [
        ("active_conversations_v", "CREATE VIEW active_conversations_v AS SELECT c.id AS conversation_id, c.tenant_id, c.title, cm.user_id, MAX(m.sequence) AS last_sequence FROM conversations c JOIN conversation_members cm ON cm.conversation_id = c.id LEFT JOIN messages m ON m.conversation_id = c.id WHERE c.active = true GROUP BY c.id, c.tenant_id, c.title, cm.user_id"),
        ("message_delivery_v", "CREATE VIEW message_delivery_v AS SELECT m.id AS message_id, m.conversation_id, m.sequence, o.id AS outbox_id, o.state AS outbox_state, r.user_id AS receipt_user_id, r.delivered_seq, r.read_seq FROM messages m LEFT JOIN outbox_jobs o ON o.message_id = m.id LEFT JOIN receipts r ON r.message_id = m.id"),
        ("pending_outbox_v", "CREATE VIEW pending_outbox_v AS SELECT id, message_id, retry_count, lease_owner FROM outbox_jobs WHERE visible = true AND state IN ('pending', 'leased')"),
        ("conversation_unread_v", "CREATE VIEW conversation_unread_v AS SELECT cm.conversation_id, cm.user_id, COUNT(m.id) AS unread_count FROM conversation_members cm LEFT JOIN messages m ON m.conversation_id = cm.conversation_id AND m.sequence > cm.last_read_seq GROUP BY cm.conversation_id, cm.user_id"),
        ("conversation_health_v", "CREATE VIEW conversation_health_v AS SELECT c.id AS conversation_id, COUNT(DISTINCT m.id) AS message_count, COUNT(DISTINCT o.id) AS outbox_count FROM conversations c LEFT JOIN messages m ON m.conversation_id = c.id LEFT JOIN outbox_jobs o ON o.message_id = m.id GROUP BY c.id"),
        ("user_sync_feed_v", "CREATE VIEW user_sync_feed_v AS SELECT s.user_id, s.sequence, s.event_type, s.message_id, m.conversation_id FROM sync_events s LEFT JOIN messages m ON m.id = s.message_id"),
        ("forwarded_attachment_access_v", "CREATE VIEW forwarded_attachment_access_v AS SELECT faa.user_id, fm.message_id AS forwarded_message_id, fm.source_message_id, a.id AS attachment_id, a.storage_key, faa.access_kind FROM forwarded_attachment_access faa JOIN forwarded_messages fm ON fm.id = faa.forward_id JOIN attachments a ON a.id = faa.attachment_id"),
        ("user_inbox_v", "CREATE VIEW user_inbox_v AS SELECT a.user_id, a.conversation_id, a.tenant_id, a.title, a.last_sequence, u.unread_count FROM active_conversations_v a LEFT JOIN conversation_unread_v u ON u.conversation_id = a.conversation_id AND u.user_id = a.user_id"),
    ]
    .into_iter()
    .map(|(id, sql)| SchemaStatement {
        id: id.to_string(),
        sql: sql.to_string(),
    })
    .collect();
    SchemaPlan {
        version: 1,
        statements,
    }
}

pub fn messenger_small_seed_plan() -> SchemaPlan {
    let statements = [
        ("seed_tenants", "INSERT INTO tenants VALUES (1, 'acme', true), (2, 'dormant', false)"),
        ("seed_users", "INSERT INTO users VALUES (1, 1, 'alice', 'Alice', 1, false), (2, 1, 'bob', 'Bob', 1, false), (3, 1, 'carol', NULL, 1, false), (4, 2, 'dave', 'Dave', 1, true)"),
        ("seed_devices", "INSERT INTO devices VALUES (1, 1, 'device-alice', 'desktop'), (2, 2, 'device-bob', 'android')"),
        ("seed_sessions", "INSERT INTO sessions VALUES (1, 1, 1, true, 1), (2, 3, NULL, true, 1)"),
        ("seed_conversations", "INSERT INTO conversations VALUES (10, 1, 1, 'group', 'General', true), (20, 1, 3, 'direct', NULL, true), (30, 2, 4, 'direct', 'Dormant', false)"),
        ("seed_members", "INSERT INTO conversation_members VALUES (1001, 10, 1, 'owner', 0, false), (1002, 10, 2, 'member', 1, false), (2003, 20, 3, 'owner', 0, false), (3004, 30, 4, 'owner', 0, true)"),
        ("seed_messages", "INSERT INTO messages VALUES (100, 10, 1, 1, 'hello', false), (101, 10, 2, 2, 'world', false)"),
        ("seed_versions", "INSERT INTO message_versions VALUES (1001, 100, 1, 'hello'), (1002, 100, 2, 'hello edited')"),
        ("seed_forwards", "INSERT INTO forwarded_messages VALUES (500, 101, 100, 10)"),
        ("seed_attachments", "INSERT INTO attachments VALUES (600, 100, 'blob-600', 4096)"),
        ("seed_forward_access", "INSERT INTO forwarded_attachment_access VALUES (700, 500, 600, 2, 'read')"),
        ("seed_reactions", "INSERT INTO reactions VALUES (800, 100, 2, 'like')"),
        ("seed_receipts", "INSERT INTO receipts VALUES (900, 100, 2, 1, 1)"),
        ("seed_outbox", "INSERT INTO outbox_jobs VALUES (1000, 100, 'pending', 0, NULL, true), (1001, 101, 'done', 0, NULL, false)"),
        ("seed_sync", "INSERT INTO sync_events VALUES (1100, 2, 1, 100, 1000, 'message.created'), (1101, 2, 2, 101, 1001, 'message.created'), (1102, 3, 1, NULL, NULL, 'membership.changed')"),
        ("seed_commands", "INSERT INTO command_results VALUES (1200, 1, 'cmd-100', 100)"),
        ("seed_tokens", "INSERT INTO refresh_tokens VALUES (1300, 1, 'token-alice', false)"),
        ("seed_audit", "INSERT INTO audit_log VALUES (1400, 1, 1, 'message', 100, 'insert'), (1401, 1, 2, 'message', 101, 'insert'), (1402, 1, NULL, 'conversation', 20, 'create'), (1403, 2, 4, 'conversation', 30, 'create')"),
    ]
    .into_iter()
    .map(|(id, sql)| SchemaStatement {
        id: id.to_string(),
        sql: sql.to_string(),
    })
    .collect();
    SchemaPlan {
        version: 1,
        statements,
    }
}
