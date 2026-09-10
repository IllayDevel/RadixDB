// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Frozen heavy-JOIN consumer corpus used by the JOIN/reference optimization gates.
//!
//! The SQL is copied from the read-only Mozaic Messenger consumer oracle. Data is
//! synthetic and generated deterministically, so RadixDB and PostgreSQL adapters
//! can execute the same schema, rows, parameters, and result checksum contract.

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use radixdb::{Database, NamedParams, Rows, Value};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const SOURCE_COMMIT: &str = "60c4e70ea5b5e036c8f5b23466e6ebe3600e1eea";
pub const SOURCE_PATH: &str = "_work/v2/RADIXDB_HEAVY_JOIN_WORKLOAD.md";
pub const SOURCE_SHA256: &str = "2b3cfef9ceb09b224af888a35ca4728d1c0a6f1f4492ae7f5951677dea34e043";
pub const SCHEMA_GENERATION: &str = "0069";
pub const FIXED_NOW: &str = "2026-08-28T12:00:00Z";
pub const RINGING_STARTED_AFTER: &str = "2026-08-28T10:00:00Z";

pub const SCHEMA_SQL: &str = include_str!("../fixtures/schema.sql");
pub const MANIFEST_JSON: &str = include_str!("../fixtures/manifest.json");

const Q1_SQL: &str = include_str!("../fixtures/q1-message-push.sql");
const Q2_SQL: &str = include_str!("../fixtures/q2-call-push.sql");
const Q3_SQL: &str = include_str!("../fixtures/q3-stream-push.sql");
const Q4_SQL: &str = include_str!("../fixtures/q4-publication-recipients.sql");
const Q5_SQL: &str = include_str!("../fixtures/q5-snapshot-users.sql");
const Q6_SQL: &str = include_str!("../fixtures/q6-snapshot-members.sql");
const C1_SQL: &str = include_str!("../fixtures/c1-outbox-poll.sql");

const USER_KIND: u16 = 0x0100;
const DEVICE_KIND: u16 = 0x0200;
const SESSION_KIND: u16 = 0x0300;
const CONVERSATION_KIND: u16 = 0x0400;
const MEMBER_KIND: u16 = 0x0500;
const MESSAGE_KIND: u16 = 0x0600;
const JOB_KIND: u16 = 0x0700;
const SYNC_EVENT_KIND: u16 = 0x0800;
const PUSH_TOKEN_KIND: u16 = 0x0900;
const CALL_KIND: u16 = 0x0a00;
const CALL_PARTICIPANT_KIND: u16 = 0x0b00;
const CALL_DELIVERY_KIND: u16 = 0x0c00;
const CONTACT_POLICY_KIND: u16 = 0x0d00;
const UPSTREAM_KIND: u16 = 0x0e00;
const STREAM_KIND: u16 = 0x0f00;
const PUBLICATION_KIND: u16 = 0x1000;
const READ_STATE_KIND: u16 = 0x1100;
const ATTACHMENT_KIND: u16 = 0x1200;
const FORWARDED_ATTACHMENT_KIND: u16 = 0x1300;

#[derive(Debug, Error)]
pub enum WorkloadError {
    #[error(transparent)]
    Database(#[from] radixdb::Error),
    #[error("invalid workload scale: {0}")]
    InvalidScale(String),
    #[error("invalid fixture manifest: {0}")]
    InvalidManifest(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[cfg(feature = "postgres-oracle")]
    #[error(transparent)]
    Postgres(#[from] postgres::Error),
}

pub type Result<T> = std::result::Result<T, WorkloadError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum CaseId {
    Q1MessagePush,
    Q2CallPush,
    Q3StreamPush,
    Q4PublicationRecipients,
    Q5SnapshotUsers,
    Q6SnapshotMembers,
    C1OutboxPoll,
}

impl CaseId {
    pub const ALL: [Self; 7] = [
        Self::Q1MessagePush,
        Self::Q2CallPush,
        Self::Q3StreamPush,
        Self::Q4PublicationRecipients,
        Self::Q5SnapshotUsers,
        Self::Q6SnapshotMembers,
        Self::C1OutboxPoll,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Q1MessagePush => "Q1-message-push",
            Self::Q2CallPush => "Q2-call-push",
            Self::Q3StreamPush => "Q3-stream-push",
            Self::Q4PublicationRecipients => "Q4-publication-recipients",
            Self::Q5SnapshotUsers => "Q5-snapshot-users",
            Self::Q6SnapshotMembers => "Q6-snapshot-members",
            Self::C1OutboxPoll => "C1-outbox-poll",
        }
    }

    pub const fn sql(self) -> &'static str {
        match self {
            Self::Q1MessagePush => Q1_SQL,
            Self::Q2CallPush => Q2_SQL,
            Self::Q3StreamPush => Q3_SQL,
            Self::Q4PublicationRecipients => Q4_SQL,
            Self::Q5SnapshotUsers => Q5_SQL,
            Self::Q6SnapshotMembers => Q6_SQL,
            Self::C1OutboxPoll => C1_SQL,
        }
    }

    pub const fn fixture_path(self) -> &'static str {
        match self {
            Self::Q1MessagePush => "q1-message-push.sql",
            Self::Q2CallPush => "q2-call-push.sql",
            Self::Q3StreamPush => "q3-stream-push.sql",
            Self::Q4PublicationRecipients => "q4-publication-recipients.sql",
            Self::Q5SnapshotUsers => "q5-snapshot-users.sql",
            Self::Q6SnapshotMembers => "q6-snapshot-members.sql",
            Self::C1OutboxPoll => "c1-outbox-poll.sql",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkloadScale {
    pub users: usize,
    pub devices_per_user: usize,
    pub conversations: usize,
    pub members_per_conversation: usize,
    pub messages: usize,
    pub stream_recipients: usize,
    pub publications: usize,
    pub outbox_jobs: usize,
    pub insert_batch_rows: usize,
}

impl WorkloadScale {
    pub const fn smoke() -> Self {
        Self {
            users: 16,
            devices_per_user: 2,
            conversations: 8,
            members_per_conversation: 4,
            messages: 32,
            stream_recipients: 8,
            publications: 32,
            outbox_jobs: 32,
            insert_batch_rows: 64,
        }
    }

    pub const fn consumer_profile() -> Self {
        Self {
            users: 10_000,
            devices_per_user: 2,
            conversations: 1_000,
            members_per_conversation: 100,
            messages: 1_000_000,
            stream_recipients: 10_000,
            publications: 1_000_000,
            outbox_jobs: 100_000,
            insert_batch_rows: 1_000,
        }
    }

    fn validate(self) -> Result<()> {
        if self.users < 4 {
            return Err(WorkloadError::InvalidScale(
                "at least four users are required".into(),
            ));
        }
        if self.devices_per_user == 0
            || self.conversations == 0
            || self.members_per_conversation < 3
            || self.members_per_conversation > self.users
            || self.messages == 0
            || self.stream_recipients == 0
            || self.stream_recipients > self.users
            || self.publications == 0
            || self.outbox_jobs < 4
            || self.insert_batch_rows == 0
        {
            return Err(WorkloadError::InvalidScale(
                "non-zero cardinalities, members <= users, streams <= users and four jobs are required"
                    .into(),
            ));
        }
        Ok(())
    }

    pub const fn expected_rows(self, case: CaseId) -> usize {
        match case {
            CaseId::Q1MessagePush | CaseId::Q2CallPush | CaseId::Q3StreamPush => {
                self.devices_per_user
            }
            CaseId::Q4PublicationRecipients => self.stream_recipients,
            CaseId::Q5SnapshotUsers | CaseId::Q6SnapshotMembers => {
                self.conversations * self.members_per_conversation
            }
            CaseId::C1OutboxPoll => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseResult {
    pub case: CaseId,
    pub rows: usize,
    pub checksum_sha256: String,
    pub canonical_result_bytes: u64,
    pub elapsed: Duration,
}

#[derive(Debug, Deserialize)]
pub struct FixtureManifest {
    pub source_commit: String,
    pub source_path: String,
    pub source_sha256: String,
    pub schema_generation: String,
    pub fixtures: Vec<FixtureDigest>,
}

#[derive(Debug, Deserialize)]
pub struct FixtureDigest {
    pub path: String,
    pub sha256: String,
}

pub fn verify_fixture_manifest() -> Result<FixtureManifest> {
    let manifest: FixtureManifest = serde_json::from_str(MANIFEST_JSON)?;
    if manifest.source_commit != SOURCE_COMMIT
        || manifest.source_path != SOURCE_PATH
        || manifest.source_sha256 != SOURCE_SHA256
        || manifest.schema_generation != SCHEMA_GENERATION
    {
        return Err(WorkloadError::InvalidManifest(
            "source provenance does not match compiled constants".into(),
        ));
    }
    for fixture in &manifest.fixtures {
        let content = fixture_content(&fixture.path).ok_or_else(|| {
            WorkloadError::InvalidManifest(format!("unknown fixture {}", fixture.path))
        })?;
        let actual = hex_digest(content.as_bytes());
        if actual != fixture.sha256 {
            return Err(WorkloadError::InvalidManifest(format!(
                "{} checksum mismatch: expected {}, got {}",
                fixture.path, fixture.sha256, actual
            )));
        }
    }
    if manifest.fixtures.len() != 8 {
        return Err(WorkloadError::InvalidManifest(
            "manifest must contain schema plus Q1-Q6/C1".into(),
        ));
    }
    Ok(manifest)
}

pub fn apply_schema(db: &Database) -> Result<()> {
    for statement in sql_statements(SCHEMA_SQL) {
        db.execute(statement, ())?;
    }
    Ok(())
}

pub fn seed_database(db: &Database, scale: WorkloadScale) -> Result<()> {
    for_each_seed_statement(scale, |statement| {
        db.execute(statement, ())?;
        Ok(())
    })
}

/// Streams deterministic INSERT statements to an adapter without retaining the
/// complete consumer-scale dataset in memory.
pub fn for_each_seed_statement<F>(scale: WorkloadScale, mut sink: F) -> Result<()>
where
    F: FnMut(&str) -> Result<()>,
{
    scale.validate()?;
    let batch = scale.insert_batch_rows;

    emit_rows(
        "attachments",
        &["id", "original_filename"],
        (0..2).map(|index| {
            format!(
                "({}, {})",
                quoted_uuid(ATTACHMENT_KIND, index),
                quoted(&format!("fixture-{index}.bin"))
            )
        }),
        batch,
        &mut sink,
    )?;

    emit_rows(
        "users",
        &[
            "id",
            "username",
            "display_name",
            "account_kind",
            "avatar_attachment_id",
            "profile_revision",
            "disabled_at",
            "deleted_at",
        ],
        (0..scale.users).map(|index| {
            format!(
                "({}, {}, {}, 'human', NULL, 1, NULL, NULL)",
                quoted_uuid(USER_KIND, index),
                quoted(&format!("user-{index:05}")),
                quoted(&format!("User {index:05}"))
            )
        }),
        batch,
        &mut sink,
    )?;

    let device_count = scale.users * scale.devices_per_user;
    emit_rows(
        "devices",
        &["id", "user_id", "last_seen_at", "revoked_at"],
        (0..device_count).map(|index| {
            let user = index / scale.devices_per_user;
            format!(
                "({}, {}, '2026-08-28 11:00:00', NULL)",
                quoted_uuid(DEVICE_KIND, index),
                quoted_uuid(USER_KIND, user)
            )
        }),
        batch,
        &mut sink,
    )?;
    emit_rows(
        "sessions",
        &[
            "id",
            "user_id",
            "device_id",
            "expires_at",
            "absolute_expires_at",
            "revoked_at",
        ],
        (0..device_count).map(|index| {
            let user = index / scale.devices_per_user;
            format!(
                "({}, {}, {}, '2027-08-28 12:00:00', '2027-08-28 12:00:00', NULL)",
                quoted_uuid(SESSION_KIND, index),
                quoted_uuid(USER_KIND, user),
                quoted_uuid(DEVICE_KIND, index)
            )
        }),
        batch,
        &mut sink,
    )?;

    emit_rows(
        "conversations",
        &["id", "self_owner_id"],
        (0..scale.conversations)
            .map(|index| format!("({}, NULL)", quoted_uuid(CONVERSATION_KIND, index))),
        batch,
        &mut sink,
    )?;
    emit_rows(
        "conversation_members",
        &[
            "id",
            "conversation_id",
            "user_id",
            "role",
            "joined_seq",
            "history_cleared_through_seq",
            "last_delivered_seq",
            "last_read_seq",
            "joined_at",
            "left_at",
            "muted_until",
            "revision",
        ],
        (0..scale.conversations * scale.members_per_conversation).map(|index| {
            let conversation = index / scale.members_per_conversation;
            let slot = index % scale.members_per_conversation;
            let user = if slot == 0 {
                0
            } else {
                1 + (conversation * (scale.members_per_conversation - 1) + slot - 1)
                    % (scale.users - 1)
            };
            format!(
                "({}, {}, {}, 'member', 1, 0, 0, 0, '2026-08-28 10:00:00', NULL, NULL, 1)",
                quoted_uuid(MEMBER_KIND, index),
                quoted_uuid(CONVERSATION_KIND, conversation),
                quoted_uuid(USER_KIND, user)
            )
        }),
        batch,
        &mut sink,
    )?;

    emit_rows(
        "messages",
        &[
            "id",
            "conversation_id",
            "sender_user_id",
            "body",
            "formatting_entities",
        ],
        (0..scale.messages).map(|index| {
            let conversation = index % scale.conversations;
            let sender = 1 + index % (scale.users - 1);
            format!(
                "({}, {}, {}, {}, '[]')",
                quoted_uuid(MESSAGE_KIND, index),
                quoted_uuid(CONVERSATION_KIND, conversation),
                quoted_uuid(USER_KIND, sender),
                quoted(&format!("Message {index}"))
            )
        }),
        batch,
        &mut sink,
    )?;

    emit_rows(
        "outbox_jobs",
        &[
            "id",
            "event_type",
            "aggregate_type",
            "aggregate_id",
            "state",
            "available_at",
            "lease_until",
            "revision",
            "created_at",
        ],
        (0..scale.outbox_jobs).map(|index| {
            let (event_type, aggregate_type, aggregate_id) = match index {
                0 => ("message.created", "message", quoted_uuid(MESSAGE_KIND, 0)),
                1 => ("call.sync_required", "call_user", quoted_uuid(USER_KIND, 2)),
                2 => (
                    "stream.publication_created",
                    "stream_publication",
                    quoted_uuid(PUBLICATION_KIND, 0),
                ),
                _ => ("fixture.noise", "fixture", "NULL".into()),
            };
            format!(
                "({}, {}, {}, {}, 'pending', '2026-08-28 11:00:00', NULL, 1, '2026-08-28 11:00:00')",
                quoted_uuid(JOB_KIND, index),
                quoted(event_type),
                quoted(aggregate_type),
                aggregate_id
            )
        }),
        batch,
        &mut sink,
    )?;
    emit_rows(
        "sync_events",
        &[
            "id",
            "user_id",
            "event_type",
            "aggregate_type",
            "aggregate_id",
            "outbox_job_id",
        ],
        std::iter::once(format!(
            "({}, {}, 'message.created', 'message', {}, {})",
            quoted_uuid(SYNC_EVENT_KIND, 0),
            quoted_uuid(USER_KIND, 2),
            quoted_uuid(MESSAGE_KIND, 0),
            quoted_uuid(JOB_KIND, 0)
        )),
        batch,
        &mut sink,
    )?;
    emit_rows(
        "message_attachments",
        &["message_id", "attachment_id", "position"],
        std::iter::once(format!(
            "({}, {}, 0)",
            quoted_uuid(MESSAGE_KIND, 0),
            quoted_uuid(ATTACHMENT_KIND, 0)
        )),
        batch,
        &mut sink,
    )?;
    emit_rows(
        "forwarded_message_attachments",
        &["id", "message_id", "attachment_id", "position"],
        std::iter::once(format!(
            "({}, {}, {}, 0)",
            quoted_uuid(FORWARDED_ATTACHMENT_KIND, 0),
            quoted_uuid(MESSAGE_KIND, 0),
            quoted_uuid(ATTACHMENT_KIND, 1)
        )),
        batch,
        &mut sink,
    )?;

    emit_rows(
        "user_notification_settings",
        &[
            "id",
            "enabled",
            "sound_enabled",
            "stream_notifications_enabled",
        ],
        (0..scale.users).map(|index| format!("({}, 1, 1, 1)", quoted_uuid(USER_KIND, index))),
        batch,
        &mut sink,
    )?;
    emit_rows(
        "push_tokens",
        &[
            "id",
            "user_id",
            "device_id",
            "provider",
            "endpoint",
            "p256dh",
            "auth_secret",
            "disabled_at",
            "revision",
        ],
        (0..device_count).map(|index| {
            let user = index / scale.devices_per_user;
            format!(
                "({}, {}, {}, 'webpush', {}, 'fixture-p256dh', 'fixture-auth', NULL, 1)",
                quoted_uuid(PUSH_TOKEN_KIND, index),
                quoted_uuid(USER_KIND, user),
                quoted_uuid(DEVICE_KIND, index),
                quoted(&format!("https://fixture.invalid/{index}"))
            )
        }),
        batch,
        &mut sink,
    )?;

    emit_rows(
        "call_reception_settings",
        &["user_id", "incoming_calls_enabled", "sound_enabled"],
        (0..scale.users).map(|index| format!("({}, 1, 1)", quoted_uuid(USER_KIND, index))),
        batch,
        &mut sink,
    )?;
    emit_rows(
        "call_contact_policies",
        &[
            "id",
            "owner_user_id",
            "contact_user_id",
            "incoming_calls_enabled",
        ],
        std::iter::once(format!(
            "({}, {}, {}, 1)",
            quoted_uuid(CONTACT_POLICY_KIND, 0),
            quoted_uuid(USER_KIND, 2),
            quoted_uuid(USER_KIND, 1)
        )),
        batch,
        &mut sink,
    )?;
    emit_rows(
        "call_sessions",
        &[
            "id",
            "kind",
            "conversation_id",
            "creator_user_id",
            "lifecycle_state",
            "ringing_deadline_at",
            "absolute_expires_at",
        ],
        std::iter::once(format!(
            "({}, 'conversation_group', {}, {}, 'ringing', '2026-08-28 13:00:00', '2026-08-28 14:00:00')",
            quoted_uuid(CALL_KIND, 0),
            quoted_uuid(CONVERSATION_KIND, 0),
            quoted_uuid(USER_KIND, 1)
        )),
        batch,
        &mut sink,
    )?;
    emit_rows(
        "call_participants",
        &["id", "call_id", "user_id", "participant_state"],
        std::iter::once(format!(
            "({}, {}, {}, 'ringing')",
            quoted_uuid(CALL_PARTICIPANT_KIND, 0),
            quoted_uuid(CALL_KIND, 0),
            quoted_uuid(USER_KIND, 2)
        )),
        batch,
        &mut sink,
    )?;
    emit_rows(
        "call_device_deliveries",
        &[
            "id",
            "call_id",
            "user_id",
            "device_id",
            "delivery_state",
            "updated_at",
        ],
        (0..scale.devices_per_user).map(|slot| {
            let device = 2 * scale.devices_per_user + slot;
            format!(
                "({}, {}, {}, {}, 'ringing', '2026-08-28 11:30:00')",
                quoted_uuid(CALL_DELIVERY_KIND, slot),
                quoted_uuid(CALL_KIND, 0),
                quoted_uuid(USER_KIND, 2),
                quoted_uuid(DEVICE_KIND, device)
            )
        }),
        batch,
        &mut sink,
    )?;

    emit_rows(
        "stream_upstreams",
        &["id", "title"],
        std::iter::once(format!(
            "({}, 'Fixture stream')",
            quoted_uuid(UPSTREAM_KIND, 0)
        )),
        batch,
        &mut sink,
    )?;
    emit_rows(
        "streams",
        &[
            "id",
            "owner_user_id",
            "upstream_id",
            "state",
            "notifications_enabled",
            "deleted_at",
        ],
        (0..scale.stream_recipients).map(|index| {
            let owner = (index + 2) % scale.users;
            format!(
                "({}, {}, {}, 'active', 1, NULL)",
                quoted_uuid(STREAM_KIND, index),
                quoted_uuid(USER_KIND, owner),
                quoted_uuid(UPSTREAM_KIND, 0)
            )
        }),
        batch,
        &mut sink,
    )?;
    emit_rows(
        "stream_publications",
        &[
            "id",
            "upstream_id",
            "sort_key",
            "text_content",
            "deleted_at",
            "expires_at",
        ],
        (0..scale.publications).map(|index| {
            let sort_key = index + 100;
            format!(
                "({}, {}, {}, {}, NULL, '2027-08-28 12:00:00')",
                quoted_uuid(PUBLICATION_KIND, index),
                quoted_uuid(UPSTREAM_KIND, 0),
                sort_key,
                quoted(&format!("Publication {index}"))
            )
        }),
        batch,
        &mut sink,
    )?;
    emit_rows(
        "stream_read_states",
        &[
            "id",
            "stream_id",
            "owner_user_id",
            "last_read_sort_key",
            "last_read_publication_id",
        ],
        (0..scale.stream_recipients).map(|index| {
            let owner = (index + 2) % scale.users;
            format!(
                "({}, {}, {}, 0, NULL)",
                quoted_uuid(READ_STATE_KIND, index),
                quoted_uuid(STREAM_KIND, index),
                quoted_uuid(USER_KIND, owner)
            )
        }),
        batch,
        &mut sink,
    )?;
    emit_rows(
        "stream_publication_push_deliveries",
        &[
            "publication_id",
            "stream_id",
            "owner_user_id",
            "outbox_job_id",
        ],
        std::iter::once(format!(
            "({}, {}, {}, {})",
            quoted_uuid(PUBLICATION_KIND, 0),
            quoted_uuid(STREAM_KIND, 0),
            quoted_uuid(USER_KIND, 2),
            quoted_uuid(JOB_KIND, 2)
        )),
        batch,
        &mut sink,
    )?;

    Ok(())
}

pub fn case_params(case: CaseId, scale: WorkloadScale) -> NamedParams {
    let now = parse_timestamp(FIXED_NOW);
    match case {
        CaseId::Q1MessagePush => NamedParams::new()
            .add("outbox_job_id", uuid(JOB_KIND, 0))
            .add("now", now),
        CaseId::Q2CallPush => NamedParams::new()
            .add("outbox_job_id", uuid(JOB_KIND, 1))
            .add("now", now)
            .add(
                "ringing_started_after",
                parse_timestamp(RINGING_STARTED_AFTER),
            ),
        CaseId::Q3StreamPush => NamedParams::new()
            .add("outbox_job_id", uuid(JOB_KIND, 2))
            .add("now", now),
        CaseId::Q4PublicationRecipients => NamedParams::new()
            .add("upstream_id", uuid(UPSTREAM_KIND, 0))
            .add("publication_id", uuid(PUBLICATION_KIND, 0))
            .add("sort_key", 100i64)
            .add("recipient_limit", (scale.stream_recipients + 1) as i64),
        CaseId::Q5SnapshotUsers | CaseId::Q6SnapshotMembers => {
            NamedParams::new().add("user_id", uuid(USER_KIND, 0))
        }
        CaseId::C1OutboxPoll => NamedParams::new().add("now", now).add("limit", 4i64),
    }
}

pub fn execute_case(db: &Database, case: CaseId, scale: WorkloadScale) -> Result<CaseResult> {
    let started = Instant::now();
    let rows = db.query_named(case.sql(), case_params(case, scale))?;
    consume_case_rows(rows, case, started)
}

pub fn execute_case_with_timeout(
    db: &Database,
    case: CaseId,
    scale: WorkloadScale,
    timeout: Duration,
) -> Result<CaseResult> {
    let started = Instant::now();
    let timeout_ms = timeout.as_millis().min(u128::from(u64::MAX)) as u64;
    let rows = db.query_named_with_timeout(case.sql(), case_params(case, scale), timeout_ms)?;
    consume_case_rows(rows, case, started)
}

fn consume_case_rows(mut rows: Rows, case: CaseId, started: Instant) -> Result<CaseResult> {
    let mut hasher = Sha256::new();
    let mut canonical_result_bytes =
        hash_fields(&mut hasher, rows.columns().iter().map(String::as_str));
    let mut row_count = 0usize;
    while rows.advance() {
        let row = rows.current_row()?;
        canonical_result_bytes += hash_fields(
            &mut hasher,
            row.as_slice()
                .iter()
                .flat_map(|value| [format!("{:?}", value.data_type()), canonical_value(value)]),
        );
        row_count += 1;
    }
    if let Some(error) = rows.error() {
        return Err(error.into());
    }
    Ok(CaseResult {
        case,
        rows: row_count,
        checksum_sha256: format!("{:x}", hasher.finalize()),
        canonical_result_bytes,
        elapsed: started.elapsed(),
    })
}

pub fn explain_case(db: &Database, case: CaseId, scale: WorkloadScale) -> Result<Vec<String>> {
    explain_case_with_timeout(db, case, scale, Duration::ZERO)
}

pub fn explain_case_with_timeout(
    db: &Database,
    case: CaseId,
    scale: WorkloadScale,
    timeout: Duration,
) -> Result<Vec<String>> {
    explain_case_mode_with_timeout(db, case, scale, timeout, false)
}

pub fn explain_analyze_case_with_timeout(
    db: &Database,
    case: CaseId,
    scale: WorkloadScale,
    timeout: Duration,
) -> Result<Vec<String>> {
    explain_case_mode_with_timeout(db, case, scale, timeout, true)
}

fn explain_case_mode_with_timeout(
    db: &Database,
    case: CaseId,
    scale: WorkloadScale,
    timeout: Duration,
    analyze: bool,
) -> Result<Vec<String>> {
    let keyword = if analyze {
        "EXPLAIN ANALYZE"
    } else {
        "EXPLAIN"
    };
    let sql = format!("{keyword} {}", case.sql().trim_end_matches(';'));
    let timeout_ms = timeout.as_millis().min(u128::from(u64::MAX)) as u64;
    let mut rows = db.query_named_with_timeout(&sql, case_params(case, scale), timeout_ms)?;
    let mut lines = Vec::new();
    while rows.advance() {
        let row = rows.current_row()?;
        lines.push(
            row.as_slice()
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join(" | "),
        );
    }
    if let Some(error) = rows.error() {
        return Err(error.into());
    }
    Ok(lines)
}

fn emit_rows<I, F>(
    table: &str,
    columns: &[&str],
    rows: I,
    batch_size: usize,
    sink: &mut F,
) -> Result<()>
where
    I: IntoIterator<Item = String>,
    F: FnMut(&str) -> Result<()>,
{
    let prefix = format!("INSERT INTO {table} ({}) VALUES ", columns.join(", "));
    let mut batch = Vec::with_capacity(batch_size);
    for row in rows {
        batch.push(row);
        if batch.len() == batch_size {
            emit_insert(&prefix, &batch, sink)?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        emit_insert(&prefix, &batch, sink)?;
    }
    Ok(())
}

fn emit_insert<F>(prefix: &str, rows: &[String], sink: &mut F) -> Result<()>
where
    F: FnMut(&str) -> Result<()>,
{
    let mut statement = String::with_capacity(
        prefix.len() + rows.iter().map(String::len).sum::<usize>() + rows.len() * 2,
    );
    statement.push_str(prefix);
    statement.push_str(&rows.join(", "));
    statement.push(';');
    sink(&statement)
}

fn sql_statements(script: &str) -> impl Iterator<Item = &str> {
    script
        .split(';')
        .map(str::trim)
        .filter(|sql| !sql.is_empty())
}

fn quoted(raw: &str) -> String {
    format!("'{}'", raw.replace('\'', "''"))
}

fn uuid(kind: u16, index: usize) -> String {
    format!("00000000-{kind:04x}-7000-8000-{index:012x}")
}

fn quoted_uuid(kind: u16, index: usize) -> String {
    quoted(&uuid(kind, index))
}

fn parse_timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("compiled fixture timestamp must be valid")
        .with_timezone(&Utc)
}

fn canonical_value(value: &Value) -> String {
    value.to_string()
}

fn hash_fields<I, S>(hasher: &mut Sha256, fields: I) -> u64
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut bytes_hashed = 0u64;
    for field in fields {
        let bytes = field.as_ref().as_bytes();
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
        bytes_hashed = bytes_hashed.saturating_add(8 + bytes.len() as u64);
    }
    bytes_hashed
}

fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn fixture_content(path: &str) -> Option<&'static str> {
    match path {
        "schema.sql" => Some(SCHEMA_SQL),
        "q1-message-push.sql" => Some(Q1_SQL),
        "q2-call-push.sql" => Some(Q2_SQL),
        "q3-stream-push.sql" => Some(Q3_SQL),
        "q4-publication-recipients.sql" => Some(Q4_SQL),
        "q5-snapshot-users.sql" => Some(Q5_SQL),
        "q6-snapshot-members.sql" => Some(Q6_SQL),
        "c1-outbox-poll.sql" => Some(C1_SQL),
        _ => None,
    }
}

#[cfg(feature = "postgres-oracle")]
pub mod postgres_oracle {
    use chrono::{DateTime, NaiveDateTime, Utc};
    use postgres::types::{ToSql, Type};
    use postgres::{GenericClient, Row};
    use sha2::{Digest, Sha256};
    use std::time::Instant;
    use uuid::Uuid;

    use super::{
        for_each_seed_statement, hash_fields, parse_timestamp, uuid, CaseId, CaseResult, Result,
        WorkloadScale, FIXED_NOW, RINGING_STARTED_AFTER, SCHEMA_SQL,
    };

    pub fn apply_schema(client: &mut impl GenericClient) -> Result<()> {
        client.batch_execute(SCHEMA_SQL)?;
        Ok(())
    }

    pub fn seed_database(client: &mut impl GenericClient, scale: WorkloadScale) -> Result<()> {
        for_each_seed_statement(scale, |statement| {
            client.batch_execute(statement)?;
            Ok(())
        })
    }

    pub fn execute_case(
        client: &mut impl GenericClient,
        case: CaseId,
        scale: WorkloadScale,
    ) -> Result<CaseResult> {
        let started = Instant::now();
        let statement = client.prepare(&postgres_sql(case))?;
        let now = parse_timestamp(FIXED_NOW).naive_utc();
        let ringing_started_after = parse_timestamp(RINGING_STARTED_AFTER).naive_utc();
        let outbox_0 = fixture_uuid(super::JOB_KIND, 0);
        let outbox_1 = fixture_uuid(super::JOB_KIND, 1);
        let outbox_2 = fixture_uuid(super::JOB_KIND, 2);
        let upstream_0 = fixture_uuid(super::UPSTREAM_KIND, 0);
        let publication_0 = fixture_uuid(super::PUBLICATION_KIND, 0);
        let user_0 = fixture_uuid(super::USER_KIND, 0);
        let sort_key = 100i32;
        let recipient_limit = (scale.stream_recipients + 1) as i64;
        let poll_limit = 4i64;

        let params: &[&(dyn ToSql + Sync)] = match case {
            CaseId::Q1MessagePush => &[&outbox_0, &now],
            CaseId::Q2CallPush => &[&outbox_1, &now, &ringing_started_after],
            CaseId::Q3StreamPush => &[&outbox_2, &now],
            CaseId::Q4PublicationRecipients => {
                &[&upstream_0, &sort_key, &publication_0, &recipient_limit]
            }
            CaseId::Q5SnapshotUsers | CaseId::Q6SnapshotMembers => &[&user_0],
            CaseId::C1OutboxPoll => &[&now, &poll_limit],
        };
        let rows = client.query(&statement, params)?;
        consume_rows(&statement, rows, case, started)
    }

    fn postgres_sql(case: CaseId) -> String {
        let replacements: &[(&str, &str)] = match case {
            CaseId::Q1MessagePush => &[(":outbox_job_id", "$1"), (":now", "$2")],
            CaseId::Q2CallPush => &[
                (":outbox_job_id", "$1"),
                (":now", "$2"),
                (":ringing_started_after", "$3"),
            ],
            CaseId::Q3StreamPush => &[(":outbox_job_id", "$1"), (":now", "$2")],
            CaseId::Q4PublicationRecipients => &[
                (":upstream_id", "$1"),
                (":sort_key", "$2"),
                (":publication_id", "$3"),
                (":recipient_limit", "$4"),
            ],
            CaseId::Q5SnapshotUsers | CaseId::Q6SnapshotMembers => &[(":user_id", "$1")],
            CaseId::C1OutboxPoll => &[(":now", "$1"), (":limit", "$2")],
        };
        let sql = replacements
            .iter()
            .fold(case.sql().to_string(), |sql, (name, value)| {
                sql.replace(name, value)
            });
        if case == CaseId::Q5SnapshotUsers {
            // USER is a PostgreSQL reserved keyword. Keep the frozen consumer
            // fixture byte-for-byte and translate only its relation alias at
            // the dialect adapter boundary.
            sql.replace("AS user", "AS member_user")
                .replace("user.", "member_user.")
        } else {
            sql
        }
    }

    fn consume_rows(
        statement: &postgres::Statement,
        rows: Vec<Row>,
        case: CaseId,
        started: Instant,
    ) -> Result<CaseResult> {
        let mut hasher = Sha256::new();
        let mut canonical_result_bytes = hash_fields(
            &mut hasher,
            statement.columns().iter().map(|column| column.name()),
        );
        for row in &rows {
            let fields = statement
                .columns()
                .iter()
                .enumerate()
                .flat_map(|(index, column)| {
                    let (data_type, value) = canonical_cell(row, index, column.type_());
                    [data_type, value]
                });
            canonical_result_bytes += hash_fields(&mut hasher, fields);
        }
        Ok(CaseResult {
            case,
            rows: rows.len(),
            checksum_sha256: format!("{:x}", hasher.finalize()),
            canonical_result_bytes,
            elapsed: started.elapsed(),
        })
    }

    fn canonical_cell(row: &Row, index: usize, sql_type: &Type) -> (String, String) {
        match *sql_type {
            Type::UUID => canonical_optional(row.get::<_, Option<Uuid>>(index), "Uuid", |value| {
                value.to_string()
            }),
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => canonical_optional(
                row.get::<_, Option<String>>(index),
                "Text",
                std::convert::identity,
            ),
            Type::INT2 => {
                canonical_optional(row.get::<_, Option<i16>>(index), "Integer", |value| {
                    value.to_string()
                })
            }
            Type::INT4 => {
                canonical_optional(row.get::<_, Option<i32>>(index), "Integer", |value| {
                    value.to_string()
                })
            }
            Type::INT8 => {
                canonical_optional(row.get::<_, Option<i64>>(index), "Integer", |value| {
                    value.to_string()
                })
            }
            Type::BOOL => {
                canonical_optional(row.get::<_, Option<bool>>(index), "Boolean", |value| {
                    value.to_string()
                })
            }
            Type::TIMESTAMP => canonical_optional(
                row.get::<_, Option<NaiveDateTime>>(index),
                "Timestamp",
                |value| DateTime::<Utc>::from_naive_utc_and_offset(value, Utc).to_rfc3339(),
            ),
            Type::TIMESTAMPTZ => canonical_optional(
                row.get::<_, Option<DateTime<Utc>>>(index),
                "Timestamp",
                |value| value.to_rfc3339(),
            ),
            ref unsupported => panic!(
                "PostgreSQL oracle returned unsupported type {unsupported} at column {index}"
            ),
        }
    }

    fn canonical_optional<T>(
        value: Option<T>,
        data_type: &str,
        format: impl FnOnce(T) -> String,
    ) -> (String, String) {
        (
            data_type.to_string(),
            value.map_or_else(|| "NULL".to_string(), format),
        )
    }

    fn fixture_uuid(kind: u16, index: usize) -> Uuid {
        Uuid::parse_str(&uuid(kind, index)).expect("compiled fixture UUID must be valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb::storage::instrumentation;

    #[derive(Debug, Clone, Copy)]
    struct JoinWorkShape {
        candidate_pairs: u64,
        lookup_candidates: u64,
        input_rows: u64,
        output_rows: u64,
        materialized_rows: u64,
        materialized_values: u64,
    }

    #[test]
    fn generated_seed_literals_escape_quotes_without_cutting_payload() {
        let payload = "O'Brien — payload kept whole";
        let escaped = quoted(payload);
        assert_eq!(escaped, "'O''Brien — payload kept whole'");
        assert!(escaped.contains("payload kept whole"));
    }

    #[test]
    fn delivery_queries_do_not_cut_recipient_sets() {
        for case in [
            CaseId::Q1MessagePush,
            CaseId::Q2CallPush,
            CaseId::Q3StreamPush,
        ] {
            let sql = case.sql().to_ascii_lowercase();
            assert!(case.name().contains("push"));
            assert!(case.fixture_path().contains("push"));
            assert!(!sql.contains("scan_limit"), "{}", case.name());
            assert!(!sql.contains(" limit "), "{}", case.name());
        }
    }

    #[test]
    fn explain_analyze_keeps_delivery_selection_roots_bounded_after_relation_growth() {
        // Debug builds give libtest workers a small default stack while this
        // production-shaped 8-10 edge query deliberately exercises the full
        // recursive parser/binder/planner/explain pipeline. Release execution
        // fits the ordinary stack; keep the debug acceptance test from making
        // compiler frame size part of the product contract.
        std::thread::Builder::new()
            .name("jr14-relation-growth".to_string())
            .stack_size(16 * 1024 * 1024)
            .spawn(explain_analyze_relation_growth_body)
            .expect("spawn JR14 relation-growth acceptance worker")
            .join()
            .expect("JR14 relation-growth acceptance worker panicked");
    }

    fn explain_analyze_relation_growth_body() {
        let db = Database::open("memory://join_workload_relation_growth")
            .expect("open relation-growth database");
        apply_schema(&db).expect("apply frozen messenger schema");
        let scale = WorkloadScale {
            users: 128,
            devices_per_user: 2,
            conversations: 64,
            members_per_conversation: 8,
            messages: 512,
            stream_recipients: 64,
            publications: 512,
            outbox_jobs: 512,
            insert_batch_rows: 128,
        };
        seed_database(&db, scale).expect("seed relation-growth profile");

        for (case, expected_root_relation, expected_root_alias) in [
            (CaseId::Q1MessagePush, "sync_events", "se"),
            (CaseId::Q2CallPush, "outbox_jobs", "job"),
            (CaseId::Q3StreamPush, "outbox_jobs", "job"),
        ] {
            let lines =
                explain_analyze_case_with_timeout(&db, case, scale, Duration::from_secs(30))
                    .unwrap_or_else(|error| {
                        panic!("{} EXPLAIN ANALYZE failed: {error}", case.name())
                    });
            let plan = lines.join("\n");
            assert!(
                lines.iter().any(|line| {
                    line.contains("Index Scan") && line.contains(expected_root_relation)
                }),
                "{} must retain its selective indexed root after unrelated row growth:\n{plan}",
                case.name()
            );
            assert!(
                plan.lines().any(|line| {
                    line.contains("Physical JOIN Root Edge:")
                        && line.contains("left_rows=1")
                        && line.contains("right_rows=1")
                        && line.contains("output_rows=1")
                }),
                "{} must resolve its first edge from bounded 1x1 inputs; the cost model may choose scan/hash for a tiny inner relation:\n{plan}",
                case.name()
            );
            assert!(
                plan.lines().any(|line| {
                    line.contains("Physical JOIN Costed Order[0]:")
                        && line.contains(&format!(": {expected_root_alias} ->"))
                }),
                "{} costed order must name the required selective root:\n{plan}",
                case.name()
            );

            if matches!(
                case,
                CaseId::Q1MessagePush | CaseId::Q2CallPush | CaseId::Q3StreamPush
            ) {
                let before = instrumentation::snapshot();
                let result = execute_case_with_timeout(&db, case, scale, Duration::from_secs(30))
                    .unwrap_or_else(|error| panic!("{} execution failed: {error}", case.name()));
                let after = instrumentation::snapshot();
                assert!(result.rows > 0, "{} must produce recipients", case.name());
                assert!(
                    after
                        .join_deferred_rows_consumed
                        .saturating_sub(before.join_deferred_rows_consumed)
                        > 0,
                    "{} must carry a complete deferred row across a recursive JOIN boundary",
                    case.name()
                );
                assert_eq!(
                    after
                        .join_rows_constructed
                        .saturating_sub(before.join_rows_constructed),
                    0,
                    "{} must not reconstruct owned rows between recursive JOIN edges",
                    case.name()
                );
            }
        }
    }

    #[test]
    fn q4_q6_work_is_independent_of_unrelated_users_growing_one_hundred_fold() {
        let db = Database::open("memory://join_workload_unrelated_growth")
            .expect("open unrelated-growth database");
        apply_schema(&db).expect("apply frozen messenger schema");
        let scale = WorkloadScale::smoke();
        seed_database(&db, scale).expect("seed smoke profile");

        let before = [
            capture_join_work(&db, CaseId::Q4PublicationRecipients, scale),
            capture_join_work(&db, CaseId::Q5SnapshotUsers, scale),
            capture_join_work(&db, CaseId::Q6SnapshotMembers, scale),
        ];
        insert_unrelated_users(&db, scale.users.saturating_mul(99));
        let after = [
            capture_join_work(&db, CaseId::Q4PublicationRecipients, scale),
            capture_join_work(&db, CaseId::Q5SnapshotUsers, scale),
            capture_join_work(&db, CaseId::Q6SnapshotMembers, scale),
        ];

        for (case, before, after) in [
            (CaseId::Q4PublicationRecipients, before[0], after[0]),
            (CaseId::Q5SnapshotUsers, before[1], after[1]),
            (CaseId::Q6SnapshotMembers, before[2], after[2]),
        ] {
            eprintln!(
                "{} unrelated-users-100x before={before:?} after={after:?}",
                case.name()
            );
            for (metric, baseline, grown) in [
                (
                    "candidate_pairs",
                    before.candidate_pairs,
                    after.candidate_pairs,
                ),
                (
                    "lookup_candidates",
                    before.lookup_candidates,
                    after.lookup_candidates,
                ),
                ("input_rows", before.input_rows, after.input_rows),
                ("output_rows", before.output_rows, after.output_rows),
                (
                    "materialized_rows",
                    before.materialized_rows,
                    after.materialized_rows,
                ),
                (
                    "materialized_values",
                    before.materialized_values,
                    after.materialized_values,
                ),
            ] {
                let ceiling = baseline.saturating_add(baseline.saturating_add(9) / 10);
                assert!(
                    grown <= ceiling,
                    "{} {metric} grew with unrelated users: {baseline} -> {grown} (limit {ceiling}); before={before:#?}, after={after:#?}",
                    case.name()
                );
            }
        }
    }

    fn capture_join_work(db: &Database, case: CaseId, scale: WorkloadScale) -> JoinWorkShape {
        instrumentation::reset();
        let result = execute_case(db, case, scale)
            .unwrap_or_else(|error| panic!("{} execution failed: {error}", case.name()));
        assert_eq!(result.rows, scale.expected_rows(case));
        let counters = instrumentation::snapshot();
        JoinWorkShape {
            candidate_pairs: counters.join_candidate_pairs,
            lookup_candidates: counters.join_lookup_candidate_rows,
            input_rows: counters
                .join_left_input_rows
                .saturating_add(counters.join_right_input_rows),
            output_rows: counters.join_output_rows,
            materialized_rows: counters.row_materialization_rows,
            materialized_values: counters.row_materialization_values,
        }
    }

    fn insert_unrelated_users(db: &Database, count: usize) {
        const BATCH: usize = 128;
        for start in (0..count).step_by(BATCH) {
            let end = (start + BATCH).min(count);
            let mut sql = String::from("INSERT INTO users VALUES ");
            for index in start..end {
                if index > start {
                    sql.push(',');
                }
                let suffix = index + 1;
                write!(
                    sql,
                    "('ffff0000-0100-7000-8000-{suffix:012x}','noise-{suffix}','Noise {suffix}','human',NULL,1,NULL,NULL)"
                )
                .unwrap();
            }
            db.execute(&sql, ())
                .unwrap_or_else(|error| panic!("insert unrelated users {start}..{end}: {error}"));
        }
    }
}
