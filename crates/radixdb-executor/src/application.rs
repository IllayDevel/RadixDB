//! System-owned application relations built on the ordinary MVCC path.
//!
//! Audit and outbox records deliberately use normal catalog tables and the
//! caller's storage transaction.  This module owns only their typed contract
//! and lifecycle; it does not introduce another log, commit marker, or
//! recovery authority.

use chrono::{DateTime, Duration, Utc};
pub use radixdb_catalog::ObjectId;
use radixdb_catalog::{CatalogGeneration, CatalogPayload, ConstraintPayload, ObjectKind};
use radixdb_core::{DataType, Error, Result, Row, Value};
pub use radixdb_procedural::{AuditEvent, OutboxMessage};
use radixdb_storage::traits::{QueryResult, Table};

use crate::context::ExecutionContext;
use crate::procedural::transaction_visible_catalog;
use crate::Executor;

pub const AUDIT_RELATION_NAME: &str = "audit.event";
pub const OUTBOX_RELATION_NAME: &str = "outbox.message";

const MAX_AUDIT_METADATA_ENTRIES: usize = 32;
const MAX_AUDIT_METADATA_KEY_BYTES: usize = 128;
const MAX_AUDIT_METADATA_BYTES: usize = 16 * 1024;
const MAX_OUTBOX_IDEMPOTENCY_KEY_BYTES: usize = 512;
const MAX_OUTBOX_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_WORKER_ID_BYTES: usize = 256;
const MAX_OUTBOX_ERROR_BYTES: usize = 4096;
const MAX_CLAIM_BATCH: usize = 1024;
const MAX_CLAIM_SCAN_ROWS: usize = 65_536;
const MAX_OUTBOX_ATTEMPTS: u32 = 1_000;
const MAX_RETENTION_BATCH: usize = 10_000;

const AUDIT_COLUMNS: &[(&str, DataType, bool)] = &[
    ("event_id", DataType::Uuid, false),
    ("occurred_at", DataType::Timestamp, false),
    ("transaction_id", DataType::Integer, false),
    ("session_principal", DataType::Uuid, false),
    ("effective_principal", DataType::Uuid, false),
    ("object_id", DataType::Uuid, false),
    ("command_fingerprint", DataType::Bytes, false),
    ("outcome", DataType::Text, false),
    ("metadata", DataType::Json, false),
];

const OUTBOX_COLUMNS: &[(&str, DataType, bool)] = &[
    ("message_id", DataType::Uuid, false),
    ("idempotency_key", DataType::Text, false),
    ("schema_version", DataType::Integer, false),
    ("payload", DataType::Json, false),
    ("state", DataType::Text, false),
    ("created_at", DataType::Timestamp, false),
    ("available_at", DataType::Timestamp, false),
    ("lease_owner", DataType::Text, true),
    ("lease_token", DataType::Uuid, true),
    ("lease_expires_at", DataType::Timestamp, true),
    ("attempt_count", DataType::Integer, false),
    ("completed_at", DataType::Timestamp, true),
    ("last_error", DataType::Text, true),
    ("dead_lettered_at", DataType::Timestamp, true),
];

const INSTALL_SQL: &str = r#"
BEGIN;
CREATE TABLE IF NOT EXISTS "audit.event" (
    event_id UUID PRIMARY KEY,
    occurred_at TIMESTAMP NOT NULL,
    transaction_id INTEGER NOT NULL,
    session_principal UUID NOT NULL,
    effective_principal UUID NOT NULL,
    object_id UUID NOT NULL,
    command_fingerprint BYTES NOT NULL,
    outcome TEXT NOT NULL,
    metadata JSON NOT NULL
);
CREATE TABLE IF NOT EXISTS "outbox.message" (
    message_id UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL,
    payload JSON NOT NULL,
    state TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    available_at TIMESTAMP NOT NULL,
    lease_owner TEXT,
    lease_token UUID,
    lease_expires_at TIMESTAMP,
    attempt_count INTEGER NOT NULL,
    completed_at TIMESTAMP,
    last_error TEXT,
    dead_lettered_at TIMESTAMP
);
COMMIT;
"#;

/// Stable per-database identities of the two typed ordinary relations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplicationRelationIdentity {
    pub audit_relation: ObjectId,
    pub outbox_relation: ObjectId,
}

/// One durable lease returned only after its claim transaction commits.
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxClaim {
    pub message_id: [u8; 16],
    pub idempotency_key: String,
    pub schema_version: u32,
    pub payload: Value,
    pub lease_token: [u8; 16],
    pub lease_expires_at: DateTime<Utc>,
    pub attempt: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxCompletion {
    Completed,
    AlreadyCompleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxRetryDisposition {
    Retried,
    DeadLettered,
}

/// Bounded retention work performed by one maintenance transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplicationRetentionPolicy {
    pub audit_retention: Duration,
    pub outbox_retention: Duration,
    pub max_rows_per_relation: usize,
}

impl Default for ApplicationRetentionPolicy {
    fn default() -> Self {
        Self {
            audit_retention: Duration::days(90),
            outbox_retention: Duration::days(30),
            max_rows_per_relation: 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplicationRetentionOutcome {
    pub audit_rows_deleted: usize,
    pub outbox_rows_deleted: usize,
}

impl Executor {
    /// Install both relation contracts in one ordinary transaction.
    ///
    /// Existing names are accepted only when owner and ordered schema match
    /// exactly. Opening a database never invokes this method implicitly.
    pub fn install_application_relations(&self) -> Result<ApplicationRelationIdentity> {
        if self.has_active_transaction() {
            return Err(Error::invalid_argument(
                "application relations cannot be installed inside an active transaction",
            ));
        }

        let catalog = self.engine.pin_catalog()?;
        validate_relation_if_present(catalog.as_ref(), AUDIT_RELATION_NAME, AUDIT_COLUMNS)?;
        validate_relation_if_present(catalog.as_ref(), OUTBOX_RELATION_NAME, OUTBOX_COLUMNS)?;
        drop(catalog);

        let mut result = self.execute(INSTALL_SQL)?;
        drain_result(result.as_mut())?;
        result.close()?;
        self.application_relation_identity()
    }

    /// Resolve and validate the durable catalog identity without mutating it.
    pub fn application_relation_identity(&self) -> Result<ApplicationRelationIdentity> {
        let (catalog, _) = transaction_visible_catalog(self)?;
        Ok(ApplicationRelationIdentity {
            audit_relation: validate_relation(
                catalog.as_ref(),
                AUDIT_RELATION_NAME,
                AUDIT_COLUMNS,
            )?,
            outbox_relation: validate_relation(
                catalog.as_ref(),
                OUTBOX_RELATION_NAME,
                OUTBOX_COLUMNS,
            )?,
        })
    }

    /// Append one immutable audit success record to the caller transaction.
    /// Parameter values are never captured implicitly.
    pub fn append_audit_event(
        &self,
        context: &ExecutionContext,
        event: AuditEvent,
    ) -> Result<[u8; 16]> {
        self.application_relation_identity()?;
        let metadata = encode_audit_metadata(&event.metadata)?;
        let transaction_id = self
            .active_transaction_id()
            .ok_or(Error::TransactionNotStarted)?;
        let event_id = *uuid::Uuid::now_v7().as_bytes();
        let occurred_at = Utc::now();
        let row = Row::from_values(vec![
            Value::uuid(event_id),
            Value::timestamp(occurred_at),
            Value::Integer(transaction_id),
            Value::uuid(context.principal_id().into_bytes()),
            Value::uuid(context.effective_principal_id().into_bytes()),
            Value::uuid(event.object_id.into_bytes()),
            Value::bytes(event.command_fingerprint.to_vec()),
            Value::text("success"),
            Value::json(metadata),
        ]);
        self.insert_active_system_row(AUDIT_RELATION_NAME, row)?;
        Ok(event_id)
    }

    /// Append one typed external side-effect intent to the caller transaction.
    pub fn append_outbox_message(&self, message: OutboxMessage) -> Result<[u8; 16]> {
        self.application_relation_identity()?;
        validate_outbox_message(&message)?;
        let message_id = *uuid::Uuid::now_v7().as_bytes();
        let created_at = Utc::now();
        let payload = message
            .payload
            .as_json()
            .expect("validated JSON payload")
            .to_owned();
        let row = Row::from_values(vec![
            Value::uuid(message_id),
            Value::text(message.idempotency_key),
            Value::Integer(i64::from(message.schema_version)),
            Value::json(payload),
            Value::text("pending"),
            Value::timestamp(created_at),
            Value::timestamp(created_at),
            Value::Null(DataType::Text),
            Value::Null(DataType::Uuid),
            Value::Null(DataType::Timestamp),
            Value::Integer(0),
            Value::Null(DataType::Timestamp),
            Value::Null(DataType::Text),
            Value::Null(DataType::Timestamp),
        ]);
        self.insert_active_system_row(OUTBOX_RELATION_NAME, row)?;
        Ok(message_id)
    }

    /// Atomically claim pending or expired-lease messages for one worker.
    ///
    /// The scan and returned batch are both bounded. A conflicting claimant
    /// loses at the ordinary MVCC write-claim/commit boundary and receives no
    /// unpublished lease records.
    pub fn claim_outbox(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_duration: Duration,
        limit: usize,
        max_attempts: u32,
    ) -> Result<Vec<OutboxClaim>> {
        validate_claim_request(worker_id, lease_duration, limit, max_attempts)?;
        self.with_owned_application_transaction(|executor| {
            executor.claim_outbox_inside(worker_id, now, lease_duration, limit, max_attempts)
        })
    }

    /// Durably mark delivery complete. Repeating the same token is idempotent.
    pub fn complete_outbox(
        &self,
        message_id: [u8; 16],
        lease_token: [u8; 16],
        completed_at: DateTime<Utc>,
    ) -> Result<OutboxCompletion> {
        self.with_owned_application_transaction(|executor| {
            executor.complete_outbox_inside(message_id, lease_token, completed_at)
        })
    }

    /// Release a failed delivery for retry or move it to the dead-letter state.
    pub fn retry_outbox(
        &self,
        message_id: [u8; 16],
        lease_token: [u8; 16],
        failed_at: DateTime<Utc>,
        retry_at: DateTime<Utc>,
        error: &str,
        max_attempts: u32,
    ) -> Result<OutboxRetryDisposition> {
        if error.len() > MAX_OUTBOX_ERROR_BYTES {
            return Err(Error::invalid_argument(format!(
                "outbox delivery error exceeds {MAX_OUTBOX_ERROR_BYTES} bytes"
            )));
        }
        if max_attempts == 0 || max_attempts > MAX_OUTBOX_ATTEMPTS {
            return Err(Error::invalid_argument(
                "outbox max_attempts is outside the supported range",
            ));
        }
        self.with_owned_application_transaction(|executor| {
            executor.retry_outbox_inside(
                message_id,
                lease_token,
                failed_at,
                retry_at,
                error,
                max_attempts,
            )
        })
    }

    /// Delete only expired immutable audit history and terminal outbox rows.
    pub fn prune_application_history(
        &self,
        now: DateTime<Utc>,
        policy: ApplicationRetentionPolicy,
    ) -> Result<ApplicationRetentionOutcome> {
        validate_retention_policy(policy)?;
        self.with_owned_application_transaction(|executor| {
            executor.prune_application_history_inside(now, policy)
        })
    }

    fn with_owned_application_transaction<T>(
        &self,
        operation: impl FnOnce(&Self) -> Result<T>,
    ) -> Result<T> {
        if self.has_active_transaction() {
            return Err(Error::invalid_argument(
                "outbox worker lifecycle requires a connection without an active transaction",
            ));
        }
        self.application_relation_identity()?;
        let boundary = self.begin_procedural_boundary()?;
        match operation(self) {
            Ok(value) => match self.complete_procedural_boundary(&boundary) {
                Ok(()) => Ok(value),
                Err(error) => {
                    let _ = self.abort_procedural_boundary(&boundary);
                    Err(error)
                }
            },
            Err(error) => {
                let _ = self.abort_procedural_boundary(&boundary);
                Err(error)
            }
        }
    }

    fn claim_outbox_inside(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_duration: Duration,
        limit: usize,
        max_attempts: u32,
    ) -> Result<Vec<OutboxClaim>> {
        let mut table = self.active_system_table(OUTBOX_RELATION_NAME)?;
        let rows = scan_bounded(&*table, MAX_CLAIM_SCAN_ROWS)?;
        let mut candidates = Vec::new();
        let mut exhausted = Vec::new();
        for (row_id, row) in rows {
            if !outbox_row_is_claimable(&row, now)? {
                continue;
            }
            let attempt = row_u32(&row, 10, "attempt_count")?;
            if attempt >= max_attempts {
                exhausted.push(row_id);
            } else {
                candidates.push((row_id, row));
            }
        }
        candidates.sort_by(|left, right| {
            row_timestamp(&left.1, 6, "available_at")
                .expect("validated candidate timestamp")
                .cmp(
                    &row_timestamp(&right.1, 6, "available_at")
                        .expect("validated candidate timestamp"),
                )
                .then_with(|| left.0.cmp(&right.0))
        });
        candidates.truncate(limit);

        let mut claimed = Vec::with_capacity(candidates.len());
        let mut row_ids = exhausted.clone();
        row_ids.extend(candidates.iter().map(|(row_id, _)| *row_id));
        row_ids.sort_unstable();
        row_ids.dedup();
        table.try_claim_rows(&row_ids)?;

        for row_id in exhausted {
            let mut setter = |mut row: Row| -> Result<(Row, bool)> {
                if !outbox_row_is_claimable(&row, now)?
                    || row_u32(&row, 10, "attempt_count")? < max_attempts
                {
                    return Ok((row, false));
                }
                row.set(4, Value::text("dead_lettered"))?;
                row.set(7, Value::Null(DataType::Text))?;
                row.set(8, Value::Null(DataType::Uuid))?;
                row.set(9, Value::Null(DataType::Timestamp))?;
                row.set(13, Value::timestamp(now))?;
                Ok((row, true))
            };
            table.update_by_row_ids(&[row_id], &mut setter)?;
        }

        for (row_id, original) in candidates {
            let lease_token = *uuid::Uuid::now_v7().as_bytes();
            let lease_expires_at = now + lease_duration;
            let attempt = row_u32(&original, 10, "attempt_count")?
                .checked_add(1)
                .ok_or_else(|| Error::invalid_argument("outbox attempt counter overflow"))?;
            let mut setter = |mut row: Row| -> Result<(Row, bool)> {
                if !outbox_row_is_claimable(&row, now)? {
                    return Ok((row, false));
                }
                row.set(4, Value::text("claimed"))?;
                row.set(7, Value::text(worker_id))?;
                row.set(8, Value::uuid(lease_token))?;
                row.set(9, Value::timestamp(lease_expires_at))?;
                row.set(10, Value::Integer(i64::from(attempt)))?;
                Ok((row, true))
            };
            if table.update_by_row_ids(&[row_id], &mut setter)? != 1 {
                return Err(Error::TransactionSerializationConflict { row_id });
            }
            claimed.push(OutboxClaim {
                message_id: row_uuid(&original, 0, "message_id")?,
                idempotency_key: row_text(&original, 1, "idempotency_key")?.to_owned(),
                schema_version: row_u32(&original, 2, "schema_version")?,
                payload: original
                    .get(3)
                    .cloned()
                    .ok_or_else(|| Error::internal("outbox payload column is missing"))?,
                lease_token,
                lease_expires_at,
                attempt,
            });
        }
        Ok(claimed)
    }

    fn complete_outbox_inside(
        &self,
        message_id: [u8; 16],
        lease_token: [u8; 16],
        completed_at: DateTime<Utc>,
    ) -> Result<OutboxCompletion> {
        let mut table = self.active_system_table(OUTBOX_RELATION_NAME)?;
        let row_id = find_unique_row_id(&*table, "message_id", Value::uuid(message_id))?
            .ok_or_else(|| Error::invalid_argument("outbox message does not exist"))?;
        table.try_claim_rows(&[row_id])?;
        let mut disposition = None;
        let mut setter = |mut row: Row| -> Result<(Row, bool)> {
            let state = row_text(&row, 4, "state")?;
            let token = row_optional_uuid(&row, 8, "lease_token")?;
            if state == "completed" && token == Some(lease_token) {
                disposition = Some(OutboxCompletion::AlreadyCompleted);
                return Ok((row, false));
            }
            if state != "claimed" || token != Some(lease_token) {
                return Err(Error::invalid_argument(
                    "outbox completion does not own the active lease",
                ));
            }
            let expires = row_timestamp(&row, 9, "lease_expires_at")?;
            if completed_at > expires {
                return Err(Error::invalid_argument(
                    "outbox completion lease has expired",
                ));
            }
            row.set(4, Value::text("completed"))?;
            row.set(9, Value::Null(DataType::Timestamp))?;
            row.set(11, Value::timestamp(completed_at))?;
            disposition = Some(OutboxCompletion::Completed);
            Ok((row, true))
        };
        table.update_by_row_ids(&[row_id], &mut setter)?;
        disposition.ok_or_else(|| Error::internal("outbox completion produced no disposition"))
    }

    fn retry_outbox_inside(
        &self,
        message_id: [u8; 16],
        lease_token: [u8; 16],
        failed_at: DateTime<Utc>,
        retry_at: DateTime<Utc>,
        error: &str,
        max_attempts: u32,
    ) -> Result<OutboxRetryDisposition> {
        let mut table = self.active_system_table(OUTBOX_RELATION_NAME)?;
        let row_id = find_unique_row_id(&*table, "message_id", Value::uuid(message_id))?
            .ok_or_else(|| Error::invalid_argument("outbox message does not exist"))?;
        table.try_claim_rows(&[row_id])?;
        let mut disposition = None;
        let mut setter = |mut row: Row| -> Result<(Row, bool)> {
            if row_text(&row, 4, "state")? != "claimed"
                || row_optional_uuid(&row, 8, "lease_token")? != Some(lease_token)
            {
                return Err(Error::invalid_argument(
                    "outbox retry does not own the active lease",
                ));
            }
            if failed_at > row_timestamp(&row, 9, "lease_expires_at")? {
                return Err(Error::invalid_argument("outbox retry lease has expired"));
            }
            let attempts = row_u32(&row, 10, "attempt_count")?;
            let dead = attempts >= max_attempts;
            row.set(
                4,
                Value::text(if dead { "dead_lettered" } else { "pending" }),
            )?;
            row.set(7, Value::Null(DataType::Text))?;
            row.set(8, Value::Null(DataType::Uuid))?;
            row.set(9, Value::Null(DataType::Timestamp))?;
            row.set(12, Value::text(error))?;
            if dead {
                row.set(13, Value::timestamp(failed_at))?;
                disposition = Some(OutboxRetryDisposition::DeadLettered);
            } else {
                row.set(6, Value::timestamp(retry_at))?;
                disposition = Some(OutboxRetryDisposition::Retried);
            }
            Ok((row, true))
        };
        table.update_by_row_ids(&[row_id], &mut setter)?;
        disposition.ok_or_else(|| Error::internal("outbox retry produced no disposition"))
    }

    fn prune_application_history_inside(
        &self,
        now: DateTime<Utc>,
        policy: ApplicationRetentionPolicy,
    ) -> Result<ApplicationRetentionOutcome> {
        let audit_cutoff = now - policy.audit_retention;
        let outbox_cutoff = now - policy.outbox_retention;
        let mut audit = self.active_system_table(AUDIT_RELATION_NAME)?;
        let audit_rows = scan_bounded(&*audit, policy.max_rows_per_relation)?;
        let audit_ids = audit_rows
            .into_iter()
            .filter_map(|(row_id, row)| {
                row_timestamp(&row, 1, "occurred_at")
                    .map(|timestamp| (timestamp < audit_cutoff).then_some(row_id))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        audit.try_claim_rows_for_delete(&audit_ids)?;
        let audit_rows_deleted = usize::try_from(audit.delete_by_row_ids(&audit_ids)?)
            .map_err(|_| Error::internal("negative audit retention delete count"))?;

        let mut outbox = self.active_system_table(OUTBOX_RELATION_NAME)?;
        let outbox_rows = scan_bounded(&*outbox, policy.max_rows_per_relation)?;
        let outbox_ids = outbox_rows
            .into_iter()
            .filter_map(|(row_id, row)| match terminal_outbox_timestamp(&row) {
                Ok(Some(timestamp)) if timestamp < outbox_cutoff => Some(Ok(row_id)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>>>()?;
        outbox.try_claim_rows_for_delete(&outbox_ids)?;
        let outbox_rows_deleted = usize::try_from(outbox.delete_by_row_ids(&outbox_ids)?)
            .map_err(|_| Error::internal("negative outbox retention delete count"))?;
        Ok(ApplicationRetentionOutcome {
            audit_rows_deleted,
            outbox_rows_deleted,
        })
    }

    fn active_system_table(&self, relation: &str) -> Result<Box<dyn Table>> {
        let mut active = self.active_transaction.lock().unwrap();
        let state = active.as_mut().ok_or(Error::TransactionNotStarted)?;
        let table = state.transaction.get_table(relation)?;
        if !state.tables.contains_key(relation) {
            state
                .tables
                .insert(relation.to_owned(), state.transaction.get_table(relation)?);
        }
        Ok(table)
    }

    fn insert_active_system_row(&self, relation: &str, row: Row) -> Result<()> {
        let mut active = self.active_transaction.lock().unwrap();
        let state = active.as_mut().ok_or(Error::TransactionNotStarted)?;
        let mut table = state.transaction.get_table(relation)?;
        if !state.tables.contains_key(relation) {
            state
                .tables
                .insert(relation.to_owned(), state.transaction.get_table(relation)?);
        }
        drop(active);
        table.insert_discard(row)
    }
}

fn validate_claim_request(
    worker_id: &str,
    lease_duration: Duration,
    limit: usize,
    max_attempts: u32,
) -> Result<()> {
    if worker_id.is_empty() || worker_id.len() > MAX_WORKER_ID_BYTES {
        return Err(Error::invalid_argument(format!(
            "outbox worker ID must contain 1..={MAX_WORKER_ID_BYTES} bytes"
        )));
    }
    if lease_duration < Duration::seconds(1) || lease_duration > Duration::hours(24) {
        return Err(Error::invalid_argument(
            "outbox lease duration must be between 1 second and 24 hours",
        ));
    }
    if limit == 0 || limit > MAX_CLAIM_BATCH {
        return Err(Error::invalid_argument(format!(
            "outbox claim batch must contain 1..={MAX_CLAIM_BATCH} rows"
        )));
    }
    if max_attempts == 0 || max_attempts > MAX_OUTBOX_ATTEMPTS {
        return Err(Error::invalid_argument(
            "outbox max_attempts is outside the supported range",
        ));
    }
    Ok(())
}

fn validate_retention_policy(policy: ApplicationRetentionPolicy) -> Result<()> {
    if policy.audit_retention < Duration::zero()
        || policy.outbox_retention < Duration::zero()
        || policy.max_rows_per_relation == 0
        || policy.max_rows_per_relation > MAX_RETENTION_BATCH
    {
        return Err(Error::invalid_argument(format!(
            "retention must be non-negative and each pass must delete 1..={MAX_RETENTION_BATCH} rows per relation"
        )));
    }
    Ok(())
}

fn scan_bounded(table: &dyn Table, limit: usize) -> Result<Vec<(i64, Row)>> {
    let projection = (0..table.schema().columns.len()).collect::<Vec<_>>();
    let mut scanner = table.scan(&projection, None)?;
    let mut rows = Vec::new();
    while rows.len() < limit && scanner.next() {
        rows.push(scanner.take_row_with_id()?);
    }
    if let Some(error) = scanner.err().cloned() {
        let _ = scanner.close();
        return Err(error);
    }
    scanner.close()?;
    Ok(rows)
}

fn find_unique_row_id(table: &dyn Table, column: &str, value: Value) -> Result<Option<i64>> {
    if let Some(row_ids) =
        table.collect_row_ids_by_index_values(column, std::slice::from_ref(&value))
    {
        let row_ids = row_ids?;
        return match row_ids.as_slice() {
            [] => Ok(None),
            [row_id] => Ok(Some(*row_id)),
            _ => Err(Error::internal(format!(
                "unique system relation column '{column}' returned multiple rows"
            ))),
        };
    }
    let column_index = table
        .schema()
        .find_column(column)
        .map(|(index, _)| index)
        .ok_or_else(|| Error::internal(format!("system relation column '{column}' is missing")))?;
    let mut scanner = table.scan(&[column_index], None)?;
    let mut found = None;
    while scanner.next() {
        let (row_id, row) = scanner.take_row_with_id()?;
        if row.get(0) == Some(&value) && found.replace(row_id).is_some() {
            let _ = scanner.close();
            return Err(Error::internal(format!(
                "unique system relation column '{column}' returned multiple rows"
            )));
        }
    }
    if let Some(error) = scanner.err().cloned() {
        let _ = scanner.close();
        return Err(error);
    }
    scanner.close()?;
    Ok(found)
}

fn outbox_row_is_claimable(row: &Row, now: DateTime<Utc>) -> Result<bool> {
    match row_text(row, 4, "state")? {
        "pending" => Ok(row_timestamp(row, 6, "available_at")? <= now),
        "claimed" => Ok(row_timestamp(row, 9, "lease_expires_at")? <= now),
        "completed" | "dead_lettered" => Ok(false),
        state => Err(Error::invalid_argument(format!(
            "outbox row contains unknown state '{state}'"
        ))),
    }
}

fn terminal_outbox_timestamp(row: &Row) -> Result<Option<DateTime<Utc>>> {
    match row_text(row, 4, "state")? {
        "completed" => row_optional_timestamp(row, 11, "completed_at"),
        "dead_lettered" => row_optional_timestamp(row, 13, "dead_lettered_at"),
        "pending" | "claimed" => Ok(None),
        state => Err(Error::invalid_argument(format!(
            "outbox row contains unknown state '{state}'"
        ))),
    }
}

fn row_text<'a>(row: &'a Row, index: usize, column: &str) -> Result<&'a str> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(value.as_str()),
        _ => Err(Error::internal(format!(
            "system relation column '{column}' is not TEXT"
        ))),
    }
}

fn row_u32(row: &Row, index: usize, column: &str) -> Result<u32> {
    match row.get(index) {
        Some(Value::Integer(value)) => u32::try_from(*value).map_err(|_| {
            Error::invalid_argument(format!("system relation column '{column}' is outside u32"))
        }),
        _ => Err(Error::internal(format!(
            "system relation column '{column}' is not INTEGER"
        ))),
    }
}

fn row_uuid(row: &Row, index: usize, column: &str) -> Result<[u8; 16]> {
    row.get(index)
        .and_then(Value::as_uuid_bytes)
        .ok_or_else(|| Error::internal(format!("system relation column '{column}' is not UUID")))
}

fn row_optional_uuid(row: &Row, index: usize, column: &str) -> Result<Option<[u8; 16]>> {
    match row.get(index) {
        Some(Value::Null(_)) => Ok(None),
        Some(value) => value.as_uuid_bytes().map(Some).ok_or_else(|| {
            Error::internal(format!("system relation column '{column}' is not UUID"))
        }),
        None => Err(Error::internal(format!(
            "system relation column '{column}' is missing"
        ))),
    }
}

fn row_timestamp(row: &Row, index: usize, column: &str) -> Result<DateTime<Utc>> {
    match row.get(index) {
        Some(Value::Timestamp(value)) => Ok(*value),
        _ => Err(Error::internal(format!(
            "system relation column '{column}' is not TIMESTAMP"
        ))),
    }
}

fn row_optional_timestamp(row: &Row, index: usize, column: &str) -> Result<Option<DateTime<Utc>>> {
    match row.get(index) {
        Some(Value::Null(_)) => Ok(None),
        Some(Value::Timestamp(value)) => Ok(Some(*value)),
        _ => Err(Error::internal(format!(
            "system relation column '{column}' is not TIMESTAMP"
        ))),
    }
}

fn drain_result(result: &mut dyn QueryResult) -> Result<()> {
    while result.next() {
        drop(result.take_row());
    }
    if let Some(error) = result.last_error() {
        return Err(error);
    }
    Ok(())
}

fn validate_relation_if_present(
    catalog: &CatalogGeneration,
    name: &str,
    columns: &[(&str, DataType, bool)],
) -> Result<()> {
    if catalog
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, name)
        .map_err(catalog_error)?
        .is_some()
    {
        validate_relation(catalog, name, columns)?;
    }
    Ok(())
}

fn validate_relation(
    catalog: &CatalogGeneration,
    name: &str,
    columns: &[(&str, DataType, bool)],
) -> Result<ObjectId> {
    let relation = catalog
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, name)
        .map_err(catalog_error)?
        .ok_or_else(|| Error::TableNotFound(name.to_owned()))?;
    if relation.kind() != ObjectKind::Table
        || relation.owner_principal_id() != ObjectId::BOOTSTRAP_OWNER
    {
        return Err(Error::invalid_argument(format!(
            "system relation '{name}' has an invalid kind or owner"
        )));
    }
    let CatalogPayload::Table(payload) = relation.payload() else {
        return Err(Error::internal("table catalog payload kind mismatch"));
    };
    if payload.column_ids().len() != columns.len() {
        return Err(Error::invalid_argument(format!(
            "system relation '{name}' has an incompatible column count"
        )));
    }
    for (column_id, (expected_name, expected_type, expected_nullable)) in
        payload.column_ids().iter().zip(columns)
    {
        let column = catalog
            .object(*column_id)
            .ok_or_else(|| Error::internal("system relation column is missing"))?;
        let CatalogPayload::Column(column_payload) = column.payload() else {
            return Err(Error::internal("system relation child is not a column"));
        };
        if column.name().normalized().as_str() != *expected_name
            || column_payload.data_type().logical_type() != *expected_type
            || column_payload.nullable() != *expected_nullable
        {
            return Err(Error::invalid_argument(format!(
                "system relation '{name}' column contract mismatch at '{expected_name}'"
            )));
        }
    }
    validate_relation_keys(catalog, name, payload)?;
    Ok(relation.id())
}

fn validate_relation_keys(
    catalog: &CatalogGeneration,
    name: &str,
    table: &radixdb_catalog::TablePayload,
) -> Result<()> {
    let expected_primary_key = table
        .column_ids()
        .first()
        .copied()
        .ok_or_else(|| Error::internal("system relation has no identity column"))?;
    let primary_key = table
        .primary_key_constraint_id()
        .and_then(|id| catalog.object(id))
        .and_then(|object| match object.payload() {
            CatalogPayload::Constraint(ConstraintPayload::PrimaryKey { local_column_ids }) => {
                Some(local_column_ids.as_slice())
            }
            _ => None,
        });
    if primary_key != Some(std::slice::from_ref(&expected_primary_key)) {
        return Err(Error::invalid_argument(format!(
            "system relation '{name}' has an incompatible primary key"
        )));
    }

    if name == OUTBOX_RELATION_NAME {
        let expected_idempotency_key = table.column_ids()[1];
        let has_unique_idempotency_key = table.constraint_ids().iter().any(|id| {
            catalog.object(*id).is_some_and(|object| {
                matches!(
                    object.payload(),
                    CatalogPayload::Constraint(ConstraintPayload::Unique { local_column_ids })
                        if local_column_ids.as_slice()
                            == std::slice::from_ref(&expected_idempotency_key)
                )
            })
        });
        if !has_unique_idempotency_key {
            return Err(Error::invalid_argument(format!(
                "system relation '{name}' lacks its idempotency-key uniqueness contract"
            )));
        }
    }
    Ok(())
}

fn validate_outbox_message(message: &OutboxMessage) -> Result<()> {
    if message.idempotency_key.is_empty()
        || message.idempotency_key.len() > MAX_OUTBOX_IDEMPOTENCY_KEY_BYTES
    {
        return Err(Error::invalid_argument(format!(
            "outbox idempotency key must contain 1..={MAX_OUTBOX_IDEMPOTENCY_KEY_BYTES} bytes"
        )));
    }
    if message.schema_version == 0 {
        return Err(Error::invalid_argument(
            "outbox payload schema version must be positive",
        ));
    }
    let payload = message
        .payload
        .as_json()
        .ok_or_else(|| Error::invalid_argument("outbox payload must be a validated JSON value"))?;
    if payload.len() > MAX_OUTBOX_PAYLOAD_BYTES {
        return Err(Error::invalid_argument(format!(
            "outbox payload exceeds {MAX_OUTBOX_PAYLOAD_BYTES} bytes"
        )));
    }
    Ok(())
}

fn encode_audit_metadata(metadata: &Value) -> Result<String> {
    let encoded = metadata
        .as_json()
        .ok_or_else(|| Error::invalid_argument("audit metadata must be a validated JSON object"))?;
    let parsed: serde_json::Value = serde_json::from_str(encoded)
        .map_err(|error| Error::invalid_argument(format!("invalid audit JSON: {error}")))?;
    let object = parsed
        .as_object()
        .ok_or_else(|| Error::invalid_argument("audit metadata must be a JSON object"))?;
    if object.len() > MAX_AUDIT_METADATA_ENTRIES {
        return Err(Error::invalid_argument(format!(
            "audit metadata exceeds {MAX_AUDIT_METADATA_ENTRIES} entries"
        )));
    }
    for key in object.keys() {
        if key.is_empty() || key.len() > MAX_AUDIT_METADATA_KEY_BYTES {
            return Err(Error::invalid_argument(format!(
                "audit metadata key must contain 1..={MAX_AUDIT_METADATA_KEY_BYTES} bytes"
            )));
        }
        let lowered = key.to_ascii_lowercase();
        if ["password", "secret", "token", "credential", "authorization"]
            .iter()
            .any(|needle| lowered.contains(needle))
        {
            return Err(Error::invalid_argument(format!(
                "audit metadata key '{key}' is secret-bearing"
            )));
        }
    }
    let encoded = serde_json::Value::Object(object.clone()).to_string();
    if encoded.len() > MAX_AUDIT_METADATA_BYTES {
        return Err(Error::invalid_argument(format!(
            "audit metadata exceeds {MAX_AUDIT_METADATA_BYTES} encoded bytes"
        )));
    }
    Ok(encoded)
}

fn catalog_error(error: impl std::fmt::Display) -> Error {
    Error::invalid_argument(format!("invalid system relation catalog: {error}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use chrono::Duration;
    use radixdb_procedural::{AuditEvent, OutboxMessage};
    use radixdb_storage::config::Config;
    use radixdb_storage::mvcc::engine::MVCCEngine;

    use super::*;

    fn executor() -> Executor {
        let engine = MVCCEngine::in_memory();
        engine.open_engine().unwrap();
        Executor::new(Arc::new(engine))
    }

    fn persistent_executor(path: &std::path::Path) -> (Executor, Arc<MVCCEngine>) {
        let mut config = Config::with_path(path.to_string_lossy().to_string());
        config.persistence.checkpoint_on_close = true;
        let engine = Arc::new(MVCCEngine::new_with_composition_binders(
            config,
            crate::mutation::partial_index::bind_from_sql,
            crate::mutation::row_validation::bind,
            crate::mutation::view_binding::bind_from_sql,
            radixdb_storage::mvcc::engine::CatalogRuntimeBinder::new(
                crate::catalog::bind_runtime_catalog,
            ),
        ));
        engine.open_engine().unwrap();
        (Executor::new(Arc::clone(&engine)), engine)
    }

    fn scalar_count(executor: &Executor, relation: &str) -> i64 {
        let mut result = executor
            .execute(&format!("SELECT COUNT(*) FROM \"{relation}\""))
            .unwrap();
        assert!(result.next());
        let row = result.take_row();
        let Value::Integer(count) = row.get(0).unwrap() else {
            panic!("COUNT must return INTEGER")
        };
        *count
    }

    fn enqueue(executor: &Executor, key: &str) -> [u8; 16] {
        let boundary = executor.begin_procedural_boundary().unwrap();
        let id = executor
            .append_outbox_message(OutboxMessage {
                idempotency_key: key.to_owned(),
                schema_version: 1,
                payload: Value::json(format!(r#"{{"key":"{key}"}}"#)),
            })
            .unwrap();
        executor.complete_procedural_boundary(&boundary).unwrap();
        id
    }

    #[test]
    fn relations_install_atomically_and_keep_stable_catalog_identity() {
        let executor = executor();
        let first = executor.install_application_relations().unwrap();
        let second = executor.install_application_relations().unwrap();
        assert_eq!(first, second);
        assert_eq!(scalar_count(&executor, AUDIT_RELATION_NAME), 0);
        assert_eq!(scalar_count(&executor, OUTBOX_RELATION_NAME), 0);

        let mut unquoted = executor
            .execute("SELECT COUNT(*) FROM audit.event")
            .unwrap();
        assert!(unquoted.next());
        assert_eq!(unquoted.row().get(0), Some(&Value::Integer(0)));
        unquoted.close().unwrap();
    }

    #[test]
    fn installer_rejects_lookalike_relations_without_required_keys() {
        let executor = executor();
        executor
            .execute(
                r#"CREATE TABLE "outbox.message" (
                    message_id UUID PRIMARY KEY,
                    idempotency_key TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    payload JSON NOT NULL,
                    state TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    available_at TIMESTAMP NOT NULL,
                    lease_owner TEXT,
                    lease_token UUID,
                    lease_expires_at TIMESTAMP,
                    attempt_count INTEGER NOT NULL,
                    completed_at TIMESTAMP,
                    last_error TEXT,
                    dead_lettered_at TIMESTAMP
                )"#,
            )
            .unwrap();

        let error = executor.install_application_relations().unwrap_err();
        assert!(error.to_string().contains("idempotency-key uniqueness"));
        assert!(executor
            .engine()
            .pin_catalog()
            .unwrap()
            .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, AUDIT_RELATION_NAME)
            .unwrap()
            .is_none());
    }

    #[test]
    fn system_relation_schema_and_immutable_audit_history_are_protected() {
        let executor = executor();
        executor.install_application_relations().unwrap();

        for statement in [
            "INSERT INTO audit.event VALUES (1)",
            "INSERT INTO outbox.message VALUES (1)",
            "UPDATE audit.event SET outcome = 'forged'",
            "DELETE FROM audit.event",
            "TRUNCATE TABLE audit.event",
            "TRUNCATE TABLE outbox.message",
            "DROP TABLE audit.event",
            "DROP TABLE outbox.message",
            "ALTER TABLE \"audit.event\" ADD COLUMN forged TEXT",
            "ALTER TABLE \"outbox.message\" ADD COLUMN forged TEXT",
        ] {
            assert!(
                matches!(
                    executor.execute(statement),
                    Err(Error::AuthorizationDenied(_))
                ),
                "system relation mutation unexpectedly passed: {statement}"
            );
        }
    }

    #[test]
    fn audit_and_outbox_share_caller_commit_and_rollback() {
        let executor = executor();
        executor.install_application_relations().unwrap();
        let context = ExecutionContext::new();

        let rollback = executor.begin_procedural_boundary().unwrap();
        executor
            .append_audit_event(
                &context,
                AuditEvent {
                    object_id: ObjectId::BOOTSTRAP_NAMESPACE,
                    command_fingerprint: [7; 32],
                    metadata: Value::json(r#"{"command":"update"}"#),
                },
            )
            .unwrap();
        executor
            .append_outbox_message(OutboxMessage {
                idempotency_key: "rollback-key".to_owned(),
                schema_version: 1,
                payload: Value::json(r#"{"kind":"rollback"}"#),
            })
            .unwrap();
        executor.abort_procedural_boundary(&rollback).unwrap();
        assert_eq!(scalar_count(&executor, AUDIT_RELATION_NAME), 0);
        assert_eq!(scalar_count(&executor, OUTBOX_RELATION_NAME), 0);

        let commit = executor.begin_procedural_boundary().unwrap();
        executor
            .append_audit_event(
                &context,
                AuditEvent {
                    object_id: ObjectId::BOOTSTRAP_NAMESPACE,
                    command_fingerprint: [8; 32],
                    metadata: Value::json("{}"),
                },
            )
            .unwrap();
        executor
            .append_outbox_message(OutboxMessage {
                idempotency_key: "commit-key".to_owned(),
                schema_version: 1,
                payload: Value::json(r#"{"kind":"commit"}"#),
            })
            .unwrap();
        executor.complete_procedural_boundary(&commit).unwrap();
        assert_eq!(scalar_count(&executor, AUDIT_RELATION_NAME), 1);
        assert_eq!(scalar_count(&executor, OUTBOX_RELATION_NAME), 1);
    }

    #[test]
    fn metadata_and_payload_contracts_fail_closed() {
        let executor = executor();
        executor.install_application_relations().unwrap();
        let boundary = executor.begin_procedural_boundary().unwrap();
        let error = executor
            .append_audit_event(
                &ExecutionContext::new(),
                AuditEvent {
                    object_id: ObjectId::BOOTSTRAP_NAMESPACE,
                    command_fingerprint: [0; 32],
                    metadata: Value::json(r#"{"password":"do-not-store"}"#),
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("secret-bearing"));
        let error = executor
            .append_outbox_message(OutboxMessage {
                idempotency_key: "not-json".to_owned(),
                schema_version: 1,
                payload: Value::text("not-json"),
            })
            .unwrap_err();
        assert!(error.to_string().contains("validated JSON"));
        executor.abort_procedural_boundary(&boundary).unwrap();
    }

    #[test]
    fn claim_expiry_and_completion_are_transactional_and_idempotent() {
        let executor = executor();
        executor.install_application_relations().unwrap();
        let message_id = enqueue(&executor, "lease-key");
        let now = Utc::now();

        let first = executor
            .claim_outbox("worker-a", now, Duration::minutes(1), 4, 3)
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].message_id, message_id);
        assert_eq!(first[0].attempt, 1);
        assert!(executor
            .claim_outbox(
                "worker-b",
                now + Duration::seconds(30),
                Duration::minutes(1),
                4,
                3,
            )
            .unwrap()
            .is_empty());

        assert!(executor
            .retry_outbox(
                message_id,
                first[0].lease_token,
                now + Duration::minutes(2),
                now + Duration::minutes(3),
                "late worker",
                3,
            )
            .is_err());

        let reclaimed = executor
            .claim_outbox(
                "worker-b",
                now + Duration::minutes(2),
                Duration::minutes(1),
                4,
                3,
            )
            .unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].attempt, 2);
        assert_ne!(reclaimed[0].lease_token, first[0].lease_token);
        assert!(executor
            .complete_outbox(message_id, first[0].lease_token, now + Duration::minutes(2),)
            .is_err());
        assert_eq!(
            executor
                .complete_outbox(
                    message_id,
                    reclaimed[0].lease_token,
                    now + Duration::minutes(2),
                )
                .unwrap(),
            OutboxCompletion::Completed
        );
        assert_eq!(
            executor
                .complete_outbox(
                    message_id,
                    reclaimed[0].lease_token,
                    now + Duration::minutes(2),
                )
                .unwrap(),
            OutboxCompletion::AlreadyCompleted
        );
    }

    #[test]
    fn retry_releases_lease_and_last_attempt_dead_letters() {
        let executor = executor();
        executor.install_application_relations().unwrap();
        let message_id = enqueue(&executor, "retry-key");
        let now = Utc::now();
        let first = executor
            .claim_outbox("worker", now, Duration::minutes(1), 1, 2)
            .unwrap()
            .remove(0);
        assert_eq!(
            executor
                .retry_outbox(
                    message_id,
                    first.lease_token,
                    now + Duration::seconds(1),
                    now + Duration::seconds(5),
                    "temporary",
                    2,
                )
                .unwrap(),
            OutboxRetryDisposition::Retried
        );
        let second = executor
            .claim_outbox(
                "worker",
                now + Duration::seconds(5),
                Duration::minutes(1),
                1,
                2,
            )
            .unwrap()
            .remove(0);
        assert_eq!(second.attempt, 2);
        assert_eq!(
            executor
                .retry_outbox(
                    message_id,
                    second.lease_token,
                    now + Duration::seconds(6),
                    now + Duration::seconds(7),
                    "permanent",
                    2,
                )
                .unwrap(),
            OutboxRetryDisposition::DeadLettered
        );
        assert!(executor
            .claim_outbox(
                "worker",
                now + Duration::hours(1),
                Duration::minutes(1),
                1,
                2,
            )
            .unwrap()
            .is_empty());
    }

    #[test]
    fn business_write_and_outbox_share_one_commit_and_idempotency_is_unique() {
        let executor = executor();
        executor.install_application_relations().unwrap();
        executor
            .execute("CREATE TABLE business_event (id INTEGER PRIMARY KEY)")
            .unwrap();

        let rollback = executor.begin_procedural_boundary().unwrap();
        executor
            .execute("INSERT INTO business_event VALUES (1)")
            .unwrap();
        executor
            .append_outbox_message(OutboxMessage {
                idempotency_key: "business-1".to_owned(),
                schema_version: 1,
                payload: Value::json(r#"{"id":1}"#),
            })
            .unwrap();
        executor.abort_procedural_boundary(&rollback).unwrap();
        assert_eq!(scalar_count(&executor, "business_event"), 0);
        assert_eq!(scalar_count(&executor, OUTBOX_RELATION_NAME), 0);

        let commit = executor.begin_procedural_boundary().unwrap();
        executor
            .execute("INSERT INTO business_event VALUES (1)")
            .unwrap();
        executor
            .append_outbox_message(OutboxMessage {
                idempotency_key: "business-1".to_owned(),
                schema_version: 1,
                payload: Value::json(r#"{"id":1}"#),
            })
            .unwrap();
        executor.complete_procedural_boundary(&commit).unwrap();
        assert_eq!(scalar_count(&executor, "business_event"), 1);
        assert_eq!(scalar_count(&executor, OUTBOX_RELATION_NAME), 1);

        let duplicate = executor.begin_procedural_boundary().unwrap();
        assert!(executor
            .append_outbox_message(OutboxMessage {
                idempotency_key: "business-1".to_owned(),
                schema_version: 1,
                payload: Value::json(r#"{"id":1}"#),
            })
            .is_err());
        executor.abort_procedural_boundary(&duplicate).unwrap();
        assert_eq!(scalar_count(&executor, OUTBOX_RELATION_NAME), 1);
    }

    #[test]
    fn retention_is_bounded_and_removes_only_terminal_history() {
        let executor = executor();
        executor.install_application_relations().unwrap();
        let context = ExecutionContext::new();
        let boundary = executor.begin_procedural_boundary().unwrap();
        executor
            .append_audit_event(
                &context,
                AuditEvent {
                    object_id: ObjectId::BOOTSTRAP_NAMESPACE,
                    command_fingerprint: [1; 32],
                    metadata: Value::json("{}"),
                },
            )
            .unwrap();
        executor.complete_procedural_boundary(&boundary).unwrap();
        let terminal_id = enqueue(&executor, "terminal");
        enqueue(&executor, "pending");
        let now = Utc::now();
        let claim = executor
            .claim_outbox("worker", now, Duration::minutes(1), 1, 3)
            .unwrap()
            .remove(0);
        assert_eq!(claim.message_id, terminal_id);
        executor
            .complete_outbox(terminal_id, claim.lease_token, now)
            .unwrap();

        let outcome = executor
            .prune_application_history(
                now + Duration::seconds(1),
                ApplicationRetentionPolicy {
                    audit_retention: Duration::zero(),
                    outbox_retention: Duration::zero(),
                    max_rows_per_relation: 16,
                },
            )
            .unwrap();
        assert_eq!(outcome.audit_rows_deleted, 1);
        assert_eq!(outcome.outbox_rows_deleted, 1);
        assert_eq!(scalar_count(&executor, AUDIT_RELATION_NAME), 0);
        assert_eq!(scalar_count(&executor, OUTBOX_RELATION_NAME), 1);
    }

    #[test]
    fn concurrent_claimers_never_publish_two_leases_for_one_message() {
        let engine = Arc::new(MVCCEngine::in_memory());
        engine.open_engine().unwrap();
        let setup = Executor::new(Arc::clone(&engine));
        setup.install_application_relations().unwrap();
        let message_id = enqueue(&setup, "concurrent-key");
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let now = Utc::now();
        let mut handles = Vec::new();
        for worker in ["worker-a", "worker-b"] {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let executor = Executor::new(engine);
                barrier.wait();
                executor.claim_outbox(worker, now, Duration::minutes(1), 1, 3)
            }));
        }
        barrier.wait();
        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        let claims = outcomes
            .iter()
            .filter_map(|outcome| outcome.as_ref().ok())
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(claims.len(), 1, "one message must have one durable lease");
        assert_eq!(claims[0].message_id, message_id);
    }

    #[test]
    fn commit_claim_delivery_and_completion_survive_each_reopen_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let message_id = {
            let (executor, engine) = persistent_executor(directory.path());
            executor.install_application_relations().unwrap();
            let id = enqueue(&executor, "reopen-key");
            engine.close_engine().unwrap();
            id
        };
        let now = Utc::now();
        let claim = {
            let (executor, engine) = persistent_executor(directory.path());
            assert_eq!(scalar_count(&executor, OUTBOX_RELATION_NAME), 1);
            let claim = executor
                .claim_outbox("worker-a", now, Duration::minutes(1), 1, 3)
                .unwrap()
                .remove(0);
            engine.close_engine().unwrap();
            claim
        };
        let reclaimed = {
            let (executor, engine) = persistent_executor(directory.path());
            assert!(executor
                .claim_outbox(
                    "worker-b",
                    now + Duration::seconds(30),
                    Duration::minutes(1),
                    1,
                    3,
                )
                .unwrap()
                .is_empty());
            let reclaimed = executor
                .claim_outbox(
                    "worker-b",
                    now + Duration::minutes(2),
                    Duration::minutes(1),
                    1,
                    3,
                )
                .unwrap()
                .remove(0);
            assert_eq!(reclaimed.message_id, message_id);
            assert_ne!(reclaimed.lease_token, claim.lease_token);
            engine.close_engine().unwrap();
            reclaimed
        };
        {
            let (executor, engine) = persistent_executor(directory.path());
            assert_eq!(
                executor
                    .complete_outbox(
                        message_id,
                        reclaimed.lease_token,
                        now + Duration::minutes(2),
                    )
                    .unwrap(),
                OutboxCompletion::Completed
            );
            engine.close_engine().unwrap();
        }
        {
            let (executor, engine) = persistent_executor(directory.path());
            assert_eq!(
                executor
                    .complete_outbox(
                        message_id,
                        reclaimed.lease_token,
                        now + Duration::minutes(2),
                    )
                    .unwrap(),
                OutboxCompletion::AlreadyCompleted
            );
            assert!(executor
                .claim_outbox(
                    "worker-c",
                    now + Duration::hours(1),
                    Duration::minutes(1),
                    1,
                    3,
                )
                .unwrap()
                .is_empty());
            engine.close_engine().unwrap();
        }
    }
}
