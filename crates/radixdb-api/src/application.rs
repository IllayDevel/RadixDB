// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Public audit and transactional-outbox lifecycle.
//!
//! These handles are thin connection/transaction facades over ordinary
//! RadixDB MVCC relations. They do not own a second log or transaction model.

use chrono::{DateTime, Duration, Utc};
use radixdb_core::Result;

pub use radixdb_executor::application::{
    ApplicationRelationIdentity, ApplicationRetentionOutcome, ApplicationRetentionPolicy,
    AuditEvent, ObjectId, OutboxClaim, OutboxCompletion, OutboxMessage, OutboxRetryDisposition,
    AUDIT_RELATION_NAME, OUTBOX_RELATION_NAME,
};
use radixdb_executor::context::ExecutionContext;

use crate::{Database, Transaction};

impl Database {
    /// Explicitly install the system-owned audit and outbox relation contract.
    /// Database open never performs this catalog mutation implicitly.
    pub fn install_application_relations(&self) -> Result<ApplicationRelationIdentity> {
        self.with_connection_executor(|executor| executor.install_application_relations())
    }

    /// Resolve the already-installed relation identities without mutation.
    pub fn application_relation_identity(&self) -> Result<ApplicationRelationIdentity> {
        self.with_connection_executor(|executor| executor.application_relation_identity())
    }

    /// Atomically claim a bounded outbox batch and publish leases only after
    /// that claim transaction commits.
    pub fn claim_outbox(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_duration: Duration,
        limit: usize,
        max_attempts: u32,
    ) -> Result<Vec<OutboxClaim>> {
        self.with_connection_executor(|executor| {
            executor.claim_outbox(worker_id, now, lease_duration, limit, max_attempts)
        })
    }

    /// Mark an externally delivered message complete with its durable lease.
    pub fn complete_outbox(
        &self,
        message_id: [u8; 16],
        lease_token: [u8; 16],
        completed_at: DateTime<Utc>,
    ) -> Result<OutboxCompletion> {
        self.with_connection_executor(|executor| {
            executor.complete_outbox(message_id, lease_token, completed_at)
        })
    }

    /// Release a failed delivery for retry or move its exhausted attempt to
    /// the dead-letter state.
    pub fn retry_outbox(
        &self,
        message_id: [u8; 16],
        lease_token: [u8; 16],
        failed_at: DateTime<Utc>,
        retry_at: DateTime<Utc>,
        error: &str,
        max_attempts: u32,
    ) -> Result<OutboxRetryDisposition> {
        self.with_connection_executor(|executor| {
            executor.retry_outbox(
                message_id,
                lease_token,
                failed_at,
                retry_at,
                error,
                max_attempts,
            )
        })
    }

    /// Perform one bounded retention pass over immutable audit history and
    /// terminal outbox rows.
    pub fn prune_application_history(
        &self,
        now: DateTime<Utc>,
        policy: ApplicationRetentionPolicy,
    ) -> Result<ApplicationRetentionOutcome> {
        self.with_connection_executor(|executor| executor.prune_application_history(now, policy))
    }
}

impl Transaction {
    /// Append one audit event to this transaction. Embedded callers act as the
    /// bootstrap principal; server/procedural execution uses its authenticated
    /// execution context inside the executor host bridge.
    pub fn append_audit_event(&mut self, event: AuditEvent) -> Result<[u8; 16]> {
        self.check_active()?;
        self.executor()
            .append_audit_event(&ExecutionContext::new(), event)
    }

    /// Append one external side-effect intent to this business transaction.
    pub fn append_outbox_message(&mut self, message: OutboxMessage) -> Result<[u8; 16]> {
        self.check_active()?;
        self.executor().append_outbox_message(message)
    }
}

#[cfg(test)]
mod tests {
    use radixdb_core::Value;

    use super::*;

    #[test]
    fn public_facade_keeps_business_audit_and_outbox_in_one_transaction() {
        let database = Database::open_in_memory().unwrap();
        database.install_application_relations().unwrap();
        database
            .execute("CREATE TABLE business_record (id INTEGER PRIMARY KEY)", ())
            .unwrap();

        let mut transaction = database.begin().unwrap();
        transaction
            .execute("INSERT INTO business_record VALUES (1)", ())
            .unwrap();
        transaction
            .append_audit_event(AuditEvent {
                object_id: ObjectId::BOOTSTRAP_NAMESPACE,
                command_fingerprint: [3; 32],
                metadata: Value::json(r#"{"operation":"create"}"#),
            })
            .unwrap();
        transaction
            .append_outbox_message(OutboxMessage {
                idempotency_key: "business-record-1".to_owned(),
                schema_version: 1,
                payload: Value::json(r#"{"id":1}"#),
            })
            .unwrap();
        transaction.commit().unwrap();

        assert_eq!(
            database
                .query_one::<i64, _>("SELECT COUNT(*) FROM business_record", ())
                .unwrap(),
            1
        );
        assert_eq!(
            database
                .query_one::<i64, _>("SELECT COUNT(*) FROM audit.event", ())
                .unwrap(),
            1
        );
        assert_eq!(
            database
                .query_one::<i64, _>("SELECT COUNT(*) FROM outbox.message", ())
                .unwrap(),
            1
        );
    }
}
