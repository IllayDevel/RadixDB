use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::{MessengerSeedPlan, TraceValue};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelTransactionState {
    Open,
    Committed,
    RolledBack,
    DeclaredFailure,
    Ambiguous,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelTransaction {
    pub id: u64,
    pub actor_id: u64,
    pub state: ModelTransactionState,
    pub operations: Vec<String>,
    pub values: BTreeMap<String, TraceValue>,
    pub committed_epoch: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelLedger {
    transactions: BTreeMap<u64, ModelTransaction>,
    committed_epoch: u64,
}

impl ModelLedger {
    pub fn begin(&mut self, id: u64, actor_id: u64) -> Result<(), String> {
        if self.transactions.contains_key(&id) {
            return Err(format!("transaction {id} already exists in the model"));
        }
        self.transactions.insert(
            id,
            ModelTransaction {
                id,
                actor_id,
                state: ModelTransactionState::Open,
                operations: Vec::new(),
                values: BTreeMap::new(),
                committed_epoch: None,
            },
        );
        Ok(())
    }

    pub fn record(
        &mut self,
        id: u64,
        operation: impl Into<String>,
        key: impl Into<String>,
        value: TraceValue,
    ) -> Result<(), String> {
        let transaction = self.open_transaction_mut(id)?;
        let operation = operation.into();
        if operation.trim().is_empty() {
            return Err("model operation must not be empty".to_string());
        }
        transaction.operations.push(operation);
        transaction.values.insert(key.into(), value);
        Ok(())
    }

    pub fn commit(&mut self, id: u64) -> Result<u64, String> {
        let state = self
            .transactions
            .get(&id)
            .ok_or_else(|| format!("transaction {id} is missing from the model"))?
            .state;
        if state != ModelTransactionState::Open {
            return Err(format!("transaction {id} is already {state:?}"));
        }
        self.committed_epoch = self
            .committed_epoch
            .checked_add(1)
            .ok_or_else(|| "model committed epoch overflow".to_string())?;
        let epoch = self.committed_epoch;
        let transaction = self.open_transaction_mut(id)?;
        transaction.state = ModelTransactionState::Committed;
        transaction.committed_epoch = Some(epoch);
        Ok(epoch)
    }

    pub fn finish(&mut self, id: u64, state: ModelTransactionState) -> Result<(), String> {
        if matches!(
            state,
            ModelTransactionState::Open | ModelTransactionState::Committed
        ) {
            return Err("finish requires rollback, failure or ambiguous state".to_string());
        }
        let transaction = self.open_transaction_mut(id)?;
        transaction.state = state;
        Ok(())
    }

    pub fn transaction(&self, id: u64) -> Option<&ModelTransaction> {
        self.transactions.get(&id)
    }

    pub fn committed_epoch(&self) -> u64 {
        self.committed_epoch
    }

    pub fn validate(&self) -> Result<(), String> {
        for transaction in self.transactions.values() {
            match transaction.state {
                ModelTransactionState::Committed if transaction.committed_epoch.is_none() => {
                    return Err(format!(
                        "committed transaction {} has no epoch",
                        transaction.id
                    ));
                }
                ModelTransactionState::Open
                | ModelTransactionState::RolledBack
                | ModelTransactionState::DeclaredFailure
                | ModelTransactionState::Ambiguous
                    if transaction.committed_epoch.is_some() =>
                {
                    return Err(format!(
                        "non-committed transaction {} has a committed epoch",
                        transaction.id
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn open_transaction_mut(&mut self, id: u64) -> Result<&mut ModelTransaction, String> {
        let transaction = self
            .transactions
            .get_mut(&id)
            .ok_or_else(|| format!("transaction {id} is missing from the model"))?;
        if transaction.state != ModelTransactionState::Open {
            return Err(format!(
                "transaction {id} is already {:?}",
                transaction.state
            ));
        }
        Ok(transaction)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImmutableMessengerBaseline {
    pub seed: u64,
    pub row_counts: BTreeMap<String, u64>,
    pub checksum: String,
}

impl ImmutableMessengerBaseline {
    pub fn from_seed_plan(plan: &MessengerSeedPlan) -> Self {
        let row_counts = super::MessengerSeedTable::ALL
            .into_iter()
            .map(|table| (table.name().to_string(), plan.row_count(table)))
            .collect();
        let checksum = format!(
            "{:016x}:{:016x}",
            plan.seed.rotate_left(7) ^ plan.total_rows(),
            plan.messages
                .wrapping_mul(31)
                .wrapping_add(plan.audit_events)
        );
        Self {
            seed: plan.seed,
            row_counts,
            checksum,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SparseModelDelta {
    pub epoch: u64,
    pub table: String,
    pub key: i64,
    pub before: Option<TraceValue>,
    pub after: Option<TraceValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessengerMessageModel {
    pub id: i64,
    pub conversation_id: i64,
    pub sequence: i64,
    pub sender_id: i64,
    pub body: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingMessengerTransaction {
    messages: Vec<MessengerMessageModel>,
    outbox_message_ids: BTreeSet<i64>,
    idempotency: Vec<(i64, String, i64)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessengerStateModel {
    pub baseline: ImmutableMessengerBaseline,
    pub sparse_deltas: Vec<SparseModelDelta>,
    users: BTreeSet<i64>,
    conversations: BTreeSet<i64>,
    committed_messages: BTreeMap<i64, MessengerMessageModel>,
    outbox_message_ids: BTreeSet<i64>,
    conversation_sequences: BTreeMap<i64, i64>,
    idempotency: BTreeMap<(i64, String), i64>,
    pending: BTreeMap<u64, PendingMessengerTransaction>,
    committed_epoch: u64,
    durable_epoch: u64,
}

impl MessengerStateModel {
    pub fn new(plan: &MessengerSeedPlan) -> Self {
        Self {
            baseline: ImmutableMessengerBaseline::from_seed_plan(plan),
            sparse_deltas: Vec::new(),
            users: BTreeSet::new(),
            conversations: BTreeSet::new(),
            committed_messages: BTreeMap::new(),
            outbox_message_ids: BTreeSet::new(),
            conversation_sequences: BTreeMap::new(),
            idempotency: BTreeMap::new(),
            pending: BTreeMap::new(),
            committed_epoch: 0,
            durable_epoch: 0,
        }
    }

    pub fn register_user(&mut self, user_id: i64) -> Result<(), String> {
        if user_id <= 0 || !self.users.insert(user_id) {
            return Err(format!("invalid or duplicate user {user_id}"));
        }
        Ok(())
    }

    pub fn register_conversation(&mut self, conversation_id: i64) -> Result<(), String> {
        if conversation_id <= 0 || !self.conversations.insert(conversation_id) {
            return Err(format!(
                "invalid or duplicate conversation {conversation_id}"
            ));
        }
        self.conversation_sequences.insert(conversation_id, 0);
        Ok(())
    }

    pub fn begin(&mut self, transaction_id: u64) -> Result<(), String> {
        if self.pending.contains_key(&transaction_id) {
            return Err(format!("transaction {transaction_id} already exists"));
        }
        self.pending.insert(
            transaction_id,
            PendingMessengerTransaction {
                messages: Vec::new(),
                outbox_message_ids: BTreeSet::new(),
                idempotency: Vec::new(),
            },
        );
        Ok(())
    }

    pub fn stage_message(
        &mut self,
        transaction_id: u64,
        message: MessengerMessageModel,
        command_user_id: i64,
        idempotency_key: impl Into<String>,
    ) -> Result<(), String> {
        if !self.users.contains(&message.sender_id) || !self.users.contains(&command_user_id) {
            return Err("message or command user violates the model FK".to_string());
        }
        if !self.conversations.contains(&message.conversation_id) {
            return Err("message conversation violates the model FK".to_string());
        }
        if message.id <= 0 || message.sequence <= 0 || message.body.is_empty() {
            return Err("message violates PK/CHECK admission".to_string());
        }
        if self.committed_messages.contains_key(&message.id)
            || self
                .pending
                .values()
                .flat_map(|pending| pending.messages.iter())
                .any(|pending| pending.id == message.id)
        {
            return Err(format!("duplicate message PK {}", message.id));
        }
        let expected_sequence = self
            .conversation_sequences
            .get(&message.conversation_id)
            .copied()
            .unwrap_or(0)
            + self.pending.get(&transaction_id).map_or(0, |pending| {
                pending
                    .messages
                    .iter()
                    .filter(|row| row.conversation_id == message.conversation_id)
                    .count() as i64
            })
            + 1;
        if message.sequence != expected_sequence {
            return Err(format!(
                "conversation {} expected sequence {expected_sequence}, got {}",
                message.conversation_id, message.sequence
            ));
        }
        let idempotency_key = idempotency_key.into();
        if idempotency_key.is_empty()
            || self
                .idempotency
                .contains_key(&(command_user_id, idempotency_key.clone()))
            || self.pending.values().any(|pending| {
                pending
                    .idempotency
                    .iter()
                    .any(|(user, key, _)| *user == command_user_id && key == &idempotency_key)
            })
        {
            return Err("duplicate or empty idempotency key".to_string());
        }
        let pending = self
            .pending
            .get_mut(&transaction_id)
            .ok_or_else(|| format!("transaction {transaction_id} is not open"))?;
        pending.outbox_message_ids.insert(message.id);
        pending
            .idempotency
            .push((command_user_id, idempotency_key, message.id));
        pending.messages.push(message);
        Ok(())
    }

    pub fn commit(&mut self, transaction_id: u64) -> Result<u64, String> {
        let pending = self
            .pending
            .remove(&transaction_id)
            .ok_or_else(|| format!("transaction {transaction_id} is not open"))?;
        self.committed_epoch = self
            .committed_epoch
            .checked_add(1)
            .ok_or_else(|| "messenger model epoch overflow".to_string())?;
        let epoch = self.committed_epoch;
        for message in pending.messages {
            self.conversation_sequences
                .insert(message.conversation_id, message.sequence);
            self.outbox_message_ids.insert(message.id);
            self.sparse_deltas.push(SparseModelDelta {
                epoch,
                table: "messages".to_string(),
                key: message.id,
                before: None,
                after: Some(TraceValue::Integer(message.sequence)),
            });
            self.committed_messages.insert(message.id, message);
        }
        for (user_id, key, message_id) in pending.idempotency {
            self.idempotency.insert((user_id, key), message_id);
        }
        Ok(epoch)
    }

    pub fn rollback(&mut self, transaction_id: u64) -> Result<(), String> {
        self.pending
            .remove(&transaction_id)
            .map(|_| ())
            .ok_or_else(|| format!("transaction {transaction_id} is not open"))
    }

    pub fn mark_durable(&mut self, epoch: u64) -> Result<(), String> {
        if epoch < self.durable_epoch || epoch > self.committed_epoch {
            return Err(format!("invalid durable barrier {epoch}"));
        }
        self.durable_epoch = epoch;
        Ok(())
    }

    pub fn durable_epoch(&self) -> u64 {
        self.durable_epoch
    }

    pub fn committed_message_count(&self) -> usize {
        self.committed_messages.len()
    }

    pub fn validate(&self) -> Result<(), String> {
        for message in self.committed_messages.values() {
            if !self.users.contains(&message.sender_id)
                || !self.conversations.contains(&message.conversation_id)
                || !self.outbox_message_ids.contains(&message.id)
            {
                return Err(format!(
                    "committed message {} violates model graph",
                    message.id
                ));
            }
        }
        if self.durable_epoch > self.committed_epoch {
            return Err("durable epoch exceeds committed epoch".to_string());
        }
        Ok(())
    }
}
