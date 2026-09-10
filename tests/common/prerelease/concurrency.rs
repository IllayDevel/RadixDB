use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::{DeterministicStream, MessengerFixtureScale, MessengerSeedPlan};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionCohort {
    ValidFinalState,
    InvalidFinalState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyDistribution {
    SharedHot,
    Disjoint,
    Cold,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionTerminal {
    Commit,
    Rollback,
    Disconnect,
    IdempotentRetry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConcurrencyTable {
    Users,
    Conversations,
    Messages,
    OutboxJobs,
    SyncEvents,
    Reactions,
    Receipts,
    CommandResults,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConcurrencyOperationKind {
    Insert,
    Update,
    Delete,
    Upsert,
    ReadYourWrites,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConcurrencyOperation {
    pub ordinal: usize,
    pub table: ConcurrencyTable,
    pub kind: ConcurrencyOperationKind,
    pub key: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConcurrencyTransaction {
    pub id: u64,
    pub actor_id: usize,
    pub cohort: TransactionCohort,
    pub distribution: KeyDistribution,
    pub terminal: TransactionTerminal,
    pub operations: Vec<ConcurrencyOperation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConcurrencyPlan {
    pub seed: u64,
    pub clients: usize,
    pub transactions_per_client: usize,
    pub hot_percent: u8,
    pub disjoint_percent: u8,
    pub cold_percent: u8,
    pub large_fixture: MessengerSeedPlan,
    pub transactions: Vec<ConcurrencyTransaction>,
}

impl ConcurrencyPlan {
    pub fn generate(
        clients: usize,
        transactions_per_client: usize,
        seed: u64,
    ) -> Result<Self, String> {
        if clients < 16 || !clients.is_power_of_two() {
            return Err(format!(
                "clients must be a power of two and at least 16, got {clients}"
            ));
        }
        if transactions_per_client == 0 {
            return Err("transactions_per_client must be greater than zero".to_string());
        }
        let total = clients
            .checked_mul(transactions_per_client)
            .ok_or_else(|| "concurrency transaction count overflow".to_string())?;
        let mut random = DeterministicStream::new(seed);
        let tables = [
            ConcurrencyTable::Users,
            ConcurrencyTable::Conversations,
            ConcurrencyTable::Messages,
            ConcurrencyTable::OutboxJobs,
            ConcurrencyTable::SyncEvents,
            ConcurrencyTable::Reactions,
            ConcurrencyTable::Receipts,
            ConcurrencyTable::CommandResults,
        ];
        let mut transactions = Vec::with_capacity(total);
        for ordinal in 0..total {
            let actor_id = ordinal % clients;
            let distribution = match ordinal % 10 {
                0..=5 => KeyDistribution::SharedHot,
                6..=8 => KeyDistribution::Disjoint,
                _ => KeyDistribution::Cold,
            };
            let cohort = if ordinal % 7 == 0 {
                TransactionCohort::InvalidFinalState
            } else {
                TransactionCohort::ValidFinalState
            };
            let terminal = if cohort == TransactionCohort::InvalidFinalState {
                TransactionTerminal::Commit
            } else {
                // Put lifecycle actors on disjoint-key slots.  Otherwise the
                // shared-hot conflict cohort can abort their body before the
                // intended rollback/disconnect/ambiguous-ACK boundary is ever
                // exercised, making a nominally covered plan physically miss
                // those contracts.
                match ordinal % 30 {
                    6 => TransactionTerminal::Rollback,
                    8 => TransactionTerminal::Disconnect,
                    16 => TransactionTerminal::IdempotentRetry,
                    _ => TransactionTerminal::Commit,
                }
            };
            let operation_count = 2 + random.index(31)?;
            let table_count = (2 + random.index(7)?)
                .min(operation_count)
                .min(tables.len());
            let table_offset = random.index(tables.len())?;
            let selected_tables: Vec<_> = (0..table_count)
                .map(|index| tables[(table_offset + index) % tables.len()])
                .collect();
            let transaction_id = u64::try_from(ordinal)
                .map_err(|_| "transaction ordinal does not fit u64".to_string())?
                .saturating_add(1);
            let base_key = match distribution {
                KeyDistribution::SharedHot => random.next_u64() % 32 + 1,
                KeyDistribution::Disjoint => (actor_id as u64 + 1)
                    .saturating_mul(1_000_000)
                    .saturating_add(transaction_id),
                KeyDistribution::Cold => random.next_u64() % 46_000_000 + 1,
            };
            let mut operations = Vec::with_capacity(operation_count);
            for operation_ordinal in 0..operation_count {
                let kind = match (ordinal + operation_ordinal) % 5 {
                    0 => ConcurrencyOperationKind::Insert,
                    1 => ConcurrencyOperationKind::Update,
                    2 => ConcurrencyOperationKind::Delete,
                    3 => ConcurrencyOperationKind::Upsert,
                    _ => ConcurrencyOperationKind::ReadYourWrites,
                };
                operations.push(ConcurrencyOperation {
                    ordinal: operation_ordinal,
                    table: selected_tables[operation_ordinal % selected_tables.len()],
                    kind,
                    key: base_key.saturating_add(operation_ordinal as u64),
                });
            }
            transactions.push(ConcurrencyTransaction {
                id: transaction_id,
                actor_id,
                cohort,
                distribution,
                terminal,
                operations,
            });
        }
        let plan = Self {
            seed,
            clients,
            transactions_per_client,
            hot_percent: 60,
            disjoint_percent: 30,
            cold_percent: 10,
            large_fixture: MessengerSeedPlan::new(MessengerFixtureScale::Large, seed),
            transactions,
        };
        plan.validate()?;
        Ok(plan)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.clients < 16 || !self.clients.is_power_of_two() {
            return Err("invalid concurrency client count".to_string());
        }
        if self.hot_percent as u16 + self.disjoint_percent as u16 + self.cold_percent as u16 != 100
        {
            return Err("key distribution percentages must sum to 100".to_string());
        }
        let expected = self
            .clients
            .checked_mul(self.transactions_per_client)
            .ok_or_else(|| "concurrency transaction count overflow".to_string())?;
        if self.transactions.len() != expected {
            return Err(format!(
                "expected {expected} transactions, got {}",
                self.transactions.len()
            ));
        }
        let mut actors = BTreeSet::new();
        let mut kinds = BTreeSet::new();
        let mut terminals = [false; 4];
        for (ordinal, transaction) in self.transactions.iter().enumerate() {
            if transaction.id != ordinal as u64 + 1 || transaction.actor_id >= self.clients {
                return Err(format!("invalid transaction identity at ordinal {ordinal}"));
            }
            if !(2..=32).contains(&transaction.operations.len()) {
                return Err(format!(
                    "transaction {} has {} operations",
                    transaction.id,
                    transaction.operations.len()
                ));
            }
            let tables: BTreeSet<_> = transaction
                .operations
                .iter()
                .map(|operation| operation.table)
                .collect();
            if !(2..=8).contains(&tables.len()) {
                return Err(format!(
                    "transaction {} touches {} tables",
                    transaction.id,
                    tables.len()
                ));
            }
            if transaction.cohort == TransactionCohort::InvalidFinalState
                && transaction.terminal != TransactionTerminal::Commit
            {
                return Err("invalid-final-state transaction must attempt COMMIT".to_string());
            }
            terminals[match transaction.terminal {
                TransactionTerminal::Commit => 0,
                TransactionTerminal::Rollback => 1,
                TransactionTerminal::Disconnect => 2,
                TransactionTerminal::IdempotentRetry => 3,
            }] = true;
            for (operation_ordinal, operation) in transaction.operations.iter().enumerate() {
                if operation.ordinal != operation_ordinal {
                    return Err(format!(
                        "transaction {} operation ordinal mismatch",
                        transaction.id
                    ));
                }
                kinds.insert(operation.kind);
            }
            actors.insert(transaction.actor_id);
        }
        if actors.len() != self.clients {
            return Err("not every configured client owns a transaction".to_string());
        }
        if kinds.len() != 5 {
            return Err("concurrency plan does not cover all operation kinds".to_string());
        }
        if terminals.into_iter().any(|covered| !covered) {
            return Err("concurrency plan does not cover every transaction terminal".to_string());
        }
        if self.large_fixture.total_rows() < 100_000_000 {
            return Err("large fixture plan is below 100 million rows".to_string());
        }
        Ok(())
    }

    pub fn distribution_counts(&self) -> (usize, usize, usize) {
        self.transactions.iter().fold(
            (0usize, 0usize, 0usize),
            |(hot, disjoint, cold), transaction| match transaction.distribution {
                KeyDistribution::SharedHot => (hot + 1, disjoint, cold),
                KeyDistribution::Disjoint => (hot, disjoint + 1, cold),
                KeyDistribution::Cold => (hot, disjoint, cold + 1),
            },
        )
    }
}
