use serde::{Deserialize, Serialize};

use super::{DeterministicStream, RunConfig, RunProfile};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Insert,
    Update,
    Delete,
    Select,
    Commit,
    Rollback,
    Disconnect,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedOperation {
    pub ordinal: u64,
    pub actor_id: u64,
    pub transaction_id: u64,
    pub kind: OperationKind,
    pub key: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActorPlan {
    pub seed: u64,
    pub actor_count: usize,
    pub operations: Vec<PlannedOperation>,
}

impl ActorPlan {
    pub fn generate(config: &RunConfig) -> Result<Self, String> {
        config.validate()?;
        let actor_count = match &config.profile {
            RunProfile::ConcurrencyLadder { clients } => *clients,
            RunProfile::Prerelease128 => 128,
            _ => config.max_connections.min(32),
        };
        let actor_count_u64 = u64::try_from(actor_count)
            .map_err(|_| "actor count does not fit into u64".to_string())?;
        let capacity = usize::try_from(config.operation_budget)
            .map_err(|_| "operation budget does not fit into usize".to_string())?;
        let mut random = DeterministicStream::new(config.seed);
        let mut operations = Vec::with_capacity(capacity);
        for ordinal in 0..config.operation_budget {
            let actor_id = random.next_u64() % actor_count_u64;
            let transaction_id = actor_id
                .checked_mul(config.operation_budget)
                .and_then(|base| base.checked_add(ordinal / 4))
                .ok_or_else(|| "planned transaction id overflow".to_string())?;
            let kind = match random.index(7)? {
                0 => OperationKind::Insert,
                1 => OperationKind::Update,
                2 => OperationKind::Delete,
                3 => OperationKind::Select,
                4 => OperationKind::Commit,
                5 => OperationKind::Rollback,
                _ => OperationKind::Disconnect,
            };
            operations.push(PlannedOperation {
                ordinal,
                actor_id,
                transaction_id,
                kind,
                key: random.next_u64(),
            });
        }
        Ok(Self {
            seed: config.seed,
            actor_count,
            operations,
        })
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.actor_count == 0 {
            return Err("actor plan must contain actors".to_string());
        }
        for (index, operation) in self.operations.iter().enumerate() {
            if operation.ordinal != index as u64 {
                return Err(format!(
                    "actor plan ordinal mismatch at {index}: {}",
                    operation.ordinal
                ));
            }
            if operation.actor_id >= self.actor_count as u64 {
                return Err(format!(
                    "actor {} is outside configured count {}",
                    operation.actor_id, self.actor_count
                ));
            }
        }
        Ok(())
    }
}
