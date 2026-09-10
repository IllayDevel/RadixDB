use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::{ArtifactStore, OperationTrace, RunManifest, TraceEvent, TraceOutcome, TraceValue};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StateOperation {
    Begin,
    Insert { key: i64, value: TraceValue },
    Update { key: i64, value: TraceValue },
    Delete { key: i64 },
    Select { key: i64 },
    CreateView { name: String },
    DropView { name: String },
    Checkpoint,
    Backup { name: String },
    Cancel,
    Restart,
    Disconnect,
    Commit,
    Rollback,
    InvalidStatement { class: String },
}

impl StateOperation {
    fn operation_name(&self) -> &'static str {
        match self {
            Self::Begin => "transaction.begin",
            Self::Insert { .. } => "dml.insert",
            Self::Update { .. } => "dml.update",
            Self::Delete { .. } => "dml.delete",
            Self::Select { .. } => "select.key",
            Self::CreateView { .. } => "ddl.create_view",
            Self::DropView { .. } => "ddl.drop_view",
            Self::Checkpoint => "checkpoint",
            Self::Backup { .. } => "backup",
            Self::Cancel => "cancel",
            Self::Restart => "restart",
            Self::Disconnect => "disconnect",
            Self::Commit => "transaction.commit",
            Self::Rollback => "transaction.rollback",
            Self::InvalidStatement { .. } => "statement.invalid",
        }
    }

    fn parameters(&self) -> Vec<TraceValue> {
        match self {
            Self::Insert { key, value } | Self::Update { key, value } => {
                vec![TraceValue::Integer(*key), value.clone()]
            }
            Self::Delete { key } | Self::Select { key } => vec![TraceValue::Integer(*key)],
            Self::CreateView { name }
            | Self::DropView { name }
            | Self::Backup { name }
            | Self::InvalidStatement { class: name } => vec![TraceValue::Text(name.clone())],
            _ => Vec::new(),
        }
    }

    fn statement(&self) -> Option<String> {
        match self {
            Self::Insert { .. } => Some("INSERT INTO state_rows VALUES ($1, $2)".to_string()),
            Self::Update { .. } => {
                Some("UPDATE state_rows SET value = $2 WHERE id = $1".to_string())
            }
            Self::Delete { .. } => Some("DELETE FROM state_rows WHERE id = $1".to_string()),
            Self::Select { .. } => Some("SELECT value FROM state_rows WHERE id = $1".to_string()),
            Self::CreateView { name } => Some(format!(
                "CREATE VIEW {name} AS SELECT id, value FROM state_rows"
            )),
            Self::DropView { name } => Some(format!("DROP VIEW {name}")),
            Self::Checkpoint => Some("PRAGMA CHECKPOINT".to_string()),
            Self::Backup { name } => Some(format!("BACKUP DATABASE TO '{name}'")),
            Self::InvalidStatement { .. } => Some("SELECT FROM".to_string()),
            _ => None,
        }
    }

    pub fn from_event(event: &TraceEvent) -> Result<Self, String> {
        let integer = |index: usize| match event.parameters.get(index) {
            Some(TraceValue::Integer(value)) => Ok(*value),
            other => Err(format!(
                "event {} expected integer parameter {index}, got {other:?}",
                event.sequence
            )),
        };
        let text = |index: usize| match event.parameters.get(index) {
            Some(TraceValue::Text(value)) => Ok(value.clone()),
            other => Err(format!(
                "event {} expected text parameter {index}, got {other:?}",
                event.sequence
            )),
        };
        let operation = match event.operation.as_str() {
            "transaction.begin" => Self::Begin,
            "dml.insert" => Self::Insert {
                key: integer(0)?,
                value: event
                    .parameters
                    .get(1)
                    .cloned()
                    .ok_or_else(|| format!("event {} has no insert value", event.sequence))?,
            },
            "dml.update" => Self::Update {
                key: integer(0)?,
                value: event
                    .parameters
                    .get(1)
                    .cloned()
                    .ok_or_else(|| format!("event {} has no update value", event.sequence))?,
            },
            "dml.delete" => Self::Delete { key: integer(0)? },
            "select.key" => Self::Select { key: integer(0)? },
            "ddl.create_view" => Self::CreateView { name: text(0)? },
            "ddl.drop_view" => Self::DropView { name: text(0)? },
            "checkpoint" => Self::Checkpoint,
            "backup" => Self::Backup { name: text(0)? },
            "cancel" => Self::Cancel,
            "restart" => Self::Restart,
            "disconnect" => Self::Disconnect,
            "transaction.commit" => Self::Commit,
            "transaction.rollback" => Self::Rollback,
            "statement.invalid" => Self::InvalidStatement { class: text(0)? },
            other => {
                return Err(format!(
                    "event {} has unknown operation `{other}`",
                    event.sequence
                ))
            }
        };
        Ok(operation)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateMachinePlan {
    pub trace: OperationTrace,
}

impl StateMachinePlan {
    pub fn generate(run_id: impl Into<String>, seed: u64) -> Result<Self, String> {
        let run_id = run_id.into();
        let key = i64::try_from(seed % 1_000_000)
            .map_err(|_| "state-machine seed key does not fit i64".to_string())?
            + 1;
        let mut trace = OperationTrace::new(run_id, seed);
        let success = || TraceOutcome::Rows {
            count: 0,
            values: Vec::new(),
        };
        let mut push = |actor_id: u64,
                        session_id: u64,
                        transaction_id: Option<u64>,
                        operation: StateOperation,
                        expected_outcome: TraceOutcome|
         -> Result<(), String> {
            let tick = u64::try_from(trace.events.len())
                .map_err(|_| "state-machine trace is too large".to_string())?;
            trace.push(TraceEvent {
                sequence: u64::MAX,
                logical_epoch: 0,
                actor_id,
                session_id,
                transaction_id,
                operation: operation.operation_name().to_string(),
                statement: operation.statement(),
                parameters: operation.parameters(),
                started_tick: tick.saturating_mul(2),
                finished_tick: tick.saturating_mul(2).saturating_add(1),
                expected_outcome,
                outcome: TraceOutcome::Pending,
            })
        };

        push(0, 10, Some(100), StateOperation::Begin, success())?;
        push(
            0,
            10,
            Some(100),
            StateOperation::Insert {
                key,
                value: TraceValue::Text("alpha".to_string()),
            },
            success(),
        )?;
        push(
            0,
            10,
            Some(100),
            StateOperation::Commit,
            TraceOutcome::Committed,
        )?;

        push(1, 11, Some(200), StateOperation::Begin, success())?;
        push(
            1,
            11,
            Some(200),
            StateOperation::Update {
                key,
                value: TraceValue::Text("rolled-back".to_string()),
            },
            success(),
        )?;
        push(
            1,
            11,
            Some(200),
            StateOperation::Rollback,
            TraceOutcome::RolledBack,
        )?;

        push(2, 12, Some(250), StateOperation::Begin, success())?;
        push(2, 12, Some(250), StateOperation::Delete { key }, success())?;
        push(
            2,
            12,
            Some(250),
            StateOperation::Rollback,
            TraceOutcome::RolledBack,
        )?;

        push(3, 13, Some(300), StateOperation::Begin, success())?;
        push(
            3,
            13,
            Some(300),
            StateOperation::Insert {
                key,
                value: TraceValue::Text("duplicate".to_string()),
            },
            TraceOutcome::DeclaredConflict {
                class: "unique_key".to_string(),
            },
        )?;
        push(
            3,
            13,
            Some(300),
            StateOperation::Rollback,
            TraceOutcome::RolledBack,
        )?;

        push(
            4,
            14,
            None,
            StateOperation::InvalidStatement {
                class: "syntax".to_string(),
            },
            TraceOutcome::DeclaredError {
                class: "syntax".to_string(),
            },
        )?;

        push(5, 15, Some(400), StateOperation::Begin, success())?;
        push(
            5,
            15,
            Some(400),
            StateOperation::Insert {
                key: key + 1,
                value: TraceValue::Text("ambiguous".to_string()),
            },
            success(),
        )?;
        push(
            5,
            15,
            Some(400),
            StateOperation::Disconnect,
            TraceOutcome::AmbiguousDisconnect,
        )?;

        push(
            6,
            16,
            None,
            StateOperation::Select { key },
            TraceOutcome::Rows {
                count: 1,
                values: vec![TraceValue::Text("alpha".to_string())],
            },
        )?;
        push(
            6,
            16,
            None,
            StateOperation::CreateView {
                name: "state_rows_v".to_string(),
            },
            success(),
        )?;
        push(6, 16, None, StateOperation::Checkpoint, success())?;
        push(
            6,
            16,
            None,
            StateOperation::Backup {
                name: "state-backup".to_string(),
            },
            success(),
        )?;
        push(
            6,
            16,
            None,
            StateOperation::Cancel,
            TraceOutcome::DeclaredError {
                class: "cancelled".to_string(),
            },
        )?;
        push(6, 16, None, StateOperation::Restart, success())?;
        push(
            6,
            16,
            None,
            StateOperation::DropView {
                name: "state_rows_v".to_string(),
            },
            success(),
        )?;

        trace.validate()?;
        Ok(Self { trace })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayModel {
    committed: BTreeMap<i64, TraceValue>,
    pending: BTreeMap<u64, BTreeMap<i64, Option<TraceValue>>>,
    views: BTreeSet<String>,
    backups: BTreeSet<String>,
    committed_epoch: u64,
    checkpoint_epoch: u64,
    restarts: u64,
}

impl ReplayModel {
    pub fn committed_value(&self, key: i64) -> Option<&TraceValue> {
        self.committed.get(&key)
    }

    pub fn committed_epoch(&self) -> u64 {
        self.committed_epoch
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.checkpoint_epoch > self.committed_epoch {
            return Err("checkpoint epoch exceeds committed epoch".to_string());
        }
        if self
            .pending
            .keys()
            .any(|transaction_id| *transaction_id == 0)
        {
            return Err("model contains transaction zero".to_string());
        }
        Ok(())
    }

    pub fn apply_verified_event(&mut self, event: &TraceEvent) -> Result<TraceOutcome, String> {
        let operation = StateOperation::from_event(event)?;
        let mut candidate = self.clone();
        let actual = candidate.execute(&operation, event.transaction_id)?;
        if actual != event.expected_outcome {
            return Err(format!(
                "event {} expected {:?}, observed {:?}",
                event.sequence, event.expected_outcome, actual
            ));
        }
        if event.outcome != TraceOutcome::Pending && event.outcome != actual {
            return Err(format!(
                "event {} recorded {:?}, replay observed {:?}",
                event.sequence, event.outcome, actual
            ));
        }
        candidate.validate()?;
        *self = candidate;
        Ok(actual)
    }

    fn transaction_id(transaction_id: Option<u64>) -> Result<u64, String> {
        transaction_id.ok_or_else(|| "operation requires a transaction id".to_string())
    }

    fn visible_value(&self, transaction_id: Option<u64>, key: i64) -> Option<&TraceValue> {
        if let Some(transaction_id) = transaction_id {
            if let Some(change) = self
                .pending
                .get(&transaction_id)
                .and_then(|changes| changes.get(&key))
            {
                return change.as_ref();
            }
        }
        self.committed.get(&key)
    }

    fn execute(
        &mut self,
        operation: &StateOperation,
        transaction_id: Option<u64>,
    ) -> Result<TraceOutcome, String> {
        let success = || TraceOutcome::Rows {
            count: 0,
            values: Vec::new(),
        };
        match operation {
            StateOperation::Begin => {
                let transaction_id = Self::transaction_id(transaction_id)?;
                if self.pending.contains_key(&transaction_id) {
                    return Ok(TraceOutcome::DeclaredConflict {
                        class: "transaction_exists".to_string(),
                    });
                }
                self.pending.insert(transaction_id, BTreeMap::new());
                Ok(success())
            }
            StateOperation::Insert { key, value } => {
                let transaction_id = Self::transaction_id(transaction_id)?;
                if !self.pending.contains_key(&transaction_id) {
                    return Err(format!("transaction {transaction_id} is not open"));
                }
                if self.visible_value(Some(transaction_id), *key).is_some() {
                    return Ok(TraceOutcome::DeclaredConflict {
                        class: "unique_key".to_string(),
                    });
                }
                self.pending
                    .get_mut(&transaction_id)
                    .unwrap()
                    .insert(*key, Some(value.clone()));
                Ok(success())
            }
            StateOperation::Update { key, value } => {
                let transaction_id = Self::transaction_id(transaction_id)?;
                if !self.pending.contains_key(&transaction_id) {
                    return Err(format!("transaction {transaction_id} is not open"));
                }
                if self.visible_value(Some(transaction_id), *key).is_none() {
                    return Ok(TraceOutcome::DeclaredError {
                        class: "missing_row".to_string(),
                    });
                }
                self.pending
                    .get_mut(&transaction_id)
                    .unwrap()
                    .insert(*key, Some(value.clone()));
                Ok(success())
            }
            StateOperation::Delete { key } => {
                let transaction_id = Self::transaction_id(transaction_id)?;
                if !self.pending.contains_key(&transaction_id) {
                    return Err(format!("transaction {transaction_id} is not open"));
                }
                if self.visible_value(Some(transaction_id), *key).is_none() {
                    return Ok(TraceOutcome::DeclaredError {
                        class: "missing_row".to_string(),
                    });
                }
                self.pending
                    .get_mut(&transaction_id)
                    .unwrap()
                    .insert(*key, None);
                Ok(success())
            }
            StateOperation::Select { key } => {
                let values = self
                    .visible_value(transaction_id, *key)
                    .cloned()
                    .into_iter()
                    .collect::<Vec<_>>();
                Ok(TraceOutcome::Rows {
                    count: values.len() as u64,
                    values,
                })
            }
            StateOperation::CreateView { name } => {
                if !self.views.insert(name.clone()) {
                    return Ok(TraceOutcome::DeclaredConflict {
                        class: "catalog_name".to_string(),
                    });
                }
                Ok(success())
            }
            StateOperation::DropView { name } => {
                if !self.views.remove(name) {
                    return Ok(TraceOutcome::DeclaredError {
                        class: "missing_view".to_string(),
                    });
                }
                Ok(success())
            }
            StateOperation::Checkpoint => {
                self.checkpoint_epoch = self.committed_epoch;
                Ok(success())
            }
            StateOperation::Backup { name } => {
                self.backups.insert(name.clone());
                Ok(success())
            }
            StateOperation::Cancel => Ok(TraceOutcome::DeclaredError {
                class: "cancelled".to_string(),
            }),
            StateOperation::Restart => {
                self.pending.clear();
                self.restarts = self.restarts.saturating_add(1);
                Ok(success())
            }
            StateOperation::Disconnect => {
                if let Some(transaction_id) = transaction_id {
                    self.pending.remove(&transaction_id);
                }
                Ok(TraceOutcome::AmbiguousDisconnect)
            }
            StateOperation::Commit => {
                let transaction_id = Self::transaction_id(transaction_id)?;
                let changes = self
                    .pending
                    .remove(&transaction_id)
                    .ok_or_else(|| format!("transaction {transaction_id} is not open"))?;
                for (key, value) in changes {
                    if let Some(value) = value {
                        self.committed.insert(key, value);
                    } else {
                        self.committed.remove(&key);
                    }
                }
                self.committed_epoch = self
                    .committed_epoch
                    .checked_add(1)
                    .ok_or_else(|| "replay committed epoch overflow".to_string())?;
                Ok(TraceOutcome::Committed)
            }
            StateOperation::Rollback => {
                let transaction_id = Self::transaction_id(transaction_id)?;
                self.pending
                    .remove(&transaction_id)
                    .ok_or_else(|| format!("transaction {transaction_id} is not open"))?;
                Ok(TraceOutcome::RolledBack)
            }
            StateOperation::InvalidStatement { class } => Ok(TraceOutcome::DeclaredError {
                class: class.clone(),
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateMachineRun {
    pub trace: OperationTrace,
    pub model: ReplayModel,
}

pub fn execute_state_machine_plan(plan: &StateMachinePlan) -> Result<StateMachineRun, String> {
    plan.trace.validate()?;
    if plan
        .trace
        .events
        .iter()
        .any(|event| event.outcome != TraceOutcome::Pending)
    {
        return Err("state-machine plan must be generated before outcomes are known".to_string());
    }
    let mut trace = plan.trace.clone();
    let mut model = ReplayModel::default();
    for event in &mut trace.events {
        let actual = model.apply_verified_event(event)?;
        event.outcome = actual;
        event.logical_epoch = model.committed_epoch();
    }
    trace.validate()?;
    model.validate()?;
    Ok(StateMachineRun { trace, model })
}

pub fn replay_trace(manifest: &RunManifest, trace: &OperationTrace) -> Result<ReplayModel, String> {
    manifest.validate()?;
    trace.validate()?;
    if manifest.config.run_id != trace.run_id || manifest.config.seed != trace.seed {
        return Err("manifest and trace identity do not match".to_string());
    }
    if trace
        .events
        .iter()
        .any(|event| event.outcome == TraceOutcome::Pending)
    {
        return Err("standalone replay requires recorded outcomes".to_string());
    }
    let mut model = ReplayModel::default();
    for event in &trace.events {
        model.apply_verified_event(event)?;
    }
    model.validate()?;
    Ok(model)
}

pub fn replay_artifacts(store: &ArtifactStore) -> Result<ReplayModel, String> {
    let manifest: RunManifest = store.read_json("manifest.json")?;
    let trace: OperationTrace = store.read_json("trace.json")?;
    replay_trace(&manifest, &trace)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShrinkStage {
    pub name: String,
    pub before_events: usize,
    pub after_events: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShrinkResult {
    pub minimized: OperationTrace,
    pub stages: Vec<ShrinkStage>,
}

pub fn trace_has_outcome_mismatch(trace: &OperationTrace) -> bool {
    trace.events.iter().any(|event| {
        event.outcome != TraceOutcome::Pending && event.outcome != event.expected_outcome
    })
}

pub fn shrink_failing_trace<F>(trace: &OperationTrace, failure: F) -> Result<ShrinkResult, String>
where
    F: Fn(&OperationTrace) -> bool,
{
    trace.validate()?;
    if !failure(trace) {
        return Err("trace does not reproduce the requested failure".to_string());
    }
    let mut minimized = trace.clone();
    let mut stages = Vec::new();

    let actor_ids: BTreeSet<u64> = minimized
        .events
        .iter()
        .map(|event| event.actor_id)
        .collect();
    let before = minimized.events.len();
    for actor_id in actor_ids {
        try_remove_events(&mut minimized, &failure, |event| event.actor_id == actor_id)?;
    }
    stages.push(ShrinkStage {
        name: "actors".to_string(),
        before_events: before,
        after_events: minimized.events.len(),
    });

    let transaction_ids: BTreeSet<u64> = minimized
        .events
        .iter()
        .filter_map(|event| event.transaction_id)
        .collect();
    let before = minimized.events.len();
    for transaction_id in transaction_ids {
        try_remove_events(&mut minimized, &failure, |event| {
            event.transaction_id == Some(transaction_id)
        })?;
    }
    stages.push(ShrinkStage {
        name: "transactions".to_string(),
        before_events: before,
        after_events: minimized.events.len(),
    });

    let before = minimized.events.len();
    let mut index = 0;
    while index < minimized.events.len() {
        let mut candidate = minimized.clone();
        candidate.events.remove(index);
        candidate.resequence()?;
        if !candidate.events.is_empty() && failure(&candidate) {
            minimized = candidate;
        } else {
            index += 1;
        }
    }
    stages.push(ShrinkStage {
        name: "statements".to_string(),
        before_events: before,
        after_events: minimized.events.len(),
    });

    let before = minimized.events.len();
    for event_index in 0..minimized.events.len() {
        for parameter_index in 0..minimized.events[event_index].parameters.len() {
            for replacement in
                simplified_values(&minimized.events[event_index].parameters[parameter_index])
            {
                let mut candidate = minimized.clone();
                candidate.events[event_index].parameters[parameter_index] = replacement;
                if failure(&candidate) {
                    minimized = candidate;
                    break;
                }
            }
        }
    }
    stages.push(ShrinkStage {
        name: "data_cardinality".to_string(),
        before_events: before,
        after_events: minimized.events.len(),
    });

    minimized.resequence()?;
    if !failure(&minimized) {
        return Err("shrinker lost the failure".to_string());
    }
    Ok(ShrinkResult { minimized, stages })
}

fn try_remove_events<F, P>(
    trace: &mut OperationTrace,
    failure: &F,
    predicate: P,
) -> Result<(), String>
where
    F: Fn(&OperationTrace) -> bool,
    P: Fn(&TraceEvent) -> bool,
{
    let mut candidate = trace.clone();
    candidate.events.retain(|event| !predicate(event));
    candidate.resequence()?;
    if !candidate.events.is_empty() && failure(&candidate) {
        *trace = candidate;
    }
    Ok(())
}

fn simplified_values(value: &TraceValue) -> Vec<TraceValue> {
    match value {
        TraceValue::Integer(value) if *value != 0 => vec![TraceValue::Integer(0)],
        TraceValue::Text(value) if value.len() > 1 => vec![TraceValue::Text(
            value.chars().next().unwrap_or_default().to_string(),
        )],
        TraceValue::Bytes(value) if value.len() > 1 => vec![TraceValue::Bytes(vec![value[0]])],
        _ => Vec::new(),
    }
}
