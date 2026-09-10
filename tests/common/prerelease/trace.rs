use serde::{Deserialize, Serialize};

pub const TRACE_FORMAT_VERSION: u32 = 2;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum TraceValue {
    Null,
    Integer(i64),
    FloatBits(u64),
    Boolean(bool),
    Text(String),
    Bytes(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TraceOutcome {
    Pending,
    Rows { count: u64, values: Vec<TraceValue> },
    Committed,
    RolledBack,
    DeclaredConflict { class: String },
    DeclaredError { class: String },
    AmbiguousDisconnect,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceEvent {
    pub sequence: u64,
    pub logical_epoch: u64,
    pub actor_id: u64,
    pub session_id: u64,
    pub transaction_id: Option<u64>,
    pub operation: String,
    pub statement: Option<String>,
    pub parameters: Vec<TraceValue>,
    pub started_tick: u64,
    pub finished_tick: u64,
    pub expected_outcome: TraceOutcome,
    pub outcome: TraceOutcome,
}

impl TraceEvent {
    pub fn validate(&self, expected_sequence: u64) -> Result<(), String> {
        if self.sequence != expected_sequence {
            return Err(format!(
                "trace sequence mismatch: expected {expected_sequence}, got {}",
                self.sequence
            ));
        }
        if self.operation.trim().is_empty() {
            return Err(format!("trace event {} has no operation", self.sequence));
        }
        if self.finished_tick < self.started_tick {
            return Err(format!(
                "trace event {} finishes before it starts",
                self.sequence
            ));
        }
        if self.expected_outcome == TraceOutcome::Pending {
            return Err(format!(
                "trace event {} has no declared expected outcome",
                self.sequence
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationTrace {
    pub format_version: u32,
    pub run_id: String,
    pub seed: u64,
    pub events: Vec<TraceEvent>,
}

impl OperationTrace {
    pub fn new(run_id: impl Into<String>, seed: u64) -> Self {
        Self {
            format_version: TRACE_FORMAT_VERSION,
            run_id: run_id.into(),
            seed,
            events: Vec::new(),
        }
    }

    pub fn push(&mut self, mut event: TraceEvent) -> Result<(), String> {
        let sequence = u64::try_from(self.events.len())
            .map_err(|_| "trace contains more events than u64 can represent".to_string())?;
        event.sequence = sequence;
        event.validate(sequence)?;
        self.events.push(event);
        Ok(())
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.format_version != TRACE_FORMAT_VERSION {
            return Err(format!(
                "unsupported trace format version {}, expected {TRACE_FORMAT_VERSION}",
                self.format_version
            ));
        }
        if self.run_id.is_empty() {
            return Err("trace run id must not be empty".to_string());
        }
        for (index, event) in self.events.iter().enumerate() {
            let sequence = u64::try_from(index)
                .map_err(|_| "trace contains more events than u64 can represent".to_string())?;
            event.validate(sequence)?;
        }
        Ok(())
    }

    pub fn resequence(&mut self) -> Result<(), String> {
        for (index, event) in self.events.iter_mut().enumerate() {
            event.sequence = u64::try_from(index)
                .map_err(|_| "trace contains more events than u64 can represent".to_string())?;
        }
        self.validate()
    }

    pub fn to_pretty_json(&self) -> Result<String, String> {
        self.validate()?;
        serde_json::to_string_pretty(self).map_err(|error| error.to_string())
    }

    pub fn from_json(json: &str) -> Result<Self, String> {
        let trace: Self = serde_json::from_str(json).map_err(|error| error.to_string())?;
        trace.validate()?;
        Ok(trace)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeterministicStream {
    state: u64,
}

impl DeterministicStream {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    pub fn index(&mut self, upper_exclusive: usize) -> Result<usize, String> {
        if upper_exclusive == 0 {
            return Err("cannot select an index from an empty range".to_string());
        }
        let upper = u64::try_from(upper_exclusive)
            .map_err(|_| "range does not fit into u64".to_string())?;
        usize::try_from(self.next_u64() % upper)
            .map_err(|_| "selected index does not fit into usize".to_string())
    }
}
