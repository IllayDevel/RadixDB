use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const DIAGNOSTIC_FORMAT_V2: u32 = 2;
const MAX_METRICS_PER_KIND: usize = 2_048;
const MAX_INCIDENT_ALERT_IDS: usize = 128;
const MAX_MISSING_EVIDENCE: usize = 128;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeartbeatSnapshot {
    pub observer_unix_millis: Option<u64>,
    pub agent_unix_millis: Option<u64>,
    pub server_unix_millis: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedSilence {
    pub reason: String,
    pub started_unix_millis: u64,
    pub deadline_unix_millis: u64,
    pub phase_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticProgressSnapshot {
    pub format: u32,
    pub sequence: u64,
    pub phase_epoch: u64,
    pub phase: String,
    pub phase_started_unix_millis: u64,
    pub workload_epoch: u64,
    pub workload_units: u64,
    pub last_workload_progress_unix_millis: u64,
    pub operation_epoch: u64,
    pub active_operation: Option<String>,
    pub last_operation_progress_unix_millis: u64,
    pub planned_silence: Option<PlannedSilence>,
}

impl SemanticProgressSnapshot {
    pub fn validate(&self) -> Result<(), String> {
        if self.format != DIAGNOSTIC_FORMAT_V2 {
            return Err(format!(
                "unsupported semantic progress format {}, expected {DIAGNOSTIC_FORMAT_V2}",
                self.format
            ));
        }
        validate_label("phase", &self.phase, 128)?;
        if self.sequence == 0 || self.phase_epoch == 0 {
            return Err("semantic progress sequence and phase epoch must be non-zero".into());
        }
        if self.last_workload_progress_unix_millis < self.phase_started_unix_millis
            || self.last_operation_progress_unix_millis < self.phase_started_unix_millis
        {
            return Err("semantic progress timestamp precedes current phase".into());
        }
        if let Some(operation) = &self.active_operation {
            validate_label("active_operation", operation, 128)?;
        }
        if let Some(silence) = &self.planned_silence {
            validate_label("planned silence reason", &silence.reason, 256)?;
            if silence.phase_epoch != self.phase_epoch {
                return Err("planned silence belongs to another phase epoch".into());
            }
            if silence.deadline_unix_millis <= silence.started_unix_millis {
                return Err("planned silence deadline must follow its start".into());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticMetricFrameV2 {
    pub format: u32,
    pub sequence: u64,
    pub monotonic_millis: u64,
    pub unix_millis: u64,
    pub boot_id: String,
    pub run_id: String,
    pub heartbeats: HeartbeatSnapshot,
    pub progress: SemanticProgressSnapshot,
    #[serde(default)]
    pub counters: BTreeMap<String, i64>,
    #[serde(default)]
    pub gauges: BTreeMap<String, f64>,
}

impl DiagnosticMetricFrameV2 {
    pub fn validate(&self) -> Result<(), String> {
        if self.format != DIAGNOSTIC_FORMAT_V2 {
            return Err(format!(
                "unsupported diagnostic metric format {}, expected {DIAGNOSTIC_FORMAT_V2}",
                self.format
            ));
        }
        if self.sequence == 0 {
            return Err("diagnostic metric sequence must be non-zero".into());
        }
        validate_label("boot_id", &self.boot_id, 128)?;
        validate_label("run_id", &self.run_id, 128)?;
        self.progress.validate()?;
        if self.counters.len() > MAX_METRICS_PER_KIND || self.gauges.len() > MAX_METRICS_PER_KIND {
            return Err(format!(
                "diagnostic frame exceeds {MAX_METRICS_PER_KIND} counters or gauges"
            ));
        }
        for name in self.counters.keys().chain(self.gauges.keys()) {
            validate_metric_name(name)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Info,
    Warning,
    Incident,
    Fatal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticConfidence {
    Possible,
    Probable,
    Confirmed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticState {
    Active,
    Recovered,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceSignal {
    pub name: String,
    pub observed: f64,
    pub expected: Option<f64>,
    pub unit: String,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticAlertV2 {
    pub format: u32,
    pub id: String,
    pub kind: String,
    pub severity: DiagnosticSeverity,
    pub confidence: DiagnosticConfidence,
    pub state: DiagnosticState,
    pub first_unix_millis: u64,
    pub last_unix_millis: u64,
    pub first_frame_sequence: u64,
    pub last_frame_sequence: u64,
    pub evidence: Vec<EvidenceSignal>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub counter_evidence: Vec<EvidenceSignal>,
}

impl DiagnosticAlertV2 {
    pub fn validate(&self) -> Result<(), String> {
        if self.format != DIAGNOSTIC_FORMAT_V2 {
            return Err("unsupported diagnostic alert format".into());
        }
        validate_label("alert id", &self.id, 128)?;
        validate_label("alert kind", &self.kind, 128)?;
        if self.first_frame_sequence == 0
            || self.last_frame_sequence < self.first_frame_sequence
            || self.last_unix_millis < self.first_unix_millis
        {
            return Err("invalid diagnostic alert interval".into());
        }
        if self.evidence.is_empty() || self.evidence.len() > 32 || self.counter_evidence.len() > 32
        {
            return Err(
                "diagnostic evidence must contain 1..=32 supporting and at most 32 counter signals"
                    .into(),
            );
        }
        for signal in self.evidence.iter().chain(&self.counter_evidence) {
            validate_metric_name(&signal.name)?;
            validate_label("diagnostic evidence unit", &signal.unit, 64)?;
            validate_label("diagnostic evidence detail", &signal.detail, 1_024)?;
            if !signal.observed.is_finite()
                || signal.expected.is_some_and(|value| !value.is_finite())
            {
                return Err("diagnostic evidence must be finite".into());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticIncidentV2 {
    pub format: u32,
    pub id: String,
    pub alert_ids: Vec<String>,
    pub opened_unix_millis: u64,
    pub closed_unix_millis: Option<u64>,
    pub candidate_cause: String,
    pub confidence: DiagnosticConfidence,
    pub artifact_directory: String,
    pub missing_evidence: Vec<String>,
}

impl DiagnosticIncidentV2 {
    pub fn validate(&self) -> Result<(), String> {
        if self.format != DIAGNOSTIC_FORMAT_V2 {
            return Err("unsupported diagnostic incident format".into());
        }
        validate_label("incident id", &self.id, 128)?;
        validate_label("candidate cause", &self.candidate_cause, 128)?;
        if self.alert_ids.is_empty() || self.alert_ids.len() > MAX_INCIDENT_ALERT_IDS {
            return Err(format!(
                "diagnostic incident must reference 1..={MAX_INCIDENT_ALERT_IDS} alerts"
            ));
        }
        for alert_id in &self.alert_ids {
            validate_label("incident alert id", alert_id, 128)?;
        }
        if self
            .closed_unix_millis
            .is_some_and(|closed| closed < self.opened_unix_millis)
        {
            return Err("diagnostic incident closes before it opens".into());
        }
        if self.missing_evidence.len() > MAX_MISSING_EVIDENCE {
            return Err(format!(
                "diagnostic incident exceeds {MAX_MISSING_EVIDENCE} missing-evidence entries"
            ));
        }
        for missing in &self.missing_evidence {
            validate_label("missing evidence", missing, 1_024)?;
        }
        validate_relative_artifact_path(&self.artifact_directory)
    }
}

fn validate_label(name: &str, value: &str, max_len: usize) -> Result<(), String> {
    if value.is_empty()
        || value.len() > max_len
        || value
            .chars()
            .any(|character| character.is_control() || character == '\0')
    {
        return Err(format!("invalid {name}"));
    }
    Ok(())
}

fn validate_metric_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b':' | b'-'))
    {
        return Err(format!("invalid diagnostic metric name `{name}`"));
    }
    Ok(())
}

fn validate_relative_artifact_path(path: &str) -> Result<(), String> {
    let path = std::path::Path::new(path);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err("incident artifact directory must be a safe relative path".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_contract_rejects_unsafe_incident_artifact_path() {
        let incident = DiagnosticIncidentV2 {
            format: DIAGNOSTIC_FORMAT_V2,
            id: "incident-1".into(),
            alert_ids: vec!["alert-1".into()],
            opened_unix_millis: 1,
            closed_unix_millis: None,
            candidate_cause: "workload_stall".into(),
            confidence: DiagnosticConfidence::Probable,
            artifact_directory: "../escape".into(),
            missing_evidence: Vec::new(),
        };
        assert!(incident.validate().is_err());
    }
}
