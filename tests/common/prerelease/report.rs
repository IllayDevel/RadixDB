use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{CounterSet, LatencySummary, OracleReport, RunManifest};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageStatus {
    Passed,
    Failed,
    Incomplete,
    ExpectedMutationDetected,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageResult {
    pub name: String,
    pub status: StageStatus,
    pub wall_millis: u64,
    pub detail: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Passed,
    Failed,
    Incomplete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunReport {
    pub manifest: RunManifest,
    pub status: RunStatus,
    pub stages: Vec<StageResult>,
    pub counters: CounterSet,
    pub latencies: BTreeMap<String, LatencySummary>,
    pub oracle: OracleReport,
}

impl RunReport {
    pub fn validate(&self) -> Result<(), String> {
        self.manifest.validate()?;
        if self.stages.is_empty() {
            return Err("run report has no stages".to_string());
        }
        let failed_stage = self
            .stages
            .iter()
            .any(|stage| matches!(stage.status, StageStatus::Failed));
        let incomplete_stage = self
            .stages
            .iter()
            .any(|stage| matches!(stage.status, StageStatus::Incomplete));
        match self.status {
            RunStatus::Passed if failed_stage || incomplete_stage => {
                return Err("passed run contains failed or incomplete stages".to_string());
            }
            RunStatus::Failed if !failed_stage => {
                return Err("failed run has no failed stage".to_string());
            }
            RunStatus::Incomplete if !incomplete_stage => {
                return Err("incomplete run has no incomplete stage".to_string());
            }
            _ => {}
        }
        if self.status == RunStatus::Passed {
            self.oracle.validate()?;
        }
        Ok(())
    }

    pub fn to_markdown(&self) -> Result<String, String> {
        self.validate()?;
        let mut output = format!(
            "# Prerelease run `{}`\n\n- Status: `{:?}`\n- Commit: `{}`\n- Seed: `{}`\n\n## Stages\n\n| Stage | Status | Wall, ms | Detail |\n|---|---|---:|---|\n",
            self.manifest.config.run_id,
            self.status,
            self.manifest.candidate.commit,
            self.manifest.config.seed,
        );
        for stage in &self.stages {
            output.push_str(&format!(
                "| {} | `{:?}` | {} | {} |\n",
                escape_table(&stage.name),
                stage.status,
                stage.wall_millis,
                escape_table(&stage.detail)
            ));
        }
        output.push_str("\n## Counters\n\n| Counter | Value |\n|---|---:|\n");
        for (name, value) in self.counters.values() {
            output.push_str(&format!("| {} | {} |\n", escape_table(name), value));
        }
        output.push_str(&format!(
            "\n## Oracle\n\n- Passed: `{}`\n- Failed: `{}`\n",
            self.oracle.passed(),
            self.oracle.failed()
        ));
        Ok(output)
    }
}

fn escape_table(value: &str) -> String {
    value.replace('|', "\\|").replace(['\n', '\r'], " ")
}
