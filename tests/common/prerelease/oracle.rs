use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OracleCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OracleReport {
    pub checks: Vec<OracleCheck>,
}

impl OracleReport {
    pub fn record(
        &mut self,
        name: impl Into<String>,
        passed: bool,
        detail: impl Into<String>,
    ) -> Result<(), String> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err("oracle check name must not be empty".to_string());
        }
        if self.checks.iter().any(|check| check.name == name) {
            return Err(format!("oracle check `{name}` was recorded twice"));
        }
        self.checks.push(OracleCheck {
            name,
            passed,
            detail: detail.into(),
        });
        Ok(())
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.checks.is_empty() {
            return Err("oracle report has no checks".to_string());
        }
        let failed: Vec<_> = self
            .checks
            .iter()
            .filter(|check| !check.passed)
            .map(|check| format!("{}: {}", check.name, check.detail))
            .collect();
        if failed.is_empty() {
            Ok(())
        } else {
            Err(format!("oracle failures: {}", failed.join("; ")))
        }
    }

    pub fn passed(&self) -> usize {
        self.checks.iter().filter(|check| check.passed).count()
    }

    pub fn failed(&self) -> usize {
        self.checks.len() - self.passed()
    }
}
