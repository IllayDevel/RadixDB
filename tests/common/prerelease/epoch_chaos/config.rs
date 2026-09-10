use std::{env, path::PathBuf, time::Duration};

pub const DATABASE: &str = "epoch_chaos";
pub const DEFAULT_SEED: u64 = 0x4550_4f43_4843_414f;
pub const ACCEPTANCE_RANDOM_RUNGS: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProfileKind {
    Full,
    Smoke,
}

#[derive(Clone, Debug)]
pub struct ChaosProfile {
    pub kind: ProfileKind,
    pub wide_actions: u64,
    pub large_workers: usize,
    pub large_actions_per_worker: u64,
    pub micro_transactions: u64,
    pub random_rungs: Vec<usize>,
    pub random_operations_per_rung: u64,
    pub cold_rows: u64,
    pub hot_rows: u64,
    pub stage_timeout: Duration,
}

impl ChaosProfile {
    pub fn full() -> Self {
        Self {
            kind: ProfileKind::Full,
            wide_actions: 100_000,
            large_workers: 64,
            large_actions_per_worker: 25_000,
            micro_transactions: 2_000_000,
            random_rungs: vec![32, 64, 128, 256, 512],
            random_operations_per_rung: 400_000,
            cold_rows: 1_000_000,
            hot_rows: 25_000,
            stage_timeout: Duration::from_secs(6 * 60 * 60),
        }
    }

    pub fn smoke() -> Self {
        Self {
            kind: ProfileKind::Smoke,
            wide_actions: 500,
            large_workers: 4,
            large_actions_per_worker: 200,
            micro_transactions: 1_000,
            random_rungs: vec![4, 8, 16],
            random_operations_per_rung: 500,
            cold_rows: 5_000,
            hot_rows: 500,
            stage_timeout: Duration::from_secs(10 * 60),
        }
    }

    pub fn is_acceptance(&self) -> bool {
        self.kind == ProfileKind::Full
    }

    pub fn acceptance_random_rungs(&self) -> &[usize] {
        if self.is_acceptance() {
            &self.random_rungs[..ACCEPTANCE_RANDOM_RUNGS]
        } else {
            &self.random_rungs
        }
    }

    pub fn capacity_rung_index(&self, clients: usize) -> Option<usize> {
        self.random_rungs
            .iter()
            .enumerate()
            .skip(ACCEPTANCE_RANDOM_RUNGS)
            .find_map(|(index, candidate)| (*candidate == clients).then_some(index))
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.wide_actions == 0
            || self.large_workers == 0
            || self.large_actions_per_worker == 0
            || self.micro_transactions == 0
            || self.random_rungs.is_empty()
            || self.random_operations_per_rung == 0
            || self.cold_rows == 0
            || self.stage_timeout.is_zero()
        {
            return Err("epoch chaos profile contains a zero limit".to_string());
        }
        if self.is_acceptance()
            && (self.wide_actions < 100_000
                || self.large_workers < 64
                || self.large_actions_per_worker < 25_000
                || self.micro_transactions < 2_000_000
                || self.random_rungs != [32, 64, 128, 256, 512]
                || self.random_operations_per_rung < 400_000)
        {
            return Err("full profile is below the frozen acceptance floor".to_string());
        }
        if self.random_rungs.contains(&0) {
            return Err("random client rung must be non-zero".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ChaosConfig {
    pub run_id: String,
    pub root: PathBuf,
    pub seed: u64,
    pub profile: ChaosProfile,
    pub resume_bundle: Option<PathBuf>,
    pub capacity_rung: Option<usize>,
    pub finalize_source: Option<PathBuf>,
    pub finalize_source_commit: Option<String>,
    pub finalize_remediation_commit: Option<String>,
}

impl ChaosConfig {
    pub fn from_env() -> Result<Self, String> {
        let profile = match env::var("RADIXDB_EPOCH_CHAOS_PROFILE") {
            Ok(value) if value == "full" => ChaosProfile::full(),
            Ok(value) if value == "smoke" => ChaosProfile::smoke(),
            Ok(value) => {
                return Err(format!(
                    "RADIXDB_EPOCH_CHAOS_PROFILE must be `full` or `smoke`, got `{value}`"
                ))
            }
            Err(env::VarError::NotPresent) => ChaosProfile::full(),
            Err(error) => return Err(error.to_string()),
        };
        profile.validate()?;

        let seed = match env::var("RADIXDB_EPOCH_CHAOS_SEED") {
            Ok(value) => value
                .parse::<u64>()
                .map_err(|error| format!("invalid epoch chaos seed: {error}"))?,
            Err(env::VarError::NotPresent) => DEFAULT_SEED,
            Err(error) => return Err(error.to_string()),
        };
        let run_id = env::var("RADIXDB_EPOCH_CHAOS_RUN_ID").unwrap_or_else(|_| {
            format!(
                "epoch-chaos-{}-{}",
                match profile.kind {
                    ProfileKind::Full => "full",
                    ProfileKind::Smoke => "smoke",
                },
                std::process::id()
            )
        });
        validate_run_id(&run_id)?;
        let root = env::var_os("RADIXDB_EPOCH_CHAOS_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("target/epoch-chaos-runs"));
        if root.as_os_str().is_empty() {
            return Err("epoch chaos root must not be empty".to_string());
        }
        let resume_bundle = env::var_os("RADIXDB_EPOCH_CHAOS_RESUME_BUNDLE")
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty());
        let capacity_rung = match env::var("RADIXDB_EPOCH_CHAOS_CAPACITY_RUNG") {
            Ok(value) => Some(
                value
                    .parse::<usize>()
                    .map_err(|error| format!("invalid epoch chaos capacity rung: {error}"))?,
            ),
            Err(env::VarError::NotPresent) => None,
            Err(error) => return Err(error.to_string()),
        };
        let finalize_source = env::var_os("RADIXDB_EPOCH_CHAOS_FINALIZE_SOURCE")
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty());
        let finalize_source_commit = env::var("RADIXDB_EPOCH_CHAOS_FINALIZE_SOURCE_COMMIT")
            .ok()
            .filter(|value| !value.is_empty());
        let finalize_remediation_commit =
            env::var("RADIXDB_EPOCH_CHAOS_FINALIZE_REMEDIATION_COMMIT")
                .ok()
                .filter(|value| !value.is_empty());
        if let Some(clients) = capacity_rung {
            if !profile.is_acceptance() {
                return Err("capacity runs require the full profile".to_string());
            }
            if profile.capacity_rung_index(clients).is_none() {
                return Err(format!(
                    "capacity rung must be one of 128, 256 or 512, got {clients}"
                ));
            }
            if resume_bundle.is_none() {
                return Err("capacity run requires RADIXDB_EPOCH_CHAOS_RESUME_BUNDLE".to_string());
            }
        }
        if finalize_source.is_some() {
            if !profile.is_acceptance() {
                return Err("existing-run finalization requires the full profile".to_string());
            }
            if resume_bundle.is_some() || capacity_rung.is_some() {
                return Err(
                    "existing-run finalization cannot be combined with resume or capacity mode"
                        .to_string(),
                );
            }
            if finalize_source_commit.is_none() {
                return Err(
                    "existing-run finalization requires RADIXDB_EPOCH_CHAOS_FINALIZE_SOURCE_COMMIT"
                        .to_string(),
                );
            }
        } else if finalize_source_commit.is_some() || finalize_remediation_commit.is_some() {
            return Err("existing-run finalization commit requires a source run".to_string());
        }
        Ok(Self {
            run_id,
            root,
            seed,
            profile,
            resume_bundle,
            capacity_rung,
            finalize_source,
            finalize_source_commit,
            finalize_remediation_commit,
        })
    }
}

fn validate_run_id(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err("epoch chaos run id contains unsupported characters".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_profile_cannot_fall_below_normative_floor() {
        ChaosProfile::full().validate().unwrap();
        let mut invalid = ChaosProfile::full();
        invalid.micro_transactions -= 1;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn smoke_profile_is_explicitly_non_acceptance() {
        let smoke = ChaosProfile::smoke();
        smoke.validate().unwrap();
        assert!(!smoke.is_acceptance());
    }

    #[test]
    fn full_profile_separates_acceptance_and_capacity_rungs() {
        let full = ChaosProfile::full();
        assert_eq!(full.acceptance_random_rungs(), [32, 64]);
        assert_eq!(full.capacity_rung_index(128), Some(2));
        assert_eq!(full.capacity_rung_index(256), Some(3));
        assert_eq!(full.capacity_rung_index(512), Some(4));
        assert_eq!(full.capacity_rung_index(64), None);
        assert_eq!(full.capacity_rung_index(1_024), None);
    }
}
