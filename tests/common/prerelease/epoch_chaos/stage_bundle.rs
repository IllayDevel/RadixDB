use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use super::{
    fixture::FixtureEvidence,
    journal::{self, JournalSummary},
    model::{merge_journals, LayerSummary},
    oracle::OracleSnapshot,
    runner::{CandidateIdentity, CrashEvidence, HistoricalEvidence, ProfileEvidence},
    telemetry::TelemetryReport,
};
use serde::{Deserialize, Serialize};

const FORMAT: &str = "epoch-chaos-stage-bundle-1";
const MANIFEST: &str = "bundle.json";
const COMPLETE: &str = "COMPLETE";
const MAX_MANIFEST_BYTES: u64 = 64 * 1024 * 1024;
const MAX_COMPLETE_BYTES: u64 = 128;
const MAX_FILES: usize = 1_000_000;
const MAX_BYTES: u64 = 256 * 1024 * 1024 * 1024;
const MAX_DEPTH: usize = 256;
const MAX_RELATIVE_PATH_BYTES: usize = 4_096;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum BundleStage {
    WideTransaction,
    LargeTransactions,
    MicroTransactions,
    HighConcurrencyRandom,
}

impl BundleStage {
    pub const ALL: [Self; 4] = [
        Self::WideTransaction,
        Self::LargeTransactions,
        Self::MicroTransactions,
        Self::HighConcurrencyRandom,
    ];

    pub fn index(self) -> usize {
        match self {
            Self::WideTransaction => 0,
            Self::LargeTransactions => 1,
            Self::MicroTransactions => 2,
            Self::HighConcurrencyRandom => 3,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::WideTransaction => "wide-transaction",
            Self::LargeTransactions => "large-transactions",
            Self::MicroTransactions => "micro-transactions",
            Self::HighConcurrencyRandom => "high-concurrency-random",
        }
    }

    fn journal_prefix(self) -> &'static str {
        match self {
            Self::WideTransaction => "wide-",
            Self::LargeTransactions => "large-",
            Self::MicroTransactions => "micro-",
            Self::HighConcurrencyRandom => "random-",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
struct FileEvidence {
    relative_path: String,
    directory: bool,
    bytes: u64,
    sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BundleManifest {
    format: String,
    source_run_id: String,
    elapsed_millis: u64,
    stage: BundleStage,
    identity: CandidateIdentity,
    profile: ProfileEvidence,
    seed: u64,
    historical: HistoricalEvidence,
    fixture: FixtureEvidence,
    layers: Vec<LayerSummary>,
    reopen_oracles: Vec<OracleSnapshot>,
    crashes: Vec<CrashEvidence>,
    post_stage_oracle: OracleSnapshot,
    telemetry: TelemetryReport,
    stage_sources: Vec<StageSourceEvidence>,
    files: Vec<FileEvidence>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StageSourceEvidence {
    pub stage: BundleStage,
    pub source_run_id: String,
    pub source_commit: String,
    pub cargo_lock_sha256: String,
    pub server_binary_sha256: String,
}

pub struct PublishInput<'a> {
    pub run_root: &'a Path,
    pub run_id: &'a str,
    pub elapsed_millis: u64,
    pub stage: BundleStage,
    pub identity: &'a CandidateIdentity,
    pub profile: &'a ProfileEvidence,
    pub seed: u64,
    pub historical: &'a HistoricalEvidence,
    pub fixture: &'a FixtureEvidence,
    pub layers: &'a [LayerSummary],
    pub reopen_oracles: &'a [OracleSnapshot],
    pub crashes: &'a [CrashEvidence],
    pub post_stage_oracle: &'a OracleSnapshot,
    pub telemetry: &'a TelemetryReport,
    pub stage_sources: &'a [StageSourceEvidence],
}

pub struct RestoredPrefix {
    pub source_bundle: PathBuf,
    pub source_run_id: String,
    pub elapsed_millis: u64,
    pub stage: BundleStage,
    pub historical: HistoricalEvidence,
    pub fixture: FixtureEvidence,
    pub layers: Vec<LayerSummary>,
    pub reopen_oracles: Vec<OracleSnapshot>,
    pub crashes: Vec<CrashEvidence>,
    pub post_stage_oracle: OracleSnapshot,
    pub telemetry: TelemetryReport,
    pub stage_sources: Vec<StageSourceEvidence>,
}

pub fn publish(input: PublishInput<'_>) -> Result<PathBuf, String> {
    validate_prefix(
        input.stage,
        input.layers,
        input.reopen_oracles,
        input.crashes,
        input.post_stage_oracle,
        input.stage_sources,
    )?;
    validate_stage_identity(input.stage_sources, input.identity)?;
    if input.identity.dirty {
        return Err("cannot publish a stage bundle from a dirty candidate".to_string());
    }
    validate_identity(input.identity)?;
    if !input.profile.acceptance || input.run_id.is_empty() || input.elapsed_millis == 0 {
        return Err("stage bundle requires complete full-profile provenance".to_string());
    }
    validate_combined_budget(&[
        &input.run_root.join("server-data"),
        &input.run_root.join("journals"),
    ])?;
    let root = input.run_root.join("stage-bundles");
    fs::create_dir_all(&root).map_err(|error| error.to_string())?;
    let destination = root.join(format!(
        "{:02}-{}",
        input.stage.index() + 1,
        input.stage.name()
    ));
    if destination.exists() {
        return Err(format!(
            "stage bundle already exists: {}",
            destination.display()
        ));
    }
    let temporary = root.join(format!(
        ".{:02}-{}.partial-{}",
        input.stage.index() + 1,
        input.stage.name(),
        std::process::id()
    ));
    fs::create_dir(&temporary).map_err(|error| error.to_string())?;

    let result = (|| {
        copy_tree(
            &input.run_root.join("server-data"),
            &temporary.join("server-data"),
        )?;
        copy_tree(
            &input.run_root.join("journals"),
            &temporary.join("journals"),
        )?;
        let files = inventory(&temporary)?;
        let manifest = BundleManifest {
            format: FORMAT.to_string(),
            source_run_id: input.run_id.to_string(),
            elapsed_millis: input.elapsed_millis,
            stage: input.stage,
            identity: input.identity.clone(),
            profile: input.profile.clone(),
            seed: input.seed,
            historical: input.historical.clone(),
            fixture: input.fixture.clone(),
            layers: input.layers.to_vec(),
            reopen_oracles: input.reopen_oracles.to_vec(),
            crashes: input.crashes.to_vec(),
            post_stage_oracle: input.post_stage_oracle.clone(),
            telemetry: input.telemetry.clone(),
            stage_sources: input.stage_sources.to_vec(),
            files,
        };
        write_json(&temporary.join(MANIFEST), &manifest)?;
        let manifest_sha256 = super::super::sha256_file(&temporary.join(MANIFEST))?;
        write_bytes(
            &temporary.join(COMPLETE),
            format!("{manifest_sha256}\n").as_bytes(),
        )?;
        sync_tree_directories(&temporary)?;
        fs::rename(&temporary, &destination).map_err(|error| error.to_string())?;
        sync_directory(&root)?;
        Ok(destination.clone())
    })();
    if result.is_err() && temporary.exists() {
        let _ = fs::remove_dir_all(&temporary);
    }
    result
}

pub fn restore(
    source: &Path,
    run_root: &Path,
    identity: &CandidateIdentity,
    profile: &ProfileEvidence,
    seed: u64,
) -> Result<RestoredPrefix, String> {
    if identity.dirty {
        return Err("full resume requires a clean candidate".to_string());
    }
    let source_metadata = fs::symlink_metadata(source)
        .map_err(|error| format!("inspect {}: {error}", source.display()))?;
    if source_metadata.file_type().is_symlink() || !source_metadata.is_dir() {
        return Err("stage bundle root must be a real directory".to_string());
    }
    validate_bundle_layout(source)?;
    let manifest_path = source.join(MANIFEST);
    let complete_path = source.join(COMPLETE);
    let expected_manifest_hash = read_completion_marker(&complete_path)?;
    validate_bounded_regular_file(&manifest_path, MAX_MANIFEST_BYTES, "manifest")?;
    let actual_manifest_hash = super::super::sha256_file(&manifest_path)?;
    if expected_manifest_hash != actual_manifest_hash {
        return Err("stage bundle completion marker does not match its manifest".to_string());
    }
    let manifest: BundleManifest = read_json(&manifest_path)?;
    if manifest.format != FORMAT {
        return Err(format!(
            "unsupported stage bundle format {}",
            manifest.format
        ));
    }
    validate_identity(&manifest.identity)?;
    if manifest.source_run_id.is_empty()
        || manifest.elapsed_millis == 0
        || !manifest.profile.acceptance
    {
        return Err("stage bundle has incomplete full-profile provenance".to_string());
    }
    if manifest.identity.dirty
        || manifest.identity.server_binary_sha256 != identity.server_binary_sha256
        || manifest.identity.cargo_lock_sha256 != identity.cargo_lock_sha256
    {
        return Err("stage bundle candidate identity does not match".to_string());
    }
    if &manifest.profile != profile || manifest.seed != seed {
        return Err("stage bundle profile or seed does not match".to_string());
    }
    validate_prefix(
        manifest.stage,
        &manifest.layers,
        &manifest.reopen_oracles,
        &manifest.crashes,
        &manifest.post_stage_oracle,
        &manifest.stage_sources,
    )?;
    validate_stage_identity(&manifest.stage_sources, &manifest.identity)?;
    let actual_files = inventory(source)?;
    if actual_files != manifest.files {
        return Err("stage bundle durable files differ from the manifest".to_string());
    }
    verify_journals(source, manifest.stage, &manifest.layers)?;

    let destination = run_root.join("server-data");
    if destination.exists() {
        return Err(format!(
            "resume destination already exists: {}",
            destination.display()
        ));
    }
    let expected_database_files = inventory(&source.join("server-data"))?;
    copy_tree(&source.join("server-data"), &destination)?;
    if inventory(&destination)? != expected_database_files {
        return Err("resumed database copy differs from its stage bundle".to_string());
    }
    let journal_destination = run_root.join("journals");
    let expected_journals = inventory(&source.join("journals"))?;
    copy_tree(&source.join("journals"), &journal_destination)?;
    if inventory(&journal_destination)? != expected_journals {
        return Err("resumed journal copy differs from its stage bundle".to_string());
    }
    let accepted = run_root.join("accepted-prefix");
    fs::create_dir(&accepted).map_err(|error| error.to_string())?;
    copy_file(&manifest_path, &accepted.join(MANIFEST))?;
    copy_file(&complete_path, &accepted.join(COMPLETE))?;
    if inventory(source)? != manifest.files
        || read_completion_marker(&complete_path)? != actual_manifest_hash
        || super::super::sha256_file(&manifest_path)? != actual_manifest_hash
    {
        return Err("stage bundle changed while it was being restored".to_string());
    }
    if read_completion_marker(&accepted.join(COMPLETE))? != actual_manifest_hash
        || super::super::sha256_file(&accepted.join(MANIFEST))? != actual_manifest_hash
    {
        return Err("accepted stage evidence differs from its source".to_string());
    }
    sync_tree_directories(run_root)?;

    Ok(RestoredPrefix {
        source_bundle: source.to_path_buf(),
        source_run_id: manifest.source_run_id,
        elapsed_millis: manifest.elapsed_millis,
        stage: manifest.stage,
        historical: manifest.historical,
        fixture: manifest.fixture,
        layers: manifest.layers,
        reopen_oracles: manifest.reopen_oracles,
        crashes: manifest.crashes,
        post_stage_oracle: manifest.post_stage_oracle,
        telemetry: manifest.telemetry,
        stage_sources: manifest.stage_sources,
    })
}

pub fn copy_durable_tree(source: &Path, destination: &Path) -> Result<(), String> {
    validate_combined_budget(&[source])?;
    let expected = inventory(source)?;
    copy_tree(source, destination)?;
    if inventory(destination)? != expected {
        return Err("diagnostic durable-tree copy differs from source".to_string());
    }
    if inventory(source)? != expected {
        return Err("diagnostic durable tree changed while being copied".to_string());
    }
    sync_tree_directories(destination)
}

fn validate_identity(identity: &CandidateIdentity) -> Result<(), String> {
    if identity.commit.is_empty()
        || identity.cargo_lock_sha256.is_empty()
        || identity.server_binary_sha256.is_empty()
        || identity.server_binary.is_empty()
    {
        return Err("stage bundle candidate identity is incomplete".to_string());
    }
    Ok(())
}

fn validate_bundle_layout(root: &Path) -> Result<(), String> {
    let expected = [COMPLETE, MANIFEST, "journals", "server-data"];
    let observed = sorted_entries(root)?
        .into_iter()
        .map(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(str::to_string)
                .ok_or_else(|| "stage bundle root entry is not UTF-8".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    if observed != expected {
        return Err(format!(
            "stage bundle root layout differs: expected={expected:?} observed={observed:?}"
        ));
    }
    for directory in ["journals", "server-data"] {
        let metadata =
            fs::symlink_metadata(root.join(directory)).map_err(|error| error.to_string())?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(format!("stage bundle {directory} is not a real directory"));
        }
    }
    Ok(())
}

fn validate_stage_identity(
    stages: &[StageSourceEvidence],
    identity: &CandidateIdentity,
) -> Result<(), String> {
    if stages.iter().any(|stage| {
        stage.cargo_lock_sha256 != identity.cargo_lock_sha256
            || stage.server_binary_sha256 != identity.server_binary_sha256
    }) {
        return Err("stage source chain does not match bundle identity".to_string());
    }
    Ok(())
}

fn validate_prefix(
    stage: BundleStage,
    layers: &[LayerSummary],
    reopen_oracles: &[OracleSnapshot],
    crashes: &[CrashEvidence],
    post_stage_oracle: &OracleSnapshot,
    stage_sources: &[StageSourceEvidence],
) -> Result<(), String> {
    let expected = stage.index() + 1;
    if layers.len() != expected {
        return Err(format!(
            "stage bundle {} has {} layers instead of {expected}",
            stage.name(),
            layers.len()
        ));
    }
    for (index, layer) in layers.iter().enumerate() {
        layer.validate()?;
        if layer.name != BundleStage::ALL[index].name() {
            return Err(format!(
                "stage bundle layer {index} is {} instead of {}",
                layer.name,
                BundleStage::ALL[index].name()
            ));
        }
    }
    if crashes.len() != expected {
        return Err(format!(
            "stage bundle {} has {} crash oracles instead of {expected}",
            stage.name(),
            crashes.len()
        ));
    }
    for (index, crash) in crashes.iter().enumerate() {
        if crash.layer != BundleStage::ALL[index].name() || crash.expected != crash.observed {
            return Err(format!(
                "invalid crash evidence at stage {index}: {crash:?}"
            ));
        }
    }
    let minimum_reopens = 1 + expected * 2;
    if reopen_oracles.len() < minimum_reopens {
        return Err(format!(
            "stage bundle {} has {} reopen oracles, expected at least {minimum_reopens}",
            stage.name(),
            reopen_oracles.len()
        ));
    }
    for snapshot in reopen_oracles {
        snapshot.validate()?;
    }
    post_stage_oracle.validate()?;
    if reopen_oracles.last() != Some(post_stage_oracle) {
        return Err("stage bundle post-stage oracle is not the last reopen oracle".to_string());
    }
    if stage_sources.len() != expected {
        return Err(format!(
            "stage bundle {} has {} source records instead of {expected}",
            stage.name(),
            stage_sources.len()
        ));
    }
    let first = stage_sources
        .first()
        .ok_or_else(|| "stage bundle has no source identity".to_string())?;
    for (index, source) in stage_sources.iter().enumerate() {
        if source.stage != BundleStage::ALL[index]
            || source.source_run_id.is_empty()
            || source.source_commit.is_empty()
            || source.cargo_lock_sha256 != first.cargo_lock_sha256
            || source.server_binary_sha256 != first.server_binary_sha256
        {
            return Err(format!("invalid source identity at stage {index}"));
        }
    }
    Ok(())
}

fn verify_journals(
    source: &Path,
    stage: BundleStage,
    layers: &[LayerSummary],
) -> Result<(), String> {
    let journal_root = source.join("journals");
    let completed_stages = &BundleStage::ALL[..=stage.index()];
    for path in sorted_entries(&journal_root)? {
        let metadata = fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| "stage bundle journal name is not UTF-8".to_string())?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || !name.ends_with(".journal")
            || !completed_stages
                .iter()
                .any(|completed| name.starts_with(completed.journal_prefix()))
        {
            return Err(format!(
                "stage bundle contains unexpected journal object {}",
                path.display()
            ));
        }
    }
    for (index, layer) in layers.iter().enumerate() {
        let prefix = BundleStage::ALL[index].journal_prefix();
        let mut paths = sorted_entries(&journal_root)?
            .into_iter()
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(prefix) && name.ends_with(".journal"))
            })
            .collect::<Vec<_>>();
        paths.sort();
        if paths.is_empty() {
            return Err(format!("stage bundle has no {prefix} journals"));
        }
        let observed = merge_journals(
            paths
                .iter()
                .map(|path| journal::inspect(path))
                .collect::<Result<Vec<JournalSummary>, String>>()?,
        )?;
        if observed != layer.journal {
            return Err(format!(
                "stage bundle journal differs for {}",
                BundleStage::ALL[index].name()
            ));
        }
    }
    if layers.last().map(|layer| layer.name.as_str()) != Some(stage.name()) {
        return Err("stage bundle terminal layer does not match stage".to_string());
    }
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(source)
        .map_err(|error| format!("inspect {}: {error}", source.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "bundle source is not a real directory: {}",
            source.display()
        ));
    }
    fs::create_dir(destination).map_err(|error| error.to_string())?;
    for source_path in sorted_entries(source)? {
        let file_name = source_path
            .file_name()
            .ok_or_else(|| format!("path has no file name: {}", source_path.display()))?;
        let destination_path = destination.join(file_name);
        let metadata = fs::symlink_metadata(&source_path).map_err(|error| error.to_string())?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "stage bundle rejects symlink {}",
                source_path.display()
            ));
        }
        if metadata.is_dir() {
            copy_tree(&source_path, &destination_path)?;
        } else if metadata.is_file() {
            copy_file(&source_path, &destination_path)?;
        } else {
            return Err(format!(
                "stage bundle rejects special file {}",
                source_path.display()
            ));
        }
    }
    sync_directory(destination)
}

fn copy_file(source: &Path, destination: &Path) -> Result<(), String> {
    fs::copy(source, destination).map_err(|error| error.to_string())?;
    File::open(destination)
        .and_then(|file| file.sync_all())
        .map_err(|error| error.to_string())
}

fn inventory(root: &Path) -> Result<Vec<FileEvidence>, String> {
    let mut files = Vec::new();
    let mut budget = InventoryBudget::default();
    collect_inventory(root, root, 0, &mut files, &mut budget)?;
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    if files.len() > MAX_FILES {
        return Err(format!("stage bundle exceeds {MAX_FILES} files"));
    }
    let bytes = files.iter().try_fold(0_u64, |total, entry| {
        total
            .checked_add(entry.bytes)
            .ok_or_else(|| "stage bundle byte count overflow".to_string())
    })?;
    if bytes > MAX_BYTES {
        return Err(format!("stage bundle exceeds {MAX_BYTES} bytes"));
    }
    Ok(files)
}

#[derive(Default)]
struct InventoryBudget {
    entries: usize,
    bytes: u64,
}

impl InventoryBudget {
    fn admit(&mut self, bytes: u64) -> Result<(), String> {
        self.entries = self
            .entries
            .checked_add(1)
            .ok_or_else(|| "stage bundle entry count overflow".to_string())?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| "stage bundle byte count overflow".to_string())?;
        if self.entries > MAX_FILES || self.bytes > MAX_BYTES {
            return Err(format!(
                "stage bundle exceeds {MAX_FILES} entries or {MAX_BYTES} bytes"
            ));
        }
        Ok(())
    }
}

fn validate_combined_budget(roots: &[&Path]) -> Result<(), String> {
    let inventories = roots
        .iter()
        .map(|root| inventory(root))
        .collect::<Result<Vec<_>, _>>()?;
    let entries = inventories.iter().map(Vec::len).sum::<usize>();
    let bytes = inventories
        .iter()
        .flatten()
        .try_fold(0_u64, |total, entry| {
            total
                .checked_add(entry.bytes)
                .ok_or_else(|| "stage bundle byte count overflow".to_string())
        })?;
    if entries > MAX_FILES || bytes > MAX_BYTES {
        return Err(format!(
            "stage bundle source exceeds {MAX_FILES} entries or {MAX_BYTES} bytes"
        ));
    }
    Ok(())
}

fn collect_inventory(
    root: &Path,
    path: &Path,
    depth: usize,
    files: &mut Vec<FileEvidence>,
    budget: &mut InventoryBudget,
) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err(format!("stage bundle exceeds path depth {MAX_DEPTH}"));
    }
    for entry in sorted_entries(path)? {
        let relative = relative_path(root, &entry)?;
        if path == root && matches!(relative.as_str(), MANIFEST | COMPLETE) {
            continue;
        }
        let metadata = fs::symlink_metadata(&entry).map_err(|error| error.to_string())?;
        if metadata.file_type().is_symlink() {
            return Err(format!("stage bundle rejects symlink {}", entry.display()));
        }
        if metadata.is_dir() {
            budget.admit(0)?;
            files.push(FileEvidence {
                relative_path: relative,
                directory: true,
                bytes: 0,
                sha256: String::new(),
            });
            collect_inventory(
                root,
                &entry,
                depth
                    .checked_add(1)
                    .ok_or_else(|| "stage bundle path depth overflow".to_string())?,
                files,
                budget,
            )?;
        } else if metadata.is_file() {
            budget.admit(metadata.len())?;
            files.push(FileEvidence {
                relative_path: relative,
                directory: false,
                bytes: metadata.len(),
                sha256: super::super::sha256_file(&entry)?,
            });
        } else {
            return Err(format!(
                "stage bundle rejects special file {}",
                entry.display()
            ));
        }
    }
    Ok(())
}

fn relative_path(root: &Path, path: &Path) -> Result<String, String> {
    let relative = path
        .strip_prefix(root)
        .map_err(|error| error.to_string())?
        .to_str()
        .ok_or_else(|| "stage bundle path is not UTF-8".to_string())
        .map(|value| value.replace(std::path::MAIN_SEPARATOR, "/"))?;
    if relative.len() > MAX_RELATIVE_PATH_BYTES {
        return Err(format!(
            "stage bundle relative path exceeds {MAX_RELATIVE_PATH_BYTES} bytes"
        ));
    }
    Ok(relative)
}

fn sorted_entries(path: &Path) -> Result<Vec<PathBuf>, String> {
    let mut entries = fs::read_dir(path)
        .map_err(|error| format!("read {}: {error}", path.display()))?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort();
    Ok(entries)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, String> {
    validate_bounded_regular_file(path, MAX_MANIFEST_BYTES, "manifest")?;
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

fn read_completion_marker(path: &Path) -> Result<String, String> {
    validate_bounded_regular_file(path, MAX_COMPLETE_BYTES, "completion marker")?;
    let value =
        fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let value = value.trim();
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("stage bundle completion marker is not a SHA-256 digest".to_string());
    }
    Ok(value.to_ascii_lowercase())
}

fn validate_bounded_regular_file(path: &Path, max_bytes: u64, name: &str) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("inspect {}: {error}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > max_bytes {
        return Err(format!(
            "stage bundle {name} is not a regular file bounded by {max_bytes} bytes"
        ));
    }
    Ok(())
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    write_bytes(path, &bytes)
}

fn write_bytes(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut file = File::create(path).map_err(|error| error.to_string())?;
    file.write_all(bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())
}

fn sync_tree_directories(path: &Path) -> Result<(), String> {
    for entry in sorted_entries(path)? {
        if fs::symlink_metadata(&entry)
            .map_err(|error| error.to_string())?
            .is_dir()
        {
            sync_tree_directories(&entry)?;
        }
    }
    sync_directory(path)
}

fn sync_directory(path: &Path) -> Result<(), String> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use crate::common::prerelease::epoch_chaos::{
        journal::{JournalShard, RecordState},
        model::LatencyHistogram,
    };

    #[test]
    fn stage_order_and_names_are_stable() {
        let names = BundleStage::ALL.map(BundleStage::name);
        assert_eq!(
            names,
            [
                "wide-transaction",
                "large-transactions",
                "micro-transactions",
                "high-concurrency-random"
            ]
        );
        for (index, stage) in BundleStage::ALL.into_iter().enumerate() {
            assert_eq!(stage.index(), index);
        }
    }

    #[test]
    fn copied_tree_has_identical_bounded_inventory() {
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source");
        let destination = temporary.path().join("destination");
        fs::create_dir(&source).unwrap();
        fs::create_dir(source.join("nested")).unwrap();
        fs::write(source.join("root.bin"), b"root").unwrap();
        fs::write(source.join("nested/member.bin"), b"member").unwrap();
        copy_tree(&source, &destination).unwrap();
        assert_eq!(
            inventory(&source).unwrap(),
            inventory(&destination).unwrap()
        );
    }

    #[test]
    fn inventory_excludes_only_root_bundle_metadata() {
        let temporary = tempfile::tempdir().unwrap();
        fs::write(temporary.path().join(MANIFEST), b"manifest").unwrap();
        fs::write(temporary.path().join(COMPLETE), b"complete").unwrap();
        fs::create_dir(temporary.path().join("nested")).unwrap();
        fs::write(temporary.path().join("nested").join(MANIFEST), b"payload").unwrap();

        let observed = inventory(temporary.path()).unwrap();
        assert_eq!(observed.len(), 2);
        assert_eq!(observed[0].relative_path, "nested");
        assert_eq!(observed[1].relative_path, "nested/bundle.json");
    }

    #[test]
    fn inventory_budget_rejects_limits_before_hashing_or_copying() {
        let mut bytes = InventoryBudget::default();
        assert!(bytes.admit(MAX_BYTES.saturating_add(1)).is_err());

        let mut entries = InventoryBudget {
            entries: MAX_FILES,
            bytes: 0,
        };
        assert!(entries.admit(0).is_err());
    }

    #[test]
    fn inventory_rejects_excessive_depth_and_path_length() {
        let temporary = tempfile::tempdir().unwrap();
        let mut nested = temporary.path().to_path_buf();
        for _ in 0..=MAX_DEPTH {
            nested.push("d");
            fs::create_dir(&nested).unwrap();
        }
        assert!(inventory(temporary.path()).is_err());

        let long = temporary
            .path()
            .join("x".repeat(MAX_RELATIVE_PATH_BYTES + 1));
        assert!(relative_path(temporary.path(), &long).is_err());
    }

    #[test]
    fn journal_validation_rejects_future_stage_files() {
        let temporary = tempfile::tempdir().unwrap();
        fs::create_dir(temporary.path().join("journals")).unwrap();
        fs::write(
            temporary.path().join("journals/random-2048.journal"),
            b"future",
        )
        .unwrap();

        let error =
            verify_journals(temporary.path(), BundleStage::WideTransaction, &[]).unwrap_err();
        assert!(error.contains("unexpected journal object"), "{error}");
    }

    #[test]
    fn bundle_layout_rejects_extra_root_objects() {
        let temporary = tempfile::tempdir().unwrap();
        for directory in ["journals", "server-data"] {
            fs::create_dir(temporary.path().join(directory)).unwrap();
        }
        for file in [MANIFEST, COMPLETE] {
            fs::write(temporary.path().join(file), b"value").unwrap();
        }
        validate_bundle_layout(temporary.path()).unwrap();
        fs::write(temporary.path().join("unexpected"), b"value").unwrap();
        assert!(validate_bundle_layout(temporary.path()).is_err());
    }

    #[test]
    fn published_bundle_restores_only_with_matching_identity_and_journals() {
        let temporary = tempfile::tempdir().unwrap();
        let run_root = temporary.path().join("run");
        fs::create_dir(&run_root).unwrap();
        fs::create_dir(run_root.join("server-data")).unwrap();
        fs::write(run_root.join("server-data/member.bin"), b"durable").unwrap();
        let mut shard = JournalShard::create(&run_root.join("journals"), "wide", 0).unwrap();
        shard.plan_and_start(1, 7).unwrap();
        shard.terminal(1, 7, RecordState::Committed).unwrap();
        let journal = shard.seal().unwrap();
        let mut latency = LatencyHistogram::default();
        latency.record(Duration::from_micros(10));
        let layer = LayerSummary {
            name: BundleStage::WideTransaction.name().to_string(),
            wall_millis: 1,
            semantic_operations: 1,
            transactions: 1,
            committed_transactions: 1,
            transaction_latency: latency,
            active_clients_high_watermark: 1,
            throughput_operations_per_second: 1.0,
            journal,
            ..LayerSummary::default()
        };
        layer.validate().unwrap();
        let source_identity = identity("source");
        let profile = profile();
        let oracle = oracle();
        let historical = HistoricalEvidence {
            committed: 1,
            rolled_back: 0,
            disconnected: 0,
            conflicts: 0,
            checkpoint_busy: 0,
        };
        let fixture = FixtureEvidence {
            cold_rows: 1,
            cold_copy_chunks: 1,
            hot_rows: 1,
            cold_messenger_bundles: 1,
            hot_messenger_bundles: 1,
            checksum: 1,
        };
        let crash = CrashEvidence {
            layer: BundleStage::WideTransaction.name().to_string(),
            point: "test-point".to_string(),
            expected: "committed".to_string(),
            observed: "committed".to_string(),
        };
        let source = StageSourceEvidence {
            stage: BundleStage::WideTransaction,
            source_run_id: "source-run".to_string(),
            source_commit: source_identity.commit.clone(),
            cargo_lock_sha256: source_identity.cargo_lock_sha256.clone(),
            server_binary_sha256: source_identity.server_binary_sha256.clone(),
        };
        let bundle = publish(PublishInput {
            run_root: &run_root,
            run_id: "source-run",
            elapsed_millis: 1,
            stage: BundleStage::WideTransaction,
            identity: &source_identity,
            profile: &profile,
            seed: 7,
            historical: &historical,
            fixture: &fixture,
            layers: std::slice::from_ref(&layer),
            reopen_oracles: &[oracle.clone(), oracle.clone(), oracle.clone()],
            crashes: std::slice::from_ref(&crash),
            post_stage_oracle: &oracle,
            telemetry: &TelemetryReport::default(),
            stage_sources: std::slice::from_ref(&source),
        })
        .unwrap();

        let mismatched_root = temporary.path().join("mismatched");
        fs::create_dir(&mismatched_root).unwrap();
        assert!(restore(&bundle, &mismatched_root, &source_identity, &profile, 8).is_err());
        assert!(!mismatched_root.join("server-data").exists());

        let restored_root = temporary.path().join("restored");
        fs::create_dir(&restored_root).unwrap();
        let restored = restore(
            &bundle,
            &restored_root,
            &identity("harness-only"),
            &profile,
            7,
        )
        .unwrap();
        assert_eq!(restored.stage, BundleStage::WideTransaction);
        assert_eq!(restored.layers[0].journal, layer.journal);
        assert_eq!(
            fs::read(restored_root.join("server-data/member.bin")).unwrap(),
            b"durable"
        );
        assert_eq!(
            fs::read(restored_root.join("journals/wide-0000.journal")).unwrap(),
            fs::read(bundle.join("journals/wide-0000.journal")).unwrap()
        );

        let completion = fs::read(bundle.join(COMPLETE)).unwrap();
        fs::write(
            bundle.join(COMPLETE),
            vec![b'x'; MAX_COMPLETE_BYTES as usize + 1],
        )
        .unwrap();
        let oversized_root = temporary.path().join("oversized-marker");
        fs::create_dir(&oversized_root).unwrap();
        assert!(restore(&bundle, &oversized_root, &source_identity, &profile, 7).is_err());
        assert!(!oversized_root.join("server-data").exists());
        fs::write(bundle.join(COMPLETE), completion).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let manifest = fs::read(bundle.join(MANIFEST)).unwrap();
            let external_manifest = temporary.path().join("external-manifest.json");
            fs::write(&external_manifest, manifest).unwrap();
            fs::remove_file(bundle.join(MANIFEST)).unwrap();
            symlink(&external_manifest, bundle.join(MANIFEST)).unwrap();
            let symlink_root = temporary.path().join("symlink-manifest");
            fs::create_dir(&symlink_root).unwrap();
            assert!(restore(&bundle, &symlink_root, &source_identity, &profile, 7).is_err());
            assert!(!symlink_root.join("server-data").exists());
            fs::remove_file(bundle.join(MANIFEST)).unwrap();
            fs::copy(&external_manifest, bundle.join(MANIFEST)).unwrap();
        }

        fs::write(bundle.join("server-data/member.bin"), b"tampered").unwrap();
        let rejected_root = temporary.path().join("rejected");
        fs::create_dir(&rejected_root).unwrap();
        assert!(restore(&bundle, &rejected_root, &source_identity, &profile, 7).is_err());
    }

    fn identity(commit: &str) -> CandidateIdentity {
        CandidateIdentity {
            commit: commit.to_string(),
            dirty: false,
            cargo_lock_sha256: "lock".to_string(),
            server_binary_sha256: "server".to_string(),
            server_binary: "radixdb-server".to_string(),
        }
    }

    fn profile() -> ProfileEvidence {
        ProfileEvidence {
            kind: "full".to_string(),
            acceptance: true,
            wide_actions: 100_000,
            large_workers: 64,
            large_actions_per_worker: 25_000,
            micro_transactions: 2_000_000,
            random_rungs: vec![32, 64, 128, 256, 512],
            random_operations_per_rung: 400_000,
            cold_rows: 1_000_000,
            hot_rows: 25_000,
            stage_timeout_millis: 6 * 60 * 60 * 1_000,
        }
    }

    fn oracle() -> OracleSnapshot {
        OracleSnapshot {
            users: 1,
            conversations: 1,
            messages: 0,
            outbox: 0,
            sync_events: 0,
            reactions: 0,
            receipts: 0,
            command_results: 0,
            chaos_cells: 1,
            crash_ledger: 1,
            orphan_messages: 0,
            orphan_outbox: 0,
            orphan_sync_messages: 0,
            orphan_sync_outbox: 0,
            orphan_reactions: 0,
            orphan_receipts: 0,
            orphan_commands: 0,
        }
    }
}
