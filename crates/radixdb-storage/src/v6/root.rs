use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use radixdb_catalog::{
    CatalogGraph, CatalogName, CatalogObject, CatalogPackMeta, CatalogPayload, NamespacePayload,
    ObjectId,
};

use super::publication::filesystem::source::{catalog_path, database_manifest_path, wal_path};
use super::publication::filesystem::{publish_members, GenerationPublicationPlan};
use super::{
    decode_database_manifest, encode_catalog_artifact, encode_database_manifest, CatalogGeneration,
    CatalogId, CatalogRef, CatalogRootRef, CompleteStagingSet, ControlRecord, ControlSlotIndex,
    DatabaseGeneration, DatabaseId, DatabaseManifest, DatabaseManifestRootRef, FormatError,
    FormatResult, ManifestGeneration, ManifestId, StagedArtifactSet, StagedMemberRole,
    StagingDiscoveryLimits, WalGeneration, WalReplayFloor, WriterInstanceId,
};

const INITIAL_GENERATION: u64 = 1;
const INITIAL_LSN: u64 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseRootState {
    Existing,
    Created(ControlRecord),
    Resumed(ControlRecord),
}

/// Owns recognition and first publication of a canonical database root.
///
/// The caller must hold the database-wide runtime lock. A root with either
/// CONTROL slot is never modified here: ordinary recovery is its sole owner.
pub struct DatabaseRoot<'a> {
    path: &'a Path,
}

impl<'a> DatabaseRoot<'a> {
    pub const fn new(path: &'a Path) -> Self {
        Self { path }
    }

    pub fn open_or_create(&self, now_unix_ns: u64) -> FormatResult<DatabaseRootState> {
        let now_unix_ns = now_unix_ns.max(1);
        ensure_root_directory(self.path)?;
        reject_legacy_root(self.path)?;
        if control_exists(self.path)? {
            return Ok(DatabaseRootState::Existing);
        }
        validate_root_names(self.path)?;

        if let Some((staging, control)) = resume_initial_publication(self.path)? {
            let plan = GenerationPublicationPlan::prepare_initial(self.path, &staging, control)?;
            publish_members(self.path, &staging, &plan)?;
            retire_staging_best_effort(staging.path());
            return Ok(DatabaseRootState::Resumed(control));
        }

        if has_final_generation_members(self.path)? {
            return Err(FormatError::InvalidDatabaseRoot {
                detail: "immutable generation members exist without CONTROL or resumable staging",
            });
        }
        reset_unpublished_staging(self.path)?;
        let (staging, control) = stage_initial_generation(self.path, now_unix_ns)?;
        let plan = GenerationPublicationPlan::prepare_initial(self.path, &staging, control)?;
        publish_members(self.path, &staging, &plan)?;
        retire_staging_best_effort(staging.path());
        Ok(DatabaseRootState::Created(control))
    }
}

fn stage_initial_generation(
    root: &Path,
    now_unix_ns: u64,
) -> FormatResult<(CompleteStagingSet, ControlRecord)> {
    let database_id = DatabaseId::new();
    let catalog_id = CatalogId::new();
    let database_generation = DatabaseGeneration::new(INITIAL_GENERATION)?;
    let catalog_generation = CatalogGeneration::new(INITIAL_GENERATION)?;
    let manifest_generation = ManifestGeneration::new(INITIAL_GENERATION)?;
    let floor = WalReplayFloor::new(WalGeneration::new(INITIAL_GENERATION)?, INITIAL_LSN);
    let writer_id = WriterInstanceId::new();

    let namespace = CatalogObject::new(
        ObjectId::BOOTSTRAP_NAMESPACE,
        None,
        None,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new("public")?,
        1,
        CatalogPayload::Namespace(NamespacePayload::new()),
    )?;
    let graph = CatalogGraph::build(vec![namespace], vec![])?;
    let catalog_meta = CatalogPackMeta::new(
        database_id.into_bytes(),
        catalog_id.into_bytes(),
        catalog_generation.get(),
        INITIAL_LSN,
        now_unix_ns,
    )?;
    let catalog_bytes = encode_catalog_artifact(catalog_meta, &graph)?;
    let catalog_reference = CatalogRef::new(
        catalog_id,
        catalog_generation,
        catalog_bytes.len() as u64,
        footer_sha(&catalog_bytes)?,
    )?;

    let manifest_id = ManifestId::new();
    let manifest = DatabaseManifest::new(
        database_id,
        manifest_id,
        database_generation,
        catalog_reference,
        floor,
        0,
        vec![],
        now_unix_ns,
    )?;
    let manifest_bytes = encode_database_manifest(&manifest)?;
    let control = ControlRecord::new(
        ControlSlotIndex::Zero,
        database_generation,
        database_id,
        DatabaseManifestRootRef::new(
            manifest_id,
            manifest_generation,
            footer_sha(&manifest_bytes)?,
        ),
        CatalogRootRef::new(catalog_id, catalog_generation, footer_sha(&catalog_bytes)?),
        floor,
        now_unix_ns,
        writer_id,
    )?;

    let staging_root = root.join("staging");
    create_durable_directory(root, &staging_root)?;
    let staged = StagedArtifactSet::create(
        &staging_root,
        writer_id,
        database_generation,
        process_id(),
        now_unix_ns,
    )?;
    write_staged_bytes(
        &staged,
        StagedMemberRole::CatalogPack,
        &catalog_path(catalog_reference),
        &catalog_bytes,
    )?;
    write_staged_bytes(
        &staged,
        StagedMemberRole::WalSuccessor,
        &wal_path(floor.generation()),
        &[],
    )?;
    write_staged_bytes(
        &staged,
        StagedMemberRole::DatabaseManifest,
        &database_manifest_path(control.database_manifest()),
        &manifest_bytes,
    )?;
    Ok((
        staged.mark_complete(now_unix_ns, StagingDiscoveryLimits::default())?,
        control,
    ))
}

fn resume_initial_publication(
    root: &Path,
) -> FormatResult<Option<(CompleteStagingSet, ControlRecord)>> {
    let staging_root = root.join("staging");
    if !path_exists(&staging_root, "inspect staging root")? {
        return Ok(None);
    }
    require_directory(&staging_root, "inspect staging root")?;
    let mut candidate = None;
    for writer_entry in bounded_entries(&staging_root)? {
        require_directory(&writer_entry.path(), "inspect staging writer")?;
        let writer_id = WriterInstanceId::from_str(writer_entry.file_name().to_str().ok_or(
            FormatError::InvalidDatabaseRoot {
                detail: "staging writer name is not UTF-8",
            },
        )?)
        .map_err(|_| FormatError::InvalidDatabaseRoot {
            detail: "staging writer name is not a canonical identity",
        })?;
        for publication_entry in bounded_entries(&writer_entry.path())? {
            require_directory(&publication_entry.path(), "inspect staging publication")?;
            if !publication_entry.path().join("COMPLETE").is_file() {
                continue;
            }
            let resumed = reopen_initial_candidate(root, &publication_entry.path(), writer_id)?;
            if candidate.replace(resumed).is_some() {
                return Err(FormatError::InvalidDatabaseRoot {
                    detail: "multiple complete initial publications exist without CONTROL",
                });
            }
        }
    }
    Ok(candidate)
}

fn reopen_initial_candidate(
    root: &Path,
    publication: &Path,
    directory_writer_id: WriterInstanceId,
) -> FormatResult<(CompleteStagingSet, ControlRecord)> {
    let generation = ManifestGeneration::new(INITIAL_GENERATION)?;
    let relative_manifest =
        PathBuf::from("manifests").join(format!("database-{:016x}.mft", generation.get()));
    let manifest_bytes = read_staged_or_final(publication, root, &relative_manifest)?;
    let manifest = decode_database_manifest(&manifest_bytes)?;
    if manifest.generation().get() != INITIAL_GENERATION
        || !manifest.tables().is_empty()
        || manifest.wal_replay_floor().generation().get() != INITIAL_GENERATION
        || manifest.wal_replay_floor().lsn() != INITIAL_LSN
        || manifest.catalog().generation().get() != INITIAL_GENERATION
    {
        return Err(FormatError::InvalidDatabaseRoot {
            detail: "staged initial database manifest is not the empty generation one",
        });
    }
    let relative_catalog = catalog_path(manifest.catalog());
    let relative_wal = wal_path(manifest.wal_replay_floor().generation());
    let paths = vec![relative_catalog, relative_wal, relative_manifest.clone()];
    let staging = CompleteStagingSet::reopen_for_publication(
        publication,
        root,
        &paths,
        StagingDiscoveryLimits::default(),
    )?;
    if staging.owner().writer_instance_id() != directory_writer_id
        || staging.owner().intended_generation().get() != INITIAL_GENERATION
    {
        return Err(FormatError::InvalidDatabaseRoot {
            detail: "staged initial owner differs from its directory or generation",
        });
    }
    let control = ControlRecord::new(
        ControlSlotIndex::Zero,
        DatabaseGeneration::new(INITIAL_GENERATION)?,
        manifest.database_id(),
        DatabaseManifestRootRef::new(
            manifest.manifest_id(),
            generation,
            footer_sha(&manifest_bytes)?,
        ),
        CatalogRootRef::new(
            manifest.catalog().id(),
            manifest.catalog().generation(),
            *manifest.catalog().body_sha256(),
        ),
        manifest.wal_replay_floor(),
        staging.owner().created_unix_ns(),
        staging.owner().writer_instance_id(),
    )?;
    Ok((staging, control))
}

fn write_staged_bytes(
    staged: &StagedArtifactSet,
    role: StagedMemberRole,
    relative: &Path,
    bytes: &[u8],
) -> FormatResult<()> {
    let relative = relative.to_str().ok_or(FormatError::InvalidDatabaseRoot {
        detail: "canonical initial member path is not UTF-8",
    })?;
    staged.write_generation_file(role, relative, |file| {
        file.write_all(bytes)
            .map_err(|error| root_io("write initial generation member", error))
    })
}

fn reject_legacy_root(root: &Path) -> FormatResult<()> {
    for name in [
        "db.lock",
        "volumes",
        ".restore-state.bin",
        ".restore-state.bin.tmp",
        ".restore-staging",
        ".restore-backup",
    ] {
        if path_exists(&root.join(name), "inspect legacy database root")? {
            return Err(FormatError::LegacyDatabaseRoot);
        }
    }
    let wal = root.join("wal");
    if wal.is_dir() {
        for entry in bounded_entries(&wal)? {
            let name = entry.file_name();
            let name = name.to_str().unwrap_or_default();
            if name == "checkpoint.meta"
                || name.starts_with("checkpoint.meta.")
                || name.starts_with("wal_")
                || name.contains("-lsn-")
                || name.ends_with(".log.bak")
                || name.starts_with("wal-temp-")
            {
                return Err(FormatError::LegacyDatabaseRoot);
            }
        }
    }
    Ok(())
}

fn validate_root_names(root: &Path) -> FormatResult<()> {
    for entry in bounded_entries(root)? {
        let name = entry.file_name();
        let name = name.to_str().ok_or(FormatError::InvalidDatabaseRoot {
            detail: "database root contains a non-UTF-8 entry",
        })?;
        if !matches!(
            name,
            "LOCK"
                | "wal"
                | "catalog"
                | "manifests"
                | "artifacts"
                | "staging"
                | "quarantine"
                | "snapshots"
        ) {
            return Err(FormatError::InvalidDatabaseRoot {
                detail: "database root contains an unknown entry without CONTROL",
            });
        }
    }
    Ok(())
}

fn has_final_generation_members(root: &Path) -> FormatResult<bool> {
    for name in ["wal", "catalog", "manifests", "artifacts"] {
        let path = root.join(name);
        if path_exists(&path, "inspect generation directory")? && contains_regular_file(&path, 0)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn contains_regular_file(path: &Path, depth: usize) -> FormatResult<bool> {
    if depth > 6 {
        return Err(FormatError::InvalidDatabaseRoot {
            detail: "database root inspection exceeded recursion limit",
        });
    }
    require_directory(path, "inspect generation directory")?;
    for entry in bounded_entries(path)? {
        let metadata = std::fs::symlink_metadata(entry.path())
            .map_err(|error| root_io("inspect generation member", error))?;
        if metadata.file_type().is_symlink() {
            return Err(FormatError::InvalidDatabaseRoot {
                detail: "database root contains a symlink",
            });
        }
        if metadata.is_file() {
            return Ok(true);
        }
        if metadata.is_dir() && contains_regular_file(&entry.path(), depth + 1)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn reset_unpublished_staging(root: &Path) -> FormatResult<()> {
    let staging = root.join("staging");
    if path_exists(&staging, "inspect unpublished staging")? {
        std::fs::remove_dir_all(&staging)
            .map_err(|error| root_io("remove unpublished staging", error))?;
        sync_directory(root, "sync unpublished staging removal")?;
    }
    create_durable_directory(root, &staging)
}

fn retire_staging_best_effort(publication: &Path) {
    let writer = publication.parent().map(Path::to_path_buf);
    let staging = writer
        .as_deref()
        .and_then(Path::parent)
        .map(Path::to_path_buf);
    let _ = std::fs::remove_dir_all(publication);
    if let Some(writer) = writer {
        let _ = std::fs::remove_dir(&writer);
    }
    if let Some(staging) = staging {
        let _ = sync_directory(&staging, "sync completed staging cleanup");
    }
}

fn ensure_root_directory(root: &Path) -> FormatResult<()> {
    match std::fs::create_dir(root) {
        Ok(()) => {
            if let Some(parent) = root.parent() {
                sync_directory(parent, "sync database root parent")?;
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(root_io("create directory", error)),
    }
    require_directory(root, "inspect directory")
}

fn create_durable_directory(root: &Path, path: &Path) -> FormatResult<()> {
    match std::fs::create_dir(path) {
        Ok(()) => sync_directory(root, "sync database root directory")?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            require_directory(path, "inspect database root child")?;
        }
        Err(error) => return Err(root_io("create database root child", error)),
    }
    Ok(())
}

fn read_staged_or_final(staging: &Path, root: &Path, relative: &Path) -> FormatResult<Vec<u8>> {
    let staged = staging.join(relative);
    if path_exists(&staged, "inspect staged initial member")? {
        return read_regular(&staged);
    }
    read_regular(&root.join(relative))
}

fn read_regular(path: &Path) -> FormatResult<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| root_io("inspect initial generation member", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(FormatError::InvalidDatabaseRoot {
            detail: "initial generation member is not a regular file",
        });
    }
    let length = usize::try_from(metadata.len()).map_err(|_| FormatError::InvalidDatabaseRoot {
        detail: "initial generation member length does not fit this platform",
    })?;
    let mut file = open_read(path)?;
    let mut bytes = vec![0; length];
    file.read_exact(&mut bytes)
        .map_err(|error| root_io("read initial generation member", error))?;
    Ok(bytes)
}

fn footer_sha(bytes: &[u8]) -> FormatResult<[u8; 32]> {
    bytes
        .get(bytes.len().saturating_sub(32)..)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(FormatError::InvalidDatabaseRoot {
            detail: "initial generation member has no checksum footer",
        })
}

fn control_exists(root: &Path) -> FormatResult<bool> {
    Ok(path_exists(&root.join("CONTROL.0"), "inspect CONTROL.0")?
        || path_exists(&root.join("CONTROL.1"), "inspect CONTROL.1")?)
}

fn bounded_entries(path: &Path) -> FormatResult<Vec<std::fs::DirEntry>> {
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(path).map_err(|error| root_io("enumerate directory", error))? {
        if entries.len() >= 4_096 {
            return Err(FormatError::InvalidDatabaseRoot {
                detail: "database root inspection exceeded entry limit",
            });
        }
        entries.push(entry.map_err(|error| root_io("enumerate directory entry", error))?);
    }
    entries.sort_by_key(std::fs::DirEntry::file_name);
    Ok(entries)
}

fn require_directory(path: &Path, operation: &'static str) -> FormatResult<()> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| root_io(operation, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(FormatError::InvalidDatabaseRoot {
            detail: "required database-root path is not a real directory",
        });
    }
    Ok(())
}

fn path_exists(path: &Path, operation: &'static str) -> FormatResult<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(root_io(operation, error)),
    }
}

fn open_read(path: &Path) -> FormatResult<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    set_no_follow(&mut options);
    options
        .open(path)
        .map_err(|error| root_io("open initial generation member", error))
}

fn sync_directory(path: &Path, operation: &'static str) -> FormatResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| root_io(operation, error))
}

#[cfg(unix)]
fn set_no_follow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(not(unix))]
fn set_no_follow(_options: &mut OpenOptions) {}

#[cfg(not(target_os = "wasi"))]
fn process_id() -> u64 {
    u64::from(std::process::id())
}

#[cfg(target_os = "wasi")]
fn process_id() -> u64 {
    0
}

fn root_io(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::DatabaseRootIo {
        operation,
        kind: error.kind(),
    }
}

#[cfg(test)]
mod tests;
