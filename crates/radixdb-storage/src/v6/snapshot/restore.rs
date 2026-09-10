use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use super::super::publication::filesystem::source::{
    catalog_path, database_manifest_path, table_manifest_path, wal_path,
};
use super::super::recovery::validate_database_root_files;
use super::super::{
    decode_database_manifest, decode_table_manifest, encode_control_slot, ArtifactId, ArtifactKind,
    ArtifactRef, ControlRecord, ControlSlotIndex, DatabaseManifest, FormatError, FormatResult,
    GenerationCrashPoint, ReachabilityLimits, SnapshotId, TableManifestRef, UnavailableIndex,
    WalGeneration, WriterInstanceId,
};
use super::publication::validate_snapshot_member_file;
use super::{open_snapshot_manifest, SnapshotManifest, SnapshotMember, SnapshotMemberKind};

const COPY_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub struct SnapshotRestoreOutcome {
    target_root: PathBuf,
    snapshot_id: SnapshotId,
    control: ControlRecord,
    unavailable_indexes: Vec<UnavailableIndex>,
}

impl SnapshotRestoreOutcome {
    pub fn target_root(&self) -> &Path {
        &self.target_root
    }

    pub const fn snapshot_id(&self) -> SnapshotId {
        self.snapshot_id
    }

    pub const fn control(&self) -> ControlRecord {
        self.control
    }

    pub fn unavailable_indexes(&self) -> &[UnavailableIndex] {
        &self.unavailable_indexes
    }
}

/// Restore one immutable physical generation into a previously absent target.
/// The source snapshot is read-only; publication is one atomic directory
/// rename after the staged database graph has passed production validation.
pub fn restore_snapshot(
    snapshot_directory: impl AsRef<Path>,
    target_root: impl AsRef<Path>,
    writer_instance_id: WriterInstanceId,
    reachability_limits: ReachabilityLimits,
) -> FormatResult<SnapshotRestoreOutcome> {
    let snapshot_directory = snapshot_directory.as_ref();
    let target_root = target_root.as_ref();
    let target_parent = target_parent(target_root)?;
    validate_target_absent(target_root)?;
    validate_real_directory(target_parent, "inspect restore target parent")?;
    reject_source_inside_target(snapshot_directory, target_parent)?;

    let manifest = open_snapshot_manifest(snapshot_directory)?;
    let layout = RestoreLayout::inspect(snapshot_directory, &manifest)?;
    let control = ControlRecord::new(
        ControlSlotIndex::Zero,
        manifest.database_generation(),
        manifest.database_id(),
        manifest.database_manifest(),
        manifest.catalog(),
        layout.database_manifest.wal_replay_floor(),
        manifest.created_unix_ns(),
        writer_instance_id,
    )?;

    let staging = target_parent.join(format!(".restore-{}", manifest.snapshot_id()));
    require_absent(&staging, "inspect restore staging root")?;
    std::fs::create_dir(&staging)
        .map_err(|error| restore_io("create restore staging root", error))?;
    let mut cleanup = StagingCleanup::new(staging.clone());

    copy_members(snapshot_directory, &staging, &manifest, &layout)?;
    create_runtime_directories(&staging)?;
    write_control(&staging, control)?;
    sync_tree_directories(&staging, &manifest, &layout)?;

    let validated = validate_database_root_files(&staging, control, reachability_limits)?;
    validate_rebuild_state(&layout, validated.unavailable_indexes())?;
    super::super::fault::reach_generation_boundary(GenerationCrashPoint::RestoreStageValidated)
        .map_err(|error| restore_io("inject after restore staging validation", error))?;

    rename_root_without_replace(&staging, target_root)?;
    cleanup.disarm();
    super::super::fault::reach_generation_boundary(
        GenerationCrashPoint::RestoreAfterRootRenameBeforeParentSync,
    )
    .map_err(|error| restore_recovery_required("inject after restore root rename", error))?;
    sync_directory(target_parent, "sync restore target parent").map_err(|error| match error {
        FormatError::SnapshotIo { kind, .. } => FormatError::SnapshotRecoveryRequired {
            operation: "sync restore target parent",
            kind,
        },
        other => other,
    })?;
    super::super::fault::reach_generation_boundary(GenerationCrashPoint::RestoreParentDirDurable)
        .map_err(|error| restore_recovery_required("inject after restore root durability", error))?;

    Ok(SnapshotRestoreOutcome {
        target_root: target_root.to_path_buf(),
        snapshot_id: manifest.snapshot_id(),
        control,
        unavailable_indexes: validated.unavailable_indexes().to_vec(),
    })
}

struct RestoreLayout {
    database_manifest: DatabaseManifest,
    database_root: super::super::DatabaseManifestRootRef,
    table_paths: BTreeMap<[u8; 16], PathBuf>,
    omitted_indexes: BTreeMap<[u8; 16], ArtifactRef>,
}

impl RestoreLayout {
    fn inspect(snapshot_root: &Path, manifest: &SnapshotManifest) -> FormatResult<Self> {
        let database_member = member_by_identity(
            manifest,
            SnapshotMemberKind::DatabaseManifest,
            manifest.database_manifest().id().into_bytes(),
        )?;
        let database_bytes = read_small_member(snapshot_root, database_member)?;
        let database_manifest = decode_database_manifest(&database_bytes)?;
        validate_database_manifest(manifest, &database_manifest, database_member)?;

        let mut expected = BTreeSet::new();
        require_exact_member(
            manifest,
            SnapshotMember::database_manifest(
                manifest.database_manifest(),
                database_member.byte_length(),
            )?,
            &mut expected,
        )?;
        require_exact_member(
            manifest,
            SnapshotMember::catalog(database_manifest.catalog())?,
            &mut expected,
        )?;

        let mut table_paths = BTreeMap::new();
        let mut omitted_indexes = BTreeMap::new();
        for reference in database_manifest.tables().iter().copied() {
            let expected_table = SnapshotMember::table_manifest(reference)?;
            require_exact_member(manifest, expected_table, &mut expected)?;
            if table_paths
                .insert(expected_table.id(), table_manifest_path(reference))
                .is_some()
            {
                return invalid("database manifest reuses a table-manifest identity");
            }
            let table_bytes = read_small_member(snapshot_root, expected_table)?;
            let table = decode_table_manifest(&table_bytes)?;
            validate_table_identity(manifest, reference, &table)?;
            for segment in table.segments() {
                let data = SnapshotMember::artifact(segment.data_artifact())?;
                require_exact_member(manifest, data, &mut expected)?;
                if let Some(index_reference) = segment.index_artifact() {
                    let index = SnapshotMember::artifact(index_reference)?;
                    if optional_exact_member(manifest, index, &mut expected)? {
                        continue;
                    }
                    omitted_indexes.insert(index_reference.id().into_bytes(), index_reference);
                }
            }
        }

        validate_wal_members(manifest, database_manifest.wal_replay_floor().generation())?;
        for member in manifest
            .members()
            .iter()
            .filter(|member| member.kind() != SnapshotMemberKind::Wal)
        {
            if !expected.contains(&(member.kind(), member.id())) {
                return invalid("snapshot contains a member outside the database graph");
            }
        }

        Ok(Self {
            database_manifest,
            database_root: manifest.database_manifest(),
            table_paths,
            omitted_indexes,
        })
    }

    fn target_path(&self, member: SnapshotMember) -> FormatResult<PathBuf> {
        match member.kind() {
            SnapshotMemberKind::DatabaseManifest => Ok(database_manifest_path(self.database_root)),
            SnapshotMemberKind::Catalog => Ok(catalog_path(self.database_manifest.catalog())),
            SnapshotMemberKind::TableManifest => {
                self.table_paths.get(&member.id()).cloned().ok_or_else(|| {
                    invalid_error("table-manifest member is outside the database graph")
                })
            }
            SnapshotMemberKind::Data | SnapshotMemberKind::Index => {
                Ok(artifact_reference(member)?.relative_path())
            }
            SnapshotMemberKind::Wal => Ok(wal_path(WalGeneration::new(member.generation())?)),
        }
    }
}

fn validate_database_manifest(
    snapshot: &SnapshotManifest,
    database: &DatabaseManifest,
    member: SnapshotMember,
) -> FormatResult<()> {
    if database.database_id() != snapshot.database_id()
        || database.generation() != snapshot.database_generation()
        || database.manifest_id() != snapshot.database_manifest().id()
        || member.body_sha256() != *snapshot.database_manifest().body_sha256()
        || database.catalog().id() != snapshot.catalog().id()
        || database.catalog().generation() != snapshot.catalog().generation()
        || database.catalog().body_sha256() != snapshot.catalog().body_sha256()
    {
        return invalid("database manifest differs from snapshot roots");
    }
    Ok(())
}

fn validate_table_identity(
    snapshot: &SnapshotManifest,
    reference: TableManifestRef,
    table: &super::super::TableManifest,
) -> FormatResult<()> {
    if table.database_id() != snapshot.database_id()
        || table.table_id() != reference.table_id()
        || table.manifest_id() != reference.manifest().id()
        || table.generation() != reference.manifest().generation()
        || table.catalog_generation().get() > snapshot.catalog().generation().get()
    {
        return invalid("table manifest differs from its database-manifest reference");
    }
    Ok(())
}

fn require_exact_member(
    manifest: &SnapshotManifest,
    expected: SnapshotMember,
    consumed: &mut BTreeSet<(SnapshotMemberKind, [u8; 16])>,
) -> FormatResult<()> {
    let actual = member_by_identity(manifest, expected.kind(), expected.id())?;
    if actual != expected {
        return invalid("snapshot member differs from the reachable reference");
    }
    consumed.insert((expected.kind(), expected.id()));
    Ok(())
}

fn optional_exact_member(
    manifest: &SnapshotManifest,
    expected: SnapshotMember,
    consumed: &mut BTreeSet<(SnapshotMemberKind, [u8; 16])>,
) -> FormatResult<bool> {
    let Some(actual) = manifest
        .members()
        .iter()
        .find(|member| member.kind() == expected.kind() && member.id() == expected.id())
        .copied()
    else {
        return Ok(false);
    };
    if actual != expected {
        return invalid("optional snapshot member differs from the reachable reference");
    }
    consumed.insert((expected.kind(), expected.id()));
    Ok(true)
}

fn member_by_identity(
    manifest: &SnapshotManifest,
    kind: SnapshotMemberKind,
    id: [u8; 16],
) -> FormatResult<SnapshotMember> {
    manifest
        .members()
        .iter()
        .find(|member| member.kind() == kind && member.id() == id)
        .copied()
        .ok_or_else(|| invalid_error("required snapshot member is absent"))
}

fn validate_wal_members(manifest: &SnapshotManifest, floor: WalGeneration) -> FormatResult<()> {
    let mut members = manifest
        .members()
        .iter()
        .filter(|member| member.kind() == SnapshotMemberKind::Wal)
        .copied()
        .collect::<Vec<_>>();
    members.sort_unstable_by_key(|member| member.generation());
    if members.is_empty() || members[0].generation() != floor.get() {
        return invalid("snapshot WAL range does not start at the replay floor");
    }
    for member in &members {
        let expected = SnapshotMember::wal(
            manifest.database_id(),
            WalGeneration::new(member.generation())?,
            member.byte_length(),
            member.body_sha256(),
        )?;
        if *member != expected {
            return invalid("snapshot WAL identity is not canonical");
        }
    }
    if members.windows(2).any(|pair| {
        pair[0]
            .generation()
            .checked_add(1)
            .is_none_or(|expected| pair[1].generation() != expected)
    }) {
        return invalid("snapshot WAL range is not contiguous");
    }
    Ok(())
}

fn artifact_reference(member: SnapshotMember) -> FormatResult<ArtifactRef> {
    let kind = match member.kind() {
        SnapshotMemberKind::Data => ArtifactKind::Data,
        SnapshotMemberKind::Index => ArtifactKind::Index,
        _ => return invalid("non-artifact member has an artifact destination"),
    };
    ArtifactRef::new(
        ArtifactId::from_bytes(member.id())?,
        kind,
        super::super::DatabaseGeneration::new(member.generation())?,
        member.byte_length(),
        member.body_sha256(),
    )
}

fn copy_members(
    snapshot_root: &Path,
    staging: &Path,
    manifest: &SnapshotManifest,
    layout: &RestoreLayout,
) -> FormatResult<()> {
    for member in manifest.members().iter().copied() {
        let source = snapshot_root.join(member.relative_path());
        let target = staging.join(layout.target_path(member)?);
        let parent = target
            .parent()
            .ok_or_else(|| invalid_error("restore member has no target parent"))?;
        std::fs::create_dir_all(parent)
            .map_err(|error| restore_io("create restore member directory", error))?;
        copy_member(&source, &target, member)?;
    }
    Ok(())
}

fn copy_member(source: &Path, target: &Path, member: SnapshotMember) -> FormatResult<()> {
    let mut source_file = open_regular(source, false)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    set_no_follow(&mut options);
    let mut target_file = options
        .open(target)
        .map_err(|error| restore_io("create restored member", error))?;
    let mut remaining = member.byte_length();
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    while remaining != 0 {
        let length = usize::try_from(remaining.min(COPY_BUFFER_BYTES as u64))
            .expect("bounded restore copy length fits usize");
        source_file
            .read_exact(&mut buffer[..length])
            .map_err(|error| restore_io("read snapshot member for restore", error))?;
        target_file
            .write_all(&buffer[..length])
            .map_err(|error| restore_io("write restored member", error))?;
        remaining -= length as u64;
    }
    let mut trailing = [0_u8; 1];
    if source_file
        .read(&mut trailing)
        .map_err(|error| restore_io("verify snapshot member end", error))?
        != 0
    {
        return invalid("snapshot member grew during restore");
    }
    target_file
        .sync_all()
        .map_err(|error| restore_io("sync restored member", error))?;
    validate_snapshot_member_file(target, member)
}

fn write_control(staging: &Path, control: ControlRecord) -> FormatResult<()> {
    let target = staging.join("CONTROL.0");
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    set_no_follow(&mut options);
    let mut file = options
        .open(target)
        .map_err(|error| restore_io("create restored CONTROL", error))?;
    file.write_all(&encode_control_slot(control))
        .map_err(|error| restore_io("write restored CONTROL", error))?;
    file.sync_all()
        .map_err(|error| restore_io("sync restored CONTROL", error))
}

fn sync_tree_directories(
    staging: &Path,
    manifest: &SnapshotManifest,
    layout: &RestoreLayout,
) -> FormatResult<()> {
    let mut directories = BTreeSet::new();
    directories.insert(staging.to_path_buf());
    for member in manifest.members().iter().copied() {
        collect_directories(
            staging,
            &staging.join(layout.target_path(member)?),
            &mut directories,
        )?;
    }
    let mut directories = directories.into_iter().collect::<Vec<_>>();
    directories.sort_unstable_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        if directory.exists() {
            sync_directory(&directory, "sync restore staging directory")?;
        }
    }
    Ok(())
}

/// Mutable runtime roots are not snapshot members, but a restored database
/// must be immediately writable. Create their empty canonical owners before
/// the absent root is published; no logical rows or immutable artifacts are
/// reconstructed here.
fn create_runtime_directories(staging: &Path) -> FormatResult<()> {
    for name in ["artifacts", "staging", "quarantine"] {
        let path = staging.join(name);
        match std::fs::create_dir(&path) {
            Ok(()) => sync_directory(&path, "sync restored runtime directory")?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                validate_real_directory(&path, "inspect restored runtime directory")?;
            }
            Err(error) => return Err(restore_io("create restored runtime directory", error)),
        }
    }
    sync_directory(staging, "sync restored runtime roots")
}

fn collect_directories(
    root: &Path,
    member: &Path,
    directories: &mut BTreeSet<PathBuf>,
) -> FormatResult<()> {
    let mut directory = member
        .parent()
        .ok_or_else(|| invalid_error("restored member has no parent directory"))?;
    loop {
        if !directory.starts_with(root) {
            return invalid("restored member escapes staging root");
        }
        directories.insert(directory.to_path_buf());
        if directory == root {
            break;
        }
        directory = directory
            .parent()
            .ok_or_else(|| invalid_error("restored member directory escapes staging root"))?;
    }
    Ok(())
}

fn validate_rebuild_state(
    layout: &RestoreLayout,
    unavailable: &[UnavailableIndex],
) -> FormatResult<()> {
    let actual = unavailable
        .iter()
        .map(|index| (index.reference().id().into_bytes(), index.reference()))
        .collect::<BTreeMap<_, _>>();
    if actual != layout.omitted_indexes {
        return invalid("restored unavailable-index state differs from omitted snapshot members");
    }
    Ok(())
}

fn read_small_member(snapshot_root: &Path, member: SnapshotMember) -> FormatResult<Vec<u8>> {
    let length =
        usize::try_from(member.byte_length()).map_err(|_| FormatError::SnapshotLimitExceeded {
            field: "restore manifest allocation",
            actual: member.byte_length(),
            limit: super::super::MAX_MANIFEST_FILE_BYTES,
        })?;
    if member.byte_length() > super::super::MAX_MANIFEST_FILE_BYTES {
        return Err(FormatError::SnapshotLimitExceeded {
            field: "restore manifest bytes",
            actual: member.byte_length(),
            limit: super::super::MAX_MANIFEST_FILE_BYTES,
        });
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| FormatError::SnapshotLimitExceeded {
            field: "restore manifest allocation",
            actual: member.byte_length(),
            limit: super::super::MAX_MANIFEST_FILE_BYTES,
        })?;
    bytes.resize(length, 0);
    let path = snapshot_root.join(member.relative_path());
    let mut file = open_regular(&path, false)?;
    file.read_exact(&mut bytes)
        .map_err(|error| restore_io("read snapshot manifest member", error))?;
    Ok(bytes)
}

fn target_parent(target: &Path) -> FormatResult<&Path> {
    if target.file_name().is_none() {
        return invalid("restore target has no final path component");
    }
    Ok(target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new(".")))
}

fn reject_source_inside_target(snapshot: &Path, target_parent: &Path) -> FormatResult<()> {
    let snapshot = std::fs::canonicalize(snapshot)
        .map_err(|error| restore_io("resolve snapshot root", error))?;
    let parent = std::fs::canonicalize(target_parent)
        .map_err(|error| restore_io("resolve restore target parent", error))?;
    if parent.starts_with(&snapshot) {
        return invalid("restore target parent is inside the source snapshot");
    }
    Ok(())
}

fn validate_target_absent(target: &Path) -> FormatResult<()> {
    match std::fs::symlink_metadata(target) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(restore_io("inspect restore target", error)),
        Ok(_) => invalid("restore target already exists"),
    }
}

fn require_absent(path: &Path, operation: &'static str) -> FormatResult<()> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(restore_io(operation, error)),
        Ok(_) => invalid("restore staging root already exists"),
    }
}

fn validate_real_directory(path: &Path, operation: &'static str) -> FormatResult<()> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| restore_io(operation, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return invalid("restore target parent is not a real directory");
    }
    Ok(())
}

fn sync_directory(path: &Path, operation: &'static str) -> FormatResult<()> {
    let file = File::open(path).map_err(|error| restore_io(operation, error))?;
    file.sync_all()
        .map_err(|error| restore_io(operation, error))
}

fn open_regular(path: &Path, writable: bool) -> FormatResult<File> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| restore_io("inspect restore source", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return invalid("restore source is not a regular file");
    }
    let mut options = OpenOptions::new();
    options.read(true).write(writable);
    set_no_follow(&mut options);
    options
        .open(path)
        .map_err(|error| restore_io("open restore source", error))
}

#[cfg(target_os = "linux")]
fn rename_root_without_replace(source: &Path, target: &Path) -> FormatResult<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let source = CString::new(source.as_os_str().as_bytes())
        .map_err(|_| invalid_error("restore staging path contains NUL"))?;
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| invalid_error("restore target path contains NUL"))?;
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(restore_io(
            "publish restored database root",
            std::io::Error::last_os_error(),
        ))
    }
}

#[cfg(not(target_os = "linux"))]
fn rename_root_without_replace(source: &Path, target: &Path) -> FormatResult<()> {
    validate_target_absent(target)?;
    std::fs::rename(source, target)
        .map_err(|error| restore_io("publish restored database root", error))
}

#[cfg(unix)]
fn set_no_follow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(not(unix))]
fn set_no_follow(_options: &mut OpenOptions) {}

struct StagingCleanup {
    path: Option<PathBuf>,
}

impl StagingCleanup {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn disarm(&mut self) {
        self.path = None;
    }
}

impl Drop for StagingCleanup {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

const fn invalid_error(detail: &'static str) -> FormatError {
    FormatError::InvalidSnapshot { detail }
}

const fn invalid<T>(detail: &'static str) -> FormatResult<T> {
    Err(invalid_error(detail))
}

fn restore_io(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::SnapshotIo {
        operation,
        kind: error.kind(),
    }
}

fn restore_recovery_required(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::SnapshotRecoveryRequired {
        operation,
        kind: error.kind(),
    }
}
