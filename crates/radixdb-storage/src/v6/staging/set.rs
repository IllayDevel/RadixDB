use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use super::limits::{StagingDiscoveryLimits, MAX_STAGED_PATH_BYTES};
use super::members::{digest_members, digest_selected_members, validate_relative_path};
use super::record::{
    decode_staging_complete, encode_staging_complete, encode_staging_owner, StagingComplete,
    StagingOwner,
};
use super::{COMPLETE_FILE, COMPLETE_PENDING_FILE, OWNER_FILE};
use crate::v6::{
    fault::reach_generation_boundary, DatabaseGeneration, FormatError, FormatResult,
    GenerationCrashPoint, PublicationId, WriterInstanceId,
};

const MAX_IDENTITY_ATTEMPTS: usize = 1_024;

/// Durable role of a staged generation member. Publication and fault tests use
/// this typed value; a pathname is never interpreted as membership authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagedMemberRole {
    Data,
    Index,
    TableManifest,
    CatalogPack,
    WalSuccessor,
    DatabaseManifest,
}

#[derive(Debug)]
pub struct StagedArtifactSet {
    staging_root: PathBuf,
    publication_directory: PathBuf,
    owner: StagingOwner,
}

impl StagedArtifactSet {
    pub fn create(
        staging_root: impl AsRef<Path>,
        writer_instance_id: WriterInstanceId,
        intended_generation: DatabaseGeneration,
        process_id: u64,
        created_unix_ns: u64,
    ) -> FormatResult<Self> {
        let staging_root = staging_root.as_ref();
        for _ in 0..MAX_IDENTITY_ATTEMPTS {
            let publication_id = PublicationId::new();
            match Self::create_with_publication_id(
                staging_root,
                writer_instance_id,
                publication_id,
                intended_generation,
                process_id,
                created_unix_ns,
            ) {
                Err(FormatError::StagingIo {
                    kind: std::io::ErrorKind::AlreadyExists,
                    ..
                }) => continue,
                result => return result,
            }
        }
        Err(FormatError::StagingLimitExceeded {
            field: "unique identity attempts",
            actual: MAX_IDENTITY_ATTEMPTS as u64,
            limit: MAX_IDENTITY_ATTEMPTS as u64,
        })
    }

    pub fn create_with_publication_id(
        staging_root: impl AsRef<Path>,
        writer_instance_id: WriterInstanceId,
        publication_id: PublicationId,
        intended_generation: DatabaseGeneration,
        process_id: u64,
        created_unix_ns: u64,
    ) -> FormatResult<Self> {
        let staging_root = staging_root.as_ref();
        let owner = StagingOwner::new(
            writer_instance_id,
            publication_id,
            process_id,
            created_unix_ns,
            created_unix_ns,
            intended_generation,
        )?;
        validate_directory(staging_root, "inspect staging root")?;
        let writer_directory = staging_root.join(writer_instance_id.to_string());
        let writer_created = create_or_validate_directory(&writer_directory)?;
        if writer_created {
            sync_directory(staging_root, "sync staging root")?;
        }

        let publication_directory = writer_directory.join(publication_id.to_string());
        std::fs::create_dir(&publication_directory)
            .map_err(|error| io_error("create publication directory", error))?;
        if let Err(error) = sync_directory(&writer_directory, "sync writer directory") {
            let _ = std::fs::remove_dir(&publication_directory);
            return Err(error);
        }

        let owner_path = publication_directory.join(OWNER_FILE);
        reach_generation_boundary(GenerationCrashPoint::StageOwnerBeforeWrite)
            .map_err(|error| io_error("inject before staging OWNER write", error))?;
        let owner_result = write_exclusive_file(
            &owner_path,
            &encode_staging_owner(owner),
            "create staging OWNER",
            "write staging OWNER",
            "sync staging OWNER",
        )
        .and_then(|()| sync_directory(&publication_directory, "sync publication directory"))
        .and_then(|()| {
            reach_generation_boundary(GenerationCrashPoint::StageOwnerAfterSync)
                .map_err(|error| io_error("inject after staging OWNER sync", error))
        });
        if let Err(error) = owner_result {
            let _ = std::fs::remove_file(&owner_path);
            let _ = std::fs::remove_dir(&publication_directory);
            let _ = sync_directory(&writer_directory, "sync writer directory after cleanup");
            return Err(error);
        }
        Ok(Self {
            staging_root: staging_root.to_path_buf(),
            publication_directory,
            owner,
        })
    }

    pub fn staging_root(&self) -> &Path {
        &self.staging_root
    }

    pub fn path(&self) -> &Path {
        &self.publication_directory
    }

    pub const fn owner(&self) -> StagingOwner {
        self.owner
    }

    pub fn write_file<T>(
        &self,
        relative_path: &str,
        writer: impl FnOnce(&mut File) -> FormatResult<T>,
    ) -> FormatResult<T> {
        self.write_file_with_role(relative_path, None, writer)
    }

    /// Write one generation member with an explicit durable role. This is the
    /// only staging API that emits role-specific lifecycle boundaries.
    pub fn write_generation_file<T>(
        &self,
        role: StagedMemberRole,
        relative_path: &str,
        writer: impl FnOnce(&mut File) -> FormatResult<T>,
    ) -> FormatResult<T> {
        self.write_file_with_role(relative_path, Some(role), writer)
    }

    fn write_file_with_role<T>(
        &self,
        relative_path: &str,
        role: Option<StagedMemberRole>,
        writer: impl FnOnce(&mut File) -> FormatResult<T>,
    ) -> FormatResult<T> {
        validate_relative_path(relative_path, MAX_STAGED_PATH_BYTES)?;
        ensure_incomplete(&self.publication_directory)?;
        let path = self.publication_directory.join(relative_path);
        let parent = path.parent().ok_or(FormatError::InvalidStagingRecord {
            record: "publication",
            detail: "staged member has no parent directory",
        })?;
        create_relative_directories(&self.publication_directory, parent)?;

        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        set_no_follow(&mut options);
        let mut file = options
            .open(&path)
            .map_err(|error| io_error("create staged member", error))?;
        let result = writer(&mut file).and_then(|value| {
            if let Some(point) = role.and_then(StagedMemberRole::before_file_sync) {
                reach_generation_boundary(point)
                    .map_err(|error| io_error("inject before staged member sync", error))?;
            }
            file.sync_all()
                .map_err(|error| io_error("sync staged member", error))?;
            if let Some(point) = role.and_then(StagedMemberRole::after_file_sync) {
                reach_generation_boundary(point)
                    .map_err(|error| io_error("inject after staged member sync", error))?;
            }
            sync_directory(parent, "sync staged member directory")?;
            if role == Some(StagedMemberRole::WalSuccessor) {
                reach_generation_boundary(GenerationCrashPoint::WalSuccessorDurable)
                    .map_err(|error| io_error("inject after successor WAL durability", error))?;
            }
            Ok(value)
        });
        if result.is_err() {
            drop(file);
            let _ = std::fs::remove_file(&path);
            let _ = sync_directory(parent, "sync staged member cleanup");
        }
        result
    }

    pub fn mark_complete(
        &self,
        completed_unix_ns: u64,
        limits: StagingDiscoveryLimits,
    ) -> FormatResult<CompleteStagingSet> {
        let complete_path = self.publication_directory.join(COMPLETE_FILE);
        if complete_path
            .try_exists()
            .map_err(|error| io_error("inspect existing staging COMPLETE", error))?
        {
            let complete = read_existing_complete(&complete_path, self.owner)?;
            validate_complete_members(&self.publication_directory, complete, limits)?;
            return Ok(CompleteStagingSet {
                publication_directory: self.publication_directory.clone(),
                owner: self.owner,
                complete,
            });
        }

        let members = digest_members(&self.publication_directory, limits)?;
        let complete =
            StagingComplete::new(self.owner, members.count, members.sha256, completed_unix_ns)?;
        let pending_path = self.publication_directory.join(COMPLETE_PENDING_FILE);
        write_exclusive_file(
            &pending_path,
            &encode_staging_complete(complete),
            "create staging COMPLETE candidate",
            "write staging COMPLETE candidate",
            "sync staging COMPLETE candidate",
        )?;
        if let Err(error) = std::fs::rename(&pending_path, &complete_path)
            .map_err(|error| io_error("publish staging COMPLETE", error))
            .and_then(|()| {
                sync_directory(
                    &self.publication_directory,
                    "sync staging COMPLETE directory",
                )
            })
        {
            let _ = std::fs::remove_file(&pending_path);
            let _ = sync_directory(&self.publication_directory, "sync staging COMPLETE cleanup");
            return Err(error);
        }
        Ok(CompleteStagingSet {
            publication_directory: self.publication_directory.clone(),
            owner: self.owner,
            complete,
        })
    }
}

impl StagedMemberRole {
    const fn before_file_sync(self) -> Option<GenerationCrashPoint> {
        match self {
            Self::WalSuccessor => Some(GenerationCrashPoint::WalSuccessorAfterCreateBeforeSync),
            Self::Data
            | Self::Index
            | Self::TableManifest
            | Self::CatalogPack
            | Self::DatabaseManifest => None,
        }
    }

    const fn after_file_sync(self) -> Option<GenerationCrashPoint> {
        match self {
            Self::Data => Some(GenerationCrashPoint::DataAfterFileSync),
            Self::Index => Some(GenerationCrashPoint::IndexAfterFileSync),
            Self::TableManifest => Some(GenerationCrashPoint::TableManifestAfterFileSync),
            Self::CatalogPack => Some(GenerationCrashPoint::CatalogPackAfterFileSync),
            Self::DatabaseManifest => Some(GenerationCrashPoint::DatabaseManifestAfterFileSync),
            Self::WalSuccessor => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteStagingSet {
    publication_directory: PathBuf,
    owner: StagingOwner,
    complete: StagingComplete,
}

impl CompleteStagingSet {
    pub(crate) fn reopen_for_publication(
        publication_directory: impl AsRef<Path>,
        final_root: impl AsRef<Path>,
        relative_paths: &[PathBuf],
        limits: StagingDiscoveryLimits,
    ) -> FormatResult<Self> {
        let publication_directory = publication_directory.as_ref();
        validate_directory(publication_directory, "inspect publication directory")?;
        let owner_path = publication_directory.join(OWNER_FILE);
        let owner_bytes = read_exact_record::<{ super::record::STAGING_OWNER_BYTES }>(
            &owner_path,
            "OWNER",
            "read staging OWNER",
        )?;
        let owner = super::record::decode_staging_owner(&owner_bytes)?;
        let complete = read_existing_complete(&publication_directory.join(COMPLETE_FILE), owner)?;
        let reopened = Self {
            publication_directory: publication_directory.to_path_buf(),
            owner,
            complete,
        };
        reopened.validate_selected_members(final_root.as_ref(), relative_paths, limits)?;
        Ok(reopened)
    }

    pub fn path(&self) -> &Path {
        &self.publication_directory
    }

    pub const fn owner(&self) -> StagingOwner {
        self.owner
    }

    pub const fn complete(&self) -> StagingComplete {
        self.complete
    }

    /// Revalidate the immutable OWNER/COMPLETE envelope without reading member
    /// bodies. Callers that possess stronger per-member content identities can
    /// then verify each body exactly once instead of hashing large artifacts a
    /// second time only to reconstruct the aggregate COMPLETE digest.
    pub(crate) fn validate_records(&self, expected_member_count: u64) -> FormatResult<()> {
        validate_directory(&self.publication_directory, "inspect publication directory")?;
        let owner = read_exact_record::<{ super::record::STAGING_OWNER_BYTES }>(
            &self.publication_directory.join(OWNER_FILE),
            "OWNER",
            "read staging OWNER",
        )
        .and_then(|bytes| super::record::decode_staging_owner(&bytes))?;
        if owner != self.owner {
            return Err(FormatError::InvalidStagingRecord {
                record: "OWNER",
                detail: "record changed after staging set was completed",
            });
        }
        let complete =
            read_existing_complete(&self.publication_directory.join(COMPLETE_FILE), owner)?;
        if complete != self.complete {
            return Err(FormatError::InvalidStagingRecord {
                record: "COMPLETE",
                detail: "record changed after staging set was completed",
            });
        }
        if complete.member_count() != expected_member_count {
            return Err(FormatError::InvalidStagingRecord {
                record: "COMPLETE",
                detail: "member count differs from exact artifact set",
            });
        }
        Ok(())
    }

    pub(crate) fn validate_selected_members(
        &self,
        final_root: &Path,
        relative_paths: &[PathBuf],
        limits: StagingDiscoveryLimits,
    ) -> FormatResult<()> {
        let owner_path = self.publication_directory.join(OWNER_FILE);
        let owner_bytes = read_exact_record::<{ super::record::STAGING_OWNER_BYTES }>(
            &owner_path,
            "OWNER",
            "read staging OWNER",
        )?;
        let owner = super::record::decode_staging_owner(&owner_bytes)?;
        if owner != self.owner {
            return Err(FormatError::InvalidStagingRecord {
                record: "OWNER",
                detail: "record changed after staging set was opened",
            });
        }
        let complete_path = self.publication_directory.join(COMPLETE_FILE);
        let complete = read_existing_complete(&complete_path, owner)?;
        if complete != self.complete {
            return Err(FormatError::InvalidStagingRecord {
                record: "COMPLETE",
                detail: "record changed after staging set was opened",
            });
        }
        let members = digest_selected_members(
            &self.publication_directory,
            final_root,
            relative_paths,
            limits,
        )?;
        if members.count != complete.member_count() || members.sha256 != complete.members_sha256() {
            return Err(FormatError::InvalidStagingRecord {
                record: "COMPLETE",
                detail: "selected staged/final member set differs from marker",
            });
        }
        Ok(())
    }

    /// Retire only this exact completed publication after its CONTROL record
    /// and runtime generation are durable. Cleanup is deliberately best effort:
    /// failure cannot turn an already committed publication into an error, and
    /// the conservative recovery sweeper may handle the residue later.
    pub(crate) fn retire_published_best_effort(&self) {
        if validate_directory(&self.publication_directory, "inspect completed publication").is_err()
        {
            return;
        }
        let owner = read_exact_record::<{ super::record::STAGING_OWNER_BYTES }>(
            &self.publication_directory.join(OWNER_FILE),
            "OWNER",
            "read completed staging OWNER",
        )
        .and_then(|bytes| super::record::decode_staging_owner(&bytes));
        let complete =
            read_existing_complete(&self.publication_directory.join(COMPLETE_FILE), self.owner);
        if !matches!(owner, Ok(owner) if owner == self.owner)
            || !matches!(complete, Ok(complete) if complete == self.complete)
        {
            return;
        }

        let Some(writer_directory) = self.publication_directory.parent() else {
            return;
        };
        if writer_directory.parent() != Some(self.staging_root()) {
            return;
        }
        let expected_writer = self.owner.writer_instance_id().to_string();
        let expected_publication = self.owner.publication_id().to_string();
        if writer_directory.file_name().and_then(|name| name.to_str())
            != Some(expected_writer.as_str())
            || self
                .publication_directory
                .file_name()
                .and_then(|name| name.to_str())
                != Some(expected_publication.as_str())
        {
            return;
        }
        if std::fs::remove_dir_all(&self.publication_directory).is_err() {
            return;
        }
        let _ = sync_directory(
            writer_directory,
            "sync writer directory after completed publication cleanup",
        );
        if std::fs::read_dir(writer_directory)
            .ok()
            .is_some_and(|mut entries| entries.next().is_none())
            && std::fs::remove_dir(writer_directory).is_ok()
        {
            let _ = sync_directory(
                self.staging_root(),
                "sync staging root after empty writer cleanup",
            );
        }
    }

    fn staging_root(&self) -> &Path {
        self.publication_directory
            .parent()
            .and_then(Path::parent)
            .expect("validated staging publication has writer and staging parents")
    }
}

fn read_exact_record<const N: usize>(
    path: &Path,
    record: &'static str,
    operation: &'static str,
) -> FormatResult<[u8; N]> {
    use std::io::Read;

    let metadata = std::fs::symlink_metadata(path).map_err(|error| io_error(operation, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() != N as u64 {
        return Err(FormatError::InvalidStagingRecord {
            record,
            detail: "record is not one exact regular file",
        });
    }
    let mut options = OpenOptions::new();
    options.read(true);
    set_no_follow(&mut options);
    let mut file = options
        .open(path)
        .map_err(|error| io_error(operation, error))?;
    let mut bytes = [0_u8; N];
    file.read_exact(&mut bytes)
        .map_err(|error| io_error(operation, error))?;
    Ok(bytes)
}

fn read_existing_complete(path: &Path, owner: StagingOwner) -> FormatResult<StagingComplete> {
    use std::io::Read;

    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| io_error("inspect staging COMPLETE", error))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() != super::record::STAGING_COMPLETE_BYTES as u64
    {
        return Err(FormatError::InvalidStagingRecord {
            record: "COMPLETE",
            detail: "marker is not one exact regular record",
        });
    }
    let mut options = OpenOptions::new();
    options.read(true);
    set_no_follow(&mut options);
    let mut file = options
        .open(path)
        .map_err(|error| io_error("open staging COMPLETE", error))?;
    let mut bytes = [0_u8; super::record::STAGING_COMPLETE_BYTES];
    file.read_exact(&mut bytes)
        .map_err(|error| io_error("read staging COMPLETE", error))?;
    let complete = decode_staging_complete(&bytes)?;
    if complete.writer_instance_id() != owner.writer_instance_id()
        || complete.publication_id() != owner.publication_id()
        || complete.intended_generation() != owner.intended_generation()
        || complete.completed_unix_ns() < owner.created_unix_ns()
    {
        return Err(FormatError::InvalidStagingRecord {
            record: "COMPLETE",
            detail: "identity or generation differs from OWNER",
        });
    }
    Ok(complete)
}

pub(crate) fn validate_complete_members(
    publication_directory: &Path,
    complete: StagingComplete,
    limits: StagingDiscoveryLimits,
) -> FormatResult<()> {
    let members = digest_members(publication_directory, limits)?;
    if members.count != complete.member_count() || members.sha256 != complete.members_sha256() {
        return Err(FormatError::InvalidStagingRecord {
            record: "COMPLETE",
            detail: "member set differs from marker",
        });
    }
    Ok(())
}

fn ensure_incomplete(publication_directory: &Path) -> FormatResult<()> {
    for name in [COMPLETE_FILE, COMPLETE_PENDING_FILE] {
        if publication_directory
            .join(name)
            .try_exists()
            .map_err(|error| io_error("inspect staging completion state", error))?
        {
            return Err(FormatError::InvalidStagingRecord {
                record: "publication",
                detail: "cannot add a member after completion started",
            });
        }
    }
    Ok(())
}

fn create_relative_directories(root: &Path, parent: &Path) -> FormatResult<()> {
    let relative = parent
        .strip_prefix(root)
        .map_err(|_| FormatError::InvalidStagingRecord {
            record: "publication",
            detail: "staged member parent escaped publication directory",
        })?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let previous = current.clone();
        current.push(component);
        match std::fs::create_dir(&current) {
            Ok(()) => sync_directory(&previous, "sync staged parent directory")?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                validate_directory(&current, "inspect staged parent directory")?;
            }
            Err(error) => return Err(io_error("create staged parent directory", error)),
        }
    }
    Ok(())
}

fn create_or_validate_directory(path: &Path) -> FormatResult<bool> {
    match std::fs::create_dir(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            validate_directory(path, "inspect writer directory")?;
            Ok(false)
        }
        Err(error) => Err(io_error("create writer directory", error)),
    }
}

fn validate_directory(path: &Path, operation: &'static str) -> FormatResult<()> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| io_error(operation, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(FormatError::InvalidStagingRecord {
            record: "publication",
            detail: "required staging path is not a real directory",
        });
    }
    Ok(())
}

fn write_exclusive_file(
    path: &Path,
    bytes: &[u8],
    create_operation: &'static str,
    write_operation: &'static str,
    sync_operation: &'static str,
) -> FormatResult<()> {
    use std::io::Write;

    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    set_no_follow(&mut options);
    let mut file = options
        .open(path)
        .map_err(|error| io_error(create_operation, error))?;
    let result = file
        .write_all(bytes)
        .map_err(|error| io_error(write_operation, error))
        .and_then(|()| {
            file.sync_all()
                .map_err(|error| io_error(sync_operation, error))
        });
    if result.is_err() {
        drop(file);
        let _ = std::fs::remove_file(path);
        if let Some(parent) = path.parent() {
            let _ = sync_directory(parent, "sync failed staging record cleanup");
        }
    }
    result
}

#[cfg(unix)]
fn set_no_follow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(not(unix))]
fn set_no_follow(_options: &mut OpenOptions) {}

fn sync_directory(path: &Path, operation: &'static str) -> FormatResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error(operation, error))
}

fn io_error(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::StagingIo {
        operation,
        kind: error.kind(),
    }
}
