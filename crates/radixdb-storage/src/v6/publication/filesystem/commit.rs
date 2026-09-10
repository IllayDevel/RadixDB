use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use crate::v6::{
    fault::reach_generation_boundary, ArtifactKind, ArtifactRef, CompleteStagingSet,
    ControlSlotIndex, FormatError, FormatResult, GenerationCrashPoint,
};

use super::plan::{GenerationPublicationPlan, PublicationMember, PublicationMemberRole};
use super::source::validate_new_artifact_identity;

const COMPARE_BUFFER_BYTES: usize = 64 * 1024;

pub(crate) fn publish_members(
    root: &Path,
    staging: &CompleteStagingSet,
    plan: &GenerationPublicationPlan,
) -> FormatResult<()> {
    validate_directory(root, "inspect database root")?;
    validate_directory(staging.path(), "inspect publication directory")?;
    validate_same_filesystem(root, staging.path())?;
    for member in plan.members() {
        publish_member(root, staging.path(), member)?;
    }
    publish_control(root, plan.target_control().slot(), plan.control_bytes())
}

/// Promote one complete set of immutable compaction outputs without changing
/// CONTROL. The artifacts remain unreachable until a following rebased
/// manifest publication commits them; a crash in between is therefore safe
/// and generation-aware GC may reclaim the orphaned files.
///
/// The caller must hold the physical publication fence across this operation
/// and the manifest/CONTROL publication that makes the artifacts reachable.
pub(crate) fn publish_prebuilt_artifacts(
    root: &Path,
    staging: &CompleteStagingSet,
    artifacts: &[ArtifactRef],
) -> FormatResult<()> {
    validate_directory(root, "inspect database root")?;
    validate_directory(staging.path(), "inspect publication directory")?;
    validate_same_filesystem(root, staging.path())?;

    let members = prebuilt_artifact_members(artifacts)?;
    for member in &members {
        publish_member(root, staging.path(), member)?;
    }
    Ok(())
}

/// Validate the exact completed prebuilt member set before entering the short
/// physical publication fence. This is the only phase that hashes complete
/// DATA/INDEX payloads; the fenced promotion and manifest rebase do not reread
/// their bodies.
pub(crate) fn validate_prebuilt_artifacts(
    root: &Path,
    staging: &CompleteStagingSet,
    artifacts: &[ArtifactRef],
) -> FormatResult<()> {
    validate_directory(root, "inspect database root")?;
    validate_directory(staging.path(), "inspect publication directory")?;
    validate_same_filesystem(root, staging.path())?;
    let members = prebuilt_artifact_members(artifacts)?;
    let member_count =
        u64::try_from(members.len()).map_err(|_| FormatError::InvalidMaintenance {
            detail: "prebuilt compaction artifact count exceeds u64",
        })?;
    staging.validate_records(member_count)?;
    for (member, reference) in members.iter().zip(artifacts_for_members(artifacts)) {
        validate_new_artifact_identity(&staging.path().join(member.relative_path()), reference)?;
    }
    Ok(())
}

fn prebuilt_artifact_members(artifacts: &[ArtifactRef]) -> FormatResult<Vec<PublicationMember>> {
    let members = artifacts_for_members(artifacts)
        .map(|reference| {
            let role = match reference.kind() {
                ArtifactKind::Data => PublicationMemberRole::Data,
                ArtifactKind::Index => PublicationMemberRole::Index,
            };
            PublicationMember::new(role, reference.relative_path())
        })
        .collect::<Vec<_>>();
    if members
        .windows(2)
        .any(|pair| pair[0].relative_path() == pair[1].relative_path())
    {
        return Err(FormatError::InvalidMaintenance {
            detail: "prebuilt compaction artifacts repeat a final locator",
        });
    }
    Ok(members)
}

fn artifacts_for_members(artifacts: &[ArtifactRef]) -> impl Iterator<Item = ArtifactRef> + '_ {
    let mut artifacts = artifacts.to_vec();
    artifacts.sort_unstable_by_key(|reference| reference.relative_path());
    artifacts.into_iter()
}

fn publish_member(root: &Path, staging: &Path, member: &PublicationMember) -> FormatResult<()> {
    let relative = member.relative_path();
    let staged = staging.join(relative);
    let final_path = root.join(relative);
    let parent = final_path.parent().ok_or(FormatError::InvalidPublication {
        detail: "publication member has no final parent",
    })?;
    create_durable_directories(root, parent)?;

    let staged_exists = path_exists(&staged, "inspect staged publication member")?;
    let final_exists = path_exists(&final_path, "inspect final publication member")?;
    match (staged_exists, final_exists) {
        (false, true) => validate_regular(&final_path)?,
        (false, false) => {
            return Err(FormatError::InvalidPublication {
                detail: "publication member is absent from staging and final location",
            });
        }
        (true, true) => {
            validate_regular(&staged)?;
            validate_regular(&final_path)?;
            let compatible = match member.role() {
                // The successor is created before publication so new commits
                // can continue while checkpoint metadata is staged. Its
                // staged snapshot must therefore be an exact prefix of the
                // live append-only WAL, not an exact-size immutable member.
                PublicationMemberRole::WalSuccessor => is_prefix(&staged, &final_path)?,
                _ => same_contents(&staged, &final_path)?,
            };
            if !compatible {
                return Err(FormatError::InvalidPublication {
                    detail: match member.role() {
                        PublicationMemberRole::WalSuccessor => {
                            "live WAL successor does not extend staged prefix"
                        }
                        _ => "immutable final locator contains different bytes",
                    },
                });
            }
        }
        (true, false) => {
            validate_regular(&staged)?;
            rename_without_replace(&staged, &final_path)?;
            if let Some(point) = after_rename_point(member.role()) {
                reach_generation_boundary(point).map_err(|error| {
                    publication_io("inject after immutable member rename", error)
                })?;
            }
        }
    }

    // Existing bytes/names may be from an interrupted attempt, not durable state.
    if final_exists {
        open_read(&final_path)?
            .sync_all()
            .map_err(|error| publication_io("sync existing publication member", error))?;
    }
    sync_directory(parent, "sync final publication directory")?;
    if let Some(point) = directory_durable_point(member.role()) {
        reach_generation_boundary(point)
            .map_err(|error| publication_io("inject after immutable member durability", error))?;
    }
    if let Some(mut staged_parent) = staged.parent() {
        // A resumed staging set may have had empty member directories removed.
        // Sync the nearest surviving parent to finish that namespace cleanup.
        while !path_exists(staged_parent, "inspect staged publication directory")? {
            staged_parent = staged_parent
                .parent()
                .filter(|parent| parent.starts_with(staging))
                .ok_or(FormatError::InvalidPublication {
                    detail: "staged publication directory disappeared",
                })?;
        }
        validate_directory(staged_parent, "inspect staged publication directory")?;
        sync_directory(staged_parent, "sync staged publication directory")?;
    }
    validate_regular(&final_path)
}

const fn after_rename_point(role: PublicationMemberRole) -> Option<GenerationCrashPoint> {
    match role {
        PublicationMemberRole::Data => {
            Some(GenerationCrashPoint::DataAfterFinalRenameBeforeDirSync)
        }
        PublicationMemberRole::Index => {
            Some(GenerationCrashPoint::IndexAfterFinalRenameBeforeDirSync)
        }
        PublicationMemberRole::TableManifest => {
            Some(GenerationCrashPoint::TableManifestAfterRenameBeforeDirSync)
        }
        PublicationMemberRole::CatalogPack => {
            Some(GenerationCrashPoint::CatalogPackAfterRenameBeforeDirSync)
        }
        PublicationMemberRole::DatabaseManifest => {
            Some(GenerationCrashPoint::DatabaseManifestAfterRenameBeforeDirSync)
        }
        PublicationMemberRole::WalSuccessor => None,
    }
}

const fn directory_durable_point(role: PublicationMemberRole) -> Option<GenerationCrashPoint> {
    match role {
        PublicationMemberRole::Data => Some(GenerationCrashPoint::DataFinalDirDurable),
        PublicationMemberRole::Index => Some(GenerationCrashPoint::IndexFinalDirDurable),
        PublicationMemberRole::TableManifest => Some(GenerationCrashPoint::TableManifestDirDurable),
        PublicationMemberRole::CatalogPack => Some(GenerationCrashPoint::CatalogPackDirDurable),
        PublicationMemberRole::DatabaseManifest => {
            Some(GenerationCrashPoint::DatabaseManifestDirDurable)
        }
        PublicationMemberRole::WalSuccessor => None,
    }
}

fn publish_control(root: &Path, slot: ControlSlotIndex, bytes: &[u8]) -> FormatResult<()> {
    let path = root.join(match slot {
        ControlSlotIndex::Zero => "CONTROL.0",
        ControlSlotIndex::One => "CONTROL.1",
    });
    if let Ok(metadata) = std::fs::symlink_metadata(&path) {
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(FormatError::InvalidPublication {
                detail: "CONTROL slot is not a regular file",
            });
        }
    }

    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    set_no_follow(&mut options);
    let mut file = options
        .open(&path)
        .map_err(|error| publication_io("open inactive CONTROL", error))?;
    let split = bytes.len() / 2;
    file.write_all(&bytes[..split])
        .map_err(|error| recovery_required("write inactive CONTROL prefix", error))?;
    reach_generation_boundary(GenerationCrashPoint::ControlAfterPartialWrite)
        .map_err(|error| publication_io("inject after partial CONTROL write", error))?;
    file.write_all(&bytes[split..])
        .map_err(|error| recovery_required("write inactive CONTROL suffix", error))?;
    reach_generation_boundary(GenerationCrashPoint::ControlAfterWriteBeforeFdatasync)
        .map_err(|error| recovery_required("inject before inactive CONTROL sync", error))?;
    file.sync_all()
        .map_err(|error| recovery_required("sync inactive CONTROL", error))?;
    reach_generation_boundary(GenerationCrashPoint::ControlDurable)
        .map_err(|error| recovery_required("inject after inactive CONTROL sync", error))?;
    sync_directory(root, "sync database root after CONTROL").map_err(|error| match error {
        FormatError::PublicationIo { kind, .. } => FormatError::PublicationRecoveryRequired {
            operation: "sync database root after CONTROL",
            kind,
        },
        other => other,
    })?;
    reach_generation_boundary(GenerationCrashPoint::ControlAfterRootDirSync)
        .map_err(|error| recovery_required("inject after database-root sync", error))
}

fn create_durable_directories(root: &Path, target: &Path) -> FormatResult<()> {
    let relative = target
        .strip_prefix(root)
        .map_err(|_| FormatError::InvalidPublication {
            detail: "final publication path escapes database root",
        })?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let previous = current.clone();
        current.push(component);
        match std::fs::create_dir(&current) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                validate_directory(&current, "inspect publication directory")?;
            }
            Err(error) => return Err(publication_io("create publication directory", error)),
        }
        // AlreadyExists may follow mkdir success and parent fsync failure.
        sync_directory(&previous, "sync publication directory parent")?;
    }
    Ok(())
}

fn path_exists(path: &Path, operation: &'static str) -> FormatResult<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(publication_io(operation, error)),
    }
}

fn validate_regular(path: &Path) -> FormatResult<()> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| publication_io("inspect publication member", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(FormatError::InvalidPublication {
            detail: "publication member is not a regular file",
        });
    }
    Ok(())
}

fn validate_directory(path: &Path, operation: &'static str) -> FormatResult<()> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|error| publication_io(operation, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(FormatError::InvalidPublication {
            detail: "required publication path is not a real directory",
        });
    }
    Ok(())
}

fn same_contents(left: &Path, right: &Path) -> FormatResult<bool> {
    let left_metadata = std::fs::symlink_metadata(left)
        .map_err(|error| publication_io("inspect staged collision", error))?;
    let right_metadata = std::fs::symlink_metadata(right)
        .map_err(|error| publication_io("inspect final collision", error))?;
    if left_metadata.len() != right_metadata.len() {
        return Ok(false);
    }
    let mut left = open_read(left)?;
    let mut right = open_read(right)?;
    let mut left_buffer = [0_u8; COMPARE_BUFFER_BYTES];
    let mut right_buffer = [0_u8; COMPARE_BUFFER_BYTES];
    loop {
        let left_count = left
            .read(&mut left_buffer)
            .map_err(|error| publication_io("read staged collision", error))?;
        let right_count = right
            .read(&mut right_buffer)
            .map_err(|error| publication_io("read final collision", error))?;
        if left_count != right_count || left_buffer[..left_count] != right_buffer[..right_count] {
            return Ok(false);
        }
        if left_count == 0 {
            return Ok(true);
        }
    }
}

fn is_prefix(prefix: &Path, complete: &Path) -> FormatResult<bool> {
    let prefix_metadata = std::fs::symlink_metadata(prefix)
        .map_err(|error| publication_io("inspect staged WAL successor", error))?;
    let complete_metadata = std::fs::symlink_metadata(complete)
        .map_err(|error| publication_io("inspect live WAL successor", error))?;
    if prefix_metadata.len() > complete_metadata.len() {
        return Ok(false);
    }
    let mut prefix = open_read(prefix)?;
    let mut complete = open_read(complete)?;
    let mut prefix_buffer = [0_u8; COMPARE_BUFFER_BYTES];
    let mut complete_buffer = [0_u8; COMPARE_BUFFER_BYTES];
    loop {
        let prefix_count = prefix
            .read(&mut prefix_buffer)
            .map_err(|error| publication_io("read staged WAL successor", error))?;
        if prefix_count == 0 {
            return Ok(true);
        }
        complete
            .read_exact(&mut complete_buffer[..prefix_count])
            .map_err(|error| publication_io("read live WAL successor", error))?;
        if prefix_buffer[..prefix_count] != complete_buffer[..prefix_count] {
            return Ok(false);
        }
    }
}

fn open_read(path: &Path) -> FormatResult<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    set_no_follow(&mut options);
    options
        .open(path)
        .map_err(|error| publication_io("open publication member", error))
}

#[cfg(target_os = "linux")]
fn rename_without_replace(source: &Path, target: &Path) -> FormatResult<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let source_path = source;
    let target_path = target;
    let source = CString::new(source.as_os_str().as_bytes()).map_err(|_| {
        FormatError::InvalidPublication {
            detail: "staged publication path contains NUL",
        }
    })?;
    let target = CString::new(target.as_os_str().as_bytes()).map_err(|_| {
        FormatError::InvalidPublication {
            detail: "final publication path contains NUL",
        }
    })?;
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
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if matches!(error.raw_os_error(), Some(code) if code == libc::ENOSYS || code == libc::EINVAL) {
        return link_without_replace(source_path, target_path);
    }
    Err(publication_io("rename publication member", error))
}

#[cfg(not(target_os = "linux"))]
fn rename_without_replace(source: &Path, target: &Path) -> FormatResult<()> {
    link_without_replace(source, target)
}

fn link_without_replace(source: &Path, target: &Path) -> FormatResult<()> {
    std::fs::hard_link(source, target)
        .map_err(|error| publication_io("link publication member", error))?;
    std::fs::remove_file(source)
        .map_err(|error| publication_io("retire staged publication member", error))
}

#[cfg(unix)]
fn validate_same_filesystem(left: &Path, right: &Path) -> FormatResult<()> {
    use std::os::unix::fs::MetadataExt;

    let left_device = std::fs::metadata(left)
        .map_err(|error| publication_io("inspect database filesystem", error))?
        .dev();
    let right_device = std::fs::metadata(right)
        .map_err(|error| publication_io("inspect staging filesystem", error))?
        .dev();
    if left_device != right_device {
        return Err(FormatError::InvalidPublication {
            detail: "staging and final root are on different filesystems",
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_same_filesystem(_left: &Path, _right: &Path) -> FormatResult<()> {
    Ok(())
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
        .map_err(|error| publication_io(operation, error))
}

fn publication_io(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::PublicationIo {
        operation,
        kind: error.kind(),
    }
}

fn recovery_required(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::PublicationRecoveryRequired {
        operation,
        kind: error.kind(),
    }
}
