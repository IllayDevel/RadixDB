use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use sha2::{Digest, Sha256};

use super::super::{
    fault::reach_generation_boundary, FormatError, FormatResult, GenerationCrashPoint,
};
use super::{
    decode_snapshot_manifest, encode_snapshot_manifest, SnapshotManifest, SnapshotMember,
    SnapshotMemberKind, MAX_SNAPSHOT_MANIFEST_BYTES,
};

pub const SNAPSHOT_MANIFEST_FILE: &str = "SNAPSHOT.mft";
const SNAPSHOT_MANIFEST_PENDING_FILE: &str = "SNAPSHOT.mft.tmp";
const COMMON_FOOTER_BYTES: u64 = 48;
const COMMON_FOOTER_MAGIC: [u8; 8] = *b"RDX6END\0";
const HASH_BUFFER_BYTES: usize = 64 * 1024;

/// Commit an already copied physical member set by publishing its manifest
/// last. The manifest is the only membership authority for this snapshot.
pub fn commit_snapshot_manifest(
    snapshot_directory: impl AsRef<Path>,
    manifest: &SnapshotManifest,
) -> FormatResult<()> {
    let snapshot_directory = snapshot_directory.as_ref();
    validate_snapshot_directory_identity(snapshot_directory, manifest)?;
    let final_path = snapshot_directory.join(SNAPSHOT_MANIFEST_FILE);
    let pending_path = snapshot_directory.join(SNAPSHOT_MANIFEST_PENDING_FILE);
    require_absent(&final_path, "inspect final snapshot manifest")?;
    require_absent(&pending_path, "inspect pending snapshot manifest")?;

    validate_and_sync_members(snapshot_directory, manifest)?;
    let bytes = encode_snapshot_manifest(manifest)?;
    let prepublication = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        set_no_follow(&mut options);
        let mut file = options
            .open(&pending_path)
            .map_err(|error| snapshot_io("create pending manifest", error))?;
        file.write_all(&bytes)
            .map_err(|error| snapshot_io("write pending manifest", error))?;
        file.sync_all()
            .map_err(|error| snapshot_io("sync pending manifest", error))?;
        reach_generation_boundary(GenerationCrashPoint::SnapshotManifestAfterSyncBeforeRename)
            .map_err(|error| snapshot_io("inject after pending manifest sync", error))?;
        rename_without_replace(&pending_path, &final_path)?;
        Ok(())
    })();
    if let Err(error) = prepublication {
        let _ = std::fs::remove_file(&pending_path);
        let _ = sync_directory(snapshot_directory, "sync pending-manifest cleanup");
        return Err(error);
    }

    reach_generation_boundary(GenerationCrashPoint::SnapshotManifestAfterRenameBeforeDirSync)
        .map_err(|error| recovery_required("inject after final manifest rename", error))?;
    sync_directory(snapshot_directory, "sync final snapshot directory").map_err(
        |error| match error {
            FormatError::SnapshotIo { kind, .. } => FormatError::SnapshotRecoveryRequired {
                operation: "sync final snapshot directory",
                kind,
            },
            other => other,
        },
    )?;
    reach_generation_boundary(GenerationCrashPoint::SnapshotManifestDirDurable)
        .map_err(|error| recovery_required("inject after snapshot manifest durability", error))
}

/// Open a committed snapshot root and validate every manifest-authorized
/// member. A directory without the final manifest is not a snapshot.
pub fn open_snapshot_manifest(
    snapshot_directory: impl AsRef<Path>,
) -> FormatResult<SnapshotManifest> {
    let snapshot_directory = snapshot_directory.as_ref();
    validate_directory(snapshot_directory, "inspect snapshot directory")?;
    let manifest_path = snapshot_directory.join(SNAPSHOT_MANIFEST_FILE);
    let bytes = read_bounded(&manifest_path)?;
    let manifest = decode_snapshot_manifest(&bytes)?;
    validate_snapshot_directory_identity(snapshot_directory, &manifest)?;
    validate_members(snapshot_directory, &manifest, false)?;
    Ok(manifest)
}

fn validate_and_sync_members(
    snapshot_directory: &Path,
    manifest: &SnapshotManifest,
) -> FormatResult<()> {
    validate_members(snapshot_directory, manifest, true)
}

fn validate_members(
    snapshot_directory: &Path,
    manifest: &SnapshotManifest,
    synchronize: bool,
) -> FormatResult<()> {
    let mut directories = BTreeSet::new();
    for member in manifest.members().iter().copied() {
        let path = snapshot_directory.join(member.relative_path());
        let parent = path.parent().ok_or(FormatError::InvalidSnapshot {
            detail: "snapshot member has no parent directory",
        })?;
        ensure_below(snapshot_directory, parent)?;
        let mut file = open_member(&path, synchronize)?;
        validate_member(&mut file, member)?;
        if synchronize {
            reach_generation_boundary(GenerationCrashPoint::SnapshotMemberAfterWriteBeforeSync)
                .map_err(|error| snapshot_io("inject before snapshot member sync", error))?;
            file.sync_all()
                .map_err(|error| snapshot_io("sync snapshot member", error))?;
            reach_generation_boundary(GenerationCrashPoint::SnapshotMemberDurable)
                .map_err(|error| snapshot_io("inject after snapshot member sync", error))?;
            let mut directory = parent;
            loop {
                directories.insert(directory.to_path_buf());
                if directory == snapshot_directory {
                    break;
                }
                directory = directory.parent().ok_or(FormatError::InvalidSnapshot {
                    detail: "snapshot member directory escapes snapshot root",
                })?;
            }
        }
    }
    if synchronize {
        let mut directories = directories.into_iter().collect::<Vec<_>>();
        directories.sort_unstable_by_key(|directory| Reverse(directory.components().count()));
        for directory in &directories {
            sync_directory(directory, "sync snapshot member directory")?;
        }
    }
    Ok(())
}

fn validate_member(file: &mut File, member: SnapshotMember) -> FormatResult<()> {
    let metadata = file
        .metadata()
        .map_err(|error| snapshot_io("inspect snapshot member", error))?;
    if !metadata.is_file() || metadata.len() != member.byte_length() {
        return Err(FormatError::InvalidSnapshot {
            detail: "snapshot member length differs from manifest",
        });
    }

    file.seek(SeekFrom::Start(0))
        .map_err(|error| snapshot_io("seek snapshot member", error))?;
    let body_length = if member.kind() == SnapshotMemberKind::Wal {
        member.byte_length()
    } else {
        member
            .byte_length()
            .checked_sub(COMMON_FOOTER_BYTES)
            .ok_or(FormatError::InvalidSnapshot {
                detail: "snapshot member is shorter than the common footer",
            })?
    };
    let mut remaining = body_length;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    while remaining != 0 {
        let length = usize::try_from(remaining.min(HASH_BUFFER_BYTES as u64))
            .expect("bounded snapshot hash read fits usize");
        file.read_exact(&mut buffer[..length])
            .map_err(|error| snapshot_io("hash snapshot member", error))?;
        digest.update(&buffer[..length]);
        remaining -= length as u64;
    }
    let body_sha256: [u8; 32] = digest.finalize().into();
    if body_sha256 != member.body_sha256() {
        return Err(FormatError::SnapshotChecksumMismatch {
            scope: "member body",
        });
    }

    if member.kind() != SnapshotMemberKind::Wal {
        let mut footer = [0_u8; COMMON_FOOTER_BYTES as usize];
        file.read_exact(&mut footer)
            .map_err(|error| snapshot_io("read snapshot member footer", error))?;
        if footer[..8] != COMMON_FOOTER_MAGIC
            || u64::from_le_bytes(footer[8..16].try_into().expect("fixed footer length"))
                != member.byte_length()
            || footer[16..48] != body_sha256
        {
            return Err(FormatError::InvalidSnapshot {
                detail: "snapshot member common footer is invalid",
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_snapshot_member_file(
    path: &Path,
    member: SnapshotMember,
) -> FormatResult<()> {
    let mut file = open_member(path, false)?;
    validate_member(&mut file, member)
}

fn validate_snapshot_directory_identity(
    snapshot_directory: &Path,
    manifest: &SnapshotManifest,
) -> FormatResult<()> {
    validate_directory(snapshot_directory, "inspect snapshot directory")?;
    let directory_name = snapshot_directory
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(FormatError::InvalidSnapshot {
            detail: "snapshot directory has no UTF-8 identity",
        })?;
    if directory_name != manifest.snapshot_id().to_string() {
        return Err(FormatError::InvalidSnapshot {
            detail: "snapshot identity differs from directory name",
        });
    }
    Ok(())
}

fn ensure_below(root: &Path, path: &Path) -> FormatResult<()> {
    path.strip_prefix(root)
        .map(|_| ())
        .map_err(|_| FormatError::InvalidSnapshot {
            detail: "snapshot member path escapes snapshot root",
        })
}

fn read_bounded(path: &Path) -> FormatResult<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| snapshot_io("inspect snapshot manifest", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(FormatError::InvalidSnapshot {
            detail: "snapshot manifest is not a regular file",
        });
    }
    if metadata.len() > MAX_SNAPSHOT_MANIFEST_BYTES as u64 {
        return Err(FormatError::SnapshotLimitExceeded {
            field: "manifest bytes",
            actual: metadata.len(),
            limit: MAX_SNAPSHOT_MANIFEST_BYTES as u64,
        });
    }
    let length =
        usize::try_from(metadata.len()).map_err(|_| FormatError::SnapshotLimitExceeded {
            field: "manifest bytes",
            actual: metadata.len(),
            limit: MAX_SNAPSHOT_MANIFEST_BYTES as u64,
        })?;
    let mut file = open_regular(path, false)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| FormatError::SnapshotLimitExceeded {
            field: "manifest allocation",
            actual: length as u64,
            limit: MAX_SNAPSHOT_MANIFEST_BYTES as u64,
        })?;
    file.read_to_end(&mut bytes)
        .map_err(|error| snapshot_io("read snapshot manifest", error))?;
    Ok(bytes)
}

fn open_member(path: &Path, writable: bool) -> FormatResult<File> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| snapshot_io("inspect snapshot member", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(FormatError::InvalidSnapshot {
            detail: "snapshot member is not a regular file",
        });
    }
    open_regular(path, writable)
}

fn open_regular(path: &Path, writable: bool) -> FormatResult<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(writable);
    set_no_follow(&mut options);
    options
        .open(path)
        .map_err(|error| snapshot_io("open snapshot file", error))
}

fn require_absent(path: &Path, operation: &'static str) -> FormatResult<()> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(snapshot_io(operation, error)),
        Ok(_) => Err(FormatError::InvalidSnapshot {
            detail: "snapshot manifest target already exists",
        }),
    }
}

fn validate_directory(path: &Path, operation: &'static str) -> FormatResult<()> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|error| snapshot_io(operation, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(FormatError::InvalidSnapshot {
            detail: "snapshot path is not a real directory",
        });
    }
    Ok(())
}

fn sync_directory(path: &Path, operation: &'static str) -> FormatResult<()> {
    let file = File::open(path).map_err(|error| snapshot_io(operation, error))?;
    file.sync_all()
        .map_err(|error| snapshot_io(operation, error))
}

#[cfg(target_os = "linux")]
fn rename_without_replace(source: &Path, target: &Path) -> FormatResult<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let source_path = source;
    let target_path = target;
    let source =
        CString::new(source.as_os_str().as_bytes()).map_err(|_| FormatError::InvalidSnapshot {
            detail: "pending snapshot path contains NUL",
        })?;
    let target =
        CString::new(target.as_os_str().as_bytes()).map_err(|_| FormatError::InvalidSnapshot {
            detail: "final snapshot path contains NUL",
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
    Err(snapshot_io("publish snapshot manifest", error))
}

#[cfg(not(target_os = "linux"))]
fn rename_without_replace(source: &Path, target: &Path) -> FormatResult<()> {
    link_without_replace(source, target)
}

fn link_without_replace(source: &Path, target: &Path) -> FormatResult<()> {
    std::fs::hard_link(source, target)
        .map_err(|error| snapshot_io("link snapshot manifest", error))?;
    std::fs::remove_file(source)
        .map_err(|error| recovery_required("retire pending snapshot manifest", error))
}

#[cfg(unix)]
fn set_no_follow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(not(unix))]
fn set_no_follow(_options: &mut OpenOptions) {}

fn snapshot_io(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::SnapshotIo {
        operation,
        kind: error.kind(),
    }
}

fn recovery_required(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::SnapshotRecoveryRequired {
        operation,
        kind: error.kind(),
    }
}
