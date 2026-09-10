use std::fs::ReadDir;
use std::io::Read;
use std::path::Path;

use sha2::{Digest, Sha256};

use super::limits::{StagingDiscoveryLimits, MAX_STAGED_PATH_COMPONENT_BYTES};
use super::{COMPLETE_FILE, COMPLETE_PENDING_FILE, OWNER_FILE};
use crate::v6::{FormatError, FormatResult};

const MEMBER_ACCOUNTING_BYTES: u64 = 64;
const HASH_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
struct MemberDescriptor {
    relative_path: String,
    byte_length: u64,
    sha256: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MemberSetDigest {
    pub(crate) count: u64,
    pub(crate) sha256: [u8; 32],
}

pub(crate) fn digest_selected_members(
    publication_directory: &Path,
    final_root: &Path,
    relative_paths: &[std::path::PathBuf],
    limits: StagingDiscoveryLimits,
) -> FormatResult<MemberSetDigest> {
    if relative_paths.len() as u64 > limits.max_files_per_publication() {
        return limit(
            "filesystem entries per publication",
            relative_paths.len() as u64,
            limits.max_files_per_publication(),
        );
    }

    let mut descriptors = Vec::with_capacity(relative_paths.len());
    let mut accounted_bytes = 0_u64;
    for relative in relative_paths {
        let relative = relative.to_str().ok_or(FormatError::InvalidStagingRecord {
            record: "COMPLETE",
            detail: "member path is not UTF-8",
        })?;
        validate_relative_path(relative, limits.max_path_bytes())?;
        let depth = relative.split('/').count() as u64;
        if depth > limits.max_recursion_depth().saturating_add(1) {
            return limit(
                "directory recursion depth",
                depth.saturating_sub(1),
                limits.max_recursion_depth(),
            );
        }
        accounted_bytes = accounted_bytes
            .checked_add(MEMBER_ACCOUNTING_BYTES)
            .and_then(|bytes| bytes.checked_add(relative.len() as u64))
            .ok_or(FormatError::StagingLimitExceeded {
                field: "discovery accounted bytes",
                actual: u64::MAX,
                limit: limits.max_accounted_bytes(),
            })?;
        if accounted_bytes > limits.max_accounted_bytes() {
            return limit(
                "discovery accounted bytes",
                accounted_bytes,
                limits.max_accounted_bytes(),
            );
        }

        let staged_path = publication_directory.join(relative);
        let actual_path = match std::fs::symlink_metadata(&staged_path) {
            Ok(_) => staged_path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => final_root.join(relative),
            Err(error) => return Err(io_error("inspect selected staged member", error)),
        };
        let metadata = std::fs::symlink_metadata(&actual_path)
            .map_err(|error| io_error("inspect selected publication member", error))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return invalid("selected publication member is not a regular file");
        }
        let (byte_length, sha256) = hash_regular_file(&actual_path, metadata.len())?;
        descriptors.push(MemberDescriptor {
            relative_path: relative.to_owned(),
            byte_length,
            sha256,
        });
    }
    digest_descriptors(descriptors)
}

pub(crate) fn digest_members(
    publication_directory: &Path,
    limits: StagingDiscoveryLimits,
) -> FormatResult<MemberSetDigest> {
    let mut pending = vec![(publication_directory.to_path_buf(), 0_u64)];
    let mut members = Vec::new();
    let mut accounted_bytes = 0_u64;
    let mut scanned_entries = 0_u64;

    while let Some((directory, depth)) = pending.pop() {
        if depth > limits.max_recursion_depth() {
            return limit(
                "directory recursion depth",
                depth,
                limits.max_recursion_depth(),
            );
        }
        let entries = read_directory(&directory)?;
        for entry in entries {
            let entry =
                entry.map_err(|error| io_error("enumerate publication directory", error))?;
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path)
                .map_err(|error| io_error("inspect staged member", error))?;
            let relative = path.strip_prefix(publication_directory).map_err(|_| {
                FormatError::InvalidStagingRecord {
                    record: "COMPLETE",
                    detail: "member escaped publication directory",
                }
            })?;
            let relative = relative.to_str().ok_or(FormatError::InvalidStagingRecord {
                record: "COMPLETE",
                detail: "member path is not UTF-8",
            })?;
            if depth == 0 && is_control_file(relative) {
                continue;
            }
            scanned_entries =
                scanned_entries
                    .checked_add(1)
                    .ok_or(FormatError::StagingLimitExceeded {
                        field: "filesystem entries per publication",
                        actual: u64::MAX,
                        limit: limits.max_files_per_publication(),
                    })?;
            if scanned_entries > limits.max_files_per_publication() {
                return limit(
                    "filesystem entries per publication",
                    scanned_entries,
                    limits.max_files_per_publication(),
                );
            }
            validate_relative_path(relative, limits.max_path_bytes())?;
            accounted_bytes = accounted_bytes
                .checked_add(MEMBER_ACCOUNTING_BYTES)
                .and_then(|bytes| bytes.checked_add(relative.len() as u64))
                .ok_or(FormatError::StagingLimitExceeded {
                    field: "discovery accounted bytes",
                    actual: u64::MAX,
                    limit: limits.max_accounted_bytes(),
                })?;
            if accounted_bytes > limits.max_accounted_bytes() {
                return limit(
                    "discovery accounted bytes",
                    accounted_bytes,
                    limits.max_accounted_bytes(),
                );
            }
            if metadata.file_type().is_symlink() {
                return invalid("staged member is a symbolic link");
            }
            if metadata.is_dir() {
                pending.push((path, depth.saturating_add(1)));
                continue;
            }
            if !metadata.is_file() {
                return invalid("staged member is not a regular file");
            }
            let (byte_length, sha256) = hash_regular_file(&path, metadata.len())?;
            members.push(MemberDescriptor {
                relative_path: relative.to_owned(),
                byte_length,
                sha256,
            });
        }
    }

    digest_descriptors(members)
}

fn digest_descriptors(mut members: Vec<MemberDescriptor>) -> FormatResult<MemberSetDigest> {
    members.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    if members
        .windows(2)
        .any(|pair| pair[0].relative_path == pair[1].relative_path)
    {
        return invalid("staged member path is duplicated");
    }
    let mut digest = Sha256::new();
    digest.update(b"RDX6STAGED-MEMBERS\0"); // versioned-format-name
    digest.update((members.len() as u64).to_le_bytes());
    for member in &members {
        let path_bytes = member.relative_path.as_bytes();
        digest.update((path_bytes.len() as u32).to_le_bytes());
        digest.update(path_bytes);
        digest.update(member.byte_length.to_le_bytes());
        digest.update(member.sha256);
    }
    Ok(MemberSetDigest {
        count: members.len() as u64,
        sha256: digest.finalize().into(),
    })
}

pub(crate) fn validate_relative_path(path: &str, max_bytes: u64) -> FormatResult<()> {
    if path.is_empty()
        || path.len() as u64 > max_bytes
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains('\\')
        || !path.is_ascii()
    {
        return invalid("staged member has invalid relative path");
    }
    for component in path.split('/') {
        if component.is_empty()
            || component == "."
            || component == ".."
            || component.len() > MAX_STAGED_PATH_COMPONENT_BYTES
        {
            return invalid("staged member has invalid path component");
        }
    }
    let first = path.split('/').next().expect("non-empty path");
    if is_control_file(first) {
        return invalid("staged member uses a reserved control name");
    }
    Ok(())
}

fn hash_regular_file(path: &Path, expected_length: u64) -> FormatResult<(u64, [u8; 32])> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    set_no_follow(&mut options);
    let mut file = options
        .open(path)
        .map_err(|error| io_error("open staged member", error))?;
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    let mut digest = Sha256::new();
    let mut read_bytes = 0_u64;
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|error| io_error("read staged member", error))?;
        if count == 0 {
            break;
        }
        read_bytes =
            read_bytes
                .checked_add(count as u64)
                .ok_or(FormatError::StagingLimitExceeded {
                    field: "staged member bytes",
                    actual: u64::MAX,
                    limit: u64::MAX - 1,
                })?;
        digest.update(&buffer[..count]);
    }
    let final_length = file
        .metadata()
        .map_err(|error| io_error("reinspect staged member", error))?
        .len();
    if read_bytes != expected_length || final_length != expected_length {
        return invalid("staged member changed during inspection");
    }
    Ok((read_bytes, digest.finalize().into()))
}

fn read_directory(path: &Path) -> FormatResult<ReadDir> {
    std::fs::read_dir(path).map_err(|error| io_error("read publication directory", error))
}

fn is_control_file(name: &str) -> bool {
    matches!(name, OWNER_FILE | COMPLETE_FILE | COMPLETE_PENDING_FILE)
}

fn invalid<T>(detail: &'static str) -> FormatResult<T> {
    Err(FormatError::InvalidStagingRecord {
        record: "publication",
        detail,
    })
}

fn limit<T>(field: &'static str, actual: u64, limit: u64) -> FormatResult<T> {
    Err(FormatError::StagingLimitExceeded {
        field,
        actual,
        limit,
    })
}

fn io_error(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::StagingIo {
        operation,
        kind: error.kind(),
    }
}

#[cfg(unix)]
fn set_no_follow(options: &mut std::fs::OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(not(unix))]
fn set_no_follow(_options: &mut std::fs::OpenOptions) {}
