use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::v6::{
    fault::reach_generation_boundary, CleanupGeneration, FormatError, FormatResult,
    GenerationCrashPoint,
};

use super::discovery::DiscoveredMember;

const CYCLE_RECORD_BYTES: usize = 128;
const CYCLE_MAGIC: [u8; 8] = *b"RDX6GCC\0";
const CYCLE_CRC_OFFSET: usize = 124;

pub(crate) fn begin_cycle(
    root: &Path,
    quarantine_generations: &[CleanupGeneration],
) -> FormatResult<CleanupGeneration> {
    let quarantine_root = root.join("quarantine");
    create_durable_directory(root, &quarantine_root)?;
    validate_same_filesystem(root, &quarantine_root)?;
    let persisted = read_cycle_slots(&quarantine_root)?;
    let observed = quarantine_generations.iter().copied().max();
    let previous = match (persisted, observed) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    };
    let next = match previous {
        Some(previous) => previous.checked_next()?,
        None => CleanupGeneration::new(1)?,
    };
    write_cycle_slot(&quarantine_root, next)?;
    Ok(next)
}

pub(crate) fn quarantine_member(
    root: &Path,
    artifact: &DiscoveredMember,
    generation: CleanupGeneration,
) -> FormatResult<()> {
    let target = quarantine_path(root, generation, &artifact.relative_path);
    let parent = target.parent().ok_or(FormatError::InvalidCleanup {
        detail: "quarantine target has no parent",
    })?;
    create_durable_directories(root, parent)?;
    validate_same_filesystem(root, parent)?;
    reach_generation_boundary(GenerationCrashPoint::GcBeforeQuarantineRename)
        .map_err(|error| io_error("inject before quarantine rename", error))?;
    rename_without_replace(&artifact.path, &target)?;
    reach_generation_boundary(GenerationCrashPoint::GcAfterQuarantineRenameBeforeSync)
        .map_err(|error| recovery_required("inject after quarantine rename", error))?;
    sync_after_rename(&artifact.path, &target)?;
    reach_generation_boundary(GenerationCrashPoint::GcQuarantineDirDurable)
        .map_err(|error| recovery_required("inject after quarantine durability", error))
}

pub(crate) fn restore_member(
    root: &Path,
    artifact: &DiscoveredMember,
    canonical_already_present: bool,
) -> FormatResult<()> {
    let target = root.join(&artifact.relative_path);
    let parent = target.parent().ok_or(FormatError::InvalidCleanup {
        detail: "artifact target has no parent",
    })?;
    create_durable_directories(root, parent)?;
    if canonical_already_present {
        std::fs::remove_file(&artifact.path)
            .map_err(|error| recovery_required("remove duplicate quarantined artifact", error))?;
        sync_directory(
            artifact
                .path
                .parent()
                .expect("quarantine file has a parent"),
            "sync quarantine after duplicate removal",
        )?;
        return Ok(());
    }
    rename_without_replace(&artifact.path, &target)?;
    sync_after_rename(&artifact.path, &target)
}

pub(crate) fn delete_member(artifact: &DiscoveredMember) -> FormatResult<()> {
    reach_generation_boundary(GenerationCrashPoint::GcBeforeUnlink)
        .map_err(|error| io_error("inject before quarantined artifact unlink", error))?;
    std::fs::remove_file(&artifact.path)
        .map_err(|error| recovery_required("unlink quarantined artifact", error))?;
    reach_generation_boundary(GenerationCrashPoint::GcAfterUnlinkBeforeDirSync)
        .map_err(|error| recovery_required("inject after quarantined artifact unlink", error))?;
    sync_directory(
        artifact
            .path
            .parent()
            .expect("quarantine file has a parent"),
        "sync quarantine after unlink",
    )?;
    reach_generation_boundary(GenerationCrashPoint::GcDeleteDirDurable)
        .map_err(|error| recovery_required("inject after quarantine deletion durability", error))
}

pub(crate) fn remove_empty_generation(
    root: &Path,
    generation: CleanupGeneration,
) -> FormatResult<()> {
    let generation_root = root
        .join("quarantine")
        .join(format!("q-{:016x}", generation.get()));
    if !path_exists(
        &generation_root,
        "inspect quarantine generation for pruning",
    )? {
        return Ok(());
    }
    let artifacts = generation_root.join("artifacts");
    for kind in ["data", "index"] {
        let kind_root = artifacts.join(kind);
        if !path_exists(&kind_root, "inspect quarantine kind for pruning")? {
            continue;
        }
        let entries = read_entries(&kind_root, "enumerate quarantine kind for pruning")?;
        for shard in entries {
            validate_directory(&shard, "inspect quarantine shard for pruning")?;
            remove_if_empty(&shard)?;
        }
        remove_if_empty(&kind_root)?;
    }
    remove_if_empty(&artifacts)?;
    remove_if_empty(&generation_root.join("catalog"))?;
    let manifests = generation_root.join("manifests");
    let tables = manifests.join("tables");
    if path_exists(&tables, "inspect quarantined table manifests for pruning")? {
        validate_directory(&tables, "inspect quarantined table manifests for pruning")?;
        for table in read_entries(&tables, "enumerate quarantined table manifests for pruning")? {
            validate_directory(&table, "inspect quarantined table directory for pruning")?;
            remove_if_empty(&table)?;
        }
        remove_if_empty(&tables)?;
    }
    remove_if_empty(&manifests)?;
    remove_if_empty(&generation_root)
}

pub(crate) fn quarantine_path(
    root: &Path,
    generation: CleanupGeneration,
    relative_path: &Path,
) -> PathBuf {
    root.join("quarantine")
        .join(format!("q-{:016x}", generation.get()))
        .join(relative_path)
}

fn read_cycle_slots(quarantine_root: &Path) -> FormatResult<Option<CleanupGeneration>> {
    let mut valid = Vec::new();
    let mut present = 0_u8;
    for slot in 0..=1 {
        let path = quarantine_root.join(format!("CYCLE.{slot}"));
        match std::fs::read(&path) {
            Ok(bytes) => {
                present += 1;
                if let Ok(generation) = decode_cycle_record(&bytes) {
                    valid.push(generation);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error("read cleanup cycle record", error)),
        }
    }
    if present != 0 && valid.is_empty() {
        return Err(FormatError::InvalidCleanup {
            detail: "no valid cleanup cycle record remains",
        });
    }
    Ok(valid.into_iter().max())
}

fn write_cycle_slot(quarantine_root: &Path, generation: CleanupGeneration) -> FormatResult<()> {
    let slot = generation.get() & 1;
    let final_path = quarantine_root.join(format!("CYCLE.{slot}"));
    let pending_path = quarantine_root.join(format!("CYCLE.{slot}.pending"));
    let bytes = encode_cycle_record(generation);
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    set_no_follow(&mut options);
    let mut pending = options
        .open(&pending_path)
        .map_err(|error| io_error("open pending cleanup cycle record", error))?;
    pending
        .write_all(&bytes)
        .map_err(|error| io_error("write cleanup cycle record", error))?;
    pending
        .sync_all()
        .map_err(|error| io_error("sync cleanup cycle record", error))?;
    std::fs::rename(&pending_path, &final_path)
        .map_err(|error| recovery_required("publish cleanup cycle record", error))?;
    sync_directory(quarantine_root, "sync cleanup cycle directory")
}

fn encode_cycle_record(generation: CleanupGeneration) -> [u8; CYCLE_RECORD_BYTES] {
    let mut output = [0_u8; CYCLE_RECORD_BYTES];
    output[..8].copy_from_slice(&CYCLE_MAGIC);
    put_u16(&mut output, 8, 6);
    put_u16(&mut output, 10, 0);
    put_u32(&mut output, 12, CYCLE_RECORD_BYTES as u32);
    put_u64(&mut output, 16, generation.get());
    let crc = radixdb_core::crc32_ieee(&output[..CYCLE_CRC_OFFSET]);
    put_u32(&mut output, CYCLE_CRC_OFFSET, crc);
    output
}

fn decode_cycle_record(bytes: &[u8]) -> FormatResult<CleanupGeneration> {
    if bytes.len() != CYCLE_RECORD_BYTES
        || bytes[..8] != CYCLE_MAGIC
        || read_u16(bytes, 8) != 6
        || read_u16(bytes, 10) != 0
        || read_u32(bytes, 12) != CYCLE_RECORD_BYTES as u32
        || bytes[24..CYCLE_CRC_OFFSET].iter().any(|byte| *byte != 0)
        || read_u32(bytes, CYCLE_CRC_OFFSET) != radixdb_core::crc32_ieee(&bytes[..CYCLE_CRC_OFFSET])
    {
        return Err(FormatError::InvalidCleanup {
            detail: "cleanup cycle record is invalid",
        });
    }
    CleanupGeneration::new(read_u64(bytes, 16))
}

fn create_durable_directories(root: &Path, target: &Path) -> FormatResult<()> {
    let relative = target
        .strip_prefix(root)
        .map_err(|_| FormatError::InvalidCleanup {
            detail: "cleanup target escapes database root",
        })?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let parent = current.clone();
        current.push(component);
        create_durable_directory(&parent, &current)?;
    }
    Ok(())
}

fn create_durable_directory(parent: &Path, directory: &Path) -> FormatResult<()> {
    match std::fs::create_dir(directory) {
        Ok(()) => sync_directory(parent, "sync new cleanup directory"),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            validate_directory(directory, "inspect cleanup directory")
        }
        Err(error) => Err(io_error("create cleanup directory", error)),
    }
}

fn remove_if_empty(path: &Path) -> FormatResult<()> {
    if !path_exists(path, "inspect cleanup directory for pruning")? {
        return Ok(());
    }
    validate_directory(path, "inspect cleanup directory for pruning")?;
    if read_entries(path, "enumerate cleanup directory for pruning")?.is_empty() {
        std::fs::remove_dir(path)
            .map_err(|error| recovery_required("remove empty cleanup directory", error))?;
        sync_directory(
            path.parent().expect("cleanup directory has a parent"),
            "sync parent after cleanup directory removal",
        )?;
    }
    Ok(())
}

fn sync_after_rename(source: &Path, target: &Path) -> FormatResult<()> {
    sync_directory(
        source.parent().expect("source has a parent"),
        "sync cleanup rename source directory",
    )?;
    sync_directory(
        target.parent().expect("target has a parent"),
        "sync cleanup rename target directory",
    )
}

#[cfg(target_os = "linux")]
fn rename_without_replace(source: &Path, target: &Path) -> FormatResult<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let source =
        CString::new(source.as_os_str().as_bytes()).map_err(|_| FormatError::InvalidCleanup {
            detail: "cleanup source path contains NUL",
        })?;
    let target =
        CString::new(target.as_os_str().as_bytes()).map_err(|_| FormatError::InvalidCleanup {
            detail: "cleanup target path contains NUL",
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
        Ok(())
    } else {
        Err(recovery_required(
            "rename artifact without replacement",
            std::io::Error::last_os_error(),
        ))
    }
}

#[cfg(not(target_os = "linux"))]
fn rename_without_replace(source: &Path, target: &Path) -> FormatResult<()> {
    if path_exists(target, "inspect cleanup rename target")? {
        return Err(FormatError::InvalidCleanup {
            detail: "cleanup rename target already exists",
        });
    }
    std::fs::rename(source, target)
        .map_err(|error| recovery_required("rename artifact without replacement", error))
}

#[cfg(unix)]
fn validate_same_filesystem(left: &Path, right: &Path) -> FormatResult<()> {
    use std::os::unix::fs::MetadataExt;
    let left =
        std::fs::metadata(left).map_err(|error| io_error("inspect database filesystem", error))?;
    let right = std::fs::metadata(right)
        .map_err(|error| io_error("inspect quarantine filesystem", error))?;
    if left.dev() != right.dev() {
        return Err(FormatError::InvalidCleanup {
            detail: "artifact and quarantine roots are on different filesystems",
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_same_filesystem(_left: &Path, _right: &Path) -> FormatResult<()> {
    Ok(())
}

fn read_entries(path: &Path, operation: &'static str) -> FormatResult<Vec<PathBuf>> {
    std::fs::read_dir(path)
        .map_err(|error| io_error(operation, error))?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|error| io_error(operation, error))
        })
        .collect()
}

fn validate_directory(path: &Path, operation: &'static str) -> FormatResult<()> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| io_error(operation, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(FormatError::InvalidCleanup {
            detail: "cleanup directory is not a real directory",
        });
    }
    Ok(())
}

fn path_exists(path: &Path, operation: &'static str) -> FormatResult<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error(operation, error)),
    }
}

fn sync_directory(path: &Path, operation: &'static str) -> FormatResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| recovery_required(operation, error))
}

#[cfg(unix)]
fn set_no_follow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(not(unix))]
fn set_no_follow(_options: &mut OpenOptions) {}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> [u8; N] {
    bytes[offset..offset + N]
        .try_into()
        .expect("fixed range is present")
}
fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(read_array(bytes, offset))
}
fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(read_array(bytes, offset))
}
fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(read_array(bytes, offset))
}
fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}
fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}
fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn io_error(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::CleanupIo {
        operation,
        kind: error.kind(),
    }
}

fn recovery_required(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::CleanupRecoveryRequired {
        operation,
        kind: error.kind(),
    }
}
