use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::v6::{
    decode_control_slot, fault::reach_generation_boundary, ControlRecord, ControlSlotIndex,
    FormatError, FormatResult, GenerationCrashPoint, WalGeneration, CONTROL_RECORD_BYTES,
};

use super::model::{WalRetirementFence, WalRetirementReport};

pub(crate) fn retire_obsolete_wal(fence: &WalRetirementFence) -> FormatResult<WalRetirementReport> {
    validate_directory(&fence.root, "inspect checkpoint database root")?;
    validate_durable_fence(&fence.root, fence.source_control, fence.target_control)?;
    validate_retirement_set(fence)?;

    let wal_root = fence.root.join("wal");
    validate_directory(&wal_root, "inspect WAL directory")?;
    let retired_root = wal_root.join("retired");
    create_or_validate_directory(&wal_root, &retired_root)?;
    reach_generation_boundary(GenerationCrashPoint::WalBeforeTruncate)
        .map_err(|error| retirement_io("inject before WAL retirement", error))?;

    let mut renamed = 0;
    let mut unlinked = 0;
    let mut already_absent = 0;
    for generation in &fence.obsolete_wal {
        let source = wal_path(&wal_root, *generation);
        let retired = wal_path(&retired_root, *generation);
        match (
            path_exists(&source, "inspect active WAL generation")?,
            path_exists(&retired, "inspect retired WAL generation")?,
        ) {
            (true, false) => {
                validate_regular(&source, "active WAL generation is not a regular file")?;
                rename_without_replace(&source, &retired)?;
                reach_generation_boundary(GenerationCrashPoint::WalAfterRenameToRetired)
                    .map_err(|error| retirement_io("inject after WAL retirement rename", error))?;
                sync_directory(&wal_root, "sync WAL directory after retirement rename")?;
                sync_directory(
                    &retired_root,
                    "sync retired WAL directory after retirement rename",
                )?;
                renamed += 1;
            }
            (false, true) => {
                validate_regular(&retired, "retired WAL generation is not a regular file")?;
            }
            (false, false) => {
                already_absent += 1;
                continue;
            }
            (true, true) => {
                validate_regular(&source, "active WAL generation is not a regular file")?;
                validate_regular(&retired, "retired WAL generation is not a regular file")?;
                if !same_contents(&source, &retired)? {
                    return invalid("active and retired WAL generation bytes differ");
                }
                std::fs::remove_file(&source)
                    .map_err(|error| retirement_io("remove duplicate active WAL link", error))?;
                sync_directory(&wal_root, "sync WAL directory after duplicate link removal")?;
            }
        }

        std::fs::remove_file(&retired)
            .map_err(|error| retirement_io("unlink retired WAL generation", error))?;
        reach_generation_boundary(GenerationCrashPoint::WalAfterUnlinkBeforeDirSync)
            .map_err(|error| retirement_io("inject after retired WAL unlink", error))?;
        sync_directory(&retired_root, "sync retired WAL directory after unlink")?;
        reach_generation_boundary(GenerationCrashPoint::WalTruncateDirDurable)
            .map_err(|error| retirement_io("inject after WAL retirement durability", error))?;
        unlinked += 1;
    }

    Ok(WalRetirementReport::new(
        fence.obsolete_wal.len(),
        renamed,
        unlinked,
        already_absent,
    ))
}

fn validate_durable_fence(
    root: &Path,
    source: ControlRecord,
    target: ControlRecord,
) -> FormatResult<()> {
    if read_control(root, source.slot())? != source {
        return invalid("retained source CONTROL differs from checkpoint fence");
    }
    if read_control(root, target.slot())? != target {
        return invalid("durable target CONTROL differs from checkpoint fence");
    }
    Ok(())
}

fn validate_retirement_set(fence: &WalRetirementFence) -> FormatResult<()> {
    let source_floor = fence.source_control.wal_replay_floor().generation();
    let target_floor = fence.target_control.wal_replay_floor().generation();
    if fence
        .obsolete_wal
        .iter()
        .any(|generation| *generation >= source_floor || *generation >= target_floor)
    {
        return invalid("WAL generation remains reachable from a retained CONTROL");
    }
    Ok(())
}

fn read_control(root: &Path, slot: ControlSlotIndex) -> FormatResult<ControlRecord> {
    let path = root.join(control_name(slot));
    let metadata = std::fs::symlink_metadata(&path)
        .map_err(|error| retirement_io("inspect retained CONTROL", error))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() != CONTROL_RECORD_BYTES as u64
    {
        return invalid("retained CONTROL is not one exact regular slot");
    }
    let mut options = OpenOptions::new();
    options.read(true);
    set_no_follow(&mut options);
    let mut file = options
        .open(path)
        .map_err(|error| retirement_io("open retained CONTROL", error))?;
    let mut bytes = [0_u8; CONTROL_RECORD_BYTES];
    file.read_exact(&mut bytes)
        .map_err(|error| retirement_io("read retained CONTROL", error))?;
    decode_control_slot(&bytes, slot)
}

fn create_or_validate_directory(parent: &Path, path: &Path) -> FormatResult<()> {
    match std::fs::create_dir(path) {
        Ok(()) => sync_directory(parent, "sync WAL directory after retired directory create"),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            validate_directory(path, "inspect retired WAL directory")
        }
        Err(error) => Err(retirement_io("create retired WAL directory", error)),
    }
}

fn path_exists(path: &Path, operation: &'static str) -> FormatResult<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(retirement_io(operation, error)),
    }
}

fn validate_regular(path: &Path, detail: &'static str) -> FormatResult<()> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| retirement_io("inspect WAL generation", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return invalid(detail);
    }
    Ok(())
}

fn validate_directory(path: &Path, operation: &'static str) -> FormatResult<()> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|error| retirement_io(operation, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return invalid("checkpoint path is not a real directory");
    }
    Ok(())
}

fn same_contents(left: &Path, right: &Path) -> FormatResult<bool> {
    const BUFFER_BYTES: usize = 64 * 1024;

    let left_metadata = std::fs::symlink_metadata(left)
        .map_err(|error| retirement_io("inspect active WAL collision", error))?;
    let right_metadata = std::fs::symlink_metadata(right)
        .map_err(|error| retirement_io("inspect retired WAL collision", error))?;
    if left_metadata.len() != right_metadata.len() {
        return Ok(false);
    }
    let mut left = open_read(left)?;
    let mut right = open_read(right)?;
    let mut left_buffer = [0_u8; BUFFER_BYTES];
    let mut right_buffer = [0_u8; BUFFER_BYTES];
    loop {
        let left_count = left
            .read(&mut left_buffer)
            .map_err(|error| retirement_io("read active WAL collision", error))?;
        let right_count = right
            .read(&mut right_buffer)
            .map_err(|error| retirement_io("read retired WAL collision", error))?;
        if left_count != right_count || left_buffer[..left_count] != right_buffer[..right_count] {
            return Ok(false);
        }
        if left_count == 0 {
            return Ok(true);
        }
    }
}

fn open_read(path: &Path) -> FormatResult<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    set_no_follow(&mut options);
    options
        .open(path)
        .map_err(|error| retirement_io("open WAL collision", error))
}

#[cfg(target_os = "linux")]
fn rename_without_replace(source: &Path, target: &Path) -> FormatResult<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let source_path = source;
    let target_path = target;
    let source = CString::new(source.as_os_str().as_bytes()).map_err(|_| {
        FormatError::InvalidCheckpoint {
            detail: "active WAL path contains NUL",
        }
    })?;
    let target = CString::new(target.as_os_str().as_bytes()).map_err(|_| {
        FormatError::InvalidCheckpoint {
            detail: "retired WAL path contains NUL",
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
    Err(retirement_io("rename WAL generation to retired", error))
}

#[cfg(not(target_os = "linux"))]
fn rename_without_replace(source: &Path, target: &Path) -> FormatResult<()> {
    link_without_replace(source, target)
}

fn link_without_replace(source: &Path, target: &Path) -> FormatResult<()> {
    std::fs::hard_link(source, target)
        .map_err(|error| retirement_io("link WAL generation to retired", error))?;
    std::fs::remove_file(source)
        .map_err(|error| retirement_io("remove active WAL link after retirement", error))
}

fn wal_path(root: &Path, generation: WalGeneration) -> PathBuf {
    root.join(format!("wal-{:016x}.log", generation.get()))
}

const fn control_name(slot: ControlSlotIndex) -> &'static str {
    match slot {
        ControlSlotIndex::Zero => "CONTROL.0",
        ControlSlotIndex::One => "CONTROL.1",
    }
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
        .map_err(|error| retirement_io(operation, error))
}

fn invalid<T>(detail: &'static str) -> FormatResult<T> {
    Err(FormatError::InvalidCheckpoint { detail })
}

fn retirement_io(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::WalRetirementIo {
        operation,
        kind: error.kind(),
    }
}
