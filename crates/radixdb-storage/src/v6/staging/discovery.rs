use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::{fs::OpenOptions, io::Read};

use super::limits::StagingDiscoveryLimits;
use super::record::{decode_staging_complete, decode_staging_owner, StagingComplete, StagingOwner};
use super::set::validate_complete_members;
use super::{COMPLETE_FILE, OWNER_FILE};
use crate::v6::{FormatError, FormatResult, PublicationId, WriterInstanceId};

const PUBLICATION_ACCOUNTING_BYTES: u64 = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagingCompletion {
    Incomplete,
    Complete(StagingComplete),
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagingDisposition {
    Active,
    AgedOrphanCandidate,
    QuarantineRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagingPublication {
    path: PathBuf,
    directory_writer_instance_id: Option<WriterInstanceId>,
    directory_publication_id: Option<PublicationId>,
    owner: Option<StagingOwner>,
    completion: StagingCompletion,
    disposition: StagingDisposition,
}

impl StagingPublication {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub const fn directory_writer_instance_id(&self) -> Option<WriterInstanceId> {
        self.directory_writer_instance_id
    }

    pub const fn directory_publication_id(&self) -> Option<PublicationId> {
        self.directory_publication_id
    }

    pub const fn owner(&self) -> Option<StagingOwner> {
        self.owner
    }

    pub const fn completion(&self) -> StagingCompletion {
        self.completion
    }

    pub const fn disposition(&self) -> StagingDisposition {
        self.disposition
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagingDiscovery {
    publications: Vec<StagingPublication>,
}

impl StagingDiscovery {
    pub fn publications(&self) -> &[StagingPublication] {
        &self.publications
    }
}

pub fn discover_staging_publications(
    staging_root: impl AsRef<Path>,
    now_unix_ns: u64,
    orphan_min_age_ns: u64,
    limits: StagingDiscoveryLimits,
) -> FormatResult<StagingDiscovery> {
    let staging_root = staging_root.as_ref();
    validate_real_directory(staging_root)?;
    let mut publications = Vec::new();
    let mut accounted_bytes = 0_u64;
    let mut writer_entries = sorted_entries(staging_root, limits.max_publications())?;
    for writer_entry in writer_entries.drain(..) {
        let writer_path = writer_entry.path();
        let writer_metadata = std::fs::symlink_metadata(&writer_path)
            .map_err(|error| io_error("inspect staging writer entry", error))?;
        let writer_id =
            file_name(&writer_path).and_then(|name| WriterInstanceId::from_str(name).ok());
        let Some(writer_id) = writer_id else {
            push_publication(
                &mut publications,
                &mut accounted_bytes,
                limits,
                invalid_entry(writer_path, None, None),
            )?;
            continue;
        };
        if writer_metadata.file_type().is_symlink() || !writer_metadata.is_dir() {
            push_publication(
                &mut publications,
                &mut accounted_bytes,
                limits,
                invalid_entry(writer_path, Some(writer_id), None),
            )?;
            continue;
        }
        let remaining = limits
            .max_publications()
            .saturating_sub(publications.len() as u64);
        let mut publication_entries = sorted_entries(&writer_path, remaining)?;
        for publication_entry in publication_entries.drain(..) {
            let publication_path = publication_entry.path();
            let metadata = std::fs::symlink_metadata(&publication_path)
                .map_err(|error| io_error("inspect staging publication entry", error))?;
            let publication_id =
                file_name(&publication_path).and_then(|name| PublicationId::from_str(name).ok());
            let publication = match publication_id {
                Some(publication_id) if !metadata.file_type().is_symlink() && metadata.is_dir() => {
                    inspect_publication(
                        publication_path,
                        writer_id,
                        publication_id,
                        now_unix_ns,
                        orphan_min_age_ns,
                        limits,
                    )?
                }
                publication_id => invalid_entry(publication_path, Some(writer_id), publication_id),
            };
            push_publication(&mut publications, &mut accounted_bytes, limits, publication)?;
        }
    }
    publications.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(StagingDiscovery { publications })
}

fn inspect_publication(
    path: PathBuf,
    writer_id: WriterInstanceId,
    publication_id: PublicationId,
    now_unix_ns: u64,
    orphan_min_age_ns: u64,
    limits: StagingDiscoveryLimits,
) -> FormatResult<StagingPublication> {
    let owner_path = path.join(OWNER_FILE);
    let owner = match read_optional_record(&owner_path, "read staging OWNER")? {
        Some(bytes) => match decode_staging_owner(&bytes) {
            Ok(owner)
                if owner.writer_instance_id() == writer_id
                    && owner.publication_id() == publication_id =>
            {
                owner
            }
            Ok(_) | Err(_) => {
                return Ok(invalid_entry(path, Some(writer_id), Some(publication_id)))
            }
        },
        None => return Ok(invalid_entry(path, Some(writer_id), Some(publication_id))),
    };

    let complete_path = path.join(COMPLETE_FILE);
    let completion = match read_optional_record(&complete_path, "read staging COMPLETE")? {
        None => StagingCompletion::Incomplete,
        Some(bytes) => match decode_staging_complete(&bytes) {
            Ok(complete)
                if complete.writer_instance_id() == owner.writer_instance_id()
                    && complete.publication_id() == owner.publication_id()
                    && complete.intended_generation() == owner.intended_generation()
                    && complete.completed_unix_ns() >= owner.created_unix_ns() =>
            {
                match validate_complete_members(&path, complete, limits) {
                    Ok(()) => StagingCompletion::Complete(complete),
                    Err(FormatError::InvalidStagingRecord { .. })
                    | Err(FormatError::StagingChecksumMismatch { .. }) => {
                        StagingCompletion::Invalid
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(_) | Err(_) => StagingCompletion::Invalid,
        },
    };
    let disposition = if completion == StagingCompletion::Invalid {
        StagingDisposition::QuarantineRequired
    } else if now_unix_ns.saturating_sub(owner.heartbeat_unix_ns()) >= orphan_min_age_ns
        && now_unix_ns >= owner.heartbeat_unix_ns()
    {
        StagingDisposition::AgedOrphanCandidate
    } else {
        StagingDisposition::Active
    };
    Ok(StagingPublication {
        path,
        directory_writer_instance_id: Some(writer_id),
        directory_publication_id: Some(publication_id),
        owner: Some(owner),
        completion,
        disposition,
    })
}

fn invalid_entry(
    path: PathBuf,
    writer_id: Option<WriterInstanceId>,
    publication_id: Option<PublicationId>,
) -> StagingPublication {
    StagingPublication {
        path,
        directory_writer_instance_id: writer_id,
        directory_publication_id: publication_id,
        owner: None,
        completion: StagingCompletion::Invalid,
        disposition: StagingDisposition::QuarantineRequired,
    }
}

fn push_publication(
    publications: &mut Vec<StagingPublication>,
    accounted_bytes: &mut u64,
    limits: StagingDiscoveryLimits,
    publication: StagingPublication,
) -> FormatResult<()> {
    let next_count = publications.len() as u64 + 1;
    if next_count > limits.max_publications() {
        return limit("publication count", next_count, limits.max_publications());
    }
    *accounted_bytes = accounted_bytes
        .checked_add(PUBLICATION_ACCOUNTING_BYTES)
        .and_then(|bytes| bytes.checked_add(publication.path.as_os_str().len() as u64))
        .ok_or(FormatError::StagingLimitExceeded {
            field: "discovery accounted bytes",
            actual: u64::MAX,
            limit: limits.max_accounted_bytes(),
        })?;
    if *accounted_bytes > limits.max_accounted_bytes() {
        return limit(
            "discovery accounted bytes",
            *accounted_bytes,
            limits.max_accounted_bytes(),
        );
    }
    publications.push(publication);
    Ok(())
}

fn sorted_entries(path: &Path, limit_value: u64) -> FormatResult<Vec<std::fs::DirEntry>> {
    let reader =
        std::fs::read_dir(path).map_err(|error| io_error("enumerate staging directory", error))?;
    let mut entries = Vec::new();
    for entry in reader {
        let entry = entry.map_err(|error| io_error("enumerate staging directory entry", error))?;
        let next_count = entries.len() as u64 + 1;
        if next_count > limit_value {
            return limit("publication count", next_count, limit_value);
        }
        entries.push(entry);
    }
    entries.sort_by_key(std::fs::DirEntry::file_name);
    Ok(entries)
}

fn read_optional_record(path: &Path, operation: &'static str) -> FormatResult<Option<[u8; 128]>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(operation, error)),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() != 128 {
        return Ok(Some([0_u8; 128]));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    set_no_follow(&mut options);
    let mut file = options
        .open(path)
        .map_err(|error| io_error(operation, error))?;
    let mut bytes = [0_u8; 128];
    file.read_exact(&mut bytes)
        .map_err(|error| io_error(operation, error))?;
    Ok(Some(bytes))
}

fn validate_real_directory(path: &Path) -> FormatResult<()> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|error| io_error("inspect staging root", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(FormatError::InvalidStagingRecord {
            record: "discovery",
            detail: "staging root is not a real directory",
        });
    }
    Ok(())
}

fn file_name(path: &Path) -> Option<&str> {
    path.file_name()?.to_str()
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
fn set_no_follow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(not(unix))]
fn set_no_follow(_options: &mut OpenOptions) {}
