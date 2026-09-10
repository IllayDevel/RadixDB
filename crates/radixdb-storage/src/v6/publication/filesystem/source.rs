use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::super::diagnostics::{record, DiagnosticEvent};

use crate::v6::{
    decode_table_manifest, open_data_artifact_metadata_with_limits,
    open_index_artifact_metadata_with_limits, ArtifactFile, ArtifactId, ArtifactInspection,
    ArtifactKind, ArtifactMetadata, ArtifactRef, CatalogRef, DataArtifactLayout,
    DataArtifactMetadata, DataOpenLimits, DatabaseManifestRootRef, FormatError, FormatResult,
    IndexArtifactMetadata, IndexOpenLimits, ReachabilityAllowance, ReachabilityError,
    ReachabilityResult, ReachabilitySource, TableManifestRef, MAX_DATA_OPEN_METADATA_BYTES,
    MAX_INDEX_OPEN_METADATA_BYTES,
};

const COMMON_FOOTER_BYTES: u64 = 48;
const HASH_BUFFER_BYTES: usize = 64 * 1024;

pub(crate) struct GenerationFileSource<'a> {
    root: &'a Path,
    staging: Option<&'a Path>,
    data_layouts: HashMap<ArtifactId, DataArtifactLayout>,
    index_data: HashMap<ArtifactId, ArtifactId>,
    body_verification: BodyVerification,
}

enum BodyVerification {
    All,
    ChangedOnly(HashSet<ArtifactRef>),
    MetadataOnly,
}

#[derive(Clone, Copy)]
enum IdentityReadRole {
    New,
    Retained,
}

struct ResolvedPath {
    path: PathBuf,
    staged: bool,
}

impl<'a> GenerationFileSource<'a> {
    pub(crate) fn for_initial_publication(root: &'a Path, staging: &'a Path) -> Self {
        Self {
            root,
            staging: Some(staging),
            data_layouts: HashMap::new(),
            index_data: HashMap::new(),
            body_verification: BodyVerification::All,
        }
    }

    pub(crate) fn for_publication(
        root: &'a Path,
        staging: &'a Path,
        retained: impl IntoIterator<Item = ArtifactRef>,
    ) -> Self {
        Self {
            root,
            staging: Some(staging),
            data_layouts: HashMap::new(),
            index_data: HashMap::new(),
            body_verification: BodyVerification::ChangedOnly(retained.into_iter().collect()),
        }
    }

    pub(crate) fn for_recovery(root: &'a Path) -> Self {
        Self {
            root,
            staging: None,
            data_layouts: HashMap::new(),
            index_data: HashMap::new(),
            body_verification: BodyVerification::MetadataOnly,
        }
    }

    fn resolve(
        &self,
        relative: &Path,
        node: crate::v6::ReachableNodeKind,
    ) -> ReachabilityResult<Option<ResolvedPath>> {
        if let Some(staging) = self.staging {
            let staged = staging.join(relative);
            match std::fs::symlink_metadata(&staged) {
                Ok(_) => {
                    return Ok(Some(ResolvedPath {
                        path: staged,
                        staged: true,
                    }))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(source_failure(node, "inspect staged path", error)),
            }
        }
        let final_path = self.root.join(relative);
        match std::fs::symlink_metadata(&final_path) {
            Ok(_) => Ok(Some(ResolvedPath {
                path: final_path,
                staged: false,
            })),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(source_failure(node, "inspect final path", error)),
        }
    }

    fn read_bounded(
        &self,
        relative: &Path,
        byte_budget: u64,
        node: crate::v6::ReachableNodeKind,
    ) -> ReachabilityResult<Option<Vec<u8>>> {
        let Some(path) = self.resolve(relative, node)? else {
            return Ok(None);
        };
        let metadata = std::fs::symlink_metadata(&path.path)
            .map_err(|error| source_failure(node, "inspect publication file", error))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(ReachabilityError::source_failure(
                node,
                "publication path is not a regular file",
            ));
        }
        if metadata.len() > byte_budget {
            return Err(ReachabilityError::source_failure(
                node,
                format!(
                    "publication file is {} bytes; read budget is {byte_budget}",
                    metadata.len()
                ),
            ));
        }
        let length = usize::try_from(metadata.len()).map_err(|_| {
            ReachabilityError::source_failure(
                node,
                "publication file length does not fit this platform",
            )
        })?;
        let mut file = open_regular(&path.path)
            .map_err(|error| source_failure(node, "open publication file", error))?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(|_| {
            ReachabilityError::source_failure(node, "publication file allocation failed")
        })?;
        bytes.resize(length, 0);
        file.read_exact(&mut bytes)
            .map_err(|error| source_failure(node, "read publication file", error))?;
        Ok(Some(bytes))
    }

    fn inspect(
        &mut self,
        reference: ArtifactRef,
        allowance: ReachabilityAllowance,
    ) -> ReachabilityResult<ArtifactInspection> {
        let node = match reference.kind() {
            ArtifactKind::Data => crate::v6::ReachableNodeKind::DataArtifact,
            ArtifactKind::Index => crate::v6::ReachableNodeKind::IndexArtifact,
        };
        let Some(path) = self.resolve(&reference.relative_path(), node)? else {
            return Ok(ArtifactInspection::Missing);
        };
        let result: Result<(ArtifactMetadata, u64), FormatError> = (|| {
            if let Some(role) = self
                .body_verification
                .identity_read_role(reference, path.staged)
            {
                validate_body_identity(&path.path, reference, Some(role))?;
            }
            let file =
                open_regular(&path.path).map_err(|error| publication_io("open artifact", error))?;
            let source = ArtifactFile::from_file(file)?;
            match reference.kind() {
                ArtifactKind::Data => {
                    let remaining = allowance.remaining_bytes();
                    if remaining == 0 {
                        return Err(FormatError::DataArtifactLimitExceeded {
                            field: "metadata-open accounted bytes",
                            actual: 1,
                            limit: 0,
                        });
                    }
                    let limit = remaining.min(MAX_DATA_OPEN_METADATA_BYTES);
                    let opened = open_data_artifact_metadata_with_limits(
                        &source,
                        reference,
                        DataOpenLimits::new(limit)?,
                    )?;
                    let accounted_bytes = opened.metrics().accounted_allocation_bytes();
                    let layout = opened.into_layout();
                    let header = layout.header();
                    self.data_layouts.insert(reference.id(), layout);
                    Ok((
                        ArtifactMetadata::Data(DataArtifactMetadata::new(
                            reference,
                            header.database_id(),
                            header.table_id(),
                            header.segment_id(),
                            header.catalog_generation(),
                            header.segment_kind(),
                            header.min_transaction_id(),
                            header.max_transaction_id(),
                            header.row_count(),
                        )),
                        accounted_bytes,
                    ))
                }
                ArtifactKind::Index => {
                    let data_id = self.index_data.get(&reference.id()).copied().ok_or(
                        FormatError::InvalidPublication {
                            detail: "index has no table-manifest DATA binding",
                        },
                    )?;
                    let data =
                        self.data_layouts
                            .get(&data_id)
                            .ok_or(FormatError::InvalidPublication {
                                detail: "index source DATA was not inspected first",
                            })?;
                    let remaining = allowance.remaining_bytes();
                    if remaining == 0 {
                        return Err(FormatError::IndexArtifactLimitExceeded {
                            field: "metadata-open accounted bytes",
                            actual: 1,
                            limit: 0,
                        });
                    }
                    let limit = remaining.min(MAX_INDEX_OPEN_METADATA_BYTES);
                    let opened = open_index_artifact_metadata_with_limits(
                        &source,
                        reference,
                        data,
                        IndexOpenLimits::new(limit)?,
                    )?;
                    let accounted_bytes = opened.metrics().accounted_allocation_bytes();
                    let header = opened.layout().header();
                    Ok((
                        ArtifactMetadata::Index(IndexArtifactMetadata::new(
                            reference,
                            header.database_id(),
                            header.table_id(),
                            header.segment_id(),
                            header.catalog_generation(),
                            header.data_artifact_id(),
                            *header.data_body_sha256(),
                        )),
                        accounted_bytes,
                    ))
                }
            }
        })();
        Ok(match result {
            Ok((metadata, accounted_bytes)) => {
                ArtifactInspection::present_accounted(metadata, accounted_bytes)
            }
            Err(error) => {
                if let Some(required) = metadata_budget_requirement(&error, allowance) {
                    return Err(allowance.exceeded(required));
                }
                ArtifactInspection::invalid(error.to_string())
            }
        })
    }
}

impl BodyVerification {
    fn identity_read_role(&self, reference: ArtifactRef, staged: bool) -> Option<IdentityReadRole> {
        match self {
            Self::All => Some(IdentityReadRole::New),
            Self::ChangedOnly(retained) if retained.contains(&reference) => {
                staged.then_some(IdentityReadRole::Retained)
            }
            Self::ChangedOnly(_) => Some(IdentityReadRole::New),
            Self::MetadataOnly => None,
        }
    }
}

impl ReachabilitySource for GenerationFileSource<'_> {
    fn read_database_manifest(
        &mut self,
        reference: DatabaseManifestRootRef,
        byte_budget: u64,
    ) -> ReachabilityResult<Option<Vec<u8>>> {
        self.read_bounded(
            &database_manifest_path(reference),
            byte_budget,
            crate::v6::ReachableNodeKind::DatabaseManifest,
        )
    }

    fn read_catalog(
        &mut self,
        reference: CatalogRef,
        byte_budget: u64,
    ) -> ReachabilityResult<Option<Vec<u8>>> {
        self.read_bounded(
            &catalog_path(reference),
            byte_budget,
            crate::v6::ReachableNodeKind::Catalog,
        )
    }

    fn read_table_manifest(
        &mut self,
        reference: TableManifestRef,
        byte_budget: u64,
    ) -> ReachabilityResult<Option<Vec<u8>>> {
        let bytes = self.read_bounded(
            &table_manifest_path(reference),
            byte_budget,
            crate::v6::ReachableNodeKind::TableManifest,
        )?;
        if let Some(bytes) = &bytes {
            let manifest = decode_table_manifest(bytes).map_err(|error| {
                ReachabilityError::source_failure(
                    crate::v6::ReachableNodeKind::TableManifest,
                    error.to_string(),
                )
            })?;
            for segment in manifest.segments() {
                if let Some(index) = segment.index_artifact() {
                    self.index_data
                        .insert(index.id(), segment.data_artifact().id());
                }
            }
        }
        Ok(bytes)
    }

    fn inspect_artifact(
        &mut self,
        reference: ArtifactRef,
        allowance: ReachabilityAllowance,
    ) -> ReachabilityResult<ArtifactInspection> {
        self.inspect(reference, allowance)
    }
}

fn metadata_budget_requirement(
    error: &FormatError,
    allowance: ReachabilityAllowance,
) -> Option<u64> {
    match error {
        FormatError::DataArtifactLimitExceeded {
            field: "metadata-open accounted bytes",
            actual,
            limit,
        }
        | FormatError::IndexArtifactLimitExceeded {
            field: "metadata-open accounted bytes",
            actual,
            limit,
        } if *limit == allowance.remaining_bytes() => Some(*actual),
        _ => None,
    }
}

pub(crate) fn database_manifest_path(reference: DatabaseManifestRootRef) -> PathBuf {
    PathBuf::from("manifests").join(format!(
        "database-{:016x}.mft",
        reference.generation().get()
    ))
}

pub(crate) fn table_manifest_path(reference: TableManifestRef) -> PathBuf {
    PathBuf::from("manifests")
        .join("tables")
        .join(reference.table_id().to_string())
        .join(format!(
            "table-{:016x}.mft",
            reference.manifest().generation().get()
        ))
}

pub(crate) fn catalog_path(reference: CatalogRef) -> PathBuf {
    PathBuf::from("catalog").join(format!("catalog-{:016x}.cat", reference.generation().get()))
}

pub(crate) fn wal_path(generation: crate::v6::WalGeneration) -> PathBuf {
    PathBuf::from("wal").join(format!("wal-{:016x}.log", generation.get()))
}

fn validate_body_identity(
    path: &Path,
    reference: ArtifactRef,
    role: Option<IdentityReadRole>,
) -> Result<(), FormatError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| publication_io("inspect artifact", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(FormatError::InvalidPublication {
            detail: "artifact path is not a regular file",
        });
    }
    if metadata.len() != reference.byte_length() || metadata.len() < COMMON_FOOTER_BYTES {
        return Err(FormatError::InvalidPublication {
            detail: "artifact length differs from manifest reference",
        });
    }
    let mut file = open_regular(path).map_err(|error| publication_io("open artifact", error))?;
    let mut remaining = metadata.len() - COMMON_FOOTER_BYTES;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    while remaining != 0 {
        let length = usize::try_from(remaining.min(HASH_BUFFER_BYTES as u64))
            .expect("bounded hash read length fits usize");
        file.read_exact(&mut buffer[..length])
            .map_err(|error| publication_io("hash artifact", error))?;
        digest.update(&buffer[..length]);
        if let Some(role) = role {
            record(
                match reference.kind() {
                    ArtifactKind::Data => DiagnosticEvent::DataIdentityRead,
                    ArtifactKind::Index => DiagnosticEvent::IndexIdentityRead,
                },
                length as u64,
            );
            record(
                match role {
                    IdentityReadRole::New => DiagnosticEvent::NewArtifactIdentityRead,
                    IdentityReadRole::Retained => DiagnosticEvent::RetainedArtifactIdentityRead,
                },
                length as u64,
            );
        }
        remaining -= length as u64;
    }
    if digest.finalize().as_slice() != reference.body_sha256() {
        return Err(match reference.kind() {
            ArtifactKind::Data => FormatError::DataArtifactChecksumMismatch {
                scope: "whole-file identity",
            },
            ArtifactKind::Index => FormatError::IndexArtifactChecksumMismatch {
                scope: "whole-file identity",
            },
        });
    }
    let end = file
        .seek(SeekFrom::End(0))
        .map_err(|error| publication_io("reinspect artifact", error))?;
    if end != metadata.len() {
        return Err(FormatError::InvalidPublication {
            detail: "artifact changed during inspection",
        });
    }
    Ok(())
}

pub(crate) fn scrub_body_identity(path: &Path, reference: ArtifactRef) -> FormatResult<u64> {
    validate_body_identity(path, reference, None)?;
    Ok(reference.byte_length() - COMMON_FOOTER_BYTES)
}

pub(super) fn validate_new_artifact_identity(
    path: &Path,
    reference: ArtifactRef,
) -> FormatResult<()> {
    validate_body_identity(path, reference, Some(IdentityReadRole::New))
}

fn open_regular(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    set_no_follow(&mut options);
    options.open(path)
}

#[cfg(unix)]
fn set_no_follow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(not(unix))]
fn set_no_follow(_options: &mut OpenOptions) {}

fn source_failure(
    node: crate::v6::ReachableNodeKind,
    operation: &'static str,
    error: std::io::Error,
) -> ReachabilityError {
    ReachabilityError::source_failure(node, format!("{operation}: {}", error.kind()))
}

fn publication_io(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::PublicationIo {
        operation,
        kind: error.kind(),
    }
}
