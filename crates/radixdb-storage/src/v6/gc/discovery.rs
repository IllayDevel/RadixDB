use std::fs::{DirEntry, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;
use std::time::UNIX_EPOCH;

use crate::v6::{
    ArtifactCleanupLimits, ArtifactId, ArtifactKind, ArtifactRef, CatalogGeneration, CatalogId,
    CatalogRef, CleanupGeneration, DatabaseGeneration, FormatError, FormatResult,
    ImmutableMemberRef, ManifestGeneration, ManifestId, ManifestKind, ManifestRef,
    TableManifestRef, MAX_ARTIFACT_FILE_BYTES, MAX_CATALOG_FILE_BYTES, MAX_MANIFEST_FILE_BYTES,
};
use radixdb_catalog::ObjectId;

const ARTIFACT_HEADER_BYTES: usize = 256;
const ARTIFACT_FOOTER_BYTES: u64 = 48;
const FOOTER_MAGIC: [u8; 8] = *b"RDX6END\0";
const DATA_MAGIC: [u8; 8] = *b"RDX6DAT\0";
const INDEX_MAGIC: [u8; 8] = *b"RDX6IDX\0";
const CATALOG_MAGIC: [u8; 8] = *b"RDX6CAT\0";
const DATABASE_MANIFEST_MAGIC: [u8; 8] = *b"RDX6DBM\0";
const TABLE_MANIFEST_MAGIC: [u8; 8] = *b"RDX6TBM\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiscoveredLocation {
    Final,
    Quarantine(CleanupGeneration),
}

#[derive(Debug, Clone)]
pub(crate) struct DiscoveredMember {
    pub(crate) reference: ImmutableMemberRef,
    pub(crate) path: PathBuf,
    pub(crate) relative_path: PathBuf,
    pub(crate) modified_unix_ns: u64,
    pub(crate) location: DiscoveredLocation,
}

#[derive(Debug)]
pub(crate) struct ArtifactDiscovery {
    pub(crate) final_artifacts: Vec<DiscoveredMember>,
    pub(crate) quarantined_artifacts: Vec<DiscoveredMember>,
    pub(crate) quarantine_generations: Vec<CleanupGeneration>,
}

pub(crate) fn discover_artifacts(
    root: &Path,
    limits: ArtifactCleanupLimits,
    started: Instant,
) -> FormatResult<ArtifactDiscovery> {
    validate_real_directory(root, "inspect database root")?;
    let mut budget = DiscoveryBudget::new(limits, started);
    let final_artifacts = discover_final(root, &mut budget)?;
    let (quarantined_artifacts, quarantine_generations) = discover_quarantine(root, &mut budget)?;
    Ok(ArtifactDiscovery {
        final_artifacts,
        quarantined_artifacts,
        quarantine_generations,
    })
}

fn discover_final(
    root: &Path,
    budget: &mut DiscoveryBudget,
) -> FormatResult<Vec<DiscoveredMember>> {
    let mut artifacts = Vec::new();
    let artifacts_root = root.join("artifacts");
    if exists(&artifacts_root, "inspect artifacts root")? {
        validate_real_directory(&artifacts_root, "inspect artifacts root")?;
        let entries = sorted_entries(&artifacts_root, "enumerate artifacts root")?;
        for entry in &entries {
            let name = entry_name(entry)?;
            if name != "data" && name != "index" {
                return invalid("artifact root contains an unknown entry");
            }
        }
        for (directory, kind) in [("data", ArtifactKind::Data), ("index", ArtifactKind::Index)] {
            let kind_root = artifacts_root.join(directory);
            if exists(&kind_root, "inspect artifact-kind directory")? {
                discover_kind(
                    &kind_root,
                    kind,
                    DiscoveredLocation::Final,
                    budget,
                    &mut artifacts,
                )?;
            }
        }
    }
    discover_catalog(root, DiscoveredLocation::Final, budget, &mut artifacts)?;
    discover_manifests(root, DiscoveredLocation::Final, budget, &mut artifacts)?;
    artifacts.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(artifacts)
}

fn discover_quarantine(
    root: &Path,
    budget: &mut DiscoveryBudget,
) -> FormatResult<(Vec<DiscoveredMember>, Vec<CleanupGeneration>)> {
    let quarantine_root = root.join("quarantine");
    if !exists(&quarantine_root, "inspect quarantine root")? {
        return Ok((Vec::new(), Vec::new()));
    }
    validate_real_directory(&quarantine_root, "inspect quarantine root")?;
    let mut artifacts = Vec::new();
    let mut generations = Vec::new();
    for entry in sorted_entries(&quarantine_root, "enumerate quarantine root")? {
        let name = entry_name(&entry)?;
        if matches!(
            name.as_str(),
            "CYCLE.0" | "CYCLE.1" | "CYCLE.0.pending" | "CYCLE.1.pending"
        ) {
            validate_small_regular(&entry.path(), "inspect cleanup cycle record")?;
            continue;
        }
        let Some(raw_generation) = name.strip_prefix("q-") else {
            return invalid("quarantine root contains an unknown entry");
        };
        let generation = parse_generation(raw_generation)?;
        validate_real_directory(&entry.path(), "inspect quarantine generation")?;
        let root_entries = sorted_entries(&entry.path(), "enumerate quarantine generation")?;
        for root_entry in &root_entries {
            if !matches!(
                entry_name(root_entry)?.as_str(),
                "artifacts" | "catalog" | "manifests"
            ) {
                return invalid("quarantine generation has an unknown entry");
            }
        }
        let member_root = entry.path().join("artifacts");
        if exists(&member_root, "inspect quarantined artifacts")? {
            validate_real_directory(&member_root, "inspect quarantined artifacts")?;
            let kind_entries = sorted_entries(&member_root, "enumerate quarantined artifacts")?;
            for kind_entry in &kind_entries {
                let name = entry_name(kind_entry)?;
                if name != "data" && name != "index" {
                    return invalid("quarantined artifact root contains an unknown entry");
                }
            }
            for (directory, kind) in [("data", ArtifactKind::Data), ("index", ArtifactKind::Index)]
            {
                let kind_root = member_root.join(directory);
                if exists(&kind_root, "inspect quarantined artifact-kind directory")? {
                    discover_kind(
                        &kind_root,
                        kind,
                        DiscoveredLocation::Quarantine(generation),
                        budget,
                        &mut artifacts,
                    )?;
                }
            }
        }
        discover_catalog(
            &entry.path(),
            DiscoveredLocation::Quarantine(generation),
            budget,
            &mut artifacts,
        )?;
        discover_manifests(
            &entry.path(),
            DiscoveredLocation::Quarantine(generation),
            budget,
            &mut artifacts,
        )?;
        generations.push(generation);
    }
    generations.sort_unstable();
    if generations.windows(2).any(|pair| pair[0] == pair[1]) {
        return invalid("duplicate quarantine generation");
    }
    artifacts.sort_by(|left, right| left.path.cmp(&right.path));
    Ok((artifacts, generations))
}

fn discover_kind(
    kind_root: &Path,
    kind: ArtifactKind,
    location: DiscoveredLocation,
    budget: &mut DiscoveryBudget,
    output: &mut Vec<DiscoveredMember>,
) -> FormatResult<()> {
    validate_real_directory(kind_root, "inspect artifact-kind directory")?;
    for shard_entry in sorted_entries(kind_root, "enumerate artifact shards")? {
        let shard_name = entry_name(&shard_entry)?;
        let shard = parse_shard(&shard_name)?;
        validate_real_directory(&shard_entry.path(), "inspect artifact shard")?;
        for file_entry in sorted_entries(&shard_entry.path(), "enumerate artifact shard")? {
            let path = file_entry.path();
            let file_name = entry_name(&file_entry)?;
            let id = parse_artifact_name(&file_name, kind)?;
            if id.as_bytes()[0] != shard {
                return invalid("artifact filename is stored under the wrong shard");
            }
            let metadata = std::fs::symlink_metadata(&path)
                .map_err(|error| io_error("inspect artifact candidate", error))?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return invalid("artifact candidate is not a regular file");
            }
            let relative_path = kind_relative_path(kind, &shard_name, &file_name);
            budget.account(&relative_path, metadata.len())?;
            budget.check_time()?;
            let (reference, modified_unix_ns) = inspect_reference(&path, id, kind, metadata.len())?;
            if reference.relative_path() != relative_path {
                return invalid("artifact identity differs from its canonical locator");
            }
            output.push(DiscoveredMember {
                reference: ImmutableMemberRef::Artifact(reference),
                path,
                relative_path,
                modified_unix_ns,
                location,
            });
        }
    }
    Ok(())
}

fn discover_catalog(
    root: &Path,
    location: DiscoveredLocation,
    budget: &mut DiscoveryBudget,
    output: &mut Vec<DiscoveredMember>,
) -> FormatResult<()> {
    let catalog_root = root.join("catalog");
    if !exists(&catalog_root, "inspect catalog cleanup root")? {
        return Ok(());
    }
    validate_real_directory(&catalog_root, "inspect catalog cleanup root")?;
    for entry in sorted_entries(&catalog_root, "enumerate catalog cleanup root")? {
        let path = entry.path();
        let name = entry_name(&entry)?;
        let generation = parse_prefixed_generation(&name, "catalog-", ".cat")?;
        let relative_path = PathBuf::from("catalog").join(&name);
        let metadata = inspect_regular_candidate(&path, "inspect catalog candidate")?;
        budget.account(&relative_path, metadata.len())?;
        budget.check_time()?;
        let (reference, modified_unix_ns) = inspect_catalog_reference(
            &path,
            CatalogGeneration::new(generation.get())?,
            metadata.len(),
        )?;
        let reference = ImmutableMemberRef::Catalog(reference);
        if reference.relative_path() != relative_path {
            return invalid("catalog identity differs from its canonical locator");
        }
        output.push(DiscoveredMember {
            reference,
            path,
            relative_path,
            modified_unix_ns,
            location,
        });
    }
    Ok(())
}

fn discover_manifests(
    root: &Path,
    location: DiscoveredLocation,
    budget: &mut DiscoveryBudget,
    output: &mut Vec<DiscoveredMember>,
) -> FormatResult<()> {
    let manifests_root = root.join("manifests");
    if !exists(&manifests_root, "inspect manifest cleanup root")? {
        return Ok(());
    }
    validate_real_directory(&manifests_root, "inspect manifest cleanup root")?;
    for entry in sorted_entries(&manifests_root, "enumerate manifest cleanup root")? {
        let path = entry.path();
        let name = entry_name(&entry)?;
        if name == "tables" {
            discover_table_manifests(&path, location, budget, output)?;
            continue;
        }
        let generation = parse_prefixed_generation(&name, "database-", ".mft")?;
        let relative_path = PathBuf::from("manifests").join(&name);
        let metadata = inspect_regular_candidate(&path, "inspect database-manifest candidate")?;
        budget.account(&relative_path, metadata.len())?;
        budget.check_time()?;
        let (reference, modified_unix_ns) =
            inspect_database_manifest_reference(&path, generation, metadata.len())?;
        if reference.relative_path() != relative_path {
            return invalid("database manifest identity differs from its canonical locator");
        }
        output.push(DiscoveredMember {
            reference,
            path,
            relative_path,
            modified_unix_ns,
            location,
        });
    }
    Ok(())
}

fn discover_table_manifests(
    tables_root: &Path,
    location: DiscoveredLocation,
    budget: &mut DiscoveryBudget,
    output: &mut Vec<DiscoveredMember>,
) -> FormatResult<()> {
    validate_real_directory(tables_root, "inspect table-manifest cleanup root")?;
    for table_entry in sorted_entries(tables_root, "enumerate table-manifest cleanup root")? {
        let table_name = entry_name(&table_entry)?;
        require_lower_hex(&table_name, 32, "table-manifest directory is not canonical")?;
        let table_id =
            ObjectId::from_str(&table_name).map_err(|_| FormatError::InvalidCleanup {
                detail: "table-manifest directory has an invalid object identity",
            })?;
        validate_real_directory(
            &table_entry.path(),
            "inspect table-manifest table directory",
        )?;
        for entry in sorted_entries(&table_entry.path(), "enumerate table-manifest table")? {
            let path = entry.path();
            let name = entry_name(&entry)?;
            let generation = parse_prefixed_generation(&name, "table-", ".mft")?;
            let relative_path = PathBuf::from("manifests")
                .join("tables")
                .join(&table_name)
                .join(&name);
            let metadata = inspect_regular_candidate(&path, "inspect table-manifest candidate")?;
            budget.account(&relative_path, metadata.len())?;
            budget.check_time()?;
            let (reference, modified_unix_ns) =
                inspect_table_manifest_reference(&path, table_id, generation, metadata.len())?;
            if reference.relative_path() != relative_path {
                return invalid("table manifest identity differs from its canonical locator");
            }
            output.push(DiscoveredMember {
                reference,
                path,
                relative_path,
                modified_unix_ns,
                location,
            });
        }
    }
    Ok(())
}

fn inspect_catalog_reference(
    path: &Path,
    expected_generation: CatalogGeneration,
    file_length: u64,
) -> FormatResult<(CatalogRef, u64)> {
    let shell = inspect_common_shell(
        path,
        file_length,
        MAX_CATALOG_FILE_BYTES,
        CATALOG_MAGIC,
        radixdb_catalog::LATEST_CATALOG_MINOR,
        "catalog",
    )?;
    let id = CatalogId::from_bytes(read_array(&shell.header, 40))?;
    let generation = CatalogGeneration::new(read_u64(&shell.header, 56))?;
    if generation != expected_generation {
        return invalid("catalog header generation differs from its filename");
    }
    Ok((
        CatalogRef::new(id, generation, file_length, shell.body_sha256)?,
        shell.modified_unix_ns,
    ))
}

fn inspect_database_manifest_reference(
    path: &Path,
    expected_generation: ManifestGeneration,
    file_length: u64,
) -> FormatResult<(ImmutableMemberRef, u64)> {
    let shell = inspect_common_shell(
        path,
        file_length,
        MAX_MANIFEST_FILE_BYTES,
        DATABASE_MANIFEST_MAGIC,
        0,
        "database manifest",
    )?;
    let id = ManifestId::from_bytes(read_array(&shell.header, 40))?;
    let generation = ManifestGeneration::new(read_u64(&shell.header, 56))?;
    if generation != expected_generation {
        return invalid("database-manifest generation differs from its filename");
    }
    Ok((
        ImmutableMemberRef::DatabaseManifest(ManifestRef::new(
            id,
            ManifestKind::Database,
            generation,
            file_length,
            shell.body_sha256,
        )?),
        shell.modified_unix_ns,
    ))
}

fn inspect_table_manifest_reference(
    path: &Path,
    expected_table_id: ObjectId,
    expected_generation: ManifestGeneration,
    file_length: u64,
) -> FormatResult<(ImmutableMemberRef, u64)> {
    let shell = inspect_common_shell(
        path,
        file_length,
        MAX_MANIFEST_FILE_BYTES,
        TABLE_MANIFEST_MAGIC,
        0,
        "table manifest",
    )?;
    let table_id = ObjectId::from_bytes(read_array(&shell.header, 40)).map_err(|_| {
        FormatError::InvalidCleanup {
            detail: "table-manifest header has an invalid table identity",
        }
    })?;
    let id = ManifestId::from_bytes(read_array(&shell.header, 56))?;
    let generation = ManifestGeneration::new(read_u64(&shell.header, 72))?;
    if table_id != expected_table_id || generation != expected_generation {
        return invalid("table-manifest header differs from its canonical locator");
    }
    let reference = ManifestRef::new(
        id,
        ManifestKind::Table,
        generation,
        file_length,
        shell.body_sha256,
    )?;
    Ok((
        ImmutableMemberRef::TableManifest(TableManifestRef::new(table_id, reference)?),
        shell.modified_unix_ns,
    ))
}

struct CommonShell {
    header: [u8; ARTIFACT_HEADER_BYTES],
    body_sha256: [u8; 32],
    modified_unix_ns: u64,
}

fn inspect_common_shell(
    path: &Path,
    file_length: u64,
    maximum_bytes: u64,
    expected_magic: [u8; 8],
    maximum_format_minor: u16,
    owner: &'static str,
) -> FormatResult<CommonShell> {
    if file_length < ARTIFACT_HEADER_BYTES as u64 + ARTIFACT_FOOTER_BYTES
        || file_length > maximum_bytes
    {
        return invalid("immutable member length is outside format bounds");
    }
    let mut file = open_regular(path, "open immutable member candidate")?;
    let opened_metadata = file
        .metadata()
        .map_err(|error| io_error("inspect opened immutable member", error))?;
    if opened_metadata.len() != file_length {
        return invalid("immutable member changed before open");
    }
    let mut header = [0_u8; ARTIFACT_HEADER_BYTES];
    file.read_exact(&mut header)
        .map_err(|error| io_error("read immutable member header", error))?;
    if header[..8] != expected_magic
        || read_u16(&header, 8) != 6
        || read_u16(&header, 10) > maximum_format_minor
        || read_u32(&header, 12) != ARTIFACT_HEADER_BYTES as u32
        || read_u64(&header, 16) != file_length
    {
        return invalid("immutable member has an invalid fixed header");
    }
    if read_u32(&header, 248) != radixdb_core::crc32_ieee(&header[..248]) {
        return Err(match owner {
            "catalog" => FormatError::InvalidCleanup {
                detail: "catalog candidate header checksum mismatch",
            },
            _ => FormatError::InvalidCleanup {
                detail: "manifest candidate header checksum mismatch",
            },
        });
    }
    file.seek(SeekFrom::Start(file_length - ARTIFACT_FOOTER_BYTES))
        .map_err(|error| io_error("seek immutable member footer", error))?;
    let mut footer = [0_u8; ARTIFACT_FOOTER_BYTES as usize];
    file.read_exact(&mut footer)
        .map_err(|error| io_error("read immutable member footer", error))?;
    if footer[..8] != FOOTER_MAGIC || read_u64(&footer, 8) != file_length {
        return invalid("immutable member has an invalid footer");
    }
    if file
        .seek(SeekFrom::End(0))
        .map_err(|error| io_error("reinspect immutable member", error))?
        != file_length
    {
        return invalid("immutable member changed during inspection");
    }
    let path_metadata = std::fs::symlink_metadata(path)
        .map_err(|error| io_error("reinspect immutable member path", error))?;
    if path_metadata.file_type().is_symlink()
        || !path_metadata.is_file()
        || !same_file(&opened_metadata, &path_metadata)
    {
        return invalid("immutable member path changed during inspection");
    }
    Ok(CommonShell {
        header,
        body_sha256: read_array(&footer, 16),
        modified_unix_ns: modified_unix_ns(&opened_metadata),
    })
}

fn inspect_regular_candidate(
    path: &Path,
    operation: &'static str,
) -> FormatResult<std::fs::Metadata> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| io_error(operation, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return invalid("immutable member candidate is not a regular file");
    }
    Ok(metadata)
}

fn inspect_reference(
    path: &Path,
    expected_id: ArtifactId,
    kind: ArtifactKind,
    file_length: u64,
) -> FormatResult<(ArtifactRef, u64)> {
    if file_length < ARTIFACT_HEADER_BYTES as u64 + ARTIFACT_FOOTER_BYTES
        || file_length > MAX_ARTIFACT_FILE_BYTES
    {
        return invalid("artifact candidate length is outside format bounds");
    }
    let mut file = open_regular(path, "open artifact candidate")?;
    let opened_metadata = file
        .metadata()
        .map_err(|error| io_error("inspect opened artifact candidate", error))?;
    if opened_metadata.len() != file_length {
        return invalid("artifact candidate changed before open");
    }
    let mut header = [0_u8; ARTIFACT_HEADER_BYTES];
    file.read_exact(&mut header)
        .map_err(|error| io_error("read artifact header", error))?;
    let expected_magic = match kind {
        ArtifactKind::Data => DATA_MAGIC,
        ArtifactKind::Index => INDEX_MAGIC,
    };
    if header[..8] != expected_magic
        || read_u16(&header, 8) != 6
        || read_u16(&header, 10) != 0
        || read_u32(&header, 12) != ARTIFACT_HEADER_BYTES as u32
        || read_u64(&header, 16) != file_length
    {
        return invalid("artifact candidate has an invalid fixed header");
    }
    if read_u32(&header, 248) != radixdb_core::crc32_ieee(&header[..248]) {
        return invalid("artifact candidate header checksum mismatch");
    }
    let header_id = ArtifactId::from_bytes(read_array(&header, 24))?;
    if header_id != expected_id {
        return invalid("artifact header ID differs from its filename");
    }
    let generation_offset = match kind {
        ArtifactKind::Data => 88,
        ArtifactKind::Index => 136,
    };
    let creation_generation = DatabaseGeneration::new(read_u64(&header, generation_offset))?;
    file.seek(SeekFrom::Start(file_length - ARTIFACT_FOOTER_BYTES))
        .map_err(|error| io_error("seek artifact footer", error))?;
    let mut footer = [0_u8; ARTIFACT_FOOTER_BYTES as usize];
    file.read_exact(&mut footer)
        .map_err(|error| io_error("read artifact footer", error))?;
    if footer[..8] != FOOTER_MAGIC || read_u64(&footer, 8) != file_length {
        return invalid("artifact candidate has an invalid footer");
    }
    let body_sha = read_array(&footer, 16);
    if file
        .seek(SeekFrom::End(0))
        .map_err(|error| io_error("reinspect artifact candidate", error))?
        != file_length
    {
        return invalid("artifact candidate changed during inspection");
    }
    let path_metadata = std::fs::symlink_metadata(path)
        .map_err(|error| io_error("reinspect artifact candidate path", error))?;
    if path_metadata.file_type().is_symlink()
        || !path_metadata.is_file()
        || !same_file(&opened_metadata, &path_metadata)
    {
        return invalid("artifact candidate path changed during inspection");
    }
    let modified_unix_ns = opened_metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let reference = ArtifactRef::new(
        expected_id,
        kind,
        creation_generation,
        file_length,
        body_sha,
    )?;
    Ok((reference, modified_unix_ns))
}

struct DiscoveryBudget {
    limits: ArtifactCleanupLimits,
    started: Instant,
    files: u64,
    bytes: u64,
}

impl DiscoveryBudget {
    const fn new(limits: ArtifactCleanupLimits, started: Instant) -> Self {
        Self {
            limits,
            started,
            files: 0,
            bytes: 0,
        }
    }

    fn account(&mut self, relative_path: &Path, file_bytes: u64) -> FormatResult<()> {
        self.check_time()?;
        self.files = self
            .files
            .checked_add(1)
            .ok_or(FormatError::InvalidCleanup {
                detail: "cleanup file accounting overflow",
            })?;
        if self.files > self.limits.max_files() {
            return Err(FormatError::CleanupLimitExceeded {
                field: "file count",
                actual: self.files,
                limit: self.limits.max_files(),
            });
        }
        let path_bytes = relative_path.as_os_str().len() as u64;
        self.bytes = self
            .bytes
            .checked_add(path_bytes)
            .and_then(|bytes| bytes.checked_add(file_bytes))
            .ok_or(FormatError::InvalidCleanup {
                detail: "cleanup byte accounting overflow",
            })?;
        if self.bytes > self.limits.max_accounted_bytes() {
            return Err(FormatError::CleanupLimitExceeded {
                field: "accounted bytes",
                actual: self.bytes,
                limit: self.limits.max_accounted_bytes(),
            });
        }
        Ok(())
    }

    fn check_time(&self) -> FormatResult<()> {
        if self.started.elapsed() >= self.limits.max_wall_time() {
            return Err(FormatError::CleanupLimitExceeded {
                field: "wall-time nanoseconds",
                actual: u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                limit: u64::try_from(self.limits.max_wall_time().as_nanos()).unwrap_or(u64::MAX),
            });
        }
        Ok(())
    }
}

fn kind_relative_path(kind: ArtifactKind, shard: &str, file_name: &str) -> PathBuf {
    PathBuf::from("artifacts")
        .join(match kind {
            ArtifactKind::Data => "data",
            ArtifactKind::Index => "index",
        })
        .join(shard)
        .join(file_name)
}

fn parse_artifact_name(name: &str, kind: ArtifactKind) -> FormatResult<ArtifactId> {
    let suffix = match kind {
        ArtifactKind::Data => ".data",
        ArtifactKind::Index => ".idx",
    };
    let Some(raw_id) = name.strip_suffix(suffix) else {
        return invalid("artifact filename has a non-canonical suffix");
    };
    ArtifactId::from_str(raw_id)
}

fn parse_shard(value: &str) -> FormatResult<u8> {
    if value.len() != 2
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return invalid("artifact shard is not two lowercase hexadecimal digits");
    }
    u8::from_str_radix(value, 16).map_err(|_| FormatError::InvalidCleanup {
        detail: "artifact shard is invalid",
    })
}

fn parse_generation(value: &str) -> FormatResult<CleanupGeneration> {
    if value.len() != 16
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return invalid("quarantine generation is not 16 lowercase hexadecimal digits");
    }
    let value = u64::from_str_radix(value, 16).map_err(|_| FormatError::InvalidCleanup {
        detail: "quarantine generation is invalid",
    })?;
    CleanupGeneration::new(value)
}

fn parse_prefixed_generation(
    value: &str,
    prefix: &'static str,
    suffix: &'static str,
) -> FormatResult<ManifestGeneration> {
    let raw = value
        .strip_prefix(prefix)
        .and_then(|value| value.strip_suffix(suffix))
        .ok_or(FormatError::InvalidCleanup {
            detail: "immutable member filename is not canonical",
        })?;
    require_lower_hex(raw, 16, "immutable member generation is not canonical")?;
    let generation = u64::from_str_radix(raw, 16).map_err(|_| FormatError::InvalidCleanup {
        detail: "immutable member generation is invalid",
    })?;
    ManifestGeneration::new(generation)
}

fn require_lower_hex(value: &str, length: usize, detail: &'static str) -> FormatResult<()> {
    if value.len() != length
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return invalid(detail);
    }
    Ok(())
}

fn modified_unix_ns(metadata: &std::fs::Metadata) -> u64 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn sorted_entries(path: &Path, operation: &'static str) -> FormatResult<Vec<DirEntry>> {
    let mut entries = std::fs::read_dir(path)
        .map_err(|error| io_error(operation, error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| io_error(operation, error))?;
    entries.sort_by_key(DirEntry::file_name);
    Ok(entries)
}

fn entry_name(entry: &DirEntry) -> FormatResult<String> {
    entry
        .file_name()
        .into_string()
        .map_err(|_| FormatError::InvalidCleanup {
            detail: "cleanup path is not valid UTF-8",
        })
}

fn exists(path: &Path, operation: &'static str) -> FormatResult<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error(operation, error)),
    }
}

fn validate_real_directory(path: &Path, operation: &'static str) -> FormatResult<()> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| io_error(operation, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return invalid("cleanup path is not a real directory");
    }
    Ok(())
}

fn validate_small_regular(path: &Path, operation: &'static str) -> FormatResult<()> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| io_error(operation, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 128 {
        return invalid("cleanup cycle record is not a bounded regular file");
    }
    Ok(())
}

fn open_regular(path: &Path, operation: &'static str) -> FormatResult<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    set_no_follow(&mut options);
    options
        .open(path)
        .map_err(|error| io_error(operation, error))
}

#[cfg(unix)]
fn same_file(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.len() == right.len()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
}

#[cfg(not(unix))]
fn same_file(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    left.len() == right.len() && left.modified().ok() == right.modified().ok()
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

fn invalid<T>(detail: &'static str) -> FormatResult<T> {
    Err(FormatError::InvalidCleanup { detail })
}

fn io_error(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::CleanupIo {
        operation,
        kind: error.kind(),
    }
}
