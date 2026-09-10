use super::super::manifest::codec::{
    finish_file, put_u16, put_u32, put_u64, read_array, read_u16, read_u32, read_u64, require_zero,
    validate_canonical_directory, validate_file_shell_with_limit, FOOTER_BYTES, FORMAT_MAJOR,
    FORMAT_MINOR, HEADER_BYTES,
};
use super::super::{
    CatalogGeneration, CatalogId, CatalogRootRef, DatabaseGeneration, DatabaseId,
    DatabaseManifestRootRef, FormatError, FormatResult, ManifestGeneration, ManifestId, SnapshotId,
};
use super::{
    SnapshotManifest, SnapshotMember, SnapshotMemberKind, MAX_SNAPSHOT_MANIFEST_BYTES,
    MAX_SNAPSHOT_MEMBERS, SNAPSHOT_MEMBER_BYTES,
};

const KIND: &str = "snapshot manifest";
const MAGIC: [u8; 8] = *b"RDX6SNP\0";
const COPY_COMPLETE: u64 = 1;

pub fn encode_snapshot_manifest(manifest: &SnapshotManifest) -> FormatResult<Vec<u8>> {
    let directory_length = manifest
        .members()
        .len()
        .checked_mul(SNAPSHOT_MEMBER_BYTES)
        .ok_or(FormatError::InvalidSnapshot {
            detail: "member directory length overflows",
        })?;
    let body_length =
        HEADER_BYTES
            .checked_add(directory_length)
            .ok_or(FormatError::InvalidSnapshot {
                detail: "snapshot manifest length overflows",
            })?;
    let file_length =
        body_length
            .checked_add(FOOTER_BYTES)
            .ok_or(FormatError::InvalidSnapshot {
                detail: "snapshot manifest length overflows",
            })?;
    if file_length > MAX_SNAPSHOT_MANIFEST_BYTES {
        return Err(FormatError::SnapshotLimitExceeded {
            field: "manifest bytes",
            actual: file_length as u64,
            limit: MAX_SNAPSHOT_MANIFEST_BYTES as u64,
        });
    }

    let mut output = vec![0_u8; body_length];
    output[..8].copy_from_slice(&MAGIC);
    put_u16(&mut output, 8, FORMAT_MAJOR);
    put_u16(&mut output, 10, FORMAT_MINOR);
    put_u32(&mut output, 12, HEADER_BYTES as u32);
    output[24..40].copy_from_slice(manifest.snapshot_id().as_bytes());
    output[40..56].copy_from_slice(manifest.database_id().as_bytes());
    put_u64(&mut output, 56, manifest.database_generation().get());
    output[64..80].copy_from_slice(manifest.database_manifest().id().as_bytes());
    output[80..96].copy_from_slice(manifest.catalog().id().as_bytes());
    put_u64(&mut output, 96, manifest.members().len() as u64);
    put_u64(&mut output, 104, HEADER_BYTES as u64);
    put_u64(&mut output, 112, directory_length as u64);
    put_u64(&mut output, 120, manifest.created_unix_ns());
    put_u64(&mut output, 128, COPY_COMPLETE);
    output[136..168].copy_from_slice(manifest.database_manifest().body_sha256());
    output[168..200].copy_from_slice(manifest.catalog().body_sha256());

    for (index, member) in manifest.members().iter().copied().enumerate() {
        let start = HEADER_BYTES + index * SNAPSHOT_MEMBER_BYTES;
        let entry = &mut output[start..start + SNAPSHOT_MEMBER_BYTES];
        put_u16(entry, 0, member.kind().tag());
        put_u16(entry, 2, member.format_version());
        put_u32(entry, 4, member.flags());
        entry[8..24].copy_from_slice(&member.id());
        put_u64(entry, 24, member.generation());
        put_u64(entry, 32, member.byte_length());
        entry[40..72].copy_from_slice(&member.body_sha256());
        put_u16(entry, 72, member.locator_shard());
        put_u16(entry, 74, member.locator_suffix().tag());
    }

    finish_file(&mut output);
    Ok(output)
}

pub fn decode_snapshot_manifest(bytes: &[u8]) -> FormatResult<SnapshotManifest> {
    validate_file_shell_with_limit(bytes, KIND, &MAGIC, MAX_SNAPSHOT_MANIFEST_BYTES).map_err(
        |error| match error {
            FormatError::ManifestLimitExceeded {
                field,
                actual,
                limit,
                ..
            } => FormatError::SnapshotLimitExceeded {
                field,
                actual,
                limit,
            },
            FormatError::ManifestChecksumMismatch { scope, .. } => {
                FormatError::SnapshotChecksumMismatch { scope }
            }
            FormatError::InvalidManifest { detail, .. } => FormatError::InvalidSnapshot { detail },
            other => other,
        },
    )?;
    if read_u64(bytes, 128) != COPY_COMPLETE {
        return invalid("COPY_COMPLETE is absent or unknown header flags are set");
    }
    require_zero(bytes, 200..248, KIND, "reserved header bytes are non-zero")
        .map_err(map_manifest_error)?;

    let member_count = read_u64(bytes, 96);
    let directory = validate_canonical_directory(
        bytes,
        KIND,
        member_count,
        MAX_SNAPSHOT_MEMBERS as u64,
        read_u64(bytes, 104),
        read_u64(bytes, 112),
        SNAPSHOT_MEMBER_BYTES as u64,
    )
    .map_err(map_manifest_error)?;
    let member_count = usize::try_from(member_count).map_err(|_| FormatError::InvalidSnapshot {
        detail: "member count does not fit this platform",
    })?;
    let mut members = Vec::new();
    members
        .try_reserve_exact(member_count)
        .map_err(|_| FormatError::SnapshotLimitExceeded {
            field: "member allocation",
            actual: member_count as u64,
            limit: MAX_SNAPSHOT_MEMBERS as u64,
        })?;
    let mut previous_key = None;
    for entry in bytes[directory].chunks_exact(SNAPSHOT_MEMBER_BYTES) {
        require_zero(entry, 76..96, KIND, "member reserved bytes are non-zero")
            .map_err(map_manifest_error)?;
        let member = SnapshotMember::from_persisted(
            SnapshotMemberKind::from_tag(read_u16(entry, 0))?,
            read_u16(entry, 2),
            read_u32(entry, 4),
            read_array(entry, 8),
            read_u64(entry, 24),
            read_u64(entry, 32),
            read_array(entry, 40),
            read_u16(entry, 72),
            read_u16(entry, 74),
        )?;
        let key = (member.kind(), member.id());
        if previous_key.is_some_and(|previous| previous >= key) {
            return invalid("members are not strictly sorted by kind and identity");
        }
        previous_key = Some(key);
        members.push(member);
    }

    let database_generation = DatabaseGeneration::new(read_u64(bytes, 56))?;
    let database_manifest = DatabaseManifestRootRef::new(
        ManifestId::from_bytes(read_array(bytes, 64))?,
        ManifestGeneration::new(database_generation.get())?,
        read_array(bytes, 136),
    );
    let catalog_member = members
        .iter()
        .find(|member| member.kind() == SnapshotMemberKind::Catalog)
        .ok_or(FormatError::InvalidSnapshot {
            detail: "snapshot has no catalog member",
        })?;
    let catalog = CatalogRootRef::new(
        CatalogId::from_bytes(read_array(bytes, 80))?,
        CatalogGeneration::new(catalog_member.generation())?,
        read_array(bytes, 168),
    );
    SnapshotManifest::new(
        SnapshotId::from_bytes(read_array(bytes, 24))?,
        DatabaseId::from_bytes(read_array(bytes, 40))?,
        database_generation,
        database_manifest,
        catalog,
        members,
        read_u64(bytes, 120),
    )
}

fn map_manifest_error(error: FormatError) -> FormatError {
    match error {
        FormatError::ManifestLimitExceeded {
            field,
            actual,
            limit,
            ..
        } => FormatError::SnapshotLimitExceeded {
            field,
            actual,
            limit,
        },
        FormatError::ManifestChecksumMismatch { scope, .. } => {
            FormatError::SnapshotChecksumMismatch { scope }
        }
        FormatError::InvalidManifest { detail, .. } => FormatError::InvalidSnapshot { detail },
        other => other,
    }
}

fn invalid<T>(detail: &'static str) -> FormatResult<T> {
    Err(FormatError::InvalidSnapshot { detail })
}
