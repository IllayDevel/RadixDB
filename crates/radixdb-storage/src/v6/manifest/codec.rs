use super::super::{
    ArtifactId, ArtifactKind, ArtifactRef, DatabaseGeneration, FormatError, FormatResult,
    MAX_MANIFEST_FILE_BYTES,
};
use radixdb_catalog::ObjectId;

pub(crate) const HEADER_BYTES: usize = 256;
pub(crate) const FOOTER_BYTES: usize = 48;
pub(super) const MAX_MANIFEST_BYTES: usize = MAX_MANIFEST_FILE_BYTES as usize;
pub(crate) const FORMAT_MAJOR: u16 = 6;
pub(crate) const FORMAT_MINOR: u16 = 0;
pub(super) const ARTIFACT_REF_BYTES: usize = 88;
const FOOTER_MAGIC: [u8; 8] = *b"RDX6END\0";

pub(super) fn validate_file_shell(
    bytes: &[u8],
    kind: &'static str,
    magic: &[u8; 8],
) -> FormatResult<()> {
    validate_file_shell_with_limit(bytes, kind, magic, MAX_MANIFEST_BYTES)
}

pub(crate) fn validate_file_shell_with_limit(
    bytes: &[u8],
    kind: &'static str,
    magic: &[u8; 8],
    maximum_bytes: usize,
) -> FormatResult<()> {
    if bytes.len() < HEADER_BYTES + FOOTER_BYTES {
        return Err(invalid(kind, "file is shorter than header plus footer"));
    }
    if bytes.len() > maximum_bytes {
        return Err(limit(
            kind,
            "file bytes",
            bytes.len() as u64,
            maximum_bytes as u64,
        ));
    }
    if &bytes[..8] != magic {
        return Err(invalid(kind, "magic mismatch"));
    }
    let major = read_u16(bytes, 8);
    let minor = read_u16(bytes, 10);
    if (major, minor) != (FORMAT_MAJOR, FORMAT_MINOR) {
        return Err(FormatError::UnsupportedFormatVersion {
            owner: kind,
            major,
            minor,
        });
    }
    if read_u32(bytes, 12) != HEADER_BYTES as u32 {
        return Err(invalid(kind, "header length is not 256"));
    }
    if read_u64(bytes, 16) != bytes.len() as u64 {
        return Err(invalid(kind, "header file length mismatch"));
    }
    if bytes[252..256].iter().any(|byte| *byte != 0) {
        return Err(invalid(kind, "reserved header bytes are non-zero"));
    }
    if read_u32(bytes, 248) != radixdb_core::crc32_ieee(&bytes[..248]) {
        return Err(FormatError::ManifestChecksumMismatch {
            kind,
            scope: "header",
        });
    }

    let footer = bytes.len() - FOOTER_BYTES;
    if bytes[footer..footer + 8] != FOOTER_MAGIC {
        return Err(invalid(kind, "footer magic mismatch"));
    }
    if read_u64(bytes, footer + 8) != bytes.len() as u64 {
        return Err(invalid(kind, "footer file length mismatch"));
    }
    if bytes[footer + 16..] != radixdb_core::sha256_digest(&bytes[..footer]) {
        return Err(FormatError::ManifestChecksumMismatch {
            kind,
            scope: "body SHA-256",
        });
    }
    Ok(())
}

pub(crate) fn finish_file(output: &mut Vec<u8>) {
    let file_length = output.len() + FOOTER_BYTES;
    put_u64(output, 16, file_length as u64);
    let header_crc = radixdb_core::crc32_ieee(&output[..248]);
    put_u32(output, 248, header_crc);
    let body_sha = radixdb_core::sha256_digest(output);
    output.extend_from_slice(&FOOTER_MAGIC);
    output.extend_from_slice(&(file_length as u64).to_le_bytes());
    output.extend_from_slice(&body_sha);
}

pub(crate) fn validate_canonical_directory(
    bytes: &[u8],
    kind: &'static str,
    count: u64,
    count_limit: u64,
    offset: u64,
    length: u64,
    entry_bytes: u64,
) -> FormatResult<std::ops::Range<usize>> {
    if count > count_limit {
        return Err(limit(kind, "directory count", count, count_limit));
    }
    let expected_length = count
        .checked_mul(entry_bytes)
        .ok_or_else(|| invalid(kind, "directory length multiplication overflow"))?;
    if length != expected_length {
        return Err(invalid(kind, "directory length does not match count"));
    }
    if count == 0 {
        if offset != 0 {
            return Err(invalid(kind, "empty directory has a non-zero offset"));
        }
        if bytes.len() != HEADER_BYTES + FOOTER_BYTES {
            return Err(invalid(kind, "empty manifest has trailing body bytes"));
        }
        return Ok(0..0);
    }
    if offset != HEADER_BYTES as u64 || !offset.is_multiple_of(8) {
        return Err(invalid(kind, "directory has a non-canonical offset"));
    }
    let end = offset
        .checked_add(length)
        .ok_or_else(|| invalid(kind, "directory range overflows"))?;
    if end != (bytes.len() - FOOTER_BYTES) as u64 {
        return Err(invalid(
            kind,
            "directory does not exactly own the file body",
        ));
    }
    let start = usize::try_from(offset)
        .map_err(|_| invalid(kind, "directory offset does not fit this platform"))?;
    let end = usize::try_from(end)
        .map_err(|_| invalid(kind, "directory end does not fit this platform"))?;
    Ok(start..end)
}

pub(super) fn encode_artifact_ref(output: &mut [u8], reference: ArtifactRef) {
    output[..16].copy_from_slice(reference.id().as_bytes());
    put_u16(output, 16, reference.kind().tag());
    put_u16(output, 18, reference.codec_version());
    put_u32(output, 20, 0);
    put_u64(output, 24, reference.creation_generation().get());
    put_u64(output, 32, reference.byte_length());
    output[40..72].copy_from_slice(reference.body_sha256());
    put_u16(output, 72, u16::from(reference.locator().shard()));
    put_u16(output, 74, reference.locator().suffix().tag());
}

pub(super) fn decode_artifact_ref(bytes: &[u8], owner: &'static str) -> FormatResult<ArtifactRef> {
    if bytes.len() != ARTIFACT_REF_BYTES {
        return Err(invalid(owner, "ArtifactRef width mismatch"));
    }
    require_zero(
        bytes,
        76..88,
        owner,
        "ArtifactRef reserved bytes are non-zero",
    )?;
    ArtifactRef::from_persisted(
        ArtifactId::from_bytes(read_array(bytes, 0))?,
        ArtifactKind::from_tag(read_u16(bytes, 16))?,
        read_u16(bytes, 18),
        read_u32(bytes, 20),
        DatabaseGeneration::new(read_u64(bytes, 24))?,
        read_u64(bytes, 32),
        read_array(bytes, 40),
        read_u16(bytes, 72),
        read_u16(bytes, 74),
    )
}

pub(super) fn decode_optional_artifact_ref(
    bytes: &[u8],
    owner: &'static str,
) -> FormatResult<Option<ArtifactRef>> {
    if bytes.iter().all(|byte| *byte == 0) {
        return Ok(None);
    }
    decode_artifact_ref(bytes, owner).map(Some)
}

pub(super) fn decode_user_object_id(
    bytes: [u8; 16],
    kind: &'static str,
    detail: &'static str,
) -> FormatResult<ObjectId> {
    ObjectId::from_user_bytes(bytes).map_err(|_| invalid(kind, detail))
}

pub(crate) fn require_zero(
    bytes: &[u8],
    range: std::ops::Range<usize>,
    kind: &'static str,
    detail: &'static str,
) -> FormatResult<()> {
    if bytes[range].iter().any(|byte| *byte != 0) {
        return Err(invalid(kind, detail));
    }
    Ok(())
}

pub(crate) const fn invalid(kind: &'static str, detail: &'static str) -> FormatError {
    FormatError::InvalidManifest { kind, detail }
}

pub(crate) const fn limit(
    kind: &'static str,
    field: &'static str,
    actual: u64,
    limit: u64,
) -> FormatError {
    FormatError::ManifestLimitExceeded {
        kind,
        field,
        actual,
        limit,
    }
}

pub(crate) fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> [u8; N] {
    bytes[offset..offset + N]
        .try_into()
        .expect("manifest range was validated before fixed-width read")
}

pub(crate) fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(read_array(bytes, offset))
}

pub(crate) fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(read_array(bytes, offset))
}

pub(crate) fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(read_array(bytes, offset))
}

pub(crate) fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
