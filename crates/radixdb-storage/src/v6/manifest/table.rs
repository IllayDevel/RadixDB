use super::super::{
    fault::reach_generation_boundary, CatalogGeneration, DatabaseId, FormatError, FormatResult,
    GenerationCrashPoint, ManifestGeneration, ManifestId, SegmentId,
};
use super::codec::*;
use super::model::{
    SegmentDescriptor, SegmentKind, SegmentTier, TableManifest, MAX_SEGMENTS_PER_TABLE_MANIFEST,
};

const KIND: &str = "table manifest";
const MAGIC: [u8; 8] = *b"RDX6TBM\0";
const SEGMENT_ENTRY_BYTES: usize = 240;
const SEGMENT_DESCRIPTOR_VERSION: u16 = 1;
const SEGMENT_FLAG_L1: u32 = 1 << 0;
const SEGMENT_KNOWN_FLAGS: u32 = SEGMENT_FLAG_L1;

pub fn encode_table_manifest(manifest: &TableManifest) -> FormatResult<Vec<u8>> {
    let directory_length = manifest
        .segments()
        .len()
        .checked_mul(SEGMENT_ENTRY_BYTES)
        .ok_or_else(|| invalid(KIND, "segment directory length overflows"))?;
    let body_length = HEADER_BYTES
        .checked_add(directory_length)
        .ok_or_else(|| invalid(KIND, "file length overflows"))?;
    let file_length = body_length
        .checked_add(FOOTER_BYTES)
        .ok_or_else(|| invalid(KIND, "file length overflows"))?;
    if file_length > MAX_MANIFEST_BYTES {
        return Err(limit(
            KIND,
            "file bytes",
            file_length as u64,
            MAX_MANIFEST_BYTES as u64,
        ));
    }

    let mut output = vec![0_u8; body_length];
    output[..8].copy_from_slice(&MAGIC);
    put_u16(&mut output, 8, FORMAT_MAJOR);
    put_u16(&mut output, 10, FORMAT_MINOR);
    put_u32(&mut output, 12, HEADER_BYTES as u32);
    output[24..40].copy_from_slice(manifest.database_id().as_bytes());
    output[40..56].copy_from_slice(manifest.table_id().as_bytes());
    output[56..72].copy_from_slice(manifest.manifest_id().as_bytes());
    put_u64(&mut output, 72, manifest.generation().get());
    put_u64(&mut output, 80, manifest.catalog_generation().get());
    put_u64(&mut output, 88, manifest.row_id_high_water());
    put_u64(&mut output, 96, manifest.next_segment_sequence());
    put_u64(&mut output, 104, manifest.segments().len() as u64);
    if !manifest.segments().is_empty() {
        put_u64(&mut output, 112, HEADER_BYTES as u64);
    }
    put_u64(&mut output, 120, directory_length as u64);
    put_u64(&mut output, 136, manifest.created_unix_ns());

    for (index, segment) in manifest.segments().iter().enumerate() {
        let start = HEADER_BYTES + index * SEGMENT_ENTRY_BYTES;
        let entry = &mut output[start..start + SEGMENT_ENTRY_BYTES];
        entry[..16].copy_from_slice(segment.id().as_bytes());
        put_u16(entry, 16, segment.kind().tag());
        put_u16(entry, 18, SEGMENT_DESCRIPTOR_VERSION);
        let flags = match segment.tier() {
            SegmentTier::L0 => 0,
            SegmentTier::L1 => SEGMENT_FLAG_L1,
        };
        put_u32(entry, 20, flags);
        put_u64(entry, 24, segment.min_transaction_id());
        put_u64(entry, 32, segment.max_transaction_id());
        put_u64(entry, 40, segment.row_count());
        put_u64(entry, 48, segment.first_row_id());
        put_u64(entry, 56, segment.last_row_id());
        encode_artifact_ref(&mut entry[64..152], segment.data_artifact());
        if let Some(index_artifact) = segment.index_artifact() {
            encode_artifact_ref(&mut entry[152..240], index_artifact);
        }
    }

    reach_generation_boundary(GenerationCrashPoint::TableManifestAfterBodyBeforeFooter).map_err(
        |error| FormatError::PublicationIo {
            operation: "inject after table-manifest body",
            kind: error.kind(),
        },
    )?;
    finish_file(&mut output);
    Ok(output)
}

pub fn decode_table_manifest(bytes: &[u8]) -> FormatResult<TableManifest> {
    validate_file_shell(bytes, KIND, &MAGIC)?;
    if read_u64(bytes, 128) != 0 {
        return Err(invalid(KIND, "unknown header flags"));
    }
    require_zero(bytes, 144..248, KIND, "reserved header bytes are non-zero")?;

    let segment_count = read_u64(bytes, 104);
    let directory = validate_canonical_directory(
        bytes,
        KIND,
        segment_count,
        MAX_SEGMENTS_PER_TABLE_MANIFEST as u64,
        read_u64(bytes, 112),
        read_u64(bytes, 120),
        SEGMENT_ENTRY_BYTES as u64,
    )?;
    let segment_count = usize::try_from(segment_count)
        .map_err(|_| invalid(KIND, "segment count does not fit this platform"))?;
    let manifest_generation = ManifestGeneration::new(read_u64(bytes, 72))?;
    let mut segments = Vec::with_capacity(segment_count);
    let mut previous_segment_id = None;
    for entry in bytes[directory].chunks_exact(SEGMENT_ENTRY_BYTES) {
        if read_u16(entry, 18) != SEGMENT_DESCRIPTOR_VERSION {
            return Err(invalid(KIND, "unsupported segment descriptor version"));
        }
        let flags = read_u32(entry, 20);
        if flags & !SEGMENT_KNOWN_FLAGS != 0 {
            return Err(invalid(KIND, "unknown segment descriptor flags"));
        }
        let segment_id = SegmentId::from_bytes(read_array(entry, 0))?;
        if previous_segment_id.is_some_and(|previous| previous >= segment_id) {
            return Err(invalid(
                KIND,
                "segment descriptors are not strictly sorted by segment ID",
            ));
        }
        previous_segment_id = Some(segment_id);
        let tier = if flags & SEGMENT_FLAG_L1 == 0 {
            SegmentTier::L0
        } else {
            SegmentTier::L1
        };
        let segment = SegmentDescriptor::new_at_tier(
            segment_id,
            SegmentKind::from_tag(read_u16(entry, 16))?,
            tier,
            read_u64(entry, 24),
            read_u64(entry, 32),
            read_u64(entry, 40),
            read_u64(entry, 48),
            read_u64(entry, 56),
            decode_artifact_ref(&entry[64..152], KIND)?,
            decode_optional_artifact_ref(&entry[152..240], KIND)?,
        )?;
        if segment.data_artifact().creation_generation().get() > manifest_generation.get()
            || segment.index_artifact().is_some_and(|artifact| {
                artifact.creation_generation().get() > manifest_generation.get()
            })
        {
            return Err(invalid(
                KIND,
                "artifact creation generation is newer than table manifest",
            ));
        }
        segments.push(segment);
    }

    TableManifest::new(
        DatabaseId::from_bytes(read_array(bytes, 24))?,
        decode_user_object_id(
            read_array(bytes, 40),
            KIND,
            "table identity is not a user catalog object ID",
        )?,
        ManifestId::from_bytes(read_array(bytes, 56))?,
        manifest_generation,
        CatalogGeneration::new(read_u64(bytes, 80))?,
        read_u64(bytes, 88),
        read_u64(bytes, 96),
        segments,
        read_u64(bytes, 136),
    )
}
