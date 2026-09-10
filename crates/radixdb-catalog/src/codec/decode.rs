use super::encode::encode_edge;
use super::payload::decode_payload;
use super::primitives::{
    align_8, checked_product, enforce_limit, range, read_array, read_u16, read_u32, read_u64,
    BlobRef, BASELINE_FORMAT_MINOR, CATALOG_MAGIC, EDGE_ENTRY_BYTES, FOOTER_BYTES, FOOTER_MAGIC,
    FORMAT_MAJOR, HEADER_BYTES, LATEST_FORMAT_MINOR, MAX_CATALOG_EDGES, MAX_CATALOG_FILE_BYTES,
    MAX_CATALOG_OBJECTS, MAX_PAYLOAD_AREA_BYTES, MAX_STRING_AREA_BYTES, OBJECT_ENTRY_BYTES,
};
use super::types::{CatalogPack, CatalogPackMeta};
use crate::{
    CatalogEdge, CatalogError, CatalogGraph, CatalogName, CatalogObject, CatalogResult, ObjectId,
    ObjectKind, MAX_DISPLAY_NAME_BYTES, MAX_NORMALIZED_NAME_BYTES,
};

struct Header {
    format_minor: u16,
    meta: CatalogPackMeta,
    object_count: u64,
    edge_count: u64,
    object_offset: u64,
    object_length: u64,
    edge_offset: u64,
    edge_length: u64,
    payload_offset: u64,
    payload_length: u64,
    string_offset: u64,
    string_length: u64,
}

struct RawObject {
    id: ObjectId,
    kind: ObjectKind,
    payload_version: u16,
    flags: u32,
    namespace_id: Option<ObjectId>,
    parent_id: Option<ObjectId>,
    owner_principal_id: ObjectId,
    normalized_name: BlobRef,
    display_name: BlobRef,
    payload: BlobRef,
    definition_revision: u64,
    dependency_digest: [u8; 32],
}

pub fn decode_catalog_pack(input: &[u8]) -> CatalogResult<CatalogPack> {
    decode_catalog_pack_for_max_minor(input, LATEST_FORMAT_MINOR)
}

/// Decode using the exact catalog-minor ceiling supported by a reader.
///
/// This is an integration-test oracle for old-reader compatibility. Production
/// callers use [`decode_catalog_pack`], which supplies this binary's current
/// ceiling.
#[doc(hidden)]
pub fn decode_catalog_pack_for_max_minor(
    input: &[u8],
    max_supported_minor: u16,
) -> CatalogResult<CatalogPack> {
    enforce_limit(
        "catalog file bytes",
        input.len() as u64,
        MAX_CATALOG_FILE_BYTES,
    )?;
    if input.len() < HEADER_BYTES + FOOTER_BYTES {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog file is shorter than header and footer",
        });
    }
    let header = decode_header(input, max_supported_minor)?;
    let body_sha = validate_footer(input)?;
    validate_layout(input, &header)?;

    let object_directory = range(
        input,
        header.object_offset,
        header.object_length,
        "object directory is out of bounds",
    )?;
    let edge_directory = if header.edge_count == 0 {
        &[][..]
    } else {
        range(
            input,
            header.edge_offset,
            header.edge_length,
            "edge directory is out of bounds",
        )?
    };
    let payload_area = range(
        input,
        header.payload_offset,
        header.payload_length,
        "payload area is out of bounds",
    )?;
    let string_area = range(
        input,
        header.string_offset,
        header.string_length,
        "string area is out of bounds",
    )?;

    let raw_objects =
        decode_object_directory(object_directory, header.object_count, header.format_minor)?;
    let edges = decode_edge_directory(edge_directory, header.edge_count, header.format_minor)?;
    let objects = decode_objects(&raw_objects, &edges, payload_area, string_area)?;
    let graph = CatalogGraph::build(objects, edges)?;
    if graph.required_format_minor() != header.format_minor {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog format minor disagrees with admitted object/edge registry",
        });
    }
    Ok(CatalogPack::new(
        header.format_minor,
        header.meta,
        graph,
        body_sha,
    ))
}

fn decode_header(input: &[u8], max_supported_minor: u16) -> CatalogResult<Header> {
    if &input[..8] != CATALOG_MAGIC {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog magic is invalid",
        });
    }
    let major = read_u16(input, 8)?;
    let format_minor = read_u16(input, 10)?;
    if max_supported_minor > LATEST_FORMAT_MINOR
        || major != FORMAT_MAJOR
        || !(BASELINE_FORMAT_MINOR..=max_supported_minor).contains(&format_minor)
    {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog format version is unsupported",
        });
    }
    if read_u32(input, 12)? != HEADER_BYTES as u32 {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog header length is invalid",
        });
    }
    if read_u64(input, 16)? != input.len() as u64 {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog header file length is invalid",
        });
    }
    if read_u64(input, 152)? != 0 || !all_zero(&input[168..248]) || !all_zero(&input[252..256]) {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog header flags/reserved bytes are non-zero",
        });
    }
    if radixdb_core::crc32_ieee(&input[..248]) != read_u32(input, 248)? {
        return Err(CatalogError::CatalogChecksumMismatch {
            scope: "header CRC32",
        });
    }

    let meta = CatalogPackMeta::new(
        read_array(input, 24)?,
        read_array(input, 40)?,
        read_u64(input, 56)?,
        read_u64(input, 64)?,
        read_u64(input, 160)?,
    )?;
    let object_count = read_u64(input, 72)?;
    let edge_count = read_u64(input, 80)?;
    enforce_limit("catalog object count", object_count, MAX_CATALOG_OBJECTS)?;
    enforce_limit("catalog edge count", edge_count, MAX_CATALOG_EDGES)?;
    if object_count == 0 {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog object directory is empty",
        });
    }
    Ok(Header {
        format_minor,
        meta,
        object_count,
        edge_count,
        object_offset: read_u64(input, 88)?,
        object_length: read_u64(input, 96)?,
        edge_offset: read_u64(input, 104)?,
        edge_length: read_u64(input, 112)?,
        payload_offset: read_u64(input, 120)?,
        payload_length: read_u64(input, 128)?,
        string_offset: read_u64(input, 136)?,
        string_length: read_u64(input, 144)?,
    })
}

fn validate_footer(input: &[u8]) -> CatalogResult<[u8; 32]> {
    let footer_offset = input.len() - FOOTER_BYTES;
    let footer = &input[footer_offset..];
    if &footer[..8] != FOOTER_MAGIC || read_u64(footer, 8)? != input.len() as u64 {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog footer magic/length is invalid",
        });
    }
    let expected: [u8; 32] = read_array(footer, 16)?;
    if radixdb_core::sha256_digest(&input[..footer_offset]) != expected {
        return Err(CatalogError::CatalogChecksumMismatch {
            scope: "whole-file SHA-256",
        });
    }
    Ok(expected)
}

fn validate_layout(input: &[u8], header: &Header) -> CatalogResult<()> {
    let expected_object_length = checked_product(
        header.object_count,
        OBJECT_ENTRY_BYTES,
        "catalog object directory bytes",
    )?;
    let expected_edge_length = checked_product(
        header.edge_count,
        EDGE_ENTRY_BYTES,
        "catalog edge directory bytes",
    )?;
    if header.object_length != expected_object_length as u64
        || header.edge_length != expected_edge_length as u64
    {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog directory length/count mismatch",
        });
    }
    enforce_limit(
        "catalog payload area bytes",
        header.payload_length,
        MAX_PAYLOAD_AREA_BYTES,
    )?;
    enforce_limit(
        "catalog string area bytes",
        header.string_length,
        MAX_STRING_AREA_BYTES,
    )?;
    if header.payload_length == 0 || header.string_length == 0 {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "required catalog payload/string area is empty",
        });
    }

    let mut cursor = HEADER_BYTES;
    let object_offset = align_8(cursor)?;
    if header.object_offset != object_offset as u64 {
        return noncanonical("object directory is not at the canonical offset");
    }
    cursor = object_offset.checked_add(expected_object_length).ok_or(
        CatalogError::InvalidCatalogFormat {
            detail: "object directory end overflow",
        },
    )?;
    if expected_edge_length == 0 {
        if header.edge_offset != 0 || header.edge_length != 0 {
            return noncanonical("empty edge directory does not use zero reference");
        }
    } else {
        let edge_offset = align_8(cursor)?;
        if header.edge_offset != edge_offset as u64 {
            return noncanonical("edge directory is not at the canonical offset");
        }
        ensure_zero_padding(input, cursor, edge_offset)?;
        cursor = edge_offset.checked_add(expected_edge_length).ok_or(
            CatalogError::InvalidCatalogFormat {
                detail: "edge directory end overflow",
            },
        )?;
    }
    let payload_offset = align_8(cursor)?;
    if header.payload_offset != payload_offset as u64 {
        return noncanonical("payload area is not at the canonical offset");
    }
    ensure_zero_padding(input, cursor, payload_offset)?;
    cursor = payload_offset
        .checked_add(header.payload_length as usize)
        .ok_or(CatalogError::InvalidCatalogFormat {
            detail: "payload area end overflow",
        })?;
    let string_offset = align_8(cursor)?;
    if header.string_offset != string_offset as u64 {
        return noncanonical("string area is not at the canonical offset");
    }
    ensure_zero_padding(input, cursor, string_offset)?;
    cursor = string_offset
        .checked_add(header.string_length as usize)
        .ok_or(CatalogError::InvalidCatalogFormat {
            detail: "string area end overflow",
        })?;
    let footer_offset = align_8(cursor)?;
    ensure_zero_padding(input, cursor, footer_offset)?;
    if footer_offset != input.len() - FOOTER_BYTES {
        return noncanonical("footer is not at the canonical offset");
    }
    Ok(())
}

fn decode_object_directory(
    input: &[u8],
    count: u64,
    format_minor: u16,
) -> CatalogResult<Vec<RawObject>> {
    let mut objects = Vec::with_capacity(count as usize);
    let mut previous_id = None;
    for index in 0..count as usize {
        let entry = &input[index * OBJECT_ENTRY_BYTES..(index + 1) * OBJECT_ENTRY_BYTES];
        let id = ObjectId::from_bytes(read_array(entry, 0)?)?;
        if previous_id.is_some_and(|previous| previous >= id) {
            return noncanonical("object directory IDs are not strictly increasing");
        }
        previous_id = Some(id);
        objects.push(RawObject {
            id,
            kind: ObjectKind::from_tag_for_minor(read_u16(entry, 16)?, format_minor)?,
            payload_version: read_u16(entry, 18)?,
            flags: read_u32(entry, 20)?,
            namespace_id: decode_optional_id(read_array(entry, 24)?)?,
            parent_id: decode_optional_id(read_array(entry, 40)?)?,
            owner_principal_id: ObjectId::from_bytes(read_array(entry, 56)?)?,
            normalized_name: BlobRef::read_at(entry, 72)?,
            display_name: BlobRef::read_at(entry, 88)?,
            payload: BlobRef::read_at(entry, 104)?,
            definition_revision: read_u64(entry, 120)?,
            dependency_digest: read_array(entry, 128)?,
        });
    }
    Ok(objects)
}

fn decode_edge_directory(
    input: &[u8],
    count: u64,
    format_minor: u16,
) -> CatalogResult<Vec<CatalogEdge>> {
    let mut edges = Vec::with_capacity(count as usize);
    for index in 0..count as usize {
        let entry = &input[index * EDGE_ENTRY_BYTES..(index + 1) * EDGE_ENTRY_BYTES];
        if read_u32(entry, 44)? != 0 {
            return Err(CatalogError::InvalidCatalogFormat {
                detail: "catalog edge reserved bytes are non-zero",
            });
        }
        let edge = CatalogEdge::from_fields_for_minor(
            ObjectId::from_bytes(read_array(entry, 0)?)?,
            ObjectId::from_bytes(read_array(entry, 16)?)?,
            read_u16(entry, 32)?,
            read_u16(entry, 34)?,
            read_u32(entry, 36)?,
            read_u32(entry, 40)?,
            format_minor,
        )?;
        if edges.last().is_some_and(|previous| previous >= &edge) {
            return noncanonical("edge directory is not strictly sorted");
        }
        edges.push(edge);
    }
    Ok(edges)
}

fn decode_objects(
    raw_objects: &[RawObject],
    edges: &[CatalogEdge],
    payload_area: &[u8],
    string_area: &[u8],
) -> CatalogResult<Vec<CatalogObject>> {
    let mut objects = Vec::with_capacity(raw_objects.len());
    let mut expected_payload_offset = 0_u64;
    let mut expected_string_offset = 0_u64;
    for raw in raw_objects {
        if raw.payload.offset != expected_payload_offset
            || raw.normalized_name.offset != expected_string_offset
        {
            return noncanonical("catalog blob references are not tightly packed by object ID");
        }
        enforce_limit(
            "catalog normalized name bytes",
            raw.normalized_name.length.into(),
            MAX_NORMALIZED_NAME_BYTES as u64,
        )?;
        let normalized = required_blob(string_area, raw.normalized_name, "normalized name")?;
        expected_string_offset += u64::from(raw.normalized_name.length);
        if raw.display_name.offset != expected_string_offset {
            return noncanonical("display-name blob does not follow normalized name");
        }
        enforce_limit(
            "catalog display name bytes",
            raw.display_name.length.into(),
            MAX_DISPLAY_NAME_BYTES as u64,
        )?;
        let display = required_blob(string_area, raw.display_name, "display name")?;
        expected_string_offset += u64::from(raw.display_name.length);
        let payload_bytes = required_blob(payload_area, raw.payload, "typed payload")?;
        expected_payload_offset += u64::from(raw.payload.length);

        let normalized =
            std::str::from_utf8(normalized).map_err(|_| CatalogError::InvalidCatalogUtf8 {
                field: "normalized name",
            })?;
        let display =
            std::str::from_utf8(display).map_err(|_| CatalogError::InvalidCatalogUtf8 {
                field: "display name",
            })?;
        let name = CatalogName::from_stored(display, normalized)?;
        if name.display().as_str() != display {
            return noncanonical("display name is not NFC canonical");
        }
        let payload = decode_payload(payload_bytes, raw.kind, raw.payload_version)?;
        let object = CatalogObject::from_fields(
            raw.id,
            raw.kind,
            raw.flags,
            raw.namespace_id,
            raw.parent_id,
            raw.owner_principal_id,
            name,
            raw.definition_revision,
            payload,
        )?;
        if edge_digest(edges, raw.id)? != raw.dependency_digest {
            return Err(CatalogError::CatalogChecksumMismatch {
                scope: "object dependency digest",
            });
        }
        objects.push(object);
    }
    if expected_payload_offset != payload_area.len() as u64
        || expected_string_offset != string_area.len() as u64
    {
        return noncanonical("catalog blob areas contain unreferenced trailing bytes");
    }
    Ok(objects)
}

fn required_blob<'a>(
    area: &'a [u8],
    reference: BlobRef,
    detail: &'static str,
) -> CatalogResult<&'a [u8]> {
    if reference == BlobRef::absent() {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "required catalog BlobRef is absent",
        });
    }
    let bytes = range(area, reference.offset, u64::from(reference.length), detail)?;
    if radixdb_core::crc32_ieee(bytes) != reference.crc32 {
        return Err(CatalogError::CatalogChecksumMismatch { scope: detail });
    }
    Ok(bytes)
}

fn edge_digest(edges: &[CatalogEdge], source: ObjectId) -> CatalogResult<[u8; 32]> {
    let selected = edges
        .iter()
        .filter(|edge| edge.source_object_id() == source)
        .copied()
        .collect::<Vec<_>>();
    let mut bytes = vec![0_u8; selected.len() * EDGE_ENTRY_BYTES];
    for (index, edge) in selected.into_iter().enumerate() {
        encode_edge(
            &mut bytes[index * EDGE_ENTRY_BYTES..(index + 1) * EDGE_ENTRY_BYTES],
            edge,
        )?;
    }
    Ok(radixdb_core::sha256_digest(&bytes))
}

fn decode_optional_id(bytes: [u8; 16]) -> CatalogResult<Option<ObjectId>> {
    if bytes == [0; 16] {
        Ok(None)
    } else {
        ObjectId::from_bytes(bytes).map(Some)
    }
}

fn ensure_zero_padding(input: &[u8], start: usize, end: usize) -> CatalogResult<()> {
    if !all_zero(
        input
            .get(start..end)
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "catalog alignment padding is out of bounds",
            })?,
    ) {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog alignment padding is non-zero",
        });
    }
    Ok(())
}

fn all_zero(bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| *byte == 0)
}

fn noncanonical<T>(detail: &'static str) -> CatalogResult<T> {
    Err(CatalogError::NonCanonicalCatalogEncoding { detail })
}
