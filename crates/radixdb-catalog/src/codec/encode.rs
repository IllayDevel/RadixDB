use super::payload::encode_payload;
use super::primitives::{
    append_aligned, enforce_limit, optional_id_bytes, put_bytes, put_u16, put_u32, put_u64,
    BlobRef, CATALOG_MAGIC, EDGE_ENTRY_BYTES, FOOTER_BYTES, FOOTER_MAGIC, FORMAT_MAJOR,
    HEADER_BYTES, MAX_CATALOG_EDGES, MAX_CATALOG_FILE_BYTES, MAX_CATALOG_OBJECTS,
    MAX_PAYLOAD_AREA_BYTES, MAX_STRING_AREA_BYTES, OBJECT_ENTRY_BYTES,
};
use super::types::CatalogPackMeta;
use crate::{CatalogEdge, CatalogError, CatalogGraph, CatalogResult, ObjectId};

struct ObjectBlobs {
    id: ObjectId,
    normalized_name: BlobRef,
    display_name: BlobRef,
    payload: BlobRef,
}

/// Opaque canonical catalog body awaiting its fixed footer.
#[doc(hidden)]
pub struct CatalogPackBody(Vec<u8>);

pub fn encode_catalog_pack(meta: CatalogPackMeta, graph: &CatalogGraph) -> CatalogResult<Vec<u8>> {
    finish_catalog_pack(encode_catalog_pack_body(meta, graph)?)
}

/// Encode the complete canonical catalog body while deliberately leaving the
/// fixed footer absent. Storage lifecycle tests use this narrow seam to stop
/// at the durable body/footer boundary without duplicating the codec.
#[doc(hidden)]
pub fn encode_catalog_pack_body(
    meta: CatalogPackMeta,
    graph: &CatalogGraph,
) -> CatalogResult<CatalogPackBody> {
    enforce_limit(
        "catalog object count",
        graph.objects().len() as u64,
        MAX_CATALOG_OBJECTS,
    )?;
    let format_minor = graph.required_format_minor();
    enforce_limit(
        "catalog edge count",
        graph.edges().len() as u64,
        MAX_CATALOG_EDGES,
    )?;

    let mut payload_area = Vec::new();
    let mut string_area = Vec::new();
    let mut blobs = Vec::with_capacity(graph.objects().len());
    for object in graph.objects() {
        let normalized_name = append_blob(
            &mut string_area,
            object.name().normalized().as_str().as_bytes(),
            "catalog normalized-name bytes",
            MAX_STRING_AREA_BYTES,
        )?;
        let display_name = append_blob(
            &mut string_area,
            object.name().display().as_str().as_bytes(),
            "catalog display-name bytes",
            MAX_STRING_AREA_BYTES,
        )?;
        let payload_bytes = encode_payload(object.payload())?;
        let payload = append_blob(
            &mut payload_area,
            &payload_bytes,
            "catalog typed-payload bytes",
            MAX_PAYLOAD_AREA_BYTES,
        )?;
        blobs.push(ObjectBlobs {
            id: object.id(),
            normalized_name,
            display_name,
            payload,
        });
    }
    enforce_limit(
        "catalog payload area bytes",
        payload_area.len() as u64,
        MAX_PAYLOAD_AREA_BYTES,
    )?;
    enforce_limit(
        "catalog string area bytes",
        string_area.len() as u64,
        MAX_STRING_AREA_BYTES,
    )?;

    let object_directory_length = graph
        .objects()
        .len()
        .checked_mul(OBJECT_ENTRY_BYTES)
        .ok_or(CatalogError::InvalidCatalogFormat {
            detail: "object directory length overflow",
        })?;
    let edge_directory_length = graph.edges().len().checked_mul(EDGE_ENTRY_BYTES).ok_or(
        CatalogError::InvalidCatalogFormat {
            detail: "edge directory length overflow",
        },
    )?;

    let mut output = vec![0_u8; HEADER_BYTES];
    let object_directory_offset = append_aligned(&mut output)?;
    output.resize(object_directory_offset + object_directory_length, 0);
    let edge_directory_offset = if edge_directory_length == 0 {
        0
    } else {
        let offset = append_aligned(&mut output)?;
        output.resize(offset + edge_directory_length, 0);
        offset
    };
    let payload_area_offset = append_aligned(&mut output)?;
    output.extend_from_slice(&payload_area);
    let string_area_offset = append_aligned(&mut output)?;
    output.extend_from_slice(&string_area);
    append_aligned(&mut output)?;

    for (index, (object, refs)) in graph.objects().zip(&blobs).enumerate() {
        debug_assert_eq!(object.id(), refs.id);
        let offset = object_directory_offset + index * OBJECT_ENTRY_BYTES;
        put_bytes(&mut output, offset, object.id().as_bytes())?;
        put_u16(&mut output, offset + 16, object.kind().tag())?;
        put_u16(&mut output, offset + 18, object.payload_version())?;
        put_u32(&mut output, offset + 20, object.flags())?;
        put_bytes(
            &mut output,
            offset + 24,
            &optional_id_bytes(object.namespace_id()),
        )?;
        put_bytes(
            &mut output,
            offset + 40,
            &optional_id_bytes(object.parent_id()),
        )?;
        put_bytes(
            &mut output,
            offset + 56,
            object.owner_principal_id().as_bytes(),
        )?;
        refs.normalized_name.write_at(&mut output, offset + 72)?;
        refs.display_name.write_at(&mut output, offset + 88)?;
        refs.payload.write_at(&mut output, offset + 104)?;
        put_u64(&mut output, offset + 120, object.definition_revision())?;
        put_bytes(
            &mut output,
            offset + 128,
            &dependency_digest(graph, object.id())?,
        )?;
    }
    if edge_directory_length != 0 {
        for (index, edge) in graph.edges().iter().copied().enumerate() {
            let offset = edge_directory_offset + index * EDGE_ENTRY_BYTES;
            encode_edge(&mut output[offset..offset + EDGE_ENTRY_BYTES], edge)?;
        }
    }

    let file_length =
        output
            .len()
            .checked_add(FOOTER_BYTES)
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "catalog file length overflow",
            })?;
    enforce_limit(
        "catalog file bytes",
        file_length as u64,
        MAX_CATALOG_FILE_BYTES,
    )?;

    put_bytes(&mut output, 0, CATALOG_MAGIC)?;
    put_u16(&mut output, 8, FORMAT_MAJOR)?;
    put_u16(&mut output, 10, format_minor)?;
    put_u32(&mut output, 12, HEADER_BYTES as u32)?;
    put_u64(&mut output, 16, file_length as u64)?;
    put_bytes(&mut output, 24, &meta.database_id())?;
    put_bytes(&mut output, 40, &meta.catalog_id())?;
    put_u64(&mut output, 56, meta.catalog_generation())?;
    put_u64(&mut output, 64, meta.snapshot_lsn())?;
    put_u64(&mut output, 72, graph.objects().len() as u64)?;
    put_u64(&mut output, 80, graph.edges().len() as u64)?;
    put_u64(&mut output, 88, object_directory_offset as u64)?;
    put_u64(&mut output, 96, object_directory_length as u64)?;
    put_u64(&mut output, 104, edge_directory_offset as u64)?;
    put_u64(&mut output, 112, edge_directory_length as u64)?;
    put_u64(&mut output, 120, payload_area_offset as u64)?;
    put_u64(&mut output, 128, payload_area.len() as u64)?;
    put_u64(&mut output, 136, string_area_offset as u64)?;
    put_u64(&mut output, 144, string_area.len() as u64)?;
    put_u64(&mut output, 152, 0)?;
    put_u64(&mut output, 160, meta.created_unix_ns())?;
    let header_crc = radixdb_core::crc32_ieee(&output[..248]);
    put_u32(&mut output, 248, header_crc)?;

    Ok(CatalogPackBody(output))
}

/// Append the canonical catalog footer to a body produced by
/// [`encode_catalog_pack_body`].
#[doc(hidden)]
pub fn finish_catalog_pack(body: CatalogPackBody) -> CatalogResult<Vec<u8>> {
    let mut output = body.0;
    let file_length =
        output
            .len()
            .checked_add(FOOTER_BYTES)
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "catalog file length overflow",
            })?;
    if file_length > MAX_CATALOG_FILE_BYTES as usize {
        return Err(CatalogError::CatalogLimitExceeded {
            field: "catalog file bytes",
            actual: file_length as u64,
            limit: MAX_CATALOG_FILE_BYTES,
        });
    }
    let body_sha = radixdb_core::sha256_digest(&output);
    output.extend_from_slice(FOOTER_MAGIC);
    output.extend_from_slice(&(file_length as u64).to_le_bytes());
    output.extend_from_slice(&body_sha);
    debug_assert_eq!(output.len(), file_length);
    Ok(output)
}

fn append_blob(
    output: &mut Vec<u8>,
    bytes: &[u8],
    field: &'static str,
    area_limit: u64,
) -> CatalogResult<BlobRef> {
    let offset = output.len() as u64;
    let length = u32::try_from(bytes.len()).map_err(|_| CatalogError::CatalogLimitExceeded {
        field,
        actual: bytes.len() as u64,
        limit: u32::MAX as u64,
    })?;
    let new_length =
        output
            .len()
            .checked_add(bytes.len())
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "catalog blob area length overflow",
            })?;
    enforce_limit(field, new_length as u64, area_limit)?;
    output.extend_from_slice(bytes);
    Ok(BlobRef {
        offset,
        length,
        crc32: radixdb_core::crc32_ieee(bytes),
    })
}

pub(crate) fn encode_edge(output: &mut [u8], edge: CatalogEdge) -> CatalogResult<()> {
    if output.len() != EDGE_ENTRY_BYTES {
        return Err(CatalogError::InvalidCatalogFormat {
            detail: "edge output entry is not 48 bytes",
        });
    }
    put_bytes(output, 0, edge.source_object_id().as_bytes())?;
    put_bytes(output, 16, edge.target_object_id().as_bytes())?;
    put_u16(output, 32, edge.kind().tag())?;
    put_u16(output, 34, edge.version())?;
    put_u32(output, 36, edge.flags())?;
    put_u32(output, 40, edge.ordinal())?;
    put_u32(output, 44, 0)
}

fn dependency_digest(graph: &CatalogGraph, id: ObjectId) -> CatalogResult<[u8; 32]> {
    let edges = graph.outgoing_edges(id).copied().collect::<Vec<_>>();
    let mut bytes = vec![0_u8; edges.len() * EDGE_ENTRY_BYTES];
    for (index, edge) in edges.into_iter().enumerate() {
        encode_edge(
            &mut bytes[index * EDGE_ENTRY_BYTES..(index + 1) * EDGE_ENTRY_BYTES],
            edge,
        )?;
    }
    Ok(radixdb_core::sha256_digest(&bytes))
}
