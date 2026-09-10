use crate::codec::{
    checked_product, decode_payload, encode_edge, encode_payload, enforce_limit, optional_id_bytes,
    put_bytes, put_u16, put_u32, put_u64, read_array, read_u16, read_u32, read_u64,
    EDGE_ENTRY_BYTES, MAX_PAYLOAD_BYTES_PER_OBJECT,
};
use crate::{
    CatalogEdge, CatalogError, CatalogMutation, CatalogMutationSet, CatalogName, CatalogObject,
    CatalogResult, ObjectId, ObjectKind, ObjectPrecondition, MAX_CATALOG_EDGE_DELTAS_PER_SET,
    MAX_CATALOG_MUTATIONS_PER_SET, MAX_DISPLAY_NAME_BYTES,
};

pub const MUTATION_SET_FORMAT_MAJOR: u16 = 6;
pub const MUTATION_SET_FORMAT_MINOR: u16 = crate::LATEST_CATALOG_MINOR;
pub const MAX_CATALOG_MUTATION_SET_BYTES: u64 = 512 * 1024 * 1024;

const MAGIC: &[u8; 8] = b"RDX6MUT\0";
const HEADER_BYTES: usize = 128;
const MUTATION_ENTRY_BYTES: usize = 80;
const OBJECT_HEADER_BYTES: usize = 96;
const OBJECT_MAGIC: &[u8; 4] = b"MOBJ";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
enum MutationKind {
    Create = 1,
    Alter = 2,
    Drop = 3,
    Rename = 4,
}

impl TryFrom<u16> for MutationKind {
    type Error = CatalogError;

    fn try_from(value: u16) -> CatalogResult<Self> {
        match value {
            1 => Ok(Self::Create),
            2 => Ok(Self::Alter),
            3 => Ok(Self::Drop),
            4 => Ok(Self::Rename),
            _ => invalid("catalog mutation operation tag is unknown"),
        }
    }
}

struct EncodedMutation {
    target: ObjectId,
    operation: MutationKind,
    kind: ObjectKind,
    payload_version: u16,
    expected_revision: u64,
    blob: Vec<u8>,
}

struct Header {
    format_minor: u16,
    database_id: [u8; 16],
    catalog_id: [u8; 16],
    expected_generation: u64,
    mutation_count: u64,
    removal_count: u64,
    addition_count: u64,
    mutation_directory_length: usize,
    removal_offset: usize,
    addition_offset: usize,
}

struct RawMutation {
    target: ObjectId,
    operation: MutationKind,
    kind: ObjectKind,
    payload_version: u16,
    expected_revision: u64,
    blob_offset: usize,
    blob_length: usize,
    blob_crc32: u32,
}

pub fn encode_catalog_mutation_set(set: &CatalogMutationSet) -> CatalogResult<Vec<u8>> {
    let encoded = set
        .mutations()
        .iter()
        .map(encode_mutation)
        .collect::<CatalogResult<Vec<_>>>()?;
    let mutation_directory_length = encoded.len().checked_mul(MUTATION_ENTRY_BYTES).ok_or(
        CatalogError::InvalidCatalogFormat {
            detail: "catalog mutation directory length overflow",
        },
    )?;
    let removal_length = set
        .edge_removals()
        .len()
        .checked_mul(EDGE_ENTRY_BYTES)
        .ok_or(CatalogError::InvalidCatalogFormat {
            detail: "catalog edge-removal directory length overflow",
        })?;
    let addition_length = set
        .edge_additions()
        .len()
        .checked_mul(EDGE_ENTRY_BYTES)
        .ok_or(CatalogError::InvalidCatalogFormat {
            detail: "catalog edge-addition directory length overflow",
        })?;
    let blob_length = encoded.iter().try_fold(0_usize, |total, mutation| {
        total
            .checked_add(mutation.blob.len())
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "catalog mutation blob area length overflow",
            })
    })?;
    let body_length = mutation_directory_length
        .checked_add(removal_length)
        .and_then(|value| value.checked_add(addition_length))
        .and_then(|value| value.checked_add(blob_length))
        .ok_or(CatalogError::InvalidCatalogFormat {
            detail: "catalog mutation-set body length overflow",
        })?;
    let total_length =
        HEADER_BYTES
            .checked_add(body_length)
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "catalog mutation-set total length overflow",
            })?;
    enforce_limit(
        "catalog mutation-set bytes",
        total_length as u64,
        MAX_CATALOG_MUTATION_SET_BYTES,
    )?;

    let removal_offset = HEADER_BYTES + mutation_directory_length;
    let addition_offset = removal_offset + removal_length;
    let blob_offset = addition_offset + addition_length;
    let mut output = vec![0_u8; total_length];
    put_bytes(&mut output, 0, MAGIC)?;
    put_u16(&mut output, 8, MUTATION_SET_FORMAT_MAJOR)?;
    put_u16(&mut output, 10, set.format_minor())?;
    put_u32(&mut output, 12, HEADER_BYTES as u32)?;
    put_u64(&mut output, 16, total_length as u64)?;
    put_bytes(&mut output, 24, set.expected_database_id())?;
    put_bytes(&mut output, 40, set.expected_catalog_id())?;
    put_u64(&mut output, 56, set.expected_catalog_generation())?;
    put_u64(&mut output, 64, encoded.len() as u64)?;
    put_u64(&mut output, 72, set.edge_removals().len() as u64)?;
    put_u64(&mut output, 80, set.edge_additions().len() as u64)?;
    put_u64(&mut output, 88, HEADER_BYTES as u64)?;
    put_u64(&mut output, 96, mutation_directory_length as u64)?;
    put_u64(
        &mut output,
        104,
        present_offset(removal_offset, removal_length) as u64,
    )?;
    put_u64(
        &mut output,
        112,
        present_offset(addition_offset, addition_length) as u64,
    )?;

    let mut next_blob = blob_offset;
    for (index, mutation) in encoded.iter().enumerate() {
        let entry = HEADER_BYTES + index * MUTATION_ENTRY_BYTES;
        put_bytes(&mut output, entry, mutation.target.as_bytes())?;
        put_u16(&mut output, entry + 16, mutation.operation as u16)?;
        put_u16(&mut output, entry + 18, mutation.kind.tag())?;
        put_u16(&mut output, entry + 20, mutation.payload_version)?;
        put_u64(&mut output, entry + 24, mutation.expected_revision)?;
        put_u64(
            &mut output,
            entry + 32,
            present_offset(next_blob, mutation.blob.len()) as u64,
        )?;
        put_u64(&mut output, entry + 40, mutation.blob.len() as u64)?;
        put_u32(
            &mut output,
            entry + 48,
            radixdb_core::crc32_ieee(&mutation.blob),
        )?;
        output[next_blob..next_blob + mutation.blob.len()].copy_from_slice(&mutation.blob);
        next_blob += mutation.blob.len();
    }
    encode_edges(
        &mut output[removal_offset..removal_offset + removal_length],
        set.edge_removals(),
    )?;
    encode_edges(
        &mut output[addition_offset..addition_offset + addition_length],
        set.edge_additions(),
    )?;
    let body_crc32 = radixdb_core::crc32_ieee(&output[HEADER_BYTES..]);
    put_u32(&mut output, 120, body_crc32)?;
    Ok(output)
}

pub fn decode_catalog_mutation_set(input: &[u8]) -> CatalogResult<CatalogMutationSet> {
    enforce_limit(
        "catalog mutation-set bytes",
        input.len() as u64,
        MAX_CATALOG_MUTATION_SET_BYTES,
    )?;
    let header = decode_header(input)?;
    let raw_mutations = decode_mutation_directory(input, &header)?;
    let removals = decode_edges(
        input,
        header.removal_offset,
        header.removal_count,
        header.format_minor,
    )?;
    let additions = decode_edges(
        input,
        header.addition_offset,
        header.addition_count,
        header.format_minor,
    )?;

    let blob_start = HEADER_BYTES
        + header.mutation_directory_length
        + checked_product(
            header.removal_count,
            EDGE_ENTRY_BYTES,
            "catalog edge-removal bytes",
        )?
        + checked_product(
            header.addition_count,
            EDGE_ENTRY_BYTES,
            "catalog edge-addition bytes",
        )?;
    let mut expected_blob_offset = blob_start;
    let mut mutations = Vec::with_capacity(raw_mutations.len());
    for raw in raw_mutations {
        if raw.blob_offset != present_offset(expected_blob_offset, raw.blob_length) {
            return noncanonical("catalog mutation blobs are not tightly packed in target order");
        }
        let end = if raw.blob_length == 0 {
            expected_blob_offset
        } else {
            raw.blob_offset.checked_add(raw.blob_length).ok_or(
                CatalogError::InvalidCatalogFormat {
                    detail: "catalog mutation blob range overflow",
                },
            )?
        };
        let blob = if raw.blob_length == 0 {
            &[][..]
        } else {
            input
                .get(raw.blob_offset..end)
                .ok_or(CatalogError::InvalidCatalogFormat {
                    detail: "catalog mutation blob is out of bounds",
                })?
        };
        if radixdb_core::crc32_ieee(blob) != raw.blob_crc32 {
            return Err(CatalogError::CatalogChecksumMismatch {
                scope: "mutation blob CRC32",
            });
        }
        mutations.push(decode_mutation(raw, blob)?);
        expected_blob_offset = end;
    }
    if expected_blob_offset != input.len() {
        return noncanonical("catalog mutation-set has trailing or unreferenced bytes");
    }
    let set = CatalogMutationSet::new_for_minor(
        header.format_minor,
        header.database_id,
        header.catalog_id,
        header.expected_generation,
        mutations,
        removals,
        additions,
    )?;
    if encode_catalog_mutation_set(&set)?.as_slice() != input {
        return noncanonical("catalog mutation-set bytes are not canonical");
    }
    Ok(set)
}

fn encode_mutation(mutation: &CatalogMutation) -> CatalogResult<EncodedMutation> {
    let encoded = match mutation {
        CatalogMutation::Create { object } => EncodedMutation {
            target: object.id(),
            operation: MutationKind::Create,
            kind: object.kind(),
            payload_version: object.payload_version(),
            expected_revision: 0,
            blob: encode_object(object)?,
        },
        CatalogMutation::Alter {
            expected,
            replacement,
        } => EncodedMutation {
            target: expected.object_id(),
            operation: MutationKind::Alter,
            kind: expected.expected_kind(),
            payload_version: replacement.payload_version(),
            expected_revision: expected.expected_definition_revision(),
            blob: encode_object(replacement)?,
        },
        CatalogMutation::Drop { expected } => EncodedMutation {
            target: expected.object_id(),
            operation: MutationKind::Drop,
            kind: expected.expected_kind(),
            payload_version: 0,
            expected_revision: expected.expected_definition_revision(),
            blob: Vec::new(),
        },
        CatalogMutation::Rename { expected, new_name } => EncodedMutation {
            target: expected.object_id(),
            operation: MutationKind::Rename,
            kind: expected.expected_kind(),
            payload_version: 0,
            expected_revision: expected.expected_definition_revision(),
            blob: new_name.display().as_str().as_bytes().to_vec(),
        },
    };
    Ok(encoded)
}

fn encode_object(object: &CatalogObject) -> CatalogResult<Vec<u8>> {
    let display = object.name().display().as_str().as_bytes();
    let payload = encode_payload(object.payload())?;
    let total_length = OBJECT_HEADER_BYTES
        .checked_add(display.len())
        .and_then(|value| value.checked_add(payload.len()))
        .ok_or(CatalogError::InvalidCatalogFormat {
            detail: "catalog mutation object length overflow",
        })?;
    enforce_limit(
        "catalog mutation object bytes",
        total_length as u64,
        MAX_PAYLOAD_BYTES_PER_OBJECT + MAX_DISPLAY_NAME_BYTES as u64 + OBJECT_HEADER_BYTES as u64,
    )?;
    let mut output = vec![0_u8; total_length];
    put_bytes(&mut output, 0, OBJECT_MAGIC)?;
    put_u32(&mut output, 4, total_length as u32)?;
    put_bytes(&mut output, 8, object.id().as_bytes())?;
    put_bytes(&mut output, 24, &optional_id_bytes(object.namespace_id()))?;
    put_bytes(&mut output, 40, &optional_id_bytes(object.parent_id()))?;
    put_bytes(&mut output, 56, object.owner_principal_id().as_bytes())?;
    put_u64(&mut output, 72, object.definition_revision())?;
    put_u32(&mut output, 80, display.len() as u32)?;
    put_u32(&mut output, 84, payload.len() as u32)?;
    output[OBJECT_HEADER_BYTES..OBJECT_HEADER_BYTES + display.len()].copy_from_slice(display);
    output[OBJECT_HEADER_BYTES + display.len()..].copy_from_slice(&payload);
    let body_crc32 = radixdb_core::crc32_ieee(&output[OBJECT_HEADER_BYTES..]);
    put_u32(&mut output, 88, body_crc32)?;
    Ok(output)
}

fn decode_header(input: &[u8]) -> CatalogResult<Header> {
    if input.len() < HEADER_BYTES || &input[..8] != MAGIC {
        return invalid("catalog mutation-set header/magic is invalid");
    }
    let major = read_u16(input, 8)?;
    let minor = read_u16(input, 10)?;
    if major != MUTATION_SET_FORMAT_MAJOR || minor > MUTATION_SET_FORMAT_MINOR {
        return invalid("catalog mutation-set format version is unsupported");
    }
    if read_u32(input, 12)? != HEADER_BYTES as u32
        || read_u64(input, 16)? != input.len() as u64
        || read_u32(input, 124)? != 0
    {
        return invalid("catalog mutation-set header length/flags are invalid");
    }
    if radixdb_core::crc32_ieee(&input[HEADER_BYTES..]) != read_u32(input, 120)? {
        return Err(CatalogError::CatalogChecksumMismatch {
            scope: "mutation-set body CRC32",
        });
    }
    let database_id = read_array(input, 24)?;
    let catalog_id = read_array(input, 40)?;
    if database_id == [0; 16] || catalog_id == [0; 16] || read_u64(input, 56)? == 0 {
        return invalid("catalog mutation-set expected identity/generation is zero");
    }
    let mutation_count = read_u64(input, 64)?;
    let removal_count = read_u64(input, 72)?;
    let addition_count = read_u64(input, 80)?;
    enforce_limit(
        "catalog mutation count",
        mutation_count,
        MAX_CATALOG_MUTATIONS_PER_SET as u64,
    )?;
    enforce_limit(
        "catalog edge-removal count",
        removal_count,
        MAX_CATALOG_EDGE_DELTAS_PER_SET as u64,
    )?;
    enforce_limit(
        "catalog edge-addition count",
        addition_count,
        MAX_CATALOG_EDGE_DELTAS_PER_SET as u64,
    )?;
    if mutation_count == 0 && removal_count == 0 && addition_count == 0 {
        return invalid("catalog mutation set is empty");
    }
    let mutation_directory_length = checked_product(
        mutation_count,
        MUTATION_ENTRY_BYTES,
        "catalog mutation directory bytes",
    )?;
    if read_u64(input, 88)? != HEADER_BYTES as u64
        || read_u64(input, 96)? != mutation_directory_length as u64
    {
        return noncanonical("catalog mutation directory offset/length is not canonical");
    }
    let removal_offset = HEADER_BYTES + mutation_directory_length;
    let removal_length = checked_product(
        removal_count,
        EDGE_ENTRY_BYTES,
        "catalog edge-removal bytes",
    )?;
    let addition_offset =
        removal_offset
            .checked_add(removal_length)
            .ok_or(CatalogError::InvalidCatalogFormat {
                detail: "catalog edge-removal range overflow",
            })?;
    let addition_length = checked_product(
        addition_count,
        EDGE_ENTRY_BYTES,
        "catalog edge-addition bytes",
    )?;
    if read_u64(input, 104)? != present_offset(removal_offset, removal_length) as u64
        || read_u64(input, 112)? != present_offset(addition_offset, addition_length) as u64
        || addition_offset
            .checked_add(addition_length)
            .is_none_or(|minimum| minimum > input.len())
    {
        return noncanonical("catalog edge-directory offsets are not canonical");
    }
    Ok(Header {
        format_minor: minor,
        database_id,
        catalog_id,
        expected_generation: read_u64(input, 56)?,
        mutation_count,
        removal_count,
        addition_count,
        mutation_directory_length,
        removal_offset,
        addition_offset,
    })
}

fn decode_mutation_directory(input: &[u8], header: &Header) -> CatalogResult<Vec<RawMutation>> {
    let count =
        usize::try_from(header.mutation_count).map_err(|_| CatalogError::CatalogLimitExceeded {
            field: "catalog mutation count",
            actual: header.mutation_count,
            limit: usize::MAX as u64,
        })?;
    let mut output = Vec::with_capacity(count);
    let mut previous = None;
    for index in 0..count {
        let offset = HEADER_BYTES + index * MUTATION_ENTRY_BYTES;
        let entry = &input[offset..offset + MUTATION_ENTRY_BYTES];
        let target = ObjectId::from_bytes(read_array(entry, 0)?)?;
        if previous.is_some_and(|value| value >= target) {
            return noncanonical("catalog mutation targets are not strictly increasing");
        }
        previous = Some(target);
        if read_u16(entry, 22)? != 0
            || read_u32(entry, 52)? != 0
            || entry[56..].iter().any(|byte| *byte != 0)
        {
            return invalid("catalog mutation entry reserved fields are non-zero");
        }
        output.push(RawMutation {
            target,
            operation: MutationKind::try_from(read_u16(entry, 16)?)?,
            kind: ObjectKind::from_tag_for_minor(read_u16(entry, 18)?, header.format_minor)?,
            payload_version: read_u16(entry, 20)?,
            expected_revision: read_u64(entry, 24)?,
            blob_offset: usize::try_from(read_u64(entry, 32)?).map_err(|_| {
                CatalogError::InvalidCatalogFormat {
                    detail: "catalog mutation blob offset does not fit usize",
                }
            })?,
            blob_length: usize::try_from(read_u64(entry, 40)?).map_err(|_| {
                CatalogError::InvalidCatalogFormat {
                    detail: "catalog mutation blob length does not fit usize",
                }
            })?,
            blob_crc32: read_u32(entry, 48)?,
        });
    }
    Ok(output)
}

fn decode_mutation(raw: RawMutation, blob: &[u8]) -> CatalogResult<CatalogMutation> {
    match raw.operation {
        MutationKind::Create => {
            require_shape(
                raw.expected_revision == 0,
                "CREATE has a precondition revision",
            )?;
            let object = decode_object(&raw, blob)?;
            Ok(CatalogMutation::create(object))
        }
        MutationKind::Alter => {
            require_shape(
                raw.expected_revision != 0,
                "ALTER has no precondition revision",
            )?;
            let expected = precondition(&raw)?;
            let replacement = decode_object(&raw, blob)?;
            Ok(CatalogMutation::alter(expected, replacement))
        }
        MutationKind::Drop => {
            require_shape(raw.payload_version == 0, "DROP has a payload version")?;
            require_shape(blob.is_empty(), "DROP has a blob")?;
            Ok(CatalogMutation::drop(precondition(&raw)?))
        }
        MutationKind::Rename => {
            require_shape(raw.payload_version == 0, "RENAME has a payload version")?;
            enforce_limit(
                "catalog rename display-name bytes",
                blob.len() as u64,
                MAX_DISPLAY_NAME_BYTES as u64,
            )?;
            let display =
                std::str::from_utf8(blob).map_err(|_| CatalogError::InvalidCatalogUtf8 {
                    field: "mutation rename display name",
                })?;
            Ok(CatalogMutation::rename(
                precondition(&raw)?,
                CatalogName::new(display)?,
            ))
        }
    }
}

fn decode_object(raw: &RawMutation, input: &[u8]) -> CatalogResult<CatalogObject> {
    enforce_limit(
        "catalog mutation object bytes",
        input.len() as u64,
        MAX_PAYLOAD_BYTES_PER_OBJECT + MAX_DISPLAY_NAME_BYTES as u64 + OBJECT_HEADER_BYTES as u64,
    )?;
    if input.len() < OBJECT_HEADER_BYTES
        || &input[..4] != OBJECT_MAGIC
        || read_u32(input, 4)? as usize != input.len()
        || read_u32(input, 92)? != 0
    {
        return invalid("catalog mutation object envelope is invalid");
    }
    if radixdb_core::crc32_ieee(&input[OBJECT_HEADER_BYTES..]) != read_u32(input, 88)? {
        return Err(CatalogError::CatalogChecksumMismatch {
            scope: "mutation object body CRC32",
        });
    }
    let id = ObjectId::from_bytes(read_array(input, 8)?)?;
    if id != raw.target {
        return invalid("catalog mutation object ID differs from target ID");
    }
    let display_length = read_u32(input, 80)? as usize;
    let payload_length = read_u32(input, 84)? as usize;
    if OBJECT_HEADER_BYTES
        .checked_add(display_length)
        .and_then(|value| value.checked_add(payload_length))
        != Some(input.len())
    {
        return invalid("catalog mutation object body lengths are invalid");
    }
    enforce_limit(
        "catalog mutation display-name bytes",
        display_length as u64,
        MAX_DISPLAY_NAME_BYTES as u64,
    )?;
    enforce_limit(
        "catalog mutation payload bytes",
        payload_length as u64,
        MAX_PAYLOAD_BYTES_PER_OBJECT,
    )?;
    let display =
        std::str::from_utf8(&input[OBJECT_HEADER_BYTES..OBJECT_HEADER_BYTES + display_length])
            .map_err(|_| CatalogError::InvalidCatalogUtf8 {
                field: "mutation object display name",
            })?;
    let payload = decode_payload(
        &input[OBJECT_HEADER_BYTES + display_length..],
        raw.kind,
        raw.payload_version,
    )?;
    CatalogObject::from_fields(
        id,
        raw.kind,
        0,
        decode_optional_id(read_array(input, 24)?)?,
        decode_optional_id(read_array(input, 40)?)?,
        ObjectId::from_bytes(read_array(input, 56)?)?,
        CatalogName::new(display)?,
        read_u64(input, 72)?,
        payload,
    )
}

fn precondition(raw: &RawMutation) -> CatalogResult<ObjectPrecondition> {
    ObjectPrecondition::new(raw.target, raw.kind, raw.expected_revision)
}

fn decode_edges(
    input: &[u8],
    offset: usize,
    count: u64,
    format_minor: u16,
) -> CatalogResult<Vec<CatalogEdge>> {
    let count = usize::try_from(count).map_err(|_| CatalogError::CatalogLimitExceeded {
        field: "catalog edge delta count",
        actual: count,
        limit: usize::MAX as u64,
    })?;
    let mut output = Vec::with_capacity(count);
    for index in 0..count {
        let start = offset + index * EDGE_ENTRY_BYTES;
        let entry = input.get(start..start + EDGE_ENTRY_BYTES).ok_or(
            CatalogError::InvalidCatalogFormat {
                detail: "catalog edge-delta entry is out of bounds",
            },
        )?;
        if read_u32(entry, 44)? != 0 {
            return invalid("catalog edge-delta reserved bytes are non-zero");
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
        if output.last().is_some_and(|previous| previous >= &edge) {
            return noncanonical("catalog edge-delta directory is not strictly increasing");
        }
        output.push(edge);
    }
    Ok(output)
}

fn encode_edges(output: &mut [u8], edges: &[CatalogEdge]) -> CatalogResult<()> {
    for (index, edge) in edges.iter().copied().enumerate() {
        encode_edge(
            &mut output[index * EDGE_ENTRY_BYTES..(index + 1) * EDGE_ENTRY_BYTES],
            edge,
        )?;
    }
    Ok(())
}

fn decode_optional_id(bytes: [u8; 16]) -> CatalogResult<Option<ObjectId>> {
    if bytes == [0; 16] {
        Ok(None)
    } else {
        ObjectId::from_bytes(bytes).map(Some)
    }
}

const fn present_offset(offset: usize, length: usize) -> usize {
    if length == 0 {
        0
    } else {
        offset
    }
}

fn require_shape(condition: bool, detail: &'static str) -> CatalogResult<()> {
    if condition {
        Ok(())
    } else {
        invalid(detail)
    }
}

fn invalid<T>(detail: &'static str) -> CatalogResult<T> {
    Err(CatalogError::InvalidCatalogFormat { detail })
}

fn noncanonical<T>(detail: &'static str) -> CatalogResult<T> {
    Err(CatalogError::NonCanonicalCatalogEncoding { detail })
}
