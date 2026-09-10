use super::super::{
    fault::reach_generation_boundary, CatalogGeneration, CatalogId, CatalogRef, DatabaseGeneration,
    DatabaseId, FormatError, FormatResult, GenerationCrashPoint, ManifestGeneration, ManifestId,
    ManifestKind, ManifestRef, WalGeneration, WalReplayFloor,
};
use super::codec::*;
use super::model::{DatabaseManifest, TableManifestRef, MAX_TABLES_PER_DATABASE};

const KIND: &str = "database manifest";
const MAGIC: [u8; 8] = *b"RDX6DBM\0";
pub(crate) const TABLE_ENTRY_BYTES: usize = 96;

pub(crate) fn encoded_database_manifest_length(manifest: &DatabaseManifest) -> FormatResult<u64> {
    let directory_length = manifest
        .tables()
        .len()
        .checked_mul(TABLE_ENTRY_BYTES)
        .ok_or_else(|| invalid(KIND, "table directory length overflows"))?;
    let file_length = HEADER_BYTES
        .checked_add(directory_length)
        .and_then(|length| length.checked_add(FOOTER_BYTES))
        .ok_or_else(|| invalid(KIND, "file length overflows"))?;
    if file_length > MAX_MANIFEST_BYTES {
        return Err(limit(
            KIND,
            "file bytes",
            file_length as u64,
            MAX_MANIFEST_BYTES as u64,
        ));
    }
    Ok(file_length as u64)
}

pub fn encode_database_manifest(manifest: &DatabaseManifest) -> FormatResult<Vec<u8>> {
    let directory_length = manifest
        .tables()
        .len()
        .checked_mul(TABLE_ENTRY_BYTES)
        .ok_or_else(|| invalid(KIND, "table directory length overflows"))?;
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
    output[40..56].copy_from_slice(manifest.manifest_id().as_bytes());
    put_u64(&mut output, 56, manifest.generation().get());
    output[64..80].copy_from_slice(manifest.catalog().id().as_bytes());
    put_u64(&mut output, 80, manifest.catalog().generation().get());
    put_u64(&mut output, 88, manifest.catalog().byte_length());
    output[96..128].copy_from_slice(manifest.catalog().body_sha256());
    put_u64(
        &mut output,
        128,
        manifest.wal_replay_floor().generation().get(),
    );
    put_u64(&mut output, 136, manifest.wal_replay_floor().lsn());
    put_u64(&mut output, 144, manifest.tables().len() as u64);
    if !manifest.tables().is_empty() {
        put_u64(&mut output, 152, HEADER_BYTES as u64);
    }
    put_u64(&mut output, 160, directory_length as u64);
    put_u64(&mut output, 176, manifest.created_unix_ns());
    put_u64(&mut output, 184, manifest.transaction_high_water());

    for (index, table) in manifest.tables().iter().enumerate() {
        let start = HEADER_BYTES + index * TABLE_ENTRY_BYTES;
        let entry = &mut output[start..start + TABLE_ENTRY_BYTES];
        entry[..16].copy_from_slice(table.table_id().as_bytes());
        entry[16..32].copy_from_slice(table.manifest().id().as_bytes());
        put_u64(entry, 32, table.manifest().generation().get());
        put_u64(entry, 40, table.manifest().byte_length());
        entry[48..80].copy_from_slice(table.manifest().body_sha256());
    }

    reach_generation_boundary(GenerationCrashPoint::DatabaseManifestAfterBodyBeforeFooter)
        .map_err(|error| FormatError::PublicationIo {
            operation: "inject after database-manifest body",
            kind: error.kind(),
        })?;
    finish_file(&mut output);
    Ok(output)
}

pub fn decode_database_manifest(bytes: &[u8]) -> FormatResult<DatabaseManifest> {
    validate_file_shell(bytes, KIND, &MAGIC)?;
    if read_u64(bytes, 168) != 0 {
        return Err(invalid(KIND, "unknown header flags"));
    }
    require_zero(bytes, 192..248, KIND, "reserved header bytes are non-zero")?;

    let table_count = read_u64(bytes, 144);
    let directory = validate_canonical_directory(
        bytes,
        KIND,
        table_count,
        MAX_TABLES_PER_DATABASE as u64,
        read_u64(bytes, 152),
        read_u64(bytes, 160),
        TABLE_ENTRY_BYTES as u64,
    )?;
    let table_count = usize::try_from(table_count)
        .map_err(|_| invalid(KIND, "table count does not fit this platform"))?;
    let mut tables = Vec::with_capacity(table_count);
    let mut previous_table_id = None;
    for entry in bytes[directory].chunks_exact(TABLE_ENTRY_BYTES) {
        if read_u32(entry, 80) != 0 {
            return Err(invalid(KIND, "unknown table-reference flags"));
        }
        require_zero(
            entry,
            84..96,
            KIND,
            "table-reference reserved bytes are non-zero",
        )?;
        let table_id = decode_user_object_id(
            read_array(entry, 0),
            KIND,
            "table reference has an invalid catalog object ID",
        )?;
        if previous_table_id.is_some_and(|previous| previous >= table_id) {
            return Err(invalid(
                KIND,
                "table references are not strictly sorted by table ID",
            ));
        }
        previous_table_id = Some(table_id);
        let manifest = ManifestRef::new(
            ManifestId::from_bytes(read_array(entry, 16))?,
            ManifestKind::Table,
            ManifestGeneration::new(read_u64(entry, 32))?,
            read_u64(entry, 40),
            read_array(entry, 48),
        )?;
        tables.push(TableManifestRef::new(table_id, manifest)?);
    }

    DatabaseManifest::new(
        DatabaseId::from_bytes(read_array(bytes, 24))?,
        ManifestId::from_bytes(read_array(bytes, 40))?,
        DatabaseGeneration::new(read_u64(bytes, 56))?,
        CatalogRef::from_persisted(
            CatalogId::from_bytes(read_array(bytes, 64))?,
            CatalogGeneration::new(read_u64(bytes, 80))?,
            FORMAT_MAJOR,
            FORMAT_MINOR,
            read_u64(bytes, 88),
            read_array(bytes, 96),
        )?,
        WalReplayFloor::new(
            WalGeneration::new(read_u64(bytes, 128))?,
            read_u64(bytes, 136),
        ),
        read_u64(bytes, 184),
        tables,
        read_u64(bytes, 176),
    )
}
