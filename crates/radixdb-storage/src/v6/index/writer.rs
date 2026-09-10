use std::io::{Read, Seek, SeekFrom, Write};

use radixdb_catalog::ObjectId;
use sha2::{Digest, Sha256};

use super::codec::{
    canonical_shape, encode_header, encode_key_descriptor, DirectoryShape, ACCELERATOR_VERSION,
    FOOTER_MAGIC, NO_PAGE, SECTION_VERSION,
};
use super::model::{
    invalid, limit, validate_accelerator_contract, validate_page_lengths, IndexAcceleratorKind,
    IndexArtifactHeader, IndexKeyColumn, IndexPage, IndexPageCodec, IndexPageSpec,
    INDEX_ACCELERATOR_ENTRY_BYTES, INDEX_FOOTER_BYTES, INDEX_HEADER_BYTES, INDEX_PAGE_ENTRY_BYTES,
    INDEX_SECTION_ENTRY_BYTES,
};
use crate::v6::publication::diagnostics::{record as record_diagnostic, DiagnosticEvent};
use crate::v6::{
    fault::reach_generation_boundary, ArtifactKind, ArtifactRef, FormatError, FormatResult,
    GenerationCrashPoint, IndexSectionKind, MAX_ARTIFACT_FILE_BYTES,
};

const HASH_BUFFER_BYTES: usize = 64 * 1024;

pub(crate) trait IndexPageSource {
    fn logical_index_id(&self) -> ObjectId;
    fn kind(&self) -> IndexAcceleratorKind;
    fn unique(&self) -> bool;
    fn constraint_owned(&self) -> bool;
    fn definition_sha256(&self) -> &[u8; 32];
    fn key_columns(&self) -> &[IndexKeyColumn];
    fn indexed_item_count(&self) -> u64;
    fn page_count(&self) -> u64;
    fn visit_pages(
        &self,
        visitor: &mut dyn FnMut(&IndexPageSpec) -> FormatResult<()>,
    ) -> FormatResult<u64>;
}

#[derive(Debug, Clone, Copy)]
struct StoredSection {
    kind: IndexSectionKind,
    accelerator_ordinal: u32,
    first_page_index: u32,
    page_count: u32,
    offset: u64,
    stored_length: u64,
    logical_length: u64,
    item_count: u64,
    stored_crc32: u32,
}

pub(crate) fn write_index_artifact_stream<W, S>(
    output: &mut W,
    header: IndexArtifactHeader,
    sources: &[S],
    metadata_resident_limit: u64,
) -> FormatResult<ArtifactRef>
where
    W: Read + Write + Seek,
    S: IndexPageSource,
{
    validate_sources(sources, header.source_row_count())?;
    if output
        .seek(SeekFrom::End(0))
        .map_err(|error| io_error("inspect index destination", error))?
        != 0
    {
        return Err(invalid("streaming index destination is not empty"));
    }
    output
        .seek(SeekFrom::Start(0))
        .map_err(|error| io_error("rewind index destination", error))?;

    let page_count = sources.iter().try_fold(0_u64, |total, source| {
        total
            .checked_add(source.page_count())
            .ok_or_else(|| invalid("index page count overflows"))
    })?;
    let section_count = sources
        .len()
        .checked_mul(2)
        .ok_or_else(|| invalid("index section count overflows"))?;
    let shape = canonical_shape(
        u32::try_from(sources.len()).map_err(|_| invalid("accelerator count does not fit u32"))?,
        u32::try_from(section_count).map_err(|_| invalid("section count does not fit u32"))?,
        page_count,
    )?;
    let metadata_resident_bytes = (section_count as u64)
        .checked_mul(std::mem::size_of::<StoredSection>() as u64)
        .and_then(|bytes| {
            page_count
                .checked_mul(std::mem::size_of::<IndexPage>() as u64)
                .and_then(|pages| bytes.checked_add(pages))
        })
        .ok_or_else(|| invalid("streaming index metadata resident bytes overflow"))?;
    if metadata_resident_bytes > metadata_resident_limit {
        return Err(limit(
            "streaming index metadata resident bytes",
            metadata_resident_bytes,
            metadata_resident_limit,
        ));
    }
    write_zeros(output, shape.body_start)?;
    let mut position = shape.body_start;
    let mut sections = Vec::with_capacity(section_count);
    let mut pages = Vec::with_capacity(
        usize::try_from(page_count).map_err(|_| invalid("page count does not fit usize"))?,
    );

    for (accelerator_ordinal, source) in sources.iter().enumerate() {
        align(output, &mut position, None)?;
        let key_descriptor = encode_key_descriptor(source.key_columns())?;
        let key_offset = position;
        write_index(output, &key_descriptor, "write index key descriptor")?;
        position = position
            .checked_add(key_descriptor.len() as u64)
            .ok_or_else(|| invalid("key descriptor range overflows"))?;
        sections.push(StoredSection {
            kind: IndexSectionKind::KeyDescriptor,
            accelerator_ordinal: accelerator_ordinal as u32,
            first_page_index: NO_PAGE,
            page_count: 0,
            offset: key_offset,
            stored_length: key_descriptor.len() as u64,
            logical_length: key_descriptor.len() as u64,
            item_count: source.key_columns().len() as u64,
            stored_crc32: radixdb_core::crc32_ieee(&key_descriptor),
        });

        align(output, &mut position, None)?;
        let section_offset = position;
        let section_index = sections.len() as u32;
        let first_page_index = pages.len() as u32;
        let mut section_crc = crc32fast::Hasher::new();
        let mut section_logical_length = 0_u64;
        let mut section_item_count = 0_u64;
        let mut emitted = 0_u64;
        source.visit_pages(&mut |page| {
            align(output, &mut position, Some(&mut section_crc))?;
            let stored = match page.codec() {
                IndexPageCodec::None => page.logical_bytes().to_vec(),
                IndexPageCodec::Lz4 => lz4_flex::block::compress(page.logical_bytes()),
            };
            validate_page_lengths(
                page.codec(),
                stored.len() as u64,
                page.logical_bytes().len() as u64,
            )?;
            let page_ordinal = u32::try_from(emitted)
                .map_err(|_| invalid("section page ordinal does not fit u32"))?;
            let stored_crc32 = radixdb_core::crc32_ieee(&stored);
            write_index(output, &stored, "write index page")?;
            section_crc.update(&stored);
            pages.push(IndexPage::new(
                section_index,
                page_ordinal,
                position,
                stored.len() as u64,
                page.logical_bytes().len() as u64,
                page.item_count(),
                page.minimum_key_hash(),
                page.maximum_key_hash(),
                stored_crc32,
                page.codec(),
            ));
            position = position
                .checked_add(stored.len() as u64)
                .ok_or_else(|| invalid("index page range overflows"))?;
            section_logical_length = section_logical_length
                .checked_add(page.logical_bytes().len() as u64)
                .ok_or_else(|| invalid("index section logical length overflows"))?;
            section_item_count = section_item_count
                .checked_add(page.item_count())
                .ok_or_else(|| invalid("index section item count overflows"))?;
            emitted = emitted
                .checked_add(1)
                .ok_or_else(|| invalid("emitted page count overflows"))?;
            Ok(())
        })?;
        if emitted != source.page_count() || emitted == 0 {
            return Err(invalid("index page source differs from planned page count"));
        }
        sections.push(StoredSection {
            kind: match source.kind() {
                IndexAcceleratorKind::Exact => IndexSectionKind::ExactPages,
                IndexAcceleratorKind::Ordered => IndexSectionKind::OrderedPages,
                IndexAcceleratorKind::Hnsw => {
                    return Err(invalid("HNSW requires its graph section writer"));
                }
            },
            accelerator_ordinal: accelerator_ordinal as u32,
            first_page_index,
            page_count: u32::try_from(emitted)
                .map_err(|_| invalid("section page count does not fit u32"))?,
            offset: section_offset,
            stored_length: position - section_offset,
            logical_length: section_logical_length,
            item_count: section_item_count,
            stored_crc32: section_crc.finalize(),
        });
    }
    if pages.len() as u64 != shape.page_count || sections.len() as u32 != shape.section_count {
        return Err(invalid(
            "streamed index shape differs from reserved directories",
        ));
    }

    let file_length = position
        .checked_add(INDEX_FOOTER_BYTES as u64)
        .ok_or_else(|| invalid("index file length overflows"))?;
    if file_length > MAX_ARTIFACT_FILE_BYTES {
        return Err(limit("file bytes", file_length, MAX_ARTIFACT_FILE_BYTES));
    }
    let directory_crc32 = write_directories(output, header, sources, &sections, &pages, shape)?;
    let mut header_bytes = [0_u8; INDEX_HEADER_BYTES];
    encode_header(
        &mut header_bytes,
        header,
        shape,
        file_length,
        directory_crc32,
    );
    let header_crc = radixdb_core::crc32_ieee(&header_bytes[..248]);
    header_bytes[248..252].copy_from_slice(&header_crc.to_le_bytes());
    write_at(output, 0, &header_bytes, "write index header")?;

    reach_generation_boundary(GenerationCrashPoint::IndexAfterBodyBeforeFooter)
        .map_err(|error| io_error("inject after index body", error))?;

    let body_sha = digest_prefix(output, position)?;
    output
        .seek(SeekFrom::Start(position))
        .map_err(|error| io_error("seek index footer", error))?;
    write_index(output, &FOOTER_MAGIC, "write index footer magic")?;
    write_index(
        output,
        &file_length.to_le_bytes(),
        "write index footer length",
    )?;
    write_index(output, &body_sha, "write index footer identity")?;
    output
        .flush()
        .map_err(|error| io_error("finalize index artifact", error))?;
    reach_generation_boundary(GenerationCrashPoint::IndexAfterFooterBeforeSync)
        .map_err(|error| io_error("inject after index footer", error))?;
    ArtifactRef::new(
        header.artifact_id(),
        ArtifactKind::Index,
        header.creation_generation(),
        file_length,
        body_sha,
    )
}

fn validate_sources<S: IndexPageSource>(sources: &[S], source_row_count: u64) -> FormatResult<()> {
    if sources.is_empty() {
        return Err(invalid("empty index artifact is forbidden"));
    }
    let mut previous_id = None;
    for source in sources {
        if previous_id.is_some_and(|id| id >= source.logical_index_id()) {
            return Err(invalid("logical index IDs are not strictly sorted"));
        }
        if !matches!(
            source.kind(),
            IndexAcceleratorKind::Exact | IndexAcceleratorKind::Ordered
        ) {
            return Err(invalid(
                "streamed posting writer accepts exact or ordered accelerators",
            ));
        }
        validate_accelerator_contract(
            source.kind(),
            source.unique(),
            source.constraint_owned(),
            source.key_columns(),
        )?;
        let canonical_empty = source.indexed_item_count() == 0
            && source.kind() == IndexAcceleratorKind::Exact
            && source.unique()
            && source.page_count() == 1;
        if source.indexed_item_count() > source_row_count
            || source.page_count() == 0
            || (source.indexed_item_count() == 0 && !canonical_empty)
        {
            return Err(invalid("streamed accelerator counts are invalid"));
        }
        previous_id = Some(source.logical_index_id());
    }
    Ok(())
}

fn write_directories<S: IndexPageSource>(
    output: &mut (impl Write + Seek),
    header: IndexArtifactHeader,
    sources: &[S],
    sections: &[StoredSection],
    pages: &[IndexPage],
    shape: DirectoryShape,
) -> FormatResult<u32> {
    output
        .seek(SeekFrom::Start(shape.accelerator_offset))
        .map_err(|error| io_error("seek index directories", error))?;
    let mut checksum = crc32fast::Hasher::new();
    for (ordinal, source) in sources.iter().enumerate() {
        let key_section = sections[ordinal * 2];
        let mut entry = [0_u8; INDEX_ACCELERATOR_ENTRY_BYTES];
        entry[..16].copy_from_slice(source.logical_index_id().as_bytes());
        put_u16(&mut entry, 16, source.kind().tag());
        put_u16(&mut entry, 18, ACCELERATOR_VERSION);
        put_u32(
            &mut entry,
            20,
            u32::from(source.unique()) | (u32::from(source.constraint_owned()) << 1),
        );
        entry[24..56].copy_from_slice(source.definition_sha256());
        put_u32(&mut entry, 56, source.key_columns().len() as u32);
        put_u32(&mut entry, 64, (ordinal * 2) as u32);
        put_u32(&mut entry, 68, 2);
        put_u64(&mut entry, 72, 0);
        put_u64(&mut entry, 80, key_section.stored_length);
        entry[88..104].copy_from_slice(header.data_artifact_id().as_bytes());
        put_u64(&mut entry, 104, source.indexed_item_count());
        write_directory_entry(output, &mut checksum, &entry)?;
    }
    for section in sections {
        let mut entry = [0_u8; INDEX_SECTION_ENTRY_BYTES];
        put_u16(&mut entry, 0, section.kind.tag());
        put_u16(&mut entry, 2, SECTION_VERSION);
        put_u32(&mut entry, 8, section.accelerator_ordinal);
        put_u32(&mut entry, 12, section.first_page_index);
        put_u32(&mut entry, 16, section.page_count);
        put_u64(&mut entry, 24, section.offset);
        put_u64(&mut entry, 32, section.stored_length);
        put_u64(&mut entry, 40, section.logical_length);
        put_u64(&mut entry, 48, section.item_count);
        put_u32(&mut entry, 56, section.stored_crc32);
        write_directory_entry(output, &mut checksum, &entry)?;
    }
    for page in pages {
        let mut entry = [0_u8; INDEX_PAGE_ENTRY_BYTES];
        put_u32(&mut entry, 0, page.section_index());
        put_u32(&mut entry, 4, page.page_ordinal());
        put_u64(&mut entry, 8, page.offset());
        put_u64(&mut entry, 16, page.stored_length());
        put_u64(&mut entry, 24, page.logical_length());
        put_u64(&mut entry, 32, page.item_count());
        put_u64(&mut entry, 40, page.minimum_key_hash());
        put_u64(&mut entry, 48, page.maximum_key_hash());
        put_u32(&mut entry, 56, page.stored_crc32());
        put_u32(&mut entry, 60, page.codec().flags());
        write_directory_entry(output, &mut checksum, &entry)?;
    }
    Ok(checksum.finalize())
}

fn write_directory_entry(
    output: &mut impl Write,
    checksum: &mut crc32fast::Hasher,
    entry: &[u8],
) -> FormatResult<()> {
    write_index(output, entry, "write index directory")?;
    checksum.update(entry);
    Ok(())
}

fn align(
    output: &mut impl Write,
    position: &mut u64,
    checksum: Option<&mut crc32fast::Hasher>,
) -> FormatResult<()> {
    let aligned = position
        .checked_add(7)
        .map(|value| value & !7)
        .ok_or_else(|| invalid("index output alignment overflows"))?;
    let padding = aligned - *position;
    if padding != 0 {
        let zeros = [0_u8; 8];
        write_index(output, &zeros[..padding as usize], "write index alignment")?;
        if let Some(checksum) = checksum {
            checksum.update(&zeros[..padding as usize]);
        }
    }
    *position = aligned;
    Ok(())
}

fn write_at(
    output: &mut (impl Write + Seek),
    offset: u64,
    bytes: &[u8],
    operation: &'static str,
) -> FormatResult<()> {
    output
        .seek(SeekFrom::Start(offset))
        .map_err(|error| io_error(operation, error))?;
    write_index(output, bytes, operation)
}

fn write_zeros(output: &mut impl Write, mut length: u64) -> FormatResult<()> {
    let zeros = [0_u8; 8192];
    while length != 0 {
        let chunk = usize::try_from(length.min(zeros.len() as u64))
            .expect("zero chunk is bounded by fixed buffer");
        write_index(output, &zeros[..chunk], "reserve index directories")?;
        length -= chunk as u64;
    }
    Ok(())
}

fn digest_prefix(output: &mut (impl Read + Seek), body_length: u64) -> FormatResult<[u8; 32]> {
    output
        .seek(SeekFrom::Start(0))
        .map_err(|error| io_error("rewind index for identity", error))?;
    let mut remaining = body_length;
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    let mut digest = Sha256::new();
    while remaining != 0 {
        let length = usize::try_from(remaining.min(buffer.len() as u64))
            .expect("identity chunk is bounded by fixed buffer");
        output
            .read_exact(&mut buffer[..length])
            .map_err(|error| io_error("read index for identity", error))?;
        record_diagnostic(DiagnosticEvent::IndexIdentityRead, length as u64);
        digest.update(&buffer[..length]);
        remaining -= length as u64;
    }
    Ok(digest.finalize().into())
}

fn write_index(output: &mut impl Write, bytes: &[u8], operation: &'static str) -> FormatResult<()> {
    output
        .write_all(bytes)
        .map_err(|error| io_error(operation, error))?;
    record_diagnostic(DiagnosticEvent::IndexWrite, bytes.len() as u64);
    Ok(())
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn io_error(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::ArtifactIo {
        operation,
        kind: error.kind(),
    }
}
