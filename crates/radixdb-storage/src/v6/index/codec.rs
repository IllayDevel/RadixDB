use std::mem::size_of;
use std::ops::Range;

use radixdb_catalog::ObjectId;
use radixdb_core::DataType;

use super::super::{
    ArtifactId, ArtifactKind, ArtifactRef, ArtifactSliceRef, ArtifactSource, CatalogGeneration,
    DataArtifactLayout, DatabaseGeneration, DatabaseId, FormatError, FormatResult,
    IndexSectionKind, SegmentId, MAX_ARTIFACT_FILE_BYTES,
};
use super::model::{
    invalid, limit, validate_accelerator_contract, validate_page_lengths, IndexAccelerator,
    IndexAcceleratorKind, IndexArtifactHeader, IndexArtifactInput, IndexArtifactLayout,
    IndexKeyColumn, IndexNullsOrder, IndexPage, IndexPageCodec, IndexSection, IndexSectionSpec,
    IndexSortDirection, INDEX_ACCELERATOR_ENTRY_BYTES, INDEX_FOOTER_BYTES, INDEX_HEADER_BYTES,
    INDEX_PAGE_ENTRY_BYTES, INDEX_SECTION_ENTRY_BYTES, MAX_ACCELERATORS_PER_INDEX_ARTIFACT,
    MAX_ENTRIES_PER_INDEX_PAGE, MAX_INDEX_DIRECTORY_BYTES, MAX_INDEX_PAGES, MAX_INDEX_SECTIONS,
    MAX_KEY_COLUMNS,
};
use super::source::{IndexOpenLimits, IndexOpenMetrics, OpenedIndexArtifact};

const MAGIC: [u8; 8] = *b"RDX6IDX\0";
pub(crate) const FOOTER_MAGIC: [u8; 8] = *b"RDX6END\0";
const KEY_MAGIC: [u8; 4] = *b"KEY1";
const FORMAT_MAJOR: u16 = 6;
const FORMAT_MINOR: u16 = 0;
pub(crate) const ACCELERATOR_VERSION: u16 = 1;
pub(crate) const SECTION_VERSION: u16 = 1;
const KEY_DESCRIPTOR_VERSION: u16 = 1;
const KEY_DESCRIPTOR_HEADER_BYTES: usize = 8;
const KEY_COLUMN_BYTES: usize = 24;
pub(crate) const NO_PAGE: u32 = u32::MAX;
const PAGE_DIRECTORY_CHUNK_ENTRIES: usize = 4_096;

#[derive(Debug)]
struct PreparedPage {
    stored: Vec<u8>,
    logical_length: u64,
    item_count: u64,
    minimum_key_hash: u64,
    maximum_key_hash: u64,
    codec: IndexPageCodec,
}

#[derive(Debug)]
struct PreparedSection {
    accelerator_ordinal: u32,
    kind: IndexSectionKind,
    metadata: Vec<u8>,
    pages: Vec<PreparedPage>,
    item_count: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DirectoryShape {
    pub(crate) accelerator_count: u32,
    pub(crate) section_count: u32,
    pub(crate) page_count: u64,
    pub(crate) accelerator_offset: u64,
    pub(crate) accelerator_length: u64,
    pub(crate) section_offset: u64,
    pub(crate) section_length: u64,
    pub(crate) page_offset: u64,
    pub(crate) page_length: u64,
    pub(crate) body_start: u64,
}

#[derive(Debug)]
struct RawAccelerator {
    logical_index_id: ObjectId,
    kind: IndexAcceleratorKind,
    unique: bool,
    constraint_owned: bool,
    definition_sha256: [u8; 32],
    key_column_count: u32,
    first_section_index: u32,
    section_count: u32,
    key_descriptor_offset: u64,
    key_descriptor_length: u64,
    indexed_item_count: u64,
}

pub fn encode_index_artifact(input: &IndexArtifactInput) -> FormatResult<(Vec<u8>, ArtifactRef)> {
    let (prepared, section_ranges) = prepare_sections(input)?;
    let page_count = prepared.iter().try_fold(0_u64, |total, section| {
        total
            .checked_add(section.pages.len() as u64)
            .ok_or_else(|| invalid("page count overflows"))
    })?;
    let shape = canonical_shape(
        input.accelerators().len() as u32,
        prepared.len() as u32,
        page_count,
    )?;
    let body_start = usize::try_from(shape.body_start)
        .map_err(|_| invalid("index directory does not fit this platform"))?;
    let mut output = vec![0_u8; body_start];
    let mut stored_section_ranges = Vec::with_capacity(prepared.len());
    let mut encoded_pages = Vec::with_capacity(page_count as usize);

    for (section_index, section) in prepared.iter().enumerate() {
        align_output(&mut output)?;
        let section_start = output.len();
        if section.pages.is_empty() {
            output.extend_from_slice(&section.metadata);
        } else {
            for (page_ordinal, page) in section.pages.iter().enumerate() {
                align_output(&mut output)?;
                let offset = output.len();
                output.extend_from_slice(&page.stored);
                encoded_pages.push(IndexPage::new(
                    section_index as u32,
                    page_ordinal as u32,
                    offset as u64,
                    page.stored.len() as u64,
                    page.logical_length,
                    page.item_count,
                    page.minimum_key_hash,
                    page.maximum_key_hash,
                    radixdb_core::crc32_ieee(&page.stored),
                    page.codec,
                ));
            }
        }
        stored_section_ranges.push(section_start..output.len());
    }

    let file_length = output
        .len()
        .checked_add(INDEX_FOOTER_BYTES)
        .ok_or_else(|| invalid("index file length overflows"))?;
    if file_length as u64 > MAX_ARTIFACT_FILE_BYTES {
        return Err(limit(
            "file bytes",
            file_length as u64,
            MAX_ARTIFACT_FILE_BYTES,
        ));
    }

    encode_accelerator_directory(&mut output, input, &prepared, &section_ranges, shape)?;
    encode_section_directory(
        &mut output,
        &prepared,
        &stored_section_ranges,
        &encoded_pages,
        shape,
    )?;
    encode_page_directory(&mut output, &encoded_pages, shape)?;
    let directory_crc32 =
        radixdb_core::crc32_ieee(&output[INDEX_HEADER_BYTES..shape.body_start as usize]);
    encode_header(
        &mut output[..INDEX_HEADER_BYTES],
        input.header(),
        shape,
        file_length as u64,
        directory_crc32,
    );
    let header_crc = radixdb_core::crc32_ieee(&output[..248]);
    put_u32(&mut output, 248, header_crc);

    let body_sha = radixdb_core::sha256_digest(&output);
    output.extend_from_slice(&FOOTER_MAGIC);
    output.extend_from_slice(&(file_length as u64).to_le_bytes());
    output.extend_from_slice(&body_sha);
    let reference = ArtifactRef::new(
        input.header().artifact_id(),
        ArtifactKind::Index,
        input.header().creation_generation(),
        file_length as u64,
        body_sha,
    )?;
    Ok((output, reference))
}

fn prepare_sections(
    input: &IndexArtifactInput,
) -> FormatResult<(Vec<PreparedSection>, Vec<Range<usize>>)> {
    let mut sections = Vec::new();
    let mut ranges = Vec::with_capacity(input.accelerators().len());
    for (accelerator_ordinal, accelerator) in input.accelerators().iter().enumerate() {
        let first = sections.len();
        let key_descriptor = encode_key_descriptor(accelerator.key_columns())?;
        sections.push(PreparedSection {
            accelerator_ordinal: accelerator_ordinal as u32,
            kind: IndexSectionKind::KeyDescriptor,
            metadata: key_descriptor,
            pages: Vec::new(),
            item_count: accelerator.key_columns().len() as u64,
        });
        for section in accelerator.sections() {
            sections.push(prepare_section(accelerator_ordinal as u32, section)?);
        }
        ranges.push(first..sections.len());
    }
    Ok((sections, ranges))
}

fn prepare_section(
    accelerator_ordinal: u32,
    section: &IndexSectionSpec,
) -> FormatResult<PreparedSection> {
    let mut pages = Vec::with_capacity(section.page_specs().len());
    for page in section.page_specs() {
        let stored = match page.codec() {
            IndexPageCodec::None => page.logical_bytes().to_vec(),
            IndexPageCodec::Lz4 => lz4_flex::block::compress(page.logical_bytes()),
        };
        validate_page_lengths(
            page.codec(),
            stored.len() as u64,
            page.logical_bytes().len() as u64,
        )?;
        pages.push(PreparedPage {
            stored,
            logical_length: page.logical_bytes().len() as u64,
            item_count: page.item_count(),
            minimum_key_hash: page.minimum_key_hash(),
            maximum_key_hash: page.maximum_key_hash(),
            codec: page.codec(),
        });
    }
    Ok(PreparedSection {
        accelerator_ordinal,
        kind: section.kind(),
        metadata: section.metadata_bytes().to_vec(),
        pages,
        item_count: section.item_count(),
    })
}

pub(crate) fn canonical_shape(
    accelerator_count: u32,
    section_count: u32,
    page_count: u64,
) -> FormatResult<DirectoryShape> {
    if accelerator_count == 0 || accelerator_count > MAX_ACCELERATORS_PER_INDEX_ARTIFACT {
        return Err(limit(
            "accelerator count",
            u64::from(accelerator_count),
            u64::from(MAX_ACCELERATORS_PER_INDEX_ARTIFACT),
        ));
    }
    if section_count == 0 || section_count > MAX_INDEX_SECTIONS {
        return Err(limit(
            "section count",
            u64::from(section_count),
            u64::from(MAX_INDEX_SECTIONS),
        ));
    }
    if page_count > MAX_INDEX_PAGES {
        return Err(limit("page count", page_count, MAX_INDEX_PAGES));
    }
    let accelerator_length = u64::from(accelerator_count)
        .checked_mul(INDEX_ACCELERATOR_ENTRY_BYTES as u64)
        .ok_or_else(|| invalid("accelerator directory length overflows"))?;
    let section_length = u64::from(section_count)
        .checked_mul(INDEX_SECTION_ENTRY_BYTES as u64)
        .ok_or_else(|| invalid("section directory length overflows"))?;
    let page_length = page_count
        .checked_mul(INDEX_PAGE_ENTRY_BYTES as u64)
        .ok_or_else(|| invalid("page directory length overflows"))?;
    let directory_bytes = accelerator_length
        .checked_add(section_length)
        .and_then(|value| value.checked_add(page_length))
        .ok_or_else(|| invalid("index directory byte count overflows"))?;
    if directory_bytes > MAX_INDEX_DIRECTORY_BYTES {
        return Err(limit(
            "directory bytes",
            directory_bytes,
            MAX_INDEX_DIRECTORY_BYTES,
        ));
    }
    let accelerator_offset = INDEX_HEADER_BYTES as u64;
    let section_offset = accelerator_offset + accelerator_length;
    let page_offset = section_offset + section_length;
    let body_start = page_offset + page_length;
    Ok(DirectoryShape {
        accelerator_count,
        section_count,
        page_count,
        accelerator_offset,
        accelerator_length,
        section_offset,
        section_length,
        page_offset,
        page_length,
        body_start,
    })
}

pub(crate) fn encode_header(
    output: &mut [u8],
    header: IndexArtifactHeader,
    shape: DirectoryShape,
    file_length: u64,
    directory_crc32: u32,
) {
    output[..8].copy_from_slice(&MAGIC);
    put_u16(output, 8, FORMAT_MAJOR);
    put_u16(output, 10, FORMAT_MINOR);
    put_u32(output, 12, INDEX_HEADER_BYTES as u32);
    put_u64(output, 16, file_length);
    output[24..40].copy_from_slice(header.artifact_id().as_bytes());
    output[40..56].copy_from_slice(header.database_id().as_bytes());
    output[56..72].copy_from_slice(header.table_id().as_bytes());
    output[72..88].copy_from_slice(header.segment_id().as_bytes());
    output[88..104].copy_from_slice(header.data_artifact_id().as_bytes());
    output[104..136].copy_from_slice(header.data_body_sha256());
    put_u64(output, 136, header.creation_generation().get());
    put_u64(output, 144, header.catalog_generation().get());
    put_u32(output, 152, shape.accelerator_count);
    put_u32(output, 156, shape.section_count);
    put_u64(output, 160, shape.page_count);
    put_u64(output, 168, shape.accelerator_offset);
    put_u64(output, 176, shape.accelerator_length);
    put_u64(output, 184, shape.section_offset);
    put_u64(output, 192, shape.section_length);
    put_u64(output, 200, shape.page_offset);
    put_u64(output, 208, shape.page_length);
    put_u32(output, 224, directory_crc32);
}

fn encode_accelerator_directory(
    output: &mut [u8],
    input: &IndexArtifactInput,
    prepared: &[PreparedSection],
    section_ranges: &[Range<usize>],
    shape: DirectoryShape,
) -> FormatResult<()> {
    let start = shape.accelerator_offset as usize;
    for (index, accelerator) in input.accelerators().iter().enumerate() {
        let offset = start + index * INDEX_ACCELERATOR_ENTRY_BYTES;
        let entry = &mut output[offset..offset + INDEX_ACCELERATOR_ENTRY_BYTES];
        let sections = &section_ranges[index];
        let key = &prepared[sections.start];
        entry[..16].copy_from_slice(accelerator.logical_index_id().as_bytes());
        put_u16(entry, 16, accelerator.kind().tag());
        put_u16(entry, 18, ACCELERATOR_VERSION);
        let flags =
            u32::from(accelerator.unique()) | (u32::from(accelerator.constraint_owned()) << 1);
        put_u32(entry, 20, flags);
        entry[24..56].copy_from_slice(accelerator.definition_sha256());
        put_u32(entry, 56, accelerator.key_columns().len() as u32);
        put_u32(entry, 64, sections.start as u32);
        put_u32(entry, 68, sections.len() as u32);
        put_u64(entry, 72, 0);
        put_u64(entry, 80, key.metadata.len() as u64);
        entry[88..104].copy_from_slice(input.header().data_artifact_id().as_bytes());
        put_u64(entry, 104, accelerator.indexed_item_count());
    }
    Ok(())
}

fn encode_section_directory(
    output: &mut [u8],
    sections: &[PreparedSection],
    ranges: &[Range<usize>],
    pages: &[IndexPage],
    shape: DirectoryShape,
) -> FormatResult<()> {
    let start = shape.section_offset as usize;
    let mut first_page = 0_usize;
    for (index, section) in sections.iter().enumerate() {
        let entry_offset = start + index * INDEX_SECTION_ENTRY_BYTES;
        let range = &ranges[index];
        let stored_crc32 = radixdb_core::crc32_ieee(&output[range.clone()]);
        let entry = &mut output[entry_offset..entry_offset + INDEX_SECTION_ENTRY_BYTES];
        put_u16(entry, 0, section.kind.tag());
        put_u16(entry, 2, SECTION_VERSION);
        put_u32(entry, 8, section.accelerator_ordinal);
        if section.pages.is_empty() {
            put_u32(entry, 12, NO_PAGE);
        } else {
            put_u32(entry, 12, first_page as u32);
            put_u32(entry, 16, section.pages.len() as u32);
        }
        put_u64(entry, 24, range.start as u64);
        put_u64(entry, 32, range.len() as u64);
        let logical_length = if section.pages.is_empty() {
            range.len() as u64
        } else {
            pages[first_page..first_page + section.pages.len()]
                .iter()
                .map(|page| page.logical_length())
                .sum()
        };
        put_u64(entry, 40, logical_length);
        put_u64(entry, 48, section.item_count);
        put_u32(entry, 56, stored_crc32);
        first_page += section.pages.len();
    }
    if first_page != pages.len() {
        return Err(invalid("encoded page ownership is inconsistent"));
    }
    Ok(())
}

fn encode_page_directory(
    output: &mut [u8],
    pages: &[IndexPage],
    shape: DirectoryShape,
) -> FormatResult<()> {
    let start = shape.page_offset as usize;
    for (index, page) in pages.iter().copied().enumerate() {
        let offset = start + index * INDEX_PAGE_ENTRY_BYTES;
        let entry = &mut output[offset..offset + INDEX_PAGE_ENTRY_BYTES];
        put_u32(entry, 0, page.section_index());
        put_u32(entry, 4, page.page_ordinal());
        put_u64(entry, 8, page.offset());
        put_u64(entry, 16, page.stored_length());
        put_u64(entry, 24, page.logical_length());
        put_u64(entry, 32, page.item_count());
        put_u64(entry, 40, page.minimum_key_hash());
        put_u64(entry, 48, page.maximum_key_hash());
        put_u32(entry, 56, page.stored_crc32());
        put_u32(entry, 60, page.codec().flags());
    }
    Ok(())
}

pub fn decode_index_artifact_layout(
    bytes: &[u8],
    expected: ArtifactRef,
    data: &DataArtifactLayout,
) -> FormatResult<IndexArtifactLayout> {
    open_index_artifact_metadata(bytes, expected, data).map(OpenedIndexArtifact::into_layout)
}

pub fn open_index_artifact_metadata(
    source: &(impl ArtifactSource + ?Sized),
    expected: ArtifactRef,
    data: &DataArtifactLayout,
) -> FormatResult<OpenedIndexArtifact> {
    open_index_artifact_metadata_with_limits(source, expected, data, IndexOpenLimits::default())
}

pub fn open_index_artifact_metadata_with_limits(
    source: &(impl ArtifactSource + ?Sized),
    expected: ArtifactRef,
    data: &DataArtifactLayout,
    limits: IndexOpenLimits,
) -> FormatResult<OpenedIndexArtifact> {
    let file_length = source.byte_length()?;
    validate_source_length(file_length, expected)?;
    let footer_start = file_length - INDEX_FOOTER_BYTES as u64;
    let mut metrics = IndexOpenMetrics::default();
    let mut header_bytes = [0_u8; INDEX_HEADER_BYTES];
    read_source(source, 0, &mut header_bytes, &mut metrics)?;
    let mut footer = [0_u8; INDEX_FOOTER_BYTES];
    read_source(source, footer_start, &mut footer, &mut metrics)?;
    validate_footer(&footer, file_length, expected)?;
    let (header, shape, directory_crc32) =
        decode_header(&header_bytes, file_length, expected, data)?;

    let fixed_allocation = u64::from(shape.accelerator_count)
        .checked_mul((size_of::<RawAccelerator>() + size_of::<IndexAccelerator>()) as u64)
        .and_then(|value| {
            value.checked_add(u64::from(shape.section_count) * size_of::<IndexSection>() as u64)
        })
        .and_then(|value| value.checked_add(shape.accelerator_length))
        .and_then(|value| value.checked_add(shape.section_length))
        .ok_or_else(|| invalid("metadata allocation accounting overflows"))?;
    metrics.account_allocation(fixed_allocation, limits)?;

    let mut directory_hasher = crc32fast::Hasher::new();
    let mut accelerator_bytes = vec![0_u8; shape.accelerator_length as usize];
    read_source(
        source,
        shape.accelerator_offset,
        &mut accelerator_bytes,
        &mut metrics,
    )?;
    directory_hasher.update(&accelerator_bytes);
    let raw_accelerators = decode_accelerators(&accelerator_bytes, shape, &header)?;
    drop(accelerator_bytes);

    let mut section_bytes = vec![0_u8; shape.section_length as usize];
    read_source(
        source,
        shape.section_offset,
        &mut section_bytes,
        &mut metrics,
    )?;
    directory_hasher.update(&section_bytes);
    let sections = decode_sections(&section_bytes, shape, expected, footer_start)?;
    drop(section_bytes);

    let retained_key_columns = raw_accelerators
        .iter()
        .try_fold(0_u64, |total, accelerator| {
            total
                .checked_add(u64::from(accelerator.key_column_count))
                .ok_or_else(|| invalid("key-column allocation accounting overflows"))
        })?;
    let page_chunk_bytes = shape
        .page_length
        .min((PAGE_DIRECTORY_CHUNK_ENTRIES * INDEX_PAGE_ENTRY_BYTES) as u64);
    let variable_allocation = shape
        .page_count
        .checked_mul(size_of::<IndexPage>() as u64)
        .and_then(|value| {
            value.checked_add(retained_key_columns * size_of::<IndexKeyColumn>() as u64)
        })
        .and_then(|value| value.checked_add(page_chunk_bytes))
        .ok_or_else(|| invalid("page allocation accounting overflows"))?;
    metrics.account_allocation(variable_allocation, limits)?;
    let pages = decode_pages_from_source(
        source,
        shape,
        &sections,
        &mut metrics,
        &mut directory_hasher,
    )?;
    if directory_hasher.finalize() != directory_crc32 {
        return Err(FormatError::IndexArtifactChecksumMismatch { scope: "directory" });
    }
    validate_ownership(&raw_accelerators, &sections, &pages)?;

    let mut accelerators = Vec::with_capacity(raw_accelerators.len());
    for raw in raw_accelerators {
        let key_section = sections
            .get(raw.first_section_index as usize)
            .copied()
            .ok_or_else(|| invalid("key descriptor section is out of range"))?;
        let descriptor_allocation = usize::try_from(key_section.reference().stored_length())
            .map_err(|_| invalid("key descriptor length does not fit this platform"))?
            .checked_add(raw.key_column_count as usize * size_of::<IndexKeyColumn>())
            .ok_or_else(|| invalid("key descriptor allocation accounting overflows"))?;
        metrics.account_allocation(descriptor_allocation as u64, limits)?;
        let descriptor_bytes = read_section_source(source, key_section, &mut metrics)?;
        if raw.key_descriptor_offset != 0
            || raw.key_descriptor_length != descriptor_bytes.len() as u64
        {
            return Err(invalid("key descriptor BlobRef is not canonical"));
        }
        let key_columns = decode_key_descriptor(&descriptor_bytes, raw.key_column_count, data)?;
        validate_accelerator_contract(raw.kind, raw.unique, raw.constraint_owned, &key_columns)?;
        if raw.indexed_item_count == 0
            && !key_columns.iter().any(|key| {
                data.columns()
                    .iter()
                    .any(|column| column.column_id() == key.column_id() && column.nullable())
            })
        {
            return Err(invalid(
                "zero-entry UNIQUE accelerator has no nullable source key column",
            ));
        }
        accelerators.push(IndexAccelerator::new(
            raw.logical_index_id,
            raw.kind,
            raw.unique,
            raw.constraint_owned,
            raw.definition_sha256,
            key_columns,
            raw.first_section_index,
            raw.section_count,
            raw.indexed_item_count,
        ));
    }

    let cache_bytes = shape
        .page_count
        .checked_mul(super::exact_validation::EXACT_VALIDATION_SLOT_BYTES as u64)
        .ok_or_else(|| invalid("exact validation cache accounting overflows"))?;
    let cache_pages =
        if cache_bytes <= limits.max_accounted_bytes() - metrics.accounted_allocation_bytes() {
            metrics.account_allocation(cache_bytes, limits)?;
            pages.len()
        } else {
            0
        };
    Ok(OpenedIndexArtifact::new(
        IndexArtifactLayout::new(expected, header, accelerators, sections, pages, cache_pages),
        metrics,
    ))
}

fn decode_header(
    bytes: &[u8; INDEX_HEADER_BYTES],
    file_length: u64,
    expected: ArtifactRef,
    data: &DataArtifactLayout,
) -> FormatResult<(IndexArtifactHeader, DirectoryShape, u32)> {
    if bytes[..8] != MAGIC {
        return Err(invalid("magic mismatch"));
    }
    let major = read_u16(bytes, 8);
    let minor = read_u16(bytes, 10);
    if (major, minor) != (FORMAT_MAJOR, FORMAT_MINOR) {
        return Err(FormatError::UnsupportedFormatVersion {
            owner: "index artifact",
            major,
            minor,
        });
    }
    if read_u32(bytes, 12) != INDEX_HEADER_BYTES as u32 {
        return Err(invalid("header length is not 256"));
    }
    if read_u64(bytes, 16) != file_length {
        return Err(invalid("header file length mismatch"));
    }
    if read_u64(bytes, 216) != 0 {
        return Err(invalid("unknown index header flags"));
    }
    require_zero(bytes, 228..248, "reserved header bytes are non-zero")?;
    require_zero(bytes, 252..256, "reserved header trailer is non-zero")?;
    if read_u32(bytes, 248) != radixdb_core::crc32_ieee(&bytes[..248]) {
        return Err(FormatError::IndexArtifactChecksumMismatch { scope: "header" });
    }
    let shape = canonical_shape(
        read_u32(bytes, 152),
        read_u32(bytes, 156),
        read_u64(bytes, 160),
    )?;
    if read_u64(bytes, 168) != shape.accelerator_offset
        || read_u64(bytes, 176) != shape.accelerator_length
        || read_u64(bytes, 184) != shape.section_offset
        || read_u64(bytes, 192) != shape.section_length
        || read_u64(bytes, 200) != shape.page_offset
        || read_u64(bytes, 208) != shape.page_length
        || shape.body_start > file_length - INDEX_FOOTER_BYTES as u64
    {
        return Err(invalid("index directories are not canonical"));
    }
    let artifact_id = ArtifactId::from_bytes(read_array(bytes, 24))?;
    let database_id = DatabaseId::from_bytes(read_array(bytes, 40))?;
    let table_id = ObjectId::from_user_bytes(read_array(bytes, 56))
        .map_err(|_| invalid("table ID is not a user catalog identity"))?;
    let segment_id = SegmentId::from_bytes(read_array(bytes, 72))?;
    let data_artifact_id = ArtifactId::from_bytes(read_array(bytes, 88))?;
    let data_body_sha256 = read_array(bytes, 104);
    let creation_generation = DatabaseGeneration::new(read_u64(bytes, 136))?;
    let catalog_generation = CatalogGeneration::new(read_u64(bytes, 144))?;
    if artifact_id != expected.id() || creation_generation != expected.creation_generation() {
        return Err(invalid("index header differs from manifest reference"));
    }
    if database_id != data.header().database_id()
        || table_id != data.header().table_id()
        || segment_id != data.header().segment_id()
        || data_artifact_id != data.reference().id()
        || data_body_sha256 != *data.reference().body_sha256()
    {
        return Err(invalid("index header differs from source data identity"));
    }
    Ok((
        IndexArtifactHeader::decoded(
            artifact_id,
            database_id,
            table_id,
            segment_id,
            data_artifact_id,
            data_body_sha256,
            creation_generation,
            catalog_generation,
            data.header().row_count(),
        ),
        shape,
        read_u32(bytes, 224),
    ))
}

fn decode_accelerators(
    bytes: &[u8],
    shape: DirectoryShape,
    header: &IndexArtifactHeader,
) -> FormatResult<Vec<RawAccelerator>> {
    let mut accelerators = Vec::with_capacity(shape.accelerator_count as usize);
    let mut next_section = 0_u32;
    for index in 0..shape.accelerator_count as usize {
        let offset = index * INDEX_ACCELERATOR_ENTRY_BYTES;
        let entry = &bytes[offset..offset + INDEX_ACCELERATOR_ENTRY_BYTES];
        let logical_index_id = ObjectId::from_user_bytes(read_array(entry, 0))
            .map_err(|_| invalid("logical index ID is not a user catalog identity"))?;
        if accelerators
            .last()
            .is_some_and(|previous: &RawAccelerator| previous.logical_index_id >= logical_index_id)
        {
            return Err(invalid("accelerator directory is not strictly sorted"));
        }
        let version = read_u16(entry, 18);
        if version != ACCELERATOR_VERSION {
            return Err(FormatError::UnsupportedIndexAcceleratorVersion { version });
        }
        let flags = read_u32(entry, 20);
        if flags & !3 != 0 {
            return Err(invalid("accelerator has unknown flags"));
        }
        let kind = IndexAcceleratorKind::from_tag(read_u16(entry, 16))?;
        let unique = flags & 1 != 0;
        let constraint_owned = flags & 2 != 0;
        let key_column_count = read_u32(entry, 56);
        if key_column_count == 0 || key_column_count > MAX_KEY_COLUMNS {
            return Err(limit(
                "key columns",
                u64::from(key_column_count),
                u64::from(MAX_KEY_COLUMNS),
            ));
        }
        if read_u32(entry, 60) != 0 {
            return Err(invalid("included columns are not admitted"));
        }
        let first_section_index = read_u32(entry, 64);
        let section_count = read_u32(entry, 68);
        if section_count == 0 || first_section_index != next_section {
            return Err(invalid("accelerator section ownership is not contiguous"));
        }
        next_section = next_section
            .checked_add(section_count)
            .ok_or_else(|| invalid("accelerator section range overflows"))?;
        if next_section > shape.section_count {
            return Err(invalid("accelerator section range is outside directory"));
        }
        if entry[88..104] != *header.data_artifact_id().as_bytes() {
            return Err(invalid("accelerator source data identity mismatch"));
        }
        let indexed_item_count = read_u64(entry, 104);
        if indexed_item_count > header.source_row_count()
            || (indexed_item_count == 0 && (kind != IndexAcceleratorKind::Exact || !unique))
        {
            return Err(invalid("indexed item count is outside source rows"));
        }
        require_zero(entry, 112..128, "accelerator reserved bytes are non-zero")?;
        accelerators.push(RawAccelerator {
            logical_index_id,
            kind,
            unique,
            constraint_owned,
            definition_sha256: read_array(entry, 24),
            key_column_count,
            first_section_index,
            section_count,
            key_descriptor_offset: read_u64(entry, 72),
            key_descriptor_length: read_u64(entry, 80),
            indexed_item_count,
        });
    }
    if next_section != shape.section_count {
        return Err(invalid("not all sections are owned by accelerators"));
    }
    Ok(accelerators)
}

fn decode_sections(
    bytes: &[u8],
    shape: DirectoryShape,
    expected: ArtifactRef,
    footer_start: u64,
) -> FormatResult<Vec<IndexSection>> {
    let mut sections = Vec::with_capacity(shape.section_count as usize);
    let mut body_cursor = shape.body_start;
    let mut previous_key = None;
    for index in 0..shape.section_count as usize {
        let offset = index * INDEX_SECTION_ENTRY_BYTES;
        let entry = &bytes[offset..offset + INDEX_SECTION_ENTRY_BYTES];
        let kind = IndexSectionKind::from_tag(read_u16(entry, 0))?;
        let version = read_u16(entry, 2);
        if version != SECTION_VERSION {
            return Err(FormatError::UnsupportedIndexSectionVersion { version });
        }
        if read_u32(entry, 4) != 0 {
            return Err(invalid("index section has unknown flags"));
        }
        let accelerator_ordinal = read_u32(entry, 8);
        if accelerator_ordinal >= shape.accelerator_count {
            return Err(invalid("section accelerator ordinal is out of range"));
        }
        let key = (accelerator_ordinal, kind.tag());
        if previous_key.is_some_and(|previous| previous >= key) {
            return Err(invalid("section directory is not strictly sorted"));
        }
        previous_key = Some(key);
        let first_page_index = read_u32(entry, 12);
        let page_count = read_u32(entry, 16);
        require_zero(entry, 20..24, "section reserved bytes are non-zero")?;
        require_zero(entry, 60..64, "section trailer is non-zero")?;
        let section_offset = read_u64(entry, 24);
        let stored_length = read_u64(entry, 32);
        let logical_length = read_u64(entry, 40);
        let item_count = read_u64(entry, 48);
        if stored_length == 0 || logical_length == 0 {
            return Err(invalid("index section cannot be empty"));
        }
        validate_canonical_range(
            &mut body_cursor,
            footer_start,
            section_offset,
            stored_length,
            "index section range is not canonical",
        )?;
        let metadata = matches!(
            kind,
            IndexSectionKind::KeyDescriptor | IndexSectionKind::HnswMetadata
        );
        if metadata {
            if first_page_index != NO_PAGE || page_count != 0 || logical_length != stored_length {
                return Err(invalid("metadata section page ownership is invalid"));
            }
        } else {
            if page_count == 0 || first_page_index == NO_PAGE {
                return Err(invalid("paged section has no page range"));
            }
            let page_end = u64::from(first_page_index)
                .checked_add(u64::from(page_count))
                .ok_or_else(|| invalid("section page range overflows"))?;
            if page_end > shape.page_count {
                return Err(invalid("section page range is outside directory"));
            }
        }
        let reference = ArtifactSliceRef::new(
            expected,
            index as u32,
            kind,
            version,
            section_offset,
            stored_length,
            logical_length,
            item_count,
            read_u32(entry, 56),
        )?;
        sections.push(IndexSection::new(
            reference,
            accelerator_ordinal,
            first_page_index,
            page_count,
        ));
    }
    if body_cursor != footer_start {
        return Err(invalid(
            "index artifact has bytes outside declared sections",
        ));
    }
    Ok(sections)
}

fn decode_pages_from_source(
    source: &(impl ArtifactSource + ?Sized),
    shape: DirectoryShape,
    sections: &[IndexSection],
    metrics: &mut IndexOpenMetrics,
    directory_hasher: &mut crc32fast::Hasher,
) -> FormatResult<Vec<IndexPage>> {
    let page_capacity = usize::try_from(shape.page_count)
        .map_err(|_| invalid("page count does not fit this platform"))?;
    let mut pages = Vec::with_capacity(page_capacity);
    let mut previous_key = None;
    let chunk_entries = page_capacity.min(PAGE_DIRECTORY_CHUNK_ENTRIES);
    let mut buffer = vec![
        0_u8;
        chunk_entries
            .checked_mul(INDEX_PAGE_ENTRY_BYTES)
            .ok_or_else(|| invalid("page directory chunk length overflows"))?
    ];
    let mut first_index = 0_usize;
    while first_index < page_capacity {
        let entry_count = (page_capacity - first_index).min(PAGE_DIRECTORY_CHUNK_ENTRIES);
        let byte_count = entry_count
            .checked_mul(INDEX_PAGE_ENTRY_BYTES)
            .ok_or_else(|| invalid("page directory chunk length overflows"))?;
        let byte_offset = first_index
            .checked_mul(INDEX_PAGE_ENTRY_BYTES)
            .ok_or_else(|| invalid("page directory source offset overflows"))?;
        let source_offset = shape
            .page_offset
            .checked_add(byte_offset as u64)
            .ok_or_else(|| invalid("page directory source offset overflows"))?;
        read_source(source, source_offset, &mut buffer[..byte_count], metrics)?;
        directory_hasher.update(&buffer[..byte_count]);
        for local_index in 0..entry_count {
            let entry_offset = local_index * INDEX_PAGE_ENTRY_BYTES;
            let entry = &buffer[entry_offset..entry_offset + INDEX_PAGE_ENTRY_BYTES];
            pages.push(decode_page_entry(entry, sections, &mut previous_key)?);
        }
        first_index += entry_count;
    }
    validate_page_ranges(sections, &pages)?;
    Ok(pages)
}

fn decode_page_entry(
    entry: &[u8],
    sections: &[IndexSection],
    previous_key: &mut Option<(u32, u32)>,
) -> FormatResult<IndexPage> {
    let section_index = read_u32(entry, 0);
    let page_ordinal = read_u32(entry, 4);
    let section = sections
        .get(section_index as usize)
        .copied()
        .ok_or_else(|| invalid("page section index is out of range"))?;
    if section.page_count() == 0 {
        return Err(invalid("metadata section owns an index page"));
    }
    let key = (section_index, page_ordinal);
    if previous_key.is_some_and(|previous| previous >= key) {
        return Err(invalid("page directory is not strictly sorted"));
    }
    *previous_key = Some(key);
    let codec = IndexPageCodec::from_flags(read_u32(entry, 60))?;
    let stored_length = read_u64(entry, 16);
    let logical_length = read_u64(entry, 24);
    validate_page_lengths(codec, stored_length, logical_length)?;
    let item_count = read_u64(entry, 32);
    if item_count > MAX_ENTRIES_PER_INDEX_PAGE {
        return Err(limit(
            "entries per page",
            item_count,
            MAX_ENTRIES_PER_INDEX_PAGE,
        ));
    }
    let minimum_key_hash = read_u64(entry, 40);
    let maximum_key_hash = read_u64(entry, 48);
    if item_count == 0
        && (section.kind() != IndexSectionKind::ExactPages
            || section.reference().item_count() != 0
            || minimum_key_hash != 0
            || maximum_key_hash != 0)
    {
        return Err(invalid(
            "zero-entry index page is not canonical exact state",
        ));
    }
    if maximum_key_hash < minimum_key_hash {
        return Err(invalid("page key-hash range is reversed"));
    }
    Ok(IndexPage::new(
        section_index,
        page_ordinal,
        read_u64(entry, 8),
        stored_length,
        logical_length,
        item_count,
        minimum_key_hash,
        maximum_key_hash,
        read_u32(entry, 56),
        codec,
    ))
}

fn validate_page_ranges(sections: &[IndexSection], pages: &[IndexPage]) -> FormatResult<()> {
    let mut next_page = 0_u32;
    for (section_index, section) in sections.iter().copied().enumerate() {
        if section.page_count() == 0 {
            continue;
        }
        if section.first_page_index() != next_page {
            return Err(invalid("section page ownership is not contiguous"));
        }
        let start = next_page as usize;
        let end = start
            .checked_add(section.page_count() as usize)
            .ok_or_else(|| invalid("section page slice overflows"))?;
        let owned = pages
            .get(start..end)
            .ok_or_else(|| invalid("section page slice is outside directory"))?;
        let section_ref = section.reference();
        let mut cursor = section_ref.offset();
        let section_end = section_ref
            .offset()
            .checked_add(section_ref.stored_length())
            .ok_or_else(|| invalid("section range overflows"))?;
        let mut item_count = 0_u64;
        let mut logical_length = 0_u64;
        for (ordinal, page) in owned.iter().copied().enumerate() {
            if page.section_index() != section_index as u32 || page.page_ordinal() != ordinal as u32
            {
                return Err(invalid("page ownership or ordinal is not canonical"));
            }
            validate_canonical_range(
                &mut cursor,
                section_end,
                page.offset(),
                page.stored_length(),
                "page range is not canonical inside section",
            )?;
            item_count = item_count
                .checked_add(page.item_count())
                .ok_or_else(|| invalid("section page item count overflows"))?;
            logical_length = logical_length
                .checked_add(page.logical_length())
                .ok_or_else(|| invalid("section logical length overflows"))?;
        }
        if cursor != section_end
            || item_count != section_ref.item_count()
            || logical_length != section_ref.logical_length()
        {
            return Err(invalid("paged section totals do not match its pages"));
        }
        next_page = next_page
            .checked_add(section.page_count())
            .ok_or_else(|| invalid("global page ownership overflows"))?;
    }
    if next_page as usize != pages.len() {
        return Err(invalid("not all pages are owned by sections"));
    }
    Ok(())
}

fn validate_ownership(
    accelerators: &[RawAccelerator],
    sections: &[IndexSection],
    pages: &[IndexPage],
) -> FormatResult<()> {
    for (ordinal, accelerator) in accelerators.iter().enumerate() {
        let start = accelerator.first_section_index as usize;
        let end = start
            .checked_add(accelerator.section_count as usize)
            .ok_or_else(|| invalid("accelerator section slice overflows"))?;
        let owned = sections
            .get(start..end)
            .ok_or_else(|| invalid("accelerator section slice is outside directory"))?;
        if owned
            .iter()
            .any(|section| section.accelerator_ordinal() != ordinal as u32)
        {
            return Err(invalid("section is owned by the wrong accelerator"));
        }
        if owned.first().map(|section| section.kind()) != Some(IndexSectionKind::KeyDescriptor) {
            return Err(invalid(
                "accelerator does not start with one key descriptor",
            ));
        }
        if owned[0].reference().item_count() != u64::from(accelerator.key_column_count) {
            return Err(invalid("key descriptor item count mismatch"));
        }
        let actual = owned
            .iter()
            .map(|section| section.kind())
            .collect::<Vec<_>>();
        let expected: &[IndexSectionKind] = match accelerator.kind {
            IndexAcceleratorKind::Exact => &[
                IndexSectionKind::KeyDescriptor,
                IndexSectionKind::ExactPages,
            ],
            IndexAcceleratorKind::Ordered => &[
                IndexSectionKind::KeyDescriptor,
                IndexSectionKind::OrderedPages,
            ],
            IndexAcceleratorKind::Hnsw => &[
                IndexSectionKind::KeyDescriptor,
                IndexSectionKind::HnswMetadata,
                IndexSectionKind::HnswNodes,
                IndexSectionKind::HnswAdjacency,
            ],
        };
        if actual != expected {
            return Err(invalid("accelerator section set does not match its kind"));
        }
        if accelerator.indexed_item_count == 0 {
            let exact = owned
                .get(1)
                .copied()
                .ok_or_else(|| invalid("empty exact page section is absent"))?;
            let first_page = exact.first_page_index() as usize;
            let canonical_empty = accelerator.kind == IndexAcceleratorKind::Exact
                && accelerator.unique
                && exact.page_count() == 1
                && exact.reference().item_count() == 0
                && pages
                    .get(first_page)
                    .is_some_and(|page| page.item_count() == 0);
            if !canonical_empty {
                return Err(invalid(
                    "zero-entry accelerator is not canonical nullable UNIQUE exact state",
                ));
            }
        }
        if owned.iter().any(|section| {
            matches!(
                section.kind(),
                IndexSectionKind::ExactPages
                    | IndexSectionKind::OrderedPages
                    | IndexSectionKind::HnswNodes
            ) && section.reference().item_count() > accelerator.indexed_item_count
        }) {
            return Err(invalid(
                "primary index section item count exceeds indexed items",
            ));
        }
    }
    validate_page_ranges(sections, pages)
}

pub(crate) fn encode_key_descriptor(columns: &[IndexKeyColumn]) -> FormatResult<Vec<u8>> {
    let length = KEY_DESCRIPTOR_HEADER_BYTES
        .checked_add(
            columns
                .len()
                .checked_mul(KEY_COLUMN_BYTES)
                .ok_or_else(|| invalid("key descriptor length overflows"))?,
        )
        .ok_or_else(|| invalid("key descriptor length overflows"))?;
    let mut output = vec![0_u8; length];
    output[..4].copy_from_slice(&KEY_MAGIC);
    put_u16(&mut output, 4, KEY_DESCRIPTOR_VERSION);
    put_u16(&mut output, 6, columns.len() as u16);
    for (index, column) in columns.iter().copied().enumerate() {
        let offset = KEY_DESCRIPTOR_HEADER_BYTES + index * KEY_COLUMN_BYTES;
        let entry = &mut output[offset..offset + KEY_COLUMN_BYTES];
        entry[..16].copy_from_slice(column.column_id().as_bytes());
        put_u16(entry, 16, u16::from(column.logical_type().as_u8()));
        entry[18] = column.direction().tag();
        entry[19] = column.nulls_order().tag();
    }
    Ok(output)
}

fn decode_key_descriptor(
    bytes: &[u8],
    expected_count: u32,
    data: &DataArtifactLayout,
) -> FormatResult<Vec<IndexKeyColumn>> {
    let expected_length = KEY_DESCRIPTOR_HEADER_BYTES
        .checked_add(
            (expected_count as usize)
                .checked_mul(KEY_COLUMN_BYTES)
                .ok_or_else(|| invalid("key descriptor length overflows"))?,
        )
        .ok_or_else(|| invalid("key descriptor length overflows"))?;
    if bytes.len() != expected_length || bytes[..4] != KEY_MAGIC {
        return Err(invalid("key descriptor framing mismatch"));
    }
    if read_u16(bytes, 4) != KEY_DESCRIPTOR_VERSION {
        return Err(invalid("unsupported key descriptor version"));
    }
    if u32::from(read_u16(bytes, 6)) != expected_count {
        return Err(invalid("key descriptor column count mismatch"));
    }
    let mut columns = Vec::with_capacity(expected_count as usize);
    for index in 0..expected_count as usize {
        let offset = KEY_DESCRIPTOR_HEADER_BYTES + index * KEY_COLUMN_BYTES;
        let entry = &bytes[offset..offset + KEY_COLUMN_BYTES];
        let column_id = ObjectId::from_user_bytes(read_array(entry, 0))
            .map_err(|_| invalid("key column ID is not a user catalog identity"))?;
        let type_tag = u8::try_from(read_u16(entry, 16))
            .map_err(|_| invalid("key logical type tag exceeds u8"))?;
        let logical_type = DataType::from_u8(type_tag)
            .ok_or_else(|| invalid("key logical type tag is unknown"))?;
        if read_u16(entry, 20) != 0 {
            return Err(invalid("non-binary key collation is not admitted"));
        }
        require_zero(entry, 22..24, "key descriptor reserved bytes are non-zero")?;
        let source = data
            .columns()
            .iter()
            .find(|column| column.column_id() == column_id)
            .ok_or_else(|| invalid("key descriptor column is absent from source data"))?;
        if source.data_type().logical_type() != logical_type {
            return Err(invalid("key descriptor type differs from source data"));
        }
        columns.push(IndexKeyColumn::new(
            column_id,
            logical_type,
            IndexSortDirection::from_tag(entry[18])?,
            IndexNullsOrder::from_tag(entry[19])?,
        ));
    }
    Ok(columns)
}

pub fn read_index_section<'a>(
    bytes: &'a [u8],
    layout: &IndexArtifactLayout,
    section_index: usize,
) -> FormatResult<&'a [u8]> {
    if bytes.len() as u64 != layout.reference().byte_length() {
        return Err(invalid(
            "section source length differs from artifact identity",
        ));
    }
    let section = layout
        .sections()
        .get(section_index)
        .copied()
        .ok_or_else(|| invalid("section index is out of range"))?;
    let reference = section.reference();
    let start = usize::try_from(reference.offset())
        .map_err(|_| invalid("section offset does not fit this platform"))?;
    let length = usize::try_from(reference.stored_length())
        .map_err(|_| invalid("section length does not fit this platform"))?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| invalid("section range overflows"))?;
    let stored = bytes
        .get(start..end)
        .ok_or_else(|| invalid("section range is outside artifact"))?;
    validate_section_crc(stored, section)?;
    Ok(stored)
}

pub fn read_index_section_from_source(
    source: &(impl ArtifactSource + ?Sized),
    layout: &IndexArtifactLayout,
    section_index: usize,
) -> FormatResult<Vec<u8>> {
    if source.byte_length()? != layout.reference().byte_length() {
        return Err(invalid(
            "section source length differs from artifact identity",
        ));
    }
    let section = layout
        .sections()
        .get(section_index)
        .copied()
        .ok_or_else(|| invalid("section index is out of range"))?;
    read_section_source_direct(source, section)
}

pub fn read_index_page(
    bytes: &[u8],
    layout: &IndexArtifactLayout,
    page_index: usize,
) -> FormatResult<Vec<u8>> {
    if bytes.len() as u64 != layout.reference().byte_length() {
        return Err(invalid("page source length differs from artifact identity"));
    }
    let page = layout
        .pages()
        .get(page_index)
        .copied()
        .ok_or_else(|| invalid("page index is out of range"))?;
    let start = usize::try_from(page.offset())
        .map_err(|_| invalid("page offset does not fit this platform"))?;
    let length = usize::try_from(page.stored_length())
        .map_err(|_| invalid("page length does not fit this platform"))?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| invalid("page range overflows"))?;
    let stored = bytes
        .get(start..end)
        .ok_or_else(|| invalid("page range is outside artifact"))?;
    decode_page(stored, page)
}

pub fn read_index_page_from_source(
    source: &(impl ArtifactSource + ?Sized),
    layout: &IndexArtifactLayout,
    page_index: usize,
) -> FormatResult<Vec<u8>> {
    if source.byte_length()? != layout.reference().byte_length() {
        return Err(invalid("page source length differs from artifact identity"));
    }
    let page = layout
        .pages()
        .get(page_index)
        .copied()
        .ok_or_else(|| invalid("page index is out of range"))?;
    let length = usize::try_from(page.stored_length())
        .map_err(|_| invalid("page length does not fit this platform"))?;
    let mut stored = vec![0_u8; length];
    source.read_exact_at(page.offset(), &mut stored)?;
    decode_page(&stored, page)
}

fn decode_page(stored: &[u8], page: IndexPage) -> FormatResult<Vec<u8>> {
    if radixdb_core::crc32_ieee(stored) != page.stored_crc32() {
        return Err(FormatError::IndexArtifactChecksumMismatch { scope: "page" });
    }
    match page.codec() {
        IndexPageCodec::None => Ok(stored.to_vec()),
        IndexPageCodec::Lz4 => {
            let logical_length = usize::try_from(page.logical_length())
                .map_err(|_| invalid("logical page length does not fit this platform"))?;
            lz4_flex::block::decompress(stored, logical_length)
                .map_err(|_| invalid("index page LZ4 payload is invalid"))
        }
    }
}

fn read_section_source(
    source: &(impl ArtifactSource + ?Sized),
    section: IndexSection,
    metrics: &mut IndexOpenMetrics,
) -> FormatResult<Vec<u8>> {
    let bytes = read_section_source_direct(source, section)?;
    metrics.record_read(bytes.len() as u64)?;
    Ok(bytes)
}

fn read_section_source_direct(
    source: &(impl ArtifactSource + ?Sized),
    section: IndexSection,
) -> FormatResult<Vec<u8>> {
    let reference = section.reference();
    let length = usize::try_from(reference.stored_length())
        .map_err(|_| invalid("section length does not fit this platform"))?;
    let mut bytes = vec![0_u8; length];
    source.read_exact_at(reference.offset(), &mut bytes)?;
    validate_section_crc(&bytes, section)?;
    Ok(bytes)
}

fn validate_section_crc(bytes: &[u8], section: IndexSection) -> FormatResult<()> {
    if radixdb_core::crc32_ieee(bytes) != section.reference().stored_crc32() {
        return Err(FormatError::IndexArtifactChecksumMismatch { scope: "section" });
    }
    Ok(())
}

fn validate_source_length(file_length: u64, expected: ArtifactRef) -> FormatResult<()> {
    if expected.kind() != ArtifactKind::Index {
        return Err(invalid("expected reference is not an index artifact"));
    }
    let minimum = (INDEX_HEADER_BYTES
        + INDEX_ACCELERATOR_ENTRY_BYTES
        + 2 * INDEX_SECTION_ENTRY_BYTES
        + INDEX_PAGE_ENTRY_BYTES
        + INDEX_FOOTER_BYTES) as u64;
    if file_length < minimum {
        return Err(invalid("file is shorter than minimum index container"));
    }
    if file_length > MAX_ARTIFACT_FILE_BYTES {
        return Err(limit("file bytes", file_length, MAX_ARTIFACT_FILE_BYTES));
    }
    if file_length != expected.byte_length() {
        return Err(invalid("file length differs from manifest reference"));
    }
    Ok(())
}

fn validate_footer(
    footer: &[u8; INDEX_FOOTER_BYTES],
    file_length: u64,
    expected: ArtifactRef,
) -> FormatResult<()> {
    if footer[..8] != FOOTER_MAGIC {
        return Err(invalid("footer magic mismatch"));
    }
    if read_u64(footer, 8) != file_length {
        return Err(invalid("footer file length mismatch"));
    }
    if footer[16..] != *expected.body_sha256() {
        return Err(FormatError::IndexArtifactChecksumMismatch {
            scope: "footer identity",
        });
    }
    Ok(())
}

fn read_source(
    source: &(impl ArtifactSource + ?Sized),
    offset: u64,
    destination: &mut [u8],
    metrics: &mut IndexOpenMetrics,
) -> FormatResult<()> {
    if destination.is_empty() {
        return Ok(());
    }
    source.read_exact_at(offset, destination)?;
    metrics.record_read(destination.len() as u64)
}

fn validate_canonical_range(
    cursor: &mut u64,
    boundary: u64,
    offset: u64,
    length: u64,
    detail: &'static str,
) -> FormatResult<()> {
    let aligned = cursor
        .checked_add(7)
        .map(|value| value & !7)
        .ok_or_else(|| invalid("range alignment overflows"))?;
    let end = offset.checked_add(length).ok_or_else(|| invalid(detail))?;
    if offset != aligned || !offset.is_multiple_of(8) || end > boundary {
        return Err(invalid(detail));
    }
    *cursor = end;
    Ok(())
}

fn align_output(output: &mut Vec<u8>) -> FormatResult<()> {
    let aligned = output
        .len()
        .checked_add(7)
        .map(|value| value & !7)
        .ok_or_else(|| invalid("output alignment overflows"))?;
    output.resize(aligned, 0);
    Ok(())
}

fn require_zero(bytes: &[u8], range: Range<usize>, detail: &'static str) -> FormatResult<()> {
    if bytes[range].iter().any(|byte| *byte != 0) {
        return Err(invalid(detail));
    }
    Ok(())
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> [u8; N] {
    bytes[offset..offset + N]
        .try_into()
        .expect("fixed-width field is in bounds")
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

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
