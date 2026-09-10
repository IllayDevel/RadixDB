use std::mem::size_of;
use std::ops::Range;

use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};

use super::super::{
    ArtifactId, ArtifactKind, ArtifactRef, ArtifactSource, CatalogGeneration, DatabaseGeneration,
    DatabaseId, FormatError, FormatResult, SegmentId, SegmentKind, MAX_ARTIFACT_FILE_BYTES,
};
use super::bloom::{decode_bloom_payload, DataBloom};
use super::column::{decode_column_payload, decode_typed_column_payload, DecodedColumn};
use super::column_model::DataColumn;
use super::model::{
    collect_column_block_map, invalid, limit, validate_block_lengths, validate_kind_layout,
    DataArtifactHeader, DataArtifactInput, DataArtifactLayout, DataBlockKind, DataBlockRef,
    DataLayout, DataPhysicalCodec, DataRowGroup, DataSectionKind, DataSectionRef,
    DATA_BLOCK_REF_BYTES, DATA_FOOTER_BYTES, DATA_HEADER_BYTES, DATA_SECTION_COUNT,
    DATA_SECTION_REF_BYTES, MAX_BLOCKS_PER_DATA_ARTIFACT, MAX_DATA_DIRECTORY_BYTES,
    MAX_STATISTICS_VALUES_BYTES,
};
use super::row_ids::decode_row_id_payload;
use super::source::{DataOpenLimits, DataOpenMetrics, OpenedDataArtifact};
use super::statistics::{decode_statistics, STATISTICS_ENTRY_BYTES};

const MAGIC: [u8; 8] = *b"RDX6DAT\0";
pub(crate) const FOOTER_MAGIC: [u8; 8] = *b"RDX6END\0";
const FORMAT_MAJOR: u16 = 6;
const FORMAT_MINOR: u16 = 0;
pub(crate) const SECTION_DIRECTORY_OFFSET: usize = DATA_HEADER_BYTES;
pub(crate) const SECTION_DIRECTORY_BYTES: usize = DATA_SECTION_COUNT * DATA_SECTION_REF_BYTES;
pub(crate) const BODY_START: usize = SECTION_DIRECTORY_OFFSET + SECTION_DIRECTORY_BYTES;

const COLUMN_ENTRY_BYTES: u64 = 64;
const ROW_GROUP_ENTRY_BYTES: u64 = 48;

pub fn encode_data_artifact(input: &DataArtifactInput) -> FormatResult<(Vec<u8>, ArtifactRef)> {
    let mut output = vec![0_u8; BODY_START];
    let mut ranges: [Range<usize>; DATA_SECTION_COUNT] = std::array::from_fn(|_| 0..0);
    let column_directory = encode_columns(input.columns());
    let row_group_directory = encode_row_groups(input.row_groups());

    ranges[0] = append_section(&mut output, &column_directory)?;
    ranges[1] = append_section(&mut output, &row_group_directory)?;
    let block_directory_bytes = input
        .blocks()
        .len()
        .checked_mul(DATA_BLOCK_REF_BYTES)
        .ok_or_else(|| invalid("block directory length overflows"))?;
    ranges[2] = append_section_placeholder(&mut output, block_directory_bytes)?;
    ranges[3] = append_section(&mut output, input.statistics_directory())?;

    for (index, block) in input.blocks().iter().enumerate() {
        align_output(&mut output);
        let offset = output.len();
        output.extend_from_slice(block.stored_bytes());
        let directory_offset = ranges[2].start + index * DATA_BLOCK_REF_BYTES;
        let entry = &mut output[directory_offset..directory_offset + DATA_BLOCK_REF_BYTES];
        let stored_crc32 = radixdb_core::crc32_ieee(block.stored_bytes());
        let block_ref = DataBlockRef::new(
            block.kind(),
            block.codec(),
            block.column_ordinal(),
            block.row_group_ordinal(),
            block.layout(),
            offset as u64,
            block.stored_bytes().len() as u64,
            block.logical_length(),
            block.item_count(),
            stored_crc32,
        )?;
        encode_block_ref(entry, &block_ref);
    }
    // Variable-size statistic values intentionally follow the row-group
    // payloads.  The four fixed-size directories therefore form a prefix that
    // a streaming writer can reserve before it consumes the source rows;
    // blocks can then be written once, directly to their final offsets.
    ranges[4] = append_section(&mut output, input.statistics_values())?;

    let body_length = output
        .len()
        .checked_add(DATA_FOOTER_BYTES)
        .ok_or_else(|| invalid("file length overflows"))?;
    if body_length as u64 > MAX_ARTIFACT_FILE_BYTES {
        return Err(limit(
            "file bytes",
            body_length as u64,
            MAX_ARTIFACT_FILE_BYTES,
        ));
    }

    encode_header(
        &mut output[..DATA_HEADER_BYTES],
        input.header(),
        body_length,
    )?;
    let counts = [
        u64::from(input.header().column_count()),
        u64::from(input.header().row_group_count()),
        input.blocks().len() as u64,
        input.statistics().len() as u64,
        input.statistics_values().len() as u64,
    ];
    for (index, kind) in DataSectionKind::ALL.into_iter().enumerate() {
        let stored_crc32 = radixdb_core::crc32_ieee(&output[ranges[index].clone()]);
        let entry_offset = SECTION_DIRECTORY_OFFSET + index * DATA_SECTION_REF_BYTES;
        let entry = &mut output[entry_offset..entry_offset + DATA_SECTION_REF_BYTES];
        encode_section_ref(entry, kind, &ranges[index], counts[index], stored_crc32)?;
    }

    let header_crc = radixdb_core::crc32_ieee(&output[..248]);
    put_u32(&mut output, 248, header_crc);
    let body_sha = radixdb_core::sha256_digest(&output);
    output.extend_from_slice(&FOOTER_MAGIC);
    output.extend_from_slice(&(body_length as u64).to_le_bytes());
    output.extend_from_slice(&body_sha);

    let reference = ArtifactRef::new(
        input.header().artifact_id(),
        ArtifactKind::Data,
        input.header().creation_generation(),
        body_length as u64,
        body_sha,
    )?;
    Ok((output, reference))
}

pub fn decode_data_artifact_layout(
    bytes: &[u8],
    expected: ArtifactRef,
) -> FormatResult<DataArtifactLayout> {
    validate_file_shell(bytes, expected)?;
    let header = decode_header(bytes, bytes.len() as u64)?;
    if header.artifact_id() != expected.id() {
        return Err(invalid("artifact ID differs from manifest reference"));
    }
    if header.creation_generation() != expected.creation_generation() {
        return Err(invalid(
            "creation generation differs from manifest reference",
        ));
    }

    let footer_start = bytes.len() - DATA_FOOTER_BYTES;
    let mut sections =
        [DataSectionRef::new(DataSectionKind::ColumnDirectory, 0, 0, 0, 0); DATA_SECTION_COUNT];
    let mut directory_bytes = SECTION_DIRECTORY_BYTES as u64;

    for (index, expected_kind) in DataSectionKind::ALL.into_iter().enumerate() {
        let entry_offset = SECTION_DIRECTORY_OFFSET + index * DATA_SECTION_REF_BYTES;
        let entry = &bytes[entry_offset..entry_offset + DATA_SECTION_REF_BYTES];
        let section = decode_section_ref(entry, expected_kind)?;
        validate_section_count(&header, section)?;
        if expected_kind != DataSectionKind::StatisticsValues {
            directory_bytes = directory_bytes
                .checked_add(section.stored_length())
                .ok_or_else(|| invalid("directory byte count overflows"))?;
        }
        let section_bytes = section_slice(bytes, section)?;
        if radixdb_core::crc32_ieee(section_bytes) != section.stored_crc32() {
            return Err(FormatError::DataArtifactChecksumMismatch { scope: "section" });
        }
        sections[index] = section;
    }
    if directory_bytes > MAX_DATA_DIRECTORY_BYTES {
        return Err(limit(
            "directory bytes",
            directory_bytes,
            MAX_DATA_DIRECTORY_BYTES,
        ));
    }

    let section_bytes: [&[u8]; DATA_SECTION_COUNT] = std::array::from_fn(|index| {
        section_slice(bytes, sections[index]).expect("validated section range")
    });
    let columns = decode_columns(section_bytes[0], sections[0], sections[3].item_count())?;
    let row_groups = decode_row_groups(section_bytes[1], &header, sections[1])?;
    let blocks = decode_blocks(section_bytes[2], &header, sections[2])?;
    validate_group_block_map(&columns, &row_groups, &blocks)?;
    validate_column_block_map(&columns, &row_groups, &blocks)?;
    let statistics = decode_statistics(
        section_bytes[3],
        section_bytes[4],
        usize::try_from(sections[3].item_count())
            .map_err(|_| invalid("statistics count does not fit this platform"))?,
        &columns,
        &row_groups,
        &blocks,
    )?;
    let cursor = validate_slice_topology(bytes, &sections, &blocks, footer_start)?;
    if cursor != footer_start {
        return Err(invalid("artifact has bytes outside declared ranges"));
    }

    Ok(DataArtifactLayout::new(
        expected, header, sections, columns, row_groups, statistics, blocks,
    ))
}

pub fn open_data_artifact_metadata(
    source: &(impl ArtifactSource + ?Sized),
    expected: ArtifactRef,
) -> FormatResult<OpenedDataArtifact> {
    open_data_artifact_metadata_with_limits(source, expected, DataOpenLimits::default())
}

pub fn open_data_artifact_metadata_with_limits(
    source: &(impl ArtifactSource + ?Sized),
    expected: ArtifactRef,
    limits: DataOpenLimits,
) -> FormatResult<OpenedDataArtifact> {
    let file_length = source.byte_length()?;
    validate_source_length(file_length, expected)?;
    let footer_start = file_length - DATA_FOOTER_BYTES as u64;
    let mut metrics = DataOpenMetrics::default();
    let mut prefix = [0_u8; BODY_START];
    read_source(source, 0, &mut prefix, &mut metrics)?;
    let mut footer = [0_u8; DATA_FOOTER_BYTES];
    read_source(source, footer_start, &mut footer, &mut metrics)?;
    validate_source_footer(&footer, file_length, expected)?;

    let header = decode_header(&prefix, file_length)?;
    validate_expected_header(header, expected)?;
    let sections = decode_source_sections(&prefix, &header, footer_start)?;
    metrics.account_allocation(metadata_allocation_bound(&sections)?, limits)?;

    let mut section_bytes: [Vec<u8>; DATA_SECTION_COUNT] = std::array::from_fn(|_| Vec::new());
    for (index, section) in sections.iter().copied().enumerate() {
        if section.stored_length() == 0 {
            continue;
        }
        let length = usize::try_from(section.stored_length())
            .map_err(|_| invalid("section length does not fit this platform"))?;
        let mut bytes = vec![0_u8; length];
        read_source(source, section.offset(), &mut bytes, &mut metrics)?;
        if radixdb_core::crc32_ieee(&bytes) != section.stored_crc32() {
            return Err(FormatError::DataArtifactChecksumMismatch { scope: "section" });
        }
        section_bytes[index] = bytes;
    }

    let columns = decode_columns(&section_bytes[0], sections[0], sections[3].item_count())?;
    let row_groups = decode_row_groups(&section_bytes[1], &header, sections[1])?;
    let blocks = decode_blocks(&section_bytes[2], &header, sections[2])?;
    validate_group_block_map(&columns, &row_groups, &blocks)?;
    validate_column_block_map(&columns, &row_groups, &blocks)?;
    let statistics = decode_statistics(
        &section_bytes[3],
        &section_bytes[4],
        usize::try_from(sections[3].item_count())
            .map_err(|_| invalid("statistics count does not fit this platform"))?,
        &columns,
        &row_groups,
        &blocks,
    )?;
    let cursor = validate_source_topology(&sections, &blocks, footer_start)?;
    if cursor != footer_start {
        return Err(invalid("artifact has bytes outside declared ranges"));
    }

    Ok(OpenedDataArtifact::new(
        DataArtifactLayout::new(
            expected, header, sections, columns, row_groups, statistics, blocks,
        ),
        metrics,
    ))
}

pub fn read_data_block<'a>(
    bytes: &'a [u8],
    layout: &DataArtifactLayout,
    block_index: usize,
) -> FormatResult<&'a [u8]> {
    if bytes.len() as u64 != layout.reference().byte_length() {
        return Err(invalid(
            "block source length differs from artifact identity",
        ));
    }
    let block = layout
        .blocks()
        .get(block_index)
        .ok_or_else(|| invalid("block index is out of range"))?;
    let start = usize::try_from(block.offset())
        .map_err(|_| invalid("block offset does not fit this platform"))?;
    let length = usize::try_from(block.stored_length())
        .map_err(|_| invalid("block length does not fit this platform"))?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| invalid("block range overflows"))?;
    let stored = bytes
        .get(start..end)
        .ok_or_else(|| invalid("block range is outside source bytes"))?;
    if radixdb_core::crc32_ieee(stored) != block.stored_crc32() {
        return Err(FormatError::DataArtifactChecksumMismatch { scope: "block" });
    }
    Ok(stored)
}

pub fn read_data_block_from_source(
    source: &(impl ArtifactSource + ?Sized),
    layout: &DataArtifactLayout,
    block_index: usize,
) -> FormatResult<Vec<u8>> {
    if source.byte_length()? != layout.reference().byte_length() {
        return Err(invalid(
            "block source length differs from artifact identity",
        ));
    }
    let block = layout
        .blocks()
        .get(block_index)
        .ok_or_else(|| invalid("block index is out of range"))?;
    let length = usize::try_from(block.stored_length())
        .map_err(|_| invalid("block length does not fit this platform"))?;
    let mut stored = vec![0_u8; length];
    source.read_exact_at(block.offset(), &mut stored)?;
    if radixdb_core::crc32_ieee(&stored) != block.stored_crc32() {
        return Err(FormatError::DataArtifactChecksumMismatch { scope: "block" });
    }
    Ok(stored)
}

pub fn read_data_row_ids(
    bytes: &[u8],
    layout: &DataArtifactLayout,
    row_group_ordinal: u32,
) -> FormatResult<Vec<u64>> {
    let group = layout
        .row_groups()
        .get(row_group_ordinal as usize)
        .copied()
        .filter(|group| group.group_ordinal() == row_group_ordinal)
        .ok_or_else(|| invalid("row-group ordinal is out of range"))?;
    let start = group.first_block_index() as usize;
    let end = start
        .checked_add(group.block_count() as usize)
        .ok_or_else(|| invalid("row-group block range overflows"))?;
    let group_blocks = layout
        .blocks()
        .get(start..end)
        .ok_or_else(|| invalid("row-group block range is outside directory"))?;
    let (block_index, block) = group_blocks
        .iter()
        .enumerate()
        .find(|(_, block)| block.kind() == DataBlockKind::RowIds)
        .map(|(index, block)| (start + index, block))
        .ok_or_else(|| invalid("row group has no row-ID block"))?;
    let stored = read_data_block(bytes, layout, block_index)?;
    decode_row_id_payload(stored, block, group)
}

pub fn read_data_column(
    bytes: &[u8],
    layout: &DataArtifactLayout,
    row_group_ordinal: u32,
    column_ordinal: u32,
) -> FormatResult<Vec<Value>> {
    let group = layout
        .row_groups()
        .get(row_group_ordinal as usize)
        .copied()
        .filter(|group| group.group_ordinal() == row_group_ordinal)
        .ok_or_else(|| invalid("row-group ordinal is out of range"))?;
    let column = layout
        .columns()
        .get(column_ordinal as usize)
        .copied()
        .filter(|column| column.ordinal() == column_ordinal)
        .ok_or_else(|| invalid("column ordinal is out of range"))?;
    let start = group.first_block_index() as usize;
    let end = start
        .checked_add(group.block_count() as usize)
        .ok_or_else(|| invalid("row-group block range overflows"))?;
    let group_blocks = layout
        .blocks()
        .get(start..end)
        .ok_or_else(|| invalid("row-group block range is outside directory"))?;
    let (block_index, block) = group_blocks
        .iter()
        .enumerate()
        .find(|(_, block)| {
            block.kind() == DataBlockKind::Column && block.column_ordinal() == column_ordinal
        })
        .map(|(index, block)| (start + index, block))
        .ok_or_else(|| invalid("row group has no block for column"))?;
    let stored = read_data_block(bytes, layout, block_index)?;
    decode_column_payload(stored, block, group, column)
}

pub fn read_data_bloom(
    bytes: &[u8],
    layout: &DataArtifactLayout,
    row_group_ordinal: u32,
    column_ordinal: u32,
) -> FormatResult<Option<DataBloom>> {
    layout
        .row_groups()
        .get(row_group_ordinal as usize)
        .filter(|group| group.group_ordinal() == row_group_ordinal)
        .ok_or_else(|| invalid("row-group ordinal is out of range"))?;
    let column = layout
        .columns()
        .get(column_ordinal as usize)
        .copied()
        .filter(|column| column.ordinal() == column_ordinal)
        .ok_or_else(|| invalid("column ordinal is out of range"))?;
    let statistics = layout.statistics().iter().find(|statistics| {
        statistics.row_group_ordinal() == row_group_ordinal
            && statistics.column_ordinal() == column_ordinal
    });
    let Some(block_index) = statistics.and_then(|statistics| statistics.bloom_block_index()) else {
        return Ok(None);
    };
    let block = layout
        .blocks()
        .get(block_index as usize)
        .ok_or_else(|| invalid("bloom block index is out of range"))?;
    let stored = read_data_block(bytes, layout, block_index as usize)?;
    decode_bloom_payload(stored, block, column).map(Some)
}

pub fn read_data_row_ids_from_source(
    source: &(impl ArtifactSource + ?Sized),
    layout: &DataArtifactLayout,
    row_group_ordinal: u32,
) -> FormatResult<Vec<u64>> {
    let group = find_group(layout, row_group_ordinal)?;
    let (block_index, block) = find_group_block(layout, group, DataBlockKind::RowIds, u32::MAX)?;
    let stored = read_data_block_from_source(source, layout, block_index)?;
    decode_row_id_payload(&stored, block, *group)
}

pub fn read_data_column_from_source(
    source: &(impl ArtifactSource + ?Sized),
    layout: &DataArtifactLayout,
    row_group_ordinal: u32,
    column_ordinal: u32,
) -> FormatResult<Vec<Value>> {
    let group = find_group(layout, row_group_ordinal)?;
    let column = find_column(layout, column_ordinal)?;
    let (block_index, block) =
        find_group_block(layout, group, DataBlockKind::Column, column_ordinal)?;
    let stored = read_data_block_from_source(source, layout, block_index)?;
    decode_column_payload(&stored, block, *group, *column)
}

pub(crate) fn read_data_typed_column_from_source(
    source: &(impl ArtifactSource + ?Sized),
    layout: &DataArtifactLayout,
    row_group_ordinal: u32,
    column_ordinal: u32,
) -> FormatResult<DecodedColumn> {
    let group = find_group(layout, row_group_ordinal)?;
    let column = find_column(layout, column_ordinal)?;
    let (block_index, block) =
        find_group_block(layout, group, DataBlockKind::Column, column_ordinal)?;
    let stored = read_data_block_from_source(source, layout, block_index)?;
    decode_typed_column_payload(&stored, block, *group, *column)
}

pub fn read_data_bloom_from_source(
    source: &(impl ArtifactSource + ?Sized),
    layout: &DataArtifactLayout,
    row_group_ordinal: u32,
    column_ordinal: u32,
) -> FormatResult<Option<DataBloom>> {
    find_group(layout, row_group_ordinal)?;
    let column = find_column(layout, column_ordinal)?;
    let statistics = layout.statistics().iter().find(|statistics| {
        statistics.row_group_ordinal() == row_group_ordinal
            && statistics.column_ordinal() == column_ordinal
    });
    let Some(block_index) = statistics.and_then(|statistics| statistics.bloom_block_index()) else {
        return Ok(None);
    };
    let block = layout
        .blocks()
        .get(block_index as usize)
        .ok_or_else(|| invalid("bloom block index is out of range"))?;
    let stored = read_data_block_from_source(source, layout, block_index as usize)?;
    decode_bloom_payload(&stored, block, *column).map(Some)
}

pub(crate) fn encode_header(
    output: &mut [u8],
    header: DataArtifactHeader,
    file_length: usize,
) -> FormatResult<()> {
    output[..8].copy_from_slice(&MAGIC);
    put_u16(output, 8, FORMAT_MAJOR);
    put_u16(output, 10, FORMAT_MINOR);
    put_u32(output, 12, DATA_HEADER_BYTES as u32);
    put_u64(output, 16, file_length as u64);
    output[24..40].copy_from_slice(header.artifact_id().as_bytes());
    output[40..56].copy_from_slice(header.database_id().as_bytes());
    output[56..72].copy_from_slice(header.table_id().as_bytes());
    output[72..88].copy_from_slice(header.segment_id().as_bytes());
    put_u64(output, 88, header.creation_generation().get());
    put_u64(output, 96, header.catalog_generation().get());
    put_u64(output, 104, header.min_transaction_id());
    put_u64(output, 112, header.max_transaction_id());
    put_u64(output, 120, header.row_count());
    put_u32(output, 128, header.column_count());
    put_u32(output, 132, header.row_group_count());
    put_u32(output, 136, DATA_SECTION_COUNT as u32);
    put_u32(
        output,
        140,
        u32::from(header.segment_kind() == SegmentKind::Tombstones),
    );
    put_u64(output, 144, SECTION_DIRECTORY_OFFSET as u64);
    put_u64(output, 152, SECTION_DIRECTORY_BYTES as u64);
    put_u64(output, 160, header.created_unix_ns());
    Ok(())
}

fn decode_header(bytes: &[u8], file_length: u64) -> FormatResult<DataArtifactHeader> {
    if bytes[..8] != MAGIC {
        return Err(invalid("magic mismatch"));
    }
    let major = read_u16(bytes, 8);
    let minor = read_u16(bytes, 10);
    if (major, minor) != (FORMAT_MAJOR, FORMAT_MINOR) {
        return Err(FormatError::UnsupportedFormatVersion {
            owner: "data artifact",
            major,
            minor,
        });
    }
    if read_u32(bytes, 12) != DATA_HEADER_BYTES as u32 {
        return Err(invalid("header length is not 256"));
    }
    if read_u64(bytes, 16) != file_length {
        return Err(invalid("header file length mismatch"));
    }
    if read_u32(bytes, 136) != DATA_SECTION_COUNT as u32 {
        return Err(invalid("section count is not five"));
    }
    let flags = read_u32(bytes, 140);
    if flags & !1 != 0 {
        return Err(invalid("unknown header flags"));
    }
    if read_u64(bytes, 144) != SECTION_DIRECTORY_OFFSET as u64
        || read_u64(bytes, 152) != SECTION_DIRECTORY_BYTES as u64
    {
        return Err(invalid("section directory is not canonical"));
    }
    require_zero(bytes, 168..248, "reserved header bytes are non-zero")?;
    require_zero(bytes, 252..256, "reserved header trailer is non-zero")?;
    if read_u32(bytes, 248) != radixdb_core::crc32_ieee(&bytes[..248]) {
        return Err(FormatError::DataArtifactChecksumMismatch { scope: "header" });
    }
    DataArtifactHeader::new(
        ArtifactId::from_bytes(read_array(bytes, 24))?,
        DatabaseId::from_bytes(read_array(bytes, 40))?,
        ObjectId::from_user_bytes(read_array(bytes, 56))
            .map_err(|_| invalid("table ID is not a user catalog identity"))?,
        SegmentId::from_bytes(read_array(bytes, 72))?,
        DatabaseGeneration::new(read_u64(bytes, 88))?,
        CatalogGeneration::new(read_u64(bytes, 96))?,
        read_u64(bytes, 104),
        read_u64(bytes, 112),
        read_u64(bytes, 120),
        read_u32(bytes, 128),
        read_u32(bytes, 132),
        if flags == 1 {
            SegmentKind::Tombstones
        } else {
            SegmentKind::Rows
        },
        read_u64(bytes, 160),
    )
}

fn validate_file_shell(bytes: &[u8], expected: ArtifactRef) -> FormatResult<()> {
    if expected.kind() != ArtifactKind::Data {
        return Err(invalid("expected reference is not a data artifact"));
    }
    if bytes.len() < BODY_START + DATA_FOOTER_BYTES {
        return Err(invalid("file is shorter than fixed metadata shell"));
    }
    if bytes.len() as u64 > MAX_ARTIFACT_FILE_BYTES {
        return Err(limit(
            "file bytes",
            bytes.len() as u64,
            MAX_ARTIFACT_FILE_BYTES,
        ));
    }
    if bytes.len() as u64 != expected.byte_length() {
        return Err(invalid("file length differs from manifest reference"));
    }
    let footer = bytes.len() - DATA_FOOTER_BYTES;
    if bytes[footer..footer + 8] != FOOTER_MAGIC {
        return Err(invalid("footer magic mismatch"));
    }
    if read_u64(bytes, footer + 8) != bytes.len() as u64 {
        return Err(invalid("footer file length mismatch"));
    }
    if bytes[footer + 16..] != *expected.body_sha256() {
        return Err(FormatError::DataArtifactChecksumMismatch {
            scope: "footer identity",
        });
    }
    Ok(())
}

fn validate_source_length(file_length: u64, expected: ArtifactRef) -> FormatResult<()> {
    if expected.kind() != ArtifactKind::Data {
        return Err(invalid("expected reference is not a data artifact"));
    }
    let minimum_length = (BODY_START + DATA_FOOTER_BYTES) as u64;
    if file_length < minimum_length {
        return Err(invalid("file is shorter than fixed metadata shell"));
    }
    if file_length > MAX_ARTIFACT_FILE_BYTES {
        return Err(limit("file bytes", file_length, MAX_ARTIFACT_FILE_BYTES));
    }
    if file_length != expected.byte_length() {
        return Err(invalid("file length differs from manifest reference"));
    }
    Ok(())
}

fn read_source(
    source: &(impl ArtifactSource + ?Sized),
    offset: u64,
    destination: &mut [u8],
    metrics: &mut DataOpenMetrics,
) -> FormatResult<()> {
    if destination.is_empty() {
        return Ok(());
    }
    source.read_exact_at(offset, destination)?;
    metrics.record_read(destination.len() as u64)
}

fn validate_source_footer(
    footer: &[u8; DATA_FOOTER_BYTES],
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
        return Err(FormatError::DataArtifactChecksumMismatch {
            scope: "footer identity",
        });
    }
    Ok(())
}

fn validate_expected_header(header: DataArtifactHeader, expected: ArtifactRef) -> FormatResult<()> {
    if header.artifact_id() != expected.id() {
        return Err(invalid("artifact ID differs from manifest reference"));
    }
    if header.creation_generation() != expected.creation_generation() {
        return Err(invalid(
            "creation generation differs from manifest reference",
        ));
    }
    Ok(())
}

fn decode_source_sections(
    prefix: &[u8; BODY_START],
    header: &DataArtifactHeader,
    footer_start: u64,
) -> FormatResult<[DataSectionRef; DATA_SECTION_COUNT]> {
    let mut sections =
        [DataSectionRef::new(DataSectionKind::ColumnDirectory, 0, 0, 0, 0); DATA_SECTION_COUNT];
    let mut directory_bytes = SECTION_DIRECTORY_BYTES as u64;

    for (index, expected_kind) in DataSectionKind::ALL.into_iter().enumerate() {
        let entry_offset = SECTION_DIRECTORY_OFFSET + index * DATA_SECTION_REF_BYTES;
        let entry = &prefix[entry_offset..entry_offset + DATA_SECTION_REF_BYTES];
        let section = decode_section_ref(entry, expected_kind)?;
        validate_section_count(header, section)?;
        if expected_kind != DataSectionKind::StatisticsValues {
            directory_bytes = directory_bytes
                .checked_add(section.stored_length())
                .ok_or_else(|| invalid("directory byte count overflows"))?;
        }
        validate_source_bounds(
            footer_start,
            section.offset(),
            section.stored_length(),
            "section range",
        )?;
        sections[index] = section;
    }
    if directory_bytes > MAX_DATA_DIRECTORY_BYTES {
        return Err(limit(
            "directory bytes",
            directory_bytes,
            MAX_DATA_DIRECTORY_BYTES,
        ));
    }
    Ok(sections)
}

fn validate_slice_topology(
    bytes: &[u8],
    sections: &[DataSectionRef; DATA_SECTION_COUNT],
    blocks: &[DataBlockRef],
    footer_start: usize,
) -> FormatResult<usize> {
    let mut cursor = BODY_START;
    for section in &sections[..4] {
        validate_canonical_range(
            bytes,
            &mut cursor,
            footer_start,
            section.offset(),
            section.stored_length(),
            "directory section range",
        )?;
    }
    for block in blocks {
        validate_canonical_range(
            bytes,
            &mut cursor,
            footer_start,
            block.offset(),
            block.stored_length(),
            "block range",
        )?;
    }
    let statistics_values = sections[4];
    validate_canonical_range(
        bytes,
        &mut cursor,
        footer_start,
        statistics_values.offset(),
        statistics_values.stored_length(),
        "statistics value section range",
    )?;
    Ok(cursor)
}

fn validate_source_topology(
    sections: &[DataSectionRef; DATA_SECTION_COUNT],
    blocks: &[DataBlockRef],
    footer_start: u64,
) -> FormatResult<u64> {
    let mut cursor = BODY_START as u64;
    for section in &sections[..4] {
        validate_source_range(
            &mut cursor,
            footer_start,
            section.offset(),
            section.stored_length(),
            "directory section range",
        )?;
    }
    for block in blocks {
        validate_source_range(
            &mut cursor,
            footer_start,
            block.offset(),
            block.stored_length(),
            "block range",
        )?;
    }
    let statistics_values = sections[4];
    validate_source_range(
        &mut cursor,
        footer_start,
        statistics_values.offset(),
        statistics_values.stored_length(),
        "statistics value section range",
    )?;
    Ok(cursor)
}

fn validate_source_bounds(
    footer_start: u64,
    offset: u64,
    length: u64,
    detail: &'static str,
) -> FormatResult<()> {
    if length == 0 {
        return if offset == 0 {
            Ok(())
        } else {
            Err(invalid("empty range has non-zero offset"))
        };
    }
    let end = offset.checked_add(length).ok_or_else(|| invalid(detail))?;
    if offset < BODY_START as u64 || !offset.is_multiple_of(8) || end > footer_start {
        return Err(invalid(detail));
    }
    Ok(())
}

fn metadata_allocation_bound(sections: &[DataSectionRef; DATA_SECTION_COUNT]) -> FormatResult<u64> {
    let columns = sections[0]
        .item_count()
        .checked_mul(size_of::<DataColumn>() as u64)
        .ok_or_else(|| invalid("column allocation accounting overflows"))?;
    let row_groups = sections[1]
        .item_count()
        .checked_mul(size_of::<DataRowGroup>() as u64)
        .ok_or_else(|| invalid("row-group allocation accounting overflows"))?;
    let blocks = sections[2]
        .item_count()
        .checked_mul(size_of::<DataBlockRef>() as u64)
        .ok_or_else(|| invalid("block allocation accounting overflows"))?;
    let statistics = sections[3]
        .item_count()
        .checked_mul(size_of::<super::statistics::DataStatistics>() as u64)
        .ok_or_else(|| invalid("statistics allocation accounting overflows"))?;

    let mut total = 0_u64;
    for section in sections {
        total = total
            .checked_add(section.stored_length())
            .ok_or_else(|| invalid("metadata section allocation accounting overflows"))?;
    }
    for decoded in [columns, row_groups, blocks, statistics] {
        total = total
            .checked_add(decoded)
            .ok_or_else(|| invalid("decoded metadata allocation accounting overflows"))?;
    }

    // Decoded min/max values may own copies of the statistics value bytes.
    // Account the complete section again instead of depending on Value's
    // current heap representation. Vec<bool> bloom ownership bookkeeping is
    // conservatively charged at one byte per block.
    total = total
        .checked_add(sections[4].stored_length())
        .and_then(|value| value.checked_add(sections[2].item_count()))
        .ok_or_else(|| invalid("variable metadata allocation accounting overflows"))?;
    Ok(total)
}

fn validate_source_range(
    cursor: &mut u64,
    footer_start: u64,
    offset: u64,
    length: u64,
    detail: &'static str,
) -> FormatResult<()> {
    if length == 0 {
        if offset != 0 {
            return Err(invalid("empty range has non-zero offset"));
        }
        return Ok(());
    }
    let aligned = cursor
        .checked_add(7)
        .map(|value| value & !7)
        .ok_or_else(|| invalid("range alignment overflows"))?;
    let end = offset.checked_add(length).ok_or_else(|| invalid(detail))?;
    if offset != aligned || offset < BODY_START as u64 || end > footer_start {
        return Err(invalid(detail));
    }
    *cursor = end;
    Ok(())
}

fn find_group(layout: &DataArtifactLayout, row_group_ordinal: u32) -> FormatResult<&DataRowGroup> {
    layout
        .row_groups()
        .get(row_group_ordinal as usize)
        .filter(|group| group.group_ordinal() == row_group_ordinal)
        .ok_or_else(|| invalid("row-group ordinal is out of range"))
}

fn find_column(layout: &DataArtifactLayout, column_ordinal: u32) -> FormatResult<&DataColumn> {
    layout
        .columns()
        .get(column_ordinal as usize)
        .filter(|column| column.ordinal() == column_ordinal)
        .ok_or_else(|| invalid("column ordinal is out of range"))
}

fn find_group_block<'a>(
    layout: &'a DataArtifactLayout,
    group: &DataRowGroup,
    kind: DataBlockKind,
    column_ordinal: u32,
) -> FormatResult<(usize, &'a DataBlockRef)> {
    let start = group.first_block_index() as usize;
    let end = start
        .checked_add(group.block_count() as usize)
        .ok_or_else(|| invalid("row-group block range overflows"))?;
    let group_blocks = layout
        .blocks()
        .get(start..end)
        .ok_or_else(|| invalid("row-group block range is outside directory"))?;
    group_blocks
        .iter()
        .enumerate()
        .find(|(_, block)| {
            block.kind() == kind
                && (kind == DataBlockKind::RowIds || block.column_ordinal() == column_ordinal)
        })
        .map(|(index, block)| (start + index, block))
        .ok_or_else(|| invalid("row group has no requested block"))
}

fn section_slice(bytes: &[u8], section: DataSectionRef) -> FormatResult<&[u8]> {
    if section.stored_length() == 0 {
        return Ok(&[]);
    }
    let start = usize::try_from(section.offset())
        .map_err(|_| invalid("section offset does not fit this platform"))?;
    let length = usize::try_from(section.stored_length())
        .map_err(|_| invalid("section length does not fit this platform"))?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| invalid("section range overflows"))?;
    bytes
        .get(start..end)
        .ok_or_else(|| invalid("section range is outside artifact"))
}

pub(crate) fn encode_section_ref(
    entry: &mut [u8],
    kind: DataSectionKind,
    range: &Range<usize>,
    item_count: u64,
    stored_crc32: u32,
) -> FormatResult<()> {
    put_u16(entry, 0, kind.tag());
    put_u16(entry, 2, 1);
    if range.is_empty() {
        if item_count != 0 {
            return Err(invalid("empty section has non-zero item count"));
        }
        return Ok(());
    }
    put_u64(entry, 8, range.start as u64);
    put_u64(entry, 16, range.len() as u64);
    put_u64(entry, 24, range.len() as u64);
    put_u64(entry, 32, item_count);
    put_u32(entry, 40, stored_crc32);
    Ok(())
}

fn decode_section_ref(
    entry: &[u8],
    expected_kind: DataSectionKind,
) -> FormatResult<DataSectionRef> {
    if read_u16(entry, 0) != expected_kind.tag() {
        return Err(invalid("section kind/order mismatch"));
    }
    if read_u16(entry, 2) != 1 {
        return Err(invalid("unsupported section version"));
    }
    if read_u32(entry, 4) != 0 {
        return Err(invalid("unknown section flags"));
    }
    require_zero(entry, 44..48, "section reserved bytes are non-zero")?;
    let offset = read_u64(entry, 8);
    let stored_length = read_u64(entry, 16);
    let logical_length = read_u64(entry, 24);
    let item_count = read_u64(entry, 32);
    let stored_crc32 = read_u32(entry, 40);
    if stored_length == 0 {
        if offset != 0 || logical_length != 0 || item_count != 0 || stored_crc32 != 0 {
            return Err(invalid("empty section reference is not canonical"));
        }
    } else if logical_length != stored_length {
        return Err(invalid("metadata section is unexpectedly compressed"));
    }
    Ok(DataSectionRef::new(
        expected_kind,
        offset,
        stored_length,
        item_count,
        stored_crc32,
    ))
}

fn validate_section_count(
    header: &DataArtifactHeader,
    section: DataSectionRef,
) -> FormatResult<()> {
    let (expected_count, width) = match section.kind() {
        DataSectionKind::ColumnDirectory => (u64::from(header.column_count()), COLUMN_ENTRY_BYTES),
        DataSectionKind::RowGroupDirectory => {
            (u64::from(header.row_group_count()), ROW_GROUP_ENTRY_BYTES)
        }
        DataSectionKind::BlockDirectory => {
            if section.item_count() > MAX_BLOCKS_PER_DATA_ARTIFACT {
                return Err(limit(
                    "block count",
                    section.item_count(),
                    MAX_BLOCKS_PER_DATA_ARTIFACT,
                ));
            }
            (section.item_count(), DATA_BLOCK_REF_BYTES as u64)
        }
        DataSectionKind::StatisticsDirectory => {
            let maximum = u64::from(header.column_count())
                .checked_mul(u64::from(header.row_group_count()))
                .ok_or_else(|| invalid("statistics count multiplication overflows"))?;
            if section.item_count() > maximum {
                return Err(limit(
                    "statistics entry count",
                    section.item_count(),
                    maximum,
                ));
            }
            (section.item_count(), STATISTICS_ENTRY_BYTES as u64)
        }
        DataSectionKind::StatisticsValues => {
            if section.stored_length() > MAX_STATISTICS_VALUES_BYTES {
                return Err(limit(
                    "statistics values bytes",
                    section.stored_length(),
                    MAX_STATISTICS_VALUES_BYTES,
                ));
            }
            if section.item_count() != section.stored_length() {
                return Err(invalid(
                    "statistics values item count is not its byte length",
                ));
            }
            return Ok(());
        }
    };
    if section.item_count() != expected_count {
        return Err(invalid("section item count differs from header"));
    }
    let expected_length = expected_count
        .checked_mul(width)
        .ok_or_else(|| invalid("section length multiplication overflows"))?;
    if section.stored_length() != expected_length {
        return Err(invalid("section length differs from fixed entry width"));
    }
    Ok(())
}

fn decode_block_ref(entry: &[u8]) -> FormatResult<DataBlockRef> {
    let flags = read_u32(entry, 12);
    if flags & !0xff != 0 {
        return Err(invalid("unknown block flags"));
    }
    require_zero(entry, 52..64, "block reserved bytes are non-zero")?;
    DataBlockRef::new(
        DataBlockKind::from_tag(read_u16(entry, 0))?,
        DataPhysicalCodec::from_tag(read_u16(entry, 2))?,
        read_u32(entry, 4),
        read_u32(entry, 8),
        DataLayout::from_tag(flags as u8)?,
        read_u64(entry, 16),
        read_u64(entry, 24),
        read_u64(entry, 32),
        read_u64(entry, 40),
        read_u32(entry, 48),
    )
}

fn validate_block_against_header(
    header: &DataArtifactHeader,
    block: &DataBlockRef,
) -> FormatResult<()> {
    validate_kind_layout(block.kind(), block.layout(), block.column_ordinal())?;
    validate_block_lengths(block.codec(), block.stored_length(), block.logical_length())?;
    if block.row_group_ordinal() >= header.row_group_count() {
        return Err(invalid("block row-group ordinal is out of range"));
    }
    if block.kind() != DataBlockKind::RowIds && block.column_ordinal() >= header.column_count() {
        return Err(invalid("block column ordinal is out of range"));
    }
    Ok(())
}

pub(crate) fn encode_columns(columns: &[DataColumn]) -> Vec<u8> {
    let mut output = vec![0_u8; columns.len() * COLUMN_ENTRY_BYTES as usize];
    for (index, column) in columns.iter().copied().enumerate() {
        let offset = index * COLUMN_ENTRY_BYTES as usize;
        let entry = &mut output[offset..offset + COLUMN_ENTRY_BYTES as usize];
        entry[..16].copy_from_slice(column.column_id().as_bytes());
        put_u32(entry, 16, column.ordinal());
        put_u16(entry, 20, column.data_type().descriptor_marker());
        put_u16(entry, 22, column.data_type().descriptor_version());
        put_u32(entry, 24, u32::from(column.nullable()));
        put_u32(entry, 28, column.data_type().parameter_1());
        put_u32(entry, 32, column.data_type().parameter_2());
        put_u32(entry, 36, column.first_block_index());
        put_u32(entry, 40, column.block_count());
        put_u32(entry, 44, column.statistics_entry_index());
        entry[48..64].copy_from_slice(&column.data_type().collation_id());
    }
    output
}

fn decode_columns(
    bytes: &[u8],
    section: DataSectionRef,
    statistics_count: u64,
) -> FormatResult<Vec<DataColumn>> {
    let count = usize::try_from(section.item_count())
        .map_err(|_| invalid("column count does not fit this platform"))?;
    let mut columns = Vec::with_capacity(count);
    let mut column_ids = Vec::with_capacity(count);
    for index in 0..count {
        let offset = index * COLUMN_ENTRY_BYTES as usize;
        let entry = &bytes[offset..offset + COLUMN_ENTRY_BYTES as usize];
        let column_id = ObjectId::from_user_bytes(read_array(entry, 0))
            .map_err(|_| invalid("column ID is not a user catalog identity"))?;
        column_ids.push(column_id);
        let ordinal = read_u32(entry, 16);
        if ordinal != index as u32 {
            return Err(invalid("column ordinal is not contiguous"));
        }
        let logical_type_marker = read_u16(entry, 20);
        let flags = read_u32(entry, 24);
        if flags & !1 != 0 {
            return Err(invalid("unknown column flags"));
        }
        let data_type = if logical_type_marker == 0xffff {
            if read_u16(entry, 22) != 2 || read_u32(entry, 32) != 0 {
                return Err(invalid("external column type descriptor is invalid"));
            }
            let type_object_id = ObjectId::from_user_bytes(read_array(entry, 48))
                .map_err(|_| invalid("external type ID is not a user catalog identity"))?;
            CatalogDataType::external(type_object_id, read_u32(entry, 28))
                .map_err(|_| invalid("external column type descriptor is invalid"))?
        } else {
            let logical_type_tag = u8::try_from(logical_type_marker)
                .map_err(|_| invalid("column logical type tag exceeds u8"))?;
            let logical_type = DataType::from_u8(logical_type_tag)
                .ok_or_else(|| invalid("column logical type tag is unknown"))?;
            CatalogDataType::from_fields(
                logical_type,
                read_u16(entry, 22),
                0,
                read_u32(entry, 28),
                read_u32(entry, 32),
                read_array(entry, 48),
            )
            .map_err(|_| invalid("column type descriptor is invalid"))?
        };
        let statistics_entry_index = read_u32(entry, 44);
        if statistics_entry_index != u32::MAX
            && u64::from(statistics_entry_index) >= statistics_count
        {
            return Err(invalid("column statistics index is out of range"));
        }
        columns.push(DataColumn::new(
            column_id,
            ordinal,
            data_type,
            flags == 1,
            read_u32(entry, 36),
            read_u32(entry, 40),
            statistics_entry_index,
        ));
    }
    column_ids.sort_unstable();
    if column_ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid("column IDs are not unique"));
    }
    Ok(columns)
}

pub(crate) fn encode_row_groups(groups: &[DataRowGroup]) -> Vec<u8> {
    let mut output = vec![0_u8; groups.len() * ROW_GROUP_ENTRY_BYTES as usize];
    for (index, group) in groups.iter().copied().enumerate() {
        let offset = index * ROW_GROUP_ENTRY_BYTES as usize;
        let entry = &mut output[offset..offset + ROW_GROUP_ENTRY_BYTES as usize];
        put_u32(entry, 0, group.group_ordinal());
        put_u32(entry, 4, group.row_count());
        put_u64(entry, 8, group.first_row_ordinal());
        put_u64(entry, 16, group.min_row_id());
        put_u64(entry, 24, group.max_row_id());
        put_u32(entry, 32, group.first_block_index());
        put_u32(entry, 36, group.block_count());
    }
    output
}

fn decode_row_groups(
    bytes: &[u8],
    header: &DataArtifactHeader,
    section: DataSectionRef,
) -> FormatResult<Vec<DataRowGroup>> {
    let count = usize::try_from(section.item_count())
        .map_err(|_| invalid("row-group count does not fit this platform"))?;
    let mut groups = Vec::with_capacity(count);
    let mut expected_first_row = 0_u64;
    for index in 0..count {
        let offset = index * ROW_GROUP_ENTRY_BYTES as usize;
        let entry = &bytes[offset..offset + ROW_GROUP_ENTRY_BYTES as usize];
        if read_u32(entry, 40) != 0 || read_u32(entry, 44) != 0 {
            return Err(invalid("row-group flags/reserved bytes are non-zero"));
        }
        let group = DataRowGroup::new(
            read_u32(entry, 0),
            read_u32(entry, 4),
            read_u64(entry, 8),
            read_u64(entry, 16),
            read_u64(entry, 24),
            read_u32(entry, 32),
            read_u32(entry, 36),
        )?;
        if group.group_ordinal() != index as u32 {
            return Err(invalid("row-group ordinal is not contiguous"));
        }
        if group.first_row_ordinal() != expected_first_row {
            return Err(invalid("row-group row ordinals are not contiguous"));
        }
        expected_first_row = expected_first_row
            .checked_add(u64::from(group.row_count()))
            .ok_or_else(|| invalid("row-group row count sum overflows"))?;
        groups.push(group);
    }
    if expected_first_row != header.row_count() {
        return Err(invalid("row-group counts do not sum to header row count"));
    }
    Ok(groups)
}

pub(crate) fn encode_block_ref(entry: &mut [u8], block: &DataBlockRef) {
    debug_assert_eq!(entry.len(), DATA_BLOCK_REF_BYTES);
    put_u16(entry, 0, block.kind().tag());
    put_u16(entry, 2, block.codec().tag());
    put_u32(entry, 4, block.column_ordinal());
    put_u32(entry, 8, block.row_group_ordinal());
    put_u32(entry, 12, u32::from(block.layout().tag()));
    put_u64(entry, 16, block.offset());
    put_u64(entry, 24, block.stored_length());
    put_u64(entry, 32, block.logical_length());
    put_u64(entry, 40, block.item_count());
    put_u32(entry, 48, block.stored_crc32());
}

fn decode_blocks(
    bytes: &[u8],
    header: &DataArtifactHeader,
    section: DataSectionRef,
) -> FormatResult<Vec<DataBlockRef>> {
    let count = usize::try_from(section.item_count())
        .map_err(|_| invalid("block count does not fit this platform"))?;
    let mut blocks = Vec::with_capacity(count);
    let mut previous_key = None;
    for index in 0..count {
        let offset = index * DATA_BLOCK_REF_BYTES;
        let block = decode_block_ref(&bytes[offset..offset + DATA_BLOCK_REF_BYTES])?;
        validate_block_against_header(header, &block)?;
        let key = (
            block.row_group_ordinal(),
            block.kind().tag(),
            block.column_ordinal(),
        );
        if previous_key.is_some_and(|previous| previous >= key) {
            return Err(invalid("block directory order is not canonical"));
        }
        previous_key = Some(key);
        blocks.push(block);
    }
    Ok(blocks)
}

fn validate_group_block_map(
    columns: &[DataColumn],
    groups: &[DataRowGroup],
    blocks: &[DataBlockRef],
) -> FormatResult<()> {
    let mut expected_first_block = 0_usize;
    for group in groups {
        if group.first_block_index() as usize != expected_first_block {
            return Err(invalid("row-group block ranges are not contiguous"));
        }
        let end = expected_first_block
            .checked_add(group.block_count() as usize)
            .ok_or_else(|| invalid("row-group block range overflows"))?;
        let group_blocks = blocks
            .get(expected_first_block..end)
            .ok_or_else(|| invalid("row-group block range is outside directory"))?;
        if group_blocks
            .iter()
            .any(|block| block.row_group_ordinal() != group.group_ordinal())
        {
            return Err(invalid("row-group block range owns another group"));
        }
        let mut row_id_blocks = group_blocks
            .iter()
            .filter(|block| block.kind() == DataBlockKind::RowIds);
        let row_ids = row_id_blocks
            .next()
            .ok_or_else(|| invalid("row group has no row-ID block"))?;
        if row_id_blocks.next().is_some() {
            return Err(invalid("row group has multiple row-ID blocks"));
        }
        if row_ids.item_count() != u64::from(group.row_count()) {
            return Err(invalid("row-ID block count differs from row group"));
        }
        let mut seen_columns = vec![false; columns.len()];
        for block in group_blocks
            .iter()
            .filter(|block| block.kind() == DataBlockKind::Column)
        {
            if block.item_count() != u64::from(group.row_count()) {
                return Err(invalid("column block count differs from row group"));
            }
            let ordinal = block.column_ordinal() as usize;
            let seen = seen_columns
                .get_mut(ordinal)
                .ok_or_else(|| invalid("column block ordinal is out of range"))?;
            if *seen {
                return Err(invalid("row group has multiple blocks for one column"));
            }
            *seen = true;
        }
        if seen_columns.iter().any(|seen| !seen) {
            return Err(invalid("row group does not have one block per column"));
        }
        expected_first_block = end;
    }
    if expected_first_block != blocks.len() {
        return Err(invalid("block directory has unowned entries"));
    }
    Ok(())
}

fn validate_column_block_map(
    columns: &[DataColumn],
    groups: &[DataRowGroup],
    blocks: &[DataBlockRef],
) -> FormatResult<()> {
    let (first_block_indexes, block_counts) = collect_column_block_map(blocks, columns.len())?;
    for column in columns {
        let ordinal = column.ordinal() as usize;
        if first_block_indexes[ordinal] != column.first_block_index() {
            return Err(invalid("column first block index is not canonical"));
        }
        let block_count = block_counts[ordinal];
        if block_count != column.block_count() || block_count as usize != groups.len() {
            return Err(invalid("column block count differs from row-group count"));
        }
    }
    Ok(())
}

fn append_section(output: &mut Vec<u8>, bytes: &[u8]) -> FormatResult<Range<usize>> {
    if bytes.is_empty() {
        return Ok(0..0);
    }
    align_output(output);
    let start = output.len();
    output.extend_from_slice(bytes);
    Ok(start..output.len())
}

fn append_section_placeholder(
    output: &mut Vec<u8>,
    byte_length: usize,
) -> FormatResult<Range<usize>> {
    if byte_length == 0 {
        return Ok(0..0);
    }
    align_output(output);
    let start = output.len();
    let end = start
        .checked_add(byte_length)
        .ok_or_else(|| invalid("section allocation length overflows"))?;
    output.resize(end, 0);
    Ok(start..end)
}

fn align_output(output: &mut Vec<u8>) {
    let padding = (8 - output.len() % 8) % 8;
    output.resize(output.len() + padding, 0);
}

fn validate_canonical_range(
    bytes: &[u8],
    cursor: &mut usize,
    footer_start: usize,
    offset: u64,
    length: u64,
    detail: &'static str,
) -> FormatResult<Range<usize>> {
    if length == 0 {
        if offset != 0 {
            return Err(invalid("empty range has non-zero offset"));
        }
        return Ok(0..0);
    }
    let aligned = cursor
        .checked_add(7)
        .map(|value| value & !7)
        .ok_or_else(|| invalid("range alignment overflows"))?;
    if bytes[*cursor..aligned].iter().any(|byte| *byte != 0) {
        return Err(invalid("alignment padding is non-zero"));
    }
    let start = usize::try_from(offset).map_err(|_| invalid(detail))?;
    let length = usize::try_from(length).map_err(|_| invalid(detail))?;
    let end = start.checked_add(length).ok_or_else(|| invalid(detail))?;
    if start != aligned || start < BODY_START || end > footer_start {
        return Err(invalid(detail));
    }
    *cursor = end;
    Ok(start..end)
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
        .expect("fixed data-artifact range was validated")
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
