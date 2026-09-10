use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;

use sha2::{Digest, Sha256};

use super::super::publication::diagnostics::{record as record_diagnostic, DiagnosticEvent};
use super::codec::{
    encode_block_ref, encode_columns, encode_header, encode_row_groups, encode_section_ref,
    BODY_START, FOOTER_MAGIC, SECTION_DIRECTORY_BYTES, SECTION_DIRECTORY_OFFSET,
};
use super::model::{
    derive_columns_from_refs, invalid, limit, DataArtifactHeader, DataArtifactLayout,
    DataBlockKind, DataBlockRef, DataBlockSpec, DataRowGroup, DataSectionKind, DataSectionRef,
    DATA_BLOCK_REF_BYTES, DATA_FOOTER_BYTES, DATA_HEADER_BYTES, DATA_SECTION_COUNT,
    DATA_SECTION_REF_BYTES, MAX_BLOCKS_PER_DATA_ARTIFACT, MAX_DATA_DIRECTORY_BYTES,
};
use super::statistics::{build_statistics_from_refs, DataStatisticsSpec, STATISTICS_ENTRY_BYTES};
use super::DataColumnSpec;
use crate::v6::{
    fault::reach_generation_boundary, ArtifactKind, ArtifactRef, FormatError, FormatResult,
    GenerationCrashPoint, MAX_ARTIFACT_FILE_BYTES,
};

const HASH_BUFFER_BYTES: usize = 64 * 1024;

/// Sequential row-group writer used by source-stream publication.
///
/// The destination must be empty. Fixed metadata space is reserved once;
/// every encoded row-group block is then written directly at its final file
/// offset. Only bounded directories/statistics remain resident until finish.
pub(crate) struct DataStreamWriter<'a, W> {
    output: &'a mut W,
    header: DataArtifactHeader,
    column_specs: Vec<DataColumnSpec>,
    fixed_ranges: [Range<usize>; 4],
    expected_block_count: usize,
    expected_statistics_count: usize,
    blocks: Vec<DataBlockRef>,
    row_groups: Vec<DataRowGroup>,
    statistics_specs: Vec<DataStatisticsSpec>,
    statistics_payload_bytes: u64,
    metadata_resident_bytes: u64,
    metadata_resident_limit: u64,
    row_count: u64,
    position: u64,
}

impl<'a, W: Read + Write + Seek> DataStreamWriter<'a, W> {
    pub(crate) fn new(
        output: &'a mut W,
        header: DataArtifactHeader,
        column_specs: &[DataColumnSpec],
        bloom_column_count: usize,
        metadata_resident_limit: u64,
    ) -> FormatResult<Self> {
        if output
            .seek(SeekFrom::End(0))
            .map_err(|error| io_error("inspect data destination", error))?
            != 0
        {
            return Err(invalid("streaming data destination is not empty"));
        }
        output
            .seek(SeekFrom::Start(0))
            .map_err(|error| io_error("rewind data destination", error))?;
        record_diagnostic(DiagnosticEvent::DataEncodePass, 1);

        let group_count = header.row_group_count() as usize;
        let blocks_per_group = 1_usize
            .checked_add(column_specs.len())
            .and_then(|count| count.checked_add(bloom_column_count))
            .ok_or_else(|| invalid("blocks per row group overflow"))?;
        let expected_block_count = group_count
            .checked_mul(blocks_per_group)
            .ok_or_else(|| invalid("streaming data block count overflows"))?;
        if expected_block_count as u64 > MAX_BLOCKS_PER_DATA_ARTIFACT {
            return Err(limit(
                "block count",
                expected_block_count as u64,
                MAX_BLOCKS_PER_DATA_ARTIFACT,
            ));
        }
        let expected_statistics_count = group_count
            .checked_mul(column_specs.len())
            .ok_or_else(|| invalid("statistics count overflows"))?;

        let lengths = [
            checked_product(column_specs.len(), 64, "column directory")?,
            checked_product(group_count, 48, "row-group directory")?,
            checked_product(
                expected_block_count,
                DATA_BLOCK_REF_BYTES,
                "block directory",
            )?,
            checked_product(
                expected_statistics_count,
                STATISTICS_ENTRY_BYTES,
                "statistics directory",
            )?,
        ];
        let directory_bytes =
            lengths
                .iter()
                .try_fold(SECTION_DIRECTORY_BYTES as u64, |total, length| {
                    total
                        .checked_add(*length as u64)
                        .ok_or_else(|| invalid("data directory byte count overflows"))
                })?;
        if directory_bytes > MAX_DATA_DIRECTORY_BYTES {
            return Err(limit(
                "directory bytes",
                directory_bytes,
                MAX_DATA_DIRECTORY_BYTES,
            ));
        }

        let metadata_resident_bytes = resident_metadata_bytes(
            column_specs.len(),
            group_count,
            expected_block_count,
            expected_statistics_count,
            directory_bytes,
        )?;
        if metadata_resident_bytes > metadata_resident_limit {
            return Err(limit(
                "streaming data metadata resident bytes",
                metadata_resident_bytes,
                metadata_resident_limit,
            ));
        }

        let mut position = BODY_START;
        let fixed_ranges = std::array::from_fn(|index| {
            reserve_range(&mut position, lengths[index])
                .expect("validated directory shape fits usize")
        });
        write_zeros(output, position as u64)?;

        Ok(Self {
            output,
            header,
            column_specs: column_specs.to_vec(),
            fixed_ranges,
            expected_block_count,
            expected_statistics_count,
            blocks: Vec::with_capacity(expected_block_count),
            row_groups: Vec::with_capacity(group_count),
            statistics_specs: Vec::with_capacity(expected_statistics_count),
            statistics_payload_bytes: 0,
            metadata_resident_bytes,
            metadata_resident_limit,
            row_count: 0,
            position: position as u64,
        })
    }

    pub(crate) fn write_row_group<I>(
        &mut self,
        group_ordinal: u32,
        row_ids: &[u64],
        blocks: I,
        statistics: Vec<DataStatisticsSpec>,
    ) -> FormatResult<()>
    where
        I: IntoIterator<Item = FormatResult<DataBlockSpec>>,
    {
        if group_ordinal as usize != self.row_groups.len() || row_ids.is_empty() {
            return Err(invalid("streamed row-group ordinal/shape is not canonical"));
        }
        let first_block_index = self.blocks.len();
        let mut previous_key = None;
        let mut emitted_blocks = 0_usize;
        for block in blocks {
            let block = block?;
            let key = block_key(&block);
            if block.row_group_ordinal() != group_ordinal
                || previous_key.is_some_and(|previous| previous >= key)
                || (emitted_blocks == 0
                    && (block.kind() != DataBlockKind::RowIds
                        || block.item_count() != row_ids.len() as u64))
                || (emitted_blocks != 0 && block.kind() == DataBlockKind::RowIds)
            {
                return Err(invalid("streamed row-group blocks are not canonical"));
            }
            if first_block_index
                .checked_add(emitted_blocks + 1)
                .is_none_or(|count| count > self.expected_block_count)
            {
                return Err(invalid("streamed block count exceeds reserved directory"));
            }
            self.align()?;
            let stored_crc32 = radixdb_core::crc32_ieee(block.stored_bytes());
            let reference = DataBlockRef::new(
                block.kind(),
                block.codec(),
                block.column_ordinal(),
                block.row_group_ordinal(),
                block.layout(),
                self.position,
                block.stored_bytes().len() as u64,
                block.logical_length(),
                block.item_count(),
                stored_crc32,
            )?;
            write_data(self.output, block.stored_bytes(), "write data block")?;
            self.position = self
                .position
                .checked_add(block.stored_bytes().len() as u64)
                .ok_or_else(|| invalid("streaming data position overflows"))?;
            self.blocks.push(reference);
            previous_key = Some(key);
            emitted_blocks += 1;
        }
        if emitted_blocks == 0 {
            return Err(invalid("streamed row group has no blocks"));
        }

        let block_count = u32::try_from(self.blocks.len() - first_block_index)
            .map_err(|_| invalid("row-group block count does not fit u32"))?;
        let row_count = u32::try_from(row_ids.len())
            .map_err(|_| invalid("row-group count does not fit u32"))?;
        self.row_groups.push(DataRowGroup::new(
            group_ordinal,
            row_count,
            self.row_count,
            row_ids[0],
            row_ids[row_ids.len() - 1],
            u32::try_from(first_block_index)
                .map_err(|_| invalid("first block index does not fit u32"))?,
            block_count,
        )?);
        self.row_count = self
            .row_count
            .checked_add(u64::from(row_count))
            .ok_or_else(|| invalid("streaming row count overflows"))?;
        let added_payload = statistics.iter().try_fold(0_u64, |total, entry| {
            total
                .checked_add(entry.retained_payload_bytes()?)
                .ok_or_else(|| invalid("statistics retained payload bytes overflow"))
        })?;
        let retained_payload = self
            .statistics_payload_bytes
            .checked_add(added_payload)
            .ok_or_else(|| invalid("statistics retained payload bytes overflow"))?;
        let peak_metadata_bytes = self
            .metadata_resident_bytes
            .checked_add(retained_payload.saturating_mul(2))
            .ok_or_else(|| invalid("streaming metadata resident bytes overflow"))?;
        if peak_metadata_bytes > self.metadata_resident_limit {
            return Err(limit(
                "streaming data metadata resident bytes",
                peak_metadata_bytes,
                self.metadata_resident_limit,
            ));
        }
        self.statistics_specs.extend(statistics);
        self.statistics_payload_bytes = retained_payload;
        record_diagnostic(DiagnosticEvent::DataRowGroupEncode, 1);
        Ok(())
    }

    pub(crate) fn finish(mut self) -> FormatResult<(ArtifactRef, DataArtifactLayout)> {
        if self.row_count != self.header.row_count()
            || self.row_groups.len() != self.header.row_group_count() as usize
            || self.blocks.len() != self.expected_block_count
            || self.statistics_specs.len() != self.expected_statistics_count
        {
            return Err(invalid("streamed data shape differs from reserved header"));
        }
        self.statistics_specs
            .sort_by_key(|entry| (entry.column_ordinal(), entry.row_group_ordinal()));
        let statistics = build_statistics_from_refs(
            &self.column_specs,
            &self.row_groups,
            &self.blocks,
            std::mem::take(&mut self.statistics_specs),
        )?;
        let columns = derive_columns_from_refs(
            &self.header,
            &self.column_specs,
            &self.row_groups,
            &statistics.entries,
            &self.blocks,
        )?;
        let fixed_bytes = [
            encode_columns(&columns),
            encode_row_groups(&self.row_groups),
            encode_block_directory(&self.blocks),
            statistics.directory,
        ];
        for (range, bytes) in self.fixed_ranges.iter().zip(&fixed_bytes) {
            if range.len() != bytes.len() {
                return Err(invalid("streamed directory differs from reserved shape"));
            }
            write_at(
                self.output,
                range.start as u64,
                bytes,
                "write data directory",
            )?;
        }

        self.seek_to_end()?;
        let statistics_values_range = if statistics.values.is_empty() {
            0..0
        } else {
            self.align()?;
            let start = usize::try_from(self.position)
                .map_err(|_| invalid("statistics value offset does not fit usize"))?;
            write_data(self.output, &statistics.values, "write statistics values")?;
            self.position = self
                .position
                .checked_add(statistics.values.len() as u64)
                .ok_or_else(|| invalid("statistics value range overflows"))?;
            start
                ..usize::try_from(self.position)
                    .map_err(|_| invalid("statistics value end does not fit usize"))?
        };

        let file_length = self
            .position
            .checked_add(DATA_FOOTER_BYTES as u64)
            .ok_or_else(|| invalid("streamed data file length overflows"))?;
        if file_length > MAX_ARTIFACT_FILE_BYTES {
            return Err(limit("file bytes", file_length, MAX_ARTIFACT_FILE_BYTES));
        }

        let ranges: [Range<usize>; DATA_SECTION_COUNT] = std::array::from_fn(|index| match index {
            0..=3 => self.fixed_ranges[index].clone(),
            _ => statistics_values_range.clone(),
        });
        let counts = [
            columns.len() as u64,
            self.row_groups.len() as u64,
            self.blocks.len() as u64,
            statistics.entries.len() as u64,
            statistics.values.len() as u64,
        ];
        let section_crcs = [
            radixdb_core::crc32_ieee(&fixed_bytes[0]),
            radixdb_core::crc32_ieee(&fixed_bytes[1]),
            radixdb_core::crc32_ieee(&fixed_bytes[2]),
            radixdb_core::crc32_ieee(&fixed_bytes[3]),
            radixdb_core::crc32_ieee(&statistics.values),
        ];
        let mut shell = [0_u8; BODY_START];
        encode_header(
            &mut shell[..DATA_HEADER_BYTES],
            self.header,
            usize::try_from(file_length)
                .map_err(|_| invalid("data file length does not fit usize"))?,
        )?;
        let mut sections =
            [DataSectionRef::new(DataSectionKind::ColumnDirectory, 0, 0, 0, 0); DATA_SECTION_COUNT];
        for (index, kind) in DataSectionKind::ALL.into_iter().enumerate() {
            let offset = SECTION_DIRECTORY_OFFSET + index * DATA_SECTION_REF_BYTES;
            encode_section_ref(
                &mut shell[offset..offset + DATA_SECTION_REF_BYTES],
                kind,
                &ranges[index],
                counts[index],
                section_crcs[index],
            )?;
            sections[index] = DataSectionRef::new(
                kind,
                ranges[index].start as u64,
                ranges[index].len() as u64,
                counts[index],
                section_crcs[index],
            );
        }
        let header_crc = radixdb_core::crc32_ieee(&shell[..248]);
        shell[248..252].copy_from_slice(&header_crc.to_le_bytes());
        write_at(self.output, 0, &shell, "write data header")?;

        reach_generation_boundary(GenerationCrashPoint::DataAfterBodyBeforeFooter)
            .map_err(|error| io_error("inject after data body", error))?;

        let body_sha = digest_prefix(self.output, self.position)?;
        self.output
            .seek(SeekFrom::Start(self.position))
            .map_err(|error| io_error("seek data footer", error))?;
        write_data(self.output, &FOOTER_MAGIC, "write data footer magic")?;
        write_data(
            self.output,
            &file_length.to_le_bytes(),
            "write data footer length",
        )?;
        write_data(self.output, &body_sha, "write data footer identity")?;
        self.output
            .flush()
            .map_err(|error| io_error("finalize data artifact", error))?;
        reach_generation_boundary(GenerationCrashPoint::DataAfterFooterBeforeSync)
            .map_err(|error| io_error("inject after data footer", error))?;

        let reference = ArtifactRef::new(
            self.header.artifact_id(),
            ArtifactKind::Data,
            self.header.creation_generation(),
            file_length,
            body_sha,
        )?;
        let layout = DataArtifactLayout::new(
            reference,
            self.header,
            sections,
            columns,
            self.row_groups,
            statistics.entries,
            self.blocks,
        );
        Ok((reference, layout))
    }

    fn align(&mut self) -> FormatResult<()> {
        let aligned = self
            .position
            .checked_add(7)
            .map(|value| value & !7)
            .ok_or_else(|| invalid("streaming data alignment overflows"))?;
        write_zeros(self.output, aligned - self.position)?;
        self.position = aligned;
        Ok(())
    }

    fn seek_to_end(&mut self) -> FormatResult<()> {
        let actual = self
            .output
            .seek(SeekFrom::End(0))
            .map_err(|error| io_error("seek to streamed data end", error))?;
        if actual != self.position {
            return Err(invalid("streaming data destination length drifted"));
        }
        Ok(())
    }
}

fn checked_product(count: usize, width: usize, owner: &'static str) -> FormatResult<usize> {
    count.checked_mul(width).ok_or_else(|| invalid(owner))
}

fn resident_metadata_bytes(
    column_count: usize,
    group_count: usize,
    block_count: usize,
    statistics_count: usize,
    directory_bytes: u64,
) -> FormatResult<u64> {
    let owners = [
        (column_count, std::mem::size_of::<DataColumnSpec>()),
        (group_count, std::mem::size_of::<DataRowGroup>()),
        (block_count, std::mem::size_of::<DataBlockRef>()),
        (statistics_count, std::mem::size_of::<DataStatisticsSpec>()),
        // `finish` moves each spec into a durable statistics entry while the
        // source Vec allocation and encoded references still exist.
        (
            statistics_count,
            std::mem::size_of::<super::super::DataStatistics>() + 32,
        ),
        (block_count, std::mem::size_of::<bool>()),
    ];
    owners
        .into_iter()
        .try_fold(directory_bytes, |total, (count, width)| {
            let bytes = count
                .checked_mul(width)
                .ok_or_else(|| invalid("streaming metadata resident shape overflows"))?;
            total
                .checked_add(bytes as u64)
                .ok_or_else(|| invalid("streaming metadata resident bytes overflow"))
        })
}

fn reserve_range(position: &mut usize, length: usize) -> FormatResult<Range<usize>> {
    if length == 0 {
        return Ok(0..0);
    }
    *position = position
        .checked_add(7)
        .map(|value| value & !7)
        .ok_or_else(|| invalid("directory alignment overflows"))?;
    let start = *position;
    *position = position
        .checked_add(length)
        .ok_or_else(|| invalid("directory range overflows"))?;
    Ok(start..*position)
}

fn block_key(block: &DataBlockSpec) -> (u32, u16, u32) {
    (
        block.row_group_ordinal(),
        block.kind().tag(),
        block.column_ordinal(),
    )
}

fn encode_block_directory(blocks: &[DataBlockRef]) -> Vec<u8> {
    let mut bytes = vec![0_u8; blocks.len() * DATA_BLOCK_REF_BYTES];
    for (index, block) in blocks.iter().enumerate() {
        let offset = index * DATA_BLOCK_REF_BYTES;
        encode_block_ref(&mut bytes[offset..offset + DATA_BLOCK_REF_BYTES], block);
    }
    bytes
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
    write_data(output, bytes, operation)
}

fn write_zeros(output: &mut impl Write, mut length: u64) -> FormatResult<()> {
    let zeros = [0_u8; 8192];
    while length != 0 {
        let chunk = usize::try_from(length.min(zeros.len() as u64))
            .expect("zero chunk is bounded by fixed buffer");
        write_data(output, &zeros[..chunk], "reserve data range")?;
        length -= chunk as u64;
    }
    Ok(())
}

fn digest_prefix(output: &mut (impl Read + Seek), body_length: u64) -> FormatResult<[u8; 32]> {
    output
        .seek(SeekFrom::Start(0))
        .map_err(|error| io_error("rewind data for identity", error))?;
    let mut remaining = body_length;
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    let mut digest = Sha256::new();
    while remaining != 0 {
        let length = usize::try_from(remaining.min(buffer.len() as u64))
            .expect("identity chunk is bounded by fixed buffer");
        output
            .read_exact(&mut buffer[..length])
            .map_err(|error| io_error("read data for identity", error))?;
        record_diagnostic(DiagnosticEvent::DataIdentityRead, length as u64);
        digest.update(&buffer[..length]);
        remaining -= length as u64;
    }
    Ok(digest.finalize().into())
}

fn write_data(output: &mut impl Write, bytes: &[u8], operation: &'static str) -> FormatResult<()> {
    output
        .write_all(bytes)
        .map_err(|error| io_error(operation, error))?;
    record_diagnostic(DiagnosticEvent::DataWrite, bytes.len() as u64);
    Ok(())
}

fn io_error(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::ArtifactIo {
        operation,
        kind: error.kind(),
    }
}
