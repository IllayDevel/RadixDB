use std::cmp::Ordering;

#[cfg(test)]
#[path = "statistics_tests.rs"]
mod tests;

use radixdb_core::Value;

use super::super::FormatResult;
use super::column::{decode_non_null_value, encode_non_null_value, validate_column_values};
use super::column_model::{DataColumn, DataColumnSpec};
use super::model::{
    invalid, limit, DataBlockKind, DataBlockRef, DataBlockSpec, DataRowGroup,
    MAX_BLOOM_BITS_PER_GROUP_COLUMN, MAX_ROWS_PER_GROUP, MAX_STATISTICS_VALUES_BYTES,
    MAX_STATISTIC_VALUE_BYTES,
};

pub(crate) const STATISTICS_ENTRY_BYTES: usize = 80;

const FLAG_MINIMUM: u32 = 1 << 0;
const FLAG_MAXIMUM: u32 = 1 << 1;
const FLAG_DISTINCT: u32 = 1 << 2;
const FLAG_BLOOM: u32 = 1 << 3;
const FLAG_NUMERIC_SUM: u32 = 1 << 4;
const KNOWN_FLAGS: u32 =
    FLAG_MINIMUM | FLAG_MAXIMUM | FLAG_DISTINCT | FLAG_BLOOM | FLAG_NUMERIC_SUM;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataStatisticsSpec {
    column_ordinal: u32,
    row_group_ordinal: u32,
    column: DataColumnSpec,
    value_count: u64,
    null_count: u64,
    distinct_estimate: Option<u64>,
    minimum: Option<Value>,
    maximum: Option<Value>,
    integer_sum: Option<i128>,
    float_sum_bits: Option<u64>,
    numeric_count: u64,
}

impl DataStatisticsSpec {
    pub fn from_values(
        column_ordinal: u32,
        row_group_ordinal: u32,
        column: DataColumnSpec,
        values: &[Value],
        distinct_estimate: Option<u64>,
    ) -> FormatResult<Self> {
        if values.is_empty() || values.len() > MAX_ROWS_PER_GROUP as usize {
            return Err(limit(
                "statistics values per group",
                values.len() as u64,
                u64::from(MAX_ROWS_PER_GROUP),
            ));
        }
        validate_column_values(column, values)?;
        Self::from_validated_values(
            column_ordinal,
            row_group_ordinal,
            column,
            values,
            distinct_estimate,
        )
    }

    pub(crate) fn from_validated_values(
        column_ordinal: u32,
        row_group_ordinal: u32,
        column: DataColumnSpec,
        values: &[Value],
        distinct_estimate: Option<u64>,
    ) -> FormatResult<Self> {
        if values.is_empty() || values.len() > MAX_ROWS_PER_GROUP as usize {
            return Err(limit(
                "statistics values per group",
                values.len() as u64,
                u64::from(MAX_ROWS_PER_GROUP),
            ));
        }
        let null_count = values.iter().filter(|value| value.is_null()).count() as u64;
        let non_null_count = values.len() as u64 - null_count;
        if column.data_type().is_external() && distinct_estimate.is_some() {
            return Err(invalid(
                "external distinct statistics require an operator class",
            ));
        }
        if distinct_estimate.is_some_and(|estimate| estimate == 0 || estimate > non_null_count) {
            return Err(invalid("distinct estimate is outside non-NULL row count"));
        }
        let (minimum, maximum) = bounded_min_max(column, values)?;
        let (integer_sum, float_sum_bits, numeric_count) = numeric_aggregate(column, values)?;
        Ok(Self {
            column_ordinal,
            row_group_ordinal,
            column,
            value_count: values.len() as u64,
            null_count,
            distinct_estimate,
            minimum,
            maximum,
            integer_sum,
            float_sum_bits,
            numeric_count,
        })
    }

    pub const fn column_ordinal(&self) -> u32 {
        self.column_ordinal
    }

    pub const fn row_group_ordinal(&self) -> u32 {
        self.row_group_ordinal
    }

    pub const fn column(&self) -> DataColumnSpec {
        self.column
    }

    pub(crate) fn retained_payload_bytes(&self) -> FormatResult<u64> {
        let bound_bytes = [self.minimum.as_ref(), self.maximum.as_ref()]
            .into_iter()
            .flatten()
            .try_fold(0_u64, |total, value| {
                let bytes = match value {
                    Value::Text(text) => text.len() as u64,
                    Value::Extension(bytes) => bytes.len() as u64,
                    _ => 0,
                };
                total
                    .checked_add(bytes)
                    .ok_or_else(|| invalid("statistics retained payload bytes overflow"))
            })?;
        bound_bytes
            .checked_add(match (self.integer_sum, self.float_sum_bits) {
                (Some(_), None) => 16,
                (None, Some(_)) => 8,
                (None, None) => 0,
                (Some(_), Some(_)) => {
                    return Err(invalid("statistics has two numeric aggregate kinds"));
                }
            })
            .ok_or_else(|| invalid("statistics retained payload bytes overflow"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataStatistics {
    column_id: radixdb_catalog::ObjectId,
    column_ordinal: u32,
    row_group_ordinal: u32,
    null_count: u64,
    distinct_estimate: Option<u64>,
    minimum: Option<Value>,
    maximum: Option<Value>,
    bloom_block_index: Option<u32>,
    integer_sum: Option<i128>,
    float_sum_bits: Option<u64>,
    numeric_count: u64,
}

impl DataStatistics {
    pub const fn column_id(&self) -> radixdb_catalog::ObjectId {
        self.column_id
    }

    pub const fn column_ordinal(&self) -> u32 {
        self.column_ordinal
    }

    pub const fn row_group_ordinal(&self) -> u32 {
        self.row_group_ordinal
    }

    pub const fn null_count(&self) -> u64 {
        self.null_count
    }

    pub const fn distinct_estimate(&self) -> Option<u64> {
        self.distinct_estimate
    }

    pub fn minimum(&self) -> Option<&Value> {
        self.minimum.as_ref()
    }

    pub fn maximum(&self) -> Option<&Value> {
        self.maximum.as_ref()
    }

    pub const fn bloom_block_index(&self) -> Option<u32> {
        self.bloom_block_index
    }

    pub const fn integer_sum(&self) -> Option<i128> {
        self.integer_sum
    }

    pub fn float_sum(&self) -> Option<f64> {
        self.float_sum_bits.map(f64::from_bits)
    }

    pub const fn numeric_count(&self) -> u64 {
        self.numeric_count
    }
}

pub(crate) struct StatisticsEncoding {
    pub entries: Vec<DataStatistics>,
    pub directory: Vec<u8>,
    pub values: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
struct ValueRef {
    offset: u64,
    length: u32,
    crc32: u32,
}

impl ValueRef {
    const fn absent() -> Self {
        Self {
            offset: 0,
            length: 0,
            crc32: 0,
        }
    }
}

pub(crate) fn build_statistics(
    columns: &[DataColumnSpec],
    groups: &[DataRowGroup],
    blocks: &[DataBlockSpec],
    specs: Vec<DataStatisticsSpec>,
) -> FormatResult<StatisticsEncoding> {
    build_statistics_with_blocks(columns, groups, blocks, specs)
}

pub(crate) fn build_statistics_from_refs(
    columns: &[DataColumnSpec],
    groups: &[DataRowGroup],
    blocks: &[DataBlockRef],
    specs: Vec<DataStatisticsSpec>,
) -> FormatResult<StatisticsEncoding> {
    build_statistics_with_blocks(columns, groups, blocks, specs)
}

trait StatisticsBlock {
    fn kind(&self) -> DataBlockKind;
    fn row_group_ordinal(&self) -> u32;
    fn column_ordinal(&self) -> u32;
    fn validate_bloom_source(&self, column: DataColumnSpec, value_count: u64) -> FormatResult<()>;
}

impl StatisticsBlock for DataBlockSpec {
    fn kind(&self) -> DataBlockKind {
        self.kind()
    }

    fn row_group_ordinal(&self) -> u32 {
        self.row_group_ordinal()
    }

    fn column_ordinal(&self) -> u32 {
        self.column_ordinal()
    }

    fn validate_bloom_source(&self, column: DataColumnSpec, value_count: u64) -> FormatResult<()> {
        if self.column_spec() != Some(column) || self.source_value_count() != Some(value_count) {
            return Err(invalid(
                "bloom source descriptor/count differs from statistics",
            ));
        }
        Ok(())
    }
}

impl StatisticsBlock for DataBlockRef {
    fn kind(&self) -> DataBlockKind {
        self.kind()
    }

    fn row_group_ordinal(&self) -> u32 {
        self.row_group_ordinal()
    }

    fn column_ordinal(&self) -> u32 {
        self.column_ordinal()
    }

    fn validate_bloom_source(&self, _column: DataColumnSpec, value_count: u64) -> FormatResult<()> {
        if value_count == 0 {
            return Err(invalid("bloom source row count is zero"));
        }
        Ok(())
    }
}

fn build_statistics_with_blocks<B: StatisticsBlock>(
    columns: &[DataColumnSpec],
    groups: &[DataRowGroup],
    blocks: &[B],
    specs: Vec<DataStatisticsSpec>,
) -> FormatResult<StatisticsEncoding> {
    let maximum = columns
        .len()
        .checked_mul(groups.len())
        .ok_or_else(|| invalid("statistics count multiplication overflows"))?;
    if specs.len() > maximum {
        return Err(limit(
            "statistics entry count",
            specs.len() as u64,
            maximum as u64,
        ));
    }
    if specs
        .windows(2)
        .any(|pair| statistics_spec_key(&pair[0]) >= statistics_spec_key(&pair[1]))
    {
        return Err(invalid("statistics entries are not in canonical order"));
    }
    if blocks
        .windows(2)
        .any(|pair| statistics_block_key(&pair[0]) >= statistics_block_key(&pair[1]))
    {
        return Err(invalid("blocks are not in canonical directory order"));
    }

    let mut values = Vec::new();
    let mut encoded = Vec::with_capacity(specs.len());
    let mut referenced_blooms = vec![false; blocks.len()];
    for spec in specs {
        let column = columns
            .get(spec.column_ordinal as usize)
            .copied()
            .ok_or_else(|| invalid("statistics column ordinal is out of range"))?;
        if spec.column != column {
            return Err(invalid(
                "statistics column descriptor differs from table column",
            ));
        }
        let group = groups
            .get(spec.row_group_ordinal as usize)
            .ok_or_else(|| invalid("statistics row-group ordinal is out of range"))?;
        if spec.value_count != u64::from(group.row_count()) || spec.null_count > spec.value_count {
            return Err(invalid("statistics counts differ from row group"));
        }
        let bloom_block_index = find_bloom_block(
            blocks,
            spec.row_group_ordinal,
            spec.column_ordinal,
            spec.column,
            spec.value_count,
        )?;
        if let Some(index) = bloom_block_index {
            referenced_blooms[index as usize] = true;
        }
        let minimum_ref = append_value(&mut values, spec.column, spec.minimum.as_ref())?;
        let maximum_ref = append_value(&mut values, spec.column, spec.maximum.as_ref())?;
        let numeric_sum_crc32 =
            append_numeric_sum(&mut values, spec.integer_sum, spec.float_sum_bits)?;
        encoded.push((
            DataStatistics {
                column_id: column.column_id(),
                column_ordinal: spec.column_ordinal,
                row_group_ordinal: spec.row_group_ordinal,
                null_count: spec.null_count,
                distinct_estimate: spec.distinct_estimate,
                minimum: spec.minimum,
                maximum: spec.maximum,
                bloom_block_index,
                integer_sum: spec.integer_sum,
                float_sum_bits: spec.float_sum_bits,
                numeric_count: spec.numeric_count,
            },
            minimum_ref,
            maximum_ref,
            numeric_sum_crc32,
        ));
    }
    for (index, block) in blocks.iter().enumerate() {
        if block.kind() == DataBlockKind::Bloom && !referenced_blooms[index] {
            return Err(invalid("bloom block has no owning statistics entry"));
        }
    }
    let mut directory = vec![0_u8; encoded.len() * STATISTICS_ENTRY_BYTES];
    for (index, (statistics, minimum, maximum, numeric_sum_crc32)) in encoded.iter().enumerate() {
        let offset = index * STATISTICS_ENTRY_BYTES;
        encode_statistics_entry(
            &mut directory[offset..offset + STATISTICS_ENTRY_BYTES],
            statistics,
            *minimum,
            *maximum,
            *numeric_sum_crc32,
        );
    }
    Ok(StatisticsEncoding {
        entries: encoded.into_iter().map(|entry| entry.0).collect(),
        directory,
        values,
    })
}

pub(crate) fn decode_statistics(
    directory_bytes: &[u8],
    value_bytes: &[u8],
    count: usize,
    columns: &[DataColumn],
    groups: &[DataRowGroup],
    blocks: &[DataBlockRef],
) -> FormatResult<Vec<DataStatistics>> {
    let expected_directory_bytes = count
        .checked_mul(STATISTICS_ENTRY_BYTES)
        .ok_or_else(|| invalid("statistics directory length overflows"))?;
    if directory_bytes.len() != expected_directory_bytes {
        return Err(invalid("statistics directory length is not canonical"));
    }
    let mut column_lookup = columns
        .iter()
        .map(|column| (column.column_id(), column.ordinal()))
        .collect::<Vec<_>>();
    column_lookup.sort_unstable_by_key(|entry| entry.0);
    if column_lookup.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(invalid("column IDs are not unique"));
    }
    let mut statistics = Vec::with_capacity(count);
    let mut value_cursor = 0_usize;
    let mut referenced_blooms = vec![false; blocks.len()];
    for index in 0..count {
        let offset = index * STATISTICS_ENTRY_BYTES;
        let entry = &directory_bytes[offset..offset + STATISTICS_ENTRY_BYTES];
        let decoded = decode_statistics_entry(
            entry,
            value_bytes,
            &mut value_cursor,
            columns,
            &column_lookup,
            groups,
            blocks,
            &mut referenced_blooms,
        )?;
        if statistics.last().is_some_and(|previous: &DataStatistics| {
            statistics_key(previous) >= statistics_key(&decoded)
        }) {
            return Err(invalid("statistics entries are not in canonical order"));
        }
        statistics.push(decoded);
    }
    if value_cursor != value_bytes.len() {
        return Err(invalid("statistics values contain unowned bytes"));
    }
    for (index, block) in blocks.iter().enumerate() {
        if block.kind() == DataBlockKind::Bloom && !referenced_blooms[index] {
            return Err(invalid("bloom block has no owning statistics entry"));
        }
    }
    validate_column_statistics_indexes(columns, &statistics)?;
    Ok(statistics)
}

fn bounded_min_max(
    column: DataColumnSpec,
    values: &[Value],
) -> FormatResult<(Option<Value>, Option<Value>)> {
    if column.data_type().is_external() {
        return Ok((None, None));
    }
    if !column.data_type().logical_type().is_orderable() {
        return Ok((None, None));
    }
    let mut minimum: Option<Value> = None;
    let mut maximum: Option<Value> = None;
    for value in values.iter().filter(|value| !value.is_null()) {
        let replaces_minimum = match minimum.as_ref() {
            Some(bound) => {
                value
                    .compare(bound)
                    .map_err(|_| invalid("statistic values cannot be compared"))?
                    == Ordering::Less
            }
            None => true,
        };
        if replaces_minimum {
            minimum = Some(value.clone());
        }
        let replaces_maximum = match maximum.as_ref() {
            Some(bound) => {
                value
                    .compare(bound)
                    .map_err(|_| invalid("statistic values cannot be compared"))?
                    == Ordering::Greater
            }
            None => true,
        };
        if replaces_maximum {
            maximum = Some(value.clone());
        }
    }
    if let (Some(minimum_value), Some(maximum_value)) = (&minimum, &maximum) {
        let minimum_bytes = encode_non_null_value(column, minimum_value)?;
        let maximum_bytes = encode_non_null_value(column, maximum_value)?;
        if minimum_bytes.len() as u64 > MAX_STATISTIC_VALUE_BYTES
            || maximum_bytes.len() as u64 > MAX_STATISTIC_VALUE_BYTES
        {
            return Ok((None, None));
        }
    }
    Ok((minimum, maximum))
}

fn numeric_aggregate(
    column: DataColumnSpec,
    values: &[Value],
) -> FormatResult<(Option<i128>, Option<u64>, u64)> {
    match column.data_type().logical_type() {
        radixdb_core::DataType::Integer => {
            let mut sum = 0_i128;
            let mut count = 0_u64;
            for value in values {
                if let Value::Integer(value) = value {
                    sum = sum
                        .checked_add(i128::from(*value))
                        .ok_or_else(|| invalid("INTEGER statistics sum overflows i128"))?;
                    count += 1;
                }
            }
            Ok((Some(sum), None, count))
        }
        radixdb_core::DataType::Float => {
            let mut sum = 0.0_f64;
            let mut count = 0_u64;
            for value in values {
                if let Value::Float(value) = value {
                    sum += value;
                    count += 1;
                }
            }
            Ok((None, Some(sum.to_bits()), count))
        }
        _ => Ok((None, None, 0)),
    }
}

fn find_bloom_block<B: StatisticsBlock>(
    blocks: &[B],
    row_group_ordinal: u32,
    column_ordinal: u32,
    column: DataColumnSpec,
    value_count: u64,
) -> FormatResult<Option<u32>> {
    // Both builders supply the canonical (group, kind, column) directory.
    let Ok(index) = blocks.binary_search_by_key(
        &(row_group_ordinal, DataBlockKind::Bloom, column_ordinal),
        statistics_block_key,
    ) else {
        return Ok(None);
    };
    blocks[index].validate_bloom_source(column, value_count)?;
    u32::try_from(index)
        .map(Some)
        .map_err(|_| invalid("bloom block index does not fit u32"))
}

fn statistics_block_key(block: &impl StatisticsBlock) -> (u32, DataBlockKind, u32) {
    (
        block.row_group_ordinal(),
        block.kind(),
        block.column_ordinal(),
    )
}

fn append_value(
    output: &mut Vec<u8>,
    column: DataColumnSpec,
    value: Option<&Value>,
) -> FormatResult<ValueRef> {
    let Some(value) = value else {
        return Ok(ValueRef::absent());
    };
    let bytes = encode_non_null_value(column, value)?;
    if bytes.len() as u64 > MAX_STATISTIC_VALUE_BYTES {
        return Err(limit(
            "statistic value bytes",
            bytes.len() as u64,
            MAX_STATISTIC_VALUE_BYTES,
        ));
    }
    let new_length = output
        .len()
        .checked_add(bytes.len())
        .ok_or_else(|| invalid("statistics values length overflows"))?;
    if new_length as u64 > MAX_STATISTICS_VALUES_BYTES {
        return Err(limit(
            "statistics values bytes",
            new_length as u64,
            MAX_STATISTICS_VALUES_BYTES,
        ));
    }
    let reference = ValueRef {
        offset: output.len() as u64,
        length: bytes.len() as u32,
        crc32: radixdb_core::crc32_ieee(&bytes),
    };
    output.extend_from_slice(&bytes);
    Ok(reference)
}

fn append_numeric_sum(
    output: &mut Vec<u8>,
    integer_sum: Option<i128>,
    float_sum_bits: Option<u64>,
) -> FormatResult<u32> {
    let bytes = match (integer_sum, float_sum_bits) {
        (Some(value), None) => value.to_le_bytes().to_vec(),
        (None, Some(bits)) => bits.to_le_bytes().to_vec(),
        (None, None) => return Ok(0),
        (Some(_), Some(_)) => return Err(invalid("statistics has two numeric aggregate kinds")),
    };
    let new_length = output
        .len()
        .checked_add(bytes.len())
        .ok_or_else(|| invalid("statistics values length overflows"))?;
    if new_length as u64 > MAX_STATISTICS_VALUES_BYTES {
        return Err(limit(
            "statistics values bytes",
            new_length as u64,
            MAX_STATISTICS_VALUES_BYTES,
        ));
    }
    let crc32 = radixdb_core::crc32_ieee(&bytes);
    output.extend_from_slice(&bytes);
    Ok(crc32)
}

fn encode_statistics_entry(
    entry: &mut [u8],
    statistics: &DataStatistics,
    minimum: ValueRef,
    maximum: ValueRef,
    numeric_sum_crc32: u32,
) {
    entry[..16].copy_from_slice(statistics.column_id().as_bytes());
    put_u32(entry, 16, statistics.row_group_ordinal());
    let mut flags = 0_u32;
    if statistics.minimum().is_some() {
        flags |= FLAG_MINIMUM | FLAG_MAXIMUM;
    }
    if statistics.distinct_estimate().is_some() {
        flags |= FLAG_DISTINCT;
    }
    if statistics.bloom_block_index().is_some() {
        flags |= FLAG_BLOOM;
    }
    if statistics.integer_sum().is_some() || statistics.float_sum().is_some() {
        flags |= FLAG_NUMERIC_SUM;
    }
    put_u32(entry, 20, flags);
    put_u64(entry, 24, statistics.null_count());
    put_u64(entry, 32, statistics.distinct_estimate().unwrap_or(0));
    put_value_ref(entry, 40, minimum);
    put_value_ref(entry, 56, maximum);
    put_u32(
        entry,
        72,
        statistics.bloom_block_index().unwrap_or(u32::MAX),
    );
    put_u32(entry, 76, numeric_sum_crc32);
}

#[allow(clippy::too_many_arguments)]
fn decode_statistics_entry(
    entry: &[u8],
    value_bytes: &[u8],
    value_cursor: &mut usize,
    columns: &[DataColumn],
    column_lookup: &[(radixdb_catalog::ObjectId, u32)],
    groups: &[DataRowGroup],
    blocks: &[DataBlockRef],
    referenced_blooms: &mut [bool],
) -> FormatResult<DataStatistics> {
    let column_id = radixdb_catalog::ObjectId::from_user_bytes(read_array(entry, 0))
        .map_err(|_| invalid("statistics column ID is not a user catalog identity"))?;
    let row_group_ordinal = read_u32(entry, 16);
    let flags = read_u32(entry, 20);
    if flags & !KNOWN_FLAGS != 0 || (flags & FLAG_MINIMUM == 0) != (flags & FLAG_MAXIMUM == 0) {
        return Err(invalid("statistics flags are invalid"));
    }
    let column_ordinal = column_lookup
        .binary_search_by_key(&column_id, |entry| entry.0)
        .map(|index| column_lookup[index].1)
        .map_err(|_| invalid("statistics column ID is not in column directory"))?;
    let column = columns[column_ordinal as usize];
    let group = groups
        .get(row_group_ordinal as usize)
        .ok_or_else(|| invalid("statistics row-group ordinal is out of range"))?;
    let null_count = read_u64(entry, 24);
    if null_count > u64::from(group.row_count()) {
        return Err(invalid("statistics NULL count exceeds row group"));
    }
    let encoded_distinct = read_u64(entry, 32);
    let distinct_estimate = if flags & FLAG_DISTINCT != 0 {
        let non_null = u64::from(group.row_count()) - null_count;
        if encoded_distinct == 0 || encoded_distinct > non_null {
            return Err(invalid("distinct estimate is outside non-NULL row count"));
        }
        Some(encoded_distinct)
    } else {
        if encoded_distinct != 0 {
            return Err(invalid("absent distinct estimate is non-zero"));
        }
        None
    };
    let has_bounds = flags & FLAG_MINIMUM != 0;
    let minimum = decode_value_ref(value_bytes, value_cursor, entry, 40, has_bounds, column)?;
    let maximum = decode_value_ref(value_bytes, value_cursor, entry, 56, has_bounds, column)?;
    if let (Some(minimum), Some(maximum)) = (&minimum, &maximum) {
        if column.data_type().is_external()
            || !column.data_type().logical_type().is_orderable()
            || minimum
                .compare(maximum)
                .map_err(|_| invalid("statistics bounds cannot be compared"))?
                == Ordering::Greater
        {
            return Err(invalid("statistics bounds are invalid"));
        }
    }
    let encoded_bloom_index = read_u32(entry, 72);
    let bloom_block_index = if flags & FLAG_BLOOM != 0 {
        let block = blocks
            .get(encoded_bloom_index as usize)
            .ok_or_else(|| invalid("statistics bloom block index is out of range"))?;
        if block.kind() != DataBlockKind::Bloom
            || block.column_ordinal() != column_ordinal
            || block.row_group_ordinal() != row_group_ordinal
            || block.item_count() == 0
            || block.item_count() > MAX_BLOOM_BITS_PER_GROUP_COLUMN
        {
            return Err(invalid("statistics bloom block does not match its owner"));
        }
        let referenced = referenced_blooms
            .get_mut(encoded_bloom_index as usize)
            .ok_or_else(|| invalid("statistics bloom block index is out of range"))?;
        if *referenced {
            return Err(invalid("bloom block has multiple statistics owners"));
        }
        *referenced = true;
        Some(encoded_bloom_index)
    } else {
        if encoded_bloom_index != u32::MAX {
            return Err(invalid("absent bloom reference is not canonical"));
        }
        None
    };
    let (integer_sum, float_sum_bits, numeric_count) = decode_numeric_sum(
        value_bytes,
        value_cursor,
        entry,
        flags,
        column,
        group,
        null_count,
    )?;
    Ok(DataStatistics {
        column_id,
        column_ordinal,
        row_group_ordinal,
        null_count,
        distinct_estimate,
        minimum,
        maximum,
        bloom_block_index,
        integer_sum,
        float_sum_bits,
        numeric_count,
    })
}

#[allow(clippy::too_many_arguments)]
fn decode_numeric_sum(
    value_bytes: &[u8],
    cursor: &mut usize,
    entry: &[u8],
    flags: u32,
    column: DataColumn,
    group: &DataRowGroup,
    null_count: u64,
) -> FormatResult<(Option<i128>, Option<u64>, u64)> {
    let encoded_crc32 = read_u32(entry, 76);
    let data_type = column.data_type().logical_type();
    let numeric_width = match data_type {
        radixdb_core::DataType::Integer => Some(16_usize),
        radixdb_core::DataType::Float => Some(8_usize),
        _ => None,
    };
    if (flags & FLAG_NUMERIC_SUM != 0) != numeric_width.is_some() {
        return Err(invalid(
            "statistics numeric sum presence differs from column type",
        ));
    }
    let Some(width) = numeric_width else {
        if encoded_crc32 != 0 {
            return Err(invalid("absent statistics numeric sum CRC is non-zero"));
        }
        return Ok((None, None, 0));
    };
    let end = cursor
        .checked_add(width)
        .ok_or_else(|| invalid("statistics numeric sum range overflows"))?;
    let bytes = value_bytes
        .get(*cursor..end)
        .ok_or_else(|| invalid("statistics numeric sum is outside section"))?;
    if radixdb_core::crc32_ieee(bytes) != encoded_crc32 {
        return Err(super::super::FormatError::DataArtifactChecksumMismatch {
            scope: "statistics numeric sum",
        });
    }
    *cursor = end;
    let numeric_count = u64::from(group.row_count()) - null_count;
    match data_type {
        radixdb_core::DataType::Integer => Ok((
            Some(i128::from_le_bytes(
                bytes.try_into().expect("checked INTEGER sum width"),
            )),
            None,
            numeric_count,
        )),
        radixdb_core::DataType::Float => Ok((
            None,
            Some(u64::from_le_bytes(
                bytes.try_into().expect("checked FLOAT sum width"),
            )),
            numeric_count,
        )),
        _ => unreachable!("numeric sum type was checked"),
    }
}

fn decode_value_ref(
    value_bytes: &[u8],
    cursor: &mut usize,
    entry: &[u8],
    offset: usize,
    present: bool,
    column: DataColumn,
) -> FormatResult<Option<Value>> {
    let reference = read_value_ref(entry, offset);
    if !present {
        if reference.offset != 0 || reference.length != 0 || reference.crc32 != 0 {
            return Err(invalid("absent statistics value reference is non-zero"));
        }
        return Ok(None);
    }
    if reference.length as u64 > MAX_STATISTIC_VALUE_BYTES || reference.offset != *cursor as u64 {
        return Err(invalid("statistics value reference is not canonical"));
    }
    let end = cursor
        .checked_add(reference.length as usize)
        .ok_or_else(|| invalid("statistics value range overflows"))?;
    let bytes = value_bytes
        .get(*cursor..end)
        .ok_or_else(|| invalid("statistics value range is outside section"))?;
    if radixdb_core::crc32_ieee(bytes) != reference.crc32 {
        return Err(super::super::FormatError::DataArtifactChecksumMismatch {
            scope: "statistics value",
        });
    }
    *cursor = end;
    decode_non_null_value(column, bytes).map(Some)
}

fn validate_column_statistics_indexes(
    columns: &[DataColumn],
    statistics: &[DataStatistics],
) -> FormatResult<()> {
    let mut first_indexes = vec![u32::MAX; columns.len()];
    for (index, entry) in statistics.iter().enumerate() {
        let slot = first_indexes
            .get_mut(entry.column_ordinal() as usize)
            .ok_or_else(|| invalid("statistics column ordinal is out of range"))?;
        if *slot == u32::MAX {
            *slot = u32::try_from(index)
                .map_err(|_| invalid("statistics entry index does not fit u32"))?;
        }
    }
    for column in columns {
        if first_indexes[column.ordinal() as usize] != column.statistics_entry_index() {
            return Err(invalid("column statistics entry index is not canonical"));
        }
    }
    Ok(())
}

fn statistics_spec_key(spec: &DataStatisticsSpec) -> (u32, u32) {
    (spec.column_ordinal, spec.row_group_ordinal)
}

fn statistics_key(statistics: &DataStatistics) -> (u32, u32) {
    (statistics.column_ordinal, statistics.row_group_ordinal)
}

fn put_value_ref(bytes: &mut [u8], offset: usize, reference: ValueRef) {
    put_u64(bytes, offset, reference.offset);
    put_u32(bytes, offset + 8, reference.length);
    put_u32(bytes, offset + 12, reference.crc32);
}

fn read_value_ref(bytes: &[u8], offset: usize) -> ValueRef {
    ValueRef {
        offset: read_u64(bytes, offset),
        length: read_u32(bytes, offset + 8),
        crc32: read_u32(bytes, offset + 12),
    }
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> [u8; N] {
    bytes[offset..offset + N]
        .try_into()
        .expect("fixed statistics field was validated")
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(read_array(bytes, offset))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(read_array(bytes, offset))
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
