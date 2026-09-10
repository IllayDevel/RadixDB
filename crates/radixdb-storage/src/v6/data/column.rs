use std::collections::{BTreeMap, BTreeSet};

use ahash::AHashMap;
use radixdb_core::{CompactArc, DataType, SmartString, Value};
use smallvec::{Array, SmallVec};

use super::super::FormatResult;
use super::column_model::{DataColumn, DataColumnSpec};
use super::model::{
    invalid, limit, DataBlockRef, DataLayout, DataRowGroup, MAX_LOGICAL_BYTES_PER_BLOCK,
    MAX_ROWS_PER_GROUP,
};
use super::physical::decode_physical;

const PLAIN_HEADER_BYTES: usize = 32;
const DICTIONARY_HEADER_BYTES: usize = 40;
pub const MAX_BYTES_PER_VALUE: usize = 256 * 1024 * 1024;
pub const MAX_DICTIONARY_ITEMS_PER_BLOCK: usize = 16_777_216;

pub(crate) trait ValueByteBuffer {
    fn len(&self) -> usize;
    fn as_mut_slice(&mut self) -> &mut [u8];
    fn push(&mut self, value: u8);
    fn extend_from_slice(&mut self, bytes: &[u8]);
    fn truncate(&mut self, length: usize);
}

impl ValueByteBuffer for Vec<u8> {
    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        Vec::as_mut_slice(self)
    }

    fn push(&mut self, value: u8) {
        Vec::push(self, value);
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        Vec::extend_from_slice(self, bytes);
    }

    fn truncate(&mut self, length: usize) {
        Vec::truncate(self, length);
    }
}

impl<A> ValueByteBuffer for SmallVec<A>
where
    A: Array<Item = u8>,
{
    fn len(&self) -> usize {
        SmallVec::len(self)
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        SmallVec::as_mut_slice(self)
    }

    fn push(&mut self, value: u8) {
        SmallVec::push(self, value);
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        SmallVec::extend_from_slice(self, bytes);
    }

    fn truncate(&mut self, length: usize) {
        SmallVec::truncate(self, length);
    }
}

/// Fully validated typed payload for one bounded DATA row-group column.
///
/// The format layer owns decoding and validation, while the runtime adapter
/// can move these buffers into its columnar representation without first
/// allocating one [`Value`] per row.
#[derive(Debug)]
pub(crate) enum DecodedColumn {
    Int64 {
        values: Vec<i64>,
        nulls: Vec<bool>,
    },
    Float64 {
        values: Vec<f64>,
        nulls: Vec<bool>,
    },
    TimestampNanos {
        values: Vec<i64>,
        nulls: Vec<bool>,
    },
    Boolean {
        values: Vec<bool>,
        nulls: Vec<bool>,
    },
    Text {
        ids: Vec<u32>,
        dictionary: Vec<SmartString>,
        nulls: Vec<bool>,
    },
    Bytes {
        data: Vec<u8>,
        offsets: Vec<(u64, u64)>,
        data_type: DataType,
        nulls: Vec<bool>,
    },
    External {
        data: Vec<u8>,
        offsets: Vec<(u64, u64)>,
        type_ref: radixdb_core::ExternalTypeRef,
        nulls: Vec<bool>,
    },
}

impl DecodedColumn {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Int64 { values, .. } => values.len(),
            Self::Float64 { values, .. } => values.len(),
            Self::TimestampNanos { values, .. } => values.len(),
            Self::Boolean { values, .. } => values.len(),
            Self::Text { ids, .. } => ids.len(),
            Self::Bytes { offsets, .. } => offsets.len(),
            Self::External { offsets, .. } => offsets.len(),
        }
    }

    fn into_values(self) -> Vec<Value> {
        match self {
            Self::Int64 { values, nulls } => values
                .into_iter()
                .zip(nulls)
                .map(|(value, is_null)| {
                    if is_null {
                        Value::null(DataType::Integer)
                    } else {
                        Value::integer(value)
                    }
                })
                .collect(),
            Self::Float64 { values, nulls } => values
                .into_iter()
                .zip(nulls)
                .map(|(value, is_null)| {
                    if is_null {
                        Value::null(DataType::Float)
                    } else {
                        Value::float(value)
                    }
                })
                .collect(),
            Self::TimestampNanos { values, nulls } => values
                .into_iter()
                .zip(nulls)
                .map(|(value, is_null)| {
                    if is_null {
                        Value::null(DataType::Timestamp)
                    } else {
                        Value::timestamp(chrono::DateTime::from_timestamp_nanos(value))
                    }
                })
                .collect(),
            Self::Boolean { values, nulls } => values
                .into_iter()
                .zip(nulls)
                .map(|(value, is_null)| {
                    if is_null {
                        Value::null(DataType::Boolean)
                    } else {
                        Value::boolean(value)
                    }
                })
                .collect(),
            Self::Text {
                ids,
                dictionary,
                nulls,
            } => ids
                .into_iter()
                .zip(nulls)
                .map(|(id, is_null)| {
                    if is_null {
                        Value::null(DataType::Text)
                    } else {
                        Value::Text(dictionary[id as usize].clone())
                    }
                })
                .collect(),
            Self::Bytes {
                data,
                offsets,
                data_type,
                nulls,
            } => offsets
                .into_iter()
                .zip(nulls)
                .map(|((offset, length), is_null)| {
                    if is_null {
                        return Value::null(data_type);
                    }
                    let start = offset as usize;
                    let end = start + length as usize;
                    let mut tagged = Vec::with_capacity(length as usize + 1);
                    tagged.push(data_type as u8);
                    tagged.extend_from_slice(&data[start..end]);
                    Value::Extension(CompactArc::from(tagged))
                })
                .collect(),
            Self::External {
                data,
                offsets,
                type_ref,
                nulls,
            } => offsets
                .into_iter()
                .zip(nulls)
                .map(|((offset, length), is_null)| {
                    if is_null {
                        return Value::null_unknown();
                    }
                    let start = offset as usize;
                    let end = start + length as usize;
                    Value::try_external(type_ref, &data[start..end])
                        .expect("decoded external payload already passed shape validation")
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataValueEncoding {
    Plain,
    Dictionary,
    /// Choose the smaller physically encoded representation for one bounded
    /// variable-width block. This is a writer policy, not an on-disk tag.
    Adaptive,
}

pub(crate) fn encode_column_payload(
    column: DataColumnSpec,
    values: &[Value],
    encoding: DataValueEncoding,
) -> FormatResult<(DataLayout, Vec<u8>)> {
    validate_value_count(values.len())?;
    validate_column_values(column, values)?;
    match encoding {
        DataValueEncoding::Plain => {
            encode_plain(column, values).map(|bytes| (DataLayout::PlainValues, bytes))
        }
        DataValueEncoding::Dictionary => {
            encode_dictionary(column, values).map(|bytes| (DataLayout::DictionaryValues, bytes))
        }
        DataValueEncoding::Adaptive => Err(invalid(
            "adaptive encoding must be resolved by the physical block builder",
        )),
    }
}

pub(crate) fn decode_column_payload(
    stored: &[u8],
    block: &DataBlockRef,
    group: DataRowGroup,
    column: DataColumn,
) -> FormatResult<Vec<Value>> {
    decode_typed_column_payload(stored, block, group, column).map(DecodedColumn::into_values)
}

pub(crate) fn decode_typed_column_payload(
    stored: &[u8],
    block: &DataBlockRef,
    group: DataRowGroup,
    column: DataColumn,
) -> FormatResult<DecodedColumn> {
    if block.item_count() != u64::from(group.row_count()) {
        return Err(invalid("column block count differs from row group"));
    }
    let logical = decode_physical(stored, block)?;
    match block.layout() {
        DataLayout::PlainValues => decode_plain(&logical, group, column),
        DataLayout::DictionaryValues => decode_dictionary(&logical, group, column),
        _ => Err(invalid("column block uses a non-column logical layout")),
    }
}

fn validate_value_count(count: usize) -> FormatResult<()> {
    if count == 0 || count > MAX_ROWS_PER_GROUP as usize {
        return Err(limit(
            "column values per group",
            count as u64,
            u64::from(MAX_ROWS_PER_GROUP),
        ));
    }
    Ok(())
}

pub(crate) fn validate_column_values(column: DataColumnSpec, values: &[Value]) -> FormatResult<()> {
    for value in values {
        if value.is_null() {
            if !column.nullable() {
                return Err(invalid("non-nullable column contains NULL"));
            }
            continue;
        }
        if value.logical_type() != column.data_type().logical_type_ref() {
            return Err(invalid("column value type differs from catalog type"));
        }
        value
            .validate_shape()
            .map_err(|_| invalid("column value has an invalid physical shape"))?;
        validate_parameterized_value(column, value)?;
    }
    Ok(())
}

fn validate_parameterized_value(column: DataColumnSpec, value: &Value) -> FormatResult<()> {
    if column.data_type().is_external() {
        let external = value
            .as_external()
            .ok_or_else(|| invalid("external column contains a built-in value"))?;
        if Some(external.type_ref()) != column.data_type().external_type_ref() {
            return Err(invalid("external value identity differs from catalog type"));
        }
        return Ok(());
    }
    match column.data_type().logical_type() {
        DataType::Decimal => {
            validate_decimal_typemod(column, value)?;
        }
        DataType::Vector => {
            let vector = value
                .as_vector_f32()
                .ok_or_else(|| invalid("invalid VECTOR value"))?;
            if vector.len() as u32 != column.data_type().parameter_1()
                || vector.iter().any(|component| !component.is_finite())
            {
                return Err(invalid("VECTOR dimensions/components are invalid"));
            }
        }
        _ => {}
    }
    Ok(())
}

fn encode_plain(column: DataColumnSpec, values: &[Value]) -> FormatResult<Vec<u8>> {
    let validity = encode_validity(values);
    let fixed_width = (!column.data_type().is_external())
        .then(|| {
            fixed_width(
                column.data_type().logical_type(),
                column.data_type().parameter_1(),
            )
        })
        .flatten();
    let (offsets, value_bytes) =
        if let Some(width) = fixed_width {
            let length = values
                .len()
                .checked_mul(width)
                .ok_or_else(|| invalid("fixed value bytes overflow"))?;
            let mut bytes = Vec::with_capacity(length);
            for value in values {
                if value.is_null() {
                    bytes.resize(bytes.len() + width, 0);
                } else {
                    append_column_non_null_bytes(column, value, &mut bytes)?;
                }
            }
            (Vec::new(), bytes)
        } else {
            let mut offsets = Vec::with_capacity(values.len() + 1);
            let mut bytes = Vec::new();
            offsets.push(0_u32);
            for value in values {
                if !value.is_null() {
                    append_column_non_null_bytes(column, value, &mut bytes)?;
                }
                offsets.push(u32::try_from(bytes.len()).map_err(|_| {
                    limit("variable value bytes", bytes.len() as u64, u32::MAX as u64)
                })?);
            }
            (offsets, bytes)
        };

    let offsets_length = offsets
        .len()
        .checked_mul(4)
        .ok_or_else(|| invalid("offset byte count overflows"))?;
    let total = PLAIN_HEADER_BYTES
        .checked_add(validity.len())
        .and_then(|value| value.checked_add(offsets_length))
        .and_then(|value| value.checked_add(value_bytes.len()))
        .ok_or_else(|| invalid("plain column payload length overflows"))?;
    validate_logical_length(total)?;

    let mut output = vec![0_u8; PLAIN_HEADER_BYTES];
    output[..4].copy_from_slice(b"VAL1");
    put_u16(&mut output, 4, 1);
    put_u16(&mut output, 6, if fixed_width.is_some() { 1 } else { 2 });
    put_u32(&mut output, 8, values.len() as u32);
    put_u32(&mut output, 12, validity.len() as u32);
    put_u32(&mut output, 16, offsets_length as u32);
    put_u64(&mut output, 20, value_bytes.len() as u64);
    output.extend_from_slice(&validity);
    for offset in offsets {
        output.extend_from_slice(&offset.to_le_bytes());
    }
    output.extend_from_slice(&value_bytes);
    Ok(output)
}

fn decode_plain(
    bytes: &[u8],
    group: DataRowGroup,
    column: DataColumn,
) -> FormatResult<DecodedColumn> {
    if bytes.len() < PLAIN_HEADER_BYTES || bytes[..4] != *b"VAL1" {
        return Err(invalid("plain column header is missing"));
    }
    if read_u16(bytes, 4) != 1 || read_u32(bytes, 28) != 0 {
        return Err(invalid("plain column version/reserved field is invalid"));
    }
    let layout = read_u16(bytes, 6);
    let count = read_u32(bytes, 8);
    if count != group.row_count() {
        return Err(invalid("plain column item count mismatch"));
    }
    let validity_length = read_u32(bytes, 12) as usize;
    let expected_validity = validity_bytes(count as usize);
    if validity_length != expected_validity {
        return Err(invalid("plain column validity length mismatch"));
    }
    let offsets_length = read_u32(bytes, 16) as usize;
    let values_length = usize::try_from(read_u64(bytes, 20))
        .map_err(|_| invalid("plain value length does not fit this platform"))?;
    let validity_end = PLAIN_HEADER_BYTES
        .checked_add(validity_length)
        .ok_or_else(|| invalid("plain validity range overflows"))?;
    let offsets_end = validity_end
        .checked_add(offsets_length)
        .ok_or_else(|| invalid("plain offset range overflows"))?;
    let values_end = offsets_end
        .checked_add(values_length)
        .ok_or_else(|| invalid("plain value range overflows"))?;
    if values_end != bytes.len() {
        return Err(invalid("plain column ranges do not own exact payload"));
    }
    let validity = &bytes[PLAIN_HEADER_BYTES..validity_end];
    validate_validity(validity, count as usize, column.nullable())?;

    let fixed = (!column.data_type().is_external())
        .then(|| {
            fixed_width(
                column.data_type().logical_type(),
                column.data_type().parameter_1(),
            )
        })
        .flatten();
    match (layout, fixed) {
        (1, Some(width)) if offsets_length == 0 && values_length == count as usize * width => {
            decode_fixed_values(
                &bytes[offsets_end..],
                validity,
                count as usize,
                column,
                width,
            )
        }
        (2, None) if offsets_length == (count as usize + 1) * 4 => decode_variable_values(
            &bytes[validity_end..offsets_end],
            &bytes[offsets_end..],
            validity,
            count as usize,
            column,
        ),
        _ => Err(invalid(
            "plain column layout/type/length combination is invalid",
        )),
    }
}

fn encode_dictionary(column: DataColumnSpec, values: &[Value]) -> FormatResult<Vec<u8>> {
    if column.data_type().is_external() {
        return Err(invalid(
            "dictionary encoding is not admitted for external types",
        ));
    }
    if !matches!(
        column.data_type().logical_type(),
        DataType::Text | DataType::Json | DataType::Bytes
    ) {
        return Err(invalid("dictionary encoding is not allowed for this type"));
    }
    let validity = encode_validity(values);
    let mut unique = BTreeSet::<Vec<u8>>::new();
    for value in values.iter().filter(|value| !value.is_null()) {
        let bytes = non_null_bytes(column.data_type().logical_type(), value)?;
        unique.insert(bytes);
    }
    if unique.len() > MAX_DICTIONARY_ITEMS_PER_BLOCK {
        return Err(limit(
            "dictionary item count",
            unique.len() as u64,
            MAX_DICTIONARY_ITEMS_PER_BLOCK as u64,
        ));
    }
    let dictionary = unique
        .into_iter()
        .enumerate()
        .map(|(ordinal, bytes)| {
            u32::try_from(ordinal)
                .map(|ordinal| (bytes, ordinal))
                .map_err(|_| invalid("dictionary item count does not fit u32"))
        })
        .collect::<FormatResult<BTreeMap<_, _>>>()?;
    let mut dictionary_bytes = Vec::new();
    let mut offsets = Vec::with_capacity(dictionary.len() + 1);
    offsets.push(0_u32);
    for bytes in dictionary.keys() {
        dictionary_bytes.extend_from_slice(bytes);
        offsets.push(u32::try_from(dictionary_bytes.len()).map_err(|_| {
            limit(
                "dictionary value bytes",
                dictionary_bytes.len() as u64,
                u32::MAX as u64,
            )
        })?);
    }
    let mut codes = Vec::with_capacity(values.len());
    for value in values {
        let code = if value.is_null() {
            0
        } else {
            let bytes = non_null_bytes(column.data_type().logical_type(), value)?;
            dictionary
                .get(&bytes)
                .copied()
                .ok_or_else(|| invalid("dictionary lookup lost an encoded value"))?
                + 1
        };
        codes.push(code);
    }
    let offsets_length = offsets
        .len()
        .checked_mul(4)
        .ok_or_else(|| invalid("dictionary offset byte count overflows"))?;
    let codes_length = codes
        .len()
        .checked_mul(4)
        .ok_or_else(|| invalid("dictionary code byte count overflows"))?;
    let total = DICTIONARY_HEADER_BYTES
        .checked_add(validity.len())
        .and_then(|value| value.checked_add(offsets_length))
        .and_then(|value| value.checked_add(dictionary_bytes.len()))
        .and_then(|value| value.checked_add(codes_length))
        .ok_or_else(|| invalid("dictionary payload length overflows"))?;
    validate_logical_length(total)?;

    let mut output = vec![0_u8; DICTIONARY_HEADER_BYTES];
    output[..4].copy_from_slice(b"DIC1");
    put_u16(&mut output, 4, 1);
    put_u32(&mut output, 8, values.len() as u32);
    put_u32(&mut output, 12, dictionary.len() as u32);
    put_u32(&mut output, 16, validity.len() as u32);
    put_u32(&mut output, 20, offsets_length as u32);
    put_u64(&mut output, 24, dictionary_bytes.len() as u64);
    put_u32(&mut output, 32, codes_length as u32);
    output.extend_from_slice(&validity);
    for offset in offsets {
        output.extend_from_slice(&offset.to_le_bytes());
    }
    output.extend_from_slice(&dictionary_bytes);
    for code in codes {
        output.extend_from_slice(&code.to_le_bytes());
    }
    Ok(output)
}

fn decode_dictionary(
    bytes: &[u8],
    group: DataRowGroup,
    column: DataColumn,
) -> FormatResult<DecodedColumn> {
    if column.data_type().is_external() {
        return Err(invalid(
            "dictionary block is not admitted for external types",
        ));
    }
    let data_type = column.data_type().logical_type();
    if !matches!(data_type, DataType::Text | DataType::Json | DataType::Bytes) {
        return Err(invalid("dictionary block is not allowed for column type"));
    }
    if bytes.len() < DICTIONARY_HEADER_BYTES || bytes[..4] != *b"DIC1" {
        return Err(invalid("dictionary column header is missing"));
    }
    if read_u16(bytes, 4) != 1 || read_u16(bytes, 6) != 0 || read_u32(bytes, 36) != 0 {
        return Err(invalid(
            "dictionary version/flags/reserved field is invalid",
        ));
    }
    let count = read_u32(bytes, 8) as usize;
    let distinct = read_u32(bytes, 12) as usize;
    if count != group.row_count() as usize
        || distinct > count
        || distinct > MAX_DICTIONARY_ITEMS_PER_BLOCK
    {
        return Err(invalid("dictionary item counts are invalid"));
    }
    let validity_length = read_u32(bytes, 16) as usize;
    let offsets_length = read_u32(bytes, 20) as usize;
    let dictionary_length = usize::try_from(read_u64(bytes, 24))
        .map_err(|_| invalid("dictionary byte length does not fit this platform"))?;
    let codes_length = read_u32(bytes, 32) as usize;
    let expected_offsets_length = distinct
        .checked_add(1)
        .and_then(|value| value.checked_mul(4))
        .ok_or_else(|| invalid("dictionary offset byte count overflows"))?;
    let expected_codes_length = count
        .checked_mul(4)
        .ok_or_else(|| invalid("dictionary code byte count overflows"))?;
    if validity_length != validity_bytes(count)
        || offsets_length != expected_offsets_length
        || codes_length != expected_codes_length
    {
        return Err(invalid("dictionary component lengths are invalid"));
    }
    let validity_end = DICTIONARY_HEADER_BYTES
        .checked_add(validity_length)
        .ok_or_else(|| invalid("dictionary validity range overflows"))?;
    let offsets_end = validity_end
        .checked_add(offsets_length)
        .ok_or_else(|| invalid("dictionary offset range overflows"))?;
    let dictionary_end = offsets_end
        .checked_add(dictionary_length)
        .ok_or_else(|| invalid("dictionary value range overflows"))?;
    let codes_end = dictionary_end
        .checked_add(codes_length)
        .ok_or_else(|| invalid("dictionary code range overflows"))?;
    if codes_end != bytes.len() {
        return Err(invalid("dictionary ranges do not own exact payload"));
    }
    let validity = &bytes[DICTIONARY_HEADER_BYTES..validity_end];
    validate_validity(validity, count, column.nullable())?;
    let offsets = decode_offsets(
        &bytes[validity_end..offsets_end],
        distinct,
        dictionary_length,
    )?;
    let dictionary_bytes = &bytes[offsets_end..dictionary_end];
    for pair in offsets.windows(3) {
        let left = &dictionary_bytes[pair[0]..pair[1]];
        let right = &dictionary_bytes[pair[1]..pair[2]];
        if left >= right {
            return Err(invalid("dictionary values are not sorted unique"));
        }
    }
    let non_null_count = (0..count)
        .filter(|index| is_valid(validity, *index))
        .count();
    if distinct > non_null_count {
        return Err(invalid("dictionary has more values than non-NULL rows"));
    }
    let mut codes = Vec::with_capacity(count);
    let mut nulls = Vec::with_capacity(count);
    for index in 0..count {
        let code_offset = dictionary_end + index * 4;
        let code = read_u32(bytes, code_offset) as usize;
        if is_valid(validity, index) {
            if code == 0 || code > distinct {
                return Err(invalid("non-NULL dictionary code is out of range"));
            }
            codes.push(code - 1);
            nulls.push(false);
        } else {
            if code != 0 {
                return Err(invalid("NULL dictionary row has a non-zero code"));
            }
            codes.push(0);
            nulls.push(true);
        }
    }
    match data_type {
        DataType::Text => {
            let mut dictionary = Vec::with_capacity(distinct);
            for pair in offsets.windows(2) {
                let text = std::str::from_utf8(&dictionary_bytes[pair[0]..pair[1]])
                    .map_err(|_| invalid("TEXT bytes are not UTF-8"))?;
                dictionary.push(SmartString::from(text));
            }
            Ok(DecodedColumn::Text {
                ids: codes
                    .into_iter()
                    .map(|code| code as u32)
                    .collect::<Vec<_>>(),
                dictionary,
                nulls,
            })
        }
        DataType::Json | DataType::Bytes => {
            for pair in offsets.windows(2) {
                validate_extension_bytes(data_type, &dictionary_bytes[pair[0]..pair[1]], column)?;
            }
            expand_dictionary_bytes(dictionary_bytes, &offsets, &codes, nulls, data_type)
        }
        _ => unreachable!("dictionary type was checked"),
    }
}

fn decode_fixed_values(
    bytes: &[u8],
    validity: &[u8],
    count: usize,
    column: DataColumn,
    width: usize,
) -> FormatResult<DecodedColumn> {
    let data_type = column.data_type().logical_type();
    let nulls = decode_fixed_nulls(bytes, validity, count, width)?;
    match data_type {
        DataType::Integer => Ok(DecodedColumn::Int64 {
            values: bytes
                .chunks_exact(8)
                .map(|chunk| i64::from_le_bytes(chunk.try_into().expect("checked INTEGER width")))
                .collect(),
            nulls,
        }),
        DataType::Float => Ok(DecodedColumn::Float64 {
            values: bytes
                .chunks_exact(8)
                .map(|chunk| {
                    f64::from_bits(u64::from_le_bytes(
                        chunk.try_into().expect("checked FLOAT width"),
                    ))
                })
                .collect(),
            nulls,
        }),
        DataType::Timestamp => Ok(DecodedColumn::TimestampNanos {
            values: bytes
                .chunks_exact(8)
                .map(|chunk| i64::from_le_bytes(chunk.try_into().expect("checked TIMESTAMP width")))
                .collect(),
            nulls,
        }),
        DataType::Boolean => {
            if bytes
                .iter()
                .zip(&nulls)
                .any(|(byte, is_null)| !*is_null && *byte > 1)
            {
                return Err(invalid("BOOLEAN value bytes are invalid"));
            }
            Ok(DecodedColumn::Boolean {
                values: bytes.iter().map(|byte| *byte == 1).collect(),
                nulls,
            })
        }
        DataType::Vector | DataType::Uuid | DataType::Decimal | DataType::Date => {
            decode_fixed_extension(bytes, count, width, column, nulls)
        }
        DataType::Text | DataType::Json | DataType::Bytes | DataType::Null => Err(invalid(
            "fixed column payload uses a variable-width catalog type",
        )),
    }
}

fn decode_variable_values(
    offset_bytes: &[u8],
    value_bytes: &[u8],
    validity: &[u8],
    count: usize,
    column: DataColumn,
) -> FormatResult<DecodedColumn> {
    let offsets = decode_offsets(offset_bytes, count, value_bytes.len())?;
    let data_type = column.data_type().logical_type();
    let mut nulls = Vec::with_capacity(count);
    for index in 0..count {
        if is_valid(validity, index) {
            let bytes = &value_bytes[offsets[index]..offsets[index + 1]];
            if column.data_type().is_external() {
                validate_external_bytes(column, bytes)?;
            } else {
                validate_variable_bytes(data_type, bytes, column)?;
            }
            nulls.push(false);
        } else {
            if offsets[index] != offsets[index + 1] {
                return Err(invalid("NULL variable value owns bytes"));
            }
            nulls.push(true);
        }
    }
    if column.data_type().is_external() {
        return copy_external_bytes(value_bytes, &offsets, nulls, column);
    }
    match data_type {
        DataType::Text => decode_plain_text(value_bytes, &offsets, nulls),
        DataType::Json | DataType::Bytes => {
            copy_variable_bytes(value_bytes, &offsets, nulls, data_type)
        }
        _ => Err(invalid(
            "variable column payload uses a fixed-width catalog type",
        )),
    }
}

fn validate_external_bytes(column: DataColumn, bytes: &[u8]) -> FormatResult<()> {
    let type_ref = column
        .data_type()
        .external_type_ref()
        .ok_or_else(|| invalid("external column descriptor has no type identity"))?;
    Value::try_external(type_ref, bytes)
        .map(|_| ())
        .map_err(|_| invalid("external bytes have an invalid physical shape"))
}

fn copy_external_bytes(
    value_bytes: &[u8],
    source_offsets: &[usize],
    nulls: Vec<bool>,
    column: DataColumn,
) -> FormatResult<DecodedColumn> {
    let type_ref = column
        .data_type()
        .external_type_ref()
        .ok_or_else(|| invalid("external column descriptor has no type identity"))?;
    let mut data = Vec::with_capacity(value_bytes.len());
    let mut offsets = Vec::with_capacity(nulls.len());
    for (index, is_null) in nulls.iter().copied().enumerate() {
        if is_null {
            offsets.push((0, 0));
            continue;
        }
        let source = &value_bytes[source_offsets[index]..source_offsets[index + 1]];
        let offset = data.len() as u64;
        data.extend_from_slice(source);
        offsets.push((offset, source.len() as u64));
    }
    Ok(DecodedColumn::External {
        data,
        offsets,
        type_ref,
        nulls,
    })
}

fn decode_fixed_nulls(
    bytes: &[u8],
    validity: &[u8],
    count: usize,
    width: usize,
) -> FormatResult<Vec<bool>> {
    let mut nulls = Vec::with_capacity(count);
    for index in 0..count {
        let is_null = !is_valid(validity, index);
        if is_null
            && bytes[index * width..(index + 1) * width]
                .iter()
                .any(|byte| *byte != 0)
        {
            return Err(invalid("NULL fixed-width slot is non-zero"));
        }
        nulls.push(is_null);
    }
    Ok(nulls)
}

fn decode_fixed_extension(
    bytes: &[u8],
    count: usize,
    width: usize,
    column: DataColumn,
    nulls: Vec<bool>,
) -> FormatResult<DecodedColumn> {
    let data_type = column.data_type().logical_type();
    let mut data = Vec::with_capacity(bytes.len());
    let mut offsets = Vec::with_capacity(count);
    for (index, is_null) in nulls.iter().copied().enumerate() {
        if is_null {
            offsets.push((0, 0));
            continue;
        }
        let value = &bytes[index * width..(index + 1) * width];
        validate_extension_bytes(data_type, value, column)?;
        let offset = data.len() as u64;
        data.extend_from_slice(value);
        offsets.push((offset, width as u64));
    }
    Ok(DecodedColumn::Bytes {
        data,
        offsets,
        data_type,
        nulls,
    })
}

fn validate_variable_bytes(
    data_type: DataType,
    bytes: &[u8],
    column: DataColumn,
) -> FormatResult<()> {
    match data_type {
        DataType::Text => std::str::from_utf8(bytes)
            .map(|_| ())
            .map_err(|_| invalid("TEXT bytes are not UTF-8")),
        DataType::Json | DataType::Bytes => validate_extension_bytes(data_type, bytes, column),
        _ => Err(invalid("variable value has a fixed-width catalog type")),
    }
}

fn validate_extension_bytes(
    data_type: DataType,
    bytes: &[u8],
    column: DataColumn,
) -> FormatResult<()> {
    Value::validate_extension_payload(data_type, bytes)
        .map_err(|_| invalid("extension bytes have an invalid physical shape"))?;
    match data_type {
        DataType::Vector => {
            let expected = column.data_type().parameter_1() as usize * 4;
            if bytes.len() != expected
                || bytes.chunks_exact(4).any(|chunk| {
                    !f32::from_le_bytes(chunk.try_into().expect("checked VECTOR width")).is_finite()
                })
            {
                return Err(invalid("VECTOR dimensions/components are invalid"));
            }
        }
        DataType::Decimal => {
            let unscaled = i128::from_le_bytes(
                bytes[..16]
                    .try_into()
                    .expect("validated DECIMAL coefficient width"),
            );
            validate_decimal_parts_typemod(column.data_type(), unscaled, bytes[17])?;
        }
        DataType::Json | DataType::Uuid | DataType::Date | DataType::Bytes => {}
        _ => return Err(invalid("catalog type is not an extension payload")),
    }
    Ok(())
}

fn decode_plain_text(
    value_bytes: &[u8],
    offsets: &[usize],
    nulls: Vec<bool>,
) -> FormatResult<DecodedColumn> {
    let mut ids = Vec::with_capacity(nulls.len());
    let mut dictionary = Vec::new();
    let mut dictionary_ids = AHashMap::<SmartString, u32>::new();
    for (index, is_null) in nulls.iter().copied().enumerate() {
        if is_null {
            ids.push(0);
            continue;
        }
        let text = std::str::from_utf8(&value_bytes[offsets[index]..offsets[index + 1]])
            .map_err(|_| invalid("TEXT bytes are not UTF-8"))?;
        let value = SmartString::from(text);
        let id = if let Some(id) = dictionary_ids.get(&value).copied() {
            id
        } else {
            let id = u32::try_from(dictionary.len())
                .map_err(|_| invalid("row-group text dictionary exceeds u32 identifiers"))?;
            dictionary.push(value.clone());
            dictionary_ids.insert(value, id);
            id
        };
        ids.push(id);
    }
    Ok(DecodedColumn::Text {
        ids,
        dictionary,
        nulls,
    })
}

fn copy_variable_bytes(
    value_bytes: &[u8],
    source_offsets: &[usize],
    nulls: Vec<bool>,
    data_type: DataType,
) -> FormatResult<DecodedColumn> {
    let mut data = Vec::with_capacity(value_bytes.len());
    let mut offsets = Vec::with_capacity(nulls.len());
    for (index, is_null) in nulls.iter().copied().enumerate() {
        if is_null {
            offsets.push((0, 0));
            continue;
        }
        let source = &value_bytes[source_offsets[index]..source_offsets[index + 1]];
        let offset = data.len() as u64;
        data.extend_from_slice(source);
        offsets.push((offset, source.len() as u64));
    }
    Ok(DecodedColumn::Bytes {
        data,
        offsets,
        data_type,
        nulls,
    })
}

fn expand_dictionary_bytes(
    dictionary_bytes: &[u8],
    dictionary_offsets: &[usize],
    codes: &[usize],
    nulls: Vec<bool>,
    data_type: DataType,
) -> FormatResult<DecodedColumn> {
    let required = codes
        .iter()
        .zip(&nulls)
        .filter(|(_, is_null)| !**is_null)
        .try_fold(0_usize, |total, (code, _)| {
            total
                .checked_add(dictionary_offsets[*code + 1] - dictionary_offsets[*code])
                .ok_or_else(|| invalid("expanded dictionary bytes overflow"))
        })?;
    if required as u64 > MAX_LOGICAL_BYTES_PER_BLOCK {
        return Err(limit(
            "expanded dictionary bytes",
            required as u64,
            MAX_LOGICAL_BYTES_PER_BLOCK,
        ));
    }
    let mut data = Vec::with_capacity(required);
    let mut offsets = Vec::with_capacity(codes.len());
    for (code, is_null) in codes.iter().copied().zip(&nulls) {
        if *is_null {
            offsets.push((0, 0));
            continue;
        }
        let value = &dictionary_bytes[dictionary_offsets[code]..dictionary_offsets[code + 1]];
        let offset = data.len() as u64;
        data.extend_from_slice(value);
        offsets.push((offset, value.len() as u64));
    }
    Ok(DecodedColumn::Bytes {
        data,
        offsets,
        data_type,
        nulls,
    })
}

fn decode_offsets(
    bytes: &[u8],
    item_count: usize,
    value_length: usize,
) -> FormatResult<Vec<usize>> {
    let expected_length = item_count
        .checked_add(1)
        .and_then(|value| value.checked_mul(4))
        .ok_or_else(|| invalid("offset array width overflows"))?;
    if bytes.len() != expected_length {
        return Err(invalid("offset array width mismatch"));
    }
    let mut offsets = Vec::with_capacity(item_count + 1);
    for index in 0..=item_count {
        offsets.push(read_u32(bytes, index * 4) as usize);
    }
    if offsets.first() != Some(&0)
        || offsets.last() != Some(&value_length)
        || offsets.windows(2).any(|pair| pair[0] > pair[1])
    {
        return Err(invalid("offset array is not canonical"));
    }
    Ok(offsets)
}

fn encode_validity(values: &[Value]) -> Vec<u8> {
    let mut validity = vec![0_u8; validity_bytes(values.len())];
    for (index, value) in values.iter().enumerate() {
        if !value.is_null() {
            validity[index / 8] |= 1 << (index % 8);
        }
    }
    validity
}

fn validate_validity(bytes: &[u8], count: usize, nullable: bool) -> FormatResult<()> {
    if !count.is_multiple_of(8) {
        let used = count % 8;
        let unused_mask = !((1_u8 << used) - 1);
        if bytes.last().is_some_and(|byte| byte & unused_mask != 0) {
            return Err(invalid("unused validity bits are non-zero"));
        }
    }
    if !nullable && (0..count).any(|index| !is_valid(bytes, index)) {
        return Err(invalid("non-nullable column contains NULL validity bit"));
    }
    Ok(())
}

fn is_valid(validity: &[u8], index: usize) -> bool {
    validity[index / 8] & (1 << (index % 8)) != 0
}

fn validity_bytes(count: usize) -> usize {
    count.div_ceil(8)
}

fn fixed_width(data_type: DataType, parameter_1: u32) -> Option<usize> {
    match data_type {
        DataType::Integer | DataType::Float | DataType::Timestamp => Some(8),
        DataType::Boolean => Some(1),
        DataType::Date => Some(4),
        DataType::Uuid => Some(16),
        DataType::Decimal => Some(18),
        DataType::Vector => usize::try_from(parameter_1).ok()?.checked_mul(4),
        DataType::Text | DataType::Json | DataType::Bytes => None,
        DataType::Null => None,
    }
}

fn append_non_null_bytes(
    data_type: DataType,
    value: &Value,
    output: &mut Vec<u8>,
) -> FormatResult<()> {
    append_non_null_representation(data_type, value, output).map(|_| ())
}

fn append_column_non_null_bytes(
    column: DataColumnSpec,
    value: &Value,
    output: &mut Vec<u8>,
) -> FormatResult<()> {
    if column.data_type().is_external() {
        let external = value
            .as_external()
            .ok_or_else(|| invalid("external value has wrong representation"))?;
        if Some(external.type_ref()) != column.data_type().external_type_ref() {
            return Err(invalid("external value identity differs from catalog type"));
        }
        if external.payload().len() > radixdb_core::value::MAX_EXTERNAL_VALUE_BYTES {
            return Err(limit(
                "external value bytes",
                external.payload().len() as u64,
                radixdb_core::value::MAX_EXTERNAL_VALUE_BYTES as u64,
            ));
        }
        output.extend_from_slice(external.payload());
        return Ok(());
    }
    append_non_null_bytes(column.data_type().logical_type(), value, output)
}

pub(crate) fn encode_non_null_value(
    column: DataColumnSpec,
    value: &Value,
) -> FormatResult<Vec<u8>> {
    validate_column_values(column, std::slice::from_ref(value))?;
    if value.is_null() {
        return Err(invalid("statistics value cannot be NULL"));
    }
    if column.data_type().is_external() {
        let mut bytes = Vec::new();
        append_column_non_null_bytes(column, value, &mut bytes)?;
        Ok(bytes)
    } else {
        non_null_bytes(column.data_type().logical_type(), value)
    }
}

pub(crate) fn append_non_null_value(
    column: DataColumnSpec,
    value: &Value,
    output: &mut impl ValueByteBuffer,
) -> FormatResult<usize> {
    validate_column_values(column, std::slice::from_ref(value))?;
    if value.is_null() {
        return Err(invalid("statistics value cannot be NULL"));
    }
    if column.data_type().is_external() {
        let external = value
            .as_external()
            .ok_or_else(|| invalid("external value has wrong representation"))?;
        if Some(external.type_ref()) != column.data_type().external_type_ref() {
            return Err(invalid("external value identity differs from catalog type"));
        }
        let start = output.len();
        output.extend_from_slice(external.payload());
        Ok(output.len() - start)
    } else {
        append_non_null_representation(column.data_type().logical_type(), value, output)
    }
}

fn non_null_bytes(data_type: DataType, value: &Value) -> FormatResult<Vec<u8>> {
    let mut bytes = Vec::new();
    append_non_null_representation(data_type, value, &mut bytes)?;
    Ok(bytes)
}

fn append_non_null_representation(
    data_type: DataType,
    value: &Value,
    output: &mut impl ValueByteBuffer,
) -> FormatResult<usize> {
    let start = output.len();
    match data_type {
        DataType::Integer => match value {
            Value::Integer(value) => output.extend_from_slice(&value.to_le_bytes()),
            _ => return Err(invalid("INTEGER value has wrong representation")),
        },
        DataType::Float => match value {
            Value::Float(value) => output.extend_from_slice(&value.to_bits().to_le_bytes()),
            _ => return Err(invalid("FLOAT value has wrong representation")),
        },
        DataType::Text => output.extend_from_slice(
            value
                .as_str()
                .ok_or_else(|| invalid("TEXT value has wrong representation"))?
                .as_bytes(),
        ),
        DataType::Boolean => match value {
            Value::Boolean(value) => output.push(u8::from(*value)),
            _ => return Err(invalid("BOOLEAN value has wrong representation")),
        },
        DataType::Timestamp => match value {
            Value::Timestamp(value) => output.extend_from_slice(
                &value
                    .timestamp_nanos_opt()
                    .ok_or_else(|| invalid("TIMESTAMP is outside i64 nanoseconds"))?
                    .to_le_bytes(),
            ),
            _ => return Err(invalid("TIMESTAMP value has wrong representation")),
        },
        DataType::Json => output.extend_from_slice(
            value
                .as_json()
                .ok_or_else(|| invalid("JSON value has wrong representation"))?
                .as_bytes(),
        ),
        DataType::Vector => {
            for component in value
                .as_vector_f32()
                .ok_or_else(|| invalid("VECTOR value has wrong representation"))?
            {
                output.extend_from_slice(&component.to_le_bytes());
            }
        }
        DataType::Uuid => {
            let bytes = value
                .as_uuid_bytes()
                .ok_or_else(|| invalid("UUID value has wrong representation"))?;
            output.extend_from_slice(&bytes);
        }
        DataType::Decimal => {
            let (unscaled, precision, scale) = value
                .as_decimal_parts()
                .ok_or_else(|| invalid("DECIMAL value has wrong representation"))?;
            output.extend_from_slice(&unscaled.to_le_bytes());
            output.extend_from_slice(&[precision, scale]);
        }
        DataType::Date => output.extend_from_slice(
            &value
                .as_date_days()
                .ok_or_else(|| invalid("DATE value has wrong representation"))?
                .to_le_bytes(),
        ),
        DataType::Bytes => output.extend_from_slice(
            value
                .as_bytes_value()
                .ok_or_else(|| invalid("BYTES value has wrong representation"))?,
        ),
        DataType::Null => return Err(invalid("NULL is not a stored column type")),
    }
    let length = output.len() - start;
    if length > MAX_BYTES_PER_VALUE {
        output.truncate(start);
        return Err(limit(
            "value bytes",
            length as u64,
            MAX_BYTES_PER_VALUE as u64,
        ));
    }
    Ok(length)
}

pub(crate) fn decode_non_null_value(column: DataColumn, bytes: &[u8]) -> FormatResult<Value> {
    if let Some(type_ref) = column.data_type().external_type_ref() {
        return Value::try_external(type_ref, bytes)
            .map_err(|_| invalid("external value bytes have an invalid shape"));
    }
    decode_non_null(column.data_type().logical_type(), bytes, column)
}

fn decode_non_null(data_type: DataType, bytes: &[u8], column: DataColumn) -> FormatResult<Value> {
    let value = match data_type {
        DataType::Integer if bytes.len() == 8 => Value::integer(i64::from_le_bytes(
            bytes.try_into().expect("checked INTEGER width"),
        )),
        DataType::Float if bytes.len() == 8 => Value::float(f64::from_bits(u64::from_le_bytes(
            bytes.try_into().expect("checked FLOAT width"),
        ))),
        DataType::Text => Value::text(
            std::str::from_utf8(bytes)
                .map_err(|_| invalid("TEXT bytes are not UTF-8"))?
                .to_owned(),
        ),
        DataType::Boolean if bytes.len() == 1 && bytes[0] <= 1 => Value::boolean(bytes[0] == 1),
        DataType::Timestamp if bytes.len() == 8 => {
            Value::timestamp(chrono::DateTime::from_timestamp_nanos(i64::from_le_bytes(
                bytes.try_into().expect("checked TIMESTAMP width"),
            )))
        }
        DataType::Json => Value::try_json(
            std::str::from_utf8(bytes)
                .map_err(|_| invalid("JSON bytes are not UTF-8"))?
                .to_owned(),
        )
        .map_err(|_| invalid("JSON bytes do not contain one valid value"))?,
        DataType::Vector if bytes.len() == column.data_type().parameter_1() as usize * 4 => {
            let mut vector = Vec::with_capacity(bytes.len() / 4);
            for chunk in bytes.chunks_exact(4) {
                let value = f32::from_le_bytes(chunk.try_into().expect("checked VECTOR width"));
                if !value.is_finite() {
                    return Err(invalid("VECTOR contains a non-finite component"));
                }
                vector.push(value);
            }
            Value::vector(vector)
        }
        DataType::Uuid if bytes.len() == 16 => {
            Value::uuid(bytes.try_into().expect("checked UUID width"))
        }
        DataType::Decimal if bytes.len() == 18 => {
            let unscaled =
                i128::from_le_bytes(bytes[..16].try_into().expect("checked DECIMAL width"));
            let precision = bytes[16];
            let scale = bytes[17];
            let value = Value::try_decimal(unscaled, precision, scale)
                .map_err(|_| invalid("DECIMAL bytes have an invalid shape"))?;
            validate_decimal_typemod(
                DataColumnSpec::new(column.column_id(), column.data_type(), column.nullable()),
                &value,
            )?;
            value
        }
        DataType::Date if bytes.len() == 4 => Value::date(i32::from_le_bytes(
            bytes.try_into().expect("checked DATE width"),
        )),
        DataType::Bytes => Value::bytes(bytes.to_vec()),
        _ => return Err(invalid("column value bytes do not match catalog type")),
    };
    Ok(value)
}

/// Validate a value against a declared `DECIMAL(p,s)` capacity without
/// rewriting its exact payload. Value precision describes the value itself;
/// it is not required to equal the column capacity. An excess scale is
/// admissible only when removing trailing zeroes brings it within `s`.
fn validate_decimal_typemod(column: DataColumnSpec, value: &Value) -> FormatResult<()> {
    let (unscaled, _, scale) = value
        .as_decimal_parts()
        .ok_or_else(|| invalid("invalid DECIMAL value"))?;
    validate_decimal_parts_typemod(column.data_type(), unscaled, scale)
}

fn validate_decimal_parts_typemod(
    data_type: radixdb_catalog::CatalogDataType,
    unscaled: i128,
    mut scale: u8,
) -> FormatResult<()> {
    let declared_precision = data_type.parameter_1();
    if declared_precision == 0 {
        return Ok(());
    }
    let declared_scale = data_type.parameter_2();
    let mut magnitude = unscaled.unsigned_abs();
    while u32::from(scale) > declared_scale && magnitude.is_multiple_of(10) {
        magnitude /= 10;
        scale -= 1;
    }
    if u32::from(scale) > declared_scale {
        return Err(invalid("DECIMAL value exceeds catalog scale"));
    }
    let coefficient_digits = if magnitude == 0 {
        1
    } else {
        magnitude.ilog10() + 1
    };
    let integer_digits = coefficient_digits.saturating_sub(u32::from(scale));
    let allowed_integer_digits = declared_precision - declared_scale;
    if integer_digits > allowed_integer_digits {
        return Err(invalid("DECIMAL value exceeds catalog precision"));
    }
    Ok(())
}

fn validate_logical_length(length: usize) -> FormatResult<()> {
    if length as u64 > MAX_LOGICAL_BYTES_PER_BLOCK {
        return Err(limit(
            "logical block bytes",
            length as u64,
            MAX_LOGICAL_BYTES_PER_BLOCK,
        ));
    }
    Ok(())
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("checked field"))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("checked field"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("checked field"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use radixdb_catalog::{CatalogDataType, ObjectId};
    use radixdb_core::ExternalTypeRef;

    fn external_fixture(codec_version: u32) -> (DataColumnSpec, DataColumn, ExternalTypeRef) {
        let column_id = ObjectId::from_user_bytes([0x31; 16]).unwrap();
        let type_id = ObjectId::from_user_bytes([0x32; 16]).unwrap();
        let data_type = CatalogDataType::external(type_id, codec_version).unwrap();
        let type_ref = data_type.external_type_ref().unwrap();
        (
            DataColumnSpec::new(column_id, data_type, true),
            DataColumn::new(column_id, 0, data_type, true, 0, 1, 0),
            type_ref,
        )
    }

    #[test]
    fn external_plain_column_round_trips_exact_identity_and_payload() {
        let (spec, column, type_ref) = external_fixture(7);
        let values = vec![
            Value::try_external(type_ref, [1, 2, 3]).unwrap(),
            Value::null_unknown(),
            Value::try_external(type_ref, [4, 5]).unwrap(),
        ];
        let (_, bytes) = encode_column_payload(spec, &values, DataValueEncoding::Plain).unwrap();
        let group = DataRowGroup::new(0, 3, 0, 1, 3, 0, 1).unwrap();

        let decoded = decode_plain(&bytes, group, column).unwrap().into_values();
        assert_eq!(decoded, values);
    }

    #[test]
    fn external_plain_column_rejects_wrong_identity_and_dictionary_encoding() {
        let (spec, _, _) = external_fixture(7);
        let wrong_type = ExternalTypeRef::new([0x33; 16], 7).unwrap();
        let wrong_codec = ExternalTypeRef::new([0x32; 16], 8).unwrap();

        for value in [
            Value::try_external(wrong_type, [1]).unwrap(),
            Value::try_external(wrong_codec, [1]).unwrap(),
        ] {
            assert!(encode_column_payload(spec, &[value], DataValueEncoding::Plain).is_err());
        }
        let valid =
            Value::try_external(spec.data_type().external_type_ref().unwrap(), [1]).unwrap();
        assert!(encode_column_payload(spec, &[valid], DataValueEncoding::Dictionary).is_err());
    }
}
