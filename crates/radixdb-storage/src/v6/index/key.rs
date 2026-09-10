use radixdb_core::{DataType, Value};
use smallvec::SmallVec;

use super::super::data::{append_non_null_value, decode_non_null_value, ValueByteBuffer};
use super::super::{DataArtifactLayout, DataColumn, DataColumnSpec, FormatResult};
use super::model::{invalid, limit, IndexKeyColumn};

const COMPONENT_HEADER_BYTES: usize = 8;

/// Canonical scalar INTEGER/FLOAT/DATE/BOOLEAN keys fit inline. Wider and
/// composite keys retain the same growable representation without imposing a
/// heap allocation on every ordinary index-build row.
pub(crate) type CanonicalKeyBytes = SmallVec<[u8; 16]>;

pub const MAX_INDEX_KEY_BYTES: u64 = 1024 * 1024;

pub(super) fn resolve_source_columns(
    data: &DataArtifactLayout,
    key_columns: &[IndexKeyColumn],
) -> FormatResult<Vec<DataColumn>> {
    if key_columns.is_empty() {
        return Err(invalid("index key descriptor is empty"));
    }
    key_columns
        .iter()
        .map(|key| {
            let source = data
                .columns()
                .iter()
                .find(|column| column.column_id() == key.column_id())
                .copied()
                .ok_or_else(|| invalid("index key column is absent from source data"))?;
            if source.data_type().logical_type() != key.logical_type() {
                return Err(invalid("index key type differs from source data"));
            }
            Ok(source)
        })
        .collect()
}

pub(super) fn encode_canonical_key(
    columns: &[DataColumn],
    values: &[Value],
) -> FormatResult<(CanonicalKeyBytes, bool)> {
    let specs = columns
        .iter()
        .map(|column| {
            DataColumnSpec::new(column.column_id(), column.data_type(), column.nullable())
        })
        .collect::<Vec<_>>();
    encode_canonical_key_specs(&specs, values)
}

pub(crate) fn encode_canonical_key_specs(
    columns: &[DataColumnSpec],
    values: &[Value],
) -> FormatResult<(CanonicalKeyBytes, bool)> {
    if columns.len() != values.len() {
        return Err(invalid("index key component count mismatch"));
    }
    let mut state = CanonicalKeyState::new(columns.len())?;
    for (column, value) in columns.iter().copied().zip(values) {
        state.append(column, value)?;
    }
    state.finish(columns.len())
}

pub(crate) fn encode_canonical_projected_key_specs(
    columns: &[DataColumnSpec],
    source_values: &[Value],
    source_ordinals: &[usize],
) -> FormatResult<(CanonicalKeyBytes, bool)> {
    if columns.len() != source_ordinals.len() {
        return Err(invalid("index key projection count mismatch"));
    }
    let mut state = CanonicalKeyState::new(columns.len())?;
    for (column, source_ordinal) in columns.iter().copied().zip(source_ordinals) {
        let value = source_values
            .get(*source_ordinal)
            .ok_or_else(|| invalid("source row is narrower than index key"))?;
        state.append(column, value)?;
    }
    state.finish(columns.len())
}

struct CanonicalKeyState {
    bytes: CanonicalKeyBytes,
    has_null_component: bool,
    encoded_components: usize,
}

impl CanonicalKeyState {
    fn new(component_count: usize) -> FormatResult<Self> {
        let minimum_bytes = component_count
            .checked_mul(COMPONENT_HEADER_BYTES)
            .ok_or_else(|| invalid("index key component header bytes overflow"))?;
        Ok(Self {
            bytes: CanonicalKeyBytes::with_capacity(minimum_bytes),
            has_null_component: false,
            encoded_components: 0,
        })
    }

    fn append(&mut self, column: DataColumnSpec, value: &Value) -> FormatResult<()> {
        self.encoded_components = self
            .encoded_components
            .checked_add(1)
            .ok_or_else(|| invalid("index key component count overflows"))?;
        let start = self.bytes.len();
        self.bytes.resize(start + COMPONENT_HEADER_BYTES, 0);
        self.bytes[start + 1] = column.data_type().logical_type().as_u8();
        if value.is_null() {
            if !column.nullable() {
                return Err(invalid("non-nullable index key contains NULL"));
            }
            self.has_null_component = true;
        } else {
            let value_length = append_canonical_non_null_bytes(column, value, &mut self.bytes)?;
            self.bytes[start] = 1;
            put_u32(
                &mut self.bytes[start..start + COMPONENT_HEADER_BYTES],
                4,
                value_length as u32,
            );
            let aligned = self
                .bytes
                .len()
                .checked_add(3)
                .map(|length| length & !3)
                .ok_or_else(|| invalid("index key alignment overflows"))?;
            self.bytes.resize(aligned, 0);
        }
        if self.bytes.len() as u64 > MAX_INDEX_KEY_BYTES {
            return Err(limit(
                "index key bytes",
                self.bytes.len() as u64,
                MAX_INDEX_KEY_BYTES,
            ));
        }
        Ok(())
    }

    fn finish(self, component_count: usize) -> FormatResult<(CanonicalKeyBytes, bool)> {
        if self.encoded_components != component_count {
            return Err(invalid("index key component count mismatch"));
        }
        Ok((self.bytes, self.has_null_component))
    }
}

pub(super) fn decode_canonical_key(
    bytes: &[u8],
    columns: &[DataColumn],
) -> FormatResult<(Vec<Value>, bool)> {
    let specs = columns
        .iter()
        .map(|column| {
            DataColumnSpec::new(column.column_id(), column.data_type(), column.nullable())
        })
        .collect::<Vec<_>>();
    decode_canonical_key_specs(bytes, &specs)
}

pub(crate) fn decode_canonical_key_specs(
    bytes: &[u8],
    columns: &[DataColumnSpec],
) -> FormatResult<(Vec<Value>, bool)> {
    let mut values = Vec::with_capacity(columns.len());
    let has_null_component =
        visit_canonical_key_specs(bytes, columns, &mut Vec::new(), |value| values.push(value))?;
    Ok((values, has_null_component))
}

pub(super) fn validate_canonical_key_specs(
    bytes: &[u8],
    columns: &[DataColumnSpec],
    scratch: &mut Vec<u8>,
) -> FormatResult<bool> {
    visit_canonical_key_specs(bytes, columns, scratch, |_| {})
}

fn visit_canonical_key_specs(
    bytes: &[u8],
    columns: &[DataColumnSpec],
    scratch: &mut Vec<u8>,
    mut visit: impl FnMut(Value),
) -> FormatResult<bool> {
    if bytes.is_empty() || bytes.len() as u64 > MAX_INDEX_KEY_BYTES {
        return Err(limit(
            "index key bytes",
            bytes.len() as u64,
            MAX_INDEX_KEY_BYTES,
        ));
    }
    let mut cursor = 0_usize;
    let mut has_null_component = false;
    for column in columns.iter().copied() {
        let header_end = cursor
            .checked_add(COMPONENT_HEADER_BYTES)
            .ok_or_else(|| invalid("index key component header overflows"))?;
        let header = bytes
            .get(cursor..header_end)
            .ok_or_else(|| invalid("index key component header is truncated"))?;
        if header[1] != column.data_type().logical_type().as_u8() || read_u16(header, 2) != 0 {
            return Err(invalid("index key component type/reserved field mismatch"));
        }
        let value_length = read_u32(header, 4) as usize;
        cursor = header_end;
        match header[0] {
            0 if value_length == 0 && column.nullable() => {
                has_null_component = true;
                visit(Value::null(column.data_type().logical_type()));
            }
            1 if value_length > 0 => {
                let value_end = cursor
                    .checked_add(value_length)
                    .ok_or_else(|| invalid("index key component value overflows"))?;
                let value_bytes = bytes
                    .get(cursor..value_end)
                    .ok_or_else(|| invalid("index key component value is truncated"))?;
                let data_column = DataColumn::new(
                    column.column_id(),
                    0,
                    column.data_type(),
                    column.nullable(),
                    0,
                    0,
                    0,
                );
                let value = decode_non_null_value(data_column, value_bytes)?;
                scratch.clear();
                append_canonical_non_null_bytes(column, &value, scratch)?;
                if scratch != value_bytes {
                    return Err(invalid("index key component is not canonical"));
                }
                visit(value);
                cursor = value_end;
                let aligned = cursor
                    .checked_add(3)
                    .map(|length| length & !3)
                    .ok_or_else(|| invalid("index key component alignment overflows"))?;
                let padding = bytes
                    .get(cursor..aligned)
                    .ok_or_else(|| invalid("index key component padding is truncated"))?;
                if padding.iter().any(|byte| *byte != 0) {
                    return Err(invalid("index key component padding is non-zero"));
                }
                cursor = aligned;
            }
            _ => return Err(invalid("index key component NULL tag/length is invalid")),
        }
    }
    if cursor != bytes.len() {
        return Err(invalid("index key has trailing components or bytes"));
    }
    Ok(has_null_component)
}

pub(super) fn diagnostic_key_hash(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    bytes.iter().fold(OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}

fn append_canonical_non_null_bytes(
    column: DataColumnSpec,
    value: &Value,
    bytes: &mut impl ValueByteBuffer,
) -> FormatResult<usize> {
    let start = bytes.len();
    let length = append_non_null_value(column, value, bytes)?;
    if column.data_type().logical_type() == DataType::Float {
        let value = match value {
            Value::Float(value) => *value,
            _ => return Err(invalid("FLOAT index key has wrong representation")),
        };
        let bits = if value.is_nan() {
            f64::NAN.to_bits()
        } else if value == 0.0 {
            0.0_f64.to_bits()
        } else {
            value.to_bits()
        };
        bytes.as_mut_slice()[start..start + length].copy_from_slice(&bits.to_le_bytes());
    }
    Ok(length)
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("checked field"))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("checked field"))
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use radixdb_catalog::{CatalogDataType, ObjectId};
    use radixdb_core::{DataType, Value};

    use super::{diagnostic_key_hash, encode_canonical_key_specs};
    use crate::v6::DataColumnSpec;

    #[test]
    fn diagnostic_fingerprint_has_stable_fnv1a_vectors() {
        assert_eq!(diagnostic_key_hash(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(diagnostic_key_hash(b"hello"), 0xa430_d846_80aa_bd0b);
    }

    #[test]
    fn ordinary_scalar_key_uses_inline_canonical_storage() {
        let column = DataColumnSpec::new(
            ObjectId::new(),
            CatalogDataType::scalar(DataType::Integer).unwrap(),
            false,
        );
        let (key, has_null) = encode_canonical_key_specs(&[column], &[Value::Integer(42)]).unwrap();

        assert!(!has_null);
        assert_eq!(key.len(), 16);
        assert!(!key.spilled(), "scalar key must not allocate a heap buffer");
    }
}
