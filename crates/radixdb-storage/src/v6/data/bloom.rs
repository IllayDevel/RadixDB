use radixdb_core::Value;

use super::super::FormatResult;
use super::column::{encode_non_null_value, validate_column_values};
use super::column_model::{DataColumn, DataColumnSpec};
use super::model::{
    invalid, limit, DataBlockRef, MAX_BLOOM_BITS_PER_GROUP_COLUMN, MAX_LOGICAL_BYTES_PER_BLOCK,
};
use super::physical::decode_physical;

const HEADER_BYTES: usize = 20;
const MAGIC: [u8; 4] = *b"BLM1";
const FORMAT_VERSION: u16 = 1;
const MAX_HASH_COUNT: u16 = 16;
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataBloomConfig {
    bit_count: u32,
    hash_count: u16,
    seed: u64,
}

impl DataBloomConfig {
    pub fn new(bit_count: u32, hash_count: u16, seed: u64) -> FormatResult<Self> {
        if bit_count == 0 || u64::from(bit_count) > MAX_BLOOM_BITS_PER_GROUP_COLUMN {
            return Err(limit(
                "bloom bits per group/column",
                u64::from(bit_count),
                MAX_BLOOM_BITS_PER_GROUP_COLUMN,
            ));
        }
        if !(1..=MAX_HASH_COUNT).contains(&hash_count) {
            return Err(invalid("bloom hash count is outside 1..=16"));
        }
        Ok(Self {
            bit_count,
            hash_count,
            seed,
        })
    }

    pub const fn bit_count(self) -> u32 {
        self.bit_count
    }

    pub const fn hash_count(self) -> u16 {
        self.hash_count
    }

    pub const fn seed(self) -> u64 {
        self.seed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataBloom {
    row_group_ordinal: u32,
    column: DataColumn,
    config: DataBloomConfig,
    bits: Vec<u8>,
}

impl DataBloom {
    pub const fn row_group_ordinal(&self) -> u32 {
        self.row_group_ordinal
    }

    pub const fn column_ordinal(&self) -> u32 {
        self.column.ordinal()
    }

    pub const fn config(&self) -> DataBloomConfig {
        self.config
    }

    pub fn bits(&self) -> &[u8] {
        &self.bits
    }

    pub fn might_contain(&self, value: &Value) -> FormatResult<bool> {
        if self.column.data_type().is_external()
            || value.is_null()
            || value.logical_type() != self.column.data_type().logical_type_ref()
        {
            return Ok(true);
        }
        let spec = DataColumnSpec::new(
            self.column.column_id(),
            self.column.data_type(),
            self.column.nullable(),
        );
        let bytes = encode_bloom_value(spec, value)?;
        Ok(test_hashes(&self.bits, self.config, spec, &bytes))
    }
}

pub(crate) fn encode_bloom_payload(
    column: DataColumnSpec,
    values: &[Value],
    config: DataBloomConfig,
) -> FormatResult<Vec<u8>> {
    if column.data_type().is_external() {
        return Err(invalid(
            "external bloom filters require a core-owned operator class",
        ));
    }
    validate_column_values(column, values)?;
    let byte_count = bloom_byte_count(config.bit_count())?;
    let logical_length = HEADER_BYTES
        .checked_add(byte_count)
        .ok_or_else(|| invalid("bloom payload length overflows"))?;
    if logical_length as u64 > MAX_LOGICAL_BYTES_PER_BLOCK {
        return Err(limit(
            "logical block bytes",
            logical_length as u64,
            MAX_LOGICAL_BYTES_PER_BLOCK,
        ));
    }
    let mut output = vec![0_u8; logical_length];
    output[..4].copy_from_slice(&MAGIC);
    put_u16(&mut output, 4, FORMAT_VERSION);
    put_u16(&mut output, 6, config.hash_count());
    put_u32(&mut output, 8, config.bit_count());
    put_u64(&mut output, 12, config.seed());
    for value in values.iter().filter(|value| !value.is_null()) {
        let bytes = encode_bloom_value(column, value)?;
        insert_hashes(&mut output[HEADER_BYTES..], config, column, &bytes);
    }
    clear_unused_bits(&mut output[HEADER_BYTES..], config.bit_count());
    Ok(output)
}

pub(crate) fn decode_bloom_payload(
    stored: &[u8],
    block: &DataBlockRef,
    column: DataColumn,
) -> FormatResult<DataBloom> {
    let bytes = decode_physical(stored, block)?;
    if bytes.len() < HEADER_BYTES || bytes[..4] != MAGIC {
        return Err(invalid("bloom header is missing"));
    }
    if read_u16(&bytes, 4) != FORMAT_VERSION {
        return Err(invalid("unsupported bloom payload version"));
    }
    let config = DataBloomConfig::new(
        read_u32(&bytes, 8),
        read_u16(&bytes, 6),
        read_u64(&bytes, 12),
    )?;
    if block.item_count() != u64::from(config.bit_count()) {
        return Err(invalid("bloom bit count differs from block directory"));
    }
    let expected = HEADER_BYTES
        .checked_add(bloom_byte_count(config.bit_count())?)
        .ok_or_else(|| invalid("bloom payload length overflows"))?;
    if bytes.len() != expected {
        return Err(invalid("bloom payload length is not canonical"));
    }
    let bits = bytes[HEADER_BYTES..].to_vec();
    validate_unused_bits(&bits, config.bit_count())?;
    Ok(DataBloom {
        row_group_ordinal: block.row_group_ordinal(),
        column,
        config,
        bits,
    })
}

fn encode_bloom_value(column: DataColumnSpec, value: &Value) -> FormatResult<Vec<u8>> {
    if let Value::Float(number) = value {
        validate_column_values(column, std::slice::from_ref(value))?;
        let bits = if number.is_nan() {
            f64::NAN.to_bits()
        } else if *number == 0.0 {
            0.0_f64.to_bits()
        } else {
            number.to_bits()
        };
        return Ok(bits.to_le_bytes().to_vec());
    }
    encode_non_null_value(column, value)
}

fn bloom_byte_count(bit_count: u32) -> FormatResult<usize> {
    usize::try_from(bit_count)
        .map(|bits| bits.div_ceil(8))
        .map_err(|_| invalid("bloom bit count does not fit this platform"))
}

fn insert_hashes(bits: &mut [u8], config: DataBloomConfig, column: DataColumnSpec, value: &[u8]) {
    for bit in bloom_positions(config, column, value) {
        bits[bit / 8] |= 1 << (bit % 8);
    }
}

fn test_hashes(bits: &[u8], config: DataBloomConfig, column: DataColumnSpec, value: &[u8]) -> bool {
    bloom_positions(config, column, value).all(|bit| bits[bit / 8] & (1 << (bit % 8)) != 0)
}

fn bloom_positions(
    config: DataBloomConfig,
    column: DataColumnSpec,
    value: &[u8],
) -> impl Iterator<Item = usize> {
    let (first, second) = bloom_hashes(config.seed(), column, value);
    let bit_count = u64::from(config.bit_count());
    (0..config.hash_count()).map(move |index| {
        first
            .wrapping_add(u64::from(index).wrapping_mul(second))
            .wrapping_rem(bit_count) as usize
    })
}

fn bloom_hashes(seed: u64, column: DataColumnSpec, value: &[u8]) -> (u64, u64) {
    let mut first = FNV_OFFSET ^ seed;
    let mut second = FNV_OFFSET ^ seed.rotate_left(31) ^ 0x9e37_79b9_7f4a_7c15;
    let type_tag = [column.data_type().logical_type().as_u8()];
    let descriptor_version = column.data_type().descriptor_version().to_le_bytes();
    mix(&mut first, &type_tag);
    mix(&mut second, &type_tag);
    mix(&mut first, &descriptor_version);
    mix(&mut second, &descriptor_version);
    let parameter_1 = column.data_type().parameter_1().to_le_bytes();
    let parameter_2 = column.data_type().parameter_2().to_le_bytes();
    mix(&mut first, &parameter_1);
    mix(&mut second, &parameter_1);
    mix(&mut first, &parameter_2);
    mix(&mut second, &parameter_2);
    mix(&mut first, &(value.len() as u64).to_le_bytes());
    mix(&mut second, &(value.len() as u64).to_le_bytes());
    mix(&mut first, value);
    mix(&mut second, value);
    (first, second | 1)
}

fn mix(state: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *state ^= u64::from(*byte);
        *state = state.wrapping_mul(FNV_PRIME);
    }
}

fn clear_unused_bits(bits: &mut [u8], bit_count: u32) {
    let used = bit_count as usize % 8;
    if used != 0 {
        let mask = (1_u8 << used) - 1;
        if let Some(last) = bits.last_mut() {
            *last &= mask;
        }
    }
}

fn validate_unused_bits(bits: &[u8], bit_count: u32) -> FormatResult<()> {
    let used = bit_count as usize % 8;
    if used != 0 {
        let mask = !((1_u8 << used) - 1);
        if bits.last().is_some_and(|last| last & mask != 0) {
            return Err(invalid("unused bloom bits are non-zero"));
        }
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
