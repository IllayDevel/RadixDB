use super::super::FormatResult;
use super::model::{
    invalid, limit, DataBlockRef, DataRowGroup, MAX_LOGICAL_BYTES_PER_BLOCK, MAX_ROWS_PER_GROUP,
};
use super::physical::decode_physical;

const HEADER_BYTES: usize = 24;
const MAGIC: [u8; 4] = *b"RID2";
const VERSION: u16 = 2;
const DENSE_FLAG: u16 = 1;
const SIGN_BIT: u64 = 1_u64 << 63;

/// Map the complete signed runtime row-ID domain onto the persisted unsigned
/// domain while preserving its natural order. DATA directories, manifests and
/// delta coding can therefore keep using canonical unsigned comparisons.
pub(crate) const fn encode_runtime_row_id(row_id: i64) -> u64 {
    (row_id as u64) ^ SIGN_BIT
}

/// Reverse [`encode_runtime_row_id`] without narrowing the persisted value.
pub(crate) const fn decode_runtime_row_id(row_id: u64) -> i64 {
    (row_id ^ SIGN_BIT) as i64
}

pub(crate) fn encode_row_id_payload(row_ids: &[u64]) -> FormatResult<Vec<u8>> {
    if row_ids.is_empty() || row_ids.len() > MAX_ROWS_PER_GROUP as usize {
        return Err(limit(
            "rows per group",
            row_ids.len() as u64,
            u64::from(MAX_ROWS_PER_GROUP),
        ));
    }
    if row_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid("row IDs are not strictly increasing"));
    }

    let dense = row_ids.windows(2).all(|pair| pair[1] - pair[0] == 1);
    let mut encoded = Vec::with_capacity(if dense { 0 } else { row_ids.len() });
    if !dense {
        for pair in row_ids.windows(2) {
            encode_unsigned(pair[1] - pair[0], &mut encoded);
        }
    }
    let encoded_length = u32::try_from(encoded.len())
        .map_err(|_| invalid("row-ID encoded length does not fit u32"))?;
    let item_count =
        u32::try_from(row_ids.len()).map_err(|_| invalid("row-ID item count does not fit u32"))?;
    let total_length = HEADER_BYTES
        .checked_add(encoded.len())
        .ok_or_else(|| invalid("row-ID payload length overflows"))?;
    if total_length as u64 > MAX_LOGICAL_BYTES_PER_BLOCK {
        return Err(limit(
            "logical block bytes",
            total_length as u64,
            MAX_LOGICAL_BYTES_PER_BLOCK,
        ));
    }

    let mut output = vec![0_u8; HEADER_BYTES];
    output[..4].copy_from_slice(&MAGIC);
    output[4..6].copy_from_slice(&VERSION.to_le_bytes());
    output[6..8].copy_from_slice(&u16::from(dense).to_le_bytes());
    output[8..12].copy_from_slice(&item_count.to_le_bytes());
    output[12..16].copy_from_slice(&encoded_length.to_le_bytes());
    output[16..24].copy_from_slice(&row_ids[0].to_le_bytes());
    output.extend_from_slice(&encoded);
    Ok(output)
}

pub(crate) fn decode_row_id_payload(
    stored: &[u8],
    block: &DataBlockRef,
    group: DataRowGroup,
) -> FormatResult<Vec<u64>> {
    let logical = decode_physical(stored, block)?;
    if logical.len() < HEADER_BYTES {
        return Err(invalid("row-ID payload is shorter than its header"));
    }
    if logical[..4] != MAGIC {
        return Err(invalid("row-ID payload magic mismatch"));
    }
    if read_u16(&logical, 4) != VERSION {
        return Err(invalid("unsupported row-ID payload version"));
    }
    let flags = read_u16(&logical, 6);
    if flags & !DENSE_FLAG != 0 {
        return Err(invalid("unknown row-ID payload flags"));
    }
    let item_count = read_u32(&logical, 8);
    if u64::from(item_count) != block.item_count() || item_count != group.row_count() {
        return Err(invalid("row-ID payload item count mismatch"));
    }
    if item_count == 0 || item_count > MAX_ROWS_PER_GROUP {
        return Err(limit(
            "rows per group",
            u64::from(item_count),
            u64::from(MAX_ROWS_PER_GROUP),
        ));
    }
    let encoded_length = usize::try_from(read_u32(&logical, 12))
        .map_err(|_| invalid("row-ID encoded length does not fit this platform"))?;
    if HEADER_BYTES.checked_add(encoded_length) != Some(logical.len()) {
        return Err(invalid("row-ID encoded length mismatch"));
    }

    let dense = flags & DENSE_FLAG != 0;
    if dense != (encoded_length == 0) {
        return Err(invalid("row-ID dense flag/encoded length mismatch"));
    }
    let mut cursor = HEADER_BYTES;
    let mut row_ids = Vec::with_capacity(item_count as usize);
    let first = read_u64(&logical, 16);
    if first != group.min_row_id() {
        return Err(invalid("row-ID first value mismatch"));
    }
    row_ids.push(first);
    while row_ids.len() < item_count as usize {
        let delta = if dense {
            1
        } else {
            decode_unsigned(&logical, &mut cursor)?
        };
        if delta == 0 {
            return Err(invalid("row-ID delta is zero"));
        }
        let next = row_ids[row_ids.len() - 1]
            .checked_add(delta)
            .ok_or_else(|| invalid("row-ID delta overflows"))?;
        row_ids.push(next);
    }
    if cursor != logical.len() {
        return Err(invalid("row-ID payload has trailing bytes"));
    }
    if row_ids[row_ids.len() - 1] != group.max_row_id() {
        return Err(invalid("row-ID last value mismatch"));
    }
    Ok(row_ids)
}

fn encode_unsigned(mut value: u64, output: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            return;
        }
    }
}

fn decode_unsigned(bytes: &[u8], cursor: &mut usize) -> FormatResult<u64> {
    let start = *cursor;
    let mut value = 0_u64;
    for index in 0..10_u32 {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| invalid("truncated row-ID LEB128 value"))?;
        *cursor += 1;
        let payload = u64::from(byte & 0x7f);
        if index == 9 && payload > 1 {
            return Err(invalid("row-ID LEB128 value overflows u64"));
        }
        value |= payload << (index * 7);
        if byte & 0x80 == 0 {
            if *cursor - start > 1 && payload == 0 {
                return Err(invalid("row-ID LEB128 value is not minimally encoded"));
            }
            return Ok(value);
        }
    }
    Err(invalid("row-ID LEB128 value exceeds ten bytes"))
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("checked header"),
    )
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("checked header"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("checked header"),
    )
}

#[cfg(test)]
mod tests {
    use super::{decode_runtime_row_id, encode_runtime_row_id};

    #[test]
    fn runtime_row_id_mapping_is_bijective_and_order_preserving() {
        let logical = [i64::MIN, -7, -1, 0, 1, 9, i64::MAX];
        let physical = logical.map(encode_runtime_row_id);

        assert_eq!(physical[0], 0);
        assert_eq!(physical[3], 1_u64 << 63);
        assert_eq!(physical[physical.len() - 1], u64::MAX);
        assert!(physical.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(physical.map(decode_runtime_row_id), logical);
    }
}
