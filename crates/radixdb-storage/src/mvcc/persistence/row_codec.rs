use super::*;

/// Current RowVersion format discriminator.
pub(super) const ROW_VERSION_MAGIC_V2: [u8; 8] = [0x52, 0x56, 0x32, 0x00, 0x00, 0x00, 0x00, 0x80];
/// Serialize a RowVersion to the current binary format.
pub fn serialize_row_version(version: &RowVersion) -> Result<Vec<u8>> {
    if version.create_time == i64::MAX {
        return Err(Error::internal(
            "RowVersion create_time exhausts the MVCC timestamp domain",
        ));
    }
    let mut buf = Vec::new();

    // Magic bytes for v2 format
    buf.extend_from_slice(&ROW_VERSION_MAGIC_V2);

    // Transaction ID
    buf.extend_from_slice(&version.txn_id.to_le_bytes());

    // Deleted at transaction ID (0 if not deleted)
    buf.extend_from_slice(&version.deleted_at_txn_id.to_le_bytes());

    // Create time
    buf.extend_from_slice(&version.create_time.to_le_bytes());

    // Data (Row - which is Vec<Value>)
    // Serialize values directly into buf using length-prefix-then-patch pattern
    // to avoid per-value Vec<u8> allocation.
    let value_count = checked_u32_len("row value count", version.data.len())?;
    buf.extend_from_slice(&value_count.to_le_bytes());
    for value in version.data.iter() {
        let len_pos = buf.len();
        buf.extend_from_slice(&0u32.to_le_bytes()); // placeholder for length
        let start = buf.len();
        serialize_value_into(&mut buf, value)?;
        let written = checked_u32_len("serialized value", buf.len() - start)?;
        buf[len_pos..len_pos + 4].copy_from_slice(&written.to_le_bytes());
    }

    Ok(buf)
}

/// Deserialize a RowVersion from the current binary format.
pub fn deserialize_row_version(data: &[u8]) -> Result<RowVersion> {
    if data.len() < ROW_VERSION_MAGIC_V2.len()
        || data[..ROW_VERSION_MAGIC_V2.len()] != ROW_VERSION_MAGIC_V2
    {
        return Err(Error::internal(
            "unsupported RowVersion format: expected RV2 discriminator",
        ));
    }
    deserialize_row_version_v2(data)
}

/// Deserialize v2 format (without row_id)
fn deserialize_row_version_v2(data: &[u8]) -> Result<RowVersion> {
    if data.len() < 32 {
        return Err(Error::internal("data too short for RowVersion v2"));
    }

    let mut pos = 8; // Skip 8-byte magic

    // Transaction ID
    let txn_id = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
    pos += 8;

    // Deleted at transaction ID
    let deleted_at_txn_id = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
    pos += 8;

    // Create time
    let create_time = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
    pos += 8;

    // Data (values)
    if pos + 4 > data.len() {
        return Err(Error::internal("missing value count"));
    }
    let value_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;

    validate_row_version_value_count(value_count, data.len().saturating_sub(pos))?;

    let mut values = Vec::with_capacity(value_count);
    for _ in 0..value_count {
        if pos + 4 > data.len() {
            return Err(Error::internal("missing value length"));
        }
        let value_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        if pos + value_len > data.len() {
            return Err(Error::internal("missing value data"));
        }
        let value = deserialize_value(&data[pos..pos + value_len])?;
        pos += value_len;
        values.push(value);
    }

    if pos != data.len() {
        return Err(Error::internal(
            "trailing bytes after RowVersion v2 payload",
        ));
    }
    if !crate::timestamp::observe_persisted_timestamp(create_time) {
        return Err(Error::internal(
            "RowVersion v2 create_time exhausts the MVCC timestamp domain",
        ));
    }

    Ok(RowVersion {
        txn_id,
        deleted_at_txn_id,
        data: Row::from_values(values),
        create_time,
    })
}

fn validate_row_version_value_count(value_count: usize, remaining: usize) -> Result<()> {
    // Every encoded Value has at least its u32 length prefix. Enforce this
    // relationship before Vec allocation so a tiny CRC-valid payload cannot
    // request memory proportional to an attacker-controlled count.
    let payload_derived_max = remaining / std::mem::size_of::<u32>();
    if value_count > payload_derived_max {
        return Err(Error::internal(format!(
            "value count {} exceeds payload-derived maximum {}",
            value_count, payload_derived_max
        )));
    }
    Ok(())
}

/// Serialize a Value to binary format
pub fn serialize_value(value: &Value) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    serialize_value_into(&mut buf, value)?;
    Ok(buf)
}

/// Serialize a Value directly into an existing buffer (zero per-value allocation).
/// Used on the hot WAL path where per-value Vec allocation is wasteful.
#[inline]
pub fn serialize_value_into(buf: &mut Vec<u8>, value: &Value) -> Result<()> {
    let original_len = buf.len();
    if let Err(error) = serialize_value_into_inner(buf, value) {
        buf.truncate(original_len);
        return Err(error);
    }
    Ok(())
}

fn serialize_value_into_inner(buf: &mut Vec<u8>, value: &Value) -> Result<()> {
    value.validate_shape()?;
    match value {
        Value::Null(dt) => {
            buf.push(0); // Type tag for Null
            buf.push(dt.as_u8()); // Store the DataType for typed nulls
        }
        Value::Boolean(b) => {
            buf.push(1);
            buf.push(if *b { 1 } else { 0 });
        }
        Value::Integer(i) => {
            buf.push(2);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        Value::Float(f) => {
            buf.push(3);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Text(s) => {
            buf.push(4);
            buf.extend_from_slice(&checked_u32_len("text value", s.len())?.to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Timestamp(ts) => {
            buf.push(8);
            buf.extend_from_slice(&ts.timestamp().to_le_bytes());
            buf.extend_from_slice(&ts.timestamp_subsec_nanos().to_le_bytes());
        }
        Value::Extension(data) => {
            if let Some(external) = value.as_external() {
                buf.push(12);
                buf.extend_from_slice(&external.type_ref().type_object_id());
                buf.extend_from_slice(&external.type_ref().codec_version().to_le_bytes());
                buf.extend_from_slice(
                    &checked_u32_len("external value", external.payload().len())?.to_le_bytes(),
                );
                buf.extend_from_slice(external.payload());
                return Ok(());
            }
            let (&tag, payload) = data.split_first().ok_or_else(|| {
                Error::invalid_argument("extension value is missing its data-type tag")
            })?;
            if tag == DataType::Json as u8 {
                buf.push(6);
                buf.extend_from_slice(&checked_u32_len("JSON value", payload.len())?.to_le_bytes());
                buf.extend_from_slice(payload);
            } else if tag == DataType::Vector as u8 {
                buf.push(10);
                let dim = checked_u32_len("vector dimension", payload.len() / 4)?;
                buf.extend_from_slice(&dim.to_le_bytes());
                buf.extend_from_slice(payload);
            } else {
                buf.push(11);
                buf.push(tag);
                buf.extend_from_slice(
                    &checked_u32_len("extension value", payload.len())?.to_le_bytes(),
                );
                buf.extend_from_slice(payload);
            }
        }
    }
    Ok(())
}

/// Deserialize a Value from binary format
pub fn deserialize_value(data: &[u8]) -> Result<Value> {
    if data.is_empty() {
        return Err(Error::internal("empty value data"));
    }

    let type_tag = data[0];
    let rest = &data[1..];

    match type_tag {
        0 => {
            if rest.len() != 1 {
                return Err(Error::internal(
                    "current NULL encoding requires exactly one data-type byte",
                ));
            }
            let dt = DataType::from_u8(rest[0]).ok_or_else(|| {
                Error::internal(format!("unknown NULL data type tag: {}", rest[0]))
            })?;
            Ok(Value::Null(dt))
        }
        1 => {
            // Boolean
            if rest.len() != 1 {
                return Err(Error::internal("invalid boolean value length"));
            }
            match rest[0] {
                0 => Ok(Value::Boolean(false)),
                1 => Ok(Value::Boolean(true)),
                tag => Err(Error::internal(format!("unknown boolean tag: {tag}"))),
            }
        }
        2 => {
            // Integer
            if rest.len() != 8 {
                return Err(Error::internal("invalid integer value length"));
            }
            Ok(Value::Integer(i64::from_le_bytes(
                rest[..8].try_into().unwrap(),
            )))
        }
        3 => {
            // Float
            if rest.len() != 8 {
                return Err(Error::internal("invalid float value length"));
            }
            Ok(Value::Float(f64::from_le_bytes(
                rest[..8].try_into().unwrap(),
            )))
        }
        4 => {
            // Text
            if rest.len() < 4 {
                return Err(Error::internal("missing text length"));
            }
            let len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            if rest.len() != 4 + len {
                return Err(Error::internal("invalid text data length"));
            }
            let s = String::from_utf8(rest[4..4 + len].to_vec())
                .map_err(|e| Error::internal(format!("invalid text: {}", e)))?;
            Ok(Value::Text(SmartString::from_string(s)))
        }
        8 => {
            // Binary timestamp format (seconds + subsec_nanos) - new efficient format
            if rest.len() != 12 {
                return Err(Error::internal("invalid timestamp data length"));
            }
            let secs = i64::from_le_bytes(rest[..8].try_into().unwrap());
            let nsecs = u32::from_le_bytes(rest[8..12].try_into().unwrap());
            let ts = chrono::DateTime::from_timestamp(secs, nsecs)
                .ok_or_else(|| Error::internal("invalid timestamp"))?;
            Ok(Value::Timestamp(ts))
        }
        6 => {
            // Json → Extension(DataType::Json, bytes)
            if rest.len() < 4 {
                return Err(Error::internal("missing json length"));
            }
            let len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            if rest.len() != 4 + len {
                return Err(Error::internal("invalid json data length"));
            }
            // Validate the full JSON document before publishing it.
            let payload = &rest[4..4 + len];
            let json = std::str::from_utf8(payload)
                .map_err(|e| Error::internal(format!("invalid json utf8: {}", e)))?;
            Value::try_json(json)
        }
        9 => Err(Error::internal(
            "retired vector value tag 9 is not accepted by the current decoder",
        )),
        10 => {
            // New binary vector format: dim_u32 + raw LE f32 bytes
            if rest.len() < 4 {
                return Err(Error::internal("missing vector dimension"));
            }
            let dim = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            let payload_len = dim
                .checked_mul(4)
                .ok_or_else(|| Error::internal("vector byte length overflow"))?;
            if rest.len() != 4 + payload_len {
                return Err(Error::internal("invalid vector data length"));
            }
            let payload = CompactArc::from(&rest[4..4 + payload_len]);
            Value::try_vector_from_bytes(payload)
        }
        11 => {
            // Generic extension: dt_u8 + len_u32 + raw bytes
            if rest.len() < 5 {
                return Err(Error::internal("missing extension header"));
            }
            let dt_byte = rest[0];
            let _dt = DataType::from_u8(dt_byte)
                .ok_or_else(|| Error::internal(format!("unknown extension type: {}", dt_byte)))?;
            let len = u32::from_le_bytes(rest[1..5].try_into().unwrap()) as usize;
            if rest.len() != 5 + len {
                return Err(Error::internal("invalid extension data length"));
            }
            let payload = &rest[5..5 + len];
            let mut bytes = Vec::with_capacity(1 + payload.len());
            bytes.push(dt_byte);
            bytes.extend_from_slice(payload);
            let value = Value::Extension(CompactArc::from(bytes));
            value.validate_shape()?;
            Ok(value)
        }
        12 => {
            const HEADER_BYTES: usize = 16 + 4 + 4;
            if rest.len() < HEADER_BYTES {
                return Err(Error::internal("missing external value header"));
            }
            let type_object_id = rest[..16]
                .try_into()
                .expect("checked external identity width");
            let codec_version = u32::from_le_bytes(
                rest[16..20]
                    .try_into()
                    .expect("checked external codec width"),
            );
            let length = u32::from_le_bytes(
                rest[20..24]
                    .try_into()
                    .expect("checked external length width"),
            ) as usize;
            if rest.len() != HEADER_BYTES + length {
                return Err(Error::internal("invalid external value data length"));
            }
            let type_ref = radixdb_core::ExternalTypeRef::new(type_object_id, codec_version)
                .map_err(|error| Error::internal(error.to_string()))?;
            Value::try_external(type_ref, &rest[HEADER_BYTES..])
                .map_err(|error| Error::internal(error.to_string()))
        }
        _ => Err(Error::internal(format!(
            "unknown value type tag: {}",
            type_tag
        ))),
    }
}
