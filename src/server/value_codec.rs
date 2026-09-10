//! Value conversion at the transport/API boundary.

use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};
use radixdb_plugin_host::PluginRegistry;

use crate::protocol::WireValue;
use crate::{DataType, NamedParams, Value};

pub(super) fn wire_parameters_to_named(
    parameters: BTreeMap<String, WireValue>,
    plugin_registry: &PluginRegistry,
) -> Result<NamedParams, String> {
    let mut params = NamedParams::with_capacity(parameters.len());
    for (name, value) in parameters {
        params.insert(name, wire_value_to_radixdb(value, plugin_registry)?);
    }
    Ok(params)
}

pub(super) fn wire_value_to_radixdb(
    value: WireValue,
    plugin_registry: &PluginRegistry,
) -> Result<Value, String> {
    match value {
        WireValue::Null => Ok(Value::null_unknown()),
        WireValue::Bool(value) => Ok(Value::Boolean(value)),
        WireValue::Int(value) => Ok(Value::Integer(value)),
        WireValue::Int8(value) => Ok(Value::Integer(value as i64)),
        WireValue::Int16(value) => Ok(Value::Integer(value as i64)),
        WireValue::Int32(value) => Ok(Value::Integer(value as i64)),
        WireValue::UInt(value) => i64::try_from(value)
            .map(Value::Integer)
            .map_err(|_| format!("UInt value {value} does not fit RadixDB INTEGER")),
        WireValue::UInt8(value) => Ok(Value::Integer(value as i64)),
        WireValue::UInt16(value) => Ok(Value::Integer(value as i64)),
        WireValue::UInt32(value) => Ok(Value::Integer(value as i64)),
        WireValue::Float64(value) => Ok(Value::Float(value)),
        WireValue::String(value) => Ok(Value::text(value)),
        WireValue::Decimal {
            unscaled,
            precision,
            scale,
        } => Value::try_decimal(unscaled, precision, scale).map_err(|error| error.to_string()),
        WireValue::Bytes(value) => Ok(Value::bytes(value)),
        WireValue::Date {
            days_since_unix_epoch,
        } => Ok(Value::date(days_since_unix_epoch)),
        WireValue::Uuid(value) => Ok(Value::uuid(value)),
        WireValue::DateTime {
            millis_since_unix_epoch_utc,
        } => Utc
            .timestamp_millis_opt(millis_since_unix_epoch_utc)
            .single()
            .map(Value::Timestamp)
            .ok_or_else(|| {
                format!("DateTime millis {millis_since_unix_epoch_utc} is outside supported range")
            }),
        WireValue::TimestampNanos {
            nanos_since_unix_epoch_utc,
        } => Ok(Value::Timestamp(chrono::DateTime::from_timestamp_nanos(
            nanos_since_unix_epoch_utc,
        ))),
        WireValue::Json(value) => Value::try_json(value).map_err(|error| error.to_string()),
        WireValue::Vector(bytes) => {
            Value::try_vector_from_bytes(crate::common::CompactArc::from(bytes))
                .map_err(|error| error.to_string())
        }
        WireValue::External {
            type_object_id,
            codec_version,
            payload,
        } => {
            let value = Value::try_external(
                radixdb_core::ExternalTypeRef::new(type_object_id, codec_version)
                    .map_err(|error| error.to_string())?,
                payload,
            )
            .map_err(|error| error.to_string())?;
            plugin_registry
                .validate_external_value(&value)
                .map_err(|error| error.to_string())?;
            Ok(value)
        }
    }
}

pub(super) fn radixdb_value_to_wire(value: &Value) -> Result<WireValue, String> {
    if let Some(external) = value.as_external() {
        return Ok(WireValue::External {
            type_object_id: external.type_ref().type_object_id(),
            codec_version: external.type_ref().codec_version(),
            payload: external.payload().to_vec(),
        });
    }
    match value {
        Value::Null(_) => Ok(WireValue::Null),
        Value::Integer(value) => Ok(WireValue::Int(*value)),
        Value::Float(value) => Ok(WireValue::Float64(*value)),
        Value::Text(value) => Ok(WireValue::String(value.to_string())),
        Value::Boolean(value) => Ok(WireValue::Bool(*value)),
        Value::Timestamp(value) => value
            .timestamp_nanos_opt()
            .map(|nanos_since_unix_epoch_utc| WireValue::TimestampNanos {
                nanos_since_unix_epoch_utc,
            })
            .ok_or_else(|| "TIMESTAMP is outside protocol nanosecond range".to_string()),
        Value::Extension(_) if value.data_type() == DataType::Uuid => value
            .as_uuid_bytes()
            .map(WireValue::Uuid)
            .ok_or_else(|| "invalid UUID payload in RadixDB value".to_string()),
        Value::Extension(_) if value.data_type() == DataType::Decimal => value
            .as_decimal_parts()
            .map(|(unscaled, precision, scale)| WireValue::Decimal {
                unscaled,
                precision,
                scale,
            })
            .ok_or_else(|| "invalid DECIMAL payload in RadixDB value".to_string()),
        Value::Extension(_) if value.data_type() == DataType::Date => value
            .as_date_days()
            .map(|days_since_unix_epoch| WireValue::Date {
                days_since_unix_epoch,
            })
            .ok_or_else(|| "invalid DATE payload in RadixDB value".to_string()),
        Value::Extension(_) if value.data_type() == DataType::Bytes => value
            .as_bytes_value()
            .map(|bytes| WireValue::Bytes(bytes.to_vec()))
            .ok_or_else(|| "invalid BYTES payload in RadixDB value".to_string()),
        Value::Extension(data) if value.data_type() == DataType::Json => {
            String::from_utf8(data[1..].to_vec())
                .map(WireValue::Json)
                .map_err(|error| error.to_string())
        }
        Value::Extension(data) if value.data_type() == DataType::Vector => {
            Ok(WireValue::Vector(data[1..].to_vec()))
        }
        Value::Extension(_) => Err(format!(
            "RadixDB value type {} is not supported by the binary protocol MVP",
            value.data_type()
        )),
    }
}

pub(super) fn slice_external_bytes(
    data: &[u8],
    offsets: &[u32],
    start: usize,
    end: usize,
) -> (Vec<u8>, Vec<u32>) {
    let byte_start = offsets[start] as usize;
    let byte_end = offsets[end] as usize;
    let sliced_data = data[byte_start..byte_end].to_vec();
    let sliced_offsets = offsets[start..=end]
        .iter()
        .map(|offset| offset - offsets[start])
        .collect();
    (sliced_data, sliced_offsets)
}

pub(super) fn wire_column_len(column: &crate::protocol::WireColumn) -> usize {
    use crate::protocol::WireColumn;
    match column {
        WireColumn::Int64 { values, .. } | WireColumn::TimestampNanos { values, .. } => {
            values.len()
        }
        WireColumn::Float64 { values, .. } => values.len(),
        WireColumn::Boolean { values, .. } => values.len(),
        WireColumn::DictionaryText { ids, .. } => ids.len(),
        WireColumn::Bytes { offsets, .. } | WireColumn::JsonText { offsets, .. } => offsets.len(),
        WireColumn::External { offsets, .. } => offsets.len().saturating_sub(1),
    }
}

pub(super) fn wire_column_retained_bytes(column: &crate::protocol::WireColumn) -> u64 {
    use crate::protocol::WireColumn;
    match column {
        WireColumn::Int64 { values, nulls } | WireColumn::TimestampNanos { values, nulls } => {
            retained_vec_bytes(values).saturating_add(retained_vec_bytes(nulls))
        }
        WireColumn::Float64 { values, nulls } => {
            retained_vec_bytes(values).saturating_add(retained_vec_bytes(nulls))
        }
        WireColumn::Boolean { values, nulls } => {
            retained_vec_bytes(values).saturating_add(retained_vec_bytes(nulls))
        }
        WireColumn::DictionaryText {
            ids,
            dictionary,
            nulls,
        } => retained_vec_bytes(ids)
            .saturating_add(retained_vec_bytes(nulls))
            .saturating_add(
                dictionary
                    .iter()
                    .map(|value| value.len() as u64)
                    .fold(0_u64, u64::saturating_add),
            ),
        WireColumn::Bytes {
            data,
            offsets,
            nulls,
        }
        | WireColumn::JsonText {
            data,
            offsets,
            nulls,
        } => retained_vec_bytes(data)
            .saturating_add(retained_vec_bytes(offsets))
            .saturating_add(retained_vec_bytes(nulls)),
        WireColumn::External {
            data,
            offsets,
            nulls,
            ..
        } => retained_vec_bytes(data)
            .saturating_add(retained_vec_bytes(offsets))
            .saturating_add(retained_vec_bytes(nulls)),
    }
}

fn retained_vec_bytes<T>(values: &[T]) -> u64 {
    values
        .len()
        .saturating_mul(std::mem::size_of::<T>())
        .try_into()
        .unwrap_or(u64::MAX)
}
