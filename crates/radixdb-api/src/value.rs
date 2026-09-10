// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Conversion from the neutral storage value to public Rust API types.

use radixdb_core::{DataType, Error, Result, Value};

use crate::params::DecimalValue;

/// Trait for converting from [`Value`] to a Rust type.
pub trait FromValue: Sized {
    /// Convert a value to `Self`.
    fn from_value(value: &Value) -> Result<Self>;
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Integer(i) => Ok(*i),
            Value::Float(f)
                if f.is_finite() && *f >= i64::MIN as f64 && *f < -(i64::MIN as f64) =>
            {
                let integer = *f as i64;
                (Value::Integer(integer) == Value::Float(*f))
                    .then_some(integer)
                    .ok_or_else(|| Error::type_conversion(format!("{value:?}"), "Integer"))
            }
            _ => Err(Error::TypeConversion {
                from: format!("{:?}", value),
                to: "Integer".to_string(),
            }),
        }
    }
}

impl FromValue for i32 {
    fn from_value(value: &Value) -> Result<Self> {
        let integer = i64::from_value(value)?;
        i32::try_from(integer).map_err(|_| Error::type_conversion(format!("{value:?}"), "Integer"))
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value) -> Result<Self> {
        let candidate = match value {
            Value::Float(f) => return Ok(*f),
            Value::Integer(i) => *i as f64,
            // DECIMAL remains exact in storage and on the wire. Converting it
            // here is an explicit caller choice (`row.get::<f64>()`) and may
            // lose precision, just like converting an integer wider than the
            // exact f64 mantissa range.
            Value::Extension(data) if data.first() == Some(&(DataType::Decimal as u8)) => value
                .as_decimal_parts()
                .and_then(|(unscaled, _, scale)| {
                    radixdb_core::value::format_decimal_parts(unscaled, scale)
                        .parse::<f64>()
                        .ok()
                })
                .ok_or_else(|| Error::TypeConversion {
                    from: format!("{:?}", value),
                    to: "Float".to_string(),
                })?,
            _ => return Err(Error::type_conversion(format!("{value:?}"), "Float")),
        };
        if Value::Float(candidate) == *value {
            Ok(candidate)
        } else {
            Err(Error::type_conversion(format!("{value:?}"), "Float"))
        }
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s.to_string()),
            Value::Extension(data) if data.first() == Some(&(DataType::Json as u8)) => {
                std::str::from_utf8(&data[1..])
                    .map(str::to_owned)
                    .map_err(|_| Error::type_conversion(format!("{value:?}"), "String"))
            }
            Value::Integer(i) => Ok(i.to_string()),
            Value::Float(f) => Ok(f.to_string()),
            Value::Boolean(b) => Ok(if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }),
            Value::Timestamp(ts) => Ok(ts.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)),
            Value::Extension(_) => value
                .as_string()
                .ok_or_else(|| Error::invalid_argument("Cannot convert extension to String")),
            Value::Null(_) => Err(Error::type_conversion("NULL", "String")),
        }
    }
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Boolean(b) => Ok(*b),
            Value::Integer(i) => Ok(*i != 0),
            _ => Err(Error::TypeConversion {
                from: format!("{:?}", value),
                to: "Boolean".to_string(),
            }),
        }
    }
}

impl FromValue for Value {
    fn from_value(value: &Value) -> Result<Self> {
        Ok(value.clone())
    }
}

impl FromValue for chrono::DateTime<chrono::Utc> {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Timestamp(timestamp) => Ok(*timestamp),
            _ => Err(Error::type_conversion(format!("{value:?}"), "Timestamp")),
        }
    }
}

impl FromValue for chrono::NaiveDate {
    fn from_value(value: &Value) -> Result<Self> {
        let days = value
            .as_date_days()
            .ok_or_else(|| Error::type_conversion(format!("{value:?}"), "Date"))?;
        chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
            .expect("valid Unix epoch date")
            .checked_add_signed(chrono::Duration::days(i64::from(days)))
            .ok_or_else(|| Error::type_conversion(format!("{value:?}"), "Date"))
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: &Value) -> Result<Self> {
        value
            .as_bytes_value()
            .map(ToOwned::to_owned)
            .ok_or_else(|| Error::type_conversion(format!("{value:?}"), "Bytes"))
    }
}

impl FromValue for Vec<f32> {
    fn from_value(value: &Value) -> Result<Self> {
        value
            .as_vector_f32()
            .ok_or_else(|| Error::type_conversion(format!("{value:?}"), "Vector"))
    }
}

impl FromValue for uuid::Uuid {
    fn from_value(value: &Value) -> Result<Self> {
        value
            .as_uuid_bytes()
            .map(uuid::Uuid::from_bytes)
            .ok_or_else(|| Error::type_conversion(format!("{value:?}"), "UUID"))
    }
}

impl FromValue for serde_json::Value {
    fn from_value(value: &Value) -> Result<Self> {
        let json = value
            .as_json()
            .ok_or_else(|| Error::type_conversion(format!("{value:?}"), "JSON"))?;
        serde_json::from_str(json).map_err(|_| Error::type_conversion(format!("{value:?}"), "JSON"))
    }
}

impl FromValue for DecimalValue {
    fn from_value(value: &Value) -> Result<Self> {
        let (unscaled, precision, scale) = value
            .as_decimal_parts()
            .ok_or_else(|| Error::type_conversion(format!("{value:?}"), "Decimal"))?;
        DecimalValue::try_new(unscaled, precision, scale)
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: &Value) -> Result<Self> {
        if value.is_null() {
            Ok(None)
        } else {
            Ok(Some(T::from_value(value)?))
        }
    }
}
