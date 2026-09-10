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

//! Stable persisted-index hashing shared by volume construction and reads.

use radixdb_core::{DataType, Value};

/// Stable, versioned hash input for exact and ordered persisted postings.
///
/// The value is part of the on-disk contract and must never use process-local
/// hash state, native endianness, or the Rust [`Hash`](std::hash::Hash)
/// implementation of [`Value`].
#[doc(hidden)]
pub struct PersistedIndexHasher {
    state: u64,
}

/// Whether a persisted hash can definitively select candidates for a probe.
#[doc(hidden)]
pub fn persisted_index_hash_is_compatible(data_type: DataType, probe: Option<&Value>) -> bool {
    if matches!(data_type, DataType::Null | DataType::Decimal) {
        return false;
    }
    probe.is_none_or(|value| !value.is_null() && value.data_type() == data_type)
}

impl PersistedIndexHasher {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    #[doc(hidden)]
    pub fn new() -> Self {
        Self {
            state: Self::FNV_OFFSET,
        }
    }

    fn mix(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.state ^= u64::from(byte);
            self.state = self.state.wrapping_mul(Self::FNV_PRIME);
        }
    }

    #[doc(hidden)]
    pub fn add_value(&mut self, data_type: DataType, value: &Value) {
        match data_type {
            DataType::Integer => self.add_storage_value(&Value::Integer(match value {
                Value::Integer(value) => *value,
                _ => 0,
            })),
            DataType::Float => self.add_storage_value(&Value::Float(match value {
                Value::Float(value) => *value,
                _ => 0.0,
            })),
            DataType::Timestamp => {
                self.mix(&[3]);
                let nanos = match value {
                    Value::Timestamp(value) => value.timestamp_nanos_opt().unwrap_or_else(|| {
                        value
                            .timestamp()
                            .wrapping_mul(1_000_000_000)
                            .wrapping_add(value.timestamp_subsec_nanos() as i64)
                    }),
                    _ => 0,
                };
                self.mix(&nanos.to_le_bytes());
            }
            DataType::Boolean => self.add_storage_value(&Value::Boolean(match value {
                Value::Boolean(value) => *value,
                _ => false,
            })),
            DataType::Text => {
                if let Value::Text(value) = value {
                    self.add_storage_value(&Value::Text(value.clone()));
                } else {
                    self.add_storage_value(&Value::text(""));
                }
            }
            extension_type => {
                let bytes = match value {
                    Value::Extension(value) if value.len() > 1 => &value[1..],
                    _ => &[],
                };
                self.mix(&[6]);
                self.mix(&(bytes.len().saturating_add(1) as u64).to_le_bytes());
                self.mix(&[extension_type as u8]);
                self.mix(bytes);
            }
        }
    }

    #[doc(hidden)]
    pub fn add_storage_value(&mut self, value: &Value) {
        match value {
            Value::Null(_) => self.mix(&[0]),
            Value::Integer(value) => {
                self.mix(&[1]);
                self.mix(&value.to_le_bytes());
            }
            Value::Float(value) => {
                self.mix(&[2]);
                let bits = if value.is_nan() {
                    f64::NAN.to_bits()
                } else if *value == 0.0 {
                    0.0_f64.to_bits()
                } else {
                    value.to_bits()
                };
                self.mix(&bits.to_le_bytes());
            }
            Value::Timestamp(value) => {
                self.mix(&[3]);
                let nanos = value.timestamp_nanos_opt().unwrap_or_else(|| {
                    value
                        .timestamp()
                        .wrapping_mul(1_000_000_000)
                        .wrapping_add(value.timestamp_subsec_nanos() as i64)
                });
                self.mix(&nanos.to_le_bytes());
            }
            Value::Boolean(value) => self.mix(&[4, u8::from(*value)]),
            Value::Text(value) => {
                self.mix(&[5]);
                self.mix(&(value.len() as u64).to_le_bytes());
                self.mix(value.as_bytes());
            }
            Value::Extension(value) => {
                self.mix(&[6]);
                self.mix(&(value.len() as u64).to_le_bytes());
                self.mix(value.as_ref());
            }
        }
    }

    #[doc(hidden)]
    pub fn finish(self) -> u64 {
        self.state
    }
}

impl Default for PersistedIndexHasher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_hash_keeps_the_existing_golden_value() {
        let mut hasher = PersistedIndexHasher::new();
        hasher.add_value(DataType::Integer, &Value::Integer(42));
        hasher.add_value(DataType::Text, &Value::text("radix"));
        assert_eq!(hasher.finish(), 0xa487_656c_6bb8_5c7e);
    }

    #[test]
    fn persisted_hash_normalizes_signed_zero_and_extension_tag() {
        let mut positive_zero = PersistedIndexHasher::new();
        positive_zero.add_value(DataType::Float, &Value::Float(0.0));
        let mut negative_zero = PersistedIndexHasher::new();
        negative_zero.add_value(DataType::Float, &Value::Float(-0.0));
        assert_eq!(positive_zero.finish(), negative_zero.finish());

        let mut canonical = PersistedIndexHasher::new();
        canonical.add_value(DataType::Json, &Value::json(r#"{"id":1}"#));
        let mut stale_tag = PersistedIndexHasher::new();
        let mut bytes = vec![DataType::Bytes as u8];
        bytes.extend_from_slice(br#"{"id":1}"#);
        stale_tag.add_value(DataType::Json, &Value::Extension(bytes.into()));
        assert_eq!(canonical.finish(), stale_tag.finish());
    }

    #[test]
    fn pre_normalized_timestamp_uses_the_same_persisted_bytes() {
        let timestamp =
            chrono::DateTime::from_timestamp(1_700_000_000, 123_456_789).expect("valid timestamp");
        let value = Value::Timestamp(timestamp);

        let mut direct = PersistedIndexHasher::new();
        direct.add_value(DataType::Timestamp, &value);
        let mut normalized = PersistedIndexHasher::new();
        normalized.add_storage_value(&value);

        assert_eq!(direct.finish(), normalized.finish());
    }
}
