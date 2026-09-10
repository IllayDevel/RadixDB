use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use radixdb_core::{DataType, Value};

use crate::byte_limiter::{InFlightByteLimiter, InFlightBytePermit};

use super::super::{DataBloomConfig, DataColumnSpec, DataValueEncoding, FormatError, FormatResult};
use super::model::{limit_data, limit_index, FanoutBuildLimits, MAX_SORT_RUN_DESCRIPTOR_SLOTS};

static RESIDENT_GOVERNOR: OnceLock<InFlightByteLimiter> = OnceLock::new();
static SPILL_GOVERNOR: OnceLock<SharedByteGovernor> = OnceLock::new();
static SORT_RUN_DESCRIPTOR_GOVERNOR: OnceLock<InFlightByteLimiter> = OnceLock::new();

pub(super) fn acquire_sort_run_descriptors(count: usize) -> FormatResult<InFlightBytePermit> {
    if count == 0 || count > MAX_SORT_RUN_DESCRIPTOR_SLOTS {
        return Err(limit_index(
            "sort-run descriptor slots",
            count as u64,
            MAX_SORT_RUN_DESCRIPTOR_SLOTS as u64,
        ));
    }
    Ok(SORT_RUN_DESCRIPTOR_GOVERNOR
        .get_or_init(|| InFlightByteLimiter::new(MAX_SORT_RUN_DESCRIPTOR_SLOTS))
        .acquire(count))
}

#[derive(Debug)]
pub(super) struct PublicationBuildBudget {
    _resident_permit: InFlightBytePermit,
    _spill_permit: Option<SharedBytePermit>,
    variable_group_bytes: u64,
    data_metadata_bytes: u64,
    rebuild_projection_bytes: u64,
    data_block_bytes: u64,
    accelerator_resident_bytes: u64,
    spill: SpillBudget,
}

impl PublicationBuildBudget {
    pub(super) fn acquire(
        limits: FanoutBuildLimits,
        accelerator_count: usize,
    ) -> FormatResult<Self> {
        Self::acquire_with_index_bytes(
            limits,
            accelerator_count,
            limits.resident_byte_budget() / 4,
            limits.row_group_variable_byte_budget(),
            limits.resident_byte_budget() * 3 / 8,
        )
    }

    /// Rebuild reads already-published DATA and therefore has no row-group
    /// source buffer or DATA codec transient. Reassign those unused shares to
    /// the sort/page owner while keeping the exact same process-wide permit.
    pub(super) fn acquire_rebuild(
        limits: FanoutBuildLimits,
        accelerator_count: usize,
    ) -> FormatResult<Self> {
        Self::acquire_with_index_bytes(
            limits,
            accelerator_count,
            limits.resident_byte_budget() * 3 / 4,
            0,
            0,
        )
    }

    fn acquire_with_index_bytes(
        limits: FanoutBuildLimits,
        accelerator_count: usize,
        index_total: u64,
        variable_group_bytes: u64,
        data_block_bytes: u64,
    ) -> FormatResult<Self> {
        let requested = usize::try_from(limits.resident_byte_budget()).map_err(|_| {
            limit_index(
                "fanout resident bytes",
                limits.resident_byte_budget(),
                usize::MAX as u64,
            )
        })?;
        let governor = RESIDENT_GOVERNOR.get_or_init(|| {
            InFlightByteLimiter::new(super::model::MAX_FANOUT_RESIDENT_BYTES as usize)
        });
        let resident_permit = governor.acquire(requested);
        let spill_permit = if accelerator_count == 0 {
            None
        } else {
            Some(
                SPILL_GOVERNOR
                    .get_or_init(|| SharedByteGovernor::new(super::model::MAX_FANOUT_SPILL_BYTES))
                    .acquire(limits.spill_byte_budget()),
            )
        };
        let accelerator_resident_bytes = if accelerator_count == 0 {
            0
        } else {
            index_total / accelerator_count as u64
        };
        let minimum_record_bytes =
            (std::mem::size_of::<u64>() + std::mem::size_of::<usize>() * 3 + 1) as u64;
        if accelerator_count != 0 && accelerator_resident_bytes < minimum_record_bytes {
            return Err(limit_index(
                "accelerator resident bytes",
                minimum_record_bytes,
                accelerator_resident_bytes,
            ));
        }
        Ok(Self {
            _resident_permit: resident_permit,
            _spill_permit: spill_permit,
            variable_group_bytes,
            data_metadata_bytes: limits.resident_byte_budget() / 8,
            rebuild_projection_bytes: limits.resident_byte_budget() / 8,
            data_block_bytes,
            accelerator_resident_bytes,
            spill: SpillBudget::new(limits.spill_byte_budget()),
        })
    }

    pub(super) const fn variable_group_bytes(&self) -> u64 {
        self.variable_group_bytes
    }

    pub(super) const fn accelerator_resident_bytes(&self) -> u64 {
        self.accelerator_resident_bytes
    }

    pub(super) const fn data_metadata_bytes(&self) -> u64 {
        self.data_metadata_bytes
    }

    pub(super) const fn index_metadata_bytes(&self) -> u64 {
        self.data_metadata_bytes
    }

    pub(super) const fn rebuild_projection_bytes(&self) -> u64 {
        self.rebuild_projection_bytes
    }

    pub(super) fn admit_row_id_block(&self, row_count: usize) -> FormatResult<()> {
        let logical = (row_count as u64)
            .checked_mul(10)
            .and_then(|bytes| bytes.checked_add(24))
            .ok_or_else(|| {
                limit_data(
                    "row-ID block resident bytes",
                    u64::MAX,
                    self.data_block_bytes,
                )
            })?;
        self.admit_data_block("row-ID block resident bytes", logical.saturating_mul(2))
    }

    pub(super) fn admit_column_block(
        &self,
        column: DataColumnSpec,
        values: &[Value],
        encoding: DataValueEncoding,
    ) -> FormatResult<()> {
        let count = values.len() as u64;
        let validity = count.div_ceil(8);
        let logical_type = column.data_type().logical_type();
        let fixed = fixed_width(logical_type, column.data_type().parameter_1());
        let payload = if let Some(width) = fixed {
            count.checked_mul(width).ok_or_else(|| {
                limit_data(
                    "column block resident bytes",
                    u64::MAX,
                    self.data_block_bytes,
                )
            })?
        } else {
            encoded_variable_bytes(column, values)?
        };
        let plain_logical = 32_u64
            .checked_add(validity)
            .and_then(|bytes| {
                if fixed.is_some() {
                    Some(bytes)
                } else {
                    count
                        .checked_add(1)
                        .and_then(|items| items.checked_mul(4))
                        .and_then(|offsets| bytes.checked_add(offsets))
                }
            })
            .and_then(|bytes| bytes.checked_add(payload));
        let dictionary_logical = || {
            if !matches!(
                logical_type,
                DataType::Text | DataType::Json | DataType::Bytes
            ) {
                return Err(FormatError::InvalidDataArtifact {
                    detail: "dictionary encoding is not allowed for this type",
                });
            }
            40_u64
                .checked_add(validity)
                .and_then(|bytes| bytes.checked_add(count.saturating_add(1).saturating_mul(4)))
                .and_then(|bytes| bytes.checked_add(payload))
                .and_then(|bytes| bytes.checked_add(count.saturating_mul(4)))
                .ok_or_else(|| {
                    limit_data(
                        "column block resident bytes",
                        u64::MAX,
                        self.data_block_bytes,
                    )
                })
        };
        let plain_logical = plain_logical.ok_or_else(|| {
            limit_data(
                "column block resident bytes",
                u64::MAX,
                self.data_block_bytes,
            )
        })?;
        let dictionary_logical = if encoding == DataValueEncoding::Plain {
            None
        } else {
            Some(dictionary_logical()?)
        };
        let resident = match encoding {
            DataValueEncoding::Plain => plain_logical.checked_mul(2),
            DataValueEncoding::Dictionary => dictionary_logical
                .expect("dictionary encoding resolved its logical bound")
                .checked_mul(4)
                .and_then(|bytes| bytes.checked_add(count.saturating_mul(128))),
            DataValueEncoding::Adaptive => payload
                .checked_mul(3)
                .and_then(|bytes| bytes.checked_add(count.saturating_mul(128)))
                .and_then(|bytes| bytes.checked_add(plain_logical.saturating_sub(payload)))
                .and_then(|bytes| {
                    bytes.checked_add(
                        dictionary_logical
                            .expect("adaptive encoding resolved its dictionary bound")
                            .saturating_sub(payload),
                    )
                }),
        }
        .ok_or_else(|| {
            limit_data(
                "column block resident bytes",
                u64::MAX,
                self.data_block_bytes,
            )
        })?;
        self.admit_data_block("column block resident bytes", resident)
    }

    pub(super) fn admit_bloom_block(
        &self,
        values: &[Value],
        config: DataBloomConfig,
    ) -> FormatResult<()> {
        let logical = u64::from(config.bit_count())
            .div_ceil(8)
            .checked_add(20)
            .ok_or_else(|| {
                limit_data(
                    "bloom block resident bytes",
                    u64::MAX,
                    self.data_block_bytes,
                )
            })?;
        let maximum_value = values
            .iter()
            .map(value_payload_len)
            .try_fold(0_u64, |maximum, bytes| {
                bytes.map(|bytes| maximum.max(bytes))
            })?;
        let resident = logical
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(maximum_value))
            .ok_or_else(|| {
                limit_data(
                    "bloom block resident bytes",
                    u64::MAX,
                    self.data_block_bytes,
                )
            })?;
        self.admit_data_block("bloom block resident bytes", resident)
    }

    fn admit_data_block(&self, field: &'static str, bytes: u64) -> FormatResult<()> {
        if bytes > self.data_block_bytes {
            return Err(limit_data(field, bytes, self.data_block_bytes));
        }
        Ok(())
    }

    pub(super) fn spill(&self) -> SpillBudget {
        self.spill.clone()
    }
}

#[derive(Debug, Clone)]
struct SharedByteGovernor {
    inner: Arc<(Mutex<u64>, Condvar)>,
    capacity: u64,
}

impl SharedByteGovernor {
    fn new(capacity: u64) -> Self {
        Self {
            inner: Arc::new((Mutex::new(capacity), Condvar::new())),
            capacity,
        }
    }

    fn acquire(&self, requested: u64) -> SharedBytePermit {
        let bytes = requested.min(self.capacity);
        let (lock, condvar) = &*self.inner;
        let mut available = lock.lock().expect("shared byte governor mutex poisoned");
        while *available < bytes {
            available = condvar
                .wait(available)
                .expect("shared byte governor mutex poisoned while waiting");
        }
        *available -= bytes;
        SharedBytePermit {
            governor: self.clone(),
            bytes,
        }
    }
}

#[derive(Debug)]
struct SharedBytePermit {
    governor: SharedByteGovernor,
    bytes: u64,
}

impl Drop for SharedBytePermit {
    fn drop(&mut self) {
        let (lock, condvar) = &*self.governor.inner;
        let mut available = lock.lock().expect("shared byte governor mutex poisoned");
        *available = available
            .saturating_add(self.bytes)
            .min(self.governor.capacity);
        condvar.notify_all();
    }
}

#[derive(Debug, Clone)]
pub(super) struct SpillBudget {
    inner: Arc<SpillBudgetState>,
}

#[derive(Debug)]
struct SpillBudgetState {
    limit: u64,
    used: AtomicU64,
}

impl SpillBudget {
    fn new(limit: u64) -> Self {
        Self {
            inner: Arc::new(SpillBudgetState {
                limit,
                used: AtomicU64::new(0),
            }),
        }
    }

    pub(super) fn reserve(&self, bytes: u64) -> FormatResult<()> {
        let mut observed = self.inner.used.load(Ordering::Acquire);
        loop {
            let next = observed
                .checked_add(bytes)
                .ok_or_else(|| limit_index("fanout spill bytes", u64::MAX, self.inner.limit))?;
            if next > self.inner.limit {
                return Err(limit_index("fanout spill bytes", next, self.inner.limit));
            }
            match self.inner.used.compare_exchange_weak(
                observed,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => observed = actual,
            }
        }
    }
}

pub(super) fn value_payload_bytes(values: &[Value]) -> FormatResult<u64> {
    values.iter().try_fold(0_u64, |total, value| {
        let bytes = match value {
            Value::Text(text) => text.len() as u64,
            Value::Extension(bytes) => bytes.len() as u64,
            _ => 0,
        };
        total
            .checked_add(bytes)
            .ok_or_else(|| limit_data("row-group variable resident bytes", u64::MAX, u64::MAX))
    })
}

fn fixed_width(data_type: DataType, parameter_1: u32) -> Option<u64> {
    match data_type {
        DataType::Integer | DataType::Float | DataType::Timestamp => Some(8),
        DataType::Boolean => Some(1),
        DataType::Uuid => Some(16),
        DataType::Decimal => Some(18),
        DataType::Date => Some(4),
        DataType::Vector => u64::from(parameter_1).checked_mul(4),
        DataType::Text | DataType::Json | DataType::Bytes | DataType::Null => None,
    }
}

fn encoded_variable_bytes(column: DataColumnSpec, values: &[Value]) -> FormatResult<u64> {
    let data_type = column.data_type().logical_type();
    values.iter().try_fold(0_u64, |total, value| {
        let bytes = if value.is_null() {
            0
        } else if column.data_type().is_external() {
            value
                .as_external()
                .filter(|external| {
                    Some(external.type_ref()) == column.data_type().external_type_ref()
                })
                .map(|external| external.payload().len() as u64)
                .ok_or(FormatError::InvalidDataArtifact {
                    detail: "column value type differs from catalog type",
                })?
        } else {
            match data_type {
                DataType::Text => value.as_str().map(|value| value.len() as u64),
                DataType::Json => value.as_json().map(|value| value.len() as u64),
                DataType::Bytes => value.as_bytes_value().map(|value| value.len() as u64),
                _ => None,
            }
            .ok_or(FormatError::InvalidDataArtifact {
                detail: "column value type differs from catalog type",
            })?
        };
        total
            .checked_add(bytes)
            .ok_or_else(|| limit_data("column block resident bytes", u64::MAX, u64::MAX))
    })
}

fn value_payload_len(value: &Value) -> FormatResult<u64> {
    match value {
        Value::Text(text) => Ok(text.len() as u64),
        Value::Extension(bytes) => Ok(bytes.len() as u64),
        _ => Ok(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_governor_backpressures_and_restores_declared_bytes() {
        let governor = SharedByteGovernor::new(10);
        let first = governor.acquire(7);
        let second = governor.acquire(3);
        assert_eq!(*governor.inner.0.lock().unwrap(), 0);
        drop(second);
        drop(first);
        assert_eq!(*governor.inner.0.lock().unwrap(), 10);
    }
}
