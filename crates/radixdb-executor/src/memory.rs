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

//! Bounded retained-memory admission for blocking executor operations.

use crate::context::ExecutionContext;
use crate::hash_table::JoinMemoryReservation;
use radixdb_core::{Error, Result, Row, Value};

/// Shared retained-memory guard for blocking relational operators.
///
/// The estimate is intentionally conservative and portable: it accounts for
/// each row/value object plus variable-width payloads.  It is a hard admission
/// boundary, not telemetry, so a query fails normally instead of growing until
/// the process is killed.
#[doc(hidden)]
pub struct RetainedRowsBudget {
    owner: &'static str,
    max_rows: usize,
    max_bytes: usize,
    rows: usize,
    bytes: usize,
    peak_rows: usize,
    peak_bytes: usize,
    request_reservation: Option<JoinMemoryReservation>,
    request_max_bytes: usize,
}

impl RetainedRowsBudget {
    pub const DEFAULT_MAX_ROWS: usize = 1_000_000;
    pub const DEFAULT_MAX_BYTES: usize = 256 * 1024 * 1024;

    pub fn new(owner: &'static str) -> Self {
        Self::with_limits(owner, Self::DEFAULT_MAX_ROWS, Self::DEFAULT_MAX_BYTES)
    }

    pub fn with_limits(owner: &'static str, max_rows: usize, max_bytes: usize) -> Self {
        Self {
            owner,
            max_rows,
            max_bytes,
            rows: 0,
            bytes: 0,
            peak_rows: 0,
            peak_bytes: 0,
            request_reservation: None,
            request_max_bytes: 0,
        }
    }

    /// Attach this blocking owner to the request-wide JOIN/relational memory
    /// ceiling. Local row/byte limits remain an independent fail-closed guard.
    pub fn with_request_memory(owner: &'static str, ctx: &ExecutionContext) -> Result<Self> {
        let mut budget = Self::new(owner);
        budget.request_reservation = ctx.reserve_join_memory(0);
        budget.request_max_bytes = ctx.join_hash_state_max_bytes();
        if budget.request_reservation.is_none() {
            return Err(Error::InvalidArgument(format!(
                "{owner} request retained-memory budget is unavailable"
            )));
        }
        Ok(budget)
    }

    pub fn admit(&mut self, row: &Row) -> Result<()> {
        let row_bytes = Self::estimate_row_bytes(row);
        self.admit_bytes(row_bytes)
    }

    /// Admit a non-owned row graph whose retained size was computed without
    /// forcing materialization (for example a deferred parallel JOIN probe).
    pub fn admit_estimated_bytes(&mut self, retained_bytes: usize) -> Result<()> {
        self.admit_bytes(retained_bytes)
    }

    pub fn admit_row_and_values(&mut self, row: &Row, values: &[Value]) -> Result<()> {
        self.admit_bytes(
            Self::estimate_row_bytes(row).saturating_add(Self::estimate_values_bytes(values)),
        )
    }

    pub fn admit_values(&mut self, values: &[Value]) -> Result<()> {
        self.admit_auxiliary_bytes(Self::estimate_values_bytes(values))
    }

    pub fn ensure_capacity(&self, rows: usize) -> Result<()> {
        let minimum_bytes = rows.saturating_mul(std::mem::size_of::<Row>());
        if rows > self.max_rows || minimum_bytes > self.max_bytes {
            return Err(Error::InvalidArgument(format!(
                "{} retained-row budget exceeded before allocation (rows {}/{}, minimum bytes {}/{})",
                self.owner, rows, self.max_rows, minimum_bytes, self.max_bytes
            )));
        }
        Ok(())
    }

    fn admit_bytes(&mut self, retained_bytes: usize) -> Result<()> {
        let next_rows = self.rows.saturating_add(1);
        let next_bytes = self.bytes.saturating_add(retained_bytes);
        if next_rows > self.max_rows || next_bytes > self.max_bytes {
            return Err(Error::InvalidArgument(format!(
                "{} retained-row budget exceeded (rows {}/{}, bytes {}/{})",
                self.owner, next_rows, self.max_rows, next_bytes, self.max_bytes
            )));
        }
        if self
            .request_reservation
            .as_mut()
            .is_some_and(|reservation| !reservation.try_resize(next_bytes, self.request_max_bytes))
        {
            return Err(Error::InvalidArgument(format!(
                "{} request retained-memory budget exceeded (bytes {}/{})",
                self.owner, next_bytes, self.request_max_bytes
            )));
        }
        self.rows = next_rows;
        self.bytes = next_bytes;
        self.peak_rows = self.peak_rows.max(next_rows);
        self.peak_bytes = self.peak_bytes.max(next_bytes);
        Ok(())
    }

    fn admit_auxiliary_bytes(&mut self, retained_bytes: usize) -> Result<()> {
        let next_bytes = self.bytes.saturating_add(retained_bytes);
        if next_bytes > self.max_bytes {
            return Err(Error::InvalidArgument(format!(
                "{} retained-row budget exceeded (rows {}/{}, bytes {}/{})",
                self.owner, self.rows, self.max_rows, next_bytes, self.max_bytes
            )));
        }
        if self
            .request_reservation
            .as_mut()
            .is_some_and(|reservation| !reservation.try_resize(next_bytes, self.request_max_bytes))
        {
            return Err(Error::InvalidArgument(format!(
                "{} request retained-memory budget exceeded (bytes {}/{})",
                self.owner, next_bytes, self.request_max_bytes
            )));
        }
        self.bytes = next_bytes;
        self.peak_bytes = self.peak_bytes.max(next_bytes);
        Ok(())
    }

    pub fn release(&mut self, row: &Row) {
        self.rows = self.rows.saturating_sub(1);
        self.bytes = self.bytes.saturating_sub(Self::estimate_row_bytes(row));
        self.resize_request_reservation_after_release();
    }

    pub fn release_row_and_values(&mut self, row: &Row, values: &[Value]) {
        self.rows = self.rows.saturating_sub(1);
        self.bytes = self.bytes.saturating_sub(
            Self::estimate_row_bytes(row).saturating_add(Self::estimate_values_bytes(values)),
        );
        self.resize_request_reservation_after_release();
    }

    pub fn release_values(&mut self, values: &[Value]) {
        self.bytes = self
            .bytes
            .saturating_sub(Self::estimate_values_bytes(values));
        self.resize_request_reservation_after_release();
    }

    fn resize_request_reservation_after_release(&mut self) {
        if let Some(reservation) = self.request_reservation.as_mut() {
            debug_assert!(reservation.try_resize(self.bytes, self.request_max_bytes));
        }
    }

    pub fn estimate_row_bytes(row: &Row) -> usize {
        std::mem::size_of::<Row>().saturating_add(Self::estimate_values_bytes(row.as_slice()))
    }

    pub fn estimate_values_bytes(values: &[Value]) -> usize {
        values.iter().fold(0, |total, value| {
            let payload = match value {
                Value::Text(text) => text.len(),
                Value::Extension(bytes) => bytes.len(),
                _ => 0,
            };
            total
                .saturating_add(std::mem::size_of::<Value>())
                .saturating_add(payload)
        })
    }

    #[doc(hidden)]
    pub fn retained_rows(&self) -> usize {
        self.rows
    }

    #[doc(hidden)]
    pub fn retained_bytes(&self) -> usize {
        self.bytes
    }

    pub fn peak_rows(&self) -> usize {
        self.peak_rows
    }

    pub fn peak_bytes(&self) -> usize {
        self.peak_bytes
    }
}
