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

//! One-way adaptation from an internal execution result to the embedded API.

use radixdb_core::CompactArc;
use radixdb_core::{Error, Result, Row};
use radixdb_executor::result::ExecutionResult;
use radixdb_storage::traits::{TypedBatchFallbackReason, TypedColumnBatch};

/// Private cursor owned by the API layer.
///
/// Keeping this concrete avoids adding a second virtual-dispatch boundary:
/// calls are forwarded directly to the single internal `QueryResult` object.
/// Public [`super::Rows`] and [`super::ResultRow`] never become execution
/// result implementations themselves.
pub(super) struct ApiResultCursor {
    inner: ExecutionResult,
}

impl ApiResultCursor {
    #[inline]
    pub(super) fn new(inner: ExecutionResult) -> Self {
        Self { inner }
    }

    #[inline]
    pub(super) fn columns(&self) -> &[String] {
        self.inner.columns()
    }

    #[inline]
    pub(super) fn columns_arc(&self) -> Option<CompactArc<Vec<String>>> {
        self.inner.columns_arc()
    }

    #[inline]
    pub(super) fn next(&mut self) -> bool {
        self.inner.next()
    }

    #[inline]
    pub(super) fn row(&self) -> &Row {
        self.inner.row()
    }

    #[inline]
    pub(super) fn take_row(&mut self) -> Row {
        self.inner.take_row()
    }

    #[inline]
    pub(super) fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    #[inline]
    pub(super) fn rows_affected(&self) -> i64 {
        self.inner.rows_affected()
    }

    #[inline]
    pub(super) fn last_insert_id(&self) -> i64 {
        self.inner.last_insert_id()
    }

    #[inline]
    pub(super) fn supports_typed_batches(&self) -> bool {
        self.inner.supports_typed_batches()
    }

    #[inline]
    pub(super) fn typed_batch_fallback_reason(&self) -> Option<TypedBatchFallbackReason> {
        self.inner.typed_batch_fallback_reason()
    }

    #[inline]
    pub(super) fn next_typed_batch(&mut self) -> Result<Option<TypedColumnBatch>> {
        self.inner.next_typed_batch()
    }

    #[inline]
    pub(super) fn last_error(&mut self) -> Option<Error> {
        self.inner.last_error()
    }
}

#[cfg(test)]
pub(super) fn close_failing_result() -> ExecutionResult {
    use radixdb_core::Value;
    use radixdb_storage::traits::QueryResult;
    use rustc_hash::FxHashMap;

    struct CloseFailResult {
        row: Row,
    }

    impl QueryResult for CloseFailResult {
        fn columns(&self) -> &[String] {
            &[]
        }

        fn next(&mut self) -> bool {
            false
        }

        fn scan(&self, _dest: &mut [Value]) -> Result<()> {
            Ok(())
        }

        fn row(&self) -> &Row {
            &self.row
        }

        fn rows_affected(&self) -> i64 {
            0
        }

        fn last_insert_id(&self) -> i64 {
            0
        }

        fn close(&mut self) -> Result<()> {
            Err(Error::internal("injected close failure"))
        }

        fn with_aliases(self: Box<Self>, _aliases: FxHashMap<String, String>) -> ExecutionResult {
            self
        }
    }

    Box::new(CloseFailResult { row: Row::new() })
}
