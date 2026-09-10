use rustc_hash::FxHashMap;

use crate::context::StoredFunctionInvoker;
use crate::operator::RowRef;
use radixdb_core::{CompactArc, Row, Value};
use radixdb_storage::DeferredRow;

/// A read-only row view accepted by the expression VM.
#[derive(Clone, Copy)]
pub(super) enum RowView<'a> {
    Owned(&'a Row),
    Deferred(&'a RowRef),
    PortableDeferred(&'a DeferredRow),
}

impl<'a> RowView<'a> {
    #[inline]
    pub(super) fn get(self, index: usize) -> Option<&'a Value> {
        match self {
            Self::Owned(row) => row.get(index),
            Self::Deferred(row) => row.get(index),
            Self::PortableDeferred(row) => row.get(index),
        }
    }

    #[cfg(test)]
    pub(super) fn len(self) -> usize {
        match self {
            Self::Owned(row) => row.len(),
            Self::Deferred(row) => row.len(),
            Self::PortableDeferred(row) => row.len(),
        }
    }
}

/// Execution context for the expression VM.
///
/// Holds row views, correlated context, parameters and the request-local
/// durable function dispatcher. It owns no query or transaction semantics.
pub struct ExecuteContext<'a> {
    /// Primary row data.
    pub(super) row: RowView<'a>,
    /// Second row for joins.
    pub(super) row2: Option<RowView<'a>>,
    /// Outer row context for correlated subqueries.
    pub(super) outer_row: Option<&'a FxHashMap<CompactArc<str>, Value>>,
    /// Positional parameters.
    pub(super) params: &'a [Value],
    /// Named parameters.
    pub(super) named_params: Option<&'a FxHashMap<String, Value>>,
    /// Current transaction ID.
    pub(super) transaction_id: Option<u64>,
    /// Request-local durable function dispatcher.
    pub(crate) stored_function_invoker: Option<&'a std::sync::Arc<dyn StoredFunctionInvoker>>,
}

impl<'a> ExecuteContext<'a> {
    #[inline]
    pub fn new(row: &'a Row) -> Self {
        Self {
            row: RowView::Owned(row),
            row2: None,
            outer_row: None,
            params: &[],
            named_params: None,
            transaction_id: None,
            stored_function_invoker: None,
        }
    }

    #[inline]
    pub fn with_common_params(
        row: &'a Row,
        params: &'a [Value],
        named_params: Option<&'a FxHashMap<String, Value>>,
        transaction_id: Option<u64>,
    ) -> Self {
        Self {
            row: RowView::Owned(row),
            row2: None,
            outer_row: None,
            params,
            named_params,
            transaction_id,
            stored_function_invoker: None,
        }
    }

    pub fn for_join(row1: &'a Row, row2: &'a Row) -> Self {
        Self {
            row: RowView::Owned(row1),
            row2: Some(RowView::Owned(row2)),
            outer_row: None,
            params: &[],
            named_params: None,
            transaction_id: None,
            stored_function_invoker: None,
        }
    }

    pub fn for_join_ref(row1: &'a RowRef, row2: &'a Row) -> Self {
        Self {
            row: RowView::Deferred(row1),
            row2: Some(RowView::Owned(row2)),
            outer_row: None,
            params: &[],
            named_params: None,
            transaction_id: None,
            stored_function_invoker: None,
        }
    }

    pub fn for_join_refs(row1: &'a RowRef, row2: &'a RowRef) -> Self {
        Self {
            row: RowView::Deferred(row1),
            row2: Some(RowView::Deferred(row2)),
            outer_row: None,
            params: &[],
            named_params: None,
            transaction_id: None,
            stored_function_invoker: None,
        }
    }

    pub fn for_deferred(row: &'a DeferredRow) -> Self {
        Self {
            row: RowView::PortableDeferred(row),
            row2: None,
            outer_row: None,
            params: &[],
            named_params: None,
            transaction_id: None,
            stored_function_invoker: None,
        }
    }

    pub fn for_row_ref(row: &'a RowRef) -> Self {
        Self {
            row: RowView::Deferred(row),
            row2: None,
            outer_row: None,
            params: &[],
            named_params: None,
            transaction_id: None,
            stored_function_invoker: None,
        }
    }

    pub fn with_transaction_id(mut self, transaction_id: Option<u64>) -> Self {
        self.transaction_id = transaction_id;
        self
    }

    pub(crate) fn with_stored_function_invoker(
        mut self,
        invoker: Option<&'a std::sync::Arc<dyn StoredFunctionInvoker>>,
    ) -> Self {
        self.stored_function_invoker = invoker;
        self
    }

    pub fn with_params(mut self, params: &'a [Value]) -> Self {
        self.params = params;
        self
    }

    pub fn with_named_params(mut self, named_params: &'a FxHashMap<String, Value>) -> Self {
        self.named_params = Some(named_params);
        self
    }

    pub fn with_outer_row(mut self, outer_row: &'a FxHashMap<CompactArc<str>, Value>) -> Self {
        self.outer_row = Some(outer_row);
        self
    }
}
