//! Scanner construction and bounded scanner draining.

use radixdb_core::{Error, Result, Row, RowVec};
use radixdb_storage::expression::Expression as StorageExpression;
use radixdb_storage::traits::{Scanner, Table, TypedBatchFallbackReason};

use crate::context::ExecutionContext;

pub fn open_scan(
    table: &dyn Table,
    columns: &[usize],
    predicate: Option<&dyn StorageExpression>,
    context: &ExecutionContext,
) -> Result<Box<dyn Scanner>> {
    table
        .scan(columns, predicate)
        .map(|scanner| budget_scanner(scanner, context))
}

pub fn open_exact_projection_scan(
    table: &dyn Table,
    columns: &[usize],
    predicate: Option<&dyn StorageExpression>,
    context: &ExecutionContext,
) -> Result<Box<dyn Scanner>> {
    table
        .scan_exact_projection(columns, predicate)
        .map(|scanner| budget_scanner(scanner, context))
}

fn budget_scanner(scanner: Box<dyn Scanner>, context: &ExecutionContext) -> Box<dyn Scanner> {
    if !context.has_public_scan_budget() {
        return scanner;
    }
    Box::new(PublicBudgetScanner {
        inner: scanner,
        context: context.clone(),
        error: None,
    })
}

struct PublicBudgetScanner {
    inner: Box<dyn Scanner>,
    context: ExecutionContext,
    error: Option<Error>,
}

impl Scanner for PublicBudgetScanner {
    fn next(&mut self) -> bool {
        if self.error.is_some() || !self.inner.next() {
            return false;
        }
        if let Err(error) = self.context.claim_public_scan_rows(1) {
            self.error = Some(error);
            return false;
        }
        true
    }

    fn row(&self) -> &Row {
        self.inner.row()
    }

    fn err(&self) -> Option<&Error> {
        self.error.as_ref().or_else(|| self.inner.err())
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    fn take_row(&mut self) -> Row {
        self.inner.take_row()
    }

    fn estimated_count(&self) -> Option<usize> {
        self.inner.estimated_count()
    }

    fn take_row_with_id(&mut self) -> Result<(i64, Row)> {
        self.inner.take_row_with_id()
    }

    fn current_row_id(&self) -> Result<i64> {
        self.inner.current_row_id()
    }

    fn warmup(&mut self) {
        self.inner.warmup();
    }

    fn typed_batch_fallback_reason(&self) -> Option<TypedBatchFallbackReason> {
        Some(TypedBatchFallbackReason::UnsupportedResultShape)
    }
}

pub fn collect_scanner_rows(
    mut scanner: Box<dyn Scanner>,
    context: &ExecutionContext,
) -> Result<RowVec> {
    let mut rows = RowVec::with_capacity(scanner.estimated_count().unwrap_or(64).min(1024));
    let mut seen = 0u64;
    while scanner.next() {
        seen += 1;
        if seen.is_multiple_of(100) {
            context.check_cancelled()?;
        }
        rows.push(scanner.take_row_with_id()?);
    }
    if let Some(error) = scanner.err() {
        return Err(error.clone());
    }
    scanner.close()?;
    Ok(rows)
}
