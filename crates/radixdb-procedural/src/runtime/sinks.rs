use crate::host::SqlRowSink;
use crate::{BudgetOwner, Diagnostic, DiagnosticKind, ProceduralResult, RuntimeType, RuntimeValue};

pub(super) fn charge_result_row(
    budget: &BudgetOwner,
    row: &[RuntimeValue],
) -> ProceduralResult<()> {
    budget.charge_rows(1)?;
    budget.charge_result_bytes(row.iter().fold(0_u64, |total, value| {
        total.saturating_add(value.owned_bytes())
    }))
}

pub(super) struct ForwardResultSink<'a> {
    inner: &'a mut dyn SqlRowSink,
    budget: &'a BudgetOwner,
    total_rows: &'a mut u64,
    forwarded_rows: u64,
    expected: &'a [RuntimeType],
}

impl<'a> ForwardResultSink<'a> {
    pub(super) fn new(
        inner: &'a mut dyn SqlRowSink,
        budget: &'a BudgetOwner,
        rows: &'a mut u64,
        expected: &'a [RuntimeType],
    ) -> Self {
        Self {
            inner,
            budget,
            total_rows: rows,
            forwarded_rows: 0,
            expected,
        }
    }

    pub(super) const fn row_count(&self) -> u64 {
        self.forwarded_rows
    }
}

impl SqlRowSink for ForwardResultSink<'_> {
    fn push_row(&mut self, row: Vec<RuntimeValue>) -> ProceduralResult<()> {
        if row.len() != self.expected.len()
            || row
                .iter()
                .zip(self.expected)
                .any(|(value, expected)| !expected.accepts(value))
        {
            return Err(Diagnostic::new(
                DiagnosticKind::RuntimeInvalidIr,
                "SQL host row violates the bound RETURN QUERY contract",
            ));
        }
        charge_result_row(self.budget, &row)?;
        self.inner.push_row(row)?;
        *self.total_rows = self.total_rows.saturating_add(1);
        self.forwarded_rows = self.forwarded_rows.saturating_add(1);
        Ok(())
    }
}

pub(super) struct RejectResultRows;

impl SqlRowSink for RejectResultRows {
    fn push_row(&mut self, _row: Vec<RuntimeValue>) -> ProceduralResult<()> {
        Err(Diagnostic::new(
            DiagnosticKind::VerifyCapabilityDenied,
            "set-returning program requires an explicit result sink",
        ))
    }
}

pub(super) struct IntoSink {
    retain_first: bool,
    pub(super) first_row: Option<Vec<RuntimeValue>>,
    pub(super) row_count: u64,
    budget: BudgetOwner,
}

impl IntoSink {
    pub(super) fn new(retain_first: bool, budget: BudgetOwner) -> Self {
        Self {
            retain_first,
            first_row: None,
            row_count: 0,
            budget,
        }
    }
}

impl SqlRowSink for IntoSink {
    fn push_row(&mut self, row: Vec<RuntimeValue>) -> ProceduralResult<()> {
        self.budget.check_boundary()?;
        self.budget.charge_rows(1)?;
        let bytes = row.iter().fold(0u64, |total, value| {
            total.saturating_add(value.owned_bytes())
        });
        self.budget.charge_result_bytes(bytes)?;
        self.row_count = self.row_count.saturating_add(1);
        if self.retain_first {
            if self.first_row.is_some() {
                return Err(Diagnostic::new(
                    DiagnosticKind::CardinalityTooManyRows,
                    "SQL INTO produced more than one row",
                ));
            }
            self.first_row = Some(row);
        }
        Ok(())
    }
}
