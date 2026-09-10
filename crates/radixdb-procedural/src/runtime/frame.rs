use radixdb_core::Value;

use crate::{
    BudgetOwner, Diagnostic, DiagnosticKind, ProceduralResult, RuntimeType, RuntimeValue, SlotId,
    SourceSpan,
};

pub(super) struct Frame {
    definitions: Vec<crate::SlotDefinition>,
    slots: Vec<Option<RuntimeValue>>,
    heap_bytes: u64,
    budget: BudgetOwner,
}

impl Frame {
    pub(super) fn new(definitions: &[crate::SlotDefinition], budget: BudgetOwner) -> Self {
        let slots = definitions
            .iter()
            .map(|definition| {
                matches!(definition.runtime_type(), RuntimeType::Collection { .. })
                    .then(|| RuntimeValue::Collection(Vec::new()))
            })
            .collect();
        Self {
            definitions: definitions.to_vec(),
            slots,
            heap_bytes: 0,
            budget,
        }
    }

    pub(super) fn slot_type(&self, slot: SlotId) -> ProceduralResult<&RuntimeType> {
        self.definitions
            .get(slot.0 as usize)
            .map(crate::SlotDefinition::runtime_type)
            .ok_or_else(|| invalid_runtime("verified slot disappeared at runtime"))
    }

    pub(super) fn read(&self, slot: SlotId) -> ProceduralResult<&RuntimeValue> {
        self.slots
            .get(slot.0 as usize)
            .and_then(Option::as_ref)
            .ok_or_else(|| invalid_runtime("verified initialized slot has no runtime value"))
    }

    pub(super) fn assign(
        &mut self,
        destination: SlotId,
        value: RuntimeValue,
        span: Option<&SourceSpan>,
    ) -> ProceduralResult<()> {
        self.assign_many(std::slice::from_ref(&destination), vec![value], span)
    }

    pub(super) fn assign_many(
        &mut self,
        destinations: &[SlotId],
        values: Vec<RuntimeValue>,
        span: Option<&SourceSpan>,
    ) -> ProceduralResult<()> {
        if destinations.len() != values.len() {
            return Err(attach_span(
                invalid_runtime("assignment width differs"),
                span,
            ));
        }
        let mut old_bytes = 0u64;
        let mut new_bytes = 0u64;
        for (destination, value) in destinations.iter().zip(&values) {
            let definition = self
                .definitions
                .get(destination.0 as usize)
                .ok_or_else(|| invalid_runtime("assignment destination is out of bounds"))?;
            if !definition.runtime_type().accepts(value) {
                return Err(attach_span(
                    Diagnostic::new(
                        DiagnosticKind::RuntimeInvalidIr,
                        "runtime value violates verified slot type",
                    )
                    .with_detail("slot", definition.name()),
                    span,
                ));
            }
            old_bytes = old_bytes.saturating_add(
                self.slots[destination.0 as usize]
                    .as_ref()
                    .map_or(0, RuntimeValue::owned_bytes),
            );
            new_bytes = new_bytes.saturating_add(value.owned_bytes());
        }
        if new_bytes > old_bytes {
            self.budget.charge_heap(new_bytes - old_bytes)?;
        }
        for (destination, value) in destinations.iter().zip(values) {
            self.slots[destination.0 as usize] = Some(value);
        }
        if old_bytes > new_bytes {
            self.budget.release_heap(old_bytes - new_bytes);
        }
        self.heap_bytes = self
            .heap_bytes
            .saturating_sub(old_bytes)
            .saturating_add(new_bytes);
        Ok(())
    }
}

impl Drop for Frame {
    fn drop(&mut self) {
        self.budget.release_heap(self.heap_bytes);
    }
}

pub(super) fn scalar(frame: &Frame, slot: SlotId) -> ProceduralResult<&Value> {
    let RuntimeValue::Scalar(value) = frame.read(slot)? else {
        return Err(invalid_runtime("verified scalar slot changed shape"));
    };
    Ok(value)
}

fn invalid_runtime(message: &'static str) -> Diagnostic {
    Diagnostic::new(DiagnosticKind::RuntimeInvalidIr, message)
}

fn attach_span(error: Diagnostic, span: Option<&SourceSpan>) -> Diagnostic {
    if error.primary_span().is_none() {
        error.with_primary_span(span.cloned())
    } else {
        error
    }
}
