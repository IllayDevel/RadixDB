use super::frame::{scalar, Frame};
use crate::{Diagnostic, DiagnosticKind, ProceduralResult, RuntimeValue, SlotId};

pub(super) fn make_record(
    frame: &mut Frame,
    destination: SlotId,
    fields: &[SlotId],
) -> ProceduralResult<()> {
    let values = fields
        .iter()
        .map(|field| scalar(frame, *field).cloned().map(Some))
        .collect::<ProceduralResult<Vec<_>>>()?;
    frame.assign(destination, RuntimeValue::Record(values), None)
}

pub(super) fn read_record_field(
    frame: &mut Frame,
    destination: SlotId,
    record: SlotId,
    field: u32,
) -> ProceduralResult<()> {
    let RuntimeValue::Record(values) = frame.read(record)? else {
        return Err(invalid("verified record slot changed type"));
    };
    let value = values
        .get(field as usize)
        .ok_or_else(|| invalid("verified record ordinal is out of bounds"))?
        .as_ref()
        .ok_or_else(|| {
            Diagnostic::new(
                DiagnosticKind::RuntimeNullNotAllowed,
                "record field is not initialized",
            )
        })?
        .clone();
    frame.assign(destination, RuntimeValue::Scalar(value), None)
}

pub(super) fn write_record_field(
    frame: &mut Frame,
    record: SlotId,
    field: u32,
    value: SlotId,
) -> ProceduralResult<()> {
    let value = scalar(frame, value)?.clone();
    let RuntimeValue::Record(current) = frame.read(record)? else {
        return Err(invalid("verified record slot changed type"));
    };
    let mut replacement = current.clone();
    let target = replacement
        .get_mut(field as usize)
        .ok_or_else(|| invalid("verified record ordinal is out of bounds"))?;
    *target = Some(value);
    frame.assign(record, RuntimeValue::Record(replacement), None)
}

fn invalid(message: &'static str) -> Diagnostic {
    Diagnostic::new(DiagnosticKind::RuntimeInvalidIr, message)
}
