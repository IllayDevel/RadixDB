use radixdb_core::{DataType, Value};

use super::frame::{scalar, Frame};
use crate::{Diagnostic, DiagnosticKind, ProceduralResult, RuntimeValue, SlotId};

pub(super) fn collection_index(frame: &Frame, slot: SlotId) -> ProceduralResult<usize> {
    match scalar(frame, slot)? {
        Value::Integer(index) if *index > 0 => usize::try_from(*index - 1).map_err(|_| {
            Diagnostic::new(
                DiagnosticKind::RuntimeArrayBounds,
                "collection index exceeds the host address range",
            )
        }),
        _ => Err(Diagnostic::new(
            DiagnosticKind::RuntimeArrayBounds,
            "collection index must be a positive INTEGER",
        )),
    }
}

pub(super) fn integer_add_checked(
    left: &RuntimeValue,
    right: &RuntimeValue,
) -> ProceduralResult<RuntimeValue> {
    integer_binary(left, right, "addition", i64::checked_add)
}

pub(super) fn integer_subtract_checked(
    left: &RuntimeValue,
    right: &RuntimeValue,
) -> ProceduralResult<RuntimeValue> {
    integer_binary(left, right, "subtraction", i64::checked_sub)
}

fn integer_binary(
    left: &RuntimeValue,
    right: &RuntimeValue,
    operation: &'static str,
    apply: fn(i64, i64) -> Option<i64>,
) -> ProceduralResult<RuntimeValue> {
    let (RuntimeValue::Scalar(left), RuntimeValue::Scalar(right)) = (left, right) else {
        return Err(invalid_runtime("verified INTEGER operand changed shape"));
    };
    let value = match (left, right) {
        (Value::Null(_), _) | (_, Value::Null(_)) => Value::null(DataType::Integer),
        (Value::Integer(left), Value::Integer(right)) => {
            Value::Integer(apply(*left, *right).ok_or_else(|| {
                Diagnostic::new(
                    DiagnosticKind::RuntimeNumericOverflow,
                    format!("INTEGER {operation} overflow"),
                )
            })?)
        }
        _ => return Err(invalid_runtime("verified INTEGER operand changed type")),
    };
    Ok(RuntimeValue::Scalar(value))
}

fn invalid_runtime(message: &'static str) -> Diagnostic {
    Diagnostic::new(DiagnosticKind::RuntimeInvalidIr, message)
}
