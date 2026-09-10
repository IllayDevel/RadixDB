use radixdb_core::{ParamVec, Row, Value};
use radixdb_procedural::{Diagnostic, DiagnosticKind, ProceduralResult, RuntimeValue};

pub(super) fn scalar_parameters(values: &[RuntimeValue]) -> ProceduralResult<ParamVec> {
    values.iter().map(scalar_value).collect()
}

pub(super) fn scalar_value(value: &RuntimeValue) -> ProceduralResult<Value> {
    match value {
        RuntimeValue::Scalar(value) => Ok(value.clone()),
        RuntimeValue::Record(_)
        | RuntimeValue::NullRecord
        | RuntimeValue::Collection(_)
        | RuntimeValue::SqlIdentifier(_) => Err(Diagnostic::new(
            DiagnosticKind::RuntimeInvalidArgument,
            "SQL parameters must be scalar values",
        )),
    }
}

pub(super) fn runtime_row(row: Row) -> Vec<RuntimeValue> {
    row.into_values()
        .into_iter()
        .map(RuntimeValue::scalar)
        .collect()
}
