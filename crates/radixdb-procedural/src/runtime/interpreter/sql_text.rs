use radixdb_catalog::CatalogName;
use radixdb_core::{DataType, Value};

use super::super::frame::{scalar, Frame};
use super::super::invalid_runtime;
use crate::{Diagnostic, DiagnosticKind, ProceduralResult, RuntimeValue, SlotId};

pub(super) fn quote_sql_identifier(
    frame: &mut Frame,
    destination: SlotId,
    source: SlotId,
) -> ProceduralResult<()> {
    let source = match scalar(frame, source)? {
        Value::Text(source) => source,
        Value::Null(_) => {
            return Err(Diagnostic::new(
                DiagnosticKind::RuntimeNullNotAllowed,
                "SQL_IDENTIFIER argument cannot be NULL",
            ));
        }
        _ => return Err(invalid_runtime("SQL_IDENTIFIER argument is not TEXT")),
    };
    let identifier = CatalogName::new(source.as_str()).map_err(|_| {
        Diagnostic::new(
            DiagnosticKind::RuntimeInvalidArgument,
            "SQL_IDENTIFIER argument is not an admitted catalog identifier",
        )
    })?;
    let quoted = format!("\"{}\"", identifier.display().as_str().replace('"', "\"\""));
    frame.assign(destination, RuntimeValue::SqlIdentifier(quoted), None)
}

pub(super) fn concatenate_sql_text(
    frame: &mut Frame,
    destination: SlotId,
    left: SlotId,
    right: SlotId,
) -> ProceduralResult<()> {
    let value = match (
        sql_text_fragment(frame.read(left)?)?,
        sql_text_fragment(frame.read(right)?)?,
    ) {
        (Some(left), Some(right)) => Value::Text(format!("{left}{right}").into()),
        _ => Value::null(DataType::Text),
    };
    frame.assign(destination, RuntimeValue::Scalar(value), None)
}

fn sql_text_fragment(value: &RuntimeValue) -> ProceduralResult<Option<&str>> {
    match value {
        RuntimeValue::Scalar(Value::Text(value)) => Ok(Some(value)),
        RuntimeValue::Scalar(Value::Null(_)) => Ok(None),
        RuntimeValue::SqlIdentifier(value) => Ok(Some(value)),
        _ => Err(invalid_runtime(
            "dynamic SQL concatenation operand changed type",
        )),
    }
}
