use radixdb_core::DataType;

use super::{invalid, scalar, slot};
use crate::{ProceduralResult, Program, RuntimeType, SlotId, SourceSpan};

pub(super) fn verify_quote_sql_identifier(
    program: &Program,
    destination: SlotId,
    source: SlotId,
    span: Option<&SourceSpan>,
) -> ProceduralResult<()> {
    scalar(program, source, DataType::Text, span)?;
    if !matches!(
        slot(program, destination, span)?.runtime_type(),
        RuntimeType::SqlIdentifier
    ) {
        return Err(invalid(span, "quoted identifier destination is not typed"));
    }
    Ok(())
}

pub(super) fn verify_concatenate_sql_text(
    program: &Program,
    destination: SlotId,
    left: SlotId,
    right: SlotId,
    span: Option<&SourceSpan>,
) -> ProceduralResult<()> {
    let (_, destination_nullable) = scalar(program, destination, DataType::Text, span)?;
    let mut nullable = false;
    for operand in [left, right] {
        match slot(program, operand, span)?.runtime_type() {
            RuntimeType::Scalar {
                data_type,
                nullable: operand_nullable,
            } if data_type.logical_type() == DataType::Text => nullable |= *operand_nullable,
            RuntimeType::SqlIdentifier => {}
            _ => {
                return Err(invalid(
                    span,
                    "dynamic SQL concatenation operand is not TEXT or identifier",
                ));
            }
        }
    }
    if nullable && !destination_nullable {
        return Err(invalid(
            span,
            "nullable dynamic SQL concatenation requires nullable TEXT",
        ));
    }
    Ok(())
}
