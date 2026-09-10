use radixdb_catalog::ObjectId;
use radixdb_core::{DataType, Value};

use super::frame::{scalar, Frame};
use super::invalid_runtime;
use crate::host::{AuditEvent, OutboxMessage, RuntimeHost};
use crate::{BudgetOwner, Diagnostic, DiagnosticKind, ProceduralResult, SlotId};

pub(super) fn append_audit<H: RuntimeHost>(
    host: &mut H,
    frame: &Frame,
    budget: &BudgetOwner,
    object_id: ObjectId,
    command_fingerprint: SlotId,
    metadata: SlotId,
) -> ProceduralResult<()> {
    budget.check_boundary()?;
    budget.charge_sql_statement()?;
    let fingerprint = match scalar(frame, command_fingerprint)? {
        Value::Null(_) => return Err(null_error("audit command fingerprint cannot be NULL")),
        value if value.data_type() == DataType::Bytes => value
            .as_bytes_value()
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| {
                Diagnostic::new(
                    DiagnosticKind::RuntimeInvalidIr,
                    "audit command fingerprint must contain exactly 32 bytes",
                )
            })?,
        _ => return Err(invalid_runtime("verified audit fingerprint changed type")),
    };
    let metadata = required_json(frame, metadata, "audit metadata cannot be NULL")?;
    host.append_audit(AuditEvent {
        object_id,
        command_fingerprint: fingerprint,
        metadata,
    })?;
    budget.check_boundary()
}

pub(super) fn append_outbox<H: RuntimeHost>(
    host: &mut H,
    frame: &Frame,
    budget: &BudgetOwner,
    idempotency_key: SlotId,
    schema_version: SlotId,
    payload: SlotId,
) -> ProceduralResult<()> {
    budget.check_boundary()?;
    budget.charge_sql_statement()?;
    let idempotency_key = match scalar(frame, idempotency_key)? {
        Value::Text(value) => value.to_string(),
        Value::Null(_) => return Err(null_error("outbox idempotency key cannot be NULL")),
        _ => return Err(invalid_runtime("verified outbox key changed type")),
    };
    let schema_version = match scalar(frame, schema_version)? {
        Value::Integer(value) => u32::try_from(*value).map_err(|_| {
            Diagnostic::new(
                DiagnosticKind::RuntimeInvalidIr,
                "outbox schema version is outside u32",
            )
        })?,
        Value::Null(_) => return Err(null_error("outbox schema version cannot be NULL")),
        _ => return Err(invalid_runtime("verified outbox version changed type")),
    };
    let payload = required_json(frame, payload, "outbox payload cannot be NULL")?;
    host.append_outbox(OutboxMessage {
        idempotency_key,
        schema_version,
        payload,
    })?;
    budget.check_boundary()
}

fn required_json(
    frame: &Frame,
    slot: SlotId,
    null_message: &'static str,
) -> ProceduralResult<Value> {
    match scalar(frame, slot)? {
        Value::Null(_) => Err(null_error(null_message)),
        value if value.data_type() == DataType::Json => Ok(value.clone()),
        _ => Err(invalid_runtime("verified application JSON changed type")),
    }
}

fn null_error(message: &'static str) -> Diagnostic {
    Diagnostic::new(DiagnosticKind::RuntimeNullNotAllowed, message)
}
