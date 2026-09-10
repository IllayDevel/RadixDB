use std::collections::{BTreeMap, BTreeSet};

use radixdb_core::Value;

use super::cursor::CursorState;
use super::frame::Frame;
use super::state::SqlStatus;
use crate::host::RuntimeHost;
use crate::ir::{BlockId, CursorId, ExceptionRoute};
use crate::{Diagnostic, DiagnosticKind, ProceduralResult, RuntimeValue, SavepointToken};

#[derive(Debug)]
pub(super) struct ExceptionFrame {
    pub(super) savepoint: SavepointToken,
    pub(super) routes: Vec<ExceptionRoute>,
    pub(super) cursors_at_entry: BTreeSet<CursorId>,
    pub(super) sql_status_at_entry: SqlStatus,
    pub(super) handling: Option<Diagnostic>,
}

pub(super) fn leave_exception_frame<H: RuntimeHost>(
    host: &mut H,
    frames: &mut Vec<ExceptionFrame>,
) -> ProceduralResult<()> {
    let frame = frames.pop().ok_or_else(stack_underflow)?;
    host.release_savepoint(frame.savepoint)
}

pub(super) fn release_all_exception_frames<H: RuntimeHost>(
    host: &mut H,
    frames: &mut Vec<ExceptionFrame>,
) -> ProceduralResult<()> {
    while !frames.is_empty() {
        leave_exception_frame(host, frames)?;
    }
    Ok(())
}

pub(super) fn abort_all_exception_frames<H: RuntimeHost>(
    host: &mut H,
    frames: &mut Vec<ExceptionFrame>,
    original: &Diagnostic,
) -> ProceduralResult<()> {
    while let Some(frame) = frames.pop() {
        if frame.handling.is_none() {
            host.rollback_savepoint(frame.savepoint)
                .map_err(|error| transaction_cleanup_error(error, original))?;
        }
        host.release_savepoint(frame.savepoint)
            .map_err(|error| transaction_cleanup_error(error, original))?;
    }
    Ok(())
}

pub(super) fn route_exception<H: RuntimeHost>(
    mut error: Diagnostic,
    frame: &mut Frame,
    host: &mut H,
    cursors: &mut BTreeMap<CursorId, CursorState>,
    sql_status: &mut SqlStatus,
    frames: &mut Vec<ExceptionFrame>,
) -> Result<BlockId, Diagnostic> {
    while let Some(mut region) = frames.pop() {
        if region.handling.is_some() {
            if let Err(release) = host.release_savepoint(region.savepoint) {
                return Err(transaction_cleanup_error(release, &error));
            }
            continue;
        }
        if let Err(rollback) = host.rollback_savepoint(region.savepoint) {
            return Err(transaction_cleanup_error(rollback, &error));
        }
        let opened_inside = cursors
            .keys()
            .filter(|cursor| !region.cursors_at_entry.contains(cursor))
            .copied()
            .collect::<Vec<_>>();
        for cursor in opened_inside {
            if let Some(state) = cursors.remove(&cursor) {
                if let Err(close) = host.close_cursor(state.token) {
                    return Err(transaction_cleanup_error(close, &error));
                }
            }
        }
        *sql_status = region.sql_status_at_entry;
        if let Some(route) = region
            .routes
            .iter()
            .find(|route| route.kinds.is_empty() || route.kinds.contains(&error.kind()))
            .cloned()
        {
            if let Some(destination) = route.error_slot {
                if let Err(alias_error) =
                    frame.assign(destination, diagnostic_record(&error), error.primary_span())
                {
                    return Err(transaction_cleanup_error(alias_error, &error));
                }
            }
            region.handling = Some(error);
            frames.push(region);
            return Ok(route.handler);
        }
        if let Err(release) = host.release_savepoint(region.savepoint) {
            return Err(transaction_cleanup_error(release, &error));
        }
        error = error.with_detail("exception_region", "unhandled");
    }
    Err(error)
}

fn diagnostic_record(error: &Diagnostic) -> RuntimeValue {
    RuntimeValue::Record(vec![
        Some(Value::Text(error.kind().as_str().into())),
        Some(Value::Text(error.category().as_str().into())),
        Some(Value::Text(error.message().into())),
        Some(Value::Boolean(error.retryable())),
    ])
}

fn transaction_cleanup_error(cleanup: Diagnostic, original: &Diagnostic) -> Diagnostic {
    Diagnostic::new(
        DiagnosticKind::RuntimeInvalidIr,
        "exception savepoint/cursor cleanup failed; transaction cannot continue",
    )
    .with_cause(original.kind())
    .with_detail("cleanup_kind", cleanup.kind().as_str())
}

fn stack_underflow() -> Diagnostic {
    Diagnostic::new(
        DiagnosticKind::RuntimeInvalidIr,
        "exception-region stack underflow",
    )
}
