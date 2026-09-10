use std::collections::BTreeMap;

use super::frame::Frame;
use crate::host::{CursorToken, RuntimeHost};
use crate::{CursorId, ProceduralResult, RuntimeType, RuntimeValue, SlotId};

#[derive(Debug, Clone, Copy)]
pub(super) struct CursorState {
    pub(super) token: CursorToken,
    pub(super) found: Option<bool>,
    pub(super) row_count: u64,
}

pub(super) fn assign_cursor_row(
    frame: &mut Frame,
    destinations: &[SlotId],
    row: Vec<RuntimeValue>,
) -> ProceduralResult<()> {
    if destinations.len() == 1
        && matches!(
            frame.slot_type(destinations[0])?,
            RuntimeType::Record {
                nullable: false,
                ..
            }
        )
    {
        let values = row
            .into_iter()
            .map(|value| match value {
                RuntimeValue::Scalar(value) => Ok(value),
                _ => Err(super::invalid_runtime(
                    "cursor row contains a non-scalar field",
                )),
            })
            .collect::<ProceduralResult<Vec<_>>>()?;
        return frame.assign_many(
            destinations,
            vec![RuntimeValue::Record(values.into_iter().map(Some).collect())],
            None,
        );
    }
    if destinations.len() != row.len() {
        return Err(super::invalid_runtime(
            "cursor row width differs from FETCH destinations",
        ));
    }
    frame.assign_many(destinations, row, None)
}

pub(super) fn close_all_cursors<H: RuntimeHost>(
    host: &mut H,
    cursors: &mut BTreeMap<CursorId, CursorState>,
) {
    for (_, state) in std::mem::take(cursors) {
        let _ = host.close_cursor(state.token);
    }
}

pub(super) fn close_all_cursors_checked<H: RuntimeHost>(
    host: &mut H,
    cursors: &mut BTreeMap<CursorId, CursorState>,
) -> ProceduralResult<()> {
    for (_, state) in std::mem::take(cursors) {
        host.close_cursor(state.token)?;
    }
    Ok(())
}
