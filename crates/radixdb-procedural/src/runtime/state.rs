use std::collections::BTreeMap;

use super::cursor::CursorState;
use crate::{CursorId, RuntimeType, RuntimeValue};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqlStatus {
    pub row_count: u64,
    pub found: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionOutcome {
    pub return_value: Option<RuntimeValue>,
    pub output_values: Vec<RuntimeValue>,
    pub result_rows: u64,
    pub sql_status: SqlStatus,
}

pub(super) struct ExecutionState<'a> {
    pub(super) sql_status: SqlStatus,
    pub(super) cursors: BTreeMap<CursorId, CursorState>,
    pub(super) result_rows: u64,
    pub(super) result_columns: &'a [RuntimeType],
}

impl<'a> ExecutionState<'a> {
    pub(super) fn new(result_columns: &'a [RuntimeType]) -> Self {
        Self {
            sql_status: SqlStatus::default(),
            cursors: BTreeMap::new(),
            result_rows: 0,
            result_columns,
        }
    }
}
