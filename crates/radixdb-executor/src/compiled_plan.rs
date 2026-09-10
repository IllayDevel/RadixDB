//! Cached execution descriptors shared by statement dispatch and fast paths.

use std::sync::Arc;

use radixdb_core::{CompactArc, DataType, Schema, SmartString, Value};

/// How a compiled statement obtains its primary-key value.
#[derive(Debug, Clone)]
pub enum PkValueSource {
    Parameter(usize),
    Literal(i64),
    NamedParameter(SmartString),
}

/// Pre-bound state for `SELECT * ... WHERE pk = value`.
#[derive(Debug, Clone)]
pub struct CompiledPkLookup {
    pub table_name: SmartString,
    pub schema: CompactArc<Schema>,
    pub column_names: CompactArc<Vec<String>>,
    pub pk_value_source: PkValueSource,
    pub cached_epoch: u64,
}

/// One pre-bound UPDATE assignment.
#[derive(Debug, Clone)]
pub struct CompiledUpdateColumn {
    pub column_idx: usize,
    pub column_type: DataType,
    pub value_source: UpdateValueSource,
}

/// How a compiled UPDATE obtains an assignment value.
#[derive(Debug, Clone)]
pub enum UpdateValueSource {
    Literal(Value),
    Parameter(usize),
    NamedParameter(SmartString),
}

/// Pre-bound state for a primary-key UPDATE.
#[derive(Debug, Clone)]
pub struct CompiledPkUpdate {
    pub table_name: SmartString,
    pub schema: CompactArc<Schema>,
    pub pk_column_name: SmartString,
    pub pk_value_source: PkValueSource,
    pub updates: Vec<CompiledUpdateColumn>,
    pub cached_epoch: u64,
}

/// Pre-bound state for a primary-key DELETE.
#[derive(Debug, Clone)]
pub struct CompiledPkDelete {
    pub table_name: SmartString,
    pub schema: CompactArc<Schema>,
    pub pk_column_name: SmartString,
    pub pk_value_source: PkValueSource,
    pub cached_epoch: u64,
}

/// Schema-derived INSERT state reused by cached statements.
#[derive(Debug, Clone)]
pub struct CompiledInsert {
    pub table_name: SmartString,
    pub column_indices: Arc<Vec<usize>>,
    pub column_types: Arc<Vec<DataType>>,
    pub column_vector_dims: Arc<Vec<u16>>,
    pub column_names: Arc<Vec<SmartString>>,
    pub all_column_types: Arc<Vec<DataType>>,
    pub default_row_template: Arc<Vec<Value>>,
    pub cached_epoch: u64,
}

/// Pre-bound state for `COUNT(DISTINCT column)`.
#[derive(Debug, Clone)]
pub struct CompiledCountDistinct {
    pub table_name: SmartString,
    pub column_name: SmartString,
    pub result_column_name: String,
    pub cached_epoch: u64,
}

/// Pre-bound state for `COUNT(*)`.
#[derive(Debug, Clone)]
pub struct CompiledCountStar {
    pub table_name: SmartString,
    pub result_column_name: String,
    pub cached_epoch: u64,
}

/// Cached execution state selected by statement dispatch.
#[derive(Debug, Clone, Default)]
pub enum CompiledExecution {
    #[default]
    Unknown,
    NotOptimizable(u64),
    PkLookup(CompiledPkLookup),
    PkUpdate(CompiledPkUpdate),
    PkDelete(CompiledPkDelete),
    Insert(CompiledInsert),
    CountDistinct(CompiledCountDistinct),
    CountStar(CompiledCountStar),
}
