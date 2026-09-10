use std::path::{Path, PathBuf};

use radixdb_catalog::ObjectId;
use radixdb_core::Value;

use super::super::{
    ArtifactId, ArtifactRef, DataArtifactHeader, DataArtifactLayout, DataBloomConfig,
    DataColumnSpec, DataPhysicalCodec, DataValueEncoding, ExactPageBuildLimits, FormatError,
    FormatResult, IndexAcceleratorKind, IndexKeyColumn, IndexPageCodec, OrderedPageBuildLimits,
    MAX_ACCELERATORS_PER_INDEX_ARTIFACT, MAX_INDEX_KEY_BYTES, MAX_ROWS_PER_GROUP,
};

pub const DEFAULT_SORT_RUN_RECORDS: u32 = 65_536;
pub const DEFAULT_SORT_RUN_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_SORT_RUN_RECORDS: u32 = 1_048_576;
pub const MAX_SORT_RUN_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_SORT_RUN_FILES: u32 = 4_096;
pub const DEFAULT_MERGE_FAN_IN: u32 = 16;
pub const MAX_MERGE_FAN_IN: u32 = 32;
pub const MAX_SORT_RUN_DESCRIPTOR_SLOTS: usize = MAX_MERGE_FAN_IN as usize + 1;
pub const MAX_ACCELERATOR_PREPARATION_WORKERS: u32 = 256;
pub const DEFAULT_FANOUT_RESIDENT_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_FANOUT_RESIDENT_BYTES: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_FANOUT_SPILL_BYTES: u64 = 64 * 1024 * 1024 * 1024;
pub const MAX_FANOUT_SPILL_BYTES: u64 = 256 * 1024 * 1024 * 1024;

const MIN_FANOUT_RESIDENT_BYTES: u64 = 4 * 1024 * 1024;
const MIN_FANOUT_SPILL_BYTES: u64 = 2 * 1024 * 1024;
const MIN_MERGE_FAN_IN: u32 = 2;

#[derive(Debug, Clone, PartialEq)]
pub struct SourceRow {
    row_id: u64,
    values: Vec<Value>,
}

impl SourceRow {
    pub fn new(row_id: u64, values: Vec<Value>) -> Self {
        Self { row_id, values }
    }

    pub const fn row_id(&self) -> u64 {
        self.row_id
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub(crate) fn into_parts(self) -> (u64, Vec<Value>) {
        (self.row_id, self.values)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnBuildPolicy {
    value_encoding: DataValueEncoding,
    physical_codec: DataPhysicalCodec,
    bloom: Option<DataBloomConfig>,
}

impl ColumnBuildPolicy {
    pub const fn new(
        value_encoding: DataValueEncoding,
        physical_codec: DataPhysicalCodec,
        bloom: Option<DataBloomConfig>,
    ) -> Self {
        Self {
            value_encoding,
            physical_codec,
            bloom,
        }
    }

    pub const fn value_encoding(self) -> DataValueEncoding {
        self.value_encoding
    }

    pub const fn physical_codec(self) -> DataPhysicalCodec {
        self.physical_codec
    }

    pub const fn bloom(self) -> Option<DataBloomConfig> {
        self.bloom
    }
}

impl Default for ColumnBuildPolicy {
    fn default() -> Self {
        Self::new(DataValueEncoding::Plain, DataPhysicalCodec::Lz4, None)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageBuildLimits {
    Exact(ExactPageBuildLimits),
    Ordered(OrderedPageBuildLimits),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceleratorBuildSpec {
    logical_index_id: ObjectId,
    kind: IndexAcceleratorKind,
    unique: bool,
    constraint_owned: bool,
    definition_sha256: [u8; 32],
    key_columns: Vec<IndexKeyColumn>,
    page_codec: IndexPageCodec,
    page_limits: PageBuildLimits,
}

impl AcceleratorBuildSpec {
    #[allow(clippy::too_many_arguments)]
    pub fn exact(
        logical_index_id: ObjectId,
        unique: bool,
        constraint_owned: bool,
        definition_sha256: [u8; 32],
        key_columns: Vec<IndexKeyColumn>,
        page_codec: IndexPageCodec,
        page_limits: ExactPageBuildLimits,
    ) -> FormatResult<Self> {
        Self::new(
            logical_index_id,
            IndexAcceleratorKind::Exact,
            unique,
            constraint_owned,
            definition_sha256,
            key_columns,
            page_codec,
            PageBuildLimits::Exact(page_limits),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn ordered(
        logical_index_id: ObjectId,
        unique: bool,
        constraint_owned: bool,
        definition_sha256: [u8; 32],
        key_columns: Vec<IndexKeyColumn>,
        page_codec: IndexPageCodec,
        page_limits: OrderedPageBuildLimits,
    ) -> FormatResult<Self> {
        Self::new(
            logical_index_id,
            IndexAcceleratorKind::Ordered,
            unique,
            constraint_owned,
            definition_sha256,
            key_columns,
            page_codec,
            PageBuildLimits::Ordered(page_limits),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        logical_index_id: ObjectId,
        kind: IndexAcceleratorKind,
        unique: bool,
        constraint_owned: bool,
        definition_sha256: [u8; 32],
        key_columns: Vec<IndexKeyColumn>,
        page_codec: IndexPageCodec,
        page_limits: PageBuildLimits,
    ) -> FormatResult<Self> {
        if key_columns.is_empty() {
            return Err(invalid_index("accelerator key descriptor is empty"));
        }
        if key_columns.iter().enumerate().any(|(index, candidate)| {
            key_columns[..index]
                .iter()
                .any(|previous| previous.column_id() == candidate.column_id())
        }) {
            return Err(invalid_index("accelerator key columns are not unique"));
        }
        if !matches!(
            (kind, page_limits),
            (IndexAcceleratorKind::Exact, PageBuildLimits::Exact(_))
                | (IndexAcceleratorKind::Ordered, PageBuildLimits::Ordered(_))
        ) {
            return Err(invalid_index(
                "accelerator kind and page-limit owner differ",
            ));
        }
        Ok(Self {
            logical_index_id,
            kind,
            unique,
            constraint_owned,
            definition_sha256,
            key_columns,
            page_codec,
            page_limits,
        })
    }

    pub const fn logical_index_id(&self) -> ObjectId {
        self.logical_index_id
    }

    pub const fn kind(&self) -> IndexAcceleratorKind {
        self.kind
    }

    pub const fn unique(&self) -> bool {
        self.unique
    }

    pub const fn constraint_owned(&self) -> bool {
        self.constraint_owned
    }

    pub const fn definition_sha256(&self) -> &[u8; 32] {
        &self.definition_sha256
    }

    pub fn key_columns(&self) -> &[IndexKeyColumn] {
        &self.key_columns
    }

    pub const fn page_codec(&self) -> IndexPageCodec {
        self.page_codec
    }

    pub(crate) const fn exact_page_limits(&self) -> Option<ExactPageBuildLimits> {
        match self.page_limits {
            PageBuildLimits::Exact(limits) => Some(limits),
            PageBuildLimits::Ordered(_) => None,
        }
    }

    pub(crate) fn bounded_exact_page_limits(
        &self,
        resident_bytes: u64,
    ) -> FormatResult<ExactPageBuildLimits> {
        let limits = self
            .exact_page_limits()
            .ok_or_else(|| invalid_index("exact page limits belong to another accelerator"))?;
        ExactPageBuildLimits::new(
            limits.max_entries(),
            limits.max_logical_bytes().min(resident_bytes),
        )
    }

    pub(crate) const fn ordered_page_limits(&self) -> Option<OrderedPageBuildLimits> {
        match self.page_limits {
            PageBuildLimits::Ordered(limits) => Some(limits),
            PageBuildLimits::Exact(_) => None,
        }
    }

    pub(crate) fn bounded_ordered_page_limits(
        &self,
        resident_bytes: u64,
    ) -> FormatResult<OrderedPageBuildLimits> {
        let limits = self
            .ordered_page_limits()
            .ok_or_else(|| invalid_index("ordered page limits belong to another accelerator"))?;
        OrderedPageBuildLimits::new(
            limits.max_entries(),
            limits.max_logical_bytes().min(resident_bytes),
        )
    }

    pub(crate) fn maximum_posting_fragment_rows(&self, resident_byte_budget: u64) -> u64 {
        let page_bound = match self.page_limits {
            PageBuildLimits::Exact(limits) => limits.max_logical_bytes(),
            PageBuildLimits::Ordered(limits) => limits.max_logical_bytes(),
        };
        let resident_bound = resident_byte_budget / std::mem::size_of::<u64>() as u64;
        page_bound.min(resident_bound.max(1))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FanoutBuildLimits {
    row_group_rows: u32,
    sort_run_records: u32,
    sort_run_bytes: u64,
    max_sort_run_files: u32,
    merge_fan_in: u32,
    accelerator_preparation_workers: u32,
    resident_byte_budget: u64,
    spill_byte_budget: u64,
}

impl FanoutBuildLimits {
    pub fn new(
        row_group_rows: u32,
        sort_run_records: u32,
        sort_run_bytes: u64,
        max_sort_run_files: u32,
    ) -> FormatResult<Self> {
        if row_group_rows == 0 || row_group_rows > MAX_ROWS_PER_GROUP {
            return Err(limit_data(
                "fanout row-group rows",
                u64::from(row_group_rows),
                u64::from(MAX_ROWS_PER_GROUP),
            ));
        }
        if sort_run_records == 0 || sort_run_records > MAX_SORT_RUN_RECORDS {
            return Err(limit_index(
                "sort-run records",
                u64::from(sort_run_records),
                u64::from(MAX_SORT_RUN_RECORDS),
            ));
        }
        if !(MAX_INDEX_KEY_BYTES..=MAX_SORT_RUN_BYTES).contains(&sort_run_bytes) {
            return Err(limit_index(
                "sort-run bytes",
                sort_run_bytes,
                MAX_SORT_RUN_BYTES,
            ));
        }
        if max_sort_run_files == 0 || max_sort_run_files > MAX_SORT_RUN_FILES {
            return Err(limit_index(
                "sort-run files",
                u64::from(max_sort_run_files),
                u64::from(MAX_SORT_RUN_FILES),
            ));
        }
        Ok(Self {
            row_group_rows,
            sort_run_records,
            sort_run_bytes,
            max_sort_run_files,
            merge_fan_in: DEFAULT_MERGE_FAN_IN,
            accelerator_preparation_workers: 0,
            resident_byte_budget: DEFAULT_FANOUT_RESIDENT_BYTES,
            spill_byte_budget: DEFAULT_FANOUT_SPILL_BYTES,
        })
    }

    /// Configure the CPU budget for independent accelerator preparation.
    /// Zero uses host/cgroup-visible parallelism; a positive value is a strict
    /// upper bound. The actual worker count is also bounded by the number of
    /// accelerators in the publication.
    pub fn with_accelerator_preparation_workers(mut self, workers: u32) -> FormatResult<Self> {
        if workers > MAX_ACCELERATOR_PREPARATION_WORKERS {
            return Err(limit_index(
                "accelerator preparation workers",
                u64::from(workers),
                u64::from(MAX_ACCELERATOR_PREPARATION_WORKERS),
            ));
        }
        self.accelerator_preparation_workers = workers;
        Ok(self)
    }

    /// Raise or lower the record-count boundary of one in-memory sort run
    /// without changing its byte or process-wide resident ceilings.
    ///
    /// This is intentionally independent from `sort_run_bytes`: narrow keys
    /// may keep more records inside the already admitted resident corridor,
    /// while wide keys still spill as soon as either byte bound is reached.
    pub fn with_sort_run_records(mut self, records: u32) -> FormatResult<Self> {
        if records == 0 || records > MAX_SORT_RUN_RECORDS {
            return Err(limit_index(
                "sort-run records",
                u64::from(records),
                u64::from(MAX_SORT_RUN_RECORDS),
            ));
        }
        self.sort_run_records = records;
        Ok(self)
    }

    /// Bound the number of input runs opened by one merge operation. A build
    /// with more runs is reduced through deterministic intermediate passes.
    /// Runtime may only choose a value within the implementation corridor.
    pub fn with_merge_fan_in(mut self, fan_in: u32) -> FormatResult<Self> {
        if !(MIN_MERGE_FAN_IN..=MAX_MERGE_FAN_IN).contains(&fan_in) {
            return Err(limit_index(
                "sort-run merge fan-in",
                u64::from(fan_in),
                u64::from(MAX_MERGE_FAN_IN),
            ));
        }
        self.merge_fan_in = fan_in;
        Ok(self)
    }

    /// Set the complete resident and temporary-spill corridors owned by one
    /// artifact build. Both values are lower-only runtime choices bounded by
    /// named format-independent implementation ceilings.
    pub fn with_resource_budgets(
        mut self,
        resident_bytes: u64,
        spill_bytes: u64,
    ) -> FormatResult<Self> {
        if !(MIN_FANOUT_RESIDENT_BYTES..=MAX_FANOUT_RESIDENT_BYTES).contains(&resident_bytes) {
            return Err(limit_index(
                "fanout resident bytes",
                resident_bytes,
                MAX_FANOUT_RESIDENT_BYTES,
            ));
        }
        if !(MIN_FANOUT_SPILL_BYTES..=MAX_FANOUT_SPILL_BYTES).contains(&spill_bytes) {
            return Err(limit_index(
                "fanout spill bytes",
                spill_bytes,
                MAX_FANOUT_SPILL_BYTES,
            ));
        }
        self.resident_byte_budget = resident_bytes;
        self.spill_byte_budget = spill_bytes;
        Ok(self)
    }

    pub const fn row_group_rows(self) -> u32 {
        self.row_group_rows
    }

    pub const fn sort_run_records(self) -> u32 {
        self.sort_run_records
    }

    pub const fn sort_run_bytes(self) -> u64 {
        self.sort_run_bytes
    }

    pub const fn max_sort_run_files(self) -> u32 {
        self.max_sort_run_files
    }

    pub const fn merge_fan_in(self) -> u32 {
        self.merge_fan_in
    }

    /// Zero means automatic host/cgroup-visible parallelism.
    pub const fn accelerator_preparation_workers(self) -> u32 {
        self.accelerator_preparation_workers
    }

    pub const fn resident_byte_budget(self) -> u64 {
        self.resident_byte_budget
    }

    pub const fn spill_byte_budget(self) -> u64 {
        self.spill_byte_budget
    }

    /// Derive the row-group shape before any row buffer allocation. One
    /// eighth of the build corridor owns fixed row/value slots; another
    /// eighth remains available for variable payload retained by the group.
    /// The remaining corridor is reserved for durable metadata, block codecs,
    /// index builders and their transient page preparation.
    pub fn planned_row_group_rows(self, row_count: u64, column_count: u32) -> FormatResult<u32> {
        let vector_headers = u64::from(column_count)
            .checked_mul(std::mem::size_of::<Vec<Value>>() as u64)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Vec<Vec<Value>>>() as u64))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Vec<u64>>() as u64))
            .ok_or_else(|| invalid_data("row-group vector metadata overflows"))?;
        let per_row = u64::from(column_count)
            .checked_mul(std::mem::size_of::<Value>() as u64)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<u64>() as u64))
            .ok_or_else(|| invalid_data("row-group fixed row bytes overflow"))?;
        let fixed_budget = self.resident_byte_budget / 8;
        let available = fixed_budget.checked_sub(vector_headers).ok_or_else(|| {
            limit_data(
                "row-group fixed resident bytes",
                vector_headers,
                fixed_budget,
            )
        })?;
        let budget_rows = available / per_row;
        if budget_rows == 0 {
            return Err(limit_data(
                "row-group fixed resident bytes",
                vector_headers.saturating_add(per_row),
                fixed_budget,
            ));
        }
        // Empty artifacts still need a non-zero divisor while validating that
        // their canonical row-group count is zero.
        let actual_rows = row_count
            .min(u64::from(self.row_group_rows))
            .min(budget_rows)
            .max(1);
        u32::try_from(actual_rows)
            .map_err(|_| invalid_data("planned row-group rows do not fit u32"))
    }

    pub(crate) const fn row_group_variable_byte_budget(self) -> u64 {
        self.resident_byte_budget / 8
    }
}

impl Default for FanoutBuildLimits {
    fn default() -> Self {
        Self {
            row_group_rows: MAX_ROWS_PER_GROUP,
            sort_run_records: DEFAULT_SORT_RUN_RECORDS,
            sort_run_bytes: DEFAULT_SORT_RUN_BYTES,
            max_sort_run_files: MAX_SORT_RUN_FILES,
            merge_fan_in: DEFAULT_MERGE_FAN_IN,
            accelerator_preparation_workers: 0,
            resident_byte_budget: DEFAULT_FANOUT_RESIDENT_BYTES,
            spill_byte_budget: DEFAULT_FANOUT_SPILL_BYTES,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactPairBuildRequest {
    data_header: DataArtifactHeader,
    columns: Vec<DataColumnSpec>,
    column_policies: Vec<ColumnBuildPolicy>,
    row_id_codec: DataPhysicalCodec,
    index_artifact_id: ArtifactId,
    accelerators: Vec<AcceleratorBuildSpec>,
    limits: FanoutBuildLimits,
    staging_directory: PathBuf,
}

/// Bounded request for a canonical DATA artifact when the table has no
/// physical accelerators to publish with this segment.
///
/// DATA is authoritative; INDEX is optional. Keeping this request separate
/// from [`ArtifactPairBuildRequest`] prevents callers from inventing a fake
/// logical index merely to satisfy the physical writer.
#[derive(Debug, Clone)]
pub struct DataArtifactBuildRequest {
    data_header: DataArtifactHeader,
    columns: Vec<DataColumnSpec>,
    column_policies: Vec<ColumnBuildPolicy>,
    row_id_codec: DataPhysicalCodec,
    limits: FanoutBuildLimits,
}

impl DataArtifactBuildRequest {
    pub fn new(
        data_header: DataArtifactHeader,
        columns: Vec<DataColumnSpec>,
        column_policies: Vec<ColumnBuildPolicy>,
        row_id_codec: DataPhysicalCodec,
        limits: FanoutBuildLimits,
    ) -> FormatResult<Self> {
        validate_data_build_request(&data_header, &columns, &column_policies, limits)?;
        Ok(Self {
            data_header,
            columns,
            column_policies,
            row_id_codec,
            limits,
        })
    }

    pub const fn data_header(&self) -> DataArtifactHeader {
        self.data_header
    }

    pub fn columns(&self) -> &[DataColumnSpec] {
        &self.columns
    }

    pub fn column_policies(&self) -> &[ColumnBuildPolicy] {
        &self.column_policies
    }

    pub const fn row_id_codec(&self) -> DataPhysicalCodec {
        self.row_id_codec
    }

    pub const fn limits(&self) -> FanoutBuildLimits {
        self.limits
    }
}

impl ArtifactPairBuildRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        data_header: DataArtifactHeader,
        columns: Vec<DataColumnSpec>,
        column_policies: Vec<ColumnBuildPolicy>,
        row_id_codec: DataPhysicalCodec,
        index_artifact_id: ArtifactId,
        mut accelerators: Vec<AcceleratorBuildSpec>,
        limits: FanoutBuildLimits,
        staging_directory: impl Into<PathBuf>,
    ) -> FormatResult<Self> {
        validate_data_build_request(&data_header, &columns, &column_policies, limits)?;
        if data_header.segment_kind() != super::super::SegmentKind::Rows {
            return Err(invalid_data(
                "artifact pair build accepts only ordinary row segments",
            ));
        }
        if accelerators.is_empty()
            || accelerators.len() as u64 > u64::from(MAX_ACCELERATORS_PER_INDEX_ARTIFACT)
        {
            return Err(limit_index(
                "accelerator count",
                accelerators.len() as u64,
                u64::from(MAX_ACCELERATORS_PER_INDEX_ARTIFACT),
            ));
        }
        let planned_group_rows = limits.planned_row_group_rows(
            data_header.row_count(),
            u32::try_from(columns.len())
                .map_err(|_| invalid_data("artifact column count does not fit u32"))?,
        )?;
        let expected_groups = data_header
            .row_count()
            .div_ceil(u64::from(planned_group_rows));
        if expected_groups != u64::from(data_header.row_group_count()) {
            return Err(invalid_data(
                "data header row-group count differs from fanout limit",
            ));
        }
        accelerators.sort_by_key(AcceleratorBuildSpec::logical_index_id);
        if accelerators
            .windows(2)
            .any(|pair| pair[0].logical_index_id() == pair[1].logical_index_id())
        {
            return Err(invalid_index("logical accelerator IDs are not unique"));
        }
        for accelerator in &accelerators {
            for key in accelerator.key_columns() {
                let column = columns
                    .iter()
                    .find(|column| column.column_id() == key.column_id())
                    .ok_or_else(|| invalid_index("accelerator key column is absent"))?;
                if column.data_type().logical_type() != key.logical_type() {
                    return Err(invalid_index(
                        "accelerator key type differs from source column",
                    ));
                }
            }
        }
        Ok(Self {
            data_header,
            columns,
            column_policies,
            row_id_codec,
            index_artifact_id,
            accelerators,
            limits,
            staging_directory: staging_directory.into(),
        })
    }

    pub const fn data_header(&self) -> DataArtifactHeader {
        self.data_header
    }

    pub fn columns(&self) -> &[DataColumnSpec] {
        &self.columns
    }

    pub fn column_policies(&self) -> &[ColumnBuildPolicy] {
        &self.column_policies
    }

    pub const fn row_id_codec(&self) -> DataPhysicalCodec {
        self.row_id_codec
    }

    pub const fn index_artifact_id(&self) -> ArtifactId {
        self.index_artifact_id
    }

    pub fn accelerators(&self) -> &[AcceleratorBuildSpec] {
        &self.accelerators
    }

    pub const fn limits(&self) -> FanoutBuildLimits {
        self.limits
    }

    pub fn staging_directory(&self) -> &Path {
        &self.staging_directory
    }
}

fn validate_data_build_request(
    data_header: &DataArtifactHeader,
    columns: &[DataColumnSpec],
    column_policies: &[ColumnBuildPolicy],
    limits: FanoutBuildLimits,
) -> FormatResult<()> {
    if data_header.row_count() == 0 {
        return Err(invalid_data("artifact build source cannot be empty"));
    }
    if columns.len() != data_header.column_count() as usize
        || columns.len() != column_policies.len()
    {
        return Err(invalid_data(
            "artifact columns/policies differ from data header",
        ));
    }
    let planned_group_rows = limits.planned_row_group_rows(
        data_header.row_count(),
        u32::try_from(columns.len())
            .map_err(|_| invalid_data("artifact column count does not fit u32"))?,
    )?;
    let expected_groups = data_header
        .row_count()
        .div_ceil(u64::from(planned_group_rows));
    if expected_groups != u64::from(data_header.row_group_count()) {
        return Err(invalid_data(
            "data header row-group count differs from build limit",
        ));
    }
    Ok(())
}

#[derive(Debug)]
pub struct BuiltArtifactPair {
    data_bytes: Vec<u8>,
    data_reference: ArtifactRef,
    data_layout: DataArtifactLayout,
    index_bytes: Vec<u8>,
    index_reference: ArtifactRef,
}

#[derive(Debug)]
pub struct BuiltDataArtifact {
    data_bytes: Vec<u8>,
    data_reference: ArtifactRef,
    data_layout: DataArtifactLayout,
}

impl BuiltDataArtifact {
    pub(crate) fn new(
        data_bytes: Vec<u8>,
        data_reference: ArtifactRef,
        data_layout: DataArtifactLayout,
    ) -> Self {
        Self {
            data_bytes,
            data_reference,
            data_layout,
        }
    }

    pub fn data_bytes(&self) -> &[u8] {
        &self.data_bytes
    }

    pub const fn data_reference(&self) -> ArtifactRef {
        self.data_reference
    }

    pub const fn data_layout(&self) -> &DataArtifactLayout {
        &self.data_layout
    }
}

#[derive(Debug)]
pub struct WrittenDataArtifact {
    data_reference: ArtifactRef,
    data_layout: DataArtifactLayout,
}

impl WrittenDataArtifact {
    pub(crate) const fn new(data_reference: ArtifactRef, data_layout: DataArtifactLayout) -> Self {
        Self {
            data_reference,
            data_layout,
        }
    }

    pub const fn data_reference(&self) -> ArtifactRef {
        self.data_reference
    }

    pub const fn data_layout(&self) -> &DataArtifactLayout {
        &self.data_layout
    }

    pub(crate) fn into_parts(self) -> (ArtifactRef, DataArtifactLayout) {
        (self.data_reference, self.data_layout)
    }
}

#[derive(Debug)]
pub struct WrittenArtifactPair {
    data_reference: ArtifactRef,
    data_layout: DataArtifactLayout,
    index_reference: ArtifactRef,
}

impl WrittenArtifactPair {
    pub(crate) const fn new(
        data_reference: ArtifactRef,
        data_layout: DataArtifactLayout,
        index_reference: ArtifactRef,
    ) -> Self {
        Self {
            data_reference,
            data_layout,
            index_reference,
        }
    }

    pub const fn data_reference(&self) -> ArtifactRef {
        self.data_reference
    }

    pub const fn data_layout(&self) -> &DataArtifactLayout {
        &self.data_layout
    }

    pub const fn index_reference(&self) -> ArtifactRef {
        self.index_reference
    }

    pub(crate) fn into_parts(self) -> (ArtifactRef, DataArtifactLayout, ArtifactRef) {
        (self.data_reference, self.data_layout, self.index_reference)
    }
}

impl BuiltArtifactPair {
    pub(crate) fn new(
        data_bytes: Vec<u8>,
        data_reference: ArtifactRef,
        data_layout: DataArtifactLayout,
        index_bytes: Vec<u8>,
        index_reference: ArtifactRef,
    ) -> Self {
        Self {
            data_bytes,
            data_reference,
            data_layout,
            index_bytes,
            index_reference,
        }
    }

    pub fn data_bytes(&self) -> &[u8] {
        &self.data_bytes
    }

    pub const fn data_reference(&self) -> ArtifactRef {
        self.data_reference
    }

    pub const fn data_layout(&self) -> &DataArtifactLayout {
        &self.data_layout
    }

    pub fn index_bytes(&self) -> &[u8] {
        &self.index_bytes
    }

    pub const fn index_reference(&self) -> ArtifactRef {
        self.index_reference
    }
}

pub(crate) const fn invalid_data(detail: &'static str) -> FormatError {
    FormatError::InvalidDataArtifact { detail }
}

pub(crate) const fn invalid_index(detail: &'static str) -> FormatError {
    FormatError::InvalidIndexArtifact { detail }
}

pub(crate) const fn limit_data(field: &'static str, actual: u64, limit: u64) -> FormatError {
    FormatError::DataArtifactLimitExceeded {
        field,
        actual,
        limit,
    }
}

pub(crate) const fn limit_index(field: &'static str, actual: u64, limit: u64) -> FormatError {
    FormatError::IndexArtifactLimitExceeded {
        field,
        actual,
        limit,
    }
}
