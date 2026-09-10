use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use radixdb_core::{Error, Result, Value};

use super::{
    decode_runtime_row_id, encode_runtime_row_id, lookup_exact_index_from_source,
    open_data_artifact_metadata, open_data_artifact_metadata_with_limits,
    open_index_artifact_metadata, open_index_artifact_metadata_with_limits,
    read_data_row_ids_from_source, read_data_typed_column_from_source,
    scan_ordered_index_from_source, scan_ordered_non_null_index_from_source, ArtifactFile,
    ArtifactRef, ArtifactSource, DataArtifactLayout, DataOpenLimits, DataOpenMetrics, FormatError,
    FormatResult, IndexAccelerator, IndexAcceleratorKind, IndexArtifactLayout, IndexOpenLimits,
    IndexOpenMetrics, IndexScanDirection, OrderedIndexBound, OrderedIndexKey,
    ReachabilityAllowance, MAX_DATA_OPEN_METADATA_BYTES, MAX_INDEX_OPEN_METADATA_BYTES,
};
use crate::volume::column::ColumnData;

/// Runtime reader for one immutable authoritative DATA artifact.
///
/// The open path retains only the checked block directory and an immutable
/// file identity. Physical descriptors come from the bounded process-wide
/// artifact pool. Values and row IDs are decoded one bounded row group at a
/// time by callers; this owner never reconstructs a complete cold segment in
/// memory.
pub struct ArtifactDataSource {
    source: Arc<ArtifactFile>,
    layout: Arc<DataArtifactLayout>,
    row_ids: Mutex<Option<Arc<ArtifactRowIdBatch>>>,
}

/// Runtime reader for the optional INDEX artifact bound to one immutable DATA
/// artifact.
///
/// Opening validates the complete index directory and its DATA identity while
/// retaining only bounded metadata and an immutable file identity. Query
/// methods lease a descriptor from the bounded process-wide artifact pool and
/// decode only the pages selected by the requested logical key.
pub struct ArtifactIndexSource {
    source: Arc<ArtifactFile>,
    layout: Arc<IndexArtifactLayout>,
    data: Arc<ArtifactDataSource>,
}

impl ArtifactSource for ArtifactDataSource {
    fn byte_length(&self) -> FormatResult<u64> {
        self.source.byte_length()
    }

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> FormatResult<()> {
        self.source.read_exact_at(offset, destination)
    }
}

/// Aggregate owner for metadata retained by all artifact readers opened for
/// one database runtime.  Per-file limits are derived from the remaining
/// allowance, never reset to their standalone defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenMetadataBudget {
    accounted_bytes: u64,
    max_accounted_bytes: u64,
}

impl OpenMetadataBudget {
    pub const fn from_allowance(allowance: ReachabilityAllowance) -> Self {
        Self {
            accounted_bytes: allowance.accounted_bytes(),
            max_accounted_bytes: allowance.max_accounted_bytes(),
        }
    }

    pub const fn accounted_bytes(self) -> u64 {
        self.accounted_bytes
    }

    pub const fn max_accounted_bytes(self) -> u64 {
        self.max_accounted_bytes
    }

    pub const fn remaining_bytes(self) -> u64 {
        self.max_accounted_bytes - self.accounted_bytes
    }

    fn data_limits(self) -> FormatResult<DataOpenLimits> {
        let remaining = self.remaining_bytes();
        if remaining == 0 {
            return Err(self.exceeded(1));
        }
        DataOpenLimits::new(remaining.min(MAX_DATA_OPEN_METADATA_BYTES))
    }

    fn index_limits(self) -> FormatResult<IndexOpenLimits> {
        let remaining = self.remaining_bytes();
        if remaining == 0 {
            return Err(self.exceeded(1));
        }
        IndexOpenLimits::new(remaining.min(MAX_INDEX_OPEN_METADATA_BYTES))
    }

    fn account(&mut self, amount: u64) -> FormatResult<()> {
        let actual = self
            .accounted_bytes
            .checked_add(amount)
            .ok_or_else(|| self.exceeded(u64::MAX))?;
        if actual > self.max_accounted_bytes {
            return Err(FormatError::MetadataOpenLimitExceeded {
                field: "accounted bytes",
                actual,
                limit: self.max_accounted_bytes,
            });
        }
        self.accounted_bytes = actual;
        Ok(())
    }

    fn exceeded(self, additional_bytes: u64) -> FormatError {
        FormatError::MetadataOpenLimitExceeded {
            field: "accounted bytes",
            actual: self.accounted_bytes.saturating_add(additional_bytes),
            limit: self.max_accounted_bytes,
        }
    }
}

impl ArtifactIndexSource {
    pub fn open(
        path: impl AsRef<Path>,
        reference: ArtifactRef,
        data: Arc<ArtifactDataSource>,
    ) -> FormatResult<Self> {
        let source = Arc::new(ArtifactFile::open(path)?);
        let layout = Arc::new(
            open_index_artifact_metadata(source.as_ref(), reference, data.layout().as_ref())?
                .into_layout(),
        );
        Ok(Self {
            source,
            layout,
            data,
        })
    }

    pub fn open_with_limits(
        path: impl AsRef<Path>,
        reference: ArtifactRef,
        data: Arc<ArtifactDataSource>,
        limits: IndexOpenLimits,
    ) -> FormatResult<(Self, IndexOpenMetrics)> {
        let source = Arc::new(ArtifactFile::open(path)?);
        let opened = open_index_artifact_metadata_with_limits(
            source.as_ref(),
            reference,
            data.layout().as_ref(),
            limits,
        )?;
        let metrics = opened.metrics();
        let layout = Arc::new(opened.into_layout());
        Ok((
            Self {
                source,
                layout,
                data,
            },
            metrics,
        ))
    }

    pub fn open_with_budget(
        path: impl AsRef<Path>,
        reference: ArtifactRef,
        data: Arc<ArtifactDataSource>,
        budget: &mut OpenMetadataBudget,
    ) -> FormatResult<Self> {
        let limits = budget.index_limits()?;
        let remaining = budget.remaining_bytes();
        match Self::open_with_limits(path, reference, data, limits) {
            Ok((source, metrics)) => {
                budget.account(metrics.accounted_allocation_bytes())?;
                Ok(source)
            }
            Err(FormatError::IndexArtifactLimitExceeded {
                field: "metadata-open accounted bytes",
                actual,
                limit,
            }) if limit == remaining => Err(budget.exceeded(actual)),
            Err(error) => Err(error),
        }
    }

    pub const fn layout(&self) -> &Arc<IndexArtifactLayout> {
        &self.layout
    }

    /// An ordered accelerator is also an equality accelerator for any
    /// non-empty leading key prefix. This deliberately reuses its one
    /// canonical posting set instead of duplicating row-scaled prefix
    /// postings.
    pub fn supports_equality(&self, column_ordinals: &[usize]) -> bool {
        self.accelerator_for_columns(column_ordinals, true)
            .is_some()
    }

    pub fn supports_ordered(&self, column_ordinals: &[usize]) -> bool {
        self.accelerator_for_columns(column_ordinals, false)
            .is_some()
    }

    /// Resolve a complete equality key through either exact or ordered pages.
    /// `Ok(None)` means the requested key has no compatible accelerator or its
    /// candidate count exceeds the caller's bounded materialization budget.
    pub fn lookup_equality(
        &self,
        column_ordinals: &[usize],
        values: &[Value],
        max_candidates: Option<usize>,
    ) -> Result<Option<Vec<u64>>> {
        if column_ordinals.is_empty() || column_ordinals.len() != values.len() {
            return Ok(None);
        }
        let Some(accelerator) = self.accelerator_for_columns(column_ordinals, true) else {
            return Ok(None);
        };
        let limit = max_candidates.unwrap_or(usize::MAX);
        let probe_limit = limit.saturating_add(1);
        let rows = match accelerator.kind() {
            IndexAcceleratorKind::Exact => lookup_exact_index_from_source(
                self.source.as_ref(),
                self.layout.as_ref(),
                self.data.layout().as_ref(),
                accelerator.logical_index_id(),
                values,
            )
            .map_err(index_runtime_error)?
            .unwrap_or_default(),
            IndexAcceleratorKind::Ordered => {
                let bound_columns = &accelerator.key_columns()[..values.len()];
                let key = OrderedIndexKey::from_values(
                    self.data.layout().as_ref(),
                    bound_columns,
                    values,
                )
                .map_err(index_runtime_error)?;
                let bound = OrderedIndexBound::new(key, true);
                scan_ordered_index_from_source(
                    self.source.as_ref(),
                    self.layout.as_ref(),
                    self.data.layout().as_ref(),
                    accelerator.logical_index_id(),
                    Some(&bound),
                    Some(&bound),
                    IndexScanDirection::Forward,
                    0,
                    probe_limit,
                )
                .map_err(index_runtime_error)?
            }
            IndexAcceleratorKind::Hnsw => return Ok(None),
        };
        if rows.len() > limit {
            return Ok(None);
        }
        Ok(Some(rows))
    }

    /// Resolve a bounded ordered range through the canonical INDEX artifact.
    /// Bound values describe the requested non-empty leading key prefix;
    /// `Ok(None)` means that this artifact has no compatible ordered
    /// accelerator.
    #[allow(clippy::too_many_arguments)]
    pub fn scan_ordered(
        &self,
        column_ordinals: &[usize],
        lower: Option<(&[Value], bool)>,
        upper: Option<(&[Value], bool)>,
        ascending: bool,
        limit: usize,
    ) -> Result<Option<Vec<u64>>> {
        if column_ordinals.is_empty()
            || lower
                .map(|(values, _)| values.len() != column_ordinals.len())
                .unwrap_or(false)
            || upper
                .map(|(values, _)| values.len() != column_ordinals.len())
                .unwrap_or(false)
        {
            return Ok(None);
        }
        let Some(accelerator) = self.accelerator_for_columns(column_ordinals, false) else {
            return Ok(None);
        };
        let bound_columns = &accelerator.key_columns()[..column_ordinals.len()];
        let lower = lower
            .map(|(values, inclusive)| {
                OrderedIndexKey::from_values(self.data.layout().as_ref(), bound_columns, values)
                    .map(|key| OrderedIndexBound::new(key, inclusive))
            })
            .transpose()
            .map_err(index_runtime_error)?;
        let upper = upper
            .map(|(values, inclusive)| {
                OrderedIndexKey::from_values(self.data.layout().as_ref(), bound_columns, values)
                    .map(|key| OrderedIndexBound::new(key, inclusive))
            })
            .transpose()
            .map_err(index_runtime_error)?;
        let direction = if ascending {
            IndexScanDirection::Forward
        } else {
            IndexScanDirection::Reverse
        };
        scan_ordered_non_null_index_from_source(
            self.source.as_ref(),
            self.layout.as_ref(),
            self.data.layout().as_ref(),
            accelerator.logical_index_id(),
            lower.as_ref(),
            upper.as_ref(),
            direction,
            0,
            limit,
        )
        .map(Some)
        .map_err(index_runtime_error)
    }

    fn accelerator_for_columns(
        &self,
        column_ordinals: &[usize],
        equality: bool,
    ) -> Option<&IndexAccelerator> {
        let column_ids = column_ordinals
            .iter()
            .map(|ordinal| {
                self.data
                    .layout()
                    .columns()
                    .get(*ordinal)
                    .map(|column| column.column_id())
            })
            .collect::<Option<Vec<_>>>()?;
        self.layout.accelerators().iter().find(|accelerator| {
            let supported_shape = match (accelerator.kind(), equality) {
                (IndexAcceleratorKind::Exact, true) => {
                    accelerator.key_columns().len() == column_ids.len()
                }
                (IndexAcceleratorKind::Ordered, _) => {
                    !column_ids.is_empty() && column_ids.len() <= accelerator.key_columns().len()
                }
                _ => false,
            };
            supported_shape
                && accelerator
                    .key_columns()
                    .iter()
                    .zip(&column_ids)
                    .all(|(key, column_id)| key.column_id() == *column_id)
        })
    }
}

impl ArtifactDataSource {
    pub fn open(path: impl AsRef<Path>, reference: ArtifactRef) -> FormatResult<Self> {
        let source = Arc::new(ArtifactFile::open(path)?);
        let layout =
            Arc::new(open_data_artifact_metadata(source.as_ref(), reference)?.into_layout());
        Ok(Self {
            source,
            layout,
            row_ids: Mutex::new(None),
        })
    }

    pub fn open_with_limits(
        path: impl AsRef<Path>,
        reference: ArtifactRef,
        limits: DataOpenLimits,
    ) -> FormatResult<(Self, DataOpenMetrics)> {
        let source = Arc::new(ArtifactFile::open(path)?);
        let opened = open_data_artifact_metadata_with_limits(source.as_ref(), reference, limits)?;
        let metrics = opened.metrics();
        let layout = Arc::new(opened.into_layout());
        Ok((
            Self {
                source,
                layout,
                row_ids: Mutex::new(None),
            },
            metrics,
        ))
    }

    pub fn open_with_budget(
        path: impl AsRef<Path>,
        reference: ArtifactRef,
        budget: &mut OpenMetadataBudget,
    ) -> FormatResult<Self> {
        let limits = budget.data_limits()?;
        let remaining = budget.remaining_bytes();
        match Self::open_with_limits(path, reference, limits) {
            Ok((source, metrics)) => {
                budget.account(metrics.accounted_allocation_bytes())?;
                Ok(source)
            }
            Err(FormatError::DataArtifactLimitExceeded {
                field: "metadata-open accounted bytes",
                actual,
                limit,
            }) if limit == remaining => Err(budget.exceeded(actual)),
            Err(error) => Err(error),
        }
    }

    pub fn from_open_parts(
        source: Arc<ArtifactFile>,
        layout: DataArtifactLayout,
    ) -> FormatResult<Self> {
        if source.byte_length()? != layout.reference().byte_length() {
            return Err(FormatError::InvalidReference {
                owner: "runtime DATA source",
                detail: "source length differs from opened DATA identity",
            });
        }
        Ok(Self {
            source,
            layout: Arc::new(layout),
            row_ids: Mutex::new(None),
        })
    }

    pub const fn layout(&self) -> &Arc<DataArtifactLayout> {
        &self.layout
    }

    pub fn row_count(&self) -> Result<usize> {
        usize::try_from(self.layout.header().row_count())
            .map_err(|_| Error::internal("DATA row count does not fit this platform"))
    }

    pub fn column_count(&self) -> usize {
        self.layout.columns().len()
    }

    pub fn row_group_count(&self) -> usize {
        self.layout.row_groups().len()
    }

    pub fn row_group_range(&self, row_group_index: usize) -> Result<Range<usize>> {
        let group = self
            .layout
            .row_groups()
            .get(row_group_index)
            .ok_or_else(|| {
                Error::invalid_argument(format!(
                    "DATA row-group index {row_group_index} is out of range"
                ))
            })?;
        let start = usize::try_from(group.first_row_ordinal())
            .map_err(|_| Error::internal("DATA row-group start does not fit this platform"))?;
        let count = usize::try_from(group.row_count())
            .map_err(|_| Error::internal("DATA row-group length does not fit this platform"))?;
        let end = start
            .checked_add(count)
            .ok_or_else(|| Error::internal("DATA row-group range overflows"))?;
        if end > self.row_count()? {
            return Err(Error::internal(
                "DATA row-group range exceeds artifact row count",
            ));
        }
        Ok(start..end)
    }

    pub fn row_group_for_row(&self, row_index: usize) -> Result<usize> {
        if row_index >= self.row_count()? {
            return Err(Error::invalid_argument(format!(
                "DATA row index {row_index} is out of range"
            )));
        }
        let group_index = self
            .layout
            .row_groups()
            .partition_point(|group| group.first_row_ordinal() <= row_index as u64)
            .saturating_sub(1);
        let range = self.row_group_range(group_index)?;
        if !range.contains(&row_index) {
            return Err(Error::internal(
                "DATA row-group directory does not cover requested row",
            ));
        }
        Ok(group_index)
    }

    pub fn read_row_ids(&self, row_group_index: usize) -> Result<ArtifactRowIdBatch> {
        let range = self.row_group_range(row_group_index)?;
        let group_ordinal = u32::try_from(row_group_index)
            .map_err(|_| Error::internal("DATA row-group index exceeds u32"))?;
        let encoded = read_data_row_ids_from_source(
            self.source.as_ref(),
            self.layout.as_ref(),
            group_ordinal,
        )
        .map_err(runtime_format_error)?;
        if encoded.len() != range.len() {
            return Err(Error::internal(
                "decoded DATA row-ID count differs from row-group directory",
            ));
        }
        let row_ids = encoded.into_iter().map(decode_runtime_row_id).collect();
        Ok(ArtifactRowIdBatch {
            row_group_index,
            range,
            row_ids,
        })
    }

    pub fn row_id_at(&self, row_index: usize) -> Result<i64> {
        let group_index = self.row_group_for_row(row_index)?;
        if let Some(row_id) = self
            .row_ids
            .lock()
            .as_ref()
            .filter(|batch| batch.row_group_index == group_index)
            .and_then(|batch| batch.row_id(row_index))
        {
            return Ok(row_id);
        }
        let batch = Arc::new(self.read_row_ids(group_index)?);
        let row_id = batch.row_id(row_index).ok_or_else(|| {
            Error::internal("decoded DATA row-ID batch does not contain requested row")
        })?;
        *self.row_ids.lock() = Some(batch);
        Ok(row_id)
    }

    pub fn read_columns(
        &self,
        row_group_index: usize,
        columns: &[usize],
    ) -> Result<ArtifactColumnBatch> {
        let range = self.row_group_range(row_group_index)?;
        let group_ordinal = u32::try_from(row_group_index)
            .map_err(|_| Error::internal("DATA row-group index exceeds u32"))?;
        let mut decoded = Vec::with_capacity(columns.len());
        for &column_index in columns {
            let column = self.layout.columns().get(column_index).ok_or_else(|| {
                Error::invalid_argument(format!("DATA column index {column_index} is out of range"))
            })?;
            let column_ordinal = u32::try_from(column_index)
                .map_err(|_| Error::internal("DATA column index exceeds u32"))?;
            if column.ordinal() != column_ordinal {
                return Err(Error::internal(
                    "DATA column directory ordinal differs from its position",
                ));
            }
            let values = read_data_typed_column_from_source(
                self.source.as_ref(),
                self.layout.as_ref(),
                group_ordinal,
                column_ordinal,
            )
            .map_err(runtime_format_error)?;
            if values.len() != range.len() {
                return Err(Error::internal(
                    "decoded DATA column length differs from row-group directory",
                ));
            }
            decoded.push((column_index, ColumnData::from_artifact(values)));
        }
        Ok(ArtifactColumnBatch {
            row_group_index,
            range,
            columns: decoded,
        })
    }

    pub fn find_row_id(&self, row_id: i64) -> Result<std::result::Result<usize, usize>> {
        let encoded_row_id = encode_runtime_row_id(row_id);
        let groups = self.layout.row_groups();
        let candidate = groups.partition_point(|group| group.max_row_id() < encoded_row_id);
        let Some(group) = groups.get(candidate) else {
            return Ok(Err(self.row_count()?));
        };
        let range = self.row_group_range(candidate)?;
        if encoded_row_id < group.min_row_id() {
            return Ok(Err(range.start));
        }
        let row_ids = Arc::new(self.read_row_ids(candidate)?);
        Ok(match row_ids.row_ids.binary_search(&row_id) {
            Ok(local) => Ok(range.start + local),
            Err(local) => Err(range.start + local),
        })
        .inspect(|_| *self.row_ids.lock() = Some(row_ids))
    }
}

pub struct ArtifactRowIdBatch {
    row_group_index: usize,
    range: Range<usize>,
    row_ids: Vec<i64>,
}

impl ArtifactRowIdBatch {
    pub const fn row_group_index(&self) -> usize {
        self.row_group_index
    }

    pub fn row_range(&self) -> Range<usize> {
        self.range.clone()
    }

    pub fn row_ids(&self) -> &[i64] {
        &self.row_ids
    }

    pub fn row_id(&self, global_index: usize) -> Option<i64> {
        global_index
            .checked_sub(self.range.start)
            .filter(|local| *local < self.row_ids.len())
            .map(|local| self.row_ids[local])
    }
}

pub struct ArtifactColumnBatch {
    row_group_index: usize,
    range: Range<usize>,
    columns: Vec<(usize, ColumnData)>,
}

impl ArtifactColumnBatch {
    pub const fn row_group_index(&self) -> usize {
        self.row_group_index
    }

    pub fn row_range(&self) -> Range<usize> {
        self.range.clone()
    }

    pub fn columns(&self) -> &[(usize, ColumnData)] {
        &self.columns
    }

    pub fn into_columns(self) -> Vec<(usize, ColumnData)> {
        self.columns
    }
}

fn runtime_format_error(error: FormatError) -> Error {
    Error::internal(format!("DATA artifact read failed: {error}"))
}

fn index_runtime_error(error: FormatError) -> Error {
    Error::internal(format!("INDEX artifact read failed: {error}"))
}

#[cfg(test)]
mod metadata_budget_tests {
    use super::*;

    #[test]
    fn aggregate_budget_preserves_structural_charge_and_rejects_next_owner() {
        let allowance = ReachabilityAllowance::new(80, 100);
        let mut budget = OpenMetadataBudget::from_allowance(allowance);

        assert_eq!(budget.remaining_bytes(), 20);
        budget.account(12).unwrap();
        assert_eq!(budget.accounted_bytes(), 92);
        assert!(matches!(
            budget.account(9),
            Err(FormatError::MetadataOpenLimitExceeded {
                field: "accounted bytes",
                actual: 101,
                limit: 100,
            })
        ));
        assert_eq!(budget.accounted_bytes(), 92);
    }

    #[test]
    fn every_file_decoder_is_lowered_to_the_shared_remainder() {
        let budget = OpenMetadataBudget::from_allowance(ReachabilityAllowance::new(80, 100));

        assert_eq!(budget.data_limits().unwrap().max_accounted_bytes(), 20);
        assert_eq!(budget.index_limits().unwrap().max_accounted_bytes(), 20);
    }
}
