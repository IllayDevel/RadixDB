use std::fmt;

use radixdb_catalog::ObjectId;
use radixdb_core::Value;

use super::super::{
    read_data_column_from_source, ArtifactRef, ArtifactSource, DataArtifactLayout, DatabaseId,
    FormatError, FormatResult, SegmentId, UnavailableIndexReason,
};
use super::exact::lookup_exact_index_from_source;
use super::key::{encode_canonical_key, resolve_source_columns};
use super::model::{
    invalid, limit, IndexAcceleratorKind, IndexArtifactLayout, IndexKeyColumn, MAX_KEY_COLUMNS,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactLookupDefinition {
    logical_index_id: ObjectId,
    unique: bool,
    constraint_owned: bool,
    definition_sha256: [u8; 32],
    key_columns: Vec<IndexKeyColumn>,
}

impl ExactLookupDefinition {
    pub fn new(
        logical_index_id: ObjectId,
        unique: bool,
        constraint_owned: bool,
        definition_sha256: [u8; 32],
        key_columns: Vec<IndexKeyColumn>,
    ) -> FormatResult<Self> {
        if constraint_owned && !unique {
            return Err(invalid(
                "constraint-owned exact definition must enforce UNIQUE semantics",
            ));
        }
        if key_columns.is_empty() || key_columns.len() as u64 > u64::from(MAX_KEY_COLUMNS) {
            return Err(limit(
                "exact lookup key columns",
                key_columns.len() as u64,
                u64::from(MAX_KEY_COLUMNS),
            ));
        }
        if key_columns.iter().enumerate().any(|(index, column)| {
            key_columns[..index]
                .iter()
                .any(|previous| previous.column_id() == column.column_id())
        }) {
            return Err(invalid("exact lookup key columns are not unique"));
        }
        Ok(Self {
            logical_index_id,
            unique,
            constraint_owned,
            definition_sha256,
            key_columns,
        })
    }

    pub const fn logical_index_id(&self) -> ObjectId {
        self.logical_index_id
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
}

pub enum IndexAccessState<'a> {
    Available {
        source: &'a dyn ArtifactSource,
        layout: &'a IndexArtifactLayout,
    },
    Unavailable(UnavailableIndexReason),
    Rebuilding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintMutationKind {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintMutationError {
    InvalidRequest(FormatError),
    AcceleratorUnavailable {
        logical_index_id: ObjectId,
        mutation: ConstraintMutationKind,
        reason: ExactFallbackReason,
    },
}

impl ConstraintMutationError {
    pub const fn logical_index_id(&self) -> Option<ObjectId> {
        match self {
            Self::InvalidRequest(_) => None,
            Self::AcceleratorUnavailable {
                logical_index_id, ..
            } => Some(*logical_index_id),
        }
    }

    pub const fn mutation(&self) -> Option<ConstraintMutationKind> {
        match self {
            Self::InvalidRequest(_) => None,
            Self::AcceleratorUnavailable { mutation, .. } => Some(*mutation),
        }
    }

    pub const fn reason(&self) -> Option<&ExactFallbackReason> {
        match self {
            Self::InvalidRequest(_) => None,
            Self::AcceleratorUnavailable { reason, .. } => Some(reason),
        }
    }
}

impl fmt::Display for ConstraintMutationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(error) => {
                write!(formatter, "invalid constraint mutation: {error}")
            }
            Self::AcceleratorUnavailable { .. } => {
                formatter.write_str("constraint accelerator unavailable")
            }
        }
    }
}

impl std::error::Error for ConstraintMutationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidRequest(error) => Some(error),
            Self::AcceleratorUnavailable { .. } => None,
        }
    }
}

impl From<FormatError> for ConstraintMutationError {
    fn from(error: FormatError) -> Self {
        Self::InvalidRequest(error)
    }
}

pub enum ConstraintMutationAdmission<'a> {
    NotRequired,
    Verified(ConstraintMutationGuard<'a>),
}

pub struct ConstraintMutationGuard<'a> {
    source: &'a dyn ArtifactSource,
    layout: &'a IndexArtifactLayout,
    data: &'a DataArtifactLayout,
    definition: &'a ExactLookupDefinition,
    mutation: ConstraintMutationKind,
}

impl ConstraintMutationGuard<'_> {
    pub const fn mutation(&self) -> ConstraintMutationKind {
        self.mutation
    }

    /// Reads only the admitted exact accelerator. A page read, checksum, or
    /// decode failure rejects the mutation; this guard has no scan fallback.
    pub fn conflicting_rows(
        &self,
        values: &[Value],
    ) -> Result<Option<Vec<u64>>, ConstraintMutationError> {
        lookup_exact_index_from_source(
            self.source,
            self.layout,
            self.data,
            self.definition.logical_index_id(),
            values,
        )
        .map_err(|error| self.unavailable(classify_access_failure(error)))
    }

    fn unavailable(&self, reason: UnavailableIndexReason) -> ConstraintMutationError {
        ConstraintMutationError::AcceleratorUnavailable {
            logical_index_id: self.definition.logical_index_id(),
            mutation: self.mutation,
            reason: ExactFallbackReason::Unavailable(reason),
        }
    }
}

/// Admits a UNIQUE/PK-changing mutation only while its exact accelerator is
/// present and bound to the same immutable data snapshot and catalog
/// definition. There is deliberately no data source or rebuild sink argument:
/// this path cannot perform a per-DML scan or silently disable enforcement.
pub fn admit_constraint_mutation<'a>(
    data: &'a DataArtifactLayout,
    definition: &'a ExactLookupDefinition,
    mutation: ConstraintMutationKind,
    changed_columns: &[ObjectId],
    state: IndexAccessState<'a>,
) -> Result<ConstraintMutationAdmission<'a>, ConstraintMutationError> {
    if !definition.unique()
        || (mutation == ConstraintMutationKind::Update
            && !changed_columns.iter().any(|column| {
                definition
                    .key_columns()
                    .iter()
                    .any(|key| key.column_id() == *column)
            }))
    {
        return Ok(ConstraintMutationAdmission::NotRequired);
    }

    let unavailable = |reason| ConstraintMutationError::AcceleratorUnavailable {
        logical_index_id: definition.logical_index_id(),
        mutation,
        reason,
    };
    match state {
        IndexAccessState::Available { source, layout } => {
            validate_available_binding(layout, data, definition)
                .map_err(|reason| unavailable(ExactFallbackReason::Unavailable(reason)))?;
            Ok(ConstraintMutationAdmission::Verified(
                ConstraintMutationGuard {
                    source,
                    layout,
                    data,
                    definition,
                    mutation,
                },
            ))
        }
        IndexAccessState::Unavailable(reason) => {
            Err(unavailable(ExactFallbackReason::Unavailable(reason)))
        }
        IndexAccessState::Rebuilding => Err(unavailable(ExactFallbackReason::Rebuilding)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactLookupPath {
    Accelerator,
    DataScan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExactFallbackReason {
    Unavailable(UnavailableIndexReason),
    Rebuilding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebuildRequestStatus {
    Enqueued,
    AlreadyPending,
    AtCapacity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRebuildRequest {
    database_id: DatabaseId,
    table_id: ObjectId,
    segment_id: SegmentId,
    data_artifact: ArtifactRef,
    logical_index_id: ObjectId,
    definition_sha256: [u8; 32],
    reason: UnavailableIndexReason,
}

impl IndexRebuildRequest {
    /// Construct a rebuild owned by synchronous DDL physical publication.
    /// Query fallback normally creates this token after observing a missing or
    /// invalid accelerator; CREATE INDEX already knows the exact new catalog
    /// definition and therefore does not need to manufacture a failed lookup.
    pub(crate) fn for_ddl_publication(
        database_id: DatabaseId,
        table_id: ObjectId,
        segment_id: SegmentId,
        data_artifact: ArtifactRef,
        logical_index_id: ObjectId,
        definition_sha256: [u8; 32],
    ) -> Self {
        Self {
            database_id,
            table_id,
            segment_id,
            data_artifact,
            logical_index_id,
            definition_sha256,
            reason: UnavailableIndexReason::Missing,
        }
    }

    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub const fn table_id(&self) -> ObjectId {
        self.table_id
    }

    pub const fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    pub const fn data_artifact(&self) -> ArtifactRef {
        self.data_artifact
    }

    pub const fn logical_index_id(&self) -> ObjectId {
        self.logical_index_id
    }

    pub const fn definition_sha256(&self) -> &[u8; 32] {
        &self.definition_sha256
    }

    pub const fn reason(&self) -> &UnavailableIndexReason {
        &self.reason
    }
}

pub trait RebuildRequestSink {
    /// Requests asynchronous repair without affecting SELECT correctness.
    ///
    /// Implementations must be bounded, non-blocking, and deduplicate by data
    /// artifact, logical index, and definition identity. Capacity rejection is
    /// observable but is not a query error.
    fn request_rebuild(&mut self, request: IndexRebuildRequest) -> RebuildRequestStatus;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactLookupReport {
    path: ExactLookupPath,
    matched_rows: u64,
    scanned_row_groups: u64,
    scanned_column_blocks: u64,
    fallback_reason: Option<ExactFallbackReason>,
    rebuild_status: Option<RebuildRequestStatus>,
}

impl ExactLookupReport {
    pub const fn path(&self) -> ExactLookupPath {
        self.path
    }

    pub const fn matched_rows(&self) -> u64 {
        self.matched_rows
    }

    pub const fn scanned_row_groups(&self) -> u64 {
        self.scanned_row_groups
    }

    pub const fn scanned_column_blocks(&self) -> u64 {
        self.scanned_column_blocks
    }

    pub const fn fallback_reason(&self) -> Option<&ExactFallbackReason> {
        self.fallback_reason.as_ref()
    }

    pub const fn rebuild_status(&self) -> Option<RebuildRequestStatus> {
        self.rebuild_status
    }
}

/// Visits exact-match row ordinals using an accelerator when safe and the
/// authoritative data artifact otherwise.
///
/// The fallback keeps only one decoded key column and one row-group candidate
/// bitmap resident. It never returns an empty set merely because the optional
/// index pack is missing, rebuilding, incorrectly bound, unreadable, or
/// corrupt.
pub fn visit_exact_index_or_scan(
    data_source: &(impl ArtifactSource + ?Sized),
    data: &DataArtifactLayout,
    definition: &ExactLookupDefinition,
    values: &[Value],
    state: IndexAccessState<'_>,
    rebuilds: &mut (impl RebuildRequestSink + ?Sized),
    visitor: &mut dyn FnMut(u64) -> FormatResult<()>,
) -> FormatResult<ExactLookupReport> {
    let source_columns = resolve_source_columns(data, definition.key_columns())?;
    let (_, target_has_null) = encode_canonical_key(&source_columns, values)?;

    match state {
        IndexAccessState::Available { source, layout } => {
            let result = validate_available_binding(layout, data, definition).and_then(|()| {
                lookup_exact_index_from_source(
                    source,
                    layout,
                    data,
                    definition.logical_index_id(),
                    values,
                )
                .map_err(classify_access_failure)
            });
            match result {
                Ok(rows) => {
                    let mut matched_rows = 0_u64;
                    for row_ordinal in rows.into_iter().flatten() {
                        visitor(row_ordinal)?;
                        matched_rows = matched_rows
                            .checked_add(1)
                            .ok_or_else(|| invalid("exact lookup match count overflows"))?;
                    }
                    Ok(ExactLookupReport {
                        path: ExactLookupPath::Accelerator,
                        matched_rows,
                        scanned_row_groups: 0,
                        scanned_column_blocks: 0,
                        fallback_reason: None,
                        rebuild_status: None,
                    })
                }
                Err(error) => scan_with_rebuild_request(
                    data_source,
                    data,
                    definition,
                    values,
                    &source_columns,
                    target_has_null,
                    error,
                    rebuilds,
                    visitor,
                ),
            }
        }
        IndexAccessState::Unavailable(reason) => scan_with_rebuild_request(
            data_source,
            data,
            definition,
            values,
            &source_columns,
            target_has_null,
            reason,
            rebuilds,
            visitor,
        ),
        IndexAccessState::Rebuilding => {
            let counts = scan_exact_data(
                data_source,
                data,
                definition,
                values,
                &source_columns,
                target_has_null,
                visitor,
            )?;
            Ok(ExactLookupReport {
                path: ExactLookupPath::DataScan,
                matched_rows: counts.matched_rows,
                scanned_row_groups: counts.row_groups,
                scanned_column_blocks: counts.column_blocks,
                fallback_reason: Some(ExactFallbackReason::Rebuilding),
                rebuild_status: None,
            })
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn scan_with_rebuild_request(
    data_source: &(impl ArtifactSource + ?Sized),
    data: &DataArtifactLayout,
    definition: &ExactLookupDefinition,
    values: &[Value],
    source_columns: &[super::super::DataColumn],
    target_has_null: bool,
    reason: UnavailableIndexReason,
    rebuilds: &mut (impl RebuildRequestSink + ?Sized),
    visitor: &mut dyn FnMut(u64) -> FormatResult<()>,
) -> FormatResult<ExactLookupReport> {
    let rebuild_status = rebuilds.request_rebuild(IndexRebuildRequest {
        database_id: data.header().database_id(),
        table_id: data.header().table_id(),
        segment_id: data.header().segment_id(),
        data_artifact: data.reference(),
        logical_index_id: definition.logical_index_id(),
        definition_sha256: *definition.definition_sha256(),
        reason: reason.clone(),
    });
    let counts = scan_exact_data(
        data_source,
        data,
        definition,
        values,
        source_columns,
        target_has_null,
        visitor,
    )?;
    Ok(ExactLookupReport {
        path: ExactLookupPath::DataScan,
        matched_rows: counts.matched_rows,
        scanned_row_groups: counts.row_groups,
        scanned_column_blocks: counts.column_blocks,
        fallback_reason: Some(ExactFallbackReason::Unavailable(reason)),
        rebuild_status: Some(rebuild_status),
    })
}

fn validate_available_binding(
    layout: &IndexArtifactLayout,
    data: &DataArtifactLayout,
    definition: &ExactLookupDefinition,
) -> Result<(), UnavailableIndexReason> {
    let header = layout.header();
    if header.database_id() != data.header().database_id()
        || header.table_id() != data.header().table_id()
        || header.segment_id() != data.header().segment_id()
        || header.data_artifact_id() != data.reference().id()
        || header.data_body_sha256() != data.reference().body_sha256()
        || header.source_row_count() != data.header().row_count()
    {
        return Err(UnavailableIndexReason::CrossReferenceMismatch(
            "available index is bound to different source data".to_owned(),
        ));
    }
    let accelerator = layout
        .accelerators()
        .iter()
        .find(|accelerator| accelerator.logical_index_id() == definition.logical_index_id())
        .ok_or_else(|| {
            UnavailableIndexReason::CrossReferenceMismatch(
                "logical exact accelerator is absent from available index".to_owned(),
            )
        })?;
    if accelerator.kind() != IndexAcceleratorKind::Exact
        || accelerator.unique() != definition.unique()
        || accelerator.constraint_owned() != definition.constraint_owned()
        || accelerator.definition_sha256() != definition.definition_sha256()
        || accelerator.key_columns() != definition.key_columns()
    {
        return Err(UnavailableIndexReason::CrossReferenceMismatch(
            "available exact accelerator differs from catalog definition".to_owned(),
        ));
    }
    Ok(())
}

fn classify_access_failure(error: FormatError) -> UnavailableIndexReason {
    UnavailableIndexReason::Invalid(error.to_string())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ScanCounts {
    matched_rows: u64,
    row_groups: u64,
    column_blocks: u64,
}

#[allow(clippy::too_many_arguments)]
fn scan_exact_data(
    source: &(impl ArtifactSource + ?Sized),
    data: &DataArtifactLayout,
    definition: &ExactLookupDefinition,
    values: &[Value],
    source_columns: &[super::super::DataColumn],
    target_has_null: bool,
    visitor: &mut dyn FnMut(u64) -> FormatResult<()>,
) -> FormatResult<ScanCounts> {
    if definition.unique() && target_has_null {
        return Ok(ScanCounts::default());
    }
    let target_components = source_columns
        .iter()
        .copied()
        .zip(values)
        .map(|(column, value)| {
            encode_canonical_key(std::slice::from_ref(&column), std::slice::from_ref(value))
                .map(|(bytes, _)| bytes)
        })
        .collect::<FormatResult<Vec<_>>>()?;

    let mut counts = ScanCounts::default();
    for group in data.row_groups().iter().copied() {
        counts.row_groups = counts
            .row_groups
            .checked_add(1)
            .ok_or_else(|| invalid("fallback row-group count overflows"))?;
        let mut candidates = vec![true; group.row_count() as usize];
        for (column, target) in source_columns.iter().copied().zip(&target_components) {
            let column_values = read_data_column_from_source(
                source,
                data,
                group.group_ordinal(),
                column.ordinal(),
            )?;
            counts.column_blocks = counts
                .column_blocks
                .checked_add(1)
                .ok_or_else(|| invalid("fallback column-block count overflows"))?;
            if column_values.len() != candidates.len() {
                return Err(invalid("fallback column row count differs from row group"));
            }
            for (candidate, value) in candidates.iter_mut().zip(&column_values) {
                if !*candidate {
                    continue;
                }
                let (encoded, has_null) = encode_canonical_key(
                    std::slice::from_ref(&column),
                    std::slice::from_ref(value),
                )?;
                if (definition.unique() && has_null) || encoded != *target {
                    *candidate = false;
                }
            }
            if !candidates.iter().any(|candidate| *candidate) {
                break;
            }
        }
        for (relative, matched) in candidates.into_iter().enumerate() {
            if !matched {
                continue;
            }
            let row_ordinal = group
                .first_row_ordinal()
                .checked_add(relative as u64)
                .ok_or_else(|| invalid("fallback row ordinal overflows"))?;
            visitor(row_ordinal)?;
            counts.matched_rows = counts
                .matched_rows
                .checked_add(1)
                .ok_or_else(|| invalid("fallback match count overflows"))?;
        }
    }
    Ok(counts)
}
