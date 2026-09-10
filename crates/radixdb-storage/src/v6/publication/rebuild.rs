use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use super::super::data::read_data_column_from_source;
use super::super::index::{
    read_index_page_from_source, write_index_artifact_stream, IndexPageSource,
};
use super::super::{
    ArtifactId, ArtifactRef, ArtifactSource, CatalogGeneration, DataArtifactLayout, DataBlockKind,
    DataColumnSpec, DataRowGroup, DatabaseGeneration, FormatError, FormatResult,
    IndexAcceleratorKind, IndexArtifactHeader, IndexArtifactLayout, IndexPageSpec,
    IndexRebuildRequest, IndexSectionKind, MAX_ACCELERATORS_PER_INDEX_ARTIFACT,
};
use super::diagnostics::{record, record_rebuild_invocation, DiagnosticEvent};
use super::model::{invalid_index, limit_index, AcceleratorBuildSpec, FanoutBuildLimits};
use super::resources::PublicationBuildBudget;
use super::runs::IndexRunBuilder;

#[derive(Debug, Clone)]
pub struct IndexReplacementBuildRequest {
    rebuild: IndexRebuildRequest,
    artifact_id: ArtifactId,
    creation_generation: DatabaseGeneration,
    catalog_generation: CatalogGeneration,
    accelerators: Vec<AcceleratorBuildSpec>,
    limits: FanoutBuildLimits,
    staging_directory: PathBuf,
}

impl IndexReplacementBuildRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rebuild: IndexRebuildRequest,
        artifact_id: ArtifactId,
        creation_generation: DatabaseGeneration,
        catalog_generation: CatalogGeneration,
        mut accelerators: Vec<AcceleratorBuildSpec>,
        limits: FanoutBuildLimits,
        staging_directory: impl Into<PathBuf>,
    ) -> FormatResult<Self> {
        if creation_generation <= rebuild.data_artifact().creation_generation() {
            return Err(invalid_index(
                "replacement index generation does not advance source data generation",
            ));
        }
        if accelerators.is_empty()
            || accelerators.len() as u64 > u64::from(MAX_ACCELERATORS_PER_INDEX_ARTIFACT)
        {
            return Err(FormatError::IndexArtifactLimitExceeded {
                field: "accelerator count",
                actual: accelerators.len() as u64,
                limit: u64::from(MAX_ACCELERATORS_PER_INDEX_ARTIFACT),
            });
        }
        accelerators.sort_by_key(AcceleratorBuildSpec::logical_index_id);
        if accelerators
            .windows(2)
            .any(|pair| pair[0].logical_index_id() == pair[1].logical_index_id())
        {
            return Err(invalid_index("logical accelerator IDs are not unique"));
        }
        let requested = accelerators
            .iter()
            .find(|candidate| candidate.logical_index_id() == rebuild.logical_index_id())
            .ok_or_else(|| invalid_index("replacement pack omits the requested accelerator"))?;
        if requested.definition_sha256() != rebuild.definition_sha256() {
            return Err(invalid_index(
                "requested accelerator definition differs from replacement pack",
            ));
        }
        Ok(Self {
            rebuild,
            artifact_id,
            creation_generation,
            catalog_generation,
            accelerators,
            limits,
            staging_directory: staging_directory.into(),
        })
    }

    pub const fn rebuild(&self) -> &IndexRebuildRequest {
        &self.rebuild
    }

    pub const fn artifact_id(&self) -> ArtifactId {
        self.artifact_id
    }

    pub const fn creation_generation(&self) -> DatabaseGeneration {
        self.creation_generation
    }

    pub const fn catalog_generation(&self) -> CatalogGeneration {
        self.catalog_generation
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

#[derive(Debug, Clone)]
pub struct WrittenIndexReplacement {
    reference: ArtifactRef,
    source_data: ArtifactRef,
    rebuild: IndexRebuildRequest,
    catalog_generation: CatalogGeneration,
}

impl WrittenIndexReplacement {
    pub const fn reference(&self) -> ArtifactRef {
        self.reference
    }

    pub const fn source_data(&self) -> ArtifactRef {
        self.source_data
    }

    pub const fn rebuild(&self) -> &IndexRebuildRequest {
        &self.rebuild
    }

    pub const fn catalog_generation(&self) -> CatalogGeneration {
        self.catalog_generation
    }
}

#[derive(Debug)]
pub struct StagedIndexReplacement {
    path: PathBuf,
    written: WrittenIndexReplacement,
}

impl StagedIndexReplacement {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub const fn written(&self) -> &WrittenIndexReplacement {
        &self.written
    }

    pub const fn reference(&self) -> ArtifactRef {
        self.written.reference()
    }
}

pub fn write_index_replacement<W>(
    request: &IndexReplacementBuildRequest,
    data_source: &(impl ArtifactSource + ?Sized),
    data: &DataArtifactLayout,
    output: &mut W,
) -> FormatResult<WrittenIndexReplacement>
where
    W: Read + Write + Seek,
{
    write_index_replacement_reusing(request, data_source, data, None, output)
}

/// Write a complete replacement pack while reusing immutable pages for
/// unchanged accelerators from the currently selected pack.
///
/// The accelerator named by the rebuild request is never reused: it is the
/// reason this publication exists and must be reconstructed from authoritative
/// DATA. Any absent or definition-mismatched accelerator is rebuilt as well.
pub(crate) fn write_index_replacement_reusing<W>(
    request: &IndexReplacementBuildRequest,
    data_source: &(impl ArtifactSource + ?Sized),
    data: &DataArtifactLayout,
    existing: Option<(&dyn ArtifactSource, &IndexArtifactLayout)>,
    output: &mut W,
) -> FormatResult<WrittenIndexReplacement>
where
    W: Read + Write + Seek,
{
    validate_source(request, data_source, data)?;
    validate_existing_source(existing, data)?;
    let reusable = request
        .accelerators()
        .iter()
        .map(|spec| reusable_pages(request, spec, existing))
        .collect::<FormatResult<Vec<_>>>()?;
    let build_count = reusable.iter().filter(|source| source.is_none()).count();
    let budget = PublicationBuildBudget::acquire_rebuild(request.limits(), build_count)?;
    let columns = data
        .columns()
        .iter()
        .map(|column| {
            DataColumnSpec::new(column.column_id(), column.data_type(), column.nullable())
        })
        .collect::<Vec<_>>();
    let mut builders = request
        .accelerators()
        .iter()
        .cloned()
        .enumerate()
        .filter(|(ordinal, _)| reusable[*ordinal].is_none())
        .map(|(ordinal, spec)| {
            IndexRunBuilder::new(
                spec,
                &columns,
                request.limits(),
                request.staging_directory(),
                ordinal,
                budget.accelerator_resident_bytes(),
                budget.spill(),
            )
            .map(|builder| (ordinal, builder))
        })
        .collect::<FormatResult<Vec<_>>>()?;
    let mut required_columns = vec![false; columns.len()];
    for (_, builder) in &builders {
        for ordinal in builder.source_ordinals() {
            required_columns[*ordinal] = true;
        }
    }

    record_rebuild_invocation();
    record(DiagnosticEvent::SourceStreamPass, 1);
    let traced_source = RebuildReadSource(data_source);
    let mut visited_rows = 0_u64;
    for group in data.row_groups().iter().copied() {
        validate_projection_resident_bytes(
            data,
            group,
            &required_columns,
            budget.rebuild_projection_bytes(),
        )?;
        let mut projected = vec![None; columns.len()];
        for (ordinal, required) in required_columns.iter().copied().enumerate() {
            if required {
                let values = read_data_column_from_source(
                    &traced_source,
                    data,
                    group.group_ordinal(),
                    ordinal as u32,
                )?;
                if values.len() != group.row_count() as usize {
                    return Err(invalid_index(
                        "rebuild column length differs from source row group",
                    ));
                }
                projected[ordinal] = Some(values);
            }
        }
        for relative_row in 0..group.row_count() as usize {
            let row_ordinal = group
                .first_row_ordinal()
                .checked_add(relative_row as u64)
                .ok_or_else(|| invalid_index("rebuild row ordinal overflows"))?;
            for (_, builder) in &mut builders {
                builder.visit_projected(&projected, relative_row, row_ordinal)?;
            }
            visited_rows = visited_rows
                .checked_add(1)
                .ok_or_else(|| invalid_index("rebuild source row count overflows"))?;
            record(DiagnosticEvent::SourceRow, 1);
        }
    }
    if visited_rows != data.header().row_count() {
        return Err(invalid_index(
            "rebuild source row count differs from data header",
        ));
    }
    let mut prepared = std::iter::repeat_with(|| None)
        .take(request.accelerators().len())
        .collect::<Vec<_>>();
    for (ordinal, builder) in builders {
        prepared[ordinal] = Some(builder.prepare(visited_rows)?);
    }
    let sources = reusable
        .into_iter()
        .zip(prepared)
        .map(|(reused, built)| match (reused, built) {
            (Some(reused), None) => Ok(ReplacementPageSource::Reused(reused)),
            (None, Some(built)) => Ok(ReplacementPageSource::Built(built)),
            _ => Err(invalid_index(
                "replacement accelerator has ambiguous page ownership",
            )),
        })
        .collect::<FormatResult<Vec<_>>>()?;
    let header = IndexArtifactHeader::for_data(
        request.artifact_id(),
        request.creation_generation(),
        request.catalog_generation(),
        data,
    )?;
    let reference =
        write_index_artifact_stream(output, header, &sources, budget.index_metadata_bytes())?;
    Ok(WrittenIndexReplacement {
        reference,
        source_data: data.reference(),
        rebuild: request.rebuild().clone(),
        catalog_generation: request.catalog_generation(),
    })
}

fn validate_existing_source(
    existing: Option<(&dyn ArtifactSource, &IndexArtifactLayout)>,
    data: &DataArtifactLayout,
) -> FormatResult<()> {
    let Some((source, layout)) = existing else {
        return Ok(());
    };
    let header = layout.header();
    if source.byte_length()? != layout.reference().byte_length()
        || header.database_id() != data.header().database_id()
        || header.table_id() != data.header().table_id()
        || header.segment_id() != data.header().segment_id()
        || header.data_artifact_id() != data.reference().id()
        || header.data_body_sha256() != data.reference().body_sha256()
        || header.source_row_count() != data.header().row_count()
    {
        return Err(invalid_index(
            "reusable index pack differs from authoritative DATA",
        ));
    }
    Ok(())
}

fn reusable_pages<'a>(
    request: &IndexReplacementBuildRequest,
    spec: &AcceleratorBuildSpec,
    existing: Option<(&'a dyn ArtifactSource, &'a IndexArtifactLayout)>,
) -> FormatResult<Option<ReusedIndexPages<'a>>> {
    if spec.logical_index_id() == request.rebuild().logical_index_id() {
        return Ok(None);
    }
    let Some((source, layout)) = existing else {
        return Ok(None);
    };
    let Some((accelerator_ordinal, accelerator)) = layout
        .accelerators()
        .iter()
        .enumerate()
        .find(|(_, accelerator)| accelerator.logical_index_id() == spec.logical_index_id())
    else {
        return Ok(None);
    };
    if accelerator.kind() != spec.kind()
        || accelerator.unique() != spec.unique()
        || accelerator.constraint_owned() != spec.constraint_owned()
        || accelerator.definition_sha256() != spec.definition_sha256()
        || accelerator.key_columns() != spec.key_columns()
    {
        return Ok(None);
    }
    let start = accelerator.first_section_index() as usize;
    let end = start
        .checked_add(accelerator.section_count() as usize)
        .ok_or_else(|| invalid_index("reusable accelerator section range overflows"))?;
    let sections = layout
        .sections()
        .get(start..end)
        .ok_or_else(|| invalid_index("reusable accelerator section range is outside pack"))?;
    let expected_kind = match accelerator.kind() {
        IndexAcceleratorKind::Exact => IndexSectionKind::ExactPages,
        IndexAcceleratorKind::Ordered => IndexSectionKind::OrderedPages,
        IndexAcceleratorKind::Hnsw => return Ok(None),
    };
    let mut matching = sections
        .iter()
        .copied()
        .filter(|section| section.kind() == expected_kind);
    let section = matching
        .next()
        .ok_or_else(|| invalid_index("reusable accelerator page section is absent"))?;
    if matching.next().is_some() || section.accelerator_ordinal() as usize != accelerator_ordinal {
        return Err(invalid_index(
            "reusable accelerator page ownership is ambiguous",
        ));
    }
    let candidate = ReusedIndexPages {
        spec: spec.clone(),
        indexed_item_count: accelerator.indexed_item_count(),
        source,
        layout,
        first_page_index: section.first_page_index() as usize,
        page_count: section.page_count() as usize,
    };
    // A reusable INDEX is only an optimization. Validate every referenced
    // page before touching the new output so a damaged old pack degrades to a
    // DATA rebuild instead of leaving a partial replacement behind.
    if candidate.preflight().is_err() {
        return Ok(None);
    }
    Ok(Some(candidate))
}

enum ReplacementPageSource<'a> {
    Built(super::runs::PreparedIndexRuns),
    Reused(ReusedIndexPages<'a>),
}

impl IndexPageSource for ReplacementPageSource<'_> {
    fn logical_index_id(&self) -> radixdb_catalog::ObjectId {
        match self {
            Self::Built(source) => source.logical_index_id(),
            Self::Reused(source) => source.logical_index_id(),
        }
    }

    fn kind(&self) -> IndexAcceleratorKind {
        match self {
            Self::Built(source) => source.kind(),
            Self::Reused(source) => source.kind(),
        }
    }

    fn unique(&self) -> bool {
        match self {
            Self::Built(source) => source.unique(),
            Self::Reused(source) => source.unique(),
        }
    }

    fn constraint_owned(&self) -> bool {
        match self {
            Self::Built(source) => source.constraint_owned(),
            Self::Reused(source) => source.constraint_owned(),
        }
    }

    fn definition_sha256(&self) -> &[u8; 32] {
        match self {
            Self::Built(source) => source.definition_sha256(),
            Self::Reused(source) => source.definition_sha256(),
        }
    }

    fn key_columns(&self) -> &[super::super::IndexKeyColumn] {
        match self {
            Self::Built(source) => source.key_columns(),
            Self::Reused(source) => source.key_columns(),
        }
    }

    fn indexed_item_count(&self) -> u64 {
        match self {
            Self::Built(source) => source.indexed_item_count(),
            Self::Reused(source) => source.indexed_item_count(),
        }
    }

    fn page_count(&self) -> u64 {
        match self {
            Self::Built(source) => source.page_count(),
            Self::Reused(source) => source.page_count(),
        }
    }

    fn visit_pages(
        &self,
        visitor: &mut dyn FnMut(&IndexPageSpec) -> FormatResult<()>,
    ) -> FormatResult<u64> {
        match self {
            Self::Built(source) => source.visit_pages(visitor),
            Self::Reused(source) => source.visit_pages(visitor),
        }
    }
}

struct ReusedIndexPages<'a> {
    spec: AcceleratorBuildSpec,
    indexed_item_count: u64,
    source: &'a dyn ArtifactSource,
    layout: &'a IndexArtifactLayout,
    first_page_index: usize,
    page_count: usize,
}

impl ReusedIndexPages<'_> {
    fn pages(&self) -> FormatResult<&[super::super::IndexPage]> {
        let end = self
            .first_page_index
            .checked_add(self.page_count)
            .ok_or_else(|| invalid_index("reusable page range overflows"))?;
        self.layout
            .pages()
            .get(self.first_page_index..end)
            .ok_or_else(|| invalid_index("reusable page range is outside pack"))
    }

    fn preflight(&self) -> FormatResult<()> {
        for offset in 0..self.pages()?.len() {
            read_index_page_from_source(self.source, self.layout, self.first_page_index + offset)?;
        }
        Ok(())
    }
}

impl IndexPageSource for ReusedIndexPages<'_> {
    fn logical_index_id(&self) -> radixdb_catalog::ObjectId {
        self.spec.logical_index_id()
    }

    fn kind(&self) -> IndexAcceleratorKind {
        self.spec.kind()
    }

    fn unique(&self) -> bool {
        self.spec.unique()
    }

    fn constraint_owned(&self) -> bool {
        self.spec.constraint_owned()
    }

    fn definition_sha256(&self) -> &[u8; 32] {
        self.spec.definition_sha256()
    }

    fn key_columns(&self) -> &[super::super::IndexKeyColumn] {
        self.spec.key_columns()
    }

    fn indexed_item_count(&self) -> u64 {
        self.indexed_item_count
    }

    fn page_count(&self) -> u64 {
        self.page_count as u64
    }

    fn visit_pages(
        &self,
        visitor: &mut dyn FnMut(&IndexPageSpec) -> FormatResult<()>,
    ) -> FormatResult<u64> {
        let pages = self.pages()?;
        for (offset, page) in pages.iter().copied().enumerate() {
            let logical = read_index_page_from_source(
                self.source,
                self.layout,
                self.first_page_index + offset,
            )?;
            let spec = if page.item_count() == 0 {
                IndexPageSpec::empty_exact(logical, page.codec())?
            } else {
                IndexPageSpec::new(
                    logical,
                    page.codec(),
                    page.item_count(),
                    page.minimum_key_hash(),
                    page.maximum_key_hash(),
                )?
            };
            visitor(&spec)?;
        }
        Ok(pages.len() as u64)
    }
}

fn validate_projection_resident_bytes(
    data: &DataArtifactLayout,
    group: DataRowGroup,
    required_columns: &[bool],
    limit: u64,
) -> FormatResult<()> {
    let mut bytes = required_columns
        .len()
        .checked_mul(std::mem::size_of::<Option<Vec<radixdb_core::Value>>>())
        .map(|value| value as u64)
        .ok_or_else(|| invalid_index("rebuild projection metadata bytes overflow"))?;
    let start = group.first_block_index() as usize;
    let end = start
        .checked_add(group.block_count() as usize)
        .ok_or_else(|| invalid_index("rebuild source block range overflows"))?;
    let blocks = data
        .blocks()
        .get(start..end)
        .ok_or_else(|| invalid_index("rebuild source block range is outside DATA"))?;
    for (ordinal, required) in required_columns.iter().copied().enumerate() {
        if !required {
            continue;
        }
        let block = blocks
            .iter()
            .find(|block| {
                block.kind() == DataBlockKind::Column && block.column_ordinal() == ordinal as u32
            })
            .ok_or_else(|| invalid_index("rebuild source column block is absent"))?;
        let value_slots = (group.row_count() as u64)
            .checked_mul(std::mem::size_of::<radixdb_core::Value>() as u64)
            .ok_or_else(|| invalid_index("rebuild projection value slots overflow"))?;
        bytes = bytes
            .checked_add(value_slots)
            .and_then(|value| value.checked_add(block.logical_length()))
            .ok_or_else(|| invalid_index("rebuild projection resident bytes overflow"))?;
    }
    if bytes > limit {
        return Err(limit_index(
            "rebuild projection resident bytes",
            bytes,
            limit,
        ));
    }
    Ok(())
}

pub fn stage_index_replacement(
    request: &IndexReplacementBuildRequest,
    data_source: &(impl ArtifactSource + ?Sized),
    data: &DataArtifactLayout,
) -> FormatResult<StagedIndexReplacement> {
    validate_staging_directory(request.staging_directory())?;
    let path = request
        .staging_directory()
        .join(format!("index-{}.idx.staged", request.artifact_id()));
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|error| io_error("create staged index replacement", error))?;
    let result =
        write_index_replacement(request, data_source, data, &mut file).and_then(|written| {
            file.sync_all()
                .map_err(|error| io_error("sync staged index replacement", error))?;
            sync_directory(request.staging_directory())?;
            Ok(StagedIndexReplacement {
                path: path.clone(),
                written,
            })
        });
    if result.is_err() {
        drop(file);
        let _ = std::fs::remove_file(&path);
        let _ = sync_directory(request.staging_directory());
    }
    result
}

fn validate_source(
    request: &IndexReplacementBuildRequest,
    data_source: &(impl ArtifactSource + ?Sized),
    data: &DataArtifactLayout,
) -> FormatResult<()> {
    let rebuild = request.rebuild();
    if rebuild.data_artifact() != data.reference()
        || rebuild.database_id() != data.header().database_id()
        || rebuild.table_id() != data.header().table_id()
        || rebuild.segment_id() != data.header().segment_id()
    {
        return Err(invalid_index(
            "rebuild request differs from authoritative data artifact",
        ));
    }
    if data_source.byte_length()? != data.reference().byte_length() {
        return Err(invalid_index(
            "rebuild data source length differs from artifact reference",
        ));
    }
    for accelerator in request.accelerators() {
        for key in accelerator.key_columns() {
            let column = data
                .columns()
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
    Ok(())
}

fn validate_staging_directory(path: &Path) -> FormatResult<()> {
    let metadata = std::fs::metadata(path)
        .map_err(|error| io_error("inspect index rebuild staging directory", error))?;
    if !metadata.is_dir() {
        return Err(invalid_index(
            "index rebuild staging path is not a directory",
        ));
    }
    Ok(())
}

fn sync_directory(path: &Path) -> FormatResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("sync index rebuild staging directory", error))
}

fn io_error(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::ArtifactIo {
        operation,
        kind: error.kind(),
    }
}

struct RebuildReadSource<'a, S: ?Sized>(&'a S);

impl<S: ArtifactSource + ?Sized> ArtifactSource for RebuildReadSource<'_, S> {
    fn byte_length(&self) -> FormatResult<u64> {
        self.0.byte_length()
    }

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> FormatResult<()> {
        self.0.read_exact_at(offset, destination)?;
        record(
            DiagnosticEvent::IndexConstructionRead,
            destination.len() as u64,
        );
        Ok(())
    }
}
