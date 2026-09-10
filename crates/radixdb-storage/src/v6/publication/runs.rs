use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use radixdb_core::Value;
use smallvec::SmallVec;

use crate::byte_limiter::InFlightBytePermit;

use super::super::index::{
    compare_ordered_keys_trusted, count_exact_index_pages_from_sorted,
    count_ordered_index_pages_from_sorted, visit_exact_index_pages_from_sorted,
    visit_ordered_index_pages_from_sorted, IndexPageSource, PostingFragmentState,
};
use super::super::{
    DataColumnSpec, ExactIndexEntry, ExactIndexKey, ExactPageBuildLimits, FormatError,
    FormatResult, IndexAcceleratorKind, IndexKeyColumn, IndexPageCodec, IndexPageSpec,
    OrderedIndexEntry, OrderedIndexKey, OrderedPageBuildLimits, MAX_INDEX_KEY_BYTES,
};
use super::diagnostics::{record as record_diagnostic, DiagnosticEvent};
use super::model::{invalid_index, limit_index, AcceleratorBuildSpec, FanoutBuildLimits};
use super::resources::{acquire_sort_run_descriptors, SpillBudget};

const RUN_MAGIC: [u8; 8] = *b"RDXRUN1\0";
const RUN_HEADER_BYTES: usize = 24;
const RUN_RECORD_HEADER_BYTES: usize = 24;

#[derive(Debug, Clone)]
enum RunKey {
    Exact(ExactIndexKey),
    Ordered(OrderedIndexKey),
}

impl RunKey {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Exact(key) => key.as_bytes(),
            Self::Ordered(key) => key.as_bytes(),
        }
    }

    fn has_null_component(&self) -> bool {
        match self {
            Self::Exact(key) => key.has_null_component(),
            Self::Ordered(key) => key.has_null_component(),
        }
    }

    fn compare(&self, other: &Self, columns: &[IndexKeyColumn]) -> FormatResult<Ordering> {
        match (self, other) {
            (Self::Exact(left), Self::Exact(right)) => Ok(left.cmp(right)),
            (Self::Ordered(left), Self::Ordered(right)) => {
                Ok(compare_ordered_keys_trusted(left, right, columns))
            }
            _ => Err(invalid_index("sort run mixes accelerator kinds")),
        }
    }
}

#[derive(Debug)]
struct RunRecord {
    key: RunKey,
    row_ordinal: u64,
}

impl RunRecord {
    fn compare(&self, other: &Self, columns: &[IndexKeyColumn]) -> Ordering {
        self.key
            .compare(&other.key, columns)
            .expect("validated run keys have a total order")
            .then_with(|| self.row_ordinal.cmp(&other.row_ordinal))
    }
}

fn write_run_header(
    writer: &mut impl Write,
    kind: IndexAcceleratorKind,
    record_count: u64,
) -> FormatResult<()> {
    let mut header = [0_u8; RUN_HEADER_BYTES];
    header[..8].copy_from_slice(&RUN_MAGIC);
    header[8..16].copy_from_slice(&record_count.to_le_bytes());
    header[16..18].copy_from_slice(&kind.tag().to_le_bytes());
    let header_crc = radixdb_core::crc32_ieee(&header[..20]);
    header[20..24].copy_from_slice(&header_crc.to_le_bytes());
    writer
        .write_all(&header)
        .map_err(|error| io_error("write sort-run header", error))?;
    record_diagnostic(DiagnosticEvent::SortRunWrite, header.len() as u64);
    Ok(())
}

fn write_run_record(writer: &mut impl Write, record: &RunRecord) -> FormatResult<()> {
    let key = record.key.bytes();
    let key_length =
        u32::try_from(key.len()).map_err(|_| invalid_index("sort-run key length exceeds u32"))?;
    let mut record_header = [0_u8; RUN_RECORD_HEADER_BYTES];
    record_header[..4].copy_from_slice(&key_length.to_le_bytes());
    record_header[4..8].copy_from_slice(&u32::from(record.key.has_null_component()).to_le_bytes());
    record_header[8..16].copy_from_slice(&record.row_ordinal.to_le_bytes());
    record_header[16..20].copy_from_slice(&radixdb_core::crc32_ieee(key).to_le_bytes());
    writer
        .write_all(&record_header)
        .map_err(|error| io_error("write sort-run record", error))?;
    record_diagnostic(DiagnosticEvent::SortRunWrite, record_header.len() as u64);
    writer
        .write_all(key)
        .map_err(|error| io_error("write sort-run key", error))?;
    record_diagnostic(DiagnosticEvent::SortRunWrite, key.len() as u64);
    Ok(())
}

#[derive(Debug)]
pub(crate) struct IndexRunBuilder {
    spec: AcceleratorBuildSpec,
    source_ordinals: Vec<usize>,
    key_specs: Vec<DataColumnSpec>,
    limits: FanoutBuildLimits,
    staging_directory: PathBuf,
    accelerator_ordinal: usize,
    buffer: Vec<RunRecord>,
    buffer_is_sorted: bool,
    buffered_bytes: u64,
    buffered_resident_bytes: u64,
    resident_byte_budget: u64,
    spill_budget: SpillBudget,
    run_paths: Vec<PathBuf>,
    indexed_item_count: u64,
    cleanup_paths: bool,
}

impl IndexRunBuilder {
    pub(crate) fn new(
        spec: AcceleratorBuildSpec,
        columns: &[DataColumnSpec],
        limits: FanoutBuildLimits,
        staging_directory: &Path,
        accelerator_ordinal: usize,
        resident_byte_budget: u64,
        spill_budget: SpillBudget,
    ) -> FormatResult<Self> {
        let mut source_ordinals = Vec::with_capacity(spec.key_columns().len());
        let mut key_specs = Vec::with_capacity(spec.key_columns().len());
        for key in spec.key_columns() {
            let source_ordinal = columns
                .iter()
                .position(|column| column.column_id() == key.column_id())
                .ok_or_else(|| invalid_index("accelerator key column is absent"))?;
            source_ordinals.push(source_ordinal);
            key_specs.push(columns[source_ordinal]);
        }
        Ok(Self {
            spec,
            source_ordinals,
            key_specs,
            limits,
            staging_directory: staging_directory.to_path_buf(),
            accelerator_ordinal,
            buffer: Vec::new(),
            buffer_is_sorted: true,
            buffered_bytes: 0,
            buffered_resident_bytes: 0,
            resident_byte_budget,
            spill_budget,
            run_paths: Vec::new(),
            indexed_item_count: 0,
            cleanup_paths: true,
        })
    }

    pub(crate) fn visit(&mut self, values: &[Value], row_ordinal: u64) -> FormatResult<()> {
        let key = match self.spec.kind() {
            IndexAcceleratorKind::Exact => RunKey::Exact(ExactIndexKey::from_projected_values(
                &self.key_specs,
                values,
                &self.source_ordinals,
            )?),
            IndexAcceleratorKind::Ordered => {
                RunKey::Ordered(OrderedIndexKey::from_projected_values(
                    &self.key_specs,
                    values,
                    &self.source_ordinals,
                )?)
            }
            IndexAcceleratorKind::Hnsw => {
                return Err(invalid_index(
                    "HNSW topology requires its dedicated graph builder",
                ));
            }
        };
        self.push_key(key, row_ordinal)
    }

    pub(crate) fn visit_projected(
        &mut self,
        columns: &[Option<Vec<Value>>],
        relative_row: usize,
        row_ordinal: u64,
    ) -> FormatResult<()> {
        // Most physical indexes have one or two key columns. Keep that common
        // projection entirely inline: the previous temporary Vec allocated
        // once per source row and then OrderedIndexKey copied it into its own
        // SmallVec, creating millions of allocator round-trips during bulk
        // index publication.
        let key_values = self
            .source_ordinals
            .iter()
            .map(|ordinal| {
                columns
                    .get(*ordinal)
                    .and_then(Option::as_ref)
                    .and_then(|values| values.get(relative_row))
                    .cloned()
                    .ok_or_else(|| invalid_index("rebuild projection is missing an index key"))
            })
            .collect::<FormatResult<SmallVec<[Value; 2]>>>()?;
        let key = self.encode_key(&key_values)?;
        self.push_key(key, row_ordinal)
    }

    pub(crate) fn source_ordinals(&self) -> &[usize] {
        &self.source_ordinals
    }

    fn encode_key(&self, key_values: &[Value]) -> FormatResult<RunKey> {
        match self.spec.kind() {
            IndexAcceleratorKind::Exact => Ok(RunKey::Exact(ExactIndexKey::from_column_specs(
                &self.key_specs,
                key_values,
            )?)),
            IndexAcceleratorKind::Ordered => Ok(RunKey::Ordered(
                OrderedIndexKey::from_column_specs(&self.key_specs, key_values)?,
            )),
            IndexAcceleratorKind::Hnsw => Err(invalid_index(
                "HNSW topology requires its dedicated graph builder",
            )),
        }
    }

    fn push_key(&mut self, key: RunKey, row_ordinal: u64) -> FormatResult<()> {
        if self.spec.kind() == IndexAcceleratorKind::Exact
            && self.spec.unique()
            && key.has_null_component()
        {
            return Ok(());
        }
        let record_bytes = (RUN_RECORD_HEADER_BYTES as u64)
            .checked_add(key.bytes().len() as u64)
            .ok_or_else(|| invalid_index("sort-run record length overflows"))?;
        if record_bytes > self.limits.sort_run_bytes() {
            return Err(limit_index(
                "sort-run record bytes",
                record_bytes,
                self.limits.sort_run_bytes(),
            ));
        }
        let resident_bytes = (std::mem::size_of::<RunRecord>() as u64)
            .checked_add(key.bytes().len() as u64)
            .ok_or_else(|| invalid_index("sort-run resident record length overflows"))?;
        if resident_bytes > self.resident_byte_budget {
            return Err(limit_index(
                "accelerator resident bytes",
                resident_bytes,
                self.resident_byte_budget,
            ));
        }
        if !self.buffer.is_empty()
            && (self.buffer.len() as u32 >= self.limits.sort_run_records()
                || self.buffered_bytes + record_bytes > self.limits.sort_run_bytes()
                || self.buffered_resident_bytes + resident_bytes > self.resident_byte_budget)
        {
            self.flush()?;
        }
        let record = RunRecord { key, row_ordinal };
        if self.buffer_is_sorted
            && self.buffer.last().is_some_and(|previous| {
                previous.compare(&record, self.spec.key_columns()) == Ordering::Greater
            })
        {
            self.buffer_is_sorted = false;
        }
        self.buffer.push(record);
        self.buffered_bytes += record_bytes;
        self.buffered_resident_bytes += resident_bytes;
        self.indexed_item_count = self
            .indexed_item_count
            .checked_add(1)
            .ok_or_else(|| invalid_index("indexed item count overflows"))?;
        Ok(())
    }

    fn flush(&mut self) -> FormatResult<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        if self.run_paths.len() as u32 >= self.limits.max_sort_run_files() {
            return Err(limit_index(
                "sort-run files",
                self.run_paths.len() as u64 + 1,
                u64::from(self.limits.max_sort_run_files()),
            ));
        }
        self.sort_buffer();
        let run_bytes = (RUN_HEADER_BYTES as u64)
            .checked_add(self.buffered_bytes)
            .ok_or_else(|| invalid_index("sort-run file length overflows"))?;
        self.spill_budget.reserve(run_bytes)?;
        let path = self.staging_directory.join(format!(
            "index-sort-{}-{:08}.run",
            self.accelerator_ordinal,
            self.run_paths.len()
        ));
        let _descriptor_permit = acquire_sort_run_descriptors(1)?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| io_error("create sort run", error))?;
        // From this point the builder owns the path even if a later write or
        // flush fails; Drop must not leave a partial run behind.
        self.run_paths.push(path);
        let mut writer = BufWriter::new(file);
        write_run_header(&mut writer, self.spec.kind(), self.buffer.len() as u64)?;
        for record in &self.buffer {
            write_run_record(&mut writer, record)?;
        }
        writer
            .flush()
            .map_err(|error| io_error("flush sort run", error))?;
        self.buffer.clear();
        self.buffer_is_sorted = true;
        self.buffered_bytes = 0;
        self.buffered_resident_bytes = 0;
        Ok(())
    }

    fn sort_buffer(&mut self) {
        if !self.buffer_is_sorted {
            // The comparator includes row ordinal after the logical key, so
            // every record has a total deterministic order. A stable sort
            // only allocates and copies an unnecessary second record buffer.
            self.buffer
                .sort_unstable_by(|left, right| left.compare(right, self.spec.key_columns()));
            self.buffer_is_sorted = true;
        }
    }

    fn consolidate_runs(&mut self) -> FormatResult<()> {
        let fan_in = self.limits.merge_fan_in() as usize;
        let mut pass = 0_u32;
        while self.run_paths.len() > fan_in {
            let source_paths = std::mem::take(&mut self.run_paths);
            let output_count = source_paths.len().div_ceil(fan_in);
            let mut output_paths = Vec::with_capacity(output_count);
            let merge_result = (|| {
                for (group_ordinal, paths) in source_paths.chunks(fan_in).enumerate() {
                    let output_path = self.staging_directory.join(format!(
                        "index-sort-{}-merge-{pass:04}-{group_ordinal:08}.run",
                        self.accelerator_ordinal
                    ));
                    merge_sorted_runs(
                        paths,
                        &output_path,
                        self.spec.kind(),
                        self.key_specs.clone(),
                        self.spec.key_columns().to_vec(),
                        &self.spill_budget,
                    )?;
                    output_paths.push(output_path);
                }
                for path in &source_paths {
                    std::fs::remove_file(path)
                        .map_err(|error| io_error("remove merged sort-run input", error))?;
                }
                Ok(())
            })();
            if let Err(error) = merge_result {
                self.run_paths = source_paths;
                self.run_paths.extend(output_paths);
                return Err(error);
            }
            record_diagnostic(DiagnosticEvent::SortMergePass, 1);
            record_diagnostic(
                DiagnosticEvent::SortMergeInputRun,
                source_paths.len() as u64,
            );
            self.run_paths = output_paths;
            pass = pass
                .checked_add(1)
                .ok_or_else(|| invalid_index("sort-run merge pass count overflows"))?;
        }
        Ok(())
    }

    pub(crate) fn prepare(mut self, source_row_count: u64) -> FormatResult<PreparedIndexRuns> {
        let page_bytes = (self.resident_byte_budget / 4).max(40);
        if self.indexed_item_count == 0 {
            if self.spec.kind() != IndexAcceleratorKind::Exact
                || !self.spec.unique()
                || !self.key_specs.iter().any(|column| column.nullable())
            {
                return Err(invalid_index(
                    "zero-entry accelerator requires nullable UNIQUE exact key",
                ));
            }
            record_diagnostic(DiagnosticEvent::IndexPlanningPass, 1);
            record_diagnostic(DiagnosticEvent::IndexEncodingPass, 1);
            let pages = collect_exact_pages(
                std::iter::empty(),
                true,
                source_row_count,
                self.spec.page_codec(),
                self.spec.bounded_exact_page_limits(page_bytes)?,
                self.resident_byte_budget,
            )?;
            return Ok(PreparedIndexRuns {
                spec: self.spec.clone(),
                key_specs: std::mem::take(&mut self.key_specs),
                indexed_item_count: 0,
                source_row_count,
                page_count: pages.len() as u64,
                maximum_fragment_rows: self
                    .spec
                    .maximum_posting_fragment_rows(self.resident_byte_budget),
                page_logical_bytes: page_bytes,
                storage: PreparedRunStorage::Pages(pages),
            });
        }
        let maximum_fragment_rows = self
            .spec
            .maximum_posting_fragment_rows(self.resident_byte_budget);
        let (storage, page_count) = if self.run_paths.is_empty() {
            self.sort_buffer();
            let records = std::mem::take(&mut self.buffer);
            record_diagnostic(DiagnosticEvent::IndexPlanningPass, 1);
            record_diagnostic(DiagnosticEvent::IndexEncodingPass, 1);
            let merger = MergedGroups::memory(
                records,
                self.spec.key_columns().to_vec(),
                maximum_fragment_rows,
                MergePurpose::PlanAndEncode,
            );
            let pages = match self.spec.kind() {
                IndexAcceleratorKind::Exact => collect_exact_pages(
                    ExactMergedEntries::new(merger),
                    self.spec.unique(),
                    source_row_count,
                    self.spec.page_codec(),
                    self.spec.bounded_exact_page_limits(page_bytes)?,
                    self.resident_byte_budget,
                )?,
                IndexAcceleratorKind::Ordered => collect_ordered_pages(
                    OrderedMergedEntries::new(merger),
                    self.spec.key_columns(),
                    self.spec.unique(),
                    source_row_count,
                    self.spec.page_codec(),
                    self.spec.bounded_ordered_page_limits(page_bytes)?,
                    self.resident_byte_budget,
                )?,
                IndexAcceleratorKind::Hnsw => unreachable!("HNSW is rejected at visit"),
            };
            let page_count = pages.len() as u64;
            (PreparedRunStorage::Pages(pages), page_count)
        } else {
            self.flush()?;
            self.consolidate_runs()?;
            let paths = std::mem::take(&mut self.run_paths);
            self.cleanup_paths = false;
            let storage = PreparedRunStorage::Files(RunFiles {
                paths,
                merge_fan_in: self.limits.merge_fan_in() as usize,
            });
            record_diagnostic(DiagnosticEvent::IndexPlanningPass, 1);
            let merger = MergedGroups::open_files(
                &storage,
                self.spec.kind(),
                self.key_specs.clone(),
                self.spec.key_columns().to_vec(),
                maximum_fragment_rows,
                MergePurpose::Plan,
            )?;
            let page_count = match self.spec.kind() {
                IndexAcceleratorKind::Exact => count_exact_index_pages_from_sorted(
                    ExactMergedEntries::new(merger),
                    self.spec.unique(),
                    source_row_count,
                    self.spec.bounded_exact_page_limits(page_bytes)?,
                )?,
                IndexAcceleratorKind::Ordered => count_ordered_index_pages_from_sorted(
                    OrderedMergedEntries::new(merger),
                    self.spec.key_columns(),
                    self.spec.unique(),
                    source_row_count,
                    self.spec.bounded_ordered_page_limits(page_bytes)?,
                )?,
                IndexAcceleratorKind::Hnsw => unreachable!("HNSW is rejected at visit"),
            };
            (storage, page_count)
        };
        Ok(PreparedIndexRuns {
            spec: self.spec.clone(),
            key_specs: std::mem::take(&mut self.key_specs),
            indexed_item_count: self.indexed_item_count,
            source_row_count,
            page_count,
            maximum_fragment_rows,
            page_logical_bytes: page_bytes,
            storage,
        })
    }
}

impl Drop for IndexRunBuilder {
    fn drop(&mut self) {
        if self.cleanup_paths {
            cleanup_paths(&self.run_paths);
        }
    }
}

#[derive(Debug)]
pub(crate) struct PreparedIndexRuns {
    spec: AcceleratorBuildSpec,
    key_specs: Vec<DataColumnSpec>,
    indexed_item_count: u64,
    source_row_count: u64,
    page_count: u64,
    maximum_fragment_rows: u64,
    page_logical_bytes: u64,
    storage: PreparedRunStorage,
}

impl IndexPageSource for PreparedIndexRuns {
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

    fn key_columns(&self) -> &[IndexKeyColumn] {
        self.spec.key_columns()
    }

    fn indexed_item_count(&self) -> u64 {
        self.indexed_item_count
    }

    fn page_count(&self) -> u64 {
        self.page_count
    }

    fn visit_pages(
        &self,
        visitor: &mut dyn FnMut(&IndexPageSpec) -> FormatResult<()>,
    ) -> FormatResult<u64> {
        if let PreparedRunStorage::Pages(pages) = &self.storage {
            for page in pages {
                visitor(page)?;
            }
            return Ok(pages.len() as u64);
        }
        record_diagnostic(DiagnosticEvent::IndexEncodingPass, 1);
        let merger = MergedGroups::open_files(
            &self.storage,
            self.spec.kind(),
            self.key_specs.clone(),
            self.spec.key_columns().to_vec(),
            self.maximum_fragment_rows,
            MergePurpose::Encode,
        )?;
        match self.spec.kind() {
            IndexAcceleratorKind::Exact => visit_exact_index_pages_from_sorted(
                ExactMergedEntries::new(merger),
                self.spec.unique(),
                self.source_row_count,
                self.spec.page_codec(),
                self.spec
                    .bounded_exact_page_limits(self.page_logical_bytes)?,
                &mut |page| visitor(&page),
            ),
            IndexAcceleratorKind::Ordered => visit_ordered_index_pages_from_sorted(
                OrderedMergedEntries::new(merger),
                self.spec.key_columns(),
                self.spec.unique(),
                self.source_row_count,
                self.spec.page_codec(),
                self.spec
                    .bounded_ordered_page_limits(self.page_logical_bytes)?,
                &mut |page| visitor(&page),
            ),
            IndexAcceleratorKind::Hnsw => Err(invalid_index("HNSW cannot use sorted posting runs")),
        }
    }
}

fn collect_exact_pages(
    entries: impl IntoIterator<Item = FormatResult<ExactIndexEntry>>,
    unique: bool,
    source_row_count: u64,
    codec: IndexPageCodec,
    limits: ExactPageBuildLimits,
    resident_limit: u64,
) -> FormatResult<Vec<IndexPageSpec>> {
    let mut pages = Vec::new();
    let mut resident_bytes = 0_u64;
    visit_exact_index_pages_from_sorted(
        entries,
        unique,
        source_row_count,
        codec,
        limits,
        &mut |page| retain_page(&mut pages, &mut resident_bytes, resident_limit, page),
    )?;
    Ok(pages)
}

fn collect_ordered_pages(
    entries: impl IntoIterator<Item = FormatResult<OrderedIndexEntry>>,
    key_columns: &[IndexKeyColumn],
    unique: bool,
    source_row_count: u64,
    codec: IndexPageCodec,
    limits: OrderedPageBuildLimits,
    resident_limit: u64,
) -> FormatResult<Vec<IndexPageSpec>> {
    let mut pages = Vec::new();
    let mut resident_bytes = 0_u64;
    visit_ordered_index_pages_from_sorted(
        entries,
        key_columns,
        unique,
        source_row_count,
        codec,
        limits,
        &mut |page| retain_page(&mut pages, &mut resident_bytes, resident_limit, page),
    )?;
    Ok(pages)
}

fn retain_page(
    pages: &mut Vec<IndexPageSpec>,
    resident_bytes: &mut u64,
    resident_limit: u64,
    page: IndexPageSpec,
) -> FormatResult<()> {
    let bytes = (std::mem::size_of::<IndexPageSpec>() as u64)
        .checked_add(page.logical_bytes().len() as u64)
        .ok_or_else(|| invalid_index("accelerator page resident bytes overflow"))?;
    let next = resident_bytes
        .checked_add(bytes)
        .ok_or_else(|| invalid_index("accelerator page resident bytes overflow"))?;
    if next > resident_limit {
        return Err(limit_index(
            "accelerator page resident bytes",
            next,
            resident_limit,
        ));
    }
    pages.push(page);
    *resident_bytes = next;
    Ok(())
}

#[derive(Debug)]
struct RunCursor {
    reader: BufReader<File>,
    remaining: u64,
    kind: IndexAcceleratorKind,
    key_specs: Arc<[DataColumnSpec]>,
}

impl RunCursor {
    fn open(
        path: &Path,
        kind: IndexAcceleratorKind,
        key_specs: Arc<[DataColumnSpec]>,
    ) -> FormatResult<Self> {
        let file = File::open(path).map_err(|error| io_error("open sort run", error))?;
        let mut reader = BufReader::new(file);
        let mut header = [0_u8; RUN_HEADER_BYTES];
        reader
            .read_exact(&mut header)
            .map_err(|error| io_error("read sort-run header", error))?;
        record_diagnostic(DiagnosticEvent::SortRunRead, header.len() as u64);
        if header[..8] != RUN_MAGIC
            || u16::from_le_bytes(header[16..18].try_into().expect("fixed field")) != kind.tag()
            || header[18..20] != [0, 0]
            || u32::from_le_bytes(header[20..24].try_into().expect("fixed field"))
                != radixdb_core::crc32_ieee(&header[..20])
        {
            return Err(invalid_index("sort-run header is invalid"));
        }
        let remaining = u64::from_le_bytes(header[8..16].try_into().expect("fixed field"));
        if remaining == 0 {
            return Err(invalid_index("sort run is empty"));
        }
        Ok(Self {
            reader,
            remaining,
            kind,
            key_specs,
        })
    }

    fn next_record(&mut self) -> FormatResult<Option<RunRecord>> {
        if self.remaining == 0 {
            let mut trailing = [0_u8; 1];
            match self.reader.read(&mut trailing) {
                Ok(length) => {
                    record_diagnostic(DiagnosticEvent::SortRunRead, length as u64);
                    return if length == 0 {
                        Ok(None)
                    } else {
                        Err(invalid_index("sort run has trailing bytes"))
                    };
                }
                Err(error) => return Err(io_error("check sort-run end", error)),
            }
        }
        let mut header = [0_u8; RUN_RECORD_HEADER_BYTES];
        self.reader
            .read_exact(&mut header)
            .map_err(|error| io_error("read sort-run record", error))?;
        record_diagnostic(DiagnosticEvent::SortRunRead, header.len() as u64);
        let key_length = u32::from_le_bytes(header[..4].try_into().expect("fixed field")) as usize;
        let flags = u32::from_le_bytes(header[4..8].try_into().expect("fixed field"));
        if key_length == 0
            || key_length as u64 > MAX_INDEX_KEY_BYTES
            || flags & !1 != 0
            || header[20..24] != [0; 4]
        {
            return Err(invalid_index("sort-run record header is invalid"));
        }
        let row_ordinal = u64::from_le_bytes(header[8..16].try_into().expect("fixed field"));
        let expected_crc = u32::from_le_bytes(header[16..20].try_into().expect("fixed field"));
        let mut bytes = vec![0_u8; key_length];
        self.reader
            .read_exact(&mut bytes)
            .map_err(|error| io_error("read sort-run key", error))?;
        record_diagnostic(DiagnosticEvent::SortRunRead, bytes.len() as u64);
        if radixdb_core::crc32_ieee(&bytes) != expected_crc {
            return Err(invalid_index("sort-run key checksum mismatch"));
        }
        let key = match self.kind {
            IndexAcceleratorKind::Exact => {
                RunKey::Exact(ExactIndexKey::from_canonical_parts(bytes, flags == 1)?)
            }
            IndexAcceleratorKind::Ordered => RunKey::Ordered(
                OrderedIndexKey::from_canonical_bytes(bytes, &self.key_specs)?,
            ),
            IndexAcceleratorKind::Hnsw => {
                return Err(invalid_index("HNSW cannot use sorted posting runs"));
            }
        };
        if key.has_null_component() != (flags == 1) {
            return Err(invalid_index("sort-run NULL flag differs from key bytes"));
        }
        self.remaining -= 1;
        Ok(Some(RunRecord { key, row_ordinal }))
    }
}

#[derive(Debug)]
struct HeapItem {
    record: RunRecord,
    run_ordinal: usize,
    key_columns: Arc<[IndexKeyColumn]>,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.record.compare(&other.record, &self.key_columns) == Ordering::Equal
            && self.run_ordinal == other.run_ordinal
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .record
            .compare(&self.record, &self.key_columns)
            .then_with(|| other.run_ordinal.cmp(&self.run_ordinal))
    }
}

#[derive(Debug)]
struct RunFiles {
    paths: Vec<PathBuf>,
    merge_fan_in: usize,
}

#[derive(Debug)]
enum PreparedRunStorage {
    Pages(Vec<IndexPageSpec>),
    Files(RunFiles),
}

impl Drop for RunFiles {
    fn drop(&mut self) {
        cleanup_paths(&self.paths);
    }
}

#[derive(Debug)]
struct RunMerger {
    _descriptor_permit: InFlightBytePermit,
    cursors: Vec<RunCursor>,
    heap: BinaryHeap<HeapItem>,
    key_columns: Arc<[IndexKeyColumn]>,
    record_count: u64,
    maximum_fragment_rows: u64,
    continuation: Option<(RunKey, u64)>,
    purpose: MergePurpose,
}

#[derive(Debug, Clone, Copy)]
enum MergePurpose {
    Plan,
    Encode,
    PlanAndEncode,
}

enum MergedGroups {
    Memory(MemoryGroups),
    Files(RunMerger),
}

struct PostingGroup {
    key: RunKey,
    rows: SmallVec<[u64; 4]>,
    fragment_state: PostingFragmentState,
}

impl MergedGroups {
    fn memory(
        records: Vec<RunRecord>,
        key_columns: Vec<IndexKeyColumn>,
        maximum_fragment_rows: u64,
        purpose: MergePurpose,
    ) -> Self {
        Self::Memory(MemoryGroups {
            records: records.into_iter().peekable(),
            key_columns,
            maximum_fragment_rows,
            continuation: None,
            purpose,
        })
    }

    fn open_files(
        storage: &PreparedRunStorage,
        kind: IndexAcceleratorKind,
        key_specs: Vec<DataColumnSpec>,
        key_columns: Vec<IndexKeyColumn>,
        maximum_fragment_rows: u64,
        purpose: MergePurpose,
    ) -> FormatResult<Self> {
        match storage {
            PreparedRunStorage::Files(files) => {
                if files.paths.len() > files.merge_fan_in {
                    return Err(limit_index(
                        "sort-run merge fan-in",
                        files.paths.len() as u64,
                        files.merge_fan_in as u64,
                    ));
                }
                RunMerger::open(
                    &files.paths,
                    kind,
                    key_specs,
                    key_columns,
                    maximum_fragment_rows,
                    purpose,
                )
                .map(Self::Files)
            }
            PreparedRunStorage::Pages(_) => Err(invalid_index(
                "pre-encoded index pages cannot be reopened as sort runs",
            )),
        }
    }

    fn next_group(&mut self) -> FormatResult<Option<PostingGroup>> {
        match self {
            Self::Memory(groups) => groups.next_group(),
            Self::Files(groups) => groups.next_group(),
        }
    }
}

struct MemoryGroups {
    records: std::iter::Peekable<std::vec::IntoIter<RunRecord>>,
    key_columns: Vec<IndexKeyColumn>,
    maximum_fragment_rows: u64,
    continuation: Option<(RunKey, u64)>,
    purpose: MergePurpose,
}

impl MemoryGroups {
    fn next_group(&mut self) -> FormatResult<Option<PostingGroup>> {
        let Some(first) = self.records.next() else {
            return if self.continuation.is_some() {
                Err(invalid_index("in-memory posting continuation is truncated"))
            } else {
                Ok(None)
            };
        };
        let key = first.key;
        let has_previous = if let Some((expected_key, previous_row)) = self.continuation.take() {
            if expected_key.compare(&key, &self.key_columns)? != Ordering::Equal {
                return Err(invalid_index("in-memory posting continuation changed key"));
            }
            if previous_row >= first.row_ordinal {
                return Err(invalid_index(
                    "in-memory posting continuation rows are not strictly increasing",
                ));
            }
            true
        } else {
            false
        };
        let mut rows = SmallVec::new();
        rows.push(first.row_ordinal);
        while let Some(record) = self.records.peek() {
            if key.compare(&record.key, &self.key_columns)? != Ordering::Equal {
                break;
            }
            if rows.len() as u64 >= self.maximum_fragment_rows {
                break;
            }
            if rows
                .last()
                .is_some_and(|previous| *previous >= record.row_ordinal)
            {
                return Err(invalid_index(
                    "in-memory merge produced duplicate or unordered row ordinals",
                ));
            }
            rows.push(
                self.records
                    .next()
                    .expect("peeked in-memory record remains available")
                    .row_ordinal,
            );
        }
        let has_next = self
            .records
            .peek()
            .map(|record| key.compare(&record.key, &self.key_columns))
            .transpose()?
            .is_some_and(|ordering| ordering == Ordering::Equal);
        if has_next {
            self.continuation = Some((
                key.clone(),
                *rows.last().expect("posting fragment has its first row"),
            ));
        }
        let fragment_state = PostingFragmentState::new(has_previous, has_next);
        record_logical_posting(self.purpose, fragment_state);
        Ok(Some(PostingGroup {
            key,
            rows,
            fragment_state,
        }))
    }
}

impl RunMerger {
    fn open(
        paths: &[PathBuf],
        kind: IndexAcceleratorKind,
        key_specs: Vec<DataColumnSpec>,
        key_columns: Vec<IndexKeyColumn>,
        maximum_fragment_rows: u64,
        purpose: MergePurpose,
    ) -> FormatResult<Self> {
        if paths.is_empty() {
            return Err(invalid_index("accelerator has no sorted runs"));
        }
        let descriptor_permit = acquire_sort_run_descriptors(paths.len())?;
        Self::open_with_permit(
            paths,
            kind,
            key_specs,
            key_columns,
            maximum_fragment_rows,
            purpose,
            descriptor_permit,
        )
    }

    fn open_for_rewrite(
        paths: &[PathBuf],
        kind: IndexAcceleratorKind,
        key_specs: Vec<DataColumnSpec>,
        key_columns: Vec<IndexKeyColumn>,
    ) -> FormatResult<Self> {
        if paths.is_empty() {
            return Err(invalid_index("accelerator has no sorted runs"));
        }
        let descriptors = paths
            .len()
            .checked_add(1)
            .ok_or_else(|| invalid_index("sort-run descriptor count overflows"))?;
        let descriptor_permit = acquire_sort_run_descriptors(descriptors)?;
        Self::open_with_permit(
            paths,
            kind,
            key_specs,
            key_columns,
            u64::MAX,
            MergePurpose::Plan,
            descriptor_permit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn open_with_permit(
        paths: &[PathBuf],
        kind: IndexAcceleratorKind,
        key_specs: Vec<DataColumnSpec>,
        key_columns: Vec<IndexKeyColumn>,
        maximum_fragment_rows: u64,
        purpose: MergePurpose,
        descriptor_permit: InFlightBytePermit,
    ) -> FormatResult<Self> {
        if paths.is_empty() {
            return Err(invalid_index("accelerator has no sorted runs"));
        }
        let key_specs: Arc<[DataColumnSpec]> = key_specs.into();
        let key_columns: Arc<[IndexKeyColumn]> = key_columns.into();
        let mut cursors = Vec::with_capacity(paths.len());
        let mut heap = BinaryHeap::with_capacity(paths.len());
        let mut record_count = 0_u64;
        for (run_ordinal, path) in paths.iter().enumerate() {
            let mut cursor = RunCursor::open(path, kind, Arc::clone(&key_specs))?;
            record_count = record_count
                .checked_add(cursor.remaining)
                .ok_or_else(|| invalid_index("sort-run record count overflows"))?;
            let record = cursor
                .next_record()?
                .ok_or_else(|| invalid_index("sort run lost its first record"))?;
            heap.push(HeapItem {
                record,
                run_ordinal,
                key_columns: Arc::clone(&key_columns),
            });
            cursors.push(cursor);
        }
        Ok(Self {
            _descriptor_permit: descriptor_permit,
            cursors,
            heap,
            key_columns,
            record_count,
            maximum_fragment_rows,
            purpose,
            continuation: None,
        })
    }

    const fn record_count(&self) -> u64 {
        self.record_count
    }

    fn pop_record(&mut self) -> FormatResult<Option<RunRecord>> {
        let Some(item) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some(record) = self.cursors[item.run_ordinal].next_record()? {
            self.heap.push(HeapItem {
                record,
                run_ordinal: item.run_ordinal,
                key_columns: Arc::clone(&self.key_columns),
            });
        }
        Ok(Some(item.record))
    }

    fn next_group(&mut self) -> FormatResult<Option<PostingGroup>> {
        let Some(first) = self.pop_record()? else {
            return if self.continuation.is_some() {
                Err(invalid_index("sort-run posting continuation is truncated"))
            } else {
                Ok(None)
            };
        };
        let key = first.key;
        let has_previous = if let Some((expected_key, previous_row)) = self.continuation.take() {
            if expected_key.compare(&key, &self.key_columns)? != Ordering::Equal {
                return Err(invalid_index("sort-run posting continuation changed key"));
            }
            if previous_row >= first.row_ordinal {
                return Err(invalid_index(
                    "sort-run posting continuation rows are not strictly increasing",
                ));
            }
            true
        } else {
            false
        };
        let mut rows = SmallVec::new();
        rows.push(first.row_ordinal);
        while let Some(next) = self.heap.peek() {
            if key.compare(&next.record.key, &self.key_columns)? != Ordering::Equal {
                break;
            }
            if rows.len() as u64 >= self.maximum_fragment_rows {
                break;
            }
            let record = self
                .pop_record()?
                .expect("peeked merge record remains available");
            if rows
                .last()
                .is_some_and(|previous| *previous >= record.row_ordinal)
            {
                return Err(invalid_index(
                    "sort-run merge produced duplicate or unordered row ordinals",
                ));
            }
            rows.push(record.row_ordinal);
        }
        let has_next = self
            .heap
            .peek()
            .map(|record| key.compare(&record.record.key, &self.key_columns))
            .transpose()?
            .is_some_and(|ordering| ordering == Ordering::Equal);
        if has_next {
            self.continuation = Some((
                key.clone(),
                *rows.last().expect("posting fragment has its first row"),
            ));
        }
        let fragment_state = PostingFragmentState::new(has_previous, has_next);
        record_logical_posting(self.purpose, fragment_state);
        Ok(Some(PostingGroup {
            key,
            rows,
            fragment_state,
        }))
    }
}

fn record_logical_posting(purpose: MergePurpose, fragment_state: PostingFragmentState) {
    if fragment_state.has_previous() {
        return;
    }
    match purpose {
        MergePurpose::Plan => record_diagnostic(DiagnosticEvent::PostingPlan, 1),
        MergePurpose::Encode => record_diagnostic(DiagnosticEvent::PostingEncode, 1),
        MergePurpose::PlanAndEncode => {
            record_diagnostic(DiagnosticEvent::PostingPlan, 1);
            record_diagnostic(DiagnosticEvent::PostingEncode, 1);
        }
    }
}

fn merge_sorted_runs(
    paths: &[PathBuf],
    output_path: &Path,
    kind: IndexAcceleratorKind,
    key_specs: Vec<DataColumnSpec>,
    key_columns: Vec<IndexKeyColumn>,
    spill_budget: &SpillBudget,
) -> FormatResult<()> {
    let output_bytes = merged_run_file_bytes(paths)?;
    spill_budget.reserve(output_bytes)?;
    let mut merger = RunMerger::open_for_rewrite(paths, kind, key_specs, key_columns)?;
    let record_count = merger.record_count();
    let mut output_created = false;
    let result = (|| {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output_path)
            .map_err(|error| io_error("create merged sort run", error))?;
        output_created = true;
        let mut writer = BufWriter::new(file);
        write_run_header(&mut writer, kind, record_count)?;
        let mut written = 0_u64;
        while let Some(record) = merger.pop_record()? {
            write_run_record(&mut writer, &record)?;
            written = written
                .checked_add(1)
                .ok_or_else(|| invalid_index("merged sort-run record count overflows"))?;
        }
        if written != record_count {
            return Err(invalid_index("merged sort run lost records"));
        }
        writer
            .flush()
            .map_err(|error| io_error("flush merged sort run", error))?;
        let actual_bytes = writer
            .get_ref()
            .metadata()
            .map_err(|error| io_error("inspect merged sort run", error))?
            .len();
        if actual_bytes != output_bytes {
            return Err(invalid_index("merged sort-run length differs from inputs"));
        }
        Ok(())
    })();
    if result.is_err() && output_created {
        let _ = std::fs::remove_file(output_path);
    }
    result
}

fn merged_run_file_bytes(paths: &[PathBuf]) -> FormatResult<u64> {
    paths
        .iter()
        .try_fold(RUN_HEADER_BYTES as u64, |total, path| {
            let bytes = std::fs::metadata(path)
                .map_err(|error| io_error("inspect sort run", error))?
                .len();
            let payload = bytes
                .checked_sub(RUN_HEADER_BYTES as u64)
                .ok_or_else(|| invalid_index("sort run is shorter than its header"))?;
            total
                .checked_add(payload)
                .ok_or_else(|| invalid_index("merged sort-run length overflows"))
        })
}

struct ExactMergedEntries {
    merger: MergedGroups,
    finished: bool,
}

impl ExactMergedEntries {
    fn new(merger: MergedGroups) -> Self {
        Self {
            merger,
            finished: false,
        }
    }
}

impl Iterator for ExactMergedEntries {
    type Item = FormatResult<ExactIndexEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match self.merger.next_group() {
            Ok(Some(PostingGroup {
                key: RunKey::Exact(key),
                rows,
                fragment_state,
            })) => Some(ExactIndexEntry::from_fragment_rows(
                key,
                rows,
                fragment_state,
            )),
            Ok(Some(_)) => {
                self.finished = true;
                Some(Err(invalid_index("exact merge received ordered key")))
            }
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
        }
    }
}

struct OrderedMergedEntries {
    merger: MergedGroups,
    finished: bool,
}

impl OrderedMergedEntries {
    fn new(merger: MergedGroups) -> Self {
        Self {
            merger,
            finished: false,
        }
    }
}

impl Iterator for OrderedMergedEntries {
    type Item = FormatResult<OrderedIndexEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match self.merger.next_group() {
            Ok(Some(PostingGroup {
                key: RunKey::Ordered(key),
                rows,
                fragment_state,
            })) => Some(OrderedIndexEntry::from_fragment_rows(
                key,
                rows,
                fragment_state,
            )),
            Ok(Some(_)) => {
                self.finished = true;
                Some(Err(invalid_index("ordered merge received exact key")))
            }
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
        }
    }
}

fn cleanup_paths(paths: &[PathBuf]) {
    for path in paths {
        let _ = std::fs::remove_file(path);
    }
}

fn io_error(operation: &'static str, error: std::io::Error) -> FormatError {
    FormatError::ArtifactIo {
        operation,
        kind: error.kind(),
    }
}
