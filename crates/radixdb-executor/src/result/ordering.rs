// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! LIMIT, ORDER BY, external sort and bounded Top-N result owners.

use super::*;

/// Limited result that applies LIMIT and OFFSET to an underlying result
pub struct LimitedResult {
    /// Underlying result
    inner: Box<dyn QueryResult>,
    /// Maximum number of rows to return
    limit: Option<usize>,
    /// Number of rows to skip
    offset: usize,
    /// Number of rows returned so far
    returned_count: usize,
    /// Whether we've skipped the offset rows
    offset_applied: bool,
    /// Columns cached
    columns: Vec<String>,
}

impl LimitedResult {
    /// Create a new limited result
    pub fn new(inner: Box<dyn QueryResult>, limit: Option<usize>, offset: usize) -> Self {
        let columns = inner.columns().to_vec();
        Self {
            inner,
            limit,
            offset,
            returned_count: 0,
            offset_applied: false,
            columns,
        }
    }

    /// Create with just a limit
    pub fn with_limit(inner: Box<dyn QueryResult>, limit: usize) -> Self {
        Self::new(inner, Some(limit), 0)
    }

    /// Create with just an offset
    pub fn with_offset(inner: Box<dyn QueryResult>, offset: usize) -> Self {
        Self::new(inner, None, offset)
    }
}

impl QueryResult for LimitedResult {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn columns_arc(&self) -> Option<CompactArc<Vec<String>>> {
        self.inner.columns_arc()
    }

    fn next(&mut self) -> bool {
        // Apply offset first (skip rows)
        if !self.offset_applied {
            for _ in 0..self.offset {
                if !self.inner.next() {
                    self.offset_applied = true;
                    return false;
                }
            }
            self.offset_applied = true;
        }

        // Check limit
        if let Some(limit) = self.limit {
            if self.returned_count >= limit {
                return false;
            }
        }

        // Get next row
        if self.inner.next() {
            self.returned_count += 1;
            true
        } else {
            false
        }
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        self.inner.scan(dest)
    }

    fn row(&self) -> &Row {
        self.inner.row()
    }

    fn take_row(&mut self) -> Row {
        self.inner.take_row()
    }

    fn take_deferred_row(&mut self) -> DeferredRow {
        self.inner.take_deferred_row()
    }

    fn preserves_deferred_rows(&self) -> bool {
        self.inner.preserves_deferred_rows()
    }

    fn ascending_nulls_last_ordering(&self) -> Option<Vec<usize>> {
        self.inner.ascending_nulls_last_ordering()
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    fn rows_affected(&self) -> i64 {
        self.inner.rows_affected()
    }

    fn last_insert_id(&self) -> i64 {
        self.inner.last_insert_id()
    }

    fn last_error(&mut self) -> Option<radixdb_core::Error> {
        self.inner.last_error()
    }

    fn estimated_count(&self) -> Option<usize> {
        self.inner.estimated_count().map(|rows| {
            let after_offset = if self.offset_applied {
                rows
            } else {
                rows.saturating_sub(self.offset)
            };
            self.limit.map_or(after_offset, |limit| {
                after_offset.min(limit.saturating_sub(self.returned_count))
            })
        })
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

/// Ordered result that sorts rows by ORDER BY expressions
pub struct OrderedResult {
    /// In-memory result for a bounded single run, or streaming external merge.
    inner: Box<dyn QueryResult>,
}

// A 64 MiB byte budget is the primary memory boundary. Allow two artifact-backed row
// groups in one run so ordinary 100K result sets do not spill merely because
// of an unrelated 64K row-count ceiling while remaining strictly bounded.
pub(super) const ORDERED_RUN_MAX_ROWS: usize = 131_072;
pub(super) const ORDERED_RUN_MAX_BYTES: usize = 64 * 1024 * 1024;
const ORDERED_SPILL_MAX_VALUE_BYTES: usize = 64 * 1024 * 1024;
static ORDERED_SPILL_ID: AtomicU64 = AtomicU64::new(1);
type OrderedRowComparator = dyn Fn(&Row, &Row) -> std::cmp::Ordering + Send;

enum BoundedOrderedRows {
    Memory {
        rows: RowVec,
        input_rows: usize,
        peak_rows: usize,
        peak_bytes: usize,
    },
    External {
        paths: Vec<PathBuf>,
        input_rows: usize,
        peak_rows: usize,
        peak_bytes: usize,
    },
}

struct OrderedSpillRun {
    path: PathBuf,
    reader: BufReader<File>,
    remaining: u64,
    head: Option<Row>,
}

impl OrderedSpillRun {
    fn open(path: PathBuf) -> Result<Self> {
        let opened = (|| {
            let file = File::open(&path).map_err(|error| {
                Error::internal(format!(
                    "failed to open ORDER BY spill run {}: {error}",
                    path.display()
                ))
            })?;
            let mut reader = BufReader::new(file);
            let remaining = read_u64(&mut reader, "ORDER BY spill row count")?;
            let mut run = Self {
                path: path.clone(),
                reader,
                remaining,
                head: None,
            };
            run.advance()?;
            Ok(run)
        })();
        if opened.is_err() {
            let _ = std::fs::remove_file(path);
        }
        opened
    }

    fn advance(&mut self) -> Result<()> {
        self.head = if self.remaining == 0 {
            None
        } else {
            self.remaining -= 1;
            Some(read_spill_row(&mut self.reader)?)
        };
        Ok(())
    }
}

impl Drop for OrderedSpillRun {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

struct ExternalOrderedResult {
    columns: CompactArc<Vec<String>>,
    runs: Vec<OrderedSpillRun>,
    compare: Box<OrderedRowComparator>,
    current: Option<Row>,
    remaining: usize,
    closed: bool,
    last_error: Option<Error>,
}

impl ExternalOrderedResult {
    fn new<F>(
        columns: Vec<String>,
        paths: Vec<PathBuf>,
        input_rows: usize,
        compare: F,
    ) -> Result<Self>
    where
        F: Fn(&Row, &Row) -> std::cmp::Ordering + Send + 'static,
    {
        let mut pending = paths.into_iter();
        let mut runs = Vec::new();
        while let Some(path) = pending.next() {
            match OrderedSpillRun::open(path) {
                Ok(run) => runs.push(run),
                Err(error) => {
                    for path in pending {
                        let _ = std::fs::remove_file(path);
                    }
                    return Err(error);
                }
            }
        }
        Ok(Self {
            columns: CompactArc::new(columns),
            runs,
            compare: Box::new(compare),
            current: None,
            remaining: input_rows,
            closed: false,
            last_error: None,
        })
    }
}

impl QueryResult for ExternalOrderedResult {
    fn columns(&self) -> &[String] {
        &self.columns
    }

    fn columns_arc(&self) -> Option<CompactArc<Vec<String>>> {
        Some(CompactArc::clone(&self.columns))
    }

    fn next(&mut self) -> bool {
        if self.closed || self.last_error.is_some() {
            return false;
        }
        let mut best: Option<usize> = None;
        for (index, run) in self.runs.iter().enumerate() {
            let Some(candidate) = run.head.as_ref() else {
                continue;
            };
            if best.is_none_or(|best_index| {
                let best_row = self.runs[best_index]
                    .head
                    .as_ref()
                    .expect("selected ORDER BY run has a head row");
                (self.compare)(candidate, best_row).is_lt()
            }) {
                best = Some(index);
            }
        }
        let Some(best) = best else {
            self.current = None;
            return false;
        };
        self.current = self.runs[best].head.take();
        if let Err(error) = self.runs[best].advance() {
            self.last_error = Some(error);
            self.current = None;
            return false;
        }
        self.remaining = self.remaining.saturating_sub(1);
        true
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        let row = self.row();
        if dest.len() != row.len() {
            return Err(Error::internal(format!(
                "scan destination has {} values for ORDER BY row with {} columns",
                dest.len(),
                row.len()
            )));
        }
        dest.clone_from_slice(row.as_slice());
        Ok(())
    }

    fn row(&self) -> &Row {
        self.current
            .as_ref()
            .expect("row() called without successful external ORDER BY next()")
    }

    fn take_row(&mut self) -> Row {
        self.current
            .take()
            .expect("take_row() called without successful external ORDER BY next()")
    }

    fn close(&mut self) -> Result<()> {
        self.closed = true;
        self.current = None;
        self.runs.clear();
        Ok(())
    }

    fn rows_affected(&self) -> i64 {
        0
    }

    fn last_insert_id(&self) -> i64 {
        0
    }

    fn last_error(&mut self) -> Option<Error> {
        self.last_error.take()
    }

    fn estimated_count(&self) -> Option<usize> {
        Some(self.remaining)
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

/// Order specification for radix sort
#[derive(Clone, Copy)]
pub struct RadixOrderSpec {
    /// Column index to sort by
    pub col_idx: usize,
    /// Whether to sort ascending
    pub ascending: bool,
    /// NULLS FIRST/LAST specification
    /// None = default (NULLS LAST for ASC, NULLS FIRST for DESC)
    /// Some(true) = NULLS FIRST
    /// Some(false) = NULLS LAST
    pub nulls_first: Option<bool>,
}

fn ordered_spill_path() -> Result<PathBuf> {
    let base = std::env::temp_dir();
    for _ in 0..32 {
        let ordinal = ORDERED_SPILL_ID.fetch_add(1, AtomicOrdering::Relaxed);
        let path = base.join(format!(
            "radixdb-order-{}-{ordinal}.run",
            std::process::id()
        ));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => {
                drop(file);
                return Ok(path);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(Error::internal(format!(
                    "failed to create ORDER BY spill path {}: {error}",
                    path.display()
                )))
            }
        }
    }
    Err(Error::internal(
        "failed to allocate a unique ORDER BY spill path",
    ))
}

fn write_u32(writer: &mut impl Write, value: usize, what: &str) -> Result<()> {
    let value =
        u32::try_from(value).map_err(|_| Error::invalid_argument(format!("{what} exceeds u32")))?;
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|error| Error::internal(format!("failed to write {what}: {error}")))
}

fn write_spill_row(writer: &mut impl Write, row: &Row) -> Result<()> {
    write_u32(writer, row.len(), "ORDER BY spill column count")?;
    let mut encoded = Vec::new();
    for value in row.iter() {
        encoded.clear();
        radixdb_storage::mvcc::persistence::serialize_value_into(&mut encoded, value)?;
        write_u32(writer, encoded.len(), "ORDER BY spill value length")?;
        writer.write_all(&encoded).map_err(|error| {
            Error::internal(format!("failed to write ORDER BY spill value: {error}"))
        })?;
    }
    Ok(())
}

fn read_u32(reader: &mut impl Read, what: &str) -> Result<u32> {
    let mut bytes = [0u8; 4];
    reader
        .read_exact(&mut bytes)
        .map_err(|error| Error::internal(format!("failed to read {what}: {error}")))?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(reader: &mut impl Read, what: &str) -> Result<u64> {
    let mut bytes = [0u8; 8];
    reader
        .read_exact(&mut bytes)
        .map_err(|error| Error::internal(format!("failed to read {what}: {error}")))?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_spill_row(reader: &mut impl Read) -> Result<Row> {
    let columns = read_u32(reader, "ORDER BY spill column count")? as usize;
    if columns > RetainedRowsBudget::DEFAULT_MAX_ROWS {
        return Err(Error::internal(format!(
            "ORDER BY spill row declares {columns} columns"
        )));
    }
    let mut row = Row::with_capacity(columns);
    for _ in 0..columns {
        let value_len = read_u32(reader, "ORDER BY spill value length")? as usize;
        if value_len > ORDERED_SPILL_MAX_VALUE_BYTES {
            return Err(Error::internal(format!(
                "ORDER BY spill value length {value_len} exceeds bounded decoder limit"
            )));
        }
        let mut encoded = vec![0u8; value_len];
        reader.read_exact(&mut encoded).map_err(|error| {
            Error::internal(format!("failed to read ORDER BY spill value: {error}"))
        })?;
        row.push(radixdb_storage::mvcc::persistence::deserialize_value(
            &encoded,
        )?);
    }
    Ok(row)
}

fn write_ordered_run<F>(rows: &mut RowVec, compare: &F) -> Result<PathBuf>
where
    F: Fn(&Row, &Row) -> std::cmp::Ordering,
{
    rows.sort_unstable_by(|(_, left), (_, right)| compare(left, right));
    let path = ordered_spill_path()?;
    let result = (|| {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|error| {
                Error::internal(format!(
                    "failed to open ORDER BY spill run {}: {error}",
                    path.display()
                ))
            })?;
        let mut writer = BufWriter::new(file);
        writer
            .write_all(&(rows.len() as u64).to_le_bytes())
            .map_err(|error| Error::internal(format!("failed to write spill header: {error}")))?;
        for (_, row) in rows.iter() {
            write_spill_row(&mut writer, row)?;
        }
        writer
            .flush()
            .map_err(|error| Error::internal(format!("failed to flush ORDER BY spill: {error}")))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&path);
    }
    result.map(|()| path)
}

fn remove_ordered_runs(paths: impl IntoIterator<Item = PathBuf>) {
    for path in paths {
        let _ = std::fs::remove_file(path);
    }
}

fn collect_bounded_ordered_rows<F>(
    mut inner: Box<dyn QueryResult>,
    compare: &F,
) -> Result<(Vec<String>, BoundedOrderedRows)>
where
    F: Fn(&Row, &Row) -> std::cmp::Ordering,
{
    let columns = inner.columns().to_vec();
    let mut rows = RowVec::with_capacity(ORDERED_RUN_MAX_ROWS.min(1024));
    let mut budget = RetainedRowsBudget::with_limits(
        "bounded ORDER BY run",
        ORDERED_RUN_MAX_ROWS,
        ORDERED_RUN_MAX_BYTES,
    );
    let mut paths = Vec::new();
    let mut input_rows = 0usize;
    let mut peak_rows = 0usize;
    let mut peak_bytes = 0usize;

    while inner.next() {
        let row = inner.take_row();
        if let Err(error) = budget.admit(&row) {
            if rows.is_empty() {
                return Err(error);
            }
            peak_rows = peak_rows.max(budget.peak_rows());
            peak_bytes = peak_bytes.max(budget.peak_bytes());
            let path = match write_ordered_run(&mut rows, compare) {
                Ok(path) => path,
                Err(error) => {
                    remove_ordered_runs(paths);
                    return Err(error);
                }
            };
            paths.push(path);
            rows.clear();
            budget = RetainedRowsBudget::with_limits(
                "bounded ORDER BY run",
                ORDERED_RUN_MAX_ROWS,
                ORDERED_RUN_MAX_BYTES,
            );
            if let Err(error) = budget.admit(&row) {
                remove_ordered_runs(paths);
                return Err(error);
            }
        }
        rows.push((input_rows as i64, row));
        input_rows = input_rows.saturating_add(1);
    }
    if let Some(error) = inner.last_error() {
        remove_ordered_runs(paths);
        return Err(error);
    }
    peak_rows = peak_rows.max(budget.peak_rows());
    peak_bytes = peak_bytes.max(budget.peak_bytes());

    if paths.is_empty() {
        return Ok((
            columns,
            BoundedOrderedRows::Memory {
                rows,
                input_rows,
                peak_rows,
                peak_bytes,
            },
        ));
    }
    if !rows.is_empty() {
        match write_ordered_run(&mut rows, compare) {
            Ok(path) => paths.push(path),
            Err(error) => {
                remove_ordered_runs(paths);
                return Err(error);
            }
        }
    }
    Ok((
        columns,
        BoundedOrderedRows::External {
            paths,
            input_rows,
            peak_rows,
            peak_bytes,
        },
    ))
}

impl OrderedResult {
    /// Create a new ordered result by materializing and sorting the inner result
    pub fn new<F>(inner: Box<dyn QueryResult>, compare: F) -> Result<Self>
    where
        F: Fn(&Row, &Row) -> std::cmp::Ordering + Send + 'static,
    {
        let collect_started = radixdb_core::time_compat::Instant::now();
        let (columns, bounded) = collect_bounded_ordered_rows(inner, &compare)?;
        let collect_elapsed = collect_started.elapsed();
        let inner: Box<dyn QueryResult> = match bounded {
            BoundedOrderedRows::Memory {
                mut rows,
                input_rows,
                peak_rows,
                peak_bytes,
            } => {
                let finalize_started = radixdb_core::time_compat::Instant::now();
                rows.sort_unstable_by(|(_, left), (_, right)| compare(left, right));
                let finalize_elapsed = finalize_started.elapsed();
                radixdb_storage::instrumentation::record_join_ordered_sort(
                    input_rows as u64,
                    0,
                    peak_rows as u64,
                    peak_bytes as u64,
                    collect_elapsed,
                    finalize_elapsed,
                );
                Box::new(ExecutorResult::new(columns, rows))
            }
            BoundedOrderedRows::External {
                paths,
                input_rows,
                peak_rows,
                peak_bytes,
            } => {
                let runs = paths.len();
                let finalize_started = radixdb_core::time_compat::Instant::now();
                let external = ExternalOrderedResult::new(columns, paths, input_rows, compare)?;
                let finalize_elapsed = finalize_started.elapsed();
                radixdb_storage::instrumentation::record_join_ordered_sort(
                    input_rows as u64,
                    runs as u64,
                    peak_rows as u64,
                    peak_bytes as u64,
                    collect_elapsed,
                    finalize_elapsed,
                );
                Box::new(external)
            }
        };
        Ok(Self { inner })
    }

    /// Create an ordered result using radix sort for integer columns
    ///
    /// This is O(n) instead of O(n log n) for comparison-based sort.
    /// For 10K rows, this can be 2-5x faster. For 1M rows, 5-20x faster.
    ///
    /// # Arguments
    /// * `inner` - Source result to materialize and sort
    /// * `order_specs` - Column indices and sort directions (must be integer columns)
    /// * `fallback_compare` - Fallback comparison function if radix sort fails
    pub fn new_radix<F>(
        inner: Box<dyn QueryResult>,
        order_specs: &[RadixOrderSpec],
        fallback_compare: F,
    ) -> Result<Self>
    where
        F: Fn(&Row, &Row) -> std::cmp::Ordering + Send + 'static,
    {
        let collect_started = radixdb_core::time_compat::Instant::now();
        let (columns, bounded) = collect_bounded_ordered_rows(inner, &fallback_compare)?;
        let collect_elapsed = collect_started.elapsed();
        let BoundedOrderedRows::Memory {
            mut rows,
            input_rows,
            peak_rows,
            peak_bytes,
        } = bounded
        else {
            let BoundedOrderedRows::External {
                paths,
                input_rows,
                peak_rows,
                peak_bytes,
            } = bounded
            else {
                unreachable!()
            };
            let runs = paths.len();
            let finalize_started = radixdb_core::time_compat::Instant::now();
            let external =
                ExternalOrderedResult::new(columns, paths, input_rows, fallback_compare)?;
            let finalize_elapsed = finalize_started.elapsed();
            radixdb_storage::instrumentation::record_join_ordered_sort(
                input_rows as u64,
                runs as u64,
                peak_rows as u64,
                peak_bytes as u64,
                collect_elapsed,
                finalize_elapsed,
            );
            return Ok(Self {
                inner: Box::new(external),
            });
        };

        // Check if any column has explicit NULLS FIRST/LAST setting
        // If so, skip radix sort (which uses fixed NULL ordering) and use comparison sort
        let has_explicit_nulls_ordering = order_specs.iter().any(|s| s.nulls_first.is_some());

        if !has_explicit_nulls_ordering {
            // Try radix sort for single integer column (most common case)
            if order_specs.len() == 1 {
                let spec = &order_specs[0];
                let finalize_started = radixdb_core::time_compat::Instant::now();
                if Self::try_radix_sort_single_int(&mut rows, spec.col_idx, spec.ascending) {
                    let finalize_elapsed = finalize_started.elapsed();
                    radixdb_storage::instrumentation::record_join_ordered_sort(
                        input_rows as u64,
                        0,
                        peak_rows as u64,
                        peak_bytes as u64,
                        collect_elapsed,
                        finalize_elapsed,
                    );
                    return Ok(Self {
                        inner: Box::new(ExecutorResult::new(columns, rows)),
                    });
                }
            }

            // Try radix sort for multiple integer columns
            let finalize_started = radixdb_core::time_compat::Instant::now();
            if order_specs.len() <= 4 && Self::try_radix_sort_multi_int(&mut rows, order_specs) {
                let finalize_elapsed = finalize_started.elapsed();
                radixdb_storage::instrumentation::record_join_ordered_sort(
                    input_rows as u64,
                    0,
                    peak_rows as u64,
                    peak_bytes as u64,
                    collect_elapsed,
                    finalize_elapsed,
                );
                return Ok(Self {
                    inner: Box::new(ExecutorResult::new(columns, rows)),
                });
            }
        }

        // Fallback to comparison sort (use sort_unstable_by for better performance)
        let finalize_started = radixdb_core::time_compat::Instant::now();
        rows.sort_unstable_by(|(_, a), (_, b)| fallback_compare(a, b));
        let finalize_elapsed = finalize_started.elapsed();
        radixdb_storage::instrumentation::record_join_ordered_sort(
            input_rows as u64,
            0,
            peak_rows as u64,
            peak_bytes as u64,
            collect_elapsed,
            finalize_elapsed,
        );

        Ok(Self {
            inner: Box::new(ExecutorResult::new(columns, rows)),
        })
    }

    /// Try to sort by a single integer column using radix sort
    /// Returns true if successful, false if column is not all integers
    fn try_radix_sort_single_int(rows: &mut RowVec, col_idx: usize, ascending: bool) -> bool {
        // UUID ordering is a bytewise lexicographic order over the canonical
        // 16-byte payload. Two stable radix passes (low 64 bits, then high
        // 64 bits) preserve that contract without O(n log n) repeated Value
        // dispatch and Arc payload comparisons.
        if Self::try_radix_sort_single_uuid(rows, col_idx, ascending) {
            return true;
        }

        // Check if all values in this column are integers
        for (_, row) in rows.iter() {
            match row.get(col_idx) {
                Some(Value::Integer(value)) if *value != i64::MIN => continue,
                _ => return false, // Non-integer found
            }
        }

        // All integers - use radix sort on (id, Row) tuples
        // We use radsort which handles negative numbers correctly
        if ascending {
            radsort::sort_by_key(rows, |(_, row)| match row.get(col_idx) {
                Some(Value::Integer(i)) => *i,
                _ => unreachable!("radix admission requires non-null integers"),
            });
        } else {
            // For descending, we negate the key (radix sort is ascending only)
            // But we need to be careful with i64::MIN
            radsort::sort_by_key(rows, |(_, row)| {
                match row.get(col_idx) {
                    Some(Value::Integer(i)) => {
                        // Negate for descending order, handle overflow
                        i.wrapping_neg().wrapping_sub(1)
                    }
                    _ => unreachable!("radix admission requires non-null integers"),
                }
            });
        }

        true
    }

    pub(super) fn try_radix_sort_single_uuid(
        rows: &mut RowVec,
        col_idx: usize,
        ascending: bool,
    ) -> bool {
        if rows
            .iter()
            .any(|(_, row)| row.get(col_idx).and_then(Value::as_uuid_bytes).is_none())
        {
            return false;
        }

        let word = |row: &Row, range: std::ops::Range<usize>| {
            let bytes = row
                .get(col_idx)
                .and_then(Value::as_uuid_bytes)
                .expect("UUID radix admission validates every key");
            let word = u64::from_be_bytes(
                bytes[range]
                    .try_into()
                    .expect("UUID radix word is exactly eight bytes"),
            );
            if ascending {
                word
            } else {
                !word
            }
        };

        // LSD radix sort requires the less-significant word first. radsort is
        // stable, so the high-word pass retains the low-word order for ties.
        radsort::sort_by_key(rows, |(_, row)| word(row, 8..16));
        radsort::sort_by_key(rows, |(_, row)| word(row, 0..8));
        true
    }

    /// Try to sort by multiple integer columns using radix sort
    /// This uses a composite key approach for up to 4 columns
    fn try_radix_sort_multi_int(rows: &mut RowVec, order_specs: &[RadixOrderSpec]) -> bool {
        // First verify all columns are integers
        for (_, row) in rows.iter() {
            for spec in order_specs {
                match row.get(spec.col_idx) {
                    Some(Value::Integer(value)) if *value != i64::MIN => continue,
                    _ => return false,
                }
            }
        }

        // For multi-column sort, we need to sort in reverse order of priority
        // (least significant column first, most significant last)
        // This is stable, so later sorts preserve order from earlier ones
        for spec in order_specs.iter().rev() {
            if spec.ascending {
                radsort::sort_by_key(rows, |(_, row)| match row.get(spec.col_idx) {
                    Some(Value::Integer(i)) => *i,
                    _ => unreachable!("radix admission requires non-null integers"),
                });
            } else {
                radsort::sort_by_key(rows, |(_, row)| match row.get(spec.col_idx) {
                    Some(Value::Integer(i)) => i.wrapping_neg().wrapping_sub(1),
                    _ => unreachable!("radix admission requires non-null integers"),
                });
            }
        }

        true
    }
}

impl QueryResult for OrderedResult {
    fn columns(&self) -> &[String] {
        self.inner.columns()
    }

    fn columns_arc(&self) -> Option<CompactArc<Vec<String>>> {
        self.inner.columns_arc()
    }

    fn next(&mut self) -> bool {
        self.inner.next()
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        self.inner.scan(dest)
    }

    fn row(&self) -> &Row {
        self.inner.row()
    }

    fn take_row(&mut self) -> Row {
        self.inner.take_row()
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    fn rows_affected(&self) -> i64 {
        0
    }

    fn last_insert_id(&self) -> i64 {
        self.inner.last_insert_id()
    }

    fn last_error(&mut self) -> Option<Error> {
        self.inner.last_error()
    }

    fn estimated_count(&self) -> Option<usize> {
        self.inner.estimated_count()
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}

/// Top-N result using a bounded heap for ORDER BY + LIMIT optimization
///
/// This is O(n log k) instead of O(n log n) for full sort, where k = limit.
/// For large datasets with small limits (e.g., 1M rows, LIMIT 10), this can be 5-50x faster.
pub struct TopNResult {
    /// Materialized top-N rows
    inner: ExecutorResult,
    /// Keeps the bounded candidate set charged to the request until this
    /// result is consumed or dropped.
    _budget: RetainedRowsBudget,
}

impl TopNResult {
    /// Create a new top-N result using BinaryHeap for bounded sorting
    ///
    /// Uses a max-heap of size k (limit + offset) to efficiently find top-k elements.
    /// Only keeps k rows in memory at any time, making it memory-efficient for small limits.
    ///
    /// # Arguments
    /// * `inner` - Source result to process
    /// * `compare` - Comparison function for ordering (returns Less if a should come before b)
    /// * `limit` - Maximum number of rows to return
    /// * `offset` - Number of rows to skip (we need limit + offset rows in heap)
    pub fn new<F>(
        inner: Box<dyn QueryResult>,
        compare: F,
        limit: usize,
        offset: usize,
    ) -> Result<Self>
    where
        F: Fn(&Row, &Row) -> std::cmp::Ordering + Clone,
    {
        Self::build(
            inner,
            compare,
            limit,
            offset,
            RetainedRowsBudget::new("TOP-N"),
        )
    }

    pub fn new_with_context<F>(
        inner: Box<dyn QueryResult>,
        compare: F,
        limit: usize,
        offset: usize,
        ctx: &crate::context::ExecutionContext,
    ) -> Result<Self>
    where
        F: Fn(&Row, &Row) -> std::cmp::Ordering + Clone,
    {
        Self::build(
            inner,
            compare,
            limit,
            offset,
            RetainedRowsBudget::with_request_memory("TOP-N", ctx)?,
        )
    }

    fn build<F>(
        mut inner: Box<dyn QueryResult>,
        compare: F,
        limit: usize,
        offset: usize,
        mut budget: RetainedRowsBudget,
    ) -> Result<Self>
    where
        F: Fn(&Row, &Row) -> std::cmp::Ordering + Clone,
    {
        use std::collections::BinaryHeap;

        let columns = inner.columns().to_vec();
        let heap_capacity = limit.saturating_add(offset);
        budget.ensure_capacity(heap_capacity)?;

        // If no limit, fall back to empty result
        if heap_capacity == 0 {
            return Ok(Self {
                inner: ExecutorResult::new(columns, RowVec::new()),
                _budget: budget,
            });
        }

        // Use Arc to wrap compare function - cloning Arc is O(1)
        let compare = std::sync::Arc::new(compare);

        // Wrapper for Row with Arc-wrapped comparison (O(1) clone)
        struct HeapRow<F: Fn(&Row, &Row) -> std::cmp::Ordering> {
            row: Row,
            compare: std::sync::Arc<F>,
        }

        impl<F: Fn(&Row, &Row) -> std::cmp::Ordering> PartialEq for HeapRow<F> {
            fn eq(&self, other: &Self) -> bool {
                (self.compare)(&self.row, &other.row) == std::cmp::Ordering::Equal
            }
        }

        impl<F: Fn(&Row, &Row) -> std::cmp::Ordering> Eq for HeapRow<F> {}

        impl<F: Fn(&Row, &Row) -> std::cmp::Ordering> PartialOrd for HeapRow<F> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl<F: Fn(&Row, &Row) -> std::cmp::Ordering> Ord for HeapRow<F> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                // For TOP-N, we want the WORST element at the top of the max-heap
                // so we can efficiently replace it when a better element comes.
                (self.compare)(&self.row, &other.row)
            }
        }

        let mut heap: BinaryHeap<HeapRow<F>> = BinaryHeap::with_capacity(heap_capacity + 1);

        let mut input_rows = 0_u64;
        while inner.next() {
            input_rows = input_rows.saturating_add(1);
            let row = inner.take_row();

            if heap.len() < heap_capacity {
                budget.admit(&row)?;
                heap.push(HeapRow {
                    row,
                    compare: std::sync::Arc::clone(&compare),
                });
            } else if let Some(worst) = heap.peek() {
                if compare(&row, &worst.row) == std::cmp::Ordering::Less {
                    if let Some(removed) = heap.pop() {
                        budget.release(&removed.row);
                    }
                    budget.admit(&row)?;
                    heap.push(HeapRow {
                        row,
                        compare: std::sync::Arc::clone(&compare),
                    });
                }
            }
        }
        if let Some(err) = inner.last_error() {
            return Err(err);
        }

        // Extract rows from heap and sort them
        let mut rows: Vec<Row> = heap.into_iter().map(|hr| hr.row).collect();
        rows.sort_unstable_by(|a, b| compare(a, b));

        // Apply offset
        if offset > 0 && offset < rows.len() {
            for row in rows.drain(..offset) {
                budget.release(&row);
            }
        } else if offset >= rows.len() {
            for row in &rows {
                budget.release(row);
            }
            rows.clear();
        }

        // Convert to RowVec format
        let result_rows: RowVec = rows
            .into_iter()
            .enumerate()
            .map(|(i, row)| (i as i64, row))
            .collect();

        radixdb_storage::instrumentation::record_join_top_n(
            input_rows,
            budget.peak_rows() as u64,
            budget.peak_bytes() as u64,
            result_rows.len() as u64,
        );

        Ok(Self {
            inner: ExecutorResult::new(columns, result_rows),
            _budget: budget,
        })
    }

    pub fn from_rows_with_budget(
        columns: Vec<String>,
        rows: RowVec,
        budget: RetainedRowsBudget,
    ) -> Self {
        Self {
            inner: ExecutorResult::new(columns, rows),
            _budget: budget,
        }
    }
}

impl QueryResult for TopNResult {
    fn columns(&self) -> &[String] {
        self.inner.columns()
    }

    fn next(&mut self) -> bool {
        self.inner.next()
    }

    fn scan(&self, dest: &mut [Value]) -> Result<()> {
        self.inner.scan(dest)
    }

    fn row(&self) -> &Row {
        self.inner.row()
    }

    fn take_row(&mut self) -> Row {
        self.inner.take_row()
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    fn rows_affected(&self) -> i64 {
        0
    }

    fn last_insert_id(&self) -> i64 {
        0
    }

    fn with_aliases(self: Box<Self>, aliases: FxHashMap<String, String>) -> Box<dyn QueryResult> {
        Box::new(AliasedResult::new(self, aliases))
    }
}
