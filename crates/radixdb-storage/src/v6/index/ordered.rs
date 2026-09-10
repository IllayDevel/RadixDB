use std::cmp::Ordering;
use std::mem::size_of;

use radixdb_catalog::ObjectId;
use radixdb_core::Value;
use smallvec::SmallVec;

use super::super::{
    ArtifactSource, DataArtifactLayout, DataColumnSpec, FormatResult, IndexSectionKind,
};
use super::codec::read_index_page_from_source;
use super::key::{
    decode_canonical_key, decode_canonical_key_specs, diagnostic_key_hash, encode_canonical_key,
    encode_canonical_key_specs, encode_canonical_projected_key_specs, resolve_source_columns,
    CanonicalKeyBytes, MAX_INDEX_KEY_BYTES,
};
use super::model::{
    invalid, is_ordered_key_type, limit, IndexAccelerator, IndexAcceleratorKind,
    IndexArtifactLayout, IndexKeyColumn, IndexNullsOrder, IndexPage, IndexPageCodec, IndexPageSpec,
    IndexSortDirection, MAX_ENTRIES_PER_INDEX_PAGE, MAX_LOGICAL_BYTES_PER_INDEX_PAGE,
};
use super::posting::{
    decode_posting, encode_posting, visit_fragments, PostingFragmentState, KNOWN_ENTRY_FLAGS,
    NULL_COMPONENT_FLAG,
};

const PAGE_MAGIC: [u8; 4] = *b"IXO2";
const PAGE_VERSION: u16 = 2;
const PAGE_HEADER_BYTES: usize = 40;
const ENTRY_BYTES: usize = 32;
const DENSE_INTEGER_PAGE_MAGIC: [u8; 4] = *b"IXD1";
const DENSE_INTEGER_PAGE_VERSION: u16 = 1;
const DENSE_INTEGER_PAGE_BYTES: usize = 56;

pub const MAX_ORDERED_PAGE_DECODE_BYTES: u64 = 512 * 1024 * 1024;
pub const DEFAULT_ORDERED_PAGE_ENTRIES: u64 = 4_096;
pub const DEFAULT_ORDERED_PAGE_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct OrderedIndexKey {
    bytes: CanonicalKeyBytes,
    values: SmallVec<[Value; 2]>,
    has_null_component: bool,
}

impl OrderedIndexKey {
    pub fn from_values(
        data: &DataArtifactLayout,
        key_columns: &[IndexKeyColumn],
        values: &[Value],
    ) -> FormatResult<Self> {
        let source_columns = resolve_source_columns(data, key_columns)?;
        validate_ordered_types(key_columns)?;
        let (bytes, has_null_component) = encode_canonical_key(&source_columns, values)?;
        Ok(Self {
            bytes,
            values: values.iter().cloned().collect(),
            has_null_component,
        })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub const fn has_null_component(&self) -> bool {
        self.has_null_component
    }

    pub(crate) fn from_column_specs(
        columns: &[DataColumnSpec],
        values: &[Value],
    ) -> FormatResult<Self> {
        let (bytes, has_null_component) = encode_canonical_key_specs(columns, values)?;
        Ok(Self {
            bytes,
            values: values.iter().cloned().collect(),
            has_null_component,
        })
    }

    pub(crate) fn from_projected_values(
        columns: &[DataColumnSpec],
        source_values: &[Value],
        source_ordinals: &[usize],
    ) -> FormatResult<Self> {
        let (bytes, has_null_component) =
            encode_canonical_projected_key_specs(columns, source_values, source_ordinals)?;
        let values = source_ordinals
            .iter()
            .map(|source_ordinal| {
                source_values
                    .get(*source_ordinal)
                    .cloned()
                    .ok_or_else(|| invalid("source row is narrower than ordered index key"))
            })
            .collect::<FormatResult<SmallVec<[Value; 2]>>>()?;
        Ok(Self {
            bytes,
            values,
            has_null_component,
        })
    }

    pub(crate) fn from_canonical_bytes(
        bytes: Vec<u8>,
        columns: &[DataColumnSpec],
    ) -> FormatResult<Self> {
        let (values, has_null_component) = decode_canonical_key_specs(&bytes, columns)?;
        Ok(Self {
            bytes: bytes.into(),
            values: values.into(),
            has_null_component,
        })
    }
}

impl PartialEq for OrderedIndexKey {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl Eq for OrderedIndexKey {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderedIndexEntry {
    key: OrderedIndexKey,
    row_ordinals: SmallVec<[u64; 4]>,
    fragment_state: PostingFragmentState,
}

impl OrderedIndexEntry {
    pub fn new(key: OrderedIndexKey, row_ordinals: Vec<u64>) -> FormatResult<Self> {
        Self::from_bounded_rows(key, row_ordinals)
    }

    pub(crate) fn from_bounded_rows(
        key: OrderedIndexKey,
        row_ordinals: impl IntoIterator<Item = u64>,
    ) -> FormatResult<Self> {
        Self::from_fragment_rows(key, row_ordinals, PostingFragmentState::COMPLETE)
    }

    pub(crate) fn from_fragment_rows(
        key: OrderedIndexKey,
        row_ordinals: impl IntoIterator<Item = u64>,
        fragment_state: PostingFragmentState,
    ) -> FormatResult<Self> {
        let row_ordinals = row_ordinals.into_iter().collect::<SmallVec<[u64; 4]>>();
        if row_ordinals.is_empty() || row_ordinals.len() > u32::MAX as usize {
            return Err(limit(
                "posting rows per key",
                row_ordinals.len() as u64,
                u64::from(u32::MAX),
            ));
        }
        if row_ordinals.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(invalid(
                "ordered posting row ordinals are not strictly increasing",
            ));
        }
        Ok(Self {
            key,
            row_ordinals,
            fragment_state,
        })
    }

    pub const fn key(&self) -> &OrderedIndexKey {
        &self.key
    }

    pub fn row_ordinals(&self) -> &[u64] {
        &self.row_ordinals
    }

    pub(crate) const fn fragment_state(&self) -> PostingFragmentState {
        self.fragment_state
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderedPageBuildLimits {
    max_entries: u64,
    max_logical_bytes: u64,
}

impl OrderedPageBuildLimits {
    pub fn new(max_entries: u64, max_logical_bytes: u64) -> FormatResult<Self> {
        if max_entries == 0 || max_entries > MAX_ENTRIES_PER_INDEX_PAGE {
            return Err(limit(
                "ordered page entries",
                max_entries,
                MAX_ENTRIES_PER_INDEX_PAGE,
            ));
        }
        if max_logical_bytes < PAGE_HEADER_BYTES as u64
            || max_logical_bytes > MAX_LOGICAL_BYTES_PER_INDEX_PAGE
        {
            return Err(limit(
                "ordered page logical bytes",
                max_logical_bytes,
                MAX_LOGICAL_BYTES_PER_INDEX_PAGE,
            ));
        }
        Ok(Self {
            max_entries,
            max_logical_bytes,
        })
    }

    pub const fn max_entries(self) -> u64 {
        self.max_entries
    }

    pub const fn max_logical_bytes(self) -> u64 {
        self.max_logical_bytes
    }
}

impl Default for OrderedPageBuildLimits {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_ORDERED_PAGE_ENTRIES,
            max_logical_bytes: DEFAULT_ORDERED_PAGE_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderedIndexPageEntry {
    key: OrderedIndexKey,
    row_ordinals: Vec<u64>,
    has_previous_fragment: bool,
    has_next_fragment: bool,
}

impl OrderedIndexPageEntry {
    pub const fn key(&self) -> &OrderedIndexKey {
        &self.key
    }

    pub fn row_ordinals(&self) -> &[u64] {
        &self.row_ordinals
    }

    pub const fn has_previous_fragment(&self) -> bool {
        self.has_previous_fragment
    }

    pub const fn has_next_fragment(&self) -> bool {
        self.has_next_fragment
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderedIndexPage {
    entries: Vec<OrderedIndexPageEntry>,
}

impl OrderedIndexPage {
    pub fn entries(&self) -> &[OrderedIndexPageEntry] {
        &self.entries
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderedIndexBound {
    key: OrderedIndexKey,
    inclusive: bool,
}

impl OrderedIndexBound {
    pub const fn new(key: OrderedIndexKey, inclusive: bool) -> Self {
        Self { key, inclusive }
    }

    pub const fn key(&self) -> &OrderedIndexKey {
        &self.key
    }

    pub const fn inclusive(&self) -> bool {
        self.inclusive
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexScanDirection {
    Forward,
    Reverse,
}

#[derive(Debug)]
struct PreparedEntry {
    key: OrderedIndexKey,
    posting: SmallVec<[u8; 16]>,
    row_count: u32,
    fragment_state: PostingFragmentState,
    single_row_ordinal: Option<u64>,
}

pub fn encode_ordered_index_pages(
    mut entries: Vec<OrderedIndexEntry>,
    key_columns: &[IndexKeyColumn],
    unique: bool,
    source_row_count: u64,
    codec: IndexPageCodec,
    limits: OrderedPageBuildLimits,
) -> FormatResult<Vec<IndexPageSpec>> {
    validate_ordered_types(key_columns)?;
    for entry in &entries {
        validate_key_shape(entry.key(), key_columns)?;
    }
    entries.sort_by(|left, right| {
        compare_ordered_keys(left.key(), right.key(), key_columns)
            .expect("validated ordered key types have a total comparison")
    });
    encode_ordered_index_pages_from_sorted(
        entries.into_iter().map(Ok),
        key_columns,
        unique,
        source_row_count,
        codec,
        limits,
    )
}

pub(crate) fn encode_ordered_index_pages_from_sorted(
    entries: impl IntoIterator<Item = FormatResult<OrderedIndexEntry>>,
    key_columns: &[IndexKeyColumn],
    unique: bool,
    source_row_count: u64,
    codec: IndexPageCodec,
    limits: OrderedPageBuildLimits,
) -> FormatResult<Vec<IndexPageSpec>> {
    let mut pages = Vec::new();
    visit_ordered_index_pages_from_sorted(
        entries,
        key_columns,
        unique,
        source_row_count,
        codec,
        limits,
        &mut |page| {
            pages.push(page);
            Ok(())
        },
    )?;
    Ok(pages)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn visit_ordered_index_pages_from_sorted(
    entries: impl IntoIterator<Item = FormatResult<OrderedIndexEntry>>,
    key_columns: &[IndexKeyColumn],
    unique: bool,
    source_row_count: u64,
    codec: IndexPageCodec,
    limits: OrderedPageBuildLimits,
    visitor: &mut dyn FnMut(IndexPageSpec) -> FormatResult<()>,
) -> FormatResult<u64> {
    if source_row_count == 0 {
        return Err(invalid("ordered accelerator source has no rows"));
    }
    validate_ordered_types(key_columns)?;

    let mut prepared = Vec::new();
    let mut key_bytes = 0_u64;
    let mut posting_bytes = 0_u64;
    let mut previous_key: Option<OrderedIndexKey> = None;
    let mut previous_state = PostingFragmentState::COMPLETE;
    let mut previous_last_row = None;
    let mut entry_count = 0_u64;
    let mut page_count = 0_u64;
    for entry in entries {
        let entry = entry?;
        validate_key_shape(entry.key(), key_columns)?;
        validate_ordered_rows(&entry.row_ordinals, source_row_count)?;
        let fixed_logical_bytes = page_length(1, entry.key.bytes.len() as u64, 0)?;
        visit_fragments(
            &entry.row_ordinals,
            entry.fragment_state(),
            fixed_logical_bytes,
            limits.max_logical_bytes(),
            "ordered page logical bytes",
            |rows, fragment_state, _| {
                validate_ordered_fragment_transition(
                    previous_key.as_ref(),
                    previous_state,
                    previous_last_row,
                    &entry.key,
                    fragment_state,
                    rows[0],
                    key_columns,
                )?;
                if unique
                    && !entry.key.has_null_component()
                    && (rows.len() > 1
                        || fragment_state.has_previous()
                        || fragment_state.has_next())
                {
                    return Err(invalid("unique ordered key owns more than one row"));
                }
                let posting = encode_posting(rows);
                let candidate_key_bytes = key_bytes
                    .checked_add(entry.key.bytes.len() as u64)
                    .ok_or_else(|| invalid("ordered page key byte count overflows"))?;
                let candidate_posting_bytes = posting_bytes
                    .checked_add(posting.len() as u64)
                    .ok_or_else(|| invalid("ordered page posting byte count overflows"))?;
                let candidate_entries = prepared.len() as u64 + 1;
                let candidate_length = page_length(
                    candidate_entries,
                    candidate_key_bytes,
                    candidate_posting_bytes,
                )?;
                if !prepared.is_empty()
                    && (candidate_entries > limits.max_entries()
                        || candidate_length > limits.max_logical_bytes())
                {
                    visitor(encode_page(&prepared, key_bytes, posting_bytes, codec)?)?;
                    page_count += 1;
                    prepared.clear();
                    key_bytes = 0;
                    posting_bytes = 0;
                }
                key_bytes += entry.key.bytes.len() as u64;
                posting_bytes += posting.len() as u64;
                previous_key = Some(entry.key.clone());
                previous_state = fragment_state;
                previous_last_row = rows.last().copied();
                prepared.push(PreparedEntry {
                    key: entry.key.clone(),
                    posting,
                    row_count: rows.len() as u32,
                    fragment_state,
                    single_row_ordinal: (rows.len() == 1
                        && fragment_state == PostingFragmentState::COMPLETE)
                        .then_some(rows[0]),
                });
                entry_count += 1;
                Ok(())
            },
        )?;
    }
    if entry_count == 0 {
        return Err(invalid("ordered accelerator has no entries"));
    }
    if previous_state.has_next() {
        return Err(invalid("ordered posting continuation is truncated"));
    }
    if !prepared.is_empty() {
        visitor(encode_page(&prepared, key_bytes, posting_bytes, codec)?)?;
        page_count += 1;
    }
    Ok(page_count)
}

pub(crate) fn count_ordered_index_pages_from_sorted(
    entries: impl IntoIterator<Item = FormatResult<OrderedIndexEntry>>,
    key_columns: &[IndexKeyColumn],
    unique: bool,
    source_row_count: u64,
    limits: OrderedPageBuildLimits,
) -> FormatResult<u64> {
    if source_row_count == 0 {
        return Err(invalid("ordered accelerator source has no rows"));
    }
    validate_ordered_types(key_columns)?;
    let mut page_entries = 0_u64;
    let mut key_bytes = 0_u64;
    let mut posting_bytes = 0_u64;
    let mut previous_key: Option<OrderedIndexKey> = None;
    let mut previous_state = PostingFragmentState::COMPLETE;
    let mut previous_last_row = None;
    let mut total_entries = 0_u64;
    let mut page_count = 0_u64;
    for entry in entries {
        let entry = entry?;
        validate_key_shape(entry.key(), key_columns)?;
        validate_ordered_rows(&entry.row_ordinals, source_row_count)?;
        let fixed_logical_bytes = page_length(1, entry.key.bytes.len() as u64, 0)?;
        visit_fragments(
            &entry.row_ordinals,
            entry.fragment_state(),
            fixed_logical_bytes,
            limits.max_logical_bytes(),
            "ordered page logical bytes",
            |rows, fragment_state, posting_length| {
                validate_ordered_fragment_transition(
                    previous_key.as_ref(),
                    previous_state,
                    previous_last_row,
                    &entry.key,
                    fragment_state,
                    rows[0],
                    key_columns,
                )?;
                if unique
                    && !entry.key.has_null_component()
                    && (rows.len() > 1
                        || fragment_state.has_previous()
                        || fragment_state.has_next())
                {
                    return Err(invalid("unique ordered key owns more than one row"));
                }
                let candidate_entries = page_entries + 1;
                let candidate_keys = key_bytes
                    .checked_add(entry.key.bytes.len() as u64)
                    .ok_or_else(|| invalid("ordered page key byte count overflows"))?;
                let candidate_postings = posting_bytes
                    .checked_add(posting_length)
                    .ok_or_else(|| invalid("ordered page posting byte count overflows"))?;
                let candidate_length =
                    page_length(candidate_entries, candidate_keys, candidate_postings)?;
                if page_entries != 0
                    && (candidate_entries > limits.max_entries()
                        || candidate_length > limits.max_logical_bytes())
                {
                    page_count += 1;
                    page_entries = 0;
                    key_bytes = 0;
                    posting_bytes = 0;
                }
                page_entries += 1;
                key_bytes += entry.key.bytes.len() as u64;
                posting_bytes += posting_length;
                previous_key = Some(entry.key.clone());
                previous_state = fragment_state;
                previous_last_row = rows.last().copied();
                total_entries += 1;
                Ok(())
            },
        )?;
    }
    if total_entries == 0 {
        return Err(invalid("ordered accelerator has no entries"));
    }
    if previous_state.has_next() {
        return Err(invalid("ordered posting continuation is truncated"));
    }
    Ok(page_count + u64::from(page_entries != 0))
}

fn validate_ordered_rows(row_ordinals: &[u64], source_row_count: u64) -> FormatResult<()> {
    if row_ordinals.is_empty()
        || row_ordinals.windows(2).any(|pair| pair[0] >= pair[1])
        || row_ordinals
            .last()
            .is_some_and(|ordinal| *ordinal >= source_row_count)
    {
        return Err(invalid(
            "ordered posting rows are empty, unordered, or outside source data",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_ordered_fragment_transition(
    previous_key: Option<&OrderedIndexKey>,
    previous_state: PostingFragmentState,
    previous_last_row: Option<u64>,
    key: &OrderedIndexKey,
    state: PostingFragmentState,
    first_row: u64,
    key_columns: &[IndexKeyColumn],
) -> FormatResult<()> {
    let Some(previous_key) = previous_key else {
        return if state.has_previous() {
            Err(invalid(
                "ordered posting continuation has no first fragment",
            ))
        } else {
            Ok(())
        };
    };
    match compare_ordered_keys(previous_key, key, key_columns)? {
        Ordering::Greater => Err(invalid("ordered accelerator entries are out of order")),
        Ordering::Less => {
            if previous_state.has_next() || state.has_previous() {
                Err(invalid("ordered posting continuation changes key"))
            } else {
                Ok(())
            }
        }
        Ordering::Equal => {
            if !previous_state.has_next() || !state.has_previous() {
                return Err(invalid(
                    "duplicate ordered key is not a canonical continuation",
                ));
            }
            if previous_last_row.is_some_and(|last| last >= first_row) {
                return Err(invalid(
                    "ordered posting continuation rows are not strictly increasing",
                ));
            }
            Ok(())
        }
    }
}

pub fn decode_ordered_index_page(
    logical_bytes: &[u8],
    page: IndexPage,
    data: &DataArtifactLayout,
    accelerator: &IndexAccelerator,
) -> FormatResult<OrderedIndexPage> {
    if accelerator.kind() != IndexAcceleratorKind::Ordered {
        return Err(invalid(
            "ordered page is bound to a non-ordered accelerator",
        ));
    }
    if logical_bytes.len() as u64 != page.logical_length() {
        return Err(invalid("ordered page length differs from page descriptor"));
    }
    if logical_bytes.starts_with(&DENSE_INTEGER_PAGE_MAGIC) {
        return decode_dense_integer_page(logical_bytes, page, data, accelerator);
    }
    if logical_bytes.len() < PAGE_HEADER_BYTES || logical_bytes[..4] != PAGE_MAGIC {
        return Err(invalid("ordered page header is missing"));
    }
    if read_u16(logical_bytes, 4) != PAGE_VERSION
        || read_u16(logical_bytes, 6) != 0
        || read_u32(logical_bytes, 36) != 0
    {
        return Err(invalid(
            "ordered page version/flags/reserved field is invalid",
        ));
    }
    let entry_count = read_u32(logical_bytes, 8);
    if entry_count == 0 || u64::from(entry_count) > MAX_ENTRIES_PER_INDEX_PAGE {
        return Err(limit(
            "ordered page entries",
            u64::from(entry_count),
            MAX_ENTRIES_PER_INDEX_PAGE,
        ));
    }
    if u64::from(entry_count) != page.item_count() {
        return Err(invalid(
            "ordered page entry count differs from page descriptor",
        ));
    }
    let directory_length = read_u32(logical_bytes, 12) as usize;
    if directory_length != entry_count as usize * ENTRY_BYTES {
        return Err(invalid("ordered page directory length mismatch"));
    }
    let key_length = usize::try_from(read_u64(logical_bytes, 16))
        .map_err(|_| invalid("ordered page key area does not fit this platform"))?;
    let posting_length = usize::try_from(read_u64(logical_bytes, 24))
        .map_err(|_| invalid("ordered page posting area does not fit this platform"))?;
    let directory_end = PAGE_HEADER_BYTES
        .checked_add(directory_length)
        .ok_or_else(|| invalid("ordered page directory range overflows"))?;
    let key_end = directory_end
        .checked_add(key_length)
        .ok_or_else(|| invalid("ordered page key range overflows"))?;
    let posting_end = key_end
        .checked_add(posting_length)
        .ok_or_else(|| invalid("ordered page posting range overflows"))?;
    if posting_end != logical_bytes.len() {
        return Err(invalid(
            "ordered page areas do not own the complete payload",
        ));
    }
    if read_u32(logical_bytes, 32) != radixdb_core::crc32_ieee(&logical_bytes[PAGE_HEADER_BYTES..])
    {
        return Err(super::super::FormatError::IndexArtifactChecksumMismatch {
            scope: "ordered page body",
        });
    }

    validate_decode_allocation(
        logical_bytes,
        entry_count,
        key_length,
        accelerator.key_columns().len(),
    )?;
    let source_columns = resolve_source_columns(data, accelerator.key_columns())?;
    validate_ordered_types(accelerator.key_columns())?;
    let key_area = &logical_bytes[directory_end..key_end];
    let posting_area = &logical_bytes[key_end..posting_end];
    let mut entries = Vec::<OrderedIndexPageEntry>::with_capacity(entry_count as usize);
    let mut previous_state = PostingFragmentState::COMPLETE;
    let mut previous_last_row = None;
    let mut next_key = 0_usize;
    let mut next_posting = 0_usize;
    let mut minimum_hash = u64::MAX;
    let mut maximum_hash = 0_u64;
    for index in 0..entry_count as usize {
        let offset = PAGE_HEADER_BYTES + index * ENTRY_BYTES;
        let descriptor = &logical_bytes[offset..offset + ENTRY_BYTES];
        let key_offset = read_u32(descriptor, 0) as usize;
        let key_bytes = read_u32(descriptor, 4) as usize;
        let posting_offset = read_u32(descriptor, 8) as usize;
        let posting_bytes = read_u32(descriptor, 12) as usize;
        let row_count = read_u32(descriptor, 16);
        let flags = read_u32(descriptor, 20);
        if key_offset != next_key || posting_offset != next_posting || key_bytes == 0 {
            return Err(invalid("ordered page entry ranges are not canonical"));
        }
        if key_bytes as u64 > MAX_INDEX_KEY_BYTES || posting_bytes == 0 || row_count == 0 {
            return Err(invalid("ordered page entry lengths/count are invalid"));
        }
        if flags & !KNOWN_ENTRY_FLAGS != 0 {
            return Err(invalid("ordered page entry has unknown flags"));
        }
        let fragment_state = PostingFragmentState::from_flags(flags);
        let key_end = key_offset
            .checked_add(key_bytes)
            .ok_or_else(|| invalid("ordered key range overflows"))?;
        let posting_end = posting_offset
            .checked_add(posting_bytes)
            .ok_or_else(|| invalid("ordered posting range overflows"))?;
        let key_slice = key_area
            .get(key_offset..key_end)
            .ok_or_else(|| invalid("ordered key range is outside key area"))?;
        let posting_slice = posting_area
            .get(posting_offset..posting_end)
            .ok_or_else(|| invalid("ordered posting range is outside posting area"))?;
        if read_u32(descriptor, 24) != radixdb_core::crc32_ieee(key_slice) {
            return Err(super::super::FormatError::IndexArtifactChecksumMismatch {
                scope: "ordered key",
            });
        }
        if read_u32(descriptor, 28) != radixdb_core::crc32_ieee(posting_slice) {
            return Err(super::super::FormatError::IndexArtifactChecksumMismatch {
                scope: "ordered posting",
            });
        }
        let (values, has_null_component) = decode_canonical_key(key_slice, &source_columns)?;
        let key = OrderedIndexKey {
            bytes: key_slice.into(),
            values: values.into(),
            has_null_component,
        };
        if key.has_null_component() != (flags & NULL_COMPONENT_FLAG != 0) {
            return Err(invalid("ordered key NULL flag mismatch"));
        }
        let row_ordinals = decode_posting(posting_slice, row_count, data.header().row_count())?;
        if let Some(previous) = entries.last() {
            match compare_ordered_keys(previous.key(), &key, accelerator.key_columns())? {
                Ordering::Greater => return Err(invalid("ordered page keys are out of order")),
                Ordering::Less => {
                    if previous_state.has_next() || fragment_state.has_previous() {
                        return Err(invalid("ordered posting continuation changes key"));
                    }
                }
                Ordering::Equal => {
                    if !previous_state.has_next() || !fragment_state.has_previous() {
                        return Err(invalid(
                            "duplicate ordered page key is not a canonical continuation",
                        ));
                    }
                    if previous_last_row.is_some_and(|last| last >= row_ordinals[0]) {
                        return Err(invalid(
                            "ordered posting continuation rows are not strictly increasing",
                        ));
                    }
                }
            }
        }
        if accelerator.unique()
            && !key.has_null_component()
            && (row_count > 1 || fragment_state.has_previous() || fragment_state.has_next())
        {
            return Err(invalid("unique ordered key owns more than one row"));
        }
        let hash = diagnostic_key_hash(key.as_bytes());
        minimum_hash = minimum_hash.min(hash);
        maximum_hash = maximum_hash.max(hash);
        previous_last_row = row_ordinals.last().copied();
        previous_state = fragment_state;
        if let Some(previous) = entries
            .last_mut()
            .filter(|entry| entry.key.bytes == key.bytes)
        {
            previous.row_ordinals.extend(row_ordinals);
            previous.has_next_fragment = fragment_state.has_next();
        } else {
            entries.push(OrderedIndexPageEntry {
                key,
                row_ordinals,
                has_previous_fragment: fragment_state.has_previous(),
                has_next_fragment: fragment_state.has_next(),
            });
        }
        next_key = key_end;
        next_posting = posting_end;
    }
    if next_key != key_area.len() || next_posting != posting_area.len() {
        return Err(invalid("ordered page has unowned key or posting bytes"));
    }
    if page.minimum_key_hash() != minimum_hash || page.maximum_key_hash() != maximum_hash {
        return Err(invalid("ordered page diagnostic key-hash range mismatch"));
    }
    Ok(OrderedIndexPage { entries })
}

#[allow(clippy::too_many_arguments)]
pub fn scan_ordered_index(
    bytes: &[u8],
    layout: &IndexArtifactLayout,
    data: &DataArtifactLayout,
    logical_index_id: ObjectId,
    lower: Option<&OrderedIndexBound>,
    upper: Option<&OrderedIndexBound>,
    direction: IndexScanDirection,
    offset: u64,
    limit: usize,
) -> FormatResult<Vec<u64>> {
    scan_ordered_index_from_source(
        bytes,
        layout,
        data,
        logical_index_id,
        lower,
        upper,
        direction,
        offset,
        limit,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn scan_ordered_index_from_source(
    source: &(impl ArtifactSource + ?Sized),
    layout: &IndexArtifactLayout,
    data: &DataArtifactLayout,
    logical_index_id: ObjectId,
    lower: Option<&OrderedIndexBound>,
    upper: Option<&OrderedIndexBound>,
    direction: IndexScanDirection,
    offset: u64,
    limit: usize,
) -> FormatResult<Vec<u64>> {
    scan_ordered_index_from_source_with_null_policy(
        source,
        layout,
        data,
        logical_index_id,
        lower,
        upper,
        direction,
        offset,
        limit,
        false,
    )
}

/// Scan a SQL range whose non-NULL bounds cannot match a NULL key component.
///
/// The persisted B-tree still orders NULL according to its descriptor.  The
/// range adapter must remove those keys before offset/limit accounting;
/// filtering row ordinals afterwards can otherwise turn a descending bounded
/// lookup into an incomplete result when NULLS LAST is scanned in reverse.
#[allow(clippy::too_many_arguments)]
pub(crate) fn scan_ordered_non_null_index_from_source(
    source: &(impl ArtifactSource + ?Sized),
    layout: &IndexArtifactLayout,
    data: &DataArtifactLayout,
    logical_index_id: ObjectId,
    lower: Option<&OrderedIndexBound>,
    upper: Option<&OrderedIndexBound>,
    direction: IndexScanDirection,
    offset: u64,
    limit: usize,
) -> FormatResult<Vec<u64>> {
    scan_ordered_index_from_source_with_null_policy(
        source,
        layout,
        data,
        logical_index_id,
        lower,
        upper,
        direction,
        offset,
        limit,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn scan_ordered_index_from_source_with_null_policy(
    source: &(impl ArtifactSource + ?Sized),
    layout: &IndexArtifactLayout,
    data: &DataArtifactLayout,
    logical_index_id: ObjectId,
    lower: Option<&OrderedIndexBound>,
    upper: Option<&OrderedIndexBound>,
    direction: IndexScanDirection,
    mut offset: u64,
    limit: usize,
    exclude_null_keys: bool,
) -> FormatResult<Vec<u64>> {
    let (accelerator_ordinal, accelerator) = layout
        .accelerators()
        .iter()
        .enumerate()
        .find(|(_, accelerator)| accelerator.logical_index_id() == logical_index_id)
        .ok_or_else(|| invalid("logical ordered accelerator is absent from index pack"))?;
    if accelerator.kind() != IndexAcceleratorKind::Ordered {
        return Err(invalid("logical accelerator is not ordered"));
    }
    validate_ordered_types(accelerator.key_columns())?;
    let exclude_null_keys = exclude_null_keys
        && lower.into_iter().chain(upper).next().is_some()
        && lower
            .into_iter()
            .chain(upper)
            .all(|bound| !bound.key().has_null_component());
    for bound in lower.into_iter().chain(upper) {
        validate_bound_key_shape(bound.key(), accelerator.key_columns())?;
    }
    if let (Some(lower), Some(upper)) = (lower, upper) {
        match compare_ordered_bounds(lower.key(), upper.key(), accelerator.key_columns())? {
            // Bounds are request input, not persisted artifact state. A
            // contradictory range is a valid empty query and must never be
            // reported as index corruption.
            Ordering::Greater => return Ok(Vec::new()),
            Ordering::Equal if !lower.inclusive() || !upper.inclusive() || limit == 0 => {
                return Ok(Vec::new());
            }
            _ => {}
        }
    }
    if limit == 0 {
        return Ok(Vec::new());
    }

    let section_index = accelerator
        .first_section_index()
        .checked_add(1)
        .ok_or_else(|| invalid("ordered section index overflows"))?
        as usize;
    let section = layout
        .sections()
        .get(section_index)
        .copied()
        .ok_or_else(|| invalid("ordered page section is absent"))?;
    if section.accelerator_ordinal() != accelerator_ordinal as u32
        || section.kind() != IndexSectionKind::OrderedPages
        || section.page_count() == 0
    {
        return Err(invalid("ordered page section ownership is invalid"));
    }

    let first_page = section.first_page_index() as usize;
    let page_count = section.page_count() as usize;
    let mut output = Vec::with_capacity(limit.min(4096));
    match direction {
        IndexScanDirection::Forward => {
            let start = first_forward_page(
                source,
                layout,
                data,
                accelerator,
                first_page,
                page_count,
                lower,
            )?;
            let mut previous_page_last: Option<OrderedIndexPageEntry> = None;
            for relative in start..page_count {
                let decoded =
                    decode_selected_page(source, layout, data, accelerator, first_page + relative)?;
                let first_entry = decoded
                    .entries()
                    .first()
                    .ok_or_else(|| invalid("decoded ordered page is empty"))?;
                if relative == 0 && lower.is_none() && first_entry.has_previous_fragment() {
                    return Err(invalid(
                        "ordered posting continuation has no first fragment",
                    ));
                }
                if let Some(previous) = previous_page_last.as_ref() {
                    validate_ordered_page_boundary(
                        previous,
                        first_entry,
                        accelerator.key_columns(),
                    )?;
                }
                for entry in decoded.entries() {
                    if exclude_null_keys && entry.key().has_null_component() {
                        continue;
                    }
                    if is_before_lower(entry.key(), lower, accelerator.key_columns())? {
                        continue;
                    }
                    if is_after_upper(entry.key(), upper, accelerator.key_columns())? {
                        return Ok(output);
                    }
                    append_rows(
                        entry.row_ordinals().iter().copied(),
                        &mut offset,
                        limit,
                        &mut output,
                    );
                    if output.len() == limit {
                        return Ok(output);
                    }
                }
                previous_page_last = decoded.entries().last().cloned();
            }
            if upper.is_none()
                && previous_page_last
                    .as_ref()
                    .is_some_and(OrderedIndexPageEntry::has_next_fragment)
            {
                return Err(invalid("ordered posting continuation is truncated"));
            }
        }
        IndexScanDirection::Reverse => {
            let Some(start) = last_reverse_page(
                source,
                layout,
                data,
                accelerator,
                first_page,
                page_count,
                upper,
            )?
            else {
                return Ok(output);
            };
            let mut higher_page_first: Option<OrderedIndexPageEntry> = None;
            let mut lowest_page_first: Option<OrderedIndexPageEntry> = None;
            for relative in (0..=start).rev() {
                let decoded =
                    decode_selected_page(source, layout, data, accelerator, first_page + relative)?;
                let first_entry = decoded
                    .entries()
                    .first()
                    .ok_or_else(|| invalid("decoded ordered page is empty"))?;
                let last_entry = decoded
                    .entries()
                    .last()
                    .ok_or_else(|| invalid("decoded ordered page is empty"))?;
                if relative + 1 == page_count && upper.is_none() && last_entry.has_next_fragment() {
                    return Err(invalid("ordered posting continuation is truncated"));
                }
                if let Some(higher) = higher_page_first.as_ref() {
                    validate_ordered_page_boundary(last_entry, higher, accelerator.key_columns())?;
                }
                for entry in decoded.entries().iter().rev() {
                    if exclude_null_keys && entry.key().has_null_component() {
                        continue;
                    }
                    if is_after_upper(entry.key(), upper, accelerator.key_columns())? {
                        continue;
                    }
                    if is_before_lower(entry.key(), lower, accelerator.key_columns())? {
                        return Ok(output);
                    }
                    append_rows(
                        entry.row_ordinals().iter().rev().copied(),
                        &mut offset,
                        limit,
                        &mut output,
                    );
                    if output.len() == limit {
                        return Ok(output);
                    }
                }
                higher_page_first = Some(first_entry.clone());
                lowest_page_first = Some(first_entry.clone());
            }
            if lower.is_none()
                && lowest_page_first
                    .as_ref()
                    .is_some_and(OrderedIndexPageEntry::has_previous_fragment)
            {
                return Err(invalid(
                    "ordered posting continuation has no first fragment",
                ));
            }
        }
    }
    Ok(output)
}

fn validate_ordered_page_boundary(
    lower: &OrderedIndexPageEntry,
    upper: &OrderedIndexPageEntry,
    columns: &[IndexKeyColumn],
) -> FormatResult<()> {
    match compare_ordered_keys(lower.key(), upper.key(), columns)? {
        Ordering::Greater => Err(invalid("ordered page key ranges overlap out of order")),
        Ordering::Less => {
            if lower.has_next_fragment() || upper.has_previous_fragment() {
                Err(invalid(
                    "ordered posting continuation changes key across pages",
                ))
            } else {
                Ok(())
            }
        }
        Ordering::Equal => {
            if !lower.has_next_fragment() || !upper.has_previous_fragment() {
                return Err(invalid(
                    "duplicate ordered page key is not a canonical continuation",
                ));
            }
            if lower
                .row_ordinals()
                .last()
                .zip(upper.row_ordinals().first())
                .is_some_and(|(left, right)| left >= right)
            {
                return Err(invalid(
                    "ordered posting continuation rows are not strictly increasing",
                ));
            }
            Ok(())
        }
    }
}

fn first_forward_page(
    source: &(impl ArtifactSource + ?Sized),
    layout: &IndexArtifactLayout,
    data: &DataArtifactLayout,
    accelerator: &IndexAccelerator,
    first_page: usize,
    page_count: usize,
    lower: Option<&OrderedIndexBound>,
) -> FormatResult<usize> {
    let Some(lower) = lower else {
        return Ok(0);
    };
    let mut low = 0_usize;
    let mut high = page_count;
    while low < high {
        let middle = low + (high - low) / 2;
        let page = decode_selected_page(source, layout, data, accelerator, first_page + middle)?;
        let last = page
            .entries()
            .last()
            .ok_or_else(|| invalid("decoded ordered page is empty"))?;
        let ordering =
            compare_ordered_key_to_bound(last.key(), lower.key(), accelerator.key_columns())?;
        if ordering == Ordering::Less || (ordering == Ordering::Equal && !lower.inclusive()) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    Ok(low)
}

fn last_reverse_page(
    source: &(impl ArtifactSource + ?Sized),
    layout: &IndexArtifactLayout,
    data: &DataArtifactLayout,
    accelerator: &IndexAccelerator,
    first_page: usize,
    page_count: usize,
    upper: Option<&OrderedIndexBound>,
) -> FormatResult<Option<usize>> {
    let Some(upper) = upper else {
        return Ok(page_count.checked_sub(1));
    };
    let mut low = 0_usize;
    let mut high = page_count;
    while low < high {
        let middle = low + (high - low) / 2;
        let page = decode_selected_page(source, layout, data, accelerator, first_page + middle)?;
        let first = page
            .entries()
            .first()
            .ok_or_else(|| invalid("decoded ordered page is empty"))?;
        let ordering =
            compare_ordered_key_to_bound(first.key(), upper.key(), accelerator.key_columns())?;
        if ordering == Ordering::Less || (ordering == Ordering::Equal && upper.inclusive()) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    Ok(low.checked_sub(1))
}

fn decode_selected_page(
    source: &(impl ArtifactSource + ?Sized),
    layout: &IndexArtifactLayout,
    data: &DataArtifactLayout,
    accelerator: &IndexAccelerator,
    page_index: usize,
) -> FormatResult<OrderedIndexPage> {
    let page = *layout
        .pages()
        .get(page_index)
        .ok_or_else(|| invalid("ordered page is outside page directory"))?;
    let logical = read_index_page_from_source(source, layout, page_index)?;
    decode_ordered_index_page(&logical, page, data, accelerator)
}

fn is_before_lower(
    key: &OrderedIndexKey,
    lower: Option<&OrderedIndexBound>,
    columns: &[IndexKeyColumn],
) -> FormatResult<bool> {
    let Some(lower) = lower else {
        return Ok(false);
    };
    Ok(
        match compare_ordered_key_to_bound(key, lower.key(), columns)? {
            Ordering::Less => true,
            Ordering::Equal => !lower.inclusive(),
            Ordering::Greater => false,
        },
    )
}

fn is_after_upper(
    key: &OrderedIndexKey,
    upper: Option<&OrderedIndexBound>,
    columns: &[IndexKeyColumn],
) -> FormatResult<bool> {
    let Some(upper) = upper else {
        return Ok(false);
    };
    Ok(
        match compare_ordered_key_to_bound(key, upper.key(), columns)? {
            Ordering::Greater => true,
            Ordering::Equal => !upper.inclusive(),
            Ordering::Less => false,
        },
    )
}

fn append_rows(
    rows: impl Iterator<Item = u64>,
    offset: &mut u64,
    limit: usize,
    output: &mut Vec<u64>,
) {
    for row in rows {
        if *offset > 0 {
            *offset -= 1;
        } else if output.len() < limit {
            output.push(row);
        } else {
            break;
        }
    }
}

pub(crate) fn compare_ordered_keys(
    left: &OrderedIndexKey,
    right: &OrderedIndexKey,
    columns: &[IndexKeyColumn],
) -> FormatResult<Ordering> {
    validate_key_shape(left, columns)?;
    validate_key_shape(right, columns)?;
    let ordering = compare_ordered_components(&left.values, &right.values, columns)?;
    if ordering != Ordering::Equal {
        return Ok(ordering);
    }
    Ok(left.bytes.cmp(&right.bytes))
}

/// Compare keys already constructed against the same validated accelerator
/// descriptor.
///
/// Artifact decoders and public lookup paths must use [`compare_ordered_keys`]
/// so corrupt or mismatched input fails closed. The publication run builder,
/// however, owns both keys and has already type-checked every component while
/// encoding it. Repeating shape and type validation for every `O(n log n)`
/// sort comparison is pure work on that trusted path.
pub(crate) fn compare_ordered_keys_trusted(
    left: &OrderedIndexKey,
    right: &OrderedIndexKey,
    columns: &[IndexKeyColumn],
) -> Ordering {
    debug_assert!(validate_key_shape(left, columns).is_ok());
    debug_assert!(validate_key_shape(right, columns).is_ok());
    let ordering = compare_ordered_components(&left.values, &right.values, columns)
        .expect("publication-built ordered keys must remain comparable");
    if ordering != Ordering::Equal {
        return ordering;
    }
    left.bytes.cmp(&right.bytes)
}

fn compare_ordered_key_to_bound(
    key: &OrderedIndexKey,
    bound: &OrderedIndexKey,
    columns: &[IndexKeyColumn],
) -> FormatResult<Ordering> {
    validate_key_shape(key, columns)?;
    validate_bound_key_shape(bound, columns)?;
    compare_ordered_components(
        &key.values[..bound.values.len()],
        &bound.values,
        &columns[..bound.values.len()],
    )
}

fn compare_ordered_bounds(
    left: &OrderedIndexKey,
    right: &OrderedIndexKey,
    columns: &[IndexKeyColumn],
) -> FormatResult<Ordering> {
    validate_bound_key_shape(left, columns)?;
    validate_bound_key_shape(right, columns)?;
    if left.values.len() != right.values.len() {
        return Err(invalid(
            "ordered lower and upper bounds have different prefix lengths",
        ));
    }
    compare_ordered_components(&left.values, &right.values, &columns[..left.values.len()])
}

fn compare_ordered_components(
    left: &[Value],
    right: &[Value],
    columns: &[IndexKeyColumn],
) -> FormatResult<Ordering> {
    if left.len() != right.len() || left.len() != columns.len() {
        return Err(invalid("ordered comparison component count mismatch"));
    }
    for ((left, right), column) in left.iter().zip(right).zip(columns) {
        let ordering = match (left.is_null(), right.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => match column.nulls_order() {
                IndexNullsOrder::First => Ordering::Less,
                IndexNullsOrder::Last => Ordering::Greater,
            },
            (false, true) => match column.nulls_order() {
                IndexNullsOrder::First => Ordering::Greater,
                IndexNullsOrder::Last => Ordering::Less,
            },
            (false, false) => {
                let ordering = left
                    .compare(right)
                    .map_err(|_| invalid("ordered index values are not comparable"))?;
                match column.direction() {
                    IndexSortDirection::Ascending => ordering,
                    IndexSortDirection::Descending => ordering.reverse(),
                }
            }
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

fn validate_key_shape(key: &OrderedIndexKey, columns: &[IndexKeyColumn]) -> FormatResult<()> {
    if key.values.len() != columns.len() {
        return Err(invalid("ordered key component count mismatch"));
    }
    for (value, column) in key.values.iter().zip(columns) {
        if !value.is_null() && value.data_type() != column.logical_type() {
            return Err(invalid("ordered key value type differs from descriptor"));
        }
    }
    Ok(())
}

fn validate_bound_key_shape(key: &OrderedIndexKey, columns: &[IndexKeyColumn]) -> FormatResult<()> {
    if key.values.is_empty() || key.values.len() > columns.len() {
        return Err(invalid(
            "ordered bound is not a non-empty leading key prefix",
        ));
    }
    for (value, column) in key.values.iter().zip(columns) {
        if !value.is_null() && value.data_type() != column.logical_type() {
            return Err(invalid("ordered bound value type differs from descriptor"));
        }
    }
    Ok(())
}

fn validate_ordered_types(columns: &[IndexKeyColumn]) -> FormatResult<()> {
    if columns.is_empty() {
        return Err(invalid("ordered key descriptor is empty"));
    }
    if columns
        .iter()
        .any(|column| !is_ordered_key_type(column.logical_type()))
    {
        return Err(invalid(
            "ordered index contains a logical type without total B-tree order",
        ));
    }
    Ok(())
}

fn validate_decode_allocation(
    logical_bytes: &[u8],
    entry_count: u32,
    key_length: usize,
    key_column_count: usize,
) -> FormatResult<()> {
    let mut accounted = u64::from(entry_count)
        .checked_mul(size_of::<OrderedIndexPageEntry>() as u64)
        .and_then(|bytes| {
            (key_length as u64)
                .checked_mul(2)
                .and_then(|keys| bytes.checked_add(keys))
        })
        .and_then(|bytes| {
            u64::from(entry_count)
                .checked_mul(key_column_count as u64)
                .and_then(|values| values.checked_mul(size_of::<Value>() as u64))
                .and_then(|values| bytes.checked_add(values))
        })
        .ok_or_else(|| invalid("ordered page decode allocation overflows"))?;
    for index in 0..entry_count as usize {
        let offset = PAGE_HEADER_BYTES + index * ENTRY_BYTES;
        let entry = &logical_bytes[offset..offset + ENTRY_BYTES];
        let posting_bytes = read_u32(entry, 12);
        let row_count = read_u32(entry, 16);
        if posting_bytes < row_count {
            return Err(invalid(
                "ordered posting cannot encode its declared row count",
            ));
        }
        accounted = accounted
            .checked_add(u64::from(row_count) * size_of::<u64>() as u64)
            .ok_or_else(|| invalid("ordered page decode allocation overflows"))?;
    }
    if accounted > MAX_ORDERED_PAGE_DECODE_BYTES {
        return Err(limit(
            "ordered page decoded bytes",
            accounted,
            MAX_ORDERED_PAGE_DECODE_BYTES,
        ));
    }
    Ok(())
}

fn encode_page(
    entries: &[PreparedEntry],
    key_bytes: u64,
    posting_bytes: u64,
    codec: IndexPageCodec,
) -> FormatResult<IndexPageSpec> {
    if let Some(page) = encode_dense_integer_page(entries, codec)? {
        return Ok(page);
    }
    let length = page_length(entries.len() as u64, key_bytes, posting_bytes)?;
    let capacity = usize::try_from(length)
        .map_err(|_| invalid("ordered page length does not fit this platform"))?;
    let mut output = vec![0_u8; PAGE_HEADER_BYTES + entries.len() * ENTRY_BYTES];
    let mut keys = Vec::with_capacity(key_bytes as usize);
    let mut postings = Vec::with_capacity(posting_bytes as usize);
    let mut minimum_hash = u64::MAX;
    let mut maximum_hash = 0_u64;
    for (index, entry) in entries.iter().enumerate() {
        let offset = PAGE_HEADER_BYTES + index * ENTRY_BYTES;
        let descriptor = &mut output[offset..offset + ENTRY_BYTES];
        put_u32(descriptor, 0, keys.len() as u32);
        put_u32(descriptor, 4, entry.key.bytes.len() as u32);
        put_u32(descriptor, 8, postings.len() as u32);
        put_u32(descriptor, 12, entry.posting.len() as u32);
        put_u32(descriptor, 16, entry.row_count);
        put_u32(
            descriptor,
            20,
            u32::from(entry.key.has_null_component()) | entry.fragment_state.flags(),
        );
        put_u32(descriptor, 24, radixdb_core::crc32_ieee(&entry.key.bytes));
        put_u32(descriptor, 28, radixdb_core::crc32_ieee(&entry.posting));
        keys.extend_from_slice(&entry.key.bytes);
        postings.extend_from_slice(&entry.posting);
        let hash = diagnostic_key_hash(entry.key.as_bytes());
        minimum_hash = minimum_hash.min(hash);
        maximum_hash = maximum_hash.max(hash);
    }
    output.reserve(capacity.saturating_sub(output.len()));
    output.extend_from_slice(&keys);
    output.extend_from_slice(&postings);
    output[..4].copy_from_slice(&PAGE_MAGIC);
    put_u16(&mut output, 4, PAGE_VERSION);
    put_u32(&mut output, 8, entries.len() as u32);
    put_u32(&mut output, 12, (entries.len() * ENTRY_BYTES) as u32);
    put_u64(&mut output, 16, key_bytes);
    put_u64(&mut output, 24, posting_bytes);
    let body_crc = radixdb_core::crc32_ieee(&output[PAGE_HEADER_BYTES..]);
    put_u32(&mut output, 32, body_crc);
    IndexPageSpec::new(
        output,
        codec,
        entries.len() as u64,
        minimum_hash,
        maximum_hash,
    )
}

fn encode_dense_integer_page(
    entries: &[PreparedEntry],
    codec: IndexPageCodec,
) -> FormatResult<Option<IndexPageSpec>> {
    let Some((first_key, first_row)) = dense_integer_entry(entries.first()) else {
        return Ok(None);
    };
    let (key_step, row_step) = if entries.len() == 1 {
        (0_i64, 0_i64)
    } else {
        let Some((second_key, second_row)) = dense_integer_entry(entries.get(1)) else {
            return Ok(None);
        };
        let Some(key_step) = second_key.checked_sub(first_key) else {
            return Ok(None);
        };
        let Some(row_step) = signed_difference(second_row, first_row) else {
            return Ok(None);
        };
        if key_step == 0 || row_step == 0 {
            return Ok(None);
        }
        (key_step, row_step)
    };

    let mut minimum_hash = u64::MAX;
    let mut maximum_hash = 0_u64;
    for (ordinal, entry) in entries.iter().enumerate() {
        let Some((key, row)) = dense_integer_entry(Some(entry)) else {
            return Ok(None);
        };
        let Some(expected_key) = checked_i64_progression(first_key, key_step, ordinal) else {
            return Ok(None);
        };
        let Some(expected_row) = checked_u64_progression(first_row, row_step, ordinal) else {
            return Ok(None);
        };
        if key != expected_key || row != expected_row {
            return Ok(None);
        }
        let hash = diagnostic_key_hash(entry.key.as_bytes());
        minimum_hash = minimum_hash.min(hash);
        maximum_hash = maximum_hash.max(hash);
    }

    let entry_count = u32::try_from(entries.len())
        .map_err(|_| invalid("dense integer page entry count does not fit u32"))?;
    let mut output = vec![0_u8; DENSE_INTEGER_PAGE_BYTES];
    output[..4].copy_from_slice(&DENSE_INTEGER_PAGE_MAGIC);
    put_u16(&mut output, 4, DENSE_INTEGER_PAGE_VERSION);
    put_u32(&mut output, 8, entry_count);
    output[16..24].copy_from_slice(&first_key.to_le_bytes());
    output[24..32].copy_from_slice(&key_step.to_le_bytes());
    put_u64(&mut output, 32, first_row);
    output[40..48].copy_from_slice(&row_step.to_le_bytes());
    let body_crc = radixdb_core::crc32_ieee(&output[8..48]);
    put_u32(&mut output, 48, body_crc);
    IndexPageSpec::new(
        output,
        codec,
        entries.len() as u64,
        minimum_hash,
        maximum_hash,
    )
    .map(Some)
}

fn dense_integer_entry(entry: Option<&PreparedEntry>) -> Option<(i64, u64)> {
    let entry = entry?;
    if entry.row_count != 1
        || entry.fragment_state != PostingFragmentState::COMPLETE
        || entry.key.has_null_component()
        || entry.key.values().len() != 1
    {
        return None;
    }
    let Value::Integer(key) = entry.key.values()[0] else {
        return None;
    };
    Some((key, entry.single_row_ordinal?))
}

fn decode_dense_integer_page(
    logical_bytes: &[u8],
    page: IndexPage,
    data: &DataArtifactLayout,
    accelerator: &IndexAccelerator,
) -> FormatResult<OrderedIndexPage> {
    if logical_bytes.len() != DENSE_INTEGER_PAGE_BYTES
        || read_u16(logical_bytes, 4) != DENSE_INTEGER_PAGE_VERSION
        || read_u16(logical_bytes, 6) != 0
        || read_u32(logical_bytes, 12) != 0
        || read_u32(logical_bytes, 52) != 0
    {
        return Err(invalid("dense integer page header is invalid"));
    }
    let entry_count = read_u32(logical_bytes, 8);
    if entry_count == 0
        || u64::from(entry_count) != page.item_count()
        || u64::from(entry_count) > MAX_ENTRIES_PER_INDEX_PAGE
    {
        return Err(invalid("dense integer page entry count is invalid"));
    }
    if accelerator.key_columns().len() != 1
        || accelerator.key_columns()[0].logical_type() != radixdb_core::DataType::Integer
    {
        return Err(invalid(
            "dense integer page is bound to a non-integer key descriptor",
        ));
    }
    if read_u32(logical_bytes, 48) != radixdb_core::crc32_ieee(&logical_bytes[8..48]) {
        return Err(super::super::FormatError::IndexArtifactChecksumMismatch {
            scope: "dense integer page body",
        });
    }
    let first_key = read_i64(logical_bytes, 16);
    let key_step = read_i64(logical_bytes, 24);
    let first_row = read_u64(logical_bytes, 32);
    let row_step = read_i64(logical_bytes, 40);
    if entry_count > 1 && (key_step == 0 || row_step == 0) {
        return Err(invalid("dense integer page progression has a zero step"));
    }

    let allocation = u64::from(entry_count)
        .checked_mul(
            (size_of::<OrderedIndexPageEntry>()
                + size_of::<OrderedIndexKey>()
                + size_of::<Value>()
                + size_of::<u64>()) as u64,
        )
        .ok_or_else(|| invalid("dense integer page decode allocation overflows"))?;
    if allocation > MAX_ORDERED_PAGE_DECODE_BYTES {
        return Err(limit(
            "ordered page decoded bytes",
            allocation,
            MAX_ORDERED_PAGE_DECODE_BYTES,
        ));
    }

    let mut entries = Vec::<OrderedIndexPageEntry>::with_capacity(entry_count as usize);
    let mut minimum_hash = u64::MAX;
    let mut maximum_hash = 0_u64;
    for ordinal in 0..entry_count as usize {
        let key_value = checked_i64_progression(first_key, key_step, ordinal)
            .ok_or_else(|| invalid("dense integer key progression overflows"))?;
        let row_ordinal = checked_u64_progression(first_row, row_step, ordinal)
            .filter(|row| *row < data.header().row_count())
            .ok_or_else(|| invalid("dense integer row progression is outside source data"))?;
        let key = OrderedIndexKey::from_values(
            data,
            accelerator.key_columns(),
            &[Value::integer(key_value)],
        )?;
        if let Some(previous) = entries.last() {
            if compare_ordered_keys(previous.key(), &key, accelerator.key_columns())?
                != Ordering::Less
            {
                return Err(invalid("dense integer page keys are out of order"));
            }
        }
        let hash = diagnostic_key_hash(key.as_bytes());
        minimum_hash = minimum_hash.min(hash);
        maximum_hash = maximum_hash.max(hash);
        entries.push(OrderedIndexPageEntry {
            key,
            row_ordinals: vec![row_ordinal],
            has_previous_fragment: false,
            has_next_fragment: false,
        });
    }
    if page.minimum_key_hash() != minimum_hash || page.maximum_key_hash() != maximum_hash {
        return Err(invalid(
            "dense integer page diagnostic key-hash range mismatch",
        ));
    }
    Ok(OrderedIndexPage { entries })
}

fn signed_difference(value: u64, base: u64) -> Option<i64> {
    if value >= base {
        i64::try_from(value - base).ok()
    } else {
        i64::try_from(base - value).ok()?.checked_neg()
    }
}

fn checked_i64_progression(first: i64, step: i64, ordinal: usize) -> Option<i64> {
    let ordinal = i64::try_from(ordinal).ok()?;
    first.checked_add(step.checked_mul(ordinal)?)
}

fn checked_u64_progression(first: u64, step: i64, ordinal: usize) -> Option<u64> {
    let ordinal = i128::try_from(ordinal).ok()?;
    let value = i128::from(first).checked_add(i128::from(step).checked_mul(ordinal)?)?;
    u64::try_from(value).ok()
}

fn page_length(entries: u64, key_bytes: u64, posting_bytes: u64) -> FormatResult<u64> {
    (PAGE_HEADER_BYTES as u64)
        .checked_add(
            entries
                .checked_mul(ENTRY_BYTES as u64)
                .ok_or_else(|| invalid("ordered page directory length overflows"))?,
        )
        .and_then(|length| length.checked_add(key_bytes))
        .and_then(|length| length.checked_add(posting_bytes))
        .ok_or_else(|| invalid("ordered page length overflows"))
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("checked field"))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("checked field"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("checked field"))
}

fn read_i64(bytes: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("checked field"))
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
