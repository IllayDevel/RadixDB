use std::mem::size_of;

use radixdb_catalog::ObjectId;
use radixdb_core::Value;
use smallvec::SmallVec;

use super::super::{ArtifactSource, DataArtifactLayout, DataColumnSpec, FormatResult};
use super::codec::read_index_page_from_source;
pub use super::key::MAX_INDEX_KEY_BYTES;
use super::key::{
    diagnostic_key_hash, encode_canonical_key, encode_canonical_key_specs,
    encode_canonical_projected_key_specs, resolve_source_columns, validate_canonical_key_specs,
    CanonicalKeyBytes,
};
use super::model::{
    invalid, limit, validate_data_binding, IndexAccelerator, IndexAcceleratorKind,
    IndexArtifactLayout, IndexKeyColumn, IndexPage, IndexPageCodec, IndexPageSpec,
    MAX_ENTRIES_PER_INDEX_PAGE, MAX_LOGICAL_BYTES_PER_INDEX_PAGE,
};
use super::posting::{
    decode_posting, encode_posting, posting_bounds, visit_fragments, PostingFragmentState,
    KNOWN_ENTRY_FLAGS, NULL_COMPONENT_FLAG,
};

const PAGE_MAGIC: [u8; 4] = *b"IXE2";
const PAGE_VERSION: u16 = 2;
const PAGE_HEADER_BYTES: usize = 40;
const ENTRY_BYTES: usize = 32;

pub const MAX_EXACT_PAGE_DECODE_BYTES: u64 = 512 * 1024 * 1024;
pub const DEFAULT_EXACT_PAGE_ENTRIES: u64 = 4_096;
pub const DEFAULT_EXACT_PAGE_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExactIndexKey {
    bytes: CanonicalKeyBytes,
    has_null_component: bool,
}

impl ExactIndexKey {
    pub fn from_values(
        data: &DataArtifactLayout,
        key_columns: &[IndexKeyColumn],
        values: &[Value],
    ) -> FormatResult<Self> {
        let source_columns = resolve_source_columns(data, key_columns)?;
        let (bytes, has_null_component) = encode_canonical_key(&source_columns, values)?;
        Ok(Self {
            bytes,
            has_null_component,
        })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
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
        Ok(Self {
            bytes,
            has_null_component,
        })
    }

    pub(crate) fn from_canonical_parts(
        bytes: Vec<u8>,
        has_null_component: bool,
    ) -> FormatResult<Self> {
        if bytes.is_empty() || bytes.len() as u64 > MAX_INDEX_KEY_BYTES {
            return Err(limit(
                "index key bytes",
                bytes.len() as u64,
                MAX_INDEX_KEY_BYTES,
            ));
        }
        Ok(Self {
            bytes: bytes.into(),
            has_null_component,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactIndexEntry {
    key: ExactIndexKey,
    row_ordinals: SmallVec<[u64; 4]>,
    fragment_state: PostingFragmentState,
}

impl ExactIndexEntry {
    pub fn new(key: ExactIndexKey, row_ordinals: Vec<u64>) -> FormatResult<Self> {
        Self::from_bounded_rows(key, row_ordinals)
    }

    pub(crate) fn from_bounded_rows(
        key: ExactIndexKey,
        row_ordinals: impl IntoIterator<Item = u64>,
    ) -> FormatResult<Self> {
        Self::from_fragment_rows(key, row_ordinals, PostingFragmentState::COMPLETE)
    }

    pub(crate) fn from_fragment_rows(
        key: ExactIndexKey,
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
                "exact posting row ordinals are not strictly increasing",
            ));
        }
        Ok(Self {
            key,
            row_ordinals,
            fragment_state,
        })
    }

    pub const fn key(&self) -> &ExactIndexKey {
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
pub struct ExactPageBuildLimits {
    max_entries: u64,
    max_logical_bytes: u64,
}

impl ExactPageBuildLimits {
    pub fn new(max_entries: u64, max_logical_bytes: u64) -> FormatResult<Self> {
        if max_entries == 0 || max_entries > MAX_ENTRIES_PER_INDEX_PAGE {
            return Err(limit(
                "exact page entries",
                max_entries,
                MAX_ENTRIES_PER_INDEX_PAGE,
            ));
        }
        if max_logical_bytes < PAGE_HEADER_BYTES as u64
            || max_logical_bytes > MAX_LOGICAL_BYTES_PER_INDEX_PAGE
        {
            return Err(limit(
                "exact page logical bytes",
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

impl Default for ExactPageBuildLimits {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_EXACT_PAGE_ENTRIES,
            max_logical_bytes: DEFAULT_EXACT_PAGE_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactIndexPageEntry {
    key: ExactIndexKey,
    row_ordinals: Vec<u64>,
    has_previous_fragment: bool,
    has_next_fragment: bool,
}

impl ExactIndexPageEntry {
    pub const fn key(&self) -> &ExactIndexKey {
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
pub struct ExactIndexPage {
    entries: Vec<ExactIndexPageEntry>,
}

impl ExactIndexPage {
    pub fn entries(&self) -> &[ExactIndexPageEntry] {
        &self.entries
    }

    pub fn lookup(&self, key: &ExactIndexKey) -> Option<&[u64]> {
        self.entries
            .binary_search_by(|entry| entry.key.cmp(key))
            .ok()
            .map(|index| self.entries[index].row_ordinals.as_slice())
    }
}

pub fn lookup_exact_index(
    bytes: &[u8],
    layout: &IndexArtifactLayout,
    data: &DataArtifactLayout,
    logical_index_id: ObjectId,
    values: &[Value],
) -> FormatResult<Option<Vec<u64>>> {
    lookup_exact_index_from_source(bytes, layout, data, logical_index_id, values)
}

pub fn lookup_exact_index_from_source(
    source: &(impl ArtifactSource + ?Sized),
    layout: &IndexArtifactLayout,
    data: &DataArtifactLayout,
    logical_index_id: ObjectId,
    values: &[Value],
) -> FormatResult<Option<Vec<u64>>> {
    validate_data_binding(layout.header(), data)?;
    let (accelerator_ordinal, accelerator) = layout
        .accelerators()
        .iter()
        .enumerate()
        .find(|(_, accelerator)| accelerator.logical_index_id() == logical_index_id)
        .ok_or_else(|| invalid("logical exact accelerator is absent from index pack"))?;
    if accelerator.kind() != IndexAcceleratorKind::Exact {
        return Err(invalid("logical accelerator is not exact"));
    }
    let key = ExactIndexKey::from_values(data, accelerator.key_columns(), values)?;
    let section_index = accelerator
        .first_section_index()
        .checked_add(1)
        .ok_or_else(|| invalid("exact section index overflows"))? as usize;
    let section = layout
        .sections()
        .get(section_index)
        .copied()
        .ok_or_else(|| invalid("exact page section is absent"))?;
    if section.accelerator_ordinal() != accelerator_ordinal as u32
        || section.kind() != super::super::IndexSectionKind::ExactPages
        || section.page_count() == 0
    {
        return Err(invalid("exact page section ownership is invalid"));
    }

    let first_page = section.first_page_index() as usize;
    let mut low = 0_usize;
    let mut high = section.page_count() as usize;
    let mut candidate_page = None;
    while low < high {
        let middle = low + (high - low) / 2;
        let page_index = first_page
            .checked_add(middle)
            .ok_or_else(|| invalid("exact page index overflows"))?;
        let page = *layout
            .pages()
            .get(page_index)
            .ok_or_else(|| invalid("exact page is outside page directory"))?;
        let logical = read_index_page_from_source(source, layout, page_index)?;
        if page.item_count() == 0 {
            visit_exact_index_page(&logical, page, data, accelerator, |_, _, _, _, _| Ok(()))?;
            layout.validated_exact_pages.remember(page_index, &logical);
            return Ok(None);
        }
        let bounds = if let Some(matches) =
            layout.validated_exact_pages.matches(page_index, &logical)
        {
            if !matches {
                return Err(super::super::FormatError::IndexArtifactChecksumMismatch {
                    scope: "previously validated exact page",
                });
            }
            Some(verified_page_bounds(&logical)?)
        } else {
            let bounds =
                visit_exact_index_page(&logical, page, data, accelerator, |_, _, _, _, _| Ok(()))?;
            layout.validated_exact_pages.remember(page_index, &logical);
            bounds
        };
        let Some((first, last)) = bounds else {
            return Ok(None);
        };
        if accelerator.unique() && first <= key.as_bytes() && key.as_bytes() <= last {
            let mut fragments =
                lookup_verified_exact_page_fragments(&logical, &key, data.header().row_count())?;
            if fragments.len() > 1 {
                return Err(invalid("unique exact key has multiple posting fragments"));
            }
            let Some((rows, fragment_state)) = fragments.pop() else {
                return Ok(None);
            };
            if fragment_state.has_previous() || fragment_state.has_next() {
                return Err(invalid("unique exact key has a continued posting"));
            }
            return Ok(Some(rows));
        }
        if last < key.as_bytes() {
            low = middle + 1;
        } else {
            candidate_page = Some((page_index, logical));
            high = middle;
        }
    }

    let mut output = Vec::new();
    let mut previous_state = PostingFragmentState::COMPLETE;
    let mut previous_last_row = None;
    let mut started = false;
    for relative in low..section.page_count() as usize {
        let page_index = first_page
            .checked_add(relative)
            .ok_or_else(|| invalid("exact page index overflows"))?;
        let page = *layout
            .pages()
            .get(page_index)
            .ok_or_else(|| invalid("exact page is outside page directory"))?;
        let reused = candidate_page
            .take()
            .and_then(|(candidate_index, logical)| {
                (candidate_index == page_index).then_some(logical)
            });
        let (logical, validated_in_routing) = match reused {
            Some(logical) => (logical, true),
            None => (
                read_index_page_from_source(source, layout, page_index)?,
                false,
            ),
        };
        if page.item_count() == 0 {
            return Ok(None);
        }
        let mut fragments = Vec::new();
        let bounds = if validated_in_routing {
            fragments =
                lookup_verified_exact_page_fragments(&logical, &key, data.header().row_count())?;
            Some(verified_page_bounds(&logical)?)
        } else if let Some(matches) = layout.validated_exact_pages.matches(page_index, &logical) {
            if !matches {
                return Err(super::super::FormatError::IndexArtifactChecksumMismatch {
                    scope: "previously validated exact page",
                });
            }
            fragments =
                lookup_verified_exact_page_fragments(&logical, &key, data.header().row_count())?;
            Some(verified_page_bounds(&logical)?)
        } else {
            let bounds = visit_exact_index_page(
                &logical,
                page,
                data,
                accelerator,
                |entry_key, _, posting, count, fragment_state| {
                    if entry_key == key.as_bytes() {
                        fragments.push((
                            decode_posting(posting, count, data.header().row_count())?,
                            fragment_state,
                        ));
                    }
                    Ok(())
                },
            )?;
            layout.validated_exact_pages.remember(page_index, &logical);
            bounds
        };
        let Some((first, last)) = bounds else {
            return Ok(None);
        };
        if key.as_bytes() < first {
            break;
        }
        if key.as_bytes() > last {
            continue;
        }
        if fragments.is_empty() {
            return if started {
                Err(invalid("exact posting continuation chain is broken"))
            } else {
                Ok(None)
            };
        }
        for (rows, fragment_state) in fragments {
            let first_row = rows[0];
            if !started {
                if fragment_state.has_previous() {
                    return Err(invalid("exact posting continuation has no first fragment"));
                }
                started = true;
            } else {
                if !previous_state.has_next() || !fragment_state.has_previous() {
                    return Err(invalid("exact posting continuation chain is broken"));
                }
                if previous_last_row.is_some_and(|last_row| last_row >= first_row) {
                    return Err(invalid(
                        "exact posting continuation rows are not strictly increasing",
                    ));
                }
            }
            previous_last_row = rows.last().copied();
            previous_state = fragment_state;
            output.extend(rows);
        }
        if !previous_state.has_next() {
            return Ok(Some(output));
        }
        if last != key.as_bytes() {
            return Err(invalid("exact posting continuation is not last in page"));
        }
    }
    if started {
        Err(invalid("exact posting continuation is truncated"))
    } else {
        Ok(None)
    }
}

fn verified_page_key(logical: &[u8], index: usize) -> FormatResult<&[u8]> {
    let entry = logical
        .get(PAGE_HEADER_BYTES + index * ENTRY_BYTES..PAGE_HEADER_BYTES + (index + 1) * ENTRY_BYTES)
        .ok_or_else(|| invalid("verified exact directory range is invalid"))?;
    let start = PAGE_HEADER_BYTES + read_u32(logical, 12) as usize + read_u32(entry, 0) as usize;
    logical
        .get(start..start + read_u32(entry, 4) as usize)
        .ok_or_else(|| invalid("verified exact key range is invalid"))
}

fn verified_page_bounds(logical: &[u8]) -> FormatResult<(&[u8], &[u8])> {
    let count = read_u32(logical, 8) as usize;
    Ok((
        verified_page_key(logical, 0)?,
        verified_page_key(logical, count - 1)?,
    ))
}

fn lookup_verified_exact_page_fragments(
    logical: &[u8],
    key: &ExactIndexKey,
    source_rows: u64,
) -> FormatResult<Vec<(Vec<u64>, PostingFragmentState)>> {
    let mut low = 0;
    let mut high = read_u32(logical, 8) as usize;
    while low < high {
        let middle = low + (high - low) / 2;
        match key.as_bytes().cmp(verified_page_key(logical, middle)?) {
            std::cmp::Ordering::Less => high = middle,
            std::cmp::Ordering::Greater => low = middle + 1,
            std::cmp::Ordering::Equal => high = middle,
        }
    }
    let mut fragments = Vec::new();
    let entry_count = read_u32(logical, 8) as usize;
    while low < entry_count && verified_page_key(logical, low)? == key.as_bytes() {
        let entry = &logical
            [PAGE_HEADER_BYTES + low * ENTRY_BYTES..PAGE_HEADER_BYTES + (low + 1) * ENTRY_BYTES];
        let start = PAGE_HEADER_BYTES
            + read_u32(logical, 12) as usize
            + read_u64(logical, 16) as usize
            + read_u32(entry, 8) as usize;
        let posting = logical
            .get(start..start + read_u32(entry, 12) as usize)
            .ok_or_else(|| invalid("verified exact posting range is invalid"))?;
        fragments.push((
            decode_posting(posting, read_u32(entry, 16), source_rows)?,
            PostingFragmentState::from_flags(read_u32(entry, 20)),
        ));
        low += 1;
    }
    Ok(fragments)
}

#[derive(Debug)]
struct PreparedEntry {
    key: ExactIndexKey,
    posting: SmallVec<[u8; 16]>,
    row_count: u32,
    fragment_state: PostingFragmentState,
}

pub fn encode_exact_index_pages(
    mut entries: Vec<ExactIndexEntry>,
    unique: bool,
    source_row_count: u64,
    codec: IndexPageCodec,
    limits: ExactPageBuildLimits,
) -> FormatResult<Vec<IndexPageSpec>> {
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    encode_exact_index_pages_from_sorted(
        entries.into_iter().map(Ok),
        unique,
        source_row_count,
        codec,
        limits,
    )
}

pub(crate) fn encode_exact_index_pages_from_sorted(
    entries: impl IntoIterator<Item = FormatResult<ExactIndexEntry>>,
    unique: bool,
    source_row_count: u64,
    codec: IndexPageCodec,
    limits: ExactPageBuildLimits,
) -> FormatResult<Vec<IndexPageSpec>> {
    let mut pages = Vec::new();
    visit_exact_index_pages_from_sorted(
        entries,
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

pub(crate) fn visit_exact_index_pages_from_sorted(
    entries: impl IntoIterator<Item = FormatResult<ExactIndexEntry>>,
    unique: bool,
    source_row_count: u64,
    codec: IndexPageCodec,
    limits: ExactPageBuildLimits,
    visitor: &mut dyn FnMut(IndexPageSpec) -> FormatResult<()>,
) -> FormatResult<u64> {
    if source_row_count == 0 {
        return Err(invalid("exact accelerator source has no rows"));
    }

    let mut prepared = Vec::new();
    let mut key_bytes = 0_u64;
    let mut posting_bytes = 0_u64;
    let mut previous_key: Option<ExactIndexKey> = None;
    let mut previous_state = PostingFragmentState::COMPLETE;
    let mut previous_last_row = None;
    let mut entry_count = 0_u64;
    let mut page_count = 0_u64;
    for entry in entries {
        let entry = entry?;
        validate_posting_shape(&entry.row_ordinals, false, source_row_count, "exact")?;
        let fixed_logical_bytes = exact_page_length(1, entry.key.bytes.len() as u64, 0)?;
        visit_fragments(
            &entry.row_ordinals,
            entry.fragment_state(),
            fixed_logical_bytes,
            limits.max_logical_bytes(),
            "exact page logical bytes",
            |rows, fragment_state, _| {
                validate_exact_fragment_transition(
                    previous_key.as_ref(),
                    previous_state,
                    previous_last_row,
                    &entry.key,
                    fragment_state,
                    rows[0],
                )?;
                if unique
                    && (rows.len() > 1
                        || fragment_state.has_previous()
                        || fragment_state.has_next())
                {
                    return Err(invalid("unique exact key owns more than one row"));
                }
                let posting = encode_posting(rows);
                let candidate_key_bytes = key_bytes
                    .checked_add(entry.key.bytes.len() as u64)
                    .ok_or_else(|| invalid("exact page key byte count overflows"))?;
                let candidate_posting_bytes = posting_bytes
                    .checked_add(posting.len() as u64)
                    .ok_or_else(|| invalid("exact page posting byte count overflows"))?;
                let candidate_entries = prepared.len() as u64 + 1;
                let candidate_length = exact_page_length(
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
                let required = exact_page_length(
                    prepared.len() as u64 + 1,
                    key_bytes + entry.key.bytes.len() as u64,
                    posting_bytes + posting.len() as u64,
                )?;
                if required > limits.max_logical_bytes() {
                    return Err(limit(
                        "exact page logical bytes",
                        required,
                        limits.max_logical_bytes(),
                    ));
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
                });
                entry_count += 1;
                Ok(())
            },
        )?;
    }
    if entry_count == 0 {
        if !unique {
            return Err(invalid("exact accelerator has no entries"));
        }
        visitor(encode_empty_page(codec)?)?;
        return Ok(1);
    }
    if previous_state.has_next() {
        return Err(invalid("exact posting continuation is truncated"));
    }
    if !prepared.is_empty() {
        visitor(encode_page(&prepared, key_bytes, posting_bytes, codec)?)?;
        page_count += 1;
    }
    Ok(page_count)
}

pub(crate) fn count_exact_index_pages_from_sorted(
    entries: impl IntoIterator<Item = FormatResult<ExactIndexEntry>>,
    unique: bool,
    source_row_count: u64,
    limits: ExactPageBuildLimits,
) -> FormatResult<u64> {
    if source_row_count == 0 {
        return Err(invalid("exact accelerator source has no rows"));
    }
    let mut page_entries = 0_u64;
    let mut key_bytes = 0_u64;
    let mut posting_bytes = 0_u64;
    let mut previous_key: Option<ExactIndexKey> = None;
    let mut previous_state = PostingFragmentState::COMPLETE;
    let mut previous_last_row = None;
    let mut total_entries = 0_u64;
    let mut page_count = 0_u64;
    for entry in entries {
        let entry = entry?;
        validate_posting_shape(&entry.row_ordinals, false, source_row_count, "exact")?;
        let fixed_logical_bytes = exact_page_length(1, entry.key.bytes.len() as u64, 0)?;
        visit_fragments(
            &entry.row_ordinals,
            entry.fragment_state(),
            fixed_logical_bytes,
            limits.max_logical_bytes(),
            "exact page logical bytes",
            |rows, fragment_state, posting_length| {
                validate_exact_fragment_transition(
                    previous_key.as_ref(),
                    previous_state,
                    previous_last_row,
                    &entry.key,
                    fragment_state,
                    rows[0],
                )?;
                if unique
                    && (rows.len() > 1
                        || fragment_state.has_previous()
                        || fragment_state.has_next())
                {
                    return Err(invalid("unique exact key owns more than one row"));
                }
                let candidate_entries = page_entries + 1;
                let candidate_keys = key_bytes
                    .checked_add(entry.key.bytes.len() as u64)
                    .ok_or_else(|| invalid("exact page key byte count overflows"))?;
                let candidate_postings = posting_bytes
                    .checked_add(posting_length)
                    .ok_or_else(|| invalid("exact page posting byte count overflows"))?;
                let candidate_length =
                    exact_page_length(candidate_entries, candidate_keys, candidate_postings)?;
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
        return if unique {
            Ok(1)
        } else {
            Err(invalid("exact accelerator has no entries"))
        };
    }
    if previous_state.has_next() {
        return Err(invalid("exact posting continuation is truncated"));
    }
    Ok(page_count + u64::from(page_entries != 0))
}

fn validate_exact_fragment_transition(
    previous_key: Option<&ExactIndexKey>,
    previous_state: PostingFragmentState,
    previous_last_row: Option<u64>,
    key: &ExactIndexKey,
    state: PostingFragmentState,
    first_row: u64,
) -> FormatResult<()> {
    let Some(previous_key) = previous_key else {
        return if state.has_previous() {
            Err(invalid("exact posting continuation has no first fragment"))
        } else {
            Ok(())
        };
    };
    match previous_key.cmp(key) {
        std::cmp::Ordering::Greater => Err(invalid("exact accelerator entries are out of order")),
        std::cmp::Ordering::Less => {
            if previous_state.has_next() || state.has_previous() {
                Err(invalid("exact posting continuation changes key"))
            } else {
                Ok(())
            }
        }
        std::cmp::Ordering::Equal => {
            if !previous_state.has_next() || !state.has_previous() {
                return Err(invalid(
                    "duplicate exact key is not a canonical continuation",
                ));
            }
            if previous_last_row.is_some_and(|last| last >= first_row) {
                return Err(invalid(
                    "exact posting continuation rows are not strictly increasing",
                ));
            }
            Ok(())
        }
    }
}

pub fn decode_exact_index_page(
    logical_bytes: &[u8],
    page: IndexPage,
    data: &DataArtifactLayout,
    accelerator: &IndexAccelerator,
) -> FormatResult<ExactIndexPage> {
    let mut entries = Vec::<ExactIndexPageEntry>::new();
    visit_exact_index_page(
        logical_bytes,
        page,
        data,
        accelerator,
        |bytes, has_null_component, posting, count, fragment_state| {
            if entries.is_empty() {
                entries.reserve_exact(page.item_count() as usize);
            }
            let row_ordinals = decode_posting(posting, count, data.header().row_count())?;
            if let Some(previous) = entries
                .last_mut()
                .filter(|entry| entry.key.bytes.as_slice() == bytes)
            {
                previous.row_ordinals.extend(row_ordinals);
                previous.has_next_fragment = fragment_state.has_next();
            } else {
                entries.push(ExactIndexPageEntry {
                    key: ExactIndexKey {
                        bytes: bytes.into(),
                        has_null_component,
                    },
                    row_ordinals,
                    has_previous_fragment: fragment_state.has_previous(),
                    has_next_fragment: fragment_state.has_next(),
                });
            }
            Ok(())
        },
    )?;
    Ok(ExactIndexPage { entries })
}

// Visitors must validate every posting, even when they do not retain its rows.
fn visit_exact_index_page<'a>(
    logical_bytes: &'a [u8],
    page: IndexPage,
    data: &DataArtifactLayout,
    accelerator: &IndexAccelerator,
    mut visit: impl FnMut(&'a [u8], bool, &[u8], u32, PostingFragmentState) -> FormatResult<()>,
) -> FormatResult<Option<(&'a [u8], &'a [u8])>> {
    if accelerator.kind() != IndexAcceleratorKind::Exact {
        return Err(invalid("exact page is bound to a non-exact accelerator"));
    }
    if logical_bytes.len() as u64 != page.logical_length() {
        return Err(invalid("exact page length differs from page descriptor"));
    }
    if logical_bytes.len() < PAGE_HEADER_BYTES || logical_bytes[..4] != PAGE_MAGIC {
        return Err(invalid("exact page header is missing"));
    }
    if read_u16(logical_bytes, 4) != PAGE_VERSION
        || read_u16(logical_bytes, 6) != 0
        || read_u32(logical_bytes, 36) != 0
    {
        return Err(invalid(
            "exact page version/flags/reserved field is invalid",
        ));
    }
    let entry_count = read_u32(logical_bytes, 8);
    if u64::from(entry_count) > MAX_ENTRIES_PER_INDEX_PAGE {
        return Err(limit(
            "exact page entries",
            u64::from(entry_count),
            MAX_ENTRIES_PER_INDEX_PAGE,
        ));
    }
    if u64::from(entry_count) != page.item_count() {
        return Err(invalid(
            "exact page entry count differs from page descriptor",
        ));
    }
    let directory_length = read_u32(logical_bytes, 12) as usize;
    if directory_length != entry_count as usize * ENTRY_BYTES {
        return Err(invalid("exact page directory length mismatch"));
    }
    let key_length = usize::try_from(read_u64(logical_bytes, 16))
        .map_err(|_| invalid("exact page key area does not fit this platform"))?;
    let posting_length = usize::try_from(read_u64(logical_bytes, 24))
        .map_err(|_| invalid("exact page posting area does not fit this platform"))?;
    let directory_end = PAGE_HEADER_BYTES
        .checked_add(directory_length)
        .ok_or_else(|| invalid("exact page directory range overflows"))?;
    let key_end = directory_end
        .checked_add(key_length)
        .ok_or_else(|| invalid("exact page key range overflows"))?;
    let posting_end = key_end
        .checked_add(posting_length)
        .ok_or_else(|| invalid("exact page posting range overflows"))?;
    if posting_end != logical_bytes.len() {
        return Err(invalid("exact page areas do not own the complete payload"));
    }
    if read_u32(logical_bytes, 32) != radixdb_core::crc32_ieee(&logical_bytes[PAGE_HEADER_BYTES..])
    {
        return Err(super::super::FormatError::IndexArtifactChecksumMismatch {
            scope: "exact page body",
        });
    }

    if entry_count == 0 {
        if !accelerator.unique()
            || accelerator.indexed_item_count() != 0
            || page.item_count() != 0
            || directory_length != 0
            || key_length != 0
            || posting_length != 0
            || page.minimum_key_hash() != 0
            || page.maximum_key_hash() != 0
            || logical_bytes.len() != PAGE_HEADER_BYTES
        {
            return Err(invalid(
                "empty exact page is not canonical nullable UNIQUE state",
            ));
        }
        return Ok(None);
    }

    validate_decode_allocation(logical_bytes, entry_count, key_length)?;

    let source_columns = resolve_source_columns(data, accelerator.key_columns())?
        .iter()
        .map(|column| {
            DataColumnSpec::new(column.column_id(), column.data_type(), column.nullable())
        })
        .collect::<Vec<_>>();
    let key_area = &logical_bytes[directory_end..key_end];
    let posting_area = &logical_bytes[key_end..posting_end];
    let mut first_key = None;
    let mut previous_key: Option<&[u8]> = None;
    let mut previous_state = PostingFragmentState::COMPLETE;
    let mut previous_last_row = None;
    let mut key_scratch = Vec::new();
    let mut next_key = 0_usize;
    let mut next_posting = 0_usize;
    let mut minimum_hash = u64::MAX;
    let mut maximum_hash = 0_u64;
    for index in 0..entry_count as usize {
        let offset = PAGE_HEADER_BYTES + index * ENTRY_BYTES;
        let entry = &logical_bytes[offset..offset + ENTRY_BYTES];
        let key_offset = read_u32(entry, 0) as usize;
        let key_bytes = read_u32(entry, 4) as usize;
        let posting_offset = read_u32(entry, 8) as usize;
        let posting_bytes = read_u32(entry, 12) as usize;
        let row_count = read_u32(entry, 16);
        let flags = read_u32(entry, 20);
        if key_offset != next_key || posting_offset != next_posting || key_bytes == 0 {
            return Err(invalid("exact page entry ranges are not canonical"));
        }
        if key_bytes as u64 > MAX_INDEX_KEY_BYTES || posting_bytes == 0 || row_count == 0 {
            return Err(invalid("exact page entry lengths/count are invalid"));
        }
        if flags & !KNOWN_ENTRY_FLAGS != 0 {
            return Err(invalid("exact page entry has unknown flags"));
        }
        let fragment_state = PostingFragmentState::from_flags(flags);
        let key_end = key_offset
            .checked_add(key_bytes)
            .ok_or_else(|| invalid("exact key range overflows"))?;
        let posting_end = posting_offset
            .checked_add(posting_bytes)
            .ok_or_else(|| invalid("exact posting range overflows"))?;
        let key_slice = key_area
            .get(key_offset..key_end)
            .ok_or_else(|| invalid("exact key range is outside key area"))?;
        let posting_slice = posting_area
            .get(posting_offset..posting_end)
            .ok_or_else(|| invalid("exact posting range is outside posting area"))?;
        if read_u32(entry, 24) != radixdb_core::crc32_ieee(key_slice) {
            return Err(super::super::FormatError::IndexArtifactChecksumMismatch {
                scope: "exact key",
            });
        }
        if read_u32(entry, 28) != radixdb_core::crc32_ieee(posting_slice) {
            return Err(super::super::FormatError::IndexArtifactChecksumMismatch {
                scope: "exact posting",
            });
        }
        let has_null_component =
            validate_canonical_key_specs(key_slice, &source_columns, &mut key_scratch)?;
        if has_null_component != (flags & NULL_COMPONENT_FLAG != 0) {
            return Err(invalid("exact key NULL flag mismatch"));
        }
        let (first_row, last_row) =
            posting_bounds(posting_slice, row_count, data.header().row_count())?;
        if let Some(previous) = previous_key {
            match previous.cmp(key_slice) {
                std::cmp::Ordering::Greater => {
                    return Err(invalid("exact page keys are out of order"));
                }
                std::cmp::Ordering::Less => {
                    if previous_state.has_next() || fragment_state.has_previous() {
                        return Err(invalid("exact posting continuation changes key"));
                    }
                }
                std::cmp::Ordering::Equal => {
                    if !previous_state.has_next() || !fragment_state.has_previous() {
                        return Err(invalid(
                            "duplicate exact page key is not a canonical continuation",
                        ));
                    }
                    if previous_last_row.is_some_and(|last| last >= first_row) {
                        return Err(invalid(
                            "exact posting continuation rows are not strictly increasing",
                        ));
                    }
                }
            }
        }
        if accelerator.unique()
            && (row_count > 1 || fragment_state.has_previous() || fragment_state.has_next())
        {
            return Err(invalid("unique exact key owns more than one row"));
        }
        visit(
            key_slice,
            has_null_component,
            posting_slice,
            row_count,
            fragment_state,
        )?;
        let hash = diagnostic_key_hash(key_slice);
        minimum_hash = minimum_hash.min(hash);
        maximum_hash = maximum_hash.max(hash);
        first_key.get_or_insert(key_slice);
        previous_key = Some(key_slice);
        previous_state = fragment_state;
        previous_last_row = Some(last_row);
        next_key = key_end;
        next_posting = posting_end;
    }
    if next_key != key_area.len() || next_posting != posting_area.len() {
        return Err(invalid("exact page has unowned key or posting bytes"));
    }
    if page.minimum_key_hash() != minimum_hash || page.maximum_key_hash() != maximum_hash {
        return Err(invalid("exact page diagnostic key-hash range mismatch"));
    }
    Ok(first_key.zip(previous_key))
}

fn validate_decode_allocation(
    logical_bytes: &[u8],
    entry_count: u32,
    key_length: usize,
) -> FormatResult<()> {
    let mut accounted = u64::from(entry_count)
        .checked_mul(size_of::<ExactIndexPageEntry>() as u64)
        .and_then(|bytes| bytes.checked_add(key_length as u64))
        .ok_or_else(|| invalid("exact page decode allocation overflows"))?;
    for index in 0..entry_count as usize {
        let offset = PAGE_HEADER_BYTES + index * ENTRY_BYTES;
        let entry = &logical_bytes[offset..offset + ENTRY_BYTES];
        let posting_bytes = read_u32(entry, 12);
        let row_count = read_u32(entry, 16);
        if posting_bytes < row_count {
            return Err(invalid(
                "exact posting cannot encode its declared row count",
            ));
        }
        accounted = accounted
            .checked_add(u64::from(row_count) * size_of::<u64>() as u64)
            .ok_or_else(|| invalid("exact page decode allocation overflows"))?;
    }
    if accounted > MAX_EXACT_PAGE_DECODE_BYTES {
        return Err(limit(
            "exact page decoded bytes",
            accounted,
            MAX_EXACT_PAGE_DECODE_BYTES,
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
    let length = exact_page_length(entries.len() as u64, key_bytes, posting_bytes)?;
    let capacity = usize::try_from(length)
        .map_err(|_| invalid("exact page length does not fit this platform"))?;
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

fn encode_empty_page(codec: IndexPageCodec) -> FormatResult<IndexPageSpec> {
    let mut output = vec![0_u8; PAGE_HEADER_BYTES];
    output[..4].copy_from_slice(&PAGE_MAGIC);
    put_u16(&mut output, 4, PAGE_VERSION);
    let body_crc = radixdb_core::crc32_ieee(&output[PAGE_HEADER_BYTES..]);
    put_u32(&mut output, 32, body_crc);
    IndexPageSpec::empty_exact(output, codec)
}

fn exact_page_length(entries: u64, key_bytes: u64, posting_bytes: u64) -> FormatResult<u64> {
    (PAGE_HEADER_BYTES as u64)
        .checked_add(
            entries
                .checked_mul(ENTRY_BYTES as u64)
                .ok_or_else(|| invalid("exact page directory length overflows"))?,
        )
        .and_then(|length| length.checked_add(key_bytes))
        .and_then(|length| length.checked_add(posting_bytes))
        .ok_or_else(|| invalid("exact page length overflows"))
}

fn validate_posting_shape(
    row_ordinals: &[u64],
    unique: bool,
    source_row_count: u64,
    owner: &'static str,
) -> FormatResult<()> {
    if row_ordinals.is_empty()
        || row_ordinals.windows(2).any(|pair| pair[0] >= pair[1])
        || row_ordinals
            .last()
            .is_some_and(|ordinal| *ordinal >= source_row_count)
    {
        return Err(invalid(if owner == "exact" {
            "exact posting rows are empty, unordered, or outside source data"
        } else {
            "ordered posting rows are empty, unordered, or outside source data"
        }));
    }
    if unique && row_ordinals.len() > 1 {
        return Err(invalid(if owner == "exact" {
            "unique exact key owns more than one row"
        } else {
            "unique ordered key owns more than one row"
        }));
    }
    Ok(())
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

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
