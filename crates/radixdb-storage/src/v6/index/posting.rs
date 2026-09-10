use smallvec::SmallVec;

use super::super::{FormatResult, MAX_LOGICAL_BYTES_PER_INDEX_PAGE};
use super::model::{invalid, limit};

pub(super) const NULL_COMPONENT_FLAG: u32 = 1;
pub(super) const PREVIOUS_FRAGMENT_FLAG: u32 = 1 << 1;
pub(super) const NEXT_FRAGMENT_FLAG: u32 = 1 << 2;
pub(super) const KNOWN_ENTRY_FLAGS: u32 =
    NULL_COMPONENT_FLAG | PREVIOUS_FRAGMENT_FLAG | NEXT_FRAGMENT_FLAG;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PostingFragmentState {
    has_previous: bool,
    has_next: bool,
}

impl PostingFragmentState {
    pub(crate) const COMPLETE: Self = Self {
        has_previous: false,
        has_next: false,
    };

    pub(crate) const fn new(has_previous: bool, has_next: bool) -> Self {
        Self {
            has_previous,
            has_next,
        }
    }

    pub(crate) const fn has_previous(self) -> bool {
        self.has_previous
    }

    pub(crate) const fn has_next(self) -> bool {
        self.has_next
    }

    pub(super) const fn flags(self) -> u32 {
        (if self.has_previous {
            PREVIOUS_FRAGMENT_FLAG
        } else {
            0
        }) | (if self.has_next { NEXT_FRAGMENT_FLAG } else { 0 })
    }

    pub(super) const fn from_flags(flags: u32) -> Self {
        Self::new(
            flags & PREVIOUS_FRAGMENT_FLAG != 0,
            flags & NEXT_FRAGMENT_FLAG != 0,
        )
    }
}

pub(super) fn visit_fragments(
    row_ordinals: &[u64],
    outer_state: PostingFragmentState,
    fixed_logical_bytes: u64,
    max_logical_bytes: u64,
    limit_field: &'static str,
    mut visitor: impl FnMut(&[u64], PostingFragmentState, u64) -> FormatResult<()>,
) -> FormatResult<()> {
    if row_ordinals.is_empty() {
        return Err(invalid("posting fragment source is empty"));
    }
    if max_logical_bytes > MAX_LOGICAL_BYTES_PER_INDEX_PAGE {
        return Err(limit(
            limit_field,
            max_logical_bytes,
            MAX_LOGICAL_BYTES_PER_INDEX_PAGE,
        ));
    }
    let posting_budget = max_logical_bytes
        .checked_sub(fixed_logical_bytes)
        .ok_or_else(|| limit(limit_field, fixed_logical_bytes, max_logical_bytes))?;
    if posting_budget == 0 {
        return Err(limit(
            limit_field,
            fixed_logical_bytes.saturating_add(1),
            max_logical_bytes,
        ));
    }

    let mut start = 0_usize;
    while start < row_ordinals.len() {
        let mut end = start;
        let mut encoded_bytes = 0_u64;
        while end < row_ordinals.len() {
            let delta = if end == start {
                row_ordinals[end]
            } else {
                row_ordinals[end]
                    .checked_sub(row_ordinals[end - 1])
                    .ok_or_else(|| invalid("posting row ordinals are not sorted"))?
            };
            if end != start && delta == 0 {
                return Err(invalid("posting row ordinals are not strictly increasing"));
            }
            let candidate = encoded_bytes
                .checked_add(varint_length(delta) as u64)
                .ok_or_else(|| invalid("posting encoded length overflows"))?;
            if candidate > posting_budget {
                break;
            }
            encoded_bytes = candidate;
            end += 1;
        }
        if end == start {
            let required = fixed_logical_bytes
                .checked_add(varint_length(row_ordinals[start]) as u64)
                .ok_or_else(|| invalid("posting fragment page length overflows"))?;
            return Err(limit(limit_field, required, max_logical_bytes));
        }
        let state = PostingFragmentState::new(
            outer_state.has_previous() || start != 0,
            outer_state.has_next() || end != row_ordinals.len(),
        );
        visitor(&row_ordinals[start..end], state, encoded_bytes)?;
        start = end;
    }
    Ok(())
}

pub(super) fn encode_posting(row_ordinals: &[u64]) -> SmallVec<[u8; 16]> {
    let mut output = SmallVec::new();
    let mut previous = 0_u64;
    for (index, ordinal) in row_ordinals.iter().copied().enumerate() {
        let value = if index == 0 {
            ordinal
        } else {
            ordinal - previous
        };
        encode_varint(value, &mut output);
        previous = ordinal;
    }
    output
}

pub(super) fn decode_posting(
    bytes: &[u8],
    row_count: u32,
    source_row_count: u64,
) -> FormatResult<Vec<u64>> {
    let mut output = Vec::with_capacity(row_count as usize);
    visit_posting(bytes, row_count, source_row_count, |ordinal| {
        output.push(ordinal)
    })?;
    Ok(output)
}

pub(super) fn posting_bounds(
    bytes: &[u8],
    row_count: u32,
    source_row_count: u64,
) -> FormatResult<(u64, u64)> {
    let mut first = None;
    let mut last = None;
    visit_posting(bytes, row_count, source_row_count, |ordinal| {
        first.get_or_insert(ordinal);
        last = Some(ordinal);
    })?;
    first
        .zip(last)
        .ok_or_else(|| invalid("posting fragment is empty"))
}

pub(super) fn visit_posting(
    bytes: &[u8],
    row_count: u32,
    source_row_count: u64,
    mut visit: impl FnMut(u64),
) -> FormatResult<()> {
    let mut cursor = 0_usize;
    let mut previous = 0_u64;
    for index in 0..row_count {
        let delta = decode_varint(bytes, &mut cursor)?;
        if index > 0 && delta == 0 {
            return Err(invalid("posting contains a zero delta"));
        }
        let ordinal = if index == 0 {
            delta
        } else {
            previous
                .checked_add(delta)
                .ok_or_else(|| invalid("posting row ordinal overflows"))?
        };
        if ordinal >= source_row_count {
            return Err(invalid("posting row ordinal is outside source data"));
        }
        visit(ordinal);
        previous = ordinal;
    }
    if cursor != bytes.len() {
        return Err(invalid("posting has trailing bytes"));
    }
    Ok(())
}

fn encode_varint(mut value: u64, output: &mut SmallVec<[u8; 16]>) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn decode_varint(bytes: &[u8], cursor: &mut usize) -> FormatResult<u64> {
    let start = *cursor;
    let mut value = 0_u64;
    let mut shift = 0_u32;
    loop {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| invalid("posting varint is truncated"))?;
        *cursor += 1;
        let low = u64::from(byte & 0x7f);
        if shift == 63 && low > 1 || shift > 63 {
            return Err(invalid("posting varint overflows u64"));
        }
        value |= low << shift;
        if byte & 0x80 == 0 {
            let used = *cursor - start;
            if used != varint_length(value) {
                return Err(invalid("posting varint is not minimal"));
            }
            return Ok(value);
        }
        shift += 7;
    }
}

fn varint_length(mut value: u64) -> usize {
    let mut length = 1;
    while value >= 0x80 {
        value >>= 7;
        length += 1;
    }
    length
}
