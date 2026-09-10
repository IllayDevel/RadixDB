use std::mem::size_of;

use radixdb_catalog::{HnswDistanceMetric, ObjectId};
use radixdb_core::DataType;

use super::super::{
    ArtifactSource, DataArtifactLayout, FormatError, FormatResult, IndexSectionKind,
};
use super::codec::{read_index_page_from_source, read_index_section_from_source};
use super::key::diagnostic_key_hash;
use super::model::{
    invalid, limit, IndexAccelerator, IndexAcceleratorKind, IndexArtifactLayout, IndexPage,
    IndexPageCodec, IndexPageSpec, IndexSection, IndexSectionSpec, MAX_ENTRIES_PER_INDEX_PAGE,
    MAX_LOGICAL_BYTES_PER_INDEX_PAGE,
};

const METADATA_MAGIC: [u8; 4] = *b"HNS1";
const METADATA_VERSION: u16 = 1;
const METADATA_BYTES: usize = 64;
const NODE_BYTES: usize = 32;
const ADJACENCY_MAGIC: [u8; 4] = *b"HAD1";
const ADJACENCY_VERSION: u16 = 1;
const ADJACENCY_HEADER_BYTES: usize = 16;
const LEVEL_RECORD_BYTES: usize = 24;

pub const MAX_HNSW_LEVELS: u16 = 64;
pub const MAX_HNSW_NEIGHBORS_PER_LEVEL: u32 = 8_192;
pub const MAX_HNSW_GRAPH_DECODE_BYTES: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_HNSW_PAGE_ENTRIES: u64 = 4_096;
pub const DEFAULT_HNSW_PAGE_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswBuildParameters {
    dimensions: u16,
    m: u16,
    ef_construction: u16,
    distance_metric: HnswDistanceMetric,
}

impl HnswBuildParameters {
    pub fn new(
        dimensions: u16,
        m: u16,
        ef_construction: u16,
        distance_metric: HnswDistanceMetric,
    ) -> FormatResult<Self> {
        if dimensions == 0 {
            return Err(limit("HNSW dimensions", 0, u64::from(u16::MAX)));
        }
        if !(2..=128).contains(&m) {
            return Err(limit("HNSW m", u64::from(m), 128));
        }
        if !(m..=4_096).contains(&ef_construction) {
            return Err(limit(
                "HNSW ef_construction",
                u64::from(ef_construction),
                4_096,
            ));
        }
        Ok(Self {
            dimensions,
            m,
            ef_construction,
            distance_metric,
        })
    }

    pub const fn dimensions(self) -> u16 {
        self.dimensions
    }

    pub const fn m(self) -> u16 {
        self.m
    }

    pub const fn ef_construction(self) -> u16 {
        self.ef_construction
    }

    pub const fn distance_metric(self) -> HnswDistanceMetric {
        self.distance_metric
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HnswGraphNode {
    source_row_ordinal: u64,
    vector_crc32: u32,
    levels: Vec<Vec<u32>>,
}

impl HnswGraphNode {
    pub fn new(
        source_row_ordinal: u64,
        vector_crc32: u32,
        levels: Vec<Vec<u32>>,
    ) -> FormatResult<Self> {
        if levels.is_empty() || levels.len() > usize::from(MAX_HNSW_LEVELS) {
            return Err(limit(
                "HNSW node levels",
                levels.len() as u64,
                u64::from(MAX_HNSW_LEVELS),
            ));
        }
        if let Some(neighbors) = levels
            .iter()
            .find(|neighbors| neighbors.len() as u64 > u64::from(MAX_HNSW_NEIGHBORS_PER_LEVEL))
        {
            return Err(limit(
                "HNSW neighbors per level",
                neighbors.len() as u64,
                u64::from(MAX_HNSW_NEIGHBORS_PER_LEVEL),
            ));
        }
        Ok(Self {
            source_row_ordinal,
            vector_crc32,
            levels,
        })
    }

    pub const fn source_row_ordinal(&self) -> u64 {
        self.source_row_ordinal
    }

    pub const fn vector_crc32(&self) -> u32 {
        self.vector_crc32
    }

    pub fn levels(&self) -> &[Vec<u32>] {
        &self.levels
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HnswIndexGraph {
    parameters: HnswBuildParameters,
    build_seed: u64,
    entry_node_ordinal: u32,
    nodes: Vec<HnswGraphNode>,
}

impl HnswIndexGraph {
    pub fn new(
        parameters: HnswBuildParameters,
        build_seed: u64,
        entry_node_ordinal: u32,
        nodes: Vec<HnswGraphNode>,
    ) -> FormatResult<Self> {
        if nodes.is_empty() || nodes.len() > u32::MAX as usize {
            return Err(limit("HNSW nodes", nodes.len() as u64, u64::from(u32::MAX)));
        }
        if entry_node_ordinal as usize >= nodes.len() {
            return Err(invalid("HNSW entry node is outside graph"));
        }
        Ok(Self {
            parameters,
            build_seed,
            entry_node_ordinal,
            nodes,
        })
    }

    pub const fn parameters(&self) -> HnswBuildParameters {
        self.parameters
    }

    pub const fn build_seed(&self) -> u64 {
        self.build_seed
    }

    pub const fn entry_node_ordinal(&self) -> u32 {
        self.entry_node_ordinal
    }

    pub fn nodes(&self) -> &[HnswGraphNode] {
        &self.nodes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswPageBuildLimits {
    max_entries: u64,
    max_logical_bytes: u64,
}

impl HnswPageBuildLimits {
    pub fn new(max_entries: u64, max_logical_bytes: u64) -> FormatResult<Self> {
        if max_entries == 0 || max_entries > MAX_ENTRIES_PER_INDEX_PAGE {
            return Err(limit(
                "HNSW page entries",
                max_entries,
                MAX_ENTRIES_PER_INDEX_PAGE,
            ));
        }
        if max_logical_bytes < (ADJACENCY_HEADER_BYTES + LEVEL_RECORD_BYTES) as u64
            || max_logical_bytes > MAX_LOGICAL_BYTES_PER_INDEX_PAGE
        {
            return Err(limit(
                "HNSW page logical bytes",
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

impl Default for HnswPageBuildLimits {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_HNSW_PAGE_ENTRIES,
            max_logical_bytes: DEFAULT_HNSW_PAGE_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswDecodeLimits {
    max_accounted_bytes: u64,
}

impl HnswDecodeLimits {
    pub fn new(max_accounted_bytes: u64) -> FormatResult<Self> {
        if max_accounted_bytes == 0 || max_accounted_bytes > MAX_HNSW_GRAPH_DECODE_BYTES {
            return Err(limit(
                "HNSW decoded graph bytes",
                max_accounted_bytes,
                MAX_HNSW_GRAPH_DECODE_BYTES,
            ));
        }
        Ok(Self {
            max_accounted_bytes,
        })
    }

    pub const fn max_accounted_bytes(self) -> u64 {
        self.max_accounted_bytes
    }
}

impl Default for HnswDecodeLimits {
    fn default() -> Self {
        Self {
            max_accounted_bytes: MAX_HNSW_GRAPH_DECODE_BYTES,
        }
    }
}

#[derive(Debug)]
struct PreparedNode {
    source_row_ordinal: u64,
    vector_crc32: u32,
    maximum_level: u16,
    first_level_record: u32,
    levels: Vec<Vec<u32>>,
}

#[derive(Debug)]
struct LevelRecord {
    node_ordinal: u32,
    level: u16,
    neighbors: Vec<u32>,
}

#[derive(Debug, Clone, Copy)]
struct Metadata {
    parameters: HnswBuildParameters,
    node_count: u32,
    entry_node_ordinal: u32,
    maximum_level: u16,
    build_seed: u64,
    nodes_crc32: u32,
    adjacency_crc32: u32,
}

pub fn encode_hnsw_index_sections(
    graph: &HnswIndexGraph,
    source_row_count: u64,
    codec: IndexPageCodec,
    limits: HnswPageBuildLimits,
) -> FormatResult<Vec<IndexSectionSpec>> {
    let (nodes, entry_node_ordinal, maximum_level) = prepare_graph(graph, source_row_count)?;
    let node_pages = encode_node_pages(&nodes, codec, limits)?;
    let level_records = collect_level_records(&nodes)?;
    let adjacency_pages = encode_adjacency_pages(&level_records, codec, limits)?;
    let metadata = encode_metadata(Metadata {
        parameters: graph.parameters(),
        node_count: nodes.len() as u32,
        entry_node_ordinal,
        maximum_level,
        build_seed: graph.build_seed(),
        nodes_crc32: logical_pages_crc32(&node_pages),
        adjacency_crc32: logical_pages_crc32(&adjacency_pages),
    });
    Ok(vec![
        IndexSectionSpec::metadata(IndexSectionKind::HnswMetadata, metadata, 1)?,
        IndexSectionSpec::pages(IndexSectionKind::HnswNodes, node_pages)?,
        IndexSectionSpec::pages(IndexSectionKind::HnswAdjacency, adjacency_pages)?,
    ])
}

pub fn decode_hnsw_index(
    bytes: &[u8],
    layout: &IndexArtifactLayout,
    data: &DataArtifactLayout,
    logical_index_id: ObjectId,
    expected: HnswBuildParameters,
) -> FormatResult<HnswIndexGraph> {
    decode_hnsw_index_from_source(
        bytes,
        layout,
        data,
        logical_index_id,
        expected,
        HnswDecodeLimits::default(),
    )
}

pub fn decode_hnsw_index_from_source(
    source: &(impl ArtifactSource + ?Sized),
    layout: &IndexArtifactLayout,
    data: &DataArtifactLayout,
    logical_index_id: ObjectId,
    expected: HnswBuildParameters,
    limits: HnswDecodeLimits,
) -> FormatResult<HnswIndexGraph> {
    let (accelerator_ordinal, accelerator) = layout
        .accelerators()
        .iter()
        .enumerate()
        .find(|(_, accelerator)| accelerator.logical_index_id() == logical_index_id)
        .ok_or_else(|| invalid("logical HNSW accelerator is absent from index pack"))?;
    validate_hnsw_binding(data, accelerator, expected)?;
    let (metadata_index, nodes, adjacency) =
        hnsw_sections(layout, accelerator_ordinal, accelerator)?;
    let metadata_bytes = read_index_section_from_source(source, layout, metadata_index)?;
    let metadata = decode_metadata(&metadata_bytes)?;
    if metadata.parameters != expected {
        return Err(invalid(
            "HNSW physical build parameters differ from catalog definition",
        ));
    }
    if u64::from(metadata.node_count) != accelerator.indexed_item_count()
        || nodes.reference().item_count() != u64::from(metadata.node_count)
    {
        return Err(invalid("HNSW node count differs from accelerator metadata"));
    }

    let mut budget = DecodeBudget::new(limits);
    budget.account(
        u64::from(metadata.node_count)
            .checked_mul(size_of::<DecodedNode>() as u64)
            .ok_or_else(|| invalid("HNSW node allocation overflows"))?,
    )?;
    let (decoded_nodes, nodes_crc32) =
        decode_node_section(source, layout, data, nodes, metadata, &mut budget)?;
    if nodes_crc32 != metadata.nodes_crc32 {
        return Err(FormatError::IndexArtifactChecksumMismatch {
            scope: "HNSW logical nodes",
        });
    }
    let expected_level_records = decoded_nodes.iter().try_fold(0_u64, |total, node| {
        total
            .checked_add(u64::from(node.level_record_count))
            .ok_or_else(|| invalid("HNSW level-record count overflows"))
    })?;
    if adjacency.reference().item_count() != expected_level_records {
        return Err(invalid(
            "HNSW adjacency count differs from node level ownership",
        ));
    }
    let (records, adjacency_crc32) =
        decode_adjacency_section(source, layout, adjacency, metadata.node_count, &mut budget)?;
    if adjacency_crc32 != metadata.adjacency_crc32 {
        return Err(FormatError::IndexArtifactChecksumMismatch {
            scope: "HNSW logical adjacency",
        });
    }
    budget.account(
        u64::from(metadata.node_count)
            .checked_mul(size_of::<HnswGraphNode>() as u64)
            .ok_or_else(|| invalid("HNSW graph-node allocation overflows"))?,
    )?;
    assemble_graph(metadata, decoded_nodes, records)
}

fn prepare_graph(
    graph: &HnswIndexGraph,
    source_row_count: u64,
) -> FormatResult<(Vec<PreparedNode>, u32, u16)> {
    if source_row_count == 0 || graph.nodes().len() as u64 > source_row_count {
        return Err(invalid("HNSW graph exceeds source rows"));
    }
    let node_count = graph.nodes().len();
    let mut order = (0..node_count).collect::<Vec<_>>();
    order.sort_by_key(|index| graph.nodes()[*index].source_row_ordinal());
    if order.windows(2).any(|pair| {
        graph.nodes()[pair[0]].source_row_ordinal() == graph.nodes()[pair[1]].source_row_ordinal()
    }) {
        return Err(invalid("HNSW graph contains duplicate source row ordinals"));
    }
    let mut remap = vec![0_u32; node_count];
    for (new_ordinal, old_ordinal) in order.iter().copied().enumerate() {
        remap[old_ordinal] = new_ordinal as u32;
    }
    let entry_node_ordinal = *remap
        .get(graph.entry_node_ordinal() as usize)
        .ok_or_else(|| invalid("HNSW entry node is outside graph"))?;
    let mut first_level_record = 0_u32;
    let mut maximum_level = 0_u16;
    let mut nodes = Vec::with_capacity(node_count);
    for (new_ordinal, old_ordinal) in order.into_iter().enumerate() {
        let source = &graph.nodes()[old_ordinal];
        if source.source_row_ordinal() >= source_row_count {
            return Err(invalid("HNSW node points outside source data"));
        }
        let maximum = u16::try_from(source.levels().len() - 1)
            .map_err(|_| invalid("HNSW node level does not fit persisted width"))?;
        maximum_level = maximum_level.max(maximum);
        let mut levels = Vec::with_capacity(source.levels().len());
        for neighbors in source.levels() {
            let mut mapped = Vec::with_capacity(neighbors.len());
            for old_neighbor in neighbors {
                let old_neighbor = *old_neighbor as usize;
                if old_neighbor >= node_count || old_neighbor == old_ordinal {
                    return Err(invalid("HNSW neighbor is invalid or a self edge"));
                }
                mapped.push(remap[old_neighbor]);
            }
            mapped.sort_unstable();
            if mapped.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(invalid("HNSW neighbor list contains duplicates"));
            }
            levels.push(mapped);
        }
        let level_count = u32::try_from(levels.len())
            .map_err(|_| invalid("HNSW level count does not fit persisted width"))?;
        nodes.push(PreparedNode {
            source_row_ordinal: source.source_row_ordinal(),
            vector_crc32: source.vector_crc32(),
            maximum_level: maximum,
            first_level_record,
            levels,
        });
        first_level_record = first_level_record
            .checked_add(level_count)
            .ok_or_else(|| invalid("HNSW level-record count overflows"))?;
        debug_assert_eq!(nodes.len() - 1, new_ordinal);
    }
    validate_prepared_topology(&nodes, entry_node_ordinal, maximum_level)?;
    Ok((nodes, entry_node_ordinal, maximum_level))
}

fn validate_prepared_topology(
    nodes: &[PreparedNode],
    entry_node_ordinal: u32,
    maximum_level: u16,
) -> FormatResult<()> {
    let entry = nodes
        .get(entry_node_ordinal as usize)
        .ok_or_else(|| invalid("HNSW entry node is outside graph"))?;
    if entry.maximum_level != maximum_level {
        return Err(invalid("HNSW entry node is not on the maximum level"));
    }
    for node in nodes {
        for (level, neighbors) in node.levels.iter().enumerate() {
            for neighbor in neighbors {
                let neighbor = nodes
                    .get(*neighbor as usize)
                    .ok_or_else(|| invalid("HNSW neighbor is outside graph"))?;
                if usize::from(neighbor.maximum_level) < level {
                    return Err(invalid(
                        "HNSW edge references a node absent from that level",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn collect_level_records(nodes: &[PreparedNode]) -> FormatResult<Vec<LevelRecord>> {
    let count = nodes.iter().try_fold(0_usize, |total, node| {
        total
            .checked_add(node.levels.len())
            .ok_or_else(|| invalid("HNSW level-record count overflows"))
    })?;
    let mut records = Vec::with_capacity(count);
    for (node_ordinal, node) in nodes.iter().enumerate() {
        for (level, neighbors) in node.levels.iter().enumerate() {
            records.push(LevelRecord {
                node_ordinal: node_ordinal as u32,
                level: level as u16,
                neighbors: neighbors.clone(),
            });
        }
    }
    Ok(records)
}

fn encode_node_pages(
    nodes: &[PreparedNode],
    codec: IndexPageCodec,
    limits: HnswPageBuildLimits,
) -> FormatResult<Vec<IndexPageSpec>> {
    let byte_limit = usize::try_from(limits.max_logical_bytes())
        .map_err(|_| invalid("HNSW page limit does not fit this platform"))?;
    let by_bytes = byte_limit / NODE_BYTES;
    let per_page = usize::try_from(limits.max_entries())
        .unwrap_or(usize::MAX)
        .min(by_bytes);
    if per_page == 0 {
        return Err(limit(
            "HNSW node page logical bytes",
            NODE_BYTES as u64,
            limits.max_logical_bytes(),
        ));
    }
    let mut pages = Vec::new();
    for chunk in nodes.chunks(per_page) {
        let mut bytes = vec![0_u8; chunk.len() * NODE_BYTES];
        let mut minimum_hash = u64::MAX;
        let mut maximum_hash = 0_u64;
        for (index, node) in chunk.iter().enumerate() {
            let entry = &mut bytes[index * NODE_BYTES..(index + 1) * NODE_BYTES];
            put_u64(entry, 0, node.source_row_ordinal);
            put_u16(entry, 8, node.maximum_level);
            put_u32(entry, 12, node.first_level_record);
            put_u32(entry, 16, node.levels.len() as u32);
            put_u32(entry, 20, node.vector_crc32);
            let hash = diagnostic_key_hash(&node.source_row_ordinal.to_le_bytes());
            minimum_hash = minimum_hash.min(hash);
            maximum_hash = maximum_hash.max(hash);
        }
        pages.push(IndexPageSpec::new(
            bytes,
            codec,
            chunk.len() as u64,
            minimum_hash,
            maximum_hash,
        )?);
    }
    Ok(pages)
}

fn encode_adjacency_pages(
    records: &[LevelRecord],
    codec: IndexPageCodec,
    limits: HnswPageBuildLimits,
) -> FormatResult<Vec<IndexPageSpec>> {
    let mut pages = Vec::new();
    let mut start = 0_usize;
    while start < records.len() {
        let mut end = start;
        let mut neighbor_bytes = 0_u64;
        while end < records.len() && ((end - start) as u64) < limits.max_entries() {
            let candidate_neighbors = neighbor_bytes
                .checked_add(records[end].neighbors.len() as u64 * 4)
                .ok_or_else(|| invalid("HNSW adjacency page length overflows"))?;
            let candidate_records = (end - start + 1) as u64;
            let candidate_length = adjacency_page_length(candidate_records, candidate_neighbors)?;
            if candidate_length > limits.max_logical_bytes() {
                break;
            }
            neighbor_bytes = candidate_neighbors;
            end += 1;
        }
        if end == start {
            let required = adjacency_page_length(1, records[start].neighbors.len() as u64 * 4)?;
            return Err(limit(
                "HNSW adjacency page logical bytes",
                required,
                limits.max_logical_bytes(),
            ));
        }
        pages.push(encode_adjacency_page(
            &records[start..end],
            neighbor_bytes,
            codec,
        )?);
        start = end;
    }
    Ok(pages)
}

fn encode_adjacency_page(
    records: &[LevelRecord],
    neighbor_bytes: u64,
    codec: IndexPageCodec,
) -> FormatResult<IndexPageSpec> {
    let length = usize::try_from(adjacency_page_length(records.len() as u64, neighbor_bytes)?)
        .map_err(|_| invalid("HNSW adjacency page does not fit this platform"))?;
    let directory_end = ADJACENCY_HEADER_BYTES + records.len() * LEVEL_RECORD_BYTES;
    let mut bytes = vec![0_u8; length];
    bytes[..4].copy_from_slice(&ADJACENCY_MAGIC);
    put_u16(&mut bytes, 4, ADJACENCY_VERSION);
    put_u32(&mut bytes, 8, records.len() as u32);
    let mut neighbor_offset = 0_usize;
    let mut minimum_hash = u64::MAX;
    let mut maximum_hash = 0_u64;
    for (index, record) in records.iter().enumerate() {
        let entry_start = ADJACENCY_HEADER_BYTES + index * LEVEL_RECORD_BYTES;
        put_u32(&mut bytes, entry_start, record.node_ordinal);
        put_u16(&mut bytes, entry_start + 4, record.level);
        put_u64(&mut bytes, entry_start + 8, neighbor_offset as u64);
        put_u32(&mut bytes, entry_start + 16, record.neighbors.len() as u32);
        let neighbor_start = directory_end + neighbor_offset;
        for (neighbor_index, neighbor) in record.neighbors.iter().copied().enumerate() {
            put_u32(&mut bytes, neighbor_start + neighbor_index * 4, neighbor);
        }
        let neighbor_end = neighbor_start + record.neighbors.len() * 4;
        let neighbor_crc32 = radixdb_core::crc32_ieee(&bytes[neighbor_start..neighbor_end]);
        put_u32(&mut bytes, entry_start + 20, neighbor_crc32);
        neighbor_offset += record.neighbors.len() * 4;
        let hash = level_record_hash(record.node_ordinal, record.level);
        minimum_hash = minimum_hash.min(hash);
        maximum_hash = maximum_hash.max(hash);
    }
    if directory_end + neighbor_offset != bytes.len() {
        return Err(invalid("HNSW adjacency page ownership is inconsistent"));
    }
    IndexPageSpec::new(
        bytes,
        codec,
        records.len() as u64,
        minimum_hash,
        maximum_hash,
    )
}

fn adjacency_page_length(records: u64, neighbor_bytes: u64) -> FormatResult<u64> {
    (ADJACENCY_HEADER_BYTES as u64)
        .checked_add(
            records
                .checked_mul(LEVEL_RECORD_BYTES as u64)
                .ok_or_else(|| invalid("HNSW adjacency directory length overflows"))?,
        )
        .and_then(|length| length.checked_add(neighbor_bytes))
        .ok_or_else(|| invalid("HNSW adjacency page length overflows"))
}

fn logical_pages_crc32(pages: &[IndexPageSpec]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    for page in pages {
        hasher.update(page.logical_bytes());
    }
    hasher.finalize()
}

fn encode_metadata(metadata: Metadata) -> Vec<u8> {
    let mut bytes = vec![0_u8; METADATA_BYTES];
    bytes[..4].copy_from_slice(&METADATA_MAGIC);
    put_u16(&mut bytes, 4, METADATA_VERSION);
    put_u16(&mut bytes, 6, metadata.parameters.distance_metric().tag());
    put_u32(&mut bytes, 8, u32::from(metadata.parameters.dimensions()));
    put_u32(&mut bytes, 12, u32::from(metadata.parameters.m()));
    put_u32(
        &mut bytes,
        16,
        u32::from(metadata.parameters.ef_construction()),
    );
    put_u32(&mut bytes, 20, metadata.node_count);
    put_u32(&mut bytes, 24, metadata.entry_node_ordinal);
    put_u16(&mut bytes, 28, metadata.maximum_level);
    put_u64(&mut bytes, 32, metadata.build_seed);
    put_u32(&mut bytes, 40, metadata.nodes_crc32);
    put_u32(&mut bytes, 44, metadata.adjacency_crc32);
    bytes
}

fn decode_metadata(bytes: &[u8]) -> FormatResult<Metadata> {
    if bytes.len() != METADATA_BYTES || bytes[..4] != METADATA_MAGIC {
        return Err(invalid("HNSW metadata header is invalid"));
    }
    if read_u16(bytes, 4) != METADATA_VERSION
        || read_u16(bytes, 30) != 0
        || bytes[48..].iter().any(|byte| *byte != 0)
    {
        return Err(invalid(
            "HNSW metadata version/flags/reserved field is invalid",
        ));
    }
    let dimensions = u16::try_from(read_u32(bytes, 8))
        .map_err(|_| invalid("HNSW dimensions do not fit supported range"))?;
    let m = u16::try_from(read_u32(bytes, 12)).map_err(|_| invalid("HNSW m is out of range"))?;
    let ef_construction = u16::try_from(read_u32(bytes, 16))
        .map_err(|_| invalid("HNSW ef_construction is out of range"))?;
    let metric = HnswDistanceMetric::try_from(read_u16(bytes, 6))
        .map_err(|_| invalid("HNSW distance metric is unknown"))?;
    let parameters = HnswBuildParameters::new(dimensions, m, ef_construction, metric)?;
    let node_count = read_u32(bytes, 20);
    let entry_node_ordinal = read_u32(bytes, 24);
    let maximum_level = read_u16(bytes, 28);
    if node_count == 0 || entry_node_ordinal >= node_count || maximum_level >= MAX_HNSW_LEVELS {
        return Err(invalid("HNSW metadata graph bounds are invalid"));
    }
    Ok(Metadata {
        parameters,
        node_count,
        entry_node_ordinal,
        maximum_level,
        build_seed: read_u64(bytes, 32),
        nodes_crc32: read_u32(bytes, 40),
        adjacency_crc32: read_u32(bytes, 44),
    })
}

fn validate_hnsw_binding(
    data: &DataArtifactLayout,
    accelerator: &IndexAccelerator,
    expected: HnswBuildParameters,
) -> FormatResult<()> {
    if accelerator.kind() != IndexAcceleratorKind::Hnsw
        || accelerator.key_columns().len() != 1
        || accelerator.key_columns()[0].logical_type() != DataType::Vector
    {
        return Err(invalid(
            "logical accelerator is not a valid HNSW definition",
        ));
    }
    let source = data
        .columns()
        .iter()
        .find(|column| column.column_id() == accelerator.key_columns()[0].column_id())
        .ok_or_else(|| invalid("HNSW key column is absent from source data"))?;
    if source.data_type().logical_type() != DataType::Vector
        || source.data_type().parameter_1() != u32::from(expected.dimensions())
    {
        return Err(invalid(
            "HNSW dimensions differ from source VECTOR definition",
        ));
    }
    Ok(())
}

fn hnsw_sections(
    layout: &IndexArtifactLayout,
    accelerator_ordinal: usize,
    accelerator: &IndexAccelerator,
) -> FormatResult<(usize, IndexSection, IndexSection)> {
    if accelerator.section_count() != 4 {
        return Err(invalid("HNSW accelerator section count is invalid"));
    }
    let first = accelerator.first_section_index() as usize;
    let owned = layout
        .sections()
        .get(first..first + 4)
        .ok_or_else(|| invalid("HNSW section range is outside directory"))?;
    let kinds = [
        IndexSectionKind::KeyDescriptor,
        IndexSectionKind::HnswMetadata,
        IndexSectionKind::HnswNodes,
        IndexSectionKind::HnswAdjacency,
    ];
    if owned.iter().zip(kinds).any(|(section, kind)| {
        section.accelerator_ordinal() != accelerator_ordinal as u32
            || section.reference().section_kind() != kind
    }) {
        return Err(invalid("HNSW section ownership is invalid"));
    }
    Ok((first + 1, owned[2], owned[3]))
}

#[derive(Debug)]
struct DecodedNode {
    source_row_ordinal: u64,
    maximum_level: u16,
    first_level_record: u32,
    level_record_count: u32,
    vector_crc32: u32,
}

fn decode_node_section(
    source: &(impl ArtifactSource + ?Sized),
    layout: &IndexArtifactLayout,
    data: &DataArtifactLayout,
    section: IndexSection,
    metadata: Metadata,
    budget: &mut DecodeBudget,
) -> FormatResult<(Vec<DecodedNode>, u32)> {
    let mut nodes = Vec::with_capacity(metadata.node_count as usize);
    let mut hasher = crc32fast::Hasher::new();
    for page_index in page_indices(section)? {
        let page = *layout
            .pages()
            .get(page_index)
            .ok_or_else(|| invalid("HNSW node page is outside directory"))?;
        budget.account(page.logical_length())?;
        let bytes = read_index_page_from_source(source, layout, page_index)?;
        hasher.update(&bytes);
        decode_node_page(&bytes, page, data.header().row_count(), &mut nodes)?;
    }
    if nodes.len() != metadata.node_count as usize {
        return Err(invalid("HNSW decoded node count is inconsistent"));
    }
    Ok((nodes, hasher.finalize()))
}

fn decode_node_page(
    bytes: &[u8],
    page: IndexPage,
    source_row_count: u64,
    output: &mut Vec<DecodedNode>,
) -> FormatResult<()> {
    let count = usize::try_from(page.item_count())
        .map_err(|_| invalid("HNSW node count does not fit this platform"))?;
    let expected_length = count
        .checked_mul(NODE_BYTES)
        .ok_or_else(|| invalid("HNSW node page length overflows"))?;
    if count == 0 || bytes.len() != expected_length || bytes.len() as u64 != page.logical_length() {
        return Err(invalid("HNSW node page shape is invalid"));
    }
    let mut minimum_hash = u64::MAX;
    let mut maximum_hash = 0_u64;
    for index in 0..count {
        let entry = &bytes[index * NODE_BYTES..(index + 1) * NODE_BYTES];
        if read_u16(entry, 10) != 0 || entry[24..].iter().any(|byte| *byte != 0) {
            return Err(invalid("HNSW node flags/reserved field is non-zero"));
        }
        let source_row_ordinal = read_u64(entry, 0);
        let maximum_level = read_u16(entry, 8);
        let first_level_record = read_u32(entry, 12);
        let level_record_count = read_u32(entry, 16);
        if source_row_ordinal >= source_row_count
            || maximum_level >= MAX_HNSW_LEVELS
            || level_record_count != u32::from(maximum_level) + 1
            || output
                .last()
                .is_some_and(|previous| previous.source_row_ordinal >= source_row_ordinal)
        {
            return Err(invalid("HNSW node record is non-canonical"));
        }
        let expected_first = output.last().map_or(Ok(0), |previous| {
            previous
                .first_level_record
                .checked_add(previous.level_record_count)
                .ok_or_else(|| invalid("HNSW node level ownership overflows"))
        })?;
        if first_level_record != expected_first {
            return Err(invalid("HNSW node level ownership is not contiguous"));
        }
        let hash = diagnostic_key_hash(&source_row_ordinal.to_le_bytes());
        minimum_hash = minimum_hash.min(hash);
        maximum_hash = maximum_hash.max(hash);
        output.push(DecodedNode {
            source_row_ordinal,
            maximum_level,
            first_level_record,
            level_record_count,
            vector_crc32: read_u32(entry, 20),
        });
    }
    if page.minimum_key_hash() != minimum_hash || page.maximum_key_hash() != maximum_hash {
        return Err(invalid("HNSW node page diagnostic hash range mismatch"));
    }
    Ok(())
}

fn decode_adjacency_section(
    source: &(impl ArtifactSource + ?Sized),
    layout: &IndexArtifactLayout,
    section: IndexSection,
    node_count: u32,
    budget: &mut DecodeBudget,
) -> FormatResult<(Vec<LevelRecord>, u32)> {
    let record_count = usize::try_from(section.reference().item_count())
        .map_err(|_| invalid("HNSW level-record count does not fit this platform"))?;
    budget.account(
        (record_count as u64)
            .checked_mul((size_of::<LevelRecord>() + size_of::<Vec<u32>>()) as u64)
            .ok_or_else(|| invalid("HNSW adjacency allocation overflows"))?,
    )?;
    let mut records = Vec::with_capacity(record_count);
    let mut hasher = crc32fast::Hasher::new();
    for page_index in page_indices(section)? {
        let page = *layout
            .pages()
            .get(page_index)
            .ok_or_else(|| invalid("HNSW adjacency page is outside directory"))?;
        budget.account(page.logical_length())?;
        let bytes = read_index_page_from_source(source, layout, page_index)?;
        hasher.update(&bytes);
        decode_adjacency_page(&bytes, page, node_count, budget, &mut records)?;
    }
    if records.len() != record_count {
        return Err(invalid(
            "HNSW decoded adjacency count differs from section metadata",
        ));
    }
    Ok((records, hasher.finalize()))
}

fn decode_adjacency_page(
    bytes: &[u8],
    page: IndexPage,
    node_count: u32,
    budget: &mut DecodeBudget,
    output: &mut Vec<LevelRecord>,
) -> FormatResult<()> {
    if bytes.len() < ADJACENCY_HEADER_BYTES
        || bytes[..4] != ADJACENCY_MAGIC
        || read_u16(bytes, 4) != ADJACENCY_VERSION
        || read_u16(bytes, 6) != 0
        || read_u32(bytes, 12) != 0
        || bytes.len() as u64 != page.logical_length()
    {
        return Err(invalid("HNSW adjacency page header is invalid"));
    }
    let count = read_u32(bytes, 8);
    if count == 0 || u64::from(count) != page.item_count() {
        return Err(invalid("HNSW adjacency page count is invalid"));
    }
    let directory_end = ADJACENCY_HEADER_BYTES
        .checked_add(count as usize * LEVEL_RECORD_BYTES)
        .ok_or_else(|| invalid("HNSW adjacency directory overflows"))?;
    let packed = bytes
        .get(directory_end..)
        .ok_or_else(|| invalid("HNSW adjacency directory is truncated"))?;
    let mut next_neighbor_offset = 0_usize;
    let mut minimum_hash = u64::MAX;
    let mut maximum_hash = 0_u64;
    for index in 0..count as usize {
        let start = ADJACENCY_HEADER_BYTES + index * LEVEL_RECORD_BYTES;
        let entry = &bytes[start..start + LEVEL_RECORD_BYTES];
        let node_ordinal = read_u32(entry, 0);
        let level = read_u16(entry, 4);
        let neighbor_offset = usize::try_from(read_u64(entry, 8))
            .map_err(|_| invalid("HNSW neighbor offset does not fit this platform"))?;
        let neighbor_count = read_u32(entry, 16);
        if read_u16(entry, 6) != 0
            || node_ordinal >= node_count
            || level >= MAX_HNSW_LEVELS
            || neighbor_count > MAX_HNSW_NEIGHBORS_PER_LEVEL
            || neighbor_offset != next_neighbor_offset
        {
            return Err(invalid("HNSW adjacency record is non-canonical"));
        }
        let neighbor_bytes = usize::try_from(neighbor_count)
            .ok()
            .and_then(|count| count.checked_mul(4))
            .ok_or_else(|| invalid("HNSW neighbor byte count overflows"))?;
        let neighbor_end = neighbor_offset
            .checked_add(neighbor_bytes)
            .ok_or_else(|| invalid("HNSW neighbor range overflows"))?;
        let neighbor_slice = packed
            .get(neighbor_offset..neighbor_end)
            .ok_or_else(|| invalid("HNSW neighbor range is outside page"))?;
        if radixdb_core::crc32_ieee(neighbor_slice) != read_u32(entry, 20) {
            return Err(FormatError::IndexArtifactChecksumMismatch {
                scope: "HNSW neighbor list",
            });
        }
        budget.account(neighbor_bytes as u64)?;
        let mut neighbors = Vec::with_capacity(neighbor_count as usize);
        for bytes in neighbor_slice.chunks_exact(4) {
            let neighbor = u32::from_le_bytes(bytes.try_into().expect("four-byte chunk"));
            if neighbor >= node_count
                || neighbor == node_ordinal
                || neighbors
                    .last()
                    .is_some_and(|previous| *previous >= neighbor)
            {
                return Err(invalid("HNSW neighbor list is non-canonical"));
            }
            neighbors.push(neighbor);
        }
        if output.last().is_some_and(|previous| {
            (previous.node_ordinal, previous.level) >= (node_ordinal, level)
        }) {
            return Err(invalid("HNSW level records are not strictly ordered"));
        }
        let hash = level_record_hash(node_ordinal, level);
        minimum_hash = minimum_hash.min(hash);
        maximum_hash = maximum_hash.max(hash);
        output.push(LevelRecord {
            node_ordinal,
            level,
            neighbors,
        });
        next_neighbor_offset = neighbor_end;
    }
    if next_neighbor_offset != packed.len() {
        return Err(invalid("HNSW adjacency page has unowned neighbor bytes"));
    }
    if page.minimum_key_hash() != minimum_hash || page.maximum_key_hash() != maximum_hash {
        return Err(invalid(
            "HNSW adjacency page diagnostic hash range mismatch",
        ));
    }
    Ok(())
}

fn assemble_graph(
    metadata: Metadata,
    nodes: Vec<DecodedNode>,
    records: Vec<LevelRecord>,
) -> FormatResult<HnswIndexGraph> {
    let actual_maximum = nodes
        .iter()
        .map(|node| node.maximum_level)
        .max()
        .ok_or_else(|| invalid("HNSW graph contains no nodes"))?;
    if actual_maximum != metadata.maximum_level
        || nodes[metadata.entry_node_ordinal as usize].maximum_level != metadata.maximum_level
    {
        return Err(invalid("HNSW maximum level or entry node is inconsistent"));
    }
    let mut records = records.into_iter();
    let mut graph_nodes = Vec::with_capacity(nodes.len());
    for (node_ordinal, node) in nodes.iter().enumerate() {
        let mut levels = Vec::with_capacity(node.level_record_count as usize);
        for expected_level in 0..node.level_record_count as usize {
            let record = records
                .next()
                .ok_or_else(|| invalid("HNSW node level range is outside adjacency"))?;
            if record.node_ordinal != node_ordinal as u32 || record.level as usize != expected_level
            {
                return Err(invalid("HNSW node does not own a canonical level range"));
            }
            for neighbor in &record.neighbors {
                if usize::from(nodes[*neighbor as usize].maximum_level) < expected_level {
                    return Err(invalid(
                        "HNSW edge references a node absent from that level",
                    ));
                }
            }
            levels.push(record.neighbors);
        }
        graph_nodes.push(HnswGraphNode {
            source_row_ordinal: node.source_row_ordinal,
            vector_crc32: node.vector_crc32,
            levels,
        });
    }
    if records.next().is_some() {
        return Err(invalid("HNSW adjacency has unowned level records"));
    }
    HnswIndexGraph::new(
        metadata.parameters,
        metadata.build_seed,
        metadata.entry_node_ordinal,
        graph_nodes,
    )
}

fn page_indices(section: IndexSection) -> FormatResult<std::ops::Range<usize>> {
    let start = section.first_page_index() as usize;
    let end = start
        .checked_add(section.page_count() as usize)
        .ok_or_else(|| invalid("HNSW page range overflows"))?;
    if start == end {
        return Err(invalid("HNSW paged section is empty"));
    }
    Ok(start..end)
}

fn level_record_hash(node_ordinal: u32, level: u16) -> u64 {
    let mut bytes = [0_u8; 6];
    bytes[..4].copy_from_slice(&node_ordinal.to_le_bytes());
    bytes[4..].copy_from_slice(&level.to_le_bytes());
    diagnostic_key_hash(&bytes)
}

#[derive(Debug)]
struct DecodeBudget {
    used: u64,
    limit: u64,
}

impl DecodeBudget {
    const fn new(limits: HnswDecodeLimits) -> Self {
        Self {
            used: 0,
            limit: limits.max_accounted_bytes(),
        }
    }

    fn account(&mut self, bytes: u64) -> FormatResult<()> {
        self.used = self
            .used
            .checked_add(bytes)
            .ok_or_else(|| invalid("HNSW decode allocation accounting overflows"))?;
        if self.used > self.limit {
            return Err(limit("HNSW decoded graph bytes", self.used, self.limit));
        }
        Ok(())
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("bounded u16"))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("bounded u32"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("bounded u64"))
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
