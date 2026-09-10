use std::cell::RefCell;

use radixdb_catalog::{CatalogDataType, HnswDistanceMetric, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::{
    decode_data_artifact_layout, decode_hnsw_index, decode_hnsw_index_from_source,
    decode_index_artifact_layout, encode_data_artifact, encode_hnsw_index_sections,
    encode_index_artifact, ArtifactId, ArtifactRef, ArtifactSource, CatalogGeneration,
    DataArtifactHeader, DataArtifactInput, DataArtifactLayout, DataBlockSpec, DataColumnSpec,
    DataPhysicalCodec, DataValueEncoding, DatabaseGeneration, DatabaseId, FormatError,
    FormatResult, HnswBuildParameters, HnswDecodeLimits, HnswGraphNode, HnswIndexGraph,
    HnswPageBuildLimits, IndexAcceleratorKind, IndexAcceleratorSpec, IndexArtifactHeader,
    IndexArtifactInput, IndexKeyColumn, IndexNullsOrder, IndexPageCodec, IndexSectionKind,
    IndexSortDirection, SegmentId, SegmentKind,
};

struct RangeTracingSource<'a> {
    bytes: &'a [u8],
    reads: RefCell<Vec<(u64, u64)>>,
}

impl<'a> RangeTracingSource<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            reads: RefCell::new(Vec::new()),
        }
    }
}

impl ArtifactSource for RangeTracingSource<'_> {
    fn byte_length(&self) -> FormatResult<u64> {
        Ok(self.bytes.len() as u64)
    }

    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> FormatResult<()> {
        ArtifactSource::read_exact_at(self.bytes, offset, destination)?;
        self.reads
            .borrow_mut()
            .push((offset, destination.len() as u64));
        Ok(())
    }
}

struct Fixture {
    data: DataArtifactLayout,
    bytes: Vec<u8>,
    reference: ArtifactRef,
    logical_index_id: ObjectId,
    parameters: HnswBuildParameters,
    canonical_graph: HnswIndexGraph,
    vector_bytes: Vec<u8>,
}

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes(raw(marker)).unwrap()
}

fn parameters() -> HnswBuildParameters {
    HnswBuildParameters::new(16, 16, 200, HnswDistanceMetric::Cosine).unwrap()
}

fn data_fixture() -> (DataArtifactLayout, DataColumnSpec, Vec<u8>) {
    let column = DataColumnSpec::new(object_id(0x31), CatalogDataType::vector(16).unwrap(), false);
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(0x41)).unwrap(),
        DatabaseId::from_bytes(raw(0x42)).unwrap(),
        object_id(0x43),
        SegmentId::from_bytes(raw(0x44)).unwrap(),
        DatabaseGeneration::new(13).unwrap(),
        CatalogGeneration::new(11).unwrap(),
        101,
        105,
        5,
        1,
        1,
        SegmentKind::Rows,
        123_456,
    )
    .unwrap();
    let row_ids = (0..5_u64).collect::<Vec<_>>();
    let vectors = (0..5)
        .map(|row| {
            Value::vector(
                (0..16)
                    .map(|component| 1.0 + row as f32 * 0.125 + component as f32 / 128.0)
                    .collect(),
            )
        })
        .collect::<Vec<_>>();
    let vector_bytes = vectors[0]
        .as_vector_f32()
        .unwrap()
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect::<Vec<_>>();
    let blocks = vec![
        DataBlockSpec::row_ids(0, &row_ids, DataPhysicalCodec::None).unwrap(),
        DataBlockSpec::column(
            0,
            0,
            column,
            &vectors,
            DataValueEncoding::Plain,
            DataPhysicalCodec::None,
        )
        .unwrap(),
    ];
    let input = DataArtifactInput::new(header, vec![column], vec![], blocks).unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    (
        decode_data_artifact_layout(&bytes, reference).unwrap(),
        column,
        vector_bytes,
    )
}

fn source_graph() -> HnswIndexGraph {
    // Input node ordinals are intentionally not in source-row order.
    HnswIndexGraph::new(
        parameters(),
        0x0123_4567_89ab_cdef,
        0,
        vec![
            HnswGraphNode::new(4, 0x4444_4444, vec![vec![1, 2], vec![1]]).unwrap(),
            HnswGraphNode::new(1, 0x1111_1111, vec![vec![0, 2], vec![0]]).unwrap(),
            HnswGraphNode::new(3, 0x3333_3333, vec![vec![0, 1]]).unwrap(),
        ],
    )
    .unwrap()
}

fn canonical_graph() -> HnswIndexGraph {
    HnswIndexGraph::new(
        parameters(),
        0x0123_4567_89ab_cdef,
        2,
        vec![
            HnswGraphNode::new(1, 0x1111_1111, vec![vec![1, 2], vec![2]]).unwrap(),
            HnswGraphNode::new(3, 0x3333_3333, vec![vec![0, 2]]).unwrap(),
            HnswGraphNode::new(4, 0x4444_4444, vec![vec![0, 1], vec![0]]).unwrap(),
        ],
    )
    .unwrap()
}

fn fixture(codec: IndexPageCodec, limits: HnswPageBuildLimits) -> Fixture {
    let (data, column, vector_bytes) = data_fixture();
    let graph = source_graph();
    let sections =
        encode_hnsw_index_sections(&graph, data.header().row_count(), codec, limits).unwrap();
    let logical_index_id = object_id(0x51);
    let accelerator = IndexAcceleratorSpec::new(
        logical_index_id,
        IndexAcceleratorKind::Hnsw,
        false,
        false,
        [0x52; 32],
        vec![IndexKeyColumn::new(
            column.column_id(),
            DataType::Vector,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
        graph.nodes().len() as u64,
        sections,
    )
    .unwrap();
    let header = IndexArtifactHeader::for_data(
        ArtifactId::from_bytes(raw(0x53)).unwrap(),
        DatabaseGeneration::new(17).unwrap(),
        CatalogGeneration::new(15).unwrap(),
        &data,
    )
    .unwrap();
    let input = IndexArtifactInput::new(header, &data, vec![accelerator]).unwrap();
    let (bytes, reference) = encode_index_artifact(&input).unwrap();
    Fixture {
        data,
        bytes,
        reference,
        logical_index_id,
        parameters: parameters(),
        canonical_graph: canonical_graph(),
        vector_bytes,
    }
}

#[test]
fn hnsw_roundtrip_is_deterministic_paged_and_contains_no_vector_copy() {
    let limits = HnswPageBuildLimits::new(1, 256).unwrap();
    let fixture = fixture(IndexPageCodec::Lz4, limits);
    let first_bytes = fixture.bytes.clone();
    let first_reference = fixture.reference;
    let duplicate = self::fixture(IndexPageCodec::Lz4, limits);
    assert_eq!(first_bytes, duplicate.bytes);
    assert_eq!(first_reference, duplicate.reference);
    assert!(
        !fixture
            .bytes
            .windows(fixture.vector_bytes.len())
            .any(|window| window == fixture.vector_bytes),
        "the rebuildable graph must not duplicate authoritative VECTOR bytes"
    );

    let layout =
        decode_index_artifact_layout(&fixture.bytes, fixture.reference, &fixture.data).unwrap();
    assert_eq!(layout.accelerators().len(), 1);
    assert_eq!(layout.sections().len(), 4);
    assert_eq!(
        layout.sections()[1].reference().section_kind(),
        IndexSectionKind::HnswMetadata
    );
    assert_eq!(layout.sections()[2].page_count(), 3);
    assert_eq!(layout.sections()[3].page_count(), 5);
    let decoded = decode_hnsw_index(
        &fixture.bytes,
        &layout,
        &fixture.data,
        fixture.logical_index_id,
        fixture.parameters,
    )
    .unwrap();
    assert_eq!(decoded, fixture.canonical_graph);
}

#[test]
fn hnsw_reader_uses_bounded_page_reads_and_rejects_parameter_drift() {
    let fixture = fixture(
        IndexPageCodec::None,
        HnswPageBuildLimits::new(2, 512).unwrap(),
    );
    let layout =
        decode_index_artifact_layout(&fixture.bytes, fixture.reference, &fixture.data).unwrap();
    let source = RangeTracingSource::new(&fixture.bytes);
    let decoded = decode_hnsw_index_from_source(
        &source,
        &layout,
        &fixture.data,
        fixture.logical_index_id,
        fixture.parameters,
        HnswDecodeLimits::default(),
    )
    .unwrap();
    assert_eq!(decoded, fixture.canonical_graph);
    let expected_reads =
        1 + layout.sections()[2].page_count() as usize + layout.sections()[3].page_count() as usize;
    assert_eq!(source.reads.borrow().len(), expected_reads);

    let mismatched = HnswBuildParameters::new(16, 17, 200, HnswDistanceMetric::Cosine).unwrap();
    assert!(matches!(
        decode_hnsw_index(
            &fixture.bytes[..],
            &layout,
            &fixture.data,
            fixture.logical_index_id,
            mismatched
        ),
        Err(FormatError::InvalidIndexArtifact { .. })
    ));
    assert!(matches!(
        decode_hnsw_index_from_source(
            &fixture.bytes[..],
            &layout,
            &fixture.data,
            fixture.logical_index_id,
            fixture.parameters,
            HnswDecodeLimits::new(1).unwrap()
        ),
        Err(FormatError::IndexArtifactLimitExceeded { .. })
    ));
}

#[test]
fn hnsw_writer_rejects_noncanonical_or_unsafe_topology() {
    let limits = HnswPageBuildLimits::default();
    let invalid_graphs = [
        HnswIndexGraph::new(
            parameters(),
            1,
            0,
            vec![
                HnswGraphNode::new(0, 1, vec![vec![0]]).unwrap(),
                HnswGraphNode::new(1, 2, vec![vec![]]).unwrap(),
            ],
        )
        .unwrap(),
        HnswIndexGraph::new(
            parameters(),
            1,
            0,
            vec![
                HnswGraphNode::new(0, 1, vec![vec![1, 1]]).unwrap(),
                HnswGraphNode::new(1, 2, vec![vec![0]]).unwrap(),
            ],
        )
        .unwrap(),
        HnswIndexGraph::new(
            parameters(),
            1,
            0,
            vec![
                HnswGraphNode::new(0, 1, vec![vec![1], vec![1]]).unwrap(),
                HnswGraphNode::new(1, 2, vec![vec![0]]).unwrap(),
            ],
        )
        .unwrap(),
        HnswIndexGraph::new(
            parameters(),
            1,
            0,
            vec![
                HnswGraphNode::new(0, 1, vec![vec![1]]).unwrap(),
                HnswGraphNode::new(0, 2, vec![vec![0]]).unwrap(),
            ],
        )
        .unwrap(),
        HnswIndexGraph::new(
            parameters(),
            1,
            1,
            vec![
                HnswGraphNode::new(0, 1, vec![vec![1], vec![]]).unwrap(),
                HnswGraphNode::new(1, 2, vec![vec![0]]).unwrap(),
            ],
        )
        .unwrap(),
    ];
    for graph in invalid_graphs {
        assert!(encode_hnsw_index_sections(&graph, 2, IndexPageCodec::None, limits).is_err());
    }
    assert!(encode_hnsw_index_sections(&source_graph(), 4, IndexPageCodec::None, limits).is_err());
}

#[test]
fn hnsw_accelerator_contract_rejects_wrong_key_and_constraint_ownership() {
    let placeholder_sections = || {
        encode_hnsw_index_sections(
            &source_graph(),
            5,
            IndexPageCodec::None,
            HnswPageBuildLimits::default(),
        )
        .unwrap()
    };
    let vector_key = IndexKeyColumn::new(
        object_id(0x31),
        DataType::Vector,
        IndexSortDirection::Ascending,
        IndexNullsOrder::Last,
    );
    assert!(IndexAcceleratorSpec::new(
        object_id(0x61),
        IndexAcceleratorKind::Hnsw,
        true,
        true,
        [0; 32],
        vec![vector_key],
        3,
        placeholder_sections(),
    )
    .is_err());
    let scalar_key = IndexKeyColumn::new(
        object_id(0x31),
        DataType::Integer,
        IndexSortDirection::Ascending,
        IndexNullsOrder::Last,
    );
    assert!(IndexAcceleratorSpec::new(
        object_id(0x62),
        IndexAcceleratorKind::Hnsw,
        false,
        false,
        [0; 32],
        vec![scalar_key],
        3,
        placeholder_sections(),
    )
    .is_err());
}

#[test]
fn hnsw_page_corruption_disables_the_complete_accelerator() {
    let fixture = fixture(
        IndexPageCodec::None,
        HnswPageBuildLimits::new(2, 512).unwrap(),
    );
    let layout =
        decode_index_artifact_layout(&fixture.bytes, fixture.reference, &fixture.data).unwrap();
    let adjacency_page = layout.sections()[3].first_page_index() as usize;
    let page = layout.pages()[adjacency_page];
    let mut corrupted = fixture.bytes.clone();
    corrupted[page.offset() as usize] ^= 0x80;
    assert!(matches!(
        decode_hnsw_index(
            &corrupted,
            &layout,
            &fixture.data,
            fixture.logical_index_id,
            fixture.parameters
        ),
        Err(FormatError::IndexArtifactChecksumMismatch { .. })
    ));
}
