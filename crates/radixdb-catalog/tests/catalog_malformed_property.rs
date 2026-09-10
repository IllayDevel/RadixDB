use proptest::prelude::*;
use radixdb_catalog::{
    decode_catalog_pack, encode_catalog_pack, CatalogDataType, CatalogEdge, CatalogError,
    CatalogGraph, CatalogName, CatalogObject, CatalogPackMeta, CatalogPayload, ColumnPayload,
    EdgeKind, NamespacePayload, ObjectId, TablePayload, ViewPayload, MAX_DEPENDENCY_DEPTH,
    MAX_OBJECT_IDS_PER_FIELD,
};
use radixdb_core::DataType;

fn id(number: u32) -> ObjectId {
    let mut bytes = [0_u8; 16];
    bytes[0] = 1;
    bytes[12..].copy_from_slice(&number.to_be_bytes());
    ObjectId::from_user_bytes(bytes).unwrap()
}

fn object(
    id: ObjectId,
    namespace: Option<ObjectId>,
    parent: Option<ObjectId>,
    name: &str,
    payload: CatalogPayload,
) -> CatalogObject {
    CatalogObject::new(
        id,
        namespace,
        parent,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(name).unwrap(),
        1,
        payload,
    )
    .unwrap()
}

fn meta() -> CatalogPackMeta {
    CatalogPackMeta::new([3; 16], [4; 16], 11, 1001, 55).unwrap()
}

fn table_graph(table_count: u32) -> (Vec<CatalogObject>, Vec<CatalogEdge>) {
    let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
    let mut objects = vec![object(
        namespace,
        None,
        None,
        "public",
        CatalogPayload::Namespace(NamespacePayload::new()),
    )];
    let mut edges = Vec::new();
    for number in 0..table_count {
        let table = id(number * 2 + 100);
        let column = id(number * 2 + 101);
        objects.push(object(
            table,
            Some(namespace),
            Some(namespace),
            &format!("table_{number}"),
            CatalogPayload::Table(TablePayload::new(vec![column], vec![], vec![], None).unwrap()),
        ));
        objects.push(object(
            column,
            Some(namespace),
            Some(table),
            "id",
            CatalogPayload::Column(
                ColumnPayload::new(
                    0,
                    CatalogDataType::scalar(DataType::Integer).unwrap(),
                    false,
                    None,
                    None,
                )
                .unwrap(),
            ),
        ));
        edges.push(CatalogEdge::new(
            namespace,
            table,
            EdgeKind::Contains,
            number,
        ));
        edges.push(CatalogEdge::new(table, column, EdgeKind::Contains, 0));
    }
    (objects, edges)
}

fn shuffle<T>(values: &mut [T], mut state: u64) {
    for index in (1..values.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        values.swap(index, state as usize % (index + 1));
    }
}

fn refresh_integrity(bytes: &mut [u8]) {
    let header_crc = radixdb_core::crc32_ieee(&bytes[..248]);
    bytes[248..252].copy_from_slice(&header_crc.to_le_bytes());
    let footer = bytes.len() - 48;
    let body_sha = radixdb_core::sha256_digest(&bytes[..footer]);
    bytes[footer + 16..].copy_from_slice(&body_sha);
}

fn encoded_fixture() -> Vec<u8> {
    let (objects, edges) = table_graph(2);
    encode_catalog_pack(meta(), &CatalogGraph::build(objects, edges).unwrap()).unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn arbitrary_bounded_catalog_bytes_never_panic(
        bytes in prop::collection::vec(any::<u8>(), 0..4096),
    ) {
        if let Ok(decoded) = decode_catalog_pack(&bytes) {
            prop_assert_eq!(
                encode_catalog_pack(decoded.meta(), decoded.graph()).unwrap(),
                bytes,
                "accepted catalog bytes must already be canonical",
            );
        }
    }

    #[test]
    fn single_byte_catalog_corruption_is_fail_closed_or_canonical(
        offset in any::<usize>(),
        replacement in any::<u8>(),
    ) {
        let mut bytes = encoded_fixture();
        let index = offset % bytes.len();
        bytes[index] = replacement;
        if let Ok(decoded) = decode_catalog_pack(&bytes) {
            prop_assert_eq!(
                encode_catalog_pack(decoded.meta(), decoded.graph()).unwrap(),
                bytes,
                "accepted mutation must remain canonical",
            );
        }
    }
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

#[test]
fn repeated_construction_permutations_are_byte_identical() {
    let (objects, edges) = table_graph(24);
    let reference = encode_catalog_pack(
        meta(),
        &CatalogGraph::build(objects.clone(), edges.clone()).unwrap(),
    )
    .unwrap();

    for seed in 1..=64 {
        let mut shuffled_objects = objects.clone();
        let mut shuffled_edges = edges.clone();
        shuffle(&mut shuffled_objects, seed);
        shuffle(&mut shuffled_edges, seed ^ 0x9e37_79b9_7f4a_7c15);
        let graph = CatalogGraph::build(shuffled_objects, shuffled_edges).unwrap();
        let bytes = encode_catalog_pack(meta(), &graph).unwrap();
        assert_eq!(bytes, reference, "construction seed {seed}");
        let decoded = decode_catalog_pack(&bytes).unwrap();
        assert_eq!(
            encode_catalog_pack(decoded.meta(), decoded.graph()).unwrap(),
            reference,
            "roundtrip seed {seed}"
        );
    }
}

#[test]
fn malformed_header_and_directory_corpus_fails_closed() {
    let original = encoded_fixture();

    let mut reserved = original.clone();
    reserved[168] = 1;
    refresh_integrity(&mut reserved);
    assert!(matches!(
        decode_catalog_pack(&reserved),
        Err(CatalogError::InvalidCatalogFormat { .. })
    ));

    let mut too_many_objects = original.clone();
    too_many_objects[72..80].copy_from_slice(&262_145_u64.to_le_bytes());
    refresh_integrity(&mut too_many_objects);
    assert!(matches!(
        decode_catalog_pack(&too_many_objects),
        Err(CatalogError::CatalogLimitExceeded { .. })
    ));

    let mut too_many_edges = original.clone();
    too_many_edges[80..88].copy_from_slice(&1_048_577_u64.to_le_bytes());
    refresh_integrity(&mut too_many_edges);
    assert!(matches!(
        decode_catalog_pack(&too_many_edges),
        Err(CatalogError::CatalogLimitExceeded { .. })
    ));

    let object_directory = read_u64(&original, 88) as usize;
    let mut duplicate_id = original.clone();
    let first_id = duplicate_id[object_directory..object_directory + 16].to_vec();
    duplicate_id[object_directory + 160..object_directory + 176].copy_from_slice(&first_id);
    refresh_integrity(&mut duplicate_id);
    assert!(matches!(
        decode_catalog_pack(&duplicate_id),
        Err(CatalogError::NonCanonicalCatalogEncoding { .. })
    ));

    let edge_directory = read_u64(&original, 104) as usize;
    let mut reserved_edge = original.clone();
    reserved_edge[edge_directory + 32..edge_directory + 34].copy_from_slice(&5_u16.to_le_bytes());
    refresh_integrity(&mut reserved_edge);
    assert!(matches!(
        decode_catalog_pack(&reserved_edge),
        Err(CatalogError::ReservedEdgeKind { .. })
    ));
}

#[test]
fn malformed_blob_and_payload_corpus_fails_closed() {
    let original = encoded_fixture();
    let object_directory = read_u64(&original, 88) as usize;
    let payload_area = read_u64(&original, 120) as usize;
    let string_area = read_u64(&original, 136) as usize;
    let payload_relative = read_u64(&original, object_directory + 104) as usize;
    let payload_length = read_u32(&original, object_directory + 112) as usize;
    let payload = payload_area + payload_relative;

    let mut too_many_fields = original.clone();
    too_many_fields[payload + 16..payload + 20].copy_from_slice(&65_u32.to_le_bytes());
    let payload_crc = radixdb_core::crc32_ieee(&too_many_fields[payload..payload + payload_length]);
    too_many_fields[object_directory + 116..object_directory + 120]
        .copy_from_slice(&payload_crc.to_le_bytes());
    refresh_integrity(&mut too_many_fields);
    assert!(matches!(
        decode_catalog_pack(&too_many_fields),
        Err(CatalogError::CatalogLimitExceeded { .. })
    ));

    let mut unknown_field_type = original.clone();
    unknown_field_type[payload + 34..payload + 36].copy_from_slice(&99_u16.to_le_bytes());
    let body_crc =
        radixdb_core::crc32_ieee(&unknown_field_type[payload + 32..payload + payload_length]);
    unknown_field_type[payload + 28..payload + 32].copy_from_slice(&body_crc.to_le_bytes());
    let payload_crc =
        radixdb_core::crc32_ieee(&unknown_field_type[payload..payload + payload_length]);
    unknown_field_type[object_directory + 116..object_directory + 120]
        .copy_from_slice(&payload_crc.to_le_bytes());
    refresh_integrity(&mut unknown_field_type);
    assert!(matches!(
        decode_catalog_pack(&unknown_field_type),
        Err(CatalogError::InvalidCatalogFormat { .. })
    ));

    let mut invalid_utf8 = original.clone();
    let normalized_length = read_u32(&original, object_directory + 80) as usize;
    invalid_utf8[string_area] = 0xff;
    let name_crc =
        radixdb_core::crc32_ieee(&invalid_utf8[string_area..string_area + normalized_length]);
    invalid_utf8[object_directory + 84..object_directory + 88]
        .copy_from_slice(&name_crc.to_le_bytes());
    refresh_integrity(&mut invalid_utf8);
    assert!(matches!(
        decode_catalog_pack(&invalid_utf8),
        Err(CatalogError::InvalidCatalogUtf8 { .. })
    ));

    let mut shifted_blob = original.clone();
    let second_entry = object_directory + 160;
    let second_payload_offset = read_u64(&shifted_blob, second_entry + 104);
    shifted_blob[second_entry + 104..second_entry + 112]
        .copy_from_slice(&(second_payload_offset + 1).to_le_bytes());
    refresh_integrity(&mut shifted_blob);
    assert!(matches!(
        decode_catalog_pack(&shifted_blob),
        Err(CatalogError::NonCanonicalCatalogEncoding { .. })
    ));
}

#[test]
fn array_and_dependency_depth_limits_are_enforced() {
    let ids = (0..=MAX_OBJECT_IDS_PER_FIELD)
        .map(|number| id(number as u32 + 10_000))
        .collect();
    assert!(matches!(
        TablePayload::new(ids, vec![], vec![], None),
        Err(CatalogError::TooManyObjectIds { .. })
    ));

    let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
    let mut objects = vec![object(
        namespace,
        None,
        None,
        "public",
        CatalogPayload::Namespace(NamespacePayload::new()),
    )];
    let mut edges = Vec::new();
    let count = MAX_DEPENDENCY_DEPTH + 2;
    for number in 0..count {
        let view = id(number as u32 + 20_000);
        let dependency = (number + 1 < count).then(|| id(number as u32 + 20_001));
        objects.push(object(
            view,
            Some(namespace),
            Some(namespace),
            &format!("view_{number}"),
            CatalogPayload::View(
                ViewPayload::new(
                    format!("SELECT {number}"),
                    dependency.into_iter().collect(),
                    [number as u8; 32],
                )
                .unwrap(),
            ),
        ));
        edges.push(CatalogEdge::new(
            namespace,
            view,
            EdgeKind::Contains,
            number as u32,
        ));
        if let Some(target) = dependency {
            edges.push(CatalogEdge::new(view, target, EdgeKind::DependsOn, 0));
        }
    }
    assert!(matches!(
        CatalogGraph::build(objects, edges),
        Err(CatalogError::CatalogDependencyDepthExceeded { .. })
    ));
}
