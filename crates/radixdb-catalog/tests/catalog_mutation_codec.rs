use radixdb_catalog::{
    decode_catalog_mutation_set, encode_catalog_mutation_set, CatalogEdge, CatalogError,
    CatalogMutation, CatalogMutationSet, CatalogName, CatalogObject, CatalogPayload, EdgeKind,
    NamespacePayload, ObjectId, ObjectKind, ObjectPrecondition, MAX_CATALOG_MUTATIONS_PER_SET,
};

fn id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes([marker; 16]).unwrap()
}

fn namespace(marker: u8, revision: u64, name: &str) -> CatalogObject {
    CatalogObject::new(
        id(marker),
        None,
        None,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(name).unwrap(),
        revision,
        CatalogPayload::Namespace(NamespacePayload::new()),
    )
    .unwrap()
}

fn expected(marker: u8, revision: u64) -> ObjectPrecondition {
    ObjectPrecondition::new(id(marker), ObjectKind::Namespace, revision).unwrap()
}

fn set(mutations: Vec<CatalogMutation>) -> CatalogMutationSet {
    CatalogMutationSet::new(
        [1; 16],
        [2; 16],
        7,
        mutations,
        vec![CatalogEdge::new(id(10), id(11), EdgeKind::Contains, 0)],
        vec![CatalogEdge::new(id(10), id(12), EdgeKind::Contains, 1)],
    )
    .unwrap()
}

#[test]
fn all_operations_and_edge_deltas_roundtrip_exactly() {
    let original = set(vec![
        CatalogMutation::create(namespace(20, 1, "created")),
        CatalogMutation::alter(expected(21, 3), namespace(21, 4, "altered")),
        CatalogMutation::drop(expected(22, 9)),
        CatalogMutation::rename(expected(23, 5), CatalogName::new("renamed").unwrap()),
    ]);

    let bytes = encode_catalog_mutation_set(&original).unwrap();
    let decoded = decode_catalog_mutation_set(&bytes).unwrap();
    assert_eq!(decoded, original);
    assert_eq!(encode_catalog_mutation_set(&decoded).unwrap(), bytes);
}

#[test]
fn caller_order_cannot_change_canonical_bytes() {
    let ordered = set(vec![
        CatalogMutation::drop(expected(30, 1)),
        CatalogMutation::rename(expected(31, 2), CatalogName::new("z").unwrap()),
    ]);
    let shuffled = CatalogMutationSet::new(
        [1; 16],
        [2; 16],
        7,
        vec![
            CatalogMutation::rename(expected(31, 2), CatalogName::new("z").unwrap()),
            CatalogMutation::drop(expected(30, 1)),
        ],
        vec![CatalogEdge::new(id(10), id(11), EdgeKind::Contains, 0)],
        vec![CatalogEdge::new(id(10), id(12), EdgeKind::Contains, 1)],
    )
    .unwrap();

    assert_eq!(
        encode_catalog_mutation_set(&ordered).unwrap(),
        encode_catalog_mutation_set(&shuffled).unwrap()
    );
}

#[test]
fn checksum_reserved_and_noncanonical_bytes_fail_closed() {
    let bytes =
        encode_catalog_mutation_set(&set(vec![CatalogMutation::drop(expected(40, 1))])).unwrap();

    let mut damaged = bytes.clone();
    *damaged.last_mut().unwrap() ^= 1;
    assert!(matches!(
        decode_catalog_mutation_set(&damaged),
        Err(CatalogError::CatalogChecksumMismatch { .. })
    ));

    let mut reserved = bytes.clone();
    reserved[124] = 1;
    assert!(matches!(
        decode_catalog_mutation_set(&reserved),
        Err(CatalogError::InvalidCatalogFormat { .. })
    ));

    let mut bad_offset = bytes;
    bad_offset[88..96].copy_from_slice(&136_u64.to_le_bytes());
    assert!(matches!(
        decode_catalog_mutation_set(&bad_offset),
        Err(CatalogError::NonCanonicalCatalogEncoding { .. })
    ));
}

#[test]
fn hard_counts_are_rejected_before_directory_allocation() {
    let mut bytes =
        encode_catalog_mutation_set(&set(vec![CatalogMutation::drop(expected(50, 1))])).unwrap();
    bytes[64..72].copy_from_slice(&((MAX_CATALOG_MUTATIONS_PER_SET as u64) + 1).to_le_bytes());
    assert!(matches!(
        decode_catalog_mutation_set(&bytes),
        Err(CatalogError::CatalogLimitExceeded { .. })
    ));
}
