use radixdb_catalog::{
    decode_catalog_pack, encode_catalog_pack, AccessMethod, CatalogDataType, CatalogEdge,
    CatalogGraph, CatalogName, CatalogObject, CatalogPackMeta, CatalogPayload, ColumnPayload,
    ConstraintPayload, EdgeKind, IndexPayload, NamespacePayload, ObjectId, TablePayload,
    ViewPayload,
};
use radixdb_core::DataType;

struct Fixture {
    objects: Vec<CatalogObject>,
    edges: Vec<CatalogEdge>,
}

fn stable_id(byte: u8) -> ObjectId {
    let mut bytes = [byte; 16];
    bytes[0] = 1;
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

fn fixture() -> Fixture {
    let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
    let table = stable_id(10);
    let column = stable_id(11);
    let constraint = stable_id(12);
    let index = stable_id(13);
    let view = stable_id(14);
    Fixture {
        objects: vec![
            object(
                namespace,
                None,
                None,
                "public",
                CatalogPayload::Namespace(NamespacePayload::new()),
            ),
            object(
                table,
                Some(namespace),
                Some(namespace),
                "messages",
                CatalogPayload::Table(
                    TablePayload::new(
                        vec![column],
                        vec![constraint],
                        vec![index],
                        Some(constraint),
                    )
                    .unwrap(),
                ),
            ),
            object(
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
            ),
            object(
                constraint,
                Some(namespace),
                Some(table),
                "messages_pkey",
                CatalogPayload::Constraint(ConstraintPayload::primary_key(vec![column]).unwrap()),
            ),
            object(
                index,
                Some(namespace),
                Some(table),
                "messages_pkey_idx",
                CatalogPayload::Index(
                    IndexPayload::new(AccessMethod::Btree, true, vec![column], vec![], None, None)
                        .unwrap(),
                ),
            ),
            object(
                view,
                Some(namespace),
                Some(namespace),
                "message_ids",
                CatalogPayload::View(
                    ViewPayload::new("SELECT id FROM messages", vec![table], [7; 32]).unwrap(),
                ),
            ),
        ],
        edges: vec![
            CatalogEdge::new(namespace, table, EdgeKind::Contains, 0),
            CatalogEdge::new(namespace, view, EdgeKind::Contains, 1),
            CatalogEdge::new(table, column, EdgeKind::Contains, 0),
            CatalogEdge::new(table, constraint, EdgeKind::Contains, 1),
            CatalogEdge::new(table, index, EdgeKind::Contains, 2),
            CatalogEdge::new(index, constraint, EdgeKind::DependsOn, 0),
            CatalogEdge::new(view, table, EdgeKind::References, 0),
        ],
    }
}

fn meta() -> CatalogPackMeta {
    CatalogPackMeta::new([1; 16], [2; 16], 7, 91, 1_777_000_000).unwrap()
}

fn refresh_integrity(bytes: &mut [u8]) {
    let header_crc = radixdb_core::crc32_ieee(&bytes[..248]);
    bytes[248..252].copy_from_slice(&header_crc.to_le_bytes());
    let footer = bytes.len() - 48;
    let body_sha = radixdb_core::sha256_digest(&bytes[..footer]);
    bytes[footer + 16..].copy_from_slice(&body_sha);
}

#[test]
fn shuffled_logical_state_is_byte_identical_and_roundtrips() {
    let first = fixture();
    let first_graph = CatalogGraph::build(first.objects, first.edges).unwrap();
    let first_bytes = encode_catalog_pack(meta(), &first_graph).unwrap();

    let mut shuffled = fixture();
    shuffled.objects.reverse();
    shuffled.edges.reverse();
    let shuffled_graph = CatalogGraph::build(shuffled.objects, shuffled.edges).unwrap();
    let shuffled_bytes = encode_catalog_pack(meta(), &shuffled_graph).unwrap();

    assert_eq!(first_bytes, shuffled_bytes);
    assert_eq!(&first_bytes[..8], b"RDX6CAT\0");
    assert_eq!(
        &first_bytes[first_bytes.len() - 48..first_bytes.len() - 40],
        b"RDX6END\0"
    );

    let decoded = decode_catalog_pack(&first_bytes).unwrap();
    assert_eq!(decoded.meta(), meta());
    assert_eq!(decoded.graph().objects().len(), 6);
    assert_eq!(decoded.graph().edges().len(), 7);
    assert_eq!(
        encode_catalog_pack(decoded.meta(), decoded.graph()).unwrap(),
        first_bytes
    );
}

#[test]
fn bootstrap_only_catalog_uses_canonical_empty_edge_reference() {
    let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
    let graph = CatalogGraph::build(
        vec![object(
            namespace,
            None,
            None,
            "public",
            CatalogPayload::Namespace(NamespacePayload::new()),
        )],
        vec![],
    )
    .unwrap();
    let bytes = encode_catalog_pack(meta(), &graph).unwrap();
    assert_eq!(u64::from_le_bytes(bytes[104..112].try_into().unwrap()), 0);
    assert_eq!(u64::from_le_bytes(bytes[112..120].try_into().unwrap()), 0);
    let decoded = decode_catalog_pack(&bytes).unwrap();
    assert_eq!(decoded.graph().objects().len(), 1);
    assert!(decoded.graph().edges().is_empty());
}

#[test]
fn whole_file_corruption_is_rejected_before_publication() {
    let fixture = fixture();
    let graph = CatalogGraph::build(fixture.objects, fixture.edges).unwrap();
    let mut bytes = encode_catalog_pack(meta(), &graph).unwrap();
    let payload_offset = u64::from_le_bytes(bytes[120..128].try_into().unwrap()) as usize;
    bytes[payload_offset] ^= 0x40;
    assert!(decode_catalog_pack(&bytes).is_err());

    let mut bytes = encode_catalog_pack(meta(), &graph).unwrap();
    let footer = bytes.len() - 48;
    bytes[footer] ^= 1;
    assert!(decode_catalog_pack(&bytes).is_err());
}

#[test]
fn count_and_layout_are_rejected_after_valid_outer_checksums() {
    let fixture = fixture();
    let graph = CatalogGraph::build(fixture.objects, fixture.edges).unwrap();

    let mut excessive_count = encode_catalog_pack(meta(), &graph).unwrap();
    excessive_count[72..80].copy_from_slice(&262_145_u64.to_le_bytes());
    refresh_integrity(&mut excessive_count);
    assert!(matches!(
        decode_catalog_pack(&excessive_count),
        Err(radixdb_catalog::CatalogError::CatalogLimitExceeded {
            field: "catalog object count",
            ..
        })
    ));

    let mut unaligned = encode_catalog_pack(meta(), &graph).unwrap();
    let payload_offset = u64::from_le_bytes(unaligned[120..128].try_into().unwrap());
    unaligned[120..128].copy_from_slice(&(payload_offset + 1).to_le_bytes());
    refresh_integrity(&mut unaligned);
    assert!(matches!(
        decode_catalog_pack(&unaligned),
        Err(radixdb_catalog::CatalogError::NonCanonicalCatalogEncoding { .. })
    ));
}
