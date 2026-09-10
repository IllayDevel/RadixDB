use std::sync::Arc;

use radixdb_catalog::{
    decode_catalog_mutation_set, decode_catalog_pack, decode_catalog_pack_for_max_minor,
    encode_catalog_mutation_set, encode_catalog_pack, CatalogEdge, CatalogGeneration, CatalogGraph,
    CatalogMutation, CatalogMutationSet, CatalogName, CatalogObject, CatalogPackMeta,
    CatalogPayload, EdgeKind, ExtensionPayload, NamespacePayload, ObjectId, ObjectKind,
    RolePayload, BASELINE_CATALOG_MINOR, EXTENSION_CATALOG_MINOR, PROCEDURAL_CATALOG_MINOR,
};

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes([marker; 16]).unwrap()
}

fn meta(generation: u64) -> CatalogPackMeta {
    CatalogPackMeta::new([1; 16], [2; 16], generation, generation * 10, generation).unwrap()
}

fn baseline() -> Arc<CatalogGeneration> {
    let namespace = CatalogObject::new(
        ObjectId::BOOTSTRAP_NAMESPACE,
        None,
        None,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new("public").unwrap(),
        1,
        CatalogPayload::Namespace(NamespacePayload::new()),
    )
    .unwrap();
    Arc::new(CatalogGeneration::new(
        meta(1),
        CatalogGraph::build(vec![namespace], vec![]).unwrap(),
    ))
}

fn extension(package_id: ObjectId, payload_package_id: ObjectId) -> CatalogObject {
    CatalogObject::new(
        package_id,
        None,
        None,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new("radix_spatial").unwrap(),
        1,
        CatalogPayload::Extension(
            ExtensionPayload::new(payload_package_id, "1.2.3", 1, 0, 0, [9; 32]).unwrap(),
        ),
    )
    .unwrap()
}

#[test]
fn extension_tag_is_exactly_minor_two() {
    assert_eq!(ObjectKind::Extension.tag(), 40);
    assert!(ObjectKind::from_tag_for_minor(40, PROCEDURAL_CATALOG_MINOR).is_err());
    assert_eq!(
        ObjectKind::from_tag_for_minor(40, EXTENSION_CATALOG_MINOR).unwrap(),
        ObjectKind::Extension
    );
}

#[test]
fn first_extension_binding_promotes_baseline_atomically_and_roundtrips() {
    let current = baseline();
    let package_id = object_id(40);
    let set = CatalogMutationSet::for_generation(
        current.as_ref(),
        vec![CatalogMutation::create(extension(package_id, package_id))],
        vec![],
        vec![],
    )
    .unwrap();
    assert_eq!(set.format_minor(), EXTENSION_CATALOG_MINOR);

    let mutation_bytes = encode_catalog_mutation_set(&set).unwrap();
    assert_eq!(
        u16::from_le_bytes(mutation_bytes[10..12].try_into().unwrap()),
        EXTENSION_CATALOG_MINOR
    );
    let decoded_set = decode_catalog_mutation_set(&mutation_bytes).unwrap();
    assert_eq!(decoded_set, set);

    let next = decoded_set.prepare(current.as_ref(), meta(2)).unwrap();
    assert_eq!(next.next().format_minor(), EXTENSION_CATALOG_MINOR);
    let binding = next.next().object(package_id).unwrap();
    let CatalogPayload::Extension(payload) = binding.payload() else {
        panic!("extension kind must retain extension payload");
    };
    assert_eq!(payload.package_id(), package_id);
    assert_eq!(payload.version(), "1.2.3");
    assert_eq!(
        next.next()
            .graph()
            .incoming_edges(package_id)
            .filter(|edge| edge.kind() == radixdb_catalog::EdgeKind::Contains)
            .count(),
        0
    );

    let pack_bytes = encode_catalog_pack(next.next().meta(), next.next().graph()).unwrap();
    assert_eq!(
        u16::from_le_bytes(pack_bytes[10..12].try_into().unwrap()),
        EXTENSION_CATALOG_MINOR
    );
    let pack = decode_catalog_pack(&pack_bytes).unwrap();
    assert_eq!(pack.format_minor(), EXTENSION_CATALOG_MINOR);
    assert_eq!(
        encode_catalog_pack(pack.meta(), pack.graph()).unwrap(),
        pack_bytes
    );

    let legacy_error =
        decode_catalog_pack_for_max_minor(&pack_bytes, PROCEDURAL_CATALOG_MINOR).unwrap_err();
    assert!(legacy_error
        .to_string()
        .contains("catalog format version is unsupported"));
}

#[test]
fn extension_binding_identity_must_equal_package_uuid() {
    let current = baseline();
    let binding_id = object_id(41);
    let other_package_id = object_id(42);
    let error = CatalogMutationSet::for_generation(
        current.as_ref(),
        vec![CatalogMutation::create(extension(
            binding_id,
            other_package_id,
        ))],
        vec![],
        vec![],
    )
    .unwrap()
    .apply(current.as_ref())
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("package UUID differs from binding object ID"));
}

#[test]
fn ordinary_minor_one_graph_stays_minor_one_until_extension_ddl() {
    let current = baseline();
    let role_id = object_id(43);
    let role = CatalogObject::new(
        role_id,
        None,
        None,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new("operators").unwrap(),
        1,
        CatalogPayload::Role(RolePayload::new(true)),
    )
    .unwrap();
    let procedural = CatalogMutationSet::for_generation(
        current.as_ref(),
        vec![CatalogMutation::create(role)],
        vec![],
        vec![],
    )
    .unwrap()
    .prepare(current.as_ref(), meta(2))
    .unwrap()
    .next()
    .clone();
    assert_eq!(procedural.format_minor(), PROCEDURAL_CATALOG_MINOR);
    let before = encode_catalog_pack(procedural.meta(), procedural.graph()).unwrap();
    let reopened = decode_catalog_pack(&before).unwrap();
    let after = encode_catalog_pack(reopened.meta(), reopened.graph()).unwrap();
    assert_eq!(before, after);
    assert_eq!(reopened.format_minor(), PROCEDURAL_CATALOG_MINOR);

    // The bootstrap principal required by 6.1 remains a normal principal; an
    // ordinary open must not synthesize an Extension or bump the generation.
    assert!(matches!(
        reopened
            .graph()
            .object(ObjectId::BOOTSTRAP_OWNER)
            .unwrap()
            .payload(),
        CatalogPayload::Principal(_)
    ));
    assert_eq!(reopened.meta().catalog_generation(), 2);
    assert_eq!(reopened.graph().objects().len(), 3);
    assert_eq!(BASELINE_CATALOG_MINOR, 0);

    let package_id = object_id(44);
    let extension_upgrade = CatalogMutationSet::for_generation(
        &procedural,
        vec![CatalogMutation::create(extension(package_id, package_id))],
        vec![],
        vec![CatalogEdge::new(
            package_id,
            ObjectId::BOOTSTRAP_OWNER,
            EdgeKind::OwnedBy,
            0,
        )],
    )
    .unwrap();
    assert_eq!(extension_upgrade.format_minor(), EXTENSION_CATALOG_MINOR);
    assert_eq!(
        extension_upgrade
            .prepare(&procedural, meta(3))
            .unwrap()
            .next()
            .format_minor(),
        EXTENSION_CATALOG_MINOR
    );
}
