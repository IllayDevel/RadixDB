use std::sync::Arc;

use radixdb_catalog::{
    decode_catalog_mutation_set, decode_catalog_pack, encode_catalog_mutation_set,
    encode_catalog_pack, AclEntryPayload, ArgumentMode, CatalogDataType, CatalogEdge, CatalogError,
    CatalogGeneration, CatalogGraph, CatalogMutation, CatalogMutationSet, CatalogName,
    CatalogObject, CatalogPackMeta, CatalogPayload, EdgeKind, FunctionPayload, NamespacePayload,
    ObjectId, ObjectKind, ObjectPrecondition, PrincipalPayload, ProceduralSource, ResourcePolicy,
    RolePayload, RoutineArgument, RoutineDefinition, RoutineResult, SecurityMode, Volatility,
    BASELINE_CATALOG_MINOR, PRIVILEGE_USAGE, PROCEDURAL_CATALOG_MINOR,
};
use radixdb_core::DataType;

fn object_id(marker: u8) -> ObjectId {
    let mut bytes = [marker; 16];
    bytes[0] = 1;
    ObjectId::from_user_bytes(bytes).unwrap()
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

fn function(id: ObjectId) -> CatalogObject {
    let scalar = CatalogDataType::scalar(DataType::Integer).unwrap();
    let definition = RoutineDefinition::new(
        ProceduralSource::new("BEGIN RETURN 1; END").unwrap(),
        vec![],
        RoutineResult::Scalar {
            data_type: scalar,
            nullable: false,
        },
        Volatility::Immutable,
        SecurityMode::Invoker,
        vec![ObjectId::BOOTSTRAP_NAMESPACE],
        vec![],
        1,
        1,
        1,
        ResourcePolicy::default_call(),
    )
    .unwrap();
    CatalogObject::new(
        id,
        Some(ObjectId::BOOTSTRAP_NAMESPACE),
        Some(ObjectId::BOOTSTRAP_NAMESPACE),
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new("answer").unwrap(),
        1,
        CatalogPayload::Function(FunctionPayload::new(definition).unwrap()),
    )
    .unwrap()
}

fn typed_function(id: ObjectId, argument_type: CatalogDataType) -> CatalogObject {
    let definition = RoutineDefinition::new(
        ProceduralSource::new("BEGIN RETURN value; END").unwrap(),
        vec![RoutineArgument::new(
            CatalogName::new("value").unwrap(),
            ArgumentMode::In,
            argument_type,
            false,
            None,
        )
        .unwrap()],
        RoutineResult::Scalar {
            data_type: argument_type,
            nullable: false,
        },
        Volatility::Immutable,
        SecurityMode::Invoker,
        vec![ObjectId::BOOTSTRAP_NAMESPACE],
        vec![],
        1,
        1,
        1,
        ResourcePolicy::default_call(),
    )
    .unwrap();
    CatalogObject::new(
        id,
        Some(ObjectId::BOOTSTRAP_NAMESPACE),
        Some(ObjectId::BOOTSTRAP_NAMESPACE),
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new("identity").unwrap(),
        1,
        CatalogPayload::Function(FunctionPayload::new(definition).unwrap()),
    )
    .unwrap()
}

fn role(id: ObjectId, name: &str) -> CatalogObject {
    CatalogObject::new(
        id,
        None,
        None,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(name).unwrap(),
        1,
        CatalogPayload::Role(RolePayload::new(true)),
    )
    .unwrap()
}

fn membership(id: ObjectId) -> CatalogObject {
    CatalogObject::new(
        id,
        None,
        None,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(format!("acl_{id}")).unwrap(),
        1,
        CatalogPayload::AclEntry(AclEntryPayload::role_membership(
            ObjectId::BOOTSTRAP_OWNER,
            false,
        )),
    )
    .unwrap()
}

fn principal(id: ObjectId, name: &str) -> CatalogObject {
    CatalogObject::new(
        id,
        None,
        None,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(name).unwrap(),
        1,
        CatalogPayload::Principal(PrincipalPayload::new(true, false)),
    )
    .unwrap()
}

#[test]
fn baseline_bytes_remain_exact_minor_zero() {
    let generation = baseline();
    let bytes = encode_catalog_pack(generation.meta(), generation.graph()).unwrap();
    assert_eq!(u16::from_le_bytes(bytes[8..10].try_into().unwrap()), 6);
    assert_eq!(
        u16::from_le_bytes(bytes[10..12].try_into().unwrap()),
        BASELINE_CATALOG_MINOR
    );
    let decoded = decode_catalog_pack(&bytes).unwrap();
    assert_eq!(decoded.format_minor(), BASELINE_CATALOG_MINOR);
    assert_eq!(
        encode_catalog_pack(decoded.meta(), decoded.graph()).unwrap(),
        bytes
    );
}

#[test]
fn job_tag_and_catalog_name_identity_are_durable_contracts() {
    assert_eq!(ObjectKind::Job.tag(), 39);
    assert_eq!(
        ObjectKind::from_tag_for_minor(39, PROCEDURAL_CATALOG_MINOR).unwrap(),
        ObjectKind::Job
    );
    assert!(ObjectKind::from_tag_for_minor(39, BASELINE_CATALOG_MINOR).is_err());

    let mixed_case = CatalogName::new("RouteDocs").unwrap();
    let unquoted_shape = CatalogName::new("routedocs").unwrap();
    assert_eq!(mixed_case.normalized(), unquoted_shape.normalized());
    assert_eq!(mixed_case.display().as_str(), "RouteDocs");
    let restored = CatalogName::from_stored("RouteDocs", "routedocs").unwrap();
    assert_eq!(restored.display().as_str(), "RouteDocs");
    assert_eq!(restored.normalized().as_str(), "routedocs");
}

#[test]
fn first_procedural_mutation_promotes_owner_and_roundtrips_as_minor_one() {
    let current = baseline();
    let function_id = object_id(41);
    let set = CatalogMutationSet::procedural_upgrade(
        &current,
        vec![CatalogMutation::create(function(function_id))],
        vec![],
        vec![
            CatalogEdge::new(
                ObjectId::BOOTSTRAP_NAMESPACE,
                function_id,
                EdgeKind::Contains,
                0,
            ),
            CatalogEdge::new(
                function_id,
                ObjectId::BOOTSTRAP_NAMESPACE,
                EdgeKind::References,
                0,
            ),
        ],
    )
    .unwrap();
    assert_eq!(set.format_minor(), PROCEDURAL_CATALOG_MINOR);

    let mutation_bytes = encode_catalog_mutation_set(&set).unwrap();
    assert_eq!(
        u16::from_le_bytes(mutation_bytes[10..12].try_into().unwrap()),
        PROCEDURAL_CATALOG_MINOR
    );
    let decoded_set = decode_catalog_mutation_set(&mutation_bytes).unwrap();
    assert_eq!(decoded_set, set);

    let prepared = decoded_set.prepare(&current, meta(2)).unwrap();
    let next = prepared.next();
    assert_eq!(next.format_minor(), PROCEDURAL_CATALOG_MINOR);
    assert_eq!(
        next.object(ObjectId::BOOTSTRAP_OWNER).unwrap().kind(),
        ObjectKind::Principal
    );
    assert_eq!(
        next.graph()
            .edges()
            .iter()
            .filter(|edge| edge.kind() == EdgeKind::OwnedBy)
            .count(),
        2
    );

    let pack_bytes = encode_catalog_pack(next.meta(), next.graph()).unwrap();
    assert_eq!(
        u16::from_le_bytes(pack_bytes[10..12].try_into().unwrap()),
        PROCEDURAL_CATALOG_MINOR
    );
    let baseline_binary_admits_header = u16::from_le_bytes(pack_bytes[8..10].try_into().unwrap())
        == 6
        && u16::from_le_bytes(pack_bytes[10..12].try_into().unwrap()) == BASELINE_CATALOG_MINOR;
    assert!(
        !baseline_binary_admits_header,
        "the immutable v1.0 exact-minor header gate must reject a 6.1 pack"
    );
    let pack = decode_catalog_pack(&pack_bytes).unwrap();
    assert_eq!(pack.format_minor(), PROCEDURAL_CATALOG_MINOR);
    assert_eq!(
        encode_catalog_pack(pack.meta(), pack.graph()).unwrap(),
        pack_bytes
    );

    let mut objects = next.graph().objects().cloned().collect::<Vec<_>>();
    let mut edges = next.graph().edges().to_vec();
    objects.reverse();
    edges.reverse();
    let reordered = CatalogGraph::build(objects, edges).unwrap();
    assert_eq!(
        encode_catalog_pack(next.meta(), &reordered).unwrap(),
        pack_bytes
    );
}

#[test]
fn procedural_generation_cannot_transition_back_to_a_baseline_writer() {
    let current = baseline();
    let function_id = object_id(51);
    let upgrade = CatalogMutationSet::procedural_upgrade(
        &current,
        vec![CatalogMutation::create(function(function_id))],
        vec![],
        vec![
            CatalogEdge::new(
                ObjectId::BOOTSTRAP_NAMESPACE,
                function_id,
                EdgeKind::Contains,
                0,
            ),
            CatalogEdge::new(
                function_id,
                ObjectId::BOOTSTRAP_NAMESPACE,
                EdgeKind::References,
                0,
            ),
        ],
    )
    .unwrap();
    let upgraded = upgrade.prepare(&current, meta(2)).unwrap().next().clone();
    assert_eq!(upgraded.format_minor(), PROCEDURAL_CATALOG_MINOR);

    let removals = upgraded
        .graph()
        .edges()
        .iter()
        .filter(|edge| {
            edge.source_object_id() == function_id || edge.target_object_id() == function_id
        })
        .cloned()
        .collect();
    let drop_last_user_procedural_object = CatalogMutationSet::for_generation(
        &upgraded,
        vec![CatalogMutation::drop(
            ObjectPrecondition::new(function_id, ObjectKind::Function, 1).unwrap(),
        )],
        removals,
        vec![],
    )
    .unwrap();
    assert_eq!(
        drop_last_user_procedural_object.format_minor(),
        PROCEDURAL_CATALOG_MINOR
    );
    let successor = drop_last_user_procedural_object
        .prepare(&upgraded, meta(3))
        .unwrap();
    assert_eq!(successor.next().format_minor(), PROCEDURAL_CATALOG_MINOR);
    assert_eq!(
        successor
            .next()
            .object(ObjectId::BOOTSTRAP_OWNER)
            .unwrap()
            .kind(),
        ObjectKind::Principal
    );
}

#[test]
fn minor_zero_and_reserved_materialized_view_fail_closed() {
    assert!(ObjectKind::from_tag_for_minor(ObjectKind::Function.tag(), 0).is_err());
    assert!(ObjectKind::from_tag_for_minor(35, 1).is_err());
    assert!(ObjectKind::from_tag_for_minor(40, 1).is_err());
    assert!(EdgeKind::from_tag_for_minor(EdgeKind::GrantedTo.tag(), 0).is_err());
}

#[test]
fn header_minor_cannot_claim_a_newer_registry_for_a_baseline_graph() {
    let generation = baseline();
    let mut bytes = encode_catalog_pack(generation.meta(), generation.graph()).unwrap();
    bytes[10..12].copy_from_slice(&PROCEDURAL_CATALOG_MINOR.to_le_bytes());
    let header_crc = radixdb_core::crc32_ieee(&bytes[..248]);
    bytes[248..252].copy_from_slice(&header_crc.to_le_bytes());
    let footer = bytes.len() - 48;
    let body_sha = radixdb_core::sha256_digest(&bytes[..footer]);
    bytes[footer + 16..footer + 48].copy_from_slice(&body_sha);

    assert!(matches!(
        decode_catalog_pack(&bytes),
        Err(CatalogError::InvalidCatalogFormat {
            detail: "catalog format minor disagrees with admitted object/edge registry"
        })
    ));
}

#[test]
fn routine_lookup_uses_ordered_input_type_signature() {
    let current = baseline();
    let integer = CatalogDataType::scalar(DataType::Integer).unwrap();
    let text = CatalogDataType::scalar(DataType::Text).unwrap();
    let integer_id = object_id(61);
    let text_id = object_id(62);
    let set = CatalogMutationSet::procedural_upgrade(
        &current,
        vec![
            CatalogMutation::create(typed_function(integer_id, integer)),
            CatalogMutation::create(typed_function(text_id, text)),
        ],
        vec![],
        vec![
            CatalogEdge::new(
                ObjectId::BOOTSTRAP_NAMESPACE,
                integer_id,
                EdgeKind::Contains,
                0,
            ),
            CatalogEdge::new(
                integer_id,
                ObjectId::BOOTSTRAP_NAMESPACE,
                EdgeKind::References,
                0,
            ),
            CatalogEdge::new(
                ObjectId::BOOTSTRAP_NAMESPACE,
                text_id,
                EdgeKind::Contains,
                0,
            ),
            CatalogEdge::new(
                text_id,
                ObjectId::BOOTSTRAP_NAMESPACE,
                EdgeKind::References,
                0,
            ),
        ],
    )
    .unwrap();
    let next = set.prepare(&current, meta(2)).unwrap();
    assert_eq!(
        next.next()
            .find_routine(
                ObjectId::BOOTSTRAP_NAMESPACE,
                ObjectKind::Function,
                "IDENTITY",
                &[integer],
            )
            .unwrap()
            .unwrap()
            .id(),
        integer_id
    );
    assert_eq!(
        next.next()
            .find_routine(
                ObjectId::BOOTSTRAP_NAMESPACE,
                ObjectKind::Function,
                "identity",
                &[text],
            )
            .unwrap()
            .unwrap()
            .id(),
        text_id
    );
}

#[test]
fn role_membership_cycle_rejects_the_complete_generation() {
    let current = baseline();
    let role_a = object_id(71);
    let role_b = object_id(72);
    let grant_a_to_b = object_id(73);
    let grant_b_to_a = object_id(74);
    let set = CatalogMutationSet::procedural_upgrade(
        &current,
        vec![
            CatalogMutation::create(role(role_a, "role_a")),
            CatalogMutation::create(role(role_b, "role_b")),
            CatalogMutation::create(membership(grant_a_to_b)),
            CatalogMutation::create(membership(grant_b_to_a)),
        ],
        vec![],
        vec![
            CatalogEdge::new(grant_a_to_b, role_a, EdgeKind::GrantedTo, 0),
            CatalogEdge::new(grant_a_to_b, role_b, EdgeKind::GrantsOn, 0),
            CatalogEdge::new(grant_b_to_a, role_b, EdgeKind::GrantedTo, 0),
            CatalogEdge::new(grant_b_to_a, role_a, EdgeKind::GrantsOn, 0),
        ],
    )
    .unwrap();

    assert!(matches!(
        set.prepare(&current, meta(2)),
        Err(CatalogError::CatalogDependencyCycle { .. })
    ));
}

#[test]
fn owner_change_grant_and_revoke_are_complete_atomic_generations() {
    let baseline = baseline();
    let alice = object_id(81);
    let promote = CatalogMutationSet::for_generation(
        &baseline,
        vec![CatalogMutation::create(principal(alice, "alice"))],
        vec![],
        vec![],
    )
    .unwrap();
    let promoted = promote.prepare(&baseline, meta(2)).unwrap().next().clone();

    let namespace = promoted.object(ObjectId::BOOTSTRAP_NAMESPACE).unwrap();
    let replacement = CatalogObject::new(
        namespace.id(),
        namespace.namespace_id(),
        namespace.parent_id(),
        alice,
        namespace.name().clone(),
        namespace.definition_revision() + 1,
        namespace.payload().clone(),
    )
    .unwrap();
    let acl_id = object_id(82);
    let acl = CatalogObject::new(
        acl_id,
        None,
        None,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(format!("acl_{acl_id}")).unwrap(),
        1,
        CatalogPayload::AclEntry(
            AclEntryPayload::object_privileges(
                ObjectId::BOOTSTRAP_OWNER,
                PRIVILEGE_USAGE,
                0,
                vec![],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let transfer_and_grant = CatalogMutationSet::for_generation(
        &promoted,
        vec![
            CatalogMutation::alter(
                ObjectPrecondition::new(
                    namespace.id(),
                    namespace.kind(),
                    namespace.definition_revision(),
                )
                .unwrap(),
                replacement,
            ),
            CatalogMutation::create(acl),
        ],
        vec![CatalogEdge::new(
            namespace.id(),
            ObjectId::BOOTSTRAP_OWNER,
            EdgeKind::OwnedBy,
            0,
        )],
        vec![
            CatalogEdge::new(namespace.id(), alice, EdgeKind::OwnedBy, 0),
            CatalogEdge::new(acl_id, ObjectId::BOOTSTRAP_OWNER, EdgeKind::OwnedBy, 0),
            CatalogEdge::new(acl_id, alice, EdgeKind::GrantedTo, 0),
            CatalogEdge::new(acl_id, ObjectId::BOOTSTRAP_NAMESPACE, EdgeKind::GrantsOn, 0),
        ],
    )
    .unwrap();
    let granted = transfer_and_grant
        .prepare(&promoted, meta(3))
        .unwrap()
        .next()
        .clone();
    assert_eq!(
        granted
            .object(ObjectId::BOOTSTRAP_NAMESPACE)
            .unwrap()
            .owner_principal_id(),
        alice
    );
    assert!(granted.object(acl_id).is_some());

    let revoke = CatalogMutationSet::for_generation(
        &granted,
        vec![CatalogMutation::drop(
            ObjectPrecondition::new(acl_id, ObjectKind::AclEntry, 1).unwrap(),
        )],
        vec![],
        vec![],
    )
    .unwrap();
    let revoked = revoke.prepare(&granted, meta(4)).unwrap();
    assert!(revoked.next().object(acl_id).is_none());
    assert!(revoked
        .next()
        .graph()
        .edges()
        .iter()
        .all(|edge| edge.source_object_id() != acl_id && edge.target_object_id() != acl_id));
}
