use std::sync::Arc;

use radixdb_catalog::{
    CatalogDataType, CatalogEdge, CatalogError, CatalogGeneration, CatalogGraph, CatalogMutation,
    CatalogMutationSet, CatalogName, CatalogObject, CatalogPackMeta, CatalogPayload,
    CatalogPublisher, ColumnPayload, EdgeKind, NamespacePayload, ObjectId, ObjectKind,
    ObjectPrecondition, TablePayload, ViewPayload,
};
use radixdb_core::DataType;

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes([marker; 16]).unwrap()
}

fn object(
    id: ObjectId,
    namespace: Option<ObjectId>,
    parent: Option<ObjectId>,
    name: &str,
    revision: u64,
    payload: CatalogPayload,
) -> CatalogObject {
    CatalogObject::new(
        id,
        namespace,
        parent,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(name).unwrap(),
        revision,
        payload,
    )
    .unwrap()
}

struct Fixture {
    generation: Arc<CatalogGeneration>,
    namespace: ObjectId,
    table: ObjectId,
    column: ObjectId,
}

fn fixture() -> Fixture {
    let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
    let table = object_id(10);
    let column = object_id(11);
    let graph = CatalogGraph::build(
        vec![
            object(
                namespace,
                None,
                None,
                "public",
                1,
                CatalogPayload::Namespace(NamespacePayload::new()),
            ),
            object(
                table,
                Some(namespace),
                Some(namespace),
                "messages",
                1,
                CatalogPayload::Table(
                    TablePayload::new(vec![column], vec![], vec![], None).unwrap(),
                ),
            ),
            object(
                column,
                Some(namespace),
                Some(table),
                "id",
                1,
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
        ],
        vec![
            CatalogEdge::new(namespace, table, EdgeKind::Contains, 0),
            CatalogEdge::new(table, column, EdgeKind::Contains, 0),
        ],
    )
    .unwrap();
    Fixture {
        generation: Arc::new(CatalogGeneration::new(
            CatalogPackMeta::new([1; 16], [2; 16], 3, 500, 123_000).unwrap(),
            graph,
        )),
        namespace,
        table,
        column,
    }
}

fn precondition(id: ObjectId, kind: ObjectKind, revision: u64) -> ObjectPrecondition {
    ObjectPrecondition::new(id, kind, revision).unwrap()
}

fn mutation_set(
    mutations: Vec<CatalogMutation>,
    edge_removals: Vec<CatalogEdge>,
    edge_additions: Vec<CatalogEdge>,
) -> CatalogMutationSet {
    CatalogMutationSet::new(
        [1; 16],
        [2; 16],
        3,
        mutations,
        edge_removals,
        edge_additions,
    )
    .unwrap()
}

#[test]
fn create_batch_resolves_new_objects_and_existing_dependencies_atomically() {
    let fixture = fixture();
    let view = object_id(20);
    let view_object = object(
        view,
        Some(fixture.namespace),
        Some(fixture.namespace),
        "message_ids",
        1,
        CatalogPayload::View(
            ViewPayload::new("SELECT id FROM messages", vec![fixture.table], [9; 32]).unwrap(),
        ),
    );
    let result = mutation_set(
        vec![CatalogMutation::create(view_object)],
        vec![],
        vec![
            CatalogEdge::new(fixture.namespace, view, EdgeKind::Contains, 1),
            CatalogEdge::new(view, fixture.table, EdgeKind::References, 0),
        ],
    )
    .apply(&fixture.generation)
    .unwrap();

    assert_eq!(result.object(view).unwrap().kind(), ObjectKind::View);
    assert!(fixture.generation.object(view).is_none());
    assert_eq!(
        result.objects().len(),
        fixture.generation.graph().objects().len() + 1
    );
}

#[test]
fn rename_preserves_identity_and_advances_revision() {
    let fixture = fixture();
    let result = mutation_set(
        vec![CatalogMutation::rename(
            precondition(fixture.table, ObjectKind::Table, 1),
            CatalogName::new("events").unwrap(),
        )],
        vec![],
        vec![],
    )
    .apply(&fixture.generation)
    .unwrap();

    let renamed = result.object(fixture.table).unwrap();
    assert_eq!(renamed.id(), fixture.table);
    assert_eq!(renamed.name().display().as_str(), "events");
    assert_eq!(renamed.definition_revision(), 2);
    assert_eq!(
        fixture
            .generation
            .object(fixture.table)
            .unwrap()
            .name()
            .display()
            .as_str(),
        "messages"
    );
}

#[test]
fn alter_and_create_are_validated_as_one_final_graph() {
    let fixture = fixture();
    let second_column = object_id(12);
    let replacement = object(
        fixture.table,
        Some(fixture.namespace),
        Some(fixture.namespace),
        "messages",
        2,
        CatalogPayload::Table(
            TablePayload::new(vec![fixture.column, second_column], vec![], vec![], None).unwrap(),
        ),
    );
    let new_column = object(
        second_column,
        Some(fixture.namespace),
        Some(fixture.table),
        "created_at",
        1,
        CatalogPayload::Column(
            ColumnPayload::new(
                1,
                CatalogDataType::scalar(DataType::Timestamp).unwrap(),
                false,
                None,
                None,
            )
            .unwrap(),
        ),
    );
    let result = mutation_set(
        vec![
            CatalogMutation::create(new_column),
            CatalogMutation::alter(
                precondition(fixture.table, ObjectKind::Table, 1),
                replacement,
            ),
        ],
        vec![],
        vec![CatalogEdge::new(
            fixture.table,
            second_column,
            EdgeKind::Contains,
            1,
        )],
    )
    .apply(&fixture.generation)
    .unwrap();

    assert_eq!(result.children(fixture.table).count(), 2);
    assert_eq!(
        result.object(fixture.table).unwrap().definition_revision(),
        2
    );
}

#[test]
fn alter_coalesces_payload_and_name_changes_without_moving_the_object() {
    let fixture = fixture();
    let second_column = object_id(12);
    let renamed_replacement = object(
        fixture.table,
        Some(fixture.namespace),
        Some(fixture.namespace),
        "events",
        2,
        CatalogPayload::Table(
            TablePayload::new(vec![fixture.column, second_column], vec![], vec![], None).unwrap(),
        ),
    );
    let new_column = object(
        second_column,
        Some(fixture.namespace),
        Some(fixture.table),
        "created_at",
        1,
        CatalogPayload::Column(
            ColumnPayload::new(
                1,
                CatalogDataType::scalar(DataType::Timestamp).unwrap(),
                false,
                None,
                None,
            )
            .unwrap(),
        ),
    );
    let result = mutation_set(
        vec![
            CatalogMutation::create(new_column),
            CatalogMutation::alter(
                precondition(fixture.table, ObjectKind::Table, 1),
                renamed_replacement,
            ),
        ],
        vec![],
        vec![CatalogEdge::new(
            fixture.table,
            second_column,
            EdgeKind::Contains,
            1,
        )],
    )
    .apply(&fixture.generation)
    .unwrap();

    let table = result.object(fixture.table).unwrap();
    assert_eq!(table.name().display().as_str(), "events");
    assert_eq!(table.definition_revision(), 2);
    assert_eq!(result.children(fixture.table).count(), 2);

    let moved = object(
        fixture.table,
        Some(fixture.namespace),
        Some(fixture.table),
        "messages",
        2,
        table.payload().clone(),
    );
    assert!(matches!(
        mutation_set(
            vec![CatalogMutation::alter(
                precondition(fixture.table, ObjectKind::Table, 1),
                moved,
            )],
            vec![],
            vec![],
        )
        .apply(&fixture.generation),
        Err(CatalogError::CatalogObjectPreconditionFailed { .. })
    ));
}

#[test]
fn drop_and_recreate_uses_a_new_identity() {
    let fixture = fixture();
    let replacement_table = object_id(30);
    let replacement_column = object_id(31);
    let result = mutation_set(
        vec![
            CatalogMutation::drop(precondition(fixture.table, ObjectKind::Table, 1)),
            CatalogMutation::drop(precondition(fixture.column, ObjectKind::Column, 1)),
            CatalogMutation::create(object(
                replacement_table,
                Some(fixture.namespace),
                Some(fixture.namespace),
                "messages",
                1,
                CatalogPayload::Table(
                    TablePayload::new(vec![replacement_column], vec![], vec![], None).unwrap(),
                ),
            )),
            CatalogMutation::create(object(
                replacement_column,
                Some(fixture.namespace),
                Some(replacement_table),
                "id",
                1,
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
            )),
        ],
        vec![],
        vec![
            CatalogEdge::new(fixture.namespace, replacement_table, EdgeKind::Contains, 0),
            CatalogEdge::new(replacement_table, replacement_column, EdgeKind::Contains, 0),
        ],
    )
    .apply(&fixture.generation)
    .unwrap();

    assert!(result.object(fixture.table).is_none());
    assert!(result.object(replacement_table).is_some());
    assert_ne!(fixture.table, replacement_table);
}

#[test]
fn stale_generation_and_object_revision_fail_before_any_publication() {
    let fixture = fixture();
    let stale_generation = CatalogMutationSet::new(
        [1; 16],
        [2; 16],
        2,
        vec![CatalogMutation::rename(
            precondition(fixture.table, ObjectKind::Table, 1),
            CatalogName::new("events").unwrap(),
        )],
        vec![],
        vec![],
    )
    .unwrap();
    assert!(matches!(
        stale_generation.apply(&fixture.generation),
        Err(CatalogError::StaleCatalogMutation {
            field: "catalog generation",
            ..
        })
    ));

    let stale_object = mutation_set(
        vec![CatalogMutation::rename(
            precondition(fixture.table, ObjectKind::Table, 2),
            CatalogName::new("events").unwrap(),
        )],
        vec![],
        vec![],
    );
    assert!(matches!(
        stale_object.apply(&fixture.generation),
        Err(CatalogError::CatalogObjectPreconditionFailed { .. })
    ));
    assert_eq!(
        fixture
            .generation
            .object(fixture.table)
            .unwrap()
            .definition_revision(),
        1
    );
}

#[test]
fn prepared_publication_uses_exact_generation_compare_and_swap() {
    let fixture = fixture();
    let publisher = CatalogPublisher::new(Arc::clone(&fixture.generation));
    let mutation = mutation_set(
        vec![CatalogMutation::rename(
            precondition(fixture.table, ObjectKind::Table, 1),
            CatalogName::new("events").unwrap(),
        )],
        vec![],
        vec![],
    );
    let prepared = mutation
        .prepare(
            &fixture.generation,
            CatalogPackMeta::new([1; 16], [4; 16], 4, 600, 124_000).unwrap(),
        )
        .unwrap();
    assert_eq!(
        prepared
            .next()
            .object(fixture.table)
            .unwrap()
            .name()
            .display()
            .as_str(),
        "events"
    );

    let concurrent = mutation_set(
        vec![CatalogMutation::rename(
            precondition(fixture.table, ObjectKind::Table, 1),
            CatalogName::new("archive").unwrap(),
        )],
        vec![],
        vec![],
    )
    .prepare(
        &fixture.generation,
        CatalogPackMeta::new([1; 16], [5; 16], 4, 600, 124_001).unwrap(),
    )
    .unwrap();
    let concurrent_generation = Arc::clone(concurrent.next());
    publisher.publish_prepared(concurrent).unwrap();
    assert!(matches!(
        publisher.publish_prepared(prepared),
        Err(CatalogError::StaleCatalogMutation { .. })
    ));
    assert!(Arc::ptr_eq(
        &publisher.pin().unwrap(),
        &concurrent_generation
    ));
}

#[test]
fn prepared_publication_swaps_only_the_validated_successor() {
    let fixture = fixture();
    let publisher = CatalogPublisher::new(Arc::clone(&fixture.generation));
    let prepared = mutation_set(
        vec![CatalogMutation::rename(
            precondition(fixture.table, ObjectKind::Table, 1),
            CatalogName::new("events").unwrap(),
        )],
        vec![],
        vec![],
    )
    .prepare(
        &fixture.generation,
        CatalogPackMeta::new([1; 16], [4; 16], 4, 600, 124_000).unwrap(),
    )
    .unwrap();
    let expected_next = Arc::clone(prepared.next());
    let previous = publisher.publish_prepared(prepared).unwrap();

    assert!(Arc::ptr_eq(&previous, &fixture.generation));
    assert!(Arc::ptr_eq(&publisher.pin().unwrap(), &expected_next));
}

#[test]
fn invalid_batch_is_atomic_and_duplicate_targets_are_rejected() {
    let fixture = fixture();
    let absent = object_id(99);
    let invalid = mutation_set(
        vec![
            CatalogMutation::rename(
                precondition(fixture.table, ObjectKind::Table, 1),
                CatalogName::new("events").unwrap(),
            ),
            CatalogMutation::drop(precondition(absent, ObjectKind::View, 1)),
        ],
        vec![],
        vec![],
    );
    assert!(invalid.apply(&fixture.generation).is_err());
    assert_eq!(
        fixture
            .generation
            .object(fixture.table)
            .unwrap()
            .name()
            .display()
            .as_str(),
        "messages"
    );

    assert!(matches!(
        CatalogMutationSet::new(
            [1; 16],
            [2; 16],
            3,
            vec![
                CatalogMutation::drop(precondition(fixture.table, ObjectKind::Table, 1)),
                CatalogMutation::rename(
                    precondition(fixture.table, ObjectKind::Table, 1),
                    CatalogName::new("events").unwrap(),
                ),
            ],
            vec![],
            vec![],
        ),
        Err(CatalogError::InvalidCatalogMutation { .. })
    ));
}

#[test]
fn final_graph_enforces_drop_restrictions_and_name_uniqueness() {
    let fixture = fixture();
    let drop_table_only = mutation_set(
        vec![CatalogMutation::drop(precondition(
            fixture.table,
            ObjectKind::Table,
            1,
        ))],
        vec![],
        vec![],
    );
    assert!(drop_table_only.apply(&fixture.generation).is_err());

    let colliding_view = object_id(40);
    let name_collision = mutation_set(
        vec![CatalogMutation::create(object(
            colliding_view,
            Some(fixture.namespace),
            Some(fixture.namespace),
            "MESSAGES",
            1,
            CatalogPayload::View(ViewPayload::new("SELECT 1", vec![], [0; 32]).unwrap()),
        ))],
        vec![],
        vec![CatalogEdge::new(
            fixture.namespace,
            colliding_view,
            EdgeKind::Contains,
            1,
        )],
    );
    assert!(matches!(
        name_collision.apply(&fixture.generation),
        Err(CatalogError::DuplicateCatalogName { .. })
    ));
}

#[test]
fn mutation_set_encoding_order_is_canonical() {
    let fixture = fixture();
    let first = object_id(50);
    let second = object_id(51);
    let first_mutation = CatalogMutation::drop(precondition(first, ObjectKind::View, 1));
    let second_mutation = CatalogMutation::drop(precondition(second, ObjectKind::View, 1));
    let edge_a = CatalogEdge::new(first, fixture.table, EdgeKind::References, 0);
    let edge_b = CatalogEdge::new(second, fixture.table, EdgeKind::References, 0);
    let left = CatalogMutationSet::new(
        [1; 16],
        [2; 16],
        3,
        vec![second_mutation.clone(), first_mutation.clone()],
        vec![],
        vec![edge_b, edge_a],
    )
    .unwrap();
    let right = CatalogMutationSet::new(
        [1; 16],
        [2; 16],
        3,
        vec![first_mutation, second_mutation],
        vec![],
        vec![edge_a, edge_b],
    )
    .unwrap();
    assert_eq!(left, right);
}
