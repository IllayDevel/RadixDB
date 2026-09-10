use radixdb_catalog::{
    AccessMethod, CatalogDataType, CatalogPayload, ColumnPayload, ConstraintPayload,
    HnswDistanceMetric, HnswParameters, IndexPayload, NamespacePayload, ObjectId, ObjectKind,
    TablePayload, ViewPayload, COLUMN_FLAG_AUTO_INCREMENT, PAYLOAD_VERSION,
};
use radixdb_core::DataType;

#[test]
fn public_contract_constructs_all_six_closed_payload_kinds() {
    let column_id = ObjectId::new();
    let constraint_id = ObjectId::new();
    let index_id = ObjectId::new();
    let table_id = ObjectId::new();

    let payloads = [
        CatalogPayload::Namespace(NamespacePayload::new()),
        CatalogPayload::Table(
            TablePayload::new(
                vec![column_id],
                vec![constraint_id],
                vec![index_id],
                Some(constraint_id),
            )
            .unwrap(),
        ),
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
        CatalogPayload::Constraint(ConstraintPayload::primary_key(vec![column_id]).unwrap()),
        CatalogPayload::Index(
            IndexPayload::new(
                AccessMethod::Btree,
                true,
                vec![column_id],
                vec![],
                None,
                None,
            )
            .unwrap(),
        ),
        CatalogPayload::View(
            ViewPayload::new("SELECT id FROM messages", vec![table_id], [42; 32]).unwrap(),
        ),
    ];

    assert_eq!(
        payloads.each_ref().map(|payload| payload.kind()),
        [
            ObjectKind::Namespace,
            ObjectKind::Table,
            ObjectKind::Column,
            ObjectKind::Constraint,
            ObjectKind::Index,
            ObjectKind::View,
        ]
    );
    assert!(payloads
        .iter()
        .all(|payload| payload.version() == PAYLOAD_VERSION));
}

#[test]
fn auto_increment_is_an_authoritative_column_flag() {
    let payload = ColumnPayload::new_with_auto_increment(
        0,
        CatalogDataType::scalar(DataType::Uuid).unwrap(),
        false,
        true,
        None,
        None,
    )
    .unwrap();

    assert!(payload.auto_increment());
    assert_eq!(payload.flags(), COLUMN_FLAG_AUTO_INCREMENT);
    assert_eq!(
        ColumnPayload::from_fields(
            PAYLOAD_VERSION,
            payload.flags(),
            payload.ordinal(),
            payload.data_type(),
            payload.nullable(),
            None,
            None,
        )
        .unwrap(),
        payload
    );
}

#[test]
fn hnsw_parameters_are_an_authoritative_index_definition() {
    let parameters = HnswParameters::new(24, 300, 80, HnswDistanceMetric::Dot).unwrap();
    let payload = IndexPayload::new_hnsw(ObjectId::new(), vec![], parameters).unwrap();

    assert_eq!(payload.hnsw_parameters(), Some(parameters));
    assert_eq!(parameters.m(), 24);
    assert_eq!(parameters.ef_construction(), 300);
    assert_eq!(parameters.ef_search(), 80);
    assert_eq!(parameters.distance_metric(), HnswDistanceMetric::Dot);
}

#[test]
fn view_contract_is_text_ids_signature_and_version_only() {
    let dependency = ObjectId::new();
    let view = ViewPayload::from_fields(
        PAYLOAD_VERSION,
        0,
        "SELECT id FROM messages",
        vec![dependency],
        [9; 32],
    )
    .unwrap();

    assert_eq!(view.canonical_sql().as_str(), "SELECT id FROM messages");
    assert_eq!(view.dependency_ids(), &[dependency]);
    assert_eq!(view.output_signature(), &[9; 32]);
    assert_eq!(view.version(), PAYLOAD_VERSION);
    assert!(ViewPayload::from_fields(2, 0, "SELECT 1", vec![], [0; 32]).is_err());
    assert!(ViewPayload::from_fields(1, 1, "SELECT 1", vec![], [0; 32]).is_err());
}
