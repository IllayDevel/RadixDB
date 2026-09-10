use radixdb_catalog::{
    encode_catalog_pack, CatalogGeneration, CatalogGraph, CatalogName, CatalogObject,
    CatalogPackMeta, CatalogPayload, NamespacePayload, ObjectId, ObjectKind, ViewPayload,
};
use radixdb_storage::v6::{
    encode_catalog_wal_transaction, CatalogWalReplayLimits, CatalogWalTransaction,
    CatalogWalTransactionId,
};

use super::{CatalogCheckpointHarness, CatalogHarnessError, DdlTransaction, TableCatalog};

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes([marker; 16]).unwrap()
}

fn empty_catalog() -> CatalogGeneration {
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
    CatalogGeneration::new(
        CatalogPackMeta::new([1; 16], [2; 16], 1, 1, 1).unwrap(),
        CatalogGraph::build(vec![namespace], vec![]).unwrap(),
    )
}

fn staged_transaction(
    source: &CatalogGeneration,
    sql: &str,
    object_ids: impl IntoIterator<Item = ObjectId>,
    transaction_marker: u8,
    successor_marker: u8,
    commit_lsn: u64,
) -> CatalogWalTransaction {
    let mut ddl = DdlTransaction::begin_with_object_ids(source, object_ids);
    ddl.stage_sql(sql).unwrap();
    CatalogWalTransaction::new(
        CatalogWalTransactionId::from_bytes([transaction_marker; 16]).unwrap(),
        [successor_marker; 16],
        commit_lsn,
        commit_lsn * 1_000,
        ddl.commit().unwrap().unwrap(),
    )
    .unwrap()
}

#[test]
fn full_pack_plus_committed_wal_reopens_one_complete_generation() {
    let initial = empty_catalog();
    let transaction = staged_transaction(
        &initial,
        "CREATE TABLE messages (id INTEGER PRIMARY KEY, body TEXT)",
        [
            object_id(10),
            object_id(11),
            object_id(12),
            object_id(13),
            object_id(14),
        ],
        20,
        3,
        10,
    );
    let mut harness = CatalogCheckpointHarness::from_generation(&initial).unwrap();

    let recovery = harness
        .append_committed(&transaction, CatalogWalReplayLimits::hard())
        .unwrap();

    assert_eq!(recovery.committed_transactions(), 1);
    assert_eq!(recovery.incomplete_tail_bytes(), 0);
    assert_eq!(recovery.generation().meta().catalog_generation(), 2);
    assert!(TableCatalog::load(recovery.generation(), "messages").is_ok());
    assert!(initial
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, "messages")
        .unwrap()
        .is_none());
}

#[test]
fn checkpoint_rebases_wal_and_accepts_a_subsequent_transaction() {
    let initial = empty_catalog();
    let first = staged_transaction(
        &initial,
        "CREATE TABLE messages (id INTEGER, body TEXT)",
        [object_id(10), object_id(11), object_id(12)],
        20,
        3,
        10,
    );
    let mut harness = CatalogCheckpointHarness::from_generation(&initial).unwrap();
    let first_recovery = harness
        .append_committed(&first, CatalogWalReplayLimits::hard())
        .unwrap();
    let first_checkpoint = harness.checkpoint(CatalogWalReplayLimits::hard()).unwrap();
    assert_eq!(first_checkpoint.folded_transactions, 1);
    assert!(harness.catalog_wal_bytes().is_empty());

    let second = staged_transaction(
        first_recovery.generation(),
        "ALTER TABLE messages RENAME TO events",
        [],
        21,
        4,
        20,
    );
    let second_recovery = harness
        .append_committed(&second, CatalogWalReplayLimits::hard())
        .unwrap();
    assert_eq!(second_recovery.generation().meta().catalog_generation(), 3);
    assert!(TableCatalog::load(second_recovery.generation(), "events").is_ok());

    harness.checkpoint(CatalogWalReplayLimits::hard()).unwrap();
    let stable_pack = harness.catalog_pack_bytes().to_vec();
    let no_op = harness.checkpoint(CatalogWalReplayLimits::hard()).unwrap();
    assert_eq!(no_op.folded_transactions, 0);
    assert_eq!(harness.catalog_pack_bytes(), stable_pack);
}

#[test]
fn torn_tail_is_invisible_and_must_be_discarded_before_append() {
    let initial = empty_catalog();
    let base = CatalogCheckpointHarness::from_generation(&initial).unwrap();
    let transaction = staged_transaction(
        &initial,
        "CREATE TABLE messages (id INTEGER)",
        [object_id(10), object_id(11)],
        20,
        3,
        10,
    );
    let encoded = encode_catalog_wal_transaction(&transaction).unwrap();
    let torn = encoded[..encoded.len() - 1].to_vec();
    let mut harness = CatalogCheckpointHarness::from_persisted(
        base.catalog_pack_bytes().to_vec(),
        torn.clone(),
        CatalogWalReplayLimits::hard(),
    )
    .unwrap();

    let recovery = harness.reopen(CatalogWalReplayLimits::hard()).unwrap();
    assert_eq!(recovery.committed_transactions(), 0);
    assert_eq!(recovery.incomplete_tail_bytes(), torn.len());
    assert!(TableCatalog::load(recovery.generation(), "messages").is_err());
    assert!(matches!(
        harness.append_committed(&transaction, CatalogWalReplayLimits::hard()),
        Err(CatalogHarnessError::IncompleteWalTail { bytes }) if bytes == torn.len()
    ));

    assert_eq!(
        harness
            .discard_incomplete_tail(CatalogWalReplayLimits::hard())
            .unwrap(),
        torn.len()
    );
    assert!(harness.catalog_wal_bytes().is_empty());
    harness
        .append_committed(&transaction, CatalogWalReplayLimits::hard())
        .unwrap();
}

#[test]
fn corrupt_or_stale_durable_bytes_fail_closed() {
    let initial = empty_catalog();
    let base = CatalogCheckpointHarness::from_generation(&initial).unwrap();
    let transaction = staged_transaction(
        &initial,
        "CREATE TABLE messages (id INTEGER)",
        [object_id(10), object_id(11)],
        20,
        3,
        10,
    );

    let mut corrupt_pack = base.catalog_pack_bytes().to_vec();
    corrupt_pack[0] ^= 1;
    assert!(matches!(
        CatalogCheckpointHarness::from_persisted(
            corrupt_pack,
            Vec::new(),
            CatalogWalReplayLimits::hard()
        ),
        Err(CatalogHarnessError::Catalog(_))
    ));

    let mut corrupt_wal = encode_catalog_wal_transaction(&transaction).unwrap();
    corrupt_wal[160] ^= 1;
    assert!(matches!(
        CatalogCheckpointHarness::from_persisted(
            base.catalog_pack_bytes().to_vec(),
            corrupt_wal,
            CatalogWalReplayLimits::hard()
        ),
        Err(CatalogHarnessError::Storage(_))
    ));

    let mut harness = CatalogCheckpointHarness::from_generation(&initial).unwrap();
    harness
        .append_committed(&transaction, CatalogWalReplayLimits::hard())
        .unwrap();
    let committed = harness.catalog_wal_bytes().to_vec();
    assert!(matches!(
        harness.append_committed(&transaction, CatalogWalReplayLimits::hard()),
        Err(CatalogHarnessError::Storage(_))
    ));
    assert_eq!(harness.catalog_wal_bytes(), committed);
}

#[test]
fn reopen_rebinds_persisted_views_before_admission() {
    let initial = empty_catalog();
    let transaction = staged_transaction(
        &initial,
        "CREATE TABLE messages (id INTEGER)",
        [object_id(10), object_id(11)],
        20,
        3,
        10,
    );
    let mut harness = CatalogCheckpointHarness::from_generation(&initial).unwrap();
    let table_recovery = harness
        .append_committed(&transaction, CatalogWalReplayLimits::hard())
        .unwrap();
    let view_transaction = staged_transaction(
        table_recovery.generation(),
        "CREATE VIEW message_ids AS SELECT id FROM messages",
        [object_id(12)],
        21,
        4,
        20,
    );
    let view_recovery = harness
        .append_committed(&view_transaction, CatalogWalReplayLimits::hard())
        .unwrap();
    let generation = view_recovery.generation();
    let view = generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, "message_ids")
        .unwrap()
        .unwrap();
    let CatalogPayload::View(payload) = view.payload() else {
        panic!("view payload expected")
    };
    let invalid_payload = CatalogPayload::View(
        ViewPayload::new(
            payload.canonical_sql().as_str(),
            payload.dependency_ids().to_vec(),
            [0; 32],
        )
        .unwrap(),
    );
    let objects = generation
        .graph()
        .objects()
        .map(|object| {
            if object.kind() == ObjectKind::View {
                CatalogObject::new(
                    object.id(),
                    object.namespace_id(),
                    object.parent_id(),
                    object.owner_principal_id(),
                    object.name().clone(),
                    object.definition_revision(),
                    invalid_payload.clone(),
                )
                .unwrap()
            } else {
                object.clone()
            }
        })
        .collect();
    let graph = CatalogGraph::build(objects, generation.graph().edges().to_vec()).unwrap();
    let invalid_pack = encode_catalog_pack(generation.meta(), &graph).unwrap();

    assert!(matches!(
        CatalogCheckpointHarness::from_persisted(
            invalid_pack,
            Vec::new(),
            CatalogWalReplayLimits::hard()
        ),
        Err(CatalogHarnessError::Semantic(_))
    ));
}
