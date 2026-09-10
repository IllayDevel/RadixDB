use radixdb_catalog::{
    CatalogGeneration, CatalogGraph, CatalogName, CatalogObject, CatalogPackMeta, CatalogPayload,
    NamespacePayload, ObjectId,
};
use radixdb_storage::v6::{
    CatalogGeneration as DurableCatalogGeneration, CatalogId, CatalogRef, CatalogWalReplayLimits,
    CatalogWalTransaction, CatalogWalTransactionId,
};

use super::{
    CatalogCheckpointHarness, CatalogHarnessError, CatalogSnapshot, DdlTransaction, TableCatalog,
};

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

fn create_table_transaction(source: &CatalogGeneration) -> CatalogWalTransaction {
    let mut ddl = DdlTransaction::begin_with_object_ids(
        source,
        [object_id(10), object_id(11), object_id(12)],
    );
    ddl.stage_sql("CREATE TABLE messages (id INTEGER, body TEXT)")
        .unwrap();
    CatalogWalTransaction::new(
        CatalogWalTransactionId::from_bytes([20; 16]).unwrap(),
        [3; 16],
        10,
        10_000,
        ddl.commit().unwrap().unwrap(),
    )
    .unwrap()
}

#[test]
fn snapshot_captures_committed_wal_as_one_full_catalog_member() {
    let initial = empty_catalog();
    let mut source = CatalogCheckpointHarness::from_generation(&initial).unwrap();
    source
        .append_committed(
            &create_table_transaction(&initial),
            CatalogWalReplayLimits::hard(),
        )
        .unwrap();

    let snapshot = CatalogSnapshot::capture(&source, CatalogWalReplayLimits::hard()).unwrap();
    let reference = snapshot.catalog_ref();
    assert_eq!(snapshot.member_count(), 1);
    assert_eq!(reference.id().into_bytes(), [3; 16]);
    assert_eq!(reference.generation().get(), 2);
    assert_eq!(
        reference.byte_length(),
        snapshot.catalog_bytes().len() as u64
    );

    let restored = snapshot.reopen(CatalogWalReplayLimits::hard()).unwrap();
    assert_eq!(restored.committed_transactions(), 0);
    assert!(TableCatalog::load(restored.generation(), "messages").is_ok());
}

#[test]
fn captured_snapshot_remains_pinned_when_live_catalog_advances() {
    let initial = empty_catalog();
    let mut source = CatalogCheckpointHarness::from_generation(&initial).unwrap();
    let created = source
        .append_committed(
            &create_table_transaction(&initial),
            CatalogWalReplayLimits::hard(),
        )
        .unwrap();
    let snapshot = CatalogSnapshot::capture(&source, CatalogWalReplayLimits::hard()).unwrap();

    let mut rename = DdlTransaction::begin(created.generation());
    rename
        .stage_sql("ALTER TABLE messages RENAME TO events")
        .unwrap();
    let transaction = CatalogWalTransaction::new(
        CatalogWalTransactionId::from_bytes([21; 16]).unwrap(),
        [4; 16],
        20,
        20_000,
        rename.commit().unwrap().unwrap(),
    )
    .unwrap();
    source
        .append_committed(&transaction, CatalogWalReplayLimits::hard())
        .unwrap();

    let live = source.reopen(CatalogWalReplayLimits::hard()).unwrap();
    assert!(TableCatalog::load(live.generation(), "events").is_ok());
    let restored = snapshot.reopen(CatalogWalReplayLimits::hard()).unwrap();
    assert!(TableCatalog::load(restored.generation(), "messages").is_ok());
    assert!(TableCatalog::load(restored.generation(), "events").is_err());
}

#[test]
fn snapshot_reference_mismatch_and_corruption_fail_closed() {
    let source = CatalogCheckpointHarness::from_generation(&empty_catalog()).unwrap();
    let snapshot = CatalogSnapshot::capture(&source, CatalogWalReplayLimits::hard()).unwrap();
    let mut corrupt = snapshot.catalog_bytes().to_vec();
    corrupt[0] ^= 1;
    assert!(matches!(
        CatalogSnapshot::from_persisted_member(
            snapshot.catalog_ref(),
            corrupt,
            CatalogWalReplayLimits::hard()
        ),
        Err(CatalogHarnessError::Catalog(_))
    ));

    let wrong_identity = CatalogRef::new(
        CatalogId::from_bytes([9; 16]).unwrap(),
        snapshot.catalog_ref().generation(),
        snapshot.catalog_ref().byte_length(),
        *snapshot.catalog_ref().body_sha256(),
    )
    .unwrap();
    assert!(matches!(
        CatalogSnapshot::from_persisted_member(
            wrong_identity,
            snapshot.catalog_bytes().to_vec(),
            CatalogWalReplayLimits::hard()
        ),
        Err(CatalogHarnessError::SnapshotReferenceMismatch {
            field: "catalog identity"
        })
    ));

    let wrong_generation = CatalogRef::new(
        snapshot.catalog_ref().id(),
        DurableCatalogGeneration::new(snapshot.catalog_ref().generation().get() + 1).unwrap(),
        snapshot.catalog_ref().byte_length(),
        *snapshot.catalog_ref().body_sha256(),
    )
    .unwrap();
    assert!(matches!(
        CatalogSnapshot::from_persisted_member(
            wrong_generation,
            snapshot.catalog_bytes().to_vec(),
            CatalogWalReplayLimits::hard()
        ),
        Err(CatalogHarnessError::SnapshotReferenceMismatch {
            field: "catalog generation"
        })
    ));

    let wrong_digest = CatalogRef::new(
        snapshot.catalog_ref().id(),
        snapshot.catalog_ref().generation(),
        snapshot.catalog_ref().byte_length(),
        [9; 32],
    )
    .unwrap();
    assert!(matches!(
        CatalogSnapshot::from_persisted_member(
            wrong_digest,
            snapshot.catalog_bytes().to_vec(),
            CatalogWalReplayLimits::hard()
        ),
        Err(CatalogHarnessError::SnapshotReferenceMismatch {
            field: "catalog body digest"
        })
    ));
}
