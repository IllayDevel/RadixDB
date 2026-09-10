#[cfg(feature = "test-failpoints")]
use radixdb_catalog::CatalogPublisher;
use radixdb_catalog::{
    encode_catalog_pack, ArgumentMode, CatalogDataType, CatalogEdge, CatalogGeneration,
    CatalogGraph, CatalogMutation, CatalogMutationSet, CatalogName, CatalogObject, CatalogPackMeta,
    CatalogPayload, EdgeKind, ExtensionPayload, FunctionPayload, NamespacePayload, ObjectId,
    ObjectKind, ObjectPrecondition, ProceduralSource, ResourcePolicy, RoutineArgument,
    RoutineDefinition, RoutineResult, SecurityMode, Volatility, EXTENSION_CATALOG_MINOR,
    PROCEDURAL_CATALOG_MINOR,
};
use radixdb_core::DataType;
#[cfg(feature = "test-failpoints")]
use radixdb_storage::v6::{
    append_catalog_wal_transaction, publish_catalog_mutation, GenerationCrashPoint,
    GenerationFaultGuard, GenerationFaultMode, SemanticExpectation,
};
use radixdb_storage::v6::{
    decode_catalog_wal, encode_catalog_wal_transaction, replay_catalog_wal,
    replay_catalog_wal_after, CatalogWalReplayLimits, CatalogWalTransaction,
    CatalogWalTransactionId, FormatError, CATALOG_WAL_RECORD_HEADER_BYTES,
};

fn initial() -> CatalogGeneration {
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
        CatalogPackMeta::new([1; 16], [2; 16], 3, 500, 123_000).unwrap(),
        CatalogGraph::build(vec![namespace], vec![]).unwrap(),
    )
}

fn procedural_function(id: ObjectId) -> CatalogObject {
    let integer = CatalogDataType::scalar(DataType::Integer).unwrap();
    let definition = RoutineDefinition::new(
        ProceduralSource::new("BEGIN RETURN value + 1; END").unwrap(),
        vec![RoutineArgument::new(
            CatalogName::new("value").unwrap(),
            ArgumentMode::In,
            integer,
            false,
            None,
        )
        .unwrap()],
        RoutineResult::Scalar {
            data_type: integer,
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
        CatalogName::new("increment").unwrap(),
        1,
        CatalogPayload::Function(FunctionPayload::new(definition).unwrap()),
    )
    .unwrap()
}

fn extension_binding(id: ObjectId) -> CatalogObject {
    CatalogObject::new(
        id,
        None,
        None,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new("sample").unwrap(),
        1,
        CatalogPayload::Extension(ExtensionPayload::new(id, "1.2.3", 1, 0, 0, [0xa5; 32]).unwrap()),
    )
    .unwrap()
}

fn rename_set(
    catalog_id: [u8; 16],
    generation: u64,
    revision: u64,
    name: &str,
) -> CatalogMutationSet {
    CatalogMutationSet::new(
        [1; 16],
        catalog_id,
        generation,
        vec![CatalogMutation::rename(
            ObjectPrecondition::new(
                ObjectId::BOOTSTRAP_NAMESPACE,
                ObjectKind::Namespace,
                revision,
            )
            .unwrap(),
            CatalogName::new(name).unwrap(),
        )],
        vec![],
        vec![],
    )
    .unwrap()
}

fn transaction(
    marker: u8,
    source_catalog: [u8; 16],
    successor_catalog: [u8; 16],
    generation: u64,
    revision: u64,
    lsn: u64,
    name: &str,
) -> CatalogWalTransaction {
    CatalogWalTransaction::new(
        CatalogWalTransactionId::from_bytes([marker; 16]).unwrap(),
        successor_catalog,
        lsn,
        lsn * 1_000,
        rename_set(source_catalog, generation, revision, name),
    )
    .unwrap()
}

fn record_length(bytes: &[u8]) -> usize {
    u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize
}

#[test]
fn incomplete_mutation_or_torn_commit_is_invisible() {
    let bytes =
        encode_catalog_wal_transaction(&transaction(10, [2; 16], [3; 16], 3, 1, 600, "events"))
            .unwrap();
    let mutation_length = record_length(&bytes);

    for cut in [
        0,
        1,
        CATALOG_WAL_RECORD_HEADER_BYTES - 1,
        mutation_length - 1,
    ] {
        let replay = decode_catalog_wal(&bytes[..cut], CatalogWalReplayLimits::hard()).unwrap();
        assert!(replay.transactions().is_empty(), "cut={cut}");
        assert_eq!(replay.committed_bytes(), 0);
        assert_eq!(replay.incomplete_tail_bytes(), cut);
    }
    let mutation_only =
        decode_catalog_wal(&bytes[..mutation_length], CatalogWalReplayLimits::hard()).unwrap();
    assert!(mutation_only.transactions().is_empty());
    assert_eq!(mutation_only.incomplete_tail_bytes(), mutation_length);

    for commit_cut in [1, 41, CATALOG_WAL_RECORD_HEADER_BYTES - 1] {
        let end = mutation_length + commit_cut;
        let replay = decode_catalog_wal(&bytes[..end], CatalogWalReplayLimits::hard()).unwrap();
        assert!(replay.transactions().is_empty(), "commit_cut={commit_cut}");
        assert_eq!(replay.incomplete_tail_bytes(), end);
    }
}

#[test]
fn committed_replay_matches_direct_deterministic_catalog_pack() {
    let initial = initial();
    let transaction = transaction(10, [2; 16], [3; 16], 3, 1, 600, "events");
    let bytes = encode_catalog_wal_transaction(&transaction).unwrap();
    let decoded = decode_catalog_wal(&bytes, CatalogWalReplayLimits::hard()).unwrap();
    assert_eq!(decoded.transactions(), std::slice::from_ref(&transaction));
    assert_eq!(decoded.committed_bytes(), bytes.len());
    assert_eq!(decoded.incomplete_tail_bytes(), 0);

    let replayed = replay_catalog_wal(&initial, &decoded).unwrap();
    let direct_graph = transaction.mutation_set().apply(&initial).unwrap();
    let direct_meta = CatalogPackMeta::new([1; 16], [3; 16], 4, 600, 600_000).unwrap();
    let replayed_bytes = encode_catalog_pack(replayed.meta(), replayed.graph()).unwrap();
    let direct_bytes = encode_catalog_pack(direct_meta, &direct_graph).unwrap();
    assert_eq!(replayed_bytes, direct_bytes);
    assert_eq!(
        replayed
            .object(ObjectId::BOOTSTRAP_NAMESPACE)
            .unwrap()
            .name()
            .display()
            .as_str(),
        "events"
    );
}

#[test]
fn committed_procedural_upgrade_replays_as_one_complete_generation() {
    let initial = initial();
    let function_id = ObjectId::from_user_bytes([42; 16]).unwrap();
    let mutation = CatalogMutationSet::for_generation(
        &initial,
        vec![CatalogMutation::create(procedural_function(function_id))],
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
    let transaction = CatalogWalTransaction::new(
        CatalogWalTransactionId::from_bytes([43; 16]).unwrap(),
        [3; 16],
        600,
        600_000,
        mutation,
    )
    .unwrap();
    let bytes = encode_catalog_wal_transaction(&transaction).unwrap();

    let mutation_record_bytes = record_length(&bytes);
    for cut in [
        mutation_record_bytes - 1,
        mutation_record_bytes,
        bytes.len() - 1,
    ] {
        let decoded = decode_catalog_wal(&bytes[..cut], CatalogWalReplayLimits::hard()).unwrap();
        let recovered = replay_catalog_wal(&initial, &decoded).unwrap();
        assert_eq!(recovered.format_minor(), 0, "cut={cut}");
        assert!(recovered.object(function_id).is_none(), "cut={cut}");
        assert!(recovered.object(ObjectId::BOOTSTRAP_OWNER).is_none());
    }

    let decoded = decode_catalog_wal(&bytes, CatalogWalReplayLimits::hard()).unwrap();
    let recovered = replay_catalog_wal(&initial, &decoded).unwrap();
    assert_eq!(recovered.format_minor(), PROCEDURAL_CATALOG_MINOR);
    assert_eq!(
        recovered.object(function_id).unwrap().kind(),
        ObjectKind::Function
    );
    assert_eq!(
        recovered.object(ObjectId::BOOTSTRAP_OWNER).unwrap().kind(),
        ObjectKind::Principal
    );
    let reopened = radixdb_catalog::decode_catalog_pack(
        &encode_catalog_pack(recovered.meta(), recovered.graph()).unwrap(),
    )
    .unwrap();
    assert_eq!(reopened.format_minor(), PROCEDURAL_CATALOG_MINOR);
    assert_eq!(
        reopened.graph().objects().len(),
        recovered.graph().objects().len()
    );
}

#[test]
fn committed_extension_upgrade_is_old_or_complete_across_every_torn_wal_boundary() {
    let initial = initial();
    let extension_id = ObjectId::from_user_bytes([44; 16]).unwrap();
    let mutation = CatalogMutationSet::for_generation(
        &initial,
        vec![CatalogMutation::create(extension_binding(extension_id))],
        vec![],
        vec![],
    )
    .unwrap();
    let transaction = CatalogWalTransaction::new(
        CatalogWalTransactionId::from_bytes([45; 16]).unwrap(),
        [4; 16],
        610,
        610_000,
        mutation,
    )
    .unwrap();
    let bytes = encode_catalog_wal_transaction(&transaction).unwrap();
    let mutation_record_bytes = record_length(&bytes);

    for cut in [
        0,
        1,
        mutation_record_bytes - 1,
        mutation_record_bytes,
        bytes.len() - 1,
    ] {
        let decoded = decode_catalog_wal(&bytes[..cut], CatalogWalReplayLimits::hard()).unwrap();
        let recovered = replay_catalog_wal(&initial, &decoded).unwrap();
        assert_eq!(recovered.format_minor(), 0, "cut={cut}");
        assert!(recovered.object(extension_id).is_none(), "cut={cut}");
        assert!(recovered.object(ObjectId::BOOTSTRAP_OWNER).is_none());
    }

    let decoded = decode_catalog_wal(&bytes, CatalogWalReplayLimits::hard()).unwrap();
    let recovered = replay_catalog_wal(&initial, &decoded).unwrap();
    assert_eq!(recovered.format_minor(), EXTENSION_CATALOG_MINOR);
    assert_eq!(
        recovered.object(extension_id).unwrap().kind(),
        ObjectKind::Extension
    );
    assert!(recovered.object(ObjectId::BOOTSTRAP_OWNER).is_some());

    let packed = encode_catalog_pack(recovered.meta(), recovered.graph()).unwrap();
    assert_eq!(
        u16::from_le_bytes(packed[10..12].try_into().unwrap()),
        EXTENSION_CATALOG_MINOR
    );
    let reopened = radixdb_catalog::decode_catalog_pack(&packed).unwrap();
    assert_eq!(reopened.format_minor(), EXTENSION_CATALOG_MINOR);
    assert!(reopened.graph().object(extension_id).is_some());
}

#[test]
fn multiple_committed_transactions_replay_in_wal_order() {
    let first = transaction(10, [2; 16], [3; 16], 3, 1, 600, "events");
    let second = transaction(11, [3; 16], [4; 16], 4, 2, 700, "archive");
    let mut bytes = encode_catalog_wal_transaction(&first).unwrap();
    bytes.extend_from_slice(&encode_catalog_wal_transaction(&second).unwrap());

    let replay = decode_catalog_wal(&bytes, CatalogWalReplayLimits::hard()).unwrap();
    assert_eq!(replay.transactions().len(), 2);
    let final_generation = replay_catalog_wal(&initial(), &replay).unwrap();
    assert_eq!(final_generation.meta().catalog_id(), [4; 16]);
    assert_eq!(final_generation.meta().catalog_generation(), 5);
    assert_eq!(
        final_generation
            .object(ObjectId::BOOTSTRAP_NAMESPACE)
            .unwrap()
            .name()
            .display()
            .as_str(),
        "archive"
    );
}

#[test]
fn complete_corruption_mismatched_marker_and_stale_replay_fail_closed() {
    let first = transaction(10, [2; 16], [3; 16], 3, 1, 600, "events");
    let mut damaged = encode_catalog_wal_transaction(&first).unwrap();
    damaged[CATALOG_WAL_RECORD_HEADER_BYTES] ^= 1;
    assert!(matches!(
        decode_catalog_wal(&damaged, CatalogWalReplayLimits::hard()),
        Err(FormatError::CatalogWalChecksumMismatch { .. })
    ));

    let other = transaction(11, [2; 16], [4; 16], 3, 1, 601, "archive");
    let first_bytes = encode_catalog_wal_transaction(&first).unwrap();
    let other_bytes = encode_catalog_wal_transaction(&other).unwrap();
    let mutation_length = record_length(&first_bytes);
    let other_mutation_length = record_length(&other_bytes);
    let mut mismatched = first_bytes[..mutation_length].to_vec();
    mismatched.extend_from_slice(&other_bytes[other_mutation_length..]);
    assert!(matches!(
        decode_catalog_wal(&mismatched, CatalogWalReplayLimits::hard()),
        Err(FormatError::InvalidCatalogWal { .. })
    ));

    let stale = transaction(12, [9; 16], [10; 16], 3, 1, 600, "events");
    let stale_bytes = encode_catalog_wal_transaction(&stale).unwrap();
    let stale_replay = decode_catalog_wal(&stale_bytes, CatalogWalReplayLimits::hard()).unwrap();
    assert!(matches!(
        replay_catalog_wal(&initial(), &stale_replay),
        Err(FormatError::CatalogMutation { .. })
    ));
}

#[test]
fn runtime_limits_can_only_tighten_hard_replay_bounds() {
    assert!(CatalogWalReplayLimits::new(u64::MAX, 1, 160).is_err());
    let bytes =
        encode_catalog_wal_transaction(&transaction(10, [2; 16], [3; 16], 3, 1, 600, "events"))
            .unwrap();
    let limits =
        CatalogWalReplayLimits::new(bytes.len() as u64 - 1, 1, bytes.len() as u64).unwrap();
    assert!(matches!(
        decode_catalog_wal(&bytes, limits),
        Err(FormatError::CatalogWalLimitExceeded { .. })
    ));
}

#[test]
fn replay_floor_skips_older_transactions_but_preserves_strict_wal_order() {
    let skipped = transaction(9, [8; 16], [9; 16], 2, 1, 500, "stale");
    let applied = transaction(10, [2; 16], [3; 16], 3, 1, 600, "events");
    let mut bytes = encode_catalog_wal_transaction(&skipped).unwrap();
    bytes.extend_from_slice(&encode_catalog_wal_transaction(&applied).unwrap());
    let replay = decode_catalog_wal(&bytes, CatalogWalReplayLimits::hard()).unwrap();

    let recovered = replay_catalog_wal_after(&initial(), &replay, 500).unwrap();
    assert_eq!(recovered.meta().catalog_id(), [3; 16]);
    assert_eq!(recovered.meta().catalog_generation(), 4);

    let newer_first = transaction(11, [2; 16], [3; 16], 3, 1, 700, "events");
    let older_second = transaction(12, [3; 16], [4; 16], 4, 2, 600, "archive");
    let mut reversed = encode_catalog_wal_transaction(&newer_first).unwrap();
    reversed.extend_from_slice(&encode_catalog_wal_transaction(&older_second).unwrap());
    let reversed = decode_catalog_wal(&reversed, CatalogWalReplayLimits::hard()).unwrap();
    assert!(matches!(
        replay_catalog_wal_after(&initial(), &reversed, u64::MAX),
        Err(FormatError::InvalidCatalogWal { .. })
    ));
}

#[test]
fn catalog_snapshot_lsn_is_an_implicit_replay_floor() {
    let already_snapshotted = transaction(9, [8; 16], [9; 16], 2, 1, 500, "stale");
    let applied = transaction(10, [2; 16], [3; 16], 3, 1, 600, "events");
    let mut bytes = encode_catalog_wal_transaction(&already_snapshotted).unwrap();
    bytes.extend_from_slice(&encode_catalog_wal_transaction(&applied).unwrap());
    let replay = decode_catalog_wal(&bytes, CatalogWalReplayLimits::hard()).unwrap();

    // CONTROL deliberately retains an older DML floor, while the selected
    // catalog pack already covers LSN 500.  Recovery must not reapply the
    // transaction represented by that pack.
    let recovered = replay_catalog_wal_after(&initial(), &replay, 0).unwrap();
    assert_eq!(recovered.meta().catalog_id(), [3; 16]);
    assert_eq!(recovered.meta().catalog_generation(), 4);
}

#[cfg(feature = "test-failpoints")]
#[test]
fn transactional_append_rolls_back_errors_before_durable_commit() {
    let transaction = transaction(10, [2; 16], [3; 16], 3, 1, 600, "events");
    for point in [
        GenerationCrashPoint::WalBeforeRecordWrite,
        GenerationCrashPoint::WalAfterRecordWriteBeforeSync,
        GenerationCrashPoint::WalBeforeCommitMarker,
        GenerationCrashPoint::WalAfterCommitMarkerWriteBeforeSync,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog.wal");
        let mut wal = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let guard = GenerationFaultGuard::arm(point, GenerationFaultMode::ReturnIoError);
        assert!(append_catalog_wal_transaction(&mut wal, &transaction).is_err());
        assert_eq!(guard.hit_count(), 1, "{} was not traversed", point.name());
        drop(guard);
        assert!(std::fs::read(&path).unwrap().is_empty());

        append_catalog_wal_transaction(&mut wal, &transaction).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let replay = decode_catalog_wal(&bytes, CatalogWalReplayLimits::hard()).unwrap();
        assert_eq!(replay.transactions(), std::slice::from_ref(&transaction));
        assert_eq!(replay.committed_bytes(), bytes.len());
        assert_eq!(replay.incomplete_tail_bytes(), 0);
    }
}

#[cfg(feature = "test-failpoints")]
#[test]
fn durable_commit_is_visible_even_if_post_durability_observation_fails() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("catalog.wal");
    let mut wal = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let transaction = transaction(10, [2; 16], [3; 16], 3, 1, 600, "events");
    let guard = GenerationFaultGuard::arm(
        GenerationCrashPoint::WalCommitMarkerDurable,
        GenerationFaultMode::ReturnIoError,
    );
    assert!(append_catalog_wal_transaction(&mut wal, &transaction).is_err());
    assert_eq!(guard.hit_count(), 1);
    drop(guard);

    let bytes = std::fs::read(&path).unwrap();
    let replay = decode_catalog_wal(&bytes, CatalogWalReplayLimits::hard()).unwrap();
    assert_eq!(replay.transactions(), std::slice::from_ref(&transaction));
    assert_eq!(replay.committed_bytes(), bytes.len());
    assert_eq!(replay.incomplete_tail_bytes(), 0);
}

#[cfg(feature = "test-failpoints")]
#[test]
#[ignore = "isolated child entrypoint; executed by catalog_process_abort_matrix"]
fn catalog_process_abort_child() {
    let root = std::path::PathBuf::from(std::env::var_os("RADIXDB_PROCESS_TEST_ROOT").unwrap());
    let path = root.join("catalog.wal");
    let mut wal = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let transaction = transaction(10, [2; 16], [3; 16], 3, 1, 600, "events");
    append_catalog_wal_transaction(&mut wal, &transaction).unwrap();

    let source = initial();
    let next_meta = CatalogPackMeta::new([1; 16], [3; 16], 4, 600, 600_000).unwrap();
    let prepared = transaction
        .mutation_set()
        .prepare(&source, next_meta)
        .unwrap();
    let publisher = CatalogPublisher::new(std::sync::Arc::new(source));
    let result = publish_catalog_mutation(&publisher, prepared);
    panic!("child reached the end instead of stopping: {result:?}");
}

#[cfg(feature = "test-failpoints")]
#[test]
fn catalog_process_abort_matrix_replays_only_complete_transactions() {
    let points = [
        GenerationCrashPoint::WalBeforeRecordWrite,
        GenerationCrashPoint::WalAfterRecordWriteBeforeSync,
        GenerationCrashPoint::WalBeforeCommitMarker,
        GenerationCrashPoint::WalAfterCommitMarkerWriteBeforeSync,
        GenerationCrashPoint::WalCommitMarkerDurable,
        GenerationCrashPoint::CatalogRuntimeBeforePublish,
        GenerationCrashPoint::CatalogRuntimePublished,
    ];

    for point in points {
        let root = tempfile::tempdir().unwrap();
        let evidence = root.path().join("boundary.hit");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("catalog_process_abort_child")
            .arg("--ignored")
            .env("RADIXDB_PROCESS_TEST_ROOT", root.path())
            .env("RADIXDB_GENERATION_FAULT_POINT", point.name())
            .env("RADIXDB_GENERATION_FAULT_READY", &evidence)
            .status()
            .unwrap();
        assert!(!status.success(), "{} did not stop the child", point.name());
        assert_eq!(
            std::fs::read_to_string(&evidence).unwrap().trim(),
            point.name()
        );

        let bytes = std::fs::read(root.path().join("catalog.wal")).unwrap();
        let mut observed_name = None;
        for _ in 0..2 {
            let replay = decode_catalog_wal(&bytes, CatalogWalReplayLimits::hard()).unwrap();
            let recovered = replay_catalog_wal(&initial(), &replay).unwrap();
            let name = recovered
                .object(ObjectId::BOOTSTRAP_NAMESPACE)
                .unwrap()
                .name()
                .display()
                .as_str()
                .to_owned();
            if let Some(previous) = observed_name.as_ref() {
                assert_eq!(previous, &name);
            }
            observed_name = Some(name);
        }
        let observed_name = observed_name.unwrap();
        match point.semantic_expectation() {
            SemanticExpectation::Source => assert_eq!(observed_name, "public"),
            SemanticExpectation::Committed => assert_eq!(observed_name, "events"),
            SemanticExpectation::SourceOrCommitted => {
                assert!(matches!(observed_name.as_str(), "public" | "events"))
            }
        }
    }
}
