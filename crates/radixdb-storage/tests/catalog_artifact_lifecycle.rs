use radixdb_catalog::{
    encode_catalog_pack, CatalogGraph, CatalogMutation, CatalogMutationSet, CatalogName,
    CatalogObject, CatalogPackMeta, CatalogPayload, CatalogPublisher, NamespacePayload, ObjectId,
    ObjectKind, ObjectPrecondition,
};
use radixdb_storage::v6::{encode_catalog_artifact, publish_catalog_mutation};

#[cfg(feature = "test-failpoints")]
use radixdb_storage::v6::{GenerationCrashPoint, GenerationFaultGuard, GenerationFaultMode};

fn fixture() -> (CatalogPackMeta, CatalogGraph) {
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
    let graph = CatalogGraph::build(vec![namespace], vec![]).unwrap();
    let meta = CatalogPackMeta::new([1; 16], [2; 16], 3, 4, 5).unwrap();
    (meta, graph)
}

#[test]
fn storage_wrapper_preserves_the_canonical_catalog_bytes() {
    let (meta, graph) = fixture();
    assert_eq!(
        encode_catalog_artifact(meta, &graph).unwrap(),
        encode_catalog_pack(meta, &graph).unwrap()
    );
}

#[cfg(feature = "test-failpoints")]
#[test]
fn catalog_encoder_exposes_the_exact_body_before_footer_boundary() {
    let (meta, graph) = fixture();
    let guard = GenerationFaultGuard::arm(
        GenerationCrashPoint::CatalogPackAfterBodyBeforeFooter,
        GenerationFaultMode::ReturnIoError,
    );
    assert!(encode_catalog_artifact(meta, &graph).is_err());
    assert_eq!(guard.hit_count(), 1);
}

#[cfg(feature = "test-failpoints")]
#[test]
fn catalog_runtime_boundaries_preserve_atomic_generation_visibility() {
    for point in [
        GenerationCrashPoint::CatalogRuntimeBeforePublish,
        GenerationCrashPoint::CatalogRuntimePublished,
    ] {
        let (meta, graph) = fixture();
        let initial = radixdb_catalog::CatalogGeneration::new(meta, graph);
        let mutation = CatalogMutationSet::new(
            [1; 16],
            [2; 16],
            3,
            vec![CatalogMutation::rename(
                ObjectPrecondition::new(ObjectId::BOOTSTRAP_NAMESPACE, ObjectKind::Namespace, 1)
                    .unwrap(),
                CatalogName::new("events").unwrap(),
            )],
            vec![],
            vec![],
        )
        .unwrap();
        let next_meta = CatalogPackMeta::new([1; 16], [3; 16], 4, 5, 6).unwrap();
        let prepared = mutation.prepare(&initial, next_meta).unwrap();
        let publisher = CatalogPublisher::new(std::sync::Arc::new(initial));
        let guard = GenerationFaultGuard::arm(point, GenerationFaultMode::ReturnIoError);

        assert!(publish_catalog_mutation(&publisher, prepared).is_err());
        assert_eq!(guard.hit_count(), 1);
        drop(guard);

        let pinned = publisher.pin().unwrap();
        let expected_name = match point {
            GenerationCrashPoint::CatalogRuntimeBeforePublish => "public",
            GenerationCrashPoint::CatalogRuntimePublished => "events",
            _ => unreachable!(),
        };
        assert_eq!(
            pinned
                .object(ObjectId::BOOTSTRAP_NAMESPACE)
                .unwrap()
                .name()
                .display()
                .as_str(),
            expected_name
        );
    }
}

#[test]
fn concurrent_catalog_publishers_admit_one_successor_and_reject_the_stale_one() {
    let (meta, graph) = fixture();
    let initial = radixdb_catalog::CatalogGeneration::new(meta, graph);
    let prepare = |name: &str, catalog_id: [u8; 16]| {
        let mutation = CatalogMutationSet::new(
            [1; 16],
            [2; 16],
            3,
            vec![CatalogMutation::rename(
                ObjectPrecondition::new(ObjectId::BOOTSTRAP_NAMESPACE, ObjectKind::Namespace, 1)
                    .unwrap(),
                CatalogName::new(name).unwrap(),
            )],
            vec![],
            vec![],
        )
        .unwrap();
        mutation
            .prepare(
                &initial,
                CatalogPackMeta::new([1; 16], catalog_id, 4, 5, 6).unwrap(),
            )
            .unwrap()
    };
    let first = prepare("events", [3; 16]);
    let second = prepare("archive", [4; 16]);
    let publisher = std::sync::Arc::new(CatalogPublisher::new(std::sync::Arc::new(initial)));
    let start = std::sync::Arc::new(std::sync::Barrier::new(3));

    let spawn = |prepared| {
        let publisher = std::sync::Arc::clone(&publisher);
        let start = std::sync::Arc::clone(&start);
        std::thread::spawn(move || {
            start.wait();
            publish_catalog_mutation(&publisher, prepared)
        })
    };
    let first = spawn(first);
    let second = spawn(second);
    start.wait();
    let results = [first.join().unwrap(), second.join().unwrap()];

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    let pinned = publisher.pin().unwrap();
    assert!(matches!(
        pinned
            .object(ObjectId::BOOTSTRAP_NAMESPACE)
            .unwrap()
            .name()
            .display()
            .as_str(),
        "events" | "archive"
    ));
}
