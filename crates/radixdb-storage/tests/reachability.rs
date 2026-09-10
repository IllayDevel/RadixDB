use std::collections::HashMap;

use radixdb_catalog::{
    encode_catalog_pack, CatalogEdge, CatalogGraph, CatalogName, CatalogObject, CatalogPackMeta,
    CatalogPayload, ColumnPayload, NamespacePayload, ObjectId, TablePayload,
};
use radixdb_core::DataType;
use radixdb_storage::v6::{
    encode_database_manifest, encode_table_manifest, validate_control_generation,
    validate_control_generation_with_limits, ArtifactId, ArtifactInspection, ArtifactKind,
    ArtifactMetadata, ArtifactRef, CatalogGeneration, CatalogId, CatalogRef, ControlRecord,
    ControlSlotIndex, DataArtifactMetadata, DatabaseGeneration, DatabaseId, DatabaseManifest,
    DatabaseManifestRootRef, IndexArtifactMetadata, ManifestGeneration, ManifestId, ManifestKind,
    ManifestRef, ReachabilityError, ReachabilityLimits, ReachabilityResult, ReachabilitySource,
    ReachableNodeKind, SegmentDescriptor, SegmentId, SegmentKind, TableManifest, TableManifestRef,
    UnavailableIndexReason, WalGeneration, WalReplayFloor, WriterInstanceId,
    MAX_REACHABILITY_BYTES,
};

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes(raw(marker)).unwrap()
}

fn footer_sha(bytes: &[u8]) -> [u8; 32] {
    bytes[bytes.len() - 32..].try_into().unwrap()
}

fn catalog_object(
    id: ObjectId,
    namespace_id: Option<ObjectId>,
    parent_id: Option<ObjectId>,
    name: &str,
    payload: CatalogPayload,
) -> CatalogObject {
    CatalogObject::new(
        id,
        namespace_id,
        parent_id,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new(name).unwrap(),
        1,
        payload,
    )
    .unwrap()
}

fn catalog_bytes(database_id: DatabaseId, catalog_id: CatalogId, table_id: ObjectId) -> Vec<u8> {
    catalog_bytes_for_tables(
        database_id,
        catalog_id,
        &[(table_id, object_id(11), "messages")],
    )
}

fn catalog_bytes_for_tables(
    database_id: DatabaseId,
    catalog_id: CatalogId,
    tables: &[(ObjectId, ObjectId, &str)],
) -> Vec<u8> {
    let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
    let mut objects = vec![catalog_object(
        namespace,
        None,
        None,
        "public",
        CatalogPayload::Namespace(NamespacePayload::new()),
    )];
    let mut edges = Vec::with_capacity(tables.len() * 2);
    for (ordinal, (table_id, column_id, table_name)) in tables.iter().copied().enumerate() {
        objects.extend([
            catalog_object(
                table_id,
                Some(namespace),
                Some(namespace),
                table_name,
                CatalogPayload::Table(
                    TablePayload::new(vec![column_id], vec![], vec![], None).unwrap(),
                ),
            ),
            catalog_object(
                column_id,
                Some(namespace),
                Some(table_id),
                "id",
                CatalogPayload::Column(
                    ColumnPayload::new(
                        0,
                        radixdb_catalog::CatalogDataType::scalar(DataType::Integer).unwrap(),
                        false,
                        None,
                        None,
                    )
                    .unwrap(),
                ),
            ),
        ]);
        edges.extend([
            CatalogEdge::new(
                namespace,
                table_id,
                radixdb_catalog::EdgeKind::Contains,
                ordinal as u32,
            ),
            CatalogEdge::new(table_id, column_id, radixdb_catalog::EdgeKind::Contains, 0),
        ]);
    }
    let graph = CatalogGraph::build(objects, edges).unwrap();
    encode_catalog_pack(
        CatalogPackMeta::new(
            database_id.into_bytes(),
            catalog_id.into_bytes(),
            3,
            500,
            123_000,
        )
        .unwrap(),
        &graph,
    )
    .unwrap()
}

fn namespace_only_catalog_bytes(database_id: DatabaseId, catalog_id: CatalogId) -> Vec<u8> {
    let namespace = ObjectId::BOOTSTRAP_NAMESPACE;
    let graph = CatalogGraph::build(
        vec![catalog_object(
            namespace,
            None,
            None,
            "public",
            CatalogPayload::Namespace(NamespacePayload::new()),
        )],
        vec![],
    )
    .unwrap();
    encode_catalog_pack(
        CatalogPackMeta::new(
            database_id.into_bytes(),
            catalog_id.into_bytes(),
            3,
            500,
            123_000,
        )
        .unwrap(),
        &graph,
    )
    .unwrap()
}

fn artifact(marker: u8, kind: ArtifactKind) -> ArtifactRef {
    ArtifactRef::new(
        ArtifactId::from_bytes(raw(marker)).unwrap(),
        kind,
        DatabaseGeneration::new(6).unwrap(),
        4096,
        [marker.wrapping_add(1); 32],
    )
    .unwrap()
}

#[derive(Clone)]
struct FixtureSource {
    database_manifest: Option<Vec<u8>>,
    catalog: Option<Vec<u8>>,
    table_manifests: HashMap<ManifestId, Vec<u8>>,
    artifacts: HashMap<ArtifactId, ArtifactInspection>,
    inspected_artifacts: Vec<ArtifactId>,
}

impl ReachabilitySource for FixtureSource {
    fn read_database_manifest(
        &mut self,
        _reference: DatabaseManifestRootRef,
        byte_budget: u64,
    ) -> ReachabilityResult<Option<Vec<u8>>> {
        assert!(self
            .database_manifest
            .as_ref()
            .is_none_or(|bytes| bytes.len() as u64 <= byte_budget));
        Ok(self.database_manifest.clone())
    }

    fn read_catalog(
        &mut self,
        _reference: CatalogRef,
        byte_budget: u64,
    ) -> ReachabilityResult<Option<Vec<u8>>> {
        assert!(self
            .catalog
            .as_ref()
            .is_none_or(|bytes| bytes.len() as u64 <= byte_budget));
        Ok(self.catalog.clone())
    }

    fn read_table_manifest(
        &mut self,
        reference: TableManifestRef,
        byte_budget: u64,
    ) -> ReachabilityResult<Option<Vec<u8>>> {
        let bytes = self.table_manifests.get(&reference.manifest().id());
        assert!(bytes.is_none_or(|bytes| bytes.len() as u64 <= byte_budget));
        Ok(bytes.cloned())
    }

    fn inspect_artifact(
        &mut self,
        reference: ArtifactRef,
        allowance: radixdb_storage::v6::ReachabilityAllowance,
    ) -> ReachabilityResult<ArtifactInspection> {
        let inspection = self
            .artifacts
            .get(&reference.id())
            .cloned()
            .unwrap_or(ArtifactInspection::Missing);
        if let ArtifactInspection::Present {
            accounted_bytes, ..
        } = &inspection
        {
            if *accounted_bytes > allowance.remaining_bytes() {
                return Err(allowance.exceeded(*accounted_bytes));
            }
        }
        self.inspected_artifacts.push(reference.id());
        Ok(inspection)
    }
}

struct Fixture {
    control: ControlRecord,
    source: FixtureSource,
    database_id: DatabaseId,
    catalog_id: CatalogId,
    table_id: ObjectId,
    table_manifest_id: ManifestId,
    segment_id: SegmentId,
    data_reference: ArtifactRef,
    index_reference: ArtifactRef,
}

fn fixture() -> Fixture {
    let database_id = DatabaseId::from_bytes(raw(1)).unwrap();
    let catalog_id = CatalogId::from_bytes(raw(2)).unwrap();
    let database_manifest_id = ManifestId::from_bytes(raw(3)).unwrap();
    let table_manifest_id = ManifestId::from_bytes(raw(4)).unwrap();
    let table_id = object_id(10);
    let segment_id = SegmentId::from_bytes(raw(5)).unwrap();
    let data_reference = artifact(6, ArtifactKind::Data);
    let index_reference = artifact(7, ArtifactKind::Index);
    let catalog = catalog_bytes(database_id, catalog_id, table_id);
    let catalog_reference = CatalogRef::new(
        catalog_id,
        CatalogGeneration::new(3).unwrap(),
        catalog.len() as u64,
        footer_sha(&catalog),
    )
    .unwrap();
    let segment = SegmentDescriptor::new(
        segment_id,
        SegmentKind::Rows,
        10,
        20,
        100,
        1,
        100,
        data_reference,
        Some(index_reference),
    )
    .unwrap();
    let table_manifest = TableManifest::new(
        database_id,
        table_id,
        table_manifest_id,
        ManifestGeneration::new(7).unwrap(),
        CatalogGeneration::new(3).unwrap(),
        100,
        2,
        vec![segment],
        456_000,
    )
    .unwrap();
    let table_bytes = encode_table_manifest(&table_manifest).unwrap();
    let table_reference = TableManifestRef::new(
        table_id,
        ManifestRef::new(
            table_manifest_id,
            ManifestKind::Table,
            ManifestGeneration::new(7).unwrap(),
            table_bytes.len() as u64,
            footer_sha(&table_bytes),
        )
        .unwrap(),
    )
    .unwrap();
    let wal_floor = WalReplayFloor::new(WalGeneration::new(5).unwrap(), 600);
    let database_manifest = DatabaseManifest::new(
        database_id,
        database_manifest_id,
        DatabaseGeneration::new(7).unwrap(),
        catalog_reference,
        wal_floor,
        10_000,
        vec![table_reference],
        789_000,
    )
    .unwrap();
    let database_bytes = encode_database_manifest(&database_manifest).unwrap();
    let control = ControlRecord::new(
        ControlSlotIndex::Zero,
        DatabaseGeneration::new(7).unwrap(),
        database_id,
        DatabaseManifestRootRef::new(
            database_manifest_id,
            ManifestGeneration::new(7).unwrap(),
            footer_sha(&database_bytes),
        ),
        radixdb_storage::v6::CatalogRootRef::new(
            catalog_id,
            CatalogGeneration::new(3).unwrap(),
            footer_sha(&catalog),
        ),
        wal_floor,
        999_000,
        WriterInstanceId::from_bytes(raw(8)).unwrap(),
    )
    .unwrap();
    let data_metadata = DataArtifactMetadata::new(
        data_reference,
        database_id,
        table_id,
        segment_id,
        CatalogGeneration::new(3).unwrap(),
        SegmentKind::Rows,
        10,
        20,
        100,
    );
    let index_metadata = IndexArtifactMetadata::new(
        index_reference,
        database_id,
        table_id,
        segment_id,
        CatalogGeneration::new(3).unwrap(),
        data_reference.id(),
        *data_reference.body_sha256(),
    );
    let mut table_manifests = HashMap::new();
    table_manifests.insert(table_manifest_id, table_bytes);
    let mut artifacts = HashMap::new();
    artifacts.insert(
        data_reference.id(),
        ArtifactInspection::present(ArtifactMetadata::Data(data_metadata)),
    );
    artifacts.insert(
        index_reference.id(),
        ArtifactInspection::present(ArtifactMetadata::Index(index_metadata)),
    );
    Fixture {
        control,
        source: FixtureSource {
            database_manifest: Some(database_bytes),
            catalog: Some(catalog),
            table_manifests,
            artifacts,
            inspected_artifacts: Vec::new(),
        },
        database_id,
        catalog_id,
        table_id,
        table_manifest_id,
        segment_id,
        data_reference,
        index_reference,
    }
}

fn replace_table_catalog_generation(fixture: &mut Fixture, generation: CatalogGeneration) {
    let table = radixdb_storage::v6::decode_table_manifest(
        fixture
            .source
            .table_manifests
            .get(&fixture.table_manifest_id)
            .unwrap(),
    )
    .unwrap();
    let table = TableManifest::new(
        table.database_id(),
        table.table_id(),
        table.manifest_id(),
        table.generation(),
        generation,
        table.row_id_high_water(),
        table.next_segment_sequence(),
        table.segments().to_vec(),
        table.created_unix_ns(),
    )
    .unwrap();
    let table_bytes = encode_table_manifest(&table).unwrap();
    let table_reference = TableManifestRef::new(
        table.table_id(),
        ManifestRef::new(
            table.manifest_id(),
            ManifestKind::Table,
            table.generation(),
            table_bytes.len() as u64,
            footer_sha(&table_bytes),
        )
        .unwrap(),
    )
    .unwrap();

    let database = radixdb_storage::v6::decode_database_manifest(
        fixture.source.database_manifest.as_ref().unwrap(),
    )
    .unwrap();
    let database = DatabaseManifest::new(
        database.database_id(),
        database.manifest_id(),
        database.generation(),
        database.catalog(),
        database.wal_replay_floor(),
        database.transaction_high_water(),
        vec![table_reference],
        database.created_unix_ns(),
    )
    .unwrap();
    let database_bytes = encode_database_manifest(&database).unwrap();
    fixture.control = ControlRecord::new(
        fixture.control.slot(),
        fixture.control.database_generation(),
        fixture.control.database_id(),
        DatabaseManifestRootRef::new(
            database.manifest_id(),
            ManifestGeneration::new(database.generation().get()).unwrap(),
            footer_sha(&database_bytes),
        ),
        fixture.control.catalog(),
        fixture.control.wal_replay_floor(),
        fixture.control.published_unix_ns(),
        fixture.control.writer_instance_id(),
    )
    .unwrap();
    fixture
        .source
        .table_manifests
        .insert(fixture.table_manifest_id, table_bytes);
    fixture.source.database_manifest = Some(database_bytes);
}

fn fixture_with_data_segments(segment_count: u8, artifact_accounted_bytes: u64) -> Fixture {
    assert!(segment_count > 1);
    let database_id = DatabaseId::from_bytes(raw(1)).unwrap();
    let catalog_id = CatalogId::from_bytes(raw(2)).unwrap();
    let database_manifest_id = ManifestId::from_bytes(raw(3)).unwrap();
    let table_manifest_id = ManifestId::from_bytes(raw(4)).unwrap();
    let table_id = object_id(10);
    let catalog = catalog_bytes(database_id, catalog_id, table_id);
    let catalog_reference = CatalogRef::new(
        catalog_id,
        CatalogGeneration::new(3).unwrap(),
        catalog.len() as u64,
        footer_sha(&catalog),
    )
    .unwrap();
    let mut segments = Vec::with_capacity(segment_count as usize);
    let mut artifacts = HashMap::with_capacity(segment_count as usize);
    for ordinal in 0..segment_count {
        let marker = 20_u8.checked_add(ordinal).unwrap();
        let segment_id = SegmentId::from_bytes(raw(marker)).unwrap();
        let data_reference = artifact(marker.checked_add(64).unwrap(), ArtifactKind::Data);
        let first_row_id = u64::from(ordinal) * 10 + 1;
        let last_row_id = first_row_id + 9;
        segments.push(
            SegmentDescriptor::new(
                segment_id,
                SegmentKind::Rows,
                10,
                20,
                10,
                first_row_id,
                last_row_id,
                data_reference,
                None,
            )
            .unwrap(),
        );
        artifacts.insert(
            data_reference.id(),
            ArtifactInspection::present_accounted(
                ArtifactMetadata::Data(DataArtifactMetadata::new(
                    data_reference,
                    database_id,
                    table_id,
                    segment_id,
                    CatalogGeneration::new(3).unwrap(),
                    SegmentKind::Rows,
                    10,
                    20,
                    10,
                )),
                artifact_accounted_bytes,
            ),
        );
    }
    let table_manifest = TableManifest::new(
        database_id,
        table_id,
        table_manifest_id,
        ManifestGeneration::new(7).unwrap(),
        CatalogGeneration::new(3).unwrap(),
        u64::from(segment_count) * 10,
        u64::from(segment_count) + 1,
        segments,
        456_000,
    )
    .unwrap();
    let table_bytes = encode_table_manifest(&table_manifest).unwrap();
    let table_reference = TableManifestRef::new(
        table_id,
        ManifestRef::new(
            table_manifest_id,
            ManifestKind::Table,
            ManifestGeneration::new(7).unwrap(),
            table_bytes.len() as u64,
            footer_sha(&table_bytes),
        )
        .unwrap(),
    )
    .unwrap();
    let wal_floor = WalReplayFloor::new(WalGeneration::new(5).unwrap(), 600);
    let database_manifest = DatabaseManifest::new(
        database_id,
        database_manifest_id,
        DatabaseGeneration::new(7).unwrap(),
        catalog_reference,
        wal_floor,
        10_000,
        vec![table_reference],
        789_000,
    )
    .unwrap();
    let database_bytes = encode_database_manifest(&database_manifest).unwrap();
    let control = ControlRecord::new(
        ControlSlotIndex::Zero,
        DatabaseGeneration::new(7).unwrap(),
        database_id,
        DatabaseManifestRootRef::new(
            database_manifest_id,
            ManifestGeneration::new(7).unwrap(),
            footer_sha(&database_bytes),
        ),
        radixdb_storage::v6::CatalogRootRef::new(
            catalog_id,
            CatalogGeneration::new(3).unwrap(),
            footer_sha(&catalog),
        ),
        wal_floor,
        999_000,
        WriterInstanceId::from_bytes(raw(8)).unwrap(),
    )
    .unwrap();
    let first_segment_id = SegmentId::from_bytes(raw(20)).unwrap();
    let first_data_reference = artifact(84, ArtifactKind::Data);
    Fixture {
        control,
        source: FixtureSource {
            database_manifest: Some(database_bytes),
            catalog: Some(catalog),
            table_manifests: HashMap::from([(table_manifest_id, table_bytes)]),
            artifacts,
            inspected_artifacts: Vec::new(),
        },
        database_id,
        catalog_id,
        table_id,
        table_manifest_id,
        segment_id: first_segment_id,
        data_reference: first_data_reference,
        index_reference: artifact(7, ArtifactKind::Index),
    }
}

#[test]
fn complete_graph_validates_only_explicit_references() {
    let mut fixture = fixture();
    let encoded_metadata_bytes = fixture.source.database_manifest.as_ref().unwrap().len()
        + fixture.source.catalog.as_ref().unwrap().len()
        + fixture
            .source
            .table_manifests
            .values()
            .map(Vec::len)
            .sum::<usize>();
    let orphan = artifact(90, ArtifactKind::Data);
    fixture.source.artifacts.insert(
        orphan.id(),
        ArtifactInspection::invalid("unreachable directory noise"),
    );

    let generation = validate_control_generation(fixture.control, &mut fixture.source).unwrap();
    assert_eq!(generation.control(), fixture.control);
    assert_eq!(generation.table_manifests().len(), 1);
    assert!(
        generation.accounted_bytes() > encoded_metadata_bytes as u64,
        "decoded catalog/manifests and validation state must share the open budget"
    );
    assert_eq!(generation.data_artifacts().len(), 1);
    assert_eq!(generation.index_artifacts().len(), 1);
    assert!(generation.unavailable_indexes().is_empty());
    assert_eq!(generation.accounted_identities(), 5);
    assert!(generation.accounted_bytes() > 0);
    assert_eq!(
        fixture.source.inspected_artifacts,
        vec![fixture.data_reference.id(), fixture.index_reference.id()]
    );
    assert!(!fixture.source.inspected_artifacts.contains(&orphan.id()));
}

#[test]
fn many_small_segments_refuse_the_next_layout_before_shared_budget_overrun() {
    const SEGMENTS: u8 = 32;
    const ARTIFACT_ACCOUNTED_BYTES: u64 = 512;

    let mut admitted = fixture_with_data_segments(SEGMENTS, ARTIFACT_ACCOUNTED_BYTES);
    let generation = validate_control_generation(admitted.control, &mut admitted.source).unwrap();
    assert_eq!(admitted.source.inspected_artifacts.len(), SEGMENTS as usize);

    let lowered_limit = generation.accounted_bytes() - ARTIFACT_ACCOUNTED_BYTES / 2;
    let mut rejected = fixture_with_data_segments(SEGMENTS, ARTIFACT_ACCOUNTED_BYTES);
    let error = validate_control_generation_with_limits(
        rejected.control,
        &mut rejected.source,
        ReachabilityLimits::new(1_000, lowered_limit).unwrap(),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        ReachabilityError::ReachabilityLimitExceeded {
            field: "accounted bytes",
            actual,
            limit,
        } if actual > limit && limit == lowered_limit
    ));
    assert_eq!(
        rejected.source.inspected_artifacts.len(),
        SEGMENTS as usize - 1,
        "the source must reject the final metadata allocation before retaining it"
    );
}

#[test]
fn generation_shape_limits_reject_counts_before_artifact_inspection() {
    let admitted_limits = ReachabilityLimits::default()
        .with_generation_counts(2, 2)
        .unwrap();
    let mut admitted = fixture_with_data_segments(2, 0);
    let generation = validate_control_generation_with_limits(
        admitted.control,
        &mut admitted.source,
        admitted_limits,
    )
    .unwrap();
    assert_eq!(generation.table_manifests().len(), 1);
    assert_eq!(generation.data_artifacts().len(), 2);

    let mut manifest_rejected = fixture_with_data_segments(2, 0);
    let manifest_error = validate_control_generation_with_limits(
        manifest_rejected.control,
        &mut manifest_rejected.source,
        ReachabilityLimits::default()
            .with_generation_counts(1, 2)
            .unwrap(),
    )
    .unwrap_err();
    assert!(matches!(
        manifest_error,
        ReachabilityError::ReachabilityLimitExceeded {
            field: "manifest count",
            actual: 2,
            limit: 1,
        }
    ));
    assert!(manifest_rejected.source.inspected_artifacts.is_empty());

    let mut segment_rejected = fixture_with_data_segments(2, 0);
    let segment_error = validate_control_generation_with_limits(
        segment_rejected.control,
        &mut segment_rejected.source,
        ReachabilityLimits::default()
            .with_generation_counts(2, 1)
            .unwrap(),
    )
    .unwrap_err();
    assert!(matches!(
        segment_error,
        ReachabilityError::ReachabilityLimitExceeded {
            field: "segment count",
            actual: 2,
            limit: 1,
        }
    ));
    assert!(segment_rejected.source.inspected_artifacts.is_empty());
}

#[test]
fn immutable_artifacts_may_predate_the_selected_catalog_generation() {
    let mut fixture = fixture();
    let historical = CatalogGeneration::new(2).unwrap();
    fixture.source.artifacts.insert(
        fixture.data_reference.id(),
        ArtifactInspection::present(ArtifactMetadata::Data(DataArtifactMetadata::new(
            fixture.data_reference,
            fixture.database_id,
            fixture.table_id,
            fixture.segment_id,
            historical,
            SegmentKind::Rows,
            10,
            20,
            100,
        ))),
    );
    fixture.source.artifacts.insert(
        fixture.index_reference.id(),
        ArtifactInspection::present(ArtifactMetadata::Index(IndexArtifactMetadata::new(
            fixture.index_reference,
            fixture.database_id,
            fixture.table_id,
            fixture.segment_id,
            historical,
            fixture.data_reference.id(),
            *fixture.data_reference.body_sha256(),
        ))),
    );

    let generation = validate_control_generation(fixture.control, &mut fixture.source).unwrap();
    assert_eq!(generation.data_artifacts().len(), 1);
    assert_eq!(generation.index_artifacts().len(), 1);
    assert!(generation.unavailable_indexes().is_empty());
}

#[test]
fn artifact_catalog_binding_from_the_future_fails_closed() {
    let mut fixture = fixture();
    fixture.source.artifacts.insert(
        fixture.data_reference.id(),
        ArtifactInspection::present(ArtifactMetadata::Data(DataArtifactMetadata::new(
            fixture.data_reference,
            fixture.database_id,
            fixture.table_id,
            fixture.segment_id,
            CatalogGeneration::new(4).unwrap(),
            SegmentKind::Rows,
            10,
            20,
            100,
        ))),
    );

    assert!(matches!(
        validate_control_generation(fixture.control, &mut fixture.source),
        Err(ReachabilityError::CrossReferenceMismatch {
            edge: "table manifest -> data artifact",
            ..
        })
    ));
}

#[test]
fn table_manifest_may_predate_selected_catalog_but_never_come_from_the_future() {
    let mut historical = fixture();
    let historical_generation = CatalogGeneration::new(2).unwrap();
    replace_table_catalog_generation(&mut historical, historical_generation);
    historical.source.artifacts.insert(
        historical.data_reference.id(),
        ArtifactInspection::present(ArtifactMetadata::Data(DataArtifactMetadata::new(
            historical.data_reference,
            historical.database_id,
            historical.table_id,
            historical.segment_id,
            historical_generation,
            SegmentKind::Rows,
            10,
            20,
            100,
        ))),
    );
    historical.source.artifacts.insert(
        historical.index_reference.id(),
        ArtifactInspection::present(ArtifactMetadata::Index(IndexArtifactMetadata::new(
            historical.index_reference,
            historical.database_id,
            historical.table_id,
            historical.segment_id,
            historical_generation,
            historical.data_reference.id(),
            *historical.data_reference.body_sha256(),
        ))),
    );
    validate_control_generation(historical.control, &mut historical.source).unwrap();

    let mut future = fixture();
    replace_table_catalog_generation(&mut future, CatalogGeneration::new(4).unwrap());
    assert!(matches!(
        validate_control_generation(future.control, &mut future.source),
        Err(ReachabilityError::CrossReferenceMismatch {
            edge: "table manifest -> catalog",
            ..
        })
    ));
}

#[test]
fn missing_or_invalid_required_data_fails_with_exact_node() {
    let mut missing = fixture();
    missing
        .source
        .artifacts
        .remove(&missing.data_reference.id());
    assert!(matches!(
        validate_control_generation(missing.control, &mut missing.source),
        Err(ReachabilityError::MissingRequiredNode {
            node: ReachableNodeKind::DataArtifact,
            ..
        })
    ));

    let mut invalid = fixture();
    invalid.source.artifacts.insert(
        invalid.data_reference.id(),
        ArtifactInspection::invalid("body checksum mismatch"),
    );
    assert!(matches!(
        validate_control_generation(invalid.control, &mut invalid.source),
        Err(ReachabilityError::InvalidRequiredNode {
            node: ReachableNodeKind::DataArtifact,
            ..
        })
    ));
}

#[test]
fn optional_index_failure_keeps_generation_and_records_reason() {
    let mut missing = fixture();
    missing
        .source
        .artifacts
        .remove(&missing.index_reference.id());
    let generation = validate_control_generation(missing.control, &mut missing.source).unwrap();
    assert!(generation.index_artifacts().is_empty());
    assert_eq!(generation.unavailable_indexes().len(), 1);
    assert_eq!(
        generation.unavailable_indexes()[0].reason(),
        &UnavailableIndexReason::Missing
    );

    let mut mismatched = fixture();
    let wrong = IndexArtifactMetadata::new(
        mismatched.index_reference,
        mismatched.database_id,
        mismatched.table_id,
        mismatched.segment_id,
        CatalogGeneration::new(3).unwrap(),
        ArtifactId::from_bytes(raw(99)).unwrap(),
        [0; 32],
    );
    mismatched.source.artifacts.insert(
        mismatched.index_reference.id(),
        ArtifactInspection::present(ArtifactMetadata::Index(wrong)),
    );
    let generation =
        validate_control_generation(mismatched.control, &mut mismatched.source).unwrap();
    assert!(matches!(
        generation.unavailable_indexes()[0].reason(),
        UnavailableIndexReason::CrossReferenceMismatch(detail)
            if detail.contains("source DATA")
    ));
}

#[test]
fn manifest_catalog_and_artifact_cross_references_fail_closed() {
    let mut missing_table = fixture();
    missing_table
        .source
        .table_manifests
        .remove(&missing_table.table_manifest_id);
    assert!(matches!(
        validate_control_generation(missing_table.control, &mut missing_table.source),
        Err(ReachabilityError::MissingRequiredNode {
            node: ReachableNodeKind::TableManifest,
            ..
        })
    ));

    let mut table_set = fixture();
    let catalog = namespace_only_catalog_bytes(table_set.database_id, table_set.catalog_id);
    let catalog_ref = CatalogRef::new(
        table_set.catalog_id,
        CatalogGeneration::new(3).unwrap(),
        catalog.len() as u64,
        footer_sha(&catalog),
    )
    .unwrap();
    let original_database = radixdb_storage::v6::decode_database_manifest(
        table_set.source.database_manifest.as_ref().unwrap(),
    )
    .unwrap();
    let replacement_database = DatabaseManifest::new(
        table_set.database_id,
        original_database.manifest_id(),
        original_database.generation(),
        catalog_ref,
        original_database.wal_replay_floor(),
        original_database.transaction_high_water(),
        original_database.tables().to_vec(),
        original_database.created_unix_ns(),
    )
    .unwrap();
    let database_bytes = encode_database_manifest(&replacement_database).unwrap();
    table_set.control = ControlRecord::new(
        table_set.control.slot(),
        table_set.control.database_generation(),
        table_set.control.database_id(),
        DatabaseManifestRootRef::new(
            replacement_database.manifest_id(),
            table_set.control.database_manifest().generation(),
            footer_sha(&database_bytes),
        ),
        radixdb_storage::v6::CatalogRootRef::new(
            table_set.catalog_id,
            CatalogGeneration::new(3).unwrap(),
            footer_sha(&catalog),
        ),
        table_set.control.wal_replay_floor(),
        table_set.control.published_unix_ns(),
        table_set.control.writer_instance_id(),
    )
    .unwrap();
    table_set.source.database_manifest = Some(database_bytes);
    table_set.source.catalog = Some(catalog);
    assert!(matches!(
        validate_control_generation(table_set.control, &mut table_set.source),
        Err(ReachabilityError::CrossReferenceMismatch {
            edge: "database manifest -> catalog",
            ..
        })
    ));

    let mut stale_high_water = fixture();
    let original_database = radixdb_storage::v6::decode_database_manifest(
        stale_high_water.source.database_manifest.as_ref().unwrap(),
    )
    .unwrap();
    let replacement_database = DatabaseManifest::new(
        original_database.database_id(),
        original_database.manifest_id(),
        original_database.generation(),
        original_database.catalog(),
        original_database.wal_replay_floor(),
        19,
        original_database.tables().to_vec(),
        original_database.created_unix_ns(),
    )
    .unwrap();
    let database_bytes = encode_database_manifest(&replacement_database).unwrap();
    stale_high_water.control = ControlRecord::new(
        stale_high_water.control.slot(),
        stale_high_water.control.database_generation(),
        stale_high_water.control.database_id(),
        DatabaseManifestRootRef::new(
            replacement_database.manifest_id(),
            stale_high_water.control.database_manifest().generation(),
            footer_sha(&database_bytes),
        ),
        stale_high_water.control.catalog(),
        stale_high_water.control.wal_replay_floor(),
        stale_high_water.control.published_unix_ns(),
        stale_high_water.control.writer_instance_id(),
    )
    .unwrap();
    stale_high_water.source.database_manifest = Some(database_bytes);
    assert!(matches!(
        validate_control_generation(stale_high_water.control, &mut stale_high_water.source),
        Err(ReachabilityError::CrossReferenceMismatch {
            edge: "database manifest -> segment descriptor",
            ..
        })
    ));

    let mut mismatched_data = fixture();
    let wrong_data = DataArtifactMetadata::new(
        mismatched_data.data_reference,
        mismatched_data.database_id,
        mismatched_data.table_id,
        SegmentId::from_bytes(raw(88)).unwrap(),
        CatalogGeneration::new(3).unwrap(),
        SegmentKind::Rows,
        10,
        20,
        100,
    );
    mismatched_data.source.artifacts.insert(
        mismatched_data.data_reference.id(),
        ArtifactInspection::present(ArtifactMetadata::Data(wrong_data)),
    );
    assert!(matches!(
        validate_control_generation(mismatched_data.control, &mut mismatched_data.source),
        Err(ReachabilityError::CrossReferenceMismatch {
            edge: "table manifest -> data artifact",
            ..
        })
    ));
}

#[test]
fn segment_identity_is_unique_across_all_table_manifests() {
    let mut fixture = fixture();
    let second_table_id = object_id(20);
    let second_manifest_id = ManifestId::from_bytes(raw(14)).unwrap();
    let second_data = artifact(16, ArtifactKind::Data);
    let second_segment = SegmentDescriptor::new(
        fixture.segment_id,
        SegmentKind::Rows,
        21,
        30,
        50,
        101,
        150,
        second_data,
        None,
    )
    .unwrap();
    let second_manifest = TableManifest::new(
        fixture.database_id,
        second_table_id,
        second_manifest_id,
        ManifestGeneration::new(7).unwrap(),
        CatalogGeneration::new(3).unwrap(),
        150,
        2,
        vec![second_segment],
        456_001,
    )
    .unwrap();
    let second_bytes = encode_table_manifest(&second_manifest).unwrap();
    let second_reference = TableManifestRef::new(
        second_table_id,
        ManifestRef::new(
            second_manifest_id,
            ManifestKind::Table,
            ManifestGeneration::new(7).unwrap(),
            second_bytes.len() as u64,
            footer_sha(&second_bytes),
        )
        .unwrap(),
    )
    .unwrap();

    let original_database = radixdb_storage::v6::decode_database_manifest(
        fixture.source.database_manifest.as_ref().unwrap(),
    )
    .unwrap();
    let catalog = catalog_bytes_for_tables(
        fixture.database_id,
        fixture.catalog_id,
        &[
            (fixture.table_id, object_id(11), "messages"),
            (second_table_id, object_id(21), "deliveries"),
        ],
    );
    let catalog_reference = CatalogRef::new(
        fixture.catalog_id,
        CatalogGeneration::new(3).unwrap(),
        catalog.len() as u64,
        footer_sha(&catalog),
    )
    .unwrap();
    let database = DatabaseManifest::new(
        fixture.database_id,
        original_database.manifest_id(),
        original_database.generation(),
        catalog_reference,
        original_database.wal_replay_floor(),
        original_database.transaction_high_water(),
        vec![original_database.tables()[0], second_reference],
        original_database.created_unix_ns(),
    )
    .unwrap();
    let database_bytes = encode_database_manifest(&database).unwrap();
    fixture.control = ControlRecord::new(
        fixture.control.slot(),
        fixture.control.database_generation(),
        fixture.database_id,
        DatabaseManifestRootRef::new(
            database.manifest_id(),
            fixture.control.database_manifest().generation(),
            footer_sha(&database_bytes),
        ),
        radixdb_storage::v6::CatalogRootRef::new(
            fixture.catalog_id,
            CatalogGeneration::new(3).unwrap(),
            footer_sha(&catalog),
        ),
        fixture.control.wal_replay_floor(),
        fixture.control.published_unix_ns(),
        fixture.control.writer_instance_id(),
    )
    .unwrap();
    fixture.source.database_manifest = Some(database_bytes);
    fixture.source.catalog = Some(catalog);
    fixture
        .source
        .table_manifests
        .insert(second_manifest_id, second_bytes);

    assert!(matches!(
        validate_control_generation(fixture.control, &mut fixture.source),
        Err(ReachabilityError::DuplicateIdentity {
            node: ReachableNodeKind::SegmentDescriptor,
            ..
        })
    ));
}

#[test]
fn lowered_budget_aborts_traversal_before_extra_nodes() {
    let mut fixture = fixture();
    let limits = ReachabilityLimits::new(2, MAX_REACHABILITY_BYTES).unwrap();
    assert!(matches!(
        validate_control_generation_with_limits(fixture.control, &mut fixture.source, limits),
        Err(ReachabilityError::ReachabilityLimitExceeded {
            field: "identity count",
            actual: 3,
            limit: 2,
        })
    ));
    assert!(fixture.source.inspected_artifacts.is_empty());
}

#[test]
fn source_failure_is_not_reclassified_as_garbage_or_missing() {
    struct BrokenSource;

    impl ReachabilitySource for BrokenSource {
        fn read_database_manifest(
            &mut self,
            _reference: DatabaseManifestRootRef,
            _byte_budget: u64,
        ) -> ReachabilityResult<Option<Vec<u8>>> {
            Err(ReachabilityError::source_failure(
                ReachableNodeKind::DatabaseManifest,
                "permission denied",
            ))
        }

        fn read_catalog(
            &mut self,
            _reference: CatalogRef,
            _byte_budget: u64,
        ) -> ReachabilityResult<Option<Vec<u8>>> {
            unreachable!()
        }

        fn read_table_manifest(
            &mut self,
            _reference: TableManifestRef,
            _byte_budget: u64,
        ) -> ReachabilityResult<Option<Vec<u8>>> {
            unreachable!()
        }

        fn inspect_artifact(
            &mut self,
            _reference: ArtifactRef,
            _allowance: radixdb_storage::v6::ReachabilityAllowance,
        ) -> ReachabilityResult<ArtifactInspection> {
            unreachable!()
        }
    }

    let fixture = fixture();
    assert!(matches!(
        validate_control_generation(fixture.control, &mut BrokenSource),
        Err(ReachabilityError::SourceFailure {
            node: ReachableNodeKind::DatabaseManifest,
            ..
        })
    ));
}
