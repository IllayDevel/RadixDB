use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::mvcc::FileLock;
use radixdb_storage::v6::{
    build_artifact_pair, decode_index_artifact_layout, encode_control_slot,
    encode_database_manifest, encode_table_manifest, lookup_exact_index, stage_index_replacement,
    visit_exact_index_or_scan, AcceleratorBuildSpec, ArtifactId, ArtifactPairBuildRequest,
    ArtifactRef, CatalogGeneration, CatalogId, CatalogRef, CatalogRootRef, ColumnBuildPolicy,
    ControlRecord, ControlSlotIndex, DataArtifactHeader, DataBlockKind, DataColumnSpec,
    DataPhysicalCodec, DatabaseGeneration, DatabaseId, DatabaseManifest, DatabaseManifestRootRef,
    ExactLookupDefinition, ExactPageBuildLimits, FanoutBuildLimits, FormatError, FormatResult,
    IndexAccessState, IndexKeyColumn, IndexNullsOrder, IndexPageCodec, IndexRebuildRequest,
    IndexReplacementBuildRequest, IndexReplacementPublicationSink, ManifestGeneration, ManifestId,
    ManifestKind, ManifestRef, OrderedPageBuildLimits, PhysicalGenerationPublisher,
    PhysicalGenerationSnapshot, RebuildRequestSink, RebuildRequestStatus, SegmentDescriptor,
    SegmentId, SegmentKind, SourceRow, StagedIndexReplacement, TableManifest, TableManifestRef,
    UnavailableIndexReason, WalGeneration, WalReplayFloor, WriterInstanceId, CONTROL_RECORD_BYTES,
};
#[cfg(feature = "test-hooks")]
use radixdb_storage::v6::{publication_diagnostics, reset_publication_diagnostics};

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes(raw(marker)).unwrap()
}

fn footer_sha(bytes: &[u8]) -> [u8; 32] {
    bytes[bytes.len() - 32..].try_into().unwrap()
}

fn rows() -> impl Iterator<Item = FormatResult<SourceRow>> {
    [
        (101, 5, "alpha"),
        (103, 1, "beta"),
        (107, 3, "alpha"),
        (109, 2, "gamma"),
        (113, 4, "beta"),
        (127, 6, "alpha"),
    ]
    .into_iter()
    .map(|(row_id, integer, text)| {
        Ok(SourceRow::new(
            row_id,
            vec![Value::integer(integer), Value::text(text)],
        ))
    })
}

fn accelerator_specs() -> Vec<AcceleratorBuildSpec> {
    vec![
        AcceleratorBuildSpec::exact(
            object_id(0x41),
            false,
            false,
            [0x51; 32],
            vec![IndexKeyColumn::new(
                object_id(0x22),
                DataType::Text,
                radixdb_storage::v6::IndexSortDirection::Ascending,
                IndexNullsOrder::Last,
            )],
            IndexPageCodec::Lz4,
            ExactPageBuildLimits::new(2, 1024 * 1024).unwrap(),
        )
        .unwrap(),
        AcceleratorBuildSpec::ordered(
            object_id(0x42),
            false,
            false,
            [0x52; 32],
            vec![IndexKeyColumn::new(
                object_id(0x21),
                DataType::Integer,
                radixdb_storage::v6::IndexSortDirection::Ascending,
                IndexNullsOrder::Last,
            )],
            IndexPageCodec::Lz4,
            OrderedPageBuildLimits::new(2, 1024 * 1024).unwrap(),
        )
        .unwrap(),
    ]
}

fn pair_request(staging: &std::path::Path) -> ArtifactPairBuildRequest {
    let columns = vec![
        DataColumnSpec::new(
            object_id(0x21),
            CatalogDataType::scalar(DataType::Integer).unwrap(),
            false,
        ),
        DataColumnSpec::new(
            object_id(0x22),
            CatalogDataType::scalar(DataType::Text).unwrap(),
            false,
        ),
    ];
    ArtifactPairBuildRequest::new(
        DataArtifactHeader::new(
            ArtifactId::from_bytes(raw(0x31)).unwrap(),
            DatabaseId::from_bytes(raw(0x32)).unwrap(),
            object_id(0x33),
            SegmentId::from_bytes(raw(0x34)).unwrap(),
            DatabaseGeneration::new(17).unwrap(),
            CatalogGeneration::new(13).unwrap(),
            501,
            509,
            6,
            2,
            3,
            SegmentKind::Rows,
            123_456,
        )
        .unwrap(),
        columns,
        vec![ColumnBuildPolicy::default(), ColumnBuildPolicy::default()],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes(raw(0x35)).unwrap(),
        accelerator_specs(),
        FanoutBuildLimits::new(2, 2, 1024 * 1024, 128).unwrap(),
        staging,
    )
    .unwrap()
}

struct CaptureRebuild(Option<IndexRebuildRequest>);

impl RebuildRequestSink for CaptureRebuild {
    fn request_rebuild(&mut self, request: IndexRebuildRequest) -> RebuildRequestStatus {
        self.0 = Some(request);
        RebuildRequestStatus::Enqueued
    }
}

fn capture_rebuild(
    data_bytes: &[u8],
    data: &radixdb_storage::v6::DataArtifactLayout,
) -> IndexRebuildRequest {
    let definition = ExactLookupDefinition::new(
        object_id(0x41),
        false,
        false,
        [0x51; 32],
        vec![IndexKeyColumn::new(
            object_id(0x22),
            DataType::Text,
            radixdb_storage::v6::IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
    )
    .unwrap();
    let mut capture = CaptureRebuild(None);
    visit_exact_index_or_scan(
        data_bytes,
        data,
        &definition,
        &[Value::text("alpha")],
        IndexAccessState::Unavailable(UnavailableIndexReason::Missing),
        &mut capture,
        &mut |_| Ok(()),
    )
    .unwrap();
    capture.0.unwrap()
}

struct Fixture {
    root: tempfile::TempDir,
    data_bytes: Vec<u8>,
    data_layout: radixdb_storage::v6::DataArtifactLayout,
    old_index: ArtifactRef,
    rebuild: IndexRebuildRequest,
    publisher: PhysicalGenerationPublisher,
}

fn fixture() -> Fixture {
    let pair_staging = tempfile::tempdir().unwrap();
    let pair = build_artifact_pair(&pair_request(pair_staging.path()), rows()).unwrap();
    let database_id = pair.data_layout().header().database_id();
    let table_id = pair.data_layout().header().table_id();
    let segment_id = pair.data_layout().header().segment_id();
    let table_manifest_id = ManifestId::from_bytes(raw(0x61)).unwrap();
    let table_manifest = TableManifest::new(
        database_id,
        table_id,
        table_manifest_id,
        ManifestGeneration::new(17).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        127,
        2,
        vec![SegmentDescriptor::new(
            segment_id,
            SegmentKind::Rows,
            501,
            509,
            6,
            101,
            127,
            pair.data_reference(),
            Some(pair.index_reference()),
        )
        .unwrap()],
        123_456,
    )
    .unwrap();
    let table_bytes = encode_table_manifest(&table_manifest).unwrap();
    let table_reference = TableManifestRef::new(
        table_id,
        ManifestRef::new(
            table_manifest_id,
            ManifestKind::Table,
            ManifestGeneration::new(17).unwrap(),
            table_bytes.len() as u64,
            footer_sha(&table_bytes),
        )
        .unwrap(),
    )
    .unwrap();
    let catalog = CatalogRef::new(
        CatalogId::from_bytes(raw(0x62)).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        304,
        [0x63; 32],
    )
    .unwrap();
    let database_manifest_id = ManifestId::from_bytes(raw(0x64)).unwrap();
    let wal_floor = WalReplayFloor::new(WalGeneration::new(3).unwrap(), 900);
    let database_manifest = DatabaseManifest::new(
        database_id,
        database_manifest_id,
        DatabaseGeneration::new(17).unwrap(),
        catalog,
        wal_floor,
        10_000,
        vec![table_reference],
        123_456,
    )
    .unwrap();
    let database_bytes = encode_database_manifest(&database_manifest).unwrap();
    let database_reference = DatabaseManifestRootRef::new(
        database_manifest_id,
        ManifestGeneration::new(17).unwrap(),
        footer_sha(&database_bytes),
    );
    let control = ControlRecord::new(
        ControlSlotIndex::Zero,
        DatabaseGeneration::new(17).unwrap(),
        database_id,
        database_reference,
        CatalogRootRef::new(catalog.id(), catalog.generation(), *catalog.body_sha256()),
        wal_floor,
        123_456,
        WriterInstanceId::from_bytes(raw(0x65)).unwrap(),
    )
    .unwrap();
    let snapshot =
        PhysicalGenerationSnapshot::new(control, database_manifest, vec![table_manifest]).unwrap();
    let rebuild = capture_rebuild(pair.data_bytes(), pair.data_layout());
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("staging")).unwrap();
    std::fs::write(root.path().join("CONTROL.0"), encode_control_slot(control)).unwrap();
    let publisher = PhysicalGenerationPublisher::open(root.path(), snapshot).unwrap();
    Fixture {
        root,
        data_bytes: pair.data_bytes().to_vec(),
        data_layout: pair.data_layout().clone(),
        old_index: pair.index_reference(),
        rebuild,
        publisher,
    }
}

fn stage(fixture: &Fixture, staging: &std::path::Path, marker: u8) -> StagedIndexReplacement {
    let request = IndexReplacementBuildRequest::new(
        fixture.rebuild.clone(),
        ArtifactId::from_bytes(raw(marker)).unwrap(),
        DatabaseGeneration::new(18).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        accelerator_specs(),
        FanoutBuildLimits::new(2, 2, 1024 * 1024, 128).unwrap(),
        staging,
    )
    .unwrap();
    stage_index_replacement(
        &request,
        fixture.data_bytes.as_slice(),
        &fixture.data_layout,
    )
    .unwrap()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishStep {
    Index,
    TableManifest,
    DatabaseManifest,
    Control,
}

struct RecordingSink {
    root: std::path::PathBuf,
    steps: Vec<PublishStep>,
    fail_at: Option<PublishStep>,
}

impl RecordingSink {
    fn new(root: &std::path::Path) -> Self {
        Self {
            root: root.to_path_buf(),
            steps: Vec::new(),
            fail_at: None,
        }
    }

    fn failing(root: &std::path::Path, step: PublishStep) -> Self {
        Self {
            fail_at: Some(step),
            ..Self::new(root)
        }
    }

    fn record(&mut self, step: PublishStep) -> FormatResult<()> {
        self.steps.push(step);
        if self.fail_at == Some(step) {
            return Err(FormatError::ArtifactIo {
                operation: "injected index replacement publication failure",
                kind: std::io::ErrorKind::Other,
            });
        }
        Ok(())
    }
}

impl IndexReplacementPublicationSink for RecordingSink {
    fn database_root(&self) -> &std::path::Path {
        &self.root
    }

    fn publish_staged_index_and_sync(
        &mut self,
        replacement: &StagedIndexReplacement,
    ) -> FormatResult<()> {
        assert!(replacement.path().is_file());
        self.record(PublishStep::Index)
    }

    fn publish_table_manifest_and_sync(
        &mut self,
        reference: TableManifestRef,
        bytes: &[u8],
    ) -> FormatResult<()> {
        assert_eq!(reference.manifest().byte_length(), bytes.len() as u64);
        self.record(PublishStep::TableManifest)
    }

    fn publish_database_manifest_and_sync(
        &mut self,
        _reference: DatabaseManifestRootRef,
        bytes: &[u8],
    ) -> FormatResult<()> {
        assert!(!bytes.is_empty());
        self.record(PublishStep::DatabaseManifest)
    }

    fn publish_control_and_sync(
        &mut self,
        slot: ControlSlotIndex,
        bytes: &[u8; CONTROL_RECORD_BYTES],
    ) -> FormatResult<()> {
        assert_eq!(slot, ControlSlotIndex::One);
        assert_eq!(bytes.len(), CONTROL_RECORD_BYTES);
        self.record(PublishStep::Control)
    }
}

#[test]
fn rebuild_reads_existing_data_and_stages_a_complete_replacement_pack() {
    let fixture = fixture();
    let staging = tempfile::tempdir().unwrap();
    let staged = stage(&fixture, staging.path(), 0x71);
    let bytes = std::fs::read(staged.path()).unwrap();
    assert_eq!(bytes.len() as u64, staged.reference().byte_length());
    let layout =
        decode_index_artifact_layout(&bytes, staged.reference(), &fixture.data_layout).unwrap();
    assert_eq!(layout.accelerators().len(), 2);
    assert_eq!(
        lookup_exact_index(
            &bytes,
            &layout,
            &fixture.data_layout,
            object_id(0x41),
            &[Value::text("alpha")],
        )
        .unwrap(),
        Some(vec![0, 2, 5])
    );
    assert_eq!(
        staged.written().source_data(),
        fixture.data_layout.reference()
    );
    assert_eq!(staged.reference().creation_generation().get(), 18);
    assert!(std::fs::read_dir(staging.path())
        .unwrap()
        .all(|entry| entry.unwrap().path() == staged.path()));
}

#[test]
fn corrupt_authoritative_key_block_aborts_rebuild_and_removes_partial_stage() {
    let fixture = fixture();
    let mut corrupt = fixture.data_bytes.clone();
    let key_block = fixture
        .data_layout
        .blocks()
        .iter()
        .find(|block| block.kind() == DataBlockKind::Column && block.column_ordinal() == 1)
        .unwrap();
    corrupt[key_block.offset() as usize] ^= 0x80;
    let staging = tempfile::tempdir().unwrap();
    let request = IndexReplacementBuildRequest::new(
        fixture.rebuild.clone(),
        ArtifactId::from_bytes(raw(0x76)).unwrap(),
        DatabaseGeneration::new(18).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        accelerator_specs(),
        FanoutBuildLimits::new(2, 2, 1024 * 1024, 128).unwrap(),
        staging.path(),
    )
    .unwrap();
    assert!(matches!(
        stage_index_replacement(&request, corrupt.as_slice(), &fixture.data_layout),
        Err(FormatError::DataArtifactChecksumMismatch { scope: "block" })
    ));
    assert!(std::fs::read_dir(staging.path()).unwrap().next().is_none());
}

#[test]
fn replacement_pack_must_contain_the_requested_catalog_definition() {
    let fixture = fixture();
    let staging = tempfile::tempdir().unwrap();
    let mut definitions = accelerator_specs();
    definitions.remove(0);
    assert!(matches!(
        IndexReplacementBuildRequest::new(
            fixture.rebuild,
            ArtifactId::from_bytes(raw(0x77)).unwrap(),
            DatabaseGeneration::new(18).unwrap(),
            CatalogGeneration::new(13).unwrap(),
            definitions,
            FanoutBuildLimits::default(),
            staging.path(),
        ),
        Err(FormatError::InvalidIndexArtifact {
            detail: "replacement pack omits the requested accelerator"
        })
    ));
}

#[test]
fn publication_orders_index_manifests_control_and_pins_old_reader_generation() {
    let fixture = fixture();
    let old_reader = fixture.publisher.pin().unwrap();
    let staging = tempfile::tempdir_in(fixture.root.path().join("staging")).unwrap();
    let staged = stage(&fixture, staging.path(), 0x72);
    let new_index = staged.reference();
    let prepared = old_reader
        .prepare_index_replacement(
            staged,
            ManifestId::from_bytes(raw(0x73)).unwrap(),
            ManifestId::from_bytes(raw(0x74)).unwrap(),
            WriterInstanceId::from_bytes(raw(0x75)).unwrap(),
            234_567,
        )
        .unwrap();
    assert_eq!(prepared.superseded_index(), Some(fixture.old_index));
    assert_eq!(prepared.control().slot(), ControlSlotIndex::One);
    assert_eq!(prepared.control().database_generation().get(), 18);

    let mut sink = RecordingSink::new(fixture.root.path());
    let new_reader = fixture
        .publisher
        .publish_index_replacement(prepared, &mut sink)
        .unwrap();
    assert_eq!(
        sink.steps,
        vec![
            PublishStep::Index,
            PublishStep::TableManifest,
            PublishStep::DatabaseManifest,
            PublishStep::Control,
        ]
    );
    assert_eq!(old_reader.database_generation().get(), 17);
    assert_eq!(new_reader.database_generation().get(), 18);
    assert_eq!(
        old_reader.index_artifact(fixture.rebuild.table_id(), fixture.rebuild.segment_id()),
        Some(fixture.old_index)
    );
    assert_eq!(
        new_reader.index_artifact(fixture.rebuild.table_id(), fixture.rebuild.segment_id()),
        Some(new_index)
    );
    assert!(old_reader.pinned_artifacts().contains(&fixture.old_index));
    assert!(!new_reader.pinned_artifacts().contains(&fixture.old_index));
    assert!(new_reader.pinned_artifacts().contains(&new_index));
    assert_eq!(
        old_reader
            .snapshot()
            .segment(fixture.rebuild.table_id(), fixture.rebuild.segment_id())
            .unwrap()
            .data_artifact(),
        new_reader
            .snapshot()
            .segment(fixture.rebuild.table_id(), fixture.rebuild.segment_id())
            .unwrap()
            .data_artifact()
    );
    assert_eq!(
        old_reader.snapshot().database_manifest().catalog(),
        new_reader.snapshot().database_manifest().catalog()
    );
    assert_eq!(
        old_reader.snapshot().database_manifest().wal_replay_floor(),
        new_reader.snapshot().database_manifest().wal_replay_floor()
    );
}

#[test]
fn index_replacement_rejects_a_sink_for_another_root_before_calls() {
    let fixture = fixture();
    let current = fixture.publisher.pin().unwrap();
    let staging = tempfile::tempdir_in(fixture.root.path().join("staging")).unwrap();
    let prepared = current
        .prepare_index_replacement(
            stage(&fixture, staging.path(), 0x76),
            ManifestId::from_bytes(raw(0x77)).unwrap(),
            ManifestId::from_bytes(raw(0x78)).unwrap(),
            WriterInstanceId::from_bytes(raw(0x79)).unwrap(),
            234_568,
        )
        .unwrap();
    let other = tempfile::tempdir().unwrap();
    let _other_lock = FileLock::acquire(other.path()).unwrap();
    let mut sink = RecordingSink::new(other.path());

    let error = fixture
        .publisher
        .publish_index_replacement(prepared, &mut sink)
        .unwrap_err();

    assert!(matches!(error, FormatError::InvalidFilesystemOwner { .. }));
    assert!(sink.steps.is_empty());
    assert_eq!(
        fixture.publisher.pin().unwrap().database_generation().get(),
        17
    );
}

#[test]
fn every_pre_control_failure_keeps_the_old_runtime_generation_authoritative() {
    for (ordinal, step) in [
        PublishStep::Index,
        PublishStep::TableManifest,
        PublishStep::DatabaseManifest,
        PublishStep::Control,
    ]
    .into_iter()
    .enumerate()
    {
        let fixture = fixture();
        let old_reader = fixture.publisher.pin().unwrap();
        let staging = tempfile::tempdir_in(fixture.root.path().join("staging")).unwrap();
        let prepared = old_reader
            .prepare_index_replacement(
                stage(&fixture, staging.path(), 0x80 + ordinal as u8),
                ManifestId::from_bytes(raw(0x90 + ordinal as u8)).unwrap(),
                ManifestId::from_bytes(raw(0xa0 + ordinal as u8)).unwrap(),
                WriterInstanceId::from_bytes(raw(0xb0 + ordinal as u8)).unwrap(),
                300_000 + ordinal as u64,
            )
            .unwrap();
        let mut sink = RecordingSink::failing(fixture.root.path(), step);
        assert!(fixture
            .publisher
            .publish_index_replacement(prepared, &mut sink)
            .is_err());
        let current = fixture.publisher.pin().unwrap();
        assert_eq!(current.database_generation().get(), 17);
        assert_eq!(
            current.index_artifact(fixture.rebuild.table_id(), fixture.rebuild.segment_id()),
            Some(fixture.old_index)
        );
    }
}

#[test]
fn stale_rebuild_is_rejected_before_any_publication_side_effect() {
    let fixture = fixture();
    let old_reader = fixture.publisher.pin().unwrap();
    let first_staging = tempfile::tempdir_in(fixture.root.path().join("staging")).unwrap();
    let second_staging = tempfile::tempdir_in(fixture.root.path().join("staging")).unwrap();
    let first = old_reader
        .prepare_index_replacement(
            stage(&fixture, first_staging.path(), 0xc1),
            ManifestId::from_bytes(raw(0xc2)).unwrap(),
            ManifestId::from_bytes(raw(0xc3)).unwrap(),
            WriterInstanceId::from_bytes(raw(0xc4)).unwrap(),
            400_000,
        )
        .unwrap();
    let stale = old_reader
        .prepare_index_replacement(
            stage(&fixture, second_staging.path(), 0xd1),
            ManifestId::from_bytes(raw(0xd2)).unwrap(),
            ManifestId::from_bytes(raw(0xd3)).unwrap(),
            WriterInstanceId::from_bytes(raw(0xd4)).unwrap(),
            400_001,
        )
        .unwrap();
    fixture
        .publisher
        .publish_index_replacement(first, &mut RecordingSink::new(fixture.root.path()))
        .unwrap();
    let mut untouched = RecordingSink::new(fixture.root.path());
    assert!(fixture
        .publisher
        .publish_index_replacement(stale, &mut untouched)
        .is_err());
    assert!(untouched.steps.is_empty());
}

#[cfg(feature = "test-hooks")]
#[test]
fn diagnostics_distinguish_rebuild_from_initial_single_pass_publication() {
    let fixture = fixture();
    let staging = tempfile::tempdir().unwrap();
    reset_publication_diagnostics();
    let staged = stage(&fixture, staging.path(), 0xe1);
    let counters = publication_diagnostics();
    assert_eq!(counters.publication_invocations, 0);
    assert_eq!(counters.rebuild_invocations, 1);
    assert_eq!(counters.source_stream_passes, 1);
    assert_eq!(counters.source_rows, 6);
    assert_eq!(counters.data_encode_passes, 0);
    assert_eq!(counters.data_write_bytes, 0);
    assert!(counters.index_construction_read_calls > 0);
    assert!(counters.index_construction_read_bytes < fixture.data_bytes.len() as u64);
    assert!(counters.index_write_bytes >= staged.reference().byte_length());
    reset_publication_diagnostics();
}
