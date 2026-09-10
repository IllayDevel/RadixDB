use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::{
    decode_index_artifact_layout, encode_database_manifest, encode_table_manifest,
    lookup_exact_index, read_data_row_ids, scan_ordered_index, write_artifact_pair,
    AcceleratorBuildSpec, ArtifactId, ArtifactPairBuildRequest, CatalogGeneration, CatalogId,
    CatalogRef, ColumnBuildPolicy, DataArtifactHeader, DataColumnSpec, DataPhysicalCodec,
    DataValueEncoding, DatabaseGeneration, DatabaseId, DatabaseManifest, ExactPageBuildLimits,
    FanoutBuildLimits, IndexKeyColumn, IndexNullsOrder, IndexPageCodec, IndexScanDirection,
    IndexSortDirection, ManifestGeneration, ManifestId, ManifestKind, ManifestRef,
    OrderedIndexBound, OrderedIndexKey, OrderedPageBuildLimits, SegmentDescriptor, SegmentId,
    SegmentKind, SourceRow, TableManifest, TableManifestRef, WalGeneration, WalReplayFloor,
};
#[cfg(feature = "test-hooks")]
use radixdb_storage::v6::{
    publication_diagnostics, reset_publication_diagnostics, PublicationDiagnostics,
};
use serde_json::{json, Value as JsonValue};

const ROWS_PER_GENERATION: u64 = 24_000;
const RUNS: usize = 5;
const MANIFEST_FOOTER_BYTES: usize = 48;

const CHECKPOINT_1_BYTE_CEILING: u64 = 1_715_303;
const CHECKPOINT_2_BYTE_CEILING: u64 = 1_714_564;
const COMPACTION_BYTE_CEILING: u64 = 3_412_869;

const CHECKPOINT_1_TIME_CEILING: Duration = Duration::from_micros(43_036);
const CHECKPOINT_2_TIME_CEILING: Duration = Duration::from_micros(44_702);
const COMPACTION_TIME_CEILING: Duration = Duration::from_micros(52_618);

const CHECKPOINT_1_BASELINE: Duration = Duration::from_micros(45_301);
const CHECKPOINT_2_BASELINE: Duration = Duration::from_micros(47_055);
const COMPACTION_BASELINE: Duration = Duration::from_micros(55_387);

#[derive(Debug)]
struct PhaseObservation {
    name: &'static str,
    elapsed: Duration,
    pair_write_elapsed: Duration,
    artifact_sync_elapsed: Duration,
    manifest_elapsed: Duration,
    directory_sync_elapsed: Duration,
    data_bytes: u64,
    index_bytes: u64,
    table_manifest_bytes: u64,
    database_manifest_bytes: u64,
    diagnostics: Option<JsonValue>,
}

impl PhaseObservation {
    fn combined_bytes(&self) -> u64 {
        self.data_bytes
            + self.index_bytes
            + self.table_manifest_bytes
            + self.database_manifest_bytes
    }

    fn json(&self) -> JsonValue {
        json!({
            "name": self.name,
            "elapsed_nanos": self.elapsed.as_nanos(),
            "timing": {
                "pair_write_nanos": self.pair_write_elapsed.as_nanos(),
                "artifact_sync_nanos": self.artifact_sync_elapsed.as_nanos(),
                "manifest_encode_write_sync_nanos": self.manifest_elapsed.as_nanos(),
                "directory_sync_nanos": self.directory_sync_elapsed.as_nanos(),
            },
            "bytes": {
                "data": self.data_bytes,
                "index": self.index_bytes,
                "table_manifest": self.table_manifest_bytes,
                "database_manifest_share": self.database_manifest_bytes,
                "combined": self.combined_bytes(),
            },
            "diagnostics": self.diagnostics,
        })
    }
}

fn raw(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes(raw(marker)).expect("fixture object ID")
}

fn rows(first_id: u64, count: u64) -> Vec<SourceRow> {
    (first_id..first_id + count)
        .map(|id| {
            let integer_id = i64::try_from(id).expect("fixture ID fits i64");
            SourceRow::new(
                id,
                vec![
                    Value::integer(integer_id),
                    Value::integer(integer_id % 256),
                    Value::integer(integer_id),
                    Value::text(format!("external-{integer_id:08}")),
                    Value::text(format!("payload-class-{:04}", integer_id % 1024)),
                ],
            )
        })
        .collect()
}

fn request(
    staging: &Path,
    generation: u64,
    marker: u8,
    row_count: u64,
) -> ArtifactPairBuildRequest {
    let database_id = DatabaseId::from_bytes(raw(0x10)).expect("fixture database ID");
    let columns = [
        (0x21, DataType::Integer),
        (0x22, DataType::Integer),
        (0x23, DataType::Integer),
        (0x24, DataType::Text),
        (0x25, DataType::Text),
    ]
    .into_iter()
    .map(|(id, data_type)| {
        DataColumnSpec::new(
            object_id(id),
            CatalogDataType::scalar(data_type).expect("fixture scalar type"),
            false,
        )
    })
    .collect::<Vec<_>>();
    let ordered = AcceleratorBuildSpec::ordered(
        object_id(0x31),
        false,
        false,
        [0x71; 32],
        vec![
            IndexKeyColumn::new(
                object_id(0x22),
                DataType::Integer,
                IndexSortDirection::Ascending,
                IndexNullsOrder::Last,
            ),
            IndexKeyColumn::new(
                object_id(0x23),
                DataType::Integer,
                IndexSortDirection::Ascending,
                IndexNullsOrder::Last,
            ),
        ],
        IndexPageCodec::Lz4,
        OrderedPageBuildLimits::default(),
    )
    .expect("fixture ordered accelerator");
    let exact = AcceleratorBuildSpec::exact(
        object_id(0x32),
        true,
        true,
        [0x72; 32],
        vec![IndexKeyColumn::new(
            object_id(0x24),
            DataType::Text,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
        IndexPageCodec::Lz4,
        ExactPageBuildLimits::default(),
    )
    .expect("fixture exact accelerator");
    let generation = DatabaseGeneration::new(generation).expect("fixture generation");
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes(raw(marker)).expect("fixture data artifact ID"),
        database_id,
        object_id(0x20),
        SegmentId::from_bytes(raw(marker.wrapping_add(1))).expect("fixture segment ID"),
        generation,
        CatalogGeneration::new(13).expect("fixture catalog generation"),
        1,
        row_count,
        row_count,
        columns.len() as u32,
        1,
        SegmentKind::Rows,
        123_456,
    )
    .expect("fixture data header");
    ArtifactPairBuildRequest::new(
        header,
        columns,
        vec![
            ColumnBuildPolicy::new(DataValueEncoding::Plain, DataPhysicalCodec::Lz4, None),
            ColumnBuildPolicy::new(DataValueEncoding::Plain, DataPhysicalCodec::Lz4, None),
            ColumnBuildPolicy::new(DataValueEncoding::Plain, DataPhysicalCodec::Lz4, None),
            ColumnBuildPolicy::new(DataValueEncoding::Plain, DataPhysicalCodec::Lz4, None),
            ColumnBuildPolicy::new(DataValueEncoding::Dictionary, DataPhysicalCodec::Lz4, None),
        ],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes(raw(marker.wrapping_add(2))).expect("fixture index artifact ID"),
        vec![ordered, exact],
        FanoutBuildLimits::default(),
        staging,
    )
    .expect("fixture publication request")
}

fn measure_phase(
    root: &Path,
    name: &'static str,
    generation: u64,
    marker: u8,
    source_rows: Vec<SourceRow>,
) -> PhaseObservation {
    let request = request(root, generation, marker, source_rows.len() as u64);
    let data_path = root.join(format!("{name}.data"));
    let index_path = root.join(format!("{name}.idx"));
    let table_manifest_path = root.join(format!("{name}.table-manifest"));
    let database_manifest_path = root.join(format!("{name}.database-manifest"));
    let mut data = empty_file(&data_path);
    let mut index = empty_file(&index_path);

    #[cfg(feature = "test-hooks")]
    reset_publication_diagnostics();
    let started = Instant::now();
    let pair_write_started = Instant::now();
    let written = write_artifact_pair(
        &request,
        source_rows.into_iter().map(Ok),
        &mut data,
        &mut index,
    )
    .expect("candidate artifact pair");
    let pair_write_elapsed = pair_write_started.elapsed();
    let artifact_sync_started = Instant::now();
    data.sync_all().expect("sync candidate data");
    index.sync_all().expect("sync candidate index");
    let artifact_sync_elapsed = artifact_sync_started.elapsed();

    let manifest_started = Instant::now();
    let table_manifest = TableManifest::new(
        request.data_header().database_id(),
        request.data_header().table_id(),
        ManifestId::from_bytes(raw(marker.wrapping_add(3))).expect("fixture table manifest ID"),
        ManifestGeneration::new(generation).expect("fixture manifest generation"),
        request.data_header().catalog_generation(),
        request.data_header().row_count(),
        2,
        vec![SegmentDescriptor::new(
            request.data_header().segment_id(),
            SegmentKind::Rows,
            request.data_header().min_transaction_id(),
            request.data_header().max_transaction_id(),
            request.data_header().row_count(),
            1,
            request.data_header().row_count(),
            written.data_reference(),
            Some(written.index_reference()),
        )
        .expect("fixture segment descriptor")],
        123_456,
    )
    .expect("fixture table manifest");
    let table_manifest_bytes =
        encode_table_manifest(&table_manifest).expect("encode table manifest");
    write_synced(&table_manifest_path, &table_manifest_bytes);
    let table_reference = ManifestRef::new(
        table_manifest.manifest_id(),
        ManifestKind::Table,
        table_manifest.generation(),
        table_manifest_bytes.len() as u64,
        body_sha(&table_manifest_bytes),
    )
    .expect("fixture table manifest reference");
    let database_manifest = DatabaseManifest::new(
        request.data_header().database_id(),
        ManifestId::from_bytes(raw(marker.wrapping_add(4))).expect("fixture database manifest ID"),
        DatabaseGeneration::new(generation).expect("fixture database generation"),
        CatalogRef::new(
            CatalogId::from_bytes(raw(0x11)).expect("fixture catalog ID"),
            request.data_header().catalog_generation(),
            304,
            [0x73; 32],
        )
        .expect("fixture catalog reference"),
        WalReplayFloor::new(
            WalGeneration::new(1).expect("fixture WAL generation"),
            row_count_lsn(request.data_header().row_count()),
        ),
        request.data_header().max_transaction_id(),
        vec![
            TableManifestRef::new(request.data_header().table_id(), table_reference)
                .expect("fixture table reference"),
        ],
        123_456,
    )
    .expect("fixture database manifest");
    let database_manifest_bytes =
        encode_database_manifest(&database_manifest).expect("encode database manifest");
    write_synced(&database_manifest_path, &database_manifest_bytes);
    let manifest_elapsed = manifest_started.elapsed();
    let directory_sync_started = Instant::now();
    File::open(root)
        .expect("open candidate root")
        .sync_all()
        .expect("sync candidate root");
    let directory_sync_elapsed = directory_sync_started.elapsed();
    let elapsed = started.elapsed();
    #[cfg(feature = "test-hooks")]
    let diagnostics = publication_diagnostics();

    verify_readback(written, &data_path, &index_path);
    #[cfg(feature = "test-hooks")]
    assert_single_pass(&request, diagnostics);

    PhaseObservation {
        name,
        elapsed,
        pair_write_elapsed,
        artifact_sync_elapsed,
        manifest_elapsed,
        directory_sync_elapsed,
        data_bytes: std::fs::metadata(data_path).expect("data metadata").len(),
        index_bytes: std::fs::metadata(index_path).expect("index metadata").len(),
        table_manifest_bytes: table_manifest_bytes.len() as u64,
        database_manifest_bytes: database_manifest_bytes.len() as u64,
        diagnostics: diagnostics_snapshot(),
    }
}

fn empty_file(path: &Path) -> File {
    OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
        .expect("create candidate output")
}

fn write_synced(path: &Path, bytes: &[u8]) {
    let mut file = empty_file(path);
    file.write_all(bytes).expect("write candidate metadata");
    file.sync_all().expect("sync candidate metadata");
}

fn body_sha(bytes: &[u8]) -> [u8; 32] {
    bytes[bytes.len() - MANIFEST_FOOTER_BYTES + 16..]
        .try_into()
        .expect("manifest footer SHA")
}

fn row_count_lsn(row_count: u64) -> u64 {
    row_count.saturating_mul(16)
}

fn verify_readback(
    written: radixdb_storage::v6::WrittenArtifactPair,
    data_path: &Path,
    index_path: &Path,
) {
    let data = std::fs::read(data_path).expect("read candidate data after publication");
    let index = std::fs::read(index_path).expect("read candidate index after publication");
    let index_layout =
        decode_index_artifact_layout(&index, written.index_reference(), written.data_layout())
            .expect("open candidate index");
    let row_ids = read_data_row_ids(&data, written.data_layout(), 0)
        .expect("read candidate row IDs after publication");
    let selected_ordinal = row_ids.len() / 2;
    let selected_id = row_ids[selected_ordinal];
    let selected_integer = i64::try_from(selected_id).expect("selected ID fits i64");
    assert_eq!(
        lookup_exact_index(
            &index,
            &index_layout,
            written.data_layout(),
            object_id(0x32),
            &[Value::text(format!("external-{selected_integer:08}"))],
        )
        .expect("exact readback"),
        Some(vec![selected_ordinal as u64])
    );
    let ordered = index_layout
        .accelerators()
        .iter()
        .find(|accelerator| accelerator.logical_index_id() == object_id(0x31))
        .expect("ordered accelerator");
    let selected_values = [
        Value::integer(selected_integer % 256),
        Value::integer(selected_integer),
    ];
    let key = OrderedIndexKey::from_values(
        written.data_layout(),
        ordered.key_columns(),
        &selected_values,
    )
    .expect("ordered readback key");
    let bound = OrderedIndexBound::new(key, true);
    assert_eq!(
        scan_ordered_index(
            &index,
            &index_layout,
            written.data_layout(),
            object_id(0x31),
            Some(&bound),
            Some(&bound),
            IndexScanDirection::Forward,
            0,
            usize::MAX,
        )
        .expect("ordered readback"),
        vec![selected_ordinal as u64]
    );
    assert_eq!(
        data.len() as u64,
        written.data_reference().byte_length(),
        "verification reads the exact published data artifact"
    );
}

#[cfg(feature = "test-hooks")]
fn assert_single_pass(request: &ArtifactPairBuildRequest, diagnostics: PublicationDiagnostics) {
    let rows = request.data_header().row_count();
    let accelerators = request.accelerators().len() as u64;
    assert_eq!(diagnostics.publication_invocations, 1);
    assert_eq!(diagnostics.rebuild_invocations, 0);
    assert_eq!(diagnostics.source_stream_passes, 1);
    assert_eq!(diagnostics.source_rows, rows);
    assert_eq!(diagnostics.data_encode_passes, 1);
    assert_eq!(diagnostics.data_row_groups_encoded, 1);
    assert_eq!(diagnostics.index_planning_passes, accelerators);
    assert_eq!(diagnostics.index_encoding_passes, accelerators);
    assert_eq!(diagnostics.postings_planned, rows * accelerators);
    assert_eq!(diagnostics.postings_encoded, rows * accelerators);
    assert_eq!(diagnostics.data_construction_read_calls, 0);
    assert_eq!(diagnostics.data_construction_read_bytes, 0);
    assert_eq!(diagnostics.index_construction_read_calls, 0);
    assert_eq!(diagnostics.index_construction_read_bytes, 0);
    assert!(diagnostics.data_identity_read_calls > 0);
    assert!(diagnostics.index_identity_read_calls > 0);
    assert!(diagnostics.data_write_calls > 0);
    assert!(diagnostics.index_write_calls > 0);
    if diagnostics.sort_run_write_calls == 0 {
        assert_eq!(diagnostics.sort_run_write_bytes, 0);
        assert_eq!(diagnostics.sort_run_read_calls, 0);
        assert_eq!(diagnostics.sort_run_read_bytes, 0);
        assert_eq!(diagnostics.sort_merge_passes, 0);
        assert_eq!(diagnostics.sort_merge_input_runs, 0);
    } else if diagnostics.sort_merge_passes == 0 {
        assert_eq!(diagnostics.sort_merge_input_runs, 0);
        assert_eq!(
            diagnostics.sort_run_read_bytes,
            diagnostics.sort_run_write_bytes * 2,
            "an unconsolidated run set has one planning and one encoding read"
        );
    } else {
        assert!(
            diagnostics.sort_merge_input_runs > diagnostics.sort_merge_passes,
            "each intermediate pass must consume one or more bounded run groups"
        );
        assert!(
            diagnostics.sort_run_read_bytes > diagnostics.sort_run_write_bytes,
            "multi-pass I/O must include every intermediate input plus two final reads"
        );
    }
}

#[cfg(feature = "test-hooks")]
fn diagnostics_snapshot() -> Option<JsonValue> {
    let diagnostics = publication_diagnostics();
    Some(json!({
        "publication_invocations": diagnostics.publication_invocations,
        "rebuild_invocations": diagnostics.rebuild_invocations,
        "source_stream_passes": diagnostics.source_stream_passes,
        "source_rows": diagnostics.source_rows,
        "data_encode_passes": diagnostics.data_encode_passes,
        "data_row_groups_encoded": diagnostics.data_row_groups_encoded,
        "index_planning_passes": diagnostics.index_planning_passes,
        "index_encoding_passes": diagnostics.index_encoding_passes,
        "postings_planned": diagnostics.postings_planned,
        "postings_encoded": diagnostics.postings_encoded,
        "data_construction_read_calls": diagnostics.data_construction_read_calls,
        "data_construction_read_bytes": diagnostics.data_construction_read_bytes,
        "data_identity_read_calls": diagnostics.data_identity_read_calls,
        "data_identity_read_bytes": diagnostics.data_identity_read_bytes,
        "data_write_calls": diagnostics.data_write_calls,
        "data_write_bytes": diagnostics.data_write_bytes,
        "index_construction_read_calls": diagnostics.index_construction_read_calls,
        "index_construction_read_bytes": diagnostics.index_construction_read_bytes,
        "index_identity_read_calls": diagnostics.index_identity_read_calls,
        "index_identity_read_bytes": diagnostics.index_identity_read_bytes,
        "index_write_calls": diagnostics.index_write_calls,
        "index_write_bytes": diagnostics.index_write_bytes,
        "sort_run_read_calls": diagnostics.sort_run_read_calls,
        "sort_run_read_bytes": diagnostics.sort_run_read_bytes,
        "sort_run_write_calls": diagnostics.sort_run_write_calls,
        "sort_run_write_bytes": diagnostics.sort_run_write_bytes,
        "sort_merge_passes": diagnostics.sort_merge_passes,
        "sort_merge_input_runs": diagnostics.sort_merge_input_runs,
    }))
}

#[cfg(not(feature = "test-hooks"))]
fn diagnostics_snapshot() -> Option<JsonValue> {
    None
}

fn median(observations: &[Duration]) -> Duration {
    let mut observations = observations.to_vec();
    observations.sort_unstable();
    observations[observations.len() / 2]
}

fn assert_timing(name: &str, observations: &[Duration], baseline: Duration, ceiling: Duration) {
    let measured_median = median(observations);
    let faster_observations = observations
        .iter()
        .filter(|observation| **observation < baseline)
        .count();
    assert!(
        measured_median <= ceiling,
        "{name} median {measured_median:?} exceeds frozen ceiling {ceiling:?}"
    );
    assert!(
        faster_observations >= 4,
        "{name} has only {faster_observations}/{} observations faster than frozen baseline {baseline:?}",
        observations.len()
    );
}

#[test]
#[ignore = "CA-50.10 release-only five-run single-pass evidence"]
#[allow(clippy::assertions_on_constants)]
fn artifact_single_pass_acceptance_gate() {
    assert!(!cfg!(debug_assertions), "timing gate requires --release");
    assert!(
        !cfg!(feature = "test-hooks"),
        "timing gate must use the production counter-free build"
    );
    let suite_root = tempfile::tempdir().expect("candidate suite root");
    let mut reports = Vec::with_capacity(RUNS);

    for run in 0..RUNS {
        let root = suite_root.path().join(format!("run-{}", run + 1));
        std::fs::create_dir(&root).expect("create candidate run root");
        let checkpoint_1 = measure_phase(
            &root,
            "checkpoint-1",
            17,
            0x40_u8.wrapping_add(run as u8 * 12),
            rows(1, ROWS_PER_GENERATION),
        );
        let checkpoint_2 = measure_phase(
            &root,
            "checkpoint-2",
            18,
            0x44_u8.wrapping_add(run as u8 * 12),
            rows(ROWS_PER_GENERATION + 1, ROWS_PER_GENERATION),
        );
        let compaction = measure_phase(
            &root,
            "compaction",
            19,
            0x48_u8.wrapping_add(run as u8 * 12),
            rows(1, ROWS_PER_GENERATION * 2),
        );

        assert!(checkpoint_1.combined_bytes() <= CHECKPOINT_1_BYTE_CEILING);
        assert!(checkpoint_2.combined_bytes() <= CHECKPOINT_2_BYTE_CEILING);
        assert!(compaction.combined_bytes() <= COMPACTION_BYTE_CEILING);
        reports.push([checkpoint_1, checkpoint_2, compaction]);
    }

    let summary = [0, 1, 2].map(|phase| {
        let durations = reports
            .iter()
            .map(|run| run[phase].elapsed)
            .collect::<Vec<_>>();
        json!({
            "name": reports[0][phase].name,
            "median_nanos": median(&durations).as_nanos(),
            "combined_bytes": reports[0][phase].combined_bytes(),
        })
    });
    let report = json!({
        "schema": "radixdb.ca-50.10.single-pass.v1",
        "source_revision": std::env::var("RADIXDB_CA50_SOURCE_REVISION")
            .unwrap_or_else(|_| "unspecified".into()),
        "build_profile": "release",
        "available_parallelism": std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
        "cache_state": "fresh unique run roots; process and OS cache intentionally warm",
        "runs": reports
            .iter()
            .enumerate()
            .map(|(run, phases)| json!({
                "run": run + 1,
                "phases": phases.iter().map(PhaseObservation::json).collect::<Vec<_>>(),
            }))
            .collect::<Vec<_>>(),
        "summary": summary,
    });
    if let Ok(output) = std::env::var("RADIXDB_CA50_OUTPUT") {
        std::fs::write(
            &output,
            serde_json::to_vec_pretty(&report).expect("encode report"),
        )
        .expect("write report");
        println!("CA50_OUTPUT={output}");
    } else {
        println!(
            "CA50_REPORT={}",
            serde_json::to_string(&report).expect("encode report")
        );
    }

    for (phase, baseline, ceiling) in [
        (0, CHECKPOINT_1_BASELINE, CHECKPOINT_1_TIME_CEILING),
        (1, CHECKPOINT_2_BASELINE, CHECKPOINT_2_TIME_CEILING),
        (2, COMPACTION_BASELINE, COMPACTION_TIME_CEILING),
    ] {
        let observations = reports
            .iter()
            .map(|run| run[phase].elapsed)
            .collect::<Vec<_>>();
        assert_timing(reports[0][phase].name, &observations, baseline, ceiling);
    }
}

#[cfg(feature = "test-hooks")]
#[test]
fn artifact_single_pass_structural_gate() {
    let root = tempfile::tempdir().expect("structural gate root");
    let observation = measure_phase(
        root.path(),
        "structural",
        17,
        0x40,
        rows(1, ROWS_PER_GENERATION * 2),
    );
    assert!(observation.combined_bytes() <= COMPACTION_BYTE_CEILING);
    assert!(observation.diagnostics.is_some());
}
