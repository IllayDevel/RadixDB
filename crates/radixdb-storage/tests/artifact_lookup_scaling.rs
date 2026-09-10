use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::*;
use serde_json::json;
use std::cell::Cell;
use std::time::Instant;

fn oid(value: u128) -> ObjectId {
    ObjectId::from_user_bytes(value.to_le_bytes()).unwrap()
}

fn request(
    root: &std::path::Path,
    rows: u64,
    width: u32,
    group_rows: u32,
    page_entries: u64,
) -> ArtifactPairBuildRequest {
    let columns = (0..width)
        .map(|column| {
            DataColumnSpec::new(
                oid(1000 + column as u128),
                CatalogDataType::scalar(DataType::Integer).unwrap(),
                false,
            )
        })
        .collect::<Vec<_>>();
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes([0x31; 16]).unwrap(),
        DatabaseId::from_bytes([0x32; 16]).unwrap(),
        oid(33),
        SegmentId::from_bytes([0x34; 16]).unwrap(),
        DatabaseGeneration::new(17).unwrap(),
        CatalogGeneration::new(13).unwrap(),
        501,
        509,
        rows,
        width,
        rows.div_ceil(group_rows as u64) as u32,
        SegmentKind::Rows,
        123456,
    )
    .unwrap();
    let exact = AcceleratorBuildSpec::exact(
        oid(41),
        true,
        false,
        [0x51; 32],
        vec![IndexKeyColumn::new(
            columns[0].column_id(),
            DataType::Integer,
            IndexSortDirection::Ascending,
            IndexNullsOrder::Last,
        )],
        IndexPageCodec::Lz4,
        ExactPageBuildLimits::new(page_entries, 4 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    ArtifactPairBuildRequest::new(
        header,
        columns,
        vec![ColumnBuildPolicy::default(); width as usize],
        DataPhysicalCodec::Lz4,
        ArtifactId::from_bytes([0x35; 16]).unwrap(),
        vec![exact],
        FanoutBuildLimits::new(group_rows, 65536, 64 * 1024 * 1024, 4096).unwrap(),
        root,
    )
    .unwrap()
}

fn build(request: &ArtifactPairBuildRequest) -> BuiltArtifactPair {
    build_artifact_pair(
        request,
        (0..request.data_header().row_count()).map(|row| {
            Ok(SourceRow::new(
                row + 1,
                (0..request.columns().len())
                    .map(|column| Value::integer((row + column as u64) as i64))
                    .collect(),
            ))
        }),
    )
    .unwrap()
}

fn median(values: &mut [u64]) -> u64 {
    values.sort_unstable();
    values[values.len() / 2]
}

fn save(name: &str, values: serde_json::Value) {
    let root = std::path::PathBuf::from(std::env::var_os("REVIEW_EVIDENCE_DIR").unwrap());
    std::fs::write(root.join(name), serde_json::to_vec_pretty(&values).unwrap()).unwrap();
    eprintln!("{values}");
}

#[test]
#[ignore = "release microbenchmark; set REVIEW_EVIDENCE_DIR"]
fn review_statistics_construction_scaling() {
    let mut results = Vec::new();
    for groups in [512, 2048, 8192] {
        let root = tempfile::tempdir().unwrap();
        let request = request(root.path(), groups, 4, 1, 4096);
        drop(build(&request));
        let mut raw = Vec::new();
        let mut bytes = 0;
        for _ in 0..5 {
            let start = Instant::now();
            let pair = build(&request);
            raw.push(start.elapsed().as_nanos() as u64);
            bytes = pair.data_bytes().len() + pair.index_bytes().len();
            assert_eq!(pair.data_layout().header().row_count(), groups);
        }
        let med = median(&mut raw.clone());
        results.push(
            json!({"groups":groups,"columns":4,"statistics_entries":groups*4,
            "blocks":groups*5,"artifact_bytes":bytes,"median_ns":med,"runs_ns":raw}),
        );
    }
    save("statistics-scaling.json", json!(results));
}

fn measure_build_shape(rows: u64, width: u32, group_rows: u32, runs: usize) -> serde_json::Value {
    let root = tempfile::tempdir().unwrap();
    let request = request(root.path(), rows, width, group_rows, 4096);
    drop(build(&request));
    let mut raw = Vec::with_capacity(runs);
    let mut bytes = 0;
    for _ in 0..runs {
        let start = Instant::now();
        let pair = build(&request);
        raw.push(start.elapsed().as_nanos() as u64);
        bytes = pair.data_bytes().len() + pair.index_bytes().len();
        assert_eq!(pair.data_layout().header().row_count(), rows);
    }
    let median_ns = median(&mut raw.clone());
    let groups = rows.div_ceil(u64::from(group_rows));
    json!({
        "rows": rows,
        "groups": groups,
        "columns": width,
        "group_rows": group_rows,
        "statistics_entries": groups * u64::from(width),
        "artifact_bytes": bytes,
        "median_ns": median_ns,
        "runs_ns": raw,
    })
}

#[test]
#[ignore = "release acceptance matrix; set REVIEW_EVIDENCE_DIR"]
fn review_statistics_acceptance_matrix() {
    let group_scaling = [512_u64, 2048, 8192]
        .into_iter()
        .map(|groups| measure_build_shape(groups, 4, 1, 5))
        .collect::<Vec<_>>();
    let width_scaling = [1_u32, 8, 64]
        .into_iter()
        .map(|width| measure_build_shape(1024, width, 1, 5))
        .collect::<Vec<_>>();
    let realistic_row_groups = [4_u32, 16]
        .into_iter()
        .flat_map(|width| {
            [256_u32, 1024, 4096]
                .into_iter()
                .map(move |group_rows| measure_build_shape(131_072, width, group_rows, 5))
        })
        .collect::<Vec<_>>();
    save(
        "statistics-acceptance-matrix.json",
        json!({
            "schema": "radixdb.statistics.acceptance.v1",
            "group_scaling": group_scaling,
            "width_scaling": width_scaling,
            "realistic_row_groups": realistic_row_groups,
        }),
    );
}

struct Traced<'a> {
    bytes: &'a [u8],
    calls: Cell<u64>,
    read_bytes: Cell<u64>,
}
impl ArtifactSource for Traced<'_> {
    fn byte_length(&self) -> FormatResult<u64> {
        Ok(self.bytes.len() as u64)
    }
    fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> FormatResult<()> {
        self.calls.set(self.calls.get() + 1);
        self.read_bytes
            .set(self.read_bytes.get() + destination.len() as u64);
        self.bytes.read_exact_at(offset, destination)
    }
}

#[test]
#[ignore = "release microbenchmark; set REVIEW_EVIDENCE_DIR"]
fn review_exact_lookup_page_decode_cost() {
    let mut results = Vec::new();
    for page_entries in [64, 256, 1024, 4096] {
        let root = tempfile::tempdir().unwrap();
        let pair = build(&request(root.path(), 65536, 1, 65536, page_entries));
        let layout = decode_index_artifact_layout(
            pair.index_bytes(),
            pair.index_reference(),
            pair.data_layout(),
        )
        .unwrap();
        let source = Traced {
            bytes: pair.index_bytes(),
            calls: Cell::new(0),
            read_bytes: Cell::new(0),
        };
        let queries = 128u64;
        let mut raw = Vec::new();
        for _ in 0..5 {
            let start = Instant::now();
            for i in 0..queries {
                let key = (i * 7919 + 17) % 65536;
                let found = lookup_exact_index_from_source(
                    &source,
                    &layout,
                    pair.data_layout(),
                    oid(41),
                    &[Value::integer(key as i64)],
                )
                .unwrap();
                assert_eq!(found, Some(vec![key]));
            }
            raw.push(start.elapsed().as_nanos() as u64);
        }
        let med = median(&mut raw.clone());
        results.push(json!({"rows":65536,"page_entries":page_entries,"index_bytes":pair.index_bytes().len(),
            "queries_per_run":queries,"runs_ns":raw,"median_ns":med,"median_ns_per_lookup":med/queries,
            "read_calls_per_lookup":source.calls.get() as f64/(queries*5) as f64,
            "read_bytes_per_lookup":source.read_bytes.get() as f64/(queries*5) as f64}));
    }
    save("lookup-page-cost.json", json!(results));
}

#[test]
#[ignore = "release microbenchmark; set REVIEW_EVIDENCE_DIR"]
fn review_exact_lookup_cold_warm_matrix() {
    let mut results = Vec::new();
    for page_entries in [64, 256, 1024, 4096] {
        let root = tempfile::tempdir().unwrap();
        let pair = build(&request(root.path(), 65536, 1, 65536, page_entries));
        let opened = open_index_artifact_metadata(
            pair.index_bytes(),
            pair.index_reference(),
            pair.data_layout(),
        )
        .unwrap();
        let metadata_bytes = opened.metrics().accounted_allocation_bytes();
        let empty = opened.into_layout();
        let values = (0..128)
            .map(|i| {
                let key = if i % 2 == 0 {
                    (i * 7919 + 17) % 65536
                } else {
                    65536 + i
                };
                vec![Value::integer(key)]
            })
            .collect::<Vec<_>>();
        for warm in [false, true] {
            let layout = empty.clone();
            let source = Traced {
                bytes: pair.index_bytes(),
                calls: Cell::new(0),
                read_bytes: Cell::new(0),
            };
            if warm {
                for value in &values {
                    lookup_exact_index_from_source(
                        &source,
                        &layout,
                        pair.data_layout(),
                        oid(41),
                        value,
                    )
                    .unwrap();
                }
            }
            source.calls.set(0);
            source.read_bytes.set(0);
            let mut samples = Vec::new();
            let mut totals = Vec::new();
            for _ in 0..5 {
                let mut total = 0;
                for (i, value) in values.iter().enumerate() {
                    let fresh;
                    let target = if warm {
                        &layout
                    } else {
                        fresh = empty.clone();
                        &fresh
                    };
                    let start = Instant::now();
                    let found = lookup_exact_index_from_source(
                        &source,
                        target,
                        pair.data_layout(),
                        oid(41),
                        value,
                    )
                    .unwrap();
                    let elapsed = start.elapsed().as_nanos() as u64;
                    samples.push(elapsed);
                    total += elapsed;
                    let expected = (i % 2 == 0).then(|| vec![(i as u64 * 7919 + 17) % 65536]);
                    assert_eq!(found, expected);
                }
                totals.push(total);
            }
            let mut sorted = samples.clone();
            sorted.sort_unstable();
            results.push(json!({
                "page_entries":page_entries,"cache":if warm {"warm"} else {"cold"},
                "rows":65536,"queries":samples.len(),"hit_fraction":0.5,
                "metadata_accounted_bytes":metadata_bytes,"index_bytes":pair.index_bytes().len(),
                "p50_ns":sorted[(sorted.len()-1)*50/100],"p95_ns":sorted[(sorted.len()-1)*95/100],
                "p99_ns":sorted[(sorted.len()-1)*99/100],"runs_ns":totals,"samples_ns":samples,
                "read_calls_per_lookup":source.calls.get() as f64/640.0,
                "read_bytes_per_lookup":source.read_bytes.get() as f64/640.0,
            }));
        }
    }
    save("lookup-cold-warm.json", json!(results));
}
