use super::*;
use radixdb_storage::v6::{IndexArtifactLayout, IndexPageSpec};

#[path = "allocation_probe.rs"]
mod allocations;

#[test]
fn lookup_allocations_do_not_scale_with_unrelated_entries() {
    let (_, _, data, columns) = data_fixture(8192);
    let key_columns = vec![key_column(columns[0])];
    let entries = (0..8192)
        .map(|row| {
            ExactIndexEntry::new(
                ExactIndexKey::from_values(&data, &key_columns, &[Value::integer(row)]).unwrap(),
                vec![row as u64],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    for size in [64, 4096] {
        let (bytes, reference) = exact_container(
            &data,
            key_columns.clone(),
            entries.clone(),
            true,
            ExactPageBuildLimits::new(size, 4 * 1024 * 1024).unwrap(),
        );
        let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
        for key in [0, 4097, 8191, -1, 10000] {
            let (found, calls) = allocations::count(|| {
                lookup_exact_index(
                    &bytes,
                    &layout,
                    &data,
                    column_id(0x51),
                    &[Value::integer(key)],
                )
                .unwrap()
            });
            let expected = (0..8192).contains(&key).then(|| vec![key as u64]);
            assert_eq!(found, expected);
            eprintln!("entries/page={size}, key={key}, allocation calls={calls}");
            assert!(
                calls <= 128,
                "lookup materializes unrelated entries: {calls} allocations"
            );
        }
    }
}

fn mutated_container(
    data: &DataArtifactLayout,
    column: DataColumnSpec,
    codec: IndexPageCodec,
    mutate: impl FnOnce(&mut [u8]),
) -> (Vec<u8>, IndexArtifactLayout) {
    let keys = vec![key_column(column)];
    let entries = [1, 2]
        .into_iter()
        .enumerate()
        .map(|(row, key)| {
            ExactIndexEntry::new(
                ExactIndexKey::from_values(data, &keys, &[Value::integer(key)]).unwrap(),
                vec![row as u64],
            )
            .unwrap()
        })
        .collect();
    let page = encode_exact_index_pages(
        entries,
        true,
        data.header().row_count(),
        codec,
        ExactPageBuildLimits::default(),
    )
    .unwrap()
    .remove(0);
    let mut logical = page.logical_bytes().to_vec();
    mutate(&mut logical);
    let body_crc = radixdb_core::crc32_ieee(&logical[40..]);
    put_u32(&mut logical, 32, body_crc);
    let page = IndexPageSpec::new(
        logical,
        codec,
        page.item_count(),
        page.minimum_key_hash(),
        page.maximum_key_hash(),
    )
    .unwrap();
    let accelerator = IndexAcceleratorSpec::new(
        column_id(0x51),
        IndexAcceleratorKind::Exact,
        true,
        true,
        [0x52; 32],
        keys,
        2,
        vec![IndexSectionSpec::pages(IndexSectionKind::ExactPages, vec![page]).unwrap()],
    )
    .unwrap();
    let header = IndexArtifactHeader::for_data(
        ArtifactId::from_bytes(raw(0x53)).unwrap(),
        DatabaseGeneration::new(17).unwrap(),
        CatalogGeneration::new(15).unwrap(),
        data,
    )
    .unwrap();
    let (bytes, reference) =
        encode_index_artifact(&IndexArtifactInput::new(header, data, vec![accelerator]).unwrap())
            .unwrap();
    let layout = decode_index_artifact_layout(&bytes, reference, data).unwrap();
    (bytes, layout)
}

#[test]
fn lookup_still_rejects_corruption_in_unselected_entries() {
    let (_, _, data, columns) = data_fixture(16);
    for codec in [IndexPageCodec::None, IndexPageCodec::Lz4] {
        for mutation in 0..5 {
            let (bytes, layout) = mutated_container(&data, columns[0], codec, |logical| {
                let second = 40 + 32;
                match mutation {
                    0 => {
                        let start = posting_start(logical) + read_u32(logical, second + 8) as usize;
                        logical[start] = 127;
                        let crc = radixdb_core::crc32_ieee(&logical[start..start + 1]);
                        put_u32(logical, second + 28, crc);
                    }
                    1 => put_u32(logical, second + 20, 2),
                    2 => put_u32(logical, second, 0),
                    3 => put_u32(logical, second + 20, 1),
                    _ => put_u32(logical, second + 24, 0),
                }
            });
            let logical = read_index_page(&bytes, &layout, 0).unwrap();
            let expected = decode_exact_index_page(
                &logical,
                layout.pages()[0],
                &data,
                &layout.accelerators()[0],
            )
            .unwrap_err();
            for value in [Value::integer(1), Value::integer(-1)] {
                let actual = lookup_exact_index(&bytes, &layout, &data, column_id(0x51), &[value])
                    .unwrap_err();
                assert_eq!(actual, expected);
            }
        }
    }
}

#[test]
fn selected_lookup_covers_all_types_and_concurrent_readers() {
    let (_, _, data, columns, values) = all_types_fixture();
    let key_columns = columns.iter().copied().map(key_column).collect::<Vec<_>>();
    let key = ExactIndexKey::from_values(&data, &key_columns, &values).unwrap();
    let (bytes, reference) = exact_container(
        &data,
        key_columns,
        vec![ExactIndexEntry::new(key, vec![0]).unwrap()],
        true,
        ExactPageBuildLimits::default(),
    );
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                for _ in 0..16 {
                    assert_eq!(
                        lookup_exact_index(&bytes, &layout, &data, column_id(0x51), &values)
                            .unwrap(),
                        Some(vec![0])
                    );
                }
            });
        }
    });
}

#[test]
fn selected_hot_posting_is_complete_without_materializing_other_keys() {
    let (_, _, data, columns) = data_fixture(16384);
    let key_columns = vec![key_column(columns[0])];
    let rows = (0..16384).collect::<Vec<_>>();
    let key = ExactIndexKey::from_values(&data, &key_columns, &[Value::integer(7)]).unwrap();
    let (bytes, reference) = exact_container(
        &data,
        key_columns,
        vec![ExactIndexEntry::new(key, rows.clone()).unwrap()],
        false,
        ExactPageBuildLimits::default(),
    );
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
    let (found, calls) = allocations::count(|| {
        lookup_exact_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            &[Value::integer(7)],
        )
        .unwrap()
    });
    assert_eq!(found, Some(rows));
    assert!(calls <= 32);
    assert_eq!(
        lookup_exact_index(
            &bytes,
            &layout,
            &data,
            column_id(0x51),
            &[Value::integer(8)]
        )
        .unwrap(),
        None
    );
}

#[test]
fn warm_validation_still_reads_corruption_and_checks_data_binding() {
    let (_, _, data, columns) = data_fixture(16);
    let (bytes, layout) = mutated_container(&data, columns[0], IndexPageCodec::None, |_| {});
    for _ in 0..3 {
        assert_eq!(
            lookup_exact_index(
                &bytes,
                &layout,
                &data,
                column_id(0x51),
                &[Value::integer(1)]
            )
            .unwrap(),
            Some(vec![0])
        );
    }
    let cloned = layout.clone();
    assert_eq!(cloned, layout);
    let mut corrupted = bytes.clone();
    corrupted[layout.pages()[0].offset() as usize + 40] ^= 1;
    for key in [1, 99] {
        assert!(lookup_exact_index(
            &corrupted,
            &cloned,
            &data,
            column_id(0x51),
            &[Value::integer(key)]
        )
        .is_err());
    }
    let (_, _, other_data, _) = data_fixture(32);
    assert!(lookup_exact_index(
        &bytes,
        &layout,
        &other_data,
        column_id(0x51),
        &[Value::integer(1)]
    )
    .is_err());
}

#[test]
fn validation_cache_is_admitted_in_the_metadata_budget() {
    use radixdb_storage::v6::{
        open_index_artifact_metadata, open_index_artifact_metadata_with_limits, IndexOpenLimits,
    };
    let (_, _, data, columns) = data_fixture(128);
    let keys = vec![key_column(columns[0])];
    let entries = (0..128)
        .map(|row| {
            ExactIndexEntry::new(
                ExactIndexKey::from_values(&data, &keys, &[Value::integer(row)]).unwrap(),
                vec![row as u64],
            )
            .unwrap()
        })
        .collect();
    let (bytes, reference) = exact_container(
        &data,
        keys,
        entries,
        true,
        ExactPageBuildLimits::new(1, 1024).unwrap(),
    );
    let opened = open_index_artifact_metadata(bytes.as_slice(), reference, &data).unwrap();
    let budget = opened.metrics().accounted_allocation_bytes();
    let cache_bytes = 128 * size_of::<std::sync::OnceLock<[u8; 32]>>() as u64;
    assert!(budget > cache_bytes);
    assert!(open_index_artifact_metadata_with_limits(
        bytes.as_slice(),
        reference,
        &data,
        IndexOpenLimits::new(budget).unwrap()
    )
    .is_ok());
    let uncached = open_index_artifact_metadata_with_limits(
        bytes.as_slice(),
        reference,
        &data,
        IndexOpenLimits::new(budget - cache_bytes).unwrap(),
    )
    .unwrap();
    assert_eq!(
        uncached.metrics().accounted_allocation_bytes(),
        budget - cache_bytes
    );
    assert_eq!(uncached.layout(), opened.layout());
    assert_eq!(
        lookup_exact_index(
            &bytes,
            uncached.layout(),
            &data,
            column_id(0x51),
            &[Value::integer(97)]
        )
        .unwrap(),
        Some(vec![97])
    );
    assert!(open_index_artifact_metadata_with_limits(
        bytes.as_slice(),
        reference,
        &data,
        IndexOpenLimits::new(budget - cache_bytes - 1).unwrap()
    )
    .is_err());
}

#[test]
fn warm_composite_long_keys_and_misses_keep_exact_results() {
    let (_, _, data, columns) = data_fixture(128);
    let keys = vec![key_column(columns[1]), key_column(columns[2])];
    let values = (0..128)
        .map(|row| {
            vec![
                Value::text(format!("{}-{row:04}", "prefix".repeat(100))),
                if row % 2 == 0 {
                    Value::null(DataType::Float)
                } else {
                    Value::float(f64::NAN)
                },
            ]
        })
        .collect::<Vec<_>>();
    let entries = values
        .iter()
        .enumerate()
        .map(|(row, value)| {
            ExactIndexEntry::new(
                ExactIndexKey::from_values(&data, &keys, value).unwrap(),
                vec![row as u64],
            )
            .unwrap()
        })
        .collect();
    let (bytes, reference) = exact_container(
        &data,
        keys,
        entries,
        false,
        ExactPageBuildLimits::new(8, 1024 * 1024).unwrap(),
    );
    let layout = decode_index_artifact_layout(&bytes, reference, &data).unwrap();
    for _ in 0..3 {
        for (row, value) in values.iter().enumerate() {
            assert_eq!(
                lookup_exact_index(&bytes, &layout, &data, column_id(0x51), value).unwrap(),
                Some(vec![row as u64])
            );
        }
        assert_eq!(
            lookup_exact_index(
                &bytes,
                &layout,
                &data,
                column_id(0x51),
                &[Value::text("absent"), Value::null(DataType::Float)]
            )
            .unwrap(),
            None
        );
    }
}
