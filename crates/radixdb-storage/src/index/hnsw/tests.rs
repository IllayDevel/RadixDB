use super::*;
use radixdb_core::vector::l2_distance_bytes;

fn make_vector_value(data: &[f32]) -> Value {
    Value::vector(data.to_vec())
}

fn extract_bytes(v: &Value) -> &[u8] {
    match v {
        Value::Extension(data) if data.first() == Some(&(DataType::Vector as u8)) => &data[1..],
        _ => panic!("not a vector value"),
    }
}

#[test]
fn hnsw_topology_is_replayable_from_build_seed() {
    fn build(seed: u64) -> HnswInner {
        let mut inner = HnswInner::with_build_seed(2, HnswDistanceMetric::L2, seed);
        let m = 8;
        let ml = 1.0 / (m as f64).ln();
        for row_id in 0..128i64 {
            let x = row_id as f32 / 7.0;
            let y = (row_id * 17 % 31) as f32;
            let mut bytes = Vec::with_capacity(8);
            bytes.extend_from_slice(&x.to_le_bytes());
            bytes.extend_from_slice(&y.to_le_bytes());
            inner.insert(&bytes, row_id, m, m * 2, 64, ml);
        }
        inner
    }

    let seed = 0x0123_4567_89ab_cdef;
    let first = build(seed);
    let replay = build(seed);
    let alternate = build(seed ^ 0xfeed_face_dead_beef);
    assert_eq!(
        first.serialize_graph().unwrap(),
        replay.serialize_graph().unwrap(),
        "same HNSW build seed and rows must reproduce byte-identical topology; seed={seed:#018x}"
    );
    assert_ne!(
        first
            .nodes
            .iter()
            .map(|node| node.neighbors.len())
            .collect::<Vec<_>>(),
        alternate
            .nodes
            .iter()
            .map(|node| node.neighbors.len())
            .collect::<Vec<_>>(),
        "different HNSW build seeds must select a different level sequence"
    );
}

#[test]
fn hnsw_graph_rejects_legacy_v1_encoding() {
    let inner = HnswInner::new(3, HnswDistanceMetric::L2);
    let mut encoded = inner.serialize_graph().unwrap();
    encoded[4..8].copy_from_slice(&1u32.to_le_bytes());

    let error = match HnswInner::deserialize_graph(&encoded) {
        Err(error) => error,
        Ok(_) => panic!("legacy HNSW v1 graph must be rejected"),
    };
    assert_eq!(error, "Unsupported HNSW version: 1");
}

#[test]
fn hnsw_constructor_rejects_unrepresentable_metadata() {
    let create = |dims, m, ef_construction, ef_search| {
        HnswIndex::new(
            "test_idx".to_string(),
            "test_table".to_string(),
            "embedding".to_string(),
            1,
            dims,
            m,
            ef_construction,
            ef_search,
            HnswDistanceMetric::L2,
        )
    };

    assert!(create(0, 16, 200, 64).is_err());
    assert!(create(3, 1, 200, 64).is_err());
    assert!(create(3, HNSW_MAX_M + 1, 200, 64).is_err());
    assert!(create(3, 16, usize::from(u16::MAX) + 1, 64).is_err());
    assert!(create(3, 16, 200, 0).is_err());
}

#[test]
fn hnsw_graph_rejects_duplicate_row_ids_and_trailing_data() {
    let index = HnswIndex::new(
        "test_idx".to_string(),
        "test_table".to_string(),
        "embedding".to_string(),
        1,
        3,
        16,
        200,
        64,
        HnswDistanceMetric::L2,
    )
    .unwrap();
    index
        .add(&[make_vector_value(&[1.0, 2.0, 3.0])], 11, 11)
        .unwrap();
    index
        .add(&[make_vector_value(&[4.0, 5.0, 6.0])], 22, 22)
        .unwrap();

    let mut duplicate = index.inner.read().serialize_graph().unwrap();
    let second_row_id = HNSW_GRAPH_HEADER_LEN + (2 * 3 * 4) + 8;
    duplicate[second_row_id..second_row_id + 8].copy_from_slice(&11i64.to_le_bytes());
    let duplicate_error = match HnswInner::deserialize_graph(&duplicate) {
        Err(error) => error,
        Ok(_) => panic!("duplicate HNSW row IDs must be rejected"),
    };
    assert!(duplicate_error.contains("duplicate row ID"));

    let mut trailing = index.inner.read().serialize_graph().unwrap();
    trailing.push(0);
    let trailing_error = match HnswInner::deserialize_graph(&trailing) {
        Err(error) => error,
        Ok(_) => panic!("trailing HNSW graph data must be rejected"),
    };
    assert!(trailing_error.contains("trailing bytes"));
}

#[test]
fn hnsw_graph_identity_must_match_requested_index() {
    let index = HnswIndex::new(
        "test_idx".to_string(),
        "test_table".to_string(),
        "embedding".to_string(),
        1,
        3,
        16,
        200,
        64,
        HnswDistanceMetric::L2,
    )
    .unwrap();
    index
        .add(&[make_vector_value(&[1.0, 2.0, 3.0])], 7, 7)
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph.bin");
    index.save_graph(&path).unwrap();

    let error = match HnswIndex::load_graph(
        &path,
        "wrong_idx".to_string(),
        "test_table".to_string(),
        "embedding".to_string(),
        1,
        3,
        16,
        200,
        64,
    ) {
        Err(error) => error,
        Ok(_) => panic!("mismatched HNSW identity must be rejected"),
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("identity"));
}

#[test]
fn hnsw_unique_transition_and_scalar_lookup_fail_closed() {
    let mut index = HnswIndex::new(
        "test_idx".to_string(),
        "test_table".to_string(),
        "embedding".to_string(),
        1,
        3,
        16,
        200,
        64,
        HnswDistanceMetric::L2,
    )
    .unwrap();
    let vector = make_vector_value(&[1.0, 2.0, 3.0]);
    index.add(std::slice::from_ref(&vector), 1, 1).unwrap();
    index.add(std::slice::from_ref(&vector), 2, 2).unwrap();

    assert!(index.set_unique(true).is_err());
    assert!(!Index::is_unique(&index));
    assert!(index.find(std::slice::from_ref(&vector)).is_err());
    assert!(index
        .find_range(
            std::slice::from_ref(&vector),
            std::slice::from_ref(&vector),
            true,
            true,
        )
        .is_err());
    assert!(index.close().is_err());
}

#[test]
fn test_hnsw_basic_search() {
    let mut index = HnswIndex::new(
        "test_idx".to_string(),
        "test_table".to_string(),
        "embedding".to_string(),
        1,
        3,   // 3 dimensions
        16,  // m
        200, // ef_construction
        64,  // ef_search
        HnswDistanceMetric::L2,
    )
    .unwrap();
    index.build().unwrap();

    // Insert 100 vectors with known pattern
    for i in 0..100i64 {
        let v = make_vector_value(&[i as f32, 0.0, 0.0]);
        index.add(&[v], i, i).unwrap();
    }

    // Search for nearest to [50, 0, 0] — should find ids around 50
    let query = make_vector_value(&[50.0, 0.0, 0.0]);
    let query_bytes = extract_bytes(&query);
    let results = index.search_nearest(query_bytes, 5, 64);

    assert_eq!(results.len(), 5);
    // The nearest should be row_id=50 (distance 0)
    assert_eq!(results[0].0, 50);
    assert!(results[0].1 < 0.01); // Near zero distance

    // All top-5 should be close to 50
    for (row_id, _dist) in &results {
        assert!(
            (*row_id - 50).abs() <= 3,
            "row_id {} too far from 50",
            row_id
        );
    }
}

#[test]
fn v2_r5_hnsw_publishes_exact_scalar_distance() {
    let dims = 128;
    let mut index = HnswIndex::new(
        "test_idx".to_string(),
        "test_table".to_string(),
        "embedding".to_string(),
        1,
        dims,
        16,
        200,
        64,
        HnswDistanceMetric::L2,
    )
    .unwrap();
    index.build().unwrap();

    let stored = make_vector_value(&vec![0.1; dims]);
    let query = make_vector_value(&vec![0.0; dims]);
    index.add(std::slice::from_ref(&stored), 7, 7).unwrap();
    let expected = l2_distance_bytes(extract_bytes(&stored), extract_bytes(&query)).unwrap();
    let result = index.search_nearest(extract_bytes(&query), 1, 64);
    assert_eq!(result, vec![(7, expected)]);
}

#[test]
fn test_hnsw_batch_dimension_validation_is_failure_atomic() {
    let index = HnswIndex::new(
        "test_idx".to_string(),
        "test_table".to_string(),
        "embedding".to_string(),
        1,
        3,
        16,
        200,
        64,
        HnswDistanceMetric::L2,
    )
    .unwrap();
    let valid = [make_vector_value(&[1.0, 2.0, 3.0])];
    let invalid = [make_vector_value(&[4.0, 5.0])];
    assert!(index
        .add_batch_slice(&[(1, &valid), (2, &invalid)])
        .is_err());
    assert!(index
        .search_nearest(extract_bytes(&valid[0]), 4, 64)
        .is_empty());
}

#[test]
fn test_hnsw_delete() {
    let mut index = HnswIndex::new(
        "test_idx".to_string(),
        "test_table".to_string(),
        "embedding".to_string(),
        1,
        3,
        16,
        200,
        64,
        HnswDistanceMetric::L2,
    )
    .unwrap();
    index.build().unwrap();

    for i in 0..50i64 {
        let v = make_vector_value(&[i as f32, 0.0, 0.0]);
        index.add(&[v], i, i).unwrap();
    }

    // Delete node closest to query
    let del_val = make_vector_value(&[25.0, 0.0, 0.0]);
    index.remove(&[del_val], 25, 25).unwrap();

    // Search for [25, 0, 0] — should NOT return row_id=25
    let query = make_vector_value(&[25.0, 0.0, 0.0]);
    let query_bytes = extract_bytes(&query);
    let results = index.search_nearest(query_bytes, 3, 64);

    for (row_id, _) in &results {
        assert_ne!(*row_id, 25, "deleted row should not appear in results");
    }
}

#[test]
fn test_hnsw_recall() {
    // Test that recall is reasonable (>= 80%) for a moderate dataset
    let dims = 16;
    let n = 1000;
    let k = 10;

    let mut index = HnswIndex::new(
        "test_idx".to_string(),
        "test_table".to_string(),
        "embedding".to_string(),
        1,
        dims,
        16,
        200,
        128, // higher ef_search for recall test
        HnswDistanceMetric::L2,
    )
    .unwrap();
    index.build().unwrap();

    // Generate vectors with deterministic pattern
    let vectors: Vec<Vec<f32>> = (0..n)
        .map(|i| (0..dims).map(|d| ((i * 7 + d * 13) as f32).sin()).collect())
        .collect();

    for (i, vec) in vectors.iter().enumerate() {
        let v = make_vector_value(vec);
        index.add(&[v], i as i64, i as i64).unwrap();
    }

    // Query vector
    let query_vec: Vec<f32> = (0..dims)
        .map(|d| ((50 * 7 + d * 13) as f32).sin() + 0.1)
        .collect();
    let query = make_vector_value(&query_vec);
    let query_bytes = extract_bytes(&query);

    // HNSW search
    let hnsw_results = index.search_nearest(query_bytes, k, 128);

    // Brute force ground truth
    let mut distances: Vec<(i64, f64)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let vb = make_vector_value(v);
            let vb_bytes = extract_bytes(&vb);
            let d = l2_distance_bytes(vb_bytes, query_bytes).unwrap();
            (i as i64, d)
        })
        .collect();
    distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let ground_truth: Vec<i64> = distances.iter().take(k).map(|(id, _)| *id).collect();

    // Compute recall
    let hnsw_ids: std::collections::HashSet<i64> = hnsw_results.iter().map(|(id, _)| *id).collect();
    let gt_ids: std::collections::HashSet<i64> = ground_truth.iter().cloned().collect();
    let matches = hnsw_ids.intersection(&gt_ids).count();
    let recall = matches as f64 / k as f64;

    assert!(
        recall >= 0.8,
        "HNSW recall too low: {:.1}% (expected >= 80%)",
        recall * 100.0
    );
}

#[test]
fn test_hnsw_batch_build_recall() {
    // Exercises add_batch_slice() which uses the parallel build path when enabled.
    // Uses clustered Gaussian vectors (same pattern as vector_search_bench.rs)
    // with a deterministic LCG to ensure reproducible results across platforms.
    let dims = 32;
    let n = 10_000;
    let k = 10;
    let num_queries = 50;
    let num_clusters = 50;

    let mut index = HnswIndex::new(
        "test_idx_batch".to_string(),
        "test_table".to_string(),
        "embedding".to_string(),
        1,
        dims,
        16,
        200,
        200,
        HnswDistanceMetric::L2,
    )
    .unwrap();
    index.build().unwrap();

    // Deterministic LCG PRNG
    let mut rng: u64 = 42;
    let next_f32 = |state: &mut u64| -> f32 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (*state >> 33) as f32 / (u32::MAX >> 1) as f32 - 1.0
    };

    // Generate cluster centers
    let centers: Vec<Vec<f32>> = (0..num_clusters)
        .map(|c| {
            (0..dims)
                .map(|d| {
                    let base = ((c * 7 + d * 13) as f32).sin() * 3.0;
                    let decay = 1.0 / (1.0 + d as f32 * 0.01);
                    base * decay
                })
                .collect()
        })
        .collect();

    // Generate clustered vectors with deterministic Gaussian noise (Box-Muller)
    let vectors: Vec<Vec<f32>> = (0..n)
        .map(|i| {
            let center = &centers[i % num_clusters];
            center
                .iter()
                .map(|&c| {
                    let u1 = (next_f32(&mut rng).abs() + 1.0) / 2.0; // map to (0, 1]
                    let u1 = u1.max(1e-10);
                    let u2 = (next_f32(&mut rng) + 1.0) / 2.0; // map to [0, 1]
                    let noise = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).cos();
                    c + noise * 0.5
                })
                .collect()
        })
        .collect();

    let row_ids: Vec<i64> = (0..n as i64).collect();
    let values: Vec<Vec<Value>> = vectors.iter().map(|v| vec![make_vector_value(v)]).collect();
    let entry_refs: Vec<(i64, &[Value])> = row_ids
        .iter()
        .zip(values.iter())
        .map(|(&row_id, vals)| (row_id, vals.as_slice()))
        .collect();

    index.add_batch_slice(&entry_refs).unwrap();

    // Generate query vectors (different seed, same cluster structure with wider noise)
    let mut qrng: u64 = 99999;
    let mut total_recall = 0.0;
    for qi in 0..num_queries {
        let center = &centers[qi % num_clusters];
        let qvec: Vec<f32> = center
            .iter()
            .map(|&c| {
                let u1 = (next_f32(&mut qrng).abs() + 1.0) / 2.0;
                let u1 = u1.max(1e-10);
                let u2 = (next_f32(&mut qrng) + 1.0) / 2.0;
                let noise = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).cos();
                c + noise * 0.6
            })
            .collect();
        let qval = make_vector_value(&qvec);
        let qbytes = extract_bytes(&qval);

        let hnsw_results = index.search_nearest(qbytes, k, 200);
        let hnsw_ids: std::collections::HashSet<i64> =
            hnsw_results.iter().map(|(id, _)| *id).collect();

        let mut distances: Vec<(i64, f64)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let vb = make_vector_value(v);
                let vb_bytes = extract_bytes(&vb);
                let d = l2_distance_bytes(vb_bytes, qbytes).unwrap();
                (i as i64, d)
            })
            .collect();
        distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let gt_ids: std::collections::HashSet<i64> =
            distances.iter().take(k).map(|(id, _)| *id).collect();

        let matches = hnsw_ids.intersection(&gt_ids).count();
        total_recall += matches as f64 / k as f64;
    }

    let avg_recall = total_recall / num_queries as f64;
    assert!(
        avg_recall >= 0.80,
        "HNSW batch-build recall too low: {:.1}% (expected >= 80%)",
        avg_recall * 100.0
    );
}

#[test]
fn test_l2_distance_sq_f32() {
    let a = [1.0f32, 2.0, 3.0];
    let b = [4.0f32, 5.0, 6.0];
    let dist = l2_distance_sq_f32(&a, &b);
    // (4-1)^2 + (5-2)^2 + (6-3)^2 = 9 + 9 + 9 = 27
    assert!((dist - 27.0).abs() < 0.001);
}

#[test]
fn distance_kernels_match_scalar_contract_across_simd_tails() {
    for dims in [1usize, 3, 4, 7, 16, 31, 128] {
        let left = (0..dims)
            .map(|index| (index as f32 * 0.375) - 3.0)
            .collect::<Vec<_>>();
        let right = (0..dims)
            .map(|index| 2.0 - index as f32 * 0.125)
            .collect::<Vec<_>>();
        let expected_l2 = left
            .iter()
            .zip(&right)
            .map(|(left, right)| (left - right) * (left - right))
            .sum::<f32>();
        let expected_dot = left
            .iter()
            .zip(&right)
            .map(|(left, right)| left * right)
            .sum::<f32>();
        let left_norm = left.iter().map(|value| value * value).sum::<f32>();
        let right_norm = right.iter().map(|value| value * value).sum::<f32>();
        let expected_cosine =
            1.0 - expected_dot / (left_norm.sqrt() * right_norm.sqrt()).max(1e-12);

        let tolerance = dims as f32 * 2e-5;
        assert!(
            (l2_distance_sq_f32(&left, &right) - expected_l2).abs() <= tolerance,
            "L2 kernel diverged at dims={dims}"
        );
        assert!(
            (ip_distance_f32(&left, &right) + expected_dot).abs() <= tolerance,
            "inner-product kernel diverged at dims={dims}"
        );
        assert!(
            (cosine_distance_f32(&left, &right) - expected_cosine).abs() <= tolerance,
            "cosine kernel diverged at dims={dims}"
        );
    }
}

#[test]
fn test_as_f32_slice_roundtrip() {
    let floats = [1.0f32, 2.5, -3.0, 4.0];
    let bytes: Vec<u8> = floats.iter().flat_map(|f| f.to_le_bytes()).collect();
    let slice = as_f32_slice(&bytes);
    assert_eq!(slice, &floats);
}
