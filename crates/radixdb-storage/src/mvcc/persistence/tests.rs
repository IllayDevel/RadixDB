use super::*;
use super::{codec::INDEX_METADATA_MARKER_V1, row_codec::ROW_VERSION_MAGIC_V2};
use crate::SyncMode;
use chrono::Utc;
use radixdb_core::time_compat::TestWallClockGuard;
use tempfile::tempdir;

#[test]
fn b6_checkpoint_deadline_is_independent_of_wall_clock() {
    let config = PersistenceConfig::default();
    let base = UNIX_EPOCH + Duration::from_secs(1_730_613_600);
    let clock = TestWallClockGuard::install(base);
    let manager = PersistenceManager::new(None, &config).unwrap();

    std::thread::sleep(Duration::from_millis(10));
    let before_shift = manager.checkpoint_elapsed();
    clock.set(base - Duration::from_secs(86_400));
    let after_rollback = manager.checkpoint_elapsed();
    clock.set(base + Duration::from_secs(86_400 * 365));
    let after_jump = manager.checkpoint_elapsed();

    assert!(before_shift >= Duration::from_millis(10));
    assert!(after_rollback >= before_shift);
    assert!(after_jump >= after_rollback);
}

#[test]
fn deferred_scheduled_checkpoint_consumes_one_cadence_slot() {
    let config = PersistenceConfig::default();
    let manager = PersistenceManager::new(None, &config).unwrap();
    let interval = Duration::from_millis(10);

    std::thread::sleep(interval);
    let successful_checkpoint_age = manager.checkpoint_elapsed();
    assert!(manager.try_begin_scheduled_checkpoint(interval));
    assert!(
        !manager.try_begin_scheduled_checkpoint(interval),
        "a deferred attempt must not retrigger on the next coordinator tick"
    );
    assert!(
        manager.checkpoint_elapsed() >= successful_checkpoint_age,
        "attempt cadence must not impersonate a durable checkpoint"
    );

    std::thread::sleep(interval);
    assert!(manager.try_begin_scheduled_checkpoint(interval));
}

#[test]
fn test_index_metadata_serialization() {
    let meta = IndexMetadata {
        name: "idx_test".to_string(),
        table_name: "test".to_string(),
        column_names: vec!["col1".to_string(), "col2".to_string()],
        column_ids: vec![0, 1],
        data_types: vec![DataType::Integer, DataType::Text],
        is_unique: true,
        index_type: IndexType::Hash,
        hnsw_m: None,
        hnsw_ef_construction: None,
        hnsw_ef_search: None,
        hnsw_distance_metric: None,
        partial_predicate: None,
    };

    let serialized = meta.serialize().unwrap();
    let deserialized = IndexMetadata::deserialize(&serialized).unwrap();

    assert_eq!(deserialized.name, "idx_test");
    assert_eq!(deserialized.table_name, "test");
    assert_eq!(deserialized.column_names, vec!["col1", "col2"]);
    assert_eq!(deserialized.column_ids, vec![0, 1]);
    assert!(deserialized.is_unique);
    assert_eq!(deserialized.index_type, IndexType::Hash);
}

#[test]
fn test_index_metadata_all_types() {
    // Test all index types serialize/deserialize correctly
    for index_type in [
        IndexType::BTree,
        IndexType::Hash,
        IndexType::Bitmap,
        IndexType::MultiColumn,
    ] {
        let meta = IndexMetadata {
            name: "idx_test".to_string(),
            table_name: "test".to_string(),
            column_names: vec!["col1".to_string()],
            column_ids: vec![0],
            data_types: vec![DataType::Integer],
            is_unique: false,
            index_type,
            hnsw_m: None,
            hnsw_ef_construction: None,
            hnsw_ef_search: None,
            hnsw_distance_metric: None,
            partial_predicate: None,
        };

        let serialized = meta.serialize().unwrap();
        let deserialized = IndexMetadata::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.index_type, index_type);
    }
}

#[test]
fn test_partial_index_metadata_serialization() {
    let meta = IndexMetadata {
        name: "idx_active_email".to_string(),
        table_name: "users".to_string(),
        column_names: vec!["email".to_string()],
        column_ids: vec![1],
        data_types: vec![DataType::Text],
        is_unique: true,
        index_type: IndexType::Hash,
        hnsw_m: None,
        hnsw_ef_construction: None,
        hnsw_ef_search: None,
        hnsw_distance_metric: None,
        partial_predicate: Some(PartialIndexPredicateMetadata::new("deleted_at IS NULL")),
    };

    let serialized = meta.serialize().unwrap();
    let deserialized = IndexMetadata::deserialize(&serialized).unwrap();

    assert_eq!(deserialized.name, "idx_active_email");
    assert_eq!(
        deserialized
            .partial_predicate
            .as_ref()
            .map(|p| p.canonical_sql()),
        Some("deleted_at IS NULL")
    );
}

fn index_metadata_tag_offsets(data: &[u8]) -> (usize, usize, usize) {
    let mut pos = INDEX_METADATA_MARKER_V1.len();
    let name_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2 + name_len;
    let table_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2 + table_len;
    let column_count = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2;
    for _ in 0..column_count {
        let column_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2 + column_len;
    }
    pos += column_count * 4;
    let data_type_count = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
    pos += 2;
    let first_data_type = pos;
    pos += data_type_count;
    (first_data_type, pos, pos + 1)
}

#[test]
fn test_index_metadata_codec_is_versioned_bounded_and_fail_closed() {
    let metadata = IndexMetadata {
        name: "idx_value".to_string(),
        table_name: "items".to_string(),
        column_names: vec!["value".to_string()],
        column_ids: vec![1],
        data_types: vec![DataType::Integer],
        is_unique: true,
        index_type: IndexType::Hash,
        hnsw_m: None,
        hnsw_ef_construction: None,
        hnsw_ef_search: None,
        hnsw_distance_metric: None,
        partial_predicate: None,
    };
    let encoded = metadata.serialize().unwrap();
    assert!(encoded.starts_with(INDEX_METADATA_MARKER_V1));
    IndexMetadata::deserialize(&encoded).unwrap();
    assert!(
        IndexMetadata::deserialize(&encoded[INDEX_METADATA_MARKER_V1.len()..])
            .unwrap_err()
            .to_string()
            .contains("expected RIX1 marker")
    );

    let (data_type_offset, unique_offset, index_type_offset) = index_metadata_tag_offsets(&encoded);
    let mut unknown_data_type = encoded.clone();
    unknown_data_type[data_type_offset] = u8::MAX;
    assert!(IndexMetadata::deserialize(&unknown_data_type)
        .unwrap_err()
        .to_string()
        .contains("unknown data type"));

    let mut unknown_unique = encoded.clone();
    unknown_unique[unique_offset] = 2;
    assert!(IndexMetadata::deserialize(&unknown_unique)
        .unwrap_err()
        .to_string()
        .contains("unknown unique flag"));

    let mut unknown_index_type = encoded.clone();
    unknown_index_type[index_type_offset] = u8::MAX;
    assert!(IndexMetadata::deserialize(&unknown_index_type)
        .unwrap_err()
        .to_string()
        .contains("unknown index type"));

    let mut missing_index_type = encoded.clone();
    missing_index_type.truncate(index_type_offset);
    assert!(IndexMetadata::deserialize(&missing_index_type)
        .unwrap_err()
        .to_string()
        .contains("missing index type"));

    let mut oversized = metadata.clone();
    oversized.name = "x".repeat(u16::MAX as usize + 1);
    assert!(oversized
        .serialize()
        .unwrap_err()
        .to_string()
        .contains("index name"));

    let mut mismatched = metadata;
    mismatched.data_types.clear();
    assert!(mismatched
        .serialize()
        .unwrap_err()
        .to_string()
        .contains("different lengths"));
}

#[test]
fn test_value_decoder_rejects_unknown_tags_and_noncanonical_lengths() {
    assert!(deserialize_value(&[0, u8::MAX])
        .unwrap_err()
        .to_string()
        .contains("unknown NULL data type"));
    assert!(deserialize_value(&[1, 2])
        .unwrap_err()
        .to_string()
        .contains("unknown boolean"));

    let mut integer = serialize_value(&Value::Integer(7)).unwrap();
    integer.push(0);
    assert!(deserialize_value(&integer)
        .unwrap_err()
        .to_string()
        .contains("invalid integer value length"));
}

#[test]
fn persistence_rejects_malformed_extension_values_before_publication() {
    let malformed_json = Value::Extension(CompactArc::from(
        [DataType::Json as u8, b'{', b'b', b'a', b'd'].as_slice(),
    ));
    assert!(serialize_value(&malformed_json).is_err());

    let malformed_vector = Value::Extension(CompactArc::from(
        [DataType::Vector as u8, 0, 0, 0].as_slice(),
    ));
    assert!(serialize_value(&malformed_vector).is_err());

    let mut encoded_json = vec![6];
    encoded_json.extend_from_slice(&4u32.to_le_bytes());
    encoded_json.extend_from_slice(b"{bad");
    assert!(deserialize_value(&encoded_json).is_err());

    let mut truncated_vector = vec![10];
    truncated_vector.extend_from_slice(&2u32.to_le_bytes());
    truncated_vector.extend_from_slice(&1.0f32.to_le_bytes());
    assert!(deserialize_value(&truncated_vector).is_err());

    let mut malformed_decimal_payload = vec![0; 19];
    malformed_decimal_payload[0] = DataType::Decimal as u8;
    malformed_decimal_payload[17] = 1;
    malformed_decimal_payload[18] = 2;
    let malformed_decimal = Value::Extension(CompactArc::from(malformed_decimal_payload));
    assert!(serialize_value(&malformed_decimal).is_err());
}

#[test]
fn external_value_serialization_preserves_identity_and_rejects_malformed_headers() {
    let type_ref = radixdb_core::ExternalTypeRef::new([0x31; 16], 7).unwrap();
    let value = Value::try_external(type_ref, vec![1, 2, 3, 4]).unwrap();
    let encoded = serialize_value(&value).unwrap();
    assert_eq!(encoded[0], 12);
    assert_eq!(deserialize_value(&encoded).unwrap(), value);

    assert!(deserialize_value(&[12, 1, 2, 3]).is_err());
    let mut zero_identity = encoded.clone();
    zero_identity[1..17].fill(0);
    assert!(deserialize_value(&zero_identity).is_err());
    let mut zero_codec = encoded.clone();
    zero_codec[17..21].fill(0);
    assert!(deserialize_value(&zero_codec).is_err());
    let mut trailing = encoded;
    trailing.push(0);
    assert!(deserialize_value(&trailing).is_err());
}

#[test]
fn test_persistence_manager_disabled() {
    let config = PersistenceConfig::default();
    let pm = PersistenceManager::new(None, &config).unwrap();
    assert!(!pm.is_enabled());
}

#[test]
fn test_persistence_manager_enabled() {
    let dir = tempdir().unwrap();
    let config = PersistenceConfig {
        enabled: true,
        ..Default::default()
    };
    let pm = PersistenceManager::new(Some(dir.path()), &config).unwrap();
    assert!(pm.is_enabled());
    assert_eq!(pm.current_lsn(), 0);
}

#[test]
fn test_persistence_manager_record_operations() {
    let dir = tempdir().unwrap();
    let config = PersistenceConfig {
        enabled: true,
        sync_mode: SyncMode::Full,
        ..Default::default()
    };
    let pm = PersistenceManager::new(Some(dir.path()), &config).unwrap();
    pm.start().unwrap();

    // Record DML
    let table_id = radixdb_catalog::ObjectId::new();
    let version = RowVersion::new(1, Row::from_values(vec![Value::Integer(42)]));
    pm.record_dml_operation(1, table_id, 100, WALOperationType::Insert, &version)
        .unwrap();
    assert_eq!(pm.current_lsn(), 1);

    // Record commit
    pm.record_commit(1).unwrap();
    assert_eq!(pm.current_lsn(), 2);
    pm.stop().unwrap();
}

#[test]
fn test_value_serialization() {
    // Test all value types
    let values = vec![
        Value::null_unknown(),
        Value::Boolean(true),
        Value::Integer(12345),
        Value::Float(3.54159),
        Value::text("hello world"),
        Value::Timestamp(Utc::now()),
        Value::json(r#"{"key": "value"}"#),
    ];

    for value in values {
        let serialized = serialize_value(&value).unwrap();
        let deserialized = deserialize_value(&serialized).unwrap();

        // Compare values - binary timestamp format preserves full nanosecond precision
        match (&value, &deserialized) {
            (Value::Timestamp(t1), Value::Timestamp(t2)) => {
                // Full nanosecond precision comparison
                assert_eq!(t1.timestamp(), t2.timestamp(), "Timestamp seconds mismatch");
                assert_eq!(
                    t1.timestamp_subsec_nanos(),
                    t2.timestamp_subsec_nanos(),
                    "Timestamp nanoseconds mismatch"
                );
            }
            _ => {
                assert_eq!(value, deserialized, "Value mismatch for {:?}", value);
            }
        }
    }
}

#[test]
fn test_row_version_serialization() {
    let version = RowVersion::new(
        123,
        Row::from_values(vec![
            Value::Integer(100),
            Value::text("test"),
            Value::Boolean(true),
        ]),
    );

    let serialized = serialize_row_version(&version).unwrap();
    let deserialized = deserialize_row_version(&serialized).unwrap();

    assert_eq!(deserialized.txn_id, 123);
    assert_eq!(deserialized.deleted_at_txn_id, 0);
    assert_eq!(deserialized.data.len(), 3);
}

#[test]
fn r7_l01_legacy_codecs_are_rejected_by_persistence() {
    let current = RowVersion::new(7, Row::from_values(vec![Value::Integer(11)]));
    let mut unversioned = serialize_row_version(&current).unwrap();
    unversioned.drain(..ROW_VERSION_MAGIC_V2.len());
    let row_error = deserialize_row_version(&unversioned).unwrap_err();
    assert!(row_error.to_string().contains("expected RV2 discriminator"));

    let timestamp = b"2024-01-02T03:04:05Z";
    let mut textual_timestamp = vec![5];
    textual_timestamp.extend_from_slice(&(timestamp.len() as u32).to_le_bytes());
    textual_timestamp.extend_from_slice(timestamp);
    let value_error = deserialize_value(&textual_timestamp).unwrap_err();
    assert!(value_error
        .to_string()
        .contains("unknown value type tag: 5"));

    let binary = Value::timestamp(chrono::DateTime::from_timestamp(1_704_164_645, 0).unwrap());
    let encoded = serialize_value(&binary).unwrap();
    assert_eq!(deserialize_value(&encoded).unwrap(), binary);
}

#[test]
fn orm_01_pre_naming_constraint_catalog_is_rejected_without_mutation() {
    let legacy = b"legacy-schema-catalog".to_vec();
    let before = legacy.clone();
    let mut position = 0;
    let error = deserialize_constraint_catalog(&legacy, &mut position).unwrap_err();
    assert!(error.to_string().contains("logical export/import"));
    assert_eq!(position, 0, "failed admission must not advance the decoder");
    assert_eq!(
        legacy, before,
        "read admission must not mutate source bytes"
    );
}

#[test]
fn test_persistence_manager_replay() {
    let dir = tempdir().unwrap();
    let config = PersistenceConfig {
        enabled: true,
        sync_mode: SyncMode::Full,
        ..Default::default()
    };

    // Write some entries with commits
    {
        let pm = PersistenceManager::new(Some(dir.path()), &config).unwrap();
        pm.start().unwrap();
        let table_id = radixdb_catalog::ObjectId::new();

        for i in 1..=5 {
            let version = RowVersion::new(i, Row::from_values(vec![Value::Integer(i * 10)]));
            pm.record_dml_operation(i, table_id, i * 100, WALOperationType::Insert, &version)
                .unwrap();
            // Commit each transaction
            pm.record_commit(i).unwrap();
        }

        pm.stop().unwrap();
    }

    // Replay entries using two-phase recovery
    {
        let pm = PersistenceManager::new(Some(dir.path()), &config).unwrap();
        pm.start().unwrap();
        let mut data_count = 0;
        let mut commit_count = 0;

        pm.replay_two_phase(0, |entry| {
            assert!(entry.lsn > 0);
            if entry.is_commit_marker() {
                commit_count += 1;
            } else {
                data_count += 1;
            }
            Ok(())
        })
        .unwrap();

        assert_eq!(data_count, 5);
        assert_eq!(commit_count, 5); // 5 commit markers for 5 transactions
        pm.stop().unwrap();
    }
}
