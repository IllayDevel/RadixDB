use super::*;
use crate::expression::{ComparisonExpr, NullCheckExpr};
use crate::mvcc::version_store::VisibilityChecker;
use crate::mvcc::RowVersion;
use radixdb_core::SchemaBuilder;

struct TestVisibilityChecker;

impl VisibilityChecker for TestVisibilityChecker {
    fn is_visible(&self, version_txn_id: i64, viewing_txn_id: i64) -> bool {
        version_txn_id <= viewing_txn_id
    }

    fn get_current_sequence(&self) -> i64 {
        0
    }

    fn get_active_transaction_ids(&self) -> Vec<i64> {
        Vec::new()
    }
}

fn test_schema() -> Schema {
    SchemaBuilder::new("test_table")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, true, false)
        .build()
}

fn simple_schema() -> Schema {
    SchemaBuilder::new("test_table")
        .column("id", DataType::Integer, false, true)
        .build()
}

fn raf_soft_delete_schema() -> Schema {
    SchemaBuilder::new("users")
        .column("id", DataType::Integer, false, true)
        .column("email", DataType::Text, false, false)
        .column("__raf_deleted_at", DataType::Timestamp, true, false)
        .build()
}

fn raf_active_predicate(schema: &Schema) -> PartialIndexPredicate {
    PartialIndexPredicate::new(
        "__raf_deleted_at IS NULL",
        vec!["__raf_deleted_at".to_string()],
        Box::new(NullCheckExpr::is_null("__raf_deleted_at")),
        schema,
    )
    .unwrap()
}

fn raf_row(id: i64, email: &str, deleted: Option<chrono::DateTime<chrono::Utc>>) -> Row {
    Row::from_values(vec![
        Value::Integer(id),
        Value::text(email),
        deleted
            .map(Value::Timestamp)
            .unwrap_or(Value::Null(DataType::Timestamp)),
    ])
}

fn raf_table(store: Arc<VersionStore>, txn_id: i64) -> MVCCTable {
    let txn_versions = TransactionVersionStore::new(Arc::clone(&store), txn_id);
    MVCCTable::new(txn_id, store, txn_versions)
}

#[test]
fn test_mvcc_table_creation() {
    let schema = test_schema();

    let version_store = Arc::new(VersionStore::new("test_table".to_string(), schema));
    let txn_id = 1;

    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);

    let table = MVCCTable::new(txn_id, version_store, txn_versions);

    assert_eq!(table.name(), "test_table");
    assert_eq!(table.schema().columns.len(), 2);
}

#[test]
fn row_validation_rejects_malformed_and_saturating_values() {
    let schema = SchemaBuilder::new("typed_values")
        .column("id", DataType::Integer, false, true)
        .column("payload", DataType::Json, false, false)
        .column("embedding", DataType::Vector, false, false)
        .set_last_vector_dimensions(1)
        .build();
    let store = Arc::new(VersionStore::new("typed_values".to_string(), schema));
    let table = raf_table(store, 1);

    let malformed_json = Value::Extension(radixdb_core::CompactArc::from(
        [DataType::Json as u8, b'{'].as_slice(),
    ));
    let valid_vector = Value::vector(vec![1.0]);
    let mut row = Row::from_values(vec![
        Value::Float(f64::INFINITY),
        malformed_json,
        valid_vector.clone(),
    ]);
    assert!(table.validate_and_coerce_row(&mut row).is_err());

    let mut malformed_vector_bytes = vec![DataType::Vector as u8];
    malformed_vector_bytes.extend_from_slice(&[0, 0, 0]);
    let mut row = Row::from_values(vec![
        Value::Integer(1),
        Value::try_json("{}").unwrap(),
        Value::Extension(radixdb_core::CompactArc::from(malformed_vector_bytes)),
    ]);
    assert!(table.validate_and_coerce_row(&mut row).is_err());
}

#[test]
fn test_partial_unique_index_tracks_soft_delete_membership() {
    let schema = raf_soft_delete_schema();
    let version_store = Arc::new(VersionStore::with_visibility_checker(
        "users".to_string(),
        schema.clone(),
        Arc::new(TestVisibilityChecker),
    ));

    let table = raf_table(Arc::clone(&version_store), 1);
    table
        .create_partial_index_with_type(
            "users_email_active_idx",
            &["email"],
            true,
            Some(IndexType::Hash),
            raf_active_predicate(&schema),
        )
        .unwrap();

    let mut table = raf_table(Arc::clone(&version_store), 2);
    table.insert(raf_row(1, "a@example.test", None)).unwrap();
    table.commit().unwrap();

    let mut table = raf_table(Arc::clone(&version_store), 3);
    let duplicate_active = table.insert(raf_row(2, "a@example.test", None));
    assert!(
        duplicate_active.is_err(),
        "active duplicate must be rejected while original active row matches partial predicate"
    );

    let mut table = raf_table(Arc::clone(&version_store), 4);
    let deleted_at = chrono::Utc::now();
    let mut mark_deleted = |mut row: Row| {
        row.set(2, Value::Timestamp(deleted_at))?;
        Ok((row, true))
    };
    assert_eq!(table.update_by_row_ids(&[1], &mut mark_deleted).unwrap(), 1);
    table.commit().unwrap();

    let mut table = raf_table(Arc::clone(&version_store), 5);
    table.insert(raf_row(2, "a@example.test", None)).unwrap();
    table.commit().unwrap();

    let mut table = raf_table(Arc::clone(&version_store), 6);
    let mut restore_active = |mut row: Row| {
        row.set(2, Value::Null(DataType::Timestamp))?;
        Ok((row, true))
    };
    assert_eq!(
        table.update_by_row_ids(&[1], &mut restore_active).unwrap(),
        1
    );
    let restore_result = table.commit();
    assert!(
        restore_result.is_err(),
        "predicate false -> true update must re-check partial unique membership"
    );
}

#[test]
fn test_partial_unique_index_rejects_duplicate_active_batch() {
    let schema = raf_soft_delete_schema();
    let version_store = Arc::new(VersionStore::with_visibility_checker(
        "users".to_string(),
        schema.clone(),
        Arc::new(TestVisibilityChecker),
    ));

    let table = raf_table(Arc::clone(&version_store), 1);
    table
        .create_partial_index_with_type(
            "users_email_active_idx",
            &["email"],
            true,
            Some(IndexType::Hash),
            raf_active_predicate(&schema),
        )
        .unwrap();

    let mut table = raf_table(Arc::clone(&version_store), 2);
    table
        .insert(raf_row(10, "batch@example.test", None))
        .unwrap();
    assert!(
        table
            .insert(raf_row(11, "batch@example.test", None))
            .is_err(),
        "the second statement must reject a duplicate partial UNIQUE key"
    );
    table
        .commit()
        .expect("statement failure must not discard the first valid row");

    let mut contender = raf_table(version_store, 3);
    assert!(
        contender
            .insert(raf_row(11, "batch@example.test", None))
            .is_err(),
        "the committed partial UNIQUE owner must remain authoritative"
    );
}

#[test]
fn transaction_can_reuse_unique_key_released_by_its_own_delete() {
    let schema = SchemaBuilder::new("accounts")
        .column("id", DataType::Integer, false, true)
        .column("email", DataType::Text, false, false)
        .build();
    let version_store = Arc::new(VersionStore::with_visibility_checker(
        "accounts".to_string(),
        schema,
        Arc::new(TestVisibilityChecker),
    ));

    let mut seed = raf_table(Arc::clone(&version_store), 1);
    seed.create_index_with_type(
        "accounts_email_unique",
        &["email"],
        true,
        Some(IndexType::Hash),
    )
    .unwrap();
    seed.insert(Row::from_values(vec![
        Value::Integer(1),
        Value::text("owner@example.test"),
    ]))
    .unwrap();
    seed.commit().unwrap();

    let mut reuse = raf_table(Arc::clone(&version_store), 2);
    assert_eq!(reuse.delete_by_row_ids(&[1]).unwrap(), 1);
    reuse
        .insert(Row::from_values(vec![
            Value::Integer(2),
            Value::text("owner@example.test"),
        ]))
        .expect("the deleting transaction owns the released UNIQUE key");
    reuse.commit().unwrap();

    let verify = raf_table(version_store, 3);
    assert_eq!(verify.collect_all_rows(None).unwrap().len(), 1);
    assert_eq!(
        verify.collect_all_rows(None).unwrap()[0].1.get(0),
        Some(&Value::Integer(2))
    );
}

#[test]
fn test_mvcc_probe_visible_row_ids_rejects_mismatched_output() {
    let schema = simple_schema();
    let version_store = Arc::new(VersionStore::new("test_table".to_string(), schema));
    let txn_id = 1;
    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);
    let table = MVCCTable::new(txn_id, version_store, txn_versions);
    let mut matches = [true];

    let err = table
        .probe_visible_row_ids(&[1, 2], &mut matches)
        .expect_err("mismatched output must be rejected");

    assert!(matches!(err, Error::InvalidArgument(_)));
    assert_eq!(
        matches,
        [true],
        "validation error must not partially overwrite output"
    );
}

#[test]
fn test_mvcc_probe_visible_row_ids_applies_local_insert_and_delete() {
    let schema = simple_schema();
    let checker = Arc::new(TestVisibilityChecker);
    let version_store = Arc::new(VersionStore::with_visibility_checker(
        "test_table".to_string(),
        schema,
        checker,
    ));
    version_store
        .add_version(
            1,
            RowVersion::new(1, Row::from_values(vec![Value::Integer(1)])),
        )
        .unwrap();

    let txn_id = 2;
    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);
    let mut table = MVCCTable::new(txn_id, version_store, txn_versions);

    table
        .insert(Row::from_values(vec![Value::Integer(2)]))
        .unwrap();
    assert_eq!(table.delete_by_row_ids(&[1]).unwrap(), 1);

    let row_ids = [1, 2, 2, 3];
    let mut matches = [true; 4];
    let count = table.probe_visible_row_ids(&row_ids, &mut matches).unwrap();

    assert_eq!(matches, [false, true, true, false]);
    assert_eq!(count, 2, "duplicate local inserts count by input position");
}

#[test]
fn test_mvcc_table_insert_and_scan() {
    let schema = test_schema();

    let version_store = Arc::new(VersionStore::new("test_table".to_string(), schema));
    let txn_id = 1;

    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);

    let mut table = MVCCTable::new(txn_id, version_store, txn_versions);

    // Insert a row
    let row = Row::from_values(vec![Value::Integer(1), Value::text("Alice")]);
    table.insert(row).unwrap();

    // Scan to verify
    let mut scanner = table.scan(&[0, 1], None).unwrap();

    assert!(scanner.next());
    let row = scanner.row();
    assert_eq!(row.get(0), Some(&Value::Integer(1)));
    assert_eq!(row.get(1), Some(&Value::text("Alice")));

    assert!(!scanner.next());
    scanner.close().unwrap();
}

#[test]
fn secondary_index_scan_merges_transaction_local_rows_without_full_scan() {
    use crate::test_failpoints::{
        current_thread_execution_path_counters, ExecutionPathControlGuard, ExecutionPathMode,
    };

    let version_store = Arc::new(VersionStore::with_visibility_checker(
        "test_table".to_string(),
        test_schema(),
        Arc::new(TestVisibilityChecker),
    ));

    let mut seed = raf_table(Arc::clone(&version_store), 1);
    seed.create_index_with_type("test_name_idx", &["name"], false, Some(IndexType::Hash))
        .unwrap();
    seed.insert(Row::from_values(vec![
        Value::Integer(1),
        Value::text("committed"),
    ]))
    .unwrap();
    seed.commit().unwrap();

    let mut table = raf_table(version_store, 2);
    table
        .insert(Row::from_values(vec![
            Value::Integer(2),
            Value::text("local-only"),
        ]))
        .unwrap();

    let mut move_committed = |mut row: Row| {
        row.set(1, Value::text("moved"))?;
        Ok((row, true))
    };
    assert_eq!(
        table.update_by_row_ids(&[1], &mut move_committed).unwrap(),
        1
    );

    let _control = ExecutionPathControlGuard::install(ExecutionPathMode::Automatic);
    let mut local_filter = ComparisonExpr::eq("name", Value::text("local-only"));
    local_filter.prepare_for_schema(table.schema());
    let mut local = table.scan(&[0, 1], Some(&local_filter)).unwrap();
    assert!(local.next());
    assert_eq!(local.row().get(0), Some(&Value::Integer(2)));
    assert!(!local.next());
    local.close().unwrap();

    let mut old_key_filter = ComparisonExpr::eq("name", Value::text("committed"));
    old_key_filter.prepare_for_schema(table.schema());
    let mut old_key = table.scan(&[0, 1], Some(&old_key_filter)).unwrap();
    assert!(
        !old_key.next(),
        "local UPDATE must shadow the committed posting"
    );
    old_key.close().unwrap();

    let counters = current_thread_execution_path_counters();
    assert_eq!(counters.hot_index_scans, 2);
    assert_eq!(counters.hot_full_scans, 0);
}

#[test]
fn r3_l04_batch_a_projected_lookup_preserves_local_global_input_order() {
    let schema = test_schema();
    let version_store = Arc::new(VersionStore::with_visibility_checker(
        "test_table".to_string(),
        schema,
        Arc::new(TestVisibilityChecker),
    ));

    let mut committed = raf_table(Arc::clone(&version_store), 1);
    committed
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::text("committed"),
        ]))
        .unwrap();
    committed.commit().unwrap();

    let mut table = raf_table(version_store, 2);
    table
        .insert(Row::from_values(vec![
            Value::Integer(2),
            Value::text("local"),
        ]))
        .unwrap();
    let requested = [2, 1, 2];
    let rows = table
        .collect_rows_by_ids_projected(&requested, &[1])
        .unwrap();

    assert_eq!(
        rows.iter().map(|(row_id, _)| *row_id).collect::<Vec<_>>(),
        requested
    );
    assert_eq!(rows[0].1.get(0), Some(&Value::text("local")));
    assert_eq!(rows[1].1.get(0), Some(&Value::text("committed")));
    assert_eq!(rows[2].1.get(0), Some(&Value::text("local")));
}

#[test]
fn test_artifact_timestamp_range_is_rejected_before_insert_publication() {
    let schema = SchemaBuilder::new("timestamp_range")
        .column("id", DataType::Integer, false, true)
        .column("created_at", DataType::Timestamp, false, false)
        .build();
    let version_store = Arc::new(VersionStore::new("timestamp_range".to_string(), schema));
    let txn_id = 1;
    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);
    let mut table = MVCCTable::new(txn_id, version_store, txn_versions);
    let timestamp = chrono::DateTime::parse_from_rfc3339("2500-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    let error = table
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::Timestamp(timestamp),
        ]))
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("exact artifact-backed nanosecond range"));
    assert_eq!(table.row_count(), 0);
}

#[test]
fn test_mvcc_scan_empty_projection_keeps_select_star_contract() {
    let schema = test_schema();
    let version_store = Arc::new(VersionStore::new("test_table".to_string(), schema));
    let txn_id = 1;
    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);
    let mut table = MVCCTable::new(txn_id, version_store, txn_versions);

    table
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::text("Alice"),
        ]))
        .unwrap();

    let mut scanner = table.scan(&[], None).unwrap();
    assert!(scanner.next());
    assert_eq!(
        scanner.row().len(),
        2,
        "scan([]) remains the SELECT * / full-row contract"
    );
}

#[test]
fn test_mvcc_scan_exact_empty_projection_returns_zero_width_rows() {
    let schema = test_schema();
    let version_store = Arc::new(VersionStore::new("test_table".to_string(), schema));
    let txn_id = 1;
    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);
    let mut table = MVCCTable::new(txn_id, version_store, txn_versions);

    table
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::text("Alice"),
        ]))
        .unwrap();

    let mut scanner = table.scan_exact_projection(&[], None).unwrap();
    assert!(scanner.next());
    assert_eq!(
        scanner.row().len(),
        0,
        "scan_exact_projection([]) must mean exactly zero columns"
    );
}

#[test]
fn test_mvcc_table_duplicate_key_error() {
    let schema = simple_schema();

    let version_store = Arc::new(VersionStore::new("test_table".to_string(), schema));
    let txn_id = 1;

    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);

    let mut table = MVCCTable::new(txn_id, version_store, txn_versions);

    // Insert first row
    let row1 = Row::from_values(vec![Value::Integer(1)]);
    table.insert(row1).unwrap();

    // Try to insert duplicate
    let row2 = Row::from_values(vec![Value::Integer(1)]);
    let result = table.insert(row2);

    assert!(result.is_err());
}

#[test]
fn test_mvcc_table_delete() {
    let schema = simple_schema();

    let version_store = Arc::new(VersionStore::new("test_table".to_string(), schema));
    let txn_id = 1;

    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);

    let mut table = MVCCTable::new(txn_id, version_store, txn_versions);

    // Insert rows
    table
        .insert(Row::from_values(vec![Value::Integer(1)]))
        .unwrap();
    table
        .insert(Row::from_values(vec![Value::Integer(2)]))
        .unwrap();
    table
        .insert(Row::from_values(vec![Value::Integer(3)]))
        .unwrap();

    // Delete all rows (no filter)
    let deleted = table.delete(None).unwrap();
    assert_eq!(deleted, 3);

    // Verify no rows remain
    let mut scanner = table.scan(&[0], None).unwrap();
    assert!(!scanner.next());
    scanner.close().unwrap();
}

#[test]
fn test_mvcc_table_update() {
    let schema = SchemaBuilder::new("test_table")
        .column("id", DataType::Integer, false, true)
        .column("value", DataType::Integer, true, false)
        .build();

    let version_store = Arc::new(VersionStore::new("test_table".to_string(), schema));
    let txn_id = 1;

    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);

    let mut table = MVCCTable::new(txn_id, version_store, txn_versions);

    // Insert row
    table
        .insert(Row::from_values(vec![
            Value::Integer(1),
            Value::Integer(10),
        ]))
        .unwrap();

    // Update the row
    let updated = table
        .update(None, &mut |row| {
            let mut new_row = row.clone();
            let _ = new_row.set(1, Value::Integer(20));
            Ok((new_row, true))
        })
        .unwrap();

    assert_eq!(updated, 1);

    // Verify update
    let mut scanner = table.scan(&[0, 1], None).unwrap();
    assert!(scanner.next());
    let row = scanner.row();
    assert_eq!(row.get(1), Some(&Value::Integer(20)));
    scanner.close().unwrap();
}

#[test]
fn test_mvcc_table_validation_error() {
    let schema = SchemaBuilder::new("test_table")
        .column("id", DataType::Integer, false, false)
        .column("name", DataType::Text, false, false) // NOT NULL
        .build();

    let version_store = Arc::new(VersionStore::new("test_table".to_string(), schema));
    let txn_id = 1;

    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);

    let mut table = MVCCTable::new(txn_id, version_store, txn_versions);

    // Try to insert with NULL in non-nullable column
    let row = Row::from_values(vec![Value::Integer(1), Value::Null(DataType::Text)]);
    let result = table.insert(row);

    assert!(result.is_err());
}

#[test]
fn test_mvcc_table_row_count() {
    let schema = simple_schema();

    let version_store = Arc::new(VersionStore::new("test_table".to_string(), schema));
    let txn_id = 1;

    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);

    let mut table = MVCCTable::new(txn_id, version_store, txn_versions);

    assert_eq!(table.row_count(), 0);

    table
        .insert(Row::from_values(vec![Value::Integer(1)]))
        .unwrap();
    table
        .insert(Row::from_values(vec![Value::Integer(2)]))
        .unwrap();

    assert_eq!(table.row_count(), 2);
}

#[test]
fn test_validate_coerce_integer_to_float() {
    let schema = SchemaBuilder::new("test_table")
        .column("id", DataType::Integer, false, true)
        .column("score", DataType::Float, true, false)
        .build();

    let version_store = Arc::new(VersionStore::new("test_table".to_string(), schema));
    let txn_id = 1;
    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);
    let mut table = MVCCTable::new(txn_id, version_store, txn_versions);

    // Insert Integer into Float column — should coerce
    let row = Row::from_values(vec![Value::Integer(1), Value::Integer(42)]);
    table.insert(row).unwrap();

    let mut scanner = table.scan(&[0, 1], None).unwrap();
    assert!(scanner.next());
    assert_eq!(scanner.row().get(1), Some(&Value::Float(42.0)));
    scanner.close().unwrap();
}

#[test]
fn test_validate_coerce_float_to_integer() {
    let schema = SchemaBuilder::new("test_table")
        .column("id", DataType::Integer, false, true)
        .column("count", DataType::Integer, true, false)
        .build();

    let version_store = Arc::new(VersionStore::new("test_table".to_string(), schema));
    let txn_id = 1;
    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);
    let mut table = MVCCTable::new(txn_id, version_store, txn_versions);

    // Insert Float into Integer column — should truncate
    let row = Row::from_values(vec![Value::Integer(1), Value::Float(9.7)]);
    table.insert(row).unwrap();

    let mut scanner = table.scan(&[0, 1], None).unwrap();
    assert!(scanner.next());
    assert_eq!(scanner.row().get(1), Some(&Value::Integer(9)));
    scanner.close().unwrap();
}

#[test]
fn test_validate_coerce_integer_to_boolean() {
    let schema = SchemaBuilder::new("test_table")
        .column("id", DataType::Integer, false, true)
        .column("active", DataType::Boolean, true, false)
        .build();

    let version_store = Arc::new(VersionStore::new("test_table".to_string(), schema));
    let txn_id = 1;
    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);
    let mut table = MVCCTable::new(txn_id, version_store, txn_versions);

    // 0 -> false
    let row = Row::from_values(vec![Value::Integer(1), Value::Integer(0)]);
    table.insert(row).unwrap();

    // non-zero -> true
    let row = Row::from_values(vec![Value::Integer(2), Value::Integer(5)]);
    table.insert(row).unwrap();

    let mut results: Vec<(i64, bool)> = Vec::new();
    let mut scanner = table.scan(&[0, 1], None).unwrap();
    while scanner.next() {
        let id = match scanner.row().get(0) {
            Some(Value::Integer(v)) => *v,
            _ => panic!("expected integer id"),
        };
        let active = match scanner.row().get(1) {
            Some(Value::Boolean(v)) => *v,
            _ => panic!("expected boolean active"),
        };
        results.push((id, active));
    }
    scanner.close().unwrap();

    results.sort_by_key(|(id, _)| *id);
    assert_eq!(results, vec![(1, false), (2, true)]);
}

#[test]
fn test_validate_coerce_type_mismatch_error() {
    let schema = SchemaBuilder::new("test_table")
        .column("id", DataType::Integer, false, true)
        .column("name", DataType::Text, true, false)
        .build();

    let version_store = Arc::new(VersionStore::new("test_table".to_string(), schema));
    let txn_id = 1;
    let txn_versions = TransactionVersionStore::new(Arc::clone(&version_store), txn_id);
    let mut table = MVCCTable::new(txn_id, version_store, txn_versions);

    // Boolean into Text column — not a supported coercion, should error
    let row = Row::from_values(vec![Value::Integer(1), Value::Boolean(true)]);
    assert!(table.insert(row).is_err());
}
