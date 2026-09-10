use radixdb_catalog::{CatalogDataType, ObjectId};
use radixdb_core::{DataType, Value};
use radixdb_storage::v6::{
    encode_data_artifact, ArtifactDataSource, ArtifactId, CatalogGeneration, DataArtifactHeader,
    DataArtifactInput, DataBlockSpec, DataColumnSpec, DataPhysicalCodec, DataValueEncoding,
    DatabaseGeneration, DatabaseId, SegmentId, SegmentKind,
};

fn identity(marker: u8) -> [u8; 16] {
    [marker; 16]
}

fn persisted_row_id(row_id: i64) -> u64 {
    (row_id as u64) ^ (1_u64 << 63)
}

#[test]
fn runtime_source_reads_bounded_rows_and_typed_columns() {
    let integer = DataColumnSpec::new(
        ObjectId::from_user_bytes(identity(0x31)).unwrap(),
        CatalogDataType::scalar(DataType::Integer).unwrap(),
        false,
    );
    let text = DataColumnSpec::new(
        ObjectId::from_user_bytes(identity(0x32)).unwrap(),
        CatalogDataType::scalar(DataType::Text).unwrap(),
        true,
    );
    let header = DataArtifactHeader::new(
        ArtifactId::from_bytes(identity(0x41)).unwrap(),
        DatabaseId::from_bytes(identity(0x42)).unwrap(),
        ObjectId::from_user_bytes(identity(0x43)).unwrap(),
        SegmentId::from_bytes(identity(0x44)).unwrap(),
        DatabaseGeneration::new(4).unwrap(),
        CatalogGeneration::new(3).unwrap(),
        10,
        12,
        5,
        2,
        2,
        SegmentKind::Rows,
        123,
    )
    .unwrap();
    // Format-layer DATA builders accept the canonical unsigned row-ID domain;
    // ArtifactDataSource exposes the signed runtime domain again.
    let first_ids = [2_i64, 5, 9].map(persisted_row_id);
    let second_ids = [11_i64, 20].map(persisted_row_id);
    let first_integers = [Value::integer(7), Value::integer(8), Value::integer(9)];
    let second_integers = [Value::integer(10), Value::integer(11)];
    let first_text = [
        Value::text("a"),
        Value::null(DataType::Text),
        Value::text("a"),
    ];
    let second_text = [Value::text("b"), Value::text("c")];
    let blocks = vec![
        DataBlockSpec::row_ids(0, &first_ids, DataPhysicalCodec::None).unwrap(),
        DataBlockSpec::column(
            0,
            0,
            integer,
            &first_integers,
            DataValueEncoding::Plain,
            DataPhysicalCodec::None,
        )
        .unwrap(),
        DataBlockSpec::column(
            0,
            1,
            text,
            &first_text,
            DataValueEncoding::Dictionary,
            DataPhysicalCodec::Lz4,
        )
        .unwrap(),
        DataBlockSpec::row_ids(1, &second_ids, DataPhysicalCodec::Lz4).unwrap(),
        DataBlockSpec::column(
            1,
            0,
            integer,
            &second_integers,
            DataValueEncoding::Plain,
            DataPhysicalCodec::Lz4,
        )
        .unwrap(),
        DataBlockSpec::column(
            1,
            1,
            text,
            &second_text,
            DataValueEncoding::Dictionary,
            DataPhysicalCodec::None,
        )
        .unwrap(),
    ];
    let input = DataArtifactInput::new(header, vec![integer, text], vec![], blocks).unwrap();
    let (bytes, reference) = encode_data_artifact(&input).unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("segment.data");
    std::fs::write(&path, bytes).unwrap();

    let source = ArtifactDataSource::open(&path, reference).unwrap();
    assert_eq!(source.row_count().unwrap(), 5);
    assert_eq!(source.column_count(), 2);
    assert_eq!(source.row_group_count(), 2);
    assert_eq!(source.row_group_for_row(0).unwrap(), 0);
    assert_eq!(source.row_group_for_row(4).unwrap(), 1);

    let row_ids = source.read_row_ids(1).unwrap();
    assert_eq!(row_ids.row_range(), 3..5);
    assert_eq!(row_ids.row_ids(), &[11, 20]);
    assert_eq!(row_ids.row_id(4), Some(20));

    let batch = source.read_columns(0, &[1, 0]).unwrap();
    assert_eq!(batch.row_group_index(), 0);
    assert_eq!(batch.row_range(), 0..3);
    assert_eq!(batch.columns()[0].0, 1);
    assert_eq!(batch.columns()[0].1.get_value(0), Value::text("a"));
    assert_eq!(
        batch.columns()[0].1.get_value(1),
        Value::null(DataType::Text)
    );
    assert_eq!(batch.columns()[1].1.get_value(2), Value::integer(9));

    assert_eq!(source.find_row_id(2).unwrap(), Ok(0));
    assert_eq!(source.find_row_id(9).unwrap(), Ok(2));
    assert_eq!(source.find_row_id(10).unwrap(), Err(3));
    assert_eq!(source.find_row_id(20).unwrap(), Ok(4));
    assert_eq!(source.find_row_id(21).unwrap(), Err(5));
}
