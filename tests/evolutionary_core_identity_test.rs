use radixdb::core::ForeignKeyAction;
use radixdb::{
    DataType, Error, IndexEntry, IndexType, IsolationLevel, Operator, Row, Schema, SchemaBuilder,
    SchemaColumn, SchemaColumnId, SchemaTableId, SmartString, Value,
};

fn accepts_core_data_type(value: radixdb_core::DataType) -> radixdb_core::DataType {
    value
}

fn accepts_core_table_id(value: radixdb_core::SchemaTableId) -> radixdb_core::SchemaTableId {
    value
}

fn accepts_core_column_id(value: radixdb_core::SchemaColumnId) -> radixdb_core::SchemaColumnId {
    value
}

fn accepts_core_value(value: radixdb_core::Value) -> radixdb_core::Value {
    value
}

fn accepts_core_string(value: radixdb_core::SmartString) -> radixdb_core::SmartString {
    value
}

fn accepts_core_row(value: radixdb_core::Row) -> radixdb_core::Row {
    value
}

fn accepts_core_schema(value: radixdb_core::Schema) -> radixdb_core::Schema {
    value
}

fn accepts_core_cow_btree(value: radixdb_core::CowBTree<i64>) -> radixdb_core::CowBTree<i64> {
    value
}

fn accepts_core_params(value: radixdb_core::ParamVec) -> radixdb_core::ParamVec {
    value
}

#[test]
fn facade_and_core_exports_have_one_type_identity() {
    let facade_data_type: DataType = DataType::Integer;
    let _: radixdb_core::DataType = accepts_core_data_type(facade_data_type);

    let facade_table: SchemaTableId = SchemaTableId::new(7, 11, "events".to_owned());
    let direct_table = accepts_core_table_id(facade_table);
    let facade_column: SchemaColumnId = SchemaColumnId::new(direct_table, 3);
    let _: radixdb_core::SchemaColumnId = accepts_core_column_id(facade_column);

    let parse_error: Error = "invalid"
        .parse::<radixdb_core::IsolationLevel>()
        .unwrap_err();
    assert!(matches!(parse_error, Error::Parse(_)));

    let _: radixdb_core::ForeignKeyAction = ForeignKeyAction::Restrict;
    let _: radixdb_core::IndexEntry = IndexEntry::new(1, 2);
    let _: radixdb_core::IndexType = IndexType::BTree;
    let _: radixdb_core::IsolationLevel = IsolationLevel::ReadCommitted;
    let _: radixdb_core::Operator = Operator::Eq;

    let facade_value: Value = Value::text("canonical");
    assert_eq!(accepts_core_value(facade_value), Value::text("canonical"));
    let facade_string: SmartString = SmartString::from("inline");
    assert_eq!(accepts_core_string(facade_string).as_str(), "inline");

    let facade_vec: radixdb::common::CompactVec<u64> = [1, 2, 3].into_iter().collect();
    let _: radixdb_core::CompactVec<u64> = facade_vec;
    let facade_arc: radixdb::common::CompactArc<str> = "shared".into();
    let _: radixdb_core::CompactArc<str> = facade_arc;

    let facade_row: Row = Row::from_values(vec![Value::integer(1), Value::text("row")]);
    assert_eq!(accepts_core_row(facade_row).len(), 2);
    let facade_rows: radixdb::core::RowVec = radixdb::core::RowVec::new();
    let _: radixdb_core::RowVec = facade_rows;
    let facade_ids: radixdb::core::RowIdVec = radixdb::core::RowIdVec::new();
    let _: radixdb_core::RowIdVec = facade_ids;
    let macro_row = radixdb::row![1_i64, "macro"];
    assert_eq!(
        macro_row.as_slice(),
        &[Value::integer(1), Value::text("macro")]
    );

    let facade_schema: Schema = SchemaBuilder::new("identity")
        .add_primary_key("id", DataType::Integer)
        .add_nullable("payload", DataType::Text)
        .build();
    let direct_schema = accepts_core_schema(facade_schema);
    let facade_column: &SchemaColumn = &direct_schema.columns()[1];
    assert_eq!(facade_column.name, "payload");
}

#[test]
fn root_common_uses_canonical_cow_btree_type() {
    let tree: radixdb::common::CowBTree<i64> = radixdb::common::CowBTree::new();
    let _: radixdb_core::CowBTree<i64> = accepts_core_cow_btree(tree);
}

#[test]
fn positional_parameters_share_one_type_identity_and_inline_capacity() {
    let mut facade_params = radixdb::ParamVec::new();
    facade_params.push(radixdb::Value::Integer(7));
    let core_params = accepts_core_params(facade_params);

    assert_eq!(core_params.as_slice(), &[radixdb_core::Value::Integer(7)]);
    assert!(!core_params.spilled());
}

#[test]
fn primitive_persisted_tags_are_unchanged() {
    let data_types = [
        DataType::Null,
        DataType::Integer,
        DataType::Float,
        DataType::Text,
        DataType::Boolean,
        DataType::Timestamp,
        DataType::Json,
        DataType::Vector,
        DataType::Uuid,
        DataType::Decimal,
        DataType::Date,
        DataType::Bytes,
    ];
    for (tag, data_type) in data_types.into_iter().enumerate() {
        assert_eq!(data_type.as_u8(), tag as u8);
        assert_eq!(DataType::from_u8(tag as u8), Some(data_type));
    }
    assert_eq!(DataType::from_u8(12), None);

    let actions = [
        ForeignKeyAction::Restrict,
        ForeignKeyAction::Cascade,
        ForeignKeyAction::SetNull,
        ForeignKeyAction::NoAction,
    ];
    for (tag, action) in actions.into_iter().enumerate() {
        assert_eq!(action.as_u8(), tag as u8);
        assert_eq!(ForeignKeyAction::from_u8(tag as u8), Some(action));
    }
    assert_eq!(ForeignKeyAction::from_u8(4), None);

    assert_eq!(IsolationLevel::ReadCommitted.as_u8(), 0);
    assert_eq!(IsolationLevel::SnapshotIsolation.as_u8(), 1);
    assert_eq!(
        IsolationLevel::from_u8(0),
        Some(IsolationLevel::ReadCommitted)
    );
    assert_eq!(
        IsolationLevel::from_u8(1),
        Some(IsolationLevel::SnapshotIsolation)
    );
    assert_eq!(IsolationLevel::from_u8(2), None);

    assert_eq!(std::mem::size_of::<DataType>(), 1);
    assert_eq!(std::mem::size_of::<IsolationLevel>(), 1);
    assert_eq!(std::mem::size_of::<Value>(), 16);
    assert_eq!(std::mem::size_of::<SmartString>(), 16);
    assert_eq!(std::mem::size_of::<radixdb::common::CompactVec<u64>>(), 16);
    assert_eq!(
        std::mem::size_of::<radixdb::common::CompactArc<[u8]>>(),
        std::mem::size_of::<usize>()
    );
}
