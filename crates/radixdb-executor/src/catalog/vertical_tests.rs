use std::collections::BTreeMap;

use radixdb_catalog::{
    CatalogGeneration, CatalogGraph, CatalogName, CatalogObject, CatalogPackMeta, CatalogPayload,
    ConstraintPayload, NamespacePayload, ObjectId,
};
use radixdb_core::{Error, Result, Value};
use radixdb_storage::v6::{
    encode_catalog_wal_transaction, CatalogWalReplayLimits, CatalogWalTransaction,
    CatalogWalTransactionId,
};

use super::{CatalogCheckpointHarness, CatalogSnapshot, DdlTransaction, TableCatalog};

type CatalogRow = BTreeMap<ObjectId, Value>;

#[derive(Debug, Default)]
struct CatalogRows {
    tables: BTreeMap<ObjectId, Vec<CatalogRow>>,
}

impl CatalogRows {
    fn insert(
        &mut self,
        generation: &CatalogGeneration,
        table_name: &str,
        values: &[(&str, Value)],
    ) -> Result<()> {
        let table = TableCatalog::load(generation, table_name)?;
        let mut row = CatalogRow::new();
        for (column_name, value) in values {
            let column = table.column(column_name)?;
            if row.insert(column.id(), value.clone()).is_some() {
                return Err(Error::DuplicateColumn);
            }
        }
        for column in table.columns() {
            let CatalogPayload::Column(payload) = column.payload() else {
                return Err(Error::internal("catalog column has a non-column payload"));
            };
            let value = row
                .entry(column.id())
                .or_insert_with(|| Value::null(payload.data_type().logical_type()));
            value.validate_shape()?;
            if matches!(value, Value::Null(_)) {
                if !payload.nullable() {
                    return Err(Error::NotNullConstraint {
                        column: column.name().display().as_str().to_owned(),
                    });
                }
            } else if value.data_type() != payload.data_type().logical_type() {
                return Err(Error::InvalidColumnType);
            }
        }

        if let Some(primary_key) = table.primary_key() {
            let CatalogPayload::Constraint(ConstraintPayload::PrimaryKey { local_column_ids }) =
                primary_key.payload()
            else {
                return Err(Error::internal("primary-key link has a non-PK payload"));
            };
            let duplicate = self
                .tables
                .get(&table.id())
                .into_iter()
                .flatten()
                .any(|existing| same_key(existing, &row, local_column_ids));
            if duplicate {
                return Err(Error::InvalidArgument(
                    "catalog vertical duplicate primary key".to_owned(),
                ));
            }
        }

        for (_, foreign_key) in table.foreign_keys() {
            let ConstraintPayload::ForeignKey {
                local_column_ids,
                referenced_table_id,
                referenced_column_ids,
                ..
            } = foreign_key
            else {
                unreachable!("foreign_keys returned another constraint kind")
            };
            if local_column_ids
                .iter()
                .any(|id| matches!(row.get(id), Some(Value::Null(_))))
            {
                continue;
            }
            let referenced = self
                .tables
                .get(referenced_table_id)
                .into_iter()
                .flatten()
                .any(|candidate| {
                    local_column_ids
                        .iter()
                        .zip(referenced_column_ids)
                        .all(|(local, remote)| row.get(local) == candidate.get(remote))
                });
            if !referenced {
                return Err(Error::InvalidArgument(
                    "catalog vertical foreign key has no referenced row".to_owned(),
                ));
            }
        }

        self.tables.entry(table.id()).or_default().push(row);
        Ok(())
    }

    fn inner_join(
        &self,
        generation: &CatalogGeneration,
        left_table: &str,
        left_column: &str,
        right_table: &str,
        right_column: &str,
    ) -> Result<Vec<(CatalogRow, CatalogRow)>> {
        let left = TableCatalog::load(generation, left_table)?;
        let right = TableCatalog::load(generation, right_table)?;
        let left_column_id = left.column(left_column)?.id();
        let right_column_id = right.column(right_column)?.id();
        let mut joined = Vec::new();
        for left_row in self.tables.get(&left.id()).into_iter().flatten() {
            for right_row in self.tables.get(&right.id()).into_iter().flatten() {
                if left_row.get(&left_column_id) == right_row.get(&right_column_id) {
                    joined.push((left_row.clone(), right_row.clone()));
                }
            }
        }
        Ok(joined)
    }
}

fn same_key(left: &CatalogRow, right: &CatalogRow, column_ids: &[ObjectId]) -> bool {
    column_ids
        .iter()
        .all(|column_id| left.get(column_id) == right.get(column_id))
}

fn object_id(marker: u8) -> ObjectId {
    ObjectId::from_user_bytes([marker; 16]).unwrap()
}

fn empty_catalog() -> CatalogGeneration {
    let namespace = CatalogObject::new(
        ObjectId::BOOTSTRAP_NAMESPACE,
        None,
        None,
        ObjectId::BOOTSTRAP_OWNER,
        CatalogName::new("public").unwrap(),
        1,
        CatalogPayload::Namespace(NamespacePayload::new()),
    )
    .unwrap();
    CatalogGeneration::new(
        CatalogPackMeta::new([1; 16], [2; 16], 1, 1, 1).unwrap(),
        CatalogGraph::build(vec![namespace], vec![]).unwrap(),
    )
}

fn schema_transaction(source: &CatalogGeneration) -> CatalogWalTransaction {
    let mut ddl = DdlTransaction::begin_with_object_ids(
        source,
        (10_u8..80).map(object_id).collect::<Vec<_>>(),
    );
    ddl.stage_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    ddl.stage_sql(
        "CREATE TABLE messages (\
            id INTEGER PRIMARY KEY, \
            sender_id INTEGER NOT NULL REFERENCES users(id), \
            body TEXT NOT NULL\
        )",
    )
    .unwrap();
    ddl.stage_sql("CREATE INDEX messages_sender ON messages(sender_id)")
        .unwrap();
    ddl.stage_sql(
        "CREATE VIEW message_senders AS \
         SELECT messages.id, users.name FROM messages \
         INNER JOIN users ON messages.sender_id = users.id",
    )
    .unwrap();
    CatalogWalTransaction::new(
        CatalogWalTransactionId::from_bytes([90; 16]).unwrap(),
        [3; 16],
        10,
        10_000,
        ddl.commit().unwrap().unwrap(),
    )
    .unwrap()
}

fn assert_join(rows: &CatalogRows, generation: &CatalogGeneration, expected_names: &[&str]) {
    let joined = rows
        .inner_join(generation, "messages", "sender_id", "users", "id")
        .unwrap();
    let users = TableCatalog::load(generation, "users").unwrap();
    let name_id = users.column("name").unwrap().id();
    let names = joined
        .iter()
        .map(|(_, user)| user.get(&name_id).unwrap().as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, expected_names);
}

#[test]
fn ddl_dml_join_checkpoint_reopen_and_snapshot_share_one_catalog_owner() {
    let initial = empty_catalog();
    let transaction = schema_transaction(&initial);
    let mut catalog = CatalogCheckpointHarness::from_generation(&initial).unwrap();
    let committed = catalog
        .append_committed(&transaction, CatalogWalReplayLimits::hard())
        .unwrap();
    let mut rows = CatalogRows::default();
    rows.insert(
        committed.generation(),
        "users",
        &[("id", Value::integer(1)), ("name", Value::text("Ada"))],
    )
    .unwrap();
    rows.insert(
        committed.generation(),
        "users",
        &[("id", Value::integer(2)), ("name", Value::text("Linus"))],
    )
    .unwrap();
    rows.insert(
        committed.generation(),
        "messages",
        &[
            ("id", Value::integer(100)),
            ("sender_id", Value::integer(1)),
            ("body", Value::text("first")),
        ],
    )
    .unwrap();
    rows.insert(
        committed.generation(),
        "messages",
        &[
            ("id", Value::integer(101)),
            ("sender_id", Value::integer(2)),
            ("body", Value::text("second")),
        ],
    )
    .unwrap();
    assert!(rows
        .insert(
            committed.generation(),
            "messages",
            &[
                ("id", Value::integer(102)),
                ("sender_id", Value::integer(99)),
                ("body", Value::text("orphan")),
            ],
        )
        .is_err());
    assert_join(&rows, committed.generation(), &["Ada", "Linus"]);

    catalog.checkpoint(CatalogWalReplayLimits::hard()).unwrap();
    let reopened = catalog.reopen(CatalogWalReplayLimits::hard()).unwrap();
    assert_eq!(reopened.committed_transactions(), 0);
    assert_join(&rows, reopened.generation(), &["Ada", "Linus"]);

    let snapshot = CatalogSnapshot::capture(&catalog, CatalogWalReplayLimits::hard()).unwrap();
    let restored = snapshot.reopen(CatalogWalReplayLimits::hard()).unwrap();
    assert_eq!(snapshot.member_count(), 1);
    assert_join(&rows, restored.generation(), &["Ada", "Linus"]);
}

#[test]
fn every_catalog_wal_prefix_reopens_old_or_complete_new_semantics() {
    let initial = empty_catalog();
    let base = CatalogCheckpointHarness::from_generation(&initial).unwrap();
    let encoded = encode_catalog_wal_transaction(&schema_transaction(&initial)).unwrap();

    for cut in 0..=encoded.len() {
        let harness = CatalogCheckpointHarness::from_persisted(
            base.catalog_pack_bytes().to_vec(),
            encoded[..cut].to_vec(),
            CatalogWalReplayLimits::hard(),
        )
        .unwrap();
        let recovery = harness.reopen(CatalogWalReplayLimits::hard()).unwrap();
        if cut == encoded.len() {
            assert_eq!(recovery.generation().meta().catalog_generation(), 2);
            assert!(TableCatalog::load(recovery.generation(), "messages").is_ok());
        } else {
            assert_eq!(recovery.generation().meta().catalog_generation(), 1);
            assert!(TableCatalog::load(recovery.generation(), "messages").is_err());
        }
    }
}
