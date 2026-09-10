use std::sync::Arc;

use radixdb_catalog::{
    decode_catalog_pack, encode_catalog_pack, AccessMethod, CatalogGeneration, CatalogGraph,
    CatalogName, CatalogObject, CatalogPackMeta, CatalogPayload, ConstraintPayload,
    HnswDistanceMetric, NamespacePayload, ObjectId, ObjectKind, ViewPayload,
};
use radixdb_core::Error;

use super::{DdlTransaction, DdlTransactionState};

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

#[test]
fn shared_transaction_and_savepoint_pin_the_immutable_generation() {
    let source = Arc::new(empty_catalog());
    let transaction = DdlTransaction::begin_shared(Arc::clone(&source));

    // External owner plus transaction source and working pins all reference
    // the same immutable generation. A savepoint clone adds two Arc pins and
    // must not rebuild the catalog graph or its lookup maps.
    assert_eq!(Arc::strong_count(&source), 3);
    let savepoint = transaction.clone();
    assert_eq!(Arc::strong_count(&source), 5);
    assert!(transaction.pending_mutation().unwrap().is_none());
    assert!(savepoint.pending_mutation().unwrap().is_none());
    drop(savepoint);
    assert_eq!(Arc::strong_count(&source), 3);
}

#[test]
fn create_table_binds_to_one_typed_atomic_mutation_set() {
    let source = empty_catalog();
    let table_id = object_id(10);
    let id_column = object_id(11);
    let body_column = object_id(12);
    let mut transaction =
        DdlTransaction::begin_with_object_ids(&source, [table_id, id_column, body_column]);

    transaction
        .stage_sql("CREATE TABLE messages (id INTEGER, body TEXT)")
        .unwrap();
    assert!(source
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, "messages")
        .unwrap()
        .is_none());
    let private_table = transaction
        .working_generation()
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, "MESSAGES")
        .unwrap()
        .unwrap();
    assert_eq!(private_table.id(), table_id);
    assert_eq!(private_table.kind(), ObjectKind::Table);
    assert_eq!(
        transaction
            .working_generation()
            .graph()
            .children(table_id)
            .count(),
        2
    );

    let mutation = transaction.commit().unwrap().unwrap();
    assert_eq!(transaction.state(), DdlTransactionState::Committed);
    let committed = mutation.apply(&source).unwrap();
    assert_eq!(
        committed.object(table_id).unwrap().kind(),
        ObjectKind::Table
    );
    assert_eq!(committed.children(table_id).count(), 2);
}

#[test]
fn staged_statements_share_private_visibility_and_commit_once() {
    let source = empty_catalog();
    let first_table = object_id(20);
    let first_column = object_id(21);
    let second_table = object_id(22);
    let second_column = object_id(23);
    let mut transaction = DdlTransaction::begin_with_object_ids(
        &source,
        [first_table, first_column, second_table, second_column],
    );
    transaction
        .stage_sql("CREATE TABLE first_table (id INTEGER)")
        .unwrap();
    transaction
        .stage_sql("CREATE TABLE second_table (id INTEGER)")
        .unwrap();
    assert!(matches!(
        transaction.stage_sql("CREATE TABLE FIRST_TABLE (id INTEGER)"),
        Err(Error::TableAlreadyExists(name)) if name == "FIRST_TABLE"
    ));

    let mutation = transaction.commit().unwrap().unwrap();
    assert_eq!(mutation.expected_catalog_generation(), 1);
    assert_eq!(mutation.mutations().len(), 4);
    let committed = mutation.apply(&source).unwrap();
    assert!(committed.object(first_table).is_some());
    assert!(committed.object(second_table).is_some());
    assert_eq!(transaction.commit(), Err(Error::TransactionCommitted));
}

#[test]
fn create_rename_is_coalesced_and_create_drop_is_a_noop() {
    let source = empty_catalog();
    let mut renamed =
        DdlTransaction::begin_with_object_ids(&source, [object_id(30), object_id(31)]);
    renamed
        .stage_sql("CREATE TABLE temporary_name (id INTEGER)")
        .unwrap();
    renamed
        .stage_sql("ALTER TABLE temporary_name RENAME TO final_name")
        .unwrap();
    let mutation = renamed.commit().unwrap().unwrap();
    assert_eq!(mutation.mutations().len(), 2);
    let committed = CatalogGeneration::new(source.meta(), mutation.apply(&source).unwrap());
    assert!(committed
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, "temporary_name")
        .unwrap()
        .is_none());
    assert!(committed
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, "final_name")
        .unwrap()
        .is_some());

    let mut cancelled =
        DdlTransaction::begin_with_object_ids(&source, [object_id(40), object_id(41)]);
    cancelled
        .stage_sql("CREATE TABLE discarded (id INTEGER)")
        .unwrap();
    cancelled.stage_sql("DROP TABLE discarded").unwrap();
    assert!(cancelled.commit().unwrap().is_none());
}

#[test]
fn rollback_and_if_exists_paths_publish_nothing() {
    let source = empty_catalog();
    let mut transaction =
        DdlTransaction::begin_with_object_ids(&source, [object_id(50), object_id(51)]);
    transaction
        .stage_sql("CREATE TABLE private_table (id INTEGER)")
        .unwrap();
    transaction.rollback().unwrap();
    assert_eq!(transaction.state(), DdlTransactionState::RolledBack);
    assert!(transaction
        .working_generation()
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, "private_table")
        .unwrap()
        .is_none());
    assert_eq!(
        transaction.stage_sql("DROP TABLE IF EXISTS absent"),
        Err(Error::TransactionEnded)
    );

    let mut noop = DdlTransaction::begin(&source);
    noop.stage_sql("DROP TABLE IF EXISTS absent").unwrap();
    assert!(noop.commit().unwrap().is_none());
}

#[test]
fn adapter_uses_sql_error_codes_and_rejects_unowned_semantics() {
    let source = empty_catalog();
    let mut transaction =
        DdlTransaction::begin_with_object_ids(&source, [object_id(60), object_id(61)]);
    let missing = transaction.stage_sql("DROP TABLE absent").unwrap_err();
    assert_eq!(missing.code().as_str(), "TABLE_NOT_FOUND");
    transaction
        .stage_sql("CREATE TABLE constrained (id INTEGER PRIMARY KEY)")
        .unwrap();
    transaction
        .stage_sql("CREATE INDEX constrained_idx ON constrained(id)")
        .unwrap();
    let query = transaction.stage_sql("SELECT 1").unwrap_err();
    assert_eq!(query.code().as_str(), "NOT_SUPPORTED");
    let invalid = transaction.stage_sql("CREATE TABLE").unwrap_err();
    assert_eq!(invalid.code().as_str(), "PARSE_ERROR");
}

#[test]
fn table_column_and_constraint_graph_is_visible_in_private_generation() {
    let source = empty_catalog();
    let mut transaction = DdlTransaction::begin(&source);
    transaction
        .stage_sql(
            "CREATE TABLE parents (\
                id INTEGER PRIMARY KEY AUTO_INCREMENT, \
                code TEXT UNIQUE, \
                value INTEGER NOT NULL DEFAULT 1 CHECK (value > 0), \
                CHECK (id > 0)\
            )",
        )
        .unwrap();
    transaction
        .stage_sql(
            "CREATE TABLE children (\
                parent_id INTEGER REFERENCES parents(id) ON DELETE CASCADE, \
                parent_code TEXT, \
                UNIQUE(parent_id, parent_code), \
                FOREIGN KEY(parent_code) REFERENCES parents(code)\
            )",
        )
        .unwrap();

    let parents = transaction.table("PARENTS").unwrap();
    assert_eq!(parents.columns().count(), 3);
    assert_eq!(parents.constraints().count(), 5);
    let id = parents.column("ID").unwrap();
    let CatalogPayload::Column(id_payload) = id.payload() else {
        panic!("id must be a column")
    };
    assert!(!id_payload.nullable());
    assert!(id_payload.auto_increment());
    let value = parents.column("value").unwrap();
    let CatalogPayload::Column(value_payload) = value.payload() else {
        panic!("value must be a column")
    };
    assert!(!value_payload.nullable());
    assert_eq!(value_payload.default_sql().unwrap().as_str(), "1");
    let primary_id = parents.primary_key().unwrap().id();

    let children = transaction.table("children").unwrap();
    assert_eq!(children.columns().count(), 2);
    assert_eq!(children.constraints().count(), 3);
    let parent_fks = children
        .foreign_keys()
        .map(|(_, payload)| payload)
        .collect::<Vec<_>>();
    assert_eq!(parent_fks.len(), 2);
    assert!(parent_fks.iter().all(|payload| matches!(
        payload,
        ConstraintPayload::ForeignKey {
            referenced_table_id,
            ..
        } if *referenced_table_id == parents.id()
    )));
    let parent_table_id = parents.id();

    transaction
        .stage_sql("ALTER TABLE parents RENAME TO accounts")
        .unwrap();
    let accounts = transaction.table("accounts").unwrap();
    assert_eq!(accounts.id(), parent_table_id);
    assert_eq!(accounts.primary_key().unwrap().id(), primary_id);
    let account_table_id = accounts.id();
    assert_eq!(
        transaction.table("parents").unwrap_err().code().as_str(),
        "TABLE_NOT_FOUND"
    );

    let mutation = transaction.commit().unwrap().unwrap();
    let committed = CatalogGeneration::new(source.meta(), mutation.apply(&source).unwrap());
    let committed_children = super::TableCatalog::load(&committed, "children").unwrap();
    assert!(committed_children
        .foreign_keys()
        .all(|(_, payload)| matches!(
            payload,
            ConstraintPayload::ForeignKey {
                referenced_table_id,
                ..
            } if *referenced_table_id == account_table_id
        )));
}

#[test]
fn self_reference_and_constraint_declaration_order_use_stable_ids() {
    let source = empty_catalog();
    let mut transaction = DdlTransaction::begin(&source);
    transaction
        .stage_sql(
            "CREATE TABLE nodes (\
                id INTEGER PRIMARY KEY, \
                parent_id INTEGER, \
                FOREIGN KEY(parent_id) REFERENCES nodes(id), \
                UNIQUE(parent_id)\
            )",
        )
        .unwrap();
    let nodes = transaction.table("nodes").unwrap();
    let primary_column = nodes.column("id").unwrap().id();
    let foreign_key = nodes.foreign_keys().next().unwrap().1;
    assert!(matches!(
        foreign_key,
        ConstraintPayload::ForeignKey {
            referenced_table_id,
            referenced_column_ids,
            ..
        } if *referenced_table_id == nodes.id()
            && referenced_column_ids.as_slice() == [primary_column]
    ));
}

#[test]
fn invalid_constraint_graphs_fail_before_private_publication() {
    let source = empty_catalog();

    let mut unknown_check = DdlTransaction::begin(&source);
    assert_eq!(
        unknown_check
            .stage_sql("CREATE TABLE bad_check (id INTEGER, CHECK (missing > 0))")
            .unwrap_err()
            .code()
            .as_str(),
        "PARSE_ERROR"
    );
    assert!(unknown_check.table("bad_check").is_err());

    let mut unknown_default = DdlTransaction::begin(&source);
    assert_eq!(
        unknown_default
            .stage_sql("CREATE TABLE bad_default (id INTEGER DEFAULT missing)")
            .unwrap_err()
            .code()
            .as_str(),
        "INVALID_ARGUMENT"
    );
    assert!(unknown_default.table("bad_default").is_err());

    let mut duplicate_primary = DdlTransaction::begin(&source);
    assert_eq!(
        duplicate_primary
            .stage_sql("CREATE TABLE bad_pk (id INTEGER PRIMARY KEY, PRIMARY KEY(id))")
            .unwrap_err()
            .code()
            .as_str(),
        "INVALID_ARGUMENT"
    );

    let mut bad_set_null = DdlTransaction::begin(&source);
    bad_set_null
        .stage_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    assert_eq!(
        bad_set_null
            .stage_sql(
                "CREATE TABLE child (parent_id INTEGER NOT NULL REFERENCES parent(id) ON DELETE SET NULL)"
            )
            .unwrap_err()
            .code()
            .as_str(),
        "INVALID_ARGUMENT"
    );

    let mut bad_target = DdlTransaction::begin(&source);
    bad_target
        .stage_sql("CREATE TABLE non_unique (id INTEGER, code TEXT)")
        .unwrap();
    assert_eq!(
        bad_target
            .stage_sql("CREATE TABLE ref_bad (code TEXT REFERENCES non_unique(code))")
            .unwrap_err()
            .code()
            .as_str(),
        "INVALID_ARGUMENT"
    );

    let mut bad_type = DdlTransaction::begin(&source);
    bad_type
        .stage_sql("CREATE TABLE typed_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    assert_eq!(
        bad_type
            .stage_sql("CREATE TABLE typed_child (id TEXT REFERENCES typed_parent(id))")
            .unwrap_err()
            .code()
            .as_str(),
        "INVALID_ARGUMENT"
    );
}

#[test]
fn duplicate_columns_and_noncanonical_catalog_types_fail_before_staging() {
    let source = empty_catalog();
    let mut duplicate = DdlTransaction::begin_with_object_ids(
        &source,
        [object_id(70), object_id(71), object_id(72)],
    );
    assert_eq!(
        duplicate
            .stage_sql("CREATE TABLE bad (id INTEGER, ID TEXT)")
            .unwrap_err(),
        Error::DuplicateColumn
    );
    assert!(duplicate
        .working_generation()
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, "bad")
        .unwrap()
        .is_none());

    let mut decimal = DdlTransaction::begin(&source);
    decimal
        .stage_sql("CREATE TABLE exact_values (value DECIMAL)")
        .unwrap();
    let generation = decimal.working_generation();
    let table = generation
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, "exact_values")
        .unwrap()
        .unwrap();
    let CatalogPayload::Table(table) = table.payload() else {
        panic!("exact_values must remain a table");
    };
    let CatalogPayload::Column(column) = generation
        .object(table.column_ids()[0])
        .expect("DECIMAL column must be present")
        .payload()
    else {
        panic!("exact_values.value must remain a column");
    };
    assert_eq!(column.data_type().parameter_1(), 0);
    assert_eq!(column.data_type().parameter_2(), 0);
    let mut vector = DdlTransaction::begin(&source);
    assert_eq!(
        vector
            .stage_sql("CREATE TABLE bad_vector (value VECTOR)")
            .unwrap_err()
            .code()
            .as_str(),
        "INVALID_ARGUMENT"
    );
}

#[test]
fn prescribed_object_ids_fail_closed_on_collision() {
    let source = empty_catalog();
    let mut existing_collision = DdlTransaction::begin_with_object_ids(
        &source,
        [ObjectId::BOOTSTRAP_NAMESPACE, object_id(81)],
    );
    assert_eq!(
        existing_collision
            .stage_sql("CREATE TABLE collided (id INTEGER)")
            .unwrap_err()
            .code()
            .as_str(),
        "INVALID_ARGUMENT"
    );
    assert!(existing_collision
        .working_generation()
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, "collided")
        .unwrap()
        .is_none());

    let duplicate_id = object_id(82);
    let mut intra_statement_collision =
        DdlTransaction::begin_with_object_ids(&source, [duplicate_id, duplicate_id]);
    assert_eq!(
        intra_statement_collision
            .stage_sql("CREATE TABLE duplicated (id INTEGER)")
            .unwrap_err()
            .code()
            .as_str(),
        "INVALID_ARGUMENT"
    );
    assert!(intra_statement_collision
        .working_generation()
        .find_relation(ObjectId::BOOTSTRAP_NAMESPACE, "duplicated")
        .unwrap()
        .is_none());
}

#[test]
fn constraint_and_explicit_indexes_use_stable_catalog_links() {
    let source = empty_catalog();
    let mut transaction = DdlTransaction::begin(&source);
    transaction
        .stage_sql(
            "CREATE TABLE accounts (id INTEGER PRIMARY KEY, email TEXT UNIQUE, active BOOLEAN)",
        )
        .unwrap();

    let accounts = transaction.table("accounts").unwrap();
    assert_eq!(accounts.indexes().count(), 2);
    let CatalogPayload::Index(unique_email) =
        accounts.index("uq_accounts_email").unwrap().payload()
    else {
        panic!("UNIQUE constraint must own its runtime-named index")
    };
    assert_eq!(unique_email.access_method(), AccessMethod::Hash);
    for constraint in accounts.constraints().filter(|object| {
        matches!(
            object.payload(),
            CatalogPayload::Constraint(
                ConstraintPayload::PrimaryKey { .. } | ConstraintPayload::Unique { .. }
            )
        )
    }) {
        let dependents = transaction
            .working_generation()
            .graph()
            .dependents(constraint.id())
            .collect::<Vec<_>>();
        assert_eq!(dependents.len(), 1);
        let CatalogPayload::Index(index) = dependents[0].payload() else {
            panic!("key constraint dependent must be an index")
        };
        let expected = match constraint.payload() {
            CatalogPayload::Constraint(ConstraintPayload::PrimaryKey { local_column_ids })
            | CatalogPayload::Constraint(ConstraintPayload::Unique { local_column_ids }) => {
                local_column_ids
            }
            _ => unreachable!(),
        };
        assert!(index.unique());
        assert_eq!(index.key_column_ids(), expected);
    }

    transaction
        .stage_sql("CREATE INDEX active_accounts ON accounts(active) WHERE active = TRUE")
        .unwrap();
    let explicit_id = transaction
        .table("accounts")
        .unwrap()
        .index("active_accounts")
        .unwrap()
        .id();
    let CatalogPayload::Index(explicit) = transaction
        .table("accounts")
        .unwrap()
        .index("active_accounts")
        .unwrap()
        .payload()
    else {
        panic!("explicit object must have Index payload")
    };
    assert_eq!(explicit.access_method(), AccessMethod::Bitmap);
    assert_eq!(
        explicit.predicate_sql().unwrap().as_str(),
        "(active = TRUE)"
    );

    transaction
        .stage_sql("ALTER INDEX active_accounts RENAME TO enabled_accounts")
        .unwrap();
    assert_eq!(
        transaction
            .table("accounts")
            .unwrap()
            .index("enabled_accounts")
            .unwrap()
            .id(),
        explicit_id
    );
    assert_eq!(
        transaction
            .stage_sql("DROP INDEX pk_accounts_idx")
            .unwrap_err()
            .code()
            .as_str(),
        "INVALID_ARGUMENT"
    );
    transaction
        .stage_sql("DROP INDEX enabled_accounts ON accounts")
        .unwrap();
    assert!(transaction
        .table("accounts")
        .unwrap()
        .index("enabled_accounts")
        .is_err());
    assert_eq!(transaction.table("accounts").unwrap().indexes().count(), 2);

    let mutation = transaction.commit().unwrap().unwrap();
    let committed = CatalogGeneration::new(source.meta(), mutation.apply(&source).unwrap());
    let accounts = super::TableCatalog::load(&committed, "accounts").unwrap();
    assert_eq!(accounts.indexes().count(), 2);
    assert!(accounts.index("enabled_accounts").is_err());
}

#[test]
fn hnsw_definition_is_complete_bounded_and_reopen_stable() {
    let source = empty_catalog();
    let mut transaction = DdlTransaction::begin(&source);
    transaction
        .stage_sql("CREATE TABLE embeddings (id INTEGER PRIMARY KEY, vector VECTOR(64))")
        .unwrap();
    transaction
        .stage_sql(
            "CREATE INDEX embedding_ann ON embeddings(vector) USING HNSW \
             WITH (m = 24, ef_construction = 300, ef_search = 80, metric = cosine)",
        )
        .unwrap();
    let index = transaction
        .table("embeddings")
        .unwrap()
        .index("embedding_ann")
        .unwrap();
    let CatalogPayload::Index(payload) = index.payload() else {
        panic!("HNSW object must have Index payload")
    };
    let parameters = payload.hnsw_parameters().unwrap();
    assert_eq!(payload.access_method(), AccessMethod::Hnsw);
    assert_eq!(parameters.m(), 24);
    assert_eq!(parameters.ef_construction(), 300);
    assert_eq!(parameters.ef_search(), 80);
    assert_eq!(parameters.distance_metric(), HnswDistanceMetric::Cosine);
    let index_id = index.id();
    let index_payload = index.payload().clone();

    transaction
        .stage_sql(
            "CREATE INDEX IF NOT EXISTS embedding_ann ON embeddings(vector) USING HNSW \
             WITH (m = 24, ef_construction = 300, ef_search = 80, metric = cosine)",
        )
        .unwrap();
    assert_eq!(
        transaction
            .stage_sql(
                "CREATE INDEX IF NOT EXISTS embedding_ann ON embeddings(vector) USING HNSW \
                 WITH (m = 16, ef_construction = 200, ef_search = 64, metric = l2)"
            )
            .unwrap_err()
            .code()
            .as_str(),
        "INDEX_ALREADY_EXISTS"
    );

    let mutation = transaction.commit().unwrap().unwrap();
    let committed = CatalogGeneration::new(source.meta(), mutation.apply(&source).unwrap());
    let reopened = super::TableCatalog::load(&committed, "embeddings")
        .unwrap()
        .index("embedding_ann")
        .unwrap();
    assert_eq!(reopened.id(), index_id);
    assert_eq!(reopened.payload(), &index_payload);
}

#[test]
fn invalid_index_definitions_fail_before_private_publication() {
    let source = empty_catalog();
    let mut transaction = DdlTransaction::begin(&source);
    transaction
        .stage_sql("CREATE TABLE indexed (id INTEGER, value TEXT, vector VECTOR(8))")
        .unwrap();
    for sql in [
        "CREATE INDEX bad_column ON indexed(missing)",
        "CREATE INDEX duplicate_key ON indexed(id, id)",
        "CREATE INDEX bad_hnsw_type ON indexed(id) USING HNSW",
        "CREATE INDEX partial_hnsw ON indexed(vector) USING HNSW WHERE id > 0",
        "CREATE INDEX bad_options ON indexed(id) USING BTREE WITH (m = 16)",
        "CREATE INDEX bad_predicate ON indexed(value) WHERE missing = 1",
        "CREATE UNIQUE INDEX unique_hnsw ON indexed(vector) USING HNSW",
    ] {
        assert!(transaction.stage_sql(sql).is_err(), "{sql} must fail");
    }
    assert_eq!(transaction.table("indexed").unwrap().indexes().count(), 0);
}

#[test]
fn physical_index_state_lives_outside_the_logical_catalog() {
    use radixdb_storage::v6::{
        ArtifactId, ArtifactKind, ArtifactRef, ArtifactSliceRef, CatalogGeneration as PhysicalCg,
        DatabaseGeneration, DatabaseId, IndexSectionKind, IndexSectionRef, ManifestGeneration,
        ManifestId, SegmentDescriptor, SegmentId, SegmentKind, TableManifest,
    };

    let source = empty_catalog();
    let mut transaction = DdlTransaction::begin(&source);
    transaction
        .stage_sql("CREATE TABLE events (id INTEGER, topic TEXT)")
        .unwrap();
    transaction
        .stage_sql("CREATE INDEX events_topic ON events(topic)")
        .unwrap();
    let table = transaction.table("events").unwrap();
    let logical_index = table.index("events_topic").unwrap();

    let generation = DatabaseGeneration::new(1).unwrap();
    let data = ArtifactRef::new(
        ArtifactId::from_bytes([10; 16]).unwrap(),
        ArtifactKind::Data,
        generation,
        312,
        [11; 32],
    )
    .unwrap();
    let physical_index = ArtifactRef::new(
        ArtifactId::from_bytes([12; 16]).unwrap(),
        ArtifactKind::Index,
        generation,
        312,
        [13; 32],
    )
    .unwrap();
    let section = ArtifactSliceRef::new(
        physical_index,
        0,
        IndexSectionKind::KeyDescriptor,
        1,
        256,
        8,
        8,
        1,
        0,
    )
    .unwrap();
    let bound_section = IndexSectionRef::new(logical_index.id(), section);
    let segment = SegmentDescriptor::new(
        SegmentId::from_bytes([14; 16]).unwrap(),
        SegmentKind::Rows,
        1,
        1,
        1,
        1,
        1,
        data,
        Some(physical_index),
    )
    .unwrap();
    let manifest = TableManifest::new(
        DatabaseId::from_bytes([15; 16]).unwrap(),
        table.id(),
        ManifestId::from_bytes([16; 16]).unwrap(),
        ManifestGeneration::new(1).unwrap(),
        PhysicalCg::new(1).unwrap(),
        1,
        2,
        vec![segment],
        1,
    )
    .unwrap();

    assert_eq!(bound_section.logical_index_id(), logical_index.id());
    assert_eq!(
        manifest.segments()[0].index_artifact(),
        Some(physical_index)
    );
    assert!(matches!(logical_index.payload(), CatalogPayload::Index(_)));
}

#[test]
fn views_bind_stable_dependencies_and_reopen_independently_of_serialization_order() {
    let source = empty_catalog();
    let table_id = object_id(140);
    let id_column = object_id(141);
    let name_column = object_id(142);
    let first_view_id = object_id(143);
    let second_view_id = object_id(144);
    let mut transaction = DdlTransaction::begin_with_object_ids(
        &source,
        [
            table_id,
            id_column,
            name_column,
            first_view_id,
            second_view_id,
        ],
    );
    transaction
        .stage_sql("CREATE TABLE accounts (id INTEGER, name TEXT)")
        .unwrap();
    transaction
        .stage_sql(
            "CREATE VIEW account_names AS WITH base AS (SELECT id, name FROM accounts) \
             SELECT id, name FROM base",
        )
        .unwrap();
    transaction
        .stage_sql(
            "CREATE VIEW active_accounts AS \
             SELECT * FROM account_names WHERE id > 0",
        )
        .unwrap();

    let first = transaction.view("ACCOUNT_NAMES").unwrap();
    assert_eq!(first.id(), first_view_id);
    assert_eq!(
        first
            .dependencies()
            .map(CatalogObject::id)
            .collect::<Vec<_>>(),
        vec![table_id]
    );
    assert_ne!(first.payload().output_signature(), &[0; 32]);
    let second = transaction.view("active_accounts").unwrap();
    assert_eq!(second.id(), second_view_id);
    assert_eq!(
        second
            .dependencies()
            .map(CatalogObject::id)
            .collect::<Vec<_>>(),
        vec![first_view_id]
    );

    let mutation = transaction.commit().unwrap().unwrap();
    let graph = mutation.apply(&source).unwrap();
    let canonical_bytes = encode_catalog_pack(source.meta(), &graph).unwrap();
    let mut objects = graph.objects().cloned().collect::<Vec<_>>();
    let mut edges = graph.edges().to_vec();
    objects.reverse();
    edges.reverse();
    let shuffled = CatalogGraph::build(objects, edges).unwrap();
    assert_eq!(
        encode_catalog_pack(source.meta(), &shuffled).unwrap(),
        canonical_bytes
    );

    let reopened = CatalogGeneration::from_pack(decode_catalog_pack(&canonical_bytes).unwrap());
    let first = super::ViewCatalog::load(&reopened, "account_names").unwrap();
    let second = super::ViewCatalog::load(&reopened, "active_accounts").unwrap();
    assert_eq!(first.id(), first_view_id);
    assert_eq!(second.id(), second_view_id);
    assert_eq!(second.dependencies().next().unwrap().id(), first.id());
}

#[test]
fn view_if_not_exists_and_dependency_lifecycle_are_exact() {
    let source = empty_catalog();
    let mut transaction = DdlTransaction::begin(&source);
    transaction
        .stage_sql("CREATE TABLE accounts (id INTEGER, name TEXT)")
        .unwrap();
    transaction
        .stage_sql("CREATE VIEW account_names AS SELECT id, name FROM accounts")
        .unwrap();
    transaction
        .stage_sql("CREATE VIEW IF NOT EXISTS account_names AS SELECT id, name FROM accounts")
        .unwrap();
    assert_eq!(
        transaction
            .stage_sql("CREATE VIEW IF NOT EXISTS account_names AS SELECT name FROM accounts")
            .unwrap_err()
            .code()
            .as_str(),
        "VIEW_ALREADY_EXISTS"
    );
    assert!(transaction.stage_sql("DROP TABLE accounts").is_err());
    assert!(transaction
        .stage_sql("ALTER TABLE accounts RENAME TO renamed_accounts")
        .is_err());

    transaction
        .stage_sql("CREATE VIEW nested_names AS SELECT * FROM account_names")
        .unwrap();
    assert!(transaction.stage_sql("DROP VIEW account_names").is_err());
    transaction.stage_sql("DROP VIEW nested_names").unwrap();
    transaction.stage_sql("DROP VIEW account_names").unwrap();
    transaction.stage_sql("DROP TABLE accounts").unwrap();
    transaction
        .stage_sql("DROP VIEW IF EXISTS absent_view")
        .unwrap();
    assert!(transaction.commit().unwrap().is_none());
}

#[test]
fn invalid_or_stale_view_binding_fails_closed() {
    let source = empty_catalog();
    let mut transaction = DdlTransaction::begin(&source);
    transaction
        .stage_sql("CREATE TABLE accounts (id INTEGER)")
        .unwrap();
    assert_eq!(
        transaction
            .stage_sql("CREATE VIEW missing_source AS SELECT * FROM absent")
            .unwrap_err()
            .code()
            .as_str(),
        "TABLE_OR_VIEW_NOT_FOUND"
    );
    transaction
        .stage_sql("CREATE VIEW account_ids AS SELECT id FROM accounts")
        .unwrap();
    let mutation = transaction.commit().unwrap().unwrap();
    let committed_graph = mutation.apply(&source).unwrap();
    let view_id = committed_graph
        .objects()
        .find(|object| object.kind() == ObjectKind::View)
        .unwrap()
        .id();
    let objects = committed_graph
        .objects()
        .map(|object| {
            if object.id() != view_id {
                return object.clone();
            }
            let CatalogPayload::View(payload) = object.payload() else {
                unreachable!("selected object is a view")
            };
            CatalogObject::new(
                object.id(),
                object.namespace_id(),
                object.parent_id(),
                object.owner_principal_id(),
                object.name().clone(),
                object.definition_revision(),
                CatalogPayload::View(
                    ViewPayload::new(
                        payload.canonical_sql().as_str(),
                        payload.dependency_ids().to_vec(),
                        [0; 32],
                    )
                    .unwrap(),
                ),
            )
            .unwrap()
        })
        .collect();
    let malformed = CatalogGeneration::new(
        source.meta(),
        CatalogGraph::build(objects, committed_graph.edges().to_vec()).unwrap(),
    );
    let error = super::ViewCatalog::load(&malformed, "account_ids").unwrap_err();
    assert!(error.to_string().contains("output signature"));
}

#[test]
fn view_output_signature_tracks_logical_column_shape_not_indexes() {
    fn signature_for(column_type: &str, with_index: bool) -> [u8; 32] {
        let source = empty_catalog();
        let mut transaction = DdlTransaction::begin_with_object_ids(
            &source,
            [
                object_id(150),
                object_id(151),
                object_id(152),
                object_id(153),
            ],
        );
        transaction
            .stage_sql(&format!("CREATE TABLE source_rows (value {column_type})"))
            .unwrap();
        if with_index {
            transaction
                .stage_sql("CREATE INDEX source_value_idx ON source_rows(value)")
                .unwrap();
        }
        transaction
            .stage_sql("CREATE VIEW projected_values AS SELECT value FROM source_rows")
            .unwrap();
        *transaction
            .view("projected_values")
            .unwrap()
            .payload()
            .output_signature()
    }

    let integer = signature_for("INTEGER", false);
    assert_eq!(
        integer,
        [
            0x38, 0x08, 0x3b, 0x58, 0x8a, 0xd5, 0x5e, 0xfb, 0xe1, 0xda, 0x3b, 0xda, 0x34, 0x57,
            0x4e, 0x2e, 0xd2, 0x7f, 0x44, 0x64, 0x22, 0xa6, 0x03, 0xc0, 0xe5, 0xcb, 0xfc, 0xac,
            0x99, 0xf0, 0xee, 0xc8,
        ]
    );
    assert_eq!(integer, signature_for("INTEGER", true));
    assert_ne!(integer, signature_for("TEXT", false));
}
