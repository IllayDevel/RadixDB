// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! NR-03 schema capability contract for navigable references.

use radixdb::core::{NavigationErrorCode, ReferenceDescriptor, ReferenceTargetKey, SchemaColumnId};
use radixdb::storage::traits::Engine;
use radixdb::{Database, Result};

fn open(name: &str) -> Database {
    Database::open(&format!("memory://navigation_schema_{name}")).unwrap()
}

fn bind_column(db: &Database, table: &str, column: &str) -> Result<SchemaColumnId> {
    let table_id = db.engine().bind_schema_table_id(table)?;
    db.engine().bind_schema_column_id(&table_id, column)
}

fn descriptor(db: &Database, table: &str, column: &str) -> Result<ReferenceDescriptor> {
    let source = bind_column(db, table, column)?;
    db.engine()
        .get_reference_descriptor(&source)?
        .ok_or_else(|| radixdb::Error::invalid_argument("column is not a reference"))
}

fn setup_primary_and_unique_targets(db: &Database) {
    db.execute(
        "CREATE TABLE targets (
            id INTEGER PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            nullable_code TEXT UNIQUE
        )",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE sources (
            id INTEGER PRIMARY KEY,
            target_id INTEGER REFERENCES targets(id),
            target_code TEXT REFERENCES targets(code),
            payload TEXT
        )",
        (),
    )
    .unwrap();
}

#[test]
fn reference_descriptor_proves_primary_and_unique_not_null_targets() -> Result<()> {
    let db = open("eligible_targets");
    setup_primary_and_unique_targets(&db);

    let primary = descriptor(&db, "sources", "target_id")?;
    assert_eq!(primary.target_key(), ReferenceTargetKey::PrimaryKey);
    assert!(primary.source_nullable());
    assert_eq!(primary.source().table().table_name(), "sources");
    assert_eq!(primary.target().table().table_name(), "targets");
    assert_eq!(primary.source().ordinal(), 1);
    assert_eq!(primary.target().ordinal(), 0);
    assert_eq!(primary.schema_generation(), db.engine().schema_epoch());

    let unique = descriptor(&db, "sources", "target_code")?;
    assert_eq!(unique.target_key(), ReferenceTargetKey::UniqueNotNull);
    assert_eq!(unique.source().ordinal(), 2);
    assert_eq!(unique.target().ordinal(), 1);

    let payload = bind_column(&db, "sources", "payload")?;
    assert!(db.engine().get_reference_descriptor(&payload)?.is_none());
    Ok(())
}

#[test]
fn reference_descriptor_rejects_nullable_unique_targets() -> Result<()> {
    let db = open("unsupported_shapes");
    db.execute(
        "CREATE TABLE parents (
            id INTEGER PRIMARY KEY,
            nullable_code TEXT UNIQUE
        )",
        (),
    )?;
    db.execute(
        "CREATE TABLE nullable_child (
            id INTEGER PRIMARY KEY,
            parent_code TEXT REFERENCES parents(nullable_code)
        )",
        (),
    )?;

    let nullable = bind_column(&db, "nullable_child", "parent_code")?;
    let error = db.engine().get_reference_descriptor(&nullable).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("NAVIGATION_UNSUPPORTED_REFERENCE_SHAPE"),
        "{error}"
    );

    // DDL itself prevents two independently declared FKs from publishing one
    // ambiguous source-column identity.
    let ambiguous = db.execute(
        "CREATE TABLE ambiguous_child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES parents(id),
            FOREIGN KEY(parent_id) REFERENCES parents(id)
        )",
        (),
    );
    assert!(ambiguous.is_err());
    Ok(())
}

#[test]
fn schema_generation_and_database_scope_invalidate_bound_ids() -> Result<()> {
    let db = open("generation_owner");
    setup_primary_and_unique_targets(&db);
    let source = bind_column(&db, "sources", "target_id")?;
    let old_generation = source.table().schema_generation();

    db.execute("ALTER TABLE sources ADD COLUMN revision INTEGER", ())?;
    assert!(db.engine().schema_epoch() > old_generation);
    let stale = db.engine().get_reference_descriptor(&source).unwrap_err();
    assert!(stale.to_string().contains("NAVIGATION_SCHEMA_CHANGED"));

    let rebound = descriptor(&db, "sources", "target_id")?;
    assert_eq!(rebound.target_key(), ReferenceTargetKey::PrimaryKey);

    let other = open("other_database_scope");
    let cross_database = other
        .engine()
        .get_reference_descriptor(rebound.source())
        .unwrap_err();
    assert!(cross_database.to_string().contains("cross-database"));
    Ok(())
}

#[test]
fn descriptor_tracks_alter_add_fk_and_source_renames() -> Result<()> {
    let db = open("alter_and_rename");
    db.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ())?;
    db.execute(
        "CREATE TABLE children (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER
        )",
        (),
    )?;

    let before = bind_column(&db, "children", "parent_id")?;
    assert!(db.engine().get_reference_descriptor(&before)?.is_none());

    db.execute(
        "ALTER TABLE children ADD CONSTRAINT
         FOREIGN KEY (parent_id) REFERENCES parents(id)",
        (),
    )?;
    let stale = db.engine().get_reference_descriptor(&before).unwrap_err();
    assert!(stale.to_string().contains("NAVIGATION_SCHEMA_CHANGED"));
    assert_eq!(
        descriptor(&db, "children", "parent_id")?.target_key(),
        ReferenceTargetKey::PrimaryKey
    );

    db.execute(
        "ALTER TABLE children RENAME COLUMN parent_id TO owner_id",
        (),
    )?;
    db.execute("ALTER TABLE children RENAME TO descendants", ())?;
    let renamed = descriptor(&db, "descendants", "owner_id")?;
    assert_eq!(renamed.source().table().table_name(), "descendants");
    assert_eq!(renamed.target().table().table_name(), "parents");
    assert_eq!(renamed.source().ordinal(), 1);
    Ok(())
}

#[test]
fn public_prepared_navigation_rebinds_or_fails_closed_after_schema_change() -> Result<()> {
    let db = open("prepared_schema_generation");
    db.execute(
        "CREATE TABLE prepared_targets (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL
        )",
        (),
    )?;
    db.execute(
        "CREATE TABLE prepared_sources (
            id INTEGER PRIMARY KEY,
            target_id INTEGER REFERENCES prepared_targets(id),
            payload TEXT
        )",
        (),
    )?;
    db.execute("INSERT INTO prepared_targets VALUES (1, 'one')", ())?;
    db.execute("INSERT INTO prepared_sources VALUES (1, 1, 'payload')", ())?;

    let prepared = db.prepare("SELECT target_id.label FROM prepared_sources WHERE id = $1")?;
    let first = prepared
        .query((1,))?
        .map(|row| row.and_then(|row| row.get::<String>(0)))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(first, vec!["one"]);

    db.execute(
        "ALTER TABLE prepared_targets RENAME COLUMN label TO title",
        (),
    )?;
    let stale = match prepared.query((1,)) {
        Err(error) => error,
        Ok(_) => panic!("prepared navigation used a stale target-column binding"),
    };
    assert_eq!(
        stale.navigation_code(),
        Some(NavigationErrorCode::TargetColumnNotFound)
    );

    let rebound = db.prepare("SELECT target_id.title FROM prepared_sources WHERE id = $1")?;
    let current = rebound
        .query((1,))?
        .map(|row| row.and_then(|row| row.get::<String>(0)))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(current, vec!["one"]);

    db.execute("DROP TABLE prepared_sources", ())?;
    db.execute(
        "CREATE TABLE prepared_sources (
            id INTEGER PRIMARY KEY,
            target_id INTEGER,
            payload TEXT
        )",
        (),
    )?;
    db.execute("INSERT INTO prepared_sources VALUES (1, 1, 'plain')", ())?;
    let replaced = match rebound.query((1,)) {
        Err(error) => error,
        Ok(_) => panic!("prepared navigation used a descriptor from the dropped source table"),
    };
    assert_eq!(
        replaced.navigation_code(),
        Some(NavigationErrorCode::NotAReference)
    );
    Ok(())
}

fn logical_descriptor(descriptor: &ReferenceDescriptor) -> (&str, usize, &str, usize) {
    (
        descriptor.source().table().table_name(),
        descriptor.source().ordinal(),
        descriptor.target().table().table_name(),
        descriptor.target().ordinal(),
    )
}

#[test]
fn descriptor_rebuilds_after_wal_replay_checkpoint_and_reopen() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let dsn = format!(
        "file://{}?checkpoint_interval=3600&cleanup_interval=3600&checkpoint_on_close=off",
        directory.path().display()
    );

    {
        let db = Database::open(&dsn)?;
        db.execute("CREATE TABLE durable_parents (id INTEGER PRIMARY KEY)", ())?;
        db.execute(
            "CREATE TABLE durable_children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER
            )",
            (),
        )?;
        db.execute(
            "ALTER TABLE durable_children ADD CONSTRAINT
             FOREIGN KEY (parent_id) REFERENCES durable_parents(id)",
            (),
        )?;
        let current = descriptor(&db, "durable_children", "parent_id")?;
        assert_eq!(
            logical_descriptor(&current),
            ("durable_children", 1, "durable_parents", 0)
        );
        db.close()?;
    }

    // No explicit checkpoint above: this open proves WAL replay rebuilds the
    // runtime-only capability graph from durable schema metadata.
    {
        let db = Database::open(&dsn)?;
        let replayed = descriptor(&db, "durable_children", "parent_id")?;
        assert_eq!(
            logical_descriptor(&replayed),
            ("durable_children", 1, "durable_parents", 0)
        );
        db.execute(
            "ALTER TABLE durable_children RENAME COLUMN parent_id TO owner_id",
            (),
        )?;
        db.execute("ALTER TABLE durable_children RENAME TO durable_links", ())?;
        db.execute("PRAGMA CHECKPOINT", ())?;
        db.close()?;
    }

    {
        let db = Database::open(&dsn)?;
        let reopened = descriptor(&db, "durable_links", "owner_id")?;
        assert_eq!(
            logical_descriptor(&reopened),
            ("durable_links", 1, "durable_parents", 0)
        );
        assert_eq!(reopened.target_key(), ReferenceTargetKey::PrimaryKey);
    }
    Ok(())
}
