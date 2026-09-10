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

//! SHOW and DESCRIBE Commands Tests
//!
//! Tests SHOW TABLES, SHOW CREATE TABLE, SHOW INDEXES, and DESCRIBE commands

use radixdb::{params, Database, DecimalValue};
use radixdb_orm::{
    DatabaseDescriptor, DescriptorEnvelope, DescriptorKind, TableDescriptor,
    SCHEMA_DESCRIPTOR_VERSION,
};

/// Test SHOW TABLES command
#[test]
fn test_show_tables() {
    let db = Database::open("memory://show_tables").expect("Failed to create database");

    // Create a test table
    db.execute(
        "CREATE TABLE test_show (id INTEGER PRIMARY KEY, name TEXT)",
        (),
    )
    .expect("Failed to create table");

    // Create another table
    db.execute("CREATE TABLE another_table (id INTEGER, value FLOAT)", ())
        .expect("Failed to create second table");

    // Execute SHOW TABLES
    let result = db
        .query("SHOW TABLES", ())
        .expect("Failed to execute SHOW TABLES");

    let mut table_names: Vec<String> = Vec::new();
    for row in result {
        let row = row.expect("Failed to get row");
        let table_name: String = row.get(0).unwrap();
        table_names.push(table_name);
    }

    // Verify our tables are in the list
    assert!(
        table_names.contains(&"test_show".to_string()),
        "SHOW TABLES should contain 'test_show', got: {:?}",
        table_names
    );
    assert!(
        table_names.contains(&"another_table".to_string()),
        "SHOW TABLES should contain 'another_table', got: {:?}",
        table_names
    );
}

/// Test SHOW CREATE TABLE command
#[test]
fn test_show_create_table() {
    let db = Database::open("memory://show_create").expect("Failed to create database");

    // Create a test table with multiple columns
    db.execute(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price FLOAT, active BOOLEAN)",
        (),
    )
    .expect("Failed to create table");

    // Execute SHOW CREATE TABLE
    let result = db
        .query("SHOW CREATE TABLE products", ())
        .expect("Failed to execute SHOW CREATE TABLE");

    for row in result {
        let row = row.expect("Failed to get row");
        let table_name: String = row.get(0).unwrap();
        let create_stmt: String = row.get(1).unwrap();

        assert_eq!(table_name, "products", "Table name should be 'products'");

        // Check that the CREATE TABLE statement contains expected parts
        assert!(
            create_stmt.contains("CREATE TABLE"),
            "CREATE TABLE statement should contain 'CREATE TABLE'"
        );
        assert!(
            create_stmt.contains("products"),
            "CREATE TABLE statement should contain table name 'products'"
        );
    }
}

#[test]
fn r6_show_create_preserves_non_primary_auto_increment() {
    let db = Database::open("memory://show_auto_increment").expect("database");
    db.execute(
        "CREATE TABLE generated (seq INTEGER AUTO_INCREMENT, label TEXT)",
        (),
    )
    .unwrap();

    let create_sql: String = db
        .query("SHOW CREATE TABLE generated", ())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .get(1)
        .unwrap();
    assert!(
        create_sql.contains("\"seq\" INTEGER AUTO_INCREMENT"),
        "{create_sql}"
    );

    db.execute("INSERT INTO generated(label) VALUES ('one'), ('two')", ())
        .unwrap();
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(DISTINCT seq) FROM generated", ())
            .unwrap(),
        2
    );
}

/// Test SHOW INDEXES command
#[test]
fn test_show_indexes() {
    let db = Database::open("memory://show_indexes").expect("Failed to create database");

    // Create a test table
    db.execute(
        "CREATE TABLE indexed_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)",
        (),
    )
    .expect("Failed to create table");

    // Create an index
    db.execute("CREATE INDEX idx_name ON indexed_table (name)", ())
        .expect("Failed to create index");

    // Execute SHOW INDEXES
    let result = db
        .query("SHOW INDEXES FROM indexed_table", ())
        .expect("Failed to execute SHOW INDEXES");

    let mut found_our_index = false;
    let mut all_indexes: Vec<String> = Vec::new();
    for row in result {
        let row = row.expect("Failed to get row");
        // First column might be index name or table name depending on format
        let col0: String = row.get(0).unwrap();
        let col1: String = row.get(1).unwrap();
        all_indexes.push(format!("{}:{}", col0, col1));

        // Check if we found our idx_name index in any column
        if col0.contains("idx_name") || col1.contains("idx_name") {
            found_our_index = true;
        }
    }

    assert!(
        found_our_index,
        "SHOW INDEXES should return idx_name index. Got: {:?}",
        all_indexes
    );
}

#[test]
fn test_show_indexes_exposes_partial_predicate() {
    let db = Database::open("memory://show_partial_indexes").expect("Failed to create database");

    db.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL,
            deleted_at INTEGER
        )",
        (),
    )
    .expect("Failed to create table");
    db.execute(
        "CREATE UNIQUE INDEX users_email_active_idx
         ON users (email)
         WHERE deleted_at IS NULL",
        (),
    )
    .expect("Failed to create partial index");

    let result = db
        .query("SHOW INDEXES FROM users", ())
        .expect("Failed to execute SHOW INDEXES");

    let mut found = false;
    for row in result {
        let row = row.expect("Failed to get row");
        let index_name: String = row.get(1).expect("index_name");
        let options: String = row.get(5).expect("options");
        if index_name == "users_email_active_idx" {
            found = true;
            let options_lower = options.to_lowercase();
            assert!(
                options_lower.contains("where=") && options_lower.contains("deleted_at is null"),
                "partial predicate must be exposed in options, got: {options}"
            );
        }
    }

    assert!(found, "partial index should be listed by SHOW INDEXES");
}

#[test]
fn r6_show_indexes_exposes_hnsw_search_budget() {
    let db = Database::open("memory://show_hnsw_options").expect("database");
    db.execute(
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, embedding VECTOR(3))",
        (),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX docs_embedding_idx ON docs(embedding) USING HNSW \
         WITH (m = 12, ef_construction = 96, ef_search = 37)",
        (),
    )
    .unwrap();

    for row in db.query("SHOW INDEXES FROM docs", ()).unwrap() {
        let row = row.unwrap();
        let name: String = row.get(1).unwrap();
        if name == "docs_embedding_idx" {
            let options: String = row.get(5).unwrap();
            assert!(options.contains("ef_search=37"), "{options}");
            return;
        }
    }
    panic!("HNSW index was not listed");
}

/// Test SHOW TABLES on empty database
#[test]
fn test_show_tables_empty() {
    let db = Database::open("memory://show_tables_empty").expect("Failed to create database");

    // Execute SHOW TABLES on empty database
    let result = db
        .query("SHOW TABLES", ())
        .expect("Failed to execute SHOW TABLES");

    let mut count = 0;
    for _ in result {
        count += 1;
    }

    // Should return zero tables
    assert_eq!(
        count, 0,
        "SHOW TABLES on empty database should return 0 tables"
    );
}

/// Test SHOW TABLES after DROP TABLE
#[test]
fn test_show_tables_after_drop() {
    let db = Database::open("memory://show_after_drop").expect("Failed to create database");

    // Create two tables
    db.execute("CREATE TABLE table1 (id INTEGER)", ())
        .expect("Failed to create table1");
    db.execute("CREATE TABLE table2 (id INTEGER)", ())
        .expect("Failed to create table2");

    // Drop one table
    db.execute("DROP TABLE table1", ())
        .expect("Failed to drop table1");

    // SHOW TABLES should only show table2
    let result = db
        .query("SHOW TABLES", ())
        .expect("Failed to execute SHOW TABLES");

    let mut table_names: Vec<String> = Vec::new();
    for row in result {
        let row = row.expect("Failed to get row");
        let name: String = row.get(0).unwrap();
        table_names.push(name);
    }

    assert!(
        !table_names.contains(&"table1".to_string()),
        "SHOW TABLES should not contain dropped table 'table1'"
    );
    assert!(
        table_names.contains(&"table2".to_string()),
        "SHOW TABLES should contain 'table2'"
    );
}

/// Test DESCRIBE command - basic table structure
#[test]
fn test_describe_basic() {
    let db = Database::open("memory://describe_basic").expect("Failed to create database");

    // Create a test table with various column types
    db.execute(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price FLOAT, active BOOLEAN)",
        (),
    )
    .expect("Failed to create table");

    // Execute DESCRIBE
    let result = db
        .query("DESCRIBE products", ())
        .expect("Failed to execute DESCRIBE");

    let mut rows_data: Vec<(String, String, String, String)> = Vec::new();
    for row in result {
        let row = row.expect("Failed to get row");
        let field: String = row.get(0).unwrap();
        let type_name: String = row.get(1).unwrap();
        let nullable: String = row.get(2).unwrap();
        let key: String = row.get(3).unwrap();
        rows_data.push((field, type_name, nullable, key));
    }

    // Should have 4 columns
    assert_eq!(
        rows_data.len(),
        4,
        "DESCRIBE should return 4 rows for 4 columns"
    );

    // Check id column (PRIMARY KEY, NOT NULL)
    assert_eq!(rows_data[0].0, "id", "First column should be 'id'");
    assert_eq!(rows_data[0].1, "INTEGER", "id should be INTEGER type");
    assert_eq!(rows_data[0].2, "NO", "id should not be nullable");
    assert_eq!(rows_data[0].3, "PRI", "id should be primary key");

    // Check name column (NOT NULL)
    assert_eq!(rows_data[1].0, "name", "Second column should be 'name'");
    assert_eq!(rows_data[1].1, "TEXT", "name should be TEXT type");
    assert_eq!(rows_data[1].2, "NO", "name should not be nullable");

    // Check price column (nullable)
    assert_eq!(rows_data[2].0, "price", "Third column should be 'price'");
    assert_eq!(rows_data[2].1, "FLOAT", "price should be FLOAT type");
    assert_eq!(rows_data[2].2, "YES", "price should be nullable");

    // Check active column (nullable)
    assert_eq!(rows_data[3].0, "active", "Fourth column should be 'active'");
    assert_eq!(rows_data[3].1, "BOOLEAN", "active should be BOOLEAN type");
    assert_eq!(rows_data[3].2, "YES", "active should be nullable");
}

#[test]
fn orm_02_describe_json_publishes_complete_versioned_catalog_without_changing_legacy_shape() {
    let db = Database::open("memory://orm_02_describe_json").expect("open database");
    db.execute(
        "CREATE TABLE departments (
            id UUID PRIMARY KEY,
            code TEXT UNIQUE NOT NULL,
            active BOOLEAN DEFAULT TRUE CHECK (active = TRUE)
        )",
        (),
    )
    .expect("create departments");
    db.execute(
        "CREATE TABLE people (
            id UUID PRIMARY KEY,
            department_id UUID REFERENCES departments(id) ON DELETE RESTRICT,
            name TEXT NOT NULL,
            salary DECIMAL(18, 2),
            CHECK (name <> '')
        )",
        (),
    )
    .expect("create people");
    db.execute(
        "CREATE INDEX idx_people_name ON people (name) USING HASH WHERE name <> 'hidden'",
        (),
    )
    .expect("create partial index");
    db.execute(
        "CREATE VIEW people_names AS SELECT id, name FROM people",
        (),
    )
    .expect("create view");

    let table_json: String = db
        .query("DESCRIBE TABLE people FORMAT JSON", ())
        .expect("describe table JSON")
        .next()
        .expect("descriptor row")
        .expect("descriptor row result")
        .get(0)
        .expect("descriptor JSON value");
    let table =
        DescriptorEnvelope::<TableDescriptor>::from_json(&table_json, DescriptorKind::Table)
            .expect("decode table descriptor");
    assert_eq!(table.descriptor, SCHEMA_DESCRIPTOR_VERSION);
    assert_eq!(table.payload.name, "people");
    assert_eq!(table.payload.columns.len(), 4);
    assert_eq!(
        table.payload.columns[3].data_type,
        radixdb_orm::DataTypeDescriptor::Decimal {
            precision: Some(18),
            scale: Some(2),
        }
    );
    assert_ne!(table.payload.catalog_id, uuid::Uuid::nil().to_string());
    assert_eq!(
        table.payload.computed_fingerprint().unwrap(),
        table.payload.fingerprint
    );
    assert!(table
        .payload
        .constraints
        .iter()
        .any(|constraint| constraint.name == "pk_people"));
    assert!(table
        .payload
        .constraints
        .iter()
        .any(|constraint| constraint.name == "fk_people_department_id___departments"));
    assert!(table
        .payload
        .indexes
        .iter()
        .any(|index| index.name == "idx_people_name" && index.predicate.is_some()));
    assert_eq!(table.to_json().unwrap(), table_json);

    let create_sql: String = db
        .query("SHOW CREATE TABLE people", ())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .get(1)
        .unwrap();
    assert!(create_sql.contains("DECIMAL(18,2)"), "{create_sql}");

    let database_json: String = db
        .query("DESCRIBE DATABASE FORMAT JSON", ())
        .expect("describe database JSON")
        .next()
        .expect("database descriptor row")
        .expect("database descriptor row result")
        .get(0)
        .expect("database descriptor JSON value");
    let database = DescriptorEnvelope::<DatabaseDescriptor>::from_json(
        &database_json,
        DescriptorKind::Database,
    )
    .expect("decode database descriptor");
    assert_eq!(
        database.payload.computed_fingerprint().unwrap(),
        database.payload.fingerprint
    );
    assert_eq!(
        database
            .payload
            .tables
            .iter()
            .map(|table| table.name.as_str())
            .collect::<Vec<_>>(),
        vec!["departments", "people"]
    );
    assert_eq!(database.payload.views.len(), 1);
    assert_eq!(database.payload.views[0].name, "people_names");
    assert_eq!(database.payload.views[0].result_columns.len(), 2);

    let legacy = db.query("DESCRIBE people", ()).expect("legacy describe");
    assert_eq!(legacy.columns().len(), 6);
}

#[test]
fn orm_02_view_descriptor_and_query_survive_checkpoint_reopen() {
    let directory = tempfile::tempdir().expect("temp dir");
    let dsn = format!(
        "file://{}",
        directory.path().join("view-descriptor").display()
    );
    let before = {
        let db = Database::open(&dsn).expect("open view fixture");
        db.execute(
            "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            (),
        )
        .unwrap();
        db.execute("INSERT INTO people VALUES (1, 'Alice')", ())
            .unwrap();
        db.execute(
            "CREATE VIEW people_names AS SELECT id, name FROM people",
            (),
        )
        .unwrap();
        let json = db
            .query_one::<String, _>("DESCRIBE DATABASE FORMAT JSON", ())
            .unwrap();
        db.execute("PRAGMA CHECKPOINT", ()).unwrap();
        db.close().unwrap();
        json
    };

    let db = Database::open(&dsn).expect("reopen view fixture");
    let after = db
        .query_one::<String, _>("DESCRIBE DATABASE FORMAT JSON", ())
        .unwrap();
    assert_eq!(after, before, "view descriptor changed after recovery");
    let descriptor = DatabaseDescriptor::from_json(&after).unwrap();
    assert_eq!(descriptor.views.len(), 1);
    assert_eq!(descriptor.views[0].dependencies, vec!["people"]);
    assert_eq!(descriptor.views[0].result_columns.len(), 2);
    assert_eq!(
        db.query_one::<String, _>("SELECT name FROM people_names WHERE id = 1", ())
            .unwrap(),
        "Alice"
    );
}

#[test]
fn orm_02_decimal_type_parameters_are_executable_and_enforced() {
    let db = Database::open("memory://orm_02_decimal_parameters").expect("open database");
    db.execute(
        "CREATE TABLE amounts (id INTEGER PRIMARY KEY, amount DECIMAL(5, 2) NOT NULL)",
        (),
    )
    .expect("create parameterized DECIMAL column");

    db.execute(
        "INSERT INTO amounts VALUES ($1, $2)",
        params![1_i64, DecimalValue::try_new(99999, 5, 2).unwrap()],
    )
    .expect("999.99 fits DECIMAL(5,2)");
    db.execute(
        "INSERT INTO amounts VALUES ($1, $2)",
        params![2_i64, DecimalValue::try_new(120, 3, 3).unwrap()],
    )
    .expect("0.120 is exactly representable as 0.12");

    let precision_error = db
        .execute(
            "INSERT INTO amounts VALUES ($1, $2)",
            params![3_i64, DecimalValue::try_new(100000, 6, 2).unwrap()],
        )
        .unwrap_err();
    assert!(precision_error.to_string().contains("declared precision"));
    let scale_error = db
        .execute(
            "INSERT INTO amounts VALUES ($1, $2)",
            params![4_i64, DecimalValue::try_new(123, 3, 3).unwrap()],
        )
        .unwrap_err();
    assert!(scale_error.to_string().contains("declared scale"));
    assert_eq!(
        db.query_one::<i64, _>("SELECT COUNT(*) FROM amounts", ())
            .unwrap(),
        2
    );
}

/// Test DESC shorthand command
#[test]
fn test_desc_shorthand() {
    let db = Database::open("memory://desc_shorthand").expect("Failed to create database");

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ())
        .expect("Failed to create table");

    // Execute DESC (shorthand for DESCRIBE)
    let result = db.query("DESC t", ()).expect("Failed to execute DESC");

    let mut count = 0;
    for row in result {
        let row = row.expect("Failed to get row");
        let _field: String = row.get(0).unwrap();
        count += 1;
    }

    assert_eq!(count, 2, "DESC should return 2 rows for 2 columns");
}

/// Test DESCRIBE TABLE syntax (with optional TABLE keyword)
#[test]
fn test_describe_table_syntax() {
    let db = Database::open("memory://describe_table").expect("Failed to create database");

    db.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)",
        (),
    )
    .expect("Failed to create table");

    // Execute DESCRIBE TABLE (with TABLE keyword)
    let result = db
        .query("DESCRIBE TABLE users", ())
        .expect("Failed to execute DESCRIBE TABLE");

    let mut fields: Vec<String> = Vec::new();
    for row in result {
        let row = row.expect("Failed to get row");
        let field: String = row.get(0).unwrap();
        fields.push(field);
    }

    assert_eq!(fields.len(), 2, "DESCRIBE TABLE should return 2 rows");
    assert_eq!(fields[0], "id", "First column should be 'id'");
    assert_eq!(fields[1], "email", "Second column should be 'email'");
}

/// Test DESCRIBE with default values
#[test]
fn test_describe_with_defaults() {
    let db = Database::open("memory://describe_defaults").expect("Failed to create database");

    db.execute(
        "CREATE TABLE config (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'unnamed', value INTEGER DEFAULT 0)",
        (),
    )
    .expect("Failed to create table");

    let result = db
        .query("DESCRIBE config", ())
        .expect("Failed to execute DESCRIBE");

    let mut defaults: Vec<String> = Vec::new();
    for row in result {
        let row = row.expect("Failed to get row");
        let default_val: String = row.get(4).unwrap(); // Default column is at index 4
        defaults.push(default_val);
    }

    assert_eq!(defaults.len(), 3, "DESCRIBE should return 3 rows");
    // id has no default (it's PRIMARY KEY)
    assert!(defaults[0].is_empty(), "id should have no default");
    // name has default 'unnamed'
    assert!(
        defaults[1].contains("unnamed"),
        "name should have default 'unnamed', got: {}",
        defaults[1]
    );
    // value has default 0
    assert!(
        defaults[2].contains("0"),
        "value should have default 0, got: {}",
        defaults[2]
    );
}

#[test]
fn r6_describe_uses_one_transaction_visible_catalog_snapshot() {
    let db = Database::open("memory://describe_transaction_snapshot").expect("database");
    db.execute(
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, embedding VECTOR(3), tag TEXT)",
        (),
    )
    .unwrap();
    db.execute("BEGIN", ()).unwrap();
    db.execute("CREATE UNIQUE INDEX docs_tag_uq ON docs(tag)", ())
        .unwrap();

    let rows = db
        .query("DESCRIBE docs", ())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let embedding = rows
        .iter()
        .find(|row| row.get::<String>(0).unwrap() == "embedding")
        .unwrap();
    assert_eq!(embedding.get::<String>(1).unwrap(), "VECTOR(3)");
    let tag = rows
        .iter()
        .find(|row| row.get::<String>(0).unwrap() == "tag")
        .unwrap();
    assert_eq!(tag.get::<String>(3).unwrap(), "UNI");

    db.execute("ROLLBACK", ()).unwrap();
}

/// Test DESCRIBE returns correct column headers
#[test]
fn test_describe_column_headers() {
    let db = Database::open("memory://describe_headers").expect("Failed to create database");

    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", ())
        .expect("Failed to create table");

    let result = db
        .query("DESCRIBE t", ())
        .expect("Failed to execute DESCRIBE");

    // Get column names from result
    let columns = result.columns();

    assert_eq!(columns.len(), 6, "DESCRIBE should return 6 columns");
    assert_eq!(columns[0], "Field", "First column header should be 'Field'");
    assert_eq!(columns[1], "Type", "Second column header should be 'Type'");
    assert_eq!(columns[2], "Null", "Third column header should be 'Null'");
    assert_eq!(columns[3], "Key", "Fourth column header should be 'Key'");
    assert_eq!(
        columns[4], "Default",
        "Fifth column header should be 'Default'"
    );
    assert_eq!(columns[5], "Extra", "Sixth column header should be 'Extra'");
}
