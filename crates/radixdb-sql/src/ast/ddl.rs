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

use super::*;

/// Bind one exact already-installed package to the current database.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateExtensionStatement {
    pub token: Token,
    pub name: Identifier,
    pub version: SmartString,
    pub if_not_exists: bool,
}

impl fmt::Display for CreateExtensionStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE EXTENSION ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(
            f,
            "{} VERSION '{}'",
            self.name,
            escape_sql_string(&self.version)
        )
    }
}

/// Remove only an extension binding with no dependent catalog objects.
#[derive(Debug, Clone, PartialEq)]
pub struct DropExtensionStatement {
    pub token: Token,
    pub name: Identifier,
    pub if_exists: bool,
}

impl fmt::Display for DropExtensionStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP EXTENSION ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{} RESTRICT", self.name)
    }
}

/// Bind one installed package type descriptor to a schema-qualified SQL name.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateExternalTypeStatement {
    pub token: Token,
    pub name: ObjectName,
    pub extension_name: Identifier,
    pub local_id: SmartString,
}

impl fmt::Display for CreateExternalTypeStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CREATE TYPE {} FROM EXTENSION {} AS '{}'",
            self.name,
            self.extension_name,
            escape_sql_string(&self.local_id)
        )
    }
}

/// Remove an external SQL type only when it has no dependent objects.
#[derive(Debug, Clone, PartialEq)]
pub struct DropExternalTypeStatement {
    pub token: Token,
    pub name: ObjectName,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QualifiedOperator {
    pub schema: Identifier,
    pub symbol: SmartString,
}

impl fmt::Display for QualifiedOperator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}", self.schema, self.symbol)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateOperatorStatement {
    pub token: Token,
    pub name: QualifiedOperator,
    pub left_argument: Option<ProceduralType>,
    pub right_argument: ProceduralType,
    pub function: RoutineSignatureSyntax,
    pub extension_name: Identifier,
    pub local_id: SmartString,
}

impl fmt::Display for CreateOperatorStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CREATE OPERATOR {} (", self.name)?;
        if let Some(left) = &self.left_argument {
            write!(formatter, "LEFTARG = {left}, ")?;
        }
        write!(
            formatter,
            "RIGHTARG = {}, FUNCTION = {}) FROM EXTENSION {} AS '{}'",
            self.right_argument,
            self.function,
            self.extension_name,
            escape_sql_string(&self.local_id)
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropOperatorStatement {
    pub token: Token,
    pub name: QualifiedOperator,
    pub left_argument: Option<ProceduralType>,
    pub right_argument: Option<ProceduralType>,
    pub if_exists: bool,
}

impl fmt::Display for DropOperatorStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DROP OPERATOR ")?;
        if self.if_exists {
            formatter.write_str("IF EXISTS ")?;
        }
        write!(formatter, "{} (", self.name)?;
        if let Some(left) = &self.left_argument {
            write!(formatter, "{left}")?;
        }
        formatter.write_str(", ")?;
        if let Some(right) = &self.right_argument {
            write!(formatter, "{right}")?;
        }
        formatter.write_str(") RESTRICT")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateOperatorClassStatement {
    pub token: Token,
    pub name: ObjectName,
    pub input_type: ProceduralType,
    pub access_method: IndexMethod,
    pub extension_name: Identifier,
    pub local_id: SmartString,
}

impl fmt::Display for CreateOperatorClassStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "CREATE OPERATOR CLASS {} FOR TYPE {} USING {} FROM EXTENSION {} AS '{}'",
            self.name,
            self.input_type,
            self.access_method,
            self.extension_name,
            escape_sql_string(&self.local_id)
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropOperatorClassStatement {
    pub token: Token,
    pub name: ObjectName,
    pub access_method: IndexMethod,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreatePlannerSupportStatement {
    pub token: Token,
    pub name: ObjectName,
    pub function: RoutineSignatureSyntax,
    pub extension_name: Identifier,
    pub local_id: SmartString,
}

impl fmt::Display for CreatePlannerSupportStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "CREATE PLANNER SUPPORT {} FOR FUNCTION {} FROM EXTENSION {} AS '{}'",
            self.name,
            self.function,
            self.extension_name,
            escape_sql_string(&self.local_id)
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropPlannerSupportStatement {
    pub token: Token,
    pub name: ObjectName,
    pub if_exists: bool,
}

impl fmt::Display for DropPlannerSupportStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DROP PLANNER SUPPORT ")?;
        if self.if_exists {
            formatter.write_str("IF EXISTS ")?;
        }
        write!(formatter, "{} RESTRICT", self.name)
    }
}

impl fmt::Display for DropOperatorClassStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DROP OPERATOR CLASS ")?;
        if self.if_exists {
            formatter.write_str("IF EXISTS ")?;
        }
        write!(
            formatter,
            "{} USING {} RESTRICT",
            self.name, self.access_method
        )
    }
}

impl fmt::Display for DropExternalTypeStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP TYPE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{} RESTRICT", self.name)
    }
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    pub token: Token,
    pub table_name: Identifier,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDefinition>,
    /// Table-level constraints (UNIQUE(cols), CHECK(expr), etc.)
    pub table_constraints: Vec<TableConstraint>,
    /// Optional SELECT statement for CREATE TABLE ... AS SELECT
    pub as_select: Option<Box<SelectStatement>>,
}

impl fmt::Display for CreateTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = String::from("CREATE TABLE ");
        if self.if_not_exists {
            result.push_str("IF NOT EXISTS ");
        }
        if let Some(ref select) = self.as_select {
            result.push_str(&format!("{} AS {}", self.table_name, select));
            return write!(f, "{}", result);
        }
        result.push_str(&format!("{} (", self.table_name));
        let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        result.push_str(&cols.join(", "));
        if !self.table_constraints.is_empty() {
            let constraints: Vec<String> = self
                .table_constraints
                .iter()
                .map(|c| c.to_string())
                .collect();
            result.push_str(", ");
            result.push_str(&constraints.join(", "));
        }
        result.push(')');
        write!(f, "{}", result)
    }
}

/// Column definition
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    pub name: Identifier,
    pub data_type: SmartString,
    pub constraints: Vec<ColumnConstraint>,
}

impl fmt::Display for ColumnDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = format!("{} {}", self.name, self.data_type);
        for constraint in &self.constraints {
            result.push_str(&format!(" {}", constraint));
        }
        write!(f, "{}", result)
    }
}

/// Column constraint
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    NotNull,
    PrimaryKey,
    Unique,
    AutoIncrement,
    Default(Expression),
    Check(Expression),
    References {
        table: Identifier,
        column: Option<Identifier>,
        on_delete: ForeignKeyAction,
        on_update: ForeignKeyAction,
    },
}

impl fmt::Display for ColumnConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnConstraint::NotNull => write!(f, "NOT NULL"),
            ColumnConstraint::PrimaryKey => write!(f, "PRIMARY KEY"),
            ColumnConstraint::Unique => write!(f, "UNIQUE"),
            ColumnConstraint::AutoIncrement => write!(f, "AUTO_INCREMENT"),
            ColumnConstraint::Default(expr) => write!(f, "DEFAULT {}", expr),
            ColumnConstraint::Check(expr) => write!(f, "CHECK ({})", expr),
            ColumnConstraint::References {
                table,
                column,
                on_delete,
                on_update,
            } => {
                write!(f, "REFERENCES {}", table)?;
                if let Some(col) = column {
                    write!(f, "({})", col)?;
                }
                if *on_delete != ForeignKeyAction::Restrict {
                    write!(f, " ON DELETE {}", on_delete)?;
                }
                if *on_update != ForeignKeyAction::Restrict {
                    write!(f, " ON UPDATE {}", on_update)?;
                }
                Ok(())
            }
        }
    }
}

/// Table-level constraint (applied to the table rather than a single column)
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    /// UNIQUE(col1, col2, ...)
    Unique(Vec<Identifier>),
    /// CHECK(expression) - boxed to reduce enum size
    Check(Box<Expression>),
    /// PRIMARY KEY(col1, col2, ...) - composite primary key (not yet fully supported)
    PrimaryKey(Vec<Identifier>),
    /// FOREIGN KEY(col) REFERENCES table(col) ON DELETE ... ON UPDATE ...
    /// Boxed to reduce enum size (Identifier is large)
    ForeignKey(Box<ForeignKeyTableConstraint>),
}

/// Fields for a table-level FOREIGN KEY constraint (boxed to reduce TableConstraint enum size)
#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKeyTableConstraint {
    pub column: Identifier,
    pub ref_table: Identifier,
    pub ref_column: Option<Identifier>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

impl fmt::Display for TableConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableConstraint::Unique(cols) => {
                let col_names: Vec<&str> = cols.iter().map(|c| c.value.as_str()).collect();
                write!(f, "UNIQUE({})", col_names.join(", "))
            }
            TableConstraint::Check(expr) => write!(f, "CHECK({})", expr),
            TableConstraint::PrimaryKey(cols) => {
                let col_names: Vec<&str> = cols.iter().map(|c| c.value.as_str()).collect();
                write!(f, "PRIMARY KEY({})", col_names.join(", "))
            }
            TableConstraint::ForeignKey(fk) => {
                write!(f, "FOREIGN KEY({}) REFERENCES {}", fk.column, fk.ref_table)?;
                if let Some(ref col) = fk.ref_column {
                    write!(f, "({})", col)?;
                }
                if fk.on_delete != ForeignKeyAction::Restrict {
                    write!(f, " ON DELETE {}", fk.on_delete)?;
                }
                if fk.on_update != ForeignKeyAction::Restrict {
                    write!(f, " ON UPDATE {}", fk.on_update)?;
                }
                Ok(())
            }
        }
    }
}

/// Helper enum for parsing - either a column definition or a table constraint
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnOrConstraint {
    Column(ColumnDefinition),
    Constraint(TableConstraint),
}

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStatement {
    pub token: Token,
    pub table_name: Identifier,
    pub if_exists: bool,
}

impl fmt::Display for DropTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = String::from("DROP TABLE ");
        if self.if_exists {
            result.push_str("IF EXISTS ");
        }
        result.push_str(&self.table_name.to_string());
        write!(f, "{}", result)
    }
}

/// TRUNCATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct TruncateStatement {
    pub token: Token,
    pub table_name: Identifier,
}

impl fmt::Display for TruncateStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TRUNCATE TABLE {}", self.table_name)
    }
}

/// VACUUM statement — triggers manual cleanup of deleted rows and index compaction
#[derive(Debug, Clone, PartialEq)]
pub struct VacuumStatement {
    pub token: Token,
    pub table_name: Option<Identifier>,
}

impl fmt::Display for VacuumStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref table_name) = self.table_name {
            write!(f, "VACUUM {}", table_name)
        } else {
            write!(f, "VACUUM")
        }
    }
}

/// Copy format for COPY FROM
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Csv,
    Json,
}

impl fmt::Display for CopyFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CopyFormat::Csv => write!(f, "CSV"),
            CopyFormat::Json => write!(f, "JSON"),
        }
    }
}

/// COPY table [(columns)] FROM 'file_path' [WITH (options)]
#[derive(Debug, Clone, PartialEq)]
pub struct CopyStatement {
    pub token: Token,
    pub table_name: Identifier,
    pub columns: Vec<Identifier>,
    pub file_path: String,
    pub format: CopyFormat,
    pub header: bool,
    pub delimiter: u8,
    pub null_string: Option<String>,
}

impl fmt::Display for CopyStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "COPY {}", self.table_name)?;
        if !self.columns.is_empty() {
            write!(f, " (")?;
            for (i, col) in self.columns.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", col)?;
            }
            write!(f, ")")?;
        }
        write!(f, " FROM '{}'", escape_sql_string(&self.file_path))?;
        write!(f, " WITH (FORMAT {}", self.format)?;
        if self.format == CopyFormat::Csv {
            if self.header {
                write!(f, ", HEADER true")?;
            }
            if self.delimiter != b',' {
                write!(
                    f,
                    ", DELIMITER '{}'",
                    escape_sql_string(&(self.delimiter as char).to_string())
                )?;
            }
        }
        if let Some(ref ns) = self.null_string {
            write!(f, ", NULL '{}'", escape_sql_string(ns))?;
        }
        write!(f, ")")
    }
}

/// ALTER TABLE operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterTableOperation {
    AddColumn,
    AddConstraint,
    DropColumn,
    DropConstraint,
    RenameColumn,
    ModifyColumn,
    RenameTable,
}

/// ALTER TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStatement {
    pub token: Token,
    pub table_name: Identifier,
    pub operation: AlterTableOperation,
    pub column_def: Option<ColumnDefinition>,
    pub table_constraint: Option<TableConstraint>,
    pub column_name: Option<Identifier>,
    pub constraint_name: Option<Identifier>,
    pub if_exists: bool,
    pub new_column_name: Option<Identifier>,
    pub new_table_name: Option<Identifier>,
}

impl fmt::Display for AlterTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = format!("ALTER TABLE {} ", self.table_name);
        match self.operation {
            AlterTableOperation::AddColumn => {
                if let Some(ref col) = self.column_def {
                    result.push_str(&format!("ADD COLUMN {}", col));
                }
            }
            AlterTableOperation::AddConstraint => {
                if let Some(ref constraint) = self.table_constraint {
                    result.push_str(&format!("ADD CONSTRAINT {}", constraint));
                }
            }
            AlterTableOperation::DropColumn => {
                if let Some(ref name) = self.column_name {
                    result.push_str(&format!("DROP COLUMN {}", name));
                }
            }
            AlterTableOperation::DropConstraint => {
                result.push_str("DROP CONSTRAINT ");
                if self.if_exists {
                    result.push_str("IF EXISTS ");
                }
                if let Some(ref name) = self.constraint_name {
                    result.push_str(&name.to_string());
                }
            }
            AlterTableOperation::RenameColumn => {
                if let (Some(ref old), Some(ref new)) = (&self.column_name, &self.new_column_name) {
                    result.push_str(&format!("RENAME COLUMN {} TO {}", old, new));
                }
            }
            AlterTableOperation::ModifyColumn => {
                if let Some(ref col) = self.column_def {
                    result.push_str(&format!("MODIFY COLUMN {}", col));
                }
            }
            AlterTableOperation::RenameTable => {
                if let Some(ref name) = self.new_table_name {
                    result.push_str(&format!("RENAME TO {}", name));
                }
            }
        }
        write!(f, "{}", result)
    }
}

/// ALTER INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct AlterIndexStatement {
    pub token: Token,
    pub index_name: Identifier,
    pub new_index_name: Identifier,
}

impl fmt::Display for AlterIndexStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ALTER INDEX {} RENAME TO {}",
            self.index_name, self.new_index_name
        )
    }
}

/// Index type for USING clause
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMethod {
    /// B-tree index (default for INTEGER, FLOAT, TIMESTAMP) - good for range queries
    BTree,
    /// Hash index (default for TEXT, JSON) - good for equality lookups
    Hash,
    /// Bitmap index (default for BOOLEAN) - good for low-cardinality columns
    Bitmap,
    /// HNSW index - approximate nearest neighbor search for vector columns
    Hnsw,
}

impl fmt::Display for IndexMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexMethod::BTree => write!(f, "BTREE"),
            IndexMethod::Hash => write!(f, "HASH"),
            IndexMethod::Bitmap => write!(f, "BITMAP"),
            IndexMethod::Hnsw => write!(f, "HNSW"),
        }
    }
}

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStatement {
    pub token: Token,
    pub index_name: Identifier,
    pub table_name: Identifier,
    pub columns: Vec<Identifier>,
    pub is_unique: bool,
    pub if_not_exists: bool,
    /// Optional index type from USING clause (None = auto-select based on column type)
    pub index_method: Option<IndexMethod>,
    /// Optional WITH clause for index parameters (e.g., HNSW m, ef_construction, ef_search, metric)
    pub options: Vec<(String, Expression)>,
    /// Optional partial-index predicate from WHERE clause
    pub where_clause: Option<Box<Expression>>,
    /// PostgreSQL-style operator class following the sole key column.
    pub operator_class: Option<ObjectName>,
}

impl fmt::Display for CreateIndexStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = String::from("CREATE ");
        if self.is_unique {
            result.push_str("UNIQUE ");
        }
        result.push_str("INDEX ");
        if self.if_not_exists {
            result.push_str("IF NOT EXISTS ");
        }
        result.push_str(&format!("{} ON {} (", self.index_name, self.table_name));
        let mut cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        if let (Some(first), Some(operator_class)) = (cols.first_mut(), &self.operator_class) {
            first.push(' ');
            first.push_str(&operator_class.to_string());
        }
        result.push_str(&cols.join(", "));
        result.push(')');
        if let Some(method) = &self.index_method {
            result.push_str(&format!(" USING {}", method));
        }
        if !self.options.is_empty() {
            result.push_str(" WITH (");
            for (i, (key, value)) in self.options.iter().enumerate() {
                if i > 0 {
                    result.push_str(", ");
                }
                result.push_str(&format!("{} = {}", key, value));
            }
            result.push(')');
        }
        if let Some(ref where_clause) = self.where_clause {
            result.push_str(&format!(" WHERE {}", where_clause));
        }
        write!(f, "{}", result)
    }
}

/// DROP INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexStatement {
    pub token: Token,
    pub index_name: Identifier,
    pub table_name: Option<Identifier>,
    pub if_exists: bool,
}

impl fmt::Display for DropIndexStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = String::from("DROP INDEX ");
        if self.if_exists {
            result.push_str("IF EXISTS ");
        }
        result.push_str(&self.index_name.to_string());
        if let Some(ref table) = self.table_name {
            result.push_str(&format!(" ON {}", table));
        }
        write!(f, "{}", result)
    }
}

/// CREATE VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStatement {
    pub token: Token,
    pub view_name: Identifier,
    pub query: Box<SelectStatement>,
    pub if_not_exists: bool,
}

impl fmt::Display for CreateViewStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = String::from("CREATE VIEW ");
        if self.if_not_exists {
            result.push_str("IF NOT EXISTS ");
        }
        result.push_str(&format!("{} AS {}", self.view_name, self.query));
        write!(f, "{}", result)
    }
}

/// DROP VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropViewStatement {
    pub token: Token,
    pub view_name: Identifier,
    pub if_exists: bool,
}

impl fmt::Display for DropViewStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = String::from("DROP VIEW ");
        if self.if_exists {
            result.push_str("IF EXISTS ");
        }
        result.push_str(&self.view_name.to_string());
        write!(f, "{}", result)
    }
}
