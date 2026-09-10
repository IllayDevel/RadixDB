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

/// BEGIN statement
#[derive(Debug, Clone, PartialEq)]
pub struct BeginStatement {
    pub token: Token,
    pub isolation_level: Option<SmartString>,
}

impl fmt::Display for BeginStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = String::from("BEGIN TRANSACTION");
        if let Some(ref level) = self.isolation_level {
            result.push_str(&format!(" ISOLATION LEVEL {}", level));
        }
        write!(f, "{}", result)
    }
}

/// COMMIT statement
#[derive(Debug, Clone, PartialEq)]
pub struct CommitStatement {
    pub token: Token,
}

impl fmt::Display for CommitStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "COMMIT")
    }
}

/// ROLLBACK statement
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackStatement {
    pub token: Token,
    pub savepoint_name: Option<Identifier>,
}

impl fmt::Display for RollbackStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref name) = self.savepoint_name {
            write!(f, "ROLLBACK TO SAVEPOINT {}", name)
        } else {
            write!(f, "ROLLBACK")
        }
    }
}

/// SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SavepointStatement {
    pub token: Token,
    pub savepoint_name: Identifier,
}

impl fmt::Display for SavepointStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SAVEPOINT {}", self.savepoint_name)
    }
}

/// RELEASE SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct ReleaseSavepointStatement {
    pub token: Token,
    pub savepoint_name: Identifier,
}

impl fmt::Display for ReleaseSavepointStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RELEASE SAVEPOINT {}", self.savepoint_name)
    }
}

/// SET statement
#[derive(Debug, Clone, PartialEq)]
pub struct SetStatement {
    pub token: Token,
    pub name: Identifier,
    pub value: Expression,
}

impl fmt::Display for SetStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SET {} = {}", self.name, self.value)
    }
}

/// PRAGMA statement
#[derive(Debug, Clone, PartialEq)]
pub struct PragmaStatement {
    pub token: Token,
    pub name: Identifier,
    pub value: Option<Expression>,
}

impl fmt::Display for PragmaStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref value) = self.value {
            write!(f, "PRAGMA {} = {}", self.name, value)
        } else {
            write!(f, "PRAGMA {}", self.name)
        }
    }
}

/// SHOW TABLES statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowTablesStatement {
    pub token: Token,
}

impl fmt::Display for ShowTablesStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SHOW TABLES")
    }
}

/// SHOW VIEWS statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowViewsStatement {
    pub token: Token,
}

impl fmt::Display for ShowViewsStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SHOW VIEWS")
    }
}

/// SHOW CREATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowCreateTableStatement {
    pub token: Token,
    pub table_name: Identifier,
}

impl fmt::Display for ShowCreateTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SHOW CREATE TABLE {}", self.table_name)
    }
}

/// SHOW CREATE VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowCreateViewStatement {
    pub token: Token,
    pub view_name: Identifier,
}

impl fmt::Display for ShowCreateViewStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SHOW CREATE VIEW {}", self.view_name)
    }
}

/// SHOW INDEXES statement
#[derive(Debug, Clone, PartialEq)]
pub struct ShowIndexesStatement {
    pub token: Token,
    pub table_name: Identifier,
}

impl fmt::Display for ShowIndexesStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SHOW INDEXES FROM {}", self.table_name)
    }
}

/// DESCRIBE target.
#[derive(Debug, Clone, PartialEq)]
pub enum DescribeTarget {
    Table(Identifier),
    Database,
}

/// DESCRIBE output representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeFormat {
    Tabular,
    Json,
}

/// DESCRIBE statement - shows table structure or a versioned JSON catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeStatement {
    pub token: Token,
    pub target: DescribeTarget,
    pub format: DescribeFormat,
}

impl fmt::Display for DescribeStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.target, self.format) {
            (DescribeTarget::Table(table), DescribeFormat::Tabular) => {
                write!(f, "DESCRIBE {table}")
            }
            (DescribeTarget::Table(table), DescribeFormat::Json) => {
                write!(f, "DESCRIBE TABLE {table} FORMAT JSON")
            }
            (DescribeTarget::Database, DescribeFormat::Json) => {
                write!(f, "DESCRIBE DATABASE FORMAT JSON")
            }
            (DescribeTarget::Database, DescribeFormat::Tabular) => {
                write!(f, "DESCRIBE DATABASE")
            }
        }
    }
}

/// Expression statement (standalone expression)
#[derive(Debug, Clone, PartialEq)]
pub struct ExpressionStatement {
    pub token: Token,
    pub expression: Expression,
}

impl fmt::Display for ExpressionStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expression)
    }
}

/// EXPLAIN statement
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainStatement {
    pub token: Token,
    pub statement: Box<Statement>,
    pub analyze: bool,
}

impl fmt::Display for ExplainStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.analyze {
            write!(f, "EXPLAIN ANALYZE {}", self.statement)
        } else {
            write!(f, "EXPLAIN {}", self.statement)
        }
    }
}

/// ANALYZE statement for collecting table statistics
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeStatement {
    pub token: Token,
    /// Table name to analyze (None = analyze all tables)
    pub table_name: Option<SmartString>,
}

impl fmt::Display for AnalyzeStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.table_name {
            Some(name) => write!(f, "ANALYZE {}", name),
            None => write!(f, "ANALYZE"),
        }
    }
}
