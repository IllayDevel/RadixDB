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

// ============================================================================
// Table Sources
// ============================================================================

/// Simple table source
#[derive(Debug, Clone, PartialEq)]
pub struct SimpleTableSource {
    pub token: Token,
    pub name: Identifier,
    pub alias: Option<Identifier>,
    pub as_of: Option<AsOfClause>,
}

impl fmt::Display for SimpleTableSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = self.name.to_string();
        if let Some(ref as_of) = self.as_of {
            result.push_str(&format!(" {}", as_of));
        }
        if let Some(ref alias) = self.alias {
            result.push_str(&format!(" AS {}", alias));
        }
        write!(f, "{}", result)
    }
}

/// AS OF clause for temporal queries
#[derive(Debug, Clone, PartialEq)]
pub struct AsOfClause {
    pub token: Token,
    pub as_of_type: SmartString, // "TRANSACTION" or "TIMESTAMP"
    pub value: Box<Expression>,
}

impl fmt::Display for AsOfClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AS OF {} {}", self.as_of_type, self.value)
    }
}

/// Join table source
#[derive(Debug, Clone, PartialEq)]
pub struct JoinTableSource {
    pub token: Token,
    pub left: Box<Expression>,
    pub join_type: SmartString,
    pub right: Box<Expression>,
    pub condition: Option<Box<Expression>>,
    /// USING clause columns (e.g., USING(id, name))
    pub using_columns: Vec<Identifier>,
}

impl fmt::Display for JoinTableSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = self.left.to_string();
        result.push_str(&format!(" {} JOIN {}", self.join_type, self.right));
        if let Some(ref cond) = self.condition {
            result.push_str(&format!(" ON {}", cond));
        } else if !self.using_columns.is_empty() {
            let cols: Vec<String> = self.using_columns.iter().map(|c| c.to_string()).collect();
            result.push_str(&format!(" USING ({})", cols.join(", ")));
        }
        write!(f, "{}", result)
    }
}

/// Subquery table source
#[derive(Debug, Clone, PartialEq)]
pub struct SubqueryTableSource {
    pub token: Token,
    pub subquery: Box<SelectStatement>,
    pub alias: Option<Identifier>,
}

impl fmt::Display for SubqueryTableSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = format!("({})", self.subquery);
        if let Some(ref alias) = self.alias {
            result.push_str(&format!(" AS {}", alias));
        }
        write!(f, "{}", result)
    }
}

/// VALUES table source (e.g., VALUES (1, 'a'), (2, 'b') AS t(col1, col2))
#[derive(Debug, Clone, PartialEq)]
pub struct ValuesTableSource {
    pub token: Token,
    /// Each row is a list of expressions
    pub rows: Vec<Vec<Expression>>,
    /// Optional alias for the derived table
    pub alias: Option<Identifier>,
    /// Optional column aliases (e.g., t(col1, col2))
    pub column_aliases: Vec<Identifier>,
}

impl fmt::Display for ValuesTableSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = String::from("(VALUES ");
        let rows_str: Vec<String> = self
            .rows
            .iter()
            .map(|row| {
                let values: Vec<String> = row.iter().map(|e| e.to_string()).collect();
                format!("({})", values.join(", "))
            })
            .collect();
        result.push_str(&rows_str.join(", "));
        result.push(')');

        if let Some(ref alias) = self.alias {
            result.push_str(&format!(" AS {}", alias));
            if !self.column_aliases.is_empty() {
                let cols: Vec<String> = self.column_aliases.iter().map(|c| c.to_string()).collect();
                result.push_str(&format!("({})", cols.join(", ")));
            }
        }
        write!(f, "{}", result)
    }
}

/// Function table source (table-valued function in FROM clause)
/// e.g., SELECT * FROM generate_series(1, 10) AS gs(value)
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionTableSource {
    pub token: Token,
    /// Function name
    pub function: Identifier,
    /// Function arguments
    pub arguments: Vec<Expression>,
    /// Optional table alias
    pub alias: Option<Identifier>,
    /// Optional column aliases (e.g., AS gs(value))
    pub column_aliases: Vec<Identifier>,
}

impl fmt::Display for FunctionTableSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.function)?;
        for (i, arg) in self.arguments.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", arg)?;
        }
        write!(f, ")")?;
        if let Some(ref alias) = self.alias {
            write!(f, " AS {}", alias)?;
            if !self.column_aliases.is_empty() {
                let cols: Vec<String> = self.column_aliases.iter().map(|c| c.to_string()).collect();
                write!(f, "({})", cols.join(", "))?;
            }
        }
        Ok(())
    }
}

/// CTE reference
#[derive(Debug, Clone, PartialEq)]
pub struct CteReference {
    pub token: Token,
    pub name: Identifier,
    pub alias: Option<Identifier>,
}

impl fmt::Display for CteReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = self.name.to_string();
        if let Some(ref alias) = self.alias {
            result.push_str(&format!(" AS {}", alias));
        }
        write!(f, "{}", result)
    }
}

// ============================================================================
// ORDER BY
// ============================================================================

/// ORDER BY expression
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpression {
    pub expression: Expression,
    pub ascending: bool,
    /// None = default (NULLS LAST for ASC, NULLS FIRST for DESC in SQL standard)
    /// Some(true) = NULLS FIRST
    /// Some(false) = NULLS LAST
    pub nulls_first: Option<bool>,
}

impl fmt::Display for OrderByExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.ascending {
            write!(f, "{} ASC", self.expression)?;
        } else {
            write!(f, "{} DESC", self.expression)?;
        }
        if let Some(nulls_first) = self.nulls_first {
            if nulls_first {
                write!(f, " NULLS FIRST")?;
            } else {
                write!(f, " NULLS LAST")?;
            }
        }
        Ok(())
    }
}
