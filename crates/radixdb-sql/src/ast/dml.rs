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

/// INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub token: Token,
    pub table_name: Identifier,
    pub columns: Vec<Identifier>,
    /// VALUES clause rows (None if using SELECT)
    pub values: Vec<Vec<Expression>>,
    /// SELECT statement for INSERT INTO ... SELECT (None if using VALUES)
    pub select: Option<Box<SelectStatement>>,
    /// ON DUPLICATE KEY UPDATE (MySQL-style) or ON CONFLICT DO UPDATE (PostgreSQL-style)
    pub on_duplicate: bool,
    pub update_columns: Vec<Identifier>,
    pub update_expressions: Vec<Expression>,
    /// ON CONFLICT DO NOTHING (skip duplicates silently)
    pub do_nothing: bool,
    /// Conflict target columns for ON CONFLICT (col1, col2, ...)
    pub conflict_target: Vec<Identifier>,
    /// RETURNING clause expressions
    pub returning: Vec<Expression>,
}

impl fmt::Display for InsertStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = format!("INSERT INTO {}", self.table_name);
        if !self.columns.is_empty() {
            let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
            result.push_str(&format!(" ({})", cols.join(", ")));
        }
        if let Some(ref select) = self.select {
            // INSERT INTO ... SELECT
            result.push_str(&format!(" {}", select));
        } else {
            // INSERT INTO ... VALUES
            result.push_str(" VALUES ");
            let rows: Vec<String> = self
                .values
                .iter()
                .map(|row| {
                    let vals: Vec<String> = row.iter().map(|v| v.to_string()).collect();
                    format!("({})", vals.join(", "))
                })
                .collect();
            result.push_str(&rows.join(", "));
        }
        if self.do_nothing {
            result.push_str(" ON CONFLICT");
            if !self.conflict_target.is_empty() {
                let cols: Vec<String> =
                    self.conflict_target.iter().map(|c| c.to_string()).collect();
                result.push_str(&format!(" ({})", cols.join(", ")));
            }
            result.push_str(" DO NOTHING");
        } else if self.on_duplicate {
            if !self.conflict_target.is_empty() {
                let cols: Vec<String> =
                    self.conflict_target.iter().map(|c| c.to_string()).collect();
                result.push_str(&format!(
                    " ON CONFLICT ({}) DO UPDATE SET ",
                    cols.join(", ")
                ));
            } else {
                result.push_str(" ON DUPLICATE KEY UPDATE ");
            }
            let updates: Vec<String> = self
                .update_columns
                .iter()
                .zip(&self.update_expressions)
                .map(|(col, expr)| format!("{} = {}", col, expr))
                .collect();
            result.push_str(&updates.join(", "));
        }
        if !self.returning.is_empty() {
            let returning: Vec<String> = self.returning.iter().map(|e| e.to_string()).collect();
            result.push_str(&format!(" RETURNING {}", returning.join(", ")));
        }
        write!(f, "{}", result)
    }
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub token: Token,
    pub table_name: Identifier,
    pub updates: FxHashMap<SmartString, Expression>,
    pub where_clause: Option<Box<Expression>>,
    /// RETURNING clause expressions
    pub returning: Vec<Expression>,
}

impl fmt::Display for UpdateStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = format!("UPDATE {} SET ", self.table_name);
        let updates: Vec<String> = self
            .updates
            .iter()
            .map(|(col, val)| format!("{} = {}", col, val))
            .collect();
        result.push_str(&updates.join(", "));
        if let Some(ref where_clause) = self.where_clause {
            result.push_str(&format!(" WHERE {}", where_clause));
        }
        if !self.returning.is_empty() {
            let returning: Vec<String> = self.returning.iter().map(|e| e.to_string()).collect();
            result.push_str(&format!(" RETURNING {}", returning.join(", ")));
        }
        write!(f, "{}", result)
    }
}

/// DELETE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub token: Token,
    pub table_name: Identifier,
    /// Optional table alias (e.g., DELETE FROM users AS u)
    pub alias: Option<Identifier>,
    pub where_clause: Option<Box<Expression>>,
    /// RETURNING clause expressions
    pub returning: Vec<Expression>,
}

impl fmt::Display for DeleteStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = format!("DELETE FROM {}", self.table_name);
        if let Some(ref alias) = self.alias {
            result.push_str(&format!(" AS {}", alias));
        }
        if let Some(ref where_clause) = self.where_clause {
            result.push_str(&format!(" WHERE {}", where_clause));
        }
        if !self.returning.is_empty() {
            let returning: Vec<String> = self.returning.iter().map(|e| e.to_string()).collect();
            result.push_str(&format!(" RETURNING {}", returning.join(", ")));
        }
        write!(f, "{}", result)
    }
}
