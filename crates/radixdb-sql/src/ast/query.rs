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
// WITH Clause (CTEs)
// ============================================================================

/// Common Table Expression
#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpression {
    pub token: Token,
    pub name: Identifier,
    pub column_names: Vec<Identifier>,
    pub query: Box<SelectStatement>,
    pub is_recursive: bool,
}

impl fmt::Display for CommonTableExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = self.name.to_string();
        if !self.column_names.is_empty() {
            let cols: Vec<String> = self.column_names.iter().map(|c| c.to_string()).collect();
            result.push_str(&format!("({})", cols.join(", ")));
        }
        result.push_str(&format!(" AS ({})", self.query));
        write!(f, "{}", result)
    }
}

/// WITH clause
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub token: Token,
    pub ctes: Vec<CommonTableExpression>,
    pub is_recursive: bool,
}

impl fmt::Display for WithClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = String::from("WITH ");
        if self.is_recursive {
            result.push_str("RECURSIVE ");
        }
        let cte_strs: Vec<String> = self.ctes.iter().map(|c| c.to_string()).collect();
        result.push_str(&cte_strs.join(", "));
        write!(f, "{}", result)
    }
}

// ============================================================================
// Statement Types
// ============================================================================

/// Set operation type for compound queries
#[derive(Debug, Clone, PartialEq)]
pub enum SetOperationType {
    Union,
    UnionAll,
    Intersect,
    IntersectAll,
    Except,
    ExceptAll,
}

impl fmt::Display for SetOperationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetOperationType::Union => write!(f, "UNION"),
            SetOperationType::UnionAll => write!(f, "UNION ALL"),
            SetOperationType::Intersect => write!(f, "INTERSECT"),
            SetOperationType::IntersectAll => write!(f, "INTERSECT ALL"),
            SetOperationType::Except => write!(f, "EXCEPT"),
            SetOperationType::ExceptAll => write!(f, "EXCEPT ALL"),
        }
    }
}

/// Set operation combining two SELECT statements
#[derive(Debug, Clone, PartialEq)]
pub struct SetOperation {
    pub operation: SetOperationType,
    pub right: Box<SelectStatement>,
}

/// Group by modifier (ROLLUP, CUBE, GROUPING SETS, or none)
#[derive(Debug, Clone, PartialEq, Default)]
pub enum GroupByModifier {
    #[default]
    None,
    Rollup,
    Cube,
    /// GROUPING SETS - each inner Vec is one grouping set
    /// e.g., GROUPING SETS ((a, b), (a), ()) has 3 sets
    GroupingSets(Vec<Vec<Expression>>),
}

/// GROUP BY clause with optional ROLLUP/CUBE modifier
#[derive(Debug, Clone, PartialEq, Default)]
pub struct GroupByClause {
    pub columns: Vec<Expression>,
    pub modifier: GroupByModifier,
}

impl fmt::Display for GroupByClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // GROUPING SETS uses its own column lists, not self.columns
        if let GroupByModifier::GroupingSets(sets) = &self.modifier {
            let sets_str: Vec<String> = sets
                .iter()
                .map(|set| {
                    let cols: Vec<String> = set.iter().map(|c| c.to_string()).collect();
                    format!("({})", cols.join(", "))
                })
                .collect();
            return write!(f, "GROUPING SETS ({})", sets_str.join(", "));
        }

        // None, Rollup, Cube all use self.columns
        if self.columns.is_empty() {
            return Ok(());
        }
        let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        match &self.modifier {
            GroupByModifier::None => write!(f, "{}", cols.join(", ")),
            GroupByModifier::Rollup => write!(f, "ROLLUP({})", cols.join(", ")),
            GroupByModifier::Cube => write!(f, "CUBE({})", cols.join(", ")),
            GroupByModifier::GroupingSets(_) => Ok(()), // Already handled above
        }
    }
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub token: Token,
    pub distinct: bool,
    /// DISTINCT ON (expr1, expr2, ...) expressions. Empty for regular DISTINCT or no DISTINCT.
    pub distinct_on: Vec<Expression>,
    pub columns: Vec<Expression>,
    pub with: Option<WithClause>,
    pub table_expr: Option<Box<Expression>>,
    pub where_clause: Option<Box<Expression>>,
    pub group_by: GroupByClause,
    pub having: Option<Box<Expression>>,
    /// Named window definitions (WINDOW w AS (...))
    pub window_defs: Vec<WindowDefinition>,
    pub order_by: Vec<OrderByExpression>,
    pub limit: Option<Box<Expression>>,
    pub offset: Option<Box<Expression>>,
    /// Set operations (UNION, INTERSECT, EXCEPT)
    pub set_operations: Vec<SetOperation>,
}

impl fmt::Display for SelectStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result = String::new();
        if let Some(ref with) = self.with {
            result.push_str(&format!("{} ", with));
        }
        result.push_str("SELECT ");
        if !self.distinct_on.is_empty() {
            let on_cols: Vec<String> = self.distinct_on.iter().map(|e| e.to_string()).collect();
            result.push_str(&format!("DISTINCT ON ({}) ", on_cols.join(", ")));
        } else if self.distinct {
            result.push_str("DISTINCT ");
        }
        let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        result.push_str(&cols.join(", "));
        if let Some(ref table) = self.table_expr {
            result.push_str(&format!(" FROM {}", table));
        }
        if let Some(ref where_clause) = self.where_clause {
            result.push_str(&format!(" WHERE {}", where_clause));
        }
        if !self.group_by.columns.is_empty()
            || matches!(self.group_by.modifier, GroupByModifier::GroupingSets(_))
        {
            result.push_str(&format!(" GROUP BY {}", self.group_by));
        }
        if let Some(ref having) = self.having {
            result.push_str(&format!(" HAVING {}", having));
        }
        if !self.window_defs.is_empty() {
            let wins: Vec<String> = self.window_defs.iter().map(|w| w.to_string()).collect();
            result.push_str(&format!(" WINDOW {}", wins.join(", ")));
        }
        // Set-operation branches bind before the outer ORDER/LIMIT/OFFSET.
        for set_op in &self.set_operations {
            result.push_str(&format!(" {} {}", set_op.operation, set_op.right));
        }
        if !self.order_by.is_empty() {
            let orders: Vec<String> = self.order_by.iter().map(|o| o.to_string()).collect();
            result.push_str(&format!(" ORDER BY {}", orders.join(", ")));
        }
        if let Some(ref limit) = self.limit {
            result.push_str(&format!(" LIMIT {}", limit));
        }
        if let Some(ref offset) = self.offset {
            result.push_str(&format!(" OFFSET {}", offset));
        }
        write!(f, "{}", result)
    }
}
