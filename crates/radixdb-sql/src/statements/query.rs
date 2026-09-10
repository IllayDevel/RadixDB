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

/// Keywords that cannot be used as implicit table aliases in FROM clauses.
/// Shared by SimpleTableSource, FunctionTableSource, and ValuesTableSource parsing.
const RESERVED_ALIAS_KEYWORDS: &[&str] = &[
    "JOIN",
    "LEFT",
    "RIGHT",
    "INNER",
    "OUTER",
    "CROSS",
    "FULL",
    "NATURAL",
    "ON",
    "USING",
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "OFFSET",
    "FETCH",
    "WINDOW",
    "FOR",
    "INTO",
    "UNION",
    "INTERSECT",
    "EXCEPT",
];

/// Check if a token's uppercase literal is a reserved alias keyword.
fn is_reserved_alias_keyword(upper: &str) -> bool {
    RESERVED_ALIAS_KEYWORDS
        .iter()
        .any(|&kw| kw.eq_ignore_ascii_case(upper))
}

fn is_table_alias_token(token: &Token) -> bool {
    matches!(token.token_type, TokenType::Identifier | TokenType::Keyword)
        && !is_reserved_alias_keyword(&token.literal)
}

impl Parser {
    /// Parse a SELECT statement
    pub fn parse_select_statement(&mut self) -> Option<SelectStatement> {
        let token = self.cur_token.clone();

        let mut stmt = SelectStatement {
            token,
            distinct: false,
            distinct_on: vec![],
            columns: Vec::new(),
            with: None,
            table_expr: None,
            where_clause: None,
            group_by: GroupByClause::default(),
            having: None,
            window_defs: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operations: Vec::new(),
        };

        // Check for DISTINCT / DISTINCT ON (expr, ...)
        if self.peek_token_is_keyword("DISTINCT") {
            self.next_token();
            stmt.distinct = true;
            self.parse_distinct_on(&mut stmt);
        }

        // Parse column list
        self.next_token();
        stmt.columns = self.parse_select_columns();

        // Parse FROM clause
        if self.peek_token_is_keyword("FROM") {
            self.next_token(); // consume FROM
            self.next_token(); // move to table expression
            stmt.table_expr = Some(Box::new(self.parse_table_expression()?));
        }

        // Parse WHERE clause
        if self.peek_token_is_keyword("WHERE") {
            self.next_token(); // consume WHERE
            self.current_clause = "WHERE".to_string();
            self.next_token();
            stmt.where_clause = Some(Box::new(self.parse_expression(Precedence::Lowest)?));
        }

        // Parse GROUP BY clause
        if self.peek_token_is_keyword("GROUP") {
            self.next_token(); // consume GROUP
            if !self.expect_keyword("BY") {
                return None;
            }
            self.current_clause = "GROUP BY".to_string();
            stmt.group_by = self.parse_group_by_clause();
        }

        // Parse HAVING clause
        if self.peek_token_is_keyword("HAVING") {
            self.next_token(); // consume HAVING
            self.current_clause = "HAVING".to_string();
            self.next_token();
            stmt.having = Some(Box::new(self.parse_expression(Precedence::Lowest)?));
        }

        // Parse WINDOW clause (named window definitions)
        if self.peek_token_is_keyword("WINDOW") {
            self.next_token(); // consume WINDOW
            self.current_clause = "WINDOW".to_string();
            stmt.window_defs = self.parse_window_definitions();
        }

        // Parse UNION, INTERSECT, EXCEPT set operations
        while self.peek_token_is_keyword("UNION")
            || self.peek_token_is_keyword("INTERSECT")
            || self.peek_token_is_keyword("EXCEPT")
        {
            if let Some(set_op) = self.parse_set_operation() {
                stmt.set_operations.push(set_op);
            } else {
                break;
            }
        }

        // Parse ORDER BY clause (applies to entire compound query)
        if self.peek_token_is_keyword("ORDER") {
            self.next_token(); // consume ORDER
            if !self.expect_keyword("BY") {
                return None;
            }
            self.current_clause = "ORDER BY".to_string();
            stmt.order_by = self.parse_order_by_expressions();
        }

        // Parse LIMIT clause
        if self.peek_token_is_keyword("LIMIT") {
            self.next_token(); // consume LIMIT
            self.current_clause = "LIMIT".to_string();
            self.next_token();
            stmt.limit = Some(Box::new(self.parse_expression(Precedence::Lowest)?));
        }

        // Parse OFFSET clause
        if self.peek_token_is_keyword("OFFSET") {
            self.next_token(); // consume OFFSET
            self.current_clause = "OFFSET".to_string();
            self.next_token();
            stmt.offset = Some(Box::new(self.parse_expression(Precedence::Lowest)?));

            // Optional ROWS/ROW keyword after OFFSET value
            if self.peek_token_is_keyword("ROWS") || self.peek_token_is_keyword("ROW") {
                self.next_token();
            }
        }

        // Parse FETCH FIRST/NEXT n ROWS ONLY clause (alternative to LIMIT)
        if self.peek_token_is_keyword("FETCH") {
            self.next_token(); // consume FETCH

            // FIRST or NEXT (both are equivalent)
            if !self.peek_token_is_keyword("FIRST") && !self.peek_token_is_keyword("NEXT") {
                self.add_error(format!(
                    "expected FIRST or NEXT after FETCH at {}",
                    self.peek_token.position
                ));
                return None;
            }
            self.next_token(); // consume FIRST/NEXT

            self.current_clause = "FETCH".to_string();
            self.next_token();
            stmt.limit = Some(Box::new(self.parse_expression(Precedence::Lowest)?));

            // Optional ROWS/ROW keyword
            if self.peek_token_is_keyword("ROWS") || self.peek_token_is_keyword("ROW") {
                self.next_token();
            }

            // Optional ONLY keyword
            if self.peek_token_is_keyword("ONLY") {
                self.next_token();
            }
        }

        self.current_clause.clear();
        Some(stmt)
    }

    /// Parse a set operation (UNION, INTERSECT, EXCEPT)
    /// Handles SQL standard precedence: INTERSECT/EXCEPT bind tighter than UNION
    pub(super) fn parse_set_operation(&mut self) -> Option<SetOperation> {
        self.next_token(); // consume UNION/INTERSECT/EXCEPT

        let keyword = self.cur_token.literal.to_uppercase();
        let operation = if keyword == "UNION" {
            if self.peek_token_is_keyword("ALL") {
                self.next_token();
                SetOperationType::UnionAll
            } else {
                SetOperationType::Union
            }
        } else if keyword == "INTERSECT" {
            if self.peek_token_is_keyword("ALL") {
                self.next_token();
                SetOperationType::IntersectAll
            } else {
                SetOperationType::Intersect
            }
        } else if keyword == "EXCEPT" {
            if self.peek_token_is_keyword("ALL") {
                self.next_token();
                SetOperationType::ExceptAll
            } else {
                SetOperationType::Except
            }
        } else {
            return None;
        };

        // Expect SELECT
        if !self.expect_keyword("SELECT") {
            return None;
        }

        // Parse the right side SELECT
        let mut right = self.parse_simple_select()?;

        // INTERSECT binds more tightly than UNION/EXCEPT. UNION and EXCEPT
        // have the same precedence and are applied left-to-right by the outer
        // set-operation loop.
        if keyword == "UNION" || keyword == "EXCEPT" {
            while self.peek_token_is_keyword("INTERSECT") {
                if let Some(set_op) = self.parse_set_operation() {
                    right.set_operations.push(set_op);
                } else {
                    break;
                }
            }
        }

        Some(SetOperation {
            operation,
            right: Box::new(right),
        })
    }

    /// Parse a simple SELECT (without set operations, used for right side of UNION etc)
    pub(super) fn parse_simple_select(&mut self) -> Option<SelectStatement> {
        let token = self.cur_token.clone();

        let mut stmt = SelectStatement {
            token,
            distinct: false,
            distinct_on: vec![],
            columns: Vec::new(),
            with: None,
            table_expr: None,
            where_clause: None,
            group_by: GroupByClause::default(),
            having: None,
            window_defs: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            set_operations: Vec::new(),
        };

        // Check for DISTINCT / DISTINCT ON (expr, ...)
        if self.peek_token_is_keyword("DISTINCT") {
            self.next_token();
            stmt.distinct = true;
            self.parse_distinct_on(&mut stmt);
        }

        // Parse column list
        self.next_token();
        stmt.columns = self.parse_select_columns();

        // Parse FROM clause
        if self.peek_token_is_keyword("FROM") {
            self.next_token(); // consume FROM
            self.next_token(); // move to table expression
            stmt.table_expr = Some(Box::new(self.parse_table_expression()?));
        }

        // Parse WHERE clause
        if self.peek_token_is_keyword("WHERE") {
            self.next_token(); // consume WHERE
            self.current_clause = "WHERE".to_string();
            self.next_token();
            stmt.where_clause = Some(Box::new(self.parse_expression(Precedence::Lowest)?));
        }

        // Parse GROUP BY clause
        if self.peek_token_is_keyword("GROUP") {
            self.next_token(); // consume GROUP
            if !self.expect_keyword("BY") {
                return None;
            }
            self.current_clause = "GROUP BY".to_string();
            stmt.group_by = self.parse_group_by_clause();
        }

        // Parse HAVING clause
        if self.peek_token_is_keyword("HAVING") {
            self.next_token(); // consume HAVING
            self.current_clause = "HAVING".to_string();
            self.next_token();
            stmt.having = Some(Box::new(self.parse_expression(Precedence::Lowest)?));
        }

        self.current_clause.clear();
        Some(stmt)
    }

    /// Parse SELECT columns
    pub(super) fn parse_select_columns(&mut self) -> Vec<Expression> {
        let mut columns = Vec::new();

        // Parse first column
        if let Some(col) = self.parse_select_column() {
            columns.push(col);
        }

        // Parse additional columns
        while self.peek_token_is_punctuator(",") {
            self.next_token(); // consume comma
            self.next_token(); // move to next column
            if let Some(col) = self.parse_select_column() {
                columns.push(col);
            }
        }

        columns
    }

    /// Parse a single SELECT column
    pub(super) fn parse_select_column(&mut self) -> Option<Expression> {
        // Check for * (all columns)
        if self.cur_token_is(TokenType::Operator) && self.cur_token.literal == "*" {
            return Some(Expression::Star(StarExpression {
                token: self.cur_token.clone(),
            }));
        }

        // Parse expression
        let expr = self.parse_expression(Precedence::Lowest)?;

        // Check for alias with AS keyword
        if self.peek_token_is_keyword("AS") {
            self.next_token(); // consume AS
                               // Allow both identifiers and keywords as aliases (e.g., AS level, AS type)
            if !self.peek_token_is(TokenType::Identifier) && !self.peek_token_is(TokenType::Keyword)
            {
                self.peek_error(TokenType::Identifier);
                return None;
            }
            self.next_token();
            return Some(Expression::Aliased(AliasedExpression {
                token: self.cur_token.clone(),
                expression: Box::new(expr),
                alias: self.cur_token_as_column_identifier(),
            }));
        }

        // Check for implicit alias (identifier without AS)
        // Must be an identifier that's not a reserved keyword like FROM, WHERE, etc.
        if self.peek_token_is(TokenType::Identifier) {
            let alias_candidate = self.peek_token.literal.to_uppercase();
            // List of keywords that cannot be implicit aliases (they end the column list or start clauses)
            let reserved = [
                "FROM",
                "WHERE",
                "GROUP",
                "HAVING",
                "ORDER",
                "LIMIT",
                "OFFSET",
                "UNION",
                "INTERSECT",
                "EXCEPT",
                "INTO",
                "FOR",
                "WINDOW",
                "FETCH",
                "ON",
                "USING",
                "NATURAL",
                "LEFT",
                "RIGHT",
                "INNER",
                "OUTER",
                "CROSS",
                "FULL",
                "JOIN",
            ];
            if !reserved.contains(&alias_candidate.as_str()) {
                self.next_token();
                return Some(Expression::Aliased(AliasedExpression {
                    token: self.cur_token.clone(),
                    expression: Box::new(expr),
                    alias: Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone()),
                }));
            }
        }

        Some(expr)
    }

    /// Parse a table expression (for FROM clause)
    pub(super) fn parse_table_expression(&mut self) -> Option<Expression> {
        let left = self.parse_simple_table_expression()?;
        self.parse_join_table_expression(left)
    }

    /// Parse a simple table expression (table name, subquery, VALUES, or CTE reference)
    pub(super) fn parse_simple_table_expression(&mut self) -> Option<Expression> {
        // Check for subquery or VALUES
        if self.cur_token_is_punctuator("(") {
            self.next_token();
            if self.cur_token_is_keyword("SELECT") {
                let subquery = self.parse_select_statement()?;

                if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                    self.add_error(format!(
                        "expected ')' after subquery at {}",
                        self.cur_token.position
                    ));
                    return None;
                }

                let mut alias = None;
                if self.peek_token_is_keyword("AS") {
                    self.next_token();
                    if !is_table_alias_token(&self.peek_token) {
                        self.add_error(format!(
                            "expected alias after AS at {}",
                            self.peek_token.position
                        ));
                        return None;
                    }
                    self.next_token();
                    alias = Some(Identifier::new(
                        self.cur_token.clone(),
                        self.cur_token.literal.clone(),
                    ));
                } else if is_table_alias_token(&self.peek_token) {
                    self.next_token();
                    alias = Some(Identifier::new(
                        self.cur_token.clone(),
                        self.cur_token.literal.clone(),
                    ));
                }

                return Some(Expression::SubquerySource(Box::new(SubqueryTableSource {
                    token: self.cur_token.clone(),
                    subquery: Box::new(subquery),
                    alias,
                })));
            } else if self.cur_token_is_keyword("VALUES") {
                // Parse VALUES clause as table source
                return self.parse_values_table_source();
            }
        }

        // Parse table name - accept both identifiers and keywords (for CTE references like 'first')
        if !self.cur_token_is(TokenType::Identifier) && !self.cur_token_is(TokenType::Keyword) {
            self.add_error(format!(
                "expected table name at {}",
                self.cur_token.position
            ));
            return None;
        }

        let token = self.cur_token.clone();

        // Check for table-valued function: identifier followed by '('
        if self.peek_token_is_punctuator("(") {
            let name = Identifier::new(token.clone(), self.cur_token.literal.clone());
            return self.parse_function_table_source(token, name);
        }

        let name = self.parse_relation_identifier_current()?;

        // Check for AS OF clause (temporal queries)
        let as_of = if self.peek_token_is_keyword("AS") {
            self.next_token(); // consume AS
            if self.peek_token_is_keyword("OF") {
                self.next_token(); // consume OF
                self.next_token(); // move to TRANSACTION or TIMESTAMP

                let as_of_type = self.cur_token.literal.to_uppercase();
                if as_of_type != "TRANSACTION" && as_of_type != "TIMESTAMP" {
                    self.add_error(format!(
                        "expected TRANSACTION or TIMESTAMP after AS OF at {}",
                        self.cur_token.position
                    ));
                    return None;
                }

                self.next_token();
                let value = self.parse_expression(Precedence::Lowest)?;

                Some(AsOfClause {
                    token: self.cur_token.clone(),
                    as_of_type,
                    value: Box::new(value),
                })
            } else {
                // This is an alias starting with AS
                None
            }
        } else {
            None
        };

        // Check for alias (can occur after AS OF or after table name)
        let mut alias = None;
        if self.peek_token_is_keyword("AS") {
            self.next_token(); // consume AS
            if !is_table_alias_token(&self.peek_token) {
                self.add_error(format!(
                    "expected alias after AS at {}",
                    self.peek_token.position
                ));
                return None;
            }
            self.next_token();
            alias = Some(Identifier::new(
                self.cur_token.clone(),
                self.cur_token.literal.clone(),
            ));
        } else if is_table_alias_token(&self.peek_token) {
            self.next_token();
            alias = Some(Identifier::new(
                self.cur_token.clone(),
                self.cur_token.literal.clone(),
            ));
        }

        Some(Expression::TableSource(Box::new(SimpleTableSource {
            token,
            name,
            alias,
            as_of,
        })))
    }

    /// Parse a JOIN table expression
    pub(super) fn parse_join_table_expression(
        &mut self,
        mut left: Expression,
    ) -> Option<Expression> {
        loop {
            // Check for JOIN keywords
            let join_type = if self.peek_token_is_keyword("JOIN") {
                self.next_token();
                SmartString::const_new("INNER")
            } else if self.peek_token_is_keyword("INNER") {
                self.next_token();
                if !self.expect_keyword("JOIN") {
                    return None;
                }
                SmartString::const_new("INNER")
            } else if self.peek_token_is_keyword("LEFT") {
                self.next_token();
                if self.peek_token_is_keyword("OUTER") {
                    self.next_token();
                }
                if !self.expect_keyword("JOIN") {
                    return None;
                }
                SmartString::const_new("LEFT")
            } else if self.peek_token_is_keyword("RIGHT") {
                self.next_token();
                if self.peek_token_is_keyword("OUTER") {
                    self.next_token();
                }
                if !self.expect_keyword("JOIN") {
                    return None;
                }
                SmartString::const_new("RIGHT")
            } else if self.peek_token_is_keyword("FULL") {
                self.next_token();
                if self.peek_token_is_keyword("OUTER") {
                    self.next_token();
                }
                if !self.expect_keyword("JOIN") {
                    return None;
                }
                SmartString::const_new("FULL")
            } else if self.peek_token_is_keyword("CROSS") {
                self.next_token();
                if !self.expect_keyword("JOIN") {
                    return None;
                }
                SmartString::const_new("CROSS")
            } else if self.peek_token_is_keyword("NATURAL") {
                self.next_token();
                let natural_type = if self.peek_token_is_keyword("LEFT") {
                    self.next_token();
                    if self.peek_token_is_keyword("OUTER") {
                        self.next_token();
                    }
                    "NATURAL LEFT"
                } else if self.peek_token_is_keyword("RIGHT") {
                    self.next_token();
                    if self.peek_token_is_keyword("OUTER") {
                        self.next_token();
                    }
                    "NATURAL RIGHT"
                } else {
                    "NATURAL"
                };
                if !self.expect_keyword("JOIN") {
                    return None;
                }
                SmartString::const_new(natural_type)
            } else if self.peek_token_is_punctuator(",") {
                // Implicit CROSS JOIN with comma syntax: FROM t1, t2
                self.next_token(); // consume comma
                SmartString::const_new("CROSS")
            } else {
                // No more joins
                break;
            };

            let token = self.cur_token.clone();
            self.next_token();
            let right = self.parse_simple_table_expression()?;

            // Parse ON or USING clause (not for CROSS JOIN or NATURAL JOIN)
            let mut condition = None;
            let mut using_columns = Vec::new();

            if !join_type.starts_with("CROSS") && !join_type.starts_with("NATURAL") {
                if self.peek_token_is_keyword("ON") {
                    self.next_token(); // consume ON
                    self.next_token();
                    condition = Some(Box::new(self.parse_expression(Precedence::Lowest)?));
                } else if self.peek_token_is_keyword("USING") {
                    self.next_token(); // consume USING
                    if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "(" {
                        self.add_error(format!(
                            "expected '(' after USING at {}",
                            self.cur_token.position
                        ));
                        return None;
                    }
                    using_columns = self.parse_identifier_list();
                    if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                        self.add_error(format!(
                            "expected ')' after USING columns at {}",
                            self.cur_token.position
                        ));
                        return None;
                    }
                } else {
                    self.add_error(format!("{} JOIN requires an ON or USING clause", join_type));
                    return None;
                }
            }

            left = Expression::JoinSource(Box::new(JoinTableSource {
                token,
                left: Box::new(left),
                join_type,
                right: Box::new(right),
                condition,
                using_columns,
            }));
        }

        Some(left)
    }

    /// Parse a WITH statement (CTE)
    pub(super) fn parse_with_statement(&mut self) -> Option<Statement> {
        let with_clause = self.parse_with_clause()?;

        // After WITH, expect SELECT or INSERT
        self.next_token();
        if self.cur_token_is_keyword("SELECT") {
            let mut select = self.parse_select_statement()?;
            select.with = Some(with_clause);
            Some(Statement::Select(select))
        } else if self.cur_token_is_keyword("INSERT") {
            // WITH ... INSERT INTO ... SELECT
            let mut insert = self.parse_insert_statement()?;
            // The INSERT must use SELECT (not VALUES) for CTE to make sense
            if let Some(ref mut select) = insert.select {
                select.with = Some(with_clause);
            } else {
                self.add_error(
                    "WITH clause requires INSERT ... SELECT, not INSERT ... VALUES".to_string(),
                );
                return None;
            }
            Some(Statement::Insert(insert))
        } else {
            self.add_error(format!(
                "expected SELECT or INSERT after WITH clause at {}",
                self.cur_token.position
            ));
            None
        }
    }

    /// Parse a WITH clause
    pub(super) fn parse_with_clause(&mut self) -> Option<WithClause> {
        let token = self.cur_token.clone();
        let mut is_recursive = false;

        // Check for RECURSIVE
        if self.peek_token_is_keyword("RECURSIVE") {
            self.next_token();
            is_recursive = true;
        }

        let mut ctes = Vec::new();

        // Parse first CTE
        self.next_token();
        if let Some(cte) = self.parse_common_table_expression(is_recursive) {
            ctes.push(cte);
        }

        // Parse additional CTEs
        while self.peek_token_is_punctuator(",") {
            self.next_token(); // consume comma
            self.next_token(); // move to CTE name
            if let Some(cte) = self.parse_common_table_expression(is_recursive) {
                ctes.push(cte);
            }
        }

        Some(WithClause {
            token,
            ctes,
            is_recursive,
        })
    }

    /// Parse a Common Table Expression
    pub(super) fn parse_common_table_expression(
        &mut self,
        is_recursive: bool,
    ) -> Option<CommonTableExpression> {
        // Accept both identifiers and keywords as CTE names (context-dependent identifiers)
        // Keywords like FIRST, LAST, VALUE, etc. are valid CTE names in SQL
        if !self.cur_token_is(TokenType::Identifier) && !self.cur_token_is(TokenType::Keyword) {
            self.add_error(format!("expected CTE name at {}", self.cur_token.position));
            return None;
        }

        let token = self.cur_token.clone();
        let name = Identifier::new(token.clone(), self.cur_token.literal.clone());

        // Optional column list
        let mut column_names = Vec::new();
        if self.peek_token_is_punctuator("(") {
            self.next_token(); // consume (
            column_names = self.parse_identifier_list();
            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                self.add_error(format!(
                    "expected ')' after column list at {}",
                    self.cur_token.position
                ));
                return None;
            }
        }

        // Expect AS
        if !self.expect_keyword("AS") {
            return None;
        }

        // Expect (
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "(" {
            self.add_error(format!(
                "expected '(' after AS at {}",
                self.cur_token.position
            ));
            return None;
        }

        // Parse the CTE query
        self.next_token();
        if !self.cur_token_is_keyword("SELECT") {
            self.add_error(format!(
                "expected SELECT in CTE at {}",
                self.cur_token.position
            ));
            return None;
        }

        let query = self.parse_select_statement()?;

        // Expect )
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
            self.add_error(format!(
                "expected ')' after CTE query at {}",
                self.cur_token.position
            ));
            return None;
        }

        Some(CommonTableExpression {
            token,
            name,
            column_names,
            query: Box::new(query),
            is_recursive,
        })
    }

    /// Parse DISTINCT ON (expr1, expr2, ...) if present after DISTINCT keyword.
    pub(super) fn parse_distinct_on(&mut self, stmt: &mut SelectStatement) {
        if self.peek_token_is_keyword("ON") {
            self.next_token(); // consume ON
            if self.peek_token_is_punctuator("(") {
                self.next_token(); // consume (
                stmt.distinct_on = self.parse_distinct_on_columns();
                if !self.peek_token_is_punctuator(")") {
                    self.add_error(format!(
                        "expected ')' after DISTINCT ON columns at {}",
                        self.peek_token.position
                    ));
                    return;
                }
                self.next_token(); // consume )
            } else {
                self.add_error(format!(
                    "expected '(' after DISTINCT ON at {}",
                    self.peek_token.position
                ));
            }
        }
    }

    /// Parse comma-separated expression list inside DISTINCT ON (...)
    pub(super) fn parse_distinct_on_columns(&mut self) -> Vec<Expression> {
        let mut exprs = Vec::new();
        self.next_token();
        if let Some(expr) = self.parse_expression(Precedence::Lowest) {
            exprs.push(expr);
        }
        while self.peek_token_is_punctuator(",") {
            self.next_token(); // consume comma
            self.next_token(); // move to next expression
            if let Some(expr) = self.parse_expression(Precedence::Lowest) {
                exprs.push(expr);
            }
        }
        exprs
    }

    /// Parse VALUES clause as a table source (e.g., (VALUES (1, 'a'), (2, 'b')) AS t(col1, col2))
    pub(super) fn parse_values_table_source(&mut self) -> Option<Expression> {
        let token = self.cur_token.clone(); // VALUES token

        // Parse value lists - we're already on VALUES, call parse_value_lists which expects (
        let rows = self.parse_value_lists()?;

        // Expect ) to close the outer parenthesis
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
            self.add_error(format!(
                "expected ')' after VALUES at {}",
                self.cur_token.position
            ));
            return None;
        }

        // Parse optional alias
        let mut alias = None;
        let mut column_aliases = Vec::new();

        if self.peek_token_is_keyword("AS") {
            self.next_token(); // consume AS
            if !is_table_alias_token(&self.peek_token) {
                self.add_error(format!(
                    "expected alias after AS at {}",
                    self.peek_token.position
                ));
                return None;
            }
            self.next_token();
            alias = Some(Identifier::new(
                self.cur_token.clone(),
                self.cur_token.literal.clone(),
            ));

            // Parse optional column aliases: AS t(col1, col2)
            if self.peek_token_is_punctuator("(") {
                self.next_token(); // consume (
                column_aliases = self.parse_identifier_list();
                if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                    self.add_error(format!(
                        "expected ')' after column aliases at {}",
                        self.cur_token.position
                    ));
                    return None;
                }
            }
        } else if is_table_alias_token(&self.peek_token) {
            // Implicit alias without AS
            self.next_token();
            alias = Some(Identifier::new(
                self.cur_token.clone(),
                self.cur_token.literal.clone(),
            ));

            // Parse optional column aliases
            if self.peek_token_is_punctuator("(") {
                self.next_token(); // consume (
                column_aliases = self.parse_identifier_list();
                if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                    self.add_error(format!(
                        "expected ')' after column aliases at {}",
                        self.cur_token.position
                    ));
                    return None;
                }
            }
        }

        Some(Expression::ValuesSource(Box::new(ValuesTableSource {
            token,
            rows,
            alias,
            column_aliases,
        })))
    }

    /// Parse a function table source (table-valued function in FROM clause)
    /// e.g., generate_series(1, 10) AS gs(value)
    pub(super) fn parse_function_table_source(
        &mut self,
        token: Token,
        name: Identifier,
    ) -> Option<Expression> {
        self.next_token(); // consume '('
        self.next_token(); // advance to first arg or ')'

        let mut arguments = Vec::new();

        // Parse arguments (comma-separated expressions)
        if !self.cur_token_is_punctuator(")") {
            if let Some(arg) = self.parse_expression(Precedence::Lowest) {
                arguments.push(arg);
            }
            while self.peek_token_is_punctuator(",") {
                self.next_token(); // consume ','
                self.next_token(); // move to next arg
                if let Some(arg) = self.parse_expression(Precedence::Lowest) {
                    arguments.push(arg);
                } else {
                    self.add_error(format!(
                        "expected expression after ',' at {}",
                        self.cur_token.position
                    ));
                    return None;
                }
            }
        }

        // Expect closing ')' - if cur_token is already ')' (zero-arg case), we're done;
        // otherwise it should be the peek token
        if !self.cur_token_is_punctuator(")")
            && (!self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")")
        {
            self.add_error(format!(
                "expected ')' after function arguments at {}",
                self.cur_token.position
            ));
            return None;
        }

        // Parse optional alias and column aliases (same pattern as VALUES table source)
        let mut alias = None;
        let mut column_aliases = Vec::new();

        if self.peek_token_is_keyword("AS") {
            self.next_token(); // consume AS
            if !is_table_alias_token(&self.peek_token) {
                self.add_error(format!(
                    "expected alias after AS at {}",
                    self.peek_token.position
                ));
                return None;
            }
            self.next_token();
            alias = Some(Identifier::new(
                self.cur_token.clone(),
                self.cur_token.literal.clone(),
            ));

            // Parse optional column aliases: AS gs(value)
            if self.peek_token_is_punctuator("(") {
                self.next_token(); // consume (
                column_aliases = self.parse_identifier_list();
                if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                    self.add_error(format!(
                        "expected ')' after column aliases at {}",
                        self.cur_token.position
                    ));
                    return None;
                }
            }
        } else if is_table_alias_token(&self.peek_token) {
            // Implicit alias without AS keyword
            self.next_token();
            alias = Some(Identifier::new(
                self.cur_token.clone(),
                self.cur_token.literal.clone(),
            ));

            // Parse optional column aliases
            if self.peek_token_is_punctuator("(") {
                self.next_token(); // consume (
                column_aliases = self.parse_identifier_list();
                if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                    self.add_error(format!(
                        "expected ')' after column aliases at {}",
                        self.cur_token.position
                    ));
                    return None;
                }
            }
        }

        Some(Expression::FunctionTableSource(Box::new(
            FunctionTableSource {
                token,
                function: name,
                arguments,
                alias,
                column_aliases,
            },
        )))
    }
}
