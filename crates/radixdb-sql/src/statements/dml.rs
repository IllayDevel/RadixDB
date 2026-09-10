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

/// Parsed conflict clause (ON DUPLICATE KEY UPDATE or ON CONFLICT)
#[derive(Default)]
struct ConflictClause {
    on_duplicate: bool,
    update_columns: Vec<Identifier>,
    update_expressions: Vec<Expression>,
    do_nothing: bool,
    conflict_target: Vec<Identifier>,
}

impl Parser {
    /// Parse an INSERT statement
    pub(super) fn parse_insert_statement(&mut self) -> Option<InsertStatement> {
        let token = self.cur_token.clone();

        // Expect INTO
        if !self.expect_keyword("INTO") {
            return None;
        }

        // Parse table name
        if !self.expect_peek_identifier_like() {
            return None;
        }
        let table_name = self.parse_relation_identifier_current()?;

        // Parse optional column list
        let mut columns = Vec::new();
        if self.peek_token_is_punctuator("(") {
            self.next_token(); // consume (
            columns = self.parse_identifier_list();
            if self.peek_token_is_punctuator(".") {
                self.add_error(format!(
                    "{}: navigation paths cannot be INSERT target columns",
                    NavigationErrorCode::ReadOnly
                ));
                return None;
            }
            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                self.add_error(format!("expected ')' at {}", self.cur_token.position));
                return None;
            }
        }

        // Check if next is VALUES, SELECT, or WITH (CTE)
        if self.peek_token_is_keyword("SELECT") {
            // INSERT INTO ... SELECT
            self.next_token(); // consume SELECT (we're now on SELECT)
            let select_stmt = self.parse_select_statement()?;

            // Check for ON DUPLICATE KEY UPDATE or ON CONFLICT
            let conflict = self.parse_conflict_clause();

            // Parse optional RETURNING clause
            let returning = self.parse_returning_clause();

            return Some(InsertStatement {
                token,
                table_name,
                columns,
                values: Vec::new(),
                select: Some(Box::new(select_stmt)),
                on_duplicate: conflict.on_duplicate,
                update_columns: conflict.update_columns,
                update_expressions: conflict.update_expressions,
                do_nothing: conflict.do_nothing,
                conflict_target: conflict.conflict_target,
                returning,
            });
        }

        if self.peek_token_is_keyword("WITH") {
            // INSERT INTO ... WITH ... SELECT
            self.next_token(); // consume WITH
            let with_clause = self.parse_with_clause()?;

            // Expect SELECT after WITH clause
            if !self.expect_keyword("SELECT") {
                return None;
            }

            // Parse the SELECT statement
            let mut select_stmt = self.parse_select_statement()?;

            // Attach the WITH clause to the SELECT
            select_stmt.with = Some(with_clause);

            // Check for ON DUPLICATE KEY UPDATE or ON CONFLICT
            let conflict = self.parse_conflict_clause();

            // Parse optional RETURNING clause
            let returning = self.parse_returning_clause();

            return Some(InsertStatement {
                token,
                table_name,
                columns,
                values: Vec::new(),
                select: Some(Box::new(select_stmt)),
                on_duplicate: conflict.on_duplicate,
                update_columns: conflict.update_columns,
                update_expressions: conflict.update_expressions,
                do_nothing: conflict.do_nothing,
                conflict_target: conflict.conflict_target,
                returning,
            });
        }

        // Expect VALUES
        if !self.expect_keyword("VALUES") {
            return None;
        }

        // Parse value lists
        let values = self.parse_value_lists()?;

        // Check for ON DUPLICATE KEY UPDATE or ON CONFLICT
        let conflict = self.parse_conflict_clause();

        // Parse optional RETURNING clause
        let returning = self.parse_returning_clause();

        Some(InsertStatement {
            token,
            table_name,
            columns,
            values,
            select: None,
            on_duplicate: conflict.on_duplicate,
            update_columns: conflict.update_columns,
            update_expressions: conflict.update_expressions,
            do_nothing: conflict.do_nothing,
            conflict_target: conflict.conflict_target,
            returning,
        })
    }

    /// Parse conflict clause: ON DUPLICATE KEY UPDATE (MySQL) or ON CONFLICT (PostgreSQL)
    /// Returns ConflictClause with all parsed fields.
    fn parse_conflict_clause(&mut self) -> ConflictClause {
        if !self.peek_token_is_keyword("ON") {
            return ConflictClause::default();
        }
        self.next_token(); // consume ON

        // Determine MySQL vs PostgreSQL style
        if self.peek_token_is_keyword("DUPLICATE") {
            // MySQL: ON DUPLICATE KEY UPDATE ...
            self.next_token(); // consume DUPLICATE
            if !self.expect_keyword("KEY") {
                return ConflictClause::default();
            }
            if !self.expect_keyword("UPDATE") {
                return ConflictClause::default();
            }
            let (update_columns, update_expressions) = self.parse_update_assignments();
            ConflictClause {
                on_duplicate: true,
                update_columns,
                update_expressions,
                ..Default::default()
            }
        } else if self.peek_token_is_keyword("CONFLICT") {
            // PostgreSQL: ON CONFLICT [(columns)] DO UPDATE SET ... | DO NOTHING
            self.next_token(); // consume CONFLICT

            // Optional conflict target: (col1, col2, ...)
            let conflict_target = if self.peek_token_is_punctuator("(") {
                self.next_token(); // consume (
                let cols = self.parse_identifier_list();
                if self.peek_token_is_punctuator(".") {
                    self.add_error(format!(
                        "{}: navigation paths cannot be conflict target columns",
                        NavigationErrorCode::ReadOnly
                    ));
                    return ConflictClause::default();
                }
                if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                    self.add_error(format!(
                        "expected ')' after conflict target at {}",
                        self.cur_token.position
                    ));
                    return ConflictClause::default();
                }
                cols
            } else {
                Vec::new()
            };

            // Expect DO
            if !self.expect_keyword("DO") {
                return ConflictClause::default();
            }

            // DO NOTHING or DO UPDATE SET
            if self.peek_token_is_keyword("NOTHING") {
                self.next_token(); // consume NOTHING
                ConflictClause {
                    do_nothing: true,
                    conflict_target,
                    ..Default::default()
                }
            } else if self.peek_token_is_keyword("UPDATE") {
                self.next_token(); // consume UPDATE
                if !self.expect_keyword("SET") {
                    return ConflictClause::default();
                }
                let (update_columns, update_expressions) = self.parse_update_assignments();
                ConflictClause {
                    on_duplicate: true,
                    update_columns,
                    update_expressions,
                    conflict_target,
                    do_nothing: false,
                }
            } else {
                self.add_error(format!(
                    "expected NOTHING or UPDATE after DO, got {}",
                    Self::format_token_for_error(&self.peek_token)
                ));
                ConflictClause::default()
            }
        } else {
            self.add_error(format!(
                "expected DUPLICATE or CONFLICT after ON, got {}",
                Self::format_token_for_error(&self.peek_token)
            ));
            ConflictClause::default()
        }
    }

    /// Parse column = expression assignment list (used by both MySQL and PostgreSQL upsert)
    pub(super) fn parse_update_assignments(&mut self) -> (Vec<Identifier>, Vec<Expression>) {
        let mut update_columns = Vec::new();
        let mut update_expressions = Vec::new();

        loop {
            // Accept both identifiers and keywords as column names (e.g., level, key, order)
            if !self.peek_token_is(TokenType::Identifier) && !self.peek_token_is(TokenType::Keyword)
            {
                self.add_error(format!(
                    "expected column name, got {}",
                    Self::format_token_for_error(&self.peek_token)
                ));
                return (update_columns, update_expressions);
            }
            self.next_token();
            update_columns.push(self.cur_token_as_column_identifier());

            if self.peek_token_is_punctuator(".") {
                self.add_error(format!(
                    "{}: navigation paths cannot be assignment targets",
                    NavigationErrorCode::ReadOnly
                ));
                return (update_columns, update_expressions);
            }

            if !self.expect_peek(TokenType::Operator) || self.cur_token.literal != "=" {
                self.add_error(format!("expected '=' at {}", self.cur_token.position));
                return (update_columns, update_expressions);
            }

            self.next_token();
            if let Some(expr) = self.parse_expression(Precedence::Lowest) {
                update_expressions.push(expr);
            } else {
                return (update_columns, update_expressions);
            }

            if !self.peek_token_is_punctuator(",") {
                break;
            }
            self.next_token(); // consume comma
        }

        (update_columns, update_expressions)
    }

    /// Parse RETURNING clause for INSERT/UPDATE/DELETE statements
    pub(super) fn parse_returning_clause(&mut self) -> Vec<Expression> {
        if !self.peek_token_is_keyword("RETURNING") {
            return Vec::new();
        }

        self.next_token(); // consume RETURNING

        // Parse the expression list
        self.parse_expression_list()
    }

    /// Parse value lists for INSERT
    pub(super) fn parse_value_lists(&mut self) -> Option<Vec<Vec<Expression>>> {
        // Pre-allocate for common case (single row INSERT)
        let mut value_lists = Vec::with_capacity(1);

        // Expect (
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "(" {
            self.add_error(format!("expected '(' at {}", self.cur_token.position));
            return None;
        }

        // Parse first value list
        let values = self.parse_expression_list();
        value_lists.push(values);

        // Expect )
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
            self.add_error(format!("expected ')' at {}", self.cur_token.position));
            return None;
        }

        // Parse additional value lists
        while self.peek_token_is_punctuator(",") {
            self.next_token(); // consume comma

            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "(" {
                self.add_error(format!("expected '(' at {}", self.cur_token.position));
                return None;
            }

            let values = self.parse_expression_list();
            value_lists.push(values);

            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                self.add_error(format!("expected ')' at {}", self.cur_token.position));
                return None;
            }
        }

        Some(value_lists)
    }

    /// Parse an UPDATE statement
    pub(super) fn parse_update_statement(&mut self) -> Option<UpdateStatement> {
        let token = self.cur_token.clone();

        // Parse table name
        if !self.expect_peek_identifier_like() {
            return None;
        }
        let table_name = self.parse_relation_identifier_current()?;

        // Expect SET
        if !self.expect_keyword("SET") {
            return None;
        }

        // Parse column-value pairs
        let mut updates = FxHashMap::default();
        loop {
            // Accept both identifiers and keywords as column names (e.g., level, key, order)
            if !self.peek_token_is(TokenType::Identifier) && !self.peek_token_is(TokenType::Keyword)
            {
                self.add_error(format!(
                    "expected column name in SET clause, got {}",
                    Self::format_token_for_error(&self.peek_token)
                ));
                return None;
            }
            self.next_token();
            let column_name = self.cur_token_as_column_identifier();
            let column_name = column_name.value;

            if self.peek_token_is_punctuator(".") {
                self.add_error(format!(
                    "{}: navigation paths cannot be assignment targets",
                    NavigationErrorCode::ReadOnly
                ));
                return None;
            }

            if !self.expect_peek(TokenType::Operator) || self.cur_token.literal != "=" {
                self.add_error(format!("expected '=' at {}", self.cur_token.position));
                return None;
            }

            self.next_token();
            let value_expr = self.parse_expression(Precedence::Lowest)?;
            if updates
                .keys()
                .any(|existing: &SmartString| existing.eq_ignore_ascii_case(&column_name))
            {
                self.add_error(format!(
                    "duplicate column '{}' in UPDATE SET clause",
                    column_name
                ));
                return None;
            }
            updates.insert(column_name, value_expr);

            if !self.peek_token_is_punctuator(",") {
                break;
            }
            self.next_token(); // consume comma
        }

        // Parse WHERE clause
        let where_clause = if self.peek_token_is_keyword("WHERE") {
            self.next_token(); // consume WHERE
            self.current_clause = "WHERE".to_string();
            self.next_token();
            Some(Box::new(self.parse_expression(Precedence::Lowest)?))
        } else {
            None
        };

        self.current_clause.clear();

        // Parse optional RETURNING clause
        let returning = self.parse_returning_clause();

        Some(UpdateStatement {
            token,
            table_name,
            updates,
            where_clause,
            returning,
        })
    }

    /// Parse a DELETE statement
    /// DELETE FROM table [AS alias] [WHERE condition] [RETURNING ...]
    pub(super) fn parse_delete_statement(&mut self) -> Option<DeleteStatement> {
        let token = self.cur_token.clone();

        // Expect FROM
        if !self.expect_keyword("FROM") {
            return None;
        }

        // Parse table name
        if !self.peek_token_is_identifier_like() {
            self.add_error(format!(
                "expected table name after DELETE FROM, got {}",
                Self::format_token_for_error(&self.peek_token)
            ));
            return None;
        }
        self.next_token();
        let table_name = self.parse_relation_identifier_current()?;

        // Parse optional alias (AS alias or just alias)
        let alias = if self.peek_token_is_keyword("AS") {
            self.next_token(); // consume AS
            if !self.expect_peek_identifier_like() {
                return None;
            }
            Some(Identifier::new(
                self.cur_token.clone(),
                self.cur_token.literal.clone(),
            ))
        } else if self.peek_token_is_identifier_like()
            && !self.peek_token_is_keyword("WHERE")
            && !self.peek_token_is_keyword("RETURNING")
        {
            // Alias without AS keyword (e.g., DELETE FROM users u WHERE ...)
            self.next_token();
            Some(Identifier::new(
                self.cur_token.clone(),
                self.cur_token.literal.clone(),
            ))
        } else {
            None
        };

        // Parse WHERE clause
        let where_clause = if self.peek_token_is_keyword("WHERE") {
            self.next_token(); // consume WHERE
            self.current_clause = "WHERE".to_string();
            self.next_token();
            Some(Box::new(self.parse_expression(Precedence::Lowest)?))
        } else {
            None
        };

        self.current_clause.clear();

        // Parse optional RETURNING clause
        let returning = self.parse_returning_clause();

        Some(DeleteStatement {
            token,
            table_name,
            alias,
            where_clause,
            returning,
        })
    }

    /// Parse a TRUNCATE statement
    /// TRUNCATE TABLE table_name or TRUNCATE table_name
    pub(super) fn parse_truncate_statement(&mut self) -> Option<TruncateStatement> {
        let token = self.cur_token.clone();

        // Optional TABLE keyword
        if self.peek_token_is_keyword("TABLE") {
            self.next_token(); // consume TABLE
        }

        // Parse table name
        if !self.expect_peek_identifier_like() {
            return None;
        }
        let table_name = self.parse_relation_identifier_current()?;

        Some(TruncateStatement { token, table_name })
    }

    /// Parse a VACUUM statement
    /// VACUUM [table_name]
    pub(super) fn parse_vacuum_statement(&mut self) -> Option<VacuumStatement> {
        let token = self.cur_token.clone();

        // Optional table name
        let table_name = if self.peek_token_is_identifier_like() {
            self.next_token();
            Some(Identifier::new(
                self.cur_token.clone(),
                self.cur_token.literal.clone(),
            ))
        } else {
            None
        };

        Some(VacuumStatement { token, table_name })
    }
}
