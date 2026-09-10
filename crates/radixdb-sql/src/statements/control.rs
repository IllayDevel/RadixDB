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

impl Parser {
    /// Parse a BEGIN statement
    pub(super) fn parse_begin_statement(&mut self) -> Option<BeginStatement> {
        let token = self.cur_token.clone();

        // Check for optional TRANSACTION keyword
        if self.peek_token_is_keyword("TRANSACTION") {
            self.next_token();
        }

        // Check for ISOLATION LEVEL
        let isolation_level = if self.peek_token_is_keyword("ISOLATION") {
            self.next_token();
            if !self.expect_keyword("LEVEL") {
                return None;
            }
            self.next_token();

            let level = self.cur_token.literal.to_uppercase();
            let isolation = match level.as_str() {
                "SNAPSHOT" | "SERIALIZABLE" => level,
                "REPEATABLE" => {
                    if !self.expect_keyword("READ") {
                        return None;
                    }
                    SmartString::const_new("REPEATABLE READ")
                }
                "READ" => {
                    if self.peek_token_is_keyword("UNCOMMITTED") {
                        self.next_token();
                        SmartString::const_new("READ UNCOMMITTED")
                    } else if self.peek_token_is_keyword("COMMITTED") {
                        self.next_token();
                        SmartString::const_new("READ COMMITTED")
                    } else {
                        self.add_error(format!(
                            "expected UNCOMMITTED or COMMITTED after READ at {}",
                            self.cur_token.position
                        ));
                        return None;
                    }
                }
                _ => {
                    self.add_error(format!(
                        "invalid isolation level: {} at {}",
                        level, self.cur_token.position
                    ));
                    return None;
                }
            };
            Some(isolation)
        } else {
            None
        };

        Some(BeginStatement {
            token,
            isolation_level,
        })
    }

    /// Parse a COMMIT statement
    pub(super) fn parse_commit_statement(&mut self) -> Option<CommitStatement> {
        let token = self.cur_token.clone();

        // Check for optional TRANSACTION keyword
        if self.peek_token_is_keyword("TRANSACTION") {
            self.next_token();
        }

        Some(CommitStatement { token })
    }

    /// Parse a ROLLBACK statement
    pub(super) fn parse_rollback_statement(&mut self) -> Option<RollbackStatement> {
        let token = self.cur_token.clone();

        // Check for optional TRANSACTION keyword
        if self.peek_token_is_keyword("TRANSACTION") {
            self.next_token();
        }

        // Check for TO SAVEPOINT clause
        let savepoint_name = if self.peek_token_is_keyword("TO") {
            self.next_token();
            if self.peek_token_is_keyword("SAVEPOINT") {
                self.next_token();
            }
            if !self.expect_peek(TokenType::Identifier) {
                return None;
            }
            Some(Identifier::new(
                self.cur_token.clone(),
                self.cur_token.literal.clone(),
            ))
        } else {
            None
        };

        Some(RollbackStatement {
            token,
            savepoint_name,
        })
    }

    /// Parse a SAVEPOINT statement
    pub(super) fn parse_savepoint_statement(&mut self) -> Option<SavepointStatement> {
        let token = self.cur_token.clone();

        if !self.expect_peek(TokenType::Identifier) {
            return None;
        }

        let savepoint_name =
            Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());

        Some(SavepointStatement {
            token,
            savepoint_name,
        })
    }

    /// Parse a RELEASE SAVEPOINT statement
    pub(super) fn parse_release_savepoint_statement(
        &mut self,
    ) -> Option<ReleaseSavepointStatement> {
        let token = self.cur_token.clone();

        // Optional SAVEPOINT keyword
        if self.peek_token_is_keyword("SAVEPOINT") {
            self.next_token();
        }

        if !self.expect_peek(TokenType::Identifier) {
            return None;
        }

        let savepoint_name =
            Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());

        Some(ReleaseSavepointStatement {
            token,
            savepoint_name,
        })
    }

    /// Parse a SET statement
    pub(super) fn parse_set_statement(&mut self) -> Option<SetStatement> {
        let token = self.cur_token.clone();

        self.next_token();
        if !self.cur_token_is(TokenType::Identifier) {
            self.add_error(format!(
                "expected variable name at {}",
                self.cur_token.position
            ));
            return None;
        }

        let name = Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());

        self.next_token();
        // Expect '=' or 'TO'
        let is_equals = self.cur_token_is(TokenType::Operator) && self.cur_token.literal == "=";
        if !is_equals && !self.cur_token_is_keyword("TO") {
            self.add_error(format!(
                "expected '=' or 'TO' after variable name at {}",
                self.cur_token.position
            ));
            return None;
        }

        self.next_token();
        let value = self.parse_expression(Precedence::Lowest)?;

        Some(SetStatement { token, name, value })
    }

    /// Parse a PRAGMA statement
    pub(super) fn parse_pragma_statement(&mut self) -> Option<PragmaStatement> {
        let token = self.cur_token.clone();

        self.next_token();
        // Accept both identifiers and keywords as pragma names (e.g., PRAGMA vacuum)
        if !self.cur_token_is(TokenType::Identifier) && !self.cur_token_is(TokenType::Keyword) {
            self.add_error(format!(
                "expected pragma name at {}",
                self.cur_token.position
            ));
            return None;
        }

        let name = Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());

        self.next_token();

        // Check for optional value
        let value = if self.cur_token_is(TokenType::Operator) && self.cur_token.literal == "=" {
            self.next_token();
            Some(self.parse_expression(Precedence::Lowest)?)
        } else {
            None
        };

        Some(PragmaStatement { token, name, value })
    }

    /// Parse a SHOW statement
    pub(super) fn parse_show_statement(&mut self) -> Option<Statement> {
        let token = self.cur_token.clone();

        if self.peek_token_is_keyword("TABLES") {
            self.next_token();
            Some(Statement::ShowTables(ShowTablesStatement { token }))
        } else if self.peek_token_is_keyword("VIEWS") {
            self.next_token();
            Some(Statement::ShowViews(ShowViewsStatement { token }))
        } else if self.peek_token_is_keyword("CREATE") {
            self.next_token();
            // Check for TABLE or VIEW
            if self.peek_token_is_keyword("TABLE") {
                self.next_token();
                if !self.expect_peek(TokenType::Identifier) {
                    return None;
                }
                let table_name =
                    Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());
                Some(Statement::ShowCreateTable(ShowCreateTableStatement {
                    token,
                    table_name,
                }))
            } else if self.peek_token_is_keyword("VIEW") {
                self.next_token();
                if !self.expect_peek(TokenType::Identifier) {
                    return None;
                }
                let view_name =
                    Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());
                Some(Statement::ShowCreateView(ShowCreateViewStatement {
                    token,
                    view_name,
                }))
            } else {
                self.add_error(format!(
                    "expected TABLE or VIEW after SHOW CREATE at {}",
                    self.cur_token.position
                ));
                None
            }
        } else if self.peek_token_is_keyword("INDEXES") || self.peek_token_is_keyword("INDEX") {
            self.next_token();
            if !self.expect_keyword("FROM") {
                return None;
            }
            if !self.expect_peek(TokenType::Identifier) {
                return None;
            }
            let table_name =
                Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());
            Some(Statement::ShowIndexes(ShowIndexesStatement {
                token,
                table_name,
            }))
        } else {
            self.add_error(format!(
                "unsupported SHOW statement at {}",
                self.cur_token.position
            ));
            None
        }
    }

    /// Parse a DESCRIBE statement
    pub(super) fn parse_describe_statement(&mut self) -> Option<DescribeStatement> {
        let token = self.cur_token.clone();

        // Move past DESCRIBE/DESC keyword
        self.next_token();

        let target = if self.cur_token.literal.eq_ignore_ascii_case("DATABASE") {
            DescribeTarget::Database
        } else {
            // Optional TABLE keyword (DESCRIBE TABLE t or just DESCRIBE t).
            if self.cur_token_is_keyword("TABLE") {
                self.next_token();
            }
            if !self.cur_token_is(TokenType::Identifier) && !self.cur_token_is(TokenType::Keyword) {
                self.add_error(format!(
                    "expected TABLE name or DATABASE after DESCRIBE at {}",
                    self.cur_token.position
                ));
                return None;
            }
            DescribeTarget::Table(Identifier::new(
                self.cur_token.clone(),
                self.cur_token.literal.clone(),
            ))
        };

        let format = if self.peek_token_is_keyword("FORMAT") {
            self.next_token();
            self.next_token();
            if !self.cur_token_is_keyword("JSON") {
                self.add_error(format!(
                    "expected JSON after DESCRIBE ... FORMAT at {}",
                    self.cur_token.position
                ));
                return None;
            }
            DescribeFormat::Json
        } else {
            DescribeFormat::Tabular
        };

        if matches!(target, DescribeTarget::Database) && format != DescribeFormat::Json {
            self.add_error(
                "DESCRIBE DATABASE requires FORMAT JSON; the legacy tabular format is table-only"
                    .to_string(),
            );
            return None;
        }

        Some(DescribeStatement {
            token,
            target,
            format,
        })
    }

    /// Parse an EXPLAIN statement
    pub(super) fn parse_explain_statement(&mut self) -> Option<ExplainStatement> {
        let token = self.cur_token.clone();

        // Check for ANALYZE option
        let analyze = if self.peek_token_is_keyword("ANALYZE") {
            self.next_token();
            true
        } else {
            false
        };

        // Move to the statement to explain
        self.next_token();

        // Parse the inner statement (SELECT, INSERT, UPDATE, DELETE)
        let statement = self.parse_statement()?;

        Some(ExplainStatement {
            token,
            statement: Box::new(statement),
            analyze,
        })
    }

    /// Parse an ANALYZE statement
    /// Syntax: ANALYZE [table_name]
    pub(super) fn parse_analyze_statement(&mut self) -> Option<AnalyzeStatement> {
        let token = self.cur_token.clone();

        // Move past ANALYZE keyword
        self.next_token();

        // Optional table name
        let table_name = if self.cur_token_is(TokenType::Identifier)
            || (self.cur_token_is(TokenType::Keyword)
                && !self.cur_token.literal.eq_ignore_ascii_case("TABLE"))
        {
            let name = self.cur_token.literal.clone();
            Some(name)
        } else if self.cur_token_is(TokenType::Keyword)
            && self.cur_token.literal.eq_ignore_ascii_case("TABLE")
        {
            // ANALYZE TABLE table_name syntax
            self.next_token();
            if self.cur_token_is(TokenType::Identifier) || self.cur_token_is(TokenType::Keyword) {
                let name = self.cur_token.literal.clone();
                Some(name)
            } else {
                self.add_error(format!(
                    "expected table name after ANALYZE TABLE at {}",
                    self.cur_token.position
                ));
                return None;
            }
        } else {
            None
        };

        Some(AnalyzeStatement { token, table_name })
    }

    /// Parse an expression statement
    pub(super) fn parse_expression_statement(&mut self) -> Option<ExpressionStatement> {
        let token = self.cur_token.clone();
        let expression = self.parse_expression(Precedence::Lowest)?;

        Some(ExpressionStatement { token, expression })
    }

    /// Parse an identifier list (allows keywords as identifiers for CTE column aliases)
    pub fn parse_identifier_list(&mut self) -> Vec<Identifier> {
        let mut list = Vec::new();

        self.next_token();
        // Accept both identifiers and keywords as column names
        if self.cur_token_is(TokenType::Identifier) || self.cur_token_is(TokenType::Keyword) {
            list.push(self.cur_token_as_column_identifier());
        }

        while self.peek_token_is_punctuator(",") {
            self.next_token(); // consume comma
            self.next_token(); // move to identifier/keyword
                               // Accept both identifiers and keywords as column names
            if self.cur_token_is(TokenType::Identifier) || self.cur_token_is(TokenType::Keyword) {
                list.push(self.cur_token_as_column_identifier());
            } else {
                self.add_error(format!(
                    "expected Identifier, got {:?} at {}",
                    self.cur_token.token_type, self.cur_token.position
                ));
                return list;
            }
        }

        list
    }

    /// Parse a COPY statement
    /// COPY table [(columns)] FROM 'file_path' [WITH (FORMAT CSV|JSON [, HEADER true|false] [, DELIMITER 'c'] [, NULL 'str'])]
    pub(super) fn parse_copy_statement(&mut self) -> Option<CopyStatement> {
        let token = self.cur_token.clone();

        // Parse table name
        self.next_token();
        if !self.cur_token_is(TokenType::Identifier) && !self.cur_token_is(TokenType::Keyword) {
            self.add_error(format!(
                "expected table name after COPY, got {:?} at {}",
                self.cur_token.token_type, self.cur_token.position
            ));
            return None;
        }
        let table_name = self.parse_relation_identifier_current()?;

        // Parse optional column list
        let mut columns = Vec::new();
        if self.peek_token_is_punctuator("(") {
            self.next_token(); // consume (
            columns = self.parse_identifier_list();
            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                self.add_error(format!("expected ')' at {}", self.cur_token.position));
                return None;
            }
        }

        // Expect FROM keyword
        if !self.expect_keyword("FROM") {
            return None;
        }

        // Parse file path (string literal)
        self.next_token();
        if !self.cur_token_is(TokenType::String) {
            self.add_error(format!(
                "expected file path string after FROM, got {:?} at {}",
                self.cur_token.token_type, self.cur_token.position
            ));
            return None;
        }
        let file_path = {
            let lit = &self.cur_token.literal;
            if lit.len() >= 2
                && (lit.starts_with('\'') || lit.starts_with('"'))
                && lit.ends_with(lit.chars().next().unwrap())
            {
                lit[1..lit.len() - 1].to_string()
            } else {
                lit.to_string()
            }
        };

        // Default options
        let mut format = None;
        let mut header = true;
        let mut delimiter = b',';
        let mut null_string = None;
        let mut header_specified = false;
        let mut delimiter_specified = false;

        // Parse optional WITH (options)
        if self.peek_token_is_keyword("WITH") {
            self.next_token(); // consume WITH

            if !self.peek_token_is_punctuator("(") {
                self.add_error(format!(
                    "expected '(' after WITH at {}",
                    self.peek_token.position
                ));
                return None;
            }
            self.next_token(); // consume (

            // Parse key-value options
            loop {
                self.next_token();
                if self.cur_token_is(TokenType::Punctuator) && self.cur_token.literal == ")" {
                    break;
                }

                let key = self.cur_token.literal.to_uppercase();
                match key.as_str() {
                    "FORMAT" => {
                        self.next_token();
                        let fmt_str = self.cur_token.literal.to_uppercase();
                        match fmt_str.as_str() {
                            "CSV" => format = Some(CopyFormat::Csv),
                            "JSON" => format = Some(CopyFormat::Json),
                            _ => {
                                self.add_error(format!(
                                    "unsupported COPY format '{}', expected CSV or JSON",
                                    fmt_str
                                ));
                                return None;
                            }
                        }
                    }
                    "HEADER" => {
                        self.next_token();
                        let val = self.cur_token.literal.to_uppercase();
                        header = match val.as_str() {
                            "TRUE" | "ON" | "1" => true,
                            "FALSE" | "OFF" | "0" => false,
                            _ => {
                                self.add_error(format!(
                                    "invalid HEADER value '{}', expected TRUE or FALSE",
                                    self.cur_token.literal
                                ));
                                return None;
                            }
                        };
                        header_specified = true;
                    }
                    "DELIMITER" => {
                        self.next_token();
                        let delim_str = &self.cur_token.literal;
                        // Strip quotes if present
                        let raw = if delim_str.len() >= 2
                            && (delim_str.starts_with('\'') || delim_str.starts_with('"'))
                        {
                            &delim_str[1..delim_str.len() - 1]
                        } else {
                            delim_str.as_str()
                        };
                        if raw.len() != 1 {
                            self.add_error(format!(
                                "DELIMITER must be a single character, got '{}'",
                                raw
                            ));
                            return None;
                        }
                        delimiter = raw.as_bytes()[0];
                        delimiter_specified = true;
                    }
                    "NULL" => {
                        self.next_token();
                        let ns = &self.cur_token.literal;
                        null_string = Some(
                            if ns.len() >= 2
                                && (ns.starts_with('\'') || ns.starts_with('"'))
                                && ns.ends_with(ns.chars().next().unwrap())
                            {
                                ns[1..ns.len() - 1].to_string()
                            } else {
                                ns.to_string()
                            },
                        );
                    }
                    _ => {
                        self.add_error(format!("unknown COPY option '{}'", key));
                        return None;
                    }
                }

                // Expect comma or closing paren
                if self.peek_token_is_punctuator(",") {
                    self.next_token(); // consume comma
                } else if self.peek_token_is_punctuator(")") {
                    self.next_token(); // consume )
                    break;
                } else if !self.peek_token_is(TokenType::Eof) {
                    self.add_error(format!(
                        "expected ',' or ')' after COPY option, got {:?} at {}",
                        self.peek_token.token_type, self.peek_token.position
                    ));
                    return None;
                }
            }
        }

        // FORMAT is required
        let format = match format {
            Some(f) => f,
            None => {
                // Default to CSV if no WITH clause
                CopyFormat::Csv
            }
        };

        if format == CopyFormat::Json && (header_specified || delimiter_specified) {
            self.add_error("COPY JSON does not support HEADER or DELIMITER options".to_string());
            return None;
        }

        Some(CopyStatement {
            token,
            table_name,
            columns,
            file_path,
            format,
            header,
            delimiter,
            null_string,
        })
    }
}
