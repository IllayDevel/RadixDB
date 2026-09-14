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
    /// Parse an ALTER statement
    pub(super) fn parse_alter_statement(&mut self) -> Option<Statement> {
        let token = self.cur_token.clone();

        if self.peek_token_is_keyword("TABLE") {
            return self.parse_alter_table_statement();
        }

        if self.peek_token_is_keyword("INDEX") {
            self.next_token();
            return self
                .parse_alter_index_statement(token)
                .map(Statement::AlterIndex);
        }

        if self.peek_token_is_keyword("FUNCTION") {
            return self
                .parse_alter_routine_owner_statement(token, RoutineKindSyntax::Function)
                .map(|statement| Statement::AlterOwner(Box::new(statement)));
        }

        if self.peek_token_is_keyword("PROCEDURE") {
            return self
                .parse_alter_routine_owner_statement(token, RoutineKindSyntax::Procedure)
                .map(|statement| Statement::AlterOwner(Box::new(statement)));
        }

        if self.peek_token_is_keyword("PRINCIPAL") {
            return self
                .parse_alter_security_subject_statement(token, SecuritySubjectKindSyntax::Principal)
                .map(Statement::AlterSecuritySubject);
        }

        if self.peek_token_is_keyword("ROLE") {
            return self
                .parse_alter_security_subject_statement(token, SecuritySubjectKindSyntax::Role)
                .map(Statement::AlterSecuritySubject);
        }

        if self.peek_token_is_keyword("JOB") {
            self.next_token();
            return self
                .parse_alter_job_statement(token)
                .map(|statement| Statement::AlterJob(Box::new(statement)));
        }

        self.add_error(format!(
            "expected TABLE, INDEX, FUNCTION, PROCEDURE, PRINCIPAL, ROLE, or JOB after ALTER at {}",
            self.cur_token.position
        ));
        None
    }

    /// Parse an ALTER TABLE statement
    pub(super) fn parse_alter_table_statement(&mut self) -> Option<Statement> {
        let token = self.cur_token.clone();

        // Expect TABLE
        if !self.expect_keyword("TABLE") {
            return None;
        }

        // Parse table name
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let qualified_name = self.parse_object_name_current()?;
        if self.peek_token_is_keyword("OWNER") {
            return self
                .parse_table_owner_after_name(token, qualified_name)
                .map(|statement| Statement::AlterOwner(Box::new(statement)));
        }
        let is_qualified = qualified_name.components.len() != 1;
        let table_name = Self::relation_identifier_from_object_name(&qualified_name);

        // Parse operation
        if !self.peek_token_is(TokenType::Keyword) {
            self.add_error(
                "expected ALTER action (ADD, DROP, RENAME) after table name".to_string(),
            );
            return None;
        }
        self.next_token();

        let operation_keyword = self.cur_token.literal.to_uppercase();
        let (
            operation,
            column_def,
            table_constraint,
            column_name,
            constraint_name,
            if_exists,
            new_column_name,
            new_table_name,
        ) = match operation_keyword.as_str() {
            "ADD" => {
                if self.peek_token_is_keyword("CONSTRAINT") {
                    self.next_token();
                    self.next_token();
                    let constraint = match self.parse_column_or_constraint()? {
                        ColumnOrConstraint::Constraint(constraint) => constraint,
                        ColumnOrConstraint::Column(_) => {
                            self.add_error(
                                "expected table constraint after ADD CONSTRAINT".to_string(),
                            );
                            return None;
                        }
                    };
                    (
                        AlterTableOperation::AddConstraint,
                        None,
                        Some(constraint),
                        None,
                        None,
                        false,
                        None,
                        None,
                    )
                } else if self.peek_token_is_keyword("UNIQUE")
                    || self.peek_token_is_keyword("CHECK")
                    || self.peek_token_is_keyword("PRIMARY")
                    || self.peek_token_is_keyword("FOREIGN")
                {
                    self.next_token();
                    let constraint = match self.parse_column_or_constraint()? {
                        ColumnOrConstraint::Constraint(constraint) => constraint,
                        ColumnOrConstraint::Column(_) => unreachable!(),
                    };
                    (
                        AlterTableOperation::AddConstraint,
                        None,
                        Some(constraint),
                        None,
                        None,
                        false,
                        None,
                        None,
                    )
                } else {
                    // Check for optional COLUMN keyword
                    if self.peek_token_is_keyword("COLUMN") {
                        self.next_token();
                    }
                    self.next_token();
                    let col_def = self.parse_column_definition()?;
                    (
                        AlterTableOperation::AddColumn,
                        Some(col_def),
                        None,
                        None,
                        None,
                        false,
                        None,
                        None,
                    )
                }
            }
            "DROP" => {
                if self.peek_token_is_keyword("CONSTRAINT") {
                    self.next_token();
                    let mut if_exists = false;
                    if self.peek_token_is_keyword("IF") {
                        self.next_token();
                        if !self.expect_keyword("EXISTS") {
                            return None;
                        }
                        if_exists = true;
                    }
                    if !self.expect_peek(TokenType::Identifier) {
                        return None;
                    }
                    let constraint_name =
                        Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());
                    (
                        AlterTableOperation::DropConstraint,
                        None,
                        None,
                        None,
                        Some(constraint_name),
                        if_exists,
                        None,
                        None,
                    )
                } else {
                    // Check for optional COLUMN keyword
                    if self.peek_token_is_keyword("COLUMN") {
                        self.next_token();
                    }
                    if !self.expect_peek(TokenType::Identifier) {
                        return None;
                    }
                    let col_name =
                        Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());
                    (
                        AlterTableOperation::DropColumn,
                        None,
                        None,
                        Some(col_name),
                        None,
                        false,
                        None,
                        None,
                    )
                }
            }
            "RENAME" => {
                if !self.expect_peek(TokenType::Keyword) {
                    return None;
                }
                let rename_keyword = self.cur_token.literal.to_uppercase();
                if rename_keyword == "COLUMN" {
                    if !self.expect_peek(TokenType::Identifier) {
                        return None;
                    }
                    let col_name =
                        Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());
                    if !self.expect_keyword("TO") {
                        return None;
                    }
                    if !self.expect_peek(TokenType::Identifier) {
                        return None;
                    }
                    let new_col_name =
                        Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());
                    (
                        AlterTableOperation::RenameColumn,
                        None,
                        None,
                        Some(col_name),
                        None,
                        false,
                        Some(new_col_name),
                        None,
                    )
                } else if rename_keyword == "TO" {
                    if !self.expect_peek(TokenType::Identifier) {
                        return None;
                    }
                    let new_tbl_name =
                        Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());
                    (
                        AlterTableOperation::RenameTable,
                        None,
                        None,
                        None,
                        None,
                        false,
                        None,
                        Some(new_tbl_name),
                    )
                } else {
                    self.add_error(format!(
                        "expected COLUMN or TO after RENAME at {}",
                        self.cur_token.position
                    ));
                    return None;
                }
            }
            "MODIFY" => {
                // Check for optional COLUMN keyword
                if self.peek_token_is_keyword("COLUMN") {
                    self.next_token();
                }
                self.next_token();
                let col_def = self.parse_column_definition()?;
                (
                    AlterTableOperation::ModifyColumn,
                    Some(col_def),
                    None,
                    None,
                    None,
                    false,
                    None,
                    None,
                )
            }
            _ => {
                self.add_error(format!(
                    "expected ADD, DROP, RENAME, or MODIFY at {}",
                    self.cur_token.position
                ));
                return None;
            }
        };

        if self.reject_unsupported_qualified_alter(is_qualified, operation) {
            return None;
        }

        Some(Statement::AlterTable(Box::new(AlterTableStatement {
            token,
            table_name,
            operation,
            column_def,
            table_constraint,
            column_name,
            constraint_name,
            if_exists,
            new_column_name,
            new_table_name,
        })))
    }

    /// Parse an ALTER INDEX statement.
    ///
    /// The public contract is intentionally narrow for now:
    /// ALTER INDEX old_name RENAME TO new_name
    pub(super) fn parse_alter_index_statement(
        &mut self,
        token: Token,
    ) -> Option<AlterIndexStatement> {
        // Current token is INDEX, parse old index name.
        if !self.expect_peek(TokenType::Identifier) {
            return None;
        }
        let index_name = Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());

        if !self.expect_keyword("RENAME") {
            return None;
        }
        if !self.expect_keyword("TO") {
            return None;
        }
        if !self.expect_peek(TokenType::Identifier) {
            return None;
        }
        let new_index_name =
            Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());

        Some(AlterIndexStatement {
            token,
            index_name,
            new_index_name,
        })
    }
}
