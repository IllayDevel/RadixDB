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
    /// Parse a CREATE statement
    pub(super) fn parse_create_statement(&mut self) -> Option<Statement> {
        let create_token = self.cur_token.clone();
        if self.peek_token_is_keyword("OR") {
            self.next_token();
            if !self.expect_keyword("REPLACE") {
                return None;
            }
            if self.peek_token_is_keyword("FUNCTION") {
                self.next_token();
                self.parse_create_routine_statement(create_token, true, RoutineKindSyntax::Function)
                    .map(|statement| Statement::CreateRoutine(Box::new(statement)))
            } else if self.peek_token_is_keyword("PROCEDURE") {
                self.next_token();
                self.parse_create_routine_statement(
                    create_token,
                    true,
                    RoutineKindSyntax::Procedure,
                )
                .map(|statement| Statement::CreateRoutine(Box::new(statement)))
            } else if self.peek_token_is_keyword("TRIGGER") {
                self.next_token();
                self.parse_create_trigger_statement(create_token, true)
                    .map(|statement| Statement::CreateTrigger(Box::new(statement)))
            } else {
                self.add_error(
                    "expected FUNCTION, PROCEDURE, or TRIGGER after CREATE OR REPLACE".to_string(),
                );
                None
            }
        } else if self.peek_token_is_keyword("FUNCTION") {
            self.next_token();
            self.parse_create_routine_statement(create_token, false, RoutineKindSyntax::Function)
                .map(|statement| Statement::CreateRoutine(Box::new(statement)))
        } else if self.peek_token_is_keyword("PROCEDURE") {
            self.next_token();
            self.parse_create_routine_statement(create_token, false, RoutineKindSyntax::Procedure)
                .map(|statement| Statement::CreateRoutine(Box::new(statement)))
        } else if self.peek_token_is_keyword("TRIGGER") {
            self.next_token();
            self.parse_create_trigger_statement(create_token, false)
                .map(|statement| Statement::CreateTrigger(Box::new(statement)))
        } else if self.peek_token_is_keyword("JOB") {
            self.next_token();
            self.parse_create_job_statement(create_token)
                .map(|statement| Statement::CreateJob(Box::new(statement)))
        } else if self.peek_token_is_keyword("SCHEMA") {
            self.next_token();
            self.parse_create_schema_statement(create_token)
                .map(Statement::CreateSchema)
        } else if self.peek_token_is_keyword("PRINCIPAL") {
            self.next_token();
            self.parse_create_principal_statement(create_token)
                .map(Statement::CreatePrincipal)
        } else if self.peek_token_is_keyword("ROLE") {
            self.next_token();
            self.parse_create_role_statement(create_token)
                .map(Statement::CreateRole)
        } else if self.peek_token_is_keyword("TABLE") {
            self.next_token();
            self.parse_create_table_statement()
                .map(Statement::CreateTable)
        } else if self.peek_token_is_keyword("UNIQUE") {
            self.next_token();
            if !self.expect_keyword("INDEX") {
                return None;
            }
            self.parse_create_index_statement(true)
                .map(Statement::CreateIndex)
        } else if self.peek_token_is_keyword("INDEX") {
            self.next_token();
            self.parse_create_index_statement(false)
                .map(Statement::CreateIndex)
        } else if self.peek_token_is_keyword("VIEW") {
            self.next_token();
            self.parse_create_view_statement()
                .map(Statement::CreateView)
        } else if self.peek_token_is_keyword("EXTENSION") {
            self.next_token();
            self.parse_create_extension_statement(create_token)
                .map(Statement::CreateExtension)
        } else if self.peek_token_is_keyword("TYPE") {
            self.next_token();
            self.parse_create_external_type_statement(create_token)
                .map(Statement::CreateExternalType)
        } else if self.peek_token_is_keyword("OPERATOR") {
            self.next_token();
            if self.peek_token_is_keyword("CLASS") {
                self.next_token();
                self.parse_create_operator_class_statement(create_token)
                    .map(|statement| Statement::CreateOperatorClass(Box::new(statement)))
            } else {
                self.parse_create_operator_statement(create_token)
                    .map(|statement| Statement::CreateOperator(Box::new(statement)))
            }
        } else if self.peek_token_is_keyword("PLANNER") {
            self.next_token();
            if !self.expect_keyword("SUPPORT") {
                return None;
            }
            self.parse_create_planner_support_statement(create_token)
                .map(|statement| Statement::CreatePlannerSupport(Box::new(statement)))
        } else {
            self.add_error(format!(
                "expected TABLE, INDEX, VIEW, EXTENSION, TYPE, SCHEMA, PRINCIPAL, ROLE, FUNCTION, PROCEDURE, TRIGGER, or JOB after CREATE at {}",
                self.cur_token.position
            ));
            None
        }
    }

    fn parse_create_external_type_statement(
        &mut self,
        token: Token,
    ) -> Option<CreateExternalTypeStatement> {
        if !self.expect_peek_identifier_like() {
            return None;
        }
        let name = self.parse_object_name_current()?;
        if name.components.len() < 2 {
            self.add_error("external type name must be schema-qualified".to_string());
            return None;
        }
        if !self.expect_keyword("FROM") || !self.expect_keyword("EXTENSION") {
            return None;
        }
        if !self.expect_peek_identifier_like() {
            return None;
        }
        let extension_name = self.cur_token_as_column_identifier();
        if !self.expect_keyword("AS") || !self.expect_peek(TokenType::String) {
            return None;
        }
        let local_id = SmartString::from(unquote_ddl_string(&self.cur_token.literal));
        if local_id.is_empty() || local_id.len() > 255 || local_id.as_bytes().contains(&0) {
            self.add_error(
                "external type local id must be 1..=255 UTF-8 bytes without NUL".to_string(),
            );
            return None;
        }
        Some(CreateExternalTypeStatement {
            token,
            name,
            extension_name,
            local_id,
        })
    }

    fn parse_create_extension_statement(
        &mut self,
        token: Token,
    ) -> Option<CreateExtensionStatement> {
        let if_not_exists = if self.peek_token_is_keyword("IF") {
            self.next_token();
            if !self.expect_keyword("NOT") || !self.expect_keyword("EXISTS") {
                return None;
            }
            true
        } else {
            false
        };
        if !self.expect_peek_identifier_like() {
            return None;
        }
        let name = self.cur_token_as_column_identifier();
        if !self.expect_keyword("VERSION") || !self.expect_peek(TokenType::String) {
            return None;
        }
        Some(CreateExtensionStatement {
            token,
            name,
            version: SmartString::from(unquote_ddl_string(&self.cur_token.literal)),
            if_not_exists,
        })
    }

    /// Parse a CREATE TABLE statement
    pub(super) fn parse_create_table_statement(&mut self) -> Option<CreateTableStatement> {
        let token = self.cur_token.clone();

        // Check for IF NOT EXISTS
        let if_not_exists = if self.peek_token_is_keyword("IF") {
            self.next_token();
            if !self.expect_keyword("NOT") {
                return None;
            }
            if !self.expect_keyword("EXISTS") {
                return None;
            }
            true
        } else {
            false
        };

        // Parse table name (allow non-reserved keywords like COPY as table names)
        if !self.expect_peek_identifier_like() {
            return None;
        }
        let table_name = self.parse_relation_identifier_current()?;

        // Check for AS SELECT (CREATE TABLE ... AS SELECT ...)
        if self.peek_token_is_keyword("AS") {
            self.next_token(); // consume AS
            if !self.expect_keyword("SELECT") {
                return None;
            }
            let select_stmt = self.parse_select_statement()?;
            return Some(CreateTableStatement {
                token,
                table_name,
                if_not_exists,
                columns: Vec::new(),
                table_constraints: Vec::new(),
                as_select: Some(Box::new(select_stmt)),
            });
        }

        // Expect (
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "(" {
            self.add_error(format!("expected '(' at {}", self.cur_token.position));
            return None;
        }

        // Parse column definitions and table-level constraints
        let (columns, table_constraints) = self.parse_column_definitions_and_constraints();

        // Expect )
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
            self.add_error(format!("expected ')' at {}", self.cur_token.position));
            return None;
        }

        Some(CreateTableStatement {
            token,
            table_name,
            if_not_exists,
            columns,
            table_constraints,
            as_select: None,
        })
    }

    /// Parse column definitions and table-level constraints
    pub(super) fn parse_column_definitions_and_constraints(
        &mut self,
    ) -> (Vec<ColumnDefinition>, Vec<TableConstraint>) {
        let mut columns = Vec::new();
        let mut table_constraints = Vec::new();

        self.next_token();

        // First item could be a column or a table constraint
        if let Some(item) = self.parse_column_or_constraint() {
            match item {
                ColumnOrConstraint::Column(col) => columns.push(col),
                ColumnOrConstraint::Constraint(tc) => table_constraints.push(tc),
            }
        }

        while self.peek_token_is_punctuator(",") {
            self.next_token(); // consume comma
            self.next_token();
            if let Some(item) = self.parse_column_or_constraint() {
                match item {
                    ColumnOrConstraint::Column(col) => columns.push(col),
                    ColumnOrConstraint::Constraint(tc) => table_constraints.push(tc),
                }
            }
        }

        (columns, table_constraints)
    }

    /// Parse either a column definition or a table-level constraint
    pub(super) fn parse_column_or_constraint(&mut self) -> Option<ColumnOrConstraint> {
        // Check if this is a table-level constraint (UNIQUE, CHECK, PRIMARY KEY)
        if self.cur_token_is_keyword("UNIQUE") {
            // UNIQUE(col1, col2, ...)
            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "(" {
                return None;
            }
            let columns = self.parse_constraint_column_list()?;
            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                return None;
            }
            return Some(ColumnOrConstraint::Constraint(TableConstraint::Unique(
                columns,
            )));
        }

        if self.cur_token_is_keyword("CHECK") {
            // CHECK(expression)
            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "(" {
                return None;
            }
            self.next_token();
            let expr = self.parse_expression(Precedence::Lowest)?;
            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                return None;
            }
            return Some(ColumnOrConstraint::Constraint(TableConstraint::Check(
                Box::new(expr),
            )));
        }

        if self.cur_token_is_keyword("PRIMARY") {
            // PRIMARY KEY(col1, col2, ...)
            if !self.expect_keyword("KEY") {
                return None;
            }
            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "(" {
                return None;
            }
            let columns = self.parse_constraint_column_list()?;
            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                return None;
            }
            return Some(ColumnOrConstraint::Constraint(TableConstraint::PrimaryKey(
                columns,
            )));
        }

        if self.cur_token_is_keyword("FOREIGN") {
            // FOREIGN KEY(col) REFERENCES parent(col) [ON DELETE ...] [ON UPDATE ...]
            if !self.expect_keyword("KEY") {
                return None;
            }
            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "(" {
                return None;
            }
            // Parse single FK column
            if !self.expect_peek(TokenType::Identifier) {
                return None;
            }
            let fk_column = Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());
            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                return None;
            }
            // Expect REFERENCES
            if !self.expect_keyword("REFERENCES") {
                return None;
            }
            if !self.expect_peek(TokenType::Identifier) {
                return None;
            }
            let ref_table = Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());
            let ref_column = if self.peek_token_is_punctuator("(") {
                self.next_token();
                if !self.expect_peek(TokenType::Identifier) {
                    return None;
                }
                let col = Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());
                if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                    return None;
                }
                Some(col)
            } else {
                None
            };
            let (on_delete, on_update) = self.parse_fk_actions();
            return Some(ColumnOrConstraint::Constraint(TableConstraint::ForeignKey(
                Box::new(ForeignKeyTableConstraint {
                    column: fk_column,
                    ref_table,
                    ref_column,
                    on_delete,
                    on_update,
                }),
            )));
        }

        // Otherwise, parse as a column definition
        self.parse_column_definition()
            .map(ColumnOrConstraint::Column)
    }

    /// Parse a comma-separated list of column identifiers (for UNIQUE(col1, col2) etc.)
    pub(super) fn parse_constraint_column_list(&mut self) -> Option<Vec<Identifier>> {
        let mut identifiers = Vec::new();

        self.next_token();
        if !self.cur_token_is_identifier_like() {
            self.add_error(format!(
                "expected column name at {}",
                self.cur_token.position
            ));
            return None;
        }
        identifiers.push(self.cur_token_as_column_identifier());

        while self.peek_token_is_punctuator(",") {
            self.next_token(); // consume comma
            self.next_token();
            if !self.cur_token_is_identifier_like() {
                self.add_error(format!(
                    "expected column name at {}",
                    self.cur_token.position
                ));
                return None;
            }
            identifiers.push(self.cur_token_as_column_identifier());
        }

        Some(identifiers)
    }

    /// Parse ON DELETE / ON UPDATE foreign key actions.
    /// Returns (on_delete, on_update) with Restrict as default.
    pub(super) fn parse_fk_actions(&mut self) -> (ForeignKeyAction, ForeignKeyAction) {
        let mut on_delete = ForeignKeyAction::Restrict;
        let mut on_update = ForeignKeyAction::Restrict;
        let mut seen_delete = false;
        let mut seen_update = false;

        // Parse ON DELETE and ON UPDATE in either order, rejecting duplicates.
        while self.peek_token_is_keyword("ON") {
            if !self.peek_token_is_keyword("ON") {
                break;
            }
            self.next_token(); // consume ON
            self.next_token(); // move to DELETE or UPDATE
            let upper = self.cur_token.literal.to_uppercase();
            match upper.as_str() {
                "DELETE" => {
                    if seen_delete {
                        self.add_error("duplicate ON DELETE foreign key action".to_string());
                    }
                    seen_delete = true;
                    self.next_token(); // move to action
                    on_delete = self.parse_fk_action_value();
                }
                "UPDATE" => {
                    if seen_update {
                        self.add_error("duplicate ON UPDATE foreign key action".to_string());
                    }
                    seen_update = true;
                    self.next_token(); // move to action
                    on_update = self.parse_fk_action_value();
                }
                _ => break,
            }
        }

        (on_delete, on_update)
    }

    /// Parse a single FK action value: RESTRICT | CASCADE | SET NULL | NO ACTION
    pub(super) fn parse_fk_action_value(&mut self) -> ForeignKeyAction {
        let upper = self.cur_token.literal.to_uppercase();
        match upper.as_str() {
            "RESTRICT" => ForeignKeyAction::Restrict,
            "CASCADE" => ForeignKeyAction::Cascade,
            "SET" => {
                // SET NULL — validate the next token is actually "NULL"
                self.next_token();
                if self.cur_token.literal.to_uppercase() != "NULL" {
                    self.add_error(format!(
                        "expected NULL after SET in foreign key action, got '{}'",
                        self.cur_token.literal
                    ));
                }
                ForeignKeyAction::SetNull
            }
            "NO" => {
                // NO ACTION — validate the next token is actually "ACTION"
                self.next_token();
                if self.cur_token.literal.to_uppercase() != "ACTION" {
                    self.add_error(format!(
                        "expected ACTION after NO in foreign key action, got '{}'",
                        self.cur_token.literal
                    ));
                }
                ForeignKeyAction::NoAction
            }
            _ => {
                self.add_error(format!(
                    "unknown foreign key action '{}'; expected RESTRICT, CASCADE, SET NULL, or NO ACTION",
                    self.cur_token.literal
                ));
                ForeignKeyAction::Restrict
            }
        }
    }

    /// Parse a single column definition
    pub(super) fn parse_column_definition(&mut self) -> Option<ColumnDefinition> {
        // Allow both identifiers and non-reserved keywords as column names
        if !self.cur_token_is_identifier_like() {
            // Check if it's a reserved keyword and give a better error message
            if self.cur_token.token_type == TokenType::Keyword
                && Self::is_reserved_keyword(&self.cur_token.literal)
            {
                self.add_error(format!(
                    "'{}' is a reserved keyword and cannot be used as a column name. Use double quotes to escape it: \"{}\"",
                    self.cur_token.literal.to_uppercase(),
                    self.cur_token.literal
                ));
            } else {
                self.add_error(format!(
                    "expected column name at {}",
                    self.cur_token.position
                ));
            }
            return None;
        }

        let name = self.cur_token_as_column_identifier();

        let data_type = self.parse_column_data_type()?;

        // Parse constraints
        let mut constraints = Vec::new();
        while self.peek_token_is(TokenType::Keyword) {
            let constraint_keyword = self.peek_token.literal.to_uppercase();
            match constraint_keyword.as_str() {
                "PRIMARY" => {
                    self.next_token(); // consume PRIMARY
                    if !self.expect_keyword("KEY") {
                        return None;
                    }
                    constraints.push(ColumnConstraint::PrimaryKey);
                }
                "NOT" => {
                    self.next_token(); // consume NOT
                    if !self.expect_keyword("NULL") {
                        return None;
                    }
                    constraints.push(ColumnConstraint::NotNull);
                }
                "UNIQUE" => {
                    self.next_token();
                    constraints.push(ColumnConstraint::Unique);
                }
                "DEFAULT" => {
                    self.next_token(); // consume DEFAULT
                    self.next_token();
                    let expr = self.parse_expression(Precedence::Lowest)?;
                    constraints.push(ColumnConstraint::Default(expr));
                }
                "CHECK" => {
                    self.next_token(); // consume CHECK
                    if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "(" {
                        return None;
                    }
                    self.next_token();
                    let expr = self.parse_expression(Precedence::Lowest)?;
                    if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                        return None;
                    }
                    constraints.push(ColumnConstraint::Check(expr));
                }
                "REFERENCES" => {
                    self.next_token(); // consume REFERENCES
                    if !self.expect_peek(TokenType::Identifier) {
                        return None;
                    }
                    let ref_table =
                        Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());
                    let ref_column = if self.peek_token_is_punctuator("(") {
                        self.next_token();
                        if !self.expect_peek(TokenType::Identifier) {
                            return None;
                        }
                        let col =
                            Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());
                        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")"
                        {
                            return None;
                        }
                        Some(col)
                    } else {
                        None
                    };
                    let (on_delete, on_update) = self.parse_fk_actions();
                    constraints.push(ColumnConstraint::References {
                        table: ref_table,
                        column: ref_column,
                        on_delete,
                        on_update,
                    });
                }
                "AUTO_INCREMENT" | "AUTOINCREMENT" => {
                    constraints.push(ColumnConstraint::AutoIncrement);
                    self.next_token();
                }
                _ => break,
            }
        }

        Some(ColumnDefinition {
            name,
            data_type,
            constraints,
        })
    }

    /// Parse a column data type name plus optional type arguments.
    ///
    /// The executor already normalizes a broad set of SQL type aliases in
    /// `parse_data_type()`. The parser should therefore preserve forms such as
    /// `VARCHAR(255)`, `DECIMAL(10,2)`, `NUMERIC(12)`, `VECTOR(384)` and aliases
    /// lexed as identifiers (`JSONB`, `BLOB`, `VARBINARY`) instead of rejecting
    /// them before DDL validation gets a chance to apply the common contract.
    pub(crate) fn parse_column_data_type(&mut self) -> Option<SmartString> {
        if !matches!(
            self.peek_token.token_type,
            TokenType::Keyword | TokenType::Identifier
        ) {
            self.add_error(format!(
                "expected column data type after {}, got {}",
                self.cur_token.literal,
                Self::format_token_for_error(&self.peek_token)
            ));
            return None;
        }

        self.next_token();
        let mut base_type = self.cur_token.literal.to_uppercase();

        while self.peek_token_is_punctuator(".") {
            self.next_token();
            if !self.expect_peek_identifier_like() {
                return None;
            }
            base_type.push('.');
            base_type.push_str(&self.cur_token.literal.to_uppercase());
        }

        if !self.peek_token_is_punctuator("(") {
            return Some(SmartString::from_string(base_type.to_string()));
        }

        self.next_token(); // consume '('
        let mut args = Vec::new();
        let mut expect_value = true;

        loop {
            if self.peek_token_is_punctuator(")") {
                if args.is_empty() {
                    self.add_error(format!("{} type parameters cannot be empty", base_type));
                    return None;
                }
                if expect_value {
                    self.add_error(format!("trailing comma in {} type parameters", base_type));
                    return None;
                }
                self.next_token(); // consume ')'
                break;
            }

            self.next_token();

            if expect_value {
                match self.cur_token.token_type {
                    TokenType::Integer | TokenType::Identifier | TokenType::Keyword => {
                        args.push(self.cur_token.literal.to_string());
                        expect_value = false;
                    }
                    _ => {
                        self.add_error(format!(
                            "expected value in {} type parameters, got {}",
                            base_type,
                            Self::format_token_for_error(&self.cur_token)
                        ));
                        return None;
                    }
                }
            } else if self.cur_token_is_punctuator(",") {
                expect_value = true;
            } else {
                self.add_error(format!(
                    "expected ',' or ')' in {} type parameters, got {}",
                    base_type,
                    Self::format_token_for_error(&self.cur_token)
                ));
                return None;
            }
        }

        if base_type == "VECTOR" {
            if args.len() != 1 {
                self.add_error(
                    "VECTOR requires exactly one positive integer dimension, e.g. VECTOR(384)"
                        .to_string(),
                );
                return None;
            }
            let dim: u16 = match args[0].parse::<u16>() {
                Ok(d) if d > 0 => d,
                _ => {
                    self.add_error(format!(
                        "VECTOR dimension must be between 1 and 65535, got '{}'",
                        args[0]
                    ));
                    return None;
                }
            };
            return Some(SmartString::from_string(format!("VECTOR({dim})")));
        }

        Some(SmartString::from_string(format!(
            "{}({})",
            base_type,
            args.join(",")
        )))
    }

    /// Parse a CREATE INDEX statement
    pub(super) fn parse_create_index_statement(
        &mut self,
        is_unique: bool,
    ) -> Option<CreateIndexStatement> {
        let token = self.cur_token.clone();

        // Check for IF NOT EXISTS
        let if_not_exists = if self.peek_token_is_keyword("IF") {
            self.next_token();
            if !self.expect_keyword("NOT") {
                return None;
            }
            if !self.expect_keyword("EXISTS") {
                return None;
            }
            true
        } else {
            false
        };

        // Parse index name
        if !self.peek_token_is(TokenType::Identifier) {
            self.add_error(format!(
                "expected index name after CREATE INDEX, got {}",
                Self::format_token_for_error(&self.peek_token)
            ));
            return None;
        }
        self.next_token();
        let index_name = Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());

        // Expect ON
        if !self.expect_keyword("ON") {
            return None;
        }

        // Parse table name
        if !self.expect_peek(TokenType::Identifier) {
            return None;
        }
        let table_name = self.parse_relation_identifier_current()?;

        // Expect (
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "(" {
            self.add_error(format!("expected '(' at {}", self.cur_token.position));
            return None;
        }

        let (columns, operator_class) = self.parse_index_keys_and_operator_class()?;

        // Expect )
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
            self.add_error(format!("expected ')' at {}", self.cur_token.position));
            return None;
        }

        // Parse optional USING clause
        let index_method = if self.peek_token_is_keyword("USING") {
            self.next_token(); // consume USING
            self.next_token(); // move to method name

            Some(self.parse_index_method_current()?)
        } else {
            None
        };

        // Parse optional WITH clause: WITH (key = value, ...)
        let options = if self.peek_token_is_keyword("WITH") {
            self.next_token(); // consume WITH
            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "(" {
                self.add_error(format!(
                    "expected '(' after WITH at {}",
                    self.cur_token.position
                ));
                return None;
            }
            let mut opts = Vec::new();
            loop {
                // Parse key
                if !self.expect_peek(TokenType::Identifier) {
                    return None;
                }
                let key = self.cur_token.literal.to_lowercase().to_string();

                // Parse = (tokenized as Operator, not Punctuator)
                if !self.expect_peek(TokenType::Operator) || self.cur_token.literal != "=" {
                    self.add_error(format!(
                        "expected '=' after option name at {}",
                        self.cur_token.position
                    ));
                    return None;
                }

                // Parse a scalar expression. Runtime parameters are resolved
                // once by the DDL executor before index metadata is stored.
                self.next_token();
                let value = self.parse_expression(Precedence::Lowest)?;

                opts.push((key, value));

                // Check for comma or closing paren
                if self.peek_token_is(TokenType::Punctuator) && self.peek_token.literal == "," {
                    self.next_token(); // consume comma
                } else {
                    break;
                }
            }
            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
                self.add_error(format!("expected ')' at {}", self.cur_token.position));
                return None;
            }
            opts
        } else {
            Vec::new()
        };

        // Parse optional WHERE clause for partial indexes.
        let where_clause = if self.peek_token_is_keyword("WHERE") {
            self.next_token(); // consume WHERE
            self.current_clause = "CREATE INDEX WHERE".to_string();
            self.next_token();
            Some(Box::new(self.parse_expression(Precedence::Lowest)?))
        } else {
            None
        };

        Some(CreateIndexStatement {
            token,
            index_name,
            table_name,
            columns,
            is_unique,
            if_not_exists,
            index_method,
            options,
            where_clause,
            operator_class,
        })
    }

    /// Parse a CREATE VIEW statement
    pub(super) fn parse_create_view_statement(&mut self) -> Option<CreateViewStatement> {
        let token = self.cur_token.clone();

        // Check for IF NOT EXISTS
        let if_not_exists = if self.peek_token_is_keyword("IF") {
            self.next_token();
            if !self.expect_keyword("NOT") {
                return None;
            }
            if !self.expect_keyword("EXISTS") {
                return None;
            }
            true
        } else {
            false
        };

        // Parse view name
        if !self.expect_peek(TokenType::Identifier) {
            return None;
        }
        let view_name = self.parse_relation_identifier_current()?;

        // Expect AS
        if !self.expect_keyword("AS") {
            return None;
        }

        // Expect SELECT or WITH (for CTEs)
        self.next_token();
        let query = if self.cur_token_is_keyword("SELECT") {
            self.parse_select_statement()?
        } else if self.cur_token_is_keyword("WITH") {
            // Parse CTE and attach to SELECT
            let with_clause = self.parse_with_clause()?;
            self.next_token();
            if !self.cur_token_is_keyword("SELECT") {
                self.add_error(format!(
                    "expected SELECT after WITH clause in CREATE VIEW at {}",
                    self.cur_token.position
                ));
                return None;
            }
            let mut select = self.parse_select_statement()?;
            select.with = Some(with_clause);
            select
        } else {
            self.add_error(format!(
                "expected SELECT or WITH after AS in CREATE VIEW at {}",
                self.cur_token.position
            ));
            return None;
        };

        Some(CreateViewStatement {
            token,
            view_name,
            query: Box::new(query),
            if_not_exists,
        })
    }

    /// Parse a DROP statement
    pub(super) fn parse_drop_statement(&mut self) -> Option<Statement> {
        if self.peek_token_is_keyword("TABLE") {
            self.next_token();
            self.parse_drop_table_statement().map(Statement::DropTable)
        } else if self.peek_token_is_keyword("INDEX") {
            self.next_token();
            self.parse_drop_index_statement().map(Statement::DropIndex)
        } else if self.peek_token_is_keyword("VIEW") {
            self.next_token();
            self.parse_drop_view_statement().map(Statement::DropView)
        } else if self.peek_token_is_keyword("EXTENSION") {
            self.next_token();
            self.parse_drop_extension_statement(self.cur_token.clone())
                .map(Statement::DropExtension)
        } else if self.peek_token_is_keyword("TYPE") {
            self.next_token();
            self.parse_drop_external_type_statement(self.cur_token.clone())
                .map(Statement::DropExternalType)
        } else if self.peek_token_is_keyword("OPERATOR") {
            self.next_token();
            if self.peek_token_is_keyword("CLASS") {
                self.next_token();
                self.parse_drop_operator_class_statement(self.cur_token.clone())
                    .map(|statement| Statement::DropOperatorClass(Box::new(statement)))
            } else {
                self.parse_drop_operator_statement(self.cur_token.clone())
                    .map(|statement| Statement::DropOperator(Box::new(statement)))
            }
        } else if self.peek_token_is_keyword("PLANNER") {
            self.next_token();
            if !self.expect_keyword("SUPPORT") {
                return None;
            }
            self.parse_drop_planner_support_statement(self.cur_token.clone())
                .map(|statement| Statement::DropPlannerSupport(Box::new(statement)))
        } else if self.peek_token_is_keyword("PRINCIPAL") {
            self.next_token();
            self.parse_drop_security_subject_statement(
                self.cur_token.clone(),
                SecuritySubjectKindSyntax::Principal,
            )
            .map(Statement::DropSecuritySubject)
        } else if self.peek_token_is_keyword("ROLE") {
            self.next_token();
            self.parse_drop_security_subject_statement(
                self.cur_token.clone(),
                SecuritySubjectKindSyntax::Role,
            )
            .map(Statement::DropSecuritySubject)
        } else if self.peek_token_is_keyword("FUNCTION") {
            self.next_token();
            self.parse_drop_routine_statement(self.cur_token.clone(), RoutineKindSyntax::Function)
                .map(|statement| Statement::DropRoutine(Box::new(statement)))
        } else if self.peek_token_is_keyword("PROCEDURE") {
            self.next_token();
            self.parse_drop_routine_statement(self.cur_token.clone(), RoutineKindSyntax::Procedure)
                .map(|statement| Statement::DropRoutine(Box::new(statement)))
        } else if self.peek_token_is_keyword("TRIGGER") {
            self.next_token();
            self.parse_drop_trigger_statement(self.cur_token.clone())
                .map(|statement| Statement::DropTrigger(Box::new(statement)))
        } else if self.peek_token_is_keyword("JOB") {
            self.next_token();
            self.parse_drop_job_statement(self.cur_token.clone())
                .map(|statement| Statement::DropJob(Box::new(statement)))
        } else {
            self.add_error(format!(
                "expected TABLE, INDEX, VIEW, EXTENSION, TYPE, PRINCIPAL, ROLE, FUNCTION, PROCEDURE, TRIGGER, or JOB after DROP at {}",
                self.cur_token.position
            ));
            None
        }
    }

    fn parse_drop_external_type_statement(
        &mut self,
        token: Token,
    ) -> Option<DropExternalTypeStatement> {
        let if_exists = if self.peek_token_is_keyword("IF") {
            self.next_token();
            if !self.expect_keyword("EXISTS") {
                return None;
            }
            true
        } else {
            false
        };
        if !self.expect_peek_identifier_like() {
            return None;
        }
        let name = self.parse_object_name_current()?;
        if name.components.len() < 2 {
            self.add_error("external type name must be schema-qualified".to_string());
            return None;
        }
        if !self.expect_keyword("RESTRICT") {
            return None;
        }
        Some(DropExternalTypeStatement {
            token,
            name,
            if_exists,
        })
    }

    fn parse_drop_extension_statement(&mut self, token: Token) -> Option<DropExtensionStatement> {
        let if_exists = if self.peek_token_is_keyword("IF") {
            self.next_token();
            if !self.expect_keyword("EXISTS") {
                return None;
            }
            true
        } else {
            false
        };
        if !self.expect_peek_identifier_like() {
            return None;
        }
        let name = self.cur_token_as_column_identifier();
        if !self.expect_keyword("RESTRICT") {
            return None;
        }
        Some(DropExtensionStatement {
            token,
            name,
            if_exists,
        })
    }

    /// Parse a DROP TABLE statement
    pub(super) fn parse_drop_table_statement(&mut self) -> Option<DropTableStatement> {
        let token = self.cur_token.clone();

        // Check for IF EXISTS
        let if_exists = if self.peek_token_is_keyword("IF") {
            self.next_token();
            if !self.expect_keyword("EXISTS") {
                return None;
            }
            true
        } else {
            false
        };

        // Parse table name
        if !self.expect_peek(TokenType::Identifier) {
            return None;
        }
        let table_name = self.parse_relation_identifier_current()?;

        Some(DropTableStatement {
            token,
            table_name,
            if_exists,
        })
    }

    /// Parse a DROP INDEX statement
    pub(super) fn parse_drop_index_statement(&mut self) -> Option<DropIndexStatement> {
        let token = self.cur_token.clone();

        // Check for IF EXISTS
        let if_exists = if self.peek_token_is_keyword("IF") {
            self.next_token();
            if !self.expect_keyword("EXISTS") {
                return None;
            }
            true
        } else {
            false
        };

        // Parse index name
        if !self.expect_peek(TokenType::Identifier) {
            return None;
        }
        let index_name = Identifier::new(self.cur_token.clone(), self.cur_token.literal.clone());

        // Check for optional ON clause
        let table_name = if self.peek_token_is_keyword("ON") {
            self.next_token();
            if !self.expect_peek(TokenType::Identifier) {
                return None;
            }
            Some(self.parse_relation_identifier_current()?)
        } else {
            None
        };

        Some(DropIndexStatement {
            token,
            index_name,
            table_name,
            if_exists,
        })
    }

    /// Parse a DROP VIEW statement
    pub(super) fn parse_drop_view_statement(&mut self) -> Option<DropViewStatement> {
        let token = self.cur_token.clone();

        // Check for IF EXISTS
        let if_exists = if self.peek_token_is_keyword("IF") {
            self.next_token();
            if !self.expect_keyword("EXISTS") {
                return None;
            }
            true
        } else {
            false
        };

        // Parse view name
        if !self.expect_peek(TokenType::Identifier) {
            return None;
        }
        let view_name = self.parse_relation_identifier_current()?;

        Some(DropViewStatement {
            token,
            view_name,
            if_exists,
        })
    }

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
        if qualified_name.components.len() != 1 {
            self.add_error(
                "qualified table names are currently supported only by ALTER TABLE ... OWNER"
                    .to_string(),
            );
            return None;
        }
        let table_name = qualified_name.components[0].clone();

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

pub(super) fn unquote_ddl_string(literal: &str) -> String {
    let inner = literal
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .unwrap_or(literal);
    inner.replace("''", "'")
}
