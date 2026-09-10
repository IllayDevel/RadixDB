// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Explicit extension operator and operator-class grammar.

use super::ddl::unquote_ddl_string;
use super::*;

impl Parser {
    pub(super) fn parse_index_keys_and_operator_class(
        &mut self,
    ) -> Option<(Vec<Identifier>, Option<ObjectName>)> {
        let mut columns = Vec::new();
        let mut operator_class = None;
        loop {
            if !self.expect_peek_identifier_like() {
                return None;
            }
            columns.push(self.cur_token_as_column_identifier());
            if self.peek_token_is_punctuator(",") {
                self.next_token();
                continue;
            }
            if !self.peek_token_is_punctuator(")") {
                self.next_token();
                operator_class = Some(self.parse_object_name_current()?);
                if columns.len() != 1 {
                    self.add_error(
                        "operator class is supported only for one index key".to_string(),
                    );
                    return None;
                }
            }
            break;
        }
        Some((columns, operator_class))
    }

    pub(super) fn parse_index_method_current(&mut self) -> Option<IndexMethod> {
        match self.cur_token.literal.to_uppercase().as_str() {
            "BTREE" | "B_TREE" => Some(IndexMethod::BTree),
            "HASH" => Some(IndexMethod::Hash),
            "BITMAP" => Some(IndexMethod::Bitmap),
            "HNSW" => Some(IndexMethod::Hnsw),
            _ => {
                self.add_error(format!(
                    "unknown index method '{}'. Supported methods: BTREE, HASH, BITMAP, HNSW",
                    self.cur_token.literal
                ));
                None
            }
        }
    }

    pub(super) fn parse_create_operator_statement(
        &mut self,
        token: Token,
    ) -> Option<CreateOperatorStatement> {
        let name = self.parse_qualified_operator()?;
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "(" {
            self.add_error("CREATE OPERATOR requires '('".to_string());
            return None;
        }
        let left_argument = if self.peek_token_is_keyword("LEFTARG") {
            self.next_token();
            if !self.expect_peek(TokenType::Operator) || self.cur_token.literal != "=" {
                return None;
            }
            self.next_token();
            let value = self.parse_procedural_type_current()?;
            if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "," {
                return None;
            }
            Some(value)
        } else {
            None
        };
        if !self.expect_keyword("RIGHTARG")
            || !self.expect_peek(TokenType::Operator)
            || self.cur_token.literal != "="
        {
            return None;
        }
        self.next_token();
        let right_argument = self.parse_procedural_type_current()?;
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "," {
            return None;
        }
        if !self.expect_keyword("FUNCTION")
            || !self.expect_peek(TokenType::Operator)
            || self.cur_token.literal != "="
        {
            return None;
        }
        self.next_token();
        let function = self.parse_routine_signature_current()?;
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
            return None;
        }
        let (extension_name, local_id) = self.parse_extension_local_binding()?;
        Some(CreateOperatorStatement {
            token,
            name,
            left_argument,
            right_argument,
            function,
            extension_name,
            local_id,
        })
    }

    pub(super) fn parse_create_operator_class_statement(
        &mut self,
        token: Token,
    ) -> Option<CreateOperatorClassStatement> {
        if !self.expect_peek_identifier_like() {
            return None;
        }
        let name = self.parse_object_name_current()?;
        if name.components.len() < 2 {
            self.add_error("operator class name must be schema-qualified".to_string());
            return None;
        }
        if !self.expect_keyword("FOR") || !self.expect_keyword("TYPE") {
            return None;
        }
        self.next_token();
        let input_type = self.parse_procedural_type_current()?;
        if !self.expect_keyword("USING") {
            return None;
        }
        self.next_token();
        let access_method = self.parse_index_method_current()?;
        let (extension_name, local_id) = self.parse_extension_local_binding()?;
        Some(CreateOperatorClassStatement {
            token,
            name,
            input_type,
            access_method,
            extension_name,
            local_id,
        })
    }

    pub(super) fn parse_create_planner_support_statement(
        &mut self,
        token: Token,
    ) -> Option<CreatePlannerSupportStatement> {
        if !self.expect_peek_identifier_like() {
            return None;
        }
        let name = self.parse_object_name_current()?;
        if name.components.len() < 2 {
            self.add_error("planner support name must be schema-qualified".to_string());
            return None;
        }
        if !self.expect_keyword("FOR") || !self.expect_keyword("FUNCTION") {
            return None;
        }
        self.next_token();
        let function = self.parse_routine_signature_current()?;
        let (extension_name, local_id) = self.parse_extension_local_binding()?;
        Some(CreatePlannerSupportStatement {
            token,
            name,
            function,
            extension_name,
            local_id,
        })
    }

    pub(super) fn parse_drop_operator_statement(
        &mut self,
        token: Token,
    ) -> Option<DropOperatorStatement> {
        let if_exists = if self.peek_token_is_keyword("IF") {
            self.next_token();
            if !self.expect_keyword("EXISTS") {
                return None;
            }
            true
        } else {
            false
        };
        let name = self.parse_qualified_operator()?;
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "(" {
            return None;
        }
        let left_argument = if self.peek_token_is_punctuator(",") {
            None
        } else {
            self.next_token();
            Some(self.parse_procedural_type_current()?)
        };
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "," {
            return None;
        }
        let right_argument = if self.peek_token_is_punctuator(")") {
            None
        } else {
            self.next_token();
            Some(self.parse_procedural_type_current()?)
        };
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != ")" {
            return None;
        }
        if !self.expect_keyword("RESTRICT") {
            return None;
        }
        Some(DropOperatorStatement {
            token,
            name,
            left_argument,
            right_argument,
            if_exists,
        })
    }

    pub(super) fn parse_drop_operator_class_statement(
        &mut self,
        token: Token,
    ) -> Option<DropOperatorClassStatement> {
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
            self.add_error("operator class name must be schema-qualified".to_string());
            return None;
        }
        if !self.expect_keyword("USING") {
            return None;
        }
        self.next_token();
        let access_method = self.parse_index_method_current()?;
        if !self.expect_keyword("RESTRICT") {
            return None;
        }
        Some(DropOperatorClassStatement {
            token,
            name,
            access_method,
            if_exists,
        })
    }

    pub(super) fn parse_drop_planner_support_statement(
        &mut self,
        token: Token,
    ) -> Option<DropPlannerSupportStatement> {
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
            self.add_error("planner support name must be schema-qualified".to_string());
            return None;
        }
        if !self.expect_keyword("RESTRICT") {
            return None;
        }
        Some(DropPlannerSupportStatement {
            token,
            name,
            if_exists,
        })
    }

    fn parse_qualified_operator(&mut self) -> Option<QualifiedOperator> {
        if !self.expect_peek_identifier_like() {
            return None;
        }
        let schema = self.cur_token_as_column_identifier();
        if !self.expect_peek(TokenType::Punctuator) || self.cur_token.literal != "." {
            self.add_error("operator name must be schema-qualified".to_string());
            return None;
        }
        if !self.expect_peek(TokenType::Operator) {
            return None;
        }
        let symbol = self.cur_token.literal.clone();
        if !matches!(
            symbol.as_str(),
            "=" | "<>"
                | "!="
                | "<"
                | "<="
                | ">"
                | ">="
                | "+"
                | "-"
                | "*"
                | "/"
                | "%"
                | "||"
                | "&"
                | "|"
                | "^"
                | "~"
                | "<<"
                | ">>"
                | "<=>"
                | "&&"
                | "@>"
                | "<@"
        ) {
            self.add_error("operator symbol is outside the closed v1 alphabet".to_string());
            return None;
        }
        Some(QualifiedOperator { schema, symbol })
    }

    fn parse_extension_local_binding(&mut self) -> Option<(Identifier, SmartString)> {
        if !self.expect_keyword("FROM") || !self.expect_keyword("EXTENSION") {
            return None;
        }
        if !self.expect_peek_identifier_like() {
            return None;
        }
        let extension = self.cur_token_as_column_identifier();
        if !self.expect_keyword("AS") || !self.expect_peek(TokenType::String) {
            return None;
        }
        let local_id = SmartString::from(unquote_ddl_string(&self.cur_token.literal));
        if local_id.is_empty() || local_id.len() > 255 || local_id.as_bytes().contains(&0) {
            self.add_error("plugin local id must be 1..=255 UTF-8 bytes without NUL".to_string());
            return None;
        }
        Some((extension, local_id))
    }
}
