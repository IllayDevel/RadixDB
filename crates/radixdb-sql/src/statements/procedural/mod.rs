// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use super::*;

mod body;
mod control;
mod exception;
mod object;
mod routine;
mod tokens;

const MAX_PROCEDURAL_NESTING: usize = 128;
const MAX_PROCEDURAL_ITEMS: usize = 1_000_000;
const MAX_PROCEDURAL_SOURCE_BYTES: usize = 16 * 1024 * 1024;
const MAX_PROCEDURAL_TOKENS: usize = 1_000_000;

impl Parser {
    pub(super) fn parse_outer_call_statement(&mut self) -> Option<CallStatement> {
        let token = self.cur_token.clone();
        self.next_token();
        let routine = self.parse_object_name_current()?;
        if !self.peek_token_is_punctuator("(") {
            self.add_error("CALL requires an argument list".to_string());
            return None;
        }
        self.next_token();
        let arguments = self.parse_call_arguments()?;
        Some(CallStatement {
            token,
            routine,
            arguments,
        })
    }

    pub(crate) fn cur_token_is_procedural_identifier(&self) -> bool {
        match self.cur_token.token_type {
            TokenType::Identifier => true,
            TokenType::Keyword => !is_procedural_reserved(&self.cur_token.literal),
            _ => false,
        }
    }

    fn peek_token_is_procedural_identifier(&self) -> bool {
        match self.peek_token.token_type {
            TokenType::Identifier => true,
            TokenType::Keyword => !is_procedural_reserved(&self.peek_token.literal),
            _ => false,
        }
    }

    pub(crate) fn expect_peek_procedural_identifier(&mut self) -> bool {
        if self.peek_token_is_procedural_identifier() {
            self.next_token();
            true
        } else {
            self.add_error_at(
                format!(
                    "expected procedural identifier, got {}",
                    Self::format_token_for_error(&self.peek_token)
                ),
                self.peek_token.position,
            );
            false
        }
    }

    pub(crate) fn parse_object_name_current(&mut self) -> Option<ObjectName> {
        if !self.cur_token_is_procedural_identifier() {
            self.add_error(format!(
                "expected object name, got {}",
                Self::format_token_for_error(&self.cur_token)
            ));
            return None;
        }
        let mut components = vec![self.cur_token_as_column_identifier()];
        while self.peek_token_is_punctuator(".") {
            self.next_token();
            if !self.expect_peek_procedural_identifier() {
                return None;
            }
            components.push(self.cur_token_as_column_identifier());
        }
        Some(ObjectName::new(components))
    }

    pub(crate) fn parse_procedural_type_current(&mut self) -> Option<ProceduralType> {
        if !matches!(
            self.cur_token.token_type,
            TokenType::Identifier | TokenType::Keyword
        ) {
            self.add_error(format!(
                "expected data type, got {}",
                Self::format_token_for_error(&self.cur_token)
            ));
            return None;
        }

        let first = self.cur_token.clone();
        let object_name = self.parse_object_name_current()?;
        if self.peek_token_is_operator("%") {
            self.next_token();
            if !self.expect_keyword("ROWTYPE") {
                return None;
            }
            return Some(ProceduralType::RowType(object_name));
        }
        let mut rendered = if object_name.components.len() == 1 {
            first.literal.to_uppercase()
        } else {
            object_name.to_string().into()
        };
        if self.peek_token_is_punctuator("(") {
            self.next_token();
            rendered.push('(');
            let mut first_argument = true;
            loop {
                self.next_token();
                if self.cur_token_is_punctuator(")") {
                    if first_argument {
                        self.add_error("data type arguments cannot be empty".to_string());
                        return None;
                    }
                    rendered.push(')');
                    break;
                }
                if !first_argument {
                    if !self.cur_token_is_punctuator(",") {
                        self.add_error("expected ',' in data type arguments".to_string());
                        return None;
                    }
                    rendered.push_str(", ");
                    self.next_token();
                }
                if self.cur_token.token_type != TokenType::Integer {
                    self.add_error("data type argument must be an integer".to_string());
                    return None;
                }
                rendered.push_str(&self.cur_token.literal);
                first_argument = false;
                if self.peek_token_is_punctuator(")") {
                    self.next_token();
                    rendered.push(')');
                    break;
                }
                if !self.peek_token_is_punctuator(",") {
                    self.add_error("expected ',' or ')' in data type arguments".to_string());
                    return None;
                }
            }
        }
        Some(ProceduralType::Scalar(rendered))
    }

    fn parse_nullable_suffix(&mut self) -> Option<bool> {
        if self.peek_token_is_keyword("NOT") {
            self.next_token();
            if !self.expect_keyword("NULL") {
                return None;
            }
            Some(false)
        } else {
            Some(true)
        }
    }

    fn parse_identifier_targets_after_current(&mut self) -> Option<Vec<Identifier>> {
        let mut targets = Vec::new();
        loop {
            if !self.expect_peek_procedural_identifier() {
                return None;
            }
            targets.push(self.cur_token_as_column_identifier());
            if !self.peek_token_is_punctuator(",") {
                break;
            }
            self.next_token();
        }
        Some(targets)
    }

    fn parse_expression_list_in_parentheses(&mut self) -> Option<Vec<Expression>> {
        debug_assert!(self.cur_token_is_punctuator("("));
        let mut values = Vec::new();
        if self.peek_token_is_punctuator(")") {
            self.next_token();
            return Some(values);
        }
        loop {
            self.next_token();
            values.push(self.parse_expression(Precedence::Lowest)?);
            if self.peek_token_is_punctuator(")") {
                self.next_token();
                break;
            }
            if !self.peek_token_is_punctuator(",") {
                self.add_error("expected ',' or ')' in expression list".to_string());
                return None;
            }
            self.next_token();
        }
        Some(values)
    }

    fn parse_call_arguments(&mut self) -> Option<Vec<CallArgumentSyntax>> {
        debug_assert!(self.cur_token_is_punctuator("("));
        let mut arguments = Vec::new();
        let mut named_started = false;
        if self.peek_token_is_punctuator(")") {
            self.next_token();
            return Some(arguments);
        }
        loop {
            self.next_token();
            let name =
                if self.cur_token_is_procedural_identifier() && self.peek_token_is_operator("=>") {
                    let name = self.cur_token_as_column_identifier();
                    self.next_token();
                    self.next_token();
                    named_started = true;
                    Some(name)
                } else {
                    if named_started {
                        self.add_error(
                            "positional argument cannot follow a named argument".to_string(),
                        );
                        return None;
                    }
                    None
                };
            let value = self.parse_expression(Precedence::Lowest)?;
            arguments.push(CallArgumentSyntax { name, value });
            if self.peek_token_is_punctuator(")") {
                self.next_token();
                break;
            }
            if !self.peek_token_is_punctuator(",") {
                self.add_error("expected ',' or ')' in call arguments".to_string());
                return None;
            }
            self.next_token();
        }
        Some(arguments)
    }

    fn expect_statement_end_and_advance(&mut self) -> bool {
        if !self.peek_token_is_punctuator(";") {
            self.add_error(format!(
                "expected ';' after procedural statement before {}",
                Self::format_token_for_error(&self.peek_token)
            ));
            return false;
        }
        self.next_token();
        self.next_token();
        true
    }
}

fn is_procedural_reserved(keyword: &str) -> bool {
    Parser::is_reserved_keyword(keyword)
        || matches!(
            keyword.to_ascii_uppercase().as_str(),
            "DECLARE"
                | "CURSOR"
                | "CONSTANT"
                | "ELSIF"
                | "THEN"
                | "LOOP"
                | "WHILE"
                | "FOR"
                | "REVERSE"
                | "EXIT"
                | "CONTINUE"
                | "RETURN"
                | "QUERY"
                | "EXCEPTION"
                | "RAISE"
                | "OTHERS"
                | "OPEN"
                | "FETCH"
                | "CLOSE"
                | "CALL"
                | "PERFORM"
                | "EXECUTE"
                | "USING"
                | "FUNCTION"
                | "PROCEDURE"
                | "RETURNS"
                | "LANGUAGE"
                | "RADIX"
                | "NATIVE"
                | "OUT"
                | "INOUT"
                | "ARRAY"
                | "ROWTYPE"
                | "IMMUTABLE"
                | "STABLE"
                | "VOLATILE"
                | "SECURITY"
                | "INVOKER"
                | "DEFINER"
                | "SEARCH"
                | "PATH"
                | "RESOURCE"
                | "POLICY"
                | "STRICT"
                | "PRIORITY"
                | "BEFORE"
                | "AFTER"
                | "EACH"
                | "STATEMENT"
                | "JOB"
                | "SCHEDULE"
                | "EVERY"
                | "AT"
                | "ENABLE"
                | "DISABLE"
                | "RUN"
                | "PRINCIPAL"
                | "ROLE"
                | "GRANT"
                | "REVOKE"
                | "OWNER"
                | "ADMIN"
                | "OPTION"
        )
}
