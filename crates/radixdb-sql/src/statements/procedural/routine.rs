// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use super::*;

impl Parser {
    pub(crate) fn parse_create_routine_statement(
        &mut self,
        create_token: Token,
        or_replace: bool,
        kind: RoutineKindSyntax,
    ) -> Option<CreateRoutineStatement> {
        if self.source.len() > MAX_PROCEDURAL_SOURCE_BYTES {
            self.add_error(format!(
                "procedural source exceeds {MAX_PROCEDURAL_SOURCE_BYTES} bytes"
            ));
            return None;
        }
        let token_start = self.token_count;
        self.procedural_definition_depth += 1;
        let result = self.parse_create_routine_statement_inner(create_token, or_replace, kind);
        self.procedural_definition_depth -= 1;
        if self.token_count.saturating_sub(token_start) > MAX_PROCEDURAL_TOKENS {
            self.add_error(format!(
                "procedural token count exceeds {MAX_PROCEDURAL_TOKENS}"
            ));
            return None;
        }
        result
    }

    fn parse_create_routine_statement_inner(
        &mut self,
        create_token: Token,
        or_replace: bool,
        kind: RoutineKindSyntax,
    ) -> Option<CreateRoutineStatement> {
        let start = create_token.position;
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let name = self.parse_object_name_current()?;
        if !self.peek_token_is_punctuator("(") {
            self.add_error("expected '(' after routine name".to_string());
            return None;
        }
        self.next_token();
        let arguments = self.parse_routine_arguments(kind)?;

        let returns = if self.peek_token_is_keyword("RETURNS") {
            self.next_token();
            self.next_token();
            Some(self.parse_return_contract_current()?)
        } else {
            None
        };
        if kind == RoutineKindSyntax::Function && returns.is_none() {
            self.add_error("FUNCTION requires a RETURNS contract".to_string());
            return None;
        }

        if !self.expect_keyword("LANGUAGE") {
            return None;
        }

        if self.peek_token_is_keyword("NATIVE") {
            self.next_token();
            if kind != RoutineKindSyntax::Function {
                self.add_error("LANGUAGE NATIVE is valid only for FUNCTION".to_string());
                return None;
            }
            if !matches!(returns, Some(RoutineReturnSyntax::Scalar { .. })) {
                self.add_error("LANGUAGE NATIVE requires one scalar RETURNS type".to_string());
                return None;
            }
            if or_replace {
                self.add_error(
                    "CREATE OR REPLACE is not supported for native functions".to_string(),
                );
                return None;
            }
            if !self.expect_keyword("FROM") || !self.expect_keyword("EXTENSION") {
                return None;
            }
            if !self.expect_peek_procedural_identifier() {
                return None;
            }
            let extension = self.parse_object_name_current()?;
            if !self.expect_keyword("AS") || !self.expect_peek(TokenType::String) {
                return None;
            }
            let local_id = self
                .cur_token
                .literal
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
                .unwrap_or(self.cur_token.literal.as_str())
                .replace("''", "'");
            if local_id.is_empty() || local_id.len() > 255 || local_id.contains('\0') {
                self.add_error(
                    "native function local id must be 1..=255 UTF-8 bytes without NUL".to_string(),
                );
                return None;
            }
            if !self.peek_token_is_punctuator(";") && self.peek_token.token_type != TokenType::Eof {
                self.add_error("native function definition ends after AS '<local-id>'".to_string());
                return None;
            }
            let span = self.source_range_through_peek_from(start);
            let normalized_source = self.normalized_source_for(&span);
            return Some(CreateRoutineStatement {
                token: create_token,
                or_replace,
                kind,
                name,
                arguments,
                returns,
                volatility: None,
                security: RoutineSecuritySyntax::Invoker,
                search_path: Vec::new(),
                resource_policy: None,
                body: None,
                native: Some(NativeFunctionBindingSyntax {
                    extension,
                    local_id: local_id.into(),
                }),
                normalized_source,
                span,
            });
        }

        if !self.expect_keyword("RADIX") {
            return None;
        }

        let volatility = if kind == RoutineKindSyntax::Function {
            if self.peek_token_is_keyword("IMMUTABLE") {
                self.next_token();
                Some(RoutineVolatilitySyntax::Immutable)
            } else if self.peek_token_is_keyword("STABLE") {
                self.next_token();
                Some(RoutineVolatilitySyntax::Stable)
            } else if self.peek_token_is_keyword("VOLATILE") {
                self.next_token();
                Some(RoutineVolatilitySyntax::Volatile)
            } else {
                self.add_error("FUNCTION requires IMMUTABLE, STABLE, or VOLATILE".to_string());
                return None;
            }
        } else {
            None
        };

        if !self.expect_keyword("SECURITY") {
            return None;
        }
        let security = if self.peek_token_is_keyword("INVOKER") {
            self.next_token();
            RoutineSecuritySyntax::Invoker
        } else if self.peek_token_is_keyword("DEFINER") {
            self.next_token();
            RoutineSecuritySyntax::Definer
        } else {
            self.add_error("expected INVOKER or DEFINER after SECURITY".to_string());
            return None;
        };

        let search_path = if self.peek_token_is_keyword("SEARCH") {
            self.next_token();
            if !self.expect_keyword("PATH") || !self.peek_token_is_punctuator("(") {
                self.add_error("expected SEARCH PATH (...)".to_string());
                return None;
            }
            self.next_token();
            self.parse_object_name_list()?
        } else {
            Vec::new()
        };

        let resource_policy = if self.peek_token_is_keyword("RESOURCE") {
            self.next_token();
            if !self.expect_keyword("POLICY") || !self.expect_peek_procedural_identifier() {
                return None;
            }
            Some(self.parse_object_name_current()?)
        } else {
            None
        };

        if !self.expect_keyword("AS") {
            return None;
        }
        self.next_token();
        let body = self.parse_procedural_block(0)?;
        if !self.peek_token_is_punctuator(";") {
            self.add_error("routine definition must end with ';' after END".to_string());
            return None;
        }
        let span = self.source_range_through_peek_from(start);
        let normalized_source = self.normalized_source_for(&span);
        Some(CreateRoutineStatement {
            token: create_token,
            or_replace,
            kind,
            name,
            arguments,
            returns,
            volatility,
            security,
            search_path,
            resource_policy,
            body: Some(body),
            native: None,
            normalized_source,
            span,
        })
    }

    fn parse_routine_arguments(
        &mut self,
        kind: RoutineKindSyntax,
    ) -> Option<Vec<RoutineArgumentSyntax>> {
        debug_assert!(self.cur_token_is_punctuator("("));
        let mut arguments = Vec::new();
        let mut saw_default = false;
        let mut saw_output = false;
        if self.peek_token_is_punctuator(")") {
            self.next_token();
            return Some(arguments);
        }
        loop {
            self.next_token();
            let start = self.cur_token.position;
            let mode = if kind == RoutineKindSyntax::Procedure && self.cur_token_is_keyword("IN") {
                self.next_token();
                RoutineArgumentMode::In
            } else if kind == RoutineKindSyntax::Procedure && self.cur_token_is_keyword("OUT") {
                self.next_token();
                saw_output = true;
                RoutineArgumentMode::Out
            } else if kind == RoutineKindSyntax::Procedure && self.cur_token_is_keyword("INOUT") {
                self.next_token();
                saw_output = true;
                RoutineArgumentMode::InOut
            } else {
                RoutineArgumentMode::In
            };
            if !self.cur_token_is_procedural_identifier() {
                self.add_error("expected argument name".to_string());
                return None;
            }
            let token = self.cur_token.clone();
            let name = self.cur_token_as_column_identifier();
            self.next_token();
            let data_type = self.parse_procedural_type_current()?;
            let nullable = self.parse_nullable_suffix()?;
            let default = if self.peek_token_is_keyword("DEFAULT") {
                if mode == RoutineArgumentMode::Out {
                    self.add_error("OUT argument cannot have DEFAULT".to_string());
                    return None;
                }
                self.next_token();
                self.next_token();
                saw_default = true;
                Some(self.parse_expression(Precedence::Lowest)?)
            } else {
                if saw_default && mode != RoutineArgumentMode::Out {
                    self.add_error(
                        "required input argument cannot follow a defaulted argument".to_string(),
                    );
                    return None;
                }
                None
            };
            let span = self.source_range_from(start);
            arguments.push(RoutineArgumentSyntax {
                token,
                mode,
                name,
                data_type,
                nullable,
                default,
                span,
            });
            if self.peek_token_is_punctuator(")") {
                self.next_token();
                break;
            }
            if !self.peek_token_is_punctuator(",") {
                self.add_error("expected ',' or ')' after routine argument".to_string());
                return None;
            }
            self.next_token();
        }
        if saw_output && self.peek_token_is_keyword("RETURNS") {
            self.add_error("OUT/INOUT arguments cannot be combined with RETURNS".to_string());
            return None;
        }
        Some(arguments)
    }

    fn parse_return_contract_current(&mut self) -> Option<RoutineReturnSyntax> {
        if self.cur_token_is_keyword("TABLE") {
            if !self.peek_token_is_punctuator("(") {
                self.add_error("expected '(' after RETURNS TABLE".to_string());
                return None;
            }
            self.next_token();
            let mut columns = Vec::new();
            loop {
                self.next_token();
                if !self.cur_token_is_procedural_identifier() {
                    self.add_error("expected result column name".to_string());
                    return None;
                }
                let name = self.cur_token_as_column_identifier();
                self.next_token();
                let data_type = self.parse_procedural_type_current()?;
                let nullable = self.parse_nullable_suffix()?;
                columns.push(ResultColumnSyntax {
                    name,
                    data_type,
                    nullable,
                });
                if self.peek_token_is_punctuator(")") {
                    self.next_token();
                    break;
                }
                if !self.peek_token_is_punctuator(",") {
                    self.add_error("expected ',' or ')' in RETURNS TABLE".to_string());
                    return None;
                }
                self.next_token();
            }
            if columns.is_empty() {
                self.add_error("RETURNS TABLE requires at least one column".to_string());
                return None;
            }
            Some(RoutineReturnSyntax::Table(columns))
        } else if self.cur_token_is_keyword("TRIGGER") {
            Some(RoutineReturnSyntax::Trigger)
        } else {
            let data_type = self.parse_procedural_type_current()?;
            let nullable = self.parse_nullable_suffix()?;
            Some(RoutineReturnSyntax::Scalar {
                data_type,
                nullable,
            })
        }
    }

    fn parse_object_name_list(&mut self) -> Option<Vec<ObjectName>> {
        debug_assert!(self.cur_token_is_punctuator("("));
        let mut names = Vec::new();
        loop {
            self.next_token();
            names.push(self.parse_object_name_current()?);
            if self.peek_token_is_punctuator(")") {
                self.next_token();
                break;
            }
            if !self.peek_token_is_punctuator(",") {
                self.add_error("expected ',' or ')' in object-name list".to_string());
                return None;
            }
            self.next_token();
        }
        Some(names)
    }
}
