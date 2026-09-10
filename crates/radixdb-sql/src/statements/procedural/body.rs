// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use super::*;

impl Parser {
    pub(super) fn parse_procedural_block(&mut self, depth: usize) -> Option<ProceduralBlock> {
        if depth >= MAX_PROCEDURAL_NESTING {
            self.add_error(format!(
                "procedural nesting depth exceeds {MAX_PROCEDURAL_NESTING}"
            ));
            return None;
        }
        let token = self.cur_token.clone();
        let start = token.position;
        let declarations = if self.cur_token_is_keyword("DECLARE") {
            self.next_token();
            let declarations = self.parse_declarations()?;
            if !self.cur_token_is_keyword("BEGIN") {
                self.add_error("expected BEGIN after declarations".to_string());
                return None;
            }
            declarations
        } else if self.cur_token_is_keyword("BEGIN") {
            Vec::new()
        } else {
            self.add_error("procedural body must begin with DECLARE or BEGIN".to_string());
            return None;
        };

        let statements = self.parse_statement_list(depth, &["EXCEPTION", "END"])?;
        let handlers = if self.cur_token_is_keyword("EXCEPTION") {
            self.parse_exception_handlers(depth)?
        } else {
            Vec::new()
        };
        if !self.cur_token_is_keyword("END") {
            self.add_error("expected END to close procedural block".to_string());
            return None;
        }
        let span = self.source_range_from(start);
        Some(ProceduralBlock {
            token,
            declarations,
            statements,
            handlers,
            span,
        })
    }

    fn parse_declarations(&mut self) -> Option<Vec<ProceduralDeclaration>> {
        let mut declarations = Vec::new();
        while !self.cur_token_is_keyword("BEGIN") && !self.cur_token_is(TokenType::Eof) {
            if declarations.len() >= MAX_PROCEDURAL_ITEMS {
                self.add_error("procedural declaration limit exceeded".to_string());
                return None;
            }
            declarations.push(self.parse_declaration_current()?);
            if !self.expect_statement_end_and_advance() {
                return None;
            }
        }
        Some(declarations)
    }

    fn parse_declaration_current(&mut self) -> Option<ProceduralDeclaration> {
        let token = self.cur_token.clone();
        let start = token.position;
        if self.cur_token_is_keyword("CURSOR") {
            if !self.expect_peek_procedural_identifier() {
                return None;
            }
            let name = self.cur_token_as_column_identifier();
            let arguments = if self.peek_token_is_punctuator("(") {
                self.next_token();
                self.parse_cursor_arguments()?
            } else {
                Vec::new()
            };
            if !self.expect_keyword("FOR") {
                return None;
            }
            self.next_token();
            if !self.cur_token_is_keyword("SELECT") && !self.cur_token_is_keyword("WITH") {
                self.add_error("cursor declaration requires SELECT or WITH query".to_string());
                return None;
            }
            let tokens = self.collect_statement_tokens();
            let (query, into, _) = self.parse_static_sql_tokens(tokens)?;
            if !into.is_empty() || !matches!(query.as_ref(), Statement::Select(_)) {
                self.add_error("cursor declaration requires a SELECT without INTO".to_string());
                return None;
            }
            return Some(ProceduralDeclaration::Cursor {
                token,
                name,
                arguments,
                query,
                span: self.source_range_from(start),
            });
        }

        if !self.cur_token_is_procedural_identifier() {
            self.add_error("expected declaration name or CURSOR".to_string());
            return None;
        }
        let name = self.cur_token_as_column_identifier();
        if self.peek_token_is_keyword("ARRAY") {
            self.next_token();
            if !self.peek_token_is_operator("<") {
                self.add_error("expected '<' after ARRAY".to_string());
                return None;
            }
            self.next_token();
            self.next_token();
            let element_type = self.parse_procedural_type_current()?;
            if !self.peek_token_is_punctuator(",") {
                self.add_error("expected ',' before ARRAY capacity".to_string());
                return None;
            }
            self.next_token();
            self.next_token();
            let capacity = self
                .cur_token
                .literal
                .parse::<u32>()
                .ok()
                .filter(|capacity| {
                    (1..=65_536).contains(capacity)
                        && self.cur_token.token_type == TokenType::Integer
                });
            let Some(capacity) = capacity else {
                self.add_error("ARRAY capacity must be an integer in 1..=65536".to_string());
                return None;
            };
            if !self.peek_token_is_operator(">") {
                self.add_error("expected '>' after ARRAY capacity".to_string());
                return None;
            }
            self.next_token();
            return Some(ProceduralDeclaration::Collection {
                token,
                name,
                element_type,
                capacity,
                span: self.source_range_from(start),
            });
        }

        let constant = if self.peek_token_is_keyword("CONSTANT") {
            self.next_token();
            true
        } else {
            false
        };
        self.next_token();
        let data_type = self.parse_procedural_type_current()?;
        let nullable = self.parse_nullable_suffix()?;
        let initializer =
            if self.peek_token_is_operator(":=") || self.peek_token_is_keyword("DEFAULT") {
                self.next_token();
                self.next_token();
                Some(self.parse_expression(Precedence::Lowest)?)
            } else {
                None
            };
        if constant && initializer.is_none() {
            self.add_error("CONSTANT declaration requires an initializer".to_string());
            return None;
        }
        Some(ProceduralDeclaration::Variable {
            token,
            name,
            constant,
            data_type,
            nullable,
            initializer,
            span: self.source_range_from(start),
        })
    }

    fn parse_cursor_arguments(&mut self) -> Option<Vec<RoutineArgumentSyntax>> {
        debug_assert!(self.cur_token_is_punctuator("("));
        let mut arguments = Vec::new();
        if self.peek_token_is_punctuator(")") {
            self.next_token();
            return Some(arguments);
        }
        loop {
            self.next_token();
            let token = self.cur_token.clone();
            let start = token.position;
            if !self.cur_token_is_procedural_identifier() {
                self.add_error("expected cursor argument name".to_string());
                return None;
            }
            let name = self.cur_token_as_column_identifier();
            self.next_token();
            let data_type = self.parse_procedural_type_current()?;
            let nullable = self.parse_nullable_suffix()?;
            arguments.push(RoutineArgumentSyntax {
                token,
                mode: RoutineArgumentMode::In,
                name,
                data_type,
                nullable,
                default: None,
                span: self.source_range_from(start),
            });
            if self.peek_token_is_punctuator(")") {
                self.next_token();
                break;
            }
            if !self.peek_token_is_punctuator(",") {
                self.add_error("expected ',' or ')' in cursor arguments".to_string());
                return None;
            }
            self.next_token();
        }
        Some(arguments)
    }

    pub(super) fn parse_statement_list(
        &mut self,
        depth: usize,
        boundaries: &[&str],
    ) -> Option<Vec<ProceduralStatement>> {
        let mut statements = Vec::new();
        self.next_token();
        while !boundaries
            .iter()
            .any(|boundary| self.cur_token_is_keyword(boundary))
            && !self.cur_token_is(TokenType::Eof)
        {
            if statements.len() >= MAX_PROCEDURAL_ITEMS {
                self.add_error("procedural statement limit exceeded".to_string());
                return None;
            }
            statements.push(self.parse_procedural_statement_current(depth + 1)?);
            if !self.expect_statement_end_and_advance() {
                return None;
            }
        }
        Some(statements)
    }

    fn parse_procedural_statement_current(&mut self, depth: usize) -> Option<ProceduralStatement> {
        if depth >= MAX_PROCEDURAL_NESTING {
            self.add_error(format!(
                "procedural nesting depth exceeds {MAX_PROCEDURAL_NESTING}"
            ));
            return None;
        }
        if self.cur_token.token_type != TokenType::Keyword {
            return self.parse_assignment_or_collection_call();
        }
        match self.cur_token.literal.to_uppercase().as_str() {
            "IF" => self.parse_if_statement(depth),
            "CASE" => self.parse_case_statement(depth),
            "LOOP" => self.parse_loop_statement(depth),
            "WHILE" => self.parse_while_statement(depth),
            "FOR" => self.parse_for_statement(depth),
            "EXIT" => self.parse_loop_control(LoopControlKind::Exit),
            "CONTINUE" => self.parse_loop_control(LoopControlKind::Continue),
            "RETURN" => self.parse_return_statement(),
            "CALL" => self.parse_call_statement(),
            "PERFORM" => self.parse_perform_statement(),
            "EXECUTE" => self.parse_dynamic_execute_statement(),
            "OPEN" => self.parse_open_cursor_statement(),
            "FETCH" => self.parse_fetch_cursor_statement(),
            "CLOSE" => self.parse_close_cursor_statement(),
            "RAISE" => self.parse_raise_statement(),
            "DECLARE" | "BEGIN" => self
                .parse_procedural_block(depth)
                .map(|block| ProceduralStatement::Block(Box::new(block))),
            "SELECT" | "WITH" | "INSERT" | "UPDATE" | "DELETE" => self.parse_static_sql_statement(),
            "COMMIT" | "ROLLBACK" | "SAVEPOINT" | "RELEASE" => {
                self.add_error(
                    "transaction control is forbidden inside a procedural body".to_string(),
                );
                None
            }
            _ => self.parse_assignment_or_collection_call(),
        }
    }

    fn parse_assignment_or_collection_call(&mut self) -> Option<ProceduralStatement> {
        let token = self.cur_token.clone();
        let start = token.position;
        let mut name = self.parse_object_name_current()?;
        if self.peek_token_is_punctuator("(") {
            self.next_token();
            let arguments = self.parse_call_arguments()?;
            return Some(ProceduralStatement::Call {
                token,
                routine: name,
                arguments,
                span: self.source_range_from(start),
            });
        }
        let target = if self.peek_token_is_punctuator("[") {
            self.next_token();
            self.next_token();
            let index = self.parse_expression(Precedence::Lowest)?;
            if !self.peek_token_is_punctuator("]") {
                self.add_error("expected ']' after collection index".to_string());
                return None;
            }
            self.next_token();
            AssignmentTargetSyntax::Index {
                collection: name,
                index,
            }
        } else {
            AssignmentTargetSyntax::Name(std::mem::replace(&mut name, ObjectName::new(Vec::new())))
        };
        if !self.peek_token_is_operator(":=") {
            self.add_error("expected ':=' assignment or collection method call".to_string());
            return None;
        }
        self.next_token();
        self.next_token();
        let value = self.parse_expression(Precedence::Lowest)?;
        Some(ProceduralStatement::Assignment {
            token,
            target,
            value,
            span: self.source_range_from(start),
        })
    }

    fn parse_call_statement(&mut self) -> Option<ProceduralStatement> {
        let token = self.cur_token.clone();
        let start = token.position;
        self.next_token();
        let routine = self.parse_object_name_current()?;
        if !self.peek_token_is_punctuator("(") {
            self.add_error("CALL requires an argument list".to_string());
            return None;
        }
        self.next_token();
        let arguments = self.parse_call_arguments()?;
        Some(ProceduralStatement::Call {
            token,
            routine,
            arguments,
            span: self.source_range_from(start),
        })
    }

    fn parse_perform_statement(&mut self) -> Option<ProceduralStatement> {
        let token = self.cur_token.clone();
        let start = token.position;
        self.next_token();
        let expression = self.parse_expression(Precedence::Lowest)?;
        Some(ProceduralStatement::Perform {
            token,
            expression,
            span: self.source_range_from(start),
        })
    }

    fn parse_static_sql_statement(&mut self) -> Option<ProceduralStatement> {
        let start = self.cur_token.position;
        let tokens = self.collect_statement_tokens();
        let (statement, into, strict) = self.parse_static_sql_tokens(tokens)?;
        Some(ProceduralStatement::Sql(ProceduralSqlStatement {
            statement,
            into,
            strict,
            span: self.source_range_from(start),
        }))
    }

    fn parse_dynamic_execute_statement(&mut self) -> Option<ProceduralStatement> {
        let token = self.cur_token.clone();
        let start = token.position;
        self.next_token();
        let source = self.parse_expression(Precedence::Lowest)?;
        let mut into = Vec::new();
        let mut strict = false;
        if self.peek_token_is_keyword("INTO") {
            self.next_token();
            if self.peek_token_is_keyword("STRICT") {
                self.next_token();
                strict = true;
            }
            into = self.parse_identifier_targets_after_current()?;
        }
        let mut using = Vec::new();
        if self.peek_token_is_keyword("USING") {
            self.next_token();
            loop {
                self.next_token();
                using.push(self.parse_expression(Precedence::Lowest)?);
                if !self.peek_token_is_punctuator(",") {
                    break;
                }
                self.next_token();
            }
        }
        Some(ProceduralStatement::DynamicExecute {
            token,
            execute: DynamicExecuteSyntax {
                source,
                into,
                strict,
                using,
            },
            span: self.source_range_from(start),
        })
    }

    fn parse_return_statement(&mut self) -> Option<ProceduralStatement> {
        let token = self.cur_token.clone();
        let start = token.position;
        let value = if self.peek_token_is_punctuator(";") {
            ReturnSyntax::Void
        } else if self.peek_token_is_keyword("QUERY") {
            self.next_token();
            self.next_token();
            let tokens = self.collect_statement_tokens();
            let (query, into, _) = self.parse_static_sql_tokens(tokens)?;
            if !into.is_empty() || !matches!(query.as_ref(), Statement::Select(_)) {
                self.add_error("RETURN QUERY requires SELECT without INTO".to_string());
                return None;
            }
            ReturnSyntax::Query(query)
        } else if self.peek_token_is_keyword("NEXT") {
            self.next_token();
            if self.peek_token_is_punctuator("(") {
                self.next_token();
                ReturnSyntax::Next(self.parse_expression_list_in_parentheses()?)
            } else {
                self.next_token();
                ReturnSyntax::Next(vec![self.parse_expression(Precedence::Lowest)?])
            }
        } else {
            self.next_token();
            ReturnSyntax::Value(self.parse_expression(Precedence::Lowest)?)
        };
        Some(ProceduralStatement::Return {
            token,
            value,
            span: self.source_range_from(start),
        })
    }

    fn parse_raise_statement(&mut self) -> Option<ProceduralStatement> {
        let token = self.cur_token.clone();
        let start = token.position;
        if self.peek_token_is_punctuator(";") {
            return Some(ProceduralStatement::Raise {
                token,
                kind: None,
                arguments: Vec::new(),
                span: self.source_range_from(start),
            });
        }
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let kind = self.cur_token_as_column_identifier();
        let arguments = if self.peek_token_is_punctuator("(") {
            self.next_token();
            self.parse_expression_list_in_parentheses()?
        } else {
            Vec::new()
        };
        Some(ProceduralStatement::Raise {
            token,
            kind: Some(kind),
            arguments,
            span: self.source_range_from(start),
        })
    }

    fn parse_open_cursor_statement(&mut self) -> Option<ProceduralStatement> {
        let token = self.cur_token.clone();
        let start = token.position;
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let cursor = self.cur_token_as_column_identifier();
        let arguments = if self.peek_token_is_punctuator("(") {
            self.next_token();
            self.parse_expression_list_in_parentheses()?
        } else {
            Vec::new()
        };
        Some(ProceduralStatement::OpenCursor {
            token,
            cursor,
            arguments,
            span: self.source_range_from(start),
        })
    }

    fn parse_fetch_cursor_statement(&mut self) -> Option<ProceduralStatement> {
        let token = self.cur_token.clone();
        let start = token.position;
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let cursor = self.cur_token_as_column_identifier();
        if !self.expect_keyword("INTO") {
            return None;
        }
        let into = self.parse_identifier_targets_after_current()?;
        Some(ProceduralStatement::FetchCursor {
            token,
            cursor,
            into,
            span: self.source_range_from(start),
        })
    }

    fn parse_close_cursor_statement(&mut self) -> Option<ProceduralStatement> {
        let token = self.cur_token.clone();
        let start = token.position;
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        Some(ProceduralStatement::CloseCursor {
            token,
            cursor: self.cur_token_as_column_identifier(),
            span: self.source_range_from(start),
        })
    }
}
