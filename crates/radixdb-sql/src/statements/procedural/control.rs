// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use super::*;

impl Parser {
    pub(super) fn parse_if_statement(&mut self, depth: usize) -> Option<ProceduralStatement> {
        let token = self.cur_token.clone();
        let start = token.position;
        let mut branches = Vec::new();

        loop {
            self.next_token();
            let condition = self.parse_expression(Precedence::Lowest)?;
            if !self.expect_keyword("THEN") {
                return None;
            }
            let statements = self.parse_statement_list(depth, &["ELSIF", "ELSE", "END"])?;
            branches.push((condition, statements));
            if !self.cur_token_is_keyword("ELSIF") {
                break;
            }
        }

        let otherwise = if self.cur_token_is_keyword("ELSE") {
            self.parse_statement_list(depth, &["END"])?
        } else {
            Vec::new()
        };
        if !self.cur_token_is_keyword("END") || !self.expect_keyword("IF") {
            self.add_error("expected END IF to close IF statement".to_string());
            return None;
        }
        Some(ProceduralStatement::If {
            token,
            branches,
            otherwise,
            span: self.source_range_from(start),
        })
    }

    pub(super) fn parse_case_statement(&mut self, depth: usize) -> Option<ProceduralStatement> {
        let token = self.cur_token.clone();
        let start = token.position;
        let operand = if self.peek_token_is_keyword("WHEN") {
            None
        } else {
            self.next_token();
            Some(self.parse_expression(Precedence::Lowest)?)
        };
        if !self.expect_keyword("WHEN") {
            self.add_error("CASE requires at least one WHEN arm".to_string());
            return None;
        }

        let mut arms = Vec::new();
        loop {
            self.next_token();
            let condition = self.parse_expression(Precedence::Lowest)?;
            if !self.expect_keyword("THEN") {
                return None;
            }
            let statements = self.parse_statement_list(depth, &["WHEN", "ELSE", "END"])?;
            arms.push(CaseArmSyntax {
                condition,
                statements,
            });
            if !self.cur_token_is_keyword("WHEN") {
                break;
            }
        }

        let otherwise = if self.cur_token_is_keyword("ELSE") {
            self.parse_statement_list(depth, &["END"])?
        } else {
            Vec::new()
        };
        if !self.cur_token_is_keyword("END") || !self.expect_keyword("CASE") {
            self.add_error("expected END CASE to close CASE statement".to_string());
            return None;
        }
        Some(ProceduralStatement::Case {
            token,
            operand,
            arms,
            otherwise,
            span: self.source_range_from(start),
        })
    }

    pub(super) fn parse_loop_statement(&mut self, depth: usize) -> Option<ProceduralStatement> {
        let token = self.cur_token.clone();
        let start = token.position;
        let statements = self.parse_statement_list(depth, &["END"])?;
        if !self.cur_token_is_keyword("END") || !self.expect_keyword("LOOP") {
            self.add_error("expected END LOOP to close LOOP statement".to_string());
            return None;
        }
        Some(ProceduralStatement::Loop {
            token,
            statements,
            span: self.source_range_from(start),
        })
    }

    pub(super) fn parse_while_statement(&mut self, depth: usize) -> Option<ProceduralStatement> {
        let token = self.cur_token.clone();
        let start = token.position;
        self.next_token();
        let condition = self.parse_expression(Precedence::Lowest)?;
        if !self.expect_keyword("LOOP") {
            return None;
        }
        let statements = self.parse_statement_list(depth, &["END"])?;
        if !self.cur_token_is_keyword("END") || !self.expect_keyword("LOOP") {
            self.add_error("expected END LOOP to close WHILE statement".to_string());
            return None;
        }
        Some(ProceduralStatement::While {
            token,
            condition,
            statements,
            span: self.source_range_from(start),
        })
    }

    pub(super) fn parse_for_statement(&mut self, depth: usize) -> Option<ProceduralStatement> {
        let token = self.cur_token.clone();
        let start = token.position;
        if !self.expect_peek_procedural_identifier() {
            return None;
        }
        let variable = self.cur_token_as_column_identifier();
        if !self.expect_keyword("IN") {
            return None;
        }

        let source = if self.peek_token_is_punctuator("(") {
            self.next_token();
            let query_tokens = self.collect_parenthesized_query_tokens()?;
            let (query, into, _) = self.parse_static_sql_tokens(query_tokens)?;
            if !into.is_empty() || !matches!(query.as_ref(), Statement::Select(_)) {
                self.add_error("query FOR requires SELECT without INTO".to_string());
                return None;
            }
            ForSourceSyntax::Query(query)
        } else {
            let reverse = if self.peek_token_is_keyword("REVERSE") {
                self.next_token();
                true
            } else {
                false
            };
            self.next_token();
            let range_start = self.parse_expression(Precedence::Lowest)?;
            if !self.expect_keyword("TO") {
                return None;
            }
            self.next_token();
            let end = self.parse_expression(Precedence::Lowest)?;
            let step = if self.peek_token_is_keyword("BY") {
                self.next_token();
                self.next_token();
                Some(Box::new(self.parse_expression(Precedence::Lowest)?))
            } else {
                None
            };
            ForSourceSyntax::Numeric {
                reverse,
                start: Box::new(range_start),
                end: Box::new(end),
                step,
            }
        };
        if !self.expect_keyword("LOOP") {
            return None;
        }
        let statements = self.parse_statement_list(depth, &["END"])?;
        if !self.cur_token_is_keyword("END") || !self.expect_keyword("LOOP") {
            self.add_error("expected END LOOP to close FOR statement".to_string());
            return None;
        }
        Some(ProceduralStatement::For {
            token,
            variable,
            source,
            statements,
            span: self.source_range_from(start),
        })
    }

    pub(super) fn parse_loop_control(
        &mut self,
        kind: LoopControlKind,
    ) -> Option<ProceduralStatement> {
        let token = self.cur_token.clone();
        let start = token.position;
        let condition = if self.peek_token_is_keyword("WHEN") {
            self.next_token();
            self.next_token();
            Some(self.parse_expression(Precedence::Lowest)?)
        } else {
            None
        };
        Some(ProceduralStatement::LoopControl {
            token,
            kind,
            condition,
            span: self.source_range_from(start),
        })
    }
}
