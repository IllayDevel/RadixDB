// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use super::*;

impl Parser {
    pub(super) fn parse_exception_handlers(
        &mut self,
        depth: usize,
    ) -> Option<Vec<ExceptionHandlerSyntax>> {
        debug_assert!(self.cur_token_is_keyword("EXCEPTION"));
        self.next_token();
        let mut handlers = Vec::new();
        while self.cur_token_is_keyword("WHEN") {
            if handlers.len() >= MAX_PROCEDURAL_ITEMS {
                self.add_error("procedural exception-handler limit exceeded".to_string());
                return None;
            }
            let token = self.cur_token.clone();
            let start = token.position;
            let mut patterns = Vec::new();
            loop {
                self.next_token();
                if self.cur_token_is_keyword("OTHERS") {
                    patterns.push(ExceptionPatternSyntax::Others);
                } else if self.cur_token_is_procedural_identifier() {
                    patterns.push(ExceptionPatternSyntax::Named(
                        self.cur_token_as_column_identifier(),
                    ));
                } else {
                    self.add_error("expected exception name or OTHERS".to_string());
                    return None;
                }
                if !self.peek_token_is_keyword("OR") {
                    break;
                }
                self.next_token();
            }
            if patterns
                .iter()
                .any(|pattern| matches!(pattern, ExceptionPatternSyntax::Others))
                && patterns.len() != 1
            {
                self.add_error("OTHERS cannot be combined with another pattern".to_string());
                return None;
            }
            let catches_others = matches!(patterns.as_slice(), [ExceptionPatternSyntax::Others]);
            let alias = if self.peek_token_is_keyword("AS") {
                self.next_token();
                if !self.expect_peek_procedural_identifier() {
                    return None;
                }
                Some(self.cur_token_as_column_identifier())
            } else {
                None
            };
            if !self.expect_keyword("THEN") {
                return None;
            }
            let statements = self.parse_statement_list(depth, &["WHEN", "END"])?;
            handlers.push(ExceptionHandlerSyntax {
                token,
                patterns,
                alias,
                statements,
                span: self.source_range_from(start),
            });
            if catches_others && self.cur_token_is_keyword("WHEN") {
                self.add_error("OTHERS handler must be last".to_string());
                return None;
            }
        }
        if handlers.is_empty() {
            self.add_error("EXCEPTION requires at least one WHEN handler".to_string());
            return None;
        }
        Some(handlers)
    }
}
