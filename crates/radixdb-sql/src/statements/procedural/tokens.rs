// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use crate::parse_sql;

impl Parser {
    pub(super) fn collect_statement_tokens(&mut self) -> Vec<Token> {
        let mut tokens = vec![self.cur_token.clone()];
        while !self.peek_token_is_punctuator(";") && !self.peek_token_is(TokenType::Eof) {
            self.next_token();
            tokens.push(self.cur_token.clone());
        }
        tokens
    }

    pub(super) fn collect_parenthesized_query_tokens(&mut self) -> Option<Vec<Token>> {
        let mut tokens = Vec::new();
        let mut depth = 0usize;
        self.next_token();
        loop {
            if self.cur_token_is(TokenType::Eof) {
                self.add_error("unterminated query FOR expression".to_string());
                return None;
            }
            if self.cur_token_is_punctuator("(") {
                depth = depth.saturating_add(1);
            } else if self.cur_token_is_punctuator(")") {
                if depth == 0 {
                    break;
                }
                depth -= 1;
            }
            tokens.push(self.cur_token.clone());
            self.next_token();
        }
        Some(tokens)
    }

    pub(super) fn parse_static_sql_tokens(
        &mut self,
        tokens: Vec<Token>,
    ) -> Option<(Box<Statement>, Vec<Identifier>, bool)> {
        if tokens.iter().any(|token| {
            token.token_type == TokenType::Parameter && !token.literal.starts_with(':')
        }) {
            self.add_error_at(
                "stored static SQL cannot contain external '$n' or '?' parameters".to_string(),
                tokens
                    .iter()
                    .find(|token| {
                        token.token_type == TokenType::Parameter && !token.literal.starts_with(':')
                    })
                    .map_or(self.cur_token.position, |token| token.position),
            );
            return None;
        }

        let (sql_tokens, target_tokens, strict) = split_procedural_into(tokens)?;
        let sql = render_tokens(&sql_tokens);
        let statements = match parse_sql(&sql) {
            Ok(statements) if statements.len() == 1 => statements,
            Ok(_) => {
                self.add_error("embedded SQL must contain exactly one statement".to_string());
                return None;
            }
            Err(error) => {
                self.add_error(format!("invalid embedded SQL: {error}"));
                return None;
            }
        };
        let targets = match parse_target_tokens(&target_tokens) {
            Ok(targets) => targets,
            Err(message) => {
                self.add_error(message);
                return None;
            }
        };
        Some((
            Box::new(statements.into_iter().next().unwrap()),
            targets,
            strict,
        ))
    }
}

fn split_procedural_into(tokens: Vec<Token>) -> Option<(Vec<Token>, Vec<Token>, bool)> {
    let mut depth = 0usize;
    let mut main_kind = None;
    let mut returning_seen = false;
    let mut into_index = None;
    let mut from_index = None;
    for (index, token) in tokens.iter().enumerate() {
        if token.is_punctuator("(") {
            depth += 1;
        } else if token.is_punctuator(")") {
            depth = depth.saturating_sub(1);
        } else if depth == 0
            && main_kind.is_none()
            && ["SELECT", "INSERT", "UPDATE", "DELETE"]
                .iter()
                .any(|kind| token.is_keyword(kind))
        {
            main_kind = Some(token.literal.to_uppercase());
        } else if depth == 0 && token.is_keyword("RETURNING") {
            returning_seen = true;
        } else if depth == 0
            && token.is_keyword("INTO")
            && (main_kind.as_deref() == Some("SELECT") || returning_seen)
        {
            into_index = Some(index);
            if main_kind.as_deref() == Some("SELECT") {
                from_index = tokens[index + 1..]
                    .iter()
                    .position(|candidate| candidate.is_keyword("FROM"))
                    .map(|offset| index + 1 + offset);
            }
            break;
        }
    }

    let Some(into_index) = into_index else {
        return Some((tokens, Vec::new(), false));
    };
    let target_end = from_index.unwrap_or(tokens.len());
    let mut target_tokens = tokens[into_index + 1..target_end].to_vec();
    let strict = target_tokens
        .first()
        .is_some_and(|token| token.is_keyword("STRICT"));
    if strict {
        target_tokens.remove(0);
    }
    let mut sql_tokens = tokens[..into_index].to_vec();
    if let Some(from_index) = from_index {
        sql_tokens.extend_from_slice(&tokens[from_index..]);
    }
    Some((sql_tokens, target_tokens, strict))
}

fn parse_target_tokens(tokens: &[Token]) -> Result<Vec<Identifier>, String> {
    if tokens.is_empty() {
        return Ok(Vec::new());
    }
    let mut targets = Vec::new();
    let mut expect_name = true;
    for token in tokens {
        if expect_name {
            if !matches!(token.token_type, TokenType::Identifier | TokenType::Keyword) {
                return Err("SQL INTO target must be a local identifier".to_string());
            }
            targets.push(Identifier::new(token.clone(), token.literal.clone()));
        } else if !token.is_punctuator(",") {
            return Err("expected ',' between SQL INTO targets".to_string());
        }
        expect_name = !expect_name;
    }
    if !expect_name {
        Ok(targets)
    } else {
        Err("SQL INTO target list has a trailing comma".to_string())
    }
}

fn render_tokens(tokens: &[Token]) -> String {
    let mut rendered = String::new();
    for token in tokens {
        if !rendered.is_empty()
            && !token.is_punctuator(",")
            && !token.is_punctuator(")")
            && !token.is_punctuator(".")
            && !rendered.ends_with(['(', '.'])
        {
            rendered.push(' ');
        }
        if token.quoted {
            rendered.push('"');
            rendered.push_str(&token.literal.replace('"', "\"\""));
            rendered.push('"');
        } else {
            rendered.push_str(&token.literal);
        }
    }
    rendered
}
