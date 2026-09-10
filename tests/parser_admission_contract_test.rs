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

//! R5-L02 batch-A admission contract for the public lexer/parser boundary.

use radixdb::parser::{parse_sql, Lexer, Parser, TokenType};

#[test]
fn embedded_nul_is_an_error_instead_of_eof() {
    let error = parse_sql("SELECT 1\0; SELECT 2")
        .expect_err("an embedded NUL must not silently terminate the SQL input");
    assert!(
        error.to_string().to_ascii_lowercase().contains("null")
            || error.format_errors().to_ascii_lowercase().contains("null"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn comments_are_whitespace_at_every_parser_boundary() {
    for sql in [
        "SELECT/* projection */1",
        "SELECT 1/* lhs */+/* rhs */2",
        "SELECT * FROM/* source */items ORDER/* clause */BY id",
    ] {
        parse_sql(sql).unwrap_or_else(|error| panic!("{sql:?} failed: {error:?}"));
    }
}

#[test]
fn unterminated_block_comment_reaches_the_parser_error() {
    let error = parse_sql("SELECT 1 /* unterminated")
        .expect_err("an unterminated block comment must fail parsing");
    assert!(
        error
            .format_errors()
            .to_ascii_lowercase()
            .contains("unterminated block comment"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn unicode_columns_count_scalars_not_utf8_bytes() {
    let mut lexer = Lexer::new("SELECT '漢' ©");
    let mut error = None;
    loop {
        let token = lexer.next_token();
        if token.token_type == TokenType::Error {
            error = Some(token);
            break;
        }
        if token.token_type == TokenType::Eof {
            break;
        }
    }

    let error = error.expect("the unsupported copyright character must produce a token error");
    assert_eq!(error.position.line, 1);
    assert_eq!(error.position.column, 12);
}

#[test]
fn diagnostics_preserve_original_source_and_point_at_the_unexpected_token() {
    let sql = "  SELECT * FROM )";
    let error = parse_sql(sql).expect_err("the malformed FROM clause must fail");
    assert_eq!(error.sql, sql);
    let first = error.errors.first().expect("at least one parser error");
    assert_eq!(first.position.line, 1);
    assert_eq!(first.position.column, 17);
    assert!(error.format_errors().contains(sql));

    let mut parser = Parser::new(sql);
    let direct = parser
        .parse_program()
        .expect_err("the direct public Parser API must retain source too");
    assert_eq!(direct.sql, sql);
}

#[test]
fn expression_nesting_is_bounded_before_stack_exhaustion() {
    let sql = format!("SELECT {}1{}", "(".repeat(300), ")".repeat(300));
    let error = parse_sql(&sql).expect_err("excessive expression nesting must be rejected");
    let message = error.format_errors().to_ascii_lowercase();
    assert!(
        message.contains("nesting") || message.contains("depth"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn long_unicode_identifier_lexes_without_changing_token_identity() {
    let identifier = "界".repeat(4096);
    let mut lexer = Lexer::new(&identifier);
    let token = lexer.next_token();
    assert_eq!(token.token_type, TokenType::Identifier);
    assert_eq!(token.literal.as_str(), identifier);
    assert_eq!(lexer.next_token().token_type, TokenType::Eof);
}
