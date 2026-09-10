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
use crate::token::TokenType;

fn make_token(tt: TokenType, literal: &str) -> Token {
    Token::new(tt, literal, Position::default())
}

#[test]
fn test_identifier_display() {
    let id = Identifier::new(
        make_token(TokenType::Identifier, "users"),
        "users".to_string(),
    );
    assert_eq!(id.to_string(), "users");
}

#[test]
fn test_qualified_identifier_display() {
    let qi = QualifiedIdentifier {
        token: make_token(TokenType::Identifier, "users"),
        qualifier: Box::new(Identifier::new(
            make_token(TokenType::Identifier, "users"),
            "users".to_string(),
        )),
        intermediate: None,
        name: Box::new(Identifier::new(
            make_token(TokenType::Identifier, "id"),
            "id".to_string(),
        )),
    };
    assert_eq!(qi.to_string(), "users.id");
}

#[test]
fn test_integer_literal_display() {
    let lit = IntegerLiteral {
        token: make_token(TokenType::Integer, "42"),
        value: 42,
    };
    assert_eq!(lit.to_string(), "42");
}

#[test]
fn test_string_literal_display() {
    let lit = StringLiteral {
        token: make_token(TokenType::String, "'hello'"),
        value: "hello".into(),
        type_hint: None,
    };
    assert_eq!(lit.to_string(), "'hello'");
}

#[test]
fn test_infix_expression_display() {
    let expr = InfixExpression::new(
        make_token(TokenType::Operator, "+"),
        Box::new(Expression::IntegerLiteral(IntegerLiteral {
            token: make_token(TokenType::Integer, "1"),
            value: 1,
        })),
        "+",
        Box::new(Expression::IntegerLiteral(IntegerLiteral {
            token: make_token(TokenType::Integer, "2"),
            value: 2,
        })),
    );
    assert_eq!(expr.to_string(), "(1 + 2)");
}

#[test]
fn test_function_call_display() {
    let fc = FunctionCall {
        token: make_token(TokenType::Identifier, "COUNT"),
        function: "COUNT".into(),
        arguments: vec![Expression::Star(StarExpression {
            token: make_token(TokenType::Operator, "*"),
        })],
        is_distinct: false,
        order_by: vec![],
        filter: None,
    };
    assert_eq!(fc.to_string(), "COUNT(*)");
}

#[test]
fn test_select_statement_display() {
    let stmt = SelectStatement {
        token: make_token(TokenType::Keyword, "SELECT"),
        distinct: false,
        distinct_on: vec![],
        columns: vec![Expression::Star(StarExpression {
            token: make_token(TokenType::Operator, "*"),
        })],
        with: None,
        table_expr: Some(Box::new(Expression::TableSource(Box::new(
            SimpleTableSource {
                token: make_token(TokenType::Identifier, "users"),
                name: Identifier::new(make_token(TokenType::Identifier, "users"), "users"),
                alias: None,
                as_of: None,
            },
        )))),
        where_clause: None,
        group_by: GroupByClause::default(),
        having: None,
        window_defs: vec![],
        order_by: vec![],
        limit: None,
        offset: None,
        set_operations: vec![],
    };
    assert_eq!(stmt.to_string(), "SELECT * FROM users");
}

#[test]
fn test_create_table_display() {
    let stmt = CreateTableStatement {
        token: make_token(TokenType::Keyword, "CREATE"),
        table_name: Identifier::new(make_token(TokenType::Identifier, "users"), "users"),
        if_not_exists: true,
        columns: vec![
            ColumnDefinition {
                name: Identifier::new(make_token(TokenType::Identifier, "id"), "id"),
                data_type: "INTEGER".into(),
                constraints: vec![ColumnConstraint::PrimaryKey],
            },
            ColumnDefinition {
                name: Identifier::new(make_token(TokenType::Identifier, "name"), "name"),
                data_type: "TEXT".into(),
                constraints: vec![ColumnConstraint::NotNull],
            },
        ],
        table_constraints: vec![],
        as_select: None,
    };
    assert_eq!(
        stmt.to_string(),
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
    );
}

#[test]
fn test_insert_statement_display() {
    let stmt = InsertStatement {
        token: make_token(TokenType::Keyword, "INSERT"),
        table_name: Identifier::new(make_token(TokenType::Identifier, "users"), "users"),
        columns: vec![
            Identifier::new(make_token(TokenType::Identifier, "id"), "id"),
            Identifier::new(make_token(TokenType::Identifier, "name"), "name"),
        ],
        values: vec![vec![
            Expression::IntegerLiteral(IntegerLiteral {
                token: make_token(TokenType::Integer, "1"),
                value: 1,
            }),
            Expression::StringLiteral(StringLiteral {
                token: make_token(TokenType::String, "'Alice'"),
                value: "Alice".into(),
                type_hint: None,
            }),
        ]],
        select: None,
        on_duplicate: false,
        update_columns: vec![],
        update_expressions: vec![],
        do_nothing: false,
        conflict_target: vec![],
        returning: vec![],
    };
    assert_eq!(
        stmt.to_string(),
        "INSERT INTO users (id, name) VALUES (1, 'Alice')"
    );
}

#[test]
fn test_case_expression_display() {
    let case_expr = CaseExpression {
        token: make_token(TokenType::Keyword, "CASE"),
        value: None,
        when_clauses: vec![WhenClause {
            token: make_token(TokenType::Keyword, "WHEN"),
            condition: Expression::BooleanLiteral(BooleanLiteral {
                token: make_token(TokenType::Keyword, "TRUE"),
                value: true,
            }),
            then_result: Expression::IntegerLiteral(IntegerLiteral {
                token: make_token(TokenType::Integer, "1"),
                value: 1,
            }),
        }],
        else_value: Some(Box::new(Expression::IntegerLiteral(IntegerLiteral {
            token: make_token(TokenType::Integer, "0"),
            value: 0,
        }))),
    };
    assert_eq!(case_expr.to_string(), "CASE WHEN TRUE THEN 1 ELSE 0 END");
}
