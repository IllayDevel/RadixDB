use radixdb_sql::{CommitStatement, Expression, Identifier, Position, Statement, Token, TokenType};

#[test]
fn ast_nodes_preserve_clone_and_equality_contracts() {
    let token = Token::new(TokenType::Identifier, "account_id", Position::new(7, 2, 4));
    let expression = Expression::Identifier(Identifier::new(token, "account_id"));
    assert_eq!(expression.clone(), expression);

    let statement = Statement::Commit(CommitStatement {
        token: Token::new(TokenType::Keyword, "COMMIT", Position::new(0, 1, 1)),
    });
    assert_eq!(statement.clone(), statement);
}

#[cfg(target_pointer_width = "64")]
#[test]
fn ast_layout_does_not_expand_during_the_crate_move() {
    use std::mem::size_of;

    assert_eq!(size_of::<Token>(), 48);
    assert_eq!(size_of::<Identifier>(), 80);
    assert_eq!(size_of::<Expression>(), 136);
    assert_eq!(size_of::<Statement>(), 344);
}
