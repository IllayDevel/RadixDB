// Copyright 2026 RadixDB Contributors

use std::panic::{catch_unwind, AssertUnwindSafe};

use radixdb::common::CompactArc;
use radixdb::core::ValueSet;
use radixdb::executor::context::ExecutionContext;
use radixdb::executor::expression::{
    clear_program_cache, CompileContext, ExprCompiler, ExprVM, JoinFilter, Op, Program, RowFilter,
};
use radixdb::functions::global_registry;
use radixdb::parser::ast::{Expression, FunctionCall, Identifier, InHashSetExpression, Statement};
use radixdb::parser::parse_sql;
use radixdb::parser::token::{Position, Token, TokenType};
use radixdb::{Row, Value};

fn token(kind: TokenType, literal: &str) -> Token {
    Token::new(kind, literal, Position::new(0, 1, 1))
}

fn where_expression(sql: &str) -> Expression {
    let statements = parse_sql(sql).expect("parse batch-A expression");
    let Statement::Select(select) = &statements[0] else {
        panic!("expected SELECT")
    };
    select
        .where_clause
        .as_deref()
        .expect("expected WHERE expression")
        .clone()
}

fn in_hash_set(values: impl IntoIterator<Item = Value>) -> Expression {
    let values: ValueSet = values.into_iter().collect();
    Expression::InHashSet(InHashSetExpression {
        token: token(TokenType::Keyword, "IN"),
        column: Box::new(Expression::Identifier(Identifier::new(
            token(TokenType::Identifier, "id"),
            "id".to_string(),
        ))),
        values: CompactArc::new(values),
        not: false,
    })
}

#[test]
fn r5_l01_batch_a_checked_vm_contract_matrix() {
    // Cache identity must include captured InHashSet values, not only cardinality.
    clear_program_cache();
    let columns = vec!["id".to_string()];
    let first = RowFilter::new(&in_hash_set([Value::Integer(1)]), &columns).unwrap();
    let second = RowFilter::new(&in_hash_set([Value::Integer(2)]), &columns).unwrap();
    let row = Row::from_values(vec![Value::Integer(2)]);
    assert!(!first.matches(&row).unwrap());
    assert!(
        second.matches(&row).unwrap(),
        "cached program captured another set"
    );

    // Join filters must carry the transaction context just like row filters.
    let txn_expr = where_expression("SELECT 1 WHERE CURRENT_TRANSACTION_ID() = 42");
    let join_filter = JoinFilter::new(&txn_expr, &[], &[], global_registry())
        .unwrap()
        .with_context(&ExecutionContext::new().with_transaction_id(42));
    assert!(join_filter.matches(&Row::new(), &Row::new()).unwrap());

    // Runtime errors in a physical join are errors, never synthetic non-matches.
    let regexp_expr = where_expression("SELECT 1 WHERE l.val REGEXP $1");
    let params = ExecutionContext::with_params(smallvec::smallvec![Value::Text("[invalid".into())]);
    let regexp_filter =
        JoinFilter::new(&regexp_expr, &["l.val".to_string()], &[], global_registry())
            .unwrap()
            .with_context(&params);
    assert!(regexp_filter
        .matches_checked(
            &Row::from_values(vec![Value::Text("left".into())]),
            &Row::new(),
        )
        .is_err());

    // Every bytecode operand must be admitted before narrowing.
    let columns: Vec<String> = (0..=u16::MAX as usize + 1)
        .map(|index| format!("c{index}"))
        .collect();
    let ctx = CompileContext::with_global_registry(&columns);
    let overflow_column = Expression::Identifier(Identifier::new(
        token(TokenType::Identifier, "c65536"),
        "c65536".to_string(),
    ));
    assert!(ExprCompiler::new(&ctx).compile(&overflow_column).is_err());

    let oversized_call = Expression::FunctionCall(Box::new(FunctionCall {
        token: token(TokenType::Identifier, "GREATEST"),
        function: "GREATEST".into(),
        arguments: (0..=u8::MAX)
            .map(|_| {
                Expression::Identifier(Identifier::new(
                    token(TokenType::Identifier, "x"),
                    "x".to_string(),
                ))
            })
            .collect(),
        is_distinct: false,
        order_by: Vec::new(),
        filter: None,
    }));
    let one_column = vec!["x".to_string()];
    let ctx = CompileContext::with_global_registry(&one_column);
    assert!(ExprCompiler::new(&ctx).compile(&oversized_call).is_err());

    // MIN / -1 must surface checked overflow for both DIV and MOD, never panic.
    for op in [Op::Div, Op::Mod] {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let program = Program::try_new(vec![
                Op::LoadConst(Value::Integer(i64::MIN)),
                Op::LoadConst(Value::Integer(-1)),
                op,
                Op::Return,
            ])
            .expect("valid test program");
            ExprVM::new().execute(
                &program,
                &radixdb::executor::expression::ExecuteContext::new(&Row::new()),
            )
        }));
        assert!(outcome.is_ok(), "integer arithmetic panicked");
        assert!(outcome.unwrap().is_err(), "overflow was not reported");
    }

    // VM subquery placeholders have no production planner/index owner and must
    // fail at compilation instead of silently using slot zero/false/NULL.
    let subquery = where_expression("SELECT 1 WHERE EXISTS (SELECT 1)");
    let ctx = CompileContext::with_global_registry(&[]);
    assert!(ExprCompiler::new(&ctx).compile(&subquery).is_err());
}
