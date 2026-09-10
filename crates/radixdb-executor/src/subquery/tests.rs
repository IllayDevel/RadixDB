use super::*;
use radixdb_sql::token::{Position, Token, TokenType};

struct TestHost;

impl SubqueryHost for TestHost {
    fn subquery_engine(&self) -> &Arc<MVCCEngine> {
        unreachable!("static subquery analysis does not access storage")
    }

    fn subquery_open_table(
        &self,
        _table_name: &str,
    ) -> Result<crate::access::handle::QueryTableHandle> {
        unreachable!("static subquery analysis does not access storage")
    }

    fn subquery_execute_select(
        &self,
        _statement: &SelectStatement,
        _context: &ExecutionContext,
    ) -> Result<Box<dyn QueryResult>> {
        unreachable!("static subquery analysis does not execute SELECT")
    }
}

fn tok() -> Token {
    Token::new(TokenType::Keyword, "", Position::new(0, 0, 0))
}

fn ident(name: &str) -> Expression {
    Expression::Identifier(Identifier::new(tok(), name))
}

fn qual(table: &str, col: &str) -> Expression {
    Expression::QualifiedIdentifier(QualifiedIdentifier {
        token: tok(),
        qualifier: Box::new(Identifier::new(tok(), table)),
        intermediate: None,
        name: Box::new(Identifier::new(tok(), col)),
    })
}

fn int_lit(val: i64) -> Expression {
    Expression::IntegerLiteral(IntegerLiteral {
        token: tok(),
        value: val,
    })
}

/// outer_t.x — outer reference (not in inner_tables)
fn outer_ref() -> Expression {
    qual("outer_t", "x")
}

/// inner_t.x — inner reference (in inner_tables)
fn inner_ref() -> Expression {
    qual("inner_t", "x")
}

fn inner_tables() -> Vec<String> {
    vec!["inner_t".to_string()]
}

fn has_outer(expr: &Expression) -> bool {
    SubqueryExecutor::<TestHost>::expression_has_outer_reference(expr, &inner_tables())
}

fn make_infix(left: Expression, right: Expression) -> Expression {
    Expression::Infix(InfixExpression {
        token: tok(),
        left: Box::new(left),
        operator: "=".into(),
        op_type: InfixOperator::Equal,
        right: Box::new(right),
    })
}

// ── Infix ──────────────────────────────────────────────────
#[test]
fn test_infix_no_outer() {
    assert!(!has_outer(&make_infix(inner_ref(), int_lit(1))));
}

#[test]
fn test_infix_left_outer() {
    // Catches || → && mutation: only left has outer ref
    assert!(has_outer(&make_infix(outer_ref(), int_lit(1))));
}

#[test]
fn test_infix_right_outer() {
    // Catches || → && mutation: only right has outer ref
    assert!(has_outer(&make_infix(int_lit(1), outer_ref())));
}

// ── Prefix ─────────────────────────────────────────────────
#[test]
fn test_prefix_no_outer() {
    let expr = Expression::Prefix(PrefixExpression {
        token: tok(),
        operator: "NOT".into(),
        op_type: PrefixOperator::Not,
        right: Box::new(inner_ref()),
    });
    assert!(!has_outer(&expr));
}

#[test]
fn test_prefix_with_outer() {
    let expr = Expression::Prefix(PrefixExpression {
        token: tok(),
        operator: "NOT".into(),
        op_type: PrefixOperator::Not,
        right: Box::new(outer_ref()),
    });
    assert!(has_outer(&expr));
}

// ── FunctionCall ───────────────────────────────────────────
#[test]
fn test_func_no_outer() {
    let expr = Expression::FunctionCall(Box::new(FunctionCall {
        token: tok(),
        function: "ABS".into(),
        arguments: vec![inner_ref()],
        is_distinct: false,
        order_by: vec![],
        filter: None,
    }));
    assert!(!has_outer(&expr));
}

#[test]
fn test_func_with_outer() {
    let expr = Expression::FunctionCall(Box::new(FunctionCall {
        token: tok(),
        function: "ABS".into(),
        arguments: vec![outer_ref()],
        is_distinct: false,
        order_by: vec![],
        filter: None,
    }));
    assert!(has_outer(&expr));
}

// ── In ─────────────────────────────────────────────────────
#[test]
fn test_in_no_outer() {
    let expr = Expression::In(InExpression {
        token: tok(),
        left: Box::new(inner_ref()),
        right: Box::new(int_lit(1)),
        not: false,
    });
    assert!(!has_outer(&expr));
}

#[test]
fn test_in_left_outer() {
    let expr = Expression::In(InExpression {
        token: tok(),
        left: Box::new(outer_ref()),
        right: Box::new(int_lit(1)),
        not: false,
    });
    assert!(has_outer(&expr));
}

#[test]
fn test_in_right_outer() {
    let expr = Expression::In(InExpression {
        token: tok(),
        left: Box::new(int_lit(1)),
        right: Box::new(outer_ref()),
        not: false,
    });
    assert!(has_outer(&expr));
}

// ── Between ────────────────────────────────────────────────
#[test]
fn test_between_no_outer() {
    let expr = Expression::Between(BetweenExpression {
        token: tok(),
        expr: Box::new(inner_ref()),
        lower: Box::new(int_lit(1)),
        upper: Box::new(int_lit(10)),
        not: false,
    });
    assert!(!has_outer(&expr));
}

#[test]
fn test_between_expr_outer() {
    let expr = Expression::Between(BetweenExpression {
        token: tok(),
        expr: Box::new(outer_ref()),
        lower: Box::new(int_lit(1)),
        upper: Box::new(int_lit(10)),
        not: false,
    });
    assert!(has_outer(&expr));
}

#[test]
fn test_between_lower_outer() {
    let expr = Expression::Between(BetweenExpression {
        token: tok(),
        expr: Box::new(int_lit(5)),
        lower: Box::new(outer_ref()),
        upper: Box::new(int_lit(10)),
        not: false,
    });
    assert!(has_outer(&expr));
}

#[test]
fn test_between_upper_outer() {
    let expr = Expression::Between(BetweenExpression {
        token: tok(),
        expr: Box::new(int_lit(5)),
        lower: Box::new(int_lit(1)),
        upper: Box::new(outer_ref()),
        not: false,
    });
    assert!(has_outer(&expr));
}

// ── Case ───────────────────────────────────────────────────
#[test]
fn test_case_no_outer() {
    let expr = Expression::Case(Box::new(CaseExpression {
        token: tok(),
        value: None,
        when_clauses: vec![WhenClause {
            token: tok(),
            condition: inner_ref(),
            then_result: int_lit(1),
        }],
        else_value: Some(Box::new(int_lit(0))),
    }));
    assert!(!has_outer(&expr));
}

#[test]
fn test_case_value_outer() {
    let expr = Expression::Case(Box::new(CaseExpression {
        token: tok(),
        value: Some(Box::new(outer_ref())),
        when_clauses: vec![WhenClause {
            token: tok(),
            condition: int_lit(1),
            then_result: int_lit(1),
        }],
        else_value: None,
    }));
    assert!(has_outer(&expr));
}

#[test]
fn test_case_when_condition_outer() {
    let expr = Expression::Case(Box::new(CaseExpression {
        token: tok(),
        value: None,
        when_clauses: vec![WhenClause {
            token: tok(),
            condition: outer_ref(),
            then_result: int_lit(1),
        }],
        else_value: None,
    }));
    assert!(has_outer(&expr));
}

#[test]
fn test_case_when_then_outer() {
    let expr = Expression::Case(Box::new(CaseExpression {
        token: tok(),
        value: None,
        when_clauses: vec![WhenClause {
            token: tok(),
            condition: int_lit(1),
            then_result: outer_ref(),
        }],
        else_value: None,
    }));
    assert!(has_outer(&expr));
}

#[test]
fn test_case_else_outer() {
    let expr = Expression::Case(Box::new(CaseExpression {
        token: tok(),
        value: None,
        when_clauses: vec![WhenClause {
            token: tok(),
            condition: int_lit(1),
            then_result: int_lit(1),
        }],
        else_value: Some(Box::new(outer_ref())),
    }));
    assert!(has_outer(&expr));
}

// ── Cast ───────────────────────────────────────────────────
#[test]
fn test_cast_no_outer() {
    let expr = Expression::Cast(CastExpression {
        token: tok(),
        expr: Box::new(inner_ref()),
        type_name: "INTEGER".into(),
    });
    assert!(!has_outer(&expr));
}

#[test]
fn test_cast_with_outer() {
    let expr = Expression::Cast(CastExpression {
        token: tok(),
        expr: Box::new(outer_ref()),
        type_name: "INTEGER".into(),
    });
    assert!(has_outer(&expr));
}

// ── Aliased ────────────────────────────────────────────────
#[test]
fn test_aliased_no_outer() {
    let expr = Expression::Aliased(AliasedExpression {
        token: tok(),
        expression: Box::new(inner_ref()),
        alias: Identifier::new(tok(), "a"),
    });
    assert!(!has_outer(&expr));
}

#[test]
fn test_aliased_with_outer() {
    let expr = Expression::Aliased(AliasedExpression {
        token: tok(),
        expression: Box::new(outer_ref()),
        alias: Identifier::new(tok(), "a"),
    });
    assert!(has_outer(&expr));
}

// ── Like ───────────────────────────────────────────────────
#[test]
fn test_like_no_outer() {
    let expr = Expression::Like(LikeExpression {
        token: tok(),
        left: Box::new(inner_ref()),
        pattern: Box::new(Expression::StringLiteral(StringLiteral {
            token: tok(),
            value: "%x%".into(),
            type_hint: None,
        })),
        operator: "LIKE".into(),
        escape: None,
    });
    assert!(!has_outer(&expr));
}

#[test]
fn test_like_left_outer() {
    let expr = Expression::Like(LikeExpression {
        token: tok(),
        left: Box::new(outer_ref()),
        pattern: Box::new(Expression::StringLiteral(StringLiteral {
            token: tok(),
            value: "%x%".into(),
            type_hint: None,
        })),
        operator: "LIKE".into(),
        escape: None,
    });
    assert!(has_outer(&expr));
}

#[test]
fn test_like_pattern_outer() {
    let expr = Expression::Like(LikeExpression {
        token: tok(),
        left: Box::new(int_lit(1)),
        pattern: Box::new(outer_ref()),
        operator: "LIKE".into(),
        escape: None,
    });
    assert!(has_outer(&expr));
}

// ── ExpressionList ─────────────────────────────────────────
#[test]
fn test_expr_list_no_outer() {
    let expr = Expression::ExpressionList(Box::new(ExpressionList {
        token: tok(),
        expressions: vec![int_lit(1), inner_ref()],
    }));
    assert!(!has_outer(&expr));
}

#[test]
fn test_expr_list_with_outer() {
    let expr = Expression::ExpressionList(Box::new(ExpressionList {
        token: tok(),
        expressions: vec![int_lit(1), outer_ref()],
    }));
    assert!(has_outer(&expr));
}

// ── List ───────────────────────────────────────────────────
#[test]
fn test_list_no_outer() {
    let expr = Expression::List(Box::new(ListExpression {
        token: tok(),
        elements: vec![int_lit(1), int_lit(2)],
    }));
    assert!(!has_outer(&expr));
}

#[test]
fn test_list_with_outer() {
    let expr = Expression::List(Box::new(ListExpression {
        token: tok(),
        elements: vec![int_lit(1), outer_ref()],
    }));
    assert!(has_outer(&expr));
}

// ── InHashSet ──────────────────────────────────────────────
#[test]
fn test_in_hash_set_returns_false() {
    let expr = Expression::InHashSet(InHashSetExpression {
        token: tok(),
        column: Box::new(outer_ref()),
        values: CompactArc::new(radixdb_core::ValueSet::default()),
        not: false,
    });
    assert!(!has_outer(&expr));
}

// ── Window ─────────────────────────────────────────────────
fn make_window(partition: Vec<Expression>, order: Vec<Expression>) -> Expression {
    Expression::Window(Box::new(WindowExpression {
        token: tok(),
        function: Box::new(FunctionCall {
            token: tok(),
            function: "ROW_NUMBER".into(),
            arguments: vec![],
            is_distinct: false,
            order_by: vec![],
            filter: None,
        }),
        window_ref: None,
        partition_by: partition,
        order_by: order
            .into_iter()
            .map(|e| OrderByExpression {
                expression: e,
                ascending: true,
                nulls_first: None,
            })
            .collect(),
        frame: None,
    }))
}

#[test]
fn test_window_no_outer() {
    assert!(!has_outer(&make_window(vec![inner_ref()], vec![])));
}

#[test]
fn test_window_partition_outer() {
    assert!(has_outer(&make_window(vec![outer_ref()], vec![])));
}

#[test]
fn test_window_order_outer() {
    assert!(has_outer(&make_window(vec![], vec![outer_ref()])));
}

// ── Literals / Identifiers (catch-all false) ───────────────
#[test]
fn test_literals_return_false() {
    assert!(!has_outer(&ident("col")));
    assert!(!has_outer(&int_lit(42)));
    assert!(!has_outer(&Expression::FloatLiteral(FloatLiteral {
        token: tok(),
        value: 3.15,
    })));
    assert!(!has_outer(&Expression::StringLiteral(StringLiteral {
        token: tok(),
        value: "hello".into(),
        type_hint: None,
    })));
    assert!(!has_outer(&Expression::BooleanLiteral(BooleanLiteral {
        token: tok(),
        value: true,
    })));
    assert!(!has_outer(&Expression::NullLiteral(NullLiteral {
        token: tok(),
    })));
}

// ── QualifiedIdentifier (the core check) ──────────────────
#[test]
fn test_qualified_inner_false() {
    assert!(!has_outer(&inner_ref()));
}

#[test]
fn test_qualified_outer_true() {
    assert!(has_outer(&outer_ref()));
}

// ====================================================================
// Tests for predicate_has_outer_refs_or_subqueries
// ====================================================================

fn pred_outer(expr: &Expression) -> bool {
    SubqueryExecutor::<TestHost>::predicate_has_outer_refs_or_subqueries(expr, "inner_t", "i")
}

/// inner_t.x — matches inner table name
fn pred_inner() -> Expression {
    qual("inner_t", "x")
}

/// i.x — matches inner table alias
fn pred_inner_alias() -> Expression {
    qual("i", "x")
}

/// outer_t.x — doesn't match inner table
fn pred_outer_ref() -> Expression {
    qual("outer_t", "x")
}

// ── QualifiedIdentifier ────────────────────────────────────
#[test]
fn test_pred_inner_table_false() {
    assert!(!pred_outer(&pred_inner()));
}

#[test]
fn test_pred_inner_alias_false() {
    assert!(!pred_outer(&pred_inner_alias()));
}

#[test]
fn test_pred_outer_ref_true() {
    assert!(pred_outer(&pred_outer_ref()));
}

// ── Infix ──────────────────────────────────────────────────
#[test]
fn test_pred_infix_no_outer() {
    assert!(!pred_outer(&make_infix(pred_inner(), int_lit(1))));
}

#[test]
fn test_pred_infix_left_outer() {
    assert!(pred_outer(&make_infix(pred_outer_ref(), int_lit(1))));
}

#[test]
fn test_pred_infix_right_outer() {
    assert!(pred_outer(&make_infix(int_lit(1), pred_outer_ref())));
}

// ── Prefix ─────────────────────────────────────────────────
#[test]
fn test_pred_prefix_no_outer() {
    let expr = Expression::Prefix(PrefixExpression {
        token: tok(),
        operator: "NOT".into(),
        op_type: PrefixOperator::Not,
        right: Box::new(pred_inner()),
    });
    assert!(!pred_outer(&expr));
}

#[test]
fn test_pred_prefix_with_outer() {
    let expr = Expression::Prefix(PrefixExpression {
        token: tok(),
        operator: "NOT".into(),
        op_type: PrefixOperator::Not,
        right: Box::new(pred_outer_ref()),
    });
    assert!(pred_outer(&expr));
}

// ── FunctionCall ───────────────────────────────────────────
#[test]
fn test_pred_func_no_outer() {
    let expr = Expression::FunctionCall(Box::new(FunctionCall {
        token: tok(),
        function: "ABS".into(),
        arguments: vec![pred_inner()],
        is_distinct: false,
        order_by: vec![],
        filter: None,
    }));
    assert!(!pred_outer(&expr));
}

#[test]
fn test_pred_func_with_outer() {
    let expr = Expression::FunctionCall(Box::new(FunctionCall {
        token: tok(),
        function: "ABS".into(),
        arguments: vec![pred_outer_ref()],
        is_distinct: false,
        order_by: vec![],
        filter: None,
    }));
    assert!(pred_outer(&expr));
}

// ── In ─────────────────────────────────────────────────────
#[test]
fn test_pred_in_no_outer() {
    let expr = Expression::In(InExpression {
        token: tok(),
        left: Box::new(pred_inner()),
        right: Box::new(int_lit(1)),
        not: false,
    });
    assert!(!pred_outer(&expr));
}

#[test]
fn test_pred_in_left_outer() {
    let expr = Expression::In(InExpression {
        token: tok(),
        left: Box::new(pred_outer_ref()),
        right: Box::new(int_lit(1)),
        not: false,
    });
    assert!(pred_outer(&expr));
}

#[test]
fn test_pred_in_right_outer() {
    let expr = Expression::In(InExpression {
        token: tok(),
        left: Box::new(int_lit(1)),
        right: Box::new(pred_outer_ref()),
        not: false,
    });
    assert!(pred_outer(&expr));
}

// ── Between ────────────────────────────────────────────────
#[test]
fn test_pred_between_no_outer() {
    let expr = Expression::Between(BetweenExpression {
        token: tok(),
        expr: Box::new(pred_inner()),
        lower: Box::new(int_lit(1)),
        upper: Box::new(int_lit(10)),
        not: false,
    });
    assert!(!pred_outer(&expr));
}

#[test]
fn test_pred_between_expr_outer() {
    let expr = Expression::Between(BetweenExpression {
        token: tok(),
        expr: Box::new(pred_outer_ref()),
        lower: Box::new(int_lit(1)),
        upper: Box::new(int_lit(10)),
        not: false,
    });
    assert!(pred_outer(&expr));
}

#[test]
fn test_pred_between_lower_outer() {
    let expr = Expression::Between(BetweenExpression {
        token: tok(),
        expr: Box::new(int_lit(5)),
        lower: Box::new(pred_outer_ref()),
        upper: Box::new(int_lit(10)),
        not: false,
    });
    assert!(pred_outer(&expr));
}

#[test]
fn test_pred_between_upper_outer() {
    let expr = Expression::Between(BetweenExpression {
        token: tok(),
        expr: Box::new(int_lit(5)),
        lower: Box::new(int_lit(1)),
        upper: Box::new(pred_outer_ref()),
        not: false,
    });
    assert!(pred_outer(&expr));
}

// ── Like ───────────────────────────────────────────────────
#[test]
fn test_pred_like_no_outer() {
    let expr = Expression::Like(LikeExpression {
        token: tok(),
        left: Box::new(pred_inner()),
        pattern: Box::new(Expression::StringLiteral(StringLiteral {
            token: tok(),
            value: "%x%".into(),
            type_hint: None,
        })),
        operator: "LIKE".into(),
        escape: None,
    });
    assert!(!pred_outer(&expr));
}

#[test]
fn test_pred_like_left_outer() {
    let expr = Expression::Like(LikeExpression {
        token: tok(),
        left: Box::new(pred_outer_ref()),
        pattern: Box::new(int_lit(1)),
        operator: "LIKE".into(),
        escape: None,
    });
    assert!(pred_outer(&expr));
}

#[test]
fn test_pred_like_pattern_outer() {
    let expr = Expression::Like(LikeExpression {
        token: tok(),
        left: Box::new(int_lit(1)),
        pattern: Box::new(pred_outer_ref()),
        operator: "LIKE".into(),
        escape: None,
    });
    assert!(pred_outer(&expr));
}

// ── Cast ───────────────────────────────────────────────────
#[test]
fn test_pred_cast_no_outer() {
    let expr = Expression::Cast(CastExpression {
        token: tok(),
        expr: Box::new(pred_inner()),
        type_name: "INTEGER".into(),
    });
    assert!(!pred_outer(&expr));
}

#[test]
fn test_pred_cast_with_outer() {
    let expr = Expression::Cast(CastExpression {
        token: tok(),
        expr: Box::new(pred_outer_ref()),
        type_name: "INTEGER".into(),
    });
    assert!(pred_outer(&expr));
}

// ── Case ───────────────────────────────────────────────────
#[test]
fn test_pred_case_no_outer() {
    let expr = Expression::Case(Box::new(CaseExpression {
        token: tok(),
        value: None,
        when_clauses: vec![WhenClause {
            token: tok(),
            condition: pred_inner(),
            then_result: int_lit(1),
        }],
        else_value: Some(Box::new(int_lit(0))),
    }));
    assert!(!pred_outer(&expr));
}

#[test]
fn test_pred_case_value_outer() {
    let expr = Expression::Case(Box::new(CaseExpression {
        token: tok(),
        value: Some(Box::new(pred_outer_ref())),
        when_clauses: vec![WhenClause {
            token: tok(),
            condition: int_lit(1),
            then_result: int_lit(1),
        }],
        else_value: None,
    }));
    assert!(pred_outer(&expr));
}

#[test]
fn test_pred_case_when_condition_outer() {
    let expr = Expression::Case(Box::new(CaseExpression {
        token: tok(),
        value: None,
        when_clauses: vec![WhenClause {
            token: tok(),
            condition: pred_outer_ref(),
            then_result: int_lit(1),
        }],
        else_value: None,
    }));
    assert!(pred_outer(&expr));
}

#[test]
fn test_pred_case_then_outer() {
    let expr = Expression::Case(Box::new(CaseExpression {
        token: tok(),
        value: None,
        when_clauses: vec![WhenClause {
            token: tok(),
            condition: int_lit(1),
            then_result: pred_outer_ref(),
        }],
        else_value: None,
    }));
    assert!(pred_outer(&expr));
}

#[test]
fn test_pred_case_else_outer() {
    let expr = Expression::Case(Box::new(CaseExpression {
        token: tok(),
        value: None,
        when_clauses: vec![WhenClause {
            token: tok(),
            condition: int_lit(1),
            then_result: int_lit(1),
        }],
        else_value: Some(Box::new(pred_outer_ref())),
    }));
    assert!(pred_outer(&expr));
}

// ── Subqueries return true ─────────────────────────────────
fn dummy_select() -> SelectStatement {
    SelectStatement {
        token: tok(),
        distinct: false,
        distinct_on: vec![],
        columns: vec![],
        with: None,
        table_expr: None,
        where_clause: None,
        group_by: GroupByClause::default(),
        having: None,
        window_defs: vec![],
        order_by: vec![],
        limit: None,
        offset: None,
        set_operations: vec![],
    }
}

#[test]
fn test_pred_exists_returns_true() {
    let expr = Expression::Exists(ExistsExpression {
        token: tok(),
        subquery: Box::new(dummy_select()),
    });
    assert!(pred_outer(&expr));
}

#[test]
fn test_pred_scalar_subquery_returns_true() {
    let expr = Expression::ScalarSubquery(ScalarSubquery {
        token: tok(),
        subquery: Box::new(dummy_select()),
    });
    assert!(pred_outer(&expr));
}

// ── Literals fall through to false ─────────────────────────
#[test]
fn test_pred_literals_false() {
    assert!(!pred_outer(&ident("col")));
    assert!(!pred_outer(&int_lit(42)));
}
