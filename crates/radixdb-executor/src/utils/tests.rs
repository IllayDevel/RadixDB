#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_projection_lookup_preserves_qualified_and_ambiguity_contract() {
        let outer = vec!["left.id".to_string(), "left.payload".to_string()];
        let inner = vec!["right.id".to_string(), "right.payload".to_string()];

        assert_eq!(
            build_join_projection_lookup(&outer, &inner)
                .get("left.id")
                .copied(),
            Some(0)
        );
        assert_eq!(
            build_join_projection_lookup(&outer, &inner)
                .get("right.payload")
                .copied(),
            Some(3)
        );
        assert_eq!(
            build_join_projection_lookup(&outer, &inner)
                .get("id")
                .copied(),
            None,
            "an unqualified duplicate must remain ambiguous"
        );
    }

    #[test]
    fn join_projection_lookup_keeps_exact_unqualified_precedence() {
        let outer = vec!["left.id".to_string(), "name".to_string()];
        let inner = vec!["right.name".to_string()];

        assert_eq!(
            build_join_projection_lookup(&outer, &inner)
                .get("name")
                .copied(),
            Some(1)
        );
        assert_eq!(
            build_join_projection_lookup(&outer, &inner)
                .get("id")
                .copied(),
            Some(0),
            "qualified lookup retains the historical unique-base fallback"
        );
    }

    #[test]
    fn join_projection_lookup_preserves_case_insensitive_fallback() {
        let outer = vec!["People.ID".to_string(), "People.Name".to_string()];
        let inner = vec!["Fio.Value".to_string()];
        let lookup = build_join_projection_lookup(&outer, &inner);

        assert_eq!(lookup.get("people.id").copied(), Some(0));
        assert_eq!(lookup.get("name").copied(), Some(1));
        assert_eq!(lookup.get("fio.value").copied(), Some(2));
    }

    #[test]
    fn r8_l01_batch_h_blocking_operator_budget_is_hard_and_releasable() {
        let row = Row::from_values(vec![Value::Text("x".repeat(32).into())]);
        let row_bytes = RetainedRowsBudget::estimate_row_bytes(&row);
        let mut budget = RetainedRowsBudget::with_limits("test owner", 2, row_bytes * 2);
        budget.admit(&row).unwrap();
        budget.admit(&row).unwrap();
        let error = budget.admit(&row).unwrap_err().to_string();
        assert!(error.contains("test owner retained-row budget exceeded"));
        budget.release(&row);
        budget.admit(&row).unwrap();
        assert_eq!(budget.retained_rows(), 2);
        assert_eq!(budget.retained_bytes(), row_bytes * 2);
    }

    #[test]
    fn auxiliary_values_are_byte_bounded_without_consuming_row_slots() {
        let value = Value::Integer(42);
        let value_bytes = RetainedRowsBudget::estimate_values_bytes(std::slice::from_ref(&value));
        let mut budget = RetainedRowsBudget::with_limits("test owner", 1, value_bytes * 3);
        for _ in 0..3 {
            budget.admit_values(std::slice::from_ref(&value)).unwrap();
        }
        assert_eq!(budget.retained_rows(), 0);
        assert_eq!(budget.retained_bytes(), value_bytes * 3);
        budget.release_values(std::slice::from_ref(&value));
        assert_eq!(budget.retained_bytes(), value_bytes * 2);
    }

    // ============================================================================
    // Token Creation Tests
    // ============================================================================

    #[test]
    fn test_dummy_token_integer() {
        let token = dummy_token("42", TokenType::Integer);
        assert_eq!(token.literal, "42");
        assert_eq!(token.token_type, TokenType::Integer);
    }

    #[test]
    fn test_dummy_token_string() {
        let token = dummy_token("hello", TokenType::String);
        assert_eq!(token.literal, "hello");
        assert_eq!(token.token_type, TokenType::String);
    }

    // ============================================================================
    // Value to Expression Tests
    // ============================================================================

    #[test]
    fn test_value_to_expression_integer() {
        let expr = value_to_expression(&Value::Integer(42));
        match expr {
            Expression::IntegerLiteral(lit) => assert_eq!(lit.value, 42),
            _ => panic!("Expected IntegerLiteral"),
        }
    }

    #[test]
    fn test_value_to_expression_float() {
        let expr = value_to_expression(&Value::Float(3.5));
        match expr {
            Expression::FloatLiteral(lit) => assert!((lit.value - 3.5).abs() < f64::EPSILON),
            _ => panic!("Expected FloatLiteral"),
        }
    }

    #[test]
    fn test_value_to_expression_text() {
        let expr = value_to_expression(&Value::Text("hello".into()));
        match expr {
            Expression::StringLiteral(lit) => assert_eq!(lit.value, "hello"),
            _ => panic!("Expected StringLiteral"),
        }
    }

    #[test]
    fn test_value_to_expression_boolean() {
        let expr_true = value_to_expression(&Value::Boolean(true));
        match expr_true {
            Expression::BooleanLiteral(lit) => assert!(lit.value),
            _ => panic!("Expected BooleanLiteral"),
        }

        let expr_false = value_to_expression(&Value::Boolean(false));
        match expr_false {
            Expression::BooleanLiteral(lit) => assert!(!lit.value),
            _ => panic!("Expected BooleanLiteral"),
        }
    }

    #[test]
    fn test_value_to_expression_null() {
        let expr = value_to_expression(&Value::Null(DataType::Integer));
        match expr {
            Expression::NullLiteral(_) => {}
            _ => panic!("Expected NullLiteral"),
        }
    }

    // ============================================================================
    // Column Index Map Tests
    // ============================================================================

    #[test]
    fn test_build_column_index_map() {
        let columns = vec!["ID".to_string(), "Name".to_string(), "Age".to_string()];
        let map = build_column_index_map(&columns);

        assert_eq!(map.get("id"), Some(&0));
        assert_eq!(map.get("name"), Some(&1));
        assert_eq!(map.get("age"), Some(&2));
        assert_eq!(map.get("unknown"), None);
    }

    #[test]
    fn test_build_column_index_map_empty() {
        let columns: Vec<String> = vec![];
        let map = build_column_index_map(&columns);
        assert!(map.is_empty());
    }

    // ============================================================================
    // Row Combination Tests
    // ============================================================================

    #[test]
    fn test_combine_rows() {
        let left = Row::from(vec![Value::Integer(1), Value::Text("a".into())]);
        let right = Row::from(vec![Value::Integer(2), Value::Text("b".into())]);

        let combined = combine_rows(&left, &right, 2, 2);
        assert_eq!(combined.len(), 4);
        assert_eq!(combined[0], Value::Integer(1));
        assert_eq!(combined[1], Value::Text("a".into()));
        assert_eq!(combined[2], Value::Integer(2));
        assert_eq!(combined[3], Value::Text("b".into()));
    }

    #[test]
    fn test_combine_rows_with_nulls_left() {
        let row = Row::from(vec![Value::Integer(1), Value::Text("a".into())]);
        let combined = combine_rows_with_nulls(&row, 2, 2, true);

        assert_eq!(combined.len(), 4);
        assert_eq!(combined[0], Value::Integer(1));
        assert_eq!(combined[1], Value::Text("a".into()));
        assert!(combined[2].is_null());
        assert!(combined[3].is_null());
    }

    #[test]
    fn test_combine_rows_with_nulls_right() {
        let row = Row::from(vec![Value::Integer(1), Value::Text("a".into())]);
        let combined = combine_rows_with_nulls(&row, 2, 2, false);

        assert_eq!(combined.len(), 4);
        assert!(combined[0].is_null());
        assert!(combined[1].is_null());
        assert_eq!(combined[2], Value::Integer(1));
        assert_eq!(combined[3], Value::Text("a".into()));
    }

    // ============================================================================
    // Hashing Tests
    // ============================================================================

    #[test]
    fn test_hash_composite_key_single() {
        let row = Row::from(vec![Value::Integer(42)]);
        let hash1 = hash_composite_key(&row, &[0]);
        let hash2 = hash_composite_key(&row, &[0]);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hash_composite_key_multiple() {
        let row = Row::from(vec![
            Value::Integer(1),
            Value::Text("test".into()),
            Value::Integer(3),
        ]);
        let hash = hash_composite_key(&row, &[0, 1]);
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_hash_composite_key_different_values() {
        let row1 = Row::from(vec![Value::Integer(1)]);
        let row2 = Row::from(vec![Value::Integer(2)]);

        let hash1 = hash_composite_key(&row1, &[0]);
        let hash2 = hash_composite_key(&row2, &[0]);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hash_row() {
        let row1 = Row::from(vec![Value::Integer(1), Value::Text("a".into())]);
        let row2 = Row::from(vec![Value::Integer(1), Value::Text("a".into())]);
        let row3 = Row::from(vec![Value::Integer(2), Value::Text("a".into())]);

        assert_eq!(hash_row(&row1), hash_row(&row2));
        assert_ne!(hash_row(&row1), hash_row(&row3));
    }

    // ============================================================================
    // Value Comparison Tests
    // ============================================================================

    #[test]
    fn test_values_equal_integers() {
        assert!(values_equal(&Value::Integer(42), &Value::Integer(42)));
        assert!(!values_equal(&Value::Integer(42), &Value::Integer(43)));
    }

    #[test]
    fn test_values_equal_floats() {
        assert!(values_equal(&Value::Float(3.5), &Value::Float(3.5)));
        assert!(!values_equal(&Value::Float(3.5), &Value::Float(3.6)));
    }

    #[test]
    fn test_values_equal_text() {
        assert!(values_equal(
            &Value::Text("hello".into()),
            &Value::Text("hello".into())
        ));
        assert!(!values_equal(
            &Value::Text("hello".into()),
            &Value::Text("world".into())
        ));
    }

    #[test]
    fn test_values_equal_null() {
        // NULL != NULL in SQL
        assert!(!values_equal(
            &Value::Null(DataType::Integer),
            &Value::Null(DataType::Integer)
        ));
    }

    #[test]
    fn test_values_equal_cross_type_numeric() {
        assert!(values_equal(&Value::Integer(42), &Value::Float(42.0)));
        assert!(values_equal(&Value::Float(42.0), &Value::Integer(42)));
    }

    #[test]
    fn test_canonical_numeric_hash_and_join_equality_boundaries() {
        use std::hash::DefaultHasher;

        fn hash(value: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            hash_value_into(value, &mut hasher);
            hasher.finish()
        }

        const EXACT: i64 = 1_i64 << 53;
        let integer = Value::Integer(EXACT);
        let exact_float = Value::Float(EXACT as f64);
        let integer_neighbor = Value::Integer(EXACT + 1);

        assert!(values_equal(&integer, &exact_float));
        assert_eq!(hash(&integer), hash(&exact_float));
        assert!(!values_equal(&integer_neighbor, &exact_float));

        let positive_zero = Value::Float(0.0);
        let negative_zero = Value::Float(-0.0);
        assert!(values_equal(&positive_zero, &negative_zero));
        assert_eq!(hash(&positive_zero), hash(&negative_zero));
        assert_eq!(hash(&positive_zero), hash(&Value::Integer(0)));

        let nan_a = Value::Float(f64::NAN);
        let nan_b = Value::Float(f64::from_bits(0x7ff8_0000_0000_0042));
        assert!(values_equal(&nan_a, &nan_b));
        assert_eq!(hash(&nan_a), hash(&nan_b));

        let null = Value::Null(DataType::Float);
        assert!(!values_equal(&null, &null));
    }

    #[test]
    fn test_canonical_composite_hash_numeric_boundaries() {
        const EXACT: i64 = 1_i64 << 53;
        let integer_row = Row::from_values(vec![
            Value::Integer(EXACT),
            Value::Float(-0.0),
            Value::Float(f64::NAN),
        ]);
        let float_row = Row::from_values(vec![
            Value::Float(EXACT as f64),
            Value::Integer(0),
            Value::Float(f64::from_bits(0x7ff8_0000_0000_0042)),
        ]);

        assert_eq!(
            hash_composite_key(&integer_row, &[0, 1, 2]),
            hash_composite_key(&float_row, &[0, 1, 2])
        );
        assert!(verify_composite_key_equality(
            &integer_row,
            &float_row,
            &[0, 1, 2],
            &[0, 1, 2]
        ));

        let neighbor = Row::from_values(vec![
            Value::Integer(EXACT + 1),
            Value::Integer(0),
            Value::Float(f64::NAN),
        ]);
        assert!(!verify_composite_key_equality(
            &neighbor,
            &float_row,
            &[0, 1, 2],
            &[0, 1, 2]
        ));
    }

    #[test]
    fn test_compare_values_integers() {
        assert_eq!(
            compare_values(&Value::Integer(1), &Value::Integer(2)),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Integer(2), &Value::Integer(1)),
            Ordering::Greater
        );
        assert_eq!(
            compare_values(&Value::Integer(1), &Value::Integer(1)),
            Ordering::Equal
        );
    }

    #[test]
    fn test_compare_values_text() {
        assert_eq!(
            compare_values(&Value::Text("a".into()), &Value::Text("b".into())),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Text("b".into()), &Value::Text("a".into())),
            Ordering::Greater
        );
    }

    #[test]
    fn test_compare_values_null_last() {
        // NULL sorts last
        assert_eq!(
            compare_values(&Value::Null(DataType::Integer), &Value::Integer(1)),
            Ordering::Greater
        );
        assert_eq!(
            compare_values(&Value::Integer(1), &Value::Null(DataType::Integer)),
            Ordering::Less
        );
    }

    #[test]
    fn test_compare_values_uses_exact_total_numeric_order() {
        const EXACT: i64 = 1_i64 << 53;

        assert_eq!(
            compare_values(&Value::Integer(EXACT), &Value::Float(EXACT as f64)),
            Ordering::Equal
        );
        assert_eq!(
            compare_values(&Value::Integer(EXACT + 1), &Value::Float(EXACT as f64)),
            Ordering::Greater
        );
        assert_eq!(
            compare_values(&Value::Float(-0.0), &Value::Integer(0)),
            Ordering::Equal
        );
        assert_eq!(
            compare_values(&Value::Float(f64::NAN), &Value::Float(1.0)),
            Ordering::Greater
        );
        assert_eq!(
            compare_values(
                &Value::Float(f64::NAN),
                &Value::Float(f64::from_bits(0x7ff8_0000_0000_0042))
            ),
            Ordering::Equal
        );
    }

    // ============================================================================
    // Row Equality Tests
    // ============================================================================

    #[test]
    fn test_rows_equal() {
        let row1 = Row::from(vec![Value::Integer(1), Value::Text("a".into())]);
        let row2 = Row::from(vec![Value::Integer(1), Value::Text("a".into())]);
        let row3 = Row::from(vec![Value::Integer(2), Value::Text("a".into())]);

        assert!(rows_equal(&row1, &row2));
        assert!(!rows_equal(&row1, &row3));
    }

    #[test]
    fn test_rows_equal_different_lengths() {
        let row1 = Row::from(vec![Value::Integer(1)]);
        let row2 = Row::from(vec![Value::Integer(1), Value::Integer(2)]);

        assert!(!rows_equal(&row1, &row2));
    }

    // ============================================================================
    // Column Name Extraction Tests
    // ============================================================================

    #[test]
    fn test_extract_column_name_identifier() {
        let expr = Expression::Identifier(Identifier::new(
            dummy_token("name", TokenType::Identifier),
            "name".to_string(),
        ));
        assert_eq!(extract_column_name(&expr), Some("name".to_string()));
    }

    #[test]
    fn test_extract_column_name_qualified() {
        let expr = Expression::QualifiedIdentifier(QualifiedIdentifier {
            token: dummy_token("t.col", TokenType::Identifier),
            qualifier: Box::new(Identifier::new(
                dummy_token("t", TokenType::Identifier),
                "t".to_string(),
            )),
            intermediate: None,
            name: Box::new(Identifier::new(
                dummy_token("col", TokenType::Identifier),
                "col".to_string(),
            )),
        });
        assert_eq!(extract_column_name(&expr), Some("col".to_string()));
    }

    #[test]
    fn test_extract_column_name_literal() {
        let expr = Expression::IntegerLiteral(IntegerLiteral {
            token: dummy_token("42", TokenType::Integer),
            value: 42,
        });
        assert_eq!(extract_column_name(&expr), None);
    }

    // ============================================================================
    // Literal Value Extraction Tests
    // ============================================================================

    #[test]
    fn test_extract_literal_value_integer() {
        let expr = Expression::IntegerLiteral(IntegerLiteral {
            token: dummy_token("42", TokenType::Integer),
            value: 42,
        });
        assert_eq!(extract_literal_value(&expr), Some(Value::Integer(42)));
    }

    #[test]
    fn test_extract_literal_value_float() {
        let expr = Expression::FloatLiteral(FloatLiteral {
            token: dummy_token("3.5", TokenType::Float),
            value: 3.5,
        });
        match extract_literal_value(&expr) {
            Some(Value::Float(f)) => assert!((f - 3.5).abs() < f64::EPSILON),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_extract_literal_value_string() {
        let expr = Expression::StringLiteral(StringLiteral {
            token: dummy_token("'hello'", TokenType::String),
            value: "hello".into(),
            type_hint: None,
        });
        assert_eq!(
            extract_literal_value(&expr),
            Some(Value::Text("hello".into()))
        );
    }

    #[test]
    fn test_extract_literal_value_preserves_timestamp_type_hint() {
        let expr = Expression::StringLiteral(StringLiteral {
            token: dummy_token("'2026-08-09 00:00:00'", TokenType::String),
            value: "2026-08-09 00:00:00".into(),
            type_hint: Some("TIMESTAMP".into()),
        });
        let Some(Value::Timestamp(value)) = extract_literal_value(&expr) else {
            panic!("typed TIMESTAMP literal must remain typed in planner helpers");
        };
        assert_eq!(value.to_rfc3339(), "2026-08-09T00:00:00+00:00");
    }

    #[test]
    fn test_extract_literal_value_boolean() {
        let expr = Expression::BooleanLiteral(BooleanLiteral {
            token: dummy_token("TRUE", TokenType::Keyword),
            value: true,
        });
        assert_eq!(extract_literal_value(&expr), Some(Value::Boolean(true)));
    }

    #[test]
    fn test_extract_literal_value_null() {
        let expr = Expression::NullLiteral(NullLiteral {
            token: dummy_token("NULL", TokenType::Keyword),
        });
        match extract_literal_value(&expr) {
            Some(Value::Null(_)) => {}
            _ => panic!("Expected Null"),
        }
    }

    // ============================================================================
    // Operator Tests
    // ============================================================================

    #[test]
    fn test_flip_operator() {
        assert_eq!(flip_operator(Operator::Lt), Operator::Gt);
        assert_eq!(flip_operator(Operator::Lte), Operator::Gte);
        assert_eq!(flip_operator(Operator::Gt), Operator::Lt);
        assert_eq!(flip_operator(Operator::Gte), Operator::Lte);
        assert_eq!(flip_operator(Operator::Eq), Operator::Eq);
        assert_eq!(flip_operator(Operator::Ne), Operator::Ne);
    }

    #[test]
    fn test_infix_to_operator() {
        assert_eq!(infix_to_operator(InfixOperator::Equal), Some(Operator::Eq));
        assert_eq!(
            infix_to_operator(InfixOperator::NotEqual),
            Some(Operator::Ne)
        );
        assert_eq!(
            infix_to_operator(InfixOperator::LessThan),
            Some(Operator::Lt)
        );
        assert_eq!(
            infix_to_operator(InfixOperator::LessEqual),
            Some(Operator::Lte)
        );
        assert_eq!(
            infix_to_operator(InfixOperator::GreaterThan),
            Some(Operator::Gt)
        );
        assert_eq!(
            infix_to_operator(InfixOperator::GreaterEqual),
            Some(Operator::Gte)
        );
        assert_eq!(infix_to_operator(InfixOperator::Add), None);
    }

    // ============================================================================
    // Base Column Name Tests
    // ============================================================================

    #[test]
    fn test_extract_base_column_name_simple() {
        assert_eq!(extract_base_column_name("column"), "column");
        assert_eq!(extract_base_column_name("COLUMN"), "column");
    }

    #[test]
    fn test_extract_base_column_name_qualified() {
        assert_eq!(extract_base_column_name("table.column"), "column");
        assert_eq!(extract_base_column_name("TABLE.COLUMN"), "column");
    }

    #[test]
    fn test_extract_base_column_name_multiple_dots() {
        // Uses rfind so finds last dot
        assert_eq!(extract_base_column_name("a.b.c"), "c");
    }

    // ============================================================================
    // Find Column Index Tests
    // ============================================================================

    #[test]
    fn test_find_column_index_exact_match() {
        let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        assert_eq!(
            find_column_index(&(None, "id".to_string()), &columns),
            Some(0)
        );
        assert_eq!(
            find_column_index(&(None, "name".to_string()), &columns),
            Some(1)
        );
    }

    #[test]
    fn test_find_column_index_qualified_match() {
        let columns = vec!["t.id".to_string(), "t.name".to_string()];
        assert_eq!(
            find_column_index(&(Some("t".to_string()), "id".to_string()), &columns),
            Some(0)
        );
    }

    #[test]
    fn test_find_column_index_suffix_match() {
        let columns = vec!["t1.id".to_string(), "t1.name".to_string()];
        // Without qualifier, can match suffix
        assert_eq!(
            find_column_index(&(None, "id".to_string()), &columns),
            Some(0)
        );
    }

    #[test]
    fn test_find_column_index_not_found() {
        let columns = vec!["id".to_string(), "name".to_string()];
        assert_eq!(
            find_column_index(&(None, "unknown".to_string()), &columns),
            None
        );
    }

    // ============================================================================
    // Verify Composite Key Equality Tests
    // ============================================================================

    #[test]
    fn test_verify_composite_key_equality_equal() {
        let row1 = Row::from(vec![Value::Integer(1), Value::Text("a".into())]);
        let row2 = Row::from(vec![Value::Integer(1), Value::Text("a".into())]);

        assert!(verify_composite_key_equality(
            &row1,
            &row2,
            &[0, 1],
            &[0, 1]
        ));
    }

    #[test]
    fn test_verify_composite_key_equality_not_equal() {
        let row1 = Row::from(vec![Value::Integer(1), Value::Text("a".into())]);
        let row2 = Row::from(vec![Value::Integer(1), Value::Text("b".into())]);

        assert!(!verify_composite_key_equality(
            &row1,
            &row2,
            &[0, 1],
            &[0, 1]
        ));
    }

    #[test]
    fn test_verify_composite_key_equality_partial() {
        let row1 = Row::from(vec![Value::Integer(1), Value::Text("a".into())]);
        let row2 = Row::from(vec![Value::Integer(1), Value::Text("b".into())]);

        // Only compare first column
        assert!(verify_composite_key_equality(&row1, &row2, &[0], &[0]));
    }

    // ============================================================================
    // Expression Contains Aggregate Tests
    // ============================================================================

    #[test]
    fn test_expression_contains_aggregate_count() {
        let expr = Expression::FunctionCall(Box::new(FunctionCall {
            token: dummy_token("COUNT", TokenType::Identifier),
            function: "COUNT".into(),
            arguments: vec![Expression::Identifier(Identifier::new(
                dummy_token("*", TokenType::Operator),
                "*".to_string(),
            ))],
            is_distinct: false,
            order_by: vec![],
            filter: None,
        }));
        assert!(expression_contains_aggregate(&expr));
    }

    #[test]
    fn test_expression_contains_aggregate_non_aggregate() {
        let expr = Expression::FunctionCall(Box::new(FunctionCall {
            token: dummy_token("UPPER", TokenType::Identifier),
            function: "UPPER".into(),
            arguments: vec![Expression::Identifier(Identifier::new(
                dummy_token("name", TokenType::Identifier),
                "name".to_string(),
            ))],
            is_distinct: false,
            order_by: vec![],
            filter: None,
        }));
        assert!(!expression_contains_aggregate(&expr));
    }

    #[test]
    fn test_expression_contains_aggregate_identifier() {
        let expr = Expression::Identifier(Identifier::new(
            dummy_token("col", TokenType::Identifier),
            "col".to_string(),
        ));
        assert!(!expression_contains_aggregate(&expr));
    }

    // ============================================================================
    // Extract Column Name With Qualifier Tests
    // ============================================================================

    #[test]
    fn test_extract_column_name_with_qualifier_simple() {
        let expr = Expression::Identifier(Identifier::new(
            dummy_token("col", TokenType::Identifier),
            "col".to_string(),
        ));
        assert_eq!(
            extract_column_name_with_qualifier(&expr),
            Some((None, "col".to_string()))
        );
    }

    #[test]
    fn test_extract_column_name_with_qualifier_qualified() {
        let expr = Expression::QualifiedIdentifier(QualifiedIdentifier {
            token: dummy_token("t.col", TokenType::Identifier),
            qualifier: Box::new(Identifier::new(
                dummy_token("t", TokenType::Identifier),
                "t".to_string(),
            )),
            intermediate: None,
            name: Box::new(Identifier::new(
                dummy_token("col", TokenType::Identifier),
                "col".to_string(),
            )),
        });
        let result = extract_column_name_with_qualifier(&expr);
        assert!(result.is_some());
        let (qual, name) = result.unwrap();
        assert_eq!(qual, Some("t".to_string()));
        assert_eq!(name, "col");
    }

    // ============================================================================
    // String to DataType Tests
    // ============================================================================

    #[test]
    fn test_string_to_datatype() {
        assert_eq!(string_to_datatype("INTEGER"), DataType::Integer);
        assert_eq!(string_to_datatype("int"), DataType::Integer);
        assert_eq!(string_to_datatype("BIGINT"), DataType::Integer);
        assert_eq!(string_to_datatype("SMALLINT"), DataType::Integer);
        assert_eq!(string_to_datatype("TINYINT"), DataType::Integer);
        assert_eq!(string_to_datatype("FLOAT"), DataType::Float);
        assert_eq!(string_to_datatype("DOUBLE"), DataType::Float);
        assert_eq!(string_to_datatype("DECIMAL(10,2)"), DataType::Decimal);
        assert_eq!(string_to_datatype("NUMERIC(12)"), DataType::Decimal);
        assert_eq!(string_to_datatype("TEXT"), DataType::Text);
        assert_eq!(string_to_datatype("VARCHAR"), DataType::Text);
        assert_eq!(string_to_datatype("VARCHAR(255)"), DataType::Text);
        assert_eq!(string_to_datatype("CHAR(8)"), DataType::Text);
        assert_eq!(string_to_datatype("CLOB"), DataType::Text);
        assert_eq!(string_to_datatype("BOOLEAN"), DataType::Boolean);
        assert_eq!(string_to_datatype("TIMESTAMP"), DataType::Timestamp);
        assert_eq!(string_to_datatype("DATE"), DataType::Date);
        assert_eq!(string_to_datatype("TIME"), DataType::Timestamp);
        assert_eq!(string_to_datatype("JSON"), DataType::Json);
        assert_eq!(string_to_datatype("JSONB"), DataType::Json);
        assert_eq!(string_to_datatype("UUID"), DataType::Uuid);
        assert_eq!(string_to_datatype("VECTOR(3)"), DataType::Vector);
        assert_eq!(string_to_datatype("BYTES"), DataType::Bytes);
        assert_eq!(string_to_datatype("BLOB"), DataType::Bytes);
        assert_eq!(string_to_datatype("BINARY"), DataType::Bytes);
        assert_eq!(string_to_datatype("VARBINARY"), DataType::Bytes);
        assert_eq!(string_to_datatype("unknown"), DataType::Text);
    }

    // ============================================================================
    // Create Column Identifier Tests
    // ============================================================================

    #[test]
    fn test_create_column_identifier_simple() {
        let expr = create_column_identifier("col");
        match expr {
            Expression::Identifier(id) => assert_eq!(id.value, "col"),
            _ => panic!("Expected Identifier"),
        }
    }

    #[test]
    fn test_create_column_identifier_qualified() {
        let expr = create_column_identifier("t.col");
        match expr {
            Expression::QualifiedIdentifier(qid) => {
                assert_eq!(qid.qualifier.value, "t");
                assert_eq!(qid.name.value, "col");
            }
            _ => panic!("Expected QualifiedIdentifier"),
        }
    }

    // ============================================================================
    // Expression Has Parameters Tests
    // ============================================================================

    #[test]
    fn test_expression_has_parameters_true() {
        use radixdb_sql::ast::Parameter;
        let expr = Expression::Parameter(Parameter {
            token: dummy_token("$1", TokenType::Parameter),
            name: "$1".into(),
            index: 1,
            field: None,
        });
        assert!(expression_has_parameters(&expr));
    }

    #[test]
    fn test_expression_has_parameters_false() {
        let expr = Expression::Identifier(Identifier::new(
            dummy_token("col", TokenType::Identifier),
            "col".to_string(),
        ));
        assert!(!expression_has_parameters(&expr));
    }

    #[test]
    fn test_expression_has_parameters_infix() {
        use radixdb_sql::ast::Parameter;
        let left = Expression::Identifier(Identifier::new(
            dummy_token("col", TokenType::Identifier),
            "col".to_string(),
        ));
        let right = Expression::Parameter(Parameter {
            token: dummy_token("$1", TokenType::Parameter),
            name: "$1".into(),
            index: 1,
            field: None,
        });
        let expr = Expression::Infix(InfixExpression::new(
            dummy_token("=", TokenType::Operator),
            Box::new(left),
            "=",
            Box::new(right),
        ));
        assert!(expression_has_parameters(&expr));
    }

    // ============================================================================
    // Expressions Equivalent Tests
    // ============================================================================

    #[test]
    fn test_expressions_equivalent_identifiers() {
        let a = Expression::Identifier(Identifier::new(
            dummy_token("col", TokenType::Identifier),
            "col".to_string(),
        ));
        let b = Expression::Identifier(Identifier::new(
            dummy_token("COL", TokenType::Identifier),
            "COL".to_string(),
        ));
        assert!(expressions_equivalent(&a, &b));
    }

    #[test]
    fn test_expressions_equivalent_integers() {
        let a = Expression::IntegerLiteral(IntegerLiteral {
            token: dummy_token("42", TokenType::Integer),
            value: 42,
        });
        let b = Expression::IntegerLiteral(IntegerLiteral {
            token: dummy_token("42", TokenType::Integer),
            value: 42,
        });
        assert!(expressions_equivalent(&a, &b));
    }

    #[test]
    fn test_expressions_equivalent_different() {
        let a = Expression::IntegerLiteral(IntegerLiteral {
            token: dummy_token("42", TokenType::Integer),
            value: 42,
        });
        let b = Expression::IntegerLiteral(IntegerLiteral {
            token: dummy_token("43", TokenType::Integer),
            value: 43,
        });
        assert!(!expressions_equivalent(&a, &b));
    }

    // ============================================================================
    // Flatten AND Predicates Tests
    // ============================================================================

    #[test]
    fn test_flatten_and_predicates_single() {
        let expr = Expression::Identifier(Identifier::new(
            dummy_token("col", TokenType::Identifier),
            "col".to_string(),
        ));
        let result = flatten_and_predicates(&expr);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_flatten_and_predicates_multiple() {
        let a = Expression::Identifier(Identifier::new(
            dummy_token("a", TokenType::Identifier),
            "a".to_string(),
        ));
        let b = Expression::Identifier(Identifier::new(
            dummy_token("b", TokenType::Identifier),
            "b".to_string(),
        ));
        let expr = Expression::Infix(InfixExpression::new(
            dummy_token("AND", TokenType::Keyword),
            Box::new(a),
            "AND".to_string(),
            Box::new(b),
        ));
        let result = flatten_and_predicates(&expr);
        assert_eq!(result.len(), 2);
    }

    // ============================================================================
    // Combine Predicates With AND Tests
    // ============================================================================

    #[test]
    fn test_combine_predicates_empty() {
        let result = combine_predicates_with_and(vec![]);
        assert!(result.is_none());
    }

    #[test]
    fn test_combine_predicates_single() {
        let expr = Expression::Identifier(Identifier::new(
            dummy_token("col", TokenType::Identifier),
            "col".to_string(),
        ));
        let result = combine_predicates_with_and(vec![expr.clone()]);
        assert!(result.is_some());
    }

    #[test]
    fn test_combine_predicates_multiple() {
        let a = Expression::Identifier(Identifier::new(
            dummy_token("a", TokenType::Identifier),
            "a".to_string(),
        ));
        let b = Expression::Identifier(Identifier::new(
            dummy_token("b", TokenType::Identifier),
            "b".to_string(),
        ));
        let result = combine_predicates_with_and(vec![a, b]);
        assert!(result.is_some());
        if let Some(Expression::Infix(infix)) = result {
            assert_eq!(infix.operator.to_uppercase(), "AND");
        } else {
            panic!("Expected Infix expression");
        }
    }

    // ============================================================================
    // Extract AND Conditions Tests
    // ============================================================================

    #[test]
    fn test_extract_and_conditions_single() {
        let expr = Expression::Identifier(Identifier::new(
            dummy_token("col", TokenType::Identifier),
            "col".to_string(),
        ));
        let result = extract_and_conditions(&expr);
        assert_eq!(result.len(), 1);
    }

    // ============================================================================
    // Collect Table Qualifiers Tests
    // ============================================================================

    #[test]
    fn test_collect_table_qualifiers_empty() {
        let expr = Expression::Identifier(Identifier::new(
            dummy_token("col", TokenType::Identifier),
            "col".to_string(),
        ));
        let result = collect_table_qualifiers(&expr);
        assert!(result.is_empty());
    }

    #[test]
    fn test_collect_table_qualifiers_qualified() {
        let expr = Expression::QualifiedIdentifier(QualifiedIdentifier {
            token: dummy_token("t.col", TokenType::Identifier),
            qualifier: Box::new(Identifier::new(
                dummy_token("t", TokenType::Identifier),
                "t".to_string(),
            )),
            intermediate: None,
            name: Box::new(Identifier::new(
                dummy_token("col", TokenType::Identifier),
                "col".to_string(),
            )),
        });
        let result = collect_table_qualifiers(&expr);
        assert_eq!(result.len(), 1);
        assert!(result.contains("t"));
    }

    // ============================================================================
    // Strip Table Qualifier Tests
    // ============================================================================

    #[test]
    fn test_strip_table_qualifier_qualified() {
        let expr = Expression::QualifiedIdentifier(QualifiedIdentifier {
            token: dummy_token("t.col", TokenType::Identifier),
            qualifier: Box::new(Identifier::new(
                dummy_token("t", TokenType::Identifier),
                "t".to_string(),
            )),
            intermediate: None,
            name: Box::new(Identifier::new(
                dummy_token("col", TokenType::Identifier),
                "col".to_string(),
            )),
        });
        let result = strip_table_qualifier(&expr, "t");
        match result {
            Expression::Identifier(id) => assert_eq!(id.value, "col"),
            _ => panic!("Expected Identifier"),
        }
    }

    #[test]
    fn test_strip_table_qualifier_unqualified() {
        let expr = Expression::Identifier(Identifier::new(
            dummy_token("col", TokenType::Identifier),
            "col".to_string(),
        ));
        let result = strip_table_qualifier(&expr, "t");
        match result {
            Expression::Identifier(id) => assert_eq!(id.value, "col"),
            _ => panic!("Expected Identifier"),
        }
    }

    // ============================================================================
    // Add Table Qualifier Tests
    // ============================================================================

    #[test]
    fn test_add_table_qualifier_simple() {
        let expr = Expression::Identifier(Identifier::new(
            dummy_token("col", TokenType::Identifier),
            "col".to_string(),
        ));
        let result = add_table_qualifier(&expr, "t");
        match result {
            Expression::QualifiedIdentifier(qi) => {
                assert_eq!(qi.qualifier.value, "t");
                assert_eq!(qi.name.value, "col");
            }
            _ => panic!("Expected QualifiedIdentifier"),
        }
    }

    // ============================================================================
    // Expression To String Tests
    // ============================================================================

    #[test]
    fn test_expression_to_string_identifier() {
        let expr = Expression::Identifier(Identifier::new(
            dummy_token("col", TokenType::Identifier),
            "col".to_string(),
        ));
        let result = expression_to_string(&expr);
        assert_eq!(result, "col");
    }

    #[test]
    fn test_expression_to_string_qualified() {
        let expr = Expression::QualifiedIdentifier(QualifiedIdentifier {
            token: dummy_token("t.col", TokenType::Identifier),
            qualifier: Box::new(Identifier::new(
                dummy_token("t", TokenType::Identifier),
                "t".to_string(),
            )),
            intermediate: None,
            name: Box::new(Identifier::new(
                dummy_token("col", TokenType::Identifier),
                "col".to_string(),
            )),
        });
        let result = expression_to_string(&expr);
        assert_eq!(result, "t.col");
    }

    #[test]
    fn test_expression_to_string_literal() {
        let expr = Expression::IntegerLiteral(IntegerLiteral {
            token: dummy_token("42", TokenType::Integer),
            value: 42,
        });
        let result = expression_to_string(&expr);
        assert_eq!(result, "42");
    }

    // ============================================================================
    // Filter References Column Tests
    // ============================================================================

    #[test]
    fn test_filter_references_column_infix() {
        // Test with infix expression: target = 1
        let left = Expression::Identifier(Identifier::new(
            dummy_token("target", TokenType::Identifier),
            "target".to_string(),
        ));
        let right = Expression::IntegerLiteral(IntegerLiteral {
            token: dummy_token("1", TokenType::Integer),
            value: 1,
        });
        let expr = Expression::Infix(InfixExpression::new(
            dummy_token("=", TokenType::Operator),
            Box::new(left),
            "=".to_string(),
            Box::new(right),
        ));
        assert!(filter_references_column(&expr, "target"));
    }

    #[test]
    fn test_filter_references_column_no_match() {
        // Test with infix expression that doesn't reference target
        let left = Expression::Identifier(Identifier::new(
            dummy_token("other", TokenType::Identifier),
            "other".to_string(),
        ));
        let right = Expression::IntegerLiteral(IntegerLiteral {
            token: dummy_token("1", TokenType::Integer),
            value: 1,
        });
        let expr = Expression::Infix(InfixExpression::new(
            dummy_token("=", TokenType::Operator),
            Box::new(left),
            "=".to_string(),
            Box::new(right),
        ));
        assert!(!filter_references_column(&expr, "target"));
    }

    #[test]
    fn test_filter_references_column_simple_identifier() {
        // Simple identifiers without a comparison operator return false
        let expr = Expression::Identifier(Identifier::new(
            dummy_token("target", TokenType::Identifier),
            "target".to_string(),
        ));
        // The function is for filter expressions, not simple identifiers
        assert!(!filter_references_column(&expr, "target"));
    }

    // ============================================================================
    // Hash Value Into Tests (detailed)
    // ============================================================================

    #[test]
    fn test_hash_value_into_null() {
        use radixdb_core::types::DataType;
        use std::hash::DefaultHasher;
        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        hash_value_into(&Value::Null(DataType::Integer), &mut hasher1);
        hash_value_into(&Value::Null(DataType::Integer), &mut hasher2);
        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_hash_value_into_different_types() {
        use std::hash::DefaultHasher;
        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        hash_value_into(&Value::Integer(42), &mut hasher1);
        hash_value_into(&Value::Text("42".into()), &mut hasher2);
        // Different types should produce different hashes
        assert_ne!(hasher1.finish(), hasher2.finish());
    }
}
