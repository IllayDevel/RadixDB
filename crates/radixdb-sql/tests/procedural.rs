// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use proptest::prelude::*;
use radixdb_sql::{parse_sql, Lexer, ProceduralStatement, Statement, TokenType};

fn parse_routine(source: &str) -> radixdb_sql::CreateRoutineStatement {
    let statements = parse_sql(source).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(statements.len(), 1);
    match statements.into_iter().next().unwrap() {
        Statement::CreateRoutine(statement) => *statement,
        statement => panic!("expected routine definition, got {statement:?}"),
    }
}

#[test]
fn longest_match_procedural_operators() {
    let mut lexer = Lexer::new(":name := = => > >=");
    let expected = [
        (TokenType::Parameter, ":name"),
        (TokenType::Operator, ":="),
        (TokenType::Operator, "="),
        (TokenType::Operator, "=>"),
        (TokenType::Operator, ">"),
        (TokenType::Operator, ">="),
    ];
    for (token_type, literal) in expected {
        let token = lexer.next_token();
        assert_eq!(token.token_type, token_type);
        assert_eq!(token.literal, literal);
    }
}

#[test]
fn parses_function_and_preserves_typed_sql_expression() {
    let routine = parse_routine(
        "CREATE FUNCTION pricing.line_total(quantity DECIMAL, unit_price DECIMAL) \
         RETURNS DECIMAL LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS \
         BEGIN RETURN quantity * unit_price; END;",
    );
    assert_eq!(routine.arguments.len(), 2);
    assert!(matches!(
        routine.body.as_ref().unwrap().statements.as_slice(),
        [ProceduralStatement::Return { .. }]
    ));
}

#[test]
fn parses_control_flow_nested_block_and_exception_handlers() {
    let routine = parse_routine(
        "CREATE PROCEDURE app.exercise(value INTEGER) LANGUAGE RADIX SECURITY DEFINER AS
         DECLARE total INTEGER := 0;
         BEGIN
           IF value IS NULL THEN RAISE invalid_argument('missing');
           ELSIF value < 0 THEN RAISE invalid_argument('negative');
           ELSE total := value; END IF;
           CASE total WHEN 0 THEN total := 1; ELSE total := total; END CASE;
           FOR item IN 1 TO 10 BY 2 LOOP CONTINUE WHEN item = 5; END LOOP;
           WHILE total < 20 LOOP total := total + 1; END LOOP;
           BEGIN PERFORM audit_note('done');
           EXCEPTION WHEN no_data_found OR too_many_rows THEN RAISE;
                     WHEN OTHERS AS caught_error THEN PERFORM audit_capture_error(caught_error);
           END;
         END;",
    );
    assert_eq!(routine.body.as_ref().unwrap().declarations.len(), 1);
    assert_eq!(routine.body.as_ref().unwrap().statements.len(), 5);
}

#[test]
fn parses_static_sql_dynamic_sql_and_cursor_contracts() {
    let routine = parse_routine(
        "CREATE PROCEDURE app.exercise(document_id UUID) LANGUAGE RADIX SECURITY INVOKER AS
         DECLARE
           CURSOR route_cursor(requested_status TEXT) FOR SELECT id FROM route WHERE status = :requested_status;
           found_id UUID;
         BEGIN
           SELECT id INTO STRICT found_id FROM route WHERE id = :document_id;
           UPDATE route SET touched = TRUE WHERE id = :document_id RETURNING id INTO found_id;
           EXECUTE 'DELETE FROM ' || table_name INTO STRICT found_id USING document_id;
           OPEN route_cursor('ready'); FETCH route_cursor INTO found_id; CLOSE route_cursor;
           FOR route_row IN (SELECT id FROM route LIMIT 10) LOOP EXIT; END LOOP;
         END;",
    );
    assert_eq!(routine.body.as_ref().unwrap().declarations.len(), 2);
    assert_eq!(routine.body.as_ref().unwrap().statements.len(), 7);
}

#[test]
fn parses_record_field_as_one_named_sql_parameter_leaf() {
    let routine = parse_routine(
        "CREATE FUNCTION audit_new() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
         SECURITY INVOKER AS BEGIN \
         INSERT INTO audit_rows VALUES (:NEW.id, :OLD.\"Quoted Value\"); \
         RETURN NEW; END;",
    );
    let ProceduralStatement::Sql(sql) = &routine.body.as_ref().unwrap().statements[0] else {
        panic!("expected embedded SQL leaf")
    };
    let rendered = sql.statement.to_string();
    assert!(rendered.contains(":NEW.id"), "{rendered}");
    assert!(rendered.contains(":OLD.\"Quoted Value\""), "{rendered}");
    assert!(parse_sql(&format!("{routine};")).is_ok());

    assert!(parse_sql(
        "CREATE FUNCTION bad() RETURNS TRIGGER LANGUAGE RADIX VOLATILE \
         SECURITY INVOKER AS BEGIN PERFORM :NEW.id.more; RETURN NEW; END;"
    )
    .is_err());
}

#[test]
fn rejects_transaction_control_and_external_parameters() {
    for source in [
        "CREATE PROCEDURE app.p() LANGUAGE RADIX SECURITY INVOKER AS BEGIN COMMIT; END;",
        "CREATE FUNCTION app.f() RETURNS INTEGER LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS BEGIN RETURN $1; END;",
        "CREATE PROCEDURE app.p() LANGUAGE RADIX SECURITY INVOKER AS BEGIN SELECT ?; END;",
    ] {
        assert!(parse_sql(source).is_err(), "accepted {source}");
    }
}

#[test]
fn normalizes_authority_source_and_requires_definition_terminator() {
    let source = "CREATE FUNCTION app.привет()\r\nRETURNS INTEGER\rLANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS\r\nBEGIN RETURN 1; END;";
    let routine = parse_routine(source);
    assert!(!routine.normalized_source.contains('\r'));
    assert!(routine.normalized_source.starts_with("CREATE FUNCTION"));
    assert!(routine.normalized_source.ends_with("END;"));

    assert!(parse_sql("\u{feff}SELECT 1").is_err());
    assert!(parse_sql(
        "CREATE PROCEDURE app.p() LANGUAGE RADIX SECURITY INVOKER AS BEGIN RETURN; END"
    )
    .is_err());
}

#[test]
fn rejects_unquoted_procedural_keywords_as_names() {
    assert!(parse_sql(
        "CREATE PROCEDURE app.p() LANGUAGE RADIX SECURITY INVOKER AS DECLARE loop INTEGER; BEGIN RETURN; END;"
    )
    .is_err());
    let quoted = parse_sql(
        "CREATE PROCEDURE app.p() LANGUAGE RADIX SECURITY INVOKER AS DECLARE \"loop\" INTEGER; BEGIN \"loop\" := 1; END;"
    );
    assert!(quoted.is_ok(), "{quoted:?}");
}

#[test]
fn parses_cte_select_into_and_formats_canonical_source_idempotently() {
    let routine = parse_routine(
        "CREATE PROCEDURE app.p(id INTEGER) LANGUAGE RADIX SECURITY INVOKER AS
         DECLARE found_id INTEGER;
         BEGIN
           WITH candidate AS (SELECT id FROM route WHERE id = :id)
           SELECT id INTO STRICT found_id FROM candidate;
         END;",
    );
    let formatted = format!("{routine};");
    assert!(
        formatted.contains("SELECT id INTO STRICT found_id FROM candidate"),
        "{formatted}"
    );
    let reparsed = parse_routine(&formatted);
    assert_eq!(format!("{reparsed};"), formatted);
}

#[test]
fn rejects_oversized_string_literal_before_ast_construction() {
    let payload = "x".repeat(8 * 1024 * 1024 + 1);
    let source = format!("SELECT '{payload}'");
    let error = parse_sql(&source).unwrap_err().to_string();
    assert!(error.contains("string literal exceeds limit"), "{error}");
}

#[test]
fn parses_trigger_metadata_as_distinct_entrypoint() {
    let source = "CREATE OR REPLACE TRIGGER route_touch BEFORE UPDATE OF status, version OR INSERT
                  ON erp.route_document FOR EACH ROW PRIORITY -20
                  WHEN (OLD.status <> NEW.status)
                  EXECUTE FUNCTION erp.route_touch_trigger();";
    let statements = parse_sql(source).unwrap_or_else(|error| panic!("{error}"));
    let Statement::CreateTrigger(trigger) = &statements[0] else {
        panic!("expected trigger definition")
    };
    assert_eq!(trigger.events.len(), 2);
    assert_eq!(trigger.priority, -20);
    let formatted = format!("{trigger};");
    assert!(matches!(
        parse_sql(&formatted).unwrap().as_slice(),
        [Statement::CreateTrigger(_)]
    ));
}

#[test]
fn parses_job_metadata_as_procedure_schedule() {
    for source in [
        "CREATE JOB maintenance.expire_sessions SCHEDULE EVERY INTERVAL '5 MINUTE' RUN AS maintenance_worker CALL auth.expire_sessions(batch_size => 1000) ENABLE;",
        "CREATE JOB reports.close_period SCHEDULE AT TIMESTAMP '2026-12-31 23:59:00' RUN AS accounting_worker CALL accounting.close_period(period_id => 202612) DISABLE;",
    ] {
        let statements = parse_sql(source).unwrap_or_else(|error| panic!("{error}"));
        let Statement::CreateJob(job) = &statements[0] else {
            panic!("expected job definition")
        };
        let formatted = format!("{job};");
        assert!(matches!(
            parse_sql(&formatted).unwrap().as_slice(),
            [Statement::CreateJob(_)]
        ));
    }
}

#[test]
fn parses_program_object_lifecycle_with_exact_overload_and_drop_behavior() {
    for source in [
        "DROP FUNCTION IF EXISTS app.calculate(INTEGER, TEXT) CASCADE",
        "DROP PROCEDURE app.refresh(UUID) RESTRICT",
        "DROP TRIGGER IF EXISTS route_touch ON app.route_document CASCADE",
        "DROP JOB IF EXISTS maintenance.expire_sessions RESTRICT",
        "ALTER JOB maintenance.expire_sessions ENABLE",
        "ALTER JOB maintenance.expire_sessions DISABLE",
    ] {
        let statements = parse_sql(source).unwrap_or_else(|error| panic!("{source}: {error}"));
        assert_eq!(statements.len(), 1, "{source}");
        let formatted = statements[0].to_string();
        let reparsed =
            parse_sql(&formatted).unwrap_or_else(|error| panic!("formatted {formatted}: {error}"));
        assert_eq!(reparsed.len(), 1, "{formatted}");
        assert_eq!(reparsed[0].to_string(), formatted);
    }

    let default_restrict = parse_sql("DROP FUNCTION app.calculate(INTEGER)").unwrap();
    assert_eq!(
        default_restrict[0].to_string(),
        "DROP FUNCTION app.calculate(INTEGER) RESTRICT"
    );
    assert!(parse_sql("DROP FUNCTION app.calculate").is_err());
    assert!(parse_sql("ALTER JOB app.j").is_err());
}

#[test]
fn rejects_duplicate_trigger_events_and_non_constant_job_shape() {
    assert!(parse_sql(
        "CREATE TRIGGER t BEFORE UPDATE OR UPDATE ON app.t FOR EACH ROW EXECUTE FUNCTION app.f();"
    )
    .is_err());
    assert!(parse_sql(
        "CREATE JOB app.j SCHEDULE EVERY '5 MINUTE' RUN AS worker CALL app.p() ENABLE;"
    )
    .is_err());
}

#[test]
fn parses_arrays_cursor_attributes_sql_case_and_nested_comments() {
    let source = "CREATE PROCEDURE app.p() LANGUAGE RADIX SECURITY INVOKER AS
                  DECLARE ids ARRAY<UUID, 1024>; route_id UUID;
                  BEGIN
                    /* outer /* nested */ comment */
                    ids.APPEND(route_id);
                    IF ids.COUNT > 0 AND SQL%FOUND THEN
                      route_id := CASE WHEN route_id IS NULL THEN NULL ELSE route_id END;
                    END IF;
                  END;";
    let routine = parse_routine(source);
    assert_eq!(routine.body.as_ref().unwrap().declarations.len(), 2);
    assert!(parse_sql(
        "CREATE PROCEDURE app.p() LANGUAGE RADIX SECURITY INVOKER AS BEGIN /* unterminated END;"
    )
    .is_err());
}

#[test]
fn parses_exact_native_function_contract() {
    let routine = parse_routine(
        "CREATE FUNCTION geo.st_distance(left_point geo.point NOT NULL, \
         right_point geo.point NOT NULL) RETURNS FLOAT NOT NULL \
         LANGUAGE NATIVE FROM EXTENSION radix_spatial AS 'distance';",
    );
    assert!(routine.body.is_none());
    let native = routine.native.as_ref().unwrap();
    assert_eq!(native.extension.to_string(), "radix_spatial");
    assert_eq!(native.local_id.as_str(), "distance");
    let canonical = routine.to_string();
    assert_eq!(
        canonical,
        "CREATE FUNCTION geo.st_distance(left_point geo.point NOT NULL, right_point geo.point NOT NULL) RETURNS FLOAT NOT NULL LANGUAGE NATIVE FROM EXTENSION radix_spatial AS 'distance'"
    );
    assert!(parse_sql(&format!("{canonical};")).is_ok());
}

#[test]
fn rejects_native_function_contract_extensions() {
    for source in [
        "CREATE OR REPLACE FUNCTION geo.f(value INTEGER) RETURNS INTEGER LANGUAGE NATIVE FROM EXTENSION sample AS 'f';",
        "CREATE PROCEDURE geo.f(value INTEGER) LANGUAGE NATIVE FROM EXTENSION sample AS 'f';",
        "CREATE FUNCTION geo.f(value INTEGER) RETURNS TABLE(value INTEGER) LANGUAGE NATIVE FROM EXTENSION sample AS 'f';",
        "CREATE FUNCTION geo.f(value INTEGER) RETURNS INTEGER LANGUAGE NATIVE FROM EXTENSION sample AS '';",
        "CREATE FUNCTION geo.f(value INTEGER) RETURNS INTEGER LANGUAGE NATIVE FROM EXTENSION sample AS 'f' SECURITY INVOKER;",
    ] {
        assert!(parse_sql(source).is_err(), "accepted {source}");
    }
}

#[test]
fn rejects_invalid_signature_and_collection_contracts() {
    for source in [
        "CREATE FUNCTION app.f() LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS BEGIN RETURN 1; END;",
        "CREATE PROCEDURE app.p(OUT value INTEGER DEFAULT 1) LANGUAGE RADIX SECURITY INVOKER AS BEGIN RETURN; END;",
        "CREATE PROCEDURE app.p(value INTEGER DEFAULT 1, required INTEGER) LANGUAGE RADIX SECURITY INVOKER AS BEGIN RETURN; END;",
        "CREATE PROCEDURE app.p(OUT value INTEGER) RETURNS INTEGER LANGUAGE RADIX SECURITY INVOKER AS BEGIN RETURN; END;",
        "CREATE PROCEDURE app.p() LANGUAGE RADIX SECURITY INVOKER AS DECLARE values ARRAY<INTEGER, 0>; BEGIN RETURN; END;",
        "CREATE PROCEDURE app.p() LANGUAGE RADIX SECURITY INVOKER AS DECLARE values ARRAY<INTEGER, 65537>; BEGIN RETURN; END;",
    ] {
        assert!(parse_sql(source).is_err(), "accepted {source}");
    }
}

proptest! {
    #[test]
    fn arbitrary_bounded_source_never_panics(source in any::<Vec<u8>>()) {
        let bounded = &source[..source.len().min(512)];
        if let Ok(source) = std::str::from_utf8(bounded) {
            let _ = parse_sql(source);
        }
    }

    #[test]
    fn canonical_simple_routine_is_parse_format_parse_stable(
        value in -1_000_000i64..=1_000_000,
        nullable in any::<bool>(),
    ) {
        let nullability = if nullable { "" } else { " NOT NULL" };
        let source = format!(
            "CREATE FUNCTION app.f(input INTEGER{nullability}) RETURNS INTEGER LANGUAGE RADIX IMMUTABLE SECURITY INVOKER AS BEGIN RETURN input + {value}; END;"
        );
        let routine = parse_routine(&source);
        let canonical = format!("{routine};");
        let reparsed = parse_routine(&canonical);
        prop_assert_eq!(format!("{reparsed};"), canonical);
    }
}
