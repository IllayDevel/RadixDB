use std::sync::Arc;

use radixdb_catalog::{CatalogObject, ObjectId, ObjectKind};
use radixdb_core::Value;
use radixdb_procedural::{PrincipalContext, ProceduralResult, RuntimeValue};
use radixdb_storage::mvcc::engine::MVCCEngine;

use crate::{ExecutionContext, Executor};

use super::ProceduralResultStage;

#[derive(Default)]
struct TestStage;

impl ProceduralResultStage for TestStage {
    fn begin(&mut self) -> ProceduralResult<()> {
        Ok(())
    }

    fn stage_row(&mut self, _row: Vec<RuntimeValue>) -> ProceduralResult<()> {
        Ok(())
    }

    fn publish(&mut self) {}

    fn discard(&mut self) {}
}

fn executor() -> Executor {
    let engine = MVCCEngine::in_memory();
    engine.open_engine().unwrap();
    Executor::new(Arc::new(engine))
}

fn routine(executor: &Executor, name: &str) -> Option<CatalogObject> {
    executor
        .engine
        .pin_catalog()
        .unwrap()
        .find_routine(
            ObjectId::BOOTSTRAP_NAMESPACE,
            ObjectKind::Procedure,
            name,
            &[],
        )
        .unwrap()
        .cloned()
}

fn principals() -> PrincipalContext {
    PrincipalContext {
        session_principal: ObjectId::BOOTSTRAP_OWNER,
        invoker_principal: ObjectId::BOOTSTRAP_OWNER,
        effective_principal: ObjectId::BOOTSTRAP_OWNER,
    }
}

fn output(executor: &Executor, caller: &str) -> Vec<RuntimeValue> {
    executor
        .execute_procedure(
            routine(executor, caller).unwrap().id(),
            Vec::new(),
            &ExecutionContext::new(),
            principals(),
            &mut TestStage,
        )
        .unwrap()
        .execution()
        .output_values
        .clone()
}

#[test]
fn nested_call_resolves_named_arguments_and_left_to_right_defaults() {
    let executor = executor();
    executor
        .execute(
            "CREATE PROCEDURE add_default( \
                 IN first_value INTEGER NOT NULL, \
                 IN second_value INTEGER NOT NULL DEFAULT first_value + 1, \
                 OUT output_value INTEGER NOT NULL \
             ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 output_value := first_value + second_value; \
             END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE named_default_caller(OUT output_value INTEGER NOT NULL) \
             LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 CALL add_default(first_value => 10, output_value => output_value); \
             END;",
        )
        .unwrap();
    assert_eq!(
        output(&executor, "named_default_caller"),
        vec![RuntimeValue::scalar(Value::Integer(21))]
    );
}

#[test]
fn routine_default_cannot_reference_a_later_argument() {
    let executor = executor();
    let error = match executor.execute(
        "CREATE PROCEDURE invalid_default( \
             IN first_value INTEGER DEFAULT later_value, \
             IN later_value INTEGER DEFAULT 1 \
         ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN RETURN; END;",
    ) {
        Ok(_) => panic!("forward argument reference unexpectedly passed DDL admission"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("procedural compilation failed"));
    assert!(routine(&executor, "invalid_default").is_none());
}

#[test]
fn procedure_overload_prefers_exact_match_over_lossless_conversion() {
    let executor = executor();
    executor
        .execute(
            "CREATE PROCEDURE choose_numeric( \
                 IN input_value DECIMAL NOT NULL, OUT output_value INTEGER NOT NULL \
             ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN output_value := 2; END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE choose_numeric( \
                 IN input_value INTEGER NOT NULL, OUT output_value INTEGER NOT NULL \
             ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN output_value := 1; END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE exact_numeric_caller(OUT output_value INTEGER NOT NULL) \
             LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 CALL choose_numeric(7, output_value); \
             END;",
        )
        .unwrap();
    assert_eq!(
        output(&executor, "exact_numeric_caller"),
        vec![RuntimeValue::scalar(Value::Integer(1))]
    );
}

#[test]
fn procedure_call_applies_checked_lossless_conversions() {
    let executor = executor();
    executor
        .execute(
            "CREATE PROCEDURE decimal_only( \
                 IN input_value DECIMAL NOT NULL, OUT output_value INTEGER NOT NULL \
             ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN output_value := 7; END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE timestamp_only( \
                 IN input_value TIMESTAMP NOT NULL, OUT output_value INTEGER NOT NULL \
             ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN output_value := 8; END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE widening_caller(OUT output_value INTEGER NOT NULL) \
             LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 CALL decimal_only(11, output_value); \
             END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE date_widening_caller(OUT output_value INTEGER NOT NULL) \
             LANGUAGE RADIX SECURITY INVOKER AS \
             DECLARE input_value DATE NOT NULL := DATE '2026-09-07'; \
             BEGIN CALL timestamp_only(input_value, output_value); END;",
        )
        .unwrap();
    assert_eq!(
        output(&executor, "widening_caller"),
        vec![RuntimeValue::scalar(Value::Integer(7))]
    );
    assert_eq!(
        output(&executor, "date_widening_caller"),
        vec![RuntimeValue::scalar(Value::Integer(8))]
    );
}

#[test]
fn procedure_call_rejects_non_lossless_implicit_conversion() {
    let executor = executor();
    executor
        .execute(
            "CREATE PROCEDURE float_only(IN input_value FLOAT) \
             LANGUAGE RADIX SECURITY INVOKER AS BEGIN RETURN; END;",
        )
        .unwrap();
    let error = match executor.execute(
        "CREATE PROCEDURE invalid_float_caller() \
         LANGUAGE RADIX SECURITY INVOKER AS BEGIN CALL float_only(1); END;",
    ) {
        Ok(_) => panic!("INTEGER to FLOAT call unexpectedly passed DDL admission"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("procedural compilation failed"));
}

#[test]
fn equal_cost_procedure_overloads_are_ambiguous() {
    let executor = executor();
    for ddl in [
        "CREATE PROCEDURE equal_cost(IN left_value DECIMAL, IN right_value INTEGER) \
         LANGUAGE RADIX SECURITY INVOKER AS BEGIN RETURN; END;",
        "CREATE PROCEDURE equal_cost(IN left_value INTEGER, IN right_value DECIMAL) \
         LANGUAGE RADIX SECURITY INVOKER AS BEGIN RETURN; END;",
    ] {
        executor.execute(ddl).unwrap();
    }
    let error = match executor.execute(
        "CREATE PROCEDURE ambiguous_cost_caller() \
         LANGUAGE RADIX SECURITY INVOKER AS BEGIN CALL equal_cost(1, 2); END;",
    ) {
        Ok(_) => panic!("equal-cost overload call unexpectedly passed DDL admission"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("procedural compilation failed"));
    assert!(routine(&executor, "ambiguous_cost_caller").is_none());
}

#[test]
fn untyped_null_is_ambiguous_but_typed_null_selects_an_overload() {
    let executor = executor();
    for ddl in [
        "CREATE PROCEDURE nullable_choice( \
             IN input_value INTEGER, OUT output_value INTEGER NOT NULL \
         ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN output_value := 1; END;",
        "CREATE PROCEDURE nullable_choice( \
             IN input_value TEXT, OUT output_value INTEGER NOT NULL \
         ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN output_value := 2; END;",
    ] {
        executor.execute(ddl).unwrap();
    }
    let error = match executor.execute(
        "CREATE PROCEDURE ambiguous_null_caller(OUT output_value INTEGER NOT NULL) \
         LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
             CALL nullable_choice(NULL, output_value); \
         END;",
    ) {
        Ok(_) => panic!("untyped NULL overload call unexpectedly passed DDL admission"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("procedural compilation failed"));

    executor
        .execute(
            "CREATE PROCEDURE typed_null_caller(OUT output_value INTEGER NOT NULL) \
             LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 CALL nullable_choice(CAST(NULL AS INTEGER), output_value); \
             END;",
        )
        .unwrap();
    assert_eq!(
        output(&executor, "typed_null_caller"),
        vec![RuntimeValue::scalar(Value::Integer(1))]
    );
}

#[test]
fn outer_call_resolves_named_defaults_and_returns_out_values() {
    let executor = executor();
    executor
        .execute(
            "CREATE PROCEDURE add_default( \
                 IN first_value INTEGER NOT NULL, \
                 IN second_value INTEGER NOT NULL DEFAULT first_value + 1, \
                 OUT output_value INTEGER NOT NULL \
             ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 output_value := first_value + second_value; \
             END;",
        )
        .unwrap();

    let mut result = executor
        .execute("CALL add_default(first_value => 10)")
        .unwrap();
    assert_eq!(result.columns(), &["output_value"]);
    assert!(result.next());
    assert_eq!(result.row()[0], Value::Integer(21));
    assert!(!result.next());
    assert!(result.last_error().is_none());
}

#[test]
fn outer_call_prefers_exact_overload_and_dynamic_call_shares_transaction() {
    let executor = executor();
    executor
        .execute("CREATE TABLE call_effects (value INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE write_choice( \
                 IN input_value DECIMAL NOT NULL, OUT output_value INTEGER NOT NULL \
             ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN output_value := 2; END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE write_choice( \
                 IN input_value INTEGER NOT NULL, OUT output_value INTEGER NOT NULL \
             ) LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 INSERT INTO call_effects VALUES (:input_value); output_value := 1; \
             END;",
        )
        .unwrap();

    let mut exact = executor.execute("CALL write_choice(7)").unwrap();
    assert!(exact.next());
    assert_eq!(exact.row()[0], Value::Integer(1));
    assert!(!exact.next());

    executor
        .execute(
            "CREATE PROCEDURE dynamic_caller(input_value INTEGER NOT NULL) \
             LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
                 EXECUTE 'CALL write_choice(?)' USING input_value; \
             END;",
        )
        .unwrap();
    executor.execute("CALL dynamic_caller(8)").unwrap();

    let mut count = executor
        .execute("SELECT COUNT(*) FROM call_effects")
        .unwrap();
    assert!(count.next());
    assert_eq!(count.row()[0], Value::Integer(2));
}

#[test]
fn outer_call_rolls_back_argument_effects_when_procedure_fails() {
    let executor = executor();
    executor
        .execute("CREATE TABLE call_argument_effects (value INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute(
            "CREATE FUNCTION write_argument(input_value INTEGER NOT NULL) \
             RETURNS INTEGER NOT NULL LANGUAGE RADIX VOLATILE SECURITY INVOKER AS \
             BEGIN INSERT INTO call_argument_effects VALUES (:input_value); \
             RETURN input_value; END;",
        )
        .unwrap();
    executor
        .execute(
            "CREATE PROCEDURE duplicate_argument(input_value INTEGER NOT NULL) \
             LANGUAGE RADIX SECURITY INVOKER AS BEGIN \
             INSERT INTO call_argument_effects VALUES (:input_value); END;",
        )
        .unwrap();

    let error = match executor.execute("CALL duplicate_argument(write_argument(5))") {
        Ok(_) => panic!("failing CALL unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(!error.to_string().is_empty());

    let mut count = executor
        .execute("SELECT COUNT(*) FROM call_argument_effects")
        .unwrap();
    assert!(count.next());
    assert_eq!(count.row()[0], Value::Integer(0));
}
