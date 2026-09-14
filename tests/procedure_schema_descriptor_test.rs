// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use radixdb::Database;
use radixdb_orm::{
    DataTypeDescriptor, DatabaseDescriptor, RoutineArgumentModeDescriptor, RoutineResultDescriptor,
};

fn descriptor(database: &Database) -> DatabaseDescriptor {
    let json: String = database
        .query_one("DESCRIBE DATABASE FORMAT JSON", ())
        .expect("describe database");
    DatabaseDescriptor::from_json(&json).expect("decode descriptor")
}

#[test]
fn database_descriptor_publishes_canonical_procedure_contracts() {
    let database = Database::open("memory://procedure-schema-descriptor").expect("open database");
    database
        .execute("CREATE SCHEMA app", ())
        .expect("create namespace");
    database
        .execute(
            "CREATE PROCEDURE app.adjust(\
                 document_id UUID NOT NULL, \
                 delta DECIMAL(18, 2) NOT NULL, \
                 allow_negative BOOLEAN DEFAULT FALSE, \
                 OUT changed BOOLEAN NOT NULL\
             ) LANGUAGE RADIX SECURITY INVOKER AS \
             BEGIN changed := delta <> 0; END;",
            (),
        )
        .expect("create procedure");

    let before = descriptor(&database);
    assert_eq!(before.procedures.len(), 1);
    let procedure = &before.procedures[0];
    assert_eq!(procedure.name, "app.adjust");
    assert_eq!(procedure.definition_revision, 1);
    assert_eq!(procedure.language, "radix_pl");
    assert_eq!(procedure.arguments.len(), 4);
    assert_eq!(
        procedure.arguments[0].mode,
        RoutineArgumentModeDescriptor::In
    );
    assert_eq!(
        procedure.arguments[0].data_type,
        Some(DataTypeDescriptor::Uuid)
    );
    assert_eq!(
        procedure.arguments[1].data_type,
        Some(DataTypeDescriptor::Decimal {
            precision: Some(18),
            scale: Some(2),
        })
    );
    assert_eq!(
        procedure.arguments[2].default_expression.as_deref(),
        Some("FALSE")
    );
    assert_eq!(
        procedure.arguments[3].mode,
        RoutineArgumentModeDescriptor::Out
    );
    assert!(matches!(procedure.result, RoutineResultDescriptor::Void));
    assert_eq!(
        procedure.computed_fingerprint().unwrap(),
        procedure.fingerprint
    );

    database
        .execute(
            "CREATE OR REPLACE PROCEDURE app.adjust(\
                 document_id UUID NOT NULL, \
                 delta DECIMAL(18, 2) NOT NULL, \
                 allow_negative BOOLEAN DEFAULT FALSE, \
                 OUT changed BOOLEAN NOT NULL\
             ) LANGUAGE RADIX SECURITY INVOKER AS \
             BEGIN changed := delta >= 0; END;",
            (),
        )
        .expect("replace procedure");
    let after = descriptor(&database);
    assert_eq!(after.procedures[0].definition_revision, 2);
    assert_ne!(after.procedures[0].source_sha256, procedure.source_sha256);
    assert_ne!(after.procedures[0].fingerprint, procedure.fingerprint);
    assert_ne!(after.fingerprint, before.fingerprint);
}
