// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Executor adapter for storage-owned commit-time row validation.

use radixdb_core::{Result, Row, Schema};
use radixdb_storage::validation::PreparedRowValidator;

struct ExecutorRowValidator {
    schema: Schema,
    checks: Vec<(String, crate::expression::SharedProgram)>,
    vm: crate::expression::ExprVM,
}

impl PreparedRowValidator for ExecutorRowValidator {
    fn validate(&mut self, row: &Row) -> Result<()> {
        crate::mutation::validation::validate_resulting_row_constraints(
            &self.schema,
            &self.checks,
            row,
            &mut self.vm,
        )
    }
}

pub fn bind(schema: &Schema) -> Result<Box<dyn PreparedRowValidator>> {
    Ok(Box::new(ExecutorRowValidator {
        schema: schema.clone(),
        checks: crate::mutation::validation::compile_table_check_constraints(schema)?,
        vm: crate::expression::ExprVM::new(),
    }))
}
