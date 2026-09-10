// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! SQL-owned binding of durable view dependencies.

use rustc_hash::FxHashSet;

use radixdb_core::{Error, Result};
use radixdb_sql::ast::{self, SelectStatement, Statement};

pub fn bind_from_select(select: &SelectStatement) -> Vec<String> {
    let mut dependencies = FxHashSet::default();
    ast::walk_physical_table_sources(select, &mut |source| {
        dependencies.insert(source.name.value_lower.to_string());
    });
    let mut dependencies: Vec<String> = dependencies.into_iter().collect();
    dependencies.sort_unstable();
    dependencies
}

/// Rebind a persisted view query during recovery without exposing SQL AST to
/// storage. The storage layer receives only the canonical dependency names.
pub fn bind_from_sql(query: &str) -> Result<Vec<String>> {
    let statements = radixdb_sql::parse_sql(query)
        .map_err(|error| Error::parse(format!("invalid persisted view query: {error}")))?;
    let [Statement::Select(select)] = statements.as_slice() else {
        return Err(Error::invalid_argument(
            "view definition must contain exactly one SELECT statement",
        ));
    };
    Ok(bind_from_select(select))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_binding_excludes_cte_aliases_and_normalizes_sources() {
        assert_eq!(
            bind_from_sql(
                "WITH local_rows AS (SELECT * FROM source_rows) \
                 SELECT * FROM local_rows JOIN other_rows \
                 ON local_rows.id = other_rows.id",
            )
            .unwrap(),
            vec!["other_rows", "source_rows"],
        );
    }

    #[test]
    fn persisted_binding_rejects_non_select_payloads() {
        let error = bind_from_sql("DELETE FROM source_rows").unwrap_err();
        assert!(error.to_string().contains("exactly one SELECT statement"));
    }
}
