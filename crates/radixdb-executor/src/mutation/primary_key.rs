use std::collections::BTreeSet;

use radixdb_core::{Error, Result, Schema};
use radixdb_sql::ast::{ColumnConstraint, CreateTableStatement, TableConstraint};

/// Primary-key shape bound from one CREATE TABLE statement.
pub(super) struct CreateTablePrimaryKey {
    table_columns: Option<Vec<String>>,
}

impl CreateTablePrimaryKey {
    pub(super) fn bind(statement: &CreateTableStatement) -> Result<Self> {
        let mut table_columns = None;
        for constraint in &statement.table_constraints {
            let TableConstraint::PrimaryKey(columns) = constraint else {
                continue;
            };
            if columns.is_empty() {
                return Err(Error::InvalidArgument(
                    "PRIMARY KEY must contain at least one column".to_owned(),
                ));
            }
            let mut seen = BTreeSet::new();
            let columns = columns
                .iter()
                .map(|column| column.value_lower.to_string())
                .map(|column| {
                    if seen.insert(column.clone()) {
                        Ok(column)
                    } else {
                        Err(Error::InvalidArgument(format!(
                            "PRIMARY KEY contains duplicate column '{column}'"
                        )))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            if table_columns.replace(columns).is_some() {
                return Err(Error::InvalidArgument(
                    "table declares more than one PRIMARY KEY constraint".to_owned(),
                ));
            }
        }

        let column_primary_keys = statement
            .columns
            .iter()
            .filter(|column| {
                column
                    .constraints
                    .iter()
                    .any(|constraint| matches!(constraint, ColumnConstraint::PrimaryKey))
            })
            .count();
        if column_primary_keys + usize::from(table_columns.is_some()) > 1 {
            return Err(Error::InvalidArgument(
                "table declares more than one PRIMARY KEY".to_owned(),
            ));
        }
        if let Some(columns) = &table_columns {
            for column in columns {
                if !statement
                    .columns
                    .iter()
                    .any(|candidate| candidate.name.value_lower.as_str() == column)
                {
                    return Err(Error::ColumnNotFound(column.clone()));
                }
            }
        }
        Ok(Self { table_columns })
    }

    pub(super) fn contains(&self, column: &str) -> bool {
        self.table_columns
            .as_ref()
            .is_some_and(|columns| columns.iter().any(|candidate| candidate == column))
    }

    pub(super) fn register_in(self, schema: &mut Schema) -> Result<()> {
        let columns = self.table_columns.map_or_else(
            || {
                schema
                    .primary_key_columns()
                    .into_iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>()
            },
            |declared| {
                declared
                    .iter()
                    .map(|name| {
                        schema
                            .get_column_by_name(name)
                            .expect("bound primary-key column exists")
                            .name
                            .clone()
                    })
                    .collect::<Vec<_>>()
            },
        );
        if !columns.is_empty() {
            schema.register_primary_key_constraint(columns)?;
        }
        Ok(())
    }
}
