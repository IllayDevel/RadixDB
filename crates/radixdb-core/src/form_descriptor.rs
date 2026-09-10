//! Language-neutral schema/form projection for administration clients.
//!
//! These models contain schema metadata only. They never evaluate expressions
//! and never contain row values or storage-private state.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    ConstraintDefinition, DataTypeDescriptor, ForeignKeyActionDescriptor, TableDescriptor,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EditorKind {
    Integer,
    Float,
    Decimal,
    Text,
    Boolean,
    Timestamp,
    Date,
    Json,
    Uuid,
    Bytes,
    Vector,
    ReferenceSelect,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceSelector {
    pub target_table: String,
    pub target_column: String,
    pub on_delete: ForeignKeyActionDescriptor,
    pub on_update: ForeignKeyActionDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormFieldDescriptor {
    pub name: String,
    pub ordinal: u32,
    pub data_type: DataTypeDescriptor,
    pub editor: EditorKind,
    pub nullable: bool,
    pub required: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub auto_increment: bool,
    pub read_only: bool,
    pub generated: bool,
    pub default_expression: Option<String>,
    pub checks: Vec<String>,
    pub reference: Option<ReferenceSelector>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableFormDescriptor {
    pub descriptor: String,
    pub table: String,
    pub schema_fingerprint: String,
    pub fields: Vec<FormFieldDescriptor>,
    pub table_checks: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, serde_json::Value>,
}

impl TableFormDescriptor {
    pub fn from_table(table: &TableDescriptor) -> Self {
        let mut primary = BTreeSet::new();
        let mut unique = BTreeSet::new();
        let mut references = BTreeMap::new();
        let mut checks: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut table_checks = Vec::new();

        for constraint in &table.constraints {
            match &constraint.definition {
                ConstraintDefinition::PrimaryKey { columns } => {
                    primary.extend(columns.iter().cloned());
                }
                ConstraintDefinition::Unique { columns, .. } if columns.len() == 1 => {
                    unique.insert(columns[0].clone());
                }
                ConstraintDefinition::Unique { .. } => {}
                ConstraintDefinition::ForeignKey {
                    columns,
                    referenced_table,
                    referenced_columns,
                    on_delete,
                    on_update,
                } if columns.len() == 1 && referenced_columns.len() == 1 => {
                    references.insert(
                        columns[0].clone(),
                        ReferenceSelector {
                            target_table: referenced_table.clone(),
                            target_column: referenced_columns[0].clone(),
                            on_delete: *on_delete,
                            on_update: *on_update,
                        },
                    );
                }
                ConstraintDefinition::ForeignKey { .. } => {}
                ConstraintDefinition::Check {
                    column, expression, ..
                } => match column {
                    Some(column) => checks
                        .entry(column.clone())
                        .or_default()
                        .push(expression.clone()),
                    None => table_checks.push(expression.clone()),
                },
            }
        }

        let fields = table
            .columns
            .iter()
            .map(|column| {
                let reference = references.remove(&column.name);
                let generated = extension_flag(&column.extensions, "generated");
                let read_only = generated || extension_flag(&column.extensions, "read_only");
                FormFieldDescriptor {
                    name: column.name.clone(),
                    ordinal: column.ordinal,
                    data_type: column.data_type.clone(),
                    editor: reference
                        .as_ref()
                        .map(|_| EditorKind::ReferenceSelect)
                        .unwrap_or_else(|| editor_for(&column.data_type)),
                    nullable: column.nullable,
                    required: !column.nullable
                        && !column.auto_increment
                        && column.default_expression.is_none()
                        && !generated,
                    primary_key: primary.contains(&column.name),
                    unique: unique.contains(&column.name) || primary.contains(&column.name),
                    auto_increment: column.auto_increment,
                    read_only,
                    generated,
                    default_expression: column.default_expression.clone(),
                    checks: checks.remove(&column.name).unwrap_or_default(),
                    reference,
                    extensions: gui_extensions(&column.extensions),
                }
            })
            .collect();

        Self {
            descriptor: "radixdb.gui.table-form.v1".to_string(),
            table: table.name.clone(),
            schema_fingerprint: table.fingerprint.clone(),
            fields,
            table_checks,
            extensions: gui_extensions(&table.extensions),
        }
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

fn editor_for(data_type: &DataTypeDescriptor) -> EditorKind {
    match data_type {
        DataTypeDescriptor::Integer => EditorKind::Integer,
        DataTypeDescriptor::Float => EditorKind::Float,
        DataTypeDescriptor::Text => EditorKind::Text,
        DataTypeDescriptor::Boolean => EditorKind::Boolean,
        DataTypeDescriptor::Timestamp => EditorKind::Timestamp,
        DataTypeDescriptor::Date => EditorKind::Date,
        DataTypeDescriptor::Json => EditorKind::Json,
        DataTypeDescriptor::Uuid => EditorKind::Uuid,
        DataTypeDescriptor::Bytes => EditorKind::Bytes,
        DataTypeDescriptor::Decimal { .. } => EditorKind::Decimal,
        DataTypeDescriptor::Vector { .. } => EditorKind::Vector,
        DataTypeDescriptor::Null => EditorKind::Unsupported,
    }
}

fn extension_flag(extensions: &BTreeMap<String, serde_json::Value>, suffix: &str) -> bool {
    extensions
        .get(&format!("radixdb.gui.{suffix}"))
        .or_else(|| extensions.get(&format!("gui.{suffix}")))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn gui_extensions(
    extensions: &BTreeMap<String, serde_json::Value>,
) -> BTreeMap<String, serde_json::Value> {
    extensions
        .iter()
        .filter(|(name, _)| name.starts_with("gui.") || name.starts_with("radixdb.gui."))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnDescriptor, ConstraintDescriptor};

    #[test]
    fn form_projection_contains_only_schema_metadata() {
        let table = TableDescriptor {
            catalog_id: "catalog".to_string(),
            name: "documents".to_string(),
            schema_generation: 1,
            fingerprint: "fingerprint".to_string(),
            created_at: "2026-08-21T00:00:00Z".to_string(),
            updated_at: "2026-08-21T00:00:00Z".to_string(),
            columns: vec![ColumnDescriptor {
                ordinal: 0,
                name: "owner_id".to_string(),
                data_type: DataTypeDescriptor::Uuid,
                nullable: false,
                auto_increment: false,
                default_expression: None,
                extensions: BTreeMap::from([
                    ("radixdb.gui.label".to_string(), serde_json::json!("Owner")),
                    ("private.note".to_string(), serde_json::json!("hidden")),
                ]),
            }],
            constraints: vec![ConstraintDescriptor {
                id: 1,
                name: "fk_documents_owner_id___people".to_string(),
                definition: ConstraintDefinition::ForeignKey {
                    columns: vec!["owner_id".to_string()],
                    referenced_table: "people".to_string(),
                    referenced_columns: vec!["id".to_string()],
                    on_delete: ForeignKeyActionDescriptor::Restrict,
                    on_update: ForeignKeyActionDescriptor::Restrict,
                },
            }],
            indexes: Vec::new(),
            extensions: BTreeMap::new(),
        };

        let form = TableFormDescriptor::from_table(&table);
        assert_eq!(form.fields[0].editor, EditorKind::ReferenceSelect);
        assert!(form.fields[0].required);
        assert_eq!(
            form.fields[0].reference.as_ref().unwrap().target_table,
            "people"
        );
        let json = form.to_json().unwrap();
        assert!(json.contains("Owner"));
        assert!(!json.contains("hidden"));
        assert_eq!(table.form_descriptor(), form);
    }
}
