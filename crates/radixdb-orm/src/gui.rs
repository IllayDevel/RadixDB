//! Language-neutral migration previews for administration clients.

use serde::{Deserialize, Serialize};

use crate::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MigrationPreview {
    pub preview: String,
    pub ir_json: String,
    pub redacted_ir_json: String,
    pub normalized_sql: String,
    pub parameter_types: Vec<DataTypeDescriptor>,
    pub shape_fingerprint: String,
}

impl MigrationPreview {
    pub fn from_builder(builder: &impl OrmBuilder) -> Result<Self, MigrationPreviewError> {
        let document = builder.document()?;
        let ir_json = document.to_json()?;
        let redacted_ir_json = document.to_redacted_json()?;
        let compiled = document.to_sql()?;
        Ok(Self {
            preview: "radixdb.orm.preview.v1".to_string(),
            ir_json,
            redacted_ir_json,
            normalized_sql: compiled.sql,
            parameter_types: compiled
                .parameters
                .iter()
                .map(TypedValue::data_type)
                .collect(),
            shape_fingerprint: compiled.shape_fingerprint,
        })
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationPreviewError {
    #[error(transparent)]
    Build(#[from] BuilderError),
    #[error(transparent)]
    Ir(#[from] IrError),
    #[error(transparent)]
    Render(#[from] RenderError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preview_separates_redacted_values() {
        let migration =
            DdlBuilder::create_table("secrets").column(Column::text("token").default("do-not-log"));
        let preview = MigrationPreview::from_builder(&migration).unwrap();
        assert!(preview.ir_json.contains("do-not-log"));
        assert!(!preview.redacted_ir_json.contains("do-not-log"));
        assert!(!preview.normalized_sql.contains("do-not-log"));
        assert_eq!(preview.parameter_types, vec![DataTypeDescriptor::Text]);
    }
}
