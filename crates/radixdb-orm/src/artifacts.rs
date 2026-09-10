//! Versioned, language-neutral conformance artifacts.

pub const ORM_IR_JSON_SCHEMA: &str = include_str!("../schemas/radixdb.orm.v1.schema.json");
pub const SCHEMA_DESCRIPTOR_JSON_SCHEMA: &str =
    include_str!("../schemas/radixdb.schema.v1.schema.json");
pub const GUI_TABLE_FORM_JSON_SCHEMA: &str =
    include_str!("../schemas/radixdb.gui.table-form.v1.schema.json");
pub const ORM_CONFORMANCE_MANIFEST: &str =
    include_str!("../schemas/radixdb.orm.v1.conformance.json");
pub const ORM_CONFORMANCE_FIXTURES: &str = include_str!("../schemas/radixdb.orm.v1.fixtures.json");

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn published_artifacts_are_json_and_language_neutral() {
        for artifact in [
            ORM_IR_JSON_SCHEMA,
            SCHEMA_DESCRIPTOR_JSON_SCHEMA,
            GUI_TABLE_FORM_JSON_SCHEMA,
            ORM_CONFORMANCE_MANIFEST,
            ORM_CONFORMANCE_FIXTURES,
        ] {
            serde_json::from_str::<serde_json::Value>(artifact).unwrap();
            assert!(!artifact.contains("std::"));
            assert!(!artifact.contains("radixdb_orm::"));
        }
    }

    #[test]
    fn published_json_schemas_are_valid_and_accept_every_ir_fixture() {
        for artifact in [
            ORM_IR_JSON_SCHEMA,
            SCHEMA_DESCRIPTOR_JSON_SCHEMA,
            GUI_TABLE_FORM_JSON_SCHEMA,
        ] {
            let schema: serde_json::Value = serde_json::from_str(artifact).unwrap();
            if let Err(error) = jsonschema::meta::validate(&schema) {
                panic!("invalid published JSON Schema: {error}");
            }
        }

        let schema: serde_json::Value = serde_json::from_str(ORM_IR_JSON_SCHEMA).unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let fixtures: serde_json::Value = serde_json::from_str(ORM_CONFORMANCE_FIXTURES).unwrap();
        for case in fixtures["cases"].as_array().expect("fixture cases") {
            let id = case["id"].as_str().expect("fixture id");
            for (index, step) in case["steps"]
                .as_array()
                .expect("fixture steps")
                .iter()
                .enumerate()
            {
                let document = &step["document"];
                let errors: Vec<_> = validator
                    .iter_errors(document)
                    .map(|error| error.to_string())
                    .collect();
                assert!(
                    errors.is_empty(),
                    "{id} step {index} violates the public ORM schema: {errors:?}"
                );
            }
        }
    }

    #[test]
    fn conformance_fixtures_compile_to_the_published_sql_and_parameters() {
        let artifact: serde_json::Value = serde_json::from_str(ORM_CONFORMANCE_FIXTURES).unwrap();
        assert_eq!(
            artifact["fixtures"],
            serde_json::Value::String("radixdb.orm.fixtures.v1".to_string())
        );
        let cases = artifact["cases"].as_array().expect("fixture cases");
        let mut fixture_ids = BTreeSet::new();
        for case in cases {
            let id = case["id"].as_str().expect("stable fixture id");
            assert!(fixture_ids.insert(id), "duplicate fixture ID {id}");
            let steps = case["steps"].as_array().expect("fixture steps");
            assert!(!steps.is_empty(), "{id}: no executable steps");
            for (index, step) in steps.iter().enumerate() {
                let document = crate::IrDocument::from_json(
                    &serde_json::to_string(&step["document"]).unwrap(),
                )
                .unwrap_or_else(|error| panic!("{id} step {index}: invalid IR: {error}"));
                let compiled = document
                    .to_sql()
                    .unwrap_or_else(|error| panic!("{id} step {index}: render failed: {error}"));
                assert_eq!(
                    compiled.sql,
                    step["sql"].as_str().unwrap(),
                    "{id} step {index}"
                );
                assert_eq!(
                    serde_json::to_value(&compiled.parameters).unwrap(),
                    step["parameters"],
                    "{id} step {index}"
                );
            }
            if let Some(rejections) = case.get("rejected_documents") {
                for rejected in rejections.as_array().expect("rejection cases") {
                    assert!(
                        crate::IrDocument::from_json(&serde_json::to_string(rejected).unwrap())
                            .is_err(),
                        "{id}: rejection unexpectedly decoded"
                    );
                }
            }
            assert!(case.get("result").is_some(), "{id}: missing result oracle");
        }

        for id in [
            "ORM-CONFORMANCE-001",
            "ORM-CONFORMANCE-002",
            "ORM-CONFORMANCE-003",
        ] {
            assert!(fixture_ids.contains(id), "missing base fixture {id}");
        }
    }
}
