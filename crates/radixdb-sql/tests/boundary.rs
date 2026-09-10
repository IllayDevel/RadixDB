use std::fs;
use std::path::PathBuf;

#[test]
fn sql_crate_has_only_the_allowed_internal_dependency() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest = fs::read_to_string(crate_dir.join("Cargo.toml")).expect("read SQL manifest");

    let internal_dependencies: Vec<_> = manifest
        .lines()
        .filter(|line| line.trim_start().starts_with("radixdb-"))
        .collect();
    assert_eq!(
        internal_dependencies,
        ["radixdb-core = { path = \"../radixdb-core\" }"]
    );
}

#[test]
fn root_token_module_is_a_compatibility_facade() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("radixdb-sql lives below the workspace root");
    let source = fs::read_to_string(workspace.join("src/lib.rs")).expect("read root facade");

    assert!(source.contains("pub use radixdb_sql as parser;"));
    assert!(!workspace.join("src/parser").exists());
    assert!(!source.contains("pub struct "));
    assert!(!source.contains("pub enum "));
    assert!(!source.contains("static KEYWORD_SET"));
}

#[test]
fn root_lexer_module_is_a_compatibility_facade() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("radixdb-sql lives below the workspace root");
    let source = fs::read_to_string(workspace.join("src/lib.rs")).expect("read root facade");

    assert!(source.contains("pub use radixdb_sql as parser;"));
    assert!(!workspace.join("src/parser").exists());
    assert!(!source.contains("pub struct Lexer"));
    assert!(!source.contains("impl Lexer"));
}

#[test]
fn root_ast_module_is_a_compatibility_facade() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("radixdb-sql lives below the workspace root");
    let source = fs::read_to_string(workspace.join("src/lib.rs")).expect("read root facade");

    assert!(source.contains("pub use radixdb_sql as parser;"));
    assert!(!workspace.join("src/parser").exists());
    assert!(!source.contains("pub struct "));
    assert!(!source.contains("pub enum "));
    assert!(!source.contains("impl "));
}

#[test]
fn ast_is_split_into_bounded_functional_modules() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let ast_dir = crate_dir.join("src/ast");

    for name in [
        "control.rs",
        "ddl.rs",
        "dml.rs",
        "expression.rs",
        "query.rs",
        "source.rs",
        "statement.rs",
        "visitor.rs",
    ] {
        let source = fs::read_to_string(ast_dir.join(name)).expect("read AST module");
        assert!(
            source.lines().count() <= 1_500,
            "AST module {name} grew beyond the migration contract"
        );
    }
}

#[test]
fn root_parser_support_modules_do_not_own_parser_implementations() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("radixdb-sql lives below the workspace root");
    assert!(!workspace.join("src/parser").exists());
}

#[test]
fn root_parser_module_is_only_a_facade() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("radixdb-sql lives below the workspace root");
    let source = fs::read_to_string(workspace.join("src/lib.rs")).expect("read root facade");

    assert!(source.contains("pub use radixdb_sql as parser;"));
    assert!(!workspace.join("src/parser").exists());
    assert!(!source.contains("pub fn parse_sql"));
    assert!(!source.contains("mod expressions"));
    assert!(!source.contains("mod statements"));
}

#[test]
fn statement_grammar_is_split_into_bounded_functional_modules() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let statements_dir = crate_dir.join("src/statements");

    for name in ["control.rs", "ddl.rs", "dispatch.rs", "dml.rs", "query.rs"] {
        let source =
            fs::read_to_string(statements_dir.join(name)).expect("read statement grammar module");
        assert!(
            source.lines().count() <= 1_500,
            "statement module {name} grew beyond the migration contract"
        );
    }
}
