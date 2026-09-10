use std::fs;
use std::path::{Path, PathBuf};

fn rust_sources_below(directory: &Path, sources: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("read source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            rust_sources_below(&path, sources);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            sources.push(path);
        }
    }
}

#[test]
fn executor_sources_use_only_lower_radixdb_crates() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    rust_sources_below(&root.join("crates/radixdb-executor/src"), &mut sources);
    sources.sort();

    let forbidden = [
        "crate::api",
        "crate::client",
        "crate::common",
        "crate::core",
        "crate::functions",
        "crate::parser",
        "crate::protocol",
        "crate::server",
        "crate::sql_dump",
        "crate::storage",
        "crate::test_failpoints",
        "super::api",
        "radixdb::",
        "radixdb_client::",
        "radixdb_orm::",
    ];
    let mut violations = Vec::new();
    for path in sources {
        let source = fs::read_to_string(&path).expect("read executor source");
        for (line_index, line) in source.lines().enumerate() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            if forbidden.iter().any(|needle| line.contains(needle)) {
                violations.push(format!("{}:{}: {line}", path.display(), line_index + 1));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "executor imports a root facade or upward RadixDB layer:\n{}",
        violations.join("\n")
    );
}

#[test]
fn executor_owns_optimizer_implementation_and_root_has_no_mirror_tree() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let optimizer_owner = root.join("crates/radixdb-executor/src/optimizer");
    let mut owned_sources = Vec::new();
    rust_sources_below(&optimizer_owner, &mut owned_sources);
    owned_sources.sort();

    assert!(
        owned_sources.len() >= 5,
        "executor optimizer owner must contain its implementation modules"
    );

    let library = fs::read_to_string(root.join("src/lib.rs")).expect("read root facade");
    assert!(library.contains("pub use radixdb_executor as executor;"));
    assert!(library.contains("pub use radixdb_executor::optimizer;"));
    assert!(!root.join("src/executor").exists());
    assert!(!root.join("src/optimizer").exists());
}

#[test]
fn neutral_schema_descriptors_are_owned_by_core() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let core = fs::read_to_string(root.join("crates/radixdb-core/src/schema_descriptor.rs"))
        .expect("read core schema descriptor owner");
    let orm =
        fs::read_to_string(root.join("crates/radixdb-orm/src/lib.rs")).expect("read ORM facade");
    let executor_show = fs::read_to_string(root.join("crates/radixdb-executor/src/show.rs"))
        .expect("read SHOW executor owner");

    assert!(core.contains("pub struct TableDescriptor"));
    assert!(core.contains("pub struct DatabaseDescriptor"));
    assert!(orm.contains("pub use radixdb_core::{"));
    assert!(!root.join("crates/radixdb-orm/src/descriptor.rs").exists());
    assert!(executor_show.contains("use radixdb_core::{"));
    assert!(!executor_show.contains("radixdb_orm"));
}

#[test]
fn positional_parameter_owner_is_core_and_api_is_a_facade() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let api =
        fs::read_to_string(root.join("crates/radixdb-api/src/params.rs")).expect("read API params");
    let core = fs::read_to_string(root.join("crates/radixdb-core/src/params.rs"))
        .expect("read core params");

    assert!(api.contains("pub use radixdb_core::ParamVec;"));
    assert!(!api.contains("pub type ParamVec"));
    assert!(core.contains("pub type ParamVec = SmallVec<[Value; 8]>;"));
}

#[test]
fn execution_result_is_adapted_once_at_the_api_boundary() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let executor_result = fs::read_to_string(root.join("crates/radixdb-executor/src/result.rs"))
        .expect("read executor result owner");
    let executor_library = fs::read_to_string(root.join("crates/radixdb-executor/src/lib.rs"))
        .expect("read executor library");
    let api_adapter = fs::read_to_string(root.join("crates/radixdb-api/src/result_adapter.rs"))
        .expect("read API result adapter");
    let api_rows =
        fs::read_to_string(root.join("crates/radixdb-api/src/rows.rs")).expect("read API rows");
    let api_transaction = fs::read_to_string(root.join("crates/radixdb-api/src/transaction.rs"))
        .expect("read API transaction");

    assert!(executor_result.contains("pub type ExecutionResult = Box<dyn QueryResult>;"));
    assert!(executor_library.contains("pub use result::{"));
    assert!(!root.join("src/executor").exists());
    assert!(api_adapter.contains("inner: ExecutionResult"));
    assert!(api_adapter.contains("pub(super) struct ApiResultCursor"));
    assert!(!api_adapter.contains("pub struct ApiResultCursor"));

    assert!(api_rows.contains("result: ApiResultCursor"));
    assert!(!api_rows.contains("result: Box<dyn QueryResult>"));
    assert!(!api_rows.contains("impl QueryResult for Rows"));
    assert!(!api_rows.contains("impl QueryResult for ResultRow"));
    assert!(!api_transaction.contains("dyn QueryResult"));
}

#[test]
fn execution_lifecycle_is_owned_by_the_executor_crate() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let owner = fs::read_to_string(root.join("crates/radixdb-executor/src/context.rs"))
        .expect("read executor context owner");
    let hash_owner = fs::read_to_string(root.join("crates/radixdb-executor/src/hash_table.rs"))
        .expect("read executor hash owner");
    let executor_library = fs::read_to_string(root.join("crates/radixdb-executor/src/lib.rs"))
        .expect("read executor library");

    assert!(owner.contains("struct SessionState"));
    assert!(owner.contains("struct QueryState"));
    assert!(owner.contains("pub struct TimeoutGuard"));
    assert!(owner.contains("pub struct CancellationHandle"));
    assert!(owner.contains("pub struct StatementCancellationScope"));
    assert!(!owner.contains("super::expression"));
    assert!(!owner.contains("crate::executor"));
    assert!(executor_library.contains("pub use context::{"));
    assert!(hash_owner.contains("pub struct JoinHashTable"));
    assert!(executor_library.contains("pub use hash_table::{"));
    assert!(!root.join("src/executor").exists());
}

#[test]
fn expression_and_row_ref_public_paths_preserve_type_identity() {
    use std::any::TypeId;

    assert_eq!(
        TypeId::of::<radixdb::Executor>(),
        TypeId::of::<radixdb_executor::Executor>()
    );
    assert_eq!(
        TypeId::of::<radixdb::executor::expression::Program>(),
        TypeId::of::<radixdb_executor::expression::Program>()
    );
    assert_eq!(
        TypeId::of::<radixdb::executor::expression::ExprVM>(),
        TypeId::of::<radixdb_executor::expression::ExprVM>()
    );
    assert_eq!(
        TypeId::of::<radixdb::executor::operator::RowRef>(),
        TypeId::of::<radixdb_executor::operator::RowRef>()
    );
    assert_eq!(
        TypeId::of::<radixdb::executor::result::ExecutorResult>(),
        TypeId::of::<radixdb_executor::result::ExecutorResult>()
    );
    assert_eq!(
        TypeId::of::<radixdb::executor::join_executor::JoinExecutor>(),
        TypeId::of::<radixdb_executor::join_executor::JoinExecutor>()
    );
    assert_eq!(
        TypeId::of::<radixdb::executor::operators::hash_join::JoinType>(),
        TypeId::of::<radixdb_executor::operators::hash_join::JoinType>()
    );
    assert_eq!(
        TypeId::of::<radixdb::executor::parallel::ParallelConfig>(),
        TypeId::of::<radixdb_executor::parallel::ParallelConfig>()
    );
    assert_eq!(
        TypeId::of::<radixdb::executor::planner::QueryPlanner>(),
        TypeId::of::<radixdb_executor::planner::QueryPlanner>()
    );
}
