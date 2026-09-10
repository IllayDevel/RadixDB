use std::fs;
use std::path::Path;

#[test]
fn embedded_handles_delegate_sql_policy_to_the_executor_owner() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let statement = fs::read_to_string(root.join("crates/radixdb-api/src/statement.rs"))
        .expect("read embedded Statement facade");
    let transaction = fs::read_to_string(root.join("crates/radixdb-api/src/transaction.rs"))
        .expect("read embedded Transaction facade");
    let owner = fs::read_to_string(root.join("crates/radixdb-executor/src/prepared.rs"))
        .expect("read executor prepared-program owner");

    for (name, source) in [
        ("statement.rs", &statement),
        ("transaction.rs", &transaction),
    ] {
        for forbidden in [
            "crate::parser",
            "Parser::new",
            "AstStatement",
            "ParameterContract::from_statements",
            "fn execute_statements(",
        ] {
            assert!(
                !source.contains(forbidden),
                "{name} retains SQL execution policy through {forbidden}"
            );
        }
    }

    assert!(statement.contains("program: PreparedProgram"));
    assert!(statement.contains("executor.prepare_program(&sql)?"));
    assert!(statement.contains("executor.execute_prepared_program"));
    assert!(transaction.contains("execute_installed_transaction_sql"));
    assert!(transaction.contains("execute_installed_transaction_prepared"));

    assert!(owner.contains("pub struct PreparedProgram"));
    assert!(owner.contains("pub fn prepare_program("));
    assert!(owner.contains("fn execute_installed_statements("));
    assert!(owner.contains("fn reject_embedded_transaction_control("));
}

#[test]
fn api_owner_is_bounded_and_root_preserves_type_identity() {
    use std::any::TypeId;

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let api = root.join("crates/radixdb-api/src");

    for entry in fs::read_dir(&api).expect("read API directory") {
        let path = entry.expect("read API entry").path();
        if path.extension().and_then(|value| value.to_str()) != Some("rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("read API source");
        let lines = source.lines().count();
        assert!(
            lines <= 3_000,
            "{} has {lines} lines; API owner limit is 3000",
            path.display()
        );
    }

    let database = fs::read_to_string(api.join("database.rs")).expect("read Database facade");
    let statement = fs::read_to_string(api.join("statement.rs")).expect("read Statement facade");
    let transaction =
        fs::read_to_string(api.join("transaction.rs")).expect("read Transaction facade");
    let rows = fs::read_to_string(api.join("rows.rs")).expect("read Rows facade");
    let adapter = fs::read_to_string(api.join("result_adapter.rs")).expect("read result adapter");

    assert!(database.contains("pub struct Database"));
    assert!(statement.contains("pub struct Statement"));
    assert!(transaction.contains("pub struct Transaction"));
    assert!(rows.contains("pub struct Rows"));
    assert!(database.contains("MVCCEngine::new_with_composition_binders"));
    assert!(database.contains("executor: Mutex<Executor>"));
    assert!(adapter.contains("inner: ExecutionResult"));

    let facade = fs::read_to_string(root.join("src/lib.rs")).expect("read root facade");
    assert!(facade.contains("pub use radixdb_api as api;"));
    assert!(!facade.contains("pub struct Database"));
    assert!(!root.join("src/api").exists());
    assert_eq!(
        TypeId::of::<radixdb::Database>(),
        TypeId::of::<radixdb_api::Database>()
    );
    assert_eq!(
        TypeId::of::<radixdb::api::Transaction>(),
        TypeId::of::<radixdb_api::Transaction>()
    );
}

#[test]
fn root_executor_path_is_an_identity_preserving_facade() {
    use std::any::TypeId;

    assert_eq!(
        TypeId::of::<radixdb::Executor>(),
        TypeId::of::<radixdb_executor::Executor>()
    );

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let facade = fs::read_to_string(root.join("src/lib.rs")).expect("read root facade");
    assert!(facade.contains("pub use radixdb_executor as executor;"));
    assert!(facade.contains("pub use radixdb_executor::Executor;"));
    assert!(!facade.contains("pub struct Executor"));
    assert!(!root.join("src/executor").exists());
}

#[test]
fn server_runtime_uses_only_facade_contracts() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let server = root.join("src/server");
    let mut production_files = Vec::new();

    fn collect(path: &Path, output: &mut Vec<std::path::PathBuf>) {
        for entry in fs::read_dir(path).expect("read server source directory") {
            let path = entry.expect("read server source entry").path();
            if path.is_dir() {
                collect(&path, output);
            } else if path.extension().and_then(|value| value.to_str()) == Some("rs")
                && path.file_name().and_then(|value| value.to_str()) != Some("tests.rs")
            {
                output.push(path);
            }
        }
    }
    collect(&server, &mut production_files);

    for path in production_files {
        let source = fs::read_to_string(&path).expect("read server production source");
        for forbidden in [
            "crate::storage",
            "crate::executor",
            "crate::parser",
            "radixdb_storage",
            "radixdb_executor",
            "radixdb_sql",
            "TypedColumnBatch",
            ".engine().lifecycle_state()",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} crosses the server boundary through {forbidden}",
                path.display()
            );
        }
        let lines = source.lines().count();
        assert!(
            lines <= 3_000,
            "{} has {lines} lines; server production limit is 3000",
            path.display()
        );
    }

    let session = fs::read_to_string(server.join("session.rs")).expect("read server session");
    let contract = fs::read_to_string(root.join("crates/radixdb-api/src/server_runtime.rs"))
        .expect("read server runtime facade contract");
    assert!(session.contains("database.query_for_server"));
    assert!(session.contains("transaction.query_for_server"));
    assert!(session.contains("database.runtime_state()"));
    assert!(session.contains("ServerRuntimeMetrics::"));
    assert!(contract.contains("pub struct ServerExecutionContext"));
    assert!(contract.contains("pub enum DatabaseRuntimeState"));
    assert!(contract.contains("pub enum ServerColumnData"));
    assert!(contract.contains("pub struct ServerRuntimeMetrics"));
}

#[test]
fn server_runtime_has_one_root_facade_owner() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = fs::read_to_string(root.join("Cargo.toml")).expect("read root manifest");
    let library = fs::read_to_string(root.join("src/lib.rs")).expect("read root library");
    let binary = fs::read_to_string(root.join("src/bin/radixdb_server.rs"))
        .expect("read server binary entrypoint");

    assert!(!root.join("crates/radixdb-server").exists());
    assert!(!manifest.contains("\"crates/radixdb-server\""));
    assert!(library.contains("pub mod server;"));
    assert!(binary.contains("radixdb::server::run_configured_server_from_env"));
}

#[test]
fn process_entrypoints_are_thin_and_domain_logic_has_library_owners() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let bins = [
        ("src/bin/radixdb.rs", "radixdb::cli::run_from_env"),
        (
            "src/bin/radixdb_server.rs",
            "radixdb::server::run_configured_server_from_env",
        ),
        (
            "src/bin/radixdb_smoke_client.rs",
            "radixdb::server::probe_smoke_endpoint",
        ),
    ];

    for (path, owner_call) in bins {
        let source = fs::read_to_string(root.join(path)).expect("read process entrypoint");
        assert!(
            source.lines().count() <= 64,
            "{path} is not a thin entrypoint"
        );
        assert!(source.contains(owner_call), "{path} omits {owner_call}");
        for forbidden in [
            "Database::open",
            "parse_sql",
            "FileLock::acquire",
            "Connection::connect",
            "connect_with_timeouts",
        ] {
            assert!(
                !source.contains(forbidden),
                "{path} retains domain logic through {forbidden}"
            );
        }
    }

    let cli = fs::read_to_string(root.join("src/cli/mod.rs")).expect("read CLI runtime owner");
    let server = fs::read_to_string(root.join("src/server/launcher.rs"))
        .expect("read server launcher owner");
    let smoke =
        fs::read_to_string(root.join("src/server/smoke.rs")).expect("read smoke runtime owner");
    assert!(cli.lines().count() <= 3_000);
    assert!(cli.contains("pub fn run_from_env() -> u8"));
    assert!(!server.contains("std::process::exit"));
    assert!(smoke.contains("pub fn probe_smoke_endpoint("));
    assert!(smoke.contains("Connection::connect_with_timeouts"));
}

#[test]
fn sql_dump_staging_and_publication_have_one_library_owner() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let cli = fs::read_to_string(root.join("src/cli/mod.rs")).expect("read CLI runtime owner");
    let dump = fs::read_to_string(root.join("src/sql_dump.rs")).expect("read SQL dump owner");

    for forbidden in [
        "fn unique_cli_sibling(",
        "fn atomic_publish_directory_no_replace(",
        "fn dsn_for_import_staging(",
        "Database::open(&staging_dsn)",
        "libc::SYS_renameat2",
    ] {
        assert!(
            !cli.contains(forbidden),
            "CLI retains logical migration ownership through {forbidden}"
        );
    }
    assert!(cli.contains("export_sql_dump_to_file"));
    assert!(cli.contains("import_sql_dump_to_new_database"));
    assert!(dump.contains("pub fn export_sql_dump_to_file("));
    assert!(dump.contains("pub fn import_sql_dump_to_new_database"));
    assert!(dump.contains("libc::SYS_renameat2"));
    assert!(dump.lines().count() <= 3_000);
}

#[test]
fn public_docs_expose_facades_and_hide_implementation_crates() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let library = fs::read_to_string(root.join("src/lib.rs")).expect("read root library");
    let readme = fs::read_to_string(root.join("README.md")).expect("read public README");
    let architecture =
        fs::read_to_string(root.join("doc/src/content/docs/en/internals/overview.md"))
            .expect("read public architecture overview");

    for package in [
        "radixdb-core",
        "radixdb-sql",
        "radixdb-functions",
        "radixdb-api",
        "radixdb-protocol",
        "radixdb-storage",
        "radixdb-executor",
    ] {
        let manifest = fs::read_to_string(root.join("crates").join(package).join("Cargo.toml"))
            .expect("read private implementation manifest");
        assert!(
            manifest
                .lines()
                .any(|line| line.trim() == "publish = false"),
            "{package} is not locked as a private implementation crate"
        );
        assert!(
            architecture.contains(&format!("| `{package}` |")),
            "{package} is missing from the public ownership map"
        );
    }

    for declaration in [
        "pub mod cli;",
        "pub use radixdb_client as client;",
        "pub mod common;",
        "pub use radixdb_core as core;",
        "pub use radixdb_executor as executor;",
        "pub use radixdb_functions as functions;",
        "pub use radixdb_executor::optimizer;",
        "pub use radixdb_sql as parser;",
        "pub use radixdb_protocol as protocol;",
        "pub use radixdb_storage as storage;",
    ] {
        assert!(
            library.contains(&format!("#[doc(hidden)]\n{declaration}")),
            "compatibility declaration {declaration} is advertised by rustdoc"
        );
    }

    assert!(library.contains("## Supported public surface"));
    assert!(readme.contains("embedded `radixdb` library"));
    assert!(readme.contains("`radixdb-client` TCP client"));
    assert!(readme.contains("doc/src/content/docs/en/internals/overview.md"));
    assert!(architecture.contains("The root `radixdb` package is the public"));
    assert!(architecture.contains("Historical root module aliases preserve source compatibility"));
    assert!(architecture.contains("not independent implementation owners"));
}

#[test]
fn protocol_facades_preserve_the_neutral_wire_type_identity() {
    fn assert_same_type<T: 'static, U: 'static>() {
        assert_eq!(std::any::TypeId::of::<T>(), std::any::TypeId::of::<U>());
    }

    assert_same_type::<radixdb_protocol::ClientMessage, radixdb_client::protocol::ClientMessage>();
    assert_same_type::<radixdb_protocol::ServerMessage, radixdb::protocol::ServerMessage>();
    assert_same_type::<radixdb_protocol::WireValue, radixdb_client::WireValue>();
}
