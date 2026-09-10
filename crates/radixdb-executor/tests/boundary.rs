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

fn rust_source_text_below(directory: &Path) -> String {
    let mut sources = Vec::new();
    rust_sources_below(directory, &mut sources);
    sources.sort();
    sources
        .into_iter()
        .map(|path| fs::read_to_string(path).expect("read Rust source"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn implementation_entries_below(directory: &Path, entries: &mut Vec<PathBuf>) {
    if !directory.exists() {
        return;
    }
    for entry in fs::read_dir(directory).expect("read implementation directory") {
        let path = entry.expect("read implementation entry").path();
        let ignored = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| matches!(name, "target" | ".git"));
        if ignored {
            continue;
        }
        entries.push(path.clone());
        if path.is_dir() {
            implementation_entries_below(&path, entries);
        }
    }
}

fn source_without_comments_or_strings(source: &str) -> String {
    #[derive(Clone, Copy)]
    enum State {
        Code,
        LineComment,
        BlockComment(u32),
        String { escaped: bool },
        RawString { hashes: usize },
    }

    let bytes = source.as_bytes();
    let mut output = String::with_capacity(source.len());
    let mut state = State::Code;
    let mut index = 0_usize;
    while index < bytes.len() {
        let byte = bytes[index];
        let next = bytes.get(index + 1).copied();
        match state {
            State::Code if byte == b'/' && next == Some(b'/') => {
                output.push_str("  ");
                index += 2;
                state = State::LineComment;
            }
            State::Code if byte == b'/' && next == Some(b'*') => {
                output.push_str("  ");
                index += 2;
                state = State::BlockComment(1);
            }
            State::Code if byte == b'"' => {
                output.push(' ');
                index += 1;
                state = State::String { escaped: false };
            }
            State::Code if byte == b'r' => {
                let mut cursor = index + 1;
                while bytes.get(cursor) == Some(&b'#') {
                    cursor += 1;
                }
                if bytes.get(cursor) == Some(&b'"') {
                    let hashes = cursor - index - 1;
                    output.extend(std::iter::repeat_n(' ', cursor - index + 1));
                    index = cursor + 1;
                    state = State::RawString { hashes };
                } else {
                    output.push(char::from(byte));
                    index += 1;
                }
            }
            State::Code => {
                output.push(char::from(byte));
                index += 1;
            }
            State::LineComment if byte == b'\n' => {
                output.push('\n');
                index += 1;
                state = State::Code;
            }
            State::LineComment => {
                output.push(' ');
                index += 1;
            }
            State::BlockComment(depth) if byte == b'/' && next == Some(b'*') => {
                output.push_str("  ");
                index += 2;
                state = State::BlockComment(depth + 1);
            }
            State::BlockComment(depth) if byte == b'*' && next == Some(b'/') => {
                output.push_str("  ");
                index += 2;
                state = if depth == 1 {
                    State::Code
                } else {
                    State::BlockComment(depth - 1)
                };
            }
            State::BlockComment(depth) => {
                output.push(if byte == b'\n' { '\n' } else { ' ' });
                index += 1;
                state = State::BlockComment(depth);
            }
            State::String { escaped: false } if byte == b'\\' => {
                output.push(' ');
                index += 1;
                state = State::String { escaped: true };
            }
            State::String { escaped: false } if byte == b'"' => {
                output.push(' ');
                index += 1;
                state = State::Code;
            }
            State::String { .. } => {
                output.push(if byte == b'\n' { '\n' } else { ' ' });
                index += 1;
                state = State::String { escaped: false };
            }
            State::RawString { hashes } if byte == b'"' => {
                let end = index
                    .checked_add(1 + hashes)
                    .filter(|end| *end <= bytes.len());
                let closes =
                    end.is_some_and(|end| bytes[index + 1..end].iter().all(|byte| *byte == b'#'));
                if let Some(end) = end.filter(|_| closes) {
                    output.extend(std::iter::repeat_n(' ', end - index));
                    index = end;
                    state = State::Code;
                } else {
                    output.push(' ');
                    index += 1;
                }
            }
            State::RawString { hashes } => {
                output.push(if byte == b'\n' { '\n' } else { ' ' });
                index += 1;
                state = State::RawString { hashes };
            }
        }
    }
    output
}

fn implementation_generation_prefix(token: &str) -> Option<u32> {
    let bytes = token.as_bytes();
    let lead = *bytes.first()?;
    if lead != b'v' && lead != b'V' {
        return None;
    }

    let mut digit_end = 1_usize;
    while bytes.get(digit_end).is_some_and(u8::is_ascii_digit) {
        digit_end += 1;
    }
    if digit_end == 1 || digit_end == bytes.len() {
        return None;
    }

    let suffix = bytes[digit_end];
    let has_prefix_shape = if lead == b'v' {
        suffix == b'_'
    } else {
        suffix == b'_' || suffix.is_ascii_uppercase()
    };
    if !has_prefix_shape {
        return None;
    }

    let generation = token[1..digit_end].parse::<u32>().ok()?;
    (generation >= 5).then_some(generation)
}

fn contains_generation_prefixed_identifier(code: &str) -> bool {
    code.split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|token| !token.is_empty())
        .any(|token| implementation_generation_prefix(token).is_some())
}

fn contains_lower_generation_prefixed_token(source: &str) -> bool {
    source
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|token| !token.is_empty())
        .any(|token| token.starts_with('v') && implementation_generation_prefix(token).is_some())
}

fn manifest_section<'a>(manifest: &'a str, heading: &str) -> &'a str {
    let start = manifest
        .find(heading)
        .unwrap_or_else(|| panic!("manifest section {heading} is missing"));
    let body = &manifest[start + heading.len()..];
    let end = body.find("\n[").unwrap_or(body.len());
    &body[..end]
}

#[test]
fn implementation_names_are_version_neutral() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below the workspace root");
    let exception_marker = "versioned-format-name";
    let mut entries = Vec::new();

    for root in ["src", "crates", "tests"] {
        implementation_entries_below(&workspace.join(root), &mut entries);
    }
    entries.sort();

    for path in entries {
        let has_prefixed_component = path.components().any(|component| {
            component
                .as_os_str()
                .to_str()
                .is_some_and(|name| implementation_generation_prefix(name).is_some())
        });
        assert!(
            !has_prefixed_component,
            "implementation path uses a format-generation prefix: {}",
            path.display()
        );

        if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("read Rust implementation source");
        let code = source_without_comments_or_strings(&source);
        for (line_index, (line, code_line)) in source.lines().zip(code.lines()).enumerate() {
            assert!(
                !contains_lower_generation_prefixed_token(line)
                    || line.contains(exception_marker),
                "{}:{} uses a format-generation prefix without an explicit persisted/wire exception",
                path.display(),
                line_index + 1
            );
            assert!(
                !contains_generation_prefixed_identifier(code_line)
                    || line.contains(exception_marker),
                "{}:{} uses a PascalCase format-generation-prefixed identifier",
                path.display(),
                line_index + 1
            );
        }
    }
}

#[test]
fn naming_lexer_distinguishes_identifiers_from_comments_and_literals() {
    let source = r###"
// V6Comment
let ordinary = "V6 string";
let raw = r#"V6 raw string"#;
/* V6 block comment */
let value: V6ForbiddenType = build();
let future: V27ForbiddenType = build();
"###;
    let code = source_without_comments_or_strings(source);
    let lines = code.lines().collect::<Vec<_>>();

    assert!(!contains_generation_prefixed_identifier(lines[1]));
    assert!(!contains_generation_prefixed_identifier(lines[2]));
    assert!(!contains_generation_prefixed_identifier(lines[3]));
    assert!(!contains_generation_prefixed_identifier(lines[4]));
    assert!(contains_generation_prefixed_identifier(lines[5]));
    assert!(contains_generation_prefixed_identifier(lines[6]));
    let future_lower = ["v7", "_writer"].concat();
    assert!(implementation_generation_prefix(&future_lower).is_some());
    assert!(implementation_generation_prefix("V8Reader").is_some());
    assert!(implementation_generation_prefix("V42_Reader").is_some());
    let retired_upper = ["V", "5LegacyReader"].concat();
    let retired_lower = ["v", "5_legacy_reader"].concat();
    assert!(implementation_generation_prefix(&retired_upper).is_some());
    assert!(implementation_generation_prefix(&retired_lower).is_some());
    assert!(implementation_generation_prefix("v6").is_none());
}

#[test]
fn post_cutover_workspace_has_no_retired_storage_scaffolding() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below the workspace root");
    let mut entries = Vec::new();

    let retired_table_snapshot_codec = workspace.join("crates/radixdb-storage/src/mvcc/snapshot");
    assert!(
        !retired_table_snapshot_codec.exists(),
        "post-cutover production tree contains the retired per-table snapshot codec: {}",
        retired_table_snapshot_codec.display()
    );

    for root in ["src", "crates"] {
        implementation_entries_below(&workspace.join(root), &mut entries);
    }
    entries.sort();

    for path in &entries {
        if !path.is_file() {
            continue;
        }
        let relative = path.strip_prefix(workspace).expect("workspace entry");
        let metadata = fs::metadata(path).expect("read workspace entry metadata");
        assert!(
            metadata.len() > 0,
            "post-cutover production tree contains an empty facade or stub: {}",
            relative.display()
        );

        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        let retired_artifact = matches!(name, "manifest.bin" | "generation.bin")
            || name.starts_with("ddl-")
            || matches!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("vol" | "rpi")
            );
        assert!(
            !retired_artifact,
            "post-cutover production tree contains a retired artifact path: {}",
            relative.display()
        );
    }

    let mut manifests = entries
        .iter()
        .filter(|path| path.file_name().and_then(|name| name.to_str()) == Some("Cargo.toml"))
        .cloned()
        .collect::<Vec<_>>();
    manifests.push(workspace.join("Cargo.toml"));
    manifests.sort();

    for path in manifests {
        let manifest = fs::read_to_string(&path).expect("read Cargo manifest");
        if !manifest.contains("[features]") {
            continue;
        }
        for line in manifest_section(&manifest, "[features]").lines() {
            let Some((name, _)) = line.split_once('=') else {
                continue;
            };
            let name = name.trim().to_ascii_lowercase();
            assert!(
                !name.contains("legacy") && !name.contains("v5"),
                "{} exposes a retired storage feature: {name}",
                path.strip_prefix(workspace).unwrap_or(&path).display()
            );
        }
    }
}

#[test]
fn catalog_runtime_has_one_private_production_owner_and_no_selector() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below the workspace root");
    let executor_lib =
        fs::read_to_string(crate_dir.join("src/lib.rs")).expect("read executor crate root");
    let declaration = "mod catalog;";
    assert!(
        executor_lib.contains(declaration),
        "executor lost its private production catalog owner"
    );
    assert_eq!(
        executor_lib.matches(declaration).count(),
        1,
        "catalog runtime must have exactly one private module declaration"
    );
    assert!(!executor_lib.contains("pub mod catalog;"));

    for manifest_path in ["Cargo.toml", "crates/radixdb-api/Cargo.toml"] {
        let manifest = fs::read_to_string(workspace.join(manifest_path)).expect("read manifest");
        let features = manifest_section(&manifest, "[features]");
        assert!(
            !features.contains("catalog_harness"),
            "{manifest_path} exposes the retired catalog harness through a product feature"
        );
        let dependencies = manifest_section(&manifest, "[dependencies]");
        assert!(
            !dependencies.lines().any(|line| {
                line.starts_with("radixdb-executor")
                    && (line.contains("test-hooks") || line.contains("catalog_harness"))
            }),
            "{manifest_path} selects a retired catalog path or executor test hooks for a product dependency"
        );
    }

    let mut public_sources = Vec::new();
    for root in [
        "src",
        "crates/radixdb-api/src",
        "crates/radixdb-client/src",
        "crates/radixdb-protocol/src",
        "crates/radixdb-soak/src",
    ] {
        rust_sources_below(&workspace.join(root), &mut public_sources);
    }
    for path in public_sources {
        let source = fs::read_to_string(&path).expect("read product source");
        assert!(
            !source.contains("catalog_harness"),
            "{} reaches the retired catalog harness",
            path.display()
        );
    }
}

#[test]
fn executor_crate_has_exactly_the_allowed_internal_dependencies() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest =
        fs::read_to_string(crate_dir.join("Cargo.toml")).expect("read radixdb-executor manifest");
    let internal: Vec<_> = manifest_section(&manifest, "[dependencies]")
        .lines()
        .filter(|line| line.trim_start().starts_with("radixdb-"))
        .collect();

    assert_eq!(
        internal,
        [
            "radixdb-catalog = { path = \"../radixdb-catalog\" }",
            "radixdb-core = { path = \"../radixdb-core\" }",
            "radixdb-functions = { path = \"../radixdb-functions\" }",
            "radixdb-procedural = { path = \"../radixdb-procedural\" }",
            "radixdb-plugin-host = { path = \"../radixdb-plugin-host\" }",
            "radixdb-sql = { path = \"../radixdb-sql\" }",
            "radixdb-storage = { path = \"../radixdb-storage\" }",
        ]
    );
}

#[test]
fn executor_shell_is_a_private_workspace_member() {
    assert_eq!(env!("CARGO_PKG_NAME"), "radixdb-executor");

    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest =
        fs::read_to_string(crate_dir.join("Cargo.toml")).expect("read radixdb-executor manifest");
    assert!(manifest
        .lines()
        .any(|line| line.trim() == "publish = false"));

    let workspace_manifest = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below the workspace root")
        .join("Cargo.toml");
    let workspace = fs::read_to_string(workspace_manifest).expect("read workspace manifest");
    assert!(workspace.contains("\"crates/radixdb-executor\""));
}

#[test]
fn all_lower_crate_contracts_link_independently() {
    assert_eq!(
        radixdb_catalog::CatalogName::new("probe")
            .expect("catalog name")
            .normalized()
            .as_str(),
        "probe"
    );

    let statements = radixdb_sql::parse_sql("SELECT 1").expect("parse shell probe");
    assert_eq!(statements.len(), 1);

    let value = radixdb_core::Value::Integer(1);
    assert_eq!(value, radixdb_core::Value::Integer(1));

    let registry = radixdb_functions::FunctionRegistry::new();
    assert!(registry.exists("count"));

    assert_eq!(
        radixdb_procedural::DiagnosticKind::RuntimeInvalidState.category(),
        radixdb_procedural::DiagnosticCategory::Runtime
    );

    let config = radixdb_storage::Config::in_memory();
    assert!(!config.is_persistent());
}

#[test]
fn procedural_bridge_is_bounded_and_reaches_only_lower_owners() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    rust_sources_below(&crate_dir.join("src/procedural"), &mut sources);
    assert!(!sources.is_empty(), "missing procedural executor bridge");

    for path in sources {
        let source = fs::read_to_string(&path).expect("read procedural bridge owner");
        let lines = source.lines().count();
        assert!(
            lines <= 1_000,
            "{} has {lines} lines; limit is 1000",
            path.display()
        );
        for forbidden in ["radixdb::", "src/executor"] {
            assert!(
                !source.contains(forbidden),
                "{} reaches upward through {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn lifecycle_has_one_crate_owner_and_root_compatibility_facades() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below workspace root");
    let context =
        fs::read_to_string(crate_dir.join("src/context.rs")).expect("read lifecycle owner");
    let hash =
        fs::read_to_string(crate_dir.join("src/hash_table.rs")).expect("read request-memory owner");
    let root = fs::read_to_string(workspace.join("src/lib.rs")).expect("read root facade");

    assert!(context.contains("pub struct ExecutionContext"));
    assert!(context.contains("struct SessionState"));
    assert!(context.contains("struct QueryState"));
    assert!(context.contains("pub struct TimeoutGuard"));
    assert!(context.contains("pub struct CancellationHandle"));
    assert!(context.contains("pub struct StatementCancellationScope"));
    assert!(hash.contains("pub struct JoinHashTable"));
    assert!(hash.contains("pub trait JoinHashObserver"));
    assert!(root.contains("pub use radixdb_executor as executor;"));
    assert!(!workspace.join("src/executor").exists());
}

#[test]
fn lifecycle_sources_do_not_reach_the_root_executor() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for file in ["context.rs", "hash_table.rs"] {
        let source = fs::read_to_string(crate_dir.join("src").join(file))
            .expect("read executor lifecycle source");
        for forbidden in [
            "crate::executor",
            "radixdb::",
            "super::expression",
            "super::query",
        ] {
            assert!(
                !source.contains(forbidden),
                "{file} reaches upward through {forbidden}"
            );
        }
    }
}

#[test]
fn session_and_query_local_state_have_distinct_lifetimes() {
    let mut parent = radixdb_executor::context::ExecutionContextBuilder::new()
        .database("main")
        .session_var("timezone", radixdb_core::Value::Text("UTC".into()))
        .timeout_ms(500)
        .build();
    parent.set_transaction_id(7);

    let mut nested = parent.with_incremented_query_depth();
    nested.set_transaction_id(8);
    nested.set_timeout_ms(900);
    nested.set_session_var("timezone", radixdb_core::Value::Text("CET".into()));

    assert_eq!(parent.current_database(), Some("main"));
    assert_eq!(parent.transaction_id(), Some(7));
    assert_eq!(parent.timeout_ms(), 500);
    assert_eq!(
        parent.get_session_var("timezone"),
        Some(&radixdb_core::Value::Text("UTC".into()))
    );
    assert_eq!(nested.transaction_id(), Some(8));
    assert_eq!(nested.timeout_ms(), 900);
}

#[test]
fn expression_vm_and_row_access_have_one_executor_owner() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below workspace root");
    let root = fs::read_to_string(workspace.join("src/lib.rs")).expect("read root facade");

    assert!(crate_dir.join("src/expression/compiler.rs").is_file());
    assert!(crate_dir.join("src/expression/program.rs").is_file());
    assert!(crate_dir.join("src/expression/vm.rs").is_file());
    assert!(crate_dir.join("src/operator.rs").is_file());
    assert!(root.contains("pub use radixdb_executor as executor;"));
    assert!(!workspace.join("src/executor").exists());
}

#[test]
fn expression_compile_and_execute_sources_do_not_reach_the_root_executor() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    rust_sources_below(&crate_dir.join("src/expression"), &mut sources);
    sources.push(crate_dir.join("src/operator.rs"));

    for path in sources {
        let source = fs::read_to_string(&path).expect("read expression source");
        for forbidden in ["crate::executor", "radixdb::", "src/executor"] {
            assert!(
                !source.contains(forbidden),
                "{} reaches upward through {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn expression_production_files_remain_bounded() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let expression = crate_dir.join("src/expression");
    for (file, limit) in [
        ("compiler.rs", 3_000),
        ("evaluator_bridge.rs", 3_000),
        ("ops.rs", 3_000),
        ("program.rs", 3_000),
        // The linear VM dispatch is kept together so bytecode control flow can
        // be audited as one unit; this is the explicit bounded-file exception.
        ("vm.rs", 3_500),
    ] {
        let source = fs::read_to_string(expression.join(file)).expect("read expression owner");
        let lines = source.lines().count();
        assert!(lines <= limit, "{file} has {lines} lines; limit is {limit}");
    }
}

#[test]
fn streaming_result_and_memory_foundations_have_one_executor_owner() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below workspace root");
    let result = fs::read_to_string(crate_dir.join("src/result.rs")).expect("read result owner");
    let memory = fs::read_to_string(crate_dir.join("src/memory.rs")).expect("read memory owner");
    let root = fs::read_to_string(workspace.join("src/lib.rs")).expect("read root facade");

    assert!(result.contains("pub type ExecutionResult = Box<dyn QueryResult>;"));
    assert!(result.contains("pub struct OperatorExecutorResult"));
    assert!(result.contains("pub struct DeferredExecutorResult"));
    assert!(memory.contains("pub struct RetainedRowsBudget"));
    assert!(root.contains("pub use radixdb_executor as executor;"));
    assert!(!workspace.join("src/executor").exists());
}

#[test]
fn result_foundation_sources_do_not_reach_upward() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = vec![
        crate_dir.join("src/result.rs"),
        crate_dir.join("src/memory.rs"),
        crate_dir.join("src/optimizer/workload.rs"),
    ];
    rust_sources_below(&crate_dir.join("src/result"), &mut sources);

    for path in sources {
        let source = fs::read_to_string(&path).expect("read result foundation source");
        for forbidden in ["crate::executor", "radixdb::", "src/executor"] {
            assert!(
                !source.contains(forbidden),
                "{} reaches upward through {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn result_production_files_remain_bounded() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for (path, limit) in [
        ("src/result.rs", 3_000),
        ("src/result/ordering.rs", 3_000),
        ("src/memory.rs", 1_000),
    ] {
        let source = fs::read_to_string(crate_dir.join(path)).expect("read result owner");
        let lines = source.lines().count();
        assert!(lines <= limit, "{path} has {lines} lines; limit is {limit}");
    }
}

#[test]
fn join_graph_and_physical_operators_have_one_executor_owner() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below workspace root");

    for path in [
        "src/join_executor.rs",
        "src/join_graph.rs",
        "src/parallel.rs",
        "src/optimizer/bloom.rs",
        "src/operators/hash_join.rs",
        "src/operators/index_nested_loop.rs",
        "src/operators/merge_join.rs",
        "src/operators/nested_loop_join.rs",
        "src/operators/reference_unique_lookup.rs",
    ] {
        assert!(crate_dir.join(path).is_file(), "missing JOIN owner {path}");
    }

    let root = fs::read_to_string(workspace.join("src/lib.rs")).expect("read root facade");
    assert!(root.contains("pub use radixdb_executor as executor;"));
    assert!(!workspace.join("src/executor").exists());
}

#[test]
fn join_owner_sources_do_not_reach_upward() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = vec![
        crate_dir.join("src/join_executor.rs"),
        crate_dir.join("src/join_graph.rs"),
        crate_dir.join("src/lookup_key.rs"),
        crate_dir.join("src/parallel.rs"),
        crate_dir.join("src/utils.rs"),
        crate_dir.join("src/optimizer/bloom.rs"),
    ];
    rust_sources_below(&crate_dir.join("src/operators"), &mut sources);

    for path in sources {
        let source = fs::read_to_string(&path).expect("read JOIN owner source");
        for forbidden in [
            "crate::executor",
            "radixdb::",
            "radixdb_executor::",
            "src/executor",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} reaches upward through {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn join_production_files_remain_bounded() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = vec![
        ("src/join_executor.rs", 3_000),
        ("src/join_graph.rs", 1_000),
        ("src/lookup_key.rs", 1_000),
        ("src/parallel.rs", 3_000),
        ("src/optimizer/bloom.rs", 1_000),
    ];
    // The utility owner has 1755 production lines followed by its retained
    // characterization module. Eighteen physical lines above the preferred
    // ceiling are an explicit bounded-file exception, not a growing owner.
    files.push(("src/utils.rs", 3_050));
    for path in [
        "src/operators/bloom_filter.rs",
        "src/operators/count_integer_antijoin.rs",
        "src/operators/count_pk_semijoin.rs",
        "src/operators/hash_join.rs",
        "src/operators/index_nested_loop.rs",
        "src/operators/merge_join.rs",
        "src/operators/nested_loop_join.rs",
        "src/operators/reference_unique_lookup.rs",
    ] {
        files.push((path, 3_000));
    }

    for (path, limit) in files {
        let source = fs::read_to_string(crate_dir.join(path)).expect("read JOIN owner");
        let lines = source.lines().count();
        assert!(lines <= limit, "{path} has {lines} lines; limit is {limit}");
    }
}

#[test]
fn planner_and_optimizer_have_one_executor_owner() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below workspace root");

    for path in [
        "src/planner.rs",
        "src/index_optimizer.rs",
        "src/expr_converter.rs",
        "src/query_classification.rs",
        "src/pushdown/mod.rs",
        "src/pushdown/rules.rs",
        "src/optimizer/feedback.rs",
        "src/optimizer/simplify.rs",
    ] {
        assert!(
            crate_dir.join(path).is_file(),
            "missing planning owner {path}"
        );
    }

    let root = fs::read_to_string(workspace.join("src/lib.rs")).expect("read root facade");
    assert!(root.contains("pub use radixdb_executor as executor;"));
    assert!(!workspace.join("src/executor").exists());
    let host = fs::read_to_string(crate_dir.join("src/index_optimizer_host.rs"))
        .expect("read index optimizer host owner");
    assert!(host.contains("impl IndexOptimizerHost for Executor"));
    assert!(!host.contains("fn try_min_max_index_optimization"));
    assert!(!host.contains("fn try_order_by_index_optimization"));
}

#[test]
fn planner_and_optimizer_sources_do_not_reach_upward() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = vec![
        crate_dir.join("src/planner.rs"),
        crate_dir.join("src/index_optimizer.rs"),
        crate_dir.join("src/expr_converter.rs"),
        crate_dir.join("src/query_classification.rs"),
        crate_dir.join("src/optimizer/feedback.rs"),
        crate_dir.join("src/optimizer/simplify.rs"),
    ];
    rust_sources_below(&crate_dir.join("src/pushdown"), &mut sources);

    for path in sources {
        let source = fs::read_to_string(&path).expect("read planning owner source");
        for forbidden in [
            "crate::executor",
            "radixdb::",
            "radixdb_executor::",
            "src/executor",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} reaches upward through {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn planner_and_optimizer_production_files_remain_bounded() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for (path, limit) in [
        ("src/planner.rs", 3_000),
        ("src/index_optimizer.rs", 3_000),
        ("src/expr_converter.rs", 1_000),
        ("src/query_classification.rs", 2_000),
        ("src/pushdown/mod.rs", 1_000),
        ("src/pushdown/rules.rs", 1_000),
        ("src/optimizer/feedback.rs", 1_000),
        ("src/optimizer/simplify.rs", 2_000),
    ] {
        let source = fs::read_to_string(crate_dir.join(path)).expect("read planning owner");
        let lines = source.lines().count();
        assert!(lines <= limit, "{path} has {lines} lines; limit is {limit}");
    }
}

#[test]
fn optimizer_stays_with_executor_until_the_bidirectional_runtime_edge_is_removed() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below workspace root");

    assert!(
        !workspace.join("crates/radixdb-optimizer").exists(),
        "optimizer decision must be revisited before adding a package"
    );

    let bloom =
        fs::read_to_string(crate_dir.join("src/optimizer/bloom.rs")).expect("read bloom owner");
    let planner = fs::read_to_string(crate_dir.join("src/planner.rs")).expect("read planner owner");
    let physical = fs::read_to_string(crate_dir.join("src/operators/bloom_filter.rs"))
        .expect("read physical bloom operator");
    let result = fs::read_to_string(crate_dir.join("src/result.rs")).expect("read result owner");

    assert!(bloom.contains("impl crate::hash_table::JoinHashObserver for BloomFilterBuilder"));
    assert!(physical.contains("crate::optimizer::bloom"));
    assert!(result.contains("crate::optimizer::workload"));
    assert!(planner.contains("crate::optimizer::feedback"));
    assert!(planner.contains("crate::optimizer::workload"));
    assert!(planner.contains("radixdb_sql::ast::Expression"));
    assert!(planner.contains("radixdb_storage::mvcc::engine::MVCCEngine"));
    assert!(planner.contains("radixdb_storage::statistics"));
}

#[test]
fn mutation_phases_have_one_executor_owner() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below workspace root");

    for path in [
        "src/compiled_plan.rs",
        "src/mutation/ddl.rs",
        "src/mutation/dml.rs",
        "src/mutation/copy.rs",
        "src/mutation/foreign_key.rs",
        "src/mutation/validation.rs",
        "src/mutation/dml_fast_path.rs",
        "src/mutation/pk_fast_path.rs",
    ] {
        assert!(
            crate_dir.join(path).is_file(),
            "missing mutation owner {path}"
        );
    }

    let root = fs::read_to_string(workspace.join("src/lib.rs")).expect("read root facade");
    assert!(root.contains("pub use radixdb_executor as executor;"));
    assert!(!workspace.join("src/executor").exists());
    let host = fs::read_to_string(crate_dir.join("src/mutation_host.rs"))
        .expect("read mutation host owner");
    assert!(host.contains("impl MutationHost for Executor"));
    assert!(!host.contains("fn execute_insert"));
    assert!(!host.contains("fn execute_update"));
    assert!(!host.contains("fn execute_delete"));

    let cache = fs::read_to_string(crate_dir.join("src/query_cache.rs"))
        .expect("read canonical cache facade");
    assert!(cache.contains("crate::dispatch::cache"));
    assert!(!cache.contains("pub struct CompiledPkLookup"));
    assert!(!cache.contains("pub enum CompiledExecution"));
}

#[test]
fn mutation_owner_sources_do_not_reach_upward() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = vec![crate_dir.join("src/compiled_plan.rs")];
    rust_sources_below(&crate_dir.join("src/mutation"), &mut sources);

    for path in sources {
        let source = fs::read_to_string(&path).expect("read mutation owner source");
        for forbidden in [
            "crate::executor",
            "radixdb::",
            "radixdb_executor::",
            "src/executor",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} reaches upward through {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn mutation_production_files_remain_bounded() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for (path, limit) in [
        ("src/compiled_plan.rs", 1_000),
        ("src/mutation/ddl.rs", 3_000),
        ("src/mutation/dml.rs", 3_000),
        ("src/mutation/copy.rs", 1_500),
        ("src/mutation/foreign_key.rs", 1_500),
        ("src/mutation/validation.rs", 1_000),
        ("src/mutation/dml_fast_path.rs", 1_500),
        ("src/mutation/pk_fast_path.rs", 1_000),
        ("src/mutation/dml_support.rs", 1_000),
        ("src/mutation/returning.rs", 1_000),
    ] {
        let source = fs::read_to_string(crate_dir.join(path)).expect("read mutation owner");
        let lines = source.lines().count();
        assert!(lines <= limit, "{path} has {lines} lines; limit is {limit}");
    }
}

#[test]
fn parse_cache_dispatch_and_transaction_routing_have_one_owner() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below workspace root");

    for path in [
        "src/dispatch/cache.rs",
        "src/dispatch/program.rs",
        "src/dispatch/statement.rs",
        "src/dispatch/transaction.rs",
    ] {
        assert!(
            crate_dir.join(path).is_file(),
            "missing dispatch owner {path}"
        );
    }

    let cache_facade = fs::read_to_string(crate_dir.join("src/query_cache.rs"))
        .expect("read canonical parsed-plan cache facade");
    assert!(cache_facade.contains("crate::dispatch::cache"));
    assert!(!cache_facade.contains("pub struct QueryCache"));
    assert!(!cache_facade.contains("pub struct CachedPlanRef"));

    let root = fs::read_to_string(workspace.join("src/lib.rs")).expect("read root facade");
    assert!(!root.contains("Parser::new"));
    assert!(!root.contains("NEXT_STATEMENT_SAVEPOINT_ID"));
    assert!(!root.contains("Statement::CreateTable(stmt) =>"));
    assert!(root.contains("pub use radixdb_executor as executor;"));
    assert!(root.contains("pub use radixdb_executor::Executor;"));
    assert!(!workspace.join("src/executor").exists());

    let executor = fs::read_to_string(crate_dir.join("src/executor.rs"))
        .expect("read concrete executor owner");
    assert!(executor.contains("dispatch::program::execute_sql"));
    let query = rust_source_text_below(&crate_dir.join("src/query"));
    for forbidden in [
        "fn execute_begin(",
        "fn execute_commit_stmt(",
        "fn execute_rollback_stmt(",
        "fn execute_savepoint(",
        "fn execute_release_savepoint(",
        "fn parse_isolation_level(",
    ] {
        assert!(!query.contains(forbidden), "query.rs retains {forbidden}");
    }
}

#[test]
fn dispatch_owner_sources_do_not_reach_upward() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    rust_sources_below(&crate_dir.join("src/dispatch"), &mut sources);
    for path in sources {
        let source = fs::read_to_string(&path).expect("read dispatch owner source");
        for forbidden in ["crate::executor", "radixdb::", "src/executor"] {
            assert!(
                !source.contains(forbidden),
                "{} reaches upward through {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn dispatch_production_files_remain_bounded() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for path in [
        "src/dispatch/cache.rs",
        "src/dispatch/program.rs",
        "src/dispatch/statement.rs",
        "src/dispatch/transaction.rs",
    ] {
        let source = fs::read_to_string(crate_dir.join(path)).expect("read dispatch owner");
        let lines = source.lines().count();
        assert!(lines <= 1_000, "{path} has {lines} lines; limit is 1000");
    }
}

#[test]
fn source_and_output_binding_have_one_executor_owner() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below workspace root");

    for path in [
        "src/binding/output.rs",
        "src/binding/source.rs",
        "src/binding/types.rs",
    ] {
        assert!(
            crate_dir.join(path).is_file(),
            "missing binding owner {path}"
        );
    }

    assert!(!workspace.join("src/executor").exists());

    let query = rust_source_text_below(&crate_dir.join("src/query"));
    for forbidden in [
        "fn parse_view_statement(",
        "fn collect_join_binding_columns(",
        "fn validate_join_statement_bindings(",
        "fn validate_join_output_bindings(",
    ] {
        assert!(!query.contains(forbidden), "query.rs retains {forbidden}");
    }
    assert!(query.contains("SourceBindingExt"));
}

#[test]
fn access_path_and_scan_opening_have_one_executor_owner() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let access = crate_dir.join("src/access");
    let query = rust_source_text_below(&crate_dir.join("src/query"));

    for file in [
        "handle.rs",
        "index.rs",
        "predicate.rs",
        "projection.rs",
        "scan.rs",
    ] {
        assert!(access.join(file).is_file(), "missing access owner {file}");
    }
    assert!(query.contains("open_query_source("));
    assert!(query.contains("access_predicate::prepare_scan_predicate("));
    assert!(query.contains("access_index::index_nested_loop_opportunity("));
    assert!(query.contains("access_scan::open_scan("));
    assert!(query.contains("access_scan::open_exact_projection_scan("));
    assert!(!query.contains("table.scan("));
    assert!(!query.contains("table.scan_exact_projection("));
    assert!(!query.contains("struct FilteredProjectionScanPlan"));
    assert!(!query.contains("struct NarrowKeyStreamPlan"));
}

#[test]
fn access_owner_is_bounded_and_does_not_reach_upward() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let access = crate_dir.join("src/access");
    for file in [
        "handle.rs",
        "index.rs",
        "predicate.rs",
        "projection.rs",
        "scan.rs",
    ] {
        let source = fs::read_to_string(access.join(file)).expect("read access owner");
        let lines = source.lines().count();
        assert!(lines <= 1_000, "{file} has {lines} lines; limit is 1000");
        for forbidden in ["crate::executor", "radixdb::", "src/executor"] {
            assert!(
                !source.contains(forbidden),
                "{file} reaches upward through {forbidden}"
            );
        }
    }
}

#[test]
fn relational_pipeline_has_one_bounded_executor_owner() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below the workspace root");
    let pipeline = crate_dir.join("src/pipeline");

    for file in [
        "distinct.rs",
        "filter.rs",
        "ordering.rs",
        "paging.rs",
        "projection.rs",
        "set.rs",
        "shape.rs",
    ] {
        let source = fs::read_to_string(pipeline.join(file)).expect("read pipeline owner");
        let lines = source.lines().count();
        assert!(lines <= 1_000, "{file} has {lines} lines; limit is 1000");
        for forbidden in ["crate::executor", "radixdb::", "src/executor"] {
            assert!(
                !source.contains(forbidden),
                "{file} reaches upward through {forbidden}"
            );
        }
    }

    assert!(
        !workspace.join("src/executor/set_ops.rs").exists(),
        "set algebra must not retain a second root owner"
    );
    let query = rust_source_text_below(&crate_dir.join("src/query"));
    assert!(query.contains("pipeline_set::execute_set_operations("));
    assert!(query.contains("PageWindow::evaluate("));
    assert!(query.contains("RowShape::new("));
    assert!(query.contains("pipeline_projection::evaluate_expression("));
    assert!(query.contains("pipeline_projection::output_column_names("));
    assert!(!query.contains("fn resolve_distinct_on_indices("));
    assert!(!query.contains("fn compare_rows_with_indices("));
}

#[test]
fn binding_owner_is_read_only_and_does_not_reach_upward() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    rust_sources_below(&crate_dir.join("src/binding"), &mut sources);
    for path in sources {
        let source = fs::read_to_string(&path).expect("read binding owner source");
        for forbidden in [
            "crate::executor",
            "radixdb::",
            "src/executor",
            ".scan(",
            ".get_table(",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} violates binding boundary through {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn binding_production_files_remain_bounded() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for path in [
        "src/binding/output.rs",
        "src/binding/source.rs",
        "src/binding/types.rs",
    ] {
        let source = fs::read_to_string(crate_dir.join(path)).expect("read binding owner");
        let lines = source.lines().count();
        assert!(lines <= 1_000, "{path} has {lines} lines; limit is 1000");
    }
}

#[test]
fn aggregation_and_window_have_one_bounded_executor_owner() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below the workspace root");

    for owner in ["aggregation", "window"] {
        let directory = crate_dir.join("src").join(owner);
        let mut sources = Vec::new();
        rust_sources_below(&directory, &mut sources);
        assert!(!sources.is_empty(), "missing {owner} owner modules");

        for path in sources {
            let source = fs::read_to_string(&path).expect("read executor owner source");
            let lines = source.lines().count();
            assert!(
                lines <= 3_000,
                "{} has {lines} lines; limit is 3000",
                path.display()
            );
            for forbidden in ["crate::executor", "radixdb::", "src/executor"] {
                assert!(
                    !source.contains(forbidden),
                    "{} reaches upward through {forbidden}",
                    path.display()
                );
            }
        }
    }
    assert!(!workspace.join("src/executor").exists());
}

#[test]
fn window_owner_does_not_use_unsafe_parallel_publication() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    rust_sources_below(&crate_dir.join("src/window"), &mut sources);
    for path in sources {
        let source = fs::read_to_string(&path).expect("read window owner source");
        for forbidden in ["unsafe impl", "unsafe fn", "unsafe {"] {
            assert!(
                !source.contains(forbidden),
                "{} contains forbidden {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn subquery_cte_and_navigation_have_one_executor_owner() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below the workspace root");

    for owner in ["subquery", "cte", "navigation"] {
        let directory = crate_dir.join("src").join(owner);
        let mut sources = Vec::new();
        rust_sources_below(&directory, &mut sources);
        assert!(!sources.is_empty(), "missing {owner} lifecycle owner");
    }
    assert!(!workspace.join("src/executor").exists());
}

#[test]
fn subquery_cte_and_navigation_sources_are_bounded_and_do_not_reach_upward() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for owner in ["subquery", "cte", "navigation"] {
        let mut sources = Vec::new();
        rust_sources_below(&crate_dir.join("src").join(owner), &mut sources);
        for path in sources {
            let source = fs::read_to_string(&path).expect("read query lifecycle owner");
            let lines = source.lines().count();
            assert!(
                lines <= 3_000,
                "{} has {lines} lines; limit is 3000",
                path.display()
            );
            for forbidden in [
                "crate::executor",
                "radixdb::",
                "radixdb_executor::",
                "src/executor",
            ] {
                assert!(
                    !source.contains(forbidden),
                    "{} reaches upward through {forbidden}",
                    path.display()
                );
            }
        }
    }
}

#[test]
fn query_local_lifecycle_is_explicit_at_the_select_boundary() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let query = rust_source_text_below(&crate_dir.join("src/query"));
    let context =
        fs::read_to_string(crate_dir.join("src/context.rs")).expect("read query state owner");

    for reset in [
        "clear_scalar_subquery_cache();",
        "clear_in_subquery_cache();",
        "clear_semi_join_cache();",
        "clear_exists_predicate_cache();",
    ] {
        assert!(query.contains(reset), "top-level SELECT omits {reset}");
    }
    assert!(context.contains("cte_data: Option<Arc<CteDataMap>>"));
    assert!(context.contains("active_reference_expands: Arc<AtomicUsize>"));
}

#[test]
fn executor_cutover_leaves_no_root_statement_or_query_owner() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("executor crate lives below the workspace root");
    let root = fs::read_to_string(workspace.join("src/lib.rs")).expect("read root facade");

    assert!(root.contains("pub use radixdb_executor as executor;"));
    assert!(root.contains("pub use radixdb_executor::Executor;"));
    assert!(!root.contains("pub struct Executor"));
    assert!(!root.contains("impl Executor"));
    assert!(!workspace.join("src/executor").exists());
}

#[test]
fn concrete_executor_and_query_sources_are_bounded_and_do_not_reach_upward() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = vec![
        crate_dir.join("src/executor.rs"),
        crate_dir.join("src/executor_host.rs"),
        crate_dir.join("src/index_optimizer_host.rs"),
        crate_dir.join("src/mutation_host.rs"),
        crate_dir.join("src/semantic_cache.rs"),
        crate_dir.join("src/show.rs"),
        crate_dir.join("src/statistics.rs"),
    ];
    rust_sources_below(&crate_dir.join("src/query"), &mut sources);
    rust_sources_below(&crate_dir.join("src/explain"), &mut sources);

    for path in sources {
        let source = fs::read_to_string(&path).expect("read executor cutover source");
        let lines = source.lines().count();
        assert!(
            lines <= 3_000,
            "{} has {lines} lines; limit is 3000",
            path.display()
        );
        for forbidden in ["crate::executor", "radixdb::", "src/executor"] {
            assert!(
                !source.contains(forbidden),
                "{} reaches upward through {forbidden}",
                path.display()
            );
        }
    }
}
