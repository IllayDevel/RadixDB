// Copyright 2026 RadixDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! RadixDB CLI - Interactive SQL database command-line interface
//!

use std::collections::{HashSet, VecDeque};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::{ArgGroup, Parser};
use comfy_table::{presets::UTF8_FULL_CONDENSED, Cell, ContentArrangement, Table};
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::{Config, DefaultEditor, EditMode, Editor};

use crate::api::{Database, Transaction as ApiTransaction};
use crate::common::version::{MAJOR, MINOR, PATCH};
use crate::parser::{parse_sql, Lexer, Statement, TokenType};
use crate::sql_dump::{
    export_sql_dump, export_sql_dump_to_file, import_sql_dump_to_new_database, SqlDumpSummary,
};
use crate::storage::mvcc::file_lock::FileLock;
use crate::{DataType, Value};

const MAX_CLI_UNLIMITED_ROWS: usize = 100_000;
const MAX_CLI_STATEMENT_BYTES: usize = 16 * 1024 * 1024;

fn append_cli_statement_with_limit(
    buffer: &mut String,
    line: &str,
    max_bytes: usize,
) -> Result<(), String> {
    let separator = usize::from(!buffer.is_empty());
    let next_len = buffer
        .len()
        .checked_add(separator)
        .and_then(|length| length.checked_add(line.len()))
        .ok_or_else(|| "CLI statement byte length overflow".to_string())?;
    if next_len > max_bytes {
        return Err(format!(
            "CLI statement exceeds {max_bytes} byte safety budget"
        ));
    }
    if separator != 0 {
        buffer.push('\n');
    }
    buffer.push_str(line);
    Ok(())
}

fn append_cli_statement(buffer: &mut String, line: &str) -> Result<(), String> {
    append_cli_statement_with_limit(buffer, line, MAX_CLI_STATEMENT_BYTES)
}

fn read_bounded_cli_input(mut reader: impl Read, source: &str) -> Result<String, String> {
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take((MAX_CLI_STATEMENT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("Error reading {source}: {error}"))?;
    if bytes.len() > MAX_CLI_STATEMENT_BYTES {
        return Err(format!(
            "CLI input from {source} exceeds {MAX_CLI_STATEMENT_BYTES} byte safety budget"
        ));
    }
    String::from_utf8(bytes).map_err(|error| format!("{source} is not valid UTF-8: {error}"))
}

fn collect_cli_rows(
    rows: crate::api::Rows,
    display_limit: usize,
) -> Result<(Vec<Vec<Value>>, usize), String> {
    let retained_limit = if display_limit == 0 {
        MAX_CLI_UNLIMITED_ROWS
    } else {
        display_limit
    };
    let head_limit = if display_limit == 0 {
        retained_limit
    } else {
        retained_limit / 2
    };
    let tail_limit = retained_limit.saturating_sub(head_limit);
    let mut head = Vec::with_capacity(head_limit.min(4_096));
    let mut tail = VecDeque::with_capacity(tail_limit.min(4_096));
    let mut row_count = 0_usize;

    for row_result in rows {
        let row = row_result.map_err(|error| error.to_string())?;
        row_count = row_count.saturating_add(1);
        let values: Vec<Value> = (0..row.len())
            .map(|index| {
                row.get_value(index)
                    .cloned()
                    .unwrap_or_else(Value::null_unknown)
            })
            .collect();
        if head.len() < head_limit {
            head.push(values);
        } else if tail_limit > 0 {
            if tail.len() == tail_limit {
                tail.pop_front();
            }
            tail.push_back(values);
        } else if display_limit == 0 {
            return Err(format!(
                "CLI unlimited output exceeded safety budget of {MAX_CLI_UNLIMITED_ROWS} rows; use --limit"
            ));
        }
        if display_limit == 0 && row_count > MAX_CLI_UNLIMITED_ROWS {
            return Err(format!(
                "CLI unlimited output exceeded safety budget of {MAX_CLI_UNLIMITED_ROWS} rows; use --limit"
            ));
        }
    }
    head.extend(tail);
    Ok((head, row_count))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CliStatementKind {
    Begin(Option<String>),
    Commit,
    Rollback,
    Savepoint(String),
    RollbackToSavepoint(String),
    ReleaseSavepoint(String),
    Rows,
    Command,
}

fn classify_cli_statement(sql: &str) -> Result<CliStatementKind, String> {
    let statements = parse_sql(sql).map_err(|error| error.to_string())?;
    let [statement] = statements.as_slice() else {
        return Err("CLI execution requires exactly one SQL statement".to_string());
    };
    Ok(match statement {
        Statement::Begin(statement) => {
            CliStatementKind::Begin(statement.isolation_level.as_ref().map(ToString::to_string))
        }
        Statement::Commit(_) => CliStatementKind::Commit,
        Statement::Rollback(statement) => match &statement.savepoint_name {
            Some(name) => CliStatementKind::RollbackToSavepoint(if name.token.quoted {
                name.value.to_string()
            } else {
                name.value_lower.to_string()
            }),
            None => CliStatementKind::Rollback,
        },
        Statement::Savepoint(statement) => {
            CliStatementKind::Savepoint(if statement.savepoint_name.token.quoted {
                statement.savepoint_name.value.to_string()
            } else {
                statement.savepoint_name.value_lower.to_string()
            })
        }
        Statement::ReleaseSavepoint(statement) => {
            CliStatementKind::ReleaseSavepoint(if statement.savepoint_name.token.quoted {
                statement.savepoint_name.value.to_string()
            } else {
                statement.savepoint_name.value_lower.to_string()
            })
        }
        Statement::Select(_)
        | Statement::ShowTables(_)
        | Statement::ShowViews(_)
        | Statement::ShowCreateTable(_)
        | Statement::ShowCreateView(_)
        | Statement::ShowIndexes(_)
        | Statement::Describe(_)
        | Statement::Expression(_)
        | Statement::Explain(_)
        | Statement::Vacuum(_) => CliStatementKind::Rows,
        Statement::Pragma(pragma) if pragma.value.is_none() => CliStatementKind::Rows,
        Statement::Insert(insert) if !insert.returning.is_empty() => CliStatementKind::Rows,
        Statement::Update(update) if !update.returning.is_empty() => CliStatementKind::Rows,
        Statement::Delete(delete) if !delete.returning.is_empty() => CliStatementKind::Rows,
        _ => CliStatementKind::Command,
    })
}

/// Version string constant
const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION_MAJOR"),
    ".",
    env!("CARGO_PKG_VERSION_MINOR"),
    ".",
    env!("CARGO_PKG_VERSION_PATCH"),
    " git=",
    env!("RADIXDB_GIT_COMMIT"),
    " profile=",
    env!("RADIXDB_BUILD_PROFILE"),
    " target=",
    env!("RADIXDB_BUILD_TARGET"),
    " lock=",
    env!("RADIXDB_CARGO_LOCK_SHA256")
);

/// RadixDB SQL Database CLI
#[derive(Parser, Debug)]
#[command(name = "radixdb-cli")]
#[command(author = "RadixDB Contributors")]
#[command(group(
    ArgGroup::new("action")
        .args([
            "execute",
            "file",
            "restore",
            "snapshot",
            "physical_format",
            "inspect_snapshot",
            "reset_storage",
            "export_sql",
            "import_sql",
        ])
        .multiple(false)
))]
#[command(version = VERSION)]
#[command(about = "High-performance embedded SQL database with MVCC")]
#[command(
    long_about = "RadixDB is a high-performance SQL database with MVCC transactions.\n\
This CLI provides an interactive interface to execute SQL queries and manage your database.\n\n\
PERSISTENCE DSN PARAMETERS:\n\
  file:///path/to/db?param=value&param2=value2\n\n\
  sync_mode=none|normal|full   Fsync mode (default: normal)\n\
    none: no fsync, durable at checkpoint only\n\
    normal: fsync every 1 second, DDL fsyncs immediately\n\
    full: fsync on every write\n\
  checkpoint_interval=SECS    Checkpoint interval in seconds (default: 60)\n\
  compact_threshold=COUNT     Sub-target segments per table before merging (default: 4)\n\
  compression=on|off          WAL + volume LZ4 compression (default: on)\n\
  wal_compression=on|off      WAL compression only (default: on)\n\
  volume_compression=on|off   Data-artifact LZ4 compression only (default: on)\n\
  keep_snapshots=COUNT        Complete database snapshots to retain (default: 3)\n\
  checkpoint_on_close=on|off  Checkpoint on shutdown (default: on)\n\
  wal_max_size=BYTES          Max WAL file size before rotation (default: 67108864)\n\
  wal_buffer_size=BYTES       WAL buffer size (default: 65536)\n\
  wal_flush_trigger=BYTES     Buffer size to trigger flush (default: 32768)\n\
  sync_interval_ms=MS         Min time between syncs in normal mode (default: 1000)\n\
  target_volume_rows=ROWS     Target rows per cold data segment (default: 1048576)\n\n\
  seal_hot_bytes=BYTES        First seal hot bytes threshold (default: 67108864)\n\
  seal_incremental_hot_bytes=BYTES Subsequent seal hot bytes threshold (default: 16777216)\n\n\
  volume_cache_bytes=BYTES    Global resident cold-artifact cache budget (default: 1073741824)\n\n\
EXAMPLES:\n\
  radixdb-cli -d memory://                                    In-memory database\n\
  radixdb-cli -d file:///tmp/mydb                             Persistent database\n\
  radixdb-cli -d file:///tmp/mydb?sync_mode=full               Maximum durability\n\
  radixdb-cli -d file:///tmp/mydb?sync_mode=none&compression=off  Max performance\n\
  radixdb-cli -d file:///tmp/mydb --profile durable           Use durable preset\n\
  radixdb-cli -d file:///tmp/mydb --sync full --compression off\n\n\
BACKUP & RESTORE:\n\
  radixdb-cli -d file:///tmp/mydb --snapshot                  Create snapshot and print its 32-hex ID\n\
  radixdb-cli -d file:///tmp/mydb --restore                   Restore from latest snapshot\n\
  radixdb-cli -d file:///tmp/mydb --restore 0123456789abcdef0123456789abcdef\n\
                                                               Restore the snapshot ID printed by --snapshot\n\n\
LOGICAL VERSION MIGRATION:\n\
  old/radixdb-cli -d file:///old --export-sql database.sql\n\
  new/radixdb-cli -d file:///new --import-sql database.sql\n\
  Use '-' for stdout/stdin. Import requires a nonexistent file:// target and\n\
  publishes it only after full-sync staging, checksum and close succeed."
)]
struct Args {
    /// Database path (`file://<path>` or `memory://`)
    #[arg(short = 'd', long = "db", default_value = "memory://")]
    db_path: String,

    /// Output results in JSON format
    #[arg(short = 'j', long = "json", default_value = "false")]
    json_output: bool,

    /// Suppress connection messages
    #[arg(short = 'q', long = "quiet", default_value = "false")]
    quiet: bool,

    /// Maximum number of rows to display (0 uses a 100000-row safety cap)
    #[arg(short = 'l', long = "limit", default_value = "40")]
    limit: usize,

    /// Execute a single SQL statement and exit
    #[arg(short = 'e', long = "execute")]
    execute: Option<String>,

    /// Execute SQL statements from a file
    #[arg(short = 'f', long = "file")]
    file: Option<String>,

    /// WAL sync mode for durability (none, normal, full)
    /// - none: No fsync, data durable at checkpoint only
    /// - normal: Fsync every 1 second, DDL fsyncs immediately (default)
    /// - full: Fsync on every write
    #[arg(short = 's', long = "sync", value_name = "MODE")]
    sync_mode: Option<String>,

    /// Persistence profile preset (fast, normal, durable)
    /// - fast: Optimized for performance, less durable
    /// - normal: Balanced performance and durability (default)
    /// - durable: Maximum durability, slower performance
    #[arg(short = 'p', long = "profile", value_name = "PROFILE")]
    persistence_profile: Option<String>,

    /// Checkpoint interval in seconds (default: 60)
    #[arg(long = "checkpoint-interval", value_name = "SECONDS")]
    checkpoint_interval: Option<u32>,

    /// Sub-target volumes per table before merging (default: 4)
    #[arg(long = "compact-threshold", value_name = "COUNT")]
    compact_threshold: Option<u32>,

    /// Global resident cold-volume payload cache budget in MB (default: 1024)
    #[arg(long = "volume-cache-size", value_name = "MB")]
    volume_cache_size: Option<u32>,

    /// Maximum WAL file size in MB before rotation (default: 64)
    #[arg(long = "wal-max-size", value_name = "MB")]
    wal_max_size: Option<u32>,

    /// Enable or disable LZ4 compression for WAL and volumes (default: on)
    #[arg(long = "compression", value_name = "on|off")]
    compression: Option<String>,

    /// Number of complete database snapshots to keep (default: 3)
    #[arg(long = "keep-snapshots", value_name = "COUNT")]
    keep_snapshots: Option<u32>,

    /// Disable checkpoint on close (for crash simulation in tests)
    #[arg(long = "no-checkpoint-on-close")]
    no_checkpoint_on_close: bool,

    /// Restore database from backup snapshot and exit.
    /// Atomically replaces the database with the selected complete snapshot.
    /// Optionally specify its stable identity: `--restore "<snapshot-id>"`
    #[arg(long = "restore", value_name = "SNAPSHOT_ID", num_args = 0..=1, default_missing_value = "")]
    restore: Option<String>,

    /// Create a backup snapshot and exit.
    #[arg(long = "snapshot")]
    snapshot: bool,

    /// Print the supported physical storage format and exit.
    #[arg(long = "physical-format")]
    physical_format: bool,

    /// Validate a committed snapshot directory and print its identity as JSON.
    #[arg(long = "inspect-snapshot", value_name = "DIRECTORY")]
    inspect_snapshot: Option<String>,

    /// Export one consistent logical SQL dump. '-' writes the dump to stdout.
    #[arg(long = "export-sql", value_name = "PATH|-")]
    export_sql: Option<String>,

    /// Import a logical SQL dump into a new file:// database. '-' reads stdin.
    #[arg(long = "import-sql", value_name = "PATH|-")]
    import_sql: Option<String>,

    /// Offline destructive reset of the complete storage generation.
    /// Cannot be combined with --restore; restore atomically replaces its generation.
    #[arg(long = "reset-storage")]
    reset_storage: bool,

    /// Query timeout in milliseconds (0 for no timeout, default: 0)
    /// Long-running queries will be cancelled after this time.
    #[arg(short = 't', long = "timeout", value_name = "MS", default_value = "0")]
    timeout_ms: u64,
}

/// CLI state for interactive mode
struct Cli {
    db: Database,
    tx: Option<ApiTransaction>,
    json_output: bool,
    limit: usize,
    timeout_ms: u64,
    editor: Editor<(), DefaultHistory>,
    current_query: String,
    in_multi_line: bool,
}

impl Cli {
    fn new(db: Database, json_output: bool, limit: usize, timeout_ms: u64) -> io::Result<Self> {
        let config = Config::builder()
            .history_ignore_space(true)
            .edit_mode(EditMode::Emacs)
            .build();

        let mut editor =
            DefaultEditor::with_config(config).map_err(|e| io::Error::other(e.to_string()))?;

        // Load history from file.
        if let Some(history_file) = history_file_path() {
            let _ = editor.load_history(&history_file);
        }

        Ok(Self {
            db,
            tx: None,
            json_output,
            limit,
            timeout_ms,
            editor,
            current_query: String::new(),
            in_multi_line: false,
        })
    }

    fn get_prompt(&self) -> &'static str {
        let in_transaction = self.transaction_active();
        if self.in_multi_line {
            if in_transaction {
                "\x1b[1;33m[TXN]->\x1b[0m "
            } else {
                "\x1b[1;36m->\x1b[0m "
            }
        } else if in_transaction {
            "\x1b[1;33m[TXN]>\x1b[0m "
        } else {
            "\x1b[1;36m>\x1b[0m "
        }
    }

    fn run(&mut self) -> io::Result<()> {
        if !self.json_output {
            println!("RadixDB v{}.{}.{}", MAJOR, MINOR, PATCH);
            println!("Enter SQL commands, 'help' for assistance, or 'exit' to quit.");
            println!("Use Up/Down arrows for history, Ctrl+R to search history.");
            println!();
        }

        loop {
            let prompt = self.get_prompt();
            match self.editor.readline(prompt) {
                Ok(line) => {
                    let trimmed = line.trim();

                    // Handle empty line
                    if !self.in_multi_line && trimmed.is_empty() {
                        continue;
                    }

                    // Handle special commands (only when not in multi-line mode)
                    if !self.in_multi_line {
                        match trimmed.to_lowercase().as_str() {
                            "exit" | "quit" | "\\q" => {
                                if self.transaction_active() {
                                    eprintln!("\x1b[1;33mWarning: Exiting with active transaction. Rolling back...\x1b[0m");
                                    self.rollback_transaction().map_err(io::Error::other)?;
                                }
                                break;
                            }
                            "help" | "\\h" | "\\?" => {
                                self.print_help();
                                continue;
                            }
                            _ => {}
                        }
                    }

                    if let Err(error) = append_cli_statement(&mut self.current_query, &line) {
                        eprintln!("\x1b[1;31mError:\x1b[0m {error}");
                        self.current_query.clear();
                        self.in_multi_line = false;
                        continue;
                    }

                    // The core SQL lexer, not line layout or string suffixes,
                    // owns statement completion.
                    if sql_input_is_complete(&self.current_query) {
                        let full_query = self.current_query.trim().to_string();
                        // Add to history
                        let history_entry = full_query.replace('\n', "\\n");
                        let _ = self.editor.add_history_entry(&history_entry);

                        self.in_multi_line = false;

                        // Split and execute statements
                        let statements = match split_sql_statements(&full_query) {
                            Ok(statements) => statements,
                            Err(error) => {
                                eprintln!("\x1b[1;31mError:\x1b[0m {error}");
                                self.current_query.clear();
                                continue;
                            }
                        };
                        for stmt in statements {
                            let stmt = stmt.trim();
                            if stmt.is_empty() {
                                continue;
                            }

                            let start = Instant::now();
                            if let Err(e) = self.execute_query(stmt) {
                                eprintln!("\x1b[1;31mError:\x1b[0m {}", e);
                            } else if !self.json_output {
                                println!(
                                    "\x1b[1;32mQuery executed in {:?}\x1b[0m",
                                    start.elapsed()
                                );
                            }
                        }

                        self.current_query.clear();
                    } else {
                        self.in_multi_line = true;
                    }
                }
                Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                    if self.transaction_active() {
                        eprintln!("\n\x1b[1;33mWarning: Exiting with active transaction. Rolling back...\x1b[0m");
                        self.rollback_transaction().map_err(io::Error::other)?;
                    }
                    break;
                }
                Err(e) => {
                    let read_error = e.to_string();
                    if self.transaction_active() {
                        if let Err(rollback_error) = self.rollback_transaction() {
                            return Err(io::Error::other(format!(
                                "interactive input failed: {read_error}; terminal rollback also failed: {rollback_error}"
                            )));
                        }
                    }
                    return Err(io::Error::other(read_error));
                }
            }
        }

        // Save history.
        if let Some(history_file) = history_file_path() {
            let _ = self.editor.save_history(&history_file);
        }

        Ok(())
    }

    fn execute_query(&mut self, query: &str) -> Result<(), String> {
        let trimmed = query.trim();

        // Handle special commands
        match trimmed.to_ascii_uppercase().as_str() {
            "HELP" | "\\H" | "\\?" => {
                self.print_help();
                return Ok(());
            }
            _ => {}
        }

        match classify_cli_statement(query)? {
            CliStatementKind::Begin(isolation) => self.begin_transaction(isolation.as_deref()),
            CliStatementKind::Commit => self.commit_transaction(),
            CliStatementKind::Rollback => self.rollback_transaction(),
            CliStatementKind::Savepoint(name) => self.create_savepoint(&name),
            CliStatementKind::RollbackToSavepoint(name) => self.rollback_to_savepoint(&name),
            CliStatementKind::ReleaseSavepoint(name) => self.release_savepoint(&name),
            CliStatementKind::Rows => self.execute_read_query(query),
            CliStatementKind::Command => self.execute_write_query(query),
        }
    }

    fn begin_transaction(&mut self, isolation: Option<&str>) -> Result<(), String> {
        if self.transaction_active() {
            return Err("already in a transaction".to_string());
        }

        let tx = match isolation {
            Some(level) => {
                let isolation =
                    crate::executor::dispatch::transaction::parse_isolation_level(level)
                        .map_err(|error| error.to_string())?;
                self.db
                    .begin_with_isolation(isolation)
                    .map_err(|error| error.to_string())?
            }
            None => self.db.begin().map_err(|error| error.to_string())?,
        };
        self.tx = Some(tx);

        if self.json_output {
            println!(r#"{{"transaction":"started"}}"#);
        } else {
            println!("\x1b[1;32mTransaction started\x1b[0m");
        }
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<(), String> {
        if !self.transaction_active() {
            return Err("not in a transaction".to_string());
        }

        let mut tx = self.tx.take().expect("active transaction has a handle");
        if let Err(error) = tx.commit() {
            if tx.is_active() {
                self.tx = Some(tx);
            }
            return Err(error.to_string());
        }
        if self.json_output {
            println!(r#"{{"transaction":"committed"}}"#);
        } else {
            println!("\x1b[1;32mTransaction committed\x1b[0m");
        }
        Ok(())
    }

    fn rollback_transaction(&mut self) -> Result<(), String> {
        if !self.transaction_active() {
            return Err("not in a transaction".to_string());
        }

        let mut tx = self.tx.take().expect("active transaction has a handle");
        tx.rollback().map_err(|error| error.to_string())?;
        if self.json_output {
            println!(r#"{{"transaction":"rolled_back"}}"#);
        } else {
            println!("\x1b[1;33mTransaction rolled back\x1b[0m");
        }
        Ok(())
    }

    fn transaction_active(&self) -> bool {
        self.tx.as_ref().is_some_and(ApiTransaction::is_active)
    }

    fn create_savepoint(&mut self, name: &str) -> Result<(), String> {
        self.tx
            .as_mut()
            .ok_or_else(|| "not in a transaction".to_string())?
            .savepoint(name)
            .map_err(|error| error.to_string())
    }

    fn rollback_to_savepoint(&mut self, name: &str) -> Result<(), String> {
        self.tx
            .as_mut()
            .ok_or_else(|| "not in a transaction".to_string())?
            .rollback_to_savepoint(name)
            .map_err(|error| error.to_string())
    }

    fn release_savepoint(&mut self, name: &str) -> Result<(), String> {
        self.tx
            .as_mut()
            .ok_or_else(|| "not in a transaction".to_string())?
            .release_savepoint(name)
            .map_err(|error| error.to_string())
    }

    fn execute_read_query(&mut self, query: &str) -> Result<(), String> {
        let rows_result = if self.transaction_active() {
            if let Some(ref mut tx) = self.tx {
                if self.timeout_ms > 0 {
                    tx.query_with_timeout(query, (), self.timeout_ms)
                        .map_err(|e| e.to_string())?
                } else {
                    tx.query(query, ()).map_err(|e| e.to_string())?
                }
            } else {
                return Err("Transaction not available".to_string());
            }
        } else if self.timeout_ms > 0 {
            self.db
                .query_with_timeout(query, (), self.timeout_ms)
                .map_err(|e| e.to_string())?
        } else {
            self.db.query(query, ()).map_err(|e| e.to_string())?
        };

        let columns: Vec<String> = rows_result.columns().to_vec();

        let (all_rows, row_count) = collect_cli_rows(rows_result, self.limit)?;

        if self.json_output {
            self.output_json(&columns, &all_rows, row_count)?;
        } else {
            self.output_table(&columns, &all_rows, row_count)?;
        }

        Ok(())
    }

    fn execute_write_query(&mut self, query: &str) -> Result<(), String> {
        let rows_affected = if self.transaction_active() {
            if let Some(ref mut tx) = self.tx {
                if self.timeout_ms > 0 {
                    tx.execute_with_timeout(query, (), self.timeout_ms)
                        .map_err(|e| e.to_string())?
                } else {
                    tx.execute(query, ()).map_err(|e| e.to_string())?
                }
            } else {
                return Err("Transaction not available".to_string());
            }
        } else if self.timeout_ms > 0 {
            self.db
                .execute_with_timeout(query, (), self.timeout_ms)
                .map_err(|e| e.to_string())?
        } else {
            self.db.execute(query, ()).map_err(|e| e.to_string())?
        };

        if self.json_output {
            println!(r#"{{"rows_affected":{}}}"#, rows_affected);
        } else {
            let row_text = if rows_affected == 1 { "row" } else { "rows" };
            println!("\x1b[1;32m{} {} affected\x1b[0m", rows_affected, row_text);
        }

        Ok(())
    }

    fn output_json(
        &self,
        columns: &[String],
        rows: &[Vec<Value>],
        row_count: usize,
    ) -> Result<(), String> {
        output_json(columns, rows, row_count, self.limit)
    }

    fn output_explain_plan(&self, rows: &[Vec<Value>], row_count: usize) -> Result<(), String> {
        println!();
        for row in rows {
            if let Some(Value::Text(line)) = row.first() {
                println!("{}", colorize_plan_line(line));
            }
        }
        println!();
        let line_text = if row_count == 1 { "line" } else { "lines" };
        println!("\x1b[2m({} {} in plan)\x1b[0m", row_count, line_text);
        Ok(())
    }

    fn output_table(
        &self,
        columns: &[String],
        rows: &[Vec<Value>],
        row_count: usize,
    ) -> Result<(), String> {
        // Render EXPLAIN output without table borders, with color highlighting.
        // Verify first row starts with a known statement keyword to avoid false positives
        // from queries that alias a column as "plan".
        if columns.len() == 1 && columns[0] == "plan" && is_explain_output(rows) {
            return self.output_explain_plan(rows, row_count);
        }

        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL_CONDENSED)
            .set_content_arrangement(ContentArrangement::Dynamic);

        // Add header
        table.set_header(columns.iter().map(Cell::new));

        // Smart truncation with limit
        if self.limit > 0 && row_count > self.limit {
            let top_rows = self.limit / 2;

            // Add top rows
            for row in rows.iter().take(top_rows) {
                table.add_row(row.iter().map(|v| Cell::new(format_value(v))));
            }

            // Add truncation indicator
            let hidden_rows = row_count - self.limit;
            let mut truncation_row: Vec<Cell> = Vec::new();
            let message = format!("... ({} more rows) ...", hidden_rows);
            for (i, _) in columns.iter().enumerate() {
                if i == columns.len() / 2 {
                    truncation_row.push(Cell::new(&message));
                } else {
                    truncation_row.push(Cell::new(""));
                }
            }
            table.add_row(truncation_row);

            // Add bottom rows
            for row in rows.iter().skip(top_rows) {
                table.add_row(row.iter().map(|v| Cell::new(format_value(v))));
            }
        } else {
            // Add all rows
            for row in rows {
                table.add_row(row.iter().map(|v| Cell::new(format_value(v))));
            }
        }

        println!("{table}");

        // Print summary
        let row_text = if row_count == 1 { "row" } else { "rows" };
        if self.limit > 0 && row_count > self.limit {
            println!(
                "\x1b[1;32m{} {} in set (showing {})\x1b[0m",
                row_count, row_text, self.limit
            );
        } else {
            println!("\x1b[1;32m{} {} in set\x1b[0m", row_count, row_text);
        }

        Ok(())
    }

    fn print_help(&self) {
        println!("\x1b[1mRadixDB SQL CLI Commands:\x1b[0m");
        println!();
        println!("  \x1b[1;33mSQL Commands:\x1b[0m");
        println!("    SELECT ...             Execute a SELECT query");
        println!("    INSERT ...             Insert data into a table");
        println!("    UPDATE ...             Update data in a table");
        println!("    DELETE ...             Delete data from a table");
        println!("    CREATE TABLE ...       Create a new table");
        println!("    CREATE INDEX ...       Create an index on a column");
        println!("    ALTER TABLE ...        Modify table schema");
        println!("    SHOW TABLES            List all tables");
        println!("    SHOW CREATE TABLE ...  Show CREATE TABLE statement for a table");
        println!("    SHOW INDEXES FROM ...  Show indexes for a table");
        println!();
        println!("  \x1b[1;33mTransaction Commands:\x1b[0m");
        println!("    BEGIN [ISOLATION LEVEL READ COMMITTED|SNAPSHOT]");
        println!("                           Start a transaction at the selected isolation");
        println!("    COMMIT                 Commit the current transaction");
        println!("    ROLLBACK               Rollback the current transaction");
        println!("    SAVEPOINT name         Create a transaction savepoint");
        println!("    ROLLBACK TO [SAVEPOINT] name");
        println!("                           Roll back to a savepoint");
        println!("    RELEASE [SAVEPOINT] name");
        println!("                           Release a savepoint");
        println!();
        println!("  \x1b[1;33mPRAGMA Commands:\x1b[0m");
        println!("    PRAGMA CHECKPOINT      Seal hot data, publish manifests, truncate WAL");
        println!("    PRAGMA SNAPSHOT        Create a physical generation snapshot");
        println!("    PRAGMA RESTORE         Restore from latest backup snapshot");
        println!("    PRAGMA ISOLATION_LEVEL Read this connection's transaction default");
        println!("    PRAGMA key[=value]     Read/set known runtime settings");
        println!("                           snapshot_interval, keep_snapshots,");
        println!("                           compact_threshold, target_volume_rows,");
        println!("                           volume_cache_bytes, read_queue_depth");
        println!("    Unknown PRAGMA names fail with an explicit error");
        println!();
        println!("  \x1b[1;33mSpecial Commands:\x1b[0m");
        println!("    exit, quit, \\q         Exit the CLI");
        println!("    help, \\h, \\?          Show this help message");
        println!();
        println!("  \x1b[1;33mKeyboard Shortcuts:\x1b[0m");
        println!("    Up/Down arrow keys     Navigate command history");
        println!("    Ctrl+R                 Search command history");
        println!("    Ctrl+A                 Move cursor to beginning of line");
        println!("    Ctrl+E                 Move cursor to end of line");
        println!("    Ctrl+W                 Delete word before cursor");
        println!("    Ctrl+U                 Delete from cursor to beginning of line");
        println!("    Ctrl+K                 Delete from cursor to end of line");
        println!("    Ctrl+L                 Clear screen");
        println!();
    }
}

fn history_file_path() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".radixdb_history"))
}

/// Build DSN with query parameters from CLI args.
/// Durability-affecting options are admitted before the database is opened.
fn build_dsn(args: &Args) -> Result<String, String> {
    let mut dsn = args.db_path.clone();

    // Only add params for file:// databases
    if !dsn.starts_with("file://") {
        return Ok(dsn);
    }

    let profile_sync = args
        .persistence_profile
        .as_deref()
        .map(|profile| match profile.to_ascii_lowercase().as_str() {
            "fast" => Ok("none"),
            "normal" => Ok("normal"),
            "durable" => Ok("full"),
            _ => Err(format!(
                "unknown persistence profile '{profile}'; expected fast, normal, or durable"
            )),
        })
        .transpose()?;
    let explicit_sync = args
        .sync_mode
        .as_deref()
        .map(|sync| match sync.to_ascii_lowercase().as_str() {
            "none" | "off" => Ok("none"),
            "normal" => Ok("normal"),
            "full" => Ok("full"),
            _ => Err(format!(
                "unknown sync mode '{sync}'; expected none, normal, or full"
            )),
        })
        .transpose()?;
    if let Some(sync_mode) = explicit_sync.or(profile_sync) {
        dsn = replace_file_dsn_option(&dsn, "sync_mode", sync_mode)?;
    }

    let mut params = Vec::new();

    // Checkpoint interval
    if let Some(interval) = args.checkpoint_interval {
        params.push(format!("checkpoint_interval={}", interval));
    }

    // Compact threshold
    if let Some(count) = args.compact_threshold {
        params.push(format!("compact_threshold={}", count));
    }

    // Resident volume payload cache budget (convert MB to bytes)
    if let Some(mb) = args.volume_cache_size {
        params.push(format!("volume_cache_bytes={}", mb as u64 * 1024 * 1024));
    }

    // WAL max size (convert MB to bytes)
    if let Some(mb) = args.wal_max_size {
        params.push(format!("wal_max_size={}", mb as u64 * 1024 * 1024));
    }

    // Compression
    if let Some(ref comp) = args.compression {
        match comp.to_lowercase().as_str() {
            "on" | "true" | "1" | "yes" => params.push("compression=on".to_string()),
            "off" | "false" | "0" | "no" => params.push("compression=off".to_string()),
            _ => {
                return Err(format!(
                    "unknown compression value '{comp}'; expected on or off"
                ));
            }
        }
    }

    // Keep snapshots
    if let Some(count) = args.keep_snapshots {
        params.push(format!("keep_snapshots={}", count));
    }

    // Checkpoint on close
    if args.no_checkpoint_on_close {
        params.push("checkpoint_on_close=off".to_string());
    }

    // Append params to DSN
    if !params.is_empty() {
        let separator = if dsn.contains('?') { "&" } else { "?" };
        dsn.push_str(separator);
        dsn.push_str(&params.join("&"));
    }

    Ok(dsn)
}

fn replace_file_dsn_option(dsn: &str, key: &str, value: &str) -> Result<String, String> {
    let (base, query) = dsn.split_once('?').unwrap_or((dsn, ""));
    let mut seen = HashSet::new();
    let mut options = Vec::new();
    for option in query.split('&').filter(|option| !option.is_empty()) {
        let option_key = option.split_once('=').map_or(option, |(name, _)| name);
        if !seen.insert(option_key) {
            return Err(format!("duplicate file database option: '{option_key}'"));
        }
        if option_key != key {
            options.push(option.to_string());
        }
    }
    options.push(format!("{key}={value}"));
    Ok(format!("{base}?{}", options.join("&")))
}

fn finish_database<T, E: std::fmt::Display>(
    db: &Database,
    outcome: Result<T, E>,
) -> Result<T, String> {
    let close = db.close();
    match (outcome, close) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(close_error)) => Err(format!(
            "operation completed but terminal database close failed; durability outcome is unknown: {close_error}"
        )),
        (Err(operation_error), Ok(())) => Err(operation_error.to_string()),
        (Err(operation_error), Err(close_error)) => Err(format!(
            "operation failed: {operation_error}; terminal database close also failed: {close_error}"
        )),
    }
}

fn sync_cli_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn export_sql_to_destination(
    database: &Database,
    destination: &str,
) -> Result<SqlDumpSummary, String> {
    if destination == "-" {
        let stdout = io::stdout();
        let mut writer = BufWriter::new(stdout.lock());
        let summary = export_sql_dump(database, &mut writer).map_err(|error| error.to_string())?;
        writer.flush().map_err(|error| error.to_string())?;
        return Ok(summary);
    }
    export_sql_dump_to_file(database, Path::new(destination)).map_err(|error| error.to_string())
}

fn import_sql_from_source(
    target_dsn: &str,
    target: &Path,
    source: &str,
) -> Result<SqlDumpSummary, String> {
    if source == "-" {
        let stdin = io::stdin();
        import_sql_dump_to_new_database(target_dsn, target, BufReader::new(stdin.lock()))
            .map_err(|error| error.to_string())
    } else {
        let file = File::open(source)
            .map_err(|error| format!("cannot open SQL dump '{source}': {error}"))?;
        import_sql_dump_to_new_database(target_dsn, target, BufReader::new(file))
            .map_err(|error| error.to_string())
    }
}

const RESETTABLE_STORAGE_MEMBERS: [&str; 8] = [
    "CONTROL.0",
    "CONTROL.1",
    "catalog",
    "manifests",
    "artifacts",
    "wal",
    "staging",
    "quarantine",
];

fn reset_storage_generation(dir: &Path) -> io::Result<()> {
    reset_storage_generation_with_hook(dir, |_| Ok(()))
}

fn storage_member_exists(path: &Path) -> io::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn reset_storage_generation_with_hook<F>(dir: &Path, mut after_stage: F) -> io::Result<()>
where
    F: FnMut(usize) -> io::Result<()>,
{
    std::fs::create_dir_all(dir)?;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut staged: Vec<(PathBuf, PathBuf)> = Vec::new();

    let rollback = |staged: &mut Vec<(PathBuf, PathBuf)>| -> io::Result<()> {
        let mut failures = Vec::new();
        for (original, quarantine) in staged.drain(..).rev() {
            let quarantine_exists = storage_member_exists(&quarantine);
            let original_exists = storage_member_exists(&original);
            match (quarantine_exists, original_exists) {
                (Err(error), _) => failures.push(format!(
                    "inspect quarantine '{}' before rollback: {error}",
                    quarantine.display()
                )),
                (_, Err(error)) => failures.push(format!(
                    "inspect original '{}' before rollback: {error}",
                    original.display()
                )),
                (Ok(true), Ok(false)) => {
                    if let Err(error) = std::fs::rename(&quarantine, &original) {
                        failures.push(format!(
                            "restore '{}' from '{}': {error}",
                            original.display(),
                            quarantine.display()
                        ));
                    }
                }
                (Ok(true), Ok(true)) => failures.push(format!(
                    "cannot restore '{}' because both it and quarantine '{}' exist",
                    original.display(),
                    quarantine.display()
                )),
                (Ok(false), Ok(false)) => failures.push(format!(
                    "cannot restore '{}' because quarantine '{}' is missing",
                    original.display(),
                    quarantine.display()
                )),
                (Ok(false), Ok(true)) => {}
            }
        }
        if let Err(error) = sync_cli_directory(dir) {
            failures.push(format!(
                "sync reset root '{}' after rollback: {error}",
                dir.display()
            ));
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(io::Error::other(failures.join("; ")))
        }
    };

    let fail_with_rollback = |primary: io::Error,
                              staged: &mut Vec<(PathBuf, PathBuf)>|
     -> io::Error {
        match rollback(staged) {
                Ok(()) => primary,
                Err(rollback_error) => io::Error::other(format!(
                    "reset transition failed ({primary}); rollback also failed and namespace state may be mixed ({rollback_error})"
                )),
            }
    };

    for member in RESETTABLE_STORAGE_MEMBERS {
        let original = dir.join(member);
        if !storage_member_exists(&original)? {
            continue;
        }
        let quarantine = dir.join(format!(
            ".radixdb-reset-{member}-{}-{nonce}",
            std::process::id()
        ));
        if let Err(error) = std::fs::rename(&original, &quarantine) {
            return Err(fail_with_rollback(error, &mut staged));
        }
        staged.push((original, quarantine));
        if let Err(error) = after_stage(staged.len()) {
            return Err(fail_with_rollback(error, &mut staged));
        }
    }

    if let Err(error) = sync_cli_directory(dir) {
        return Err(fail_with_rollback(error, &mut staged));
    }

    // The namespace transition is committed. Quarantine cleanup is not part
    // of the reset outcome and may be retried manually without risking a
    // mixed catalog/artifact/WAL generation.
    for (_, quarantine) in staged {
        let removal = match std::fs::symlink_metadata(&quarantine) {
            Ok(metadata) if metadata.file_type().is_dir() => std::fs::remove_dir_all(&quarantine),
            Ok(_) => std::fs::remove_file(&quarantine),
            Err(error) => Err(error),
        };
        if let Err(error) = removal {
            eprintln!(
                "Warning: reset committed but quarantine '{}' could not be removed: {error}",
                quarantine.display()
            );
        }
    }
    Ok(())
}

/// Print persistence configuration info
fn effective_sync_description(db: &Database) -> Result<&'static str, String> {
    let mode = db
        .query_one::<i64, _>("PRAGMA sync_mode", ())
        .map_err(|error| format!("cannot read effective sync_mode: {error}"))?;
    match mode {
        0 => Ok("none (fastest, less durable)"),
        1 => Ok("normal (balanced)"),
        2 => Ok("full (slowest, most durable)"),
        value => Err(format!("engine returned unknown sync_mode value {value}")),
    }
}

fn print_persistence_info(db: &Database, args: &Args) -> Result<(), String> {
    let sync_desc = effective_sync_description(db)?;

    println!("Persistence: WAL sync mode = {}", sync_desc);

    if let Some(interval) = args.checkpoint_interval {
        println!("Persistence: Checkpoint interval = {}s", interval);
    };

    if let Some(count) = args.compact_threshold {
        println!("Persistence: Compact threshold = {}", count);
    }

    if let Some(mb) = args.volume_cache_size {
        println!("Persistence: Volume payload cache budget = {}MB", mb);
    }

    if let Some(count) = args.keep_snapshots {
        println!("Persistence: Keep snapshots = {}", count);
    }

    if let Some(mb) = args.wal_max_size {
        println!("Persistence: WAL max size = {}MB", mb);
    }

    if let Some(ref comp) = args.compression {
        println!("Persistence: Compression = {}", comp);
    }

    if args.no_checkpoint_on_close {
        println!("Persistence: Checkpoint on close = off");
    }
    Ok(())
}

fn validate_cli_args(args: &Args) -> Result<(), String> {
    if args.reset_storage && !args.db_path.starts_with("file://") {
        return Err("--reset-storage requires a file:// database".to_string());
    }
    if args.import_sql.is_some() && !args.db_path.starts_with("file://") {
        return Err("--import-sql requires a file:// database".to_string());
    }
    Ok(())
}

/// Run the CLI process contract using the current process arguments and I/O.
///
/// The returned value is a process exit code; the binary owns mapping it to
/// `std::process::ExitCode`.
pub fn run_from_env() -> u8 {
    let args = Args::parse();

    if args.physical_format {
        let format = radixdb_storage::v6::FORMAT_VERSION;
        println!("{}.{}", format.major(), format.minor());
        return 0;
    }

    if let Some(snapshot_directory) = &args.inspect_snapshot {
        let path = std::path::Path::new(snapshot_directory);
        match radixdb_storage::v6::open_snapshot_manifest(path) {
            Ok(manifest) => {
                let format = radixdb_storage::v6::FORMAT_VERSION;
                println!(
                    "{}",
                    serde_json::json!({
                        "snapshot_id": manifest.snapshot_id().to_string(),
                        "database_id": manifest.database_id().to_string(),
                        "physical_format": format!("{}.{}", format.major(), format.minor()),
                    })
                );
                return 0;
            }
            Err(error) => {
                eprintln!("Error inspecting snapshot: {error}");
                return 1;
            }
        }
    }

    if let Err(error) = validate_cli_args(&args) {
        eprintln!("Error: {error}");
        return 1;
    }

    // Build the DSN with optional query parameters
    let db_path = match build_dsn(&args) {
        Ok(dsn) => dsn,
        Err(error) => {
            eprintln!("Error: {error}");
            return 1;
        }
    };

    // ── Filesystem-level operations (before Database::open) ────────
    // These work directly on disk so they can recover from broken state.

    let db_dir = if let Some(path) = db_path.strip_prefix("file://") {
        Some(std::path::PathBuf::from(
            path.split('?').next().unwrap_or(path),
        ))
    } else {
        None
    };

    // Standalone destructive recovery. Restore is deliberately excluded above:
    // its journaled engine path must retain the current generation until the
    // replacement has been fully staged and reopened.
    if args.reset_storage {
        if let Some(ref dir) = db_dir {
            let _offline_owner = match FileLock::acquire(dir) {
                Ok(owner) => owner,
                Err(e) => {
                    eprintln!("Error: cannot reset an active database: {e}");
                    return 1;
                }
            };
            if let Err(error) = reset_storage_generation(dir) {
                eprintln!("Error resetting the storage generation as one transition: {error}");
                return 1;
            }
            if !args.quiet {
                eprintln!("Reset persistent storage generation");
            }
        }
        return 0;
    }

    // Logical import never opens or mutates the requested target root. It
    // builds a separate sibling database and publishes it with RENAME_NOREPLACE
    // only after checksum, SQL execution, checkpoint and close all succeed.
    if let Some(ref source) = args.import_sql {
        let target = db_dir.as_ref().expect("validated file:// import target");
        match import_sql_from_source(&db_path, target, source) {
            Ok(summary) => {
                if !args.quiet {
                    eprintln!(
                        "Imported {} tables ({} statements), sha256 {}",
                        summary.tables, summary.statements, summary.sha256
                    );
                }
            }
            Err(error) => {
                eprintln!("Error importing logical SQL dump: {error}");
                return 1;
            }
        }
        return 0;
    }

    // Handle --restore through the engine-owned validated staging/swap path.
    if args.restore.is_some() {
        let dir = match &db_dir {
            Some(d) => d,
            None => {
                eprintln!("Error: --restore requires a file:// database");
                return 1;
            }
        };

        let snapshot_dir = dir.join("snapshots");
        if !snapshot_dir.exists() {
            eprintln!("Error: No snapshots/ directory found in {:?}", dir);
            eprintln!(
                "Create a backup first with: radixdb-cli -d {} --snapshot",
                db_path
            );
            return 1;
        }

        let requested = args.restore.as_deref().filter(|value| !value.is_empty());
        if !args.quiet {
            match requested {
                Some(snapshot_id) => eprintln!("[restore] Restoring snapshot {snapshot_id}"),
                None => eprintln!("[restore] Restoring latest committed generation"),
            }
        }

        let db = match Database::open(&db_path) {
            Ok(db) => db,
            Err(error) => {
                eprintln!("Error opening database: {error}");
                return 1;
            }
        };
        match finish_database(&db, db.restore_snapshot(requested)) {
            Ok(message) => println!("{message}"),
            Err(error) => {
                eprintln!("Error restoring snapshot: {error}");
                return 1;
            }
        }
        return 0;
    }

    // Standalone --reset-storage was handled above.

    // Open the database
    let db = match Database::open(&db_path) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error opening database: {}", e);
            return 1;
        }
    };

    // stdout is part of the dump contract for `--export-sql -`; no connection
    // banner or progress text may be mixed into it.
    if let Some(ref destination) = args.export_sql {
        let outcome = export_sql_to_destination(&db, destination);
        match finish_database(&db, outcome) {
            Ok(summary) => {
                if !args.quiet {
                    eprintln!(
                        "Exported {} tables / {} rows ({} statements), sha256 {}",
                        summary.tables, summary.rows, summary.statements, summary.sha256
                    );
                }
            }
            Err(error) => {
                eprintln!("Error exporting logical SQL dump: {error}");
                return 1;
            }
        }
        return 0;
    }

    if !args.quiet && !args.json_output {
        println!("Connected to database: {}", db_path);
        // Show persistence info for file databases
        if db_path.starts_with("file://") {
            if let Err(error) = print_persistence_info(&db, &args) {
                eprintln!("Error reading persistence configuration: {error}");
                let _ = db.close();
                return 1;
            }
        }
    }

    // Handle --snapshot: open DB, create backup, close, exit
    if args.snapshot {
        let outcome = match db.query("PRAGMA SNAPSHOT", ()) {
            Ok(mut rows) => {
                if let Some(Ok(row)) = rows.next() {
                    let snapshot_id = row.get::<String>(0).map_err(|error| error.to_string());
                    let database_id = row.get::<String>(1).map_err(|error| error.to_string());
                    let physical_format = row.get::<String>(2).map_err(|error| error.to_string());
                    match (snapshot_id, database_id, physical_format) {
                        (Ok(snapshot_id), Ok(database_id), Ok(physical_format)) => {
                            if args.json_output {
                                Ok(serde_json::json!({
                                    "snapshot_id": snapshot_id,
                                    "database_id": database_id,
                                    "physical_format": physical_format,
                                })
                                .to_string())
                            } else {
                                Ok(snapshot_id)
                            }
                        }
                        (Err(error), _, _) | (_, Err(error), _) | (_, _, Err(error)) => Err(error),
                    }
                } else {
                    Err("snapshot returned no terminal result".to_string())
                }
            }
            Err(error) => Err(error.to_string()),
        };
        match finish_database(&db, outcome) {
            Ok(message) => println!("{message}"),
            Err(error) => {
                eprintln!("Error creating snapshot: {error}");
                return 1;
            }
        }
        return 0;
    }

    // Handle execute flag - run single query and exit
    if let Some(ref sql) = args.execute {
        let result = execute_batch_input(
            &db,
            sql,
            args.json_output,
            args.quiet,
            args.limit,
            args.timeout_ms,
        );
        if let Err(e) = finish_database(&db, result) {
            eprintln!("Error: {}", e);
            return 1;
        }
        return 0;
    }

    // Handle file flag - execute SQL from file
    if let Some(ref filename) = args.file {
        let result = execute_from_file(
            &db,
            filename,
            args.json_output,
            args.quiet,
            args.limit,
            args.timeout_ms,
        );
        if let Err(e) = finish_database(&db, result) {
            eprintln!("Error: {}", e);
            return 1;
        }
        return 0;
    }

    // Check if we're getting input from a pipe
    let is_pipe = !std::io::stdin().is_terminal();

    if is_pipe {
        let result = execute_piped_input(
            &db,
            args.json_output,
            args.quiet,
            args.limit,
            args.timeout_ms,
        );
        if let Err(e) = finish_database(&db, result) {
            eprintln!("Error: {}", e);
            return 1;
        }
        return 0;
    }

    // Interactive mode
    let mut cli = match Cli::new(db, args.json_output, args.limit, args.timeout_ms) {
        Ok(cli) => cli,
        Err(e) => {
            eprintln!("Error initializing CLI: {}", e);
            return 1;
        }
    };

    let run_result = cli.run();
    let terminal_result = finish_database(&cli.db, run_result);
    drop(cli);
    if let Err(e) = terminal_result {
        eprintln!("Error: {}", e);
        return 1;
    }
    0
}

fn execute_from_file(
    db: &Database,
    filename: &str,
    json_output: bool,
    quiet: bool,
    row_limit: usize,
    timeout_ms: u64,
) -> Result<(), String> {
    let file =
        File::open(filename).map_err(|error| format!("Error reading file {filename}: {error}"))?;
    let input = read_bounded_cli_input(file, filename)?;
    execute_batch_input(db, &input, json_output, quiet, row_limit, timeout_ms)
}

fn execute_piped_input(
    db: &Database,
    json_output: bool,
    quiet: bool,
    row_limit: usize,
    timeout_ms: u64,
) -> Result<(), String> {
    let input = read_bounded_cli_input(io::stdin(), "stdin")?;
    execute_batch_input(db, &input, json_output, quiet, row_limit, timeout_ms)
}

fn execute_batch_input(
    db: &Database,
    input: &str,
    json_output: bool,
    quiet: bool,
    row_limit: usize,
    timeout_ms: u64,
) -> Result<(), String> {
    if input.len() > MAX_CLI_STATEMENT_BYTES {
        return Err(format!(
            "CLI input exceeds {MAX_CLI_STATEMENT_BYTES} byte safety budget"
        ));
    }
    let mut session = CliBatchSession::new(db, json_output, quiet, row_limit, timeout_ms);
    for statement in split_sql_statements(input)? {
        let statement = statement.trim();
        if statement.is_empty() {
            continue;
        }
        let start = Instant::now();
        if let Err(error) = session.execute(statement) {
            return match session.rollback_open_transaction() {
                Ok(()) => Err(error),
                Err(rollback) => Err(format!(
                    "{error}; rollback after statement failure also failed: {rollback}"
                )),
            };
        } else if !json_output && !quiet {
            println!("Query executed in {:?}", start.elapsed());
        }
    }
    session.finish()
}

struct CliBatchSession<'database> {
    db: &'database Database,
    transaction: Option<ApiTransaction>,
    json_output: bool,
    quiet: bool,
    row_limit: usize,
    timeout_ms: u64,
}

impl<'database> CliBatchSession<'database> {
    fn new(
        db: &'database Database,
        json_output: bool,
        quiet: bool,
        row_limit: usize,
        timeout_ms: u64,
    ) -> Self {
        Self {
            db,
            transaction: None,
            json_output,
            quiet,
            row_limit,
            timeout_ms,
        }
    }

    fn execute(&mut self, query: &str) -> Result<(), String> {
        let trimmed = query.trim();
        match trimmed.to_ascii_uppercase().as_str() {
            "HELP" | "\\H" | "\\?" => {
                print_help_main();
                return Ok(());
            }
            "EXIT" | "QUIT" | "\\Q" => return Err("exit requested".to_string()),
            _ => {}
        }

        reject_legacy_cli_params(query)?;
        match classify_cli_statement(query)? {
            CliStatementKind::Begin(isolation) => self.begin(isolation.as_deref()),
            CliStatementKind::Commit => self.commit(),
            CliStatementKind::Rollback => self.rollback(),
            CliStatementKind::Savepoint(name) => self.savepoint(&name),
            CliStatementKind::RollbackToSavepoint(name) => self.rollback_to_savepoint(&name),
            CliStatementKind::ReleaseSavepoint(name) => self.release_savepoint(&name),
            CliStatementKind::Rows => self.execute_rows(query),
            CliStatementKind::Command => self.execute_command(query),
        }
    }

    fn begin(&mut self, isolation: Option<&str>) -> Result<(), String> {
        if self.transaction_active() {
            return Err("batch transaction is already active".to_string());
        }
        self.transaction = Some(match isolation {
            Some(level) => {
                let isolation =
                    crate::executor::dispatch::transaction::parse_isolation_level(level)
                        .map_err(|error| error.to_string())?;
                self.db
                    .begin_with_isolation(isolation)
                    .map_err(|error| error.to_string())?
            }
            None => self.db.begin().map_err(|error| error.to_string())?,
        });
        Ok(())
    }

    fn savepoint(&mut self, name: &str) -> Result<(), String> {
        self.transaction
            .as_mut()
            .ok_or_else(|| "batch transaction is not active".to_string())?
            .savepoint(name)
            .map_err(|error| error.to_string())
    }

    fn rollback_to_savepoint(&mut self, name: &str) -> Result<(), String> {
        self.transaction
            .as_mut()
            .ok_or_else(|| "batch transaction is not active".to_string())?
            .rollback_to_savepoint(name)
            .map_err(|error| error.to_string())
    }

    fn release_savepoint(&mut self, name: &str) -> Result<(), String> {
        self.transaction
            .as_mut()
            .ok_or_else(|| "batch transaction is not active".to_string())?
            .release_savepoint(name)
            .map_err(|error| error.to_string())
    }

    fn commit(&mut self) -> Result<(), String> {
        let Some(mut transaction) = self.transaction.take() else {
            return Err("batch transaction is not active".to_string());
        };
        if let Err(error) = transaction.commit() {
            if transaction.is_active() {
                self.transaction = Some(transaction);
            }
            return Err(error.to_string());
        }
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), String> {
        let Some(mut transaction) = self.transaction.take() else {
            return Err("batch transaction is not active".to_string());
        };
        transaction.rollback().map_err(|error| error.to_string())
    }

    fn execute_rows(&mut self, query: &str) -> Result<(), String> {
        let rows_result = if let Some(transaction) = self.transaction.as_mut() {
            if self.timeout_ms > 0 {
                transaction
                    .query_with_timeout(query, (), self.timeout_ms)
                    .map_err(|error| error.to_string())?
            } else {
                transaction
                    .query(query, ())
                    .map_err(|error| error.to_string())?
            }
        } else if self.timeout_ms > 0 {
            self.db
                .query_with_timeout(query, (), self.timeout_ms)
                .map_err(|error| error.to_string())?
        } else {
            self.db
                .query(query, ())
                .map_err(|error| error.to_string())?
        };

        let columns: Vec<String> = rows_result.columns().to_vec();

        let (all_rows, row_count) = collect_cli_rows(rows_result, self.row_limit)?;

        if self.json_output {
            output_json(&columns, &all_rows, row_count, self.row_limit)?;
        } else {
            output_table(&columns, &all_rows, row_count, self.row_limit, self.quiet)?;
        }
        Ok(())
    }

    fn execute_command(&mut self, query: &str) -> Result<(), String> {
        let rows_affected = if let Some(transaction) = self.transaction.as_mut() {
            if self.timeout_ms > 0 {
                transaction
                    .execute_with_timeout(query, (), self.timeout_ms)
                    .map_err(|error| error.to_string())?
            } else {
                transaction
                    .execute(query, ())
                    .map_err(|error| error.to_string())?
            }
        } else if self.timeout_ms > 0 {
            self.db
                .execute_with_timeout(query, (), self.timeout_ms)
                .map_err(|error| error.to_string())?
        } else {
            self.db
                .execute(query, ())
                .map_err(|error| error.to_string())?
        };

        if self.json_output {
            println!(r#"{{"rows_affected":{}}}"#, rows_affected);
        } else if !self.quiet {
            println!("{} rows affected", rows_affected);
        }
        Ok(())
    }

    fn transaction_active(&self) -> bool {
        self.transaction
            .as_ref()
            .is_some_and(ApiTransaction::is_active)
    }

    fn rollback_open_transaction(&mut self) -> Result<(), String> {
        let Some(mut transaction) = self.transaction.take() else {
            return Ok(());
        };
        transaction.rollback().map_err(|error| error.to_string())
    }

    fn finish(mut self) -> Result<(), String> {
        if !self.transaction_active() {
            return Ok(());
        }
        self.rollback_open_transaction().map_err(|error| {
            format!("batch ended with an open transaction and rollback failed: {error}")
        })?;
        Err("batch ended with an open transaction; changes were rolled back".to_string())
    }
}

fn reject_legacy_cli_params(query: &str) -> Result<(), String> {
    let mut lexer = Lexer::new(query);
    loop {
        let token = lexer.next_token();
        if token.token_type == TokenType::Comment
            && token
                .literal
                .trim_start_matches('-')
                .trim_start()
                .to_ascii_uppercase()
                .starts_with("PARAMS:")
        {
            return Err("CLI `-- PARAMS:` was removed because it could not preserve typed scalar identity; use the Rust or TCP parameter API".to_string());
        }
        if token.is_eof() || token.is_error() {
            return Ok(());
        }
    }
}

fn json_query_result(
    columns: &[String],
    rows: &[Vec<Value>],
    row_count: usize,
    row_limit: usize,
) -> serde_json::Value {
    let json_rows: Vec<Vec<serde_json::Value>> = rows
        .iter()
        .map(|row| row.iter().map(value_to_json).collect())
        .collect();
    let retained_count = rows.len();
    let truncated = retained_count < row_count;
    let head_count = if truncated {
        retained_count / 2
    } else {
        retained_count
    };
    let tail_count = retained_count.saturating_sub(head_count);

    serde_json::json!({
        "columns": columns,
        "rows": json_rows,
        "count": row_count,
        "retained_count": retained_count,
        "omitted_count": row_count.saturating_sub(retained_count),
        "truncated": truncated,
        "window": {
            "kind": if truncated { "head_tail" } else { "complete" },
            "head_count": head_count,
            "tail_count": tail_count,
            "requested_limit": row_limit,
        }
    })
}

fn output_json(
    columns: &[String],
    rows: &[Vec<Value>],
    row_count: usize,
    row_limit: usize,
) -> Result<(), String> {
    let result = json_query_result(columns, rows, row_count, row_limit);

    println!(
        "{}",
        serde_json::to_string(&result).map_err(|e| e.to_string())?
    );
    Ok(())
}

fn output_table(
    columns: &[String],
    rows: &[Vec<Value>],
    row_count: usize,
    row_limit: usize,
    quiet: bool,
) -> Result<(), String> {
    // Print the column names
    for (i, column) in columns.iter().enumerate() {
        if i > 0 {
            print!(" | ");
        }
        print!("{}", column);
    }
    println!();

    // Print a separator
    for (i, _) in columns.iter().enumerate() {
        if i > 0 {
            print!("-+-");
        }
        print!("----");
    }
    println!();

    // Display rows with smart truncation
    if row_limit == 0 || row_count <= row_limit {
        for row in rows {
            for (i, value) in row.iter().enumerate() {
                if i > 0 {
                    print!(" | ");
                }
                print!("{}", format_value(value));
            }
            println!();
        }

        if !quiet {
            println!("{} rows in set", row_count);
        }
    } else {
        // Smart truncation
        let top_rows = row_limit / 2;

        // Show top rows
        for row in rows.iter().take(top_rows) {
            for (i, value) in row.iter().enumerate() {
                if i > 0 {
                    print!(" | ");
                }
                print!("{}", format_value(value));
            }
            println!();
        }

        // Show truncation indicator
        let hidden_rows = row_count - row_limit;
        println!();
        println!("    \x1b[2m... ({} more rows) ...\x1b[0m", hidden_rows);
        println!();

        // Show bottom rows
        for row in rows.iter().skip(top_rows) {
            for (i, value) in row.iter().enumerate() {
                if i > 0 {
                    print!(" | ");
                }
                print!("{}", format_value(value));
            }
            println!();
        }

        if !quiet {
            println!("{} rows in set (showing {})", row_count, row_limit);
        }
    }

    Ok(())
}

// ============================================================================
// EXPLAIN Plan Colorization
// ============================================================================

// ANSI color codes for EXPLAIN plan output (256-color, high contrast)
const C_HEADER: &str = "\x1b[1;4m"; // Bold + underline for headers
const C_DIM: &str = "\x1b[38;5;245m"; // Medium gray for arrows
const C_RESET: &str = "\x1b[0m";
const C_INDEX: &str = "\x1b[1;38;5;46m"; // Bright green for index access
const C_SEQ: &str = "\x1b[1;38;5;208m"; // Orange for seq scan warning
const C_JOIN: &str = "\x1b[1;38;5;75m"; // Light blue for join operators
const C_STATS: &str = "\x1b[1;38;5;196m"; // Bright pure red for timing/row stats
const C_LABEL: &str = "\x1b[38;5;117m"; // Sky blue for labels
const C_SUBIDX: &str = "\x1b[38;5;114m"; // Medium green for sub-index details
const C_CTE: &str = "\x1b[38;5;183m"; // Lavender for CTE/subquery scans

/// Check if rows look like EXPLAIN plan output (not a regular query with a "plan" column)
fn is_explain_output(rows: &[Vec<Value>]) -> bool {
    if let Some(first_row) = rows.first() {
        if let Some(Value::Text(line)) = first_row.first() {
            let trimmed = line.trim();
            return trimmed.starts_with("SELECT")
                || trimmed.starts_with("INSERT")
                || trimmed.starts_with("UPDATE")
                || trimmed.starts_with("DELETE")
                || trimmed.starts_with("WITH ");
        }
    }
    false
}

/// Colorize a single EXPLAIN plan line with ANSI escape codes
fn colorize_plan_line(line: &str) -> String {
    let trimmed = line.trim_start();
    let indent = &line[..line.len() - trimmed.len()];

    let mut result = String::with_capacity(line.len() + 80);
    result.push_str(indent);

    if trimmed.starts_with("-> ") {
        colorize_node(trimmed, &mut result);
    } else if trimmed.starts_with("SELECT")
        || trimmed.starts_with("INSERT")
        || trimmed.starts_with("UPDATE")
        || trimmed.starts_with("DELETE")
    {
        colorize_header(trimmed, &mut result);
    } else if trimmed.starts_with("WITH ") || trimmed.starts_with("Statement:") {
        result.push_str(C_HEADER);
        result.push_str(trimmed);
        result.push_str(C_RESET);
    } else {
        colorize_detail(trimmed, &mut result);
    }

    result
}

/// Split a line into the main content and trailing stats parenthetical.
/// Stats always start with "(actual " or "(cost=".
fn split_stats(line: &str) -> (&str, Option<&str>) {
    for marker in &["(actual ", "(cost="] {
        if let Some(pos) = line.rfind(marker) {
            return (line[..pos].trim_end(), Some(&line[pos..]));
        }
    }
    (line, None)
}

/// Colorize a node line starting with "-> "
fn colorize_node(line: &str, result: &mut String) {
    let after_arrow = &line[3..]; // skip "-> "

    // Dim arrow
    result.push_str(C_DIM);
    result.push_str("->");
    result.push_str(C_RESET);
    result.push(' ');

    // Indexed access patterns (green)
    let indexed = [
        "Index Scan",
        "PK Lookup",
        "Multi-Index Scan",
        "Composite Index Scan",
        "Index Nested Loop",
    ];
    // Sequential scan patterns (orange warning)
    let sequential = ["Seq Scan", "Parallel Seq Scan"];
    // Join operator patterns (blue)
    let joins = ["Hash Join", "Merge Join", "Nested Loop ("];

    let color = if indexed.iter().any(|p| after_arrow.starts_with(p)) {
        C_INDEX
    } else if sequential.iter().any(|p| after_arrow.starts_with(p)) {
        C_SEQ
    } else if joins.iter().any(|p| after_arrow.starts_with(p)) {
        C_JOIN
    } else if after_arrow.starts_with("Subquery Scan") || after_arrow.starts_with("CTE Scan") {
        C_CTE
    } else {
        // Sub-index detail lines like "idx_cat on category: = Electronics"
        result.push_str(C_SUBIDX);
        result.push_str(after_arrow);
        result.push_str(C_RESET);
        return;
    };

    let (scan_part, stats_part) = split_stats(after_arrow);
    result.push_str(color);
    result.push_str(scan_part);
    result.push_str(C_RESET);
    if let Some(stats) = stats_part {
        result.push(' ');
        result.push_str(C_STATS);
        result.push_str(stats);
        result.push_str(C_RESET);
    }
}

/// Colorize a statement header line (SELECT, INSERT, UPDATE, DELETE)
fn colorize_header(line: &str, result: &mut String) {
    let (scan_part, stats_part) = split_stats(line);
    result.push_str(C_HEADER);
    result.push_str(scan_part);
    result.push_str(C_RESET);
    if let Some(stats) = stats_part {
        result.push(' ');
        result.push_str(C_STATS);
        result.push_str(stats);
        result.push_str(C_RESET);
    }
}

/// Colorize a detail line (labels like Columns:, Filter:, Group By:, etc.)
fn colorize_detail(line: &str, result: &mut String) {
    let labels = [
        "Columns:",
        "Filter:",
        "Index Cond:",
        "Join Cond:",
        "Group By:",
        "Order By:",
        "Having:",
        "Limit:",
        "Offset:",
        "Using:",
        "Alias:",
        "Values:",
        "Set:",
        "Source:",
    ];

    for label in &labels {
        if let Some(rest) = line.strip_prefix(label) {
            result.push_str(C_LABEL);
            result.push_str(label);
            result.push_str(C_RESET);
            result.push_str(rest);
            return;
        }
    }

    // No recognized label — render as-is
    result.push_str(line);
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null(_) => "NULL".to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => format_float(*f),
        Value::Text(s) => s.to_string(),
        Value::Boolean(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Timestamp(ts) => format_timestamp(ts),
        Value::Extension(data) if data.first() == Some(&(crate::DataType::Json as u8)) => {
            std::str::from_utf8(&data[1..]).unwrap_or("").to_string()
        }
        Value::Extension(data) if data.first() == Some(&(crate::DataType::Vector as u8)) => {
            crate::core::value::format_vector_bytes(&data[1..])
        }
        Value::Extension(data) if data.first() == Some(&(crate::DataType::Uuid as u8)) => {
            crate::core::value::format_uuid_bytes(&data[1..])
                .unwrap_or_else(|| "<invalid-uuid>".to_string())
        }
        Value::Extension(data)
            if matches!(
                data.first().and_then(|tag| DataType::from_u8(*tag)),
                Some(DataType::Decimal | DataType::Date | DataType::Bytes)
            ) =>
        {
            value.to_string()
        }
        Value::Extension(data) => format!(
            "<extension:{}:{}>",
            data.first().copied().unwrap_or_default(),
            bytes_to_hex(data.get(1..).unwrap_or_default())
        ),
    }
}

/// Format a timestamp preserving sub-second precision.
/// Omits fractional part when zero, trims trailing zeros otherwise.
fn format_timestamp(ts: &chrono::DateTime<chrono::Utc>) -> String {
    let nanos = ts.timestamp_subsec_nanos();
    if nanos == 0 {
        return ts.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    }
    // Format with 9-digit nanoseconds, then trim trailing zeros
    let s = ts.format("%Y-%m-%dT%H:%M:%S.%9fZ").to_string();
    // Find the 'Z' and trim zeros before it
    if let Some(z_pos) = s.rfind('Z') {
        let trimmed = s[..z_pos].trim_end_matches('0');
        format!("{}Z", trimmed)
    } else {
        s
    }
}

/// Format a float value consistently, using scientific notation for extreme values
fn format_float(v: f64) -> String {
    // Handle special cases
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v.is_sign_positive() {
            "Infinity"
        } else {
            "-Infinity"
        }
        .to_string();
    }

    // Rust's shortest representation round-trips to the exact same f64.
    // Retain a Float marker for integral values so copied table output cannot
    // silently change the scalar family to INTEGER.
    let rendered = v.to_string();
    if rendered.contains(['.', 'e', 'E']) {
        rendered
    } else {
        format!("{rendered}.0")
    }
}

fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null(data_type) => serde_json::json!({
            "type": "NULL",
            "data_type": format!("{data_type:?}"),
            "value": serde_json::Value::Null,
        }),
        Value::Integer(integer) => serde_json::json!({
            "type": "INTEGER",
            "value": integer.to_string(),
        }),
        Value::Float(float) => serde_json::json!({
            "type": "FLOAT",
            "value": float.to_string(),
            "bits": format!("{:016x}", float.to_bits()),
        }),
        Value::Text(text) => serde_json::json!({"type": "TEXT", "value": text.as_str()}),
        Value::Boolean(boolean) => {
            serde_json::json!({"type": "BOOLEAN", "value": boolean})
        }
        Value::Timestamp(timestamp) => serde_json::json!({
            "type": "TIMESTAMP",
            "nanos_since_unix_epoch_utc": timestamp
                .timestamp_nanos_opt()
                .map(|nanos| nanos.to_string()),
            "display": format_timestamp(timestamp),
        }),
        Value::Extension(data) if data.first() == Some(&(crate::DataType::Json as u8)) => {
            serde_json::json!({
                "type": "JSON",
                "value": serde_json::from_slice::<serde_json::Value>(&data[1..]).ok(),
                "raw_hex": bytes_to_hex(&data[1..]),
            })
        }
        Value::Extension(data) if data.first() == Some(&(crate::DataType::Vector as u8)) => {
            serde_json::json!({
                "type": "VECTOR",
                "raw_f32_le_hex": bytes_to_hex(&data[1..]),
            })
        }
        Value::Extension(data) if data.first() == Some(&(crate::DataType::Uuid as u8)) => {
            serde_json::json!({
                "type": "UUID",
                "value": crate::core::value::format_uuid_bytes(&data[1..]),
                "raw_hex": bytes_to_hex(&data[1..]),
            })
        }
        Value::Extension(data) if data.first() == Some(&(DataType::Decimal as u8)) => {
            let parts = value.as_decimal_parts();
            serde_json::json!({
                "type": "DECIMAL",
                "unscaled": parts.map(|(unscaled, _, _)| unscaled.to_string()),
                "precision": parts.map(|(_, precision, _)| precision),
                "scale": parts.map(|(_, _, scale)| scale),
                "raw_hex": bytes_to_hex(data),
            })
        }
        Value::Extension(data) if data.first() == Some(&(DataType::Date as u8)) => {
            serde_json::json!({
                "type": "DATE",
                "days_since_unix_epoch": value.as_date_days(),
                "raw_hex": bytes_to_hex(data),
            })
        }
        Value::Extension(data) if data.first() == Some(&(DataType::Bytes as u8)) => {
            serde_json::json!({
                "type": "BYTES",
                "raw_hex": bytes_to_hex(&data[1..]),
            })
        }
        Value::Extension(data) => serde_json::json!({
            "type": "EXTENSION",
            "data_type": format!("{:?}", value.data_type()),
            "raw_hex": bytes_to_hex(data),
        }),
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

/// Split original SQL text at semicolon tokens emitted by the core lexer.
fn split_sql_statements(input: &str) -> Result<Vec<String>, String> {
    let mut statements = Vec::new();
    let mut start = 0;
    let mut lexer = Lexer::new(input);
    loop {
        let token = lexer.next_token();
        if token.is_error() {
            return Err(token.to_string());
        }
        if token.is_punctuator(";") {
            statements.push(input[start..token.position.offset].to_string());
            start = token.position.offset + 1;
        }
        if token.is_eof() {
            break;
        }
    }
    if !input[start..].trim().is_empty() {
        statements.push(input[start..].to_string());
    }
    Ok(statements)
}

fn sql_input_is_complete(input: &str) -> bool {
    let mut lexer = Lexer::new(input);
    let mut terminal = false;
    loop {
        let token = lexer.next_token();
        if token.is_error() {
            return false;
        }
        if token.is_eof() {
            return terminal;
        }
        if token.token_type != TokenType::Comment {
            terminal = token.is_punctuator(";");
        }
    }
}

fn print_help_main() {
    println!("RadixDB SQL CLI");
    println!();
    println!("  SQL Commands:");
    println!("    SELECT ...             Execute a SELECT query");
    println!("    INSERT ...             Insert data into a table");
    println!("    UPDATE ...             Update data in a table");
    println!("    DELETE ...             Delete data from a table");
    println!("    CREATE TABLE ...       Create a new table");
    println!("    CREATE INDEX ...       Create an index on a column");
    println!("    SHOW TABLES            List all tables");
    println!("    SHOW CREATE TABLE ...  Show CREATE TABLE statement for a table");
    println!("    SHOW INDEXES FROM ...  Show indexes for a table");
    println!();
    println!("  Transaction Commands:");
    println!("    BEGIN [ISOLATION LEVEL READ COMMITTED|SNAPSHOT]");
    println!("                           Start a transaction at the selected isolation");
    println!("    COMMIT                 Commit the current transaction");
    println!("    ROLLBACK               Rollback the current transaction");
    println!("    SAVEPOINT name         Create a transaction savepoint");
    println!("    ROLLBACK TO [SAVEPOINT] name");
    println!("                           Roll back to a savepoint");
    println!("    RELEASE [SAVEPOINT] name");
    println!("                           Release a savepoint");
    println!();
    println!("  Special Commands:");
    println!("    help, \\h, \\?          Show this help message");
    println!();
}

#[cfg(test)]
mod tests;
