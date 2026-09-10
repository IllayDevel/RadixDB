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

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use radixdb::server::{
    default_seal_hot_bytes_threshold, default_seal_incremental_hot_bytes_threshold,
    default_target_volume_rows, Server, ServerConfig,
};
use radixdb_client::{
    ClientError, ColumnCursorBatch, Connection, CursorFetchMode, ExecuteResult, ProtocolCapability,
    ProtocolErrorCode, Row, WireColumn, WireValue,
};

fn test_config(data_dir: std::path::PathBuf) -> ServerConfig {
    ServerConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        port: 0,
        data_dir,
        transport: Default::default(),
        authentication: Default::default(),
        max_connections: 8,
        max_inflight_frame_bytes: radixdb::server::default_max_inflight_frame_bytes(),
        max_databases: radixdb::server::default_max_databases(),
        max_database_name_bytes: radixdb::server::default_max_database_name_bytes(),
        connect_timeout_secs: 5,
        connection_idle_timeout_secs: 30,
        net_read_timeout_secs: 30,
        net_write_timeout_secs: 30,
        cursor_batch_max_rows: 128,
        cursor_batch_max_bytes: 1024 * 1024,
        max_frame_bytes: 4 * 1024 * 1024,
        copy_max_transaction_bytes: radixdb::server::default_copy_max_transaction_bytes(),
        max_compaction_jobs: radixdb::server::default_max_compaction_jobs(),
        storage_cpu_workers: radixdb::server::default_storage_cpu_workers(),
        page_cache_level: radixdb::server::default_page_cache_level(),
        page_cache_max_bytes: radixdb::server::default_page_cache_max_bytes(),
        page_cache_memory_reserve: radixdb::server::default_page_cache_memory_reserve(),
        target_volume_rows: default_target_volume_rows(),
        seal_hot_bytes_threshold: default_seal_hot_bytes_threshold(),
        seal_incremental_hot_bytes_threshold: default_seal_incremental_hot_bytes_threshold(),
        read_queue_depth: 1,
    }
}

fn with_one_connection_server(
    config: ServerConfig,
    database: &str,
    work: impl FnOnce(&mut Connection),
) {
    let server = Server::bind_ephemeral(&config).expect("server binds");
    let address = server.local_addr().expect("local addr");
    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.serve_one());
        let mut client = connect_and_select(address, database);
        work(&mut client);
        drop(client);
        server_worker
            .join()
            .expect("server thread joins")
            .expect("server serves one connection");
    });
}

fn connect_and_select(address: SocketAddr, database: &str) -> Connection {
    let mut client = Connection::connect(address).expect("client connects");
    client.authenticate("root", None).expect("auth");
    client.select_database(database).expect("select database");
    client
}

fn assert_command(result: ExecuteResult) {
    assert!(
        matches!(result, ExecuteResult::CommandComplete { .. }),
        "expected command completion, got {result:?}"
    );
}

fn execute_and_drain(client: &mut Connection, sql: &str) {
    match client.execute(sql).expect("statement should execute") {
        ExecuteResult::CommandComplete { .. } => {}
        ExecuteResult::Cursor(cursor) => {
            let _ = client
                .fetch(&cursor)
                .expect("statement cursor should fetch");
        }
    }
}

fn fetch_count(client: &mut Connection, sql: &str) -> i64 {
    let ExecuteResult::Cursor(cursor) = client.execute(sql).expect("open count cursor") else {
        panic!("COUNT query should open a cursor");
    };
    let batch = client.fetch(&cursor).expect("fetch count cursor");
    assert!(batch.eof);
    assert_eq!(batch.rows.len(), 1);
    match &batch.rows[0].values[0] {
        WireValue::Int(value) => *value,
        other => panic!("COUNT should return Int, got {other:?}"),
    }
}

fn fetch_single_row(client: &mut Connection) -> Vec<WireValue> {
    let ExecuteResult::Cursor(cursor) = client
        .execute(
            "SELECT amount, accounting_date, payload
             FROM wire_scalars
             WHERE id = 1",
        )
        .expect("select scalars")
    else {
        panic!("SELECT should open a cursor");
    };
    let batch = client.fetch(&cursor).expect("fetch row");
    assert!(batch.eof);
    assert_eq!(batch.rows.len(), 1);
    let Row { values } = batch.rows.into_iter().next().expect("single row");
    values
}

fn assert_exact_wire_scalars(values: &[WireValue]) {
    assert_eq!(
        values,
        &[
            WireValue::Decimal {
                unscaled: -1234567890123456789,
                precision: 22,
                scale: 4,
            },
            WireValue::Date {
                days_since_unix_epoch: 20_001,
            },
            WireValue::Bytes(vec![0, 1, 2, 3, 254, 255, b'R', b'D']),
        ]
    );
}

fn fetch_column_batch_single_row(client: &mut Connection, sql: &str) -> Vec<WireValue> {
    let ExecuteResult::Cursor(cursor) = client.execute(sql).expect("select fallback row") else {
        panic!("SELECT should open a cursor");
    };
    match client
        .fetch_column_batch(&cursor)
        .expect("fetch through ColumnBatchV1")
    {
        ColumnCursorBatch::Rows(batch) => {
            assert!(batch.eof);
            assert_eq!(batch.rows.len(), 1);
            let Row { values } = batch.rows.into_iter().next().expect("single row");
            values
        }
        ColumnCursorBatch::Columnar {
            columns, row_count, ..
        } => panic!(
            "this query is expected to use row fallback, got {row_count} rows and {} typed columns",
            columns.len()
        ),
    }
}

fn collect_split_column_batch_rows(
    client: &mut Connection,
    sql: &str,
) -> (Vec<(i64, Option<String>)>, usize) {
    let ExecuteResult::Cursor(cursor) = client.execute(sql).expect("select split column batch")
    else {
        panic!("SELECT should open a cursor");
    };

    let mut rows = Vec::new();
    let mut non_empty_columnar_batches = 0_usize;
    loop {
        match client
            .fetch_column_batch(&cursor)
            .expect("fetch through ColumnBatchV1")
        {
            ColumnCursorBatch::Rows(batch) => panic!(
                "eligible artifact-backed split query must stay columnar, got row fallback with {} rows",
                batch.rows.len()
            ),
            ColumnCursorBatch::Columnar {
                columns,
                row_count,
                eof,
            } => {
                if eof {
                    assert_eq!(row_count, 0, "terminal column batch must be empty");
                    assert!(
                        columns.is_empty(),
                        "terminal column batch must not carry columns"
                    );
                    break;
                }
                assert!(row_count > 0, "non-terminal column batch must carry rows");
                assert_eq!(columns.len(), 2, "SELECT projects id and category");
                let ids = match &columns[0] {
                    WireColumn::Int64 { values, nulls } => {
                        assert_eq!(values.len(), row_count as usize);
                        assert_eq!(nulls, &vec![false; row_count as usize]);
                        values
                    }
                    column => panic!("id must be an Int64 wire column, got {column:?}"),
                };
                let (text_ids, dictionary, text_nulls) = match &columns[1] {
                    WireColumn::DictionaryText {
                        ids,
                        dictionary,
                        nulls,
                    } => {
                        assert_eq!(ids.len(), row_count as usize);
                        assert_eq!(nulls.len(), row_count as usize);
                        (ids, dictionary, nulls)
                    }
                    column => {
                        panic!("category must be a DictionaryText wire column, got {column:?}")
                    }
                };
                for local_row in 0..row_count as usize {
                    let category = if text_nulls[local_row] {
                        None
                    } else {
                        let dictionary_id = text_ids[local_row] as usize;
                        Some(
                            dictionary
                                .get(dictionary_id)
                                .unwrap_or_else(|| {
                                    panic!(
                                        "dictionary id {dictionary_id} is out of range {}",
                                        dictionary.len()
                                    )
                                })
                                .clone(),
                        )
                    };
                    rows.push((ids[local_row], category));
                }
                non_empty_columnar_batches += 1;
            }
        }
    }
    (rows, non_empty_columnar_batches)
}

fn create_split_source_table(client: &mut Connection, table: &str, row_count: i64) {
    assert_command(
        client
            .execute(format!(
                "CREATE TABLE {table} (
                    id INTEGER PRIMARY KEY,
                    category TEXT
                )"
            ))
            .expect("create split source table"),
    );

    for id in 1..=row_count {
        let category = if id % 11 == 0 {
            "NULL".to_string()
        } else if id % 2 == 0 {
            "'north'".to_string()
        } else {
            "'south'".to_string()
        };
        assert_command(
            client
                .execute(format!(
                    "INSERT INTO {table} (id, category) VALUES ({id}, {category})"
                ))
                .expect("insert split source row"),
        );
    }
    execute_and_drain(client, "PRAGMA CHECKPOINT");
}

fn fetch_columnar_row_count(batch: ColumnCursorBatch) -> (u32, bool) {
    match batch {
        ColumnCursorBatch::Columnar {
            columns,
            row_count,
            eof,
        } => {
            if eof {
                assert_eq!(row_count, 0, "terminal column batch must be empty");
                assert!(
                    columns.is_empty(),
                    "terminal column batch must not carry columns"
                );
            } else {
                assert!(row_count > 0, "non-terminal column batch must carry rows");
                assert_eq!(columns.len(), 2, "SELECT projects id and category");
            }
            (row_count, eof)
        }
        ColumnCursorBatch::Rows(batch) => panic!(
            "eligible artifact-backed split query must stay columnar, got row fallback with {} rows",
            batch.rows.len()
        ),
    }
}

fn wire_bytes_at(data: &[u8], offsets: &[(u64, u64)], row_idx: usize) -> Vec<u8> {
    let (offset, len) = offsets[row_idx];
    let offset = usize::try_from(offset).expect("wire byte offset fits usize");
    let len = usize::try_from(len).expect("wire byte length fits usize");
    data[offset..offset + len].to_vec()
}

#[test]
fn decimal_date_and_bytes_roundtrip_over_public_tcp_after_reopen() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");

    with_one_connection_server(test_config(data_dir.clone()), "wire_scalars", |client| {
        assert_command(
            client
                .execute(
                    "CREATE TABLE wire_scalars (
                        id INTEGER PRIMARY KEY,
                        amount DECIMAL,
                        accounting_date DATE,
                        payload BYTES
                    )",
                )
                .expect("create table"),
        );

        let mut params = BTreeMap::new();
        params.insert(
            "amount".to_string(),
            WireValue::Decimal {
                unscaled: -1234567890123456789,
                precision: 22,
                scale: 4,
            },
        );
        params.insert(
            "accounting_date".to_string(),
            WireValue::Date {
                days_since_unix_epoch: 20_001,
            },
        );
        params.insert(
            "payload".to_string(),
            WireValue::Bytes(vec![0, 1, 2, 3, 254, 255, b'R', b'D']),
        );

        assert_command(
            client
                .execute_with_parameters(
                    "INSERT INTO wire_scalars (id, amount, accounting_date, payload)
                     VALUES (1, :amount, :accounting_date, :payload)",
                    params,
                )
                .expect("parameterized insert"),
        );

        let values = fetch_single_row(client);
        assert_exact_wire_scalars(&values);
    });

    with_one_connection_server(test_config(data_dir), "wire_scalars", |client| {
        let values = fetch_single_row(client);
        assert_exact_wire_scalars(&values);
    });
}

#[test]
fn column_batch_v1_uses_typed_bytes_and_json_columns_after_reopen() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");

    with_one_connection_server(test_config(data_dir.clone()), "wire_bytes_json", |client| {
        assert_command(
            client
                .execute(
                    "CREATE TABLE wire_bytes_json (
                        id INTEGER PRIMARY KEY,
                        payload BYTES,
                        document JSON
                    )",
                )
                .expect("create bytes/json table"),
        );

        for (id, payload, document) in [
            (
                1,
                vec![0, 1, 2, 3, 254, 255, b'R', b'D'],
                r#"{"kind":"typed","n":1}"#,
            ),
            (2, Vec::new(), r#"{"kind":"empty"}"#),
        ] {
            let mut params = BTreeMap::new();
            params.insert("payload".to_string(), WireValue::Bytes(payload));
            params.insert(
                "document".to_string(),
                WireValue::Json(document.to_string()),
            );
            assert_command(
                client
                    .execute_with_parameters(
                        format!(
                            "INSERT INTO wire_bytes_json (id, payload, document)
                             VALUES ({id}, :payload, :document)"
                        ),
                        params,
                    )
                    .expect("insert bytes/json values"),
            );
        }
        assert_command(
            client
                .execute(
                    "INSERT INTO wire_bytes_json (id, payload, document) VALUES (3, NULL, NULL)",
                )
                .expect("insert bytes/json nulls"),
        );
        execute_and_drain(client, "PRAGMA CHECKPOINT");
    });

    with_one_connection_server(test_config(data_dir), "wire_bytes_json", |client| {
        let ExecuteResult::Cursor(cursor) = client
            .execute("SELECT payload, document FROM wire_bytes_json")
            .expect("open bytes/json typed cursor")
        else {
            panic!("SELECT should open a cursor");
        };

        match client
            .fetch_column_batch(&cursor)
            .expect("fetch bytes/json typed batch")
        {
            ColumnCursorBatch::Columnar {
                columns,
                row_count,
                eof,
            } => {
                assert_eq!(row_count, 3);
                assert!(!eof);
                assert_eq!(columns.len(), 2);
                match &columns[0] {
                    WireColumn::Bytes {
                        data,
                        offsets,
                        nulls,
                    } => {
                        assert_eq!(nulls, &[false, false, true]);
                        assert_eq!(
                            wire_bytes_at(data, offsets, 0),
                            vec![0, 1, 2, 3, 254, 255, b'R', b'D']
                        );
                        assert_eq!(wire_bytes_at(data, offsets, 1), Vec::<u8>::new());
                    }
                    other => panic!("payload must be a typed Bytes column, got {other:?}"),
                }
                match &columns[1] {
                    WireColumn::JsonText {
                        data,
                        offsets,
                        nulls,
                    } => {
                        assert_eq!(nulls, &[false, false, true]);
                        assert_eq!(
                            String::from_utf8(wire_bytes_at(data, offsets, 0)).unwrap(),
                            r#"{"kind":"typed","n":1}"#
                        );
                        assert_eq!(
                            String::from_utf8(wire_bytes_at(data, offsets, 1)).unwrap(),
                            r#"{"kind":"empty"}"#
                        );
                    }
                    other => panic!("document must be a typed JsonText column, got {other:?}"),
                }
            }
            ColumnCursorBatch::Rows(batch) => panic!(
                "BYTES/JSON have a typed ColumnBatchV1 contract, got {} fallback rows",
                batch.rows.len()
            ),
        }

        match client
            .fetch_column_batch(&cursor)
            .expect("fetch terminal bytes/json typed batch")
        {
            ColumnCursorBatch::Columnar {
                columns,
                row_count,
                eof,
            } => {
                assert!(eof);
                assert_eq!(row_count, 0);
                assert!(columns.is_empty());
            }
            ColumnCursorBatch::Rows(batch) => panic!(
                "terminal bytes/json typed cursor must stay columnar, got {} fallback rows",
                batch.rows.len()
            ),
        }
    });
}

#[test]
fn column_batch_v1_matches_row_fetch_for_float_bool_timestamp_and_nulls_after_reopen() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let first_ts = 1_700_000_000_123_i64;
    let second_ts = 1_700_000_001_999_i64;

    with_one_connection_server(
        test_config(data_dir.clone()),
        "wire_typed_matrix",
        |client| {
            assert_command(
                client
                    .execute(
                        "CREATE TABLE wire_typed_matrix (
                            id INTEGER PRIMARY KEY,
                            score FLOAT,
                            active BOOLEAN,
                            happened_at TIMESTAMP
                        )",
                    )
                    .expect("create typed matrix table"),
            );

            let mut first = BTreeMap::new();
            first.insert("score".to_string(), WireValue::Float64(3.5));
            first.insert("active".to_string(), WireValue::Bool(true));
            first.insert(
                "happened_at".to_string(),
                WireValue::TimestampNanos {
                    nanos_since_unix_epoch_utc: first_ts * 1_000_000,
                },
            );
            assert_command(
                client
                    .execute_with_parameters(
                        "INSERT INTO wire_typed_matrix (id, score, active, happened_at)
                         VALUES (1, :score, :active, :happened_at)",
                        first,
                    )
                    .expect("insert first typed row"),
            );

            assert_command(
                client
                    .execute(
                        "INSERT INTO wire_typed_matrix (id, score, active, happened_at)
                         VALUES (2, NULL, false, NULL)",
                    )
                    .expect("insert nullable typed row"),
            );

            let mut second = BTreeMap::new();
            second.insert("score".to_string(), WireValue::Float64(-2.25));
            second.insert("active".to_string(), WireValue::Null);
            second.insert(
                "happened_at".to_string(),
                WireValue::TimestampNanos {
                    nanos_since_unix_epoch_utc: second_ts * 1_000_000,
                },
            );
            assert_command(
                client
                    .execute_with_parameters(
                        "INSERT INTO wire_typed_matrix (id, score, active, happened_at)
                         VALUES (3, :score, :active, :happened_at)",
                        second,
                    )
                    .expect("insert second typed row"),
            );
            execute_and_drain(client, "PRAGMA CHECKPOINT");
        },
    );

    with_one_connection_server(test_config(data_dir), "wire_typed_matrix", |client| {
        let expected_rows = vec![
            vec![
                WireValue::Float64(3.5),
                WireValue::Bool(true),
                WireValue::TimestampNanos {
                    nanos_since_unix_epoch_utc: first_ts * 1_000_000,
                },
            ],
            vec![WireValue::Null, WireValue::Bool(false), WireValue::Null],
            vec![
                WireValue::Float64(-2.25),
                WireValue::Null,
                WireValue::TimestampNanos {
                    nanos_since_unix_epoch_utc: second_ts * 1_000_000,
                },
            ],
        ];
        let sql = "SELECT score, active, happened_at FROM wire_typed_matrix";

        let ExecuteResult::Cursor(row_cursor) = client.execute(sql).expect("open row cursor")
        else {
            panic!("SELECT should open a row cursor");
        };
        let row_batch = client.fetch(&row_cursor).expect("fetch row cursor");
        assert!(row_batch.eof);
        assert_eq!(
            row_batch
                .rows
                .into_iter()
                .map(|row| row.values)
                .collect::<Vec<_>>(),
            expected_rows
        );

        let ExecuteResult::Cursor(column_cursor) =
            client.execute(sql).expect("open typed matrix cursor")
        else {
            panic!("SELECT should open a column cursor");
        };
        match client
            .fetch_batch(&column_cursor, CursorFetchMode::Auto)
            .expect("auto-fetch typed matrix column batch")
        {
            ColumnCursorBatch::Columnar {
                columns,
                row_count,
                eof,
            } => {
                assert_eq!(row_count, 3);
                assert!(!eof);
                assert_eq!(columns.len(), 3);
                match &columns[0] {
                    WireColumn::Float64 { values, nulls } => {
                        assert_eq!(values, &[3.5, 0.0, -2.25]);
                        assert_eq!(nulls, &[false, true, false]);
                    }
                    other => panic!("score must be a typed Float64 column, got {other:?}"),
                }
                match &columns[1] {
                    WireColumn::Boolean { values, nulls } => {
                        assert_eq!(values, &[true, false, false]);
                        assert_eq!(nulls, &[false, false, true]);
                    }
                    other => panic!("active must be a typed Boolean column, got {other:?}"),
                }
                match &columns[2] {
                    WireColumn::TimestampNanos { values, nulls } => {
                        assert_eq!(values, &[first_ts * 1_000_000, 0, second_ts * 1_000_000]);
                        assert_eq!(nulls, &[false, true, false]);
                    }
                    other => {
                        panic!("happened_at must be a typed TimestampNanos column, got {other:?}")
                    }
                }
            }
            ColumnCursorBatch::Rows(batch) => panic!(
                "float/bool/timestamp/null matrix must stay columnar, got {} fallback rows",
                batch.rows.len()
            ),
        }

        match client
            .fetch_batch(&column_cursor, CursorFetchMode::Auto)
            .expect("auto-fetch terminal typed matrix batch")
        {
            ColumnCursorBatch::Columnar {
                columns,
                row_count,
                eof,
            } => {
                assert!(eof);
                assert_eq!(row_count, 0);
                assert!(columns.is_empty());
            }
            ColumnCursorBatch::Rows(batch) => panic!(
                "terminal typed matrix cursor must stay columnar, got {} fallback rows",
                batch.rows.len()
            ),
        }
    });
}

#[test]
fn column_batch_v1_splits_large_artifact_group_across_public_fetches_after_reopen() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let row_count = 512_i64;

    with_one_connection_server(test_config(data_dir.clone()), "wire_split", |client| {
        assert!(
            client
                .capabilities()
                .contains(&ProtocolCapability::ColumnBatchV1),
            "public client should negotiate ColumnBatchV1 by default"
        );
        assert_command(
            client
                .execute(
                    "CREATE TABLE wire_split (
                        id INTEGER PRIMARY KEY,
                        category TEXT
                    )",
                )
                .expect("create split table"),
        );

        for id in 1..=row_count {
            let category = if id % 11 == 0 {
                "NULL".to_string()
            } else if id % 2 == 0 {
                "'north'".to_string()
            } else {
                "'south'".to_string()
            };
            assert_command(
                client
                    .execute(format!(
                        "INSERT INTO wire_split (id, category) VALUES ({id}, {category})"
                    ))
                    .expect("insert split row"),
            );
        }
        execute_and_drain(client, "PRAGMA CHECKPOINT");
    });

    let mut split_config = test_config(data_dir);
    split_config.cursor_batch_max_bytes = 1024;
    split_config.max_frame_bytes = 1024;
    with_one_connection_server(split_config, "wire_split", |client| {
        let (actual, batches) =
            collect_split_column_batch_rows(client, "SELECT id, category FROM wire_split");
        assert!(
            batches > 1,
            "small frame limits must split one artifact-backed group across multiple fetches"
        );

        let expected = (1..=row_count)
            .map(|id| {
                let category = if id % 11 == 0 {
                    None
                } else if id % 2 == 0 {
                    Some("north".to_string())
                } else {
                    Some("south".to_string())
                };
                (id, category)
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    });
}

#[test]
fn column_batch_v1_splits_large_artifact_group_across_multiple_frame_limits_after_reopen() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let row_count = 2_048_i64;

    with_one_connection_server(
        test_config(data_dir.clone()),
        "wire_split_limits",
        |client| {
            assert_command(
                client
                    .execute(
                        "CREATE TABLE wire_split_limits (
                        id INTEGER PRIMARY KEY,
                        category TEXT
                    )",
                    )
                    .expect("create split limits table"),
            );

            for id in 1..=row_count {
                let category = if id % 13 == 0 {
                    "NULL".to_string()
                } else if id % 3 == 0 {
                    "'north'".to_string()
                } else if id % 3 == 1 {
                    "'south'".to_string()
                } else {
                    "'central'".to_string()
                };
                assert_command(
                    client
                        .execute(format!(
                        "INSERT INTO wire_split_limits (id, category) VALUES ({id}, {category})"
                    ))
                        .expect("insert split limits row"),
                );
            }
            execute_and_drain(client, "PRAGMA CHECKPOINT");
        },
    );

    let expected = (1..=row_count)
        .map(|id| {
            let category = if id % 13 == 0 {
                None
            } else if id % 3 == 0 {
                Some("north".to_string())
            } else if id % 3 == 1 {
                Some("south".to_string())
            } else {
                Some("central".to_string())
            };
            (id, category)
        })
        .collect::<Vec<_>>();

    let mut observed_batches = Vec::new();
    for frame_limit in [1024_usize, 2048, 4096] {
        let mut split_config = test_config(data_dir.clone());
        split_config.cursor_batch_max_bytes = frame_limit;
        split_config.max_frame_bytes = frame_limit as u32;
        with_one_connection_server(split_config, "wire_split_limits", |client| {
            let (actual, batches) = collect_split_column_batch_rows(
                client,
                "SELECT id, category FROM wire_split_limits",
            );
            assert_eq!(actual, expected, "frame limit {frame_limit}");
            assert!(
                batches > 1,
                "frame limit {frame_limit} must split the artifact-backed group across multiple fetches"
            );
            observed_batches.push((frame_limit, batches));
        });
    }

    assert!(
        observed_batches
            .windows(2)
            .all(|pair| pair[0].1 >= pair[1].1),
        "larger frame limits should not require more non-empty batches: {observed_batches:?}"
    );
}

#[test]
fn column_batch_v1_slow_reader_completes_split_cursor_with_bounded_pending_lifecycle() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let row_count = 2_048_i64;

    with_one_connection_server(test_config(data_dir.clone()), "wire_slow_split", |client| {
        create_split_source_table(client, "wire_slow_split", row_count);
    });

    let before = radixdb::storage::instrumentation::snapshot();
    let mut split_config = test_config(data_dir);
    split_config.cursor_batch_max_bytes = 1024;
    split_config.max_frame_bytes = 1024;
    with_one_connection_server(split_config, "wire_slow_split", |client| {
        let ExecuteResult::Cursor(cursor) = client
            .execute("SELECT id, category FROM wire_slow_split")
            .expect("open slow split cursor")
        else {
            panic!("SELECT should open a cursor");
        };

        let mut rows = 0_u32;
        let mut batches = 0_usize;
        loop {
            thread::sleep(Duration::from_millis(1));
            let (row_count, eof) = fetch_columnar_row_count(
                client
                    .fetch_column_batch(&cursor)
                    .expect("slow reader fetches split typed batch"),
            );
            if eof {
                break;
            }
            rows = rows.saturating_add(row_count);
            batches += 1;
        }

        assert_eq!(rows, row_count as u32);
        assert!(
            batches > 1,
            "small frame limits must keep the cursor in split-tail mode"
        );
    });
    let after = radixdb::storage::instrumentation::snapshot();
    let opened = after
        .protocol_column_batch_pending_opened
        .saturating_sub(before.protocol_column_batch_pending_opened);
    let completed = after
        .protocol_column_batch_pending_completed
        .saturating_sub(before.protocol_column_batch_pending_completed);
    let max = after
        .protocol_column_batch_pending_max
        .saturating_sub(before.protocol_column_batch_pending_max);
    assert!(
        opened >= 1,
        "slow split cursor must exercise pending ColumnBatch lifecycle"
    );
    assert!(
        completed >= 1,
        "slow split cursor must complete its pending ColumnBatch lifecycle"
    );
    assert!(
        max <= opened,
        "one cursor should not retain more pending batches than it opened: opened={opened}, max={max}"
    );
}

#[test]
fn column_batch_v1_slow_typed_cursor_does_not_block_another_connection() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let row_count = 2_048_i64;

    with_one_connection_server(
        test_config(data_dir.clone()),
        "wire_split_backpressure",
        |client| {
            create_split_source_table(client, "wire_split_backpressure", row_count);
        },
    );

    let mut config = test_config(data_dir);
    config.cursor_batch_max_bytes = 1024;
    config.max_frame_bytes = 1024;
    config.max_connections = 4;
    let server = Server::bind_ephemeral(&config).expect("server binds");
    let address = server.local_addr().expect("local addr");
    let shutdown = AtomicBool::new(false);
    let (slow_ready_tx, slow_ready_rx) = mpsc::channel();

    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.run_until(&shutdown));
        let slow_worker = scope.spawn(|| {
            let mut slow_client = connect_and_select(address, "wire_split_backpressure");
            let ExecuteResult::Cursor(cursor) = slow_client
                .execute("SELECT id, category FROM wire_split_backpressure")
                .expect("open slow typed cursor")
            else {
                panic!("SELECT should open a cursor");
            };

            let (first_rows, first_eof) = fetch_columnar_row_count(
                slow_client
                    .fetch_column_batch(&cursor)
                    .expect("fetch first slow typed batch"),
            );
            assert!(!first_eof, "split cursor should not finish in one batch");
            slow_ready_tx
                .send(())
                .expect("signal that slow cursor is retaining a pending batch");
            thread::sleep(Duration::from_millis(100));

            let mut rows = first_rows;
            loop {
                thread::sleep(Duration::from_millis(1));
                let (row_count, eof) = fetch_columnar_row_count(
                    slow_client
                        .fetch_column_batch(&cursor)
                        .expect("continue slow typed cursor"),
                );
                if eof {
                    break;
                }
                rows = rows.saturating_add(row_count);
            }
            rows
        });

        slow_ready_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("slow cursor should reach pending split state");

        let started = Instant::now();
        let mut fast_client = connect_and_select(address, "wire_split_backpressure");
        let count = fetch_count(
            &mut fast_client,
            "SELECT COUNT(*) FROM wire_split_backpressure",
        );
        let elapsed = started.elapsed();
        assert_eq!(count, row_count);
        assert!(
            elapsed < Duration::from_secs(2),
            "fast connection should not wait for slow typed cursor, elapsed={elapsed:?}"
        );
        drop(fast_client);

        let slow_rows = slow_worker.join().expect("slow client joins");
        assert_eq!(slow_rows, row_count as u32);
        shutdown.store(true, Ordering::Release);
        server_worker
            .join()
            .expect("server thread joins")
            .expect("server run_until exits");
    });
}

#[test]
fn column_batch_v1_applies_tombstones_columnar_after_reopen() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");

    with_one_connection_server(
        test_config(data_dir.clone()),
        "wire_tombstone_batch",
        |client| {
            assert_command(
                client
                    .execute(
                        "CREATE TABLE wire_tombstone_batch (
                            id INTEGER PRIMARY KEY,
                            category TEXT
                        )",
                    )
                    .expect("create tombstone batch table"),
            );
            for (id, category) in [
                (1, "north"),
                (2, "south"),
                (3, "central"),
                (4, "north"),
                (5, "south"),
                (6, "central"),
            ] {
                assert_command(
                    client
                        .execute(format!(
                            "INSERT INTO wire_tombstone_batch (id, category)
                             VALUES ({id}, '{category}')"
                        ))
                        .expect("insert tombstone batch row"),
                );
            }
            execute_and_drain(client, "PRAGMA CHECKPOINT");
            assert_command(
                client
                    .execute("DELETE FROM wire_tombstone_batch WHERE id = 2")
                    .expect("delete first cold row"),
            );
            assert_command(
                client
                    .execute("DELETE FROM wire_tombstone_batch WHERE id = 5")
                    .expect("delete second cold row"),
            );
        },
    );

    with_one_connection_server(test_config(data_dir), "wire_tombstone_batch", |client| {
        let (actual, batches) = collect_split_column_batch_rows(
            client,
            "SELECT id, category FROM wire_tombstone_batch",
        );
        assert_eq!(
            actual,
            vec![
                (1, Some("north".to_string())),
                (3, Some("central".to_string())),
                (4, Some("north".to_string())),
                (6, Some("central".to_string())),
            ]
        );
        assert_eq!(batches, 1);
    });
}

#[test]
fn column_batch_v1_applies_pending_cold_deletes_columnar_inside_transaction() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");

    with_one_connection_server(
        test_config(data_dir),
        "wire_pending_delete_batch",
        |client| {
            assert_command(
                client
                    .execute(
                        "CREATE TABLE wire_pending_delete_batch (
                            id INTEGER PRIMARY KEY,
                            category TEXT
                        )",
                    )
                    .expect("create pending delete batch table"),
            );
            for (id, category) in [(1, "north"), (2, "south"), (3, "central"), (4, "east")] {
                assert_command(
                    client
                        .execute(format!(
                            "INSERT INTO wire_pending_delete_batch (id, category)
                             VALUES ({id}, '{category}')"
                        ))
                        .expect("insert pending delete fixture row"),
                );
            }
            execute_and_drain(client, "PRAGMA CHECKPOINT");

            client.begin().expect("begin pending delete transaction");
            assert_eq!(
                client
                    .execute("DELETE FROM wire_pending_delete_batch WHERE id = 2")
                    .expect("delete cold row inside transaction"),
                ExecuteResult::CommandComplete {
                    affected_rows: 1,
                    last_insert_id: 0,
                }
            );

            let (actual, batches) = collect_split_column_batch_rows(
                client,
                "SELECT id, category FROM wire_pending_delete_batch",
            );
            assert_eq!(
                actual,
                vec![
                    (1, Some("north".to_string())),
                    (3, Some("central".to_string())),
                    (4, Some("east".to_string())),
                ],
                "pending cold delete must be visible to the deleting transaction"
            );
            assert_eq!(batches, 1);

            client
                .rollback()
                .expect("rollback pending cold delete transaction");
            let (after_rollback, batches) = collect_split_column_batch_rows(
                client,
                "SELECT id, category FROM wire_pending_delete_batch",
            );
            assert_eq!(
                after_rollback,
                vec![
                    (1, Some("north".to_string())),
                    (2, Some("south".to_string())),
                    (3, Some("central".to_string())),
                    (4, Some("east".to_string())),
                ],
                "rollback must restore the cold row to the same typed cursor path"
            );
            assert_eq!(batches, 1);
        },
    );
}

#[test]
fn column_batch_v1_synthesizes_schema_evolved_defaults_columnar_after_reopen() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");

    with_one_connection_server(
        test_config(data_dir.clone()),
        "wire_schema_defaults",
        |client| {
            assert_command(
                client
                    .execute(
                        "CREATE TABLE wire_schema_defaults (
                            id INTEGER PRIMARY KEY,
                            name TEXT
                        )",
                    )
                    .expect("create schema default table"),
            );
            assert_command(
                client
                    .execute("INSERT INTO wire_schema_defaults (id, name) VALUES (1, 'legacy')")
                    .expect("insert pre-alter row"),
            );
            execute_and_drain(client, "PRAGMA CHECKPOINT");
            assert_command(
                client
                    .execute(
                        "ALTER TABLE wire_schema_defaults
                         ADD COLUMN status TEXT DEFAULT 'active'",
                    )
                    .expect("add defaulted schema column"),
            );
        },
    );

    with_one_connection_server(test_config(data_dir), "wire_schema_defaults", |client| {
        let ExecuteResult::Cursor(cursor) = client
            .execute("SELECT id, status FROM wire_schema_defaults")
            .expect("open schema default typed cursor")
        else {
            panic!("SELECT should open a cursor");
        };

        match client
            .fetch_column_batch(&cursor)
            .expect("fetch schema default typed batch")
        {
            ColumnCursorBatch::Columnar {
                columns,
                row_count,
                eof,
            } => {
                assert_eq!(row_count, 1);
                assert!(!eof);
                assert_eq!(columns.len(), 2);
                match &columns[0] {
                    WireColumn::Int64 { values, nulls } => {
                        assert_eq!(values, &[1]);
                        assert_eq!(nulls, &[false]);
                    }
                    other => panic!("id must stay an Int64 typed column, got {other:?}"),
                }
                match &columns[1] {
                    WireColumn::DictionaryText {
                        ids,
                        dictionary,
                        nulls,
                    } => {
                        assert_eq!(ids, &[0]);
                        assert_eq!(dictionary, &vec!["active".to_string()]);
                        assert_eq!(nulls, &[false]);
                    }
                    other => panic!("status default must be DictionaryText, got {other:?}"),
                }
            }
            ColumnCursorBatch::Rows(batch) => panic!(
                "schema-evolved supported defaults must stay columnar, got {} fallback rows",
                batch.rows.len()
            ),
        }

        match client
            .fetch_column_batch(&cursor)
            .expect("fetch terminal schema default typed batch")
        {
            ColumnCursorBatch::Columnar {
                columns,
                row_count,
                eof,
            } => {
                assert!(eof);
                assert_eq!(row_count, 0);
                assert!(columns.is_empty());
            }
            ColumnCursorBatch::Rows(batch) => panic!(
                "terminal schema default cursor must stay columnar, got {} fallback rows",
                batch.rows.len()
            ),
        }
    });
}

#[test]
fn column_batch_v1_keeps_mixed_cold_hot_and_hot_shadow_rows_columnar() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");

    with_one_connection_server(test_config(data_dir), "wire_mixed_hot_cold", |client| {
        assert!(
            client
                .capabilities()
                .contains(&ProtocolCapability::ColumnBatchV1),
            "public client should negotiate ColumnBatchV1 by default"
        );
        assert_command(
            client
                .execute(
                    "CREATE TABLE wire_mixed_hot_cold (
                        id INTEGER PRIMARY KEY,
                        name TEXT
                    )",
                )
                .expect("create mixed hot/cold table"),
        );
        assert_command(
            client
                .execute("INSERT INTO wire_mixed_hot_cold (id, name) VALUES (1, 'cold')")
                .expect("insert cold row"),
        );
        execute_and_drain(client, "PRAGMA CHECKPOINT");
        assert_command(
            client
                .execute("UPDATE wire_mixed_hot_cold SET name = 'shadow' WHERE id = 1")
                .expect("update cold row into hot shadow"),
        );
        assert_command(
            client
                .execute("INSERT INTO wire_mixed_hot_cold (id, name) VALUES (2, 'hot')")
                .expect("insert hot row"),
        );

        let (values, batches) =
            collect_split_column_batch_rows(client, "SELECT id, name FROM wire_mixed_hot_cold");
        assert_eq!(
            values,
            vec![
                (1, Some("shadow".to_string())),
                (2, Some("hot".to_string()))
            ]
        );
        assert!(
            batches >= 1,
            "mixed cold/hot rows must use typed transport without row fallback"
        );
    });
}

#[test]
fn column_batch_v1_snapshot_cursor_keeps_pre_delete_cold_rows_columnar() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config = test_config(temp.path().join("data"));
    let server = Server::bind_ephemeral(&config).expect("server binds");
    let address = server.local_addr().expect("local addr");
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.run_until(&shutdown));
        let mut reader = connect_and_select(address, "wire_snapshot_typed");
        let mut writer = connect_and_select(address, "wire_snapshot_typed");

        assert_command(
            writer
                .execute(
                    "CREATE TABLE wire_snapshot_typed (
                        id INTEGER PRIMARY KEY,
                        name TEXT
                    )",
                )
                .expect("create snapshot typed table"),
        );
        assert_command(
            writer
                .execute("INSERT INTO wire_snapshot_typed (id, name) VALUES (1, 'old-one')")
                .expect("insert first cold row"),
        );
        assert_command(
            writer
                .execute("INSERT INTO wire_snapshot_typed (id, name) VALUES (2, 'old-two')")
                .expect("insert second cold row"),
        );
        execute_and_drain(&mut writer, "PRAGMA CHECKPOINT");

        reader
            .begin_with_isolation(radixdb_client::TransactionIsolation::Snapshot)
            .expect("begin snapshot transaction");
        assert_command(
            writer
                .execute("DELETE FROM wire_snapshot_typed WHERE id = 2")
                .expect("delete cold row after snapshot starts"),
        );

        let ExecuteResult::Cursor(cursor) = reader
            .execute("SELECT id, name FROM wire_snapshot_typed")
            .expect("open snapshot typed cursor")
        else {
            panic!("SELECT should open a cursor");
        };
        match reader
            .fetch_column_batch(&cursor)
            .expect("snapshot typed fetch should succeed")
        {
            ColumnCursorBatch::Columnar {
                columns,
                row_count,
                eof,
            } => {
                assert_eq!(row_count, 2);
                assert!(!eof);
                assert_eq!(columns.len(), 2);
                match &columns[0] {
                    WireColumn::Int64 { values, nulls } => {
                        assert_eq!(values, &[1, 2]);
                        assert_eq!(nulls, &[false, false]);
                    }
                    other => panic!("id must stay an Int64 typed column, got {other:?}"),
                }
                match &columns[1] {
                    WireColumn::DictionaryText {
                        ids,
                        dictionary,
                        nulls,
                    } => {
                        assert_eq!(ids.len(), 2);
                        assert_eq!(nulls, &[false, false]);
                        let actual = ids
                            .iter()
                            .map(|id| dictionary[*id as usize].as_str())
                            .collect::<Vec<_>>();
                        assert_eq!(actual, vec!["old-one", "old-two"]);
                    }
                    other => panic!("name must stay a DictionaryText typed column, got {other:?}"),
                }
            }
            ColumnCursorBatch::Rows(batch) => panic!(
                "snapshot cold tombstone scan has a typed contract, got {} fallback rows",
                batch.rows.len()
            ),
        }
        match reader
            .fetch_column_batch(&cursor)
            .expect("fetch terminal snapshot typed batch")
        {
            ColumnCursorBatch::Columnar {
                columns,
                row_count,
                eof,
            } => {
                assert!(eof);
                assert_eq!(row_count, 0);
                assert!(columns.is_empty());
            }
            ColumnCursorBatch::Rows(batch) => panic!(
                "terminal snapshot cursor must stay columnar, got {} fallback rows",
                batch.rows.len()
            ),
        }
        reader.rollback().expect("rollback snapshot");
        assert_eq!(
            fetch_column_batch_single_row(
                &mut writer,
                "SELECT id, name FROM wire_snapshot_typed WHERE id = 1"
            ),
            vec![WireValue::Int(1), WireValue::String("old-one".to_string())]
        );
        assert_eq!(
            fetch_count(&mut writer, "SELECT COUNT(*) FROM wire_snapshot_typed"),
            1
        );

        drop(reader);
        drop(writer);
        shutdown.store(true, Ordering::Release);
        server_worker
            .join()
            .expect("server thread joins")
            .expect("server shuts down");
    });
}

#[test]
fn column_batch_v1_rejects_switching_to_row_fetch_after_typed_fetch() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");

    with_one_connection_server(test_config(data_dir.clone()), "wire_fetch_mode", |client| {
        assert_command(
            client
                .execute(
                    "CREATE TABLE wire_fetch_mode (
                        id INTEGER PRIMARY KEY,
                        name TEXT
                    )",
                )
                .expect("create fetch mode table"),
        );
        assert_command(
            client
                .execute("INSERT INTO wire_fetch_mode (id, name) VALUES (1, 'typed')")
                .expect("insert typed row"),
        );
        execute_and_drain(client, "PRAGMA CHECKPOINT");
    });

    with_one_connection_server(test_config(data_dir), "wire_fetch_mode", |client| {
        let ExecuteResult::Cursor(cursor) = client
            .execute("SELECT id, name FROM wire_fetch_mode")
            .expect("open typed cursor")
        else {
            panic!("SELECT should open a cursor");
        };
        match client
            .fetch_column_batch(&cursor)
            .expect("first typed fetch must succeed")
        {
            ColumnCursorBatch::Columnar {
                row_count,
                eof,
                columns,
            } => {
                assert_eq!(row_count, 1);
                assert!(!eof);
                assert_eq!(columns.len(), 2);
            }
            ColumnCursorBatch::Rows(batch) => panic!(
                "clean cold identity query should stay columnar, got {} fallback rows",
                batch.rows.len()
            ),
        }

        let error = client
            .fetch(&cursor)
            .expect_err("row fetch must not continue a typed cursor");
        match error {
            ClientError::Server(failure) => {
                assert_eq!(failure.code, ProtocolErrorCode::CommandsOutOfSync);
                assert!(
                    failure.message.contains("continue a typed cursor"),
                    "unexpected out-of-sync message: {}",
                    failure.message
                );
            }
            other => panic!("expected server CommandsOutOfSync, got {other:?}"),
        }
        client
            .close_cursor(cursor)
            .expect("typed cursor can be closed after out-of-sync fetch attempt");
    });
}

#[test]
fn column_batch_v1_typed_cursor_can_be_cancelled_after_typed_fetch() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");

    with_one_connection_server(
        test_config(data_dir.clone()),
        "wire_typed_cancel",
        |client| {
            assert_command(
                client
                    .execute(
                        "CREATE TABLE wire_typed_cancel (
                        id INTEGER PRIMARY KEY,
                        name TEXT
                    )",
                    )
                    .expect("create typed cancel table"),
            );
            assert_command(
                client
                    .execute("INSERT INTO wire_typed_cancel (id, name) VALUES (1, 'cancel-me')")
                    .expect("insert cancel row"),
            );
            execute_and_drain(client, "PRAGMA CHECKPOINT");
        },
    );

    with_one_connection_server(test_config(data_dir), "wire_typed_cancel", |client| {
        let ExecuteResult::Cursor(cursor) = client
            .execute("SELECT id, name FROM wire_typed_cancel")
            .expect("open typed cursor")
        else {
            panic!("SELECT should open a cursor");
        };
        match client
            .fetch_column_batch(&cursor)
            .expect("typed fetch before cancel must succeed")
        {
            ColumnCursorBatch::Columnar {
                row_count,
                eof,
                columns,
            } => {
                assert_eq!(row_count, 1);
                assert!(!eof);
                assert_eq!(columns.len(), 2);
            }
            ColumnCursorBatch::Rows(batch) => panic!(
                "clean cold identity query should stay columnar before cancel, got {} fallback rows",
                batch.rows.len()
            ),
        }

        client
            .cancel(cursor)
            .expect("typed cursor can be cancelled after typed fetch");

        let ExecuteResult::Cursor(cursor) = client
            .execute("SELECT id FROM wire_typed_cancel")
            .expect("connection should accept a new cursor after typed cancel")
        else {
            panic!("SELECT should open a new cursor");
        };
        let batch = client.fetch(&cursor).expect("fetch after typed cancel");
        assert!(batch.eof);
        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].values, vec![WireValue::Int(1)]);
    });
}

#[test]
fn column_batch_v1_uses_row_fallback_when_vector_is_projected_after_reopen() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");

    with_one_connection_server(test_config(data_dir.clone()), "wire_fallbacks", |client| {
        assert!(
            client
                .capabilities()
                .contains(&ProtocolCapability::ColumnBatchV1),
            "public client should negotiate ColumnBatchV1 by default"
        );
        assert_command(
            client
                .execute(
                    "CREATE TABLE wire_fallbacks (
                        id INTEGER PRIMARY KEY,
                        payload BYTES,
                        document JSON,
                        embedding VECTOR(3)
                    )",
                )
                .expect("create fallback table"),
        );

        let mut params = BTreeMap::new();
        params.insert(
            "payload".to_string(),
            WireValue::Bytes(vec![0, 1, 2, 3, 254, 255, b'R', b'D']),
        );
        params.insert(
            "document".to_string(),
            WireValue::Json(r#"{"kind":"fallback","n":1}"#.to_string()),
        );
        params.insert(
            "embedding".to_string(),
            WireValue::Vector(
                [1.0_f32, 2.5, 3.0]
                    .into_iter()
                    .flat_map(f32::to_le_bytes)
                    .collect(),
            ),
        );

        assert_command(
            client
                .execute_with_parameters(
                    "INSERT INTO wire_fallbacks (id, payload, document, embedding)
                     VALUES (1, :payload, :document, :embedding)",
                    params,
                )
                .expect("insert fallback values"),
        );
        execute_and_drain(client, "PRAGMA CHECKPOINT");
    });

    with_one_connection_server(test_config(data_dir), "wire_fallbacks", |client| {
        let values = fetch_column_batch_single_row(
            client,
            "SELECT payload, document, embedding FROM wire_fallbacks WHERE id = 1",
        );
        assert_eq!(
            values,
            vec![
                WireValue::Bytes(vec![0, 1, 2, 3, 254, 255, b'R', b'D']),
                WireValue::Json(r#"{"kind":"fallback","n":1}"#.to_string()),
                WireValue::Vector(
                    [1.0_f32, 2.5, 3.0]
                        .into_iter()
                        .flat_map(f32::to_le_bytes)
                        .collect(),
                ),
            ]
        );
    });
}
