use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

use crate::client::{ClientError, Connection, ExecuteResult, TlsClientConfig, TlsConnection};
use crate::protocol::{ProtocolErrorCode, Row, TransactionIsolation, WireValue};

use super::*;

fn write_private_key(path: &std::path::Path, pem: impl AsRef<[u8]>) {
    use std::os::unix::fs::PermissionsExt;

    std::fs::write(path, pem).expect("write private key");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .expect("restrict private key permissions");
}

#[test]
fn peer_disconnect_probe_never_blocks_after_readiness_is_consumed() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind test listener");
    let address = listener.local_addr().expect("test listener address");
    let client = TcpStream::connect(address).expect("connect test peer");
    let (server, _) = listener.accept().expect("accept test peer");

    let started = Instant::now();
    assert!(!peer_stream_reached_eof(&server));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "an idle live peer probe exceeded the functional liveness deadline"
    );

    drop(client);
    let deadline = Instant::now() + Duration::from_secs(1);
    while !peer_stream_reached_eof(&server) && Instant::now() < deadline {
        thread::yield_now();
    }
    assert!(peer_stream_reached_eof(&server));
}

#[test]
fn ordinary_bind_uses_the_zero_cost_empty_plugin_registry() {
    let temp = tempfile::tempdir().expect("temp dir");
    let server = Server::bind_ephemeral(&test_config(temp.path().join("data")))
        .expect("bind server without plugins");
    let status = server.plugin_registry_status();
    assert_eq!(status.generation, 0);
    assert_eq!(status.packages, 0);
    assert_eq!(status.loaded_library_bytes, 0);
}

#[test]
fn server_executes_sql_through_binary_client() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config = test_config(temp.path().join("data"));
    let server = bind_test_server(&config);
    let address = server.local_addr().expect("local addr");

    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.serve_one());
        let mut client = Connection::connect(address).expect("client connects");
        client.authenticate("root", None).expect("auth");
        client.select_database("test").expect("select database");

        assert!(matches!(
            client
                .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
                .expect("create table"),
            ExecuteResult::CommandComplete { .. }
        ));
        assert!(matches!(
            client
                .execute("INSERT INTO users VALUES (1, 'Alice')")
                .expect("insert"),
            ExecuteResult::CommandComplete {
                affected_rows: 1,
                last_insert_id: 0,
            }
        ));

        let ExecuteResult::Cursor(cursor) = client
            .execute("SELECT name FROM users WHERE id = 1")
            .expect("select")
        else {
            panic!("select should open a cursor");
        };
        let batch = client.fetch(&cursor).expect("fetch");
        assert!(batch.eof);
        assert_eq!(batch.rows.len(), 1);
        assert_eq!(
            batch.rows[0].values,
            vec![WireValue::String("Alice".into())]
        );

        drop(client);
        server_worker
            .join()
            .expect("server thread joins")
            .expect("server serves one connection");
    });
}

#[test]
fn configured_root_password_is_enforced_over_the_wire() {
    let temp = tempfile::tempdir().expect("temp dir");
    let mut config = test_config(temp.path().join("data"));
    let encoded = radixdb_executor::credentials::hash_password_verifier("root-secret")
        .expect("derive root verifier");
    config.authentication.root_password_verifier = Some(
        crate::server::config::RootPasswordVerifier::parse(encoded).expect("valid root verifier"),
    );

    for (password, accepted) in [
        (Some("root-secret".to_string()), true),
        (Some("wrong-secret".to_string()), false),
        (None, false),
    ] {
        let server = bind_test_server(&config);
        let address = server.local_addr().expect("local addr");
        thread::scope(|scope| {
            let server_worker = scope.spawn(|| server.serve_one());
            let mut client = Connection::connect(address).expect("client connects");
            let result = client.authenticate("root", password);
            if accepted {
                result.expect("configured root password accepted");
            } else {
                assert_server_code(
                    result.expect_err("missing or wrong root password must fail"),
                    ProtocolErrorCode::AuthenticationFailed,
                );
            }
            drop(client);
            server_worker
                .join()
                .expect("server thread joins")
                .expect("server serves one connection");
        });
    }
}

#[test]
fn r12_batch_d_tls_and_catalog_principal_are_end_to_end() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let database_dir = data_dir.join("databases").join("secure");
    std::fs::create_dir_all(&database_dir).expect("create database directory");
    let database = crate::Database::open(&format!("file://{}", database_dir.display()))
        .expect("provision database");
    database
        .execute("CREATE TABLE secret_rows (id INTEGER PRIMARY KEY)", ())
        .expect("create protected table");
    database
        .execute("INSERT INTO secret_rows VALUES (7)", ())
        .expect("insert protected row");
    database
        .execute("CREATE PRINCIPAL alice PASSWORD 'correct horse'", ())
        .expect("create principal credential");
    database
        .execute("GRANT CONNECT ON DATABASE secure TO alice", ())
        .expect("grant connect");
    database
        .execute("GRANT USAGE ON SCHEMA public TO alice", ())
        .expect("grant usage");
    database
        .execute("GRANT SELECT ON TABLE secret_rows TO alice", ())
        .expect("grant select");
    database.close().expect("close provisioned database");

    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate certificate");
    let certificate_path = temp.path().join("server-cert.pem");
    let key_path = temp.path().join("server-key.pem");
    std::fs::write(&certificate_path, cert.pem()).expect("write certificate");
    write_private_key(&key_path, signing_key.serialize_pem());

    let mut config = test_config(data_dir);
    config.transport = ServerTransportConfig::Tls {
        certificate_chain: certificate_path.clone(),
        private_key: key_path,
    };
    let server = Server::bind_ephemeral(&config).expect("bind TLS server");
    assert_eq!(
        server.tls_status().expect("TLS status"),
        TlsRuntimeStatus {
            enabled: true,
            generation: 1,
        }
    );
    assert_eq!(
        server.reload_tls().expect("atomic TLS reload").generation,
        2
    );
    let address = server.local_addr().expect("TLS address");
    let tls =
        TlsClientConfig::from_ca_pem(&certificate_path, "localhost").expect("build client trust");

    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.serve_one());
        let mut client = TlsConnection::connect_tls(address, &tls).expect("verified TLS connect");
        let open_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match client.authenticate_database("secure", "alice", "correct horse") {
                Ok(()) => break,
                Err(ClientError::Server(failure))
                    if failure.code == ProtocolErrorCode::AuthenticationFailed
                        && Instant::now() < open_deadline =>
                {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("catalog Principal login: {error}"),
            }
        }
        let ExecuteResult::Cursor(cursor) = client
            .execute("SELECT id FROM secret_rows")
            .expect("authorized query")
        else {
            panic!("query must return a cursor");
        };
        assert_eq!(
            client.fetch(&cursor).expect("fetch protected row").rows[0].values,
            vec![WireValue::Int(7)]
        );
        drop(client);
        server_worker
            .join()
            .expect("TLS server thread")
            .expect("TLS server connection");
    });
}

#[test]
fn r12_batch_d_plaintext_password_login_has_durable_acl_lifecycle() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let database_dir = data_dir.join("databases").join("secure");
    std::fs::create_dir_all(&database_dir).expect("create database directory");
    let database = crate::Database::open(&format!("file://{}", database_dir.display()))
        .expect("provision database");
    database
        .execute("CREATE TABLE secret_rows (id INTEGER PRIMARY KEY)", ())
        .expect("create protected table");
    database
        .execute("INSERT INTO secret_rows VALUES (7)", ())
        .expect("insert protected row");
    database
        .execute("CREATE PRINCIPAL alice PASSWORD 'initial-secret'", ())
        .expect("create principal credential");
    database
        .execute("GRANT CONNECT ON DATABASE secure TO alice", ())
        .expect("grant connect");
    database
        .execute("GRANT USAGE ON SCHEMA public TO alice", ())
        .expect("grant usage");
    database
        .execute("GRANT SELECT ON TABLE secret_rows TO alice", ())
        .expect("grant select");
    database.close().expect("close provisioned database");

    let config = test_config(data_dir);
    assert!(matches!(config.transport, ServerTransportConfig::Plaintext));
    let server = Server::bind_ephemeral(&config).expect("bind plaintext server");
    let address = server.local_addr().expect("plaintext address");
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.run_until(&shutdown));

        let mut administrator = Connection::connect(address).expect("connect bootstrap recovery");
        administrator
            .authenticate("root", None)
            .expect("loopback plaintext bootstrap recovery auth");
        let open_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match administrator.select_database("secure") {
                Ok(()) => break,
                Err(ClientError::Server(failure))
                    if failure.message.contains("opening/recovering")
                        && Instant::now() < open_deadline =>
                {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("select secure database: {error}"),
            }
        }

        let unknown = principal_login_error(address, "secure", "unknown", "initial-secret");
        let wrong_password = principal_login_error(address, "secure", "alice", "wrong-secret");
        assert_eq!(
            unknown, wrong_password,
            "login failure must not enumerate principals"
        );
        assert_eq!(unknown.0, ProtocolErrorCode::AuthenticationFailed);
        assert_eq!(unknown.1, "authentication failed");

        let mut alice = Connection::connect(address).expect("connect principal session");
        alice
            .authenticate_database("secure", "alice", "initial-secret")
            .expect("authenticate durable Principal over ordinary TCP");
        // Authentication already selected the database; a same-name SelectDatabase is
        // intentionally idempotent and must not leak another database session lease.
        alice
            .select_database("secure")
            .expect("idempotent selected database");

        let prepared = alice
            .prepare("SELECT id FROM secret_rows")
            .expect("prepare protected query");
        let ExecuteResult::Cursor(cursor) = alice
            .execute_prepared(&prepared, Vec::new())
            .expect("execute prepared query with session Principal")
        else {
            panic!("prepared SELECT must open a cursor");
        };
        assert_eq!(
            alice.fetch(&cursor).expect("fetch authorized cursor").rows[0].values,
            vec![WireValue::Int(7)]
        );

        administrator
            .execute("ALTER PRINCIPAL alice DISABLE")
            .expect("disable principal");
        // Defined revocation policy: DISABLE rejects future authentication. An already
        // authenticated session retains its stable ID and continues while its ACL remains.
        assert_eq!(
            fetch_count(&mut alice, "SELECT COUNT(*) FROM secret_rows"),
            1
        );
        let disabled = principal_login_error(address, "secure", "alice", "initial-secret");
        assert_eq!(disabled, unknown);

        administrator
            .execute("ALTER PRINCIPAL alice ENABLE")
            .expect("enable principal");
        administrator
            .execute("ALTER PRINCIPAL alice PASSWORD 'rotated-secret'")
            .expect("rotate principal password");
        assert_eq!(
            principal_login_error(address, "secure", "alice", "initial-secret"),
            unknown,
            "old verifier must stop admitting reconnects"
        );
        let mut rotated = Connection::connect(address).expect("connect rotated credential");
        rotated
            .authenticate_database("secure", "alice", "rotated-secret")
            .expect("rotated credential authenticates");

        administrator
            .execute("REVOKE CONNECT ON DATABASE secure FROM alice")
            .expect("revoke connect");
        let active_error = alice
            .execute_prepared(&prepared, Vec::new())
            .expect_err("active session must observe transaction-visible CONNECT revoke");
        assert_server_code(active_error, ProtocolErrorCode::AuthorizationDenied);
        assert_eq!(
            principal_login_error(address, "secure", "alice", "rotated-secret"),
            unknown
        );

        administrator
            .execute("GRANT CONNECT ON DATABASE secure TO alice")
            .expect("restore connect");
        let mut reconnected = Connection::connect(address).expect("connect after grant");
        reconnected
            .authenticate_database("secure", "alice", "rotated-secret")
            .expect("reconnect after grant");
        assert_eq!(
            fetch_count(&mut reconnected, "SELECT COUNT(*) FROM secret_rows"),
            1
        );

        drop(reconnected);
        drop(rotated);
        drop(alice);
        drop(administrator);
        shutdown.store(true, Ordering::Release);
        server_worker
            .join()
            .expect("plaintext server thread")
            .expect("plaintext server shutdown");
    });
}

#[test]
fn r12_batch_d_tls_policy_rejects_invalid_identity_and_plaintext_downgrade() {
    let temp = tempfile::tempdir().expect("temp dir");
    let trusted = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("generate trusted certificate");
    let certificate_path = temp.path().join("trusted-cert.pem");
    let key_path = temp.path().join("trusted-key.pem");
    std::fs::write(&certificate_path, trusted.cert.pem()).expect("write trusted certificate");
    write_private_key(&key_path, trusted.signing_key.serialize_pem());

    let insecure_key_path = temp.path().join("insecure-key.pem");
    write_private_key(&insecure_key_path, trusted.signing_key.serialize_pem());
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&insecure_key_path, std::fs::Permissions::from_mode(0o644))
            .expect("make negative key fixture group-readable");
    }
    let mut insecure_config = test_config(temp.path().join("data-insecure-key"));
    insecure_config.transport = ServerTransportConfig::Tls {
        certificate_chain: certificate_path.clone(),
        private_key: insecure_key_path,
    };
    let error = match Server::bind_ephemeral(&insecure_config) {
        Err(error) => error,
        Ok(_) => panic!("TLS mode must reject a group-readable private key"),
    };
    assert!(error.to_string().contains("private key permissions"));

    let valid_tls =
        TlsClientConfig::from_ca_pem(&certificate_path, "localhost").expect("valid TLS policy");
    let wrong_host_tls = TlsClientConfig::from_ca_pem(&certificate_path, "wrong.example")
        .expect("wrong-host TLS policy builds");
    let unrelated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("generate unrelated authority");
    let unrelated_path = temp.path().join("unrelated.pem");
    std::fs::write(&unrelated_path, unrelated.cert.pem()).expect("write unrelated authority");
    let unknown_ca_tls =
        TlsClientConfig::from_ca_pem(&unrelated_path, "localhost").expect("unknown CA policy");

    for (label, tls) in [
        ("wrong-host", &wrong_host_tls),
        ("unknown-ca", &unknown_ca_tls),
    ] {
        let mut config = test_config(temp.path().join(format!("data-{label}")));
        config.transport = ServerTransportConfig::Tls {
            certificate_chain: certificate_path.clone(),
            private_key: key_path.clone(),
        };
        let server = Server::bind_ephemeral(&config).expect("bind TLS negative server");
        let address = server.local_addr().expect("TLS negative address");
        thread::scope(|scope| {
            let worker = scope.spawn(|| server.serve_one());
            assert!(
                TlsConnection::connect_tls(address, tls).is_err(),
                "{label} identity must fail closed"
            );
            let _ = worker.join().expect("TLS negative server thread");
        });
    }

    let signing_key = rcgen::KeyPair::generate().expect("generate expired key");
    let mut expired_params =
        rcgen::CertificateParams::new(vec!["localhost".to_owned()]).expect("expired params");
    expired_params.not_before = rcgen::date_time_ymd(2009, 1, 1);
    expired_params.not_after = rcgen::date_time_ymd(2010, 1, 1);
    let expired = expired_params
        .self_signed(&signing_key)
        .expect("generate expired certificate");
    let expired_certificate_path = temp.path().join("expired-cert.pem");
    let expired_key_path = temp.path().join("expired-key.pem");
    std::fs::write(&expired_certificate_path, expired.pem()).expect("write expired certificate");
    write_private_key(&expired_key_path, signing_key.serialize_pem());
    let expired_tls = TlsClientConfig::from_ca_pem(&expired_certificate_path, "localhost")
        .expect("expired certificate remains a syntactically valid trust anchor");
    let mut expired_config = test_config(temp.path().join("data-expired"));
    expired_config.transport = ServerTransportConfig::Tls {
        certificate_chain: expired_certificate_path,
        private_key: expired_key_path,
    };
    let expired_server = Server::bind_ephemeral(&expired_config).expect("bind expired TLS server");
    let expired_address = expired_server.local_addr().expect("expired TLS address");
    thread::scope(|scope| {
        let worker = scope.spawn(|| expired_server.serve_one());
        assert!(
            TlsConnection::connect_tls(expired_address, &expired_tls).is_err(),
            "expired server identity must fail closed"
        );
        let _ = worker.join().expect("expired TLS server thread");
    });

    let mut downgrade_config = test_config(temp.path().join("data-downgrade"));
    downgrade_config.transport = ServerTransportConfig::Tls {
        certificate_chain: certificate_path.clone(),
        private_key: key_path.clone(),
    };
    let downgrade_server =
        Server::bind_ephemeral(&downgrade_config).expect("bind downgrade TLS server");
    let downgrade_address = downgrade_server
        .local_addr()
        .expect("downgrade TLS address");
    thread::scope(|scope| {
        let worker = scope.spawn(|| downgrade_server.serve_one());
        assert!(
            Connection::connect(downgrade_address).is_err(),
            "plaintext protocol handshake must not be accepted by a TLS endpoint"
        );
        let _ = worker.join().expect("downgrade server thread");
    });

    let mut reload_config = test_config(temp.path().join("data-reload"));
    reload_config.transport = ServerTransportConfig::Tls {
        certificate_chain: certificate_path.clone(),
        private_key: key_path,
    };
    let reload_server = Server::bind_ephemeral(&reload_config).expect("bind reload TLS server");
    std::fs::write(&certificate_path, "not a PEM certificate")
        .expect("replace certificate with invalid material");
    assert!(reload_server.reload_tls().is_err());
    assert_eq!(
        reload_server
            .tls_status()
            .expect("TLS status after failed reload"),
        TlsRuntimeStatus {
            enabled: true,
            generation: 1,
        },
        "failed reload must preserve the prior in-memory generation"
    );
    let reload_address = reload_server.local_addr().expect("reload TLS address");
    thread::scope(|scope| {
        let worker = scope.spawn(|| reload_server.serve_one());
        let client = TlsConnection::connect_tls(reload_address, &valid_tls)
            .expect("old certificate remains active after failed reload");
        drop(client);
        worker
            .join()
            .expect("reload server thread")
            .expect("reload server connection");
    });
}

#[test]
fn server_reports_last_insert_id_for_auto_increment_primary_key() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config = test_config(temp.path().join("data"));
    let server = bind_test_server(&config);
    let address = server.local_addr().expect("local addr");

    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.serve_one());
        let mut client = Connection::connect(address).expect("client connects");
        client.authenticate("root", None).expect("auth");
        client.select_database("test").expect("select database");

        assert_command(
            client
                .execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT)")
                .expect("create table"),
        );

        assert_eq!(
            client
                .execute("INSERT INTO items (name) VALUES ('first')")
                .expect("insert first"),
            ExecuteResult::CommandComplete {
                affected_rows: 1,
                last_insert_id: 1,
            }
        );
        assert_eq!(
            client
                .execute("INSERT INTO items (name) VALUES ('second')")
                .expect("insert second"),
            ExecuteResult::CommandComplete {
                affected_rows: 1,
                last_insert_id: 2,
            }
        );

        let ExecuteResult::Cursor(cursor) = client
            .execute("INSERT INTO items (name) VALUES ('third') RETURNING id")
            .expect("insert returning")
        else {
            panic!("insert returning should open a cursor");
        };
        let batch = client.fetch(&cursor).expect("fetch returning");
        assert!(batch.eof);
        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].values, vec![WireValue::Int(3)]);

        drop(client);
        server_worker
            .join()
            .expect("server thread joins")
            .expect("server serves one connection");
    });
}

#[test]
fn server_roundtrips_uuid_values_through_binary_client() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config = test_config(temp.path().join("data"));
    let server = bind_test_server(&config);
    let address = server.local_addr().expect("local addr");
    let uuid = [
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00,
        0x00,
    ];

    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.serve_one());
        let mut client = Connection::connect(address).expect("client connects");
        client.authenticate("root", None).expect("auth");
        client.select_database("test").expect("select database");

        assert_command(
            client
                .execute("CREATE TABLE items (id UUID PRIMARY KEY, name TEXT)")
                .expect("create table"),
        );

        let mut params = BTreeMap::new();
        params.insert("id".to_string(), WireValue::Uuid(uuid));
        assert_command(
            client
                .execute_with_parameters(
                    "INSERT INTO items (id, name) VALUES (:id, 'wire')",
                    params,
                )
                .expect("insert uuid parameter"),
        );

        let ExecuteResult::Cursor(cursor) = client
            .execute("SELECT id FROM items WHERE name = 'wire'")
            .expect("select uuid")
        else {
            panic!("select should open a cursor");
        };
        let batch = client.fetch(&cursor).expect("fetch");
        assert!(batch.eof);
        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0].values, vec![WireValue::Uuid(uuid)]);

        drop(client);
        server_worker
            .join()
            .expect("server thread joins")
            .expect("server serves one connection");
    });
}

#[test]
fn server_updates_matching_row_with_conjunctive_named_predicate() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config = test_config(temp.path().join("data"));

    with_one_connection_server(config, "raf_update", |client| {
        assert_command(
            client
                .execute(
                    "CREATE TABLE raf080b_scoped_customers (
                            id INTEGER PRIMARY KEY,
                            group_id INTEGER NOT NULL,
                            name TEXT NOT NULL,
                            revision INTEGER NOT NULL
                        )",
                )
                .expect("create table"),
        );
        assert_eq!(
            client
                .execute(
                    "INSERT INTO raf080b_scoped_customers (id, group_id, name, revision)
                         VALUES (101, 1, 'created', 0)",
                )
                .expect("insert fixture"),
            ExecuteResult::CommandComplete {
                affected_rows: 1,
                last_insert_id: 0,
            }
        );

        let mut params = BTreeMap::new();
        params.insert("id".to_string(), WireValue::Int(101));
        params.insert("expected".to_string(), WireValue::Int(0));
        params.insert("name".to_string(), WireValue::String("updated".into()));
        assert_eq!(
            client
                .execute_with_parameters(
                    "UPDATE raf080b_scoped_customers
                         SET name = :name
                         WHERE id = :id AND revision = :expected",
                    params,
                )
                .expect("update by pk and revision"),
            ExecuteResult::CommandComplete {
                affected_rows: 1,
                last_insert_id: 0,
            }
        );
        assert_eq!(
            fetch_single_value(
                client,
                "SELECT name FROM raf080b_scoped_customers WHERE id = 101"
            ),
            WireValue::String("updated".into())
        );

        let mut mismatch = BTreeMap::new();
        mismatch.insert("id".to_string(), WireValue::Int(101));
        mismatch.insert("expected".to_string(), WireValue::Int(99));
        mismatch.insert("name".to_string(), WireValue::String("wrong".into()));
        assert_eq!(
            client
                .execute_with_parameters(
                    "UPDATE raf080b_scoped_customers
                         SET name = :name
                         WHERE id = :id AND revision = :expected",
                    mismatch,
                )
                .expect("version mismatch update"),
            ExecuteResult::CommandComplete {
                affected_rows: 0,
                last_insert_id: 0,
            }
        );
        assert_eq!(
            fetch_single_value(
                client,
                "SELECT name FROM raf080b_scoped_customers WHERE id = 101"
            ),
            WireValue::String("updated".into())
        );
    });
}

#[test]
fn server_renames_index_through_binary_client() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config = test_config(temp.path().join("data"));

    with_one_connection_server(config, "raf_alter_index", |client| {
        assert_command(
            client
                .execute(
                    "CREATE TABLE raf080b_orders (
                            id INTEGER PRIMARY KEY,
                            revision INTEGER NOT NULL,
                            group_id INTEGER NOT NULL,
                            payload TEXT NOT NULL
                        )",
                )
                .expect("create table"),
        );
        assert_command(
            client
                .execute(
                    "INSERT INTO raf080b_orders (id, revision, group_id, payload)
                         VALUES (1, 0, 10, 'created')",
                )
                .expect("insert fixture"),
        );
        assert_command(
            client
                .execute(
                    "CREATE UNIQUE INDEX orders_revision_idx
                         ON raf080b_orders (revision, group_id)",
                )
                .expect("create index"),
        );
        assert_command(
            client
                .execute("ALTER INDEX orders_revision_idx RENAME TO ___orders_revision_idx")
                .expect("rename index"),
        );

        let ExecuteResult::Cursor(cursor) = client
            .execute("SHOW INDEXES FROM raf080b_orders")
            .expect("show indexes")
        else {
            panic!("show indexes should open a cursor");
        };
        let batch = client.fetch(&cursor).expect("fetch indexes");
        assert!(batch.eof);

        let mut has_old = false;
        let mut has_new = false;
        for Row { values } in batch.rows {
            match &values[1] {
                WireValue::String(name) if name == "orders_revision_idx" => has_old = true,
                WireValue::String(name) if name == "___orders_revision_idx" => {
                    has_new = true;
                    assert_eq!(values[2], WireValue::String("(revision, group_id)".into()));
                    assert_eq!(values[4], WireValue::Bool(true));
                }
                _ => {}
            }
        }
        assert!(!has_old, "old index name must disappear");
        assert!(has_new, "new index name must be visible");

        assert_eq!(
            fetch_single_value(
                client,
                "SELECT payload FROM raf080b_orders WHERE revision = 0 AND group_id = 10"
            ),
            WireValue::String("created".into())
        );
    });
}

#[test]
fn transactions_keep_uncommitted_rows_private_and_rollback_discards_them() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config = test_config(temp.path().join("data"));
    let server = bind_test_server(&config);
    let address = server.local_addr().expect("local addr");
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.run_until(&shutdown));

        let mut writer = connect_and_select(address, "tx_test");
        let mut reader = connect_and_select(address, "tx_test");

        assert_command(
            writer
                .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
                .expect("create table"),
        );

        writer.begin().expect("begin rollback transaction");
        assert_command(
            writer
                .execute("INSERT INTO items VALUES (1, 'rolled-back')")
                .expect("insert rolled-back row"),
        );
        assert_eq!(fetch_count(&mut writer, "SELECT COUNT(*) FROM items"), 1);
        assert_eq!(fetch_count(&mut reader, "SELECT COUNT(*) FROM items"), 0);
        writer.rollback().expect("rollback");
        assert!(!writer.in_transaction());
        assert_eq!(fetch_count(&mut reader, "SELECT COUNT(*) FROM items"), 0);

        writer.begin().expect("begin commit transaction");
        assert_command(
            writer
                .execute("INSERT INTO items VALUES (2, 'committed')")
                .expect("insert committed row"),
        );
        writer.commit().expect("commit");
        assert!(!writer.in_transaction());
        assert_eq!(fetch_count(&mut reader, "SELECT COUNT(*) FROM items"), 1);
        assert_eq!(
            fetch_single_value(&mut reader, "SELECT name FROM items WHERE id = 2"),
            WireValue::String("committed".into())
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
fn r12_batch_a_tcp_set_isolation_is_connection_local() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config = test_config(temp.path().join("data"));
    let server = bind_test_server(&config);
    let address = server.local_addr().expect("local addr");
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.run_until(&shutdown));
        let mut snapshot = connect_and_select(address, "r12_batch_a_tcp_isolation");
        let mut committed = connect_and_select(address, "r12_batch_a_tcp_isolation");
        let mut writer = connect_and_select(address, "r12_batch_a_tcp_isolation");

        assert_command(
            writer
                .execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
                .expect("create table"),
        );
        assert_command(
            writer
                .execute("INSERT INTO items VALUES (1)")
                .expect("seed"),
        );

        assert_command(
            snapshot
                .execute("SET ISOLATION_LEVEL = 'SNAPSHOT'")
                .expect("configure snapshot connection"),
        );
        assert_eq!(
            fetch_single_value(&mut snapshot, "PRAGMA ISOLATION_LEVEL"),
            WireValue::String("SNAPSHOT".into())
        );
        assert_eq!(
            fetch_single_value(&mut committed, "PRAGMA ISOLATION_LEVEL"),
            WireValue::String("READ COMMITTED".into())
        );
        snapshot
            .begin_with_isolation(TransactionIsolation::Snapshot)
            .expect("begin snapshot");
        committed.begin().expect("begin read committed");
        assert_eq!(fetch_count(&mut snapshot, "SELECT COUNT(*) FROM items"), 1);
        assert_eq!(fetch_count(&mut committed, "SELECT COUNT(*) FROM items"), 1);

        assert_command(
            writer
                .execute("INSERT INTO items VALUES (2)")
                .expect("write"),
        );
        assert_eq!(
            fetch_count(&mut snapshot, "SELECT COUNT(*) FROM items"),
            1,
            "SET leaked or snapshot isolation was not retained"
        );
        assert_eq!(
            fetch_count(&mut committed, "SELECT COUNT(*) FROM items"),
            2,
            "SET changed the neighbouring connection default"
        );

        snapshot.rollback().expect("rollback snapshot");
        committed.rollback().expect("rollback committed");
        drop(writer);
        drop(committed);
        drop(snapshot);
        shutdown.store(true, Ordering::Release);
        server_worker
            .join()
            .expect("server thread")
            .expect("server shutdown");
    });
}

#[test]
fn r2_l05_c_shutdown_cancels_an_executing_tcp_statement_within_deadline() {
    use std::sync::mpsc;
    use std::time::Instant;

    let temp = tempfile::tempdir().expect("temp dir");
    let config = test_config(temp.path().join("data"));
    let server = bind_test_server(&config);
    let address = server.local_addr().expect("local addr");
    let shutdown = AtomicBool::new(false);
    let (entered_tx, entered_rx) = mpsc::channel();
    let _execute_hook =
        super::super::session::ExecuteSqlTestHookGuard::install(std::sync::Arc::new(move || {
            let _ = entered_tx.send(());
        }));

    thread::scope(|scope| {
        let (server_done_tx, server_done_rx) = mpsc::channel();
        let server_ref = &server;
        let shutdown_ref = &shutdown;
        let server_worker = scope.spawn(move || {
            let result = server_ref.run_until(shutdown_ref);
            server_done_tx.send(()).expect("report server shutdown");
            result
        });
        let mut client = connect_and_select(address, "shutdown_active");
        client.begin().expect("begin active shutdown transaction");
        let statement_worker = scope.spawn(move || client.execute("SELECT SLEEP(2)"));
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("statement reached server execution boundary");

        let shutdown_started = Instant::now();
        shutdown.store(true, Ordering::Release);
        let completed_within_deadline = server_done_rx
            .recv_timeout(Duration::from_millis(750))
            .is_ok();
        if !completed_within_deadline {
            server_done_rx
                .recv_timeout(Duration::from_secs(4))
                .expect("server eventually exits after uncancelled statement");
        }
        let elapsed = shutdown_started.elapsed();
        let _ = statement_worker.join().expect("statement client thread");
        server_worker
            .join()
            .expect("server thread")
            .expect("server shutdown result");
        assert!(
            completed_within_deadline,
            "server waited {elapsed:?} for an executing statement after shutdown"
        );
    });
}

#[test]
fn navigable_reference_disconnect_releases_request_and_session_resources() {
    let temp = tempfile::tempdir().expect("temp dir");
    let mut config = test_config(temp.path().join("data"));
    config.max_connections = 2;
    let server = bind_test_server(&config);
    let address = server.local_addr().expect("local addr");
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.run_until(&shutdown));
        let mut setup = connect_and_select(address, "navigation_disconnect");
        for sql in [
            "CREATE TABLE nr12_disconnect_targets (id INTEGER PRIMARY KEY, label TEXT)",
            "CREATE TABLE nr12_disconnect_roots (
                    id INTEGER PRIMARY KEY,
                    target_id INTEGER REFERENCES nr12_disconnect_targets(id)
                )",
            "INSERT INTO nr12_disconnect_targets VALUES (1, 'target')",
            "INSERT INTO nr12_disconnect_roots VALUES (1, 1)",
        ] {
            assert_command(setup.execute(sql).expect("navigation fixture statement"));
        }
        drop(setup);

        let (reached_tx, reached_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Arc::new(Mutex::new(release_rx));
        let hook_release = Arc::clone(&release_rx);
        let _navigation_hook =
            crate::executor::navigation::SourceMaterializedTestHookGuard::install(Arc::new(
                move |plan, context| {
                    let is_probe = plan.edges().first().is_some_and(|edge| {
                        edge.source_column().table().table_name() == "nr12_disconnect_roots"
                    });
                    if !is_probe {
                        return;
                    }
                    reached_tx
                        .send(context.clone())
                        .expect("report navigation execution context");
                    hook_release
                        .lock()
                        .expect("navigation disconnect release lock")
                        .recv()
                        .expect("release disconnected navigation lookup");
                },
            ));

        let client_worker = scope.spawn(move || {
            let mut client = Connection::connect_with_timeouts(
                address,
                Duration::from_millis(250),
                Duration::from_millis(50),
                Duration::from_millis(250),
            )
            .expect("navigation client connects");
            client.authenticate("root", None).expect("auth");
            client
                .select_database("navigation_disconnect")
                .expect("select database");
            let result =
                client.execute("SELECT target_id.label FROM nr12_disconnect_roots WHERE id = 1");
            assert!(result.is_err(), "blocked navigation request must time out");
            assert!(client.is_poisoned());
        });

        let observer = reached_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("navigation source reaches lifecycle gate");
        assert_eq!(observer.active_reference_expands(), 1);
        client_worker.join().expect("timed-out client thread");

        let cancellation_deadline = Instant::now() + Duration::from_secs(2);
        while !observer.is_cancelled() && Instant::now() < cancellation_deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            observer.is_cancelled(),
            "disconnect did not cancel navigation"
        );
        release_tx
            .send(())
            .expect("release disconnected navigation execution");

        let cleanup_deadline = Instant::now() + Duration::from_secs(2);
        while (observer.active_reference_expands() != 0
            || server.runtime.active_connections.load(Ordering::Acquire) != 0
            || server.runtime.active_execution_count() != 0)
            && Instant::now() < cleanup_deadline
        {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(observer.active_reference_expands(), 0);
        assert_eq!(server.runtime.active_execution_count(), 0);
        assert_eq!(server.runtime.active_connections.load(Ordering::Acquire), 0);

        let mut health = connect_and_select(address, "navigation_disconnect");
        assert_eq!(
            fetch_single_value(&mut health, "SELECT 1"),
            WireValue::Int(1)
        );
        drop(health);

        shutdown.store(true, Ordering::Release);
        server_worker
            .join()
            .expect("server thread")
            .expect("server shutdown result");
    });
}

#[test]
fn r6_l01_a_disconnected_inflight_query_releases_connection_permit() {
    use std::time::Instant;

    let temp = tempfile::tempdir().expect("temp dir");
    let mut config = test_config(temp.path().join("data"));
    config.max_connections = 1;
    let server = bind_test_server(&config);
    let address = server.local_addr().expect("local addr");
    let shutdown = AtomicBool::new(false);

    let released_within_deadline = thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.run_until(&shutdown));
        let mut all_released = true;
        for _attempt in 0..128 {
            let connect_deadline = Instant::now() + Duration::from_millis(750);
            let mut timed_out = loop {
                match Connection::connect_with_timeouts(
                    address,
                    Duration::from_millis(100),
                    Duration::from_millis(50),
                    Duration::from_millis(100),
                ) {
                    Ok(client) => break Some(client),
                    Err(_) if Instant::now() < connect_deadline => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break None,
                }
            };
            let Some(mut timed_out) = timed_out.take() else {
                all_released = false;
                break;
            };
            timed_out.authenticate("root", None).expect("auth");
            timed_out
                .select_database("disconnect_active")
                .expect("select database");

            if timed_out
                .execute(
                    "SELECT COUNT(*) FROM generate_series(1, 5000) AS a(value) \
                         CROSS JOIN generate_series(1, 5000) AS b(value)",
                )
                .is_ok()
            {
                all_released = false;
                break;
            }
            if !timed_out.is_poisoned() || timed_out.is_reusable() {
                all_released = false;
                break;
            }
            drop(timed_out);

            let deadline = Instant::now() + Duration::from_millis(750);
            let mut health_succeeded = false;
            while Instant::now() < deadline {
                if let Ok(mut health) = Connection::connect_with_timeouts(
                    address,
                    Duration::from_millis(100),
                    Duration::from_millis(100),
                    Duration::from_millis(100),
                ) {
                    if health.authenticate("root", None).is_ok()
                        && health.select_database("disconnect_active").is_ok()
                        && fetch_single_value(&mut health, "SELECT 1") == WireValue::Int(1)
                    {
                        health_succeeded = true;
                        break;
                    }
                }
                thread::sleep(Duration::from_millis(10));
            }
            if !health_succeeded {
                all_released = false;
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }

        if all_released {
            let mut final_health = Connection::connect_with_timeouts(
                address,
                Duration::from_millis(100),
                Duration::from_millis(100),
                Duration::from_millis(100),
            )
            .expect("final health client connects");
            final_health.authenticate("root", None).expect("auth");
            final_health
                .select_database("disconnect_active")
                .expect("select database");
            all_released = fetch_single_value(&mut final_health, "SELECT 1") == WireValue::Int(1);
        }

        shutdown.store(true, Ordering::Release);
        server_worker
            .join()
            .expect("server thread")
            .expect("server shutdown result");
        all_released
    });

    assert!(
            released_within_deadline,
            "PRV-B10 invariant session_cleanup: peer disconnect must cancel the in-flight statement and release its permit"
        );
}

#[test]
fn r6_l01_c_disconnect_releases_cold_and_indexed_join_sessions() {
    use std::time::Instant;

    struct ShutdownOnDrop<'a>(&'a AtomicBool);
    impl Drop for ShutdownOnDrop<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    fn local_socket_fd_count(port: u16) -> usize {
        let expected_port = format!("{port:04X}");
        let mut inodes = std::collections::HashSet::new();
        for table in ["/proc/net/tcp", "/proc/net/tcp6"] {
            let Ok(contents) = std::fs::read_to_string(table) else {
                continue;
            };
            for line in contents.lines().skip(1) {
                let fields = line.split_whitespace().collect::<Vec<_>>();
                if fields.len() > 9
                    && fields[1]
                        .rsplit_once(':')
                        .is_some_and(|(_, port)| port == expected_port)
                {
                    inodes.insert(fields[9].to_string());
                }
            }
        }
        std::fs::read_dir("/proc/self/fd")
            .expect("read process fd directory")
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::read_link(entry.path()).ok())
            .filter_map(|target| target.to_str().map(str::to_string))
            .filter(|target| {
                target
                    .strip_prefix("socket:[")
                    .and_then(|value| value.strip_suffix(']'))
                    .is_some_and(|inode| inodes.contains(inode))
            })
            .count()
    }

    fn close_wait_count(port: u16) -> usize {
        let expected_port = format!("{port:04X}");
        std::fs::read_to_string("/proc/net/tcp")
            .expect("read Linux TCP table")
            .lines()
            .skip(1)
            .filter(|line| {
                let fields = line.split_whitespace().collect::<Vec<_>>();
                fields.len() > 3
                    && fields[1]
                        .rsplit_once(':')
                        .is_some_and(|(_, port)| port == expected_port)
                    && fields[3] == "08"
            })
            .count()
    }

    let temp = tempfile::tempdir().expect("temp dir");
    let mut config = test_config(temp.path().join("data"));
    config.max_connections = 1;
    let server = bind_test_server(&config);
    let address = server.local_addr().expect("local addr");
    let shutdown = AtomicBool::new(false);

    thread::scope(|scope| {
        let shutdown_guard = ShutdownOnDrop(&shutdown);
        let server_worker = scope.spawn(|| server.run_until(&shutdown));
        let mut setup = connect_and_select(address, "disconnect_storage_modes");
        for sql in [
            "CREATE TABLE cold_rows (id INTEGER PRIMARY KEY, payload INTEGER)",
            "INSERT INTO cold_rows SELECT value, value % 97 FROM generate_series(1, 1000)",
            "CREATE TABLE join_outer (id INTEGER PRIMARY KEY, join_key INTEGER)",
            "INSERT INTO join_outer SELECT value, value % 100 FROM generate_series(1, 50000)",
            "CREATE TABLE join_inner (id INTEGER PRIMARY KEY, join_key INTEGER)",
            "INSERT INTO join_inner SELECT value, value % 100 FROM generate_series(1, 50000)",
            "CREATE INDEX idx_join_inner_key ON join_inner(join_key)",
            "CREATE TABLE hash_outer (id INTEGER PRIMARY KEY, join_key INTEGER)",
            "INSERT INTO hash_outer SELECT value, value % 100 FROM generate_series(1, 15000)",
            "CREATE TABLE hash_inner (id INTEGER PRIMARY KEY, join_key INTEGER)",
            "INSERT INTO hash_inner SELECT value, value % 100 FROM generate_series(1, 15000)",
            "PRAGMA CHECKPOINT",
        ] {
            match setup
                .execute(sql)
                .unwrap_or_else(|error| panic!("setup statement failed: {sql}: {error}"))
            {
                ExecuteResult::CommandComplete { .. } => {}
                ExecuteResult::Cursor(cursor) => loop {
                    let batch = setup.fetch(&cursor).expect("drain setup cursor");
                    if batch.eof {
                        break;
                    }
                },
            }
        }
        // The scalar aggregate forces the indexed join to consume all
        // 25M matching pairs before it can publish one result row. A plain
        // projection is pull-streamed in small typed batches and can
        // legitimately outrun the client's short read deadline in release.
        let indexed_join_sql = "SELECT SUM(pair_sum) FROM (\
                 SELECT o.id + i.id AS pair_sum FROM join_outer o \
                 INNER JOIN join_inner i ON o.join_key = i.join_key LIMIT 25000000\
                 ) indexed_pairs";
        let plan_cursor = match setup
            .execute(format!("EXPLAIN {indexed_join_sql}"))
            .expect("explain indexed join")
        {
            ExecuteResult::Cursor(cursor) => cursor,
            result => panic!("EXPLAIN returned non-cursor result: {result:?}"),
        };
        let mut plan_lines = Vec::new();
        loop {
            let batch = setup.fetch(&plan_cursor).expect("fetch indexed join plan");
            for row in batch.rows {
                if let Some(WireValue::String(line)) = row.values.into_iter().next() {
                    plan_lines.push(line);
                }
            }
            if batch.eof {
                break;
            }
        }
        assert!(
            plan_lines.iter().any(|line| line.contains("Index")),
            "fixture must exercise indexed join path: {plan_lines:#?}"
        );
        let parallel_join_sql = "SELECT * FROM hash_outer o \
                 INNER JOIN hash_inner i ON o.join_key = i.join_key";
        let parallel_plan_cursor = match setup
            .execute(format!("EXPLAIN {parallel_join_sql}"))
            .expect("explain parallel hash join")
        {
            ExecuteResult::Cursor(cursor) => cursor,
            result => panic!("EXPLAIN returned non-cursor result: {result:?}"),
        };
        let mut parallel_plan_lines = Vec::new();
        loop {
            let batch = setup
                .fetch(&parallel_plan_cursor)
                .expect("fetch parallel hash join plan");
            for row in batch.rows {
                if let Some(WireValue::String(line)) = row.values.into_iter().next() {
                    parallel_plan_lines.push(line);
                }
            }
            if batch.eof {
                break;
            }
        }
        assert!(
            parallel_plan_lines.iter().any(|line| line.contains("Hash")),
            "fixture must exercise hash join path: {parallel_plan_lines:#?}"
        );
        drop(setup);

        // Account for the process-wide Rayon pool in the steady-state
        // baseline rather than mistaking its lazy initialization for a
        // leaked session thread after the parallel-join probe.
        #[cfg(feature = "parallel")]
        let _ = rayon::current_num_threads();

        let baseline_socket_fds = local_socket_fd_count(address.port());

        for sql in [
            "SELECT SUM(c.payload + g.value) FROM cold_rows c \
                 CROSS JOIN generate_series(1, 50000) g(value)",
            indexed_join_sql,
            parallel_join_sql,
        ] {
            let cancellation_count = PEER_DISCONNECT_CANCELLATIONS.load(Ordering::Relaxed);
            let parallel_cancellation_count =
                crate::executor::parallel::PARALLEL_JOIN_CANCELLATION_OBSERVED
                    .load(Ordering::Relaxed);
            // The other two probes intentionally use a tiny transport deadline
            // to force disconnect while server-side work is in flight. The
            // parallel probe must first receive its pull-cursor handle; making
            // that handshake share the 10 ms deadline turns ordinary scheduler
            // contention into a spurious WouldBlock failure in the full suite.
            let read_timeout = if sql == parallel_join_sql {
                Duration::from_secs(1)
            } else {
                Duration::from_millis(10)
            };
            let connect_deadline = Instant::now() + Duration::from_secs(1);
            let mut client = loop {
                match Connection::connect_with_timeouts(
                    address,
                    Duration::from_millis(250),
                    read_timeout,
                    Duration::from_millis(250),
                ) {
                    Ok(client) => break client,
                    Err(_) if Instant::now() < connect_deadline => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("mode probe connects: {error}"),
                }
            };
            client.authenticate("root", None).expect("auth");
            client
                .select_database("disconnect_storage_modes")
                .expect("select database");
            if sql == parallel_join_sql {
                // The parallel JOIN is a pull cursor: disconnect while the
                // cursor is live so teardown releases its hash table,
                // bounded probe batch and snapshot.
                match client.execute(sql).expect("parallel cursor execute") {
                    ExecuteResult::Cursor(_) => {}
                    result => panic!("parallel join returned non-cursor result: {result:?}"),
                }
            } else if sql == indexed_join_sql {
                // Depending on where the lazy aggregate is opened, either
                // EXECUTE or the first FETCH owns the blocking work. Both
                // are valid; one of them must still cross the transport
                // deadline and exercise peer cancellation.
                if let Ok(result) = client.execute(sql) {
                    let cursor = match result {
                        ExecuteResult::Cursor(cursor) => cursor,
                        result => {
                            panic!("indexed join returned non-cursor result: {result:?}")
                        }
                    };
                    assert!(
                        client.fetch(&cursor).is_err(),
                        "indexed aggregate probe must time out while fetching: {sql}"
                    );
                }
                assert!(client.is_poisoned());
            } else {
                assert!(
                    client.execute(sql).is_err(),
                    "mode probe must time out: {sql}"
                );
                assert!(client.is_poisoned());
            }
            drop(client);

            let deadline = Instant::now() + Duration::from_secs(1);
            while PEER_DISCONNECT_CANCELLATIONS.load(Ordering::Relaxed) == cancellation_count
                && Instant::now() < deadline
            {
                thread::sleep(Duration::from_millis(5));
            }
            assert!(
                PEER_DISCONNECT_CANCELLATIONS.load(Ordering::Relaxed) > cancellation_count,
                "server did not observe peer disconnect for: {sql}"
            );
            let mut recovered = false;
            while Instant::now() < deadline {
                if let Ok(mut health) = Connection::connect_with_timeouts(
                    address,
                    Duration::from_millis(100),
                    Duration::from_millis(100),
                    Duration::from_millis(100),
                ) {
                    if health.authenticate("root", None).is_ok()
                        && health.select_database("disconnect_storage_modes").is_ok()
                        && fetch_single_value(&mut health, "SELECT 1") == WireValue::Int(1)
                    {
                        drop(health);
                        recovered = true;
                        break;
                    }
                }
                thread::sleep(Duration::from_millis(10));
            }
            assert!(
                recovered,
                "server did not recover the permit for: {sql}; indexed plan: {plan_lines:#?}",
            );
            if sql == parallel_join_sql {
                assert!(
                    crate::executor::parallel::PARALLEL_JOIN_CANCELLATION_OBSERVED
                        .load(Ordering::Relaxed)
                        > parallel_cancellation_count,
                    "parallel hash join did not observe the peer cancellation"
                );
            }

            while Instant::now() < deadline
                && (local_socket_fd_count(address.port()) > baseline_socket_fds
                    || close_wait_count(address.port()) != 0)
            {
                thread::sleep(Duration::from_millis(10));
            }
            assert_eq!(close_wait_count(address.port()), 0, "CLOSE-WAIT leaked");
            assert!(
                local_socket_fd_count(address.port()) <= baseline_socket_fds,
                "server-port socket FD leaked"
            );
        }

        let shutdown_started = Instant::now();
        shutdown.store(true, Ordering::Release);
        server_worker
            .join()
            .expect("server thread")
            .expect("server shutdown result");
        assert!(
            shutdown_started.elapsed() < Duration::from_secs(30),
            "shutdown exceeded the functional liveness deadline"
        );
        drop(shutdown_guard);
    });
}

#[test]
fn r2_l05_c_release_stop_wrapper_refuses_invalid_and_foreign_processes() {
    use std::process::{Command, Stdio};

    let temp = tempfile::tempdir().expect("temp dir");
    let pid_file = temp.path().join("radixdb-server.pid");
    std::fs::write(&pid_file, "-1").expect("write invalid pid file");
    let invalid_status = Command::new("bash")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/release/stop.sh"))
        .env("RADIXDB_PID_FILE", &pid_file)
        .status()
        .expect("run release stop wrapper with invalid pid");
    assert_eq!(invalid_status.code(), Some(65));

    let ready_file = temp.path().join("ready");
    let mut child = Command::new("bash")
        .arg("-c")
        .arg("trap '' TERM; printf ready > \"$1\"; while :; do sleep 1; done")
        .arg("r2-l05-stop-fixture")
        .arg(&ready_file)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn TERM-ignoring fixture");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !ready_file.exists() && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert!(ready_file.exists(), "fixture installed its TERM handler");
    let stat = std::fs::read_to_string(format!("/proc/{}/stat", child.id()))
        .expect("read fixture process stat");
    let stat_fields = stat
        .split_once(") ")
        .expect("process stat command delimiter")
        .1;
    let start_time = stat_fields
        .split_whitespace()
        .nth(19)
        .expect("process stat start time");
    std::fs::write(&pid_file, format!("{} {start_time}\n", child.id()))
        .expect("write isolated pid record");

    let wrong_identity_status = Command::new("bash")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/release/stop.sh"))
        .env("RADIXDB_PID_FILE", &pid_file)
        .status()
        .expect("run release stop wrapper against a foreign process");
    assert_eq!(wrong_identity_status.code(), Some(65));
    assert!(
        child.try_wait().expect("inspect foreign fixture").is_none(),
        "identity mismatch must not signal the foreign process"
    );

    child.kill().expect("stop foreign fixture after refusal");
    let child_status = child.wait().expect("reap fixture");
    assert!(
        !child_status.success(),
        "test cleanup must kill the foreign fixture"
    );
    std::fs::remove_file(&pid_file).expect("remove foreign pid record");
}

#[test]
fn committed_transaction_survives_restart_and_rollback_stays_gone() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");

    with_one_connection_server(test_config(data_dir.clone()), "restart_test", |client| {
        assert_command(
            client
                .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
                .expect("create table"),
        );

        client.begin().expect("begin committed transaction");
        assert_command(
            client
                .execute("INSERT INTO items VALUES (1, 'committed-before-restart')")
                .expect("insert committed row"),
        );
        client.commit().expect("commit");

        client.begin().expect("begin rolled-back transaction");
        assert_command(
            client
                .execute("INSERT INTO items VALUES (2, 'rolled-back-before-restart')")
                .expect("insert rolled-back row"),
        );
        client.rollback().expect("rollback");
    });

    with_one_connection_server(test_config(data_dir), "restart_test", |client| {
        assert_eq!(fetch_count(client, "SELECT COUNT(*) FROM items"), 1);
        assert_eq!(
            fetch_single_value(client, "SELECT name FROM items WHERE id = 1"),
            WireValue::String("committed-before-restart".into())
        );
        assert_eq!(
            fetch_count(client, "SELECT COUNT(*) FROM items WHERE id = 2"),
            0
        );
    });
}

#[test]
fn stock_server_scheduler_executes_persists_and_resumes_jobs_over_tcp() {
    let temp = tempfile::tempdir().expect("temp dir");
    let data_dir = temp.path().join("data");
    let config = test_config(data_dir);

    let recurring_before_restart = {
        let server = bind_test_server(&config);
        let address = server.local_addr().expect("local addr");
        let shutdown = AtomicBool::new(false);
        thread::scope(|scope| {
            let worker = scope.spawn(|| server.run_until(&shutdown));
            let mut client = connect_and_select_eventually(address, "jobs");
            assert_command(
                client
                    .execute(
                        "CREATE TABLE job_effects (id INTEGER PRIMARY KEY, executions INTEGER NOT NULL)",
                    )
                    .expect("create job effect relation"),
            );
            assert_command(
                client
                    .execute("INSERT INTO job_effects VALUES (1, 0), (2, 0)")
                    .expect("seed job effects"),
            );
            assert_command(
                client
                    .execute(
                        "CREATE PROCEDURE bump_effect(IN input_id INTEGER NOT NULL) LANGUAGE RADIX \
                         SECURITY INVOKER AS BEGIN UPDATE job_effects SET executions = executions + 1 \
                         WHERE id = :input_id; END;",
                    )
                    .expect("create scheduled procedure"),
            );
            assert_command(
                client
                    .execute(
                        "CREATE JOB once_job SCHEDULE AT TIMESTAMP '2020-01-01T00:00:00Z' \
                         RUN AS radix_system CALL bump_effect(1) ENABLE;",
                    )
                    .expect("create one-time job"),
            );
            assert_command(
                client
                    .execute(
                        "CREATE JOB recurring_job SCHEDULE EVERY INTERVAL '1 SECOND' \
                         RUN AS radix_system CALL bump_effect(2) ENABLE;",
                    )
                    .expect("create recurring job"),
            );

            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let once = fetch_count(
                    &mut client,
                    "SELECT executions FROM job_effects WHERE id = 1",
                );
                let recurring = fetch_count(
                    &mut client,
                    "SELECT executions FROM job_effects WHERE id = 2",
                );
                if once == 1 && recurring >= 1 {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "stock scheduler did not make progress"
                );
                thread::sleep(Duration::from_millis(20));
            }

            let history = fetch_count(
                &mut client,
                "SELECT COUNT(*) FROM radix_system_job_history WHERE outcome = 'succeeded'",
            );
            assert!(history >= 2, "public durable job history is incomplete");
            let status = client.server_status().expect("server runtime status");
            assert!(status.runtime.job_scheduler_cycles > 0);
            assert!(status.runtime.job_attempts_started >= 2);
            assert!(status.runtime.job_attempts_succeeded >= 2);
            assert_eq!(status.runtime.job_attempts_failed, 0);
            assert_eq!(status.runtime.job_attempts_active, 0);

            let recurring = fetch_count(
                &mut client,
                "SELECT executions FROM job_effects WHERE id = 2",
            );
            drop(client);
            shutdown.store(true, Ordering::Release);
            worker
                .join()
                .expect("scheduler server thread")
                .expect("scheduler server shutdown");
            recurring
        })
    };

    let server = bind_test_server(&config);
    let address = server.local_addr().expect("restart local addr");
    let shutdown = AtomicBool::new(false);
    thread::scope(|scope| {
        let worker = scope.spawn(|| server.run_until(&shutdown));
        let mut client = connect_and_select_eventually(address, "jobs");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let recurring = fetch_count(
                &mut client,
                "SELECT executions FROM job_effects WHERE id = 2",
            );
            if recurring > recurring_before_restart {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "persisted interval schedule did not resume after restart"
            );
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            fetch_count(
                &mut client,
                "SELECT executions FROM job_effects WHERE id = 1"
            ),
            1,
            "one-time schedule must not be duplicated after restart"
        );
        drop(client);
        shutdown.store(true, Ordering::Release);
        worker
            .join()
            .expect("restarted scheduler thread")
            .expect("restarted scheduler shutdown");
    });
}

fn test_config(data_dir: std::path::PathBuf) -> ServerConfig {
    ServerConfig {
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        // Non-zero placeholder for production validation. The test binder
        // replaces it with the port of its already-owned listener.
        port: 1,
        data_dir,
        transport: Default::default(),
        authentication: Default::default(),
        max_connections: 8,
        max_inflight_frame_bytes: crate::server::config::default_max_inflight_frame_bytes(),
        max_databases: crate::server::config::default_max_databases(),
        max_database_name_bytes: crate::server::config::default_max_database_name_bytes(),
        connect_timeout_secs: 5,
        connection_idle_timeout_secs: 30,
        net_read_timeout_secs: 30,
        net_write_timeout_secs: 30,
        cursor_batch_max_rows: 128,
        cursor_batch_max_bytes: 1024 * 1024,
        max_frame_bytes: 4 * 1024 * 1024,
        copy_max_transaction_bytes: crate::server::config::default_copy_max_transaction_bytes(),
        max_compaction_jobs: crate::server::config::default_max_compaction_jobs(),
        storage_cpu_workers: crate::server::config::default_storage_cpu_workers(),
        page_cache_level: crate::server::config::default_page_cache_level(),
        page_cache_max_bytes: crate::server::config::default_page_cache_max_bytes(),
        page_cache_memory_reserve: crate::server::config::default_page_cache_memory_reserve(),
        target_volume_rows: crate::server::config::default_target_volume_rows(),
        seal_hot_bytes_threshold: crate::server::config::default_seal_hot_bytes_threshold(),
        seal_incremental_hot_bytes_threshold:
            crate::server::config::default_seal_incremental_hot_bytes_threshold(),
        read_queue_depth: 1,
    }
}

fn with_one_connection_server(
    config: ServerConfig,
    database: &str,
    work: impl FnOnce(&mut Connection),
) {
    let server = bind_test_server(&config);
    let address = server.local_addr().expect("local addr");
    thread::scope(|scope| {
        let server_worker = scope.spawn(|| server.serve_one());
        let mut client = connect_and_select_eventually(address, database);
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

fn connect_and_select_eventually(address: SocketAddr, database: &str) -> Connection {
    let mut client = Connection::connect(address).expect("client connects");
    client.authenticate("root", None).expect("auth");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match client.select_database(database) {
            Ok(()) => return client,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "database did not become selectable: {error}"
                );
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

fn assert_command(result: ExecuteResult) {
    assert!(
        matches!(result, ExecuteResult::CommandComplete { .. }),
        "expected command completion, got {result:?}"
    );
}

fn fetch_count(client: &mut Connection, sql: &str) -> i64 {
    match fetch_single_value(client, sql) {
        WireValue::Int(value) => value,
        value => panic!("expected integer count, got {value:?}"),
    }
}

fn fetch_single_value(client: &mut Connection, sql: &str) -> WireValue {
    let ExecuteResult::Cursor(cursor) = client.execute(sql).expect("select") else {
        panic!("select should open a cursor");
    };
    let batch = client.fetch(&cursor).expect("fetch");
    assert!(batch.eof);
    assert_eq!(batch.rows.len(), 1);
    let Row { values } = batch.rows.into_iter().next().expect("single row");
    assert_eq!(values.len(), 1);
    values.into_iter().next().expect("single value")
}

fn principal_login_error(
    address: SocketAddr,
    database: &str,
    login: &str,
    password: &str,
) -> (ProtocolErrorCode, String) {
    let mut client = Connection::connect(address).expect("connect rejected principal attempt");
    match client
        .authenticate_database(database, login, password)
        .expect_err("principal authentication must be rejected")
    {
        ClientError::Server(failure) => (failure.code, failure.message),
        error => panic!("expected classified authentication rejection, got {error}"),
    }
}

fn assert_server_code(error: ClientError, expected: ProtocolErrorCode) {
    match error {
        ClientError::Server(failure) => assert_eq!(failure.code, expected),
        error => panic!("expected classified server error {expected:?}, got {error}"),
    }
}

fn bind_test_server(config: &ServerConfig) -> Server {
    config.validate().expect("valid test server config");
    std::fs::create_dir_all(config.data_dir.join("databases")).expect("create test database root");
    let listener = TcpListener::bind(SocketAddr::new(config.bind_ip, 0))
        .expect("bind ephemeral test listener");
    let mut bound_config = config.clone();
    bound_config.port = listener.local_addr().expect("test listener address").port();
    Server {
        listener,
        config: bound_config,
        databases: Mutex::new(BTreeMap::new()),
        runtime: Arc::new(RuntimeState::new()),
        served: AtomicBool::new(false),
        tls: None,
        plugin_registry: Arc::new(PluginRegistry::empty()),
    }
}
