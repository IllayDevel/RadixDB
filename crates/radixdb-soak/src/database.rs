use std::{fs::File, io, path::Path};

use postgres::{Client as PostgresClient, Config as PostgresConfig, NoTls};
use radixdb_client::{
    ClientError, Connection, ExecuteResult, Row, TransactionIsolation, WireValue,
};

use crate::{
    config::{DatabaseEngine, ResolvedDatabaseConfig},
    status::ServerRuntimeSnapshot,
};

pub enum DatabaseConnection {
    Radixdb(Connection),
    Postgresql(Box<Option<PostgresClient>>, bool),
}

#[derive(Debug)]
pub enum CopyCsvError {
    Retryable(String),
    Fatal(String),
}

impl CopyCsvError {
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

impl std::fmt::Display for CopyCsvError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retryable(message) | Self::Fatal(message) => formatter.write_str(message),
        }
    }
}

fn radixdb_copy_error(error: ClientError) -> CopyCsvError {
    let retryable = error.is_retryable();
    let message = error.to_string();
    if retryable {
        CopyCsvError::Retryable(message)
    } else {
        CopyCsvError::Fatal(message)
    }
}

impl DatabaseConnection {
    pub const fn engine(&self) -> DatabaseEngine {
        match self {
            Self::Radixdb(_) => DatabaseEngine::Radixdb,
            Self::Postgresql(..) => DatabaseEngine::Postgresql,
        }
    }

    pub fn radixdb_mut(&mut self) -> Option<&mut Connection> {
        match self {
            Self::Radixdb(connection) => Some(connection),
            Self::Postgresql(..) => None,
        }
    }

    fn postgres_mut(&mut self) -> Result<&mut PostgresClient, String> {
        match self {
            Self::Postgresql(client, _) => client
                .as_mut()
                .as_mut()
                .ok_or_else(|| "PostgreSQL connection is closed".into()),
            Self::Radixdb(_) => Err("expected PostgreSQL connection".into()),
        }
    }

    pub fn begin(&mut self) -> Result<(), String> {
        match self {
            Self::Radixdb(connection) => connection.begin().map_err(|error| error.to_string()),
            Self::Postgresql(client, in_transaction) => {
                let client = client
                    .as_mut()
                    .as_mut()
                    .ok_or_else(|| "PostgreSQL connection is closed".to_string())?;
                client
                    .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
                    .map_err(postgres_error)?;
                *in_transaction = true;
                Ok(())
            }
        }
    }

    pub fn begin_snapshot(&mut self) -> Result<(), String> {
        match self {
            Self::Radixdb(connection) => connection
                .begin_with_isolation(TransactionIsolation::Snapshot)
                .map_err(|error| error.to_string()),
            Self::Postgresql(client, in_transaction) => {
                let client = client
                    .as_mut()
                    .as_mut()
                    .ok_or_else(|| "PostgreSQL connection is closed".to_string())?;
                client
                    .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    .map_err(postgres_error)?;
                *in_transaction = true;
                Ok(())
            }
        }
    }

    pub fn commit(&mut self) -> Result<(), String> {
        match self {
            Self::Radixdb(connection) => connection.commit().map_err(|error| error.to_string()),
            Self::Postgresql(client, in_transaction) => {
                let client = client
                    .as_mut()
                    .as_mut()
                    .ok_or_else(|| "PostgreSQL connection is closed".to_string())?;
                let result = client.batch_execute("COMMIT").map_err(postgres_error);
                *in_transaction = false;
                result
            }
        }
    }

    pub fn rollback(&mut self) -> Result<(), String> {
        match self {
            Self::Radixdb(connection) => connection.rollback().map_err(|error| error.to_string()),
            Self::Postgresql(client, in_transaction) => {
                let client = client
                    .as_mut()
                    .as_mut()
                    .ok_or_else(|| "PostgreSQL connection is closed".to_string())?;
                let result = client.batch_execute("ROLLBACK").map_err(postgres_error);
                *in_transaction = false;
                result
            }
        }
    }

    pub fn in_transaction(&self) -> bool {
        match self {
            Self::Radixdb(connection) => connection.in_transaction(),
            Self::Postgresql(_, in_transaction) => *in_transaction,
        }
    }

    pub fn shutdown(&mut self) -> Result<(), String> {
        match self {
            Self::Radixdb(connection) => connection.shutdown().map_err(|error| error.to_string()),
            Self::Postgresql(client, in_transaction) => {
                client.as_mut().take();
                *in_transaction = false;
                Ok(())
            }
        }
    }

    pub fn command(&mut self, sql: &str) -> Result<(), String> {
        match self {
            Self::Radixdb(connection) => {
                match connection.execute(sql).map_err(|error| error.to_string())? {
                    ExecuteResult::CommandComplete { .. } => Ok(()),
                    ExecuteResult::Cursor(cursor) => {
                        loop {
                            let batch = connection
                                .fetch(&cursor)
                                .map_err(|error| error.to_string())?;
                            if batch.eof {
                                break;
                            }
                        }
                        Ok(())
                    }
                }
            }
            Self::Postgresql(..) => self
                .postgres_mut()?
                .batch_execute(sql)
                .map_err(postgres_error),
        }
    }

    pub fn command_exactly_one(&mut self, sql: &str) -> Result<(), String> {
        let affected_rows = match self {
            Self::Radixdb(connection) => {
                match connection.execute(sql).map_err(|error| error.to_string())? {
                    ExecuteResult::CommandComplete { affected_rows, .. } => affected_rows,
                    ExecuteResult::Cursor(cursor) => {
                        connection
                            .close_cursor(cursor)
                            .map_err(|error| error.to_string())?;
                        return Err(format!("DML unexpectedly opened a cursor: {sql}"));
                    }
                }
            }
            Self::Postgresql(..) => self
                .postgres_mut()?
                .execute(sql, &[])
                .map_err(postgres_error)?,
        };
        if affected_rows == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected one affected row, got {affected_rows}: {sql}"
            ))
        }
    }

    pub fn scalar_i64(&mut self, sql: &str) -> Result<i64, String> {
        match self {
            Self::Radixdb(connection) => {
                let ExecuteResult::Cursor(cursor) =
                    connection.execute(sql).map_err(|error| error.to_string())?
                else {
                    return Err("scalar query did not open a cursor".into());
                };
                let mut rows = Vec::<Row>::new();
                loop {
                    let batch = connection
                        .fetch(&cursor)
                        .map_err(|error| error.to_string())?;
                    rows.extend(batch.rows);
                    if batch.eof {
                        break;
                    }
                }
                if rows.len() != 1 || rows[0].values.len() != 1 {
                    return Err(format!("scalar query returned {} rows", rows.len()));
                }
                match rows[0].values[0] {
                    WireValue::Int(value) => Ok(value),
                    WireValue::Int32(value) => Ok(i64::from(value)),
                    ref other => Err(format!("scalar query returned {other:?}")),
                }
            }
            Self::Postgresql(..) => {
                // PostgreSQL widens SUM(BIGINT) to NUMERIC while COUNT remains
                // BIGINT. Normalize the single scalar in SQL so the workload
                // has one query execution and one stable i64 contract.
                let normalized = format!(
                    "SELECT CAST(radixdb_soak_scalar.value AS BIGINT) \
                     FROM ({sql}) AS radixdb_soak_scalar(value)"
                );
                let row = self
                    .postgres_mut()?
                    .query_one(&normalized, &[])
                    .map_err(postgres_error)?;
                row.try_get::<_, i64>(0).map_err(|error| {
                    format!("PostgreSQL scalar query returned non-integer: {error}")
                })
            }
        }
    }

    pub fn query_text(&mut self, sql: &str) -> Result<String, String> {
        match self {
            Self::Radixdb(_) => Err("text query is only used by PostgreSQL diagnostics".into()),
            Self::Postgresql(..) => self
                .postgres_mut()?
                .query_one(sql, &[])
                .map_err(postgres_error)?
                .try_get::<_, String>(0)
                .map_err(|error| error.to_string()),
        }
    }

    pub fn copy_csv(&mut self, path: &Path) -> Result<(), CopyCsvError> {
        match self {
            Self::Radixdb(connection) => {
                let sql = format!(
                    "COPY soak_cold_rows FROM '{}' WITH (FORMAT CSV, HEADER true)",
                    path.display()
                );
                match connection.execute(sql).map_err(radixdb_copy_error)? {
                    ExecuteResult::CommandComplete { .. } => Ok(()),
                    ExecuteResult::Cursor(cursor) => {
                        connection
                            .close_cursor(cursor)
                            .map_err(radixdb_copy_error)?;
                        Err(CopyCsvError::Fatal(
                            "RadixDB COPY unexpectedly opened a cursor".to_string(),
                        ))
                    }
                }
            }
            Self::Postgresql(..) => {
                let mut input = File::open(path).map_err(|error| {
                    CopyCsvError::Fatal(format!("open PostgreSQL COPY input: {error}"))
                })?;
                let mut sink = self
                    .postgres_mut()
                    .map_err(CopyCsvError::Fatal)?
                    .copy_in(
                        "COPY soak_cold_rows (id, bucket, value) FROM STDIN \
                         WITH (FORMAT CSV, HEADER true)",
                    )
                    .map_err(|error| {
                        CopyCsvError::Fatal(format!(
                            "start PostgreSQL COPY: {}",
                            postgres_error(error)
                        ))
                    })?;
                io::copy(&mut input, &mut sink).map_err(|error| {
                    CopyCsvError::Fatal(format!("stream PostgreSQL COPY: {error}"))
                })?;
                sink.finish().map_err(|error| {
                    CopyCsvError::Fatal(format!(
                        "finish PostgreSQL COPY: {}",
                        postgres_error(error)
                    ))
                })?;
                Ok(())
            }
        }
    }
}

pub fn connect(config: &ResolvedDatabaseConfig) -> Result<DatabaseConnection, String> {
    match config.engine {
        DatabaseEngine::Radixdb => {
            let mut connection = Connection::connect_with_timeouts(
                config.address,
                config.connect_timeout,
                config.read_timeout,
                config.write_timeout,
            )
            .map_err(|error| format!("connect {}: {error}", config.address))?;
            connection
                .authenticate(
                    config.login.clone(),
                    load_optional_password(config.password_file.as_deref())?,
                )
                .map_err(|error| format!("authenticate: {error}"))?;
            connection
                .select_database(config.name.clone())
                .map_err(|error| format!("select database `{}`: {error}", config.name))?;
            Ok(DatabaseConnection::Radixdb(connection))
        }
        DatabaseEngine::Postgresql => {
            let mut postgres = PostgresConfig::new();
            postgres
                .host(&config.address.ip().to_string())
                .port(config.address.port())
                .user(&config.login)
                .dbname(&config.name)
                .application_name("radixdb-soak-postgresql")
                .connect_timeout(config.connect_timeout);
            if let Some(password) = load_optional_password(config.password_file.as_deref())? {
                postgres.password(password);
            }
            let mut client = postgres.connect(NoTls).map_err(|error| {
                format!(
                    "connect PostgreSQL {}: {}",
                    config.address,
                    postgres_error(error)
                )
            })?;
            let statement_timeout = duration_millis(config.read_timeout);
            let lock_timeout = duration_millis(config.write_timeout);
            client
                .batch_execute(&format!(
                    "SET statement_timeout = {statement_timeout}; \
                     SET lock_timeout = {lock_timeout}; \
                     SET idle_in_transaction_session_timeout = 0"
                ))
                .map_err(|error| {
                    format!("configure PostgreSQL session: {}", postgres_error(error))
                })?;
            Ok(DatabaseConnection::Postgresql(
                Box::new(Some(client)),
                false,
            ))
        }
    }
}

pub fn server_identity(config: &ResolvedDatabaseConfig) -> Result<String, String> {
    let mut connection = connect(config)?;
    match config.engine {
        DatabaseEngine::Radixdb => {
            let connection = connection
                .radixdb_mut()
                .ok_or_else(|| "expected RadixDB connection".to_string())?;
            let status = connection
                .server_status()
                .map_err(|error| format!("server status: {error}"))?;
            let build = status
                .build
                .ok_or_else(|| "server status omitted build identity".to_string())?;
            Ok(format!(
                "radixdb-server {} git={} protocol={} profile={} target={}",
                build.semantic_version,
                build.git_revision,
                build.protocol_version,
                build.build_profile,
                build.target
            ))
        }
        DatabaseEngine::Postgresql => connection.query_text("SELECT version()"),
    }
}

pub fn server_runtime(
    connection: &mut DatabaseConnection,
) -> Result<ServerRuntimeSnapshot, String> {
    match connection {
        DatabaseConnection::Radixdb(connection) => {
            let runtime = connection
                .server_status()
                .map_err(|error| format!("sample server runtime: {error}"))?
                .runtime;
            Ok(ServerRuntimeSnapshot {
                open_databases: runtime.open_databases,
                retained_databases: runtime.retained_databases,
                max_databases: runtime.max_databases,
                active_connections: runtime.active_connections,
                max_connections: runtime.max_connections,
                inflight_frame_bytes: runtime.inflight_frame_bytes,
                max_inflight_frame_bytes: runtime.max_inflight_frame_bytes,
            })
        }
        DatabaseConnection::Postgresql(..) => {
            let row = connection
                .postgres_mut()?
                .query_one(
                    "SELECT
                       (SELECT COUNT(DISTINCT datid) FROM pg_stat_activity
                        WHERE datid IS NOT NULL)::bigint,
                       (SELECT COUNT(*) FROM pg_database WHERE datallowconn)::bigint,
                       (SELECT COUNT(*) FROM pg_stat_activity
                        WHERE datname = current_database())::bigint,
                       current_setting('max_connections')::bigint",
                    &[],
                )
                .map_err(|error| format!("sample PostgreSQL runtime: {}", postgres_error(error)))?;
            Ok(ServerRuntimeSnapshot {
                open_databases: nonnegative_u64(row.get::<_, i64>(0), "open databases")?,
                retained_databases: nonnegative_u64(row.get::<_, i64>(1), "databases")?,
                max_databases: 0,
                active_connections: nonnegative_u64(row.get::<_, i64>(2), "connections")?,
                max_connections: nonnegative_u64(row.get::<_, i64>(3), "max connections")?,
                inflight_frame_bytes: 0,
                max_inflight_frame_bytes: 0,
            })
        }
    }
}

pub fn load_optional_password(path: Option<&Path>) -> Result<Option<String>, String> {
    let Some(path) = path else {
        return Ok(None);
    };
    let value = std::fs::read_to_string(path)
        .map_err(|error| format!("read database password file: {error}"))?;
    let value = value.trim_end_matches(['\r', '\n']);
    if value.is_empty() || value.contains(['\r', '\n']) {
        return Err("database password file must contain one non-empty line".into());
    }
    Ok(Some(value.to_string()))
}

fn duration_millis(duration: std::time::Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn postgres_error(error: postgres::Error) -> String {
    let Some(database) = error.as_db_error() else {
        return error.to_string();
    };
    let mut detail = format!(
        "PostgreSQL SQLSTATE {}: {}",
        database.code().code(),
        database.message()
    );
    if let Some(value) = database.detail() {
        detail.push_str(&format!("; detail={value}"));
    }
    if let Some(value) = database.hint() {
        detail.push_str(&format!("; hint={value}"));
    }
    detail
}

fn nonnegative_u64(value: i64, name: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("PostgreSQL {name} is negative"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_engine_is_explicit() {
        let connection = DatabaseConnection::Postgresql(Box::new(None), false);
        assert_eq!(connection.engine(), DatabaseEngine::Postgresql);
    }
}
