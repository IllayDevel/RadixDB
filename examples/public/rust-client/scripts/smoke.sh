#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${EXAMPLE_ROOT}/../../.." && pwd)"
ORM_EXAMPLE_ROOT="${REPO_ROOT}/examples/public/rust-orm"
RUNTIME_DIR="${EXAMPLE_ROOT}/runtime"
SERVER_DATA_DIR="${RUNTIME_DIR}/server-data"
LOG_FILE="${RUNTIME_DIR}/server.log"
CONFIG_FILE="${RUNTIME_DIR}/server.toml"
PID_FILE="${RUNTIME_DIR}/radixdb-server.pid"
PORT="${RADIXDB_CLIENT_SMOKE_PORT:-15442}"
ADDRESS="127.0.0.1:${PORT}"
SERVER_BIN="${RADIXDB_SERVER_BIN:-${REPO_ROOT}/target/release/radixdb-server}"
DATABASE_PREFIX="radixtrade_client_smoke"
DATABASE_NAME="${RADIXDB_CLIENT_SMOKE_DATABASE:-${DATABASE_PREFIX}_$(date +%Y%m%d%H%M%S)}"

mkdir -p "${RUNTIME_DIR}" "${SERVER_DATA_DIR}"

if [[ ! -x "${SERVER_BIN}" ]]; then
  echo "missing ${SERVER_BIN}" >&2
  echo "build it first: cargo build --release --bin radixdb-server" >&2
  exit 2
fi

if command -v ss >/dev/null 2>&1; then
  PORT_OWNER="$(ss -ltnp "sport = :${PORT}" 2>/dev/null | awk 'NR > 1 { print; exit }')"
  if [[ -n "${PORT_OWNER}" ]]; then
    echo "port ${PORT} is already in use" >&2
    echo "${PORT_OWNER}" >&2
    exit 1
  fi
fi

cat >"${CONFIG_FILE}" <<EOF
[server]
bind_ip = "127.0.0.1"
port = ${PORT}
data_dir = "${SERVER_DATA_DIR}"
max_connections = 16
connect_timeout_secs = 10
connection_idle_timeout_secs = 3600
net_read_timeout_secs = 30
net_write_timeout_secs = 60
cursor_batch_max_rows = 1024
cursor_batch_max_bytes = 8388608
max_frame_bytes = 67108864
copy_max_transaction_bytes = 536870912
target_volume_rows = 1048576
seal_hot_bytes_threshold = 67108864
seal_incremental_hot_bytes_threshold = 16777216
read_queue_depth = 1
EOF

"${SERVER_BIN}" --config "${CONFIG_FILE}" >"${LOG_FILE}" 2>&1 &
PID="$!"
echo "${PID}" >"${PID_FILE}"

cleanup() {
  if kill -0 "${PID}" 2>/dev/null; then
    kill -TERM "${PID}" 2>/dev/null || true
    for _ in {1..100}; do
      if ! kill -0 "${PID}" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done
  fi
  rm -f "${PID_FILE}"
}
trap cleanup EXIT

for _ in {1..100}; do
  if ! kill -0 "${PID}" 2>/dev/null; then
    echo "radixdb-server exited early; log follows" >&2
    tail -n 80 "${LOG_FILE}" >&2 || true
    exit 1
  fi
  if (: >"/dev/tcp/127.0.0.1/${PORT}") >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

if ! (: >"/dev/tcp/127.0.0.1/${PORT}") >/dev/null 2>&1; then
  echo "radixdb-server did not become ready on ${ADDRESS}; log follows" >&2
  tail -n 80 "${LOG_FILE}" >&2 || true
  exit 1
fi

echo "radixdb-server ready on ${ADDRESS}; database ${DATABASE_NAME}"

cargo run --locked --offline --manifest-path "${EXAMPLE_ROOT}/Cargo.toml" --bin basic -- \
  "${ADDRESS}" "${DATABASE_NAME}_basic"

cargo run --locked --offline --manifest-path "${EXAMPLE_ROOT}/Cargo.toml" --bin parameters -- \
  "${ADDRESS}" "${DATABASE_NAME}_parameters"

cargo run --locked --offline --manifest-path "${EXAMPLE_ROOT}/Cargo.toml" --bin import_radixtrade -- \
  "${ADDRESS}" "${DATABASE_NAME}_import" "${REPO_ROOT}/examples/public/radixtrade"

RADIXDB_ADDRESS="${ADDRESS}" \
RADIXDB_DATABASE="${DATABASE_NAME}_orm" \
RADIXDB_LOGIN="root" \
cargo run --locked --offline --manifest-path "${ORM_EXAMPLE_ROOT}/Cargo.toml" \
  --bin orm_quickstart -- --transaction-smoke

echo "Rust client and ORM transaction public examples smoke passed."
