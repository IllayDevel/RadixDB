#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_BIN="${RADIXDB_TEST_SERVER_BIN:-${REPO_DIR}/target/debug/radixdb-server}"
[[ -x "${SERVER_BIN}" ]] || { echo "build radixdb-server before this gate" >&2; exit 1; }

TEST_ROOT="$(mktemp -d)"
cleanup() {
  RADIXDB_RELEASE_ROOT="${TEST_ROOT}" RADIXDB_SERVER_BIN="${SERVER_BIN}" \
    "${REPO_DIR}/release/stop.sh" >/dev/null 2>&1 || true
  rm -rf -- "${TEST_ROOT}"
}
trap cleanup EXIT

PORT="$((20000 + RANDOM % 20000))"
mkdir -p "${TEST_ROOT}/data" "${TEST_ROOT}/run" "${TEST_ROOT}/logs"
sed -e "s/^port = .*/port = ${PORT}/" \
    -e "s|^data_dir = .*|data_dir = \"${TEST_ROOT}/data\" # legal inline TOML comment|" \
    "${REPO_DIR}/release/server.toml" >"${TEST_ROOT}/server.toml"
sed -i "s/^port = .*/port = ${PORT} # parsed by the server binary/" \
  "${TEST_ROOT}/server.toml"

export RADIXDB_RELEASE_ROOT="${TEST_ROOT}"
export RADIXDB_SERVER_BIN="${SERVER_BIN}"
export RADIXDB_RELEASE_CONFIG="${TEST_ROOT}/server.toml"

# A live foreign PID in the record is stale, never "already running" and
# never signalled. start replaces only the record and launches its own binary.
SHELL_START="$(awk '{rest=$0; sub(/^[^)]*\) /, "", rest); split(rest,a," "); print a[20]}' "/proc/$$/stat")"
printf '%s %s\n' "$$" "${SHELL_START}" >"${TEST_ROOT}/run/radixdb-server.pid"
# Two concurrent starts must converge on one owned process. Depending on which
# transition gets the lock first, the peer either observes the ready process or
# fails with the explicit lifecycle-lock status.
set +e
"${REPO_DIR}/release/start.sh" >"${TEST_ROOT}/start-a.out" 2>"${TEST_ROOT}/start-a.err" &
START_A=$!
"${REPO_DIR}/release/start.sh" >"${TEST_ROOT}/start-b.out" 2>"${TEST_ROOT}/start-b.err" &
START_B=$!
wait "${START_A}"
STATUS_A=$?
wait "${START_B}"
STATUS_B=$?
set -e
if [[ "${STATUS_A}" -ne 0 && "${STATUS_B}" -ne 0 ]]; then
  cat "${TEST_ROOT}/start-a.err" "${TEST_ROOT}/start-b.err" >&2
  echo "neither concurrent start established process ownership" >&2
  exit 1
fi
"${REPO_DIR}/release/status.sh"
kill -0 "$$"
# Stop owns the recorded process even if an operator has moved/broken the next
# start configuration after the process became ready.
mv "${TEST_ROOT}/server.toml" "${TEST_ROOT}/server.toml.offline"
"${REPO_DIR}/release/stop.sh"
mv "${TEST_ROOT}/server.toml.offline" "${TEST_ROOT}/server.toml"
[[ ! -e "${TEST_ROOT}/run/radixdb-server.pid" ]]

# A child that never listens must be terminated and its pid record removed.
cat >"${TEST_ROOT}/not-ready.rs" <<'RS'
fn main() {
    if std::env::args().any(|arg| arg == "--print-endpoint") {
        println!("{}", std::env::var("RADIXDB_FAKE_ENDPOINT").unwrap());
        return;
    }
    std::thread::sleep(std::time::Duration::from_secs(60));
}
RS
rustc "${TEST_ROOT}/not-ready.rs" -o "${TEST_ROOT}/not-ready"
if RADIXDB_SERVER_BIN="${TEST_ROOT}/not-ready" RADIXDB_FAKE_ENDPOINT="127.0.0.1 ${PORT}" \
   RADIXDB_START_READY_ATTEMPTS=2 RADIXDB_START_READY_INTERVAL=0.01 \
   "${REPO_DIR}/release/start.sh"; then
  echo "not-ready child was accepted" >&2
  exit 1
fi
[[ ! -e "${TEST_ROOT}/run/radixdb-server.pid" ]]
! pgrep -f "${TEST_ROOT}/not-ready" >/dev/null

echo "release lifecycle contract: ok"
