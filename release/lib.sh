#!/usr/bin/env bash

release_init() {
  local require_endpoint="${1:-1}"
  SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
  RELEASE_ROOT="${RADIXDB_RELEASE_ROOT:-${SCRIPT_DIR}}"
  SERVER_BIN="${RADIXDB_SERVER_BIN:-${RELEASE_ROOT}/bin/radixdb-server}"
  CONFIG="${RADIXDB_RELEASE_CONFIG:-${RELEASE_ROOT}/server.toml}"
  RUN_DIR="${RELEASE_ROOT}/run"
  LOG_DIR="${RELEASE_ROOT}/logs"
  PID_FILE="${RADIXDB_PID_FILE:-${RUN_DIR}/radixdb-server.pid}"
  LOCK_FILE="${RADIXDB_LOCK_FILE:-${RUN_DIR}/lifecycle.lock}"
  LOG_FILE="${RADIXDB_LOG_FILE:-${LOG_DIR}/server.log}"
  SERVER_BIN_REAL="$(realpath -e -- "${SERVER_BIN}" 2>/dev/null || true)"
  if [[ -n "${SERVER_BIN_REAL}" ]]; then
    SERVER_BIN="${SERVER_BIN_REAL}"
  fi
  if [[ -z "${SERVER_BIN_REAL}" || ! -x "${SERVER_BIN}" ]]; then
    echo "missing executable ${SERVER_BIN}" >&2
    return 66
  fi
  HOST=""
  PORT=""
  if [[ "${require_endpoint}" == 1 ]]; then
    local endpoint extra
    endpoint="$("${SERVER_BIN}" --config "${CONFIG}" --print-endpoint)" || {
      echo "server rejected config ${CONFIG}" >&2
      return 64
    }
    read -r HOST PORT extra <<<"${endpoint}"
    if [[ -n "${extra:-}" || -z "${HOST:-}" || ! "${PORT:-}" =~ ^[1-9][0-9]*$ || "${PORT}" -gt 65535 ]]; then
      echo "server returned invalid endpoint for ${CONFIG}: ${endpoint}" >&2
      return 64
    fi
  fi
}

acquire_lifecycle_lock() {
  mkdir -p "${RUN_DIR}"
  command -v flock >/dev/null 2>&1 || {
    echo "flock is required for release lifecycle ownership" >&2
    return 69
  }
  exec 9>"${LOCK_FILE}"
  if ! flock -n 9; then
    echo "another release lifecycle transition owns ${LOCK_FILE}" >&2
    return 75
  fi
}

read_process_state() {
  local stat_line rest
  stat_line="$(<"/proc/${PID}/stat")" || return 1
  rest="${stat_line##*) }"
  PROCESS_STATE="${rest%% *}"
  PROCESS_START_TIME="$(awk '{print $20}' <<<"${rest}")"
  [[ -n "${PROCESS_STATE}" && -n "${PROCESS_START_TIME}" ]]
}

read_pid_record() {
  [[ -f "${PID_FILE}" ]] || return 1
  local extra
  read -r PID EXPECTED_START_TIME extra <"${PID_FILE}" || return 1
  [[ -z "${extra:-}" && "${PID:-}" =~ ^[1-9][0-9]*$ && "${EXPECTED_START_TIME:-}" =~ ^[0-9]+$ ]]
}

process_matches_record() {
  kill -0 "${PID}" 2>/dev/null || return 1
  read_process_state || return 1
  [[ "${PROCESS_STATE}" != "Z" && "${PROCESS_START_TIME}" == "${EXPECTED_START_TIME}" ]] || return 1
  local actual_exe
  actual_exe="$(readlink -f -- "/proc/${PID}/exe" 2>/dev/null || true)"
  [[ -n "${SERVER_BIN_REAL}" && "${actual_exe}" == "${SERVER_BIN_REAL}" ]]
}

listener_matches_record() {
  if command -v ss >/dev/null 2>&1; then
    ss -ltnp "sport = :${PORT}" 2>/dev/null | grep -F "pid=${PID}," >/dev/null
  else
    (: >"/dev/tcp/${HOST}/${PORT}") >/dev/null 2>&1
  fi
}

write_pid_record() {
  local temporary="${PID_FILE}.tmp.$$"
  (umask 077; printf '%s %s\n' "${PID}" "${EXPECTED_START_TIME}" >"${temporary}")
  mv -f -- "${temporary}" "${PID_FILE}"
  sync -d "${PID_FILE}" 2>/dev/null || true
}

wait_for_process_exit() {
  local attempts="$1" interval="$2"
  for ((attempt = 0; attempt < attempts; attempt++)); do
    process_matches_record || return 0
    sleep "${interval}"
  done
  ! process_matches_record
}

terminate_owned_process() {
  process_matches_record || return 0
  kill -TERM "${PID}" 2>/dev/null || true
  if wait_for_process_exit "${RADIXDB_TERM_ATTEMPTS:-100}" "${RADIXDB_TERM_INTERVAL:-0.1}"; then
    return 0
  fi
  process_matches_record || return 0
  kill -KILL "${PID}" 2>/dev/null || true
  wait_for_process_exit "${RADIXDB_KILL_ATTEMPTS:-50}" "${RADIXDB_KILL_INTERVAL:-0.1}"
}
