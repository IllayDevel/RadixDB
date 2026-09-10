#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=release/lib.sh
source "${SCRIPT_DIR}/lib.sh"
release_init

mkdir -p "${RUN_DIR}" "${LOG_DIR}" "${RELEASE_ROOT}/data"
acquire_lifecycle_lock

if [[ -f "${PID_FILE}" ]]; then
  if read_pid_record && process_matches_record; then
    if listener_matches_record; then
      echo "radixdb-server already running: pid ${PID}"
      exit 0
    fi
    echo "owned radixdb-server pid ${PID} exists but is not ready" >&2
    exit 1
  fi
  rm -f "${PID_FILE}"
fi

if command -v ss >/dev/null 2>&1 && ss -ltn "sport = :${PORT}" 2>/dev/null | awk 'NR > 1 {found=1} END {exit !found}'; then
  echo "radixdb-server port is already in use: ${HOST}:${PORT}" >&2
  exit 1
fi

cd "${RELEASE_ROOT}"
if command -v setsid >/dev/null 2>&1; then
  setsid "${SERVER_BIN}" --config "${CONFIG}" >>"${LOG_FILE}" 2>&1 < /dev/null 9>&- &
else
  nohup "${SERVER_BIN}" --config "${CONFIG}" >>"${LOG_FILE}" 2>&1 < /dev/null 9>&- &
fi
PID="$!"
for _ in {1..20}; do
  if read_process_state; then
    EXPECTED_START_TIME="${PROCESS_START_TIME}"
    break
  fi
  sleep 0.01
done
if [[ -z "${EXPECTED_START_TIME:-}" ]]; then
  wait "${PID}" 2>/dev/null || true
  echo "radixdb-server failed before its process identity could be recorded" >&2
  exit 1
fi
write_pid_record

for ((attempt = 0; attempt < ${RADIXDB_START_READY_ATTEMPTS:-100}; attempt++)); do
  if ! process_matches_record; then
    rm -f "${PID_FILE}"
    echo "radixdb-server failed to stay running; log ${LOG_FILE}" >&2
    tail -n 40 "${LOG_FILE}" >&2 || true
    exit 1
  fi
  if listener_matches_record; then
    echo "radixdb-server started: pid ${PID}, ${HOST}:${PORT}, log ${LOG_FILE}"
    exit 0
  fi
  sleep "${RADIXDB_START_READY_INTERVAL:-0.1}"
done

echo "radixdb-server did not become ready on ${HOST}:${PORT}; rolling back start" >&2
if terminate_owned_process; then
  rm -f "${PID_FILE}"
else
  echo "start rollback could not terminate owned pid ${PID}; retaining ${PID_FILE}" >&2
fi
tail -n 40 "${LOG_FILE}" >&2 || true
exit 1
