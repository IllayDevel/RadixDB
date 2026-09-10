#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=release/lib.sh
source "${SCRIPT_DIR}/lib.sh"
release_init 0
acquire_lifecycle_lock

if [[ ! -f "${PID_FILE}" ]]; then
  echo "radixdb-server is not running"
  exit 0
fi
if ! read_pid_record; then
  echo "refusing invalid pid record: ${PID_FILE}" >&2
  exit 65
fi
if ! process_matches_record; then
  echo "refusing to signal stale or foreign pid ${PID}" >&2
  exit 65
fi
if terminate_owned_process; then
  rm -f "${PID_FILE}"
  echo "radixdb-server stopped"
  exit 0
fi
echo "radixdb-server remains alive after TERM/KILL: pid ${PID}" >&2
exit 1
