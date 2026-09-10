#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=release/lib.sh
source "${SCRIPT_DIR}/lib.sh"
release_init

if [[ ! -f "${PID_FILE}" ]]; then
  echo "stopped"
  exit 3
fi
if ! read_pid_record; then
  echo "invalid pid record: ${PID_FILE}" >&2
  exit 1
fi
if ! process_matches_record; then
  echo "stale or foreign pid record: ${PID_FILE}" >&2
  exit 1
fi
if ! listener_matches_record; then
  echo "starting/not-ready: pid ${PID}, ${HOST}:${PORT}" >&2
  exit 2
fi
echo "running: pid ${PID}, ${HOST}:${PORT}"
