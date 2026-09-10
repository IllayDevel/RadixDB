#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 1 || "$#" -gt 2 ]]; then
  echo "usage: $0 BENCHMARK_ROOT [POSTGRES_PORT]" >&2
  exit 64
fi

BENCHMARK_ROOT="$(realpath -e -- "$1")"
PG_ROOT="${BENCHMARK_ROOT}/PG"
DATA_DIR="$(realpath -e -- "${PG_ROOT}/data")"
PORT="${2:-55432}"
MARKER="${PG_ROOT}/BENCHMARK_OWNER.json"

[[ "${PORT}" =~ ^[1-9][0-9]*$ && "${PORT}" -le 65535 ]] || {
  echo "invalid PostgreSQL port: ${PORT}" >&2
  exit 64
}
[[ -x "${PG_ROOT}/start.sh" && -x "${PG_ROOT}/stop.sh" ]] || {
  echo "benchmark PostgreSQL root lacks start.sh/stop.sh: ${PG_ROOT}" >&2
  exit 66
}
command -v psql >/dev/null 2>&1 || {
  echo "psql is required to register benchmark cluster ownership" >&2
  exit 69
}
command -v python3 >/dev/null 2>&1 || {
  echo "python3 is required to write the ownership marker" >&2
  exit 69
}

IFS=$'\t' read -r ACTUAL_DATA ACTUAL_PORT SYSTEM_IDENTIFIER EXTRA < <(
  psql -w -XAt -F $'\t' -h "${PG_ROOT}" -p "${PORT}" -U postgres -d postgres \
    -c "SELECT current_setting('data_directory'), current_setting('port'), system_identifier::text FROM pg_control_system()"
)
[[ -z "${EXTRA:-}" && -n "${ACTUAL_DATA:-}" && -n "${SYSTEM_IDENTIFIER:-}" ]] || {
  echo "PostgreSQL endpoint returned an incomplete cluster identity" >&2
  exit 65
}
ACTUAL_DATA="$(realpath -e -- "${ACTUAL_DATA}")"
if [[ "${ACTUAL_DATA}" != "${DATA_DIR}" || "${ACTUAL_PORT}" != "${PORT}" ||
      ! "${SYSTEM_IDENTIFIER}" =~ ^[0-9]+$ ]]; then
  echo "refusing ownership registration for mismatched endpoint: data=${ACTUAL_DATA} port=${ACTUAL_PORT}" >&2
  exit 65
fi

TEMPORARY="${MARKER}.tmp.$$"
cleanup() { rm -f -- "${TEMPORARY}"; }
trap cleanup EXIT
python3 - "${TEMPORARY}" "${DATA_DIR}" "${PORT}" "${SYSTEM_IDENTIFIER}" <<'PY'
import json
import os
import sys

path, data_dir, port, system_identifier = sys.argv[1:]
with open(path, "x", encoding="utf-8") as output:
    json.dump(
        {
            "format": "radixdb-benchmark-postgres-owner-v1",
            "data_dir": data_dir,
            "port": int(port),
            "system_identifier": system_identifier,
        },
        output,
        indent=2,
        sort_keys=True,
    )
    output.write("\n")
    output.flush()
    os.fsync(output.fileno())
PY

if [[ -e "${MARKER}" ]]; then
  if cmp -s -- "${TEMPORARY}" "${MARKER}"; then
    echo "benchmark PostgreSQL ownership already registered: ${MARKER}"
    exit 0
  fi
  if [[ "${RADIXDB_REPLACE_PG_OWNER:-0}" != 1 ]]; then
    echo "refusing to replace a different ownership marker: ${MARKER}" >&2
    exit 73
  fi
  chmod u+w -- "${MARKER}" 2>/dev/null || true
fi
mv -f -- "${TEMPORARY}" "${MARKER}"
chmod 0444 -- "${MARKER}"
sync -d "${MARKER}" 2>/dev/null || true
trap - EXIT
echo "benchmark PostgreSQL ownership registered: ${MARKER}"
