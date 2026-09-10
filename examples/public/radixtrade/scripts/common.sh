#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RADIXTRADE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RADIXDB_REPO_ROOT="${RADIXDB_REPO_ROOT:-$(cd "${RADIXTRADE_ROOT}/../../.." && pwd)}"
RADIXTRADE_DB_DSN="${RADIXTRADE_DB_DSN:-file://${RADIXTRADE_ROOT}/runtime/radixtrade-demo?sync_mode=none&checkpoint_interval=3600}"
RADIXTRADE_LIMIT="${RADIXTRADE_LIMIT:-80}"

mkdir -p "${RADIXTRADE_ROOT}/runtime"

build_radixdb_cli_cmd() {
  if [[ -n "${RADIXDB_CLI:-}" ]]; then
    RADIXDB_CLI_CMD=("${RADIXDB_CLI}")
  elif [[ -x /opt/radixdb/bin/radixdb-cli ]]; then
    RADIXDB_CLI_CMD=(/opt/radixdb/bin/radixdb-cli)
  elif command -v radixdb-cli >/dev/null 2>&1; then
    RADIXDB_CLI_CMD=(radixdb-cli)
  else
    RADIXDB_CLI_CMD=(cargo run -q --manifest-path "${RADIXDB_REPO_ROOT}/Cargo.toml" --bin radixdb-cli --features cli --)
  fi
}

run_radixdb_cli() {
  build_radixdb_cli_cmd
  "${RADIXDB_CLI_CMD[@]}" --quiet --limit "${RADIXTRADE_LIMIT}" -d "${RADIXTRADE_DB_DSN}" "$@"
}

run_radixdb_cli_checked() {
  local output_file="${RADIXTRADE_ROOT}/runtime/last-cli-command.out"
  set +e
  run_radixdb_cli "$@" 2>&1 | tee "${output_file}"
  local status="${PIPESTATUS[0]}"
  set -e

  if [[ "${status}" -ne 0 ]] || grep -q '^Error:' "${output_file}"; then
    echo "RadixDB CLI command failed; see ${output_file}" >&2
    exit 1
  fi
}

print_radixtrade_context() {
  build_radixdb_cli_cmd
  echo "RadixTrade root: ${RADIXTRADE_ROOT}"
  echo "Database DSN: ${RADIXTRADE_DB_DSN}"
  echo "CLI: ${RADIXDB_CLI_CMD[*]}"
}
