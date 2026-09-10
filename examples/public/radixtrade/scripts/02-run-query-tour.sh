#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

if [[ "${RADIXTRADE_SKIP_IMPORT:-0}" != "1" ]]; then
  "${SCRIPT_DIR}/01-import-schema.sh"
fi

echo
echo "== Run query tour =="

for sql_file in "${RADIXTRADE_ROOT}"/queries/*.sql; do
  echo
  echo "== ${sql_file#"${RADIXTRADE_ROOT}/"} =="
  run_radixdb_cli_checked -f "${sql_file}"
done

echo
echo "RadixTrade query tour completed."
