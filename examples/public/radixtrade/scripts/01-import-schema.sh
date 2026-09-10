#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

if [[ "${RADIXTRADE_DB_DSN}" == memory://* ]]; then
  echo "RADIXTRADE_DB_DSN must be persistent for this multi-step import script." >&2
  echo "Use the default file:// DSN or run schema.sql and seed-small.sql in one CLI process." >&2
  exit 2
fi

print_radixtrade_context

echo
echo "== Import schema =="
run_radixdb_cli_checked -f "${RADIXTRADE_ROOT}/schema.sql"

echo
echo "== Load seed-small =="
run_radixdb_cli_checked -f "${RADIXTRADE_ROOT}/seed-small.sql"

echo
echo "== Verify metadata =="
run_radixdb_cli_checked -e "SHOW TABLES"
run_radixdb_cli_checked -e "DESCRIBE rt_sales_orders"
run_radixdb_cli_checked -e "SHOW INDEXES FROM rt_customers"

echo
echo "RadixTrade import completed."
