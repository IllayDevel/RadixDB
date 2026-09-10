#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

probe_output="${RADIXTRADE_ROOT}/runtime/partial-index-reject-probe.out"

echo "== Partial unique expected-error probe =="

set +e
run_radixdb_cli -f <(printf '%s\n' \
  "DROP TABLE IF EXISTS rt_expected_error_users;" \
  "CREATE TABLE rt_expected_error_users (id UUID PRIMARY KEY AUTO_INCREMENT, email TEXT NOT NULL, __deleted_at TIMESTAMP);" \
  "CREATE UNIQUE INDEX rt_expected_error_users_email_active_uidx ON rt_expected_error_users (email) WHERE __deleted_at IS NULL;" \
  "INSERT INTO rt_expected_error_users (email) VALUES ('duplicate@example.test');" \
  "INSERT INTO rt_expected_error_users (email) VALUES ('duplicate@example.test');") >"${probe_output}" 2>&1
status=$?
set -e

cat "${probe_output}"

run_radixdb_cli_checked -e "DROP TABLE IF EXISTS rt_expected_error_users" >/dev/null

if ! grep -q 'unique constraint' "${probe_output}"; then
  echo "ERROR: expected unique constraint rejection was not observed." >&2
  echo "CLI exit status was ${status}." >&2
  exit 1
fi

echo
echo "Expected duplicate active email was rejected."
