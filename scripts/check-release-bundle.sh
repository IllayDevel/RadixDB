#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 4 ]]; then
  echo "usage: $0 OUTPUT_DIR RADIXDB_SERVER RADIXDB_CLI RADIXDB_SMOKE_CLIENT" >&2
  exit 64
fi

REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="$1"
SERVER_BIN="$2"
CLI_BIN="$3"
SMOKE_BIN="$4"
TEST_ROOT="$(mktemp -d)"
cleanup() {
  chmod -R u+w -- "${TEST_ROOT}" 2>/dev/null || true
  rm -rf -- "${TEST_ROOT}"
}
trap cleanup EXIT

"${REPO_DIR}/release/package-artifacts.sh" \
  "${OUTPUT_DIR}" "${SERVER_BIN}" "${CLI_BIN}" "${SMOKE_BIN}"
(
  cd "${OUTPUT_DIR}"
  sha256sum -c SHA256SUMS
)

expected_inventory="${TEST_ROOT}/expected-inventory"
actual_inventory="${TEST_ROOT}/actual-inventory"
cat >"${expected_inventory}" <<'EOF'
LICENSE
NOTICE
PROVENANCE.env
README.md
SHA256SUMS
backup-external.sh
bin/radixdb-cli
bin/radixdb-password
bin/radixdb-server
bin/radixdb-smoke-client
debug/radixdb-cli.debug
debug/radixdb-password.debug
debug/radixdb-server.debug
debug/radixdb-smoke-client.debug
install-systemd.sh
lib.sh
radixdb.service
restore-external.sh
server.toml
smoke-client.sh
start.sh
status.sh
stop.sh
uninstall-systemd.sh
EOF
find "${OUTPUT_DIR}" -type f -printf '%P\n' | LC_ALL=C sort >"${actual_inventory}"
diff -u "${expected_inventory}" "${actual_inventory}"

for artifact_name in radixdb-server radixdb-password radixdb-cli radixdb-smoke-client; do
  readelf --string-dump=.gnu_debuglink "${OUTPUT_DIR}/bin/${artifact_name}" |
    grep -F "${artifact_name}.debug" >/dev/null
  binary_build_id="$(readelf -n "${OUTPUT_DIR}/bin/${artifact_name}" |
    sed -n 's/.*Build ID: //p' | head -n1)"
  debug_build_id="$(readelf -n "${OUTPUT_DIR}/debug/${artifact_name}.debug" 2>/dev/null |
    sed -n 's/.*Build ID: //p' | head -n1)"
  [[ -n "${binary_build_id}" && "${debug_build_id}" == "${binary_build_id}" ]]
done

install_root="${TEST_ROOT}/install"
unit_root="${TEST_ROOT}/systemd"
RADIXDB_INSTALL_PREFIX="${install_root}" \
RADIXDB_SYSTEMD_UNIT_DIR="${unit_root}" \
RADIXDB_SKIP_SYSTEMCTL=1 RADIXDB_SKIP_USER_SETUP=1 \
  "${OUTPUT_DIR}/install-systemd.sh"
[[ -x "${install_root}/bin/radixdb-server" ]]
[[ -x "${install_root}/bin/radixdb-password" ]]
[[ -f "${install_root}/server.toml" ]]
grep -F "ExecStart=${install_root}/bin/radixdb-server" \
  "${unit_root}/radixdb.service" >/dev/null
RADIXDB_INSTALL_PREFIX="${install_root}" \
RADIXDB_SYSTEMD_UNIT_DIR="${unit_root}" RADIXDB_SKIP_SYSTEMCTL=1 \
  "${OUTPUT_DIR}/uninstall-systemd.sh"
[[ ! -e "${install_root}/bin/radixdb-server" ]]
[[ ! -e "${install_root}/bin/radixdb-password" ]]
[[ -f "${install_root}/server.toml" && -d "${install_root}/data" ]]

source_db="${TEST_ROOT}/source-db"
backup="${TEST_ROOT}/external-backup"
restored_db="${TEST_ROOT}/restored-db"
cli="${OUTPUT_DIR}/bin/radixdb-cli"
"${cli}" --quiet --db "file://${source_db}" --execute \
  "CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT UNIQUE, rank INTEGER); CREATE INDEX items_rank_idx ON items(rank); INSERT INTO items (id, code, rank) VALUES (1, 'alpha', 10), (2, 'beta', 20);"
RADIXDB_CLI_BIN="${cli}" "${OUTPUT_DIR}/backup-external.sh" "${source_db}" "${backup}"
if find "${backup}" -perm /222 -print -quit | grep -q .; then
  echo "external backup contains writable artifacts" >&2
  exit 1
fi
RADIXDB_CLI_BIN="${cli}" "${OUTPUT_DIR}/restore-external.sh" "${backup}" "${restored_db}"

result_json="${TEST_ROOT}/restored-result.json"
indexes_json="${TEST_ROOT}/restored-indexes.json"
"${cli}" --quiet --json --db "file://${restored_db}" --execute \
  "SELECT COUNT(*) AS count, SUM(rank) AS total FROM items" >"${result_json}"
"${cli}" --quiet --json --db "file://${restored_db}" --execute \
  "SHOW INDEXES FROM items" >"${indexes_json}"
python3 - "${result_json}" "${indexes_json}" <<'PY'
import json
import sys

result = json.load(open(sys.argv[1], encoding="utf-8"))
indexes = json.load(open(sys.argv[2], encoding="utf-8"))
assert result["truncated"] is False
assert [cell["value"] for cell in result["rows"][0]] == ["2", "30"]
names = {row[1]["value"] for row in indexes["rows"]}
assert {"uq_items_code", "__pk_items_id", "items_rank_idx"} <= names
PY

tampered="${TEST_ROOT}/tampered-backup"
cp -a -- "${backup}" "${tampered}"
tampered_file="$(find "${tampered}/snapshots" -type f -print -quit)"
chmod u+w -- "${tampered_file}"
printf 'tamper' >>"${tampered_file}"
if RADIXDB_CLI_BIN="${cli}" "${OUTPUT_DIR}/restore-external.sh" \
  "${tampered}" "${TEST_ROOT}/tampered-restore" >/dev/null 2>&1; then
  echo "tampered external backup was accepted" >&2
  exit 1
fi
[[ ! -e "${TEST_ROOT}/tampered-restore" ]]

echo "release bundle, systemd and external restore contracts: ok"
