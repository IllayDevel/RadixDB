#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PREFIX="${RADIXDB_INSTALL_PREFIX:-/opt/radixdb}"
UNIT_DIR="${RADIXDB_SYSTEMD_UNIT_DIR:-/etc/systemd/system}"
SKIP_SYSTEMCTL="${RADIXDB_SKIP_SYSTEMCTL:-0}"
SKIP_USER_SETUP="${RADIXDB_SKIP_USER_SETUP:-0}"

[[ "${PREFIX}" =~ ^/[A-Za-z0-9._/-]+$ && "${PREFIX}" != "/" &&
   "/${PREFIX#/}/" != *"/../"* && "/${PREFIX#/}/" != *"/./"* ]] || {
  echo "RADIXDB_INSTALL_PREFIX must be a safe absolute path without '.' or '..' components: ${PREFIX}" >&2
  exit 64
}
[[ -x "${SCRIPT_DIR}/bin/radixdb-server" &&
   -x "${SCRIPT_DIR}/bin/radixdb-password" &&
   -f "${SCRIPT_DIR}/radixdb.service" ]] || {
  echo "installer must run from a complete RadixDB release bundle" >&2
  exit 66
}
if [[ "${SKIP_USER_SETUP}" != 1 ]]; then
  [[ "${EUID}" -eq 0 ]] || { echo "system installation requires root" >&2; exit 77; }
  getent group radixdb >/dev/null || groupadd --system radixdb
  id radixdb >/dev/null 2>&1 || useradd --system --gid radixdb --home-dir "${PREFIX}" --shell /usr/sbin/nologin radixdb
fi

install -d -m 0755 "${PREFIX}" "${PREFIX}/bin"
install -d -m 0750 "${PREFIX}/data" "${PREFIX}/logs" "${PREFIX}/run"
install -m 0755 "${SCRIPT_DIR}/bin/radixdb-server" "${PREFIX}/bin/radixdb-server"
install -m 0755 "${SCRIPT_DIR}/bin/radixdb-password" "${PREFIX}/bin/radixdb-password"
install -m 0755 "${SCRIPT_DIR}/bin/radixdb-cli" "${PREFIX}/bin/radixdb-cli"
install -m 0755 "${SCRIPT_DIR}/bin/radixdb-smoke-client" "${PREFIX}/bin/radixdb-smoke-client"
if [[ ! -e "${PREFIX}/server.toml" || "${RADIXDB_INSTALL_REPLACE_CONFIG:-0}" == 1 ]]; then
  install -m 0640 "${SCRIPT_DIR}/server.toml" "${PREFIX}/server.toml"
fi
install -d -m 0755 "${UNIT_DIR}"
sed "s|@PREFIX@|${PREFIX}|g" "${SCRIPT_DIR}/radixdb.service" >"${UNIT_DIR}/radixdb.service"
chmod 0644 "${UNIT_DIR}/radixdb.service"

if [[ "${SKIP_USER_SETUP}" != 1 ]]; then
  chown -R radixdb:radixdb "${PREFIX}/data" "${PREFIX}/logs" "${PREFIX}/run"
  chown root:radixdb "${PREFIX}/server.toml"
fi
if [[ "${SKIP_SYSTEMCTL}" != 1 ]]; then
  systemctl daemon-reload
  systemctl enable radixdb.service
fi
echo "RadixDB installed at ${PREFIX}; start with: systemctl start radixdb"
