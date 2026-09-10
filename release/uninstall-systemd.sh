#!/usr/bin/env bash
set -euo pipefail

PREFIX="${RADIXDB_INSTALL_PREFIX:-/opt/radixdb}"
UNIT_DIR="${RADIXDB_SYSTEMD_UNIT_DIR:-/etc/systemd/system}"
SKIP_SYSTEMCTL="${RADIXDB_SKIP_SYSTEMCTL:-0}"

[[ "${PREFIX}" =~ ^/[A-Za-z0-9._/-]+$ && "${PREFIX}" != "/" &&
   "/${PREFIX#/}/" != *"/../"* && "/${PREFIX#/}/" != *"/./"* ]] || {
  echo "RADIXDB_INSTALL_PREFIX must be a safe absolute path without '.' or '..' components: ${PREFIX}" >&2
  exit 64
}

if [[ "${SKIP_SYSTEMCTL}" != 1 ]]; then
  [[ "${EUID}" -eq 0 ]] || { echo "system uninstall requires root" >&2; exit 77; }
  systemctl disable --now radixdb.service 2>/dev/null || true
fi
rm -f -- "${UNIT_DIR}/radixdb.service"
if [[ "${SKIP_SYSTEMCTL}" != 1 ]]; then
  systemctl daemon-reload
fi

rm -f -- "${PREFIX}/bin/radixdb-server" "${PREFIX}/bin/radixdb-password" \
  "${PREFIX}/bin/radixdb-cli" \
  "${PREFIX}/bin/radixdb-smoke-client"
if [[ "${RADIXDB_UNINSTALL_PURGE:-0}" == 1 ]]; then
  rm -rf -- "${PREFIX}"
  echo "RadixDB binaries, configuration and data removed from ${PREFIX}"
else
  echo "RadixDB binaries removed; configuration and data retained under ${PREFIX}"
fi
