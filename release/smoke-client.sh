#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=release/lib.sh
source "${SCRIPT_DIR}/lib.sh"
release_init
SMOKE_BIN="${RADIXDB_SMOKE_BIN:-${RELEASE_ROOT}/bin/radixdb-smoke-client}"
[[ -x "${SMOKE_BIN}" ]] || {
  echo "missing executable ${SMOKE_BIN}" >&2
  exit 66
}
"${SMOKE_BIN}" "${HOST}:${PORT}"
