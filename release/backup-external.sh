#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  echo "usage: $0 DATABASE_ROOT NEW_BACKUP_DIRECTORY" >&2
  exit 64
fi
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CLI_BIN="${RADIXDB_CLI_BIN:-${SCRIPT_DIR}/bin/radixdb-cli}"
DATABASE_ROOT="$(realpath -e -- "$1")"
BACKUP_PATH="$2"
BACKUP_PARENT="$(realpath -e -- "$(dirname -- "${BACKUP_PATH}")")"
BACKUP_DIR="${BACKUP_PARENT}/$(basename -- "${BACKUP_PATH}")"

[[ -x "${CLI_BIN}" ]] || { echo "missing executable ${CLI_BIN}" >&2; exit 66; }
[[ -d "${DATABASE_ROOT}" ]] || { echo "database root is not a directory" >&2; exit 66; }
[[ ! -e "${BACKUP_DIR}" ]] || { echo "backup destination already exists: ${BACKUP_DIR}" >&2; exit 73; }
case "${BACKUP_DIR}/" in
  "${DATABASE_ROOT}/"*) echo "backup must be external to the database root" >&2; exit 64 ;;
esac

VERSION_LINE="$("${CLI_BIN}" --version)"
if [[ ! "${VERSION_LINE}" =~ ^radixdb-cli[[:space:]]+([^[:space:]]+)[[:space:]]+git=([^[:space:]]+)[[:space:]]+profile=([^[:space:]]+)[[:space:]]+target=([^[:space:]]+)[[:space:]]+lock=([0-9a-f]{64})$ ]]; then
  echo "radixdb-cli returned an unsupported build identity: ${VERSION_LINE}" >&2
  exit 65
fi
CLI_VERSION="${BASH_REMATCH[1]}"
GIT_COMMIT="${BASH_REMATCH[2]}"
BUILD_PROFILE="${BASH_REMATCH[3]}"
BUILD_TARGET="${BASH_REMATCH[4]}"
CARGO_LOCK_SHA256="${BASH_REMATCH[5]}"

SNAPSHOT_JSON="$("${CLI_BIN}" --quiet --json --db "file://${DATABASE_ROOT}" --snapshot)"
SNAPSHOT_ID="$(printf '%s\n' "${SNAPSHOT_JSON}" | sed -n 's/.*"snapshot_id":"\([0-9a-f]\{32\}\)".*/\1/p')"
DATABASE_ID="$(printf '%s\n' "${SNAPSHOT_JSON}" | sed -n 's/.*"database_id":"\([0-9a-f]\{32\}\)".*/\1/p')"
PHYSICAL_FORMAT="$(printf '%s\n' "${SNAPSHOT_JSON}" | sed -n 's/.*"physical_format":"\([0-9][0-9]*\.[0-9][0-9]*\)".*/\1/p')"
[[ "${SNAPSHOT_ID}" =~ ^[0-9a-f]{32}$ && "${DATABASE_ID}" =~ ^[0-9a-f]{32}$ && "${PHYSICAL_FORMAT}" =~ ^[0-9]+\.[0-9]+$ ]] || {
  echo "snapshot command returned invalid machine-readable identity: ${SNAPSHOT_JSON}" >&2
  exit 65
}
SNAPSHOT_SOURCE="${DATABASE_ROOT}/snapshots/${SNAPSHOT_ID}"
[[ -f "${SNAPSHOT_SOURCE}/SNAPSHOT.mft" ]] || {
  echo "committed snapshot ${SNAPSHOT_ID} is missing from the source tree" >&2
  exit 1
}
if find "${SNAPSHOT_SOURCE}" -type l -print -quit | grep -q .; then
  echo "snapshot tree contains a symbolic link" >&2
  exit 1
fi

mkdir -m 0700 -- "${BACKUP_DIR}"
cleanup_incomplete() { chmod -R u+w "${BACKUP_DIR}" 2>/dev/null || true; rm -rf -- "${BACKUP_DIR}"; }
trap cleanup_incomplete ERR INT TERM
mkdir -m 0700 -- "${BACKUP_DIR}/snapshots"
cp -a -- "${SNAPSHOT_SOURCE}" "${BACKUP_DIR}/snapshots/${SNAPSHOT_ID}"
COPIED_IDENTITY="$("${CLI_BIN}" --inspect-snapshot "${BACKUP_DIR}/snapshots/${SNAPSHOT_ID}")"
[[ "${COPIED_IDENTITY}" == "${SNAPSHOT_JSON}" ]] || {
  echo "copied snapshot identity differs from the committed source snapshot" >&2
  exit 65
}
{
  printf 'format=radixdb-external-backup-v2\n'
  printf 'created_unix_seconds=%s\n' "$(date +%s)"
  printf 'cli_version=%s\n' "${CLI_VERSION}"
  printf 'git_commit=%s\n' "${GIT_COMMIT}"
  printf 'build_profile=%s\n' "${BUILD_PROFILE}"
  printf 'build_target=%s\n' "${BUILD_TARGET}"
  printf 'cargo_lock_sha256=%s\n' "${CARGO_LOCK_SHA256}"
  printf 'physical_format=%s\n' "${PHYSICAL_FORMAT}"
  printf 'database_id=%s\n' "${DATABASE_ID}"
  printf 'snapshot_id=%s\n' "${SNAPSHOT_ID}"
} >"${BACKUP_DIR}/BACKUP.env"
(
  cd "${BACKUP_DIR}"
  find BACKUP.env snapshots -type f -print0 |
    LC_ALL=C sort -z |
    xargs -0 sha256sum >SHA256SUMS
  sha256sum -c SHA256SUMS
)
chmod -R a-w "${BACKUP_DIR}"
trap - ERR INT TERM
echo "immutable external backup ${SNAPSHOT_ID} created: ${BACKUP_DIR}"
