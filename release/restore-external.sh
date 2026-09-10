#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  echo "usage: $0 BACKUP_DIRECTORY NEW_DATABASE_ROOT" >&2
  exit 64
fi
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CLI_BIN="${RADIXDB_CLI_BIN:-${SCRIPT_DIR}/bin/radixdb-cli}"
BACKUP_DIR="$(realpath -e -- "$1")"
TARGET_PATH="$2"
TARGET_PARENT="$(realpath -e -- "$(dirname -- "${TARGET_PATH}")")"
TARGET_ROOT="${TARGET_PARENT}/$(basename -- "${TARGET_PATH}")"

[[ -x "${CLI_BIN}" ]] || { echo "missing executable ${CLI_BIN}" >&2; exit 66; }
[[ -f "${BACKUP_DIR}/BACKUP.env" && -f "${BACKUP_DIR}/SHA256SUMS" ]] || {
  echo "not a RadixDB external backup: ${BACKUP_DIR}" >&2
  exit 65
}
if find "${BACKUP_DIR}" -type l -print -quit | grep -q .; then
  echo "external backup contains a symbolic link" >&2
  exit 65
fi
[[ ! -e "${TARGET_ROOT}" ]] || { echo "restore target already exists: ${TARGET_ROOT}" >&2; exit 73; }
case "${TARGET_ROOT}/" in
  "${BACKUP_DIR}/"*) echo "restore target must be outside the backup artifact" >&2; exit 64 ;;
esac
COMPUTED_SUMS="$(mktemp)"
cleanup_sums() { rm -f -- "${COMPUTED_SUMS}"; }
trap cleanup_sums EXIT
(
  cd "${BACKUP_DIR}"
  find BACKUP.env snapshots -type f -print0 |
    LC_ALL=C sort -z |
    xargs -0 sha256sum >"${COMPUTED_SUMS}"
)
if ! cmp -s -- "${COMPUTED_SUMS}" "${BACKUP_DIR}/SHA256SUMS"; then
  echo "external backup inventory or checksum does not match SHA256SUMS" >&2
  exit 65
fi
rm -f -- "${COMPUTED_SUMS}"
trap - EXIT

declare -A BACKUP_METADATA=()
while IFS='=' read -r key value; do
  [[ -n "${key}" && -n "${value}" && "${key}" =~ ^[a-z0-9_]+$ ]] || {
    echo "invalid BACKUP.env entry" >&2
    exit 65
  }
  [[ -z "${BACKUP_METADATA[${key}]+present}" ]] || {
    echo "duplicate BACKUP.env field: ${key}" >&2
    exit 65
  }
  BACKUP_METADATA["${key}"]="${value}"
done <"${BACKUP_DIR}/BACKUP.env"

REQUIRED_FIELDS=(format created_unix_seconds cli_version git_commit build_profile build_target cargo_lock_sha256 physical_format database_id snapshot_id)
for key in "${REQUIRED_FIELDS[@]}"; do
  [[ -n "${BACKUP_METADATA[${key}]+present}" ]] || {
    echo "BACKUP.env is missing required field: ${key}" >&2
    exit 65
  }
done
[[ "${#BACKUP_METADATA[@]}" -eq "${#REQUIRED_FIELDS[@]}" ]] || {
  echo "BACKUP.env contains unsupported fields" >&2
  exit 65
}
[[ "${BACKUP_METADATA[format]}" == "radixdb-external-backup-v2" ]] || {
  echo "unsupported RadixDB external backup format: ${BACKUP_METADATA[format]}" >&2
  exit 65
}
[[ "${BACKUP_METADATA[created_unix_seconds]}" =~ ^[0-9]+$ \
  && "${BACKUP_METADATA[cargo_lock_sha256]}" =~ ^[0-9a-f]{64}$ \
  && "${BACKUP_METADATA[physical_format]}" =~ ^[0-9]+\.[0-9]+$ \
  && "${BACKUP_METADATA[database_id]}" =~ ^[0-9a-f]{32}$ \
  && "${BACKUP_METADATA[snapshot_id]}" =~ ^[0-9a-f]{32}$ ]] || {
  echo "BACKUP.env contains an invalid provenance value" >&2
  exit 65
}

SUPPORTED_PHYSICAL_FORMAT="$("${CLI_BIN}" --physical-format)"
if [[ "${BACKUP_METADATA[physical_format]}" != "${SUPPORTED_PHYSICAL_FORMAT}" ]]; then
  echo "incompatible physical format ${BACKUP_METADATA[physical_format]}; this reader supports ${SUPPORTED_PHYSICAL_FORMAT}" >&2
  echo "required reader identity: radixdb-cli ${BACKUP_METADATA[cli_version]} git=${BACKUP_METADATA[git_commit]} profile=${BACKUP_METADATA[build_profile]} target=${BACKUP_METADATA[build_target]} lock=${BACKUP_METADATA[cargo_lock_sha256]}" >&2
  exit 65
fi

SNAPSHOT_ID="${BACKUP_METADATA[snapshot_id]}"
[[ -f "${BACKUP_DIR}/snapshots/${SNAPSHOT_ID}/SNAPSHOT.mft" ]] || {
  echo "exact backup snapshot is missing: ${SNAPSHOT_ID}" >&2
  exit 65
}
SNAPSHOT_IDENTITY="$("${CLI_BIN}" --inspect-snapshot "${BACKUP_DIR}/snapshots/${SNAPSHOT_ID}")"
MANIFEST_SNAPSHOT_ID="$(printf '%s\n' "${SNAPSHOT_IDENTITY}" | sed -n 's/.*"snapshot_id":"\([0-9a-f]\{32\}\)".*/\1/p')"
MANIFEST_DATABASE_ID="$(printf '%s\n' "${SNAPSHOT_IDENTITY}" | sed -n 's/.*"database_id":"\([0-9a-f]\{32\}\)".*/\1/p')"
MANIFEST_PHYSICAL_FORMAT="$(printf '%s\n' "${SNAPSHOT_IDENTITY}" | sed -n 's/.*"physical_format":"\([0-9][0-9]*\.[0-9][0-9]*\)".*/\1/p')"
[[ "${MANIFEST_SNAPSHOT_ID}" == "${SNAPSHOT_ID}" \
  && "${MANIFEST_DATABASE_ID}" == "${BACKUP_METADATA[database_id]}" \
  && "${MANIFEST_PHYSICAL_FORMAT}" == "${BACKUP_METADATA[physical_format]}" ]] || {
  echo "BACKUP.env identity differs from the exact snapshot manifest" >&2
  exit 65
}

mkdir -m 0700 -- "${TARGET_ROOT}"
cleanup_incomplete() { rm -rf -- "${TARGET_ROOT}"; }
trap cleanup_incomplete ERR INT TERM
cp -a -- "${BACKUP_DIR}/snapshots" "${TARGET_ROOT}/snapshots"
chmod -R u+w "${TARGET_ROOT}/snapshots"
RESTORED_ID="$("${CLI_BIN}" --quiet --db "file://${TARGET_ROOT}" --restore "${SNAPSHOT_ID}")"
[[ "${RESTORED_ID}" == "${SNAPSHOT_ID}" ]] || {
  echo "restore selected ${RESTORED_ID}, expected exact snapshot ${SNAPSHOT_ID}" >&2
  exit 65
}
trap - ERR INT TERM
echo "external backup ${SNAPSHOT_ID} restored into new root: ${TARGET_ROOT}"
