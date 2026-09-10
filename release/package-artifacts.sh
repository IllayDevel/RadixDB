#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 4 ]]; then
  echo "usage: $0 OUTPUT_DIR RADIXDB_SERVER RADIXDB_CLI RADIXDB_SMOKE_CLIENT" >&2
  exit 64
fi

OUTPUT_DIR="$1"
SERVER_BIN="$2"
CLI_BIN="$3"
SMOKE_BIN="$4"
PASSWORD_BIN="$(dirname -- "${SERVER_BIN}")/radixdb-password"
REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

for artifact in "${SERVER_BIN}" "${PASSWORD_BIN}" "${CLI_BIN}" "${SMOKE_BIN}"; do
  [[ -x "${artifact}" ]] || {
    echo "missing executable artifact: ${artifact}" >&2
    exit 1
  }
done
if [[ -e "${OUTPUT_DIR}" ]]; then
  echo "refusing existing release output: ${OUTPUT_DIR}" >&2
  exit 73
fi

mkdir -m 0755 -- "${OUTPUT_DIR}"
cleanup_incomplete() {
  rm -rf -- "${OUTPUT_DIR}"
}
trap cleanup_incomplete ERR INT TERM
mkdir -m 0755 -- "${OUTPUT_DIR}/bin" "${OUTPUT_DIR}/debug"

for artifact in \
  "${SERVER_BIN}:radixdb-server" \
  "${PASSWORD_BIN}:radixdb-password" \
  "${CLI_BIN}:radixdb-cli" \
  "${SMOKE_BIN}:radixdb-smoke-client"; do
  source_binary="${artifact%%:*}"
  artifact_name="${artifact#*:}"
  "${REPO_DIR}/scripts/install-split-debug-elf.sh" \
    "${source_binary}" \
    "${OUTPUT_DIR}/bin/${artifact_name}" \
    "${OUTPUT_DIR}/debug/${artifact_name}.debug"
done
for script in lib.sh start.sh stop.sh status.sh smoke-client.sh install-systemd.sh \
  uninstall-systemd.sh backup-external.sh restore-external.sh; do
  install -m 0755 "${REPO_DIR}/release/${script}" "${OUTPUT_DIR}/${script}"
done
install -m 0644 "${REPO_DIR}/release/radixdb.service" "${OUTPUT_DIR}/radixdb.service"
install -m 0644 "${REPO_DIR}/release/server.toml" "${OUTPUT_DIR}/server.toml"
install -m 0644 "${REPO_DIR}/release/README.md" "${OUTPUT_DIR}/README.md"
install -m 0644 "${REPO_DIR}/LICENSE" "${OUTPUT_DIR}/LICENSE"
install -m 0644 "${REPO_DIR}/NOTICE" "${OUTPUT_DIR}/NOTICE"

server_identity="$("${OUTPUT_DIR}/bin/radixdb-server" --version)"
password_identity="$("${OUTPUT_DIR}/bin/radixdb-password" --version)"
cli_identity="$("${OUTPUT_DIR}/bin/radixdb-cli" --version)"
smoke_identity="$("${OUTPUT_DIR}/bin/radixdb-smoke-client" --version)"

identity_field() {
  local identity="$1" key="$2" field
  for field in ${identity}; do
    if [[ "${field}" == "${key}="* ]]; then
      printf '%s\n' "${field#*=}"
      return 0
    fi
  done
  return 1
}

artifact_revision="$(identity_field "${server_identity}" git)" || {
  echo "server binary does not expose git build identity" >&2
  exit 1
}
artifact_profile="$(identity_field "${server_identity}" profile)"
artifact_target="$(identity_field "${server_identity}" target)"
artifact_lock="$(identity_field "${server_identity}" lock)"
for identity in "${password_identity}" "${cli_identity}" "${smoke_identity}"; do
  [[ "$(identity_field "${identity}" git)" == "${artifact_revision}" &&
     "$(identity_field "${identity}" profile)" == "${artifact_profile}" &&
     "$(identity_field "${identity}" target)" == "${artifact_target}" &&
     "$(identity_field "${identity}" lock)" == "${artifact_lock}" ]] || {
    echo "release binaries do not share one embedded build identity" >&2
    exit 1
  }
done
[[ "${artifact_lock}" =~ ^[0-9a-f]{64}$ ]] || {
  echo "invalid embedded Cargo.lock identity: ${artifact_lock}" >&2
  exit 1
}

checkout_revision="$(git -C "${REPO_DIR}" rev-parse HEAD 2>/dev/null || printf 'source-archive')"
if git -C "${REPO_DIR}" diff --quiet --ignore-submodules -- 2>/dev/null &&
   git -C "${REPO_DIR}" diff --cached --quiet --ignore-submodules -- 2>/dev/null; then
  checkout_dirty=false
else
  checkout_dirty=true
fi
{
  printf 'format=radixdb-release-provenance-v2\n'
  printf 'artifact_revision=%s\n' "${artifact_revision}"
  printf 'artifact_profile=%s\n' "${artifact_profile}"
  printf 'artifact_target=%s\n' "${artifact_target}"
  printf 'artifact_cargo_lock_sha256=%s\n' "${artifact_lock}"
  printf 'server_sha256=%s\n' "$(sha256sum "${OUTPUT_DIR}/bin/radixdb-server" | awk '{print $1}')"
  printf 'password_sha256=%s\n' "$(sha256sum "${OUTPUT_DIR}/bin/radixdb-password" | awk '{print $1}')"
  printf 'cli_sha256=%s\n' "$(sha256sum "${OUTPUT_DIR}/bin/radixdb-cli" | awk '{print $1}')"
  printf 'smoke_client_sha256=%s\n' "$(sha256sum "${OUTPUT_DIR}/bin/radixdb-smoke-client" | awk '{print $1}')"
  printf 'server_debug_sha256=%s\n' "$(sha256sum "${OUTPUT_DIR}/debug/radixdb-server.debug" | awk '{print $1}')"
  printf 'password_debug_sha256=%s\n' "$(sha256sum "${OUTPUT_DIR}/debug/radixdb-password.debug" | awk '{print $1}')"
  printf 'cli_debug_sha256=%s\n' "$(sha256sum "${OUTPUT_DIR}/debug/radixdb-cli.debug" | awk '{print $1}')"
  printf 'smoke_client_debug_sha256=%s\n' "$(sha256sum "${OUTPUT_DIR}/debug/radixdb-smoke-client.debug" | awk '{print $1}')"
  printf 'server_identity=%s\n' "${server_identity}"
  printf 'password_identity=%s\n' "${password_identity}"
  printf 'cli_identity=%s\n' "${cli_identity}"
  printf 'smoke_client_identity=%s\n' "${smoke_identity}"
  printf 'checkout_revision_context=%s\n' "${checkout_revision}"
  printf 'checkout_dirty_context=%s\n' "${checkout_dirty}"
} >"${OUTPUT_DIR}/PROVENANCE.env"

(
  cd "${OUTPUT_DIR}"
  find . -type f ! -name SHA256SUMS -print0 |
    LC_ALL=C sort -z |
    xargs -0 sha256sum >SHA256SUMS
  sha256sum -c SHA256SUMS
)

trap - ERR INT TERM
printf 'release bundle created: %s\n' "${OUTPUT_DIR}"
