#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
secret_file="${RADIXDB_SOAK_REMOTE_SECRET:-${repo_dir}/.secret/soak-remote.env}"
[[ -f "${secret_file}" ]] || { echo "missing remote secret file" >&2; exit 66; }
# shellcheck disable=SC1090
source "${secret_file}"
: "${RADIXDB_SOAK_SSH_HOST:?missing SSH host}"
: "${RADIXDB_SOAK_SSH_USER:?missing SSH user}"
: "${RADIXDB_SOAK_SSH_PASSWORD:?missing SSH password}"
bundle="${1:?usage: deploy-postgresql-soak-bundle.sh BUNDLE_DIR}"
bundle="$(realpath -e -- "${bundle}")"
commit="$(awk -F= '$1 == "git_commit" {print $2}' "${bundle}/RELEASE")"
engine="$(awk -F= '$1 == "engine" {print $2}' "${bundle}/RELEASE")"
[[ "${commit}" =~ ^[0-9a-f]{40}$ && "${engine}" == postgresql ]] || {
  echo "invalid PostgreSQL bundle identity" >&2; exit 65;
}
(cd "${bundle}" && sha256sum -c SHA256SUMS)

http_secret="${repo_dir}/.secret/soak-http-auth.env"
database_secret="${repo_dir}/.secret/soak-postgresql-password"
[[ -f "${http_secret}" ]] || { echo "missing soak HTTP auth secret" >&2; exit 66; }
if [[ ! -f "${database_secret}" ]]; then
  umask 077
  openssl rand -base64 36 | tr -d '\n' >"${database_secret}"
fi
[[ "$(stat -c %a "${http_secret}")" == 600 ]]
[[ "$(stat -c %a "${database_secret}")" == 600 ]]

temporary="$(mktemp -d)"
cleanup() { rm -rf -- "${temporary}"; }
trap cleanup EXIT
archive="${temporary}/radixdb-soak-${commit}-postgresql.tar.gz"
tar -C "${bundle}" -czf "${archive}" .
(cd "${temporary}" && sha256sum "$(basename -- "${archive}")" >"$(basename -- "${archive}.sha256")")
remote_http="/tmp/radixdb-soak-http-${commit}.env"
remote_database="/tmp/radixdb-soak-postgresql-${commit}.password"
export SSHPASS="${RADIXDB_SOAK_SSH_PASSWORD}"
ssh_opts=(
  -o PreferredAuthentications=password
  -o PubkeyAuthentication=no
  -o StrictHostKeyChecking=accept-new
  -o ConnectTimeout=15
)
sshpass -e scp -O "${ssh_opts[@]}" "${archive}" "${archive}.sha256" \
  "${RADIXDB_SOAK_SSH_USER}@${RADIXDB_SOAK_SSH_HOST}:/tmp/"
sshpass -e scp -O "${ssh_opts[@]}" "${http_secret}" \
  "${RADIXDB_SOAK_SSH_USER}@${RADIXDB_SOAK_SSH_HOST}:${remote_http}"
sshpass -e scp -O "${ssh_opts[@]}" "${database_secret}" \
  "${RADIXDB_SOAK_SSH_USER}@${RADIXDB_SOAK_SSH_HOST}:${remote_database}"

remote_script="${temporary}/install-remote.sh"
cat >"${remote_script}" <<'REMOTE'
set -euo pipefail
commit="$1"
archive="/tmp/radixdb-soak-${commit}-postgresql.tar.gz"
checksum="${archive}.sha256"
http="/tmp/radixdb-soak-http-${commit}.env"
database="/tmp/radixdb-soak-postgresql-${commit}.password"
cd /tmp
sha256sum -c "$(basename "${checksum}")"
staging="/storage/radixdb-soak-staging/${commit}-postgresql"
rm -rf -- "${staging}"
install -d -o root -g root -m 0755 "${staging}"
tar -xzf "${archive}" -C "${staging}"
(cd "${staging}" && sha256sum -c SHA256SUMS)
install -d -o root -g root -m 0755 /etc/radixdb-soak
install -o root -g root -m 0600 "${http}" /etc/radixdb-soak/status-auth.env
"${staging}/bin/install.sh"
install -o root -g radixdb-soak -m 0640 "${database}" /etc/radixdb-soak/postgresql-password
rm -rf -- "${staging}"
rm -f -- "${archive}" "${checksum}" "${http}" "${database}"
REMOTE
{
  printf '%s\n' "${RADIXDB_SOAK_SSH_PASSWORD}"
  cat "${remote_script}"
} | sshpass -e ssh "${ssh_opts[@]}" \
  "${RADIXDB_SOAK_SSH_USER}@${RADIXDB_SOAK_SSH_HOST}" \
  "sudo -S -p '' bash -s -- '${commit}'"
unset SSHPASS RADIXDB_SOAK_SSH_PASSWORD
echo "deployed isolated PostgreSQL soak release ${commit}; services remain stopped"
