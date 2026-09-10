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
allow_inactive_production="${RADIXDB_SOAK_ALLOW_INACTIVE_PRODUCTION:-0}"
[[ "${allow_inactive_production}" =~ ^[01]$ ]] || {
  echo "RADIXDB_SOAK_ALLOW_INACTIVE_PRODUCTION must be 0 or 1" >&2
  exit 65
}
bundle="${1:?usage: deploy-remote-soak-bundle.sh BUNDLE_DIR}"
bundle="$(realpath -e -- "${bundle}")"
commit="$(awk -F= '$1 == "git_commit" {print $2}' "${bundle}/RELEASE")"
[[ "${commit}" =~ ^[0-9a-f]{40}$ ]] || { echo "invalid bundle release identity" >&2; exit 65; }
(cd "${bundle}" && sha256sum -c SHA256SUMS)

http_secret="${repo_dir}/.secret/soak-http-auth.env"
if [[ ! -f "${http_secret}" ]]; then
  umask 077
  http_password="$(openssl rand -base64 36 | tr -d '\n')"
  printf 'RADIXDB_SOAK_HTTP_USER=observer\nRADIXDB_SOAK_HTTP_PASSWORD=%s\n' \
    "${http_password}" >"${http_secret}"
  unset http_password
fi
[[ "$(stat -c %a "${http_secret}")" == 600 ]]

temporary="$(mktemp -d)"
cleanup() { rm -rf -- "${temporary}"; }
trap cleanup EXIT
archive="${temporary}/radixdb-soak-${commit}.tar.gz"
tar -C "${bundle}" -czf "${archive}" .
(
  cd "${temporary}"
  sha256sum "$(basename -- "${archive}")" >"$(basename -- "${archive}.sha256")"
)
remote_transfer_root="/storage/radixdb-soak-transfer"
remote_transfer="${remote_transfer_root%/}/${commit}"
remote_archive="${remote_transfer}/radixdb-soak-${commit}.tar.gz"
remote_auth="${remote_transfer}/radixdb-soak-status-auth-${commit}.env"
export SSHPASS="${RADIXDB_SOAK_SSH_PASSWORD}"
ssh_opts=(
  -o StrictHostKeyChecking=accept-new
  -o ConnectTimeout=15
  -o ConnectionAttempts=1
  -o PreferredAuthentications=password
  -o PubkeyAuthentication=no
  -o NumberOfPasswordPrompts=1
  -o ServerAliveInterval=5
  -o ServerAliveCountMax=3
)

remote_prepare="${temporary}/prepare-remote-transfer.sh"
cat >"${remote_prepare}" <<'REMOTE'
set -euo pipefail
remote_user="$1"
transfer_root="$2"
commit="$3"
transfer="${transfer_root%/}/${commit}"
[[ "${transfer_root}" == /storage/radixdb-soak-transfer ]]
[[ "${commit}" =~ ^[0-9a-f]{40}$ ]]
[[ "${transfer}" == "${transfer_root%/}/${commit}" ]]
remote_group="$(id -gn "${remote_user}")"
install -d -o root -g root -m 0755 "${transfer_root}"
rm -rf -- "${transfer}"
install -d -o "${remote_user}" -g "${remote_group}" -m 0700 "${transfer}"
REMOTE
echo "preparing isolated remote transfer directory"
{
  printf '%s\n' "${RADIXDB_SOAK_SSH_PASSWORD}"
  cat "${remote_prepare}"
} | sshpass -e ssh "${ssh_opts[@]}" \
  "${RADIXDB_SOAK_SSH_USER}@${RADIXDB_SOAK_SSH_HOST}" \
  "sudo -S -p '' bash -s -- '${RADIXDB_SOAK_SSH_USER}' '${remote_transfer_root}' '${commit}'"

echo "uploading release archive and status credentials"
sshpass -e scp "${ssh_opts[@]}" "${archive}" "${archive}.sha256" \
  "${RADIXDB_SOAK_SSH_USER}@${RADIXDB_SOAK_SSH_HOST}:${remote_transfer}/"
sshpass -e scp "${ssh_opts[@]}" "${http_secret}" \
  "${RADIXDB_SOAK_SSH_USER}@${RADIXDB_SOAK_SSH_HOST}:${remote_auth}"

remote_script="${temporary}/install-remote.sh"
cat >"${remote_script}" <<'REMOTE'
set -euo pipefail
commit="$1"
transfer="$2"
transfer_root="$3"
allow_inactive_production="$4"
archive="${transfer}/radixdb-soak-${commit}.tar.gz"
checksum="${archive}.sha256"
auth="${transfer}/radixdb-soak-status-auth-${commit}.env"
[[ "${commit}" =~ ^[0-9a-f]{40}$ ]]
[[ "${transfer_root}" == /storage/radixdb-soak-transfer ]]
[[ "${transfer}" == "${transfer_root%/}/${commit}" ]]
[[ "${allow_inactive_production}" =~ ^[01]$ ]]
production_was_active=0
production_pid=""
production_exe=""
if systemctl is-active --quiet radixdb.service; then
  production_was_active=1
  production_pid="$(systemctl show radixdb.service -p MainPID --value)"
  production_exe="$(readlink -f "/proc/${production_pid}/exe")"
  ss -ltnp 'sport = :15441' | grep -F "pid=${production_pid}," >/dev/null
else
  [[ "${allow_inactive_production}" == 1 ]] || {
    echo "production radixdb.service is not active; refusing deployment" >&2
    exit 69
  }
  if ss -ltn 'sport = :15441' | grep -q LISTEN; then
    echo "production service is inactive but port 15441 is occupied" >&2
    exit 69
  fi
  echo "warning: production radixdb.service is inactive; preserving that state" >&2
fi
cd "${transfer}"
sha256sum -c "$(basename "${checksum}")"
staging="/storage/radixdb-soak-staging/${commit}"
rm -rf -- "${staging}"
install -d -o root -g root -m 0755 "${staging}"
tar -xzf "${archive}" -C "${staging}"
(cd "${staging}" && sha256sum -c SHA256SUMS)
install -d -o root -g root -m 0755 /etc/radixdb-soak
install -o root -g root -m 0600 "${auth}" /etc/radixdb-soak/status-auth.env
"${staging}/bin/install.sh"
if [[ "${production_was_active}" == 1 ]]; then
  [[ "$(systemctl show radixdb.service -p MainPID --value)" == "${production_pid}" ]]
  [[ "$(readlink -f "/proc/${production_pid}/exe")" == "${production_exe}" ]]
  ss -ltnp 'sport = :15441' | grep -F "pid=${production_pid}," >/dev/null
else
  ! systemctl is-active --quiet radixdb.service
  ! ss -ltn 'sport = :15441' | grep -q LISTEN
fi
rm -rf -- "${staging}"
rm -rf -- "${transfer}"
REMOTE
echo "installing isolated release; soak services remain stopped"
{
  printf '%s\n' "${RADIXDB_SOAK_SSH_PASSWORD}"
  cat "${remote_script}"
} | sshpass -e ssh "${ssh_opts[@]}" \
  "${RADIXDB_SOAK_SSH_USER}@${RADIXDB_SOAK_SSH_HOST}" \
  "sudo -S -p '' bash -s -- '${commit}' '${remote_transfer}' '${remote_transfer_root}' '${allow_inactive_production}'"
unset SSHPASS RADIXDB_SOAK_SSH_PASSWORD
echo "deployed isolated soak release ${commit}; services remain stopped"
