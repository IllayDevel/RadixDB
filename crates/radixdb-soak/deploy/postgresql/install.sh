#!/usr/bin/env bash
set -euo pipefail

[[ "$(id -u)" == 0 ]] || { echo "install.sh must run as root" >&2; exit 77; }
release_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
commit="$(awk -F= '$1 == "git_commit" {print $2}' "${release_dir}/RELEASE")"
engine="$(awk -F= '$1 == "engine" {print $2}' "${release_dir}/RELEASE")"
[[ "${commit}" =~ ^[0-9a-f]{40}$ && "${engine}" == postgresql ]] || {
  echo "invalid PostgreSQL release identity" >&2; exit 65;
}
root=/storage/radixdb-soak
target="${root}/releases/${commit}-postgresql"
if systemctl is-active --quiet radixdb-soak-observer.service 2>/dev/null ||
   systemctl is-active --quiet radixdb-soak-agent.service 2>/dev/null ||
   systemctl is-active --quiet radixdb-soak-db.service 2>/dev/null; then
  echo "refusing install while a soak service is active" >&2
  exit 75
fi
command -v pg_config >/dev/null || { echo "missing PostgreSQL pg_config" >&2; exit 69; }
pg_bindir="$(pg_config --bindir)"
for program in postgres initdb pg_isready createdb; do
  [[ -x "${pg_bindir}/${program}" ]] || {
    echo "missing PostgreSQL program ${pg_bindir}/${program}" >&2; exit 69;
  }
done
pg_major="$("${pg_bindir}/postgres" --version | awk '{split($3, value, "."); print value[1]}')"
[[ "${pg_major}" =~ ^[0-9]+$ && "${pg_major}" -ge 14 ]] || {
  echo "PostgreSQL 14 or newer is required" >&2; exit 69;
}
getent group radixdb-soak >/dev/null || groupadd --system radixdb-soak
id radixdb-soak >/dev/null 2>&1 || useradd --system --gid radixdb-soak \
  --home-dir "${root}" --shell /usr/sbin/nologin radixdb-soak
install -d -o radixdb-soak -g radixdb-soak -m 0700 \
  "${root}" "${root}/releases" "${root}/runs" "${root}/logs" \
  "${root}/postgresql" "${root}/postgresql/bin" \
  "${root}/postgresql/data" "${root}/postgresql/import"
install -d -o root -g root -m 0755 /etc/radixdb-soak

if [[ -e "${target}" ]]; then
  [[ -d "${target}" ]] || { echo "release target is not a directory" >&2; exit 65; }
  (cd "${target}" && sha256sum -c SHA256SUMS)
else
  install -d -o root -g root -m 0755 "${target}"
  cp -a -- "${release_dir}/." "${target}/"
  chown -R root:root "${target}"
  (cd "${target}" && sha256sum -c SHA256SUMS)
fi

temporary="${root}/.current.${commit}.postgresql"
ln -s "releases/${commit}-postgresql" "${temporary}"
mv -Tf -- "${temporary}" "${root}/current"
for program in postgres initdb pg_isready createdb; do
  temporary="${root}/postgresql/bin/.${program}.${commit}"
  ln -s "${pg_bindir}/${program}" "${temporary}"
  mv -Tf -- "${temporary}" "${root}/postgresql/bin/${program}"
done
install -m 0644 "${target}/etc/postgresql.conf" /etc/radixdb-soak/postgresql.conf
install -m 0644 "${target}/etc/pg_hba.conf" /etc/radixdb-soak/pg_hba.conf
install -m 0644 "${target}/systemd/radixdb-soak.slice" /etc/systemd/system/radixdb-soak.slice
install -m 0644 "${target}/systemd/radixdb-soak-db.service" /etc/systemd/system/radixdb-soak-db.service
install -m 0644 "${target}/systemd/radixdb-soak-agent.service" /etc/systemd/system/radixdb-soak-agent.service
install -m 0644 "${target}/systemd/radixdb-soak-observer.service" /etc/systemd/system/radixdb-soak-observer.service
systemctl daemon-reload
echo "installed isolated PostgreSQL soak release ${commit}; no service was started"
