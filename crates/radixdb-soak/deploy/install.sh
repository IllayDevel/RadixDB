#!/usr/bin/env bash
set -euo pipefail

[[ "$(id -u)" == 0 ]] || { echo "install.sh must run as root" >&2; exit 77; }
release_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
commit="$(awk -F= '$1 == "git_commit" {print $2}' "${release_dir}/RELEASE")"
[[ "${commit}" =~ ^[0-9a-f]{40}$ ]] || { echo "invalid release identity" >&2; exit 65; }
install_root="/storage/radixdb-soak"
target="${install_root}/releases/${commit}"

if systemctl is-active --quiet radixdb-soak-observer.service 2>/dev/null ||
   systemctl is-active --quiet radixdb-soak-agent.service 2>/dev/null ||
   systemctl is-active --quiet radixdb-soak-db.service 2>/dev/null; then
  echo "refusing install while a soak service is active" >&2
  exit 75
fi
getent group radixdb-soak >/dev/null || groupadd --system radixdb-soak
id radixdb-soak >/dev/null 2>&1 || useradd --system --gid radixdb-soak \
  --home-dir "${install_root}" --shell /usr/sbin/nologin radixdb-soak
install -d -o radixdb-soak -g radixdb-soak -m 0700 \
  "${install_root}" "${install_root}/releases" "${install_root}/data" \
  "${install_root}/runs" "${install_root}/logs"
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

temporary="${install_root}/.current.${commit}"
ln -s "releases/${commit}" "${temporary}"
mv -Tf -- "${temporary}" "${install_root}/current"
install -m 0644 "${target}/etc/server.toml" /etc/radixdb-soak/server.toml
install -m 0644 "${target}/systemd/radixdb-soak.slice" /etc/systemd/system/radixdb-soak.slice
install -m 0644 "${target}/systemd/radixdb-soak-db.service" /etc/systemd/system/radixdb-soak-db.service
install -m 0644 "${target}/systemd/radixdb-soak-agent.service" /etc/systemd/system/radixdb-soak-agent.service
install -m 0644 "${target}/systemd/radixdb-soak-observer.service" /etc/systemd/system/radixdb-soak-observer.service
systemctl daemon-reload
echo "installed isolated soak release ${commit}; no service was started"
