#!/usr/bin/env bash
set -euo pipefail

[[ "$(id -u)" == 0 ]] || { echo "prepare-run.sh must run as root" >&2; exit 77; }
[[ "$#" == 2 ]] || { echo "usage: prepare-run.sh PROFILE RUN_ID" >&2; exit 64; }
profile="$1"
run_id="$2"
case "${profile}" in smoke|tuning|6h|24h|48h) ;; *) echo "invalid profile" >&2; exit 64;; esac
[[ "${run_id}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || {
  echo "invalid run id" >&2; exit 64;
}

root=/storage/radixdb-soak
config="${root}/current/etc/soak-${profile}.toml"
[[ -f "${config}" ]] || { echo "missing profile config ${config}" >&2; exit 66; }
[[ -f /etc/radixdb-soak/status-auth.env ]] || {
  echo "missing /etc/radixdb-soak/status-auth.env" >&2; exit 66;
}
[[ "$(stat -c %a /etc/radixdb-soak/status-auth.env)" == 600 ]] || {
  echo "status auth file must have mode 0600" >&2; exit 65;
}
if systemctl is-active --quiet radixdb-soak-observer.service ||
   systemctl is-active --quiet radixdb-soak-agent.service ||
   systemctl is-active --quiet radixdb-soak-db.service; then
  echo "refusing to replace an active soak run" >&2
  exit 75
fi
for port in 25441 18088 18089; do
  if ss -ltn "sport = :${port}" | awk 'NR > 1 {found=1} END {exit !found}'; then
    echo "reserved soak port ${port} is occupied" >&2
    exit 75
  fi
done
[[ ! -e "${root}/data/${run_id}" && ! -e "${root}/runs/${run_id}" ]] || {
  echo "run id already owns data or evidence" >&2; exit 73;
}

install -d -o radixdb-soak -g radixdb-soak -m 0700 "${root}/data/${run_id}"
temporary="${root}/data/.current.${run_id}"
ln -s "${run_id}" "${temporary}"
mv -Tf -- "${temporary}" "${root}/data/current"
install -m 0644 "${config}" /etc/radixdb-soak/soak.toml
run_env="$(mktemp /etc/radixdb-soak/run.env.XXXXXX)"
chmod 0600 "${run_env}"
printf 'SOAK_RUN_ID=%s\n' "${run_id}" >"${run_env}"
mv -f -- "${run_env}" /etc/radixdb-soak/run.env

systemctl start radixdb-soak-db.service
for _ in {1..600}; do
  ss -ltn 'sport = :25441' | awk 'NR > 1 {found=1} END {exit !found}' && break
  sleep 0.1
done
ss -ltn 'sport = :25441' | awk 'NR > 1 {found=1} END {exit !found}' || {
  systemctl stop radixdb-soak-db.service
  echo "soak database did not become ready" >&2
  exit 1
}
systemctl start radixdb-soak-observer.service
for _ in {1..100}; do
  [[ -S /run/radixdb-soak/agent.sock ]] && break
  sleep 0.1
done
[[ -S /run/radixdb-soak/agent.sock ]] || {
  systemctl stop radixdb-soak-observer.service radixdb-soak-db.service
  echo "soak observer telemetry socket did not become ready" >&2
  exit 1
}
systemctl start radixdb-soak-agent.service
echo "started isolated Atom/HDD soak run ${run_id} profile=${profile}"
