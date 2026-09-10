#!/usr/bin/env bash
set -euo pipefail

[[ "$(id -u)" == 0 ]] || { echo "prepare-run.sh must run as root" >&2; exit 77; }
[[ "$#" == 1 ]] || { echo "usage: prepare-run.sh PAIR_ID" >&2; exit 64; }
pair_id="$1"
[[ "${pair_id}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,116}$ ]] || {
  echo "invalid pair id" >&2; exit 64;
}
run_id="${pair_id}-postgresql"
root=/storage/radixdb-soak
config="${root}/current/etc/soak-6h.toml"
password_file=/etc/radixdb-soak/postgresql-password
for file in "${config}" /etc/radixdb-soak/status-auth.env "${password_file}"; do
  [[ -f "${file}" ]] || { echo "missing required file ${file}" >&2; exit 66; }
done
[[ "$(stat -c %a /etc/radixdb-soak/status-auth.env)" == 600 ]] || {
  echo "status auth file must have mode 0600" >&2; exit 65;
}
[[ "$(stat -c %a "${password_file}")" == 640 ]] || {
  echo "PostgreSQL password file must have mode 0640" >&2; exit 65;
}
[[ "$(stat -c %U:%G "${password_file}")" == root:radixdb-soak ]] || {
  echo "PostgreSQL password file must be root:radixdb-soak" >&2; exit 65;
}
if systemctl is-active --quiet radixdb-soak-observer.service ||
   systemctl is-active --quiet radixdb-soak-agent.service ||
   systemctl is-active --quiet radixdb-soak-db.service; then
  echo "refusing to replace an active soak run" >&2
  exit 75
fi
for port in 25432 18088 18089; do
  if ss -ltn "sport = :${port}" | awk 'NR > 1 {found=1} END {exit !found}'; then
    echo "reserved PostgreSQL soak port ${port} is occupied" >&2
    exit 75
  fi
done
data="${root}/postgresql/data/${run_id}"
import="${root}/postgresql/import/${run_id}"
[[ ! -e "${data}" && ! -e "${import}" && ! -e "${root}/runs/${run_id}" ]] || {
  echo "run id already owns data, import or evidence" >&2; exit 73;
}
install -d -o radixdb-soak -g radixdb-soak -m 0700 "${data}" "${import}"
runuser -u radixdb-soak -- "${root}/postgresql/bin/initdb" \
  -D "${data}" -U radixdb-soak --encoding=UTF8 --no-locale \
  --auth-local=trust --auth-host=scram-sha-256 --pwfile="${password_file}"
for pair in "data:${data}" "import:${import}"; do
  area="${pair%%:*}"
  target="${pair#*:}"
  temporary="${root}/postgresql/${area}/.current.${run_id}"
  ln -s "${run_id}" "${temporary}"
  mv -Tf -- "${temporary}" "${root}/postgresql/${area}/current"
done
install -m 0644 "${config}" /etc/radixdb-soak/soak.toml
run_env="$(mktemp /etc/radixdb-soak/run.env.XXXXXX)"
chmod 0600 "${run_env}"
printf 'SOAK_RUN_ID=%s\n' "${run_id}" >"${run_env}"
mv -f -- "${run_env}" /etc/radixdb-soak/run.env

systemctl start radixdb-soak-db.service
for _ in {1..600}; do
  "${root}/postgresql/bin/pg_isready" -q -h 127.0.0.1 -p 25432 -U radixdb-soak && break
  sleep 0.1
done
"${root}/postgresql/bin/pg_isready" -q -h 127.0.0.1 -p 25432 -U radixdb-soak || {
  systemctl stop radixdb-soak-db.service
  echo "PostgreSQL soak database did not become ready" >&2
  exit 1
}
PGPASSWORD="$(<"${password_file}")" "${root}/postgresql/bin/createdb" \
  -h 127.0.0.1 -p 25432 -U radixdb-soak radixdb_soak
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
echo "started isolated PostgreSQL 6h soak run ${run_id}"
