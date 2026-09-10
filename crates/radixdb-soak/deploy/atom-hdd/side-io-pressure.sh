#!/usr/bin/env bash
set -euo pipefail

# Diagnostic side-load for a running Atom/HDD soak.  The pressure window is
# deliberately shorter than the soak watchdog: 15 minutes of direct random
# writes followed by 5 minutes in which the engine can drain accumulated work.

readonly PRESSURE_SECONDS=900
readonly RECOVERY_SECONDS=300
readonly SAFETY_MARGIN_SECONDS=30
readonly SAMPLE_SECONDS=60
readonly BLOCK_SIZE=4M
readonly BLOCK_COUNT=11921
readonly TARGET_BYTES=50000297984
readonly REQUIRED_FREE_BYTES=$((TARGET_BYTES + 10 * 1024 * 1024 * 1024))
readonly ROOT=/storage/radixdb-soak
readonly PRESSURE_ROOT="${ROOT}/io-pressure"
readonly AUTH_FILE=/etc/radixdb-soak/status-auth.env
readonly STATUS_URL=http://127.0.0.1:18088/api/v1/status

fail() {
  echo "side-io-pressure: $*" >&2
  exit 1
}

[[ "$(id -u)" == 0 ]] || fail "must run as root"
[[ "$#" == 0 ]] || fail "usage: side-io-pressure.sh"

for command in curl date dd df flock jq stat systemctl timeout; do
  command -v "${command}" >/dev/null || fail "missing command: ${command}"
done

[[ -d "${ROOT}" ]] || fail "missing soak root: ${ROOT}"
[[ -f "${AUTH_FILE}" ]] || fail "missing status credentials: ${AUTH_FILE}"
systemctl is-active --quiet radixdb-soak-agent.service ||
  fail "radixdb-soak-agent.service is not active"
systemctl is-active --quiet radixdb-soak-db.service ||
  fail "radixdb-soak-db.service is not active"

exec 9>/run/radixdb-soak-side-io-pressure.lock
flock -n 9 || fail "another side-I/O pressure run is active"

set -a
# shellcheck disable=SC1090 -- deployment-owned root-only credential file.
source "${AUTH_FILE}"
set +a
: "${RADIXDB_SOAK_HTTP_USER:?missing RADIXDB_SOAK_HTTP_USER}"
: "${RADIXDB_SOAK_HTTP_PASSWORD:?missing RADIXDB_SOAK_HTTP_PASSWORD}"

status_json() {
  curl --fail --silent --show-error --max-time 10 \
    --user "${RADIXDB_SOAK_HTTP_USER}:${RADIXDB_SOAK_HTTP_PASSWORD}" \
    "${STATUS_URL}"
}

status_line() {
  local label="$1"
  local status
  status="$(status_json)" || fail "cannot read soak status"
  jq -c --arg label "${label}" --arg timestamp "$(date -Ins)" '
    {
      timestamp: $timestamp,
      label: $label,
      state,
      phase,
      watchdog_state,
      watchdog_silence_millis,
      current_tps,
      active_clients,
      target_clients,
      planned: .counters.transactions_planned,
      committed: .counters.transactions_committed,
      rolled_back: .counters.transactions_rolled_back,
      conflicts: .counters.conflicts,
      operations: .counters.operations,
      invariant_failures: .counters.invariant_failures,
      failure
    }
  ' <<<"${status}"
}

initial_status="$(status_json)" || fail "cannot read initial soak status"
initial_state="$(jq -r '.state' <<<"${initial_status}")"
initial_failure="$(jq -r '.failure // empty' <<<"${initial_status}")"
initial_watchdog="$(jq -r '.watchdog_state' <<<"${initial_status}")"
initial_silence="$(jq -r '.watchdog_silence_millis' <<<"${initial_status}")"
watchdog_timeout="$(jq -r '.watchdog_timeout_millis' <<<"${initial_status}")"
required_watchdog_millis=$((
  (PRESSURE_SECONDS + RECOVERY_SECONDS + SAFETY_MARGIN_SECONDS) * 1000
))

[[ "${initial_state}" == running ]] || fail "soak state is ${initial_state}, not running"
[[ -z "${initial_failure}" ]] || fail "soak already reports failure: ${initial_failure}"
[[ "${initial_watchdog}" == healthy ]] ||
  fail "soak watchdog is ${initial_watchdog}, not healthy"
(( initial_silence <= SAFETY_MARGIN_SECONDS * 1000 )) ||
  fail "workload progress is already ${initial_silence} ms stale"
(( watchdog_timeout >= required_watchdog_millis )) ||
  fail "watchdog ${watchdog_timeout} ms is shorter than the diagnostic contract"

available_bytes="$(df --block-size=1 --output=avail "${ROOT}" | tail -n 1 | tr -d ' ')"
(( available_bytes >= REQUIRED_FREE_BYTES )) ||
  fail "need ${REQUIRED_FREE_BYTES} free bytes, found ${available_bytes}"

install -d -m 0700 "${PRESSURE_ROOT}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
target="${PRESSURE_ROOT}/random-${timestamp}.bin"
[[ ! -e "${target}" ]] || fail "refusing to overwrite ${target}"

pressure_pid=""
stop_pressure() {
  if [[ -n "${pressure_pid}" ]] && kill -0 "${pressure_pid}" 2>/dev/null; then
    kill -TERM "${pressure_pid}" 2>/dev/null || true
    wait "${pressure_pid}" 2>/dev/null || true
  fi
}
trap stop_pressure EXIT INT TERM HUP

echo "side-io-pressure: target=${target}"
echo "side-io-pressure: pressure_seconds=${PRESSURE_SECONDS} recovery_seconds=${RECOVERY_SECONDS}"
status_line pressure-start

# Repeat the bounded 50 GB write if unusually fast storage completes it before
# the time window.  GNU timeout owns the process group and terminates both the
# loop and the current dd at the exact pressure deadline.
timeout --signal=TERM --kill-after=30s "${PRESSURE_SECONDS}s" \
  bash -c '
    while :; do
      dd if=/dev/urandom of="$1" bs="$2" count="$3" \
        iflag=fullblock oflag=direct status=none
    done
  ' _ "${target}" "${BLOCK_SIZE}" "${BLOCK_COUNT}" &
pressure_pid=$!

for ((elapsed = SAMPLE_SECONDS; elapsed <= PRESSURE_SECONDS; elapsed += SAMPLE_SECONDS)); do
  sleep "${SAMPLE_SECONDS}"
  if ! kill -0 "${pressure_pid}" 2>/dev/null; then
    break
  fi
  current_bytes="$(stat -c %s "${target}" 2>/dev/null || echo 0)"
  echo "side-io-pressure: pressure_elapsed_seconds=${elapsed} file_bytes=${current_bytes}"
  status_line "pressure-${elapsed}s"
done

set +e
wait "${pressure_pid}"
pressure_status=$?
set -e
pressure_pid=""
case "${pressure_status}" in
  124|137|143) ;;
  *) fail "pressure writer exited unexpectedly with status ${pressure_status}" ;;
esac

final_bytes="$(stat -c %s "${target}")"
echo "side-io-pressure: pressure-stopped file_bytes=${final_bytes}"
status_line pressure-stopped

for ((elapsed = SAMPLE_SECONDS; elapsed <= RECOVERY_SECONDS; elapsed += SAMPLE_SECONDS)); do
  sleep "${SAMPLE_SECONDS}"
  status_line "recovery-${elapsed}s"
done

final_status="$(status_json)" || fail "cannot read final soak status"
final_state="$(jq -r '.state' <<<"${final_status}")"
final_watchdog="$(jq -r '.watchdog_state' <<<"${final_status}")"
final_invariant_failures="$(jq -r '.counters.invariant_failures' <<<"${final_status}")"
final_failure="$(jq -r '.failure // empty' <<<"${final_status}")"

[[ "${final_state}" == running || "${final_state}" == passed ]] ||
  fail "soak ended in state ${final_state}"
[[ "${final_watchdog}" == healthy ]] ||
  fail "watchdog ended ${final_watchdog}"
[[ "${final_invariant_failures}" == 0 ]] ||
  fail "soak reports ${final_invariant_failures} invariant failures"
[[ -z "${final_failure}" ]] || fail "soak reports failure: ${final_failure}"

echo "side-io-pressure: completed; retained ${target}"
