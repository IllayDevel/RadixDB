#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACT_DIR="${1:-${REPO_DIR}/target/prerelease/b10-mutation-proof}"
NEGATIVE_REPORT="${ARTIFACT_DIR}/negative-proof.json"
GREEN_REPORT="${ARTIFACT_DIR}/green-control.json"

cd "${REPO_DIR}"
mkdir -p "${ARTIFACT_DIR}/logs"
: >"${ARTIFACT_DIR}/expected-red.tsv"
: >"${ARTIFACT_DIR}/green.tsv"

run_expected_red() {
    local mutation="$1"
    local test_name="$2"
    local invariant="$3"
    shift 3
    local log="${ARTIFACT_DIR}/logs/${mutation}.log"
    local status

    set +e
    RADIXDB_TEST_MUTATION="${mutation}" "$@" >"${log}" 2>&1
    status=$?
    set -e

    if [[ ${status} -eq 0 ]]; then
        echo "mutation ${mutation} was unexpectedly green" >&2
        return 1
    fi
    if ! grep -Fq -- "${invariant}" "${log}"; then
        echo "mutation ${mutation} failed outside its assigned invariant" >&2
        sed -n '1,220p' "${log}" >&2
        return 1
    fi
    if ! grep -Fq -- "test result: FAILED" "${log}"; then
        echo "mutation ${mutation} did not fail through the assigned Rust test" >&2
        return 1
    fi

    printf '%s\t%s\t%s\t%s\n' \
        "${mutation}" "${test_name}" "${invariant}" "${status}" \
        >>"${ARTIFACT_DIR}/expected-red.tsv"
}

run_green() {
    local name="$1"
    shift
    local log="${ARTIFACT_DIR}/logs/green-${name}.log"
    "$@" >"${log}" 2>&1
    printf '%s\tPASS\n' "${name}" >>"${ARTIFACT_DIR}/green.tsv"
}

INTEGRATION=(cargo test --locked --features test-mutations,stress-tests \
    --test prerelease_mutation_test)

run_expected_red \
    constraint_validation \
    b10_constraint_validation_invariant \
    "PRV-B10 invariant constraint_validation: invalid CHECK row was published" \
    "${INTEGRATION[@]}" b10_constraint_validation_invariant -- --exact --test-threads=1

run_expected_red \
    index_publication \
    b10_index_publication_invariant \
    "PRV-B10 invariant index_publication: duplicate key was admitted after index publication was skipped" \
    "${INTEGRATION[@]}" b10_index_publication_invariant -- --exact --test-threads=1

run_expected_red \
    visibility_fence \
    b10_visibility_fence_invariant \
    "PRV-B10 invariant visibility_fence: one SELECT observed two committed epochs" \
    "${INTEGRATION[@]}" b10_visibility_fence_invariant -- --exact --test-threads=1

run_expected_red \
    checksum_verification \
    storage::mvcc::wal_manager::tests::b10_checksum_verification_invariant \
    "PRV-B10 invariant checksum_verification: corrupted WAL payload was accepted" \
    cargo test --locked --features test-mutations --lib \
    storage::mvcc::wal_manager::tests::b10_checksum_verification_invariant \
    -- --exact --test-threads=1

run_expected_red \
    session_cleanup \
    server::tcp_server::tests::r6_l01_a_disconnected_inflight_query_releases_connection_permit \
    "PRV-B10 invariant session_cleanup: peer disconnect must cancel the in-flight statement and release its permit" \
    cargo test --locked --features test-mutations --lib \
    server::tcp_server::tests::r6_l01_a_disconnected_inflight_query_releases_connection_permit \
    -- --exact --test-threads=1

run_expected_red \
    view_invalidation \
    b10_view_invalidation_invariant \
    "PRV-B10 invariant view_invalidation: DROP VIEW left the old catalog object executable" \
    "${INTEGRATION[@]}" b10_view_invalidation_invariant -- --exact --test-threads=1

# Green controls use the same feature build without a selected mutation, then
# prove that the default server build is mutation-free. The feature-enabled
# server artifact must refuse startup before reading configuration.
run_green mutation-integration \
    "${INTEGRATION[@]}" -- --test-threads=1
run_green checksum-owner \
    cargo test --locked --features test-mutations --lib \
    storage::mvcc::wal_manager::tests::b10_checksum_verification_invariant \
    -- --exact --test-threads=1
run_green session-owner \
    cargo test --locked --features test-mutations --lib \
    server::tcp_server::tests::r6_l01_a_disconnected_inflight_query_releases_connection_permit \
    -- --exact --test-threads=1
run_green default-server-check cargo check --locked --bin radixdb-server

cargo build --locked --bin radixdb-server --features test-mutations \
    >"${ARTIFACT_DIR}/logs/mutation-server-build.log" 2>&1
set +e
target/debug/radixdb-server \
    >"${ARTIFACT_DIR}/logs/mutation-server-start.log" 2>&1
mutation_server_status=$?
set -e
if [[ ${mutation_server_status} -ne 78 ]] \
    || ! grep -Fq -- \
        "test-mutations is forbidden in the production radixdb-server binary" \
        "${ARTIFACT_DIR}/logs/mutation-server-start.log"; then
    echo "feature-enabled server did not fail closed" >&2
    exit 1
fi
printf 'feature-server-start-refusal\tPASS\n' >>"${ARTIFACT_DIR}/green.tsv"

# Restore and inspect the ordinary artifact. The selector string is compiled
# only into the mutation feature graph and must be absent here.
cargo build --locked --bin radixdb-server \
    >"${ARTIFACT_DIR}/logs/default-server-build.log" 2>&1
if strings target/debug/radixdb-server | grep -Fq -- "RADIXDB_TEST_MUTATION"; then
    echo "default radixdb-server contains the mutation selector" >&2
    exit 1
fi
printf 'default-server-symbol-absence\tPASS\n' >>"${ARTIFACT_DIR}/green.tsv"

commit="$(git rev-parse HEAD)"
{
    printf '{\n  "schema_version": 1,\n  "kind": "expected-red",\n'
    printf '  "commit": "%s",\n  "mutations": [\n' "${commit}"
    first=1
    while IFS=$'\t' read -r mutation test_name invariant status; do
        if [[ ${first} -eq 0 ]]; then printf ',\n'; fi
        first=0
        printf '    {"mutation":"%s","test":"%s","caught_invariant":"%s","exit_status":%s}' \
            "${mutation}" "${test_name}" "${invariant}" "${status}"
    done <"${ARTIFACT_DIR}/expected-red.tsv"
    printf '\n  ]\n}\n'
} >"${NEGATIVE_REPORT}"

{
    printf '{\n  "schema_version": 1,\n  "kind": "green-control",\n'
    printf '  "commit": "%s",\n  "checks": [\n' "${commit}"
    first=1
    while IFS=$'\t' read -r name status; do
        if [[ ${first} -eq 0 ]]; then printf ',\n'; fi
        first=0
        printf '    {"name":"%s","status":"%s"}' "${name}" "${status}"
    done <"${ARTIFACT_DIR}/green.tsv"
    printf '\n  ]\n}\n'
} >"${GREEN_REPORT}"

echo "prerelease B10 mutation proof: PASS"
echo "negative report: ${NEGATIVE_REPORT}"
echo "green report: ${GREEN_REPORT}"
