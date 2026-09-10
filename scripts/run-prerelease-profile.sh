#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
profile_root="${RADIXDB_B11_ROOT:-${repo_root}/target/prerelease/b11-profile}"
run_tag="${RADIXDB_B11_RUN_TAG:-20260822}"
bench="${repo_root}/target/release/radixdb-bench"
artifact_dir="${profile_root}/artifacts-${run_tag}"

require_command() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "required command is missing: $1" >&2
        exit 2
    }
}

require_command sha256sum
require_command valgrind
require_command callgrind_annotate
require_command ms_print

if [[ ! -x "${bench}" ]]; then
    echo "release benchmark binary is missing: ${bench}" >&2
    exit 2
fi
if [[ -e "${artifact_dir}" ]]; then
    echo "profile artifact directory already exists: ${artifact_dir}" >&2
    exit 2
fi

mkdir -p "${artifact_dir}"
binary_sha="$(sha256sum "${bench}" | awk '{print $1}')"
git_revision="$(git -C "${repo_root}" rev-parse HEAD)"
worktree_diff_sha="$(git -C "${repo_root}" diff --binary | sha256sum | awk '{print $1}')"

common_args=(
    --run
    --scale dev
    --participant server
    --root "${profile_root}"
)

run_native() {
    local ordinal="$1"
    /usr/bin/time -v -o "${artifact_dir}/native-r${ordinal}.time.txt" \
        "${bench}" "${common_args[@]}" \
        --run-id "prerelease-b11-native-r${ordinal}-${run_tag}"
}

run_native 1
run_native 2

/usr/bin/time -v -o "${artifact_dir}/callgrind.time.txt" \
    valgrind --tool=callgrind --trace-children=yes --collect-jumps=yes \
    --callgrind-out-file="${artifact_dir}/callgrind.%p.out" \
    "${bench}" "${common_args[@]}" \
    --run-id "prerelease-b11-callgrind-${run_tag}"
callgrind_annotate --auto=yes "${artifact_dir}"/callgrind.*.out \
    >"${artifact_dir}/callgrind.summary.txt"

/usr/bin/time -v -o "${artifact_dir}/massif.time.txt" \
    valgrind --tool=massif --stacks=yes --time-unit=ms \
    --massif-out-file="${artifact_dir}/massif.%p.out" \
    "${bench}" "${common_args[@]}" \
    --run-id "prerelease-b11-massif-${run_tag}"
ms_print "${artifact_dir}"/massif.*.out >"${artifact_dir}/massif.summary.txt"

{
    echo "binary=${bench}"
    echo "binary_sha256=${binary_sha}"
    echo "git_revision=${git_revision}"
    echo "worktree_diff_sha256=${worktree_diff_sha}"
    echo "scale=dev"
    echo "participant=server"
    echo "run_tag=${run_tag}"
} >"${artifact_dir}/MANIFEST.txt"

echo "B11 profiles complete: ${artifact_dir}"
