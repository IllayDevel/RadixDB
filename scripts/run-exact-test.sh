#!/usr/bin/env bash
set -euo pipefail

if (( $# < 1 )); then
  echo "usage: $0 <exact-test-name> [cargo test selection arguments...]" >&2
  exit 2
fi

test_name=$1
shift

inventory=$(mktemp)
trap 'rm -f -- "${inventory}"' EXIT

cargo test --locked "$@" -- --list --format terse >"${inventory}"
selected=$(awk -v expected="${test_name}: test" '$0 == expected { count++ } END { print count + 0 }' "${inventory}")
if [[ "${selected}" != 1 ]]; then
  echo "exact test inventory must contain ${test_name} once, found ${selected}" >&2
  exit 1
fi

cargo test --locked "$@" "${test_name}" -- --exact --nocapture
