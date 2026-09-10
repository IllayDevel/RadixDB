#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/02-run-query-tour.sh"
"${SCRIPT_DIR}/03-partial-index-reject-probe.sh"

echo
echo "RadixTrade public examples smoke passed."

