#!/usr/bin/env bash
set -euo pipefail

[[ "$(id -u)" == 0 ]] || { echo "stop-run.sh must run as root" >&2; exit 77; }
# Stop observation before intentionally removing either observed process. This
# administrative path must not manufacture agent/server failure incidents.
systemctl stop radixdb-soak-observer.service || true
systemctl stop radixdb-soak-agent.service || true
systemctl stop radixdb-soak-db.service || true
echo "isolated Atom/HDD soak services stopped; data and evidence preserved"
