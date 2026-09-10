#!/usr/bin/env bash
set -euo pipefail

[[ "$(id -u)" == 0 ]] || { echo "stop-run.sh must run as root" >&2; exit 77; }
systemctl stop radixdb-soak-observer.service || true
systemctl stop radixdb-soak-agent.service || true
systemctl stop radixdb-soak-db.service || true
echo "isolated PostgreSQL soak services stopped; data and evidence preserved"

