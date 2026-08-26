#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_dir/tests/cluster/docker/compose.sentinel.yml"
project="meridian-valkey-failover"

cleanup() {
  docker compose -p "$project" -f "$compose_file" down --volumes --remove-orphans
}
trap cleanup EXIT

docker compose -p "$project" -f "$compose_file" up --detach --wait
MERIDIAN_VALKEY_SENTINEL_SEEDS="valkey://127.0.0.1:26391,valkey://127.0.0.1:26392,valkey://127.0.0.1:26393" \
MERIDIAN_VALKEY_SENTINEL_COMPOSE="$compose_file" \
  "$repo_dir/.venv/bin/pytest" -m cluster tests/cluster
