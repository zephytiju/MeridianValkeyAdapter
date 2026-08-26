#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_dir/tests/integration/docker/compose.single.yml"
project="meridian-valkey-standalone"
if [[ -n "${PYTHON:-}" ]]; then
  python_bin="$PYTHON"
elif [[ -x "$repo_dir/.venv/bin/python" ]]; then
  python_bin="$repo_dir/.venv/bin/python"
else
  python_bin="python"
fi

cleanup() {
  docker compose -p "$project" -f "$compose_file" down --volumes --remove-orphans
}
trap cleanup EXIT

docker compose -p "$project" -f "$compose_file" up --detach --wait
MERIDIAN_VALKEY_STANDALONE_ENDPOINT="valkey://127.0.0.1:6391" \
MERIDIAN_VALKEY_STANDALONE_COMPOSE="$compose_file" \
  "$python_bin" -m pytest -m integration tests/integration
