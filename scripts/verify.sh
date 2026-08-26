#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python_bin="${PYTHON:-$repo_dir/.venv/bin/python}"

cd "$repo_dir"
"$python_bin" scripts/check-spdx.py
"$python_bin" -m ruff check .
"$python_bin" -m ruff format --check .
"$python_bin" -m mypy
"$python_bin" -m bandit -q -r src
"$python_bin" -m pytest -m "not integration and not cluster and not packaging" \
  --cov=meridian_storage.adapters.valkey --cov-report=term-missing

rm -rf dist
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1787778000}" \
  "$python_bin" -m build --no-isolation
"$python_bin" -m twine check dist/*
"$python_bin" -m pytest -m packaging tests/packaging
PYTHON="$python_bin" "$repo_dir/scripts/reproducible-build.sh"
"$python_bin" -m pip_audit -r requirements.lock --disable-pip
