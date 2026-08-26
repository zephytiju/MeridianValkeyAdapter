#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python_bin="${PYTHON:-$repo_dir/.venv/bin/python}"
build_root="$(mktemp -d)"

cleanup() {
  rm -rf "$build_root"
}
trap cleanup EXIT

export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1787778000}"
"$python_bin" -m build --no-isolation --outdir "$build_root/first" "$repo_dir"
"$python_bin" -m build --no-isolation --outdir "$build_root/second" "$repo_dir"

for artifact in meridian_storage_valkey-1.0.0-py3-none-any.whl meridian_storage_valkey-1.0.0.tar.gz; do
  first="$(shasum -a 256 "$build_root/first/$artifact" | awk '{print $1}')"
  second="$(shasum -a 256 "$build_root/second/$artifact" | awk '{print $1}')"
  test "$first" = "$second"
  printf '%s  %s\n' "$first" "$artifact"
done
