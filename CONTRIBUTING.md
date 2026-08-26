<!-- SPDX-License-Identifier: Apache-2.0 -->

# Contributing

Changes must preserve the Meridian V1 authority boundary: this package is one
Valkey adapter and Valkey remains disposable Cache storage. Do not add
authoritative fallbacks, consumer commands or scripts, unbounded scans, or new
Catalog operation names. An interface or architecture change requires an
approved design update before implementation.

Use Python 3.12 or newer, install `.[test]`, and run `./scripts/verify.sh`.
Changes to advertised standalone or Sentinel behavior also require the matching
real-server script. Tests must be deterministic, avoid secrets in output, and
include SPDX headers on source, workflow, schema, script, and fixture files.

Commits must be reviewable and must not include generated environments, cache
directories, credentials, or mutable production configuration.
