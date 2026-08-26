<!-- SPDX-License-Identifier: Apache-2.0 -->

# Changelog

All notable changes use semantic versioning.

## 1.0.0 - 2026-08-26

- Implement the Meridian Cache Catalog Valkey adapter against Core and
  Semantics 1.0.0.
- Add canonical JSON and raw-byte envelopes, TTL and staleness enforcement,
  exact-key and namespace-generation invalidation, cache-aside single flight,
  PIFA, CAS, and Schema-declared numeric increment helpers.
- Add authenticated standalone and Sentinel profiles, startup/physical probes,
  safe degradation, redacted errors and telemetry, conformance vectors, and
  real Valkey 8.1.9 standalone/failover tests.
- Add Apache-2.0 packaging, deterministic build verification, CI, and release
  provenance.
