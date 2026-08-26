<!-- SPDX-License-Identifier: Apache-2.0 -->

# Conformance evidence

The committed vectors in `evidence/conformance-vectors.json` cover deterministic
key encoding, envelope framing, serializer/schema validation, TTL/staleness,
negative caching, generation invalidation, single flight, PIFA/CAS, shared hash
slots, corruption cleanup, unavailable-as-miss, redaction, descriptor denial,
and packaging metadata.

`./scripts/verify.sh` runs unit, contract, conformance, packaging, lint, typing,
security, SPDX, lockfile, and reproducible-build checks. The standalone script
runs the same Cache contract against a real Valkey 8.1.9 primary with persistence
off and bounded eviction. The failover script runs Sentinel with one primary,
two replicas, and three Sentinels, stops the original primary, waits for quorum
promotion, and verifies writes/reads through the existing adapter runtime.

`evidence/release-evidence.json` is generated for a release commit and contains
the source revision, test summary, environment/profile versions, conformance
vector digest, and distribution digests. It is not embedded back into the
distribution it describes.
