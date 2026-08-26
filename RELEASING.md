<!-- SPDX-License-Identifier: Apache-2.0 -->

# Releasing

1. Verify the version, `compatibility.json`, locked dependency hashes, engine
   image digest, changelog, and conformance evidence agree.
2. Run `./scripts/verify.sh`, `./scripts/test-integration.sh`, and
   `./scripts/test-failover.sh` from a clean checkout.
3. Merge a green reviewed pull request without bypassing branch protection.
4. Create an annotated `vX.Y.Z` tag at the merged commit and push it.
5. The release workflow rebuilds, compares distributions, runs tests, emits
   checksums and build provenance, publishes a GitHub release, and publishes to
   PyPI through its trusted publisher.

The first PyPI publication is an owner-assisted namespace/trusted-publisher
gate. Never bypass ownership, MFA, or credential controls. Subsequent releases
are CI-owned.
