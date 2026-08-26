<!-- SPDX-License-Identifier: Apache-2.0 -->

# Releasing

1. Verify the version, `compatibility.json`, locked dependency hashes, engine
   image digest, changelog, and conformance evidence agree.
2. Run `./scripts/verify.sh`, `./scripts/test-integration.sh`, and
   `./scripts/test-failover.sh` from a clean checkout.
3. Merge a green reviewed pull request without bypassing branch protection.
4. Create an annotated `vX.Y.Z` tag at the merged commit and push it.
5. The release workflow verifies tag/version/main ancestry, rebuilds and
   compares distributions, runs every test profile, emits checksums and build
   provenance, and publishes or byte-verifies an immutable GitHub release.
6. PyPI publication runs automatically only after the repository variable
   `PYPI_TRUSTED_PUBLISHING_ENABLED` is `true`. A manual recovery dispatch must
   name the existing tag and explicitly set `publish_pypi`; it rebuilds from
   that tag and verifies existing GitHub assets before publishing.

The first PyPI publication is an owner-assisted namespace/trusted-publisher
gate. Never bypass ownership, MFA, or credential controls. Subsequent releases
are CI-owned.
