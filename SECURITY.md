<!-- SPDX-License-Identifier: Apache-2.0 -->

# Security policy

Report vulnerabilities privately through GitHub's **Security advisories** page
for this repository. Do not open a public issue containing exploit details,
credentials, endpoints, logical keys, cached values, or tenant data.

Only the latest release is supported. Production composition must use verified
server TLS, a least-privilege ACL identity, bounded client pools and timeouts,
bounded maxmemory with an approved eviction policy, and deployment-managed
secret references. This package never logs credentials or cached payloads.

Cached content is disposable and may be evicted or lost. Do not rely on Valkey
for authoritative records, recovery, evidence, audit, or durable coordination.
