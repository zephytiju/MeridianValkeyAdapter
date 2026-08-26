<!-- SPDX-License-Identifier: Apache-2.0 -->

# Operations and recovery

## Standalone profile

`valkey-standalone` expects one writable Valkey 8.1.9 primary, zero replicas,
persistence disabled, bounded maxmemory, and an approved eviction policy. Loss
or restart flushes the cache; callers recover through cache misses and
authoritative reloads.

## Sentinel profile

`valkey-sentinel` expects Valkey 8.1.9, a named Sentinel master, at least the
Binding's configured replica count, and enough Sentinel seeds for quorum. The
client discovers and reconnects to a promoted primary. Reads may temporarily
degrade to misses; mutating and atomic operations report bounded Meridian
unavailability until the primary is writable.

## Startup and health

Open performs authenticated PING and verifies exact engine version, maxmemory,
eviction policy, persistence-disabled policy, writable-primary role, minimum
replicas, script commands, and capability fingerprint. A later capability
fingerprint drift fails the probe. Physical verification accepts only Cache
Resources whose schema fingerprints match the closed Binding policy.

## Invalidation and recovery

Use exact-key invalidation for one key or a bounded explicit list. For a whole
namespace, increment `namespaceGeneration` in deployment IaC and roll the
Binding; old entries become unreachable without a scan. Recovery never restores
Valkey state: restore the authority, replace or restart Valkey, then allow normal
reads to warm the new generation.

## Alerts

Alert on startup probe failures, unavailable misses, population failures,
corruption, cleanup failures, CAS conflicts above the workload baseline, and
failover duration. Counters expose no key or value labels. Investigate Engine
health and IaC drift; never treat retained Valkey bytes as recovery evidence.
