<!-- SPDX-License-Identifier: Apache-2.0 -->

# Architecture and authority boundary

The package implements one adapter (`org.meridian.storage.valkey`) for one
Meridian Catalog (`cache`). It consumes released Core/Semantics 1.0.0 artifacts
and has no dependency on unreleased repository heads.

```text
mapping-first Cache Expression
        -> serialized Meridian Operation
        -> Core Adapter SPI session
        -> Valkey key/envelope/cache primitives
        -> deployment-selected Valkey standalone or Sentinel service
```

Consumers see ResourceRefs, Expressions, Operations, Cache entries, and stable
Meridian errors. They do not see Valkey commands, scripts, credentials,
endpoints, topology, failover, or hash-slot mechanics.

Platform/Vangu IaC is authoritative for Engine selection, provisioning and
references, topology, maxmemory and eviction, ACL identity, secret and TLS
material, migrations, namespace-generation rollout, recovery, and lifecycle.
The adapter validates those decisions at startup but does not provision or
mutate them.

The consistency class is `disposable-cache`. Data may disappear through TTL,
eviction, restart, failover, corruption cleanup, or namespace-generation
change. Cache-aside loaders execute outside Valkey atomic sections and are the
only authority. This package denies transactional, durable, structured, object,
evidence, and streaming guarantees.
