# SPDX-License-Identifier: Apache-2.0
"""Closed, deployment-owned Valkey Binding settings."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Never, cast

from meridian_storage.errors import ConfigurationError, ErrorCode, SafeCause

from ._canonical import boolean, integer, require_fingerprint, safe_token

SERIALIZER_CANONICAL_JSON = "canonical-json"
SERIALIZER_RAW_BYTES = "raw-bytes"
SUPPORTED_SERIALIZERS = (SERIALIZER_CANONICAL_JSON, SERIALIZER_RAW_BYTES)
TOPOLOGY_STANDALONE = "standalone"
TOPOLOGY_SENTINEL = "sentinel"


def _fail(message: str, exc: BaseException | None = None) -> Never:
    details = {"cause": SafeCause.from_exception(exc)} if exc is not None else {}
    raise ConfigurationError(ErrorCode.CONFIG_INVALID, message, **details)


def _closed(
    value: object,
    field_name: str,
    *,
    allowed: frozenset[str],
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail(f"{field_name} must be an object")
    item = cast(Mapping[str, object], value)
    unknown = set(item) - allowed
    if unknown:
        _fail(f"{field_name} contains unknown fields: {sorted(unknown)!r}")
    return item


def _strings(value: object, field_name: str, *, maximum: int = 256) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        _fail(f"{field_name} must be an array")
    try:
        result = tuple(sorted(safe_token(item, field_name, maximum=maximum) for item in value))
    except ValueError as exc:
        _fail(str(exc), exc)
    if not result or len(set(result)) != len(result):
        _fail(f"{field_name} must contain unique values")
    return result


@dataclass(frozen=True, slots=True)
class AdapterLimits:
    max_key_bytes: int = 512
    max_value_bytes: int = 4 * 1024 * 1024
    max_batch_size: int = 128

    @classmethod
    def from_mapping(cls, value: object) -> AdapterLimits:
        item = _closed(
            value,
            "binding.settings.limits",
            allowed=frozenset({"maxKeyBytes", "maxValueBytes", "maxBatchSize"}),
        )
        try:
            return cls(
                max_key_bytes=integer(item.get("maxKeyBytes", 512), "limits.maxKeyBytes", 96, 4096),
                max_value_bytes=integer(
                    item.get("maxValueBytes", 4 * 1024 * 1024),
                    "limits.maxValueBytes",
                    1,
                    64 * 1024 * 1024,
                ),
                max_batch_size=integer(
                    item.get("maxBatchSize", 128), "limits.maxBatchSize", 1, 10_000
                ),
            )
        except ValueError as exc:
            _fail(str(exc), exc)


@dataclass(frozen=True, slots=True)
class TTLPolicy:
    default_ttl_ms: int = 60_000
    minimum_ttl_ms: int = 10
    maximum_ttl_ms: int = 86_400_000
    negative_ttl_ms: int = 5_000

    @classmethod
    def from_mapping(cls, value: object) -> TTLPolicy:
        item = _closed(
            value,
            "binding.settings.ttl",
            allowed=frozenset({"defaultTtlMs", "minimumTtlMs", "maximumTtlMs", "negativeTtlMs"}),
        )
        try:
            result = cls(
                default_ttl_ms=integer(
                    item.get("defaultTtlMs", 60_000),
                    "ttl.defaultTtlMs",
                    1,
                    2_147_483_647,
                ),
                minimum_ttl_ms=integer(
                    item.get("minimumTtlMs", 10),
                    "ttl.minimumTtlMs",
                    1,
                    2_147_483_647,
                ),
                maximum_ttl_ms=integer(
                    item.get("maximumTtlMs", 86_400_000),
                    "ttl.maximumTtlMs",
                    1,
                    2_147_483_647,
                ),
                negative_ttl_ms=integer(
                    item.get("negativeTtlMs", 5_000),
                    "ttl.negativeTtlMs",
                    1,
                    2_147_483_647,
                ),
            )
        except ValueError as exc:
            _fail(str(exc), exc)
        if not (
            result.minimum_ttl_ms <= result.default_ttl_ms <= result.maximum_ttl_ms
            and result.minimum_ttl_ms <= result.negative_ttl_ms <= result.maximum_ttl_ms
        ):
            _fail("TTL defaults must fall within the deployment TTL bounds")
        return result


@dataclass(frozen=True, slots=True)
class SingleFlightPolicy:
    lease_ms: int = 5_000
    wait_ms: int = 250
    poll_ms: int = 10

    @classmethod
    def from_mapping(cls, value: object) -> SingleFlightPolicy:
        item = _closed(
            value,
            "binding.settings.singleFlight",
            allowed=frozenset({"leaseMs", "waitMs", "pollMs"}),
        )
        try:
            result = cls(
                lease_ms=integer(item.get("leaseMs", 5_000), "singleFlight.leaseMs", 10, 600_000),
                wait_ms=integer(item.get("waitMs", 250), "singleFlight.waitMs", 0, 600_000),
                poll_ms=integer(item.get("pollMs", 10), "singleFlight.pollMs", 1, 60_000),
            )
        except ValueError as exc:
            _fail(str(exc), exc)
        if result.wait_ms > result.lease_ms:
            _fail("singleFlight.waitMs cannot exceed leaseMs")
        return result


@dataclass(frozen=True, slots=True)
class MemoryExpectation:
    require_maxmemory: bool = True
    allowed_eviction_policies: tuple[str, ...] = (
        "allkeys-lfu",
        "allkeys-lru",
        "allkeys-random",
        "volatile-lfu",
        "volatile-lru",
        "volatile-random",
        "volatile-ttl",
    )
    require_persistence_disabled: bool = True

    @classmethod
    def from_mapping(cls, value: object) -> MemoryExpectation:
        item = _closed(
            value,
            "binding.settings.memory",
            allowed=frozenset(
                {"requireMaxmemory", "allowedEvictionPolicies", "requirePersistenceDisabled"}
            ),
        )
        try:
            return cls(
                require_maxmemory=boolean(
                    item.get("requireMaxmemory", True), "memory.requireMaxmemory"
                ),
                allowed_eviction_policies=_strings(
                    item.get(
                        "allowedEvictionPolicies",
                        (
                            "allkeys-lfu",
                            "allkeys-lru",
                            "allkeys-random",
                            "volatile-lfu",
                            "volatile-lru",
                            "volatile-random",
                            "volatile-ttl",
                        ),
                    ),
                    "memory.allowedEvictionPolicies",
                    maximum=64,
                ),
                require_persistence_disabled=boolean(
                    item.get("requirePersistenceDisabled", True),
                    "memory.requirePersistenceDisabled",
                ),
            )
        except ValueError as exc:
            _fail(str(exc), exc)


@dataclass(frozen=True, slots=True)
class TopologyExpectation:
    mode: str = TOPOLOGY_STANDALONE
    minimum_replicas: int = 0
    sentinel_master: str | None = None

    @classmethod
    def from_mapping(cls, value: object) -> TopologyExpectation:
        item = _closed(
            value,
            "binding.settings.topology",
            allowed=frozenset({"mode", "minimumReplicas", "sentinelMaster"}),
        )
        try:
            mode = safe_token(item.get("mode", TOPOLOGY_STANDALONE), "topology.mode", maximum=32)
            replicas = integer(item.get("minimumReplicas", 0), "topology.minimumReplicas", 0, 100)
            raw_master = item.get("sentinelMaster")
            master = (
                None
                if raw_master is None
                else safe_token(raw_master, "topology.sentinelMaster", maximum=256)
            )
        except ValueError as exc:
            _fail(str(exc), exc)
        if mode not in {TOPOLOGY_STANDALONE, TOPOLOGY_SENTINEL}:
            _fail("topology.mode must be standalone or sentinel")
        if mode == TOPOLOGY_STANDALONE and (master is not None or replicas != 0):
            _fail("standalone topology requires zero replicas and no Sentinel master")
        if mode == TOPOLOGY_SENTINEL and (master is None or replicas < 1):
            _fail("sentinel topology requires sentinelMaster and at least one replica")
        return cls(mode, replicas, master)


@dataclass(frozen=True, slots=True)
class ResourceCachePolicy:
    schema_fingerprint: str
    serializer_id: str = SERIALIZER_CANONICAL_JSON
    default_ttl_ms: int | None = None
    maximum_staleness_ms: int | None = None
    negative_caching: bool = False

    @classmethod
    def from_mapping(cls, value: object, field_name: str) -> ResourceCachePolicy:
        item = _closed(
            value,
            field_name,
            allowed=frozenset(
                {
                    "schemaFingerprint",
                    "serializerId",
                    "defaultTtlMs",
                    "maximumStalenessMs",
                    "negativeCaching",
                    "authoritative",
                }
            ),
        )
        if item.get("authoritative", False) is not False:
            _fail(f"{field_name}.authoritative must be false")
        try:
            schema = require_fingerprint(item.get("schemaFingerprint"), "schemaFingerprint")
            serializer = safe_token(
                item.get("serializerId", SERIALIZER_CANONICAL_JSON),
                "serializerId",
                maximum=128,
            )
            if serializer not in SUPPORTED_SERIALIZERS:
                raise ValueError("serializerId is not supported by the Valkey V1 envelope")
            default = item.get("defaultTtlMs")
            staleness = item.get("maximumStalenessMs")
            return cls(
                schema_fingerprint=schema,
                serializer_id=serializer,
                default_ttl_ms=(
                    None if default is None else integer(default, "defaultTtlMs", 1, 2_147_483_647)
                ),
                maximum_staleness_ms=(
                    None
                    if staleness is None
                    else integer(staleness, "maximumStalenessMs", 1, 2_147_483_647)
                ),
                negative_caching=boolean(item.get("negativeCaching", False), "negativeCaching"),
            )
        except ValueError as exc:
            _fail(str(exc), exc)
        raise AssertionError("unreachable")


@dataclass(frozen=True, slots=True)
class ValkeySettings:
    namespace_generation: int
    resources: Mapping[str, ResourceCachePolicy]
    limits: AdapterLimits = field(default_factory=AdapterLimits)
    ttl: TTLPolicy = field(default_factory=TTLPolicy)
    single_flight: SingleFlightPolicy = field(default_factory=SingleFlightPolicy)
    memory: MemoryExpectation = field(default_factory=MemoryExpectation)
    topology: TopologyExpectation = field(default_factory=TopologyExpectation)
    database: int = 0
    seed_endpoints: tuple[str, ...] = ()
    allow_insecure_test_profile: bool = False

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> ValkeySettings:
        item = _closed(
            value,
            "binding.settings",
            allowed=frozenset(
                {
                    "namespaceGeneration",
                    "resources",
                    "limits",
                    "ttl",
                    "singleFlight",
                    "memory",
                    "topology",
                    "database",
                    "seedEndpoints",
                    "allowInsecureTestProfile",
                }
            ),
        )
        if "namespaceGeneration" not in item or "resources" not in item:
            _fail("binding.settings requires namespaceGeneration and resources")
        raw_resources = item["resources"]
        if not isinstance(raw_resources, Mapping) or any(
            not isinstance(key, str) for key in raw_resources
        ):
            _fail("binding.settings.resources must be an object")
        policies = {
            safe_token(key, "resource reference", maximum=768): ResourceCachePolicy.from_mapping(
                policy, f"binding.settings.resources[{key!r}]"
            )
            for key, policy in sorted(cast(Mapping[str, object], raw_resources).items())
        }
        if not policies:
            _fail("binding.settings.resources cannot be empty")
        raw_seeds = item.get("seedEndpoints", ())
        if not isinstance(raw_seeds, Sequence) or isinstance(raw_seeds, str | bytes | bytearray):
            _fail("binding.settings.seedEndpoints must be an array")
        try:
            seeds_list: list[str] = []
            for endpoint in raw_seeds:
                if (
                    not isinstance(endpoint, str)
                    or not endpoint
                    or len(endpoint.encode("utf-8")) > 2048
                ):
                    raise ValueError("invalid seed endpoint")
                seeds_list.append(endpoint)
            seeds = tuple(seeds_list)
            result = cls(
                namespace_generation=integer(
                    item["namespaceGeneration"], "namespaceGeneration", 1, 2_147_483_647
                ),
                resources=MappingProxyType(policies),
                limits=AdapterLimits.from_mapping(item.get("limits", {})),
                ttl=TTLPolicy.from_mapping(item.get("ttl", {})),
                single_flight=SingleFlightPolicy.from_mapping(item.get("singleFlight", {})),
                memory=MemoryExpectation.from_mapping(item.get("memory", {})),
                topology=TopologyExpectation.from_mapping(item.get("topology", {})),
                database=integer(item.get("database", 0), "database", 0, 15),
                seed_endpoints=seeds,
                allow_insecure_test_profile=boolean(
                    item.get("allowInsecureTestProfile", False),
                    "allowInsecureTestProfile",
                ),
            )
        except ValueError as exc:
            _fail(str(exc), exc)
        return result

    def policy_for(self, resource_ref: str) -> ResourceCachePolicy:
        policy = self.resources.get(resource_ref)
        if policy is None:
            raise ConfigurationError(
                ErrorCode.PLACEMENT_UNRESOLVED,
                "Binding has no disposable Cache policy for the requested Resource",
                resource_ref=resource_ref,
            )
        return policy

    def effective_ttl_ms(
        self,
        policy: ResourceCachePolicy,
        requested_ttl_ms: int | None,
        *,
        negative: bool = False,
    ) -> int:
        selected = (
            self.ttl.negative_ttl_ms
            if negative
            else requested_ttl_ms
            if requested_ttl_ms is not None
            else policy.default_ttl_ms
            if policy.default_ttl_ms is not None
            else self.ttl.default_ttl_ms
        )
        if isinstance(selected, bool) or not isinstance(selected, int) or selected <= 0:
            raise ConfigurationError(ErrorCode.CONFIG_INVALID, "Cache TTL must be positive")
        upper_bounds = [self.ttl.maximum_ttl_ms]
        if policy.maximum_staleness_ms is not None:
            upper_bounds.append(policy.maximum_staleness_ms)
        result = min(selected, *upper_bounds)
        if result < self.ttl.minimum_ttl_ms:
            raise ConfigurationError(
                ErrorCode.CONFIG_INVALID,
                "effective Cache TTL falls below the deployment minimum",
            )
        return result


__all__ = [
    "SERIALIZER_CANONICAL_JSON",
    "SERIALIZER_RAW_BYTES",
    "SUPPORTED_SERIALIZERS",
    "TOPOLOGY_SENTINEL",
    "TOPOLOGY_STANDALONE",
    "AdapterLimits",
    "MemoryExpectation",
    "ResourceCachePolicy",
    "SingleFlightPolicy",
    "TTLPolicy",
    "TopologyExpectation",
    "ValkeySettings",
]
