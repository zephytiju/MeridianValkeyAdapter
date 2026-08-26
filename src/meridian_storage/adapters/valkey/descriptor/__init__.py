# SPDX-License-Identifier: Apache-2.0
"""Immutable Meridian V1 capability declarations for Valkey."""

from __future__ import annotations

from meridian_storage.spi import AdapterDescriptor, CapabilityManifest, OperationCapability

from ..atomic import CAS_SCRIPT_DIGEST, RELEASE_LEASE_SCRIPT_DIGEST
from ..configuration import (
    SERIALIZER_CANONICAL_JSON,
    SERIALIZER_RAW_BYTES,
    TOPOLOGY_SENTINEL,
    ValkeySettings,
)

ADAPTER_ID = "org.meridian.storage.valkey"
ADAPTER_CONTRACT_VERSION = "1.0.0"
ENGINE_PROFILE_STANDALONE = "valkey-standalone"
ENGINE_PROFILE_SENTINEL = "valkey-sentinel"
SUPPORTED_ENGINE_VERSION = "8.1.9"
SUPPORTED_ENGINE_VERSIONS = (SUPPORTED_ENGINE_VERSION,)
OPERATION_VERSION = "1.0.0"

GET_CONTRACT = "meridian.cache.get"
PUT_CONTRACT = "meridian.cache.put"
PUT_IF_ABSENT_CONTRACT = "meridian.cache.put_if_absent"
COMPARE_AND_SET_CONTRACT = "meridian.cache.compare_and_set"
DELETE_CONTRACT = "meridian.cache.delete"
INVALIDATE_CONTRACT = "meridian.cache.invalidate"
DATA_PLANE_CONTRACTS = (
    COMPARE_AND_SET_CONTRACT,
    DELETE_CONTRACT,
    GET_CONTRACT,
    INVALIDATE_CONTRACT,
    PUT_CONTRACT,
    PUT_IF_ABSENT_CONTRACT,
)


def _limits(settings: ValkeySettings) -> dict[str, int]:
    return {
        "batchSize": settings.limits.max_batch_size,
        "keyBytes": settings.limits.max_key_bytes,
        "maximumTtlMs": settings.ttl.maximum_ttl_ms,
        "valueBytes": settings.limits.max_value_bytes,
    }


def adapter_descriptor(settings: ValkeySettings) -> AdapterDescriptor:
    shared_extensions = {
        "consistencyClass": "disposable-cache",
        "serializers": [SERIALIZER_CANONICAL_JSON, SERIALIZER_RAW_BYTES],
        "ttlRequired": True,
    }
    common = {
        "operation_versions": (OPERATION_VERSION,),
        "limits": _limits(settings),
        "cursor_behavior": "none",
        "migration_behavior": "external-namespace-generation",
        "health_probes": (
            "authenticated",
            "engine-version",
            "eviction-policy",
            "memory-limit",
            "persistence-policy",
            "script-support",
            "topology",
        ),
    }
    return AdapterDescriptor(
        adapter_id=ADAPTER_ID,
        adapter_contract_version=ADAPTER_CONTRACT_VERSION,
        driver="valkey-py/6.1",
        supported_engine_versions={
            ENGINE_PROFILE_STANDALONE: SUPPORTED_ENGINE_VERSIONS,
            ENGINE_PROFILE_SENTINEL: SUPPORTED_ENGINE_VERSIONS,
        },
        capabilities=(
            OperationCapability(
                operation_contract=GET_CONTRACT,
                guarantees=(
                    "disposable-cache",
                    "miss-on-unavailable",
                    "schema-validated",
                    "scope-isolation",
                    "ttl-bounded",
                ),
                extensions={**shared_extensions, "corruptionMode": "delete-and-miss"},
                **common,
            ),
            OperationCapability(
                operation_contract=PUT_CONTRACT,
                guarantees=("disposable-cache", "scope-isolation", "ttl-bounded"),
                extensions=shared_extensions,
                **common,
            ),
            OperationCapability(
                operation_contract=PUT_IF_ABSENT_CONTRACT,
                guarantees=(
                    "atomic-single-key",
                    "disposable-cache",
                    "scope-isolation",
                    "ttl-bounded",
                ),
                extensions=shared_extensions,
                **common,
            ),
            OperationCapability(
                operation_contract=COMPARE_AND_SET_CONTRACT,
                guarantees=(
                    "atomic-single-key",
                    "disposable-cache",
                    "scope-isolation",
                    "ttl-bounded",
                ),
                extensions={**shared_extensions, "scriptDigest": CAS_SCRIPT_DIGEST},
                **common,
            ),
            OperationCapability(
                operation_contract=DELETE_CONTRACT,
                guarantees=("disposable-cache", "scope-isolation"),
                extensions=shared_extensions,
                **common,
            ),
            OperationCapability(
                operation_contract=INVALIDATE_CONTRACT,
                guarantees=("bounded-exact-keys", "disposable-cache", "scope-isolation"),
                extensions={
                    **shared_extensions,
                    "namespaceInvalidation": "deployment-generation-only",
                },
                **common,
            ),
        ),
    )


def capability_manifest(engine_version: str, settings: ValkeySettings) -> CapabilityManifest:
    profile = (
        ENGINE_PROFILE_SENTINEL
        if settings.topology.mode == TOPOLOGY_SENTINEL
        else ENGINE_PROFILE_STANDALONE
    )
    return CapabilityManifest(
        descriptor=adapter_descriptor(settings),
        engine_profile=profile,
        engine_version=engine_version,
        available_operation_contracts=DATA_PLANE_CONTRACTS,
        extensions={
            "consistencyClass": "disposable-cache",
            "degradationModes": [
                "corrupt-envelope-delete-and-miss",
                "eviction-miss",
                "read-unavailable-miss",
            ],
            "namespaceGeneration": settings.namespace_generation,
            "releaseLeaseScriptDigest": RELEASE_LEASE_SCRIPT_DIGEST,
            "topology": settings.topology.mode,
        },
    )


def expected_capability_fingerprint(engine_version: str, settings: ValkeySettings) -> str:
    return str(capability_manifest(engine_version, settings).fingerprint)


__all__ = [
    "ADAPTER_CONTRACT_VERSION",
    "ADAPTER_ID",
    "COMPARE_AND_SET_CONTRACT",
    "DATA_PLANE_CONTRACTS",
    "DELETE_CONTRACT",
    "ENGINE_PROFILE_SENTINEL",
    "ENGINE_PROFILE_STANDALONE",
    "GET_CONTRACT",
    "INVALIDATE_CONTRACT",
    "OPERATION_VERSION",
    "PUT_CONTRACT",
    "PUT_IF_ABSENT_CONTRACT",
    "SUPPORTED_ENGINE_VERSION",
    "SUPPORTED_ENGINE_VERSIONS",
    "adapter_descriptor",
    "capability_manifest",
    "expected_capability_fingerprint",
]
