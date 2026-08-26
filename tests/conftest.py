# SPDX-License-Identifier: Apache-2.0
"""Shared released-contract fixtures."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import cast

import pytest
from meridian_storage.context import OperationContext
from meridian_storage.runtime.config import BindingConfig
from meridian_storage.semantics import JsonValue
from meridian_storage.spi import AdapterCreateContext, ExecutionRequest, SecretValue

from meridian_storage.adapters.valkey.configuration import ValkeySettings
from meridian_storage.adapters.valkey.descriptor import (
    ADAPTER_ID,
    ENGINE_PROFILE_SENTINEL,
    ENGINE_PROFILE_STANDALONE,
    SUPPORTED_ENGINE_VERSION,
    expected_capability_fingerprint,
)
from tests.fakes import FakeValkey

SCHEMA = "sha256:" + "1" * 64
REGISTRY = "sha256:" + "2" * 64
RESOURCE = "cache:demo.items"


def settings_mapping(*, sentinel: bool = False, generation: int = 1) -> dict[str, JsonValue]:
    topology: dict[str, JsonValue]
    if sentinel:
        topology = {
            "mode": "sentinel",
            "minimumReplicas": 2,
            "sentinelMaster": "meridian-cache",
        }
    else:
        topology = {"mode": "standalone", "minimumReplicas": 0}
    return {
        "namespaceGeneration": generation,
        "resources": {
            RESOURCE: {
                "schemaFingerprint": SCHEMA,
                "serializerId": "canonical-json",
                "defaultTtlMs": 1000,
                "maximumStalenessMs": 2000,
                "negativeCaching": True,
                "authoritative": False,
            }
        },
        "limits": {"maxKeyBytes": 512, "maxValueBytes": 1048576, "maxBatchSize": 4},
        "ttl": {
            "defaultTtlMs": 1000,
            "minimumTtlMs": 10,
            "maximumTtlMs": 10000,
            "negativeTtlMs": 100,
        },
        "singleFlight": {"leaseMs": 100, "waitMs": 20, "pollMs": 5},
        "memory": {
            "requireMaxmemory": True,
            "allowedEvictionPolicies": ["allkeys-lru"],
            "requirePersistenceDisabled": True,
        },
        "topology": topology,
        "database": 0,
        "seedEndpoints": [],
        "allowInsecureTestProfile": True,
    }


def make_context(
    client: FakeValkey,
    *,
    sentinel: bool = False,
    settings_override: Mapping[str, JsonValue] | None = None,
    endpoint: str | None = None,
) -> AdapterCreateContext:
    raw = dict(settings_override or settings_mapping(sentinel=sentinel))
    settings = ValkeySettings.from_mapping(cast(Mapping[str, object], raw))
    profile = ENGINE_PROFILE_SENTINEL if sentinel else ENGINE_PROFILE_STANDALONE
    binding = BindingConfig.from_mapping(
        {
            "id": "cache-binding",
            "adapterId": ADAPTER_ID,
            "adapterContract": "1.0.0",
            "engineProfile": profile,
            "engineVersion": SUPPORTED_ENGINE_VERSION,
            "endpoint": endpoint or "valkey://127.0.0.1:6379",
            "serviceRef": None,
            "physicalNamespace": "test-only-physical-namespace",
            "tls": {
                "mode": "disabled",
                "serverName": None,
                "caRef": None,
                "clientCertificateRef": None,
            },
            "identityRef": {"provider": "test", "reference": "valkey-username"},
            "secretRef": {"provider": "test", "reference": "valkey-password"},
            "client": {
                "minSize": 0,
                "maxSize": 16,
                "acquireTimeoutMs": 1000,
                "idleTimeoutMs": 1000,
                "operationTimeoutMs": 1000,
                "maxResultBytes": 1048576,
                "iteratorLifetimeMs": 1000,
            },
            "requiredCapabilityFingerprint": expected_capability_fingerprint(
                SUPPORTED_ENGINE_VERSION, settings
            ),
            "requiredPhysicalFingerprint": None,
            "compatibilityPins": {
                "adapterContract": "1.0.0",
                "coreVersion": "1.0.0",
                "driver": "valkey-py/6.1",
                "engineProfile": profile,
                "engineVersion": SUPPORTED_ENGINE_VERSION,
            },
            "settings": raw,
            "extensions": {},
        },
        "binding",
    )
    return AdapterCreateContext(
        binding,
        SecretValue(b"default"),
        SecretValue(b"meridian-test-password"),
    )


def fake_builder(client: FakeValkey) -> Callable[..., FakeValkey]:
    def build(*_args: object, **_kwargs: object) -> FakeValkey:
        return client

    return build


def operation_context() -> OperationContext:
    return OperationContext(
        principal_ref="principal:test",
        request_id="request-1",
        tenant="tenant-private",
        scope={"region": "test-region"},
    )


def execution_request(operation: object) -> ExecutionRequest:
    return ExecutionRequest(
        operation=operation,  # type: ignore[arg-type]
        context=operation_context(),
        request_id="request-1",
        execution_id="execution-1",
        binding_id="cache-binding",
        registry_revision=1,
        registry_fingerprint=REGISTRY,
        attempt=1,
    )


@pytest.fixture
def fake_client() -> FakeValkey:
    return FakeValkey()


@pytest.fixture
def settings() -> ValkeySettings:
    return ValkeySettings.from_mapping(cast(Mapping[str, object], settings_mapping()))


@pytest.fixture
def context() -> OperationContext:
    return operation_context()
