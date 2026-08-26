# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pytest
from meridian_storage.errors import ConfigurationError
from meridian_storage.semantics import JsonValue

from meridian_storage.adapters.valkey.client import parse_endpoint
from meridian_storage.adapters.valkey.configuration import (
    ResourceCachePolicy,
    ValkeySettings,
)
from tests.conftest import RESOURCE, SCHEMA, settings_mapping


def _settings(raw: Mapping[str, JsonValue]) -> ValkeySettings:
    return ValkeySettings.from_mapping(cast(Mapping[str, object], raw))


def test_closed_settings_parse_all_v1_fields() -> None:
    settings = _settings(settings_mapping())
    assert settings.namespace_generation == 1
    assert settings.policy_for(RESOURCE).schema_fingerprint == SCHEMA
    assert settings.limits.max_batch_size == 4
    assert settings.effective_ttl_ms(settings.policy_for(RESOURCE), None) == 1000
    assert settings.effective_ttl_ms(settings.policy_for(RESOURCE), None, negative=True) == 100


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda raw: raw.update({"unknown": True}), "unknown fields"),
        (
            lambda raw: cast(dict[str, object], raw["resources"])[RESOURCE].update(  # type: ignore[union-attr]
                {"authoritative": True}
            ),
            "authoritative must be false",
        ),
        (lambda raw: raw.update({"namespaceGeneration": 0}), "namespaceGeneration"),
        (lambda raw: raw.update({"resources": {}}), "cannot be empty"),
    ],
)
def test_settings_fail_closed(mutation: object, message: str) -> None:
    raw = settings_mapping()
    mutation(raw)  # type: ignore[operator]
    with pytest.raises(ConfigurationError, match=message):
        _settings(raw)


def test_resource_serializer_and_ttl_bounds_are_enforced() -> None:
    raw = settings_mapping()
    resources = cast(dict[str, dict[str, JsonValue]], raw["resources"])
    resources[RESOURCE]["serializerId"] = "pickle"
    with pytest.raises(ConfigurationError, match="serializer"):
        _settings(raw)

    policy = ResourceCachePolicy(SCHEMA, default_ttl_ms=1)
    settings = _settings(settings_mapping())
    with pytest.raises(ConfigurationError, match="below"):
        settings.effective_ttl_ms(policy, None)
    assert settings.effective_ttl_ms(policy, 20_000) == 10_000


def test_topology_settings_are_coherent() -> None:
    standalone = settings_mapping()
    cast(dict[str, JsonValue], standalone["topology"])["minimumReplicas"] = 1
    with pytest.raises(ConfigurationError, match="standalone"):
        _settings(standalone)

    sentinel = settings_mapping(sentinel=True)
    parsed = _settings(sentinel)
    assert parsed.topology.sentinel_master == "meridian-cache"
    assert parsed.topology.minimum_replicas == 2


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("valkey://cache.internal", ("cache.internal", 6379, False)),
        ("valkeys://cache.internal:6380", ("cache.internal", 6380, True)),
    ],
)
def test_endpoint_parser(value: str, expected: tuple[str, int, bool]) -> None:
    parsed = parse_endpoint(value)
    assert (parsed.host, parsed.port, parsed.tls) == expected


@pytest.mark.parametrize(
    "value",
    [
        "https://cache.internal",
        "valkey://user:password@cache.internal",
        "valkey://cache.internal/1",
        "valkey://cache.internal?secret=yes",
    ],
)
def test_endpoint_parser_rejects_unsafe_forms(value: str) -> None:
    with pytest.raises(ConfigurationError, match="endpoint is invalid"):
        parse_endpoint(value)
