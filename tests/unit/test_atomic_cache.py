# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pytest
from meridian_storage.registry import ResourceRef
from meridian_storage.semantics import JsonValue
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from meridian_storage.adapters.valkey.atomic import AtomicExecutor
from meridian_storage.adapters.valkey.cache import ValkeyCache
from meridian_storage.adapters.valkey.codec import EnvelopeCodec
from meridian_storage.adapters.valkey.key import KeyEncoder
from tests.conftest import RESOURCE, operation_context, settings_mapping
from tests.fakes import FakeValkey, RecordingSink

RESOURCE_REF = ResourceRef.parse(RESOURCE)


def _cache(client: FakeValkey, sink: RecordingSink | None = None) -> ValkeyCache:
    from meridian_storage.adapters.valkey.configuration import ValkeySettings

    settings = ValkeySettings.from_mapping(cast(Mapping[str, object], settings_mapping()))
    return ValkeyCache(
        client,
        KeyEncoder("private-namespace", 1, maximum_key_bytes=512),
        EnvelopeCodec(maximum_value_bytes=1048576, clock_ms=lambda: client.now_ms),
        settings,
        event_sink=sink,
    )


def test_put_lookup_ttl_corruption_and_safe_telemetry(fake_client: FakeValkey) -> None:
    sink = RecordingSink()
    cache = _cache(fake_client, sink)
    envelope = cache.put(
        operation_context(), RESOURCE_REF, "logical-secret", {"answer": 42}, source_version=3
    )
    lookup = cache.lookup(operation_context(), RESOURCE_REF, "logical-secret")
    assert lookup.hit and lookup.value == {"answer": 42}
    key, _ = cache.physical_key(operation_context(), RESOURCE_REF, "logical-secret")
    assert fake_client.pttl(key) == 1000
    assert b"logical-secret" not in key
    assert envelope.source_version == 3
    assert all("resource" not in attrs for _, attrs in sink.events)
    assert all("logical-secret" not in repr(item) for item in sink.events)

    fake_client.values[key].value = b"corrupt"
    assert cache.lookup(operation_context(), RESOURCE_REF, "logical-secret").hit is False
    assert fake_client.get(key) is None
    assert cache.metrics.snapshot()["corruption"] == 1


def test_expiry_negative_cache_and_generation_invalidation(fake_client: FakeValkey) -> None:
    cache = _cache(fake_client)
    negative = cache.put(operation_context(), RESOURCE_REF, "missing", None)
    key, _ = cache.physical_key(operation_context(), RESOURCE_REF, "missing")
    assert negative.negative
    assert not cache.lookup(operation_context(), RESOURCE_REF, "absent").negative
    cached_negative = cache.lookup(operation_context(), RESOURCE_REF, "missing")
    assert cached_negative.hit and cached_negative.negative
    assert fake_client.pttl(key) == 100
    fake_client.advance(101)
    assert not cache.lookup(operation_context(), RESOURCE_REF, "missing").hit

    generation_two = settings_mapping(generation=2)
    from meridian_storage.adapters.valkey.configuration import ValkeySettings

    another = ValkeyCache(
        fake_client,
        KeyEncoder("private-namespace", 2, maximum_key_bytes=512),
        EnvelopeCodec(maximum_value_bytes=1048576, clock_ms=lambda: fake_client.now_ms),
        ValkeySettings.from_mapping(cast(Mapping[str, object], generation_two)),
    )
    cache.put(operation_context(), RESOURCE_REF, "warm", 1)
    assert not another.lookup(operation_context(), RESOURCE_REF, "warm").hit


def test_batch_put_if_absent_and_compare_and_set(fake_client: FakeValkey) -> None:
    cache = _cache(fake_client)
    assert [
        item.hit for item in cache.batch_lookup(operation_context(), RESOURCE_REF, ["a", "b"])
    ] == [False, False]
    with pytest.raises(ValueError, match="batch"):
        cache.batch_lookup(operation_context(), RESOURCE_REF, [1, 2, 3, 4, 5])

    stored, first = cache.put_if_absent(operation_context(), RESOURCE_REF, "a", 1)
    assert stored
    stored_again, _ = cache.put_if_absent(operation_context(), RESOURCE_REF, "a", 2)
    assert not stored_again
    swapped, replacement = cache.compare_and_set(
        operation_context(), RESOURCE_REF, "a", first.envelope_version, 3
    )
    assert swapped.swapped and replacement is not None and replacement.value == 3
    conflict, current = cache.compare_and_set(operation_context(), RESOURCE_REF, "a", "wrong", 4)
    assert not conflict.swapped and current is not None and current.value == 3
    missing, _ = cache.compare_and_set(operation_context(), RESOURCE_REF, "absent", "v", 1)
    assert missing.missing


def test_single_flight_loads_outside_atomic_and_degrades_safely(fake_client: FakeValkey) -> None:
    cache = _cache(fake_client)
    loaded: list[str] = []

    def loader() -> JsonValue:
        loaded.append("loaded")
        return {"from": "authority"}

    result = cache.get_or_load(operation_context(), RESOURCE_REF, "a", loader)
    assert result.loaded and result.value == {"from": "authority"}
    assert loaded == ["loaded"]
    hit = cache.get_or_load(operation_context(), RESOURCE_REF, "a", loader)
    assert hit.hit and loaded == ["loaded"]

    fake_client.raise_connection = True
    degraded = cache.get_or_load(operation_context(), RESOURCE_REF, "b", loader)
    assert degraded.degraded and degraded.loaded
    assert loaded == ["loaded", "loaded"]


def test_atomic_increment_retries_and_validates_schema(fake_client: FakeValkey) -> None:
    cache = _cache(fake_client)
    cache.put(operation_context(), RESOURCE_REF, "counter", 2)
    key, policy = cache.physical_key(operation_context(), RESOURCE_REF, "counter")
    fake_client.force_watch_conflict = True
    result = cache.atomic.increment(
        key, amount=0.5, schema_fingerprint=policy.schema_fingerprint, ttl_ms=1000
    )
    assert result.value == 2.5
    with pytest.raises(ValueError, match="finite"):
        cache.atomic.increment(
            key, amount=float("inf"), schema_fingerprint=policy.schema_fingerprint, ttl_ms=1000
        )


def test_lease_release_uses_exact_token(fake_client: FakeValkey) -> None:
    executor = AtomicExecutor(fake_client, EnvelopeCodec(maximum_value_bytes=1024))
    assert executor.acquire_lease(b"lease", b"owner", 100)
    assert not executor.release_lease(b"lease", b"other")
    assert executor.release_lease(b"lease", b"owner")
    assert fake_client.get(b"lease") is None


def test_cleanup_failure_does_not_expose_corruption(fake_client: FakeValkey) -> None:
    cache = _cache(fake_client)
    cache.put(operation_context(), RESOURCE_REF, "a", 1)
    key, _ = cache.physical_key(operation_context(), RESOURCE_REF, "a")
    fake_client.values[key].value = b"bad"
    original_delete = fake_client.delete

    def unavailable(*_keys: bytes) -> int:
        raise ValkeyConnectionError("secret")

    fake_client.delete = unavailable  # type: ignore[method-assign]
    assert not cache.lookup(operation_context(), RESOURCE_REF, "a").hit
    assert cache.metrics.snapshot()["cleanup_failure"] == 1
    fake_client.delete = original_delete  # type: ignore[method-assign]
