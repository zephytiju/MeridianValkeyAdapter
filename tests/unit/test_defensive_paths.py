# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pytest
from meridian_storage.errors import ValidationError
from meridian_storage.registry import ResourceRef
from meridian_storage.semantics import JsonValue
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from meridian_storage.adapters.valkey.cache import CacheLookup, ValkeyCache
from meridian_storage.adapters.valkey.codec import EnvelopeCodec
from meridian_storage.adapters.valkey.configuration import ValkeySettings
from meridian_storage.adapters.valkey.key import KeyEncoder
from meridian_storage.adapters.valkey.runtime import (
    _entry,
    _invalidation_keys,
    _optional_int,
    _required_version,
    _source_version,
)
from tests.conftest import RESOURCE, operation_context, settings_mapping
from tests.fakes import FakeValkey

RESOURCE_REF = ResourceRef.parse(RESOURCE)


def _cache(client: FakeValkey, *, negative: bool = True) -> ValkeyCache:
    raw = settings_mapping()
    resources = cast(dict[str, dict[str, JsonValue]], raw["resources"])
    resources[RESOURCE]["negativeCaching"] = negative
    settings = ValkeySettings.from_mapping(cast(Mapping[str, object], raw))
    return ValkeyCache(
        client,
        KeyEncoder("defensive", 1, maximum_key_bytes=512),
        EnvelopeCodec(maximum_value_bytes=1048576, clock_ms=lambda: client.now_ms),
        settings,
    )


def test_lookup_treats_engine_type_and_stale_envelope_as_misses() -> None:
    client = FakeValkey()
    cache = _cache(client)
    key, _ = cache.physical_key(operation_context(), RESOURCE_REF, "wrong-type")
    client.set(key, "not-bytes", px=1000)
    assert not cache.lookup(operation_context(), RESOURCE_REF, "wrong-type").hit

    cache.put(operation_context(), RESOURCE_REF, "stale", 1)
    stale_key, _ = cache.physical_key(operation_context(), RESOURCE_REF, "stale")
    client.values[stale_key].expires_at_ms = None
    client.advance(2001)
    assert not cache.lookup(operation_context(), RESOURCE_REF, "stale").hit
    assert cache.metrics.snapshot()["stale"] == 1


def test_batch_lookup_covers_hit_type_corruption_and_staleness() -> None:
    client = FakeValkey()
    cache = _cache(client)
    cache.put(operation_context(), RESOURCE_REF, "hit", 1)
    type_key, _ = cache.physical_key(operation_context(), RESOURCE_REF, "type")
    corrupt_key, _ = cache.physical_key(operation_context(), RESOURCE_REF, "corrupt")
    stale_key, policy = cache.physical_key(operation_context(), RESOURCE_REF, "stale")
    client.set(type_key, 99, px=1000)
    client.set(corrupt_key, b"broken", px=1000)
    stale = cache.codec.encode(
        1,
        serializer_id=policy.serializer_id,
        schema_fingerprint=policy.schema_fingerprint,
        ttl_ms=10,
    )
    client.set(stale_key, stale, px=1000)
    client.advance(11)
    results = cache.batch_lookup(
        operation_context(), RESOURCE_REF, ["hit", "type", "corrupt", "stale"]
    )
    assert [item.hit for item in results] == [True, False, False, False]
    with pytest.raises(ValueError, match="batch"):
        cache.batch_lookup(operation_context(), RESOURCE_REF, [])


def test_compare_and_set_discards_nonbytes_and_malformed_envelopes() -> None:
    client = FakeValkey()
    cache = _cache(client)
    key, _ = cache.physical_key(operation_context(), RESOURCE_REF, "value")
    client.set(key, 1, px=1000)
    result, envelope = cache.compare_and_set(
        operation_context(), RESOURCE_REF, "value", "version", 2
    )
    assert result.corrupt and envelope is None
    client.set(key, b"bad", px=1000)
    result, envelope = cache.compare_and_set(
        operation_context(), RESOURCE_REF, "value", "version", 2
    )
    assert result.corrupt and envelope is None


def test_single_flight_handles_lease_population_and_release_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeValkey()
    cache = _cache(client)

    def unavailable(*_args: object, **_kwargs: object) -> bool:
        raise ValkeyConnectionError("unavailable")

    monkeypatch.setattr(cache.atomic, "acquire_lease", unavailable)
    result = cache.get_or_load(operation_context(), RESOURCE_REF, "lease", lambda: 1)
    assert result.degraded and result.loaded

    cache = _cache(client)
    monkeypatch.setattr(cache, "put", unavailable)
    result = cache.get_or_load(operation_context(), RESOURCE_REF, "population", lambda: 2)
    assert result.degraded and result.value == 2

    cache = _cache(client)
    monkeypatch.setattr(cache.atomic, "release_lease", unavailable)
    result = cache.get_or_load(operation_context(), RESOURCE_REF, "release", lambda: 3)
    assert result.loaded
    assert cache.metrics.snapshot()["lease_release_failure"] == 1


def test_single_flight_contention_peer_and_duplicate_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeValkey()
    cache = _cache(client, negative=False)
    monkeypatch.setattr(cache.atomic, "acquire_lease", lambda *_args: False)
    calls = iter([CacheLookup(False), CacheLookup(True, 9)])
    monkeypatch.setattr(cache, "lookup", lambda *_args: next(calls))
    peer = cache.get_or_load(
        operation_context(),
        RESOURCE_REF,
        "peer",
        lambda: 1,
        sleep=lambda _: None,
        monotonic=iter([0.0, 0.0]).__next__,
    )
    assert peer.hit and peer.value == 9

    cache = _cache(client, negative=False)
    monkeypatch.setattr(cache.atomic, "acquire_lease", lambda *_args: False)
    times = iter([0.0, 1.0])
    duplicate = cache.get_or_load(
        operation_context(),
        RESOURCE_REF,
        "duplicate",
        lambda: None,
        sleep=lambda _: None,
        monotonic=times.__next__,
    )
    assert duplicate.loaded and duplicate.value is None


def test_runtime_helpers_validate_and_encode_raw_entries() -> None:
    codec = EnvelopeCodec(maximum_value_bytes=4096, clock_ms=lambda: 1000)
    raw = codec.encode(
        b"\x00\xff",
        serializer_id="raw-bytes",
        schema_fingerprint="sha256:" + "1" * 64,
        ttl_ms=100,
    )
    entry = _entry(codec.decode(raw, expected_schema_fingerprint="sha256:" + "1" * 64, now_ms=1001))
    assert entry["value"] == "AP8"
    assert entry["valueEncoding"] == "base64url"
    with pytest.raises(ValidationError, match="absent"):
        _entry(None)

    assert _optional_int(None, "ttl") is None
    assert _optional_int(1, "ttl") == 1
    assert _source_version("v") == "v"
    assert _required_version(1) == 1
    for value in (False, 0, "bad"):
        with pytest.raises(ValidationError):
            _optional_int(value, "ttl")
    with pytest.raises(ValidationError):
        _source_version(False)
    with pytest.raises(ValidationError):
        _required_version(None)
    assert _invalidation_keys({"key": "a"}, 4) == ("a",)
    for selector in (None, {"keys": "a"}, {"keys": []}, {"keys": [1, 2, 3, 4, 5]}):
        with pytest.raises(ValidationError):
            _invalidation_keys(selector, 4)
