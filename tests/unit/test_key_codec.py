# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from typing import cast

import pytest
from hypothesis import given
from hypothesis import strategies as st
from meridian_storage.registry import ResourceRef
from meridian_storage.semantics import JsonValue, canonical_json_bytes

from meridian_storage.adapters.valkey._canonical import sha256_hex
from meridian_storage.adapters.valkey.codec import (
    ENVELOPE_FORMAT_VERSION,
    EnvelopeCodec,
    EnvelopeCorruptionError,
    EnvelopeError,
    EnvelopeExpiredError,
    EnvelopeSchemaMismatchError,
    EnvelopeTooLargeError,
)
from meridian_storage.adapters.valkey.key import (
    KeyEncoder,
    KeyMaterial,
    hash_slot,
    require_shared_hash_slot,
)
from tests.conftest import SCHEMA, operation_context

RESOURCE_REF = ResourceRef.parse("cache:demo.items")


def test_physical_key_is_deterministic_scoped_and_private() -> None:
    encoder = KeyEncoder("private-namespace", 7, maximum_key_bytes=512)
    material = KeyMaterial(operation_context(), RESOURCE_REF, ["customer-123", 9], SCHEMA)
    first = encoder.encode(material)
    second = encoder.encode(material)
    assert first == second
    assert b"customer" not in first
    assert b"tenant-private" not in first
    assert b"demo.items" not in first

    another_generation = KeyEncoder("private-namespace", 8, maximum_key_bytes=512)
    assert another_generation.encode(material) != first


def test_atomic_group_controls_hash_slot_without_exposing_group() -> None:
    encoder = KeyEncoder("namespace", 1, maximum_key_bytes=512)
    left = encoder.encode(KeyMaterial(operation_context(), RESOURCE_REF, "a", SCHEMA, "group"))
    right = encoder.encode(KeyMaterial(operation_context(), RESOURCE_REF, "b", SCHEMA, "group"))
    other = encoder.encode(KeyMaterial(operation_context(), RESOURCE_REF, "b", SCHEMA, "other"))
    assert hash_slot(left) == hash_slot(right)
    assert require_shared_hash_slot((left, right)) == hash_slot(left)
    assert b"group" not in left
    with pytest.raises(ValueError, match="hash slot"):
        require_shared_hash_slot((left, other))
    with pytest.raises(ValueError, match="at least one"):
        require_shared_hash_slot(())


@given(st.one_of(st.text(max_size=100), st.integers(), st.booleans(), st.lists(st.integers())))
def test_key_encoding_is_deterministic_for_json_keys(logical_key: object) -> None:
    encoder = KeyEncoder("namespace", 1, maximum_key_bytes=512)
    material = KeyMaterial(operation_context(), RESOURCE_REF, logical_key, SCHEMA)  # type: ignore[arg-type]
    assert encoder.encode(material) == encoder.encode(material)


def test_canonical_json_envelope_round_trip_and_versions() -> None:
    codec = EnvelopeCodec(maximum_value_bytes=4096, clock_ms=lambda: 1000)
    encoded = codec.encode(
        {"b": 2, "a": [True, None]},
        serializer_id="canonical-json",
        schema_fingerprint=SCHEMA,
        ttl_ms=500,
        source_version="authority-v3",
    )
    decoded = codec.decode(encoded, expected_schema_fingerprint=SCHEMA, now_ms=1200)
    assert decoded.value == {"a": [True, None], "b": 2}
    assert decoded.source_version == "authority-v3"
    assert decoded.expires_at_ms == 1500
    assert decoded.envelope_version == codec.extract_version(encoded)


def test_raw_bytes_round_trip_and_validation_failures() -> None:
    codec = EnvelopeCodec(maximum_value_bytes=2048, clock_ms=lambda: 1000)
    encoded = codec.encode(
        b"\x00private-bytes",
        serializer_id="raw-bytes",
        schema_fingerprint=SCHEMA,
        ttl_ms=100,
    )
    assert (
        codec.decode(encoded, expected_schema_fingerprint=SCHEMA, now_ms=1001).value
        == b"\x00private-bytes"
    )
    with pytest.raises(EnvelopeError, match="byte payload"):
        codec.encode({}, serializer_id="raw-bytes", schema_fingerprint=SCHEMA, ttl_ms=100)
    with pytest.raises(EnvelopeError, match="cannot encode"):
        codec.encode(b"x", serializer_id="canonical-json", schema_fingerprint=SCHEMA, ttl_ms=100)


def test_envelope_rejects_expired_stale_schema_and_corruption() -> None:
    codec = EnvelopeCodec(maximum_value_bytes=4096, clock_ms=lambda: 1000)
    encoded = codec.encode(
        {"ok": True},
        serializer_id="canonical-json",
        schema_fingerprint=SCHEMA,
        ttl_ms=100,
    )
    with pytest.raises(EnvelopeExpiredError, match="expired"):
        codec.decode(encoded, expected_schema_fingerprint=SCHEMA, now_ms=1100)
    with pytest.raises(EnvelopeExpiredError, match="staleness"):
        codec.decode(
            encoded,
            expected_schema_fingerprint=SCHEMA,
            maximum_staleness_ms=50,
            now_ms=1050,
        )
    with pytest.raises(EnvelopeSchemaMismatchError):
        codec.decode(encoded, expected_schema_fingerprint="sha256:" + "2" * 64, now_ms=1001)
    corrupted = encoded[:-1] + bytes([encoded[-1] ^ 1])
    with pytest.raises(EnvelopeCorruptionError, match="digest"):
        codec.decode(corrupted, expected_schema_fingerprint=SCHEMA, now_ms=1001)
    with pytest.raises(EnvelopeCorruptionError):
        codec.extract_version(b"not-an-envelope")


def test_envelope_rejects_noncanonical_header_and_size_overflow() -> None:
    codec = EnvelopeCodec(maximum_value_bytes=1024, clock_ms=lambda: 1000)
    with pytest.raises(EnvelopeTooLargeError):
        codec.encode("x" * 600, serializer_id="canonical-json", schema_fingerprint=SCHEMA, ttl_ms=1)

    encoded = codec.encode(1, serializer_id="canonical-json", schema_fingerprint=SCHEMA, ttl_ms=10)
    prefix_end = 6 + 71 + 1
    header_len = int(encoded[prefix_end : prefix_end + 8], 16)
    header_start = prefix_end + 9
    header = json.loads(encoded[header_start : header_start + header_len])
    noncanonical = json.dumps(header, indent=1).encode()
    rebuilt = (
        encoded[:prefix_end]
        + f"{len(noncanonical):08x}".encode()
        + b"|"
        + noncanonical
        + encoded[header_start + header_len :]
    )
    with pytest.raises(EnvelopeCorruptionError, match="canonical"):
        codec.decode(rebuilt, expected_schema_fingerprint=SCHEMA, now_ms=1001)


def _frame(header: dict[str, JsonValue], payload: bytes, *, recompute: bool = True) -> bytes:
    base = dict(header)
    base.pop("envelopeVersion", None)
    if recompute:
        version = sha256_hex(canonical_json_bytes(cast(JsonValue, base)) + b"\x00" + payload)
        header = {**base, "envelopeVersion": version}
    else:
        version = cast(str, header["envelopeVersion"])
    encoded = canonical_json_bytes(cast(JsonValue, header))
    prefix = b"MRVK1|" + version.encode() + b"|" + f"{len(encoded):08x}".encode() + b"|"
    return prefix + encoded + payload


def _header(payload: bytes = b"1") -> dict[str, JsonValue]:
    return {
        "formatVersion": ENVELOPE_FORMAT_VERSION,
        "serializerId": "canonical-json",
        "schemaFingerprint": SCHEMA,
        "sourceVersion": None,
        "createdAtMs": 1000,
        "expiresAtMs": 2000,
        "payloadDigest": sha256_hex(payload),
        "payloadLength": len(payload),
    }


def test_codec_rejects_invalid_encode_and_decode_parameters() -> None:
    with pytest.raises(ValueError, match="positive"):
        EnvelopeCodec(maximum_value_bytes=0)
    codec = EnvelopeCodec(maximum_value_bytes=4096, clock_ms=lambda: 1000)
    invalid = [
        ({"serializer_id": "unknown", "ttl_ms": 1}, "unsupported"),
        ({"serializer_id": "canonical-json", "ttl_ms": 0}, "TTL"),
        ({"serializer_id": "canonical-json", "ttl_ms": 1, "source_version": False}, "source"),
        ({"serializer_id": "canonical-json", "ttl_ms": 1, "created_at_ms": -1}, "creation"),
    ]
    for options, message in invalid:
        with pytest.raises(EnvelopeError, match=message):
            codec.encode(1, schema_fingerprint=SCHEMA, **options)  # type: ignore[arg-type]
    with pytest.raises(EnvelopeError, match="finite"):
        codec.encode(
            float("nan"),  # type: ignore[arg-type]
            serializer_id="canonical-json",
            schema_fingerprint=SCHEMA,
            ttl_ms=1,
        )
    with pytest.raises(EnvelopeCorruptionError, match="framing"):
        codec.decode(b"", expected_schema_fingerprint=SCHEMA)
    valid = codec.encode(1, serializer_id="canonical-json", schema_fingerprint=SCHEMA, ttl_ms=10)
    with pytest.raises(EnvelopeError, match="staleness"):
        codec.decode(valid, expected_schema_fingerprint=SCHEMA, maximum_staleness_ms=0)


def test_codec_rejects_every_malformed_frame_layer() -> None:
    codec = EnvelopeCodec(maximum_value_bytes=4096, clock_ms=lambda: 1000)
    valid = codec.encode(1, serializer_id="canonical-json", schema_fingerprint=SCHEMA, ttl_ms=1000)
    with pytest.raises(EnvelopeCorruptionError, match="prefix is malformed"):
        codec.extract_version(valid[:77] + b"!" + valid[78:])
    with pytest.raises(EnvelopeCorruptionError, match="version is malformed"):
        codec.extract_version(b"MRVK1|" + b"x" * 71 + b"|")
    with pytest.raises(EnvelopeCorruptionError, match="truncated"):
        codec.decode(valid[:78], expected_schema_fingerprint=SCHEMA)
    with pytest.raises(EnvelopeCorruptionError, match="length is malformed"):
        codec.decode(valid[:78] + b"zzzzzzzz|{}", expected_schema_fingerprint=SCHEMA)
    with pytest.raises(EnvelopeCorruptionError, match="inconsistent"):
        codec.decode(valid[:78] + b"ffffffff|{}", expected_schema_fingerprint=SCHEMA)

    version = valid[6:77]
    for header_bytes, message in ((b"xx", "not JSON"), (b"[]", "must be an object")):
        prefix = b"MRVK1|" + version + b"|" + f"{len(header_bytes):08x}".encode() + b"|"
        framed = prefix + header_bytes
        with pytest.raises(EnvelopeCorruptionError, match=message):
            codec.decode(framed, expected_schema_fingerprint=SCHEMA)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("formatVersion", "future", "format version"),
        ("serializerId", "pickle", "serializer"),
        ("schemaFingerprint", "bad", "fingerprint"),
        ("createdAtMs", -1, "non-negative"),
        ("expiresAtMs", 999, "expiry"),
        ("payloadLength", 2, "length"),
    ],
)
def test_codec_rejects_invalid_header_fields(field: str, value: JsonValue, message: str) -> None:
    codec = EnvelopeCodec(maximum_value_bytes=4096, clock_ms=lambda: 1000)
    header = _header()
    header[field] = value
    with pytest.raises(EnvelopeCorruptionError, match=message):
        codec.decode(_frame(header, b"1"), expected_schema_fingerprint=SCHEMA, now_ms=1001)


def test_codec_rejects_missing_fields_and_invalid_source_version() -> None:
    codec = EnvelopeCodec(maximum_value_bytes=4096, clock_ms=lambda: 1000)
    missing = _header()
    missing.pop("payloadLength")
    with pytest.raises(EnvelopeCorruptionError, match="fields"):
        codec.decode(_frame(missing, b"1"), expected_schema_fingerprint=SCHEMA)
    source = _header()
    source["sourceVersion"] = False
    with pytest.raises(EnvelopeCorruptionError, match="sourceVersion"):
        codec.decode(_frame(source, b"1"), expected_schema_fingerprint=SCHEMA, now_ms=1001)
