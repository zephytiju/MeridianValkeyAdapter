# SPDX-License-Identifier: Apache-2.0
"""Deterministic, self-validating Valkey value envelopes."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import cast

from meridian_storage.semantics import JsonValue, canonical_json_bytes

from .._canonical import require_fingerprint, sha256_hex
from ..configuration import (
    SERIALIZER_CANONICAL_JSON,
    SERIALIZER_RAW_BYTES,
    SUPPORTED_SERIALIZERS,
)

ENVELOPE_FORMAT_VERSION = "meridian-valkey-envelope.v1"
_MAGIC = b"MRVK1|"
_VERSION_BYTES = 71  # len("sha256:") + 64 lowercase hex characters
_HEADER_LENGTH_BYTES = 8


class EnvelopeError(ValueError):
    """Base class for bounded envelope validation failures."""


class EnvelopeCorruptionError(EnvelopeError):
    """Stored bytes fail framing, length, canonicalization, or digest checks."""


class EnvelopeExpiredError(EnvelopeError):
    """Stored bytes exceeded their TTL or maximum-staleness bound."""


class EnvelopeSchemaMismatchError(EnvelopeError):
    """Stored bytes were written against another Schema fingerprint."""


class EnvelopeTooLargeError(EnvelopeError):
    """Payload or complete envelope exceeds the configured byte bound."""


@dataclass(frozen=True, slots=True)
class DecodedEnvelope:
    value: JsonValue | bytes
    serializer_id: str
    schema_fingerprint: str
    source_version: str | int | None
    created_at_ms: int
    expires_at_ms: int
    payload_digest: str
    envelope_version: str

    @property
    def negative(self) -> bool:
        return self.serializer_id == SERIALIZER_CANONICAL_JSON and self.value is None


class EnvelopeCodec:
    """Encode and verify canonical JSON or raw-byte payloads."""

    def __init__(
        self,
        *,
        maximum_value_bytes: int,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        if maximum_value_bytes < 1:
            raise ValueError("maximum value bytes must be positive")
        self.maximum_value_bytes = maximum_value_bytes
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)

    def encode(
        self,
        value: JsonValue | bytes | bytearray | memoryview,
        *,
        serializer_id: str,
        schema_fingerprint: str,
        ttl_ms: int,
        source_version: str | int | None = None,
        created_at_ms: int | None = None,
    ) -> bytes:
        if serializer_id not in SUPPORTED_SERIALIZERS:
            raise EnvelopeError("unsupported Valkey serializer")
        schema = require_fingerprint(schema_fingerprint, "schema fingerprint")
        if isinstance(ttl_ms, bool) or not isinstance(ttl_ms, int) or ttl_ms <= 0:
            raise EnvelopeError("envelope TTL must be positive")
        if source_version is not None and (
            isinstance(source_version, bool) or not isinstance(source_version, str | int)
        ):
            raise EnvelopeError("source version must be a string, integer, or null")
        created = self._clock_ms() if created_at_ms is None else created_at_ms
        if isinstance(created, bool) or not isinstance(created, int) or created < 0:
            raise EnvelopeError("creation time must be a non-negative millisecond instant")
        if serializer_id == SERIALIZER_CANONICAL_JSON:
            if isinstance(value, bytes | bytearray | memoryview):
                raise EnvelopeError("canonical-json cannot encode a byte payload")
            try:
                payload = canonical_json_bytes(value)
            except (TypeError, ValueError) as exc:
                raise EnvelopeError("value is not canonical finite JSON") from exc
        elif serializer_id == SERIALIZER_RAW_BYTES:
            if not isinstance(value, bytes | bytearray | memoryview):
                raise EnvelopeError("raw-bytes requires a byte payload")
            payload = bytes(value)
        if len(payload) > self.maximum_value_bytes:
            raise EnvelopeTooLargeError("payload exceeds the configured value limit")
        base_header: dict[str, JsonValue] = {
            "formatVersion": ENVELOPE_FORMAT_VERSION,
            "serializerId": serializer_id,
            "schemaFingerprint": schema,
            "sourceVersion": source_version,
            "createdAtMs": created,
            "expiresAtMs": created + ttl_ms,
            "payloadDigest": sha256_hex(payload),
            "payloadLength": len(payload),
        }
        version = sha256_hex(canonical_json_bytes(base_header) + b"\x00" + payload)
        header = {**base_header, "envelopeVersion": version}
        header_bytes = canonical_json_bytes(cast(JsonValue, header))
        if len(header_bytes) > 0xFFFFFFFF:
            raise EnvelopeTooLargeError("envelope header exceeds its framing bound")
        framed = (
            _MAGIC
            + version.encode("ascii")
            + b"|"
            + f"{len(header_bytes):08x}".encode("ascii")
            + b"|"
            + header_bytes
            + payload
        )
        if len(framed) > self.maximum_value_bytes:
            raise EnvelopeTooLargeError("complete envelope exceeds the configured value limit")
        return framed

    def decode(
        self,
        value: bytes | bytearray | memoryview,
        *,
        expected_schema_fingerprint: str,
        maximum_staleness_ms: int | None = None,
        now_ms: int | None = None,
    ) -> DecodedEnvelope:
        raw = bytes(value)
        if not raw or len(raw) > self.maximum_value_bytes:
            raise EnvelopeCorruptionError("envelope exceeds its configured framing bound")
        version, header, payload = self._parse(raw)
        schema = require_fingerprint(expected_schema_fingerprint, "expected schema fingerprint")
        if header["schemaFingerprint"] != schema:
            raise EnvelopeSchemaMismatchError(
                "envelope Schema fingerprint does not match the Resource"
            )
        current = self._clock_ms() if now_ms is None else now_ms
        expires = cast(int, header["expiresAtMs"])
        created = cast(int, header["createdAtMs"])
        if expires <= current:
            raise EnvelopeExpiredError("envelope TTL has expired")
        if maximum_staleness_ms is not None:
            if maximum_staleness_ms <= 0:
                raise EnvelopeError("maximum staleness must be positive")
            if created + maximum_staleness_ms <= current:
                raise EnvelopeExpiredError("envelope exceeded maximum staleness")
        serializer = cast(str, header["serializerId"])
        if serializer == SERIALIZER_CANONICAL_JSON:
            try:
                decoded = cast(JsonValue, json.loads(payload.decode("utf-8")))
                if canonical_json_bytes(decoded) != payload:
                    raise EnvelopeCorruptionError("JSON payload is not canonical")
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
                if isinstance(exc, EnvelopeCorruptionError):
                    raise
                raise EnvelopeCorruptionError("JSON payload cannot be decoded canonically") from exc
            result: JsonValue | bytes = decoded
        else:
            result = payload
        return DecodedEnvelope(
            value=result,
            serializer_id=serializer,
            schema_fingerprint=header["schemaFingerprint"],
            source_version=cast(str | int | None, header["sourceVersion"]),
            created_at_ms=created,
            expires_at_ms=expires,
            payload_digest=cast(str, header["payloadDigest"]),
            envelope_version=version,
        )

    @staticmethod
    def extract_version(value: bytes | bytearray | memoryview) -> str:
        raw = bytes(value)
        minimum = len(_MAGIC) + _VERSION_BYTES + 1
        if len(raw) < minimum or not raw.startswith(_MAGIC):
            raise EnvelopeCorruptionError("envelope has no valid version prefix")
        version = raw[len(_MAGIC) : len(_MAGIC) + _VERSION_BYTES]
        if raw[len(_MAGIC) + _VERSION_BYTES : minimum] != b"|":
            raise EnvelopeCorruptionError("envelope version prefix is malformed")
        try:
            return require_fingerprint(version.decode("ascii"), "envelope version")
        except (UnicodeDecodeError, ValueError) as exc:
            raise EnvelopeCorruptionError("envelope version is malformed") from exc

    def _parse(self, raw: bytes) -> tuple[str, Mapping[str, JsonValue], bytes]:
        version = self.extract_version(raw)
        offset = len(_MAGIC) + _VERSION_BYTES + 1
        length_end = offset + _HEADER_LENGTH_BYTES
        if len(raw) <= length_end or raw[length_end : length_end + 1] != b"|":
            raise EnvelopeCorruptionError("envelope header framing is truncated")
        try:
            header_length = int(raw[offset:length_end].decode("ascii"), 16)
        except (UnicodeDecodeError, ValueError) as exc:
            raise EnvelopeCorruptionError("envelope header length is malformed") from exc
        header_start = length_end + 1
        header_end = header_start + header_length
        if header_length < 2 or header_end > len(raw):
            raise EnvelopeCorruptionError("envelope header length is inconsistent")
        header_bytes = raw[header_start:header_end]
        payload = raw[header_end:]
        try:
            parsed = json.loads(header_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EnvelopeCorruptionError("envelope header is not JSON") from exc
        if not isinstance(parsed, Mapping) or any(not isinstance(key, str) for key in parsed):
            raise EnvelopeCorruptionError("envelope header must be an object")
        header = cast(Mapping[str, JsonValue], parsed)
        required = {
            "formatVersion",
            "serializerId",
            "schemaFingerprint",
            "sourceVersion",
            "createdAtMs",
            "expiresAtMs",
            "payloadDigest",
            "payloadLength",
            "envelopeVersion",
        }
        if set(header) != required:
            raise EnvelopeCorruptionError("envelope header fields do not match V1")
        if header["formatVersion"] != ENVELOPE_FORMAT_VERSION:
            raise EnvelopeCorruptionError("envelope format version is unsupported")
        if header["serializerId"] not in SUPPORTED_SERIALIZERS:
            raise EnvelopeCorruptionError("envelope serializer is unsupported")
        try:
            require_fingerprint(header["schemaFingerprint"], "schema fingerprint")
            require_fingerprint(header["payloadDigest"], "payload digest")
            require_fingerprint(header["envelopeVersion"], "envelope version")
        except ValueError as exc:
            raise EnvelopeCorruptionError(str(exc)) from exc
        for field_name in ("createdAtMs", "expiresAtMs", "payloadLength"):
            item = header[field_name]
            if isinstance(item, bool) or not isinstance(item, int) or item < 0:
                raise EnvelopeCorruptionError(f"{field_name} must be a non-negative integer")
        if cast(int, header["expiresAtMs"]) <= cast(int, header["createdAtMs"]):
            raise EnvelopeCorruptionError("envelope expiry must follow creation")
        if header["payloadLength"] != len(payload):
            raise EnvelopeCorruptionError("envelope payload length does not match")
        if header["payloadDigest"] != sha256_hex(payload):
            raise EnvelopeCorruptionError("envelope payload digest does not match")
        if header["envelopeVersion"] != version:
            raise EnvelopeCorruptionError("envelope version prefix does not match its header")
        base_header = dict(header)
        del base_header["envelopeVersion"]
        computed = sha256_hex(
            canonical_json_bytes(cast(JsonValue, base_header)) + b"\x00" + payload
        )
        if computed != version:
            raise EnvelopeCorruptionError("envelope version digest does not match")
        if canonical_json_bytes(cast(JsonValue, dict(header))) != header_bytes:
            raise EnvelopeCorruptionError("envelope header is not canonical")
        source = header["sourceVersion"]
        if source is not None and (isinstance(source, bool) or not isinstance(source, str | int)):
            raise EnvelopeCorruptionError("sourceVersion is malformed")
        return version, header, payload


__all__ = [
    "ENVELOPE_FORMAT_VERSION",
    "DecodedEnvelope",
    "EnvelopeCodec",
    "EnvelopeCorruptionError",
    "EnvelopeError",
    "EnvelopeExpiredError",
    "EnvelopeSchemaMismatchError",
    "EnvelopeTooLargeError",
]
