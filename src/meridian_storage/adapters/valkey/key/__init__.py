# SPDX-License-Identifier: Apache-2.0
"""Versioned, scope-isolated physical Valkey key encoding."""

from __future__ import annotations

import binascii
from dataclasses import dataclass
from typing import cast

from meridian_storage.context import OperationContext
from meridian_storage.registry import ResourceRef
from meridian_storage.semantics import JsonValue, canonical_json_bytes

from .._canonical import base64url_digest, json_scalar_or_array, require_fingerprint

KEY_FORMAT_VERSION = "meridian-valkey-key.v1"


@dataclass(frozen=True, slots=True)
class KeyMaterial:
    context: OperationContext
    resource_ref: ResourceRef
    logical_key: JsonValue
    schema_fingerprint: str
    atomic_group: JsonValue | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "resource_ref", ResourceRef.parse(self.resource_ref))
        if self.resource_ref.catalog != "cache":
            raise ValueError("Valkey physical keys are restricted to the Cache Catalog")
        object.__setattr__(
            self, "logical_key", json_scalar_or_array(self.logical_key, "logical Cache key")
        )
        object.__setattr__(
            self,
            "schema_fingerprint",
            require_fingerprint(self.schema_fingerprint, "schema fingerprint"),
        )
        if self.atomic_group is not None:
            object.__setattr__(
                self,
                "atomic_group",
                json_scalar_or_array(self.atomic_group, "atomic group"),
            )


class KeyEncoder:
    """Build digest-only physical keys without exposing logical identifiers."""

    def __init__(
        self,
        physical_namespace: str,
        namespace_generation: int,
        *,
        maximum_key_bytes: int,
    ) -> None:
        if not physical_namespace or len(physical_namespace.encode("utf-8")) > 512:
            raise ValueError("physical namespace must be bounded and non-empty")
        if (
            isinstance(namespace_generation, bool)
            or not isinstance(namespace_generation, int)
            or namespace_generation < 1
        ):
            raise ValueError("namespace generation must be positive")
        self.namespace_generation = namespace_generation
        self.maximum_key_bytes = maximum_key_bytes
        namespace = base64url_digest(physical_namespace.encode("utf-8"))[:16]
        self._prefix = f"mrvk:v1:{namespace}:g{namespace_generation}"

    @property
    def safe_prefix(self) -> str:
        return self._prefix

    def encode(self, material: KeyMaterial) -> bytes:
        scope: dict[str, JsonValue] = {
            "tenant": material.context.tenant,
            "labels": cast(JsonValue, dict(sorted(material.context.scope.items()))),
        }
        body: dict[str, JsonValue] = {
            "formatVersion": KEY_FORMAT_VERSION,
            "scope": scope,
            "resource": cast(JsonValue, material.resource_ref.to_dict()),
            "logicalKey": material.logical_key,
            "schemaFingerprint": material.schema_fingerprint,
            "namespaceGeneration": self.namespace_generation,
        }
        digest = base64url_digest(canonical_json_bytes(cast(JsonValue, body)))
        if material.atomic_group is None:
            value = f"{self._prefix}:d:{digest}".encode("ascii")
        else:
            tag_body: dict[str, JsonValue] = {
                "formatVersion": KEY_FORMAT_VERSION,
                "scope": scope,
                "resource": cast(JsonValue, material.resource_ref.to_dict()),
                "atomicGroup": material.atomic_group,
                "namespaceGeneration": self.namespace_generation,
            }
            tag = base64url_digest(canonical_json_bytes(cast(JsonValue, tag_body)))[:22]
            value = f"{self._prefix}:{{{tag}}}:d:{digest}".encode("ascii")
        if len(value) > self.maximum_key_bytes:
            raise ValueError("encoded physical key exceeds the configured key limit")
        return value

    def lease_key(self, key: bytes) -> bytes:
        digest = base64url_digest(key)
        tag = _hash_tag(key)
        if tag is None:
            value = f"{self._prefix}:l:{digest}".encode("ascii")
        else:
            value = f"{self._prefix}:{{{tag.decode('ascii')}}}:l:{digest}".encode("ascii")
        if len(value) > self.maximum_key_bytes:
            raise ValueError("encoded lease key exceeds the configured key limit")
        return value


def _hash_tag(key: bytes) -> bytes | None:
    start = key.find(b"{")
    if start < 0:
        return None
    end = key.find(b"}", start + 1)
    if end <= start + 1:
        return None
    return key[start + 1 : end]


def hash_slot(key: bytes) -> int:
    """Return the Valkey Cluster slot for a physical key."""

    tagged = _hash_tag(key)
    return binascii.crc_hqx(tagged if tagged is not None else key, 0) % 16_384


def require_shared_hash_slot(keys: tuple[bytes, ...]) -> int:
    if not keys:
        raise ValueError("at least one key is required")
    slots = {hash_slot(key) for key in keys}
    if len(slots) != 1:
        raise ValueError("multi-key atomic groups must share one Valkey hash slot")
    return next(iter(slots))


__all__ = [
    "KEY_FORMAT_VERSION",
    "KeyEncoder",
    "KeyMaterial",
    "hash_slot",
    "require_shared_hash_slot",
]
