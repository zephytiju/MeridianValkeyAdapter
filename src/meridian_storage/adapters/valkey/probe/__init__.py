# SPDX-License-Identifier: Apache-2.0
"""Authenticated, deterministic Valkey startup and physical probes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from meridian_storage.errors import CompatibilityError, ErrorCode, ValidationError
from meridian_storage.semantics import JsonValue, sha256_fingerprint
from meridian_storage.spi import AdapterProbe, PhysicalResource, PhysicalVerification

from .._canonical import base64url_digest
from ..atomic import CAS_SCRIPT_DIGEST, RELEASE_LEASE_SCRIPT_DIGEST
from ..client import ClientProtocol
from ..configuration import ValkeySettings
from ..descriptor import SUPPORTED_ENGINE_VERSIONS, capability_manifest


def _normalized_mapping(value: Mapping[object, object]) -> dict[str, object]:
    result: dict[str, object] = {}
    for raw_key, raw_value in value.items():
        key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else str(raw_key)
        if isinstance(raw_value, bytes):
            try:
                result[key] = raw_value.decode("utf-8")
            except UnicodeDecodeError:
                result[key] = "<binary>"
        else:
            result[key] = raw_value
    return result


def _text(value: object, field_name: str) -> str:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CompatibilityError(
                ErrorCode.CAPABILITY_UNSUPPORTED,
                f"Valkey probe field {field_name} is not UTF-8",
            ) from exc
    if not isinstance(value, str) or not value or len(value) > 512:
        raise CompatibilityError(
            ErrorCode.CAPABILITY_UNSUPPORTED,
            f"Valkey probe field {field_name} is absent or malformed",
        )
    return value


def _number(value: object, field_name: str) -> int:
    try:
        result = int(cast(str | int, value))
    except (TypeError, ValueError) as exc:
        raise CompatibilityError(
            ErrorCode.CAPABILITY_UNSUPPORTED,
            f"Valkey probe field {field_name} is not an integer",
        ) from exc
    if result < 0:
        raise CompatibilityError(
            ErrorCode.CAPABILITY_UNSUPPORTED,
            f"Valkey probe field {field_name} is negative",
        )
    return result


class ValkeyProbe:
    def __init__(self, client: ClientProtocol, settings: ValkeySettings, *, tls_mode: str) -> None:
        self._client = client
        self._settings = settings
        self._tls_mode = tls_mode

    def probe(self) -> AdapterProbe:
        if not self._client.ping():
            raise CompatibilityError(
                ErrorCode.CAPABILITY_UNSUPPORTED, "authenticated Valkey PING failed"
            )
        server = _normalized_mapping(self._client.info("server"))
        memory = _normalized_mapping(self._client.info("memory"))
        replication = _normalized_mapping(self._client.info("replication"))
        engine_version = _text(
            server.get("valkey_version", server.get("redis_version")), "valkey_version"
        )
        if engine_version not in SUPPORTED_ENGINE_VERSIONS:
            raise CompatibilityError(
                ErrorCode.CAPABILITY_UNSUPPORTED,
                "probed Valkey version is not advertised by this Adapter release",
            )
        maxmemory = _number(memory.get("maxmemory", 0), "maxmemory")
        config = _normalized_mapping(self._client.config_get("maxmemory-policy"))
        eviction = _text(config.get("maxmemory-policy"), "maxmemory-policy")
        if self._settings.memory.require_maxmemory and maxmemory == 0:
            raise CompatibilityError(
                ErrorCode.CAPABILITY_UNSUPPORTED,
                "Valkey maxmemory must be bounded for disposable Cache placement",
            )
        if eviction not in self._settings.memory.allowed_eviction_policies:
            raise CompatibilityError(
                ErrorCode.CAPABILITY_UNSUPPORTED,
                "Valkey eviction policy is incompatible with the selected Cache profile",
            )
        persistence = _normalized_mapping(self._client.config_get("appendonly"))
        persistence.update(_normalized_mapping(self._client.config_get("save")))
        appendonly = str(persistence.get("appendonly", "")).lower()
        save = str(persistence.get("save", ""))
        if self._settings.memory.require_persistence_disabled and (
            appendonly not in {"no", "0"} or save.strip()
        ):
            raise CompatibilityError(
                ErrorCode.CAPABILITY_UNSUPPORTED,
                "Valkey persistence must be disabled for the selected disposable profile",
            )
        role = _text(replication.get("role"), "role")
        replicas = _number(replication.get("connected_slaves", 0), "connected_slaves")
        if role != "master":
            raise CompatibilityError(
                ErrorCode.CAPABILITY_UNSUPPORTED,
                "Valkey Adapter must connect to the writable primary",
            )
        if replicas < self._settings.topology.minimum_replicas:
            raise CompatibilityError(
                ErrorCode.CAPABILITY_UNSUPPORTED,
                "Valkey replica count is below the selected topology requirement",
            )
        script_shas = (
            CAS_SCRIPT_DIGEST.removeprefix("sha1:"),
            RELEASE_LEASE_SCRIPT_DIGEST.removeprefix("sha1:"),
        )
        script_presence = tuple(bool(item) for item in self._client.script_exists(*script_shas))
        if len(script_presence) != 2:
            raise CompatibilityError(
                ErrorCode.CAPABILITY_UNSUPPORTED,
                "Valkey SCRIPT EXISTS response is malformed",
            )
        manifest = capability_manifest(engine_version, self._settings)
        return AdapterProbe(
            manifest,
            evidence={
                "authentication": "verified",
                "evictionPolicy": eviction,
                "maxmemoryBytes": str(maxmemory),
                "persistence": "disabled" if appendonly in {"no", "0"} and not save else "enabled",
                "replicas": str(replicas),
                "role": role,
                "scriptSupport": "verified",
                "tlsMode": self._tls_mode,
                "topology": self._settings.topology.mode,
            },
        )

    def verify_physical(self, resources: tuple[PhysicalResource, ...]) -> PhysicalVerification:
        if not resources:
            raise ValidationError(
                ErrorCode.OPERATION_INVALID,
                "Valkey physical verification requires at least one Cache Resource",
            )
        mappings: dict[str, str] = {}
        records: list[JsonValue] = []
        for resource in sorted(resources, key=lambda item: item.resource_ref):
            logical = str(resource.resource_ref)
            if resource.resource_ref.catalog != "cache" or resource.profile not in {
                "cache",
                "disposable-cache",
                "disposable-key-value",
            }:
                raise ValidationError(
                    ErrorCode.CAPABILITY_UNSUPPORTED,
                    "Valkey cannot provide authoritative or non-Cache semantics",
                    resource_ref=logical,
                )
            policy = self._settings.policy_for(logical)
            if resource.schema_fingerprint != policy.schema_fingerprint:
                raise ValidationError(
                    ErrorCode.PHYSICAL_FINGERPRINT,
                    "Valkey Resource Schema fingerprint differs from its Binding policy",
                    resource_ref=logical,
                )
            resource_digest = base64url_digest(logical.encode("utf-8"))[:22]
            mapping = f"mrvk:v1:g{self._settings.namespace_generation}:r:{resource_digest}"
            mappings[logical] = mapping
            records.append(
                {
                    "resource": cast(JsonValue, resource.resource_ref.to_dict()),
                    "resourceFingerprint": resource.resource_fingerprint,
                    "schemaFingerprint": resource.schema_fingerprint,
                    "profile": resource.profile,
                    "physicalMapping": mapping,
                }
            )
        fingerprint = sha256_fingerprint(
            {
                "formatVersion": "meridian-valkey-physical.v1",
                "consistencyClass": "disposable-cache",
                "namespaceGeneration": self._settings.namespace_generation,
                "resources": records,
                "topology": self._settings.topology.mode,
            }
        )
        return PhysicalVerification(
            fingerprint=fingerprint,
            mappings=mappings,
            evidence={
                "authority": "disposable-only",
                "namespaceGeneration": str(self._settings.namespace_generation),
                "resourceCount": str(len(resources)),
                "topology": self._settings.topology.mode,
            },
        )


__all__ = ["ValkeyProbe"]
