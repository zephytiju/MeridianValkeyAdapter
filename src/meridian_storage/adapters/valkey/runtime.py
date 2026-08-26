# SPDX-License-Identifier: Apache-2.0
"""Meridian Core 1.0.0 Adapter factory, runtime, and Cache session."""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import cast

from meridian_storage.errors import (
    CompatibilityError,
    ConfigurationError,
    ErrorCode,
    LifecycleError,
    ValidationError,
)
from meridian_storage.semantics import JsonValue, canonical_json_bytes
from meridian_storage.spi import (
    AdapterCreateContext,
    AdapterProbe,
    ExecutionRequest,
    ExecutionResult,
    PhysicalResource,
    PhysicalVerification,
)

from meridian_storage import Operation
from valkey.exceptions import ValkeyError

from .cache import CacheEventSink, CacheMetrics, ValkeyCache
from .client import (
    AddressMapper,
    ClientBuilder,
    ClientHandle,
    ServiceResolver,
    create_client_handle,
)
from .codec import DecodedEnvelope, EnvelopeCodec, EnvelopeError
from .configuration import SERIALIZER_RAW_BYTES, TOPOLOGY_SENTINEL, ValkeySettings
from .descriptor import (
    ADAPTER_ID,
    COMPARE_AND_SET_CONTRACT,
    DELETE_CONTRACT,
    ENGINE_PROFILE_SENTINEL,
    ENGINE_PROFILE_STANDALONE,
    GET_CONTRACT,
    INVALIDATE_CONTRACT,
    OPERATION_VERSION,
    PUT_CONTRACT,
    PUT_IF_ABSENT_CONTRACT,
    capability_manifest,
)
from .errors import translate_engine_error
from .key import KeyEncoder
from .probe import ValkeyProbe


class ValkeyAdapterFactory:
    """Discoverable factory for the package's sole Adapter implementation."""

    def __init__(
        self,
        *,
        service_resolver: ServiceResolver | None = None,
        client_builder: ClientBuilder | None = None,
        address_mapper: AddressMapper | None = None,
        event_sink: CacheEventSink | None = None,
    ) -> None:
        self._service_resolver = service_resolver
        self._client_builder = client_builder
        self._address_mapper = address_mapper
        self._event_sink = event_sink

    @property
    def adapter_id(self) -> str:
        return ADAPTER_ID

    def create(self, context: AdapterCreateContext) -> ValkeyAdapterRuntime:
        binding = context.binding
        if binding.adapter_id != ADAPTER_ID:
            raise ConfigurationError(
                ErrorCode.CONFIG_INVALID, "Binding Adapter identity does not select Valkey"
            )
        settings = ValkeySettings.from_mapping(binding.settings)
        expected_profile = (
            ENGINE_PROFILE_SENTINEL
            if settings.topology.mode == TOPOLOGY_SENTINEL
            else ENGINE_PROFILE_STANDALONE
        )
        if binding.engine_profile != expected_profile:
            raise ConfigurationError(
                ErrorCode.CONFIG_INVALID,
                "Valkey Binding Engine profile differs from its topology settings",
            )
        handle = create_client_handle(
            context,
            settings,
            service_resolver=self._service_resolver,
            client_builder=self._client_builder,
            address_mapper=self._address_mapper,
        )
        return ValkeyAdapterRuntime(context, handle, settings, event_sink=self._event_sink)


class ValkeyAdapterRuntime:
    def __init__(
        self,
        context: AdapterCreateContext,
        handle: ClientHandle,
        settings: ValkeySettings,
        *,
        event_sink: CacheEventSink | None = None,
    ) -> None:
        self._context = context
        self._handle = handle
        self._settings = settings
        self._probe = ValkeyProbe(handle.client, settings, tls_mode=context.binding.tls.mode)
        encoder = KeyEncoder(
            context.binding.physical_namespace,
            settings.namespace_generation,
            maximum_key_bytes=settings.limits.max_key_bytes,
        )
        codec = EnvelopeCodec(maximum_value_bytes=settings.limits.max_value_bytes)
        self._metrics = CacheMetrics()
        self._cache = ValkeyCache(
            handle.client,
            encoder,
            codec,
            settings,
            event_sink=event_sink,
            metrics=self._metrics,
        )
        self._opened = False
        self._closed = False
        self._manifest: AdapterProbe | None = None

    @property
    def cache(self) -> ValkeyCache:
        """Internal composition hook for transparent cache-aside reads."""

        self._ensure_open()
        return self._cache

    @property
    def metrics(self) -> Mapping[str, int]:
        return self._metrics.snapshot()

    def open(self) -> None:
        self._ensure_not_closed()
        if self._opened:
            return
        try:
            probe = self._probe.probe()
        except ValkeyError as exc:
            translate_engine_error(exc)
        binding = self._context.binding
        if probe.manifest.engine_version != binding.engine_version:
            raise CompatibilityError(
                ErrorCode.CAPABILITY_UNSUPPORTED,
                "probed Valkey version differs from the Binding pin",
            )
        if probe.manifest.fingerprint != binding.required_capability_fingerprint:
            raise CompatibilityError(
                ErrorCode.CAPABILITY_FINGERPRINT,
                "Valkey capability fingerprint differs from the Binding pin",
            )
        self._manifest = probe
        self._opened = True

    def probe(self) -> AdapterProbe:
        self._ensure_open()
        try:
            probe = self._probe.probe()
        except ValkeyError as exc:
            translate_engine_error(exc)
        startup_probe = self._manifest
        if startup_probe is None:
            raise LifecycleError(ErrorCode.RUNTIME_STATE, "Valkey startup probe is absent")
        if probe.manifest.fingerprint != startup_probe.manifest.fingerprint:
            raise CompatibilityError(
                ErrorCode.CAPABILITY_FINGERPRINT,
                "Valkey capability changed after startup",
            )
        return probe

    def verify_physical(self, resources: tuple[PhysicalResource, ...]) -> PhysicalVerification:
        self._ensure_open()
        return self._probe.verify_physical(resources)

    def open_session(self, *, transactional: bool) -> ValkeyAdapterSession:
        self._ensure_open()
        if transactional:
            raise ValidationError(
                ErrorCode.CAPABILITY_UNSUPPORTED,
                "disposable Valkey Cache does not provide authoritative transactions",
            )
        return ValkeyAdapterSession(
            self._cache,
            self._context.binding.id,
            max_result_bytes=self._context.binding.client.max_result_bytes,
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._opened = False
        self._handle.close()

    def _ensure_not_closed(self) -> None:
        if self._closed:
            raise LifecycleError(ErrorCode.RUNTIME_CLOSED, "Valkey runtime is closed")

    def _ensure_open(self) -> None:
        self._ensure_not_closed()
        if not self._opened:
            raise LifecycleError(ErrorCode.RUNTIME_STATE, "Valkey runtime is not open")


class ValkeyAdapterSession:
    def __init__(self, cache: ValkeyCache, binding_id: str, *, max_result_bytes: int) -> None:
        self._cache = cache
        self._binding_id = binding_id
        self._max_result_bytes = max_result_bytes
        self._closed = False

    def begin(self) -> None:
        raise ValidationError(
            ErrorCode.CAPABILITY_UNSUPPORTED,
            "Valkey Cache sessions are non-transactional",
        )

    def execute(self, request: ExecutionRequest) -> ExecutionResult:
        if self._closed:
            raise LifecycleError(ErrorCode.RUNTIME_CLOSED, "Valkey session is closed")
        if request.binding_id != self._binding_id:
            raise ValidationError(
                ErrorCode.OPERATION_SCOPE, "request Binding does not match session"
            )
        operation = request.operation
        self._validate_operation(operation)
        resource_ref = operation.resources[0]
        logical = str(resource_ref)
        input_value = operation.input
        try:
            if operation.operation_contract == GET_CONTRACT:
                try:
                    lookup = self._cache.lookup(
                        request.context, resource_ref, cast(JsonValue, input_value["key"])
                    )
                    data: JsonValue = (
                        {"hit": True, "entry": _entry(lookup.envelope)}
                        if lookup.hit
                        else {"hit": False}
                    )
                    provenance = {"cacheOutcome": "hit" if lookup.hit else "miss"}
                except ValkeyError:
                    data = {"hit": False, "degraded": True}
                    provenance = {"cacheOutcome": "unavailable-miss"}
            elif operation.operation_contract == PUT_CONTRACT:
                envelope = self._cache.put(
                    request.context,
                    resource_ref,
                    cast(JsonValue, input_value["key"]),
                    _operation_value(self._cache, resource_ref, input_value["value"]),
                    requested_ttl_ms=_optional_int(input_value.get("ttlMs"), "ttlMs"),
                    source_version=_source_version(input_value.get("sourceVersion")),
                )
                data = {"stored": True, "entry": _entry(envelope)}
                provenance = {"cacheOutcome": "stored"}
            elif operation.operation_contract == PUT_IF_ABSENT_CONTRACT:
                stored, envelope = self._cache.put_if_absent(
                    request.context,
                    resource_ref,
                    cast(JsonValue, input_value["key"]),
                    _operation_value(self._cache, resource_ref, input_value["value"]),
                    requested_ttl_ms=_optional_int(input_value.get("ttlMs"), "ttlMs"),
                    source_version=_source_version(input_value.get("sourceVersion")),
                )
                data = {"stored": stored, "entry": _entry(envelope) if stored else None}
                provenance = {"cacheOutcome": "stored" if stored else "conflict"}
            elif operation.operation_contract == COMPARE_AND_SET_CONTRACT:
                expected = _required_version(input_value.get("expectedVersion"))
                result, cas_envelope = self._cache.compare_and_set(
                    request.context,
                    resource_ref,
                    cast(JsonValue, input_value["key"]),
                    expected,
                    _operation_value(self._cache, resource_ref, input_value["value"]),
                    requested_ttl_ms=_optional_int(input_value.get("ttlMs"), "ttlMs"),
                )
                data = {
                    "swapped": result.swapped,
                    "missing": result.missing,
                    "corrupt": result.corrupt,
                    "entry": _entry(cas_envelope) if cas_envelope is not None else None,
                }
                provenance = {"cacheOutcome": "swapped" if result.swapped else "conflict"}
            elif operation.operation_contract == DELETE_CONTRACT:
                deleted = self._cache.delete(
                    request.context, resource_ref, cast(JsonValue, input_value["key"])
                )
                data = {"deleted": deleted}
                provenance = {"cacheOutcome": "deleted" if deleted else "miss"}
            elif operation.operation_contract == INVALIDATE_CONTRACT:
                selector = input_value["selector"]
                keys = _invalidation_keys(selector, self._cache.settings.limits.max_batch_size)
                invalidated = sum(
                    self._cache.delete(request.context, resource_ref, key) for key in keys
                )
                data = {"invalidated": invalidated, "requested": len(keys)}
                provenance = {"cacheOutcome": "invalidated"}
            else:
                raise ValidationError(
                    ErrorCode.CAPABILITY_UNSUPPORTED,
                    "Valkey does not advertise this Cache Operation",
                    operation_contract=operation.operation_contract,
                )
        except ValkeyError as exc:
            translate_engine_error(
                exc,
                operation_contract=operation.operation_contract,
                resource_ref=logical,
                request_id=request.request_id,
                execution_id=request.execution_id,
            )
        except EnvelopeError as exc:
            raise ValidationError(
                ErrorCode.OPERATION_INVALID,
                "Cache value does not match the Binding serializer or size policy",
                operation_contract=operation.operation_contract,
                resource_ref=logical,
                request_id=request.request_id,
                execution_id=request.execution_id,
            ) from exc
        result_bytes = len(canonical_json_bytes(data))
        if result_bytes > self._max_result_bytes:
            raise ValidationError(
                ErrorCode.OPERATION_RESULT_LIMIT,
                "normalized Cache result exceeds the Binding result limit",
            )
        return ExecutionResult(
            data,
            result_bytes=result_bytes,
            provenance={
                "adapter": ADAPTER_ID,
                "consistency": "disposable-cache",
                "namespaceGeneration": str(self._cache.settings.namespace_generation),
                **provenance,
            },
        )

    def commit(self) -> None:
        raise ValidationError(
            ErrorCode.CAPABILITY_UNSUPPORTED, "Valkey Cache sessions cannot commit"
        )

    def rollback(self) -> None:
        raise ValidationError(
            ErrorCode.CAPABILITY_UNSUPPORTED, "Valkey Cache sessions cannot roll back"
        )

    def close(self) -> None:
        self._closed = True

    @staticmethod
    def _validate_operation(operation: Operation) -> None:
        if (
            operation.catalog != "cache"
            or operation.operation_version != OPERATION_VERSION
            or len(operation.resources) != 1
            or operation.operation_contract
            not in {
                GET_CONTRACT,
                PUT_CONTRACT,
                PUT_IF_ABSENT_CONTRACT,
                COMPARE_AND_SET_CONTRACT,
                DELETE_CONTRACT,
                INVALIDATE_CONTRACT,
            }
        ):
            raise ValidationError(
                ErrorCode.CAPABILITY_UNSUPPORTED,
                "Valkey accepts only one-Resource Cache data-plane Operations at V1",
                operation_contract=operation.operation_contract,
            )


def _entry(envelope: DecodedEnvelope | None) -> JsonValue:
    if envelope is None:
        raise ValidationError(ErrorCode.INTERNAL, "Cache entry envelope is absent")
    if isinstance(envelope.value, bytes):
        value: JsonValue = base64.urlsafe_b64encode(envelope.value).rstrip(b"=").decode("ascii")
        value_encoding: JsonValue = "base64url"
    else:
        value = envelope.value
        value_encoding = None
    return {
        "value": value,
        "valueEncoding": value_encoding,
        "serializerId": envelope.serializer_id,
        "schemaFingerprint": envelope.schema_fingerprint,
        "sourceVersion": envelope.source_version,
        "createdAt": _timestamp(envelope.created_at_ms),
        "expiresAt": _timestamp(envelope.expires_at_ms),
        "payloadDigest": envelope.payload_digest,
        "version": envelope.envelope_version,
    }


def _operation_value(
    cache: ValkeyCache,
    resource_ref: object,
    value: object,
) -> JsonValue | bytes:
    policy = cache.settings.policy_for(str(resource_ref))
    if policy.serializer_id != SERIALIZER_RAW_BYTES:
        return cast(JsonValue, value)
    if not isinstance(value, str):
        raise EnvelopeError("raw-byte Cache values must be unpadded base64url strings")
    try:
        padding = "=" * (-len(value) % 4)
        decoded = base64.b64decode(value + padding, altchars=b"-_", validate=True)
    except (ValueError, UnicodeEncodeError) as exc:
        raise EnvelopeError("raw-byte Cache value is not valid base64url") from exc
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii")
    if canonical != value:
        raise EnvelopeError("raw-byte Cache value is not canonical unpadded base64url")
    return decoded


def _timestamp(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _optional_int(value: object, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValidationError(ErrorCode.OPERATION_INVALID, f"{field_name} must be positive")
    return value


def _source_version(value: object) -> str | int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, str | int):
        raise ValidationError(
            ErrorCode.OPERATION_INVALID, "sourceVersion must be a string or integer"
        )
    return value


def _required_version(value: object) -> str | int:
    result = _source_version(value)
    if result is None:
        raise ValidationError(ErrorCode.OPERATION_INVALID, "expectedVersion is required")
    return result


def _invalidation_keys(value: object, maximum: int) -> tuple[JsonValue, ...]:
    if not isinstance(value, Mapping):
        raise ValidationError(ErrorCode.OPERATION_INVALID, "invalidate selector must be an object")
    if set(value) == {"key"}:
        return (cast(JsonValue, value["key"]),)
    if set(value) == {"keys"}:
        raw = value["keys"]
        if not isinstance(raw, Sequence) or isinstance(raw, str | bytes | bytearray):
            raise ValidationError(
                ErrorCode.OPERATION_INVALID, "invalidate selector keys must be an array"
            )
        if not raw or len(raw) > maximum:
            raise ValidationError(
                ErrorCode.OPERATION_INVALID,
                "invalidate selector exceeds the configured exact-key bound",
            )
        return tuple(cast(JsonValue, item) for item in raw)
    raise ValidationError(
        ErrorCode.OPERATION_INVALID,
        "invalidate supports exactly one key or one bounded keys array; scans are forbidden",
    )


def expected_capability_fingerprint(engine_version: str, settings: ValkeySettings) -> str:
    """Return the exact capability pin that Platform IaC places in a Binding."""

    return str(capability_manifest(engine_version, settings).fingerprint)


__all__ = [
    "ValkeyAdapterFactory",
    "ValkeyAdapterRuntime",
    "ValkeyAdapterSession",
    "expected_capability_fingerprint",
]
