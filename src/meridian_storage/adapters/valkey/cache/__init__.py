# SPDX-License-Identifier: Apache-2.0
"""Disposable Cache access and transparent cache-aside coordination."""

from __future__ import annotations

import secrets
import threading
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from meridian_storage.context import OperationContext
from meridian_storage.registry import ResourceRef
from meridian_storage.semantics import JsonValue

from valkey.exceptions import ValkeyError

from .._canonical import base64url_digest
from ..atomic import AtomicExecutor, CompareAndSetResult
from ..client import ClientProtocol
from ..codec import (
    DecodedEnvelope,
    EnvelopeCodec,
    EnvelopeCorruptionError,
    EnvelopeExpiredError,
    EnvelopeSchemaMismatchError,
)
from ..configuration import ResourceCachePolicy, ValkeySettings
from ..key import KeyEncoder, KeyMaterial


class CacheEventSink(Protocol):
    def emit(self, name: str, attributes: Mapping[str, str]) -> None: ...


class NullCacheEventSink:
    def emit(self, name: str, attributes: Mapping[str, str]) -> None:
        del name, attributes


class CacheMetrics:
    """Small thread-safe counter set; host telemetry may scrape or bridge it."""

    def __init__(self) -> None:
        self._values: Counter[str] = Counter()
        self._lock = threading.Lock()

    def increment(self, name: str) -> None:
        with self._lock:
            self._values[name] += 1

    def snapshot(self) -> Mapping[str, int]:
        with self._lock:
            return dict(sorted(self._values.items()))


@dataclass(frozen=True, slots=True)
class CacheLookup:
    hit: bool
    value: JsonValue | bytes | None = None
    envelope: DecodedEnvelope | None = None
    degraded: bool = False
    loaded: bool = False

    @property
    def negative(self) -> bool:
        return self.hit and (
            self.envelope.negative if self.envelope is not None else self.value is None
        )


class ValkeyCache:
    """Typed Cache operations over a single deployment Binding."""

    def __init__(
        self,
        client: ClientProtocol,
        key_encoder: KeyEncoder,
        codec: EnvelopeCodec,
        settings: ValkeySettings,
        *,
        event_sink: CacheEventSink | None = None,
        metrics: CacheMetrics | None = None,
    ) -> None:
        self.client = client
        self.key_encoder = key_encoder
        self.codec = codec
        self.settings = settings
        self.atomic = AtomicExecutor(client, codec)
        self.events = event_sink or NullCacheEventSink()
        self.metrics = metrics or CacheMetrics()

    def physical_key(
        self,
        context: OperationContext,
        resource_ref: ResourceRef,
        logical_key: JsonValue,
        *,
        atomic_group: JsonValue | None = None,
    ) -> tuple[bytes, ResourceCachePolicy]:
        logical = str(resource_ref)
        policy = self.settings.policy_for(logical)
        key = self.key_encoder.encode(
            KeyMaterial(
                context=context,
                resource_ref=resource_ref,
                logical_key=logical_key,
                schema_fingerprint=policy.schema_fingerprint,
                atomic_group=atomic_group,
            )
        )
        return key, policy

    def lookup(
        self,
        context: OperationContext,
        resource_ref: ResourceRef,
        logical_key: JsonValue,
    ) -> CacheLookup:
        key, policy = self.physical_key(context, resource_ref, logical_key)
        raw = self.client.get(key)
        if raw is None:
            self.metrics.increment("miss")
            return CacheLookup(False)
        if not isinstance(raw, bytes | bytearray | memoryview):
            self._discard_corrupt(key, resource_ref, "unexpected-engine-type")
            return CacheLookup(False)
        try:
            decoded = self.codec.decode(
                raw,
                expected_schema_fingerprint=policy.schema_fingerprint,
                maximum_staleness_ms=policy.maximum_staleness_ms,
            )
        except EnvelopeExpiredError:
            self.metrics.increment("stale")
            self._best_effort_delete(key)
            self._event("stale", resource_ref)
            return CacheLookup(False)
        except (EnvelopeCorruptionError, EnvelopeSchemaMismatchError):
            self._discard_corrupt(key, resource_ref, "envelope-validation")
            return CacheLookup(False)
        self.metrics.increment("hit")
        self._event("hit", resource_ref)
        return CacheLookup(True, decoded.value, decoded)

    def batch_lookup(
        self,
        context: OperationContext,
        resource_ref: ResourceRef,
        logical_keys: Sequence[JsonValue],
    ) -> tuple[CacheLookup, ...]:
        if not logical_keys or len(logical_keys) > self.settings.limits.max_batch_size:
            raise ValueError("bounded multi-get exceeds the configured batch size")
        pairs = tuple(self.physical_key(context, resource_ref, key) for key in logical_keys)
        physical = tuple(pair[0] for pair in pairs)
        raw_values = self.atomic.batch_get(physical)
        results: list[CacheLookup] = []
        for key, policy, raw in zip(physical, (pair[1] for pair in pairs), raw_values, strict=True):
            if raw is None:
                self.metrics.increment("miss")
                results.append(CacheLookup(False))
                continue
            if not isinstance(raw, bytes | bytearray | memoryview):
                self._discard_corrupt(key, resource_ref, "unexpected-engine-type")
                results.append(CacheLookup(False))
                continue
            try:
                decoded = self.codec.decode(
                    raw,
                    expected_schema_fingerprint=policy.schema_fingerprint,
                    maximum_staleness_ms=policy.maximum_staleness_ms,
                )
            except EnvelopeExpiredError:
                self.metrics.increment("stale")
                self._best_effort_delete(key)
                results.append(CacheLookup(False))
            except (EnvelopeCorruptionError, EnvelopeSchemaMismatchError):
                self._discard_corrupt(key, resource_ref, "envelope-validation")
                results.append(CacheLookup(False))
            else:
                self.metrics.increment("hit")
                results.append(CacheLookup(True, decoded.value, decoded))
        return tuple(results)

    def put(
        self,
        context: OperationContext,
        resource_ref: ResourceRef,
        logical_key: JsonValue,
        value: JsonValue | bytes,
        *,
        requested_ttl_ms: int | None = None,
        source_version: str | int | None = None,
    ) -> DecodedEnvelope:
        key, policy = self.physical_key(context, resource_ref, logical_key)
        negative = value is None
        ttl = self.settings.effective_ttl_ms(
            policy, requested_ttl_ms, negative=negative and policy.negative_caching
        )
        envelope = self.codec.encode(
            value,
            serializer_id=policy.serializer_id,
            schema_fingerprint=policy.schema_fingerprint,
            ttl_ms=ttl,
            source_version=source_version,
        )
        self.client.set(key, envelope, px=ttl)
        self.metrics.increment("put")
        self._event("put", resource_ref)
        return self.codec.decode(
            envelope,
            expected_schema_fingerprint=policy.schema_fingerprint,
            maximum_staleness_ms=policy.maximum_staleness_ms,
        )

    def put_if_absent(
        self,
        context: OperationContext,
        resource_ref: ResourceRef,
        logical_key: JsonValue,
        value: JsonValue | bytes,
        *,
        requested_ttl_ms: int | None = None,
        source_version: str | int | None = None,
    ) -> tuple[bool, DecodedEnvelope]:
        key, policy = self.physical_key(context, resource_ref, logical_key)
        ttl = self.settings.effective_ttl_ms(
            policy,
            requested_ttl_ms,
            negative=value is None and policy.negative_caching,
        )
        envelope = self.codec.encode(
            value,
            serializer_id=policy.serializer_id,
            schema_fingerprint=policy.schema_fingerprint,
            ttl_ms=ttl,
            source_version=source_version,
        )
        stored = self.atomic.put_if_absent(key, envelope, ttl)
        self.metrics.increment("put_if_absent_stored" if stored else "put_if_absent_conflict")
        return stored, self.codec.decode(
            envelope,
            expected_schema_fingerprint=policy.schema_fingerprint,
            maximum_staleness_ms=policy.maximum_staleness_ms,
        )

    def compare_and_set(
        self,
        context: OperationContext,
        resource_ref: ResourceRef,
        logical_key: JsonValue,
        expected_version: str | int,
        value: JsonValue | bytes,
        *,
        requested_ttl_ms: int | None = None,
    ) -> tuple[CompareAndSetResult, DecodedEnvelope | None]:
        key, policy = self.physical_key(context, resource_ref, logical_key)
        raw = self.client.get(key)
        if raw is None:
            return CompareAndSetResult(False, missing=True), None
        if not isinstance(raw, bytes | bytearray | memoryview):
            self._discard_corrupt(key, resource_ref, "unexpected-engine-type")
            return CompareAndSetResult(False, corrupt=True), None
        try:
            current = self.codec.decode(
                raw,
                expected_schema_fingerprint=policy.schema_fingerprint,
                maximum_staleness_ms=policy.maximum_staleness_ms,
            )
        except (
            EnvelopeCorruptionError,
            EnvelopeExpiredError,
            EnvelopeSchemaMismatchError,
        ):
            self._discard_corrupt(key, resource_ref, "envelope-validation")
            return CompareAndSetResult(False, corrupt=True), None
        if expected_version not in {current.envelope_version, current.source_version}:
            self.metrics.increment("cas_conflict")
            return CompareAndSetResult(False), current
        ttl = self.settings.effective_ttl_ms(policy, requested_ttl_ms)
        envelope = self.codec.encode(
            value,
            serializer_id=policy.serializer_id,
            schema_fingerprint=policy.schema_fingerprint,
            ttl_ms=ttl,
            source_version=current.source_version,
        )
        result = self.atomic.compare_and_set(key, current.envelope_version, envelope, ttl)
        self.metrics.increment("cas_swapped" if result.swapped else "cas_conflict")
        decoded = (
            self.codec.decode(
                envelope,
                expected_schema_fingerprint=policy.schema_fingerprint,
                maximum_staleness_ms=policy.maximum_staleness_ms,
            )
            if result.swapped
            else current
        )
        return result, decoded

    def delete(
        self,
        context: OperationContext,
        resource_ref: ResourceRef,
        logical_key: JsonValue,
    ) -> bool:
        key, _ = self.physical_key(context, resource_ref, logical_key)
        deleted = bool(self.client.delete(key))
        self.metrics.increment("delete")
        self._event("delete", resource_ref)
        return deleted

    def get_or_load(
        self,
        context: OperationContext,
        resource_ref: ResourceRef,
        logical_key: JsonValue,
        loader: Callable[[], JsonValue | bytes],
        *,
        requested_ttl_ms: int | None = None,
        source_version: str | int | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> CacheLookup:
        """Return a valid hit or safely load from authority outside atomic sections."""

        try:
            hit = self.lookup(context, resource_ref, logical_key)
        except ValkeyError:
            self.metrics.increment("unavailable_miss")
            self._event("unavailable-miss", resource_ref)
            return CacheLookup(False, loader(), degraded=True, loaded=True)
        if hit.hit:
            return hit
        key, policy = self.physical_key(context, resource_ref, logical_key)
        lease_key = self.key_encoder.lease_key(key)
        token = secrets.token_bytes(32)
        try:
            acquired = self.atomic.acquire_lease(
                lease_key, token, self.settings.single_flight.lease_ms
            )
        except ValkeyError:
            self.metrics.increment("unavailable_miss")
            return CacheLookup(False, loader(), degraded=True, loaded=True)
        if acquired:
            try:
                value = loader()
                self.metrics.increment("load")
                if value is None and not policy.negative_caching:
                    return CacheLookup(False, value, loaded=True)
                try:
                    envelope = self.put(
                        context,
                        resource_ref,
                        logical_key,
                        value,
                        requested_ttl_ms=requested_ttl_ms,
                        source_version=source_version,
                    )
                except ValkeyError:
                    self.metrics.increment("population_failure")
                    self._event("population-failure", resource_ref)
                    return CacheLookup(False, value, degraded=True, loaded=True)
                return CacheLookup(False, value, envelope, loaded=True)
            finally:
                try:
                    self.atomic.release_lease(lease_key, token)
                except ValkeyError:
                    self.metrics.increment("lease_release_failure")
        self.metrics.increment("single_flight_contention")
        deadline = monotonic() + self.settings.single_flight.wait_ms / 1000
        while monotonic() < deadline:
            sleep(self.settings.single_flight.poll_ms / 1000)
            try:
                peer = self.lookup(context, resource_ref, logical_key)
            except ValkeyError:
                break
            if peer.hit:
                return peer
        value = loader()
        self.metrics.increment("duplicate_load")
        if value is not None or policy.negative_caching:
            try:
                envelope = self.put(
                    context,
                    resource_ref,
                    logical_key,
                    value,
                    requested_ttl_ms=requested_ttl_ms,
                    source_version=source_version,
                )
            except ValkeyError:
                return CacheLookup(False, value, degraded=True, loaded=True)
            return CacheLookup(False, value, envelope, loaded=True)
        return CacheLookup(False, value, loaded=True)

    def _discard_corrupt(self, key: bytes, resource_ref: ResourceRef, reason: str) -> None:
        self.metrics.increment("corruption")
        self._best_effort_delete(key)
        self._event("corruption", resource_ref, reason=reason)

    def _best_effort_delete(self, key: bytes) -> None:
        try:
            self.client.delete(key)
        except ValkeyError:
            self.metrics.increment("cleanup_failure")

    def _event(self, name: str, resource_ref: ResourceRef, **attributes: str) -> None:
        safe = {
            "resourceDigest": base64url_digest(resource_ref.canonical.encode("utf-8"))[:22],
            **attributes,
        }
        self.events.emit(f"meridian.cache.{name}", safe)


__all__ = [
    "CacheEventSink",
    "CacheLookup",
    "CacheMetrics",
    "NullCacheEventSink",
    "ValkeyCache",
]
